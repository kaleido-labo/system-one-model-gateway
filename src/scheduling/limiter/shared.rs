//! The GCRA of `Gcra`, kept in Redis so that every replica books against one
//! budget.
//!
//! Each limiter is one Redis hash holding the theoretical arrival time (TAT)
//! and the end of a pause. A Lua script (`gcra.lua`) reads it, decides and
//! writes it back in one atomic step, so two replicas booking at the same
//! moment never see the same state. The script reads the time from the Redis
//! server, not from the replica: clocks drift between machines, and a budget
//! they share needs one clock. What comes back is a wait, and the replica adds
//! it to its own `Instant::now()` when the answer arrives. That can only delay
//! a start by up to the round trip, never bring it forward.
//!
//! The script takes the rate as an argument and keeps only times, so a replica
//! can change its rate at runtime with `set_rate` without migrating anything.
//!
//! Redis is an optimisation of pacing, not a dependency of serving. When it
//! cannot be reached, or does not answer within `redis_timeout_ms`, the
//! booking is made on a local `Gcra` holding this replica's share of the limit
//! (the limit divided by `expected_replicas`), and the call goes on. The
//! failure is counted and logged once per outage, and Redis is left alone for
//! a second before the next booking tries it again, so a Redis that hangs
//! costs one timeout per second rather than one per booking.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::Context;
use prometheus_client::metrics::counter::Counter;
use redis::Script;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::time::{Instant, timeout};
use tracing::{info, warn};

use super::{Gcra, Limiter};
use crate::config::{ClusterConfig, millis};
use crate::metrics::Metrics;

static SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("gcra.lua")));

/// How long Redis is left alone after a failure before a booking tries it
/// again.
const COOLDOWN: Duration = Duration::from_secs(1);

/// The connection to Redis, shared by every limiter of the process, and what
/// is known about its health.
struct Redis {
    /// Reconnects by itself. Created lazily, so a Redis that is not up yet
    /// when the gateway starts does not stop it.
    connection: ConnectionManager,
    timeout: Duration,
    errors: Counter,
    health: Mutex<Health>,
}

#[derive(Default)]
struct Health {
    /// An outage is running: it was logged, and its end will be.
    down: bool,
    /// Redis is not tried before this instant.
    retry_at: Option<Instant>,
}

/// One run of the script: its key and the arguments it documents.
struct Call<'a> {
    key: &'a str,
    operation: &'a str,
    emission_us: f64,
    burst: u64,
    amount: i64,
    longest_wait: i64,
}

/// What the script answered.
struct Reply {
    accepted: bool,
    wait: Duration,
    pause_left: Duration,
}

impl Redis {
    /// Runs the script. `None` means Redis did not answer and the caller must
    /// use its local share instead.
    async fn run(&self, call: Call<'_>) -> Option<Reply> {
        if !self.may_try() {
            return None;
        }
        let mut connection = self.connection.clone();
        let mut invocation = SCRIPT.key(call.key);
        invocation
            .arg(call.operation)
            .arg(call.emission_us)
            .arg(call.burst)
            .arg(call.amount)
            .arg(call.longest_wait);
        let outcome = timeout(
            self.timeout,
            invocation.invoke_async::<Vec<i64>>(&mut connection),
        )
        .await;
        let reason = match outcome {
            Ok(Ok(values)) => match parse(&values) {
                Some(reply) => {
                    self.succeeded();
                    return Some(reply);
                }
                None => format!("unexpected reply {values:?}"),
            },
            Ok(Err(err)) => err.to_string(),
            Err(_) => format!("no answer within {} ms", self.timeout.as_millis()),
        };
        self.failed(&reason);
        None
    }

    /// Whether to try Redis now. After a failure, one booking in a cooldown
    /// period is let through to find out whether Redis is back.
    fn may_try(&self) -> bool {
        let mut health = self.health();
        let now = Instant::now();
        match health.retry_at {
            Some(at) if now < at => false,
            Some(_) => {
                health.retry_at = Some(now + COOLDOWN);
                true
            }
            None => true,
        }
    }

    fn succeeded(&self) {
        let mut health = self.health();
        health.retry_at = None;
        if std::mem::take(&mut health.down) {
            info!("the shared rate limiter's Redis answers again; pacing is shared again");
        }
    }

    fn failed(&self, reason: &str) {
        self.errors.inc();
        let mut health = self.health();
        health.retry_at = Some(Instant::now() + COOLDOWN);
        // Once per outage: a Redis that stays down fails every booking.
        if !std::mem::replace(&mut health.down, true) {
            warn!(
                reason,
                "the shared rate limiter's Redis does not answer; every replica paces with its \
                 own share of the limits until it does"
            );
        }
    }

    fn health(&self) -> MutexGuard<'_, Health> {
        // Two flags; a panic elsewhere cannot leave them torn.
        self.health
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn parse(values: &[i64]) -> Option<Reply> {
    let [accepted, wait, pause_left] = *values else {
        return None;
    };
    let micros = |value: i64| Duration::from_micros(u64::try_from(value).unwrap_or(0));
    Some(Reply {
        accepted: accepted == 1,
        wait: micros(wait),
        pause_left: micros(pause_left),
    })
}

/// A `Gcra` whose state lives in Redis.
pub struct Shared {
    redis: Arc<Redis>,
    key: String,
    /// Microseconds one unit occupies at the sustained rate, as the bits of
    /// an `f64`: a rate change must not wait for a lock.
    emission_us: AtomicU64,
    burst: u64,
    /// How many replicas the limits are divided between while Redis is down.
    replicas: u32,
    /// This replica's share of the limit, used while Redis is down.
    fallback: Gcra,
}

impl Shared {
    fn new(redis: Arc<Redis>, key: String, replicas: u32, per_second: f64, burst: u64) -> Self {
        let burst = burst.max(1);
        let share = f64::from(replicas);
        Self {
            redis,
            key,
            emission_us: AtomicU64::new((1e6 / per_second).to_bits()),
            burst,
            replicas,
            fallback: Gcra::per_second(per_second / share, burst.div_ceil(u64::from(replicas))),
        }
    }

    /// Runs one operation on the script, folding in a pause that another
    /// replica set so that this replica's own share honours it too if Redis
    /// goes away.
    async fn run(&self, operation: &str, amount: i64, longest_wait: i64) -> Option<Reply> {
        let reply = self
            .redis
            .run(Call {
                key: &self.key,
                operation,
                emission_us: f64::from_bits(self.emission_us.load(Ordering::Relaxed)),
                burst: self.burst,
                amount,
                longest_wait,
            })
            .await?;
        if !reply.pause_left.is_zero() {
            self.fallback.pause_until(Instant::now() + reply.pause_left);
        }
        Some(reply)
    }

    pub async fn try_book(
        &self,
        now: Instant,
        cost: u64,
        max_wait: Duration,
    ) -> Result<Instant, Duration> {
        match self.run("book", units(cost), micros(max_wait)).await {
            Some(reply) if reply.accepted => Ok(Instant::now() + reply.wait),
            Some(reply) => Err(reply.wait),
            None => self.fallback.try_book(now, cost, max_wait),
        }
    }

    pub async fn book(&self, now: Instant, cost: u64) -> Instant {
        match self.run("book", units(cost), -1).await {
            Some(reply) => Instant::now() + reply.wait,
            None => self.fallback.book(now, cost),
        }
    }

    pub async fn adjust(&self, delta: i64) {
        if self.run("adjust", delta, 0).await.is_none() {
            self.fallback.adjust(delta);
        }
    }

    pub async fn pause_until(&self, until: Instant) {
        // The local share always learns of the pause, so that it still holds
        // if Redis is out: the pause is about the vendor, not about Redis.
        self.fallback.pause_until(until);
        let delay = until.saturating_duration_since(Instant::now());
        let _ = self.run("pause", micros(delay), 0).await;
    }

    pub async fn resume_at(&self, now: Instant) -> Option<Instant> {
        match self.run("peek", 0, 0).await {
            Some(reply) => (!reply.pause_left.is_zero()).then(|| Instant::now() + reply.pause_left),
            None => self.fallback.resume_at(now),
        }
    }

    pub async fn backlog(&self, now: Instant) -> Duration {
        match self.run("peek", 0, 0).await {
            Some(reply) => reply.wait,
            None => self.fallback.backlog(now),
        }
    }

    pub fn set_rate(&self, per_second: f64) {
        assert!(per_second > 0.0, "rate must be positive");
        self.emission_us
            .store((1e6 / per_second).to_bits(), Ordering::Relaxed);
        self.fallback
            .set_rate(per_second / f64::from(self.replicas));
    }
}

fn units(cost: u64) -> i64 {
    i64::try_from(cost).unwrap_or(i64::MAX)
}

fn micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

/// Makes the gateway's limiters: in memory, or shared through Redis when the
/// configuration has a `[cluster]` table.
pub struct Limiters {
    cluster: Option<Cluster>,
}

struct Cluster {
    redis: Arc<Redis>,
    prefix: String,
    replicas: u32,
}

impl Limiters {
    /// Every limiter lives in this process and Redis is never contacted.
    pub fn local() -> Self {
        Self { cluster: None }
    }

    /// Limiters as the configuration asks for. `env` looks up an environment
    /// variable by name, for the Redis URL. Redis is not contacted here: the
    /// first booking connects.
    pub fn from_config(
        cluster: Option<&ClusterConfig>,
        env: &dyn Fn(&str) -> Option<String>,
        metrics: &Metrics,
    ) -> anyhow::Result<Self> {
        let Some(cluster) = cluster else {
            return Ok(Self::local());
        };
        let variable = &cluster.redis_url_env;
        let url = env(variable).unwrap_or_default();
        let url = url.trim();
        anyhow::ensure!(
            !url.is_empty(),
            "cluster: the Redis URL must be in the {variable} environment variable"
        );
        // The URL can hold a password: the errors below never repeat it.
        let client = redis::Client::open(url).map_err(|_| {
            anyhow::anyhow!(
                "cluster: the value of {variable} is not a Redis URL \
                 (expected redis://[:password@]host[:port][/db])"
            )
        })?;
        let timeout = millis(cluster.redis_timeout_ms);
        let connection = ConnectionManager::new_lazy_with_config(
            client,
            ConnectionManagerConfig::new()
                .set_connection_timeout(Some(timeout))
                .set_response_timeout(Some(timeout))
                // The booking's own timeout bounds the wait; the manager only
                // has to keep trying in the background.
                .set_number_of_retries(2),
        )
        .context("cluster: could not set up the Redis connection")?;
        Ok(Self {
            cluster: Some(Cluster {
                redis: Arc::new(Redis {
                    connection,
                    timeout,
                    errors: metrics.shared_limiter_errors.clone(),
                    health: Mutex::new(Health::default()),
                }),
                prefix: cluster.key_prefix.clone(),
                replicas: cluster.expected_replicas,
            }),
        })
    }

    /// A limiter for `per_second` units per second with bursts of `burst`.
    /// `name` identifies it across replicas (the key prefix is added); two
    /// limiters with the same name share one budget.
    pub fn per_second(&self, name: &str, per_second: f64, burst: u64) -> Limiter {
        match &self.cluster {
            None => Limiter::Local(Gcra::per_second(per_second, burst)),
            Some(cluster) => Limiter::Shared(Shared::new(
                Arc::clone(&cluster.redis),
                format!("{}:{name}", cluster.prefix),
                cluster.replicas,
                per_second,
                burst,
            )),
        }
    }

    pub fn per_minute(&self, name: &str, per_minute: f64, burst: u64) -> Limiter {
        match &self.cluster {
            None => Limiter::Local(Gcra::per_minute(per_minute, burst)),
            Some(_) => self.per_second(name, per_minute / 60.0, burst),
        }
    }

    /// Whether limiters made here share their budget through Redis.
    #[cfg(test)]
    pub fn is_shared(&self) -> bool {
        self.cluster.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that need a real Redis (or Valkey) run when this variable holds
    /// its URL, and are skipped otherwise.
    const TEST_REDIS_URL: &str = "SYSTEMONE_GATEWAY_TEST_REDIS_URL";

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn cluster(prefix: &str, expected_replicas: u32) -> ClusterConfig {
        ClusterConfig {
            redis_url_env: "REDIS_URL".to_owned(),
            key_prefix: prefix.to_owned(),
            redis_timeout_ms: 200,
            expected_replicas,
        }
    }

    /// One replica's limiters, against the Redis at `url`.
    fn replica(url: &str, prefix: &str, replicas: u32, metrics: &Metrics) -> Limiters {
        let url = url.to_owned();
        Limiters::from_config(
            Some(&cluster(prefix, replicas)),
            &move |_| Some(url.clone()),
            metrics,
        )
        .unwrap()
    }

    /// A URL where nothing listens: the port was free a moment ago.
    fn dead_url() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("redis://127.0.0.1:{port}")
    }

    /// The URL of the test Redis, and a key prefix no other test run uses.
    fn real_redis() -> Option<(String, String)> {
        let Ok(url) = std::env::var(TEST_REDIS_URL) else {
            eprintln!("skipped: {TEST_REDIS_URL} is not set");
            return None;
        };
        Some((url, format!("test-{:016x}", fastrand::u64(..))))
    }

    fn near(actual: Duration, expected: Duration) -> bool {
        actual.abs_diff(expected) < ms(60)
    }

    #[tokio::test]
    async fn without_a_cluster_table_limiters_stay_in_memory() {
        let limiters = Limiters::from_config(None, &|_| None, &Metrics::new()).unwrap();
        assert!(!limiters.is_shared());
        let limiter = limiters.per_second("x", 10.0, 1);
        assert!(matches!(limiter, Limiter::Local(_)));
    }

    #[tokio::test]
    async fn a_missing_redis_url_stops_the_start_and_a_bad_one_is_not_echoed() {
        let config = cluster("p", 1);
        let err = Limiters::from_config(Some(&config), &|_| None, &Metrics::new())
            .err()
            .unwrap();
        assert!(err.to_string().contains("REDIS_URL"), "{err}");

        let password = "hunter2-the-password";
        let err = Limiters::from_config(
            Some(&config),
            &|_| Some(format!("not a url, password {password}")),
            &Metrics::new(),
        )
        .err()
        .unwrap();
        let message = format!("{err:#}");
        assert!(message.contains("REDIS_URL"), "{message}");
        assert!(!message.contains(password), "{message}");
    }

    #[tokio::test]
    async fn starting_does_not_need_redis_to_be_up() {
        let metrics = Metrics::new();
        let limiters = replica(&dead_url(), "p", 1, &metrics);
        assert!(limiters.is_shared());
        assert_eq!(metrics.shared_limiter_errors.get(), 0);
    }

    #[tokio::test]
    async fn bookings_fall_back_to_the_local_share_while_redis_is_down() {
        let metrics = Metrics::new();
        let limiters = replica(&dead_url(), "p", 1, &metrics);
        let limiter = limiters.per_second("x", 10.0, 1);
        let t0 = Instant::now();
        assert_eq!(limiter.book(t0, 1).await, t0);
        assert_eq!(limiter.book(t0, 1).await, t0 + ms(100));
        assert_eq!(limiter.try_book(t0, 1, ms(50)).await, Err(ms(200)));
        assert_eq!(limiter.try_book(t0, 1, ms(200)).await, Ok(t0 + ms(200)));
        // Three slots are booked by now. Giving two back leaves one.
        limiter.adjust(-2).await;
        assert_eq!(limiter.backlog(t0).await, ms(100));
        // Redis failed once, and was then left alone for a while: every
        // booking above was answered, and the failure was counted once.
        assert_eq!(metrics.shared_limiter_errors.get(), 1);
    }

    /// Collects what the `tracing` subscriber of the test prints.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_outage_is_logged_once_however_many_bookings_fail() {
        let logs = Logs::default();
        let writer = logs.clone();
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .finish(),
        );
        let metrics = Metrics::new();
        let limiter = replica(&dead_url(), "p", 1, &metrics).per_second("x", 10.0, 1);
        for _ in 0..3 {
            limiter.book(Instant::now(), 1).await;
            // Past the cooldown, so the next booking tries Redis again.
            tokio::time::advance(COOLDOWN + ms(1)).await;
        }
        assert_eq!(metrics.shared_limiter_errors.get(), 3);
        let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("WARN").count(), 1, "{logs}");
        assert!(logs.contains("does not answer"), "{logs}");
    }

    #[tokio::test]
    async fn while_redis_is_down_each_replica_paces_with_its_share() {
        let limiters = replica(&dead_url(), "p", 4, &Metrics::new());
        // 240 a minute for the cluster is 60 for each of four replicas: one
        // a second, with a burst of one.
        let limiter = limiters.per_minute("x", 240.0, 4);
        let t0 = Instant::now();
        assert_eq!(limiter.book(t0, 1).await, t0);
        assert_eq!(limiter.book(t0, 1).await, t0 + Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_pause_still_holds_while_redis_is_down() {
        let limiters = replica(&dead_url(), "p", 1, &Metrics::new());
        let limiter = limiters.per_second("x", 100.0, 1);
        let t0 = Instant::now();
        limiter.pause_until(t0 + ms(500)).await;
        assert_eq!(limiter.resume_at(t0).await, Some(t0 + ms(500)));
        assert_eq!(limiter.book(t0, 1).await, t0 + ms(500));
    }

    #[tokio::test]
    async fn replicas_share_one_budget() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let metrics = Metrics::new();
        // Two replicas of one gateway: same key prefix, same limiter name.
        let a = replica(&url, &prefix, 2, &metrics).per_second("backend:x", 10.0, 1);
        let b = replica(&url, &prefix, 2, &metrics).per_second("backend:x", 10.0, 1);
        let t0 = Instant::now();
        let first = a.book(t0, 1).await;
        let second = b.book(t0, 1).await;
        let third = a.book(t0, 1).await;
        let fourth = b.book(t0, 1).await;
        // One slot every 100 ms, whichever replica asks.
        assert!(
            near(first.saturating_duration_since(t0), ms(0)),
            "{first:?}"
        );
        assert!(near(second.saturating_duration_since(first), ms(100)));
        assert!(near(third.saturating_duration_since(second), ms(100)));
        assert!(near(fourth.saturating_duration_since(third), ms(100)));
        assert_eq!(metrics.shared_limiter_errors.get(), 0);

        // The key is written with an expiry, so idle limiters leave nothing.
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        let ttl: i64 = redis::cmd("PTTL")
            .arg(format!("{prefix}:backend:x"))
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(ttl > 0, "PTTL was {ttl}");
    }

    #[tokio::test]
    async fn another_limiter_name_is_another_budget() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let limiters = replica(&url, &prefix, 1, &Metrics::new());
        let x = limiters.per_second("x", 1.0, 1);
        let y = limiters.per_second("y", 1.0, 1);
        let t0 = Instant::now();
        x.book(t0, 1).await;
        assert!(near(x.backlog(t0).await, Duration::from_secs(1)));
        assert!(near(y.backlog(t0).await, ms(0)));
    }

    #[tokio::test]
    async fn the_script_keeps_the_semantics_of_the_local_gcra() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let metrics = Metrics::new();
        let shared = replica(&url, &prefix, 1, &metrics).per_second("x", 1.0, 2);
        let local = Limiter::Local(Gcra::per_second(1.0, 2));
        let second = Duration::from_secs(1);

        // The same steps on both. The local one keeps a fixed "now" and the
        // shared one the real clock, so waits agree to within a few
        // milliseconds.
        let t0 = Instant::now();
        for (cost, expected) in [(1, 0), (1, 0), (1, 1), (2, 3), (5, 5)] {
            let now = Instant::now();
            let shared_wait = shared.book(now, cost).await.saturating_duration_since(now);
            let local_wait = local.book(t0, cost).await.saturating_duration_since(t0);
            assert!(near(local_wait, second * expected), "local {local_wait:?}");
            assert!(
                near(shared_wait, local_wait),
                "{shared_wait:?} vs {local_wait:?}"
            );
        }

        // Charging and refunding.
        for limiter in [&shared, &local] {
            limiter.adjust(2).await;
        }
        assert!(near(
            shared.backlog(Instant::now()).await,
            local.backlog(t0).await
        ));
        for limiter in [&shared, &local] {
            limiter.adjust(-3).await;
        }
        assert!(near(
            shared.backlog(Instant::now()).await,
            local.backlog(t0).await
        ));

        // A refused booking leaves no trace.
        let refused_shared = shared.try_book(Instant::now(), 1, ms(10)).await;
        let refused_local = local.try_book(t0, 1, ms(10)).await;
        let (Err(shared_wait), Err(local_wait)) = (refused_shared, refused_local) else {
            panic!("both should refuse: {refused_shared:?} {refused_local:?}");
        };
        assert!(near(shared_wait, local_wait));
        assert!(near(
            shared.backlog(Instant::now()).await,
            local.backlog(t0).await
        ));

        // A cost above the burst waits for a full bucket and overdraws it.
        let long = Duration::from_secs(600);
        let now = Instant::now();
        let over_shared = shared.try_book(now, 10, long).await.unwrap();
        let over_local = local.try_book(t0, 10, long).await.unwrap();
        assert!(near(
            over_shared.saturating_duration_since(now),
            over_local.saturating_duration_since(t0)
        ));
        assert!(near(
            shared.backlog(Instant::now()).await,
            local.backlog(t0).await
        ));
        assert_eq!(metrics.shared_limiter_errors.get(), 0);
    }

    #[tokio::test]
    async fn a_pause_is_shared_and_ends_without_a_burst() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let metrics = Metrics::new();
        let a = replica(&url, &prefix, 2, &metrics).per_second("x", 20.0, 5);
        let b = replica(&url, &prefix, 2, &metrics).per_second("x", 20.0, 5);
        assert_eq!(b.resume_at(Instant::now()).await, None);
        let t0 = Instant::now();
        a.pause_until(t0 + ms(600)).await;
        // The other replica sees it, and its bookings wait for the end of
        // the pause, then leave one slot apart.
        let resume = b.resume_at(Instant::now()).await.expect("b sees the pause");
        assert!(near(resume.saturating_duration_since(t0), ms(600)));
        let first = b.book(Instant::now(), 1).await;
        let second = a.book(Instant::now(), 1).await;
        assert!(
            near(first.saturating_duration_since(t0), ms(600)),
            "{first:?}"
        );
        assert!(near(second.saturating_duration_since(first), ms(50)));
        assert_eq!(metrics.shared_limiter_errors.get(), 0);
    }

    /// Forwards TCP connections to a real Redis, and can be cut and brought
    /// back on the same port, to play an outage.
    struct Proxy {
        port: u16,
        target: (String, u16),
        running: Option<tokio::task::JoinSet<()>>,
    }

    impl Proxy {
        fn new(redis_url: &str) -> Self {
            let client = redis::Client::open(redis_url).unwrap();
            let target = match client.get_connection_info().addr() {
                redis::ConnectionAddr::Tcp(host, port) => (host.clone(), *port),
                other => panic!("the proxy needs a TCP Redis, not {other:?}"),
            };
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            Self {
                port,
                target,
                running: None,
            }
        }

        fn url(&self) -> String {
            format!("redis://127.0.0.1:{}", self.port)
        }

        /// Starts accepting again. Stopped proxies refuse connections.
        async fn start(&mut self) {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", self.port))
                .await
                .unwrap();
            let target = self.target.clone();
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                while let Ok((mut client, _)) = listener.accept().await {
                    let target = target.clone();
                    connections.spawn(async move {
                        if let Ok(mut server) = tokio::net::TcpStream::connect(target).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                }
            });
            self.running = Some(tasks);
        }

        /// Closes the port and every open connection.
        fn stop(&mut self) {
            self.running = None;
        }
    }

    #[tokio::test]
    async fn the_limiter_uses_redis_again_once_it_is_back() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let metrics = Metrics::new();
        let mut proxy = Proxy::new(&url);
        // The replica reaches Redis through the proxy; `direct` reaches it
        // without, and shows what the replica managed to book there.
        let replica = replica(&proxy.url(), &prefix, 1, &metrics);
        let direct = replica_direct(&url, &prefix);
        let second = Duration::from_secs(1);

        // Redis is not up when the replica starts: it paces by itself.
        let limiter = replica.per_second("recovery:a", 10.0, 1);
        limiter.book(Instant::now(), 50).await;
        assert_eq!(metrics.shared_limiter_errors.get(), 1);
        let shared = direct.per_second("recovery:a", 10.0, 1);
        assert!(shared.backlog(Instant::now()).await < second);

        // Redis comes up, and the replica finds it again on its own.
        proxy.start().await;
        wait_until_booked_in_redis(&limiter, &shared).await;
        let errors = metrics.shared_limiter_errors.get();

        // Redis goes away while the replica is using it.
        proxy.stop();
        let limiter = replica.per_second("recovery:b", 10.0, 1);
        let shared = direct.per_second("recovery:b", 10.0, 1);
        // These two bookings are answered, locally, and count once.
        limiter.book(Instant::now(), 50).await;
        limiter.book(Instant::now(), 50).await;
        assert_eq!(metrics.shared_limiter_errors.get(), errors + 1);
        assert!(shared.backlog(Instant::now()).await < second);

        // And comes back.
        proxy.start().await;
        wait_until_booked_in_redis(&limiter, &shared).await;
    }

    fn replica_direct(url: &str, prefix: &str) -> Limiters {
        replica(url, prefix, 1, &Metrics::new())
    }

    /// Books 50 units a second on `limiter` until they show in Redis, where
    /// `shared`, which reaches it directly, sees a backlog of seconds.
    async fn wait_until_booked_in_redis(limiter: &Limiter, shared: &Limiter) {
        for _ in 0..40 {
            limiter.book(Instant::now(), 50).await;
            if shared.backlog(Instant::now()).await > Duration::from_secs(3) {
                return;
            }
            tokio::time::sleep(ms(250)).await;
        }
        panic!("the limiter never reached Redis again");
    }

    #[tokio::test]
    async fn a_new_rate_applies_to_later_bookings() {
        let Some((url, prefix)) = real_redis() else {
            return;
        };
        let limiter = replica(&url, &prefix, 1, &Metrics::new()).per_second("x", 10.0, 1);
        let t0 = Instant::now();
        let first = limiter.book(t0, 1).await;
        limiter.set_rate(100.0);
        let second = limiter.book(t0, 1).await;
        let third = limiter.book(t0, 1).await;
        // Booked at 10 a second, so the second slot is 100 ms after the
        // first; then the new rate spaces slots 10 ms apart.
        assert!(near(second.saturating_duration_since(first), ms(100)));
        assert!(near(third.saturating_duration_since(second), ms(10)));
    }
}
