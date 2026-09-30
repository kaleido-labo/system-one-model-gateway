//! Replacing the configuration of a running gateway.
//!
//! A reload reads and checks a whole new configuration first, builds what it
//! changes next, and only then swaps the result in with one assignment
//! (`AppState::replace`). Until that last step nothing the running gateway
//! uses has been touched, so a file that is invalid, or names an environment
//! variable that is not set, leaves it running on the old configuration.
//!
//! What a reload keeps, so that it does not reset what is in progress:
//!
//! - A backend whose `[[backend]]` block is unchanged, while `[coalescing]`
//!   is unchanged too (bar `bytes_per_token`, which no backend holds), is the
//!   same `Backend` object afterwards: its merge queue, pacing, circuit
//!   breaker and models list carry on.
//! - A service whose block is unchanged (bar `key_sha256`, so that rotating
//!   a key does not give the service a fresh quota) keeps its rate limiter
//!   and the slots of its calls in flight.
//! - The answer cache is kept when `[cache]` is unchanged.
//!
//! A backend that changed is built again, and the old one is dropped once the
//! calls that were queued on it are done: those calls hold it (through the
//! snapshot they took when they started), so none of them is lost.
//!
//! Compared whole, never field by field: a setting added to a table later is
//! covered without anyone remembering to list it here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::backend::Backends;
use crate::cache::AnswerCache;
use crate::config::{
    CoalescingConfig, Config, ContentHash, ServerConfig, ServiceConfig, content_hash, millis,
};
use crate::http::{AppState, Shared};
use crate::metrics::Metrics;
use crate::scheduling::Limiters;
use crate::services::ServiceRegistry;
use crate::wire::TokenEstimator;

type Env = Box<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// What a successful reload did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reloaded {
    pub services: usize,
    /// Backends that carry on as they were: same queue, pacing and breaker.
    pub backends_kept: Vec<String>,
    /// Backends that are new, or changed and so built again.
    pub backends_built: Vec<String>,
    /// Backends that are gone. They take no new calls; the ones already
    /// queued on them are answered.
    pub backends_removed: Vec<String>,
    /// Keys that changed in the file but only take effect after a restart.
    pub restart_required: Vec<&'static str>,
}

/// Reloads the configuration of a running gateway. Cloning it shares it.
#[derive(Clone)]
pub struct Reloader(Arc<Inner>);

struct Inner {
    state: AppState,
    /// The environment as the gateway started with it, to check the new
    /// file against what a restart would check.
    env: Env,
    /// Kept from the start: it holds the Redis connection, and `[cluster]`
    /// is not reloaded.
    limiters: Limiters,
    /// The configuration in force. Also what makes reloads take turns: a
    /// SIGHUP and the file watcher cannot apply two files at once.
    applied: Mutex<Config>,
    /// Hash of the file content last looked at, so that the watcher reloads
    /// a change once and does not retry a file that failed, every interval.
    seen: Mutex<Option<ContentHash>>,
    /// `server.config_reload_interval_ms`, for the watcher to follow.
    interval: watch::Sender<Duration>,
}

impl Reloader {
    pub(super) fn new(state: AppState, env: Env, limiters: Limiters, config: &Config) -> Self {
        Self(Arc::new(Inner {
            state,
            env,
            limiters,
            applied: Mutex::new(config.clone()),
            seen: Mutex::new(None),
            interval: watch::Sender::new(interval(config)),
        }))
    }

    /// Makes `config` the configuration of the running gateway, or changes
    /// nothing and says why not. Either way the attempt is logged and counted
    /// in `config_reloads_total`.
    ///
    /// Calls that started before it keep the configuration they started
    /// with; calls that start after it use the new one.
    pub fn reload(&self, config: &Config) -> anyhow::Result<Reloaded> {
        match self.apply(config) {
            Ok(reloaded) => {
                self.0.state.snapshot().metrics.record_reload(true);
                info!(
                    services = reloaded.services,
                    backends_kept = %reloaded.backends_kept.join(","),
                    backends_built = %reloaded.backends_built.join(","),
                    backends_removed = %reloaded.backends_removed.join(","),
                    "configuration reloaded"
                );
                for key in &reloaded.restart_required {
                    warn!(
                        key,
                        "changed in the configuration file, but it only takes effect after a restart"
                    );
                }
                Ok(reloaded)
            }
            Err(error) => {
                self.failed(&error);
                Err(error)
            }
        }
    }

    /// Reads the file at `path` and reloads it, as `SIGHUP` does. Reloads
    /// even if the content is the same as last time: whoever asked wants the
    /// file looked at again.
    pub fn reload_file(&self, path: &Path) -> anyhow::Result<Reloaded> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("could not read {}", path.display()))
            .inspect_err(|error| self.failed(error))?;
        *lock(&self.0.seen) = Some(content_hash(&text));
        self.reload_text(&text, path)
    }

    /// Looks at the file at `path` every `server.config_reload_interval_ms`,
    /// and reloads it when its content changed since `loaded`, the hash of
    /// what the gateway started with. Runs until dropped. While the interval
    /// is 0 it waits; a reload that sets it starts the watching.
    pub async fn watch_file(&self, path: PathBuf, loaded: ContentHash) {
        *lock(&self.0.seen) = Some(loaded);
        let mut interval = self.0.interval.subscribe();
        // A file that cannot be read is reported once, not every interval.
        let mut unreadable = false;
        loop {
            let every = *interval.borrow_and_update();
            if every.is_zero() {
                if interval.changed().await.is_err() {
                    return;
                }
                continue;
            }
            tokio::select! {
                () = tokio::time::sleep(every) => self.poll(&path, &mut unreadable),
                // A new interval: start the wait over.
                _ = interval.changed() => {}
            }
        }
    }

    fn poll(&self, path: &Path, unreadable: &mut bool) {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => {
                *unreadable = false;
                text
            }
            Err(error) => {
                if !std::mem::replace(unreadable, true) {
                    self.failed(
                        &anyhow::Error::new(error)
                            .context(format!("could not read {}", path.display())),
                    );
                }
                return;
            }
        };
        let hash = content_hash(&text);
        {
            let mut seen = lock(&self.0.seen);
            if *seen == Some(hash) {
                return;
            }
            *seen = Some(hash);
        }
        info!(path = %path.display(), "configuration file changed; reloading");
        let _ = self.reload_text(&text, path);
    }

    fn reload_text(&self, text: &str, path: &Path) -> anyhow::Result<Reloaded> {
        let config = Config::from_toml(text)
            .with_context(|| format!("invalid configuration in {}", path.display()))
            .inspect_err(|error| self.failed(error))?;
        self.reload(&config)
    }

    fn failed(&self, error: &anyhow::Error) {
        self.0.state.snapshot().metrics.record_reload(false);
        error!(
            error = %format!("{error:#}"),
            "configuration reload failed; still running on the previous configuration"
        );
    }

    /// Checks `new`, builds what it changes and swaps it in. Changes nothing
    /// if any step fails.
    fn apply(&self, new: &Config) -> anyhow::Result<Reloaded> {
        // A `Config` built by hand has not been through `from_toml`.
        new.validate()?;
        let mut applied = lock(&self.0.applied);
        let old = &*applied;
        let env = &*self.0.env;

        // What `serve` refuses to start on, so that a file that would not
        // survive the next restart is not accepted now. The cluster is only
        // looked at: its limiters are kept, so the ones made here are dropped.
        super::admin_token(new, env)?;
        Limiters::from_config(new.cluster.as_ref(), env, &Metrics::new())?;

        let current = self.0.state.snapshot();
        let metrics = &current.metrics;
        let batching_kept = same_batching(&old.coalescing, &new.coalescing);

        let registry = ServiceRegistry::reusing(&new.services, &self.0.limiters, |wanted| {
            let before = old.services.iter().find(|s| s.name == wanted.name)?;
            same_service(before, wanted)
                .then(|| current.registry.named(&wanted.name).cloned())
                .flatten()
        });

        // Building a backend resets its circuit gauge, and the gauge is
        // shared with the backend it replaces. If a later step fails, that
        // backend carries on, so its gauge goes back to what it was.
        let gauges: Vec<(&str, i64)> = new
            .backends
            .iter()
            .filter_map(|backend| {
                let gauge = metrics
                    .circuit_state
                    .get(&Metrics::backend(&backend.name))?;
                Some((backend.name.as_str(), gauge.get()))
            })
            .collect();
        let built = Backends::build_reusing(new, env, &self.0.limiters, metrics, |wanted| {
            let before = old.backends.iter().find(|b| b.name == wanted.name)?;
            (batching_kept && before == wanted)
                .then(|| current.backends.named(&wanted.name).cloned())
                .flatten()
        });
        let backends = match built {
            Ok(backends) => backends,
            Err(error) => {
                for (name, value) in gauges {
                    metrics
                        .circuit_state
                        .get_or_create(&Metrics::backend(name))
                        .set(value);
                }
                return Err(error);
            }
        };

        let cache = if old.cache == new.cache {
            current.cache.clone()
        } else {
            // The entries of the old cache are not carried over: they were
            // kept under another time to live, or another size.
            metrics.cache_entries.set(0);
            new.cache
                .enabled
                .then(|| Arc::new(AnswerCache::new(&new.cache, metrics.cache_entries.clone())))
        };

        let mut reloaded = Reloaded {
            services: new.services.len(),
            restart_required: restart_required(old, new),
            ..Reloaded::default()
        };
        for backend in &new.backends {
            let kept = current
                .backends
                .named(&backend.name)
                .zip(backends.named(&backend.name))
                .is_some_and(|(before, after)| Arc::ptr_eq(before, after));
            if kept {
                reloaded.backends_kept.push(backend.name.clone());
            } else {
                reloaded.backends_built.push(backend.name.clone());
            }
        }
        for before in &old.backends {
            if backends.named(&before.name).is_none() {
                reloaded.backends_removed.push(before.name.clone());
                // A gauge is the state of something that is running; there
                // is nothing left for it to describe.
                metrics
                    .circuit_state
                    .remove(&Metrics::backend(&before.name));
            }
        }

        self.0.state.replace(Shared {
            registry,
            backends,
            metrics: Arc::clone(metrics),
            estimator: TokenEstimator::new(new.coalescing.bytes_per_token),
            cache,
            request_timeout: millis(new.server.request_timeout_ms),
        });
        let every = interval(new);
        self.0.interval.send_if_modified(|current| {
            let changed = *current != every;
            *current = every;
            changed
        });
        *applied = new.clone();
        Ok(reloaded)
    }
}

fn interval(config: &Config) -> Duration {
    millis(config.server.config_reload_interval_ms)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // What these mutexes guard is replaced whole, so it is never half written.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether two `[coalescing]` tables build the same backends. Every field
/// goes into a backend's batching limits except `bytes_per_token`, which
/// only the handlers read, through the estimator, and which is swapped with
/// the rest of the state.
fn same_batching(old: &CoalescingConfig, new: &CoalescingConfig) -> bool {
    let old = CoalescingConfig {
        bytes_per_token: new.bytes_per_token,
        ..old.clone()
    };
    old == *new
}

/// Whether the service keeps what it has used of its quota. The keys are
/// left out: they only say who the service is, and the registry has them
/// anew, so a rotation costs the service nothing.
fn same_service(old: &ServiceConfig, new: &ServiceConfig) -> bool {
    let old = ServiceConfig {
        key_sha256: new.key_sha256.clone(),
        ..old.clone()
    };
    old == *new
}

/// The settings that changed between `old` and `new` and that nothing short
/// of a restart can apply: the listeners and the body limit are bound or
/// set up once, the log format and the tracing exporter are installed once
/// for the process, and the admin token and the Redis connection were read
/// from the environment at start.
fn restart_required(old: &Config, new: &Config) -> Vec<&'static str> {
    // Every field of `[server]` is named, so that adding one does not go by
    // unnoticed: it has to be placed here as live or restart-only.
    let ServerConfig {
        listen,
        admin_listen,
        admin_token_env,
        request_timeout_ms: _,
        max_body_bytes,
        log_format,
        config_reload_interval_ms: _,
    } = &old.server;
    let server = &new.server;
    [
        ("server.listen", *listen != server.listen),
        ("server.admin_listen", *admin_listen != server.admin_listen),
        (
            "server.admin_token_env",
            *admin_token_env != server.admin_token_env,
        ),
        (
            "server.max_body_bytes",
            *max_body_bytes != server.max_body_bytes,
        ),
        ("server.log_format", *log_format != server.log_format),
        ("[cluster]", old.cluster != new.cluster),
        ("[tracing]", old.tracing != new.tracing),
    ]
    .into_iter()
    .filter_map(|(key, changed)| changed.then_some(key))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::services::hash_key;

    const BACKENDS: &str = r#"
        [[backend]]
        name = "a"
        models = ["a-*"]
        base_url = "http://127.0.0.1:1"
        fallback = ["b"]
        [[backend]]
        name = "b"
        models = ["b-*"]
        base_url = "http://127.0.0.1:2"
    "#;

    fn service(name: &str, key: &str, extra: &str) -> String {
        format!(
            "[[service]]\nname = \"{name}\"\nkey_sha256 = [\"{}\"]\n{extra}\n",
            hex::encode(hash_key(key))
        )
    }

    fn config(head: &str, backends: &str, services: &str) -> Config {
        Config::from_toml(&format!("{head}\n{backends}\n{services}")).unwrap()
    }

    /// A gateway state and its reloader, for `first`.
    fn running(first: &Config) -> (AppState, Reloader) {
        let env = |_: &str| Some("key".to_owned());
        let (state, limiters) = super::super::build_state(first, &env).unwrap();
        let reloader = Reloader::new(state.clone(), Box::new(env), limiters, first);
        (state, reloader)
    }

    fn backend(state: &AppState, name: &str) -> Option<Arc<Backend>> {
        state.snapshot().backends.named(name).cloned()
    }

    #[tokio::test]
    async fn an_unchanged_backend_is_the_same_object_and_a_changed_one_is_built_again() {
        let first = config("", BACKENDS, &service("s", "k", ""));
        let (state, reloader) = running(&first);
        let (a, b) = (backend(&state, "a").unwrap(), backend(&state, "b").unwrap());

        // Only b changes.
        let slower = BACKENDS.replacen(
            "base_url = \"http://127.0.0.1:2\"",
            "base_url = \"http://127.0.0.1:2\"\nrequests_per_minute = 600",
            1,
        );
        let reloaded = reloader
            .reload(&config("", &slower, &service("s", "k", "")))
            .unwrap();
        assert!(Arc::ptr_eq(&a, &backend(&state, "a").unwrap()));
        assert!(!Arc::ptr_eq(&b, &backend(&state, "b").unwrap()));
        assert_eq!(reloaded.backends_kept, ["a"]);
        assert_eq!(reloaded.backends_built, ["b"]);
        assert!(reloaded.backends_removed.is_empty());
        assert!(reloaded.restart_required.is_empty());

        // The same file again changes nothing.
        let b = backend(&state, "b").unwrap();
        let again = reloader
            .reload(&config("", &slower, &service("s", "k", "")))
            .unwrap();
        assert_eq!(again.backends_kept, ["a", "b"]);
        assert!(again.backends_built.is_empty());
        assert!(Arc::ptr_eq(&b, &backend(&state, "b").unwrap()));
    }

    #[tokio::test]
    async fn a_removed_backend_is_gone_and_a_new_one_is_built() {
        let first = config("", BACKENDS, &service("s", "k", ""));
        let (state, reloader) = running(&first);
        let a = backend(&state, "a").unwrap();
        let only_a = r#"
            [[backend]]
            name = "a"
            models = ["a-*"]
            base_url = "http://127.0.0.1:1"
            [[backend]]
            name = "c"
            models = ["c-*"]
            base_url = "http://127.0.0.1:3"
        "#;
        // a no longer falls back to b, so its block changed.
        let reloaded = reloader
            .reload(&config("", only_a, &service("s", "k", "")))
            .unwrap();
        assert_eq!(reloaded.backends_removed, ["b"]);
        assert_eq!(reloaded.backends_built, ["a", "c"]);
        assert!(backend(&state, "b").is_none());
        assert!(state.snapshot().backends.route("b-1").is_none());
        assert!(!Arc::ptr_eq(&a, &backend(&state, "a").unwrap()));
        assert_eq!(state.snapshot().backends.route("c-1").unwrap().name, "c");
    }

    #[tokio::test]
    async fn a_change_to_the_merge_window_builds_every_backend_again_but_the_token_estimate_does_not()
     {
        let services = service("s", "k", "");
        let first = config("", BACKENDS, &services);
        let (state, reloader) = running(&first);
        let a = backend(&state, "a").unwrap();

        let estimate = "[coalescing]\nbytes_per_token = 4.0\n";
        let reloaded = reloader
            .reload(&config(estimate, BACKENDS, &services))
            .unwrap();
        assert_eq!(reloaded.backends_kept, ["a", "b"]);
        assert!(Arc::ptr_eq(&a, &backend(&state, "a").unwrap()));

        let window = "[coalescing]\nbytes_per_token = 4.0\nwindow_ms = 50\n";
        let reloaded = reloader
            .reload(&config(window, BACKENDS, &services))
            .unwrap();
        assert_eq!(reloaded.backends_built, ["a", "b"]);
        assert!(!Arc::ptr_eq(&a, &backend(&state, "a").unwrap()));
    }

    #[tokio::test]
    async fn a_rotated_key_keeps_the_quota_and_a_changed_quota_starts_again() {
        let limited = "requests_per_minute = 60";
        let first = config("", BACKENDS, &service("s", "old", limited));
        let (state, reloader) = running(&first);
        let before = state.snapshot();
        let ptr = |shared: &Shared| shared.registry.named("s").cloned().unwrap();

        // Both keys work during a rotation, and the service is the same one.
        let both = format!(
            "[[service]]\nname = \"s\"\nkey_sha256 = [\"{}\", \"{}\"]\n{limited}\n",
            hex::encode(hash_key("old")),
            hex::encode(hash_key("new")),
        );
        reloader.reload(&config("", BACKENDS, &both)).unwrap();
        let after = state.snapshot();
        assert!(Arc::ptr_eq(&ptr(&before), &ptr(&after)));

        let rotated = service("s", "new", limited);
        reloader.reload(&config("", BACKENDS, &rotated)).unwrap();
        assert!(Arc::ptr_eq(&ptr(&before), &ptr(&state.snapshot())));

        let faster = service("s", "new", "requests_per_minute = 600");
        reloader.reload(&config("", BACKENDS, &faster)).unwrap();
        assert!(!Arc::ptr_eq(&ptr(&before), &ptr(&state.snapshot())));
    }

    #[tokio::test]
    async fn the_cache_is_kept_unless_its_table_changed() {
        let services = service("s", "k", "");
        let cache = "[cache]\nenabled = true\nttl_ms = 60000\n";
        let (state, reloader) = running(&config(cache, BACKENDS, &services));
        let held = state.snapshot().cache.clone().unwrap();

        reloader
            .reload(&config(cache, BACKENDS, &services))
            .unwrap();
        assert!(Arc::ptr_eq(&held, &state.snapshot().cache.clone().unwrap()));

        let longer = "[cache]\nenabled = true\nttl_ms = 120000\n";
        reloader
            .reload(&config(longer, BACKENDS, &services))
            .unwrap();
        assert!(!Arc::ptr_eq(
            &held,
            &state.snapshot().cache.clone().unwrap()
        ));

        reloader.reload(&config("", BACKENDS, &services)).unwrap();
        assert!(state.snapshot().cache.is_none());
    }

    #[tokio::test]
    async fn the_request_timeout_is_swapped() {
        let services = service("s", "k", "");
        let (state, reloader) = running(&config("", BACKENDS, &services));
        assert_eq!(state.snapshot().request_timeout, Duration::from_secs(9));
        let head = "[server]\nrequest_timeout_ms = 4000\n";
        reloader.reload(&config(head, BACKENDS, &services)).unwrap();
        assert_eq!(state.snapshot().request_timeout, Duration::from_secs(4));
    }

    #[test]
    fn keys_that_need_a_restart_are_named() {
        let services = service("s", "k", "");
        let old = config("", BACKENDS, &services);
        assert!(restart_required(&old, &old).is_empty());

        // Live settings are not restart-only.
        let live = config(
            "[server]\nrequest_timeout_ms = 4000\nconfig_reload_interval_ms = 500\n\
             [coalescing]\nwindow_ms = 30\n[cache]\nenabled = true\n",
            BACKENDS,
            &services,
        );
        assert!(restart_required(&old, &live).is_empty());

        let moved = config(
            "[server]\nlisten = \"127.0.0.1:1234\"\nadmin_listen = \"127.0.0.1:1235\"\n\
             admin_token_env = \"TOKEN\"\nmax_body_bytes = 10\nlog_format = \"json\"\n\
             [cluster]\nredis_url_env = \"REDIS_URL\"\n\
             [tracing]\nservice_name = \"other\"\n",
            BACKENDS,
            &services,
        );
        assert_eq!(
            restart_required(&old, &moved),
            [
                "server.listen",
                "server.admin_listen",
                "server.admin_token_env",
                "server.max_body_bytes",
                "server.log_format",
                "[cluster]",
                "[tracing]",
            ]
        );
    }

    #[tokio::test]
    async fn a_reload_reports_restart_only_keys_and_applies_the_rest() {
        let services = service("s", "k", "");
        let (state, reloader) = running(&config("", BACKENDS, &services));
        let head = "[server]\nlisten = \"127.0.0.1:1234\"\nrequest_timeout_ms = 4000\n";
        let reloaded = reloader.reload(&config(head, BACKENDS, &services)).unwrap();
        assert_eq!(reloaded.restart_required, ["server.listen"]);
        assert_eq!(state.snapshot().request_timeout, Duration::from_secs(4));
    }

    #[tokio::test]
    async fn a_reload_that_fails_changes_nothing_and_is_counted() {
        let services = service("s", "k", "");
        let env = |variable: &str| (variable == "KEY_A").then(|| "key".to_owned());
        let with_key =
            BACKENDS.replacen("name = \"a\"", "name = \"a\"\napi_key_env = \"KEY_A\"", 1);
        let first = config("", &with_key, &services);
        let (state, limiters) = super::super::build_state(&first, &env).unwrap();
        let reloader = Reloader::new(state.clone(), Box::new(env), limiters, &first);
        let a = backend(&state, "a").unwrap();
        // b's breaker is open, as far as the gauge can tell.
        let b_gauge = Metrics::backend("b");
        let metrics = Arc::clone(&state.snapshot().metrics);
        metrics.circuit_state.get_or_create(&b_gauge).set(2);

        // A new backend whose key is not in the environment: the whole
        // reload is refused, including the change to a service that is fine
        // and the new block of b, which was built before c failed.
        let with_key = with_key.replacen(
            "models = [\"b-*\"]",
            "models = [\"b-*\"]\nmax_retries = 0",
            1,
        );
        let missing = format!(
            "{with_key}\n[[backend]]\nname = \"c\"\nmodels = [\"c-*\"]\napi_key_env = \"NOPE\"\n\
             base_url = \"http://127.0.0.1:3\"\n{}",
            service("new", "n", "")
        );
        let err = reloader
            .reload(&Config::from_toml(&missing).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("NOPE"), "{err}");
        let after = state.snapshot();
        assert!(after.registry.named("new").is_none());
        assert!(after.backends.named("c").is_none());
        assert!(Arc::ptr_eq(&a, &backend(&state, "a").unwrap()));
        assert_eq!(metrics.circuit_state.get_or_create(&b_gauge).get(), 2);

        let text = after.metrics.render();
        assert!(
            text.contains(r#"systemone_gateway_config_reloads_total{result="error"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"systemone_gateway_config_reloads_total{result="ok"} 0"#),
            "{text}"
        );
    }

    #[tokio::test]
    async fn the_environment_checks_of_serve_apply_to_a_reload() {
        let services = service("s", "k", "");
        let (state, reloader) = running(&config("", BACKENDS, &services));

        // The `running` environment answers every variable, so ask for the
        // admin token with an environment that does not have it.
        let env = |_: &str| None;
        let first = config("", BACKENDS, &services);
        let (strict_state, limiters) = super::super::build_state(&first, &env).unwrap();
        let strict = Reloader::new(strict_state, Box::new(env), limiters, &first);
        let with_token = config(
            "[server]\nadmin_token_env = \"TOKEN\"\n",
            BACKENDS,
            &services,
        );
        let err = strict.reload(&with_token).unwrap_err();
        assert!(err.to_string().contains("TOKEN"), "{err}");

        let with_cluster = config(
            "[cluster]\nredis_url_env = \"REDIS\"\n",
            BACKENDS,
            &services,
        );
        let err = strict.reload(&with_cluster).unwrap_err();
        assert!(err.to_string().contains("REDIS"), "{err}");

        // With the variables present, the same files load.
        reloader.reload(&with_token).unwrap();
        assert!(state.snapshot().registry.named("s").is_some());
    }

    #[tokio::test]
    async fn the_circuit_gauge_goes_with_a_removed_backend() {
        let services = service("s", "k", "");
        let (state, reloader) = running(&config("", BACKENDS, &services));
        let metrics = Arc::clone(&state.snapshot().metrics);
        assert!(metrics.render().contains(r#"circuit_state{backend="b"}"#));
        let only_a =
            "[[backend]]\nname = \"a\"\nmodels = [\"a-*\"]\nbase_url = \"http://127.0.0.1:1\"\n";
        reloader.reload(&config("", only_a, &services)).unwrap();
        let text = metrics.render();
        assert!(!text.contains(r#"circuit_state{backend="b"}"#), "{text}");
        assert!(text.contains(r#"circuit_state{backend="a"}"#), "{text}");
    }
}
