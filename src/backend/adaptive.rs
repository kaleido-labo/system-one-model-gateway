//! The adaptive request rate of one backend: the AIMD rule of
//! `scheduling::aimd`, applied to the backend's request pacer, its metrics and
//! its logs.
//!
//! `Upstream` reports every 429 here. The rule decides whether it starts a
//! congestion episode, and if it does, the new rate goes to the pacer with
//! `Limiter::set_rate`, so a limiter kept in Redis follows too. Recovery needs
//! a clock, because rates must rise while nobody is being refused: a small task
//! wakes when the next step is due and asks the rule for it. The task holds
//! only a weak reference, so it ends once the backend is dropped.
//!
//! With `[cluster]`, each replica runs its own copy of this and sets its own
//! rate: Redis keeps times, not rates (see `Limiter::set_rate`), and a 429 is
//! reported only by the replica that received it.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, Weak};

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, info};

use crate::metrics::Metrics;
use crate::scheduling::{Aimd, Change, Limiter, Params};

pub struct RateAdapter {
    backend: String,
    aimd: Aimd,
    pacer: Arc<Limiter>,
    /// `requests_per_minute_limit` of this backend.
    limit: Gauge<f64, AtomicU64>,
    /// `rate_decreases_total` of this backend.
    decreases: Counter,
    /// Held while a decision is applied, so that the pacer and the gauge end
    /// on the rule's latest rate even when a 429 and a recovery step land at
    /// the same moment.
    applying: Mutex<()>,
}

impl RateAdapter {
    /// Starts adapting `pacer`, which must run at `params.ceiling` requests
    /// per minute. Needs a Tokio runtime, for the recovery task.
    pub fn start(
        backend: &str,
        params: Params,
        pacer: Arc<Limiter>,
        metrics: &Metrics,
    ) -> Arc<Self> {
        let labels = Metrics::backend(backend);
        let limit = metrics
            .requests_per_minute_limit
            .get_or_create(&labels)
            .clone();
        limit.set(displayed(params.ceiling));
        let adapter = Arc::new(Self {
            backend: backend.to_owned(),
            aimd: Aimd::new(params, Instant::now()),
            pacer,
            limit,
            decreases: metrics.rate_decreases.get_or_create(&labels).clone(),
            applying: Mutex::new(()),
        });
        tokio::spawn(recover_when_due(Arc::downgrade(&adapter)));
        adapter
    }

    /// The backend answered 429.
    pub fn on_rate_limited(&self) {
        let _applying = self.applying();
        if let Some(change) = self.aimd.on_rate_limited(Instant::now()) {
            self.apply(change);
        }
    }

    fn recover(&self) {
        let _applying = self.applying();
        if let Some(change) = self.aimd.recover(Instant::now()) {
            self.apply(change);
        }
    }

    fn apply(&self, change: Change) {
        self.pacer.set_rate(change.to / 60.0);
        self.limit.set(displayed(change.to));
        let params = self.aimd.params();
        if change.is_decrease() {
            self.decreases.inc();
            info!(
                backend = %self.backend,
                from_rpm = displayed(change.from),
                to_rpm = displayed(change.to),
                min_rpm = params.floor,
                "lowered the request rate after a 429; it rises again while the backend stays quiet"
            );
        } else if change.to >= params.ceiling {
            info!(
                backend = %self.backend,
                requests_per_minute = params.ceiling,
                "the request rate is back at the configured requests_per_minute"
            );
        } else {
            debug!(
                backend = %self.backend,
                from_rpm = displayed(change.from),
                to_rpm = displayed(change.to),
                "raised the request rate"
            );
        }
    }

    fn applying(&self) -> std::sync::MutexGuard<'_, ()> {
        // Holds no data, so a panic elsewhere leaves nothing torn.
        self.applying
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A rate as shown in metrics and logs: multiplying by a factor leaves float
/// noise in the far decimals.
fn displayed(rate: f64) -> f64 {
    (rate * 100.0).round() / 100.0
}

/// Raises the rate whenever a step falls due, until the adapter goes away.
async fn recover_when_due(adapter: Weak<RateAdapter>) {
    loop {
        let Some(strong) = adapter.upgrade() else {
            return;
        };
        // At the ceiling nothing is due. Looking again after one interval
        // finds the next 429's recovery without a wake-up for it.
        let due = strong
            .aimd
            .next_step()
            .unwrap_or_else(|| Instant::now() + strong.aimd.params().recovery);
        drop(strong);
        sleep_until(due).await;
        let Some(strong) = adapter.upgrade() else {
            return;
        };
        strong.recover();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::scheduling::Limiters;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn params(recovery: Duration) -> Params {
        Params {
            ceiling: 600.0,
            floor: 60.0,
            decrease: 0.5,
            step: 150.0,
            recovery,
        }
    }

    fn pacer() -> Arc<Limiter> {
        // 10 requests per second, no burst: one slot every 100 ms.
        Arc::new(Limiters::local().per_minute("test", 600.0, 1))
    }

    #[tokio::test]
    async fn a_429_slows_the_pacer_and_the_gauge_and_counts_once_per_episode() {
        let metrics = Metrics::new();
        let pacer = pacer();
        let adapter = RateAdapter::start(
            "hf",
            params(Duration::from_secs(3600)),
            Arc::clone(&pacer),
            &metrics,
        );
        let labels = Metrics::backend("hf");
        assert_eq!(
            metrics
                .requests_per_minute_limit
                .get_or_create(&labels)
                .get(),
            600.0
        );

        adapter.on_rate_limited();
        adapter.on_rate_limited();
        adapter.on_rate_limited();

        assert_eq!(
            metrics
                .requests_per_minute_limit
                .get_or_create(&labels)
                .get(),
            300.0
        );
        assert_eq!(metrics.rate_decreases.get_or_create(&labels).get(), 1);
        // 300 per minute is one slot every 200 ms: the pacer follows the gauge.
        let now = Instant::now();
        pacer.book(now, 1).await;
        assert_eq!(pacer.backlog(now).await, ms(200));
    }

    #[tokio::test]
    async fn the_rate_climbs_back_to_the_ceiling_by_itself() {
        let metrics = Metrics::new();
        let pacer = pacer();
        let adapter = RateAdapter::start("hf", params(ms(40)), Arc::clone(&pacer), &metrics);
        adapter.on_rate_limited();
        let labels = Metrics::backend("hf");
        let gauge = metrics.requests_per_minute_limit.get_or_create(&labels);
        // 300 -> 450 -> 600 takes two quiet intervals of 40 ms.
        let deadline = Instant::now() + Duration::from_secs(5);
        while gauge.get() < 600.0 {
            assert!(
                Instant::now() < deadline,
                "the rate stopped at {}",
                gauge.get()
            );
            tokio::time::sleep(ms(10)).await;
        }
        let now = Instant::now();
        pacer.book(now, 1).await;
        assert_eq!(pacer.backlog(now).await, ms(100));
    }
}
