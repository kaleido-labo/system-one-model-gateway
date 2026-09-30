//! Prometheus metrics, served in OpenMetrics text on the admin port.

use std::sync::atomic::AtomicU64;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CallLabels {
    pub service: String,
    pub status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ServiceLabels {
    pub service: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BackendLabels {
    pub backend: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct UpstreamLabels {
    pub backend: String,
    /// HTTP status, or 0 when the backend could not be reached.
    pub status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FallbackLabels {
    /// The backend the model routes to.
    pub from: String,
    /// The backend that took the call instead.
    pub to: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TokenLabels {
    pub service: String,
    pub backend: String,
}

type HistogramFamily<L> = Family<L, Histogram, fn() -> Histogram>;

pub struct Metrics {
    registry: Registry,
    pub calls: Family<CallLabels, Counter>,
    pub call_duration: HistogramFamily<ServiceLabels>,
    pub questions: Family<ServiceLabels, Counter>,
    pub input_tokens: Family<TokenLabels, Counter>,
    pub upstream_calls: Family<UpstreamLabels, Counter>,
    pub upstream_duration: HistogramFamily<BackendLabels>,
    pub upstream_retries: Family<BackendLabels, Counter>,
    pub circuit_state: Family<BackendLabels, Gauge>,
    pub requests_per_minute_limit: Family<BackendLabels, Gauge<f64, AtomicU64>>,
    pub rate_decreases: Family<BackendLabels, Counter>,
    pub fallback_calls: Family<FallbackLabels, Counter>,
    pub batch_callers: HistogramFamily<BackendLabels>,
    pub batch_questions: HistogramFamily<BackendLabels>,
    pub deduplicated_questions: Family<BackendLabels, Counter>,
    pub estimated_tokens_saved: Family<BackendLabels, Counter>,
    pub isolated_replays: Family<BackendLabels, Counter>,
    pub shared_limiter_errors: Counter,
    pub queue_wait: HistogramFamily<BackendLabels>,
    pub cache_hits: Family<BackendLabels, Counter>,
    pub cache_misses: Family<BackendLabels, Counter>,
    pub cache_entries: Gauge,
}

fn latency_histogram() -> Histogram {
    // 5 ms to about 10 s.
    Histogram::new(exponential_buckets(0.005, 2.0, 12))
}

fn callers_histogram() -> Histogram {
    Histogram::new([1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 32.0, 64.0])
}

fn questions_histogram() -> Histogram {
    Histogram::new(exponential_buckets(1.0, 2.0, 9))
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("systemone_gateway");

        let calls = Family::<CallLabels, Counter>::default();
        registry.register(
            "calls",
            "Calls received from services, by service and response status",
            calls.clone(),
        );
        let call_duration: HistogramFamily<ServiceLabels> =
            Family::new_with_constructor(latency_histogram);
        registry.register(
            "call_duration_seconds",
            "Time from receiving a call to answering it, queueing included",
            call_duration.clone(),
        );
        let questions = Family::<ServiceLabels, Counter>::default();
        registry.register(
            "questions",
            "Questions asked by each service",
            questions.clone(),
        );
        let input_tokens = Family::<TokenLabels, Counter>::default();
        registry.register(
            "input_tokens",
            "Upstream input tokens charged to each service, by backend; merged calls are split between their callers",
            input_tokens.clone(),
        );
        let upstream_calls = Family::<UpstreamLabels, Counter>::default();
        registry.register(
            "upstream_calls",
            "HTTP calls made to each backend, by final status after retries",
            upstream_calls.clone(),
        );
        let upstream_duration: HistogramFamily<BackendLabels> =
            Family::new_with_constructor(latency_histogram);
        registry.register(
            "upstream_duration_seconds",
            "Time spent in each backend's HTTP calls, retries included",
            upstream_duration.clone(),
        );
        let upstream_retries = Family::<BackendLabels, Counter>::default();
        registry.register(
            "upstream_retries",
            "Upstream attempts retried after a 429, 529, 5xx or network error",
            upstream_retries.clone(),
        );
        let circuit_state = Family::<BackendLabels, Gauge>::default();
        registry.register(
            "circuit_state",
            "Circuit breaker of each backend: 0 closed, 1 half-open (one trial call allowed), 2 open (calls turned away)",
            circuit_state.clone(),
        );
        let requests_per_minute_limit = Family::<BackendLabels, Gauge<f64, AtomicU64>>::default();
        registry.register(
            "requests_per_minute_limit",
            "Requests per minute the gateway paces each backend at now: its requests_per_minute, or the rate it adapted to when adaptive_rate is on",
            requests_per_minute_limit.clone(),
        );
        let rate_decreases = Family::<BackendLabels, Counter>::default();
        registry.register(
            "rate_decreases",
            "Times the adaptive rate lowered a backend's request rate after a 429; one per episode of 429s",
            rate_decreases.clone(),
        );
        let fallback_calls = Family::<FallbackLabels, Counter>::default();
        registry.register(
            "fallback_calls",
            "Calls served by a fallback backend (to) instead of the backend their model routes to (from)",
            fallback_calls.clone(),
        );
        let batch_callers: HistogramFamily<BackendLabels> =
            Family::new_with_constructor(callers_histogram);
        registry.register(
            "batch_callers",
            "Service calls answered by one merged batch, by backend",
            batch_callers.clone(),
        );
        let batch_questions: HistogramFamily<BackendLabels> =
            Family::new_with_constructor(questions_histogram);
        registry.register(
            "batch_questions",
            "Distinct questions sent in one merged batch, by backend",
            batch_questions.clone(),
        );
        let deduplicated_questions = Family::<BackendLabels, Counter>::default();
        registry.register(
            "deduplicated_questions",
            "Questions answered by an identical question already in the same upstream call, by backend",
            deduplicated_questions.clone(),
        );
        let estimated_tokens_saved = Family::<BackendLabels, Counter>::default();
        registry.register(
            "estimated_tokens_saved",
            "Estimated input tokens not billed because calls sharing a state were merged (System One backends only), by backend",
            estimated_tokens_saved.clone(),
        );
        let isolated_replays = Family::<BackendLabels, Counter>::default();
        registry.register(
            "isolated_replays",
            "Calls replayed on their own after the merged call they were in was rejected, by backend",
            isolated_replays.clone(),
        );
        let shared_limiter_errors = Counter::default();
        registry.register(
            "shared_limiter_errors",
            "Bookings on the shared rate limiter that failed because Redis was unreachable or too slow; each was paced on the replica's own share instead",
            shared_limiter_errors.clone(),
        );
        let queue_wait: HistogramFamily<BackendLabels> =
            Family::new_with_constructor(latency_histogram);
        registry.register(
            "queue_wait_seconds",
            "Time a batch waited between its first call arriving and going upstream, by backend",
            queue_wait.clone(),
        );
        let cache_hits = Family::<BackendLabels, Counter>::default();
        registry.register(
            "cache_hits",
            "Questions answered from the answer cache, by backend",
            cache_hits.clone(),
        );
        let cache_misses = Family::<BackendLabels, Counter>::default();
        registry.register(
            "cache_misses",
            "Questions looked up in the answer cache and not found, by backend",
            cache_misses.clone(),
        );
        let cache_entries = Gauge::default();
        registry.register(
            "cache_entries",
            "Answers held by the answer cache, expired ones included until they are dropped",
            cache_entries.clone(),
        );

        Self {
            registry,
            calls,
            call_duration,
            questions,
            input_tokens,
            upstream_calls,
            upstream_duration,
            upstream_retries,
            circuit_state,
            requests_per_minute_limit,
            rate_decreases,
            fallback_calls,
            batch_callers,
            batch_questions,
            deduplicated_questions,
            estimated_tokens_saved,
            isolated_replays,
            shared_limiter_errors,
            queue_wait,
            cache_hits,
            cache_misses,
            cache_entries,
        }
    }

    pub fn service(name: &str) -> ServiceLabels {
        ServiceLabels {
            service: name.to_owned(),
        }
    }

    pub fn backend(name: &str) -> BackendLabels {
        BackendLabels {
            backend: name.to_owned(),
        }
    }

    pub fn fallback(from: &str, to: &str) -> FallbackLabels {
        FallbackLabels {
            from: from.to_owned(),
            to: to.to_owned(),
        }
    }

    pub fn tokens(service: &str, backend: &str) -> TokenLabels {
        TokenLabels {
            service: service.to_owned(),
            backend: backend.to_owned(),
        }
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        encode(&mut out, &self.registry).expect("writing to a String cannot fail");
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_registered_metrics_with_the_prefix() {
        let metrics = Metrics::new();
        metrics
            .calls
            .get_or_create(&CallLabels {
                service: "ocr".to_owned(),
                status: 200,
            })
            .inc();
        metrics
            .batch_callers
            .get_or_create(&Metrics::backend("typesafe"))
            .observe(3.0);
        metrics
            .isolated_replays
            .get_or_create(&Metrics::backend("hf"))
            .inc();
        let text = metrics.render();
        assert!(
            text.contains(r#"systemone_gateway_calls_total{service="ocr",status="200"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"systemone_gateway_batch_callers_count{backend="typesafe"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"systemone_gateway_isolated_replays_total{backend="hf"} 1"#),
            "{text}"
        );
        assert!(text.ends_with("# EOF\n"));
    }
}
