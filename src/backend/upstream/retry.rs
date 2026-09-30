//! How an upstream call is retried.
//!
//! Retries follow TypeSafe's guidance (https://docs.typesafe.ai/api.md),
//! which suits OpenAI-compatible servers as well:
//! 429 and 529 are retried with exponential backoff, and `retry-after-ms` or
//! `retry-after` wins over the computed delay when the vendor sends one. 5xx
//! and network errors are retried the same way. Every retry stays inside the
//! deadline of the calls waiting on it.

use std::time::{Duration, SystemTime};

use axum::http::HeaderMap;
use axum::http::header::RETRY_AFTER;

use super::UpstreamFailure;
use crate::error::RETRY_AFTER_MS;

/// Longest `retry-after` the gateway will honour. A larger value is capped
/// rather than trusted, so one odd header cannot stall every service.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub attempt_timeout: Duration,
}

impl RetryPolicy {
    /// Delay before retry number `attempt` (0 for the first retry): doubling
    /// from `backoff_initial` up to `backoff_max`, minus up to 25% of jitter
    /// so that callers retrying together spread out. `jitter` is in [0, 1).
    pub fn backoff(&self, attempt: u32, jitter: f64) -> Duration {
        let factor = 2u32.saturating_pow(attempt.min(16));
        let delay = self
            .backoff_initial
            .saturating_mul(factor)
            .min(self.backoff_max);
        delay.mul_f64(1.0 - 0.25 * jitter.clamp(0.0, 1.0))
    }
}

pub(super) fn is_retryable(failure: &UpstreamFailure) -> bool {
    match failure {
        UpstreamFailure::Status { status, .. } => {
            matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504 | 529)
        }
        UpstreamFailure::Transport { .. } => true,
        UpstreamFailure::Paused { .. }
        | UpstreamFailure::Deadline
        | UpstreamFailure::Unreadable(_) => false,
    }
}

/// Reads `retry-after-ms` (milliseconds), then `retry-after` (seconds or an
/// HTTP date), capped at `MAX_RETRY_AFTER`.
pub(super) fn parse_retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let header = |name| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
    };
    // Capped before the conversion: `Duration::from_secs_f64` panics on
    // values that overflow it, and a header is no reason to crash.
    let seconds = |seconds: f64| {
        (seconds.is_finite() && seconds >= 0.0)
            .then(|| Duration::from_secs_f64(seconds.min(MAX_RETRY_AFTER.as_secs_f64())))
    };
    let from_ms = header(RETRY_AFTER_MS)
        .and_then(|ms| ms.parse::<f64>().ok())
        .and_then(|ms| seconds(ms / 1000.0));
    let delay = from_ms.or_else(|| {
        let value = header(RETRY_AFTER)?;
        match value.parse::<f64>() {
            Ok(parsed) => seconds(parsed),
            Err(_) => httpdate::parse_http_date(value)
                .ok()
                .map(|at| at.duration_since(now).unwrap_or_default()),
        }
    })?;
    Some(delay.min(MAX_RETRY_AFTER))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, StatusCode};
    use bytes::Bytes;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_retries: 3,
            backoff_initial: Duration::from_millis(200),
            backoff_max: Duration::from_millis(1_000),
            attempt_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let policy = policy();
        assert_eq!(policy.backoff(0, 0.0), Duration::from_millis(200));
        assert_eq!(policy.backoff(1, 0.0), Duration::from_millis(400));
        assert_eq!(policy.backoff(2, 0.0), Duration::from_millis(800));
        assert_eq!(policy.backoff(3, 0.0), Duration::from_millis(1_000));
        assert_eq!(policy.backoff(40, 0.0), Duration::from_millis(1_000));
    }

    #[test]
    fn jitter_takes_off_at_most_a_quarter() {
        let policy = policy();
        let most_jitter = policy.backoff(0, 0.999_999);
        assert!(most_jitter >= Duration::from_millis(150), "{most_jitter:?}");
        assert!(most_jitter < Duration::from_millis(151), "{most_jitter:?}");
        let some_jitter = policy.backoff(1, 0.5);
        assert!(
            some_jitter >= Duration::from_millis(300) && some_jitter < Duration::from_millis(400)
        );
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn retry_after_ms_wins_over_retry_after() {
        let headers = headers(&[("retry-after-ms", "250"), ("retry-after", "3")]);
        assert_eq!(
            parse_retry_after(&headers, SystemTime::now()),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "2")]), now),
            Some(Duration::from_secs(2))
        );
        let in_five = httpdate::fmt_http_date(now + Duration::from_secs(5));
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", &in_five)]), now),
            Some(Duration::from_secs(5))
        );
        let past = httpdate::fmt_http_date(now - Duration::from_secs(5));
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", &past)]), now),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn retry_after_is_capped_and_garbage_ignored() {
        let now = SystemTime::now();
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "86400")]), now),
            Some(MAX_RETRY_AFTER)
        );
        // Values that would overflow a Duration are capped, not a panic.
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "100000000000000000000")]), now),
            Some(MAX_RETRY_AFTER)
        );
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after-ms", "1e300")]), now),
            Some(MAX_RETRY_AFTER)
        );
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after", "soon")]), now),
            None
        );
        assert_eq!(
            parse_retry_after(&headers(&[("retry-after-ms", "-5")]), now),
            None
        );
        assert_eq!(parse_retry_after(&HeaderMap::new(), now), None);
    }

    #[test]
    fn only_transient_failures_are_retried() {
        let status = |code: u16| UpstreamFailure::Status {
            status: StatusCode::from_u16(code).unwrap(),
            body: Bytes::new(),
            retry_after: None,
            request_id: None,
        };
        for code in [429, 500, 502, 503, 504, 529] {
            assert!(is_retryable(&status(code)), "{code}");
        }
        for code in [400, 401, 403, 404, 422] {
            assert!(!is_retryable(&status(code)), "{code}");
        }
        assert!(is_retryable(&UpstreamFailure::Transport {
            timed_out: true,
            detail: String::new()
        }));
        assert!(!is_retryable(&UpstreamFailure::Deadline));
    }
}
