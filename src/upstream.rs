//! The one HTTP client that talks to TypeSafe, with its retry policy.
//!
//! Retries follow the vendor's guidance (https://docs.typesafe.ai/api.md):
//! 429 and 529 are retried with exponential backoff, and `retry-after-ms` or
//! `retry-after` wins over the computed delay when the vendor sends one. 5xx
//! and network errors are retried the same way. Every retry stays inside the
//! deadline of the calls waiting on it.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::http::header::{ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use tokio::time::{Instant, sleep_until};
use tracing::warn;

use crate::error::RETRY_AFTER_MS;
use crate::limiter::Gcra;
use crate::metrics::{Metrics, StatusLabels};

/// Response header carrying the vendor's id for a request.
pub const REQUEST_ID: &str = "x-typesafe-request-id";

/// Longest `retry-after` the gateway will honour. A larger value is capped
/// rather than trusted, so one odd header cannot stall every service.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// The TypeSafe API key. Debug output never shows it.
#[derive(Clone)]
pub struct ApiKey(String);

impl ApiKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(redacted)")
    }
}

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

#[derive(Debug)]
pub struct UpstreamReply {
    pub body: Bytes,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum UpstreamFailure {
    /// TypeSafe answered with an error status, after any retries.
    Status {
        status: StatusCode,
        body: Bytes,
        retry_after: Option<Duration>,
        request_id: Option<String>,
    },
    /// No usable response: refused or reset connection, TLS failure, or an
    /// attempt that timed out, after any retries.
    Transport { timed_out: bool, detail: String },
    /// TypeSafe asked everyone to back off past the calls' deadline.
    Paused { retry_after: Duration },
    /// The deadline passed before an attempt could start.
    Deadline,
}

pub struct Upstream {
    client: reqwest::Client,
    systemone_url: reqwest::Url,
    models_url: reqwest::Url,
    api_key: ApiKey,
    retry: RetryPolicy,
    /// The shared request pacer, paused here when the vendor answers 429.
    pacer: Arc<Gcra>,
    metrics: Arc<Metrics>,
}

impl Upstream {
    pub fn new(
        base_url: &str,
        api_key: ApiKey,
        connect_timeout: Duration,
        retry: RetryPolicy,
        pacer: Arc<Gcra>,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        let mut base = reqwest::Url::parse(base_url)?;
        // `join` replaces the last path segment unless the path ends in '/'.
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let client = reqwest::Client::builder()
            .user_agent(concat!("systemone-gateway/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(connect_timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;
        Ok(Self {
            client,
            systemone_url: base.join("v1/systemone")?,
            models_url: base.join("v1/models")?,
            api_key,
            retry,
            pacer,
            metrics,
        })
    }

    /// `POST /v1/systemone` with `body`, retried until `deadline`.
    pub async fn systemone(
        &self,
        body: Bytes,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        self.call(Some(body), deadline).await
    }

    /// `GET /v1/models`, retried until `deadline`.
    pub async fn models(&self, deadline: Instant) -> Result<UpstreamReply, UpstreamFailure> {
        self.call(None, deadline).await
    }

    async fn call(
        &self,
        body: Option<Bytes>,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        let started = Instant::now();
        let mut attempt = 0;
        let mut last_failure = None;
        let result = loop {
            if let Some(resume) = self.pacer.resume_at(Instant::now()) {
                if resume >= deadline {
                    break Err(last_failure.unwrap_or(UpstreamFailure::Paused {
                        retry_after: resume.saturating_duration_since(Instant::now()),
                    }));
                }
                sleep_until(resume).await;
            }
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            if remaining < Duration::from_millis(1) {
                break Err(last_failure.unwrap_or(UpstreamFailure::Deadline));
            }

            let failure = match self
                .attempt(body.clone(), remaining.min(self.retry.attempt_timeout))
                .await
            {
                Ok(reply) => break Ok(reply),
                Err(failure) if !is_retryable(&failure) => break Err(failure),
                Err(failure) => failure,
            };

            let delay = match &failure {
                UpstreamFailure::Status {
                    retry_after: Some(retry_after),
                    ..
                } => *retry_after,
                _ => self.retry.backoff(attempt, fastrand::f64()),
            };
            if let UpstreamFailure::Status { status, .. } = &failure
                && *status == StatusCode::TOO_MANY_REQUESTS
            {
                // Everyone is over the vendor's limit, not just this call.
                self.pacer.pause_until(Instant::now() + delay);
            }
            if attempt >= self.retry.max_retries {
                break Err(failure);
            }
            // A retry is one more request against the account's limit, so it
            // takes a slot like any other call.
            let now = Instant::now();
            let retry_at = self.pacer.book(now, 1).max(now + delay);
            if retry_at >= deadline {
                self.pacer.adjust(-1);
                break Err(failure);
            }
            warn!(
                attempt = attempt + 1,
                delay_ms = retry_at.saturating_duration_since(now).as_millis() as u64,
                failure = %describe(&failure),
                "retrying the TypeSafe call"
            );
            self.metrics.upstream_retries.inc();
            sleep_until(retry_at).await;
            attempt += 1;
            last_failure = Some(failure);
        };

        let status = match &result {
            Ok(_) => Some(200),
            Err(UpstreamFailure::Status { status, .. }) => Some(status.as_u16()),
            Err(UpstreamFailure::Transport { .. }) => Some(0),
            Err(UpstreamFailure::Paused { .. } | UpstreamFailure::Deadline) => None,
        };
        // A call that never started is not an upstream call.
        if let Some(status) = status {
            self.metrics
                .upstream_calls
                .get_or_create(&StatusLabels { status })
                .inc();
            self.metrics
                .upstream_duration
                .observe(started.elapsed().as_secs_f64());
        }
        result
    }

    async fn attempt(
        &self,
        body: Option<Bytes>,
        timeout: Duration,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        let request = match body {
            Some(body) => self
                .client
                .post(self.systemone_url.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body),
            None => self.client.get(self.models_url.clone()),
        };
        let response = request
            .bearer_auth(&self.api_key.0)
            .header(ACCEPT, "application/json")
            .timeout(timeout)
            .send()
            .await
            .map_err(transport_failure)?;

        let status = response.status();
        let request_id = response
            .headers()
            .get(REQUEST_ID)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let retry_after = parse_retry_after(response.headers(), SystemTime::now());
        let body = response.bytes().await.map_err(transport_failure)?;
        if status.is_success() {
            Ok(UpstreamReply { body, request_id })
        } else {
            Err(UpstreamFailure::Status {
                status,
                body,
                retry_after,
                request_id,
            })
        }
    }
}

fn transport_failure(err: reqwest::Error) -> UpstreamFailure {
    UpstreamFailure::Transport {
        timed_out: err.is_timeout(),
        detail: err.to_string(),
    }
}

fn is_retryable(failure: &UpstreamFailure) -> bool {
    match failure {
        UpstreamFailure::Status { status, .. } => {
            matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504 | 529)
        }
        UpstreamFailure::Transport { .. } => true,
        UpstreamFailure::Paused { .. } | UpstreamFailure::Deadline => false,
    }
}

pub fn describe(failure: &UpstreamFailure) -> String {
    match failure {
        UpstreamFailure::Status { status, .. } => format!("status {}", status.as_u16()),
        UpstreamFailure::Transport {
            timed_out: true, ..
        } => "attempt timed out".to_owned(),
        UpstreamFailure::Transport { detail, .. } => format!("network error: {detail}"),
        UpstreamFailure::Paused { retry_after } => {
            format!("paused for {} ms after a 429", retry_after.as_millis())
        }
        UpstreamFailure::Deadline => "deadline reached".to_owned(),
    }
}

/// Reads `retry-after-ms` (milliseconds), then `retry-after` (seconds or an
/// HTTP date), capped at `MAX_RETRY_AFTER`.
pub fn parse_retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let header = |name| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
    };
    let from_ms = header(RETRY_AFTER_MS)
        .and_then(|ms| ms.parse::<f64>().ok())
        .filter(|ms| ms.is_finite() && *ms >= 0.0)
        .map(|ms| Duration::from_secs_f64(ms / 1000.0));
    let delay = from_ms.or_else(|| {
        let value = header(RETRY_AFTER)?;
        match value.parse::<f64>() {
            Ok(seconds) if seconds.is_finite() && seconds >= 0.0 => {
                Some(Duration::from_secs_f64(seconds))
            }
            Ok(_) => None,
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
    use axum::http::HeaderValue;

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

    #[test]
    fn api_key_debug_is_redacted() {
        assert_eq!(
            format!("{:?}", ApiKey::new("sk-secret")),
            "ApiKey(redacted)"
        );
    }
}
