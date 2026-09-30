//! The HTTP client of one backend: authentication, the request pacer, retries
//! (see `retry`), the metrics of every call and one trace span per attempt.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use bytes::Bytes;
use tokio::time::{Instant, sleep_until};
use tracing::{Instrument, Span, warn};

use crate::metrics::{Metrics, UpstreamLabels};
use crate::scheduling::Limiter;
use crate::telemetry;

mod reply;
mod retry;

pub use reply::{UpstreamFailure, UpstreamReply, describe};
pub use retry::RetryPolicy;
use retry::{is_retryable, parse_retry_after};

/// Response header carrying the vendor's id for a request. The gateway
/// hands the id back under this name whatever the backend, so SDK users
/// find it where they expect it.
pub const REQUEST_ID: &str = "x-typesafe-request-id";
/// Where OpenAI-compatible servers put their request id.
const GENERIC_REQUEST_ID: &str = "x-request-id";

/// A backend's API key. Debug output never shows it.
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

pub struct Upstream {
    /// The backend's name, for metrics and logs.
    name: String,
    client: reqwest::Client,
    base: reqwest::Url,
    api_key: Option<ApiKey>,
    retry: RetryPolicy,
    /// The shared request pacer, paused here when the vendor answers 429.
    pacer: Arc<Limiter>,
    metrics: Arc<Metrics>,
}

impl Upstream {
    pub fn new(
        name: &str,
        base_url: &str,
        api_key: Option<ApiKey>,
        connect_timeout: Duration,
        retry: RetryPolicy,
        pacer: Arc<Limiter>,
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
            name: name.to_owned(),
            client,
            base,
            api_key,
            retry,
            pacer,
            metrics,
        })
    }

    /// The backend's request pacer.
    pub fn pacer(&self) -> &Arc<Limiter> {
        &self.pacer
    }

    /// `path` resolved against the base URL, which keeps its own path:
    /// `chat/completions` under `https://router.huggingface.co/v1` is
    /// `https://router.huggingface.co/v1/chat/completions`.
    pub fn endpoint(&self, path: &str) -> anyhow::Result<reqwest::Url> {
        Ok(self.base.join(path)?)
    }

    /// POSTs the JSON `body` to `url`, retried until `deadline`.
    pub async fn post(
        &self,
        url: &reqwest::Url,
        body: Bytes,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        self.call(url, Some(body), deadline).await
    }

    /// GETs `url`, retried until `deadline`.
    pub async fn get(
        &self,
        url: &reqwest::Url,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        self.call(url, None, deadline).await
    }

    async fn call(
        &self,
        url: &reqwest::Url,
        body: Option<Bytes>,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        let started = Instant::now();
        let mut attempt = 0;
        let mut last_failure = None;
        let result = loop {
            let now = Instant::now();
            if self.pacer.resume_at(now).await.is_some() {
                // This attempt's slot fell inside a pause the backend asked for.
                // Waking up at the end of the pause would send every held
                // batch at the same instant, so take a fresh slot after it,
                // spaced like any other booking, then look again in case the
                // pause got longer meanwhile.
                let slot = self.pacer.book(now, 1).await;
                if slot >= deadline {
                    self.pacer.adjust(-1).await;
                    break Err(last_failure.unwrap_or(UpstreamFailure::Paused {
                        retry_after: slot.saturating_duration_since(now),
                    }));
                }
                sleep_until(slot).await;
                continue;
            }
            let remaining = deadline.saturating_duration_since(now);
            if remaining < Duration::from_millis(1) {
                break Err(last_failure.unwrap_or(UpstreamFailure::Deadline));
            }

            // The span lives until the end of the iteration, to say how long
            // the retry that followed waited. Its duration is the attempt's
            // own: a span ends when it was last exited.
            let span = telemetry::attempt(
                &self.name,
                if body.is_some() { "POST" } else { "GET" },
                attempt + 1,
            );
            let failure = match self
                .attempt(
                    url,
                    body.clone(),
                    remaining.min(self.retry.attempt_timeout),
                    &span,
                )
                .instrument(span.clone())
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
                self.pacer.pause_until(Instant::now() + delay).await;
            }
            if attempt >= self.retry.max_retries {
                break Err(failure);
            }
            // A retry is one more request against the account's limit, so it
            // takes a slot like any other call.
            let now = Instant::now();
            let retry_at = self.pacer.book(now, 1).await.max(now + delay);
            if retry_at >= deadline {
                self.pacer.adjust(-1).await;
                break Err(failure);
            }
            let delay_ms = retry_at.saturating_duration_since(now).as_millis() as u64;
            span.record("gateway.retry_delay_ms", delay_ms);
            warn!(
                backend = %self.name,
                attempt = attempt + 1,
                delay_ms,
                failure = %describe(&failure),
                "retrying the upstream call"
            );
            self.metrics
                .upstream_retries
                .get_or_create(&Metrics::backend(&self.name))
                .inc();
            sleep_until(retry_at).await;
            attempt += 1;
            last_failure = Some(failure);
        };

        let status = match &result {
            Ok(_) => Some(200),
            Err(UpstreamFailure::Status { status, .. }) => Some(status.as_u16()),
            Err(UpstreamFailure::Transport { .. }) => Some(0),
            Err(
                UpstreamFailure::Paused { .. }
                | UpstreamFailure::Deadline
                | UpstreamFailure::Unreadable(_),
            ) => None,
        };
        // A call that never started is not an upstream call.
        if let Some(status) = status {
            self.metrics
                .upstream_calls
                .get_or_create(&UpstreamLabels {
                    backend: self.name.clone(),
                    status,
                })
                .inc();
            self.metrics
                .upstream_duration
                .get_or_create(&Metrics::backend(&self.name))
                .observe(started.elapsed().as_secs_f64());
        }
        result
    }

    async fn attempt(
        &self,
        url: &reqwest::Url,
        body: Option<Bytes>,
        timeout: Duration,
        span: &Span,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        let mut request = match body {
            Some(body) => self
                .client
                .post(url.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body),
            None => self.client.get(url.clone()),
        };
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(&key.0);
        }
        // Lets the backend, if it traces, continue the caller's trace.
        let mut trace_headers = HeaderMap::new();
        telemetry::inject(span, &mut trace_headers);
        let response = request
            .headers(trace_headers)
            .header(ACCEPT, "application/json")
            .timeout(timeout)
            .send()
            .await
            .map_err(|err| traced(span, transport_failure(err)))?;

        let status = response.status();
        let request_id = [REQUEST_ID, GENERIC_REQUEST_ID]
            .into_iter()
            .find_map(|name| {
                response
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            });
        let retry_after = parse_retry_after(response.headers(), SystemTime::now());
        let body = response
            .bytes()
            .await
            .map_err(|err| traced(span, transport_failure(err)))?;
        if status.is_success() {
            telemetry::attempt_ok(span, status.as_u16());
            Ok(UpstreamReply { body, request_id })
        } else {
            telemetry::attempt_failed(span, Some(status.as_u16()), "");
            Err(UpstreamFailure::Status {
                status,
                body,
                retry_after,
                request_id,
            })
        }
    }
}

/// Notes a transport failure on the attempt's span, and passes it on.
fn traced(span: &Span, failure: UpstreamFailure) -> UpstreamFailure {
    if let UpstreamFailure::Transport { timed_out, .. } = &failure {
        telemetry::attempt_failed(
            span,
            None,
            if *timed_out { "timeout" } else { "connection" },
        );
    }
    failure
}

fn transport_failure(err: reqwest::Error) -> UpstreamFailure {
    UpstreamFailure::Transport {
        timed_out: err.is_timeout(),
        detail: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_debug_is_redacted() {
        assert_eq!(
            format!("{:?}", ApiKey::new("sk-secret")),
            "ApiKey(redacted)"
        );
    }
}
