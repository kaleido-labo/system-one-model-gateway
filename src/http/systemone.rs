//! `POST /v1/systemone`: authenticates the caller, validates the request,
//! routes it to a backend, queues it for merging and turns the outcome into
//! an HTTP response.

use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};
use tracing::Span;

use super::{AppState, BACKEND, Shared, outcome_response};
use crate::backend::Backend;
use crate::error::GatewayError;
use crate::metrics::{CallLabels, Metrics};
use crate::scheduling::{MIN_RETRY_AFTER, Outcome, Saturated};
use crate::services::{Refusal, ServiceId};
use crate::wire::{Invalid, PreparedRequest, RequestError};

/// Least time a call must have left for a fallback to be worth trying after
/// its backend failed. Less than that, and the fallback would only be
/// answering a caller who has already been given a 504.
const MIN_FALLBACK_BUDGET: Duration = Duration::from_secs(1);

pub(super) async fn systemone(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let arrived = Instant::now();
    let Some(service_id) = state.0.registry.authenticate(&headers) else {
        record_call(&state.0.metrics, "-", StatusCode::UNAUTHORIZED, arrived);
        return GatewayError::unauthorized().into_response();
    };
    let service = state.0.registry.get(service_id);
    Span::current().record("service", service.name.as_str());
    let response = handle_systemone(&state.0, service_id, &body, arrived).await;
    record_call(&state.0.metrics, &service.name, response.status(), arrived);
    response
}

async fn handle_systemone(
    state: &Shared,
    service_id: ServiceId,
    body: &[u8],
    arrived: Instant,
) -> Response {
    let service = state.registry.get(service_id);
    let request = match PreparedRequest::parse(body, &state.estimator) {
        Ok(request) => request,
        Err(RequestError::Malformed(message)) => {
            return GatewayError::malformed(message).into_response();
        }
        Err(RequestError::Invalid(invalid)) => {
            return GatewayError::invalid(invalid).into_response();
        }
    };
    if !service.allows_model(&request.model) {
        return GatewayError::model_not_allowed(&request.model).into_response();
    }
    let Some(primary) = state.backends.route(&request.model) else {
        return GatewayError::invalid(Invalid::new(
            "model",
            format!(
                "{:?} is not served by any backend of this gateway",
                request.model
            ),
        ))
        .into_response();
    };
    if let Err(invalid) = primary.check(&request) {
        return GatewayError::invalid(invalid).into_response();
    }
    // Held until the answer is sent, so max_concurrent counts in-flight calls.
    let _admission = match service.admit(arrived) {
        Ok(admission) => admission,
        Err(Refusal::RateLimited { retry_after }) => {
            return GatewayError::rate_limited(
                format!("service {:?} is over its requests_per_minute", service.name),
                retry_after,
            )
            .into_response();
        }
        Err(Refusal::TooManyInFlight) => {
            return GatewayError::rate_limited(
                format!(
                    "service {:?} already has max_concurrent calls in flight",
                    service.name
                ),
                Duration::from_secs(1),
            )
            .into_response();
        }
    };

    let questions = request.questions.len() as u64;
    let deadline = arrived + state.request_timeout;
    let chain = state.backends.chain(primary);
    let mut request = request;
    // Where to look for the next backend to try, the shortest wait among the
    // breakers that turned the call away, and what the last backend that
    // failed had to say.
    let mut next = 0;
    let mut wait: Option<Duration> = None;
    let mut failed: Option<(&Backend, Outcome)> = None;
    while let Some((index, backend)) = choose(&chain, next, &request, &mut wait) {
        next = index + 1;
        // The first backend gets the call as it arrived. A fallback gets it
        // now: the queue deadline is about waiting for capacity there, and
        // the time lost on the backend before does not count against it.
        let queued_at = if index == 0 { arrived } else { Instant::now() };
        if index > 0 {
            state
                .metrics
                .fallback_calls
                .get_or_create(&Metrics::fallback(&primary.name, &backend.name))
                .inc();
        }
        let (reply, answer) = oneshot::channel();
        if let Err(Saturated { retry_after }) = backend
            .coalescer
            .submit(request, queued_at, deadline, reply)
        {
            return GatewayError::rate_limited(
                format!(
                    "backend {:?}'s shared quota is booked beyond max_queue_wait_ms; retry later",
                    backend.name
                ),
                retry_after,
            )
            .into_response();
        }
        if failed.is_none() {
            state
                .metrics
                .questions
                .get_or_create(&Metrics::service(&service.name))
                .inc_by(questions);
        }

        let outcome = match timeout_at(deadline, answer).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => {
                return GatewayError::internal("the batch carrying this call was dropped")
                    .into_response();
            }
            Err(_) => {
                return GatewayError::timeout(format!(
                    "no answer within request_timeout_ms ({} ms)",
                    state.request_timeout.as_millis()
                ))
                .into_response();
            }
        };
        // A fallback is worth a try only if the backend is unavailable (not
        // if it refused the call itself, which another backend would too),
        // and if there is time for it to answer. Replaying the call is safe:
        // a failed call gave the caller nothing, and System One calls change
        // no state on the backend.
        let has_time = deadline.saturating_duration_since(Instant::now()) >= MIN_FALLBACK_BUDGET;
        if !(outcome.is_unavailable() && next < chain.len() && has_time) {
            return answer_with(state, &service.name, backend, outcome);
        }
        // The request moved into the batch; read it again from the body.
        request = match PreparedRequest::parse(body, &state.estimator) {
            Ok(request) => request,
            Err(_) => {
                return GatewayError::internal("the request could not be read a second time")
                    .into_response();
            }
        };
        failed = Some((backend, outcome));
    }

    // Nobody is left to try. If a backend failed on the way, that is the
    // answer; otherwise every breaker on the way was open.
    match failed {
        Some((backend, outcome)) => answer_with(state, &service.name, backend, outcome),
        None => {
            let retry_after = wait.unwrap_or(MIN_RETRY_AFTER);
            let message = if chain.len() > 1 {
                format!(
                    "backend {:?} and its fallbacks are failing; their circuit breakers are open",
                    primary.name
                )
            } else {
                format!(
                    "backend {:?} is failing; its circuit breaker is open",
                    primary.name
                )
            };
            GatewayError::unavailable(message, retry_after).into_response()
        }
    }
}

/// The first backend from `chain[from..]` that can take the call: the
/// backend can express the request, and its circuit breaker lets a call
/// through. A breaker that turns the call away adds its wait to `wait`, the
/// shortest one being the `retry-after` when nobody is left.
fn choose<'a>(
    chain: &[&'a Backend],
    from: usize,
    request: &PreparedRequest,
    wait: &mut Option<Duration>,
) -> Option<(usize, &'a Backend)> {
    for (index, backend) in chain.iter().enumerate().skip(from) {
        // The routed backend's check came first and answered 422. A fallback
        // that cannot express the request is skipped instead: the caller did
        // nothing wrong.
        if index > 0 && backend.check(request).is_err() {
            continue;
        }
        match backend.breaker.admit(Instant::now()) {
            Ok(()) => return Some((index, backend)),
            Err(retry_after) => {
                *wait = Some(wait.map_or(retry_after, |shortest| shortest.min(retry_after)));
            }
        }
    }
    None
}

/// The HTTP response for what `backend` answered, naming it in the
/// `x-systemone-gateway-backend` header.
fn answer_with(state: &Shared, service: &str, backend: &Backend, outcome: Outcome) -> Response {
    let mut response = outcome_response(state, service, &backend.name, outcome);
    if let Ok(value) = HeaderValue::from_str(&backend.name) {
        response.headers_mut().insert(BACKEND, value);
    }
    response
}

fn record_call(metrics: &Metrics, service: &str, status: StatusCode, arrived: Instant) {
    metrics
        .calls
        .get_or_create(&CallLabels {
            service: service.to_owned(),
            status: status.as_u16(),
        })
        .inc();
    metrics
        .call_duration
        .get_or_create(&Metrics::service(service))
        .observe(arrived.elapsed().as_secs_f64());
}
