//! `POST /v1/systemone`: authenticates the caller, validates the request,
//! routes it to a backend, queues it for merging and turns the outcome into
//! an HTTP response.
//!
//! Two spans are open while a call is handled: the log span of the router,
//! which `Span::current()` is before the trace span is entered, and the trace
//! span (see `telemetry`). Fields go to each by name, never through
//! `Span::current()`, which would reach only the innermost.

use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};
use tracing::{Instrument, Span};

use super::{AppState, BACKEND, Shared, outcome_response, request_id};
use crate::error::GatewayError;
use crate::metrics::{CallLabels, Metrics};
use crate::scheduling::{Outcome, Saturated};
use crate::services::{Refusal, ServiceId};
use crate::telemetry;
use crate::wire::{Invalid, PreparedRequest, RequestError};

pub(super) async fn systemone(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let arrived = Instant::now();
    let log_span = Span::current();
    let call = telemetry::systemone_call(request_id(&headers));
    let response = async {
        let Some(service_id) = state.0.registry.authenticate(&headers) else {
            record_call(&state.0.metrics, "-", StatusCode::UNAUTHORIZED, arrived);
            return GatewayError::unauthorized().into_response();
        };
        telemetry::adopt_caller(&call, &headers);
        let service = state.0.registry.get(service_id);
        log_span.record("service", service.name.as_str());
        call.record("gateway.service", service.name.as_str());
        let response = handle_systemone(&state.0, service_id, &body, arrived, &call).await;
        record_call(&state.0.metrics, &service.name, response.status(), arrived);
        response
    }
    .instrument(call.clone())
    .await;
    telemetry::finish_call(&call, response.status());
    response
}

async fn handle_systemone(
    state: &Shared,
    service_id: ServiceId,
    body: &[u8],
    arrived: Instant,
    call: &Span,
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
    let Some(backend) = state.backends.route(&request.model) else {
        return GatewayError::invalid(Invalid::new(
            "model",
            format!(
                "{:?} is not served by any backend of this gateway",
                request.model
            ),
        ))
        .into_response();
    };
    call.record("gateway.backend", backend.name.as_str());
    call.record("gateway.model", request.model.as_str());
    if let Err(invalid) = backend.check(&request) {
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
    let (reply, answer) = oneshot::channel();
    if let Err(Saturated { retry_after }) =
        backend.coalescer.submit(request, arrived, deadline, reply)
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
    state
        .metrics
        .questions
        .get_or_create(&Metrics::service(&service.name))
        .inc_by(questions);

    match timeout_at(deadline, answer).await {
        Ok(Ok(outcome)) => {
            if let Outcome::Answered { batch_callers, .. } = &outcome {
                call.record("gateway.batch_callers", *batch_callers as u64);
            }
            let mut response = outcome_response(state, &service.name, &backend.name, outcome);
            if let Ok(value) = HeaderValue::from_str(&backend.name) {
                response.headers_mut().insert(BACKEND, value);
            }
            response
        }
        Ok(Err(_)) => {
            GatewayError::internal("the batch carrying this call was dropped").into_response()
        }
        Err(_) => GatewayError::timeout(format!(
            "no answer within request_timeout_ms ({} ms)",
            state.request_timeout.as_millis()
        ))
        .into_response(),
    }
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
