//! HTTP handlers: the TypeSafe-compatible public API and the admin endpoints.
//!
//! Whatever backend answers, services see TypeSafe's API: the same request,
//! the same answers, the same errors.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};
use tower_http::LatencyUnit;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::{Level, Span};

use crate::backend::Backends;
use crate::error::{GatewayError, insert_retry_after};
use crate::metrics::{CallLabels, Metrics};
use crate::scheduling::{Outcome, Saturated};
use crate::services::{Refusal, ServiceId, ServiceRegistry};
use crate::upstream::REQUEST_ID;
use crate::wire::{Invalid, PreparedRequest, RequestError, TokenEstimator};

/// How many calls shared the upstream call that answered this one.
pub const BATCH_CALLERS: HeaderName = HeaderName::from_static("x-systemone-gateway-batch-callers");
/// The backend that answered.
pub const BACKEND: HeaderName = HeaderName::from_static("x-systemone-gateway-backend");

#[derive(Clone)]
pub struct AppState(Arc<Shared>);

pub struct Shared {
    pub registry: ServiceRegistry,
    pub backends: Backends,
    pub metrics: Arc<Metrics>,
    pub estimator: TokenEstimator,
    pub request_timeout: Duration,
    pub ready: AtomicBool,
}

impl AppState {
    pub fn new(shared: Shared) -> Self {
        Self(Arc::new(shared))
    }

    pub fn set_ready(&self, ready: bool) {
        self.0.ready.store(ready, Ordering::SeqCst);
    }
}

pub fn public_router(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/v1/systemone", post(systemone))
        .route("/v1/models", get(models))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(CatchPanicLayer::new())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    let request_id = request
                        .headers()
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("-");
                    // Never the body: states and questions can carry
                    // personal or financial data.
                    tracing::info_span!(
                        "call",
                        method = %request.method(),
                        path = %request.uri().path(),
                        request_id,
                        service = tracing::field::Empty,
                    )
                })
                .on_request(())
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .with_state(state)
}

pub fn admin_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn systemone(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
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

fn outcome_response(state: &Shared, service: &str, backend: &str, outcome: Outcome) -> Response {
    match outcome {
        Outcome::Answered {
            body,
            request_id,
            input_tokens,
            batch_callers,
        } => {
            if let Some(tokens) = input_tokens {
                state
                    .metrics
                    .input_tokens
                    .get_or_create(&Metrics::tokens(service, backend))
                    .inc_by(tokens);
            }
            let mut headers = json_headers(request_id.as_deref());
            headers.insert(BATCH_CALLERS, HeaderValue::from(batch_callers as u64));
            (StatusCode::OK, headers, body).into_response()
        }
        Outcome::Failed(error) => error.into_response(),
        Outcome::Rejected {
            status,
            body,
            retry_after,
            request_id,
        } => {
            let mut headers = json_headers(request_id.as_deref());
            if let Some(retry_after) = retry_after {
                insert_retry_after(&mut headers, retry_after);
            }
            (status, headers, body).into_response()
        }
    }
}

fn json_headers(request_id: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(value) = request_id.and_then(|id| HeaderValue::from_str(id).ok()) {
        headers.insert(REQUEST_ID, value);
    }
    headers
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

/// `GET /v1/models`: the models of every backend, in configuration order.
/// A System One backend's own list is served from memory for
/// `models_cache_ttl_ms`; a chat backend lists the exact names it serves.
async fn models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let state = &state.0;
    if state.registry.authenticate(&headers).is_none() {
        return GatewayError::unauthorized().into_response();
    }
    let deadline = Instant::now() + state.request_timeout;
    let mut entries = Vec::new();
    for backend in state.backends.iter() {
        match backend.list_models(deadline).await {
            Ok(list) => entries.extend(list),
            Err(outcome) => return outcome_response(state, "-", &backend.name, outcome),
        }
    }
    let mut list = String::from("[");
    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            list.push(',');
        }
        list.push_str(entry.get());
    }
    list.push(']');
    let mut body = crate::wire::ObjectWriter::with_capacity(list.len() + 12);
    body.field("models", &list);
    (StatusCode::OK, json_headers(None), body.finish()).into_response()
}

async fn not_found(method: Method, uri: Uri) -> Response {
    GatewayError::not_found(format!("no route for {method} {}", uri.path())).into_response()
}

async fn readyz(State(state): State<AppState>) -> Response {
    if state.0.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/openmetrics-text; version=1.0.0; charset=utf-8"),
        )],
        state.0.metrics.render(),
    )
        .into_response()
}
