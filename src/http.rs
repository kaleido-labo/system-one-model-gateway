//! The HTTP surface: the TypeSafe-compatible public API and the admin endpoints.
//!
//! Whatever backend answers, services see TypeSafe's API: the same request,
//! the same answers, the same errors.
//!
//! This file holds the routers, the shared state and what every handler uses
//! to build a response. The two public endpoints live in `systemone` and
//! `models`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tower_http::LatencyUnit;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::backend::{Backends, REQUEST_ID};
use crate::error::{GatewayError, insert_retry_after};
use crate::metrics::Metrics;
use crate::scheduling::Outcome;
use crate::services::ServiceRegistry;
use crate::wire::TokenEstimator;

mod models;
mod systemone;

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
        .route("/v1/systemone", post(systemone::systemone))
        .route("/v1/models", get(models::models))
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
        // No fallback took over: the caller gets what the backend gave.
        Outcome::Unavailable(outcome) => outcome_response(state, service, backend, *outcome),
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
