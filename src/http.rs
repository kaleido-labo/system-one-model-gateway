//! The HTTP surface: the TypeSafe-compatible public API and the admin endpoints.
//!
//! Whatever backend answers, services see TypeSafe's API: the same request,
//! the same answers, the same errors.
//!
//! This file holds the routers, the shared state and what every handler uses
//! to build a response. The two public endpoints live in `systemone` and
//! `models`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
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
use crate::cache::AnswerCache;
use crate::error::{GatewayError, insert_retry_after};
use crate::metrics::Metrics;
use crate::scheduling::Outcome;
use crate::services::ServiceRegistry;
use crate::wire::TokenEstimator;

mod admin;
mod models;
mod systemone;

pub use admin::AdminToken;

/// How many calls shared the upstream call that answered this one.
pub const BATCH_CALLERS: HeaderName = HeaderName::from_static("x-systemone-gateway-batch-callers");
/// Whether the answer cache served the call: `hit`, `partial` or `miss`.
pub const CACHE: HeaderName = HeaderName::from_static("x-systemone-gateway-cache");
/// The backend that answered.
pub const BACKEND: HeaderName = HeaderName::from_static("x-systemone-gateway-backend");

/// What the handlers share. Cloning it shares it.
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    /// The configuration in force. A handler clones the `Arc` once, at the
    /// start of a call (`AppState::snapshot`), and uses that for the whole
    /// call, so a reload can land at any moment without a call ever seeing
    /// half of the old configuration and half of the new one.
    ///
    /// A lock that is only held to clone or replace an `Arc` costs about what
    /// an atomic does, never waits on anything slow and needs no dependency,
    /// which is why this is not an `ArcSwap`.
    current: RwLock<Arc<Shared>>,
    /// Set when `server.admin_token_env` is: `/metrics` then needs it. It is
    /// read at start and never reloaded.
    admin_token: Option<AdminToken>,
    ready: AtomicBool,
}

/// The part of the state that a configuration reload replaces: everything a
/// call reads from the configuration, as of one moment.
pub struct Shared {
    pub registry: ServiceRegistry,
    pub backends: Backends,
    /// The same for every generation: counters keep counting across reloads.
    pub metrics: Arc<Metrics>,
    pub estimator: TokenEstimator,
    /// `None` unless `[cache]` is enabled. Shared with the next generation
    /// when `[cache]` did not change, so a reload does not empty it.
    pub cache: Option<Arc<AnswerCache>>,
    pub request_timeout: Duration,
}

impl AppState {
    pub fn new(shared: Shared, admin_token: Option<AdminToken>) -> Self {
        Self(Arc::new(Inner {
            current: RwLock::new(Arc::new(shared)),
            admin_token,
            ready: AtomicBool::new(false),
        }))
    }

    /// The configuration as it is now. Take it once per call.
    pub fn snapshot(&self) -> Arc<Shared> {
        // The lock guards a pointer that is replaced in one statement, so a
        // panic elsewhere cannot leave it half written.
        Arc::clone(
            &self
                .0
                .current
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Makes `shared` the configuration of every call that starts from now
    /// on. Calls already running keep the one they started with.
    pub fn replace(&self, shared: Shared) {
        *self
            .0
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(shared);
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
                    let request_id = request_id(request.headers());
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

/// The `x-request-id` of a request, set by the router before any handler
/// runs: the id that logs, traces and the response header share.
fn request_id(headers: &HeaderMap) -> &str {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
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

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(token) = &state.0.admin_token
        && !token.allows(&headers)
    {
        return GatewayError::admin_unauthorized().into_response();
    }
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/openmetrics-text; version=1.0.0; charset=utf-8"),
        )],
        state.snapshot().metrics.render(),
    )
        .into_response()
}
