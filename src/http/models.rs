//! `GET /v1/models`.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;
use tracing::{Instrument, Span};

use super::{AppState, Shared, json_headers, outcome_response, request_id};
use crate::error::GatewayError;
use crate::telemetry;

/// `GET /v1/models`: the models of every backend, in configuration order.
/// A System One backend's own list is served from memory for
/// `models_cache_ttl_ms`; a chat backend lists the exact names it serves.
pub(super) async fn models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let call = telemetry::models_call(request_id(&headers));
    let response = list_models(&state.snapshot(), &headers, &call)
        .instrument(call.clone())
        .await;
    telemetry::finish_call(&call, response.status());
    response
}

async fn list_models(state: &Shared, headers: &HeaderMap, call: &Span) -> Response {
    let Some(service_id) = state.registry.authenticate(headers) else {
        return GatewayError::unauthorized().into_response();
    };
    telemetry::adopt_caller(call, headers);
    call.record(
        "gateway.service",
        state.registry.get(service_id).name.as_str(),
    );
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
