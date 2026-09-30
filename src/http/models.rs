//! `GET /v1/models`.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;

use super::{AppState, json_headers, outcome_response};
use crate::error::GatewayError;

/// `GET /v1/models`: the models of every backend, in configuration order.
/// A System One backend's own list is served from memory for
/// `models_cache_ttl_ms`; a chat backend lists the exact names it serves.
pub(super) async fn models(State(state): State<AppState>, headers: HeaderMap) -> Response {
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
