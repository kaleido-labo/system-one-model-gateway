//! The spans of a trace, and the `traceparent` that ties them to callers and
//! to upstream requests.
//!
//! One call makes this tree:
//!
//! ```text
//! POST /v1/systemone            server span, a child of the caller's span
//!   └─ systemone.batch          the upstream call that carried this call
//!        ├─ upstream.attempt    one per HTTP attempt, retries included
//!        └─ upstream.attempt
//! ```
//!
//! A batch serves several callers, and a span has one parent. The batch span
//! is a child of the call that opened the batch, so that an unmerged call
//! (the usual case) has its whole story in its own trace, and the
//! `traceparent` sent upstream carries the trace id of that call. Every other
//! call in the batch has a span link to the batch span, and the batch span
//! links back to each of them. From any caller's span you reach the batch and
//! its upstream attempts in one click, and from the batch you reach everyone
//! it served.
//!
//! Spans carry the service, backend, model, status and request id, and never
//! a state or a question: those can hold personal or financial data.

use axum::http::{HeaderMap, StatusCode};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context, KeyValue};
use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::{Span, field::Empty, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::TARGET;

/// The server span of a `POST /v1/systemone`. It is a root span until
/// `adopt_caller` gives it a parent.
pub fn systemone_call(request_id: &str) -> Span {
    info_span!(
        target: TARGET,
        "POST /v1/systemone",
        otel.kind = "server",
        otel.status_code = Empty,
        http.request.method = "POST",
        http.route = "/v1/systemone",
        http.response.status_code = Empty,
        gateway.request_id = request_id,
        gateway.service = Empty,
        gateway.backend = Empty,
        gateway.model = Empty,
        gateway.batch_callers = Empty,
    )
}

/// The server span of a `GET /v1/models`.
pub fn models_call(request_id: &str) -> Span {
    info_span!(
        target: TARGET,
        "GET /v1/models",
        otel.kind = "server",
        otel.status_code = Empty,
        http.request.method = "GET",
        http.route = "/v1/models",
        http.response.status_code = Empty,
        gateway.request_id = request_id,
        gateway.service = Empty,
    )
}

/// Makes `call` a child of the caller's span, from the `traceparent` and
/// `tracestate` headers. A missing or malformed header leaves `call` a root
/// span.
///
/// Only authenticated callers are adopted, so call this once the key has been
/// checked: a `traceparent` can carry a sampled flag, and honouring it from
/// anyone on the network would let anyone make the gateway record spans.
pub fn adopt_caller(call: &Span, headers: &HeaderMap) {
    if call.is_disabled() {
        return;
    }
    // Not `Context::current()`: the gateway attaches no OpenTelemetry context,
    // and an empty one cannot leak a stray parent.
    let parent = TraceContextPropagator::new()
        .extract_with_context(&Context::new(), &HeaderExtractor(headers));
    // Fails only when the span already started, which a fresh one has not.
    let _ = call.set_parent(parent);
}

/// Ends the story of a call: its status, and whether it counts as an error
/// (only the gateway's own and the backend's failures do, not a caller's
/// mistakes).
pub fn finish_call(call: &Span, status: StatusCode) {
    call.record("http.response.status_code", i64::from(status.as_u16()));
    if status.is_server_error() {
        call.record("otel.status_code", "ERROR");
    }
}

/// The span of a batch, opened for the call `first`.
pub fn batch(first: &Span, backend: &str, model: &str) -> Span {
    info_span!(
        target: TARGET,
        parent: first,
        "systemone.batch",
        otel.kind = "internal",
        otel.status_code = Empty,
        gateway.backend = backend,
        gateway.model = model,
        gateway.batch.callers = Empty,
        gateway.batch.questions = Empty,
        gateway.batch.deduplicated = Empty,
        gateway.queue_wait_ms = Empty,
        gateway.batch.replay = Empty,
    )
}

/// Links a call that joined a batch to that batch, and the batch to the call.
/// Links can point either way, and each side finds the other from its own
/// span.
pub fn link_to_batch(batch: &Span, call: &Span) {
    if batch.is_disabled() || call.is_disabled() {
        return;
    }
    let batch_context = batch.context().span().span_context().clone();
    let call_context = call.context().span().span_context().clone();
    let relation = |name: &'static str| vec![KeyValue::new("gateway.link", name)];
    call.add_link_with_attributes(batch_context, relation("batch"));
    batch.add_link_with_attributes(call_context, relation("caller"));
}

/// The span of one HTTP attempt to a backend. `number` counts from 1.
pub fn attempt(backend: &str, method: &'static str, number: u32) -> Span {
    info_span!(
        target: TARGET,
        "upstream.attempt",
        otel.kind = "client",
        otel.status_code = Empty,
        http.request.method = method,
        http.response.status_code = Empty,
        "error.type" = Empty,
        gateway.backend = backend,
        gateway.attempt = i64::from(number),
        gateway.retry_delay_ms = Empty,
    )
}

/// The attempt got an answer that is a success.
pub fn attempt_ok(attempt: &Span, status: u16) {
    attempt.record("http.response.status_code", i64::from(status));
}

/// The attempt failed. `status` is the HTTP status when there was one, and
/// `kind` names what went wrong when there was not (`timeout`,
/// `connection`): the OpenTelemetry `error.type`.
pub fn attempt_failed(attempt: &Span, status: Option<u16>, kind: &str) {
    if let Some(status) = status {
        attempt.record("http.response.status_code", i64::from(status));
        attempt.record("error.type", status.to_string().as_str());
    } else {
        attempt.record("error.type", kind);
    }
    attempt.record("otel.status_code", "ERROR");
}

/// Adds the `traceparent` and `tracestate` of `span` to the headers of an
/// upstream request. Does nothing when `span` is not part of a trace, which
/// is the case for every span when tracing is off.
pub fn inject(span: &Span, headers: &mut HeaderMap) {
    if span.is_disabled() {
        return;
    }
    TraceContextPropagator::new().inject_context(&span.context(), &mut HeaderInjector(headers));
}
