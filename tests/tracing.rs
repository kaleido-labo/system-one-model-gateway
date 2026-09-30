//! Distributed tracing end to end, with no collector: the gateway runs with an
//! in-memory exporter, and the tests read back the spans it made and the
//! headers its mock backend received.
//!
//! A subscriber can be installed once per process, so every test shares one,
//! with the exporter and a log buffer behind it. Each test makes its own trace
//! ids and looks only at those, which keeps the tests independent.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{Harness, Reply, Scripted, Setup};
use opentelemetry::trace::{SpanId, SpanKind, Status, TraceId};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};
use serde_json::{Value, json};
use systemone_gateway::{Telemetry, TracingConfig, logs_filter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

struct Shared {
    exporter: InMemorySpanExporter,
    logs: Arc<Mutex<Vec<u8>>>,
    /// Kept alive: dropping it would stop the exporter.
    _telemetry: Telemetry,
}

struct LogSink(Arc<Mutex<Vec<u8>>>);

impl Write for LogSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs the subscriber the gateway's binary would: logs in text, minus the
/// trace spans, next to the trace layer.
fn shared() -> &'static Shared {
    static SHARED: OnceLock<Shared> = OnceLock::new();
    SHARED.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let telemetry = Telemetry::with_exporter(&TracingConfig::default(), exporter.clone());
        let logs = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&logs);
        Registry::default()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || LogSink(Arc::clone(&writer)))
                    .with_filter(logs_filter(EnvFilter::new("info"))),
            )
            .with(telemetry.layer())
            .init();
        Shared {
            exporter,
            logs,
            _telemetry: telemetry,
        }
    })
}

/// A trace id and span id no other test uses.
fn fresh_ids() -> (String, String) {
    (
        format!("{:032x}", fastrand::u128(1..)),
        format!("{:016x}", fastrand::u64(1..)),
    )
}

fn traceparent(trace: &str, span: &str, flags: &str) -> String {
    format!("00-{trace}-{span}-{flags}")
}

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn document() -> Value {
    json!({"document": "Order 1042 - refund request, 42.50 EUR", "customer": "Example Shop"})
}

fn body(state: &Value, model: &str, questions: Value) -> Value {
    json!({"state": state, "model": model, "questions": questions})
}

async fn call(h: &Harness, key: &str, body: &Value, headers: &[(&str, &str)]) -> Reply {
    let mut request = h
        .client
        .post(h.url("/v1/systemone"))
        .bearer_auth(key)
        .header("content-type", "application/json")
        .body(body.to_string());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    common::reply(request.send().await.unwrap()).await
}

fn header_of(headers: &axum::http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .unwrap_or_else(|| panic!("no {name} header in {headers:?}"))
        .to_str()
        .unwrap()
        .to_owned()
}

/// `(trace id, span id, flags)` of a `traceparent` header.
fn parse_traceparent(value: &str) -> (String, String, String) {
    let parts: Vec<&str> = value.split('-').collect();
    assert_eq!(parts.len(), 4, "{value}");
    assert_eq!(parts[0], "00", "{value}");
    (
        parts[1].to_owned(),
        parts[2].to_owned(),
        parts[3].to_owned(),
    )
}

/// Waits until `ready` holds for the spans of `trace`, then returns them,
/// oldest first. Spans end when the last task holding them lets go, a moment
/// after the caller has its answer.
async fn spans_of(trace: &str, ready: impl Fn(&[SpanData]) -> bool) -> Vec<SpanData> {
    for _ in 0..250 {
        let mut spans: Vec<SpanData> = shared()
            .exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|span| span.span_context.trace_id().to_string() == trace)
            .collect();
        if ready(&spans) {
            spans.sort_by_key(|span| span.start_time);
            return spans;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seen: Vec<String> = shared()
        .exporter
        .get_finished_spans()
        .unwrap()
        .iter()
        .map(|span| {
            format!(
                "{} trace={} parent={}",
                span.name,
                span.span_context.trace_id(),
                span.parent_span_id
            )
        })
        .collect();
    panic!("the spans of trace {trace} never showed up; exported: {seen:#?}");
}

fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
}

fn the<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let found = named(spans, name);
    assert_eq!(
        found.len(),
        1,
        "expected one {name} span, got {}",
        found.len()
    );
    found[0]
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.to_string())
}

fn span_id(span: &SpanData) -> String {
    span.span_context.span_id().to_string()
}

fn has_names(names: &'static [&'static str]) -> impl Fn(&[SpanData]) -> bool {
    move |spans| names.iter().all(|name| !named(spans, name).is_empty())
}

#[tokio::test]
async fn a_call_joins_its_callers_trace_and_carries_it_upstream() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let (trace, caller_span) = fresh_ids();
    let parent = traceparent(&trace, &caller_span, "01");
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &parent), ("tracestate", "vendor=abc")],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);

    let spans = spans_of(
        &trace,
        has_names(&["POST /v1/systemone", "systemone.batch", "upstream.attempt"]),
    )
    .await;
    let call = the(&spans, "POST /v1/systemone");
    let batch = the(&spans, "systemone.batch");
    let attempt = the(&spans, "upstream.attempt");

    // The gateway's span is a child of the caller's, and the tree below it.
    assert_eq!(call.parent_span_id.to_string(), caller_span);
    assert!(call.parent_span_is_remote);
    assert_eq!(call.span_kind, SpanKind::Server);
    assert_eq!(batch.parent_span_id, call.span_context.span_id());
    assert_eq!(attempt.parent_span_id, batch.span_context.span_id());
    assert_eq!(attempt.span_kind, SpanKind::Client);
    assert_eq!(call.span_context.trace_state().get("vendor"), Some("abc"));

    // What the spans say about the call.
    assert_eq!(attribute(call, "gateway.service").as_deref(), Some("ocr"));
    assert_eq!(
        attribute(call, "gateway.backend").as_deref(),
        Some("typesafe")
    );
    assert_eq!(
        attribute(call, "gateway.model").as_deref(),
        Some("jev-latest")
    );
    assert_eq!(
        attribute(call, "http.response.status_code").as_deref(),
        Some("200")
    );
    assert_eq!(
        attribute(call, "gateway.batch_callers").as_deref(),
        Some("1")
    );
    assert_eq!(
        attribute(call, "gateway.request_id").as_deref(),
        Some(reply.header("x-request-id"))
    );
    assert_eq!(
        attribute(batch, "gateway.batch.callers").as_deref(),
        Some("1")
    );
    assert_eq!(
        attribute(batch, "gateway.batch.questions").as_deref(),
        Some("1")
    );
    assert!(attribute(batch, "gateway.queue_wait_ms").is_some());
    assert_eq!(attribute(attempt, "gateway.attempt").as_deref(), Some("1"));
    assert_eq!(
        attribute(attempt, "http.response.status_code").as_deref(),
        Some("200")
    );
    assert_eq!(call.status, Status::Unset);
    assert_eq!(attempt.status, Status::Unset);

    // The upstream request continues the same trace from the attempt span.
    let sent = h.mock.state.headers_on("/v1/systemone");
    assert_eq!(sent.len(), 1);
    let (sent_trace, sent_span, flags) = parse_traceparent(&header_of(&sent[0], "traceparent"));
    assert_eq!(sent_trace, trace);
    assert_eq!(sent_span, span_id(attempt));
    assert_eq!(flags, "01");
    assert!(header_of(&sent[0], "tracestate").contains("vendor=abc"));
}

#[tokio::test]
async fn no_span_holds_a_state_or_a_question() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is the customer Example Shop?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    let spans = spans_of(
        &trace,
        has_names(&["POST /v1/systemone", "systemone.batch", "upstream.attempt"]),
    )
    .await;
    let everything = format!("{spans:?}");
    for secret in [
        "Order 1042",
        "Example Shop",
        "42.50",
        "refund request",
        "instructions",
    ] {
        assert!(!everything.contains(secret), "{secret} leaked into a span");
    }
}

#[tokio::test]
async fn calls_that_merge_share_one_batch_span_with_links_both_ways() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let ids: Vec<(String, String)> = (0..3).map(|_| fresh_ids()).collect();
    let header: Vec<String> = ids.iter().map(|(t, s)| traceparent(t, s, "01")).collect();
    let keys = ["key-ocr", "key-fraud", "key-triage"];
    let questions = [
        "Is this a refund?",
        "Does it look altered?",
        "Is the total readable?",
    ];
    let bodies: Vec<Value> = questions
        .iter()
        .map(|question| body(&document(), "jev-latest", json!({"q": noul(question)})))
        .collect();
    let headers: Vec<[(&str, &str); 1]> = header
        .iter()
        .map(|value| [("traceparent", value.as_str())])
        .collect();
    let (a, b, c) = tokio::join!(
        call(&h, keys[0], &bodies[0], &headers[0]),
        call(&h, keys[1], &bodies[1], &headers[1]),
        call(&h, keys[2], &bodies[2], &headers[2]),
    );
    for reply in [&a, &b, &c] {
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert_eq!(reply.header("x-systemone-gateway-batch-callers"), "3");
    }
    assert_eq!(h.mock.state.calls(), 1);

    let mut calls = Vec::new();
    for (trace, _) in &ids {
        let spans = spans_of(trace, has_names(&["POST /v1/systemone"])).await;
        calls.push(the(&spans, "POST /v1/systemone").clone());
    }
    // The batch hangs from whichever call opened it: find it by its parent.
    let all = shared().exporter.get_finished_spans().unwrap();
    let batches: Vec<&SpanData> = all
        .iter()
        .filter(|span| {
            span.name == "systemone.batch"
                && calls
                    .iter()
                    .any(|call| call.span_context.span_id() == span.parent_span_id)
        })
        .collect();
    assert_eq!(batches.len(), 1, "three merged calls make one batch");
    let batch = batches[0];
    assert_eq!(
        attribute(batch, "gateway.batch.callers").as_deref(),
        Some("3")
    );

    let first = calls
        .iter()
        .position(|call| call.span_context.span_id() == batch.parent_span_id)
        .unwrap();
    for (index, call) in calls.iter().enumerate() {
        assert_eq!(
            attribute(call, "gateway.batch_callers").as_deref(),
            Some("3")
        );
        let to_batch: Vec<_> = call
            .links
            .iter()
            .filter(|link| link.span_context.span_id() == batch.span_context.span_id())
            .collect();
        let from_batch = batch
            .links
            .iter()
            .filter(|link| link.span_context.span_id() == call.span_context.span_id())
            .count();
        if index == first {
            // The opener is the batch's parent: no link needed.
            assert!(to_batch.is_empty());
            assert_eq!(from_batch, 0);
        } else {
            assert_eq!(to_batch.len(), 1, "caller {index} links to the batch");
            assert_eq!(from_batch, 1, "the batch links to caller {index}");
        }
    }
    assert_eq!(batch.links.len(), 2);

    // The one upstream call belongs to the opener's trace.
    let sent = h.mock.state.headers_on("/v1/systemone");
    let (sent_trace, _, _) = parse_traceparent(&header_of(&sent[0], "traceparent"));
    assert_eq!(sent_trace, ids[first].0);
}

#[tokio::test]
async fn every_retry_is_its_own_attempt_span() {
    shared();
    let h = Harness::start(Setup::default()).await;
    h.mock
        .state
        .push(Scripted::status(503, r#"{"detail":"busy"}"#).header("retry-after-ms", "40"));
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);

    let spans = spans_of(&trace, |spans| named(spans, "upstream.attempt").len() == 2).await;
    let batch = the(&spans, "systemone.batch");
    let attempts = named(&spans, "upstream.attempt");
    let (failed, retried) = (attempts[0], attempts[1]);
    for attempt in &attempts {
        assert_eq!(attempt.parent_span_id, batch.span_context.span_id());
    }
    assert_eq!(attribute(failed, "gateway.attempt").as_deref(), Some("1"));
    assert_eq!(
        attribute(failed, "http.response.status_code").as_deref(),
        Some("503")
    );
    assert_eq!(attribute(failed, "error.type").as_deref(), Some("503"));
    assert!(
        matches!(failed.status, Status::Error { .. }),
        "{:?}",
        failed.status
    );
    let delay: u64 = attribute(failed, "gateway.retry_delay_ms")
        .unwrap()
        .parse()
        .unwrap();
    assert!(delay >= 30, "{delay}");
    assert_eq!(attribute(retried, "gateway.attempt").as_deref(), Some("2"));
    assert_eq!(
        attribute(retried, "http.response.status_code").as_deref(),
        Some("200")
    );
    assert!(attribute(retried, "gateway.retry_delay_ms").is_none());
    // The attempt that failed ended before the retry started.
    assert!(failed.end_time <= retried.start_time);

    // Each attempt carries its own span id in the same trace.
    let sent = h.mock.state.headers_on("/v1/systemone");
    assert_eq!(sent.len(), 2);
    let ids: Vec<String> = sent
        .iter()
        .map(|headers| {
            let (sent_trace, sent_span, _) = parse_traceparent(&header_of(headers, "traceparent"));
            assert_eq!(sent_trace, trace);
            sent_span
        })
        .collect();
    assert_eq!(ids, [span_id(failed), span_id(retried)]);
}

#[tokio::test]
async fn a_call_without_a_usable_traceparent_starts_its_own_trace() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let q = body(
        &document(),
        "jev-latest",
        json!({"q": noul("Is this a refund request?")}),
    );
    for header in [
        None,
        Some("not-a-traceparent"),
        Some("00-00000000000000000000000000000000-0000000000000000-01"),
    ] {
        let headers: Vec<(&str, &str)> = header
            .map(|value| ("traceparent", value))
            .into_iter()
            .collect();
        let before = h.mock.state.calls();
        let reply = call(&h, "key-ocr", &q, &headers).await;
        assert_eq!(reply.status, 200, "{}", reply.text);

        // The upstream request names the trace the gateway started.
        let sent = h.mock.state.headers_on("/v1/systemone");
        let (trace, sent_span, _) = parse_traceparent(&header_of(&sent[before], "traceparent"));
        let spans = spans_of(
            &trace,
            has_names(&["POST /v1/systemone", "upstream.attempt"]),
        )
        .await;
        let call = the(&spans, "POST /v1/systemone");
        assert_eq!(call.parent_span_id, SpanId::INVALID, "{header:?}");
        assert_eq!(sent_span, span_id(the(&spans, "upstream.attempt")));
    }
}

#[tokio::test]
async fn a_caller_that_did_not_sample_gets_no_spans_but_keeps_its_trace_going() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "00"))],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    // The upstream still hears about the trace, flagged as not sampled, so
    // that the decision holds all the way down.
    let sent = h.mock.state.headers_on("/v1/systemone");
    let (sent_trace, _, flags) = parse_traceparent(&header_of(&sent[0], "traceparent"));
    assert_eq!(sent_trace, trace);
    assert_eq!(flags, "00");
    // Give the spans time to show up, had they been kept.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let kept = shared()
        .exporter
        .get_finished_spans()
        .unwrap()
        .iter()
        .filter(|span| span.span_context.trace_id().to_string() == trace)
        .count();
    assert_eq!(kept, 0);
}

#[tokio::test]
async fn an_unauthenticated_caller_does_not_choose_the_trace() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "a-key-nobody-has",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 401);
    // The refused call is traced on its own, and its status is recorded.
    let request_id = reply.header("x-request-id").to_owned();
    let mut found = None;
    for _ in 0..250 {
        found = shared()
            .exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .find(|span| {
                attribute(span, "gateway.request_id").as_deref() == Some(request_id.as_str())
            });
        if found.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let span = found.expect("the refused call has a span");
    assert_eq!(span.parent_span_id, SpanId::INVALID);
    assert_ne!(
        span.span_context.trace_id(),
        TraceId::from_hex(&trace).unwrap()
    );
    assert_eq!(
        attribute(&span, "http.response.status_code").as_deref(),
        Some("401")
    );
    assert_eq!(h.mock.state.calls(), 0);
}

#[tokio::test]
async fn a_failed_call_is_an_error_span() {
    shared();
    let h = Harness::start(Setup {
        upstream: "max_retries = 0",
        ..Setup::default()
    })
    .await;
    h.mock
        .state
        .push(Scripted::status(500, r#"{"detail":"boom"}"#));
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 500, "{}", reply.text);
    let spans = spans_of(
        &trace,
        has_names(&["POST /v1/systemone", "systemone.batch", "upstream.attempt"]),
    )
    .await;
    for name in ["POST /v1/systemone", "systemone.batch", "upstream.attempt"] {
        assert!(
            matches!(the(&spans, name).status, Status::Error { .. }),
            "{name}"
        );
    }
    assert_eq!(
        attribute(
            the(&spans, "POST /v1/systemone"),
            "http.response.status_code"
        )
        .as_deref(),
        Some("500")
    );
}

#[tokio::test]
async fn a_replay_after_a_rejected_merge_hangs_from_each_calls_own_span() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let ids: Vec<(String, String)> = (0..2).map(|_| fresh_ids()).collect();
    let parents: Vec<String> = ids
        .iter()
        .map(|(trace, span)| traceparent(trace, span, "01"))
        .collect();
    let headers: Vec<[(&str, &str); 1]> = parents
        .iter()
        .map(|parent| [("traceparent", parent.as_str())])
        .collect();
    let poisoned = body(
        &document(),
        "jev-latest",
        json!({"q": noul("POISON: reject me")}),
    );
    let fine = body(
        &document(),
        "jev-latest",
        json!({"q": noul("Is this a refund request?")}),
    );
    let (bad, good) = tokio::join!(
        call(&h, "key-ocr", &poisoned, &headers[0]),
        call(&h, "key-fraud", &fine, &headers[1]),
    );
    assert_eq!(bad.status, 422, "{}", bad.text);
    assert_eq!(good.status, 200, "{}", good.text);

    for (trace, _) in &ids {
        // The caller's own trace holds a replay of its call alone. Both
        // calls are in the trace of the batch's first call, too.
        let spans = spans_of(trace, |spans| {
            named(spans, "systemone.batch")
                .iter()
                .any(|batch| attribute(batch, "gateway.batch.replay").is_some())
        })
        .await;
        let call = the(&spans, "POST /v1/systemone");
        let replays: Vec<_> = named(&spans, "systemone.batch")
            .into_iter()
            .filter(|batch| attribute(batch, "gateway.batch.replay").is_some())
            .collect();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].parent_span_id, call.span_context.span_id());
        assert_eq!(
            attribute(replays[0], "gateway.batch.callers").as_deref(),
            Some("1")
        );
    }
}

#[tokio::test]
async fn the_model_list_is_traced_and_propagated_too() {
    shared();
    let h = Harness::start(Setup::default()).await;
    let (trace, caller_span) = fresh_ids();
    let response = h
        .client
        .get(h.url("/v1/models"))
        .bearer_auth("key-ocr")
        .header("traceparent", traceparent(&trace, &caller_span, "01"))
        .send()
        .await
        .unwrap();
    let reply = common::reply(response).await;
    assert_eq!(reply.status, 200, "{}", reply.text);

    let spans = spans_of(&trace, has_names(&["GET /v1/models", "upstream.attempt"])).await;
    let call = the(&spans, "GET /v1/models");
    let attempt = the(&spans, "upstream.attempt");
    assert_eq!(call.parent_span_id.to_string(), caller_span);
    assert_eq!(attempt.parent_span_id, call.span_context.span_id());
    assert_eq!(
        attribute(attempt, "http.request.method").as_deref(),
        Some("GET")
    );
    let sent = h.mock.state.headers_on("/v1/models");
    let (sent_trace, sent_span, _) = parse_traceparent(&header_of(&sent[0], "traceparent"));
    assert_eq!(sent_trace, trace);
    assert_eq!(sent_span, span_id(attempt));
}

#[tokio::test]
async fn a_chat_backend_makes_one_attempt_span_per_question_under_the_batch() {
    shared();
    let h = Harness::start(Setup {
        chat: Some(""),
        ..Setup::default()
    })
    .await;
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "Qwen/Qwen2.5-7B-Instruct",
            json!({"a": noul("Is this a refund request?"), "b": noul("Is the total readable?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);

    let spans = spans_of(&trace, |spans| named(spans, "upstream.attempt").len() == 2).await;
    let batch = the(&spans, "systemone.batch");
    let attempts = named(&spans, "upstream.attempt");
    assert_eq!(attribute(batch, "gateway.backend").as_deref(), Some("hf"));
    let mut sent_spans: Vec<String> = h
        .mock
        .state
        .headers_on("/v1/chat/completions")
        .iter()
        .map(|headers| {
            let (sent_trace, sent_span, _) = parse_traceparent(&header_of(headers, "traceparent"));
            assert_eq!(sent_trace, trace);
            sent_span
        })
        .collect();
    let mut attempt_spans: Vec<String> = attempts
        .iter()
        .map(|attempt| {
            assert_eq!(attempt.parent_span_id, batch.span_context.span_id());
            span_id(attempt)
        })
        .collect();
    sent_spans.sort();
    attempt_spans.sort();
    assert_eq!(sent_spans, attempt_spans);
}

#[tokio::test]
async fn logs_keep_their_lines_and_show_none_of_the_trace_spans() {
    let shared = shared();
    let h = Harness::start(Setup::default()).await;
    h.mock
        .state
        .push(Scripted::status(503, r#"{"detail":"busy"}"#).header("retry-after-ms", "20"));
    let (trace, caller_span) = fresh_ids();
    let reply = call(
        &h,
        "key-ocr",
        &body(
            &document(),
            "jev-latest",
            json!({"q": noul("Is this a refund request?")}),
        ),
        &[("traceparent", &traceparent(&trace, &caller_span, "01"))],
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    spans_of(&trace, has_names(&["POST /v1/systemone"])).await;

    let logs = String::from_utf8(shared.logs.lock().unwrap().clone()).unwrap();
    // The router's log span and its fields, as before.
    let finished = logs
        .lines()
        .find(|line| {
            line.contains("finished processing request")
                && line.contains(reply.header("x-request-id"))
        })
        .unwrap_or_else(|| panic!("no access log line in:\n{logs}"));
    for field in [
        "method=POST",
        "path=/v1/systemone",
        "service=\"ocr\"",
        "status=200",
    ] {
        assert!(finished.contains(field), "{field} missing from {finished}");
    }
    assert!(logs.contains("retrying the upstream call"), "{logs}");
    // Nothing of the trace side: not a span name, not an attribute.
    for trace_only in [
        "upstream.attempt",
        "systemone.batch",
        "gateway.",
        "otel.",
        "http.response",
    ] {
        assert!(
            !logs.contains(trace_only),
            "{trace_only} leaked into the logs:\n{logs}"
        );
    }
}
