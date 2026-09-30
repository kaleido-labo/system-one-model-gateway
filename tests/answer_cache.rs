//! End-to-end tests of the answer cache: identical calls a moment apart,
//! through the gateway to the mock TypeSafe, which counts what reaches it.

mod common;

use std::time::Duration;

use common::{Harness, Scripted, Setup};
use serde_json::{Value, json};
use tokio::time::sleep;

/// The harness has no slot for a `[cache]` table, and `coalescing` is the
/// last table before the services, so the cache rides along there.
const CACHE_ON: &str =
    "window_ms = 150\n[cache]\nenabled = true\nttl_ms = 60000\nmax_entries = 100";
const CACHE_SHORT_TTL: &str = "window_ms = 150\n[cache]\nenabled = true\nttl_ms = 200";

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn call_body(state: &str, questions: Value) -> Value {
    json!({"state": state, "model": "jev-latest", "questions": questions})
}

fn cached() -> Setup {
    Setup {
        coalescing: CACHE_ON,
        ..Setup::default()
    }
}

#[tokio::test]
async fn the_cache_is_off_unless_the_file_turns_it_on() {
    let h = Harness::start(Setup::default()).await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    for _ in 0..2 {
        let reply = h.call("key-ocr", &body).await;
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert!(reply.headers.get("x-systemone-gateway-cache").is_none());
    }
    assert_eq!(h.mock.state.calls(), 2);
    assert!(!h.metrics().await.contains("cache_hits_total"));
}

#[tokio::test]
async fn an_identical_call_is_answered_without_asking_the_backend() {
    let h = Harness::start(cached()).await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));

    let first = h.call("key-ocr", &body).await;
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(first.header("x-systemone-gateway-cache"), "miss");

    let second = h.call("key-ocr", &body).await;
    assert_eq!(second.status, 200, "{}", second.text);
    assert_eq!(second.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(second.header("x-systemone-gateway-backend"), "typesafe");
    assert!(second.headers.get("x-typesafe-request-id").is_none());
    assert!(
        second
            .headers
            .get("x-systemone-gateway-batch-callers")
            .is_none()
    );
    assert_eq!(h.mock.state.calls(), 1);

    // Same answers and model as the backend gave, and nothing billed.
    assert_eq!(second.body["answers"], first.body["answers"]);
    assert_eq!(second.body["model"], "jev-1.13.0");
    assert_eq!(
        second.body["usage"],
        json!({"input_tokens": 0, "output_tokens": 0})
    );

    let metrics = h.metrics().await;
    assert!(
        metrics.contains(r#"systemone_gateway_cache_hits_total{backend="typesafe"} 1"#),
        "{metrics}"
    );
    assert!(
        metrics.contains(r#"systemone_gateway_cache_misses_total{backend="typesafe"} 1"#),
        "{metrics}"
    );
    assert!(
        metrics.contains("systemone_gateway_cache_entries 1"),
        "{metrics}"
    );
    // The hit is still a call, and its question still counts.
    assert!(
        metrics.contains(r#"systemone_gateway_calls_total{service="ocr",status="200"} 2"#),
        "{metrics}"
    );
    assert!(
        metrics.contains(r#"systemone_gateway_questions_total{service="ocr"} 2"#),
        "{metrics}"
    );
    // Only the first call was billed.
    assert!(
        metrics.contains(&format!(
            r#"systemone_gateway_input_tokens_total{{service="ocr",backend="typesafe"}} {}"#,
            h.mock.state.input_tokens_of(0)
        )),
        "{metrics}"
    );
}

#[tokio::test]
async fn another_service_with_other_ids_reuses_the_answer() {
    let h = Harness::start(cached()).await;
    h.call(
        "key-ocr",
        call_body("Order 1042", json!({"refund": noul("Refund?")})),
    )
    .await;
    let reply = h
        .call(
            "key-fraud",
            call_body("Order 1042", json!({"is_refund": noul("Refund?")})),
        )
        .await;
    assert_eq!(reply.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(reply.body["answers"]["is_refund"]["echo"], "Refund?");
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_partial_hit_sends_only_the_missing_question() {
    let h = Harness::start(cached()).await;
    h.call(
        "key-ocr",
        call_body("Order 1042", json!({"refund": noul("Refund?")})),
    )
    .await;

    let reply = h
        .call(
            "key-ocr",
            call_body(
                "Order 1042",
                json!({"legible": noul("Is it legible?"), "refund": noul("Refund?")}),
            ),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.header("x-systemone-gateway-cache"), "partial");
    assert_eq!(reply.header("x-typesafe-request-id"), "req_2");
    assert_eq!(h.mock.state.calls(), 2);
    assert_eq!(
        h.mock.state.questions_of(1),
        vec![("legible".to_owned(), json!("Is it legible?"))]
    );
    // Both answers come back under the caller's ids, in the caller's order.
    let ids: Vec<_> = reply.body["answers"].as_object().unwrap().keys().collect();
    assert_eq!(ids, ["legible", "refund"]);
    assert_eq!(reply.body["answers"]["legible"]["echo"], "Is it legible?");
    assert_eq!(reply.body["answers"]["refund"]["echo"], "Refund?");
    // The usage is what the smaller upstream call cost; the cached part is free.
    assert_eq!(
        reply.body["usage"]["input_tokens"],
        h.mock.state.input_tokens_of(1)
    );
    assert_eq!(reply.body["usage"]["output_tokens"], 10);

    // Both questions are now cached.
    let third = h
        .call(
            "key-ocr",
            call_body(
                "Order 1042",
                json!({"legible": noul("Is it legible?"), "refund": noul("Refund?")}),
            ),
        )
        .await;
    assert_eq!(third.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn another_state_or_model_is_not_a_hit() {
    let h = Harness::start(cached()).await;
    h.call(
        "key-ocr",
        call_body("Order 1042", json!({"refund": noul("Refund?")})),
    )
    .await;
    let other_state = h
        .call(
            "key-ocr",
            call_body("Order 1043", json!({"refund": noul("Refund?")})),
        )
        .await;
    assert_eq!(other_state.header("x-systemone-gateway-cache"), "miss");

    let other_model = h
        .call(
            "key-ocr",
            json!({"state": "Order 1042", "model": "jev-preview", "questions": {"refund": noul("Refund?")}}),
        )
        .await;
    assert_eq!(other_model.header("x-systemone-gateway-cache"), "miss");

    let other_extra = h
        .call(
            "key-ocr",
            json!({"state": "Order 1042", "model": "jev-latest", "temperature": 1, "questions": {"refund": noul("Refund?")}}),
        )
        .await;
    assert_eq!(other_extra.header("x-systemone-gateway-cache"), "miss");
    assert_eq!(h.mock.state.calls(), 4);
}

#[tokio::test]
async fn no_cache_skips_the_read_but_still_writes() {
    let h = Harness::start(cached()).await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    h.call("key-ocr", &body).await;

    let response = h
        .client
        .post(h.url("/v1/systemone"))
        .bearer_auth("key-ocr")
        .header("cache-control", "no-cache")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let fresh = common::reply(response).await;
    assert_eq!(fresh.status, 200, "{}", fresh.text);
    assert_eq!(fresh.header("x-systemone-gateway-cache"), "miss");
    assert_eq!(h.mock.state.calls(), 2);
    // The fresh answer is billed, and the bypass is neither hit nor miss.
    assert_eq!(
        fresh.body["usage"]["input_tokens"],
        h.mock.state.input_tokens_of(1)
    );
    let metrics = h.metrics().await;
    assert!(
        metrics.contains(r#"systemone_gateway_cache_misses_total{backend="typesafe"} 1"#),
        "{metrics}"
    );

    let next = h.call("key-ocr", &body).await;
    assert_eq!(next.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn errors_are_never_cached() {
    let h = Harness::start(cached()).await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    h.mock
        .state
        .push(Scripted::status(422, r#"{"detail":"rejected"}"#));
    let failed = h.call("key-ocr", &body).await;
    assert_eq!(failed.status, 422, "{}", failed.text);
    assert!(failed.headers.get("x-systemone-gateway-cache").is_none());

    let retried = h.call("key-ocr", &body).await;
    assert_eq!(retried.status, 200, "{}", retried.text);
    assert_eq!(retried.header("x-systemone-gateway-cache"), "miss");
    assert_eq!(h.mock.state.calls(), 2);

    let next = h.call("key-ocr", &body).await;
    assert_eq!(next.header("x-systemone-gateway-cache"), "hit");

    // A body the gateway cannot read is not kept either.
    let other = call_body("Order 1043", json!({"refund": noul("Refund?")}));
    h.mock.state.push(Scripted::status(200, "<html>"));
    let garbage = h.call("key-ocr", &other).await;
    assert_eq!(garbage.text, "<html>");
    let again = h.call("key-ocr", &other).await;
    assert_eq!(again.header("x-systemone-gateway-cache"), "miss");
}

#[tokio::test]
async fn an_answer_is_dropped_when_its_ttl_passes() {
    let h = Harness::start(Setup {
        coalescing: CACHE_SHORT_TTL,
        ..Setup::default()
    })
    .await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    h.call("key-ocr", &body).await;
    assert_eq!(
        h.call("key-ocr", &body)
            .await
            .header("x-systemone-gateway-cache"),
        "hit"
    );
    sleep(Duration::from_millis(300)).await;
    let late = h.call("key-ocr", &body).await;
    assert_eq!(late.header("x-systemone-gateway-cache"), "miss");
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn a_merged_call_fills_the_cache_for_each_of_its_questions() {
    let h = Harness::start(cached()).await;
    let (ocr, fraud) = tokio::join!(
        h.call(
            "key-ocr",
            call_body("Order 1042", json!({"refund": noul("Refund?")}))
        ),
        h.call(
            "key-fraud",
            call_body("Order 1042", json!({"altered": noul("Altered?")}))
        ),
    );
    assert_eq!(ocr.header("x-systemone-gateway-batch-callers"), "2");
    assert_eq!(fraud.header("x-systemone-gateway-batch-callers"), "2");
    assert_eq!(h.mock.state.calls(), 1);

    let both = h
        .call(
            "key-triage",
            call_body(
                "Order 1042",
                json!({"a": noul("Altered?"), "b": noul("Refund?")}),
            ),
        )
        .await;
    assert_eq!(both.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(both.body["answers"]["a"]["echo"], "Altered?");
    assert_eq!(both.body["answers"]["b"]["echo"], "Refund?");
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_backend_can_opt_out_of_the_cache() {
    let h = Harness::start(Setup {
        coalescing: CACHE_ON,
        upstream: "cache = false",
        ..Setup::default()
    })
    .await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    for _ in 0..2 {
        let reply = h.call("key-ocr", &body).await;
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert!(reply.headers.get("x-systemone-gateway-cache").is_none());
    }
    assert_eq!(h.mock.state.calls(), 2);
    assert!(
        h.metrics()
            .await
            .contains("systemone_gateway_cache_entries 0")
    );
}

#[tokio::test]
async fn a_chat_backend_is_cached_like_any_other() {
    let h = Harness::start(Setup {
        coalescing: CACHE_ON,
        chat: Some(""),
        ..Setup::default()
    })
    .await;
    let body = json!({
        "state": "Order 1042",
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "questions": {"refund": noul("Refund? yes=0.9")},
    });
    let first = h.call("key-ocr", &body).await;
    assert_eq!(first.status, 200, "{}", first.text);
    let second = h.call("key-ocr", &body).await;
    assert_eq!(second.header("x-systemone-gateway-cache"), "hit");
    assert_eq!(second.header("x-systemone-gateway-backend"), "hf");
    assert_eq!(second.body["answers"], first.body["answers"]);
    assert_eq!(h.mock.state.chat_requests().len(), 1);
}

#[tokio::test]
async fn a_hit_is_still_checked_and_counted_against_the_service() {
    let h = Harness::start(Setup {
        coalescing: CACHE_ON,
        services: vec![
            ("ocr", "key-ocr", ""),
            (
                "limited",
                "key-limited",
                "requests_per_minute = 60\nburst = 1",
            ),
            ("narrow", "key-narrow", "allowed_models = [\"jev-preview\"]"),
        ],
        ..Setup::default()
    })
    .await;
    let body = call_body("Order 1042", json!({"refund": noul("Refund?")}));
    h.call("key-ocr", &body).await;

    // The answer is cached, but it is not for this service to see.
    assert_eq!(h.call("key-narrow", &body).await.status, 403);
    let unknown = h.call("wrong-key", &body).await;
    assert_eq!(unknown.status, 401);

    // A hit spends one request of the service's own budget.
    let first = h.call("key-limited", &body).await;
    assert_eq!(first.header("x-systemone-gateway-cache"), "hit");
    let second = h.call("key-limited", &body).await;
    assert_eq!(second.status, 429, "{}", second.text);
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn an_invalid_call_is_refused_even_when_its_state_is_cached() {
    let h = Harness::start(cached()).await;
    h.call(
        "key-ocr",
        call_body("Order 1042", json!({"refund": noul("Refund?")})),
    )
    .await;
    let invalid = h
        .call(
            "key-ocr",
            call_body(
                "Order 1042",
                json!({"refund": noul("Refund?"), "bad": {"type": "noul"}}),
            ),
        )
        .await;
    assert_eq!(invalid.status, 422, "{}", invalid.text);
    assert_eq!(h.mock.state.calls(), 1);
}
