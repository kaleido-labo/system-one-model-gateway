//! End-to-end tests of the adaptive request rate: a 429 from the mock lowers
//! the backend's rate, and the gauge and the spacing of upstream calls show it.

mod common;

use std::time::{Duration, Instant};

use common::{Harness, Scripted, Setup};
use serde_json::{Value, json};
use tokio::time::sleep;

const QWEN: &str = "Qwen/Qwen2.5-7B-Instruct";

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn call_body(model: &str, questions: Value) -> Value {
    json!({
        "state": {"document": "Order 1042 - refund request, 42.50 EUR"},
        "model": model,
        "questions": questions,
    })
}

fn too_many_requests() -> Scripted {
    Scripted::status(429, r#"{"detail":"Too Many Requests"}"#).header("retry-after-ms", "20")
}

/// A gateway whose calls are not held to merge, so that each call books its
/// own request slot.
fn setup(upstream: &'static str) -> Setup {
    Setup {
        upstream,
        coalescing: "window_ms = 0",
        ..Setup::default()
    }
}

/// The value of a gauge or counter series, `None` while it is not exported.
fn sample(metrics: &str, name: &str, backend: &str) -> Option<f64> {
    let prefix = format!(r#"systemone_gateway_{name}{{backend="{backend}"}} "#);
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.parse().expect("a number"))
}

async fn limit(h: &Harness, backend: &str) -> Option<f64> {
    sample(&h.metrics().await, "requests_per_minute_limit", backend)
}

async fn decreases(h: &Harness, backend: &str) -> Option<f64> {
    sample(&h.metrics().await, "rate_decreases_total", backend)
}

#[tokio::test]
async fn every_backend_exports_its_limit_and_a_static_one_does_not_move() {
    let h = Harness::start(Setup {
        chat: Some("requests_per_minute = 300"),
        ..setup("requests_per_minute = 600")
    })
    .await;
    assert_eq!(limit(&h, "typesafe").await, Some(600.0));
    assert_eq!(limit(&h, "hf").await, Some(300.0));

    h.mock.state.push(too_many_requests());
    let reply = h
        .call(
            "key-ocr",
            call_body(
                "jev-latest",
                json!({"q": noul("Is this a refund request?")}),
            ),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);

    // A 429 was retried, and with adaptive_rate off, it taught the gateway nothing.
    assert_eq!(limit(&h, "typesafe").await, Some(600.0));
    assert_eq!(decreases(&h, "typesafe").await, None);
}

#[tokio::test]
async fn a_429_lowers_the_rate_and_the_calls_that_follow_are_spaced_further_apart() {
    // 600 a minute is a slot every 100 ms; a fifth of it is one every 500 ms.
    // The rate cannot recover within the test.
    let h = Harness::start(setup(
        "requests_per_minute = 600\nburst = 1\nadaptive_rate = true\n\
         adaptive_decrease = 0.2\nadaptive_recovery_ms = 60000",
    ))
    .await;
    assert_eq!(limit(&h, "typesafe").await, Some(600.0));

    h.mock.state.push(too_many_requests());
    let ask = |n: u32| {
        h.call(
            "key-ocr",
            call_body("jev-latest", json!({"q": noul(&format!("Question {n}?"))})),
        )
    };
    let first = ask(1).await;
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(limit(&h, "typesafe").await, Some(120.0));
    assert_eq!(decreases(&h, "typesafe").await, Some(1.0));

    for n in 2..=3 {
        assert_eq!(ask(n).await.status, 200);
    }
    // The 429 and its retry, then the two calls made at the lower rate.
    let arrivals = h.mock.state.arrivals();
    assert_eq!(arrivals.len(), 4);
    let gap = arrivals[3].duration_since(arrivals[2]);
    assert!(
        gap >= Duration::from_millis(400),
        "calls {gap:?} apart, where 120 a minute allows one every 500 ms"
    );
}

#[tokio::test]
async fn the_rate_climbs_back_once_the_backend_stops_refusing() {
    let h = Harness::start(setup(
        "requests_per_minute = 600\nadaptive_rate = true\nadaptive_decrease = 0.5\n\
         adaptive_increase_per_minute = 300\nadaptive_recovery_ms = 300",
    ))
    .await;
    h.mock.state.push(too_many_requests());
    let reply = h
        .call(
            "key-ocr",
            call_body(
                "jev-latest",
                json!({"q": noul("Is this a refund request?")}),
            ),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    // The counter cannot go back down, so it proves the rate was lowered even
    // if the recovery got there before this line did.
    assert_eq!(decreases(&h, "typesafe").await, Some(1.0));

    let deadline = Instant::now() + Duration::from_secs(10);
    while limit(&h, "typesafe").await != Some(600.0) {
        assert!(
            Instant::now() < deadline,
            "the limit stopped at {:?}",
            limit(&h, "typesafe").await
        );
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(decreases(&h, "typesafe").await, Some(1.0));
}

#[tokio::test]
async fn the_429s_of_a_chat_batch_lower_the_rate_once() {
    // Four questions make four chat calls at the same moment, and the backend
    // refuses each of them: one episode, one decrease.
    let h = Harness::start(Setup {
        chat: Some(
            "requests_per_minute = 6000\nadaptive_rate = true\nadaptive_decrease = 0.5\n\
             adaptive_recovery_ms = 60000",
        ),
        ..setup("")
    })
    .await;
    for _ in 0..4 {
        h.mock.state.push(too_many_requests());
    }
    let questions = json!({
        "a": noul("Is this a refund request?"),
        "b": noul("Is this an invoice?"),
        "c": noul("Is this a complaint?"),
        "d": noul("Is this urgent?"),
    });
    let reply = h.call("key-ocr", call_body(QWEN, questions)).await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        h.mock.state.chat_requests().len(),
        8,
        "four refused, four retried"
    );

    assert_eq!(limit(&h, "hf").await, Some(3000.0));
    assert_eq!(decreases(&h, "hf").await, Some(1.0));
    // The other backend has nothing to do with it.
    assert_eq!(limit(&h, "typesafe").await, Some(1200.0));
}
