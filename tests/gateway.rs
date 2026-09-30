//! End-to-end tests: real HTTP from test "services" through the gateway to a
//! mock TypeSafe (see `common/mock_upstream.rs`).

mod common;

use std::time::{Duration, Instant};

use common::{Harness, Scripted, Setup, reply};
use serde_json::{Value, json};
use tokio::time::sleep;

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn document() -> Value {
    json!({"document": "Order 1042 - refund request, 42.50 EUR", "customer": "Example Shop"})
}

fn call_body(state: Value, questions: Value) -> Value {
    json!({"state": state, "model": "jev-latest", "questions": questions})
}

#[tokio::test]
async fn a_lone_call_is_forwarded_and_answered_as_is() {
    let h = Harness::start(Setup::default()).await;
    let reply = h
        .call(
            "key-ocr",
            call_body(
                document(),
                json!({"is_refund": noul("Is this a refund request?")}),
            ),
        )
        .await;

    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.body["model"], "jev-1.13.0");
    assert_eq!(
        reply.body["answers"]["is_refund"]["echo"],
        "Is this a refund request?"
    );
    assert_eq!(
        reply.body["usage"]["input_tokens"],
        h.mock.state.input_tokens_of(0)
    );
    assert_eq!(reply.header("x-typesafe-request-id"), "req_1");
    assert_eq!(reply.header("x-systemone-gateway-batch-callers"), "1");
    assert!(!reply.header("x-request-id").is_empty());
    // Alone, the call goes upstream under the caller's own question ids.
    assert_eq!(
        h.mock.state.questions_of(0),
        vec![("is_refund".to_owned(), json!("Is this a refund request?"))]
    );
}

#[tokio::test]
async fn calls_sharing_a_state_share_one_upstream_call() {
    let h = Harness::start(Setup::default()).await;
    let (ocr, fraud, triage) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(
                document(),
                json!({"is_refund": noul("Is this a refund request?"), "q": noul("Is the total readable?")})
            )
        ),
        h.call(
            "key-fraud",
            call_body(document(), json!({"q": noul("Does this document look altered?")}))
        ),
        h.call(
            "key-triage",
            call_body(
                document(),
                json!({"category": {
                    "type": "choice",
                    "instructions": "Which topic is this?",
                    "criteria": {"returns": null, "logistics": "Delivery, tracking, shipping"}
                }})
            )
        ),
    );

    for reply in [&ocr, &fraud, &triage] {
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert_eq!(reply.header("x-systemone-gateway-batch-callers"), "3");
        assert_eq!(reply.header("x-typesafe-request-id"), "req_1");
    }
    assert_eq!(h.mock.state.calls(), 1);
    assert_eq!(h.mock.state.questions_of(0).len(), 4);

    // Both ocr and fraud used the id "q"; each gets the answer to its own.
    assert_eq!(ocr.body["answers"].as_object().unwrap().len(), 2);
    assert_eq!(
        ocr.body["answers"]["is_refund"]["echo"],
        "Is this a refund request?"
    );
    assert_eq!(ocr.body["answers"]["q"]["echo"], "Is the total readable?");
    assert_eq!(fraud.body["answers"].as_object().unwrap().len(), 1);
    assert_eq!(
        fraud.body["answers"]["q"]["echo"],
        "Does this document look altered?"
    );
    assert_eq!(triage.body["answers"]["category"]["type"], "choice");
    assert_eq!(
        triage.body["answers"]["category"]["echo"],
        "Which topic is this?"
    );

    // The token usage handed back adds up to what TypeSafe billed.
    let charged: u64 = [&ocr, &fraud, &triage]
        .iter()
        .map(|reply| reply.body["usage"]["input_tokens"].as_u64().unwrap())
        .sum();
    assert_eq!(charged, h.mock.state.input_tokens_of(0));
    let output: u64 = [&ocr, &fraud, &triage]
        .iter()
        .map(|reply| reply.body["usage"]["output_tokens"].as_u64().unwrap())
        .sum();
    assert_eq!(output, 40);
}

#[tokio::test]
async fn state_formatting_does_not_prevent_merging() {
    let h = Harness::start(Setup::default()).await;
    let compact = r#"{"state":{"document":"Order 1042, 42.50 EUR","customer":"Example Shop"},"model":"jev-latest","questions":{"a":{"type":"noul","instructions":"A?"}}}"#;
    let pretty = "{\n  \"model\": \"jev-latest\",\n  \"state\": { \"document\" : \"Order 1042, 42.50 EUR\",\n    \"customer\" : \"Example Shop\" },\n  \"questions\": { \"b\": { \"type\": \"noul\", \"instructions\": \"B?\" } }\n}";
    let (a, b) = tokio::join!(
        h.call_raw("key-ocr", compact),
        h.call_raw("key-fraud", pretty)
    );
    assert_eq!(a.status, 200, "{}", a.text);
    assert_eq!(b.status, 200, "{}", b.text);
    assert_eq!(h.mock.state.calls(), 1);
    assert_eq!(a.body["answers"]["a"]["echo"], "A?");
    assert_eq!(b.body["answers"]["b"]["echo"], "B?");
}

#[tokio::test]
async fn identical_questions_from_two_services_are_sent_once() {
    let h = Harness::start(Setup::default()).await;
    let (ocr, fraud) = tokio::join!(
        h.call("key-ocr", call_body(document(), json!({"refund": noul("Is this a refund request?")}))),
        h.call(
            "key-fraud",
            call_body(
                document(),
                json!({"is_refund": noul("Is this a refund request?"), "altered": noul("Does it look altered?")})
            )
        ),
    );
    assert_eq!(ocr.status, 200, "{}", ocr.text);
    assert_eq!(fraud.status, 200, "{}", fraud.text);
    assert_eq!(h.mock.state.calls(), 1);
    assert_eq!(h.mock.state.questions_of(0).len(), 2);
    assert_eq!(
        ocr.body["answers"]["refund"]["echo"],
        "Is this a refund request?"
    );
    assert_eq!(
        fraud.body["answers"]["is_refund"]["echo"],
        "Is this a refund request?"
    );
    assert_eq!(
        fraud.body["answers"]["altered"]["echo"],
        "Does it look altered?"
    );
    assert!(
        h.metrics()
            .await
            .contains("systemone_gateway_deduplicated_questions_total 1")
    );
}

#[tokio::test]
async fn different_states_or_models_are_not_merged() {
    let h = Harness::start(Setup::default()).await;
    let question = json!({"q": noul("Is this a refund request?")});
    let mut other_model = call_body(document(), question.clone());
    other_model["model"] = json!("jev-1.13.0");
    let (a, b, c) = tokio::join!(
        h.call("key-ocr", call_body(document(), question.clone())),
        h.call(
            "key-fraud",
            call_body(json!("another document"), question.clone())
        ),
        h.call("key-triage", &other_model),
    );
    for reply in [&a, &b, &c] {
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert_eq!(reply.header("x-systemone-gateway-batch-callers"), "1");
    }
    assert_eq!(h.mock.state.calls(), 3);
}

#[tokio::test]
async fn unknown_keys_never_reach_typesafe() {
    let h = Harness::start(Setup::default()).await;
    let body = call_body(document(), json!({"q": noul("?")}));
    let wrong = h.call("key-nobody", &body).await;
    assert_eq!(wrong.status, 401);
    assert_eq!(wrong.body["error"]["type"], "authentication_error");

    let missing = reply(
        h.client
            .post(h.url("/v1/systemone"))
            .body(body.to_string())
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(missing.status, 401);
    assert_eq!(h.mock.state.calls(), 0);
}

#[tokio::test]
async fn bad_requests_are_answered_locally_with_the_field_at_fault() {
    let h = Harness::start(Setup::default()).await;
    let one_level = call_body(
        document(),
        json!({"size": {"type": "score", "instructions": "How big?", "criteria": ["only one"]}}),
    );
    let reply = h.call("key-ocr", &one_level).await;
    assert_eq!(reply.status, 422);
    assert_eq!(reply.body["error"]["type"], "validation_error");
    assert_eq!(reply.body["error"]["param"], "questions.size.criteria");

    let numeric_state = json!({"state": 42, "model": "jev-latest", "questions": {"q": noul("?")}});
    let reply = h.call("key-ocr", &numeric_state).await;
    assert_eq!(reply.status, 422);
    assert_eq!(reply.body["error"]["param"], "state");

    let reply = h.call_raw("key-ocr", "{not json").await;
    assert_eq!(reply.status, 400);
    assert_eq!(reply.body["error"]["type"], "invalid_request_error");

    assert_eq!(h.mock.state.calls(), 0);
}

#[tokio::test]
async fn a_429_from_typesafe_is_retried_after_retry_after_ms() {
    let h = Harness::start(Setup::default()).await;
    h.mock.state.push(
        Scripted::status(429, r#"{"detail":"Too Many Requests"}"#).header("retry-after-ms", "50"),
    );
    let started = Instant::now();
    let reply = h
        .call(
            "key-ocr",
            call_body(document(), json!({"q": noul("Is this a refund request?")})),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.body["answers"]["q"]["echo"],
        "Is this a refund request?"
    );
    assert_eq!(h.mock.state.calls(), 2);
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(
        h.metrics()
            .await
            .contains(r#"systemone_gateway_upstream_retries_total{backend="typesafe"} 1"#)
    );
}

#[tokio::test]
async fn when_retries_run_out_the_vendor_status_is_passed_through() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 1",
        ..Setup::default()
    })
    .await;
    let overloaded = r#"{"detail":"Overloaded"}"#;
    h.mock.state.push(Scripted::status(529, overloaded));
    h.mock.state.push(Scripted::status(529, overloaded));
    let reply = h
        .call("key-ocr", call_body(document(), json!({"q": noul("?")})))
        .await;
    assert_eq!(reply.status, 529);
    assert_eq!(reply.text, overloaded);
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn a_rejected_merged_call_is_replayed_so_only_the_culprit_fails() {
    let h = Harness::start(Setup::default()).await;
    let (bad, good) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(document(), json!({"q": noul("POISON: reject me")}))
        ),
        h.call(
            "key-fraud",
            call_body(document(), json!({"q": noul("Is this a refund request?")}))
        ),
    );
    assert_eq!(bad.status, 422, "{}", bad.text);
    assert_eq!(bad.body["detail"][0]["msg"], "poisoned question");
    assert_eq!(good.status, 200, "{}", good.text);
    assert_eq!(
        good.body["answers"]["q"]["echo"],
        "Is this a refund request?"
    );
    // The merged call, then one replay per caller.
    assert_eq!(h.mock.state.calls(), 3);
    assert!(
        h.metrics()
            .await
            .contains("systemone_gateway_isolated_replays_total 2")
    );
}

#[tokio::test]
async fn a_vendor_client_error_on_a_lone_call_is_passed_through() {
    let h = Harness::start(Setup::default()).await;
    let detail = r#"{"detail":[{"loc":["body","questions","q"],"msg":"context too long"}]}"#;
    h.mock.state.push(Scripted::status(422, detail));
    let reply = h
        .call("key-ocr", call_body(document(), json!({"q": noul("?")})))
        .await;
    assert_eq!(reply.status, 422);
    assert_eq!(reply.text, detail);
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_service_over_its_own_rate_gets_429_with_retry_after() {
    let h = Harness::start(Setup {
        services: vec![("ocr", "key-ocr", "requests_per_minute = 60\nburst = 1")],
        ..Setup::default()
    })
    .await;
    let body = call_body(document(), json!({"q": noul("?")}));
    assert_eq!(h.call("key-ocr", &body).await.status, 200);
    let refused = h.call("key-ocr", &body).await;
    assert_eq!(refused.status, 429);
    assert_eq!(refused.body["error"]["type"], "rate_limit_error");
    assert_eq!(refused.header("retry-after"), "1");
    assert!(refused.header("retry-after-ms").parse::<u64>().unwrap() > 500);
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn under_saturation_calls_join_a_waiting_batch_and_the_rest_are_shed() {
    // One upstream request per second: the second batch waits ~900 ms for
    // its slot, and a call arriving meanwhile for the same state rides along.
    let h = Harness::start(Setup {
        server: "request_timeout_ms = 5000",
        upstream: "requests_per_minute = 60\nburst = 1\nmax_queue_wait_ms = 1500",
        coalescing: "window_ms = 20",
        ..Setup::default()
    })
    .await;
    let first = call_body(json!("document A"), json!({"a": noul("A?")}));
    let second = call_body(json!("document B"), json!({"b": noul("B?")}));
    let joins = call_body(json!("document B"), json!({"c": noul("C?")}));
    let too_late = call_body(json!("document D"), json!({"d": noul("D?")}));

    let (a, b, c, d) = tokio::join!(
        h.call("key-ocr", &first),
        async {
            sleep(Duration::from_millis(100)).await;
            h.call("key-fraud", &second).await
        },
        async {
            sleep(Duration::from_millis(200)).await;
            h.call("key-triage", &joins).await
        },
        async {
            sleep(Duration::from_millis(300)).await;
            h.call("key-ocr", &too_late).await
        },
    );

    assert_eq!(a.status, 200, "{}", a.text);
    assert_eq!(a.header("x-systemone-gateway-batch-callers"), "1");
    for reply in [&b, &c] {
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert_eq!(reply.header("x-systemone-gateway-batch-callers"), "2");
    }
    assert_eq!(c.body["answers"]["c"]["echo"], "C?");
    // Its slot would have been ~1.7 s away, past max_queue_wait_ms.
    assert_eq!(d.status, 429, "{}", d.text);
    assert_eq!(d.header("retry-after"), "2");
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn models_are_proxied_and_cached() {
    let h = Harness::start(Setup::default()).await;
    let first = h.get("key-ocr", "/v1/models").await;
    let second = h.get("key-fraud", "/v1/models").await;
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(first.body["models"][0]["name"], "jev-latest");
    assert_eq!(second.body, first.body);
    assert_eq!(h.mock.state.models_calls(), 1);
    assert_eq!(h.get("key-nobody", "/v1/models").await.status, 401);
}

#[tokio::test]
async fn a_bad_gateway_key_shows_up_as_502_not_as_the_callers_401() {
    let h = Harness::start(Setup {
        gateway_key: "not-the-right-key",
        ..Setup::default()
    })
    .await;
    let reply = h
        .call("key-ocr", call_body(document(), json!({"q": noul("?")})))
        .await;
    assert_eq!(reply.status, 502);
    assert_eq!(reply.body["error"]["type"], "upstream_error");
    // A 401 is not retried.
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_slow_upstream_gets_a_504_within_the_request_timeout() {
    let h = Harness::start(Setup {
        server: "request_timeout_ms = 600",
        upstream: "max_queue_wait_ms = 100",
        coalescing: "window_ms = 10",
        ..Setup::default()
    })
    .await;
    h.mock.state.set_delay(Duration::from_secs(3));
    let started = Instant::now();
    let reply = h
        .call("key-ocr", call_body(document(), json!({"q": noul("?")})))
        .await;
    assert_eq!(reply.status, 504, "{}", reply.text);
    assert_eq!(reply.body["error"]["type"], "timeout_error");
    assert!(
        started.elapsed() < Duration::from_millis(1_500),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_service_limited_to_some_models_gets_403_for_others() {
    let h = Harness::start(Setup {
        services: vec![("ocr", "key-ocr", "allowed_models = [\"jev-latest\"]")],
        ..Setup::default()
    })
    .await;
    let mut body = call_body(document(), json!({"q": noul("?")}));
    body["model"] = json!("jev-preview");
    let reply = h.call("key-ocr", &body).await;
    assert_eq!(reply.status, 403);
    assert_eq!(reply.body["error"]["type"], "permission_error");
    assert_eq!(h.mock.state.calls(), 0);
}

#[tokio::test]
async fn metrics_account_per_service_and_per_upstream_call() {
    let h = Harness::start(Setup::default()).await;
    let (ocr, fraud) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(document(), json!({"a": noul("A?"), "b": noul("B?")}))
        ),
        h.call("key-fraud", call_body(document(), json!({"c": noul("C?")}))),
    );
    let _ = h
        .call(
            "key-nobody",
            call_body(document(), json!({"c": noul("C?")})),
        )
        .await;
    let metrics = h.metrics().await;
    for line in [
        r#"systemone_gateway_calls_total{service="ocr",status="200"} 1"#,
        r#"systemone_gateway_calls_total{service="fraud",status="200"} 1"#,
        r#"systemone_gateway_calls_total{service="-",status="401"} 1"#,
        r#"systemone_gateway_questions_total{service="ocr"} 2"#,
        r#"systemone_gateway_questions_total{service="fraud"} 1"#,
        r#"systemone_gateway_upstream_calls_total{backend="typesafe",status="200"} 1"#,
        "systemone_gateway_batch_callers_sum 2.0",
        "systemone_gateway_batch_callers_count 1",
    ] {
        assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
    }
    let ocr_tokens = ocr.body["usage"]["input_tokens"].as_u64().unwrap();
    let fraud_tokens = fraud.body["usage"]["input_tokens"].as_u64().unwrap();
    assert!(metrics.contains(&format!(
        r#"systemone_gateway_input_tokens_total{{service="ocr",backend="typesafe"}} {ocr_tokens}"#
    )));
    assert!(metrics.contains(&format!(
        r#"systemone_gateway_input_tokens_total{{service="fraud",backend="typesafe"}} {fraud_tokens}"#
    )));
}

#[tokio::test]
async fn health_readiness_and_unknown_routes() {
    let h = Harness::start(Setup::default()).await;
    assert_eq!(
        h.admin("/healthz").await,
        (reqwest::StatusCode::OK, "ok".to_owned())
    );
    assert_eq!(
        h.admin("/readyz").await,
        (reqwest::StatusCode::OK, "ready".to_owned())
    );
    let (status, metrics) = h.admin("/metrics").await;
    assert_eq!(status, 200);
    assert!(metrics.ends_with("# EOF\n"));
    let missing = h.get("key-ocr", "/v1/unknown").await;
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body["error"]["type"], "not_found_error");
}

#[tokio::test]
async fn shutdown_lets_in_flight_calls_finish() {
    let h = Harness::start(Setup::default()).await;
    h.mock.state.set_delay(Duration::from_millis(300));
    let request = h
        .client
        .post(h.url("/v1/systemone"))
        .bearer_auth("key-ocr")
        .body(call_body(document(), json!({"q": noul("Still there?")})).to_string())
        .send();
    let in_flight = tokio::spawn(request);
    sleep(Duration::from_millis(100)).await;
    h.gateway.shutdown().await.unwrap();
    let finished = reply(in_flight.await.unwrap().unwrap()).await;
    assert_eq!(finished.status, 200, "{}", finished.text);
    assert_eq!(finished.body["answers"]["q"]["echo"], "Still there?");
}

#[tokio::test]
async fn a_spent_tokens_per_second_budget_sheds_with_429() {
    // Ten tokens per second: the first call (~45 estimated tokens) overdraws
    // the bucket for about 3.7 s. TypeSafe takes 1.5 s to answer it, so the
    // next call would wait about 2.2 s: inside its 3.5 s deadline, far past
    // its 500 ms queue wait.
    let h = Harness::start(Setup {
        server: "request_timeout_ms = 3500",
        upstream: "tokens_per_second = 10\nmax_queue_wait_ms = 500",
        coalescing: "window_ms = 10",
        ..Setup::default()
    })
    .await;
    h.mock.state.set_delay(Duration::from_millis(1_500));
    let first = h
        .call("key-ocr", call_body(document(), json!({"a": noul("A?")})))
        .await;
    assert_eq!(first.status, 200, "{}", first.text);
    h.mock.state.set_delay(Duration::ZERO);
    let sent = Instant::now();
    let second = h
        .call(
            "key-fraud",
            call_body(json!("another document"), json!({"b": noul("B?")})),
        )
        .await;
    assert_eq!(second.status, 429, "{}", second.text);
    let message = second.body["error"]["message"].as_str().unwrap();
    assert!(message.contains("tokens_per_second"), "{message}");
    // Refused as soon as the budget shows it cannot go in time, instead of
    // being held until its deadline and sent anyway.
    assert!(
        sent.elapsed() < Duration::from_millis(300),
        "answered after {:?}",
        sent.elapsed()
    );
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_call_stuck_behind_busy_upstream_slots_gets_429_instead_of_a_late_send() {
    // One upstream call at a time, and TypeSafe takes a second to answer: the
    // second call runs out of patience while it waits for the only connection.
    let h = Harness::start(Setup {
        server: "request_timeout_ms = 5000",
        upstream: "max_concurrency = 1\nmax_queue_wait_ms = 300",
        coalescing: "window_ms = 10",
        ..Setup::default()
    })
    .await;
    h.mock.state.set_delay(Duration::from_secs(1));
    let (first, (second, waited)) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(json!("document A"), json!({"a": noul("A?")}))
        ),
        async {
            sleep(Duration::from_millis(50)).await;
            let sent = Instant::now();
            let reply = h
                .call(
                    "key-fraud",
                    call_body(json!("document B"), json!({"b": noul("B?")})),
                )
                .await;
            (reply, sent.elapsed())
        },
    );
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(second.status, 429, "{}", second.text);
    assert!(second.header("retry-after-ms").parse::<u64>().unwrap() >= 500);
    // The 429 comes when the caller's 300 ms run out, not when the busy
    // connection frees up a second later.
    assert!(
        waited < Duration::from_millis(700),
        "answered after {waited:?}"
    );
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_zero_queue_wait_still_serves_an_idle_gateway() {
    // The merge window is the gateway's own delay and must not count against
    // a caller that accepts no queueing at all.
    let h = Harness::start(Setup {
        upstream: "max_queue_wait_ms = 0",
        coalescing: "window_ms = 10",
        ..Setup::default()
    })
    .await;
    for n in 0..3 {
        let reply = h
            .call(
                "key-ocr",
                call_body(json!(format!("document {n}")), json!({"q": noul("?")})),
            )
            .await;
        assert_eq!(reply.status, 200, "{}", reply.text);
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(h.mock.state.calls(), 3);
}

#[tokio::test]
async fn batches_held_by_a_429_pause_leave_spaced_out() {
    // Two requests per second. TypeSafe takes 300 ms to answer, so the other
    // two batches book their slots (at 0.5 s and 1 s) before the 429 asking
    // for 1.5 s comes back. They must not all fire the moment the pause ends.
    let h = Harness::start(Setup {
        upstream: "requests_per_minute = 120\nburst = 1\nmax_queue_wait_ms = 2500",
        coalescing: "window_ms = 10",
        ..Setup::default()
    })
    .await;
    h.mock.state.set_delay(Duration::from_millis(300));
    h.mock.state.push(
        Scripted::status(429, r#"{"detail":"Too Many Requests"}"#).header("retry-after-ms", "1500"),
    );
    let (a, b, c) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(json!("document A"), json!({"a": noul("A?")}))
        ),
        async {
            sleep(Duration::from_millis(50)).await;
            h.call(
                "key-fraud",
                call_body(json!("document B"), json!({"b": noul("B?")})),
            )
            .await
        },
        async {
            sleep(Duration::from_millis(100)).await;
            h.call(
                "key-triage",
                call_body(json!("document C"), json!({"c": noul("C?")})),
            )
            .await
        },
    );
    for reply in [&a, &b, &c] {
        assert_eq!(reply.status, 200, "{}", reply.text);
    }
    // The 429, then three calls after the pause, half a second apart.
    let arrivals = h.mock.state.arrivals();
    assert_eq!(arrivals.len(), 4);
    assert!(arrivals[1].duration_since(arrivals[0]) >= Duration::from_millis(1_400));
    for pair in arrivals[1..].windows(2) {
        let gap = pair[1].duration_since(pair[0]);
        assert!(
            gap >= Duration::from_millis(400),
            "calls {gap:?} apart after the pause"
        );
    }
}

#[tokio::test]
async fn a_vendor_403_reaches_the_caller_as_is() {
    // Unlike a 401, a 403 says something about what was asked, such as a
    // model the account may not use, so the caller needs to see it.
    let h = Harness::start(Setup::default()).await;
    let denied = r#"{"detail":"model not available on this plan"}"#;
    h.mock.state.push(Scripted::status(403, denied));
    let reply = h
        .call("key-ocr", call_body(document(), json!({"q": noul("?")})))
        .await;
    assert_eq!(reply.status, 403);
    assert_eq!(reply.text, denied);
}
