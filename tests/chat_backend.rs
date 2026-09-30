//! End-to-end tests of a chat backend: services call the gateway in System One
//! format, and the gateway asks the mock's OpenAI-compatible chat endpoint,
//! one question at a time, reading answers from `top_logprobs`.

mod common;

use std::time::{Duration, Instant};

use common::{Harness, Scripted, Setup};
use serde_json::{Value, json};

const QWEN: &str = "Qwen/Qwen2.5-7B-Instruct";

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn call_body(model: &str, questions: Value) -> Value {
    json!({
        "state": {"document": "Order 1042 - refund request, 42.50 EUR", "customer": "Example Shop"},
        "model": model,
        "questions": questions,
    })
}

fn chat_setup(chat: &'static str) -> Setup {
    Setup {
        chat: Some(chat),
        ..Setup::default()
    }
}

fn close(value: &Value, expected: f64) -> bool {
    value
        .as_f64()
        .is_some_and(|actual| (actual - expected).abs() < 1e-9)
}

#[tokio::test]
async fn every_question_type_comes_back_in_typesafe_shape() {
    let h = Harness::start(chat_setup("")).await;
    // Raw text: `serde_json::Value` would sort the options, and their order
    // decides which letter each one gets.
    let body = r#"{
        "state": {"document": "Order 1042 - refund request, 42.50 EUR", "customer": "Example Shop"},
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "questions": {
            "refund": {"type": "noul", "instructions": "Is this a refund request? yes=0.9"},
            "category": {
                "type": "choice",
                "instructions": "Which topic?",
                "criteria": {"billing": "Billing and payments", "shipping": null, "returns": "Returns and exchanges"}
            },
            "legibility": {
                "type": "score",
                "instructions": "How legible is the document?",
                "criteria": ["Unreadable", "Partly readable", "Clear"]
            }
        }
    }"#;
    let reply = h.call_raw("key-ocr", body).await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.header("x-systemone-gateway-backend"), "hf");
    assert!(reply.header("x-typesafe-request-id").starts_with("chat_"));
    let answers = &reply.body["answers"];

    // The mock puts 0.9 on " Yes" and "Yes" together and 0.09 on "No";
    // "Maybe" is no label and drops out.
    assert_eq!(answers["refund"]["type"], "noul");
    assert!(
        close(&answers["refund"]["noul"], 0.9 / 0.99),
        "{}",
        reply.text
    );
    assert!(answers["refund"].get("confidence").is_none());

    // A: 0.6, " B": 0.3, "The": 0.1 dropped.
    let category = &answers["category"];
    assert_eq!(category["choice"], "billing");
    assert!(close(&category["probabilities"]["billing"], 2.0 / 3.0));
    assert!(close(&category["probabilities"]["shipping"], 1.0 / 3.0));
    assert!(close(&category["probabilities"]["returns"], 0.0));
    assert!(close(&category["confidence"], 0.5));

    // "1": 0.5, "2": 0.3, "Level": 0.2 dropped.
    let legibility = &answers["legibility"];
    assert!(close(&legibility["score"], 0.625 + 2.0 * 0.375));
    assert_eq!(legibility["legend"]["2"], "Clear");
    assert!(close(&legibility["probabilities"]["0"], 0.0));
    assert!(close(&legibility["confidence"], (3.0 * 0.625 - 1.0) / 2.0));

    // One chat call per question, each asking for one token and its logprobs.
    let requests = h.mock.state.chat_requests();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert_eq!(request["model"], QWEN);
        assert_eq!(request["max_tokens"], 1);
        assert_eq!(request["logprobs"], true);
        assert_eq!(request["top_logprobs"], 5);
        let prompt = request["messages"][1]["content"].as_str().unwrap();
        assert!(
            prompt.starts_with("State:\n{\"document\":\"Order 1042 - refund request, 42.50 EUR\"")
        );
    }
    assert!(
        requests[1]["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains(
                "A. billing: Billing and payments\nB. shipping\nC. returns: Returns and exchanges\n"
            )
    );
    assert_eq!(
        h.mock.state.calls(),
        0,
        "nothing went to the System One backend"
    );

    // Usage adds up what the three chat calls billed.
    assert_eq!(
        reply.body["usage"]["input_tokens"],
        h.mock.state.chat_billed()
    );
    assert_eq!(reply.body["usage"]["output_tokens"], 3);
    assert_eq!(reply.body["model"], QWEN);
}

#[tokio::test]
async fn services_share_a_chat_batch_and_a_shared_question_is_asked_once() {
    let h = Harness::start(chat_setup("")).await;
    let (ocr, fraud) = tokio::join!(
        h.call(
            "key-ocr",
            call_body(QWEN, json!({"refund": noul("Is this a refund? yes=0.2")}))
        ),
        h.call(
            "key-fraud",
            call_body(
                QWEN,
                json!({
                    "is_refund": noul("Is this a refund? yes=0.2"),
                    "altered": noul("Was the amount altered? yes=0.6"),
                })
            )
        ),
    );
    assert_eq!(ocr.status, 200, "{}", ocr.text);
    assert_eq!(fraud.status, 200, "{}", fraud.text);
    assert_eq!(ocr.header("x-systemone-gateway-batch-callers"), "2");
    assert_eq!(h.mock.state.chat_requests().len(), 2);

    // yes=0.2: 0.2 on Yes, 0.72 on No. yes=0.6: 0.6 and 0.36.
    assert!(close(&ocr.body["answers"]["refund"]["noul"], 0.2 / 0.92));
    assert!(close(
        &fraud.body["answers"]["is_refund"]["noul"],
        0.2 / 0.92
    ));
    assert!(close(&fraud.body["answers"]["altered"]["noul"], 0.6 / 0.96));
    assert!(ocr.body["answers"].get("altered").is_none());

    // The two shares add up to exactly what the chat calls billed.
    assert_eq!(
        ocr.body["usage"]["input_tokens"].as_u64().unwrap()
            + fraud.body["usage"]["input_tokens"].as_u64().unwrap(),
        h.mock.state.chat_billed()
    );
    assert_eq!(
        ocr.body["usage"]["output_tokens"].as_u64().unwrap()
            + fraud.body["usage"]["output_tokens"].as_u64().unwrap(),
        2
    );
}

#[tokio::test]
async fn the_model_name_picks_the_backend() {
    let h = Harness::start(chat_setup("")).await;
    let jev = h
        .call("key-ocr", call_body("jev-latest", json!({"q": noul("A?")})))
        .await;
    assert_eq!(jev.status, 200);
    assert_eq!(jev.header("x-systemone-gateway-backend"), "typesafe");
    assert_eq!(h.mock.state.calls(), 1);
    assert_eq!(h.mock.state.chat_requests().len(), 0);

    let qwen = h
        .call("key-ocr", call_body(QWEN, json!({"q": noul("A?")})))
        .await;
    assert_eq!(qwen.status, 200);
    assert_eq!(qwen.header("x-systemone-gateway-backend"), "hf");
    assert_eq!(h.mock.state.calls(), 1);
    assert_eq!(h.mock.state.chat_requests().len(), 1);

    let unknown = h
        .call("key-ocr", call_body("gpt-4", json!({"q": noul("A?")})))
        .await;
    assert_eq!(unknown.status, 422);
    assert_eq!(unknown.body["error"]["param"], "model");
    assert_eq!(h.mock.state.calls() + h.mock.state.chat_requests().len(), 2);
}

#[tokio::test]
async fn a_choice_with_more_options_than_letters_is_refused_up_front() {
    let h = Harness::start(chat_setup("")).await;
    let options: serde_json::Map<String, Value> = (0..27)
        .map(|i| (format!("option{i}"), Value::Null))
        .collect();
    let questions = json!({"q": {"type": "choice", "instructions": "Which?", "criteria": options}});

    let refused = h.call("key-ocr", call_body(QWEN, questions.clone())).await;
    assert_eq!(refused.status, 422, "{}", refused.text);
    assert_eq!(refused.body["error"]["param"], "questions.q.criteria");
    assert_eq!(h.mock.state.chat_requests().len(), 0);

    // TypeSafe takes up to 255 options.
    let jev = h.call("key-ocr", call_body("jev-latest", questions)).await;
    assert_eq!(jev.status, 200);
}

#[tokio::test]
async fn an_answer_that_is_no_label_is_a_502() {
    let h = Harness::start(chat_setup("")).await;
    let reply = h
        .call("key-ocr", call_body(QWEN, json!({"q": noul("POISON")})))
        .await;
    assert_eq!(reply.status, 502, "{}", reply.text);
    assert_eq!(reply.body["error"]["type"], "upstream_error");
    let message = reply.body["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("labels") && message.contains("Sorry"),
        "{message}"
    );
}

#[tokio::test]
async fn a_429_on_a_chat_call_is_retried() {
    let h = Harness::start(chat_setup("")).await;
    h.mock
        .state
        .push(Scripted::status(429, r#"{"error":"rate limited"}"#).header("retry-after-ms", "50"));
    let reply = h
        .call("key-ocr", call_body(QWEN, json!({"q": noul("A? yes=0.9")})))
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(h.mock.state.chat_requests().len(), 2);
    let metrics = h.metrics().await;
    for line in [
        r#"systemone_gateway_upstream_retries_total{backend="hf"} 1"#,
        r#"systemone_gateway_upstream_calls_total{backend="hf",status="200"} 1"#,
    ] {
        assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
    }
}

#[tokio::test]
async fn a_refused_chat_key_is_a_502() {
    let h = Harness::start(Setup {
        gateway_key: "wrong-key",
        ..chat_setup("")
    })
    .await;
    let reply = h
        .call("key-ocr", call_body(QWEN, json!({"q": noul("A?")})))
        .await;
    assert_eq!(reply.status, 502);
    let message = reply.body["error"]["message"].as_str().unwrap();
    assert!(message.contains("\"hf\""), "{message}");
}

#[tokio::test]
async fn upstream_model_top_logprobs_and_extras_reach_the_chat_api() {
    let h = Harness::start(chat_setup(
        "upstream_model = \"example/document-judge\"\ntop_logprobs = 3\n\
         [backend.request_extras]\ntemperature = 0\nmax_tokens = 50\n",
    ))
    .await;
    let reply = h
        .call("key-ocr", call_body(QWEN, json!({"q": noul("A? yes=0.9")})))
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    let request = &h.mock.state.chat_requests()[0];
    assert_eq!(request["model"], "example/document-judge");
    assert_eq!(request["top_logprobs"], 3);
    assert_eq!(request["temperature"], 0);
    // The gateway's own fields win over the extras.
    assert_eq!(request["max_tokens"], 1);
    // The mock returned 3 candidates: " Yes", "Yes", "No".
    assert!(close(&reply.body["answers"]["q"]["noul"], 0.9 / 0.99));
    assert_eq!(reply.body["model"], "example/document-judge");
}

#[tokio::test]
async fn every_question_takes_its_own_request_slot() {
    // Two requests per second, no burst: three questions go 500 ms apart.
    let h = Harness::start(chat_setup("requests_per_minute = 120\nburst = 1")).await;
    let started = Instant::now();
    let reply = h
        .call(
            "key-ocr",
            call_body(
                QWEN,
                json!({"a": noul("A?"), "b": noul("B?"), "c": noul("C?")}),
            ),
        )
        .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert!(
        started.elapsed() >= Duration::from_millis(950),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn questions_that_cannot_all_leave_before_the_deadline_get_a_429() {
    // One request per second: the twelfth question would leave after 11 s,
    // past the 9 s request timeout, so nothing is sent.
    let h = Harness::start(chat_setup("requests_per_minute = 60\nburst = 1")).await;
    let questions: serde_json::Map<String, Value> = (0..12)
        .map(|i| (format!("q{i}"), noul(&format!("Question {i}?"))))
        .collect();
    let started = Instant::now();
    let reply = h
        .call("key-ocr", call_body(QWEN, Value::Object(questions)))
        .await;
    assert_eq!(reply.status, 429, "{}", reply.text);
    assert!(reply.headers.contains_key("retry-after"));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(h.mock.state.chat_requests().len(), 0);
}

#[tokio::test]
async fn the_model_list_covers_every_backend() {
    let h = Harness::start(chat_setup("models = [\"Qwen/*\", \"my-judge\"]")).await;
    let reply = h.get("key-ocr", "/v1/models").await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    let names: Vec<&str> = reply.body["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["jev-latest", "jev-preview", "my-judge"]);
}
