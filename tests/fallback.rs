//! End-to-end tests of the circuit breaker and of fallback backends: what a
//! service sees when a backend is down, and which backend answers.

mod common;

use std::time::Duration;

use common::{Harness, MockUpstream, Reply, Scripted, Setup, UPSTREAM_KEY, reply};
use serde_json::{Value, json};
use systemone_gateway::{Config, Gateway, hash_key};
use tokio::time::sleep;

/// Nothing listens on the discard port: connecting is refused at once.
const DEAD: &str = "http://127.0.0.1:9";

fn noul(instructions: &str) -> Value {
    json!({"type": "noul", "instructions": instructions})
}

fn call_body(model: &str) -> Value {
    json!({
        "state": {"document": "Order 1042 - refund request, 42.50 EUR"},
        "model": model,
        "questions": {"q": noul("Is this a refund request?")},
    })
}

/// A gateway with a dead `primary` backend serving `jev-*`, and whatever
/// other backends `backends` adds. `{MOCK_URL}` in it stands for the mock's URL.
struct Fallback {
    mock: MockUpstream,
    gateway: Gateway,
    client: reqwest::Client,
}

impl Fallback {
    async fn start(primary_extra: &str, backends: &str) -> Self {
        let mock = MockUpstream::start("127.0.0.1:0".parse().unwrap(), UPSTREAM_KEY).await;
        let toml = format!(
            "[server]\nlisten = \"127.0.0.1:0\"\nadmin_listen = \"127.0.0.1:0\"\n\
             [[backend]]\nname = \"primary\"\nbase_url = \"{DEAD}\"\nmodels = [\"jev-*\"]\n\
             max_retries = 1\nbackoff_initial_ms = 10\nbackoff_max_ms = 10\n{primary_extra}\n\
             {backends}\n\
             [coalescing]\nwindow_ms = 0\n\
             [[service]]\nname = \"ocr\"\nkey_sha256 = [\"{}\"]\nallowed_models = [\"jev-*\", \"Qwen/*\"]\n",
            hex::encode(hash_key("key-ocr")),
        )
        .replace("{MOCK_URL}", &mock.url);
        let config = Config::from_toml(&toml).expect("the test configuration is valid");
        let gateway = Gateway::start(&config, |variable| {
            (variable == "MOCK_KEY").then(|| UPSTREAM_KEY.to_owned())
        })
        .await
        .unwrap();
        Self {
            mock,
            gateway,
            client: reqwest::Client::new(),
        }
    }

    async fn call(&self, model: &str) -> Reply {
        self.call_body(&call_body(model)).await
    }

    async fn call_body(&self, body: &Value) -> Reply {
        reply(
            self.client
                .post(format!("http://{}/v1/systemone", self.gateway.public_addr))
                .bearer_auth("key-ocr")
                .header("content-type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .unwrap(),
        )
        .await
    }

    async fn metrics(&self) -> String {
        self.client
            .get(format!("http://{}/metrics", self.gateway.admin_addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }
}

/// A System One backend on the mock, named `backup`.
const BACKUP: &str = "[[backend]]\nname = \"backup\"\nbase_url = \"{MOCK_URL}\"\n\
                      api_key_env = \"MOCK_KEY\"\nmodels = [\"spare-*\"]\n";

fn assert_metric(metrics: &str, line: &str) {
    assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
}

#[tokio::test]
async fn a_dead_backend_is_answered_by_its_fallback() {
    let h = Fallback::start("fallback = [\"backup\"]", BACKUP).await;
    let reply = h.call("jev-latest").await;

    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.body["answers"]["q"]["echo"],
        "Is this a refund request?"
    );
    // The header names who answered, not who was asked.
    assert_eq!(reply.header("x-systemone-gateway-backend"), "backup");
    assert_eq!(h.mock.state.calls(), 1);
    let metrics = h.metrics().await;
    assert_metric(
        &metrics,
        r#"systemone_gateway_fallback_calls_total{from="primary",to="backup"} 1"#,
    );
    assert_metric(
        &metrics,
        r#"systemone_gateway_upstream_calls_total{backend="primary",status="0"} 1"#,
    );
    assert_metric(
        &metrics,
        r#"systemone_gateway_input_tokens_total{service="ocr",backend="backup"}"#,
    );
    // One failure is not an outage yet.
    assert_metric(
        &metrics,
        r#"systemone_gateway_circuit_state{backend="primary"} 0"#,
    );
    // The service's own model limits still decide what it may ask for.
    let refused = h.call("gpt-4").await;
    assert_eq!(refused.status, 403);
}

#[tokio::test]
async fn an_open_breaker_sends_calls_straight_to_the_fallback() {
    let h = Fallback::start(
        "fallback = [\"backup\"]\ncircuit_breaker_failures = 2\ncircuit_breaker_cooldown_ms = 60000",
        BACKUP,
    )
    .await;
    for _ in 0..2 {
        let reply = h.call("jev-latest").await;
        assert_eq!(reply.header("x-systemone-gateway-backend"), "backup");
    }
    assert_metric(
        &h.metrics().await,
        r#"systemone_gateway_circuit_state{backend="primary"} 2"#,
    );

    let reply = h.call("jev-latest").await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.header("x-systemone-gateway-backend"), "backup");
    let metrics = h.metrics().await;
    // The dead backend was not tried a third time.
    assert_metric(
        &metrics,
        r#"systemone_gateway_upstream_calls_total{backend="primary",status="0"} 2"#,
    );
    assert_metric(
        &metrics,
        r#"systemone_gateway_fallback_calls_total{from="primary",to="backup"} 3"#,
    );
    assert_eq!(h.mock.state.calls(), 3);
}

#[tokio::test]
async fn a_chat_fallback_answers_under_its_own_model_name() {
    let chat = "[[backend]]\nname = \"judge\"\nprotocol = \"chat\"\nbase_url = \"{MOCK_URL}/v1\"\n\
                api_key_env = \"MOCK_KEY\"\nmodels = [\"Qwen/*\"]\nupstream_model = \"org/judge\"\n";
    let h = Fallback::start("fallback = [\"judge\"]", chat).await;
    let reply = h.call("jev-latest").await;

    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.header("x-systemone-gateway-backend"), "judge");
    assert_eq!(reply.body["answers"]["q"]["type"], "noul");
    let requests = h.mock.state.chat_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["model"], "org/judge");

    // Without upstream_model, the model the service asked for goes as it is.
    let chat = "[[backend]]\nname = \"judge\"\nprotocol = \"chat\"\nbase_url = \"{MOCK_URL}/v1\"\n\
                api_key_env = \"MOCK_KEY\"\nmodels = [\"Qwen/*\"]\n";
    let h = Fallback::start("fallback = [\"judge\"]", chat).await;
    assert_eq!(h.call("jev-latest").await.status, 200);
    assert_eq!(h.mock.state.chat_requests()[0]["model"], "jev-latest");
}

#[tokio::test]
async fn a_fallback_that_cannot_express_the_question_is_skipped() {
    let chat = "[[backend]]\nname = \"judge\"\nprotocol = \"chat\"\nbase_url = \"{MOCK_URL}/v1\"\n\
                api_key_env = \"MOCK_KEY\"\nmodels = [\"Qwen/*\"]\n";
    let h = Fallback::start("fallback = [\"judge\"]", chat).await;
    let options: serde_json::Map<String, Value> = (0..27)
        .map(|n| (format!("option{n}"), Value::Null))
        .collect();
    let body = json!({
        "state": "a state",
        "model": "jev-latest",
        "questions": {"pick": {"type": "choice", "instructions": "Which?", "criteria": options}},
    });
    let reply = h.call_body(&body).await;

    // The caller gets the primary's failure: nothing was sent to the chat
    // backend, which would have refused the question.
    assert_eq!(reply.status, 502, "{}", reply.text);
    assert_eq!(reply.header("x-systemone-gateway-backend"), "primary");
    assert!(h.mock.state.chat_requests().is_empty());
}

#[tokio::test]
async fn when_every_backend_is_down_the_calls_fail_fast_with_a_503() {
    let second = format!(
        "[[backend]]\nname = \"backup\"\nbase_url = \"{DEAD}\"\nmodels = [\"spare-*\"]\nmax_retries = 0\ncircuit_breaker_failures = 1\n"
    );
    let h = Fallback::start(
        "fallback = [\"backup\"]\ncircuit_breaker_failures = 1\ncircuit_breaker_cooldown_ms = 30000",
        &second,
    )
    .await;
    // Both fail, and the caller gets the last failure.
    let first = h.call("jev-latest").await;
    assert_eq!(first.status, 502, "{}", first.text);
    assert_eq!(first.header("x-systemone-gateway-backend"), "backup");

    let reply = h.call("jev-latest").await;
    assert_eq!(reply.status, 503, "{}", reply.text);
    assert_eq!(reply.body["error"]["type"], "unavailable_error");
    assert!(
        reply.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("fallbacks")
    );
    let wait: u64 = reply.header("retry-after-ms").parse().unwrap();
    assert!((20_000..=30_000).contains(&wait), "{wait}");
    // No backend answered, so none is named.
    assert!(reply.headers.get("x-systemone-gateway-backend").is_none());
}

#[tokio::test]
async fn an_open_breaker_without_a_fallback_answers_503_without_calling_the_backend() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 0\ncircuit_breaker_failures = 2\ncircuit_breaker_cooldown_ms = 30000",
        ..Setup::default()
    })
    .await;
    for _ in 0..2 {
        h.mock
            .state
            .push(Scripted::status(503, r#"{"detail":"overloaded"}"#));
    }
    let body = call_body("jev-latest");
    for _ in 0..2 {
        // The backend's own error reaches the caller as it was sent.
        let reply = h.call("key-ocr", &body).await;
        assert_eq!(reply.status, 503);
        assert_eq!(reply.body["detail"], "overloaded");
    }

    let reply = h.call("key-ocr", &body).await;
    assert_eq!(reply.status, 503);
    assert_eq!(reply.body["error"]["type"], "unavailable_error");
    let wait: u64 = reply.header("retry-after-ms").parse().unwrap();
    assert!((25_000..=30_000).contains(&wait), "{wait}");
    assert_eq!(reply.header("retry-after"), "30");
    // The third call never left the gateway.
    assert_eq!(h.mock.state.calls(), 2);
    let metrics = h.metrics().await;
    assert_metric(
        &metrics,
        r#"systemone_gateway_circuit_state{backend="typesafe"} 2"#,
    );
    assert_metric(
        &metrics,
        r#"systemone_gateway_calls_total{service="ocr",status="503"} 3"#,
    );
}

#[tokio::test]
async fn rate_limits_and_client_errors_do_not_trip_the_breaker() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 0\ncircuit_breaker_failures = 2",
        ..Setup::default()
    })
    .await;
    let body = call_body("jev-latest");
    for status in [429, 422, 403, 429, 400, 429] {
        h.mock
            .state
            .push(Scripted::status(status, r#"{"detail":"no"}"#).header("retry-after-ms", "10"));
        let reply = h.call("key-ocr", &body).await;
        assert_eq!(reply.status, status, "{}", reply.text);
    }
    // Still closed: the backend answered every one of them.
    let reply = h.call("key-ocr", &body).await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_metric(
        &h.metrics().await,
        r#"systemone_gateway_circuit_state{backend="typesafe"} 0"#,
    );
}

#[tokio::test]
async fn a_key_the_backend_refuses_counts_as_an_outage() {
    let h = Harness::start(Setup {
        upstream: "circuit_breaker_failures = 2\ncircuit_breaker_cooldown_ms = 30000",
        gateway_key: "not-the-right-key",
        ..Setup::default()
    })
    .await;
    let body = call_body("jev-latest");
    for _ in 0..2 {
        assert_eq!(h.call("key-ocr", &body).await.status, 502);
    }
    assert_eq!(h.call("key-ocr", &body).await.status, 503);
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn after_the_cool_down_a_trial_call_closes_the_breaker() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 0\ncircuit_breaker_failures = 1\ncircuit_breaker_cooldown_ms = 300",
        ..Setup::default()
    })
    .await;
    let body = call_body("jev-latest");
    h.mock.state.push(Scripted::status(500, "{}"));
    assert_eq!(h.call("key-ocr", &body).await.status, 500);
    assert_eq!(h.call("key-ocr", &body).await.status, 503);

    sleep(Duration::from_millis(350)).await;
    let trial = h.call("key-ocr", &body).await;
    assert_eq!(trial.status, 200, "{}", trial.text);
    assert_metric(
        &h.metrics().await,
        r#"systemone_gateway_circuit_state{backend="typesafe"} 0"#,
    );
    assert_eq!(h.call("key-ocr", &body).await.status, 200);
}

#[tokio::test]
async fn a_failed_trial_call_opens_the_breaker_again() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 0\ncircuit_breaker_failures = 1\ncircuit_breaker_cooldown_ms = 300",
        ..Setup::default()
    })
    .await;
    let body = call_body("jev-latest");
    h.mock.state.push(Scripted::status(500, "{}"));
    h.mock.state.push(Scripted::status(500, "{}"));
    assert_eq!(h.call("key-ocr", &body).await.status, 500);

    sleep(Duration::from_millis(350)).await;
    // The trial goes out, and fails.
    assert_eq!(h.call("key-ocr", &body).await.status, 500);
    assert_eq!(h.mock.state.calls(), 2);
    let reply = h.call("key-ocr", &body).await;
    assert_eq!(reply.status, 503);
    assert_eq!(reply.body["error"]["type"], "unavailable_error");
    assert_eq!(h.mock.state.calls(), 2);
    assert_metric(
        &h.metrics().await,
        r#"systemone_gateway_circuit_state{backend="typesafe"} 2"#,
    );
}

#[tokio::test]
async fn only_one_trial_call_goes_out_while_the_breaker_is_half_open() {
    let h = Harness::start(Setup {
        upstream: "max_retries = 0\ncircuit_breaker_failures = 1\ncircuit_breaker_cooldown_ms = 300",
        ..Setup::default()
    })
    .await;
    let body = call_body("jev-latest");
    h.mock.state.push(Scripted::status(500, "{}"));
    assert_eq!(h.call("key-ocr", &body).await.status, 500);

    sleep(Duration::from_millis(350)).await;
    // The trial is slow: the calls that arrive meanwhile are turned away.
    h.mock.state.set_delay(Duration::from_millis(300));
    let (trial, others) = tokio::join!(h.call("key-ocr", &body), async {
        sleep(Duration::from_millis(100)).await;
        tokio::join!(h.call("key-ocr", &body), h.call("key-ocr", &body))
    });
    assert_eq!(trial.status, 200, "{}", trial.text);
    for other in [&others.0, &others.1] {
        assert_eq!(other.status, 503, "{}", other.text);
        assert!(other.header("retry-after-ms").parse::<u64>().unwrap() <= 1_000);
    }
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn a_fallback_answer_is_never_kept_in_the_answer_cache() {
    // A fallback stands in for the routed backend while it is down; caching
    // its answer would keep serving it after the routed backend is back.
    let h = Fallback::start(
        "fallback = [\"backup\"]",
        &format!("{BACKUP}[cache]\nenabled = true\n"),
    )
    .await;

    for _ in 0..2 {
        let reply = h.call("jev-latest").await;
        assert_eq!(reply.status, 200, "{}", reply.text);
        assert_eq!(reply.header("x-systemone-gateway-backend"), "backup");
        assert_eq!(reply.header("x-systemone-gateway-cache"), "miss");
    }
    assert_eq!(h.mock.state.calls(), 2);
}
