//! Tracing is off unless an OTLP endpoint is configured, and then nothing about
//! a call changes: no `traceparent` is sent upstream, even when the caller
//! sent one, and the logs are what they always were.
//!
//! The subscriber is the one the binary sets up without an endpoint: logs
//! only, filtered like the real ones, with no trace layer behind it.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use common::{Harness, Setup};
use serde_json::json;
use systemone_gateway::logs_filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

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

#[tokio::test]
async fn nothing_is_propagated_and_logs_do_not_change() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&logs);
    let _subscriber = tracing::subscriber::set_default(
        Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || LogSink(Arc::clone(&writer)))
                .with_filter(logs_filter(EnvFilter::new("info"))),
        ),
    );

    let h = Harness::start(Setup::default()).await;
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let reply = h
        .client
        .post(h.url("/v1/systemone"))
        .bearer_auth("key-ocr")
        .header("traceparent", traceparent)
        .header("tracestate", "vendor=abc")
        .header("content-type", "application/json")
        .body(
            json!({
                "state": {"document": "Order 1042"},
                "model": "jev-latest",
                "questions": {"q": {"type": "noul", "instructions": "Is this a refund request?"}},
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 200);
    assert!(reply.headers().get("traceparent").is_none());
    let models = h
        .client
        .get(h.url("/v1/models"))
        .bearer_auth("key-ocr")
        .header("traceparent", traceparent)
        .send()
        .await
        .unwrap();
    assert_eq!(models.status(), 200);

    for path in ["/v1/systemone", "/v1/models"] {
        let sent = h.mock.state.headers_on(path);
        assert_eq!(sent.len(), 1, "{path}");
        assert!(
            sent[0].get("traceparent").is_none(),
            "{path}: {:?}",
            sent[0]
        );
        assert!(sent[0].get("tracestate").is_none(), "{path}: {:?}", sent[0]);
    }

    let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains(r#"call{method=POST path=/v1/systemone request_id=""#),
        "{logs}"
    );
    assert!(
        logs.contains(
            r#"service="ocr"}: tower_http::trace::on_response: finished processing request"#
        ),
        "{logs}"
    );
    for trace_only in ["upstream.attempt", "systemone.batch", "gateway.", "otel."] {
        assert!(
            !logs.contains(trace_only),
            "{trace_only} in the logs:\n{logs}"
        );
    }
}
