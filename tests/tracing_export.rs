//! The OTLP side of tracing: spans leave the gateway as OTLP/HTTP requests,
//! and shutdown flushes the ones still queued. A small axum server plays the
//! collector, so no real one is needed.
//!
//! These tests run on a current-thread runtime and install their subscriber
//! for their own thread only, so they do not disturb each other.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use common::{Harness, Setup};
use serde_json::json;
use systemone_gateway::{Telemetry, TracingConfig};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// What the fake collector received: `(path, content-type, body)`.
type Received = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

async fn collector() -> (String, Received) {
    let received = Received::default();
    let app = Router::new()
        .route(
            "/v1/traces",
            post(
                |State(received): State<Received>, headers: HeaderMap, body: Bytes| async move {
                    let content_type = headers
                        .get("content-type")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    received.lock().unwrap().push((
                        "/v1/traces".to_owned(),
                        content_type,
                        body.to_vec(),
                    ));
                },
            ),
        )
        .with_state(Arc::clone(&received));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, received)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn config(endpoint: &str) -> TracingConfig {
    TracingConfig {
        otlp_endpoint: Some(endpoint.to_owned()),
        service_name: "systemone-test".to_owned(),
        ..TracingConfig::default()
    }
}

/// Makes one call inside the caller's trace `trace`.
async fn one_call(h: &Harness, trace: &str) {
    let reply = h
        .client
        .post(h.url("/v1/systemone"))
        .bearer_auth("key-ocr")
        .header("traceparent", format!("00-{trace}-00f067aa0ba902b7-01"))
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
}

#[tokio::test]
async fn spans_reach_the_collector_as_otlp_protobuf_when_the_gateway_shuts_down() {
    let (url, received) = collector().await;
    let config = config(&url);
    let endpoint = config.endpoint(&|_| None).unwrap();
    assert_eq!(endpoint.url, format!("{url}/v1/traces"));
    let telemetry = Telemetry::otlp(&config, &endpoint).unwrap();
    let _subscriber = tracing::subscriber::set_default(Registry::default().with(telemetry.layer()));

    let h = Harness::start(Setup::default()).await;
    let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
    one_call(&h, trace).await;
    // The batch exporter holds spans for a few seconds: nothing has left yet.
    assert!(received.lock().unwrap().is_empty());

    let started = Instant::now();
    h.gateway.shutdown().await.unwrap();
    telemetry.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    let received = received.lock().unwrap();
    assert!(!received.is_empty(), "shutdown flushed nothing");
    let (_, content_type, body) = &received[0];
    assert_eq!(content_type, "application/x-protobuf");
    // Strings and ids are plain bytes in a protobuf message.
    assert!(contains(body, b"systemone-test"), "the service name");
    assert!(contains(body, b"POST /v1/systemone"), "the server span");
    assert!(contains(body, b"upstream.attempt"), "the attempt span");
    assert!(
        contains(body, &hex::decode(trace).unwrap()),
        "the caller's trace id"
    );
}

#[tokio::test]
async fn a_collector_that_is_down_does_not_hold_up_shutdown_or_calls() {
    // Nothing listens on port 9.
    let config = config("http://127.0.0.1:9");
    let endpoint = config.endpoint(&|_| None).unwrap();
    let telemetry = Telemetry::otlp(&config, &endpoint).unwrap();
    let _subscriber = tracing::subscriber::set_default(Registry::default().with(telemetry.layer()));

    let h = Harness::start(Setup::default()).await;
    one_call(&h, "0af7651916cd43dd8448eb211c80319c").await;
    h.gateway.shutdown().await.unwrap();
    let started = Instant::now();
    telemetry.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
}
