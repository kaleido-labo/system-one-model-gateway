//! A stand-in for TypeSafe's API, shared by the integration tests and
//! `examples/mock_upstream.rs`.
//!
//! It speaks the documented wire format (https://docs.typesafe.ai/api.md)
//! with one addition: every answer carries an `echo` field holding the
//! question's `instructions`. The real API has no such field, and the gateway
//! treats answers as opaque, so the echo lets a test check that each caller
//! got the answer to its own question after a merge.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use indexmap::IndexMap;
use serde_json::value::RawValue;
use serde_json::{Value, json};

/// A canned response, served before the default behaviour.
#[derive(Clone, Debug)]
pub struct Scripted {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

impl Scripted {
    pub fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.to_owned(),
        }
    }

    pub fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_owned()));
        self
    }
}

pub struct MockState {
    api_key: String,
    /// Bodies of every `POST /v1/systemone`, as received.
    received: Mutex<Vec<String>>,
    /// When each of them arrived.
    arrivals: Mutex<Vec<std::time::Instant>>,
    script: Mutex<VecDeque<Scripted>>,
    delay: Mutex<Duration>,
    models_calls: AtomicUsize,
}

impl MockState {
    pub fn calls(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    /// Arrival time of every `POST /v1/systemone`, oldest first.
    pub fn arrivals(&self) -> Vec<std::time::Instant> {
        self.arrivals.lock().unwrap().clone()
    }

    /// The `usage.input_tokens` the mock billed for a received body.
    pub fn input_tokens_of(&self, call: usize) -> u64 {
        (self.received.lock().unwrap()[call].len() / 4) as u64
    }

    /// Question ids and instructions of a received body, in sent order.
    pub fn questions_of(&self, call: usize) -> Vec<(String, Value)> {
        let body = self.received.lock().unwrap()[call].clone();
        ordered_questions(&body)
            .into_iter()
            .map(|(id, question)| (id, question["instructions"].clone()))
            .collect()
    }

    pub fn push(&self, response: Scripted) {
        self.script.lock().unwrap().push_back(response);
    }

    pub fn set_delay(&self, delay: Duration) {
        *self.delay.lock().unwrap() = delay;
    }

    pub fn models_calls(&self) -> usize {
        self.models_calls.load(Ordering::SeqCst)
    }
}

pub struct MockUpstream {
    pub url: String,
    pub state: Arc<MockState>,
}

impl MockUpstream {
    /// Starts the mock on `addr` (port 0 picks a free one). Requests must
    /// carry `Authorization: Bearer <api_key>`.
    pub async fn start(addr: SocketAddr, api_key: &str) -> Self {
        let state = Arc::new(MockState {
            api_key: api_key.to_owned(),
            received: Mutex::new(Vec::new()),
            arrivals: Mutex::new(Vec::new()),
            script: Mutex::new(VecDeque::new()),
            delay: Mutex::new(Duration::ZERO),
            models_calls: AtomicUsize::new(0),
        });
        let app = Router::new()
            .route("/v1/systemone", post(systemone))
            .route("/v1/models", get(models))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { url, state }
    }
}

/// The questions of a request body, in the order they were written.
/// `serde_json::Value` would sort them by id.
fn ordered_questions(body: &str) -> IndexMap<String, Value> {
    let fields: IndexMap<String, Box<RawValue>> = serde_json::from_str(body).unwrap();
    serde_json::from_str(fields["questions"].get()).unwrap()
}

fn authorized(state: &MockState, headers: &HeaderMap) -> bool {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(format!("Bearer {}", state.api_key).as_str())
}

fn json_response(status: u16, headers: &[(&'static str, String)], body: String) -> Response {
    let mut map = HeaderMap::new();
    map.insert("content-type", HeaderValue::from_static("application/json"));
    for (name, value) in headers {
        map.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    (StatusCode::from_u16(status).unwrap(), map, body).into_response()
}

async fn systemone(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Recorded before the key check, so a test sees every attempt made.
    let text = String::from_utf8(body.to_vec()).unwrap();
    let call = {
        let mut received = state.received.lock().unwrap();
        received.push(text.clone());
        state
            .arrivals
            .lock()
            .unwrap()
            .push(std::time::Instant::now());
        received.len()
    };
    if !authorized(&state, &headers) {
        return json_response(401, &[], json!({"detail": "Invalid API key"}).to_string());
    }
    let delay = *state.delay.lock().unwrap();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    let scripted = state.script.lock().unwrap().pop_front();
    if let Some(scripted) = scripted {
        return json_response(scripted.status, &scripted.headers, scripted.body);
    }

    let questions = ordered_questions(&text);
    if let Some((id, _)) = questions.iter().find(|(_, q)| {
        q["instructions"]
            .as_str()
            .is_some_and(|text| text.contains("POISON"))
    }) {
        let detail =
            json!({"detail": [{"loc": ["body", "questions", id], "msg": "poisoned question"}]});
        return json_response(422, &[], detail.to_string());
    }

    let mut answers = serde_json::Map::new();
    for (id, question) in &questions {
        answers.insert(id.clone(), answer(question));
    }
    let body = json!({
        "model": "jev-1.13.0",
        "answers": answers,
        "usage": {"input_tokens": text.len() / 4, "output_tokens": 10 * questions.len()},
    });
    json_response(
        200,
        &[("x-typesafe-request-id", format!("req_{call}"))],
        body.to_string(),
    )
}

fn answer(question: &Value) -> Value {
    let echo = question["instructions"].clone();
    match question["type"].as_str() {
        Some("choice") => {
            let options: Vec<String> = question["criteria"]
                .as_object()
                .map(|criteria| criteria.keys().cloned().collect())
                .unwrap_or_default();
            let share = 1.0 / options.len().max(1) as f64;
            let probabilities: serde_json::Map<String, Value> = options
                .iter()
                .map(|option| (option.clone(), json!(share)))
                .collect();
            json!({
                "type": "choice",
                "choice": options.first(),
                "probabilities": probabilities,
                "confidence": 0.9,
                "echo": echo,
            })
        }
        Some("score") => {
            let levels = question["criteria"].as_array().cloned().unwrap_or_default();
            let legend: serde_json::Map<String, Value> = levels
                .iter()
                .enumerate()
                .map(|(i, level)| (i.to_string(), level.clone()))
                .collect();
            json!({
                "type": "score",
                "score": 1.0,
                "legend": legend,
                "probabilities": {"0": 0.0, "1": 1.0},
                "confidence": 0.8,
                "echo": echo,
            })
        }
        _ => json!({"type": "noul", "noul": 0.75, "echo": echo}),
    }
}

async fn models(State(state): State<Arc<MockState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return json_response(401, &[], json!({"detail": "Invalid API key"}).to_string());
    }
    state.models_calls.fetch_add(1, Ordering::SeqCst);
    let body = json!({"models": [
        {"name": "jev-latest", "description": "Most recent stable release", "release_date": "2026-09-15"},
        {"name": "jev-preview", "description": "Most recent release", "release_date": "2026-09-15"},
    ]});
    json_response(200, &[], body.to_string())
}
