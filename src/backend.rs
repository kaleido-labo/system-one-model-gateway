//! The model providers behind the gateway, and which one serves a call.
//!
//! Each backend has its own key, limits, merge queue and protocol. A call
//! goes to the backend whose `models` match its `model` most closely, so
//! services pick a provider by naming a model, and the operator can move a
//! model from one provider to another in the configuration alone.
//!
//! This file builds the backends and routes calls to them. How each one talks
//! to its provider lives underneath:
//!
//! - `engine`: the protocol a backend speaks, System One or chat.
//! - `upstream`: the HTTP client, with authentication, pacing and retries.
//! - `chat`: the chat protocol, which answers questions from token
//!   probabilities.

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep_until};

use crate::config::{BackendConfig, Config, ModelPattern, Protocol, best_match, millis, parse_all};
use crate::error::GatewayError;
use crate::metrics::Metrics;
use crate::scheduling::{BatchLimits, Coalescer, Dispatcher, Gcra, Outcome};
use crate::wire::{Invalid, PreparedRequest};

mod chat;
mod engine;
mod upstream;

use chat::ChatEngine;
pub use engine::Engine;
pub use upstream::{
    ApiKey, REQUEST_ID, RetryPolicy, Upstream, UpstreamFailure, UpstreamReply, describe,
};

pub struct Backend {
    pub name: String,
    patterns: Vec<ModelPattern>,
    pub coalescer: Arc<Coalescer>,
    upstream: Arc<Upstream>,
    models: ModelList,
    max_queue_wait: Duration,
    /// Whether the answer cache may serve and keep this backend's answers.
    pub cached: bool,
}

/// What `GET /v1/models` shows for a backend.
enum ModelList {
    /// The backend's own `GET /v1/models`, kept in memory for `ttl`.
    Remote {
        url: reqwest::Url,
        ttl: Duration,
        cache: Mutex<Option<(Instant, Vec<Box<RawValue>>)>>,
    },
    /// Entries for the exact names in `models`; a chat API has no list in
    /// the System One format.
    Fixed(Vec<Box<RawValue>>),
}

impl Backend {
    fn build(
        config: &BackendConfig,
        coalescing: &crate::config::CoalescingConfig,
        env: &dyn Fn(&str) -> Option<String>,
        metrics: &Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        let name = &config.name;
        let api_key = match &config.api_key_env {
            Some(variable) => {
                let key = env(variable).unwrap_or_default();
                let key = key.trim();
                anyhow::ensure!(
                    !key.is_empty(),
                    "backend {name:?}: its API key must be in the {variable} environment variable"
                );
                Some(ApiKey::new(key))
            }
            None => None,
        };
        let pacer = Arc::new(Gcra::per_minute(
            f64::from(config.requests_per_minute),
            u64::from(config.burst()),
        ));
        let upstream = Arc::new(Upstream::new(
            name,
            config.base_url(),
            api_key,
            millis(config.connect_timeout_ms),
            RetryPolicy {
                max_retries: config.max_retries,
                backoff_initial: millis(config.backoff_initial_ms),
                backoff_max: millis(config.backoff_max_ms),
                attempt_timeout: millis(config.attempt_timeout_ms),
            },
            Arc::clone(&pacer),
            Arc::clone(metrics),
        )?);
        let patterns = parse_all(&config.models);
        let (engine, models) = match config.protocol {
            Protocol::Systemone => (
                Engine::SystemOne {
                    url: upstream.endpoint("v1/systemone")?,
                    upstream: Arc::clone(&upstream),
                },
                ModelList::Remote {
                    url: upstream.endpoint("v1/models")?,
                    ttl: millis(config.models_cache_ttl_ms),
                    cache: Mutex::new(None),
                },
            ),
            Protocol::Chat => {
                let extras = match serde_json::to_value(&config.request_extras)? {
                    serde_json::Value::Object(extras) => extras,
                    _ => unreachable!("a TOML table serializes to a JSON object"),
                };
                let engine = ChatEngine::new(
                    Arc::clone(&upstream),
                    config.upstream_model.clone(),
                    config.top_logprobs,
                    extras,
                )?;
                let entries = patterns
                    .iter()
                    .filter_map(ModelPattern::exact)
                    .map(|model| {
                        let entry = serde_json::json!({
                            "name": model,
                            "description": format!("Served by backend {name:?} through a chat model"),
                        });
                        RawValue::from_string(entry.to_string()).expect("json! is valid JSON")
                    })
                    .collect();
                (Engine::Chat(engine), ModelList::Fixed(entries))
            }
        };
        // One second of tokens may go at once.
        let tokens_per_second = u64::from(config.tokens_per_second);
        let dispatcher = Arc::new(Dispatcher::new(
            name,
            engine,
            pacer,
            Gcra::per_second(tokens_per_second as f64, tokens_per_second),
            config.max_concurrency,
            Arc::clone(metrics),
        ));
        let coalescer = Arc::new(Coalescer::new(
            BatchLimits {
                window: millis(coalescing.window_ms),
                max_questions: coalescing.max_questions,
                max_request_tokens: coalescing.max_request_tokens,
                max_state_plus_question_tokens: coalescing.max_state_plus_question_tokens,
                max_queue_wait: millis(config.max_queue_wait_ms),
            },
            dispatcher,
        ));
        Ok(Self {
            name: name.clone(),
            patterns,
            coalescer,
            upstream,
            models,
            max_queue_wait: millis(config.max_queue_wait_ms),
            cached: config.cache,
        })
    }

    pub fn check(&self, request: &PreparedRequest) -> Result<(), Invalid> {
        self.coalescer.engine().check(request)
    }

    /// The backend's entries for `GET /v1/models`.
    pub async fn list_models(&self, deadline: Instant) -> Result<Vec<Box<RawValue>>, Outcome> {
        let (url, ttl, cache) = match &self.models {
            ModelList::Fixed(entries) => return Ok(entries.clone()),
            ModelList::Remote { url, ttl, cache } => (url, *ttl, cache),
        };
        let now = Instant::now();
        if let Some((fetched, entries)) = cache.lock().await.as_ref()
            && now.saturating_duration_since(*fetched) < ttl
        {
            return Ok(entries.clone());
        }
        // The list rarely changes, and every upstream call counts against
        // the backend's limit: book a slot like any call.
        let slot = self
            .upstream
            .pacer()
            .try_book(now, 1, self.max_queue_wait)
            .map_err(|retry_after| {
                Outcome::Failed(GatewayError::rate_limited(
                    format!("backend {:?} is fully booked", self.name),
                    retry_after,
                ))
            })?;
        sleep_until(slot).await;
        let reply = self
            .upstream
            .get(url, deadline)
            .await
            .map_err(|failure| Outcome::from_failure(&self.name, &failure))?;
        let entries = model_entries(&reply.body).map_err(|reason| {
            Outcome::from_failure(&self.name, &UpstreamFailure::Unreadable(reason))
        })?;
        *cache.lock().await = Some((Instant::now(), entries.clone()));
        Ok(entries)
    }
}

/// The `models` array of a `GET /v1/models` body, entries kept as sent.
fn model_entries(body: &[u8]) -> Result<Vec<Box<RawValue>>, String> {
    let envelope: IndexMap<String, Box<RawValue>> =
        serde_json::from_slice(body).map_err(|err| format!("model list: {err}"))?;
    let models = envelope
        .get("models")
        .ok_or("model list: no `models` field")?;
    serde_json::from_str(models.get()).map_err(|err| format!("model list: {err}"))
}

pub struct Backends(Vec<Backend>);

impl Backends {
    /// Builds every configured backend. `env` looks up an environment
    /// variable by name, for the API keys.
    pub fn build(
        config: &Config,
        env: &dyn Fn(&str) -> Option<String>,
        metrics: &Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        config
            .backends
            .iter()
            .map(|backend| Backend::build(backend, &config.coalescing, env, metrics))
            .collect::<anyhow::Result<_>>()
            .map(Self)
    }

    /// The backend that serves `model`: the one with the most specific
    /// match, an exact name before the longest prefix.
    pub fn route(&self, model: &str) -> Option<&Backend> {
        let mut best: Option<(&Backend, _)> = None;
        for backend in &self.0 {
            if let Some(specificity) = best_match(&backend.patterns, model)
                && best.is_none_or(|(_, current)| specificity > current)
            {
                best = Some((backend, specificity));
            }
        }
        best.map(|(backend, _)| backend)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Backend> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backends(toml: &str) -> Backends {
        let config = Config::from_toml(&format!(
            "{toml}\n[[service]]\nname = \"s\"\nkey_sha256 = [\"{}\"]\n",
            "0".repeat(64)
        ))
        .unwrap();
        Backends::build(
            &config,
            &|_| Some("key".to_owned()),
            &Arc::new(Metrics::new()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn the_most_specific_pattern_wins_whatever_the_order() {
        let backends = backends(
            r#"
            [[backend]]
            name = "fallback"
            models = ["*"]
            [[backend]]
            name = "typesafe"
            models = ["jev-*"]
            [[backend]]
            name = "hf"
            protocol = "chat"
            base_url = "https://router.huggingface.co/v1"
            models = ["Qwen/*", "jev-local"]
            "#,
        );
        let route = |model| backends.route(model).map(|b| b.name.as_str());
        assert_eq!(route("jev-latest"), Some("typesafe"));
        assert_eq!(route("jev-local"), Some("hf"));
        assert_eq!(route("Qwen/Qwen2.5-7B-Instruct"), Some("hf"));
        assert_eq!(route("mistral"), Some("fallback"));
    }

    #[tokio::test]
    async fn a_model_nobody_serves_has_no_backend() {
        let backends = backends("[[backend]]\nname = \"typesafe\"\nmodels = [\"jev-*\"]\n");
        assert!(backends.route("gpt-4").is_none());
    }

    #[tokio::test]
    async fn without_backends_the_file_means_typesafe_for_every_model() {
        let backends = backends("");
        assert_eq!(backends.route("anything").unwrap().name, "typesafe");
    }

    #[tokio::test]
    async fn a_missing_key_stops_the_start() {
        let config = Config::from_toml(&format!(
            "[[backend]]\nname = \"hf\"\nprotocol = \"chat\"\nbase_url = \"http://x/v1\"\n\
             api_key_env = \"HF_TOKEN\"\n[[service]]\nname = \"s\"\nkey_sha256 = [\"{}\"]\n",
            "0".repeat(64)
        ))
        .unwrap();
        let err = Backends::build(&config, &|_| None, &Arc::new(Metrics::new()))
            .err()
            .unwrap();
        assert!(err.to_string().contains("HF_TOKEN"), "{err}");
        // No api_key_env: no key needed.
        let config = Config::from_toml(&format!(
            "[[backend]]\nname = \"local\"\nprotocol = \"chat\"\nbase_url = \"http://x/v1\"\n\
             [[service]]\nname = \"s\"\nkey_sha256 = [\"{}\"]\n",
            "0".repeat(64)
        ))
        .unwrap();
        assert!(Backends::build(&config, &|_| None, &Arc::new(Metrics::new())).is_ok());
    }

    #[test]
    fn model_lists_keep_entries_as_sent() {
        let entries =
            model_entries(br#"{"models":[{"name":"jev-latest","release_date":"2026-09-15"}]}"#)
                .unwrap();
        assert_eq!(
            entries[0].get(),
            r#"{"name":"jev-latest","release_date":"2026-09-15"}"#
        );
        assert!(model_entries(b"{}").is_err());
    }
}
