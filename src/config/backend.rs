//! One `[[backend]]` block: a model provider behind the gateway, with its own
//! key, protocol and limits.

use anyhow::{Context, ensure};
use serde::Deserialize;

use crate::config::{ModelPattern, ServerConfig, valid_name};

/// How the gateway talks to a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// `POST /v1/systemone`, as documented by TypeSafe. Calls are forwarded
    /// as they are, merged calls included. Any server speaking the same API
    /// fits here, TypeSafe or not.
    Systemone,
    /// An OpenAI-compatible `POST /chat/completions` with `logprobs`: the
    /// Hugging Face router, Inference Endpoints, TGI, vLLM. Each question
    /// becomes one chat call, and its answer is read from the probabilities
    /// of the first generated token (see `chat`).
    Chat,
}

/// One model provider behind the gateway, with its own key and limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BackendConfig {
    /// Shows up in metrics, logs and the `x-systemone-gateway-backend` header.
    pub name: String,
    pub protocol: Protocol,
    /// Defaults to TypeSafe for `systemone`. Required for `chat`, where it
    /// is the URL `chat/completions` hangs off, such as
    /// `https://router.huggingface.co/v1`.
    pub base_url: Option<String>,
    /// Environment variable holding the backend's API key, sent as a bearer
    /// token. Unset sends no key, for a server on a private network.
    pub api_key_env: Option<String>,
    /// Model names this backend serves. A trailing `*` matches any name
    /// starting with what comes before it, and `*` alone matches every name.
    /// A call goes to the backend with the most specific match: an exact
    /// name first, then the longest prefix.
    pub models: Vec<String>,
    /// `chat` only: the model id sent upstream in place of the name the
    /// service asked for. Unset forwards the service's name as it is.
    pub upstream_model: Option<String>,
    /// `chat` only: how many most likely tokens the backend returns. The
    /// Hugging Face router accepts at most 5; vLLM and TGI allow more.
    pub top_logprobs: u8,
    /// `chat` only: extra fields merged into every chat request, for
    /// settings the gateway does not know about, such as
    /// `chat_template_kwargs = { enable_thinking = false }` for vLLM. The
    /// fields the gateway sets itself win over these.
    pub request_extras: toml::Table,
    /// The backend's limits, shared by every service behind the gateway.
    /// Defaults are Jev 1.13's published limits (https://docs.typesafe.ai/models.md).
    pub requests_per_minute: u32,
    /// Requests that may leave back to back. Defaults to one second's worth.
    pub burst: Option<u32>,
    pub tokens_per_second: u32,
    /// Upstream calls in flight at once. For `chat`, one call is one merged
    /// batch, which sends a chat request per question.
    pub max_concurrency: usize,
    pub connect_timeout_ms: u64,
    /// Timeout of one upstream attempt; retries get a fresh one.
    pub attempt_timeout_ms: u64,
    /// Retries after the first attempt, for 429, 529, 5xx and network errors.
    pub max_retries: u32,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    /// Longest a call may wait for upstream capacity (a request slot, a free
    /// connection, the token budget), on top of the merge window. A call that
    /// would wait longer gets a 429 with a retry-after at that moment, and
    /// the caller's SDK backs off.
    pub max_queue_wait_ms: u64,
    /// `systemone` only: how long the backend's `GET /v1/models` is served
    /// from memory.
    pub models_cache_ttl_ms: u64,
    /// Whether this backend's answers may be kept in the answer cache, when
    /// `[cache]` is enabled. Turn it off for a backend whose answers should
    /// differ from one call to the next, such as a chat model sampled at a
    /// temperature above zero.
    pub cache: bool,
    /// Consecutive failed upstream calls (after retries) that open the
    /// backend's circuit breaker. A failure is a network error, a 5xx
    /// (529 included) or a 401 for the gateway's key. A 429 or a client
    /// error is not one. 0 turns the breaker off.
    pub circuit_breaker_failures: u32,
    /// How long an open breaker turns calls away before it lets one trial
    /// call through.
    pub circuit_breaker_cooldown_ms: u64,
    /// Backends that take over a call when this one is unavailable, in order
    /// of preference. Empty means no fallback.
    pub fallback: Vec<String>,
}

pub const TYPESAFE_URL: &str = "https://api.typesafe.ai";

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            protocol: Protocol::Systemone,
            base_url: None,
            api_key_env: None,
            models: vec!["*".to_owned()],
            upstream_model: None,
            top_logprobs: 5,
            request_extras: toml::Table::new(),
            requests_per_minute: 1_200,
            burst: None,
            tokens_per_second: 250_000,
            max_concurrency: 64,
            connect_timeout_ms: 2_000,
            attempt_timeout_ms: 5_000,
            max_retries: 3,
            backoff_initial_ms: 200,
            backoff_max_ms: 3_000,
            max_queue_wait_ms: 2_000,
            models_cache_ttl_ms: 300_000,
            cache: true,
            circuit_breaker_failures: 5,
            circuit_breaker_cooldown_ms: 30_000,
            fallback: Vec::new(),
        }
    }
}

impl BackendConfig {
    /// The backend used when the file declares none: TypeSafe, for every
    /// model, with its key in `TYPESAFE_API_KEY`.
    pub fn typesafe() -> Self {
        Self {
            name: "typesafe".to_owned(),
            api_key_env: Some("TYPESAFE_API_KEY".to_owned()),
            ..Self::default()
        }
    }

    pub fn burst(&self) -> u32 {
        self.burst.unwrap_or(self.requests_per_minute / 60).max(1)
    }

    pub fn base_url(&self) -> &str {
        self.base_url.as_deref().unwrap_or(TYPESAFE_URL)
    }
}

impl BackendConfig {
    pub(super) fn validate(&self, server: &ServerConfig) -> anyhow::Result<()> {
        let name = &self.name;
        ensure!(
            valid_name(name),
            "backend name {name:?} must be non-empty and use only letters, digits, '-', '_' or '.'"
        );
        let url = reqwest::Url::parse(self.base_url())
            .with_context(|| format!("backend {name:?}: base_url is not a URL"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "backend {name:?}: base_url must be an http or https URL"
        );
        if let Some(variable) = &self.api_key_env {
            ensure!(
                !variable.is_empty(),
                "backend {name:?}: api_key_env must name a variable, or be left out for no key"
            );
        }
        ensure!(
            !self.models.is_empty(),
            "backend {name:?}: models must list at least one model name or pattern"
        );
        for model in &self.models {
            ModelPattern::parse(model).with_context(|| format!("backend {name:?}: models"))?;
        }
        match self.protocol {
            Protocol::Systemone => {
                ensure!(
                    self.upstream_model.is_none(),
                    "backend {name:?}: upstream_model only applies to protocol = \"chat\""
                );
                ensure!(
                    self.request_extras.is_empty(),
                    "backend {name:?}: request_extras only applies to protocol = \"chat\""
                );
            }
            Protocol::Chat => {
                ensure!(
                    self.base_url.is_some(),
                    "backend {name:?}: a chat backend needs a base_url, \
                     such as https://router.huggingface.co/v1"
                );
                ensure!(
                    (1..=20).contains(&self.top_logprobs),
                    "backend {name:?}: top_logprobs must be between 1 and 20"
                );
                if let Some(model) = &self.upstream_model {
                    ensure!(
                        !model.trim().is_empty(),
                        "backend {name:?}: upstream_model must not be empty"
                    );
                }
            }
        }
        ensure!(
            self.requests_per_minute > 0,
            "backend {name:?}: requests_per_minute must be positive"
        );
        ensure!(
            self.tokens_per_second > 0,
            "backend {name:?}: tokens_per_second must be positive"
        );
        ensure!(
            self.max_concurrency > 0,
            "backend {name:?}: max_concurrency must be positive"
        );
        ensure!(
            self.attempt_timeout_ms > 0,
            "backend {name:?}: attempt_timeout_ms must be positive"
        );
        ensure!(
            self.circuit_breaker_failures == 0 || self.circuit_breaker_cooldown_ms > 0,
            "backend {name:?}: circuit_breaker_cooldown_ms must be positive \
             (or set circuit_breaker_failures = 0 to turn the breaker off)"
        );
        for (index, other) in self.fallback.iter().enumerate() {
            ensure!(
                other != name,
                "backend {name:?}: fallback must not list the backend itself"
            );
            ensure!(
                !self.fallback[..index].contains(other),
                "backend {name:?}: fallback lists {other:?} twice"
            );
        }
        ensure!(
            self.max_queue_wait_ms < server.request_timeout_ms,
            "backend {name:?}: max_queue_wait_ms ({}) must be shorter than \
             server.request_timeout_ms ({}), or queued calls would time out before they are sent",
            self.max_queue_wait_ms,
            server.request_timeout_ms
        );
        Ok(())
    }
}
