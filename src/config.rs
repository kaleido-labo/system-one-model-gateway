//! Gateway configuration, read from a TOML file.
//!
//! The file holds no secret: services are identified by the SHA-256 of their
//! key, and each backend's key is read from the environment variable the
//! file names. The file can live in git or in a Kubernetes ConfigMap.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;

use crate::pattern::ModelPattern;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    /// The `[upstream]` table of earlier versions, kept only to explain
    /// that `[[backend]]` replaced it.
    #[serde(default)]
    upstream: Option<toml::Value>,
    #[serde(default, rename = "backend")]
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub coalescing: CoalescingConfig,
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Where services send their System One calls.
    pub listen: SocketAddr,
    /// Health checks and Prometheus metrics, kept off the public port.
    pub admin_listen: SocketAddr,
    /// Longest a call may take end to end, queueing included. The TypeSafe
    /// SDKs give up after 10 s by default, so this stays under that: a caller
    /// then gets the gateway's own 504 rather than a client-side timeout that
    /// its SDK would retry blindly.
    pub request_timeout_ms: u64,
    pub max_body_bytes: usize,
    pub log_format: LogFormat,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 8080)),
            admin_listen: SocketAddr::from(([0, 0, 0, 0], 9090)),
            request_timeout_ms: 9_000,
            max_body_bytes: 2 * 1024 * 1024,
            log_format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CoalescingConfig {
    /// How long a new batch stays open for other callers with the same state.
    /// Zero turns merging off: every call goes upstream on its own.
    pub window_ms: u64,
    /// Most distinct questions in one merged call.
    pub max_questions: usize,
    /// Budgets a merged call must fit, as estimated by the gateway. Kept
    /// below the vendor's 64k and 32k because the estimate is approximate.
    pub max_request_tokens: u32,
    pub max_state_plus_question_tokens: u32,
    /// Bytes of minified JSON counted as one token (see `tokens`).
    pub bytes_per_token: f64,
}

impl Default for CoalescingConfig {
    fn default() -> Self {
        Self {
            window_ms: 10,
            max_questions: 128,
            max_request_tokens: 56_000,
            max_state_plus_question_tokens: 28_000,
            bytes_per_token: 3.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Shows up in metrics and logs.
    pub name: String,
    /// SHA-256 (hex) of each key the service may present. Several keys let a
    /// key be rotated without downtime. `systemone-gateway gen-key` makes one.
    pub key_sha256: Vec<String>,
    /// The service's own share of the upstream limit. Unset means the
    /// service is bounded only by the shared limit.
    pub requests_per_minute: Option<u32>,
    /// Requests the service may send back to back. Defaults to ten seconds'
    /// worth of its rate.
    pub burst: Option<u32>,
    /// Calls the service may have in flight at once.
    pub max_concurrent: Option<usize>,
    /// Models the service may ask for. Unset allows any model.
    pub allowed_models: Option<Vec<String>>,
}

impl ServiceConfig {
    pub fn burst(&self) -> Option<u32> {
        self.requests_per_minute
            .map(|rpm| self.burst.unwrap_or(rpm / 6).max(1))
    }
}

impl BackendConfig {
    fn validate(&self, server: &ServerConfig) -> anyhow::Result<()> {
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
            self.max_queue_wait_ms < server.request_timeout_ms,
            "backend {name:?}: max_queue_wait_ms ({}) must be shorter than \
             server.request_timeout_ms ({}), or queued calls would time out before they are sent",
            self.max_queue_wait_ms,
            server.request_timeout_ms
        );
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("could not read {}", path.display()))?;
        Self::from_toml(&text)
            .with_context(|| format!("invalid configuration in {}", path.display()))
    }

    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let mut config: Self = toml::from_str(text)?;
        if config.backends.is_empty() && config.upstream.is_none() {
            config.backends.push(BackendConfig::typesafe());
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let server = &self.server;
        ensure!(
            server.request_timeout_ms > 0,
            "server.request_timeout_ms must be positive"
        );
        ensure!(
            server.max_body_bytes > 0,
            "server.max_body_bytes must be positive"
        );

        ensure!(
            self.upstream.is_none(),
            "[upstream] was replaced by [[backend]] blocks: rename the table to [[backend]] \
             and give it name = \"typesafe\" and api_key_env = \"TYPESAFE_API_KEY\""
        );
        ensure!(!self.backends.is_empty(), "no [[backend]] configured");
        let mut backend_names = HashSet::new();
        let mut patterns = HashMap::new();
        for backend in &self.backends {
            backend.validate(server)?;
            ensure!(
                backend_names.insert(backend.name.as_str()),
                "backend {:?} is configured twice",
                backend.name
            );
            for pattern in &backend.models {
                if let Some(other) = patterns.insert(pattern.as_str(), backend.name.as_str()) {
                    bail!(
                        "model {pattern:?} is listed by backends {other:?} and {:?}: \
                         a model name must lead to one backend",
                        backend.name
                    );
                }
            }
        }

        let coalescing = &self.coalescing;
        ensure!(
            coalescing.max_questions > 0,
            "coalescing.max_questions must be positive"
        );
        ensure!(
            coalescing.bytes_per_token.is_finite() && coalescing.bytes_per_token > 0.0,
            "coalescing.bytes_per_token must be a positive number"
        );
        ensure!(
            coalescing.max_request_tokens > 0,
            "coalescing.max_request_tokens must be positive"
        );
        ensure!(
            coalescing.max_state_plus_question_tokens > 0,
            "coalescing.max_state_plus_question_tokens must be positive"
        );

        if self.services.is_empty() {
            bail!("no [[service]] configured: nobody could call the gateway");
        }
        let mut names = HashSet::new();
        let mut hashes = HashSet::new();
        for service in &self.services {
            let name = &service.name;
            ensure!(
                valid_name(name),
                "service name {name:?} must be non-empty and use only letters, digits, '-', '_' or '.'"
            );
            ensure!(
                names.insert(name.as_str()),
                "service {name:?} is configured twice"
            );
            ensure!(
                !service.key_sha256.is_empty(),
                "service {name:?} has no key_sha256"
            );
            for hash in &service.key_sha256 {
                ensure!(
                    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()),
                    "service {name:?}: {hash:?} is not a SHA-256 in hex (64 characters)"
                );
                ensure!(
                    hashes.insert(hash.to_ascii_lowercase()),
                    "service {name:?}: a key hash is used by more than one service"
                );
            }
            ensure!(
                service.requests_per_minute != Some(0),
                "service {name:?}: requests_per_minute must be positive"
            );
            ensure!(
                service.max_concurrent != Some(0),
                "service {name:?}: max_concurrent must be positive"
            );
            if let Some(models) = &service.allowed_models {
                ensure!(
                    !models.is_empty(),
                    "service {name:?}: allowed_models must list model names"
                );
                for model in models {
                    ModelPattern::parse(model)
                        .with_context(|| format!("service {name:?}: allowed_models"))?;
                }
            }
        }
        Ok(())
    }
}

pub fn millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const HASH_B: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    fn with_service(extra: &str) -> String {
        format!("[[service]]\nname = \"ocr\"\nkey_sha256 = [\"{HASH_A}\"]\n{extra}")
    }

    #[test]
    fn a_single_service_is_enough_and_defaults_fill_the_rest() {
        let config = Config::from_toml(&with_service("")).unwrap();
        assert_eq!(config.backends.len(), 1);
        let backend = &config.backends[0];
        assert_eq!(backend.name, "typesafe");
        assert_eq!(backend.protocol, Protocol::Systemone);
        assert_eq!(backend.base_url(), TYPESAFE_URL);
        assert_eq!(backend.api_key_env.as_deref(), Some("TYPESAFE_API_KEY"));
        assert_eq!(backend.models, ["*"]);
        assert_eq!(backend.requests_per_minute, 1_200);
        assert_eq!(backend.burst(), 20);
        assert_eq!(config.coalescing.window_ms, 10);
        assert_eq!(config.server.listen.port(), 8080);
        assert_eq!(config.services[0].burst(), None);
    }

    #[test]
    fn the_example_file_is_valid() {
        let text = include_str!("../config.example.toml");
        let config = Config::from_toml(text).unwrap();
        assert!(config.services.len() >= 2);
        assert!(
            config
                .backends
                .iter()
                .any(|backend| backend.protocol == Protocol::Chat)
        );
    }

    #[test]
    fn service_burst_defaults_to_ten_seconds_of_rate() {
        let config = Config::from_toml(&with_service("requests_per_minute = 120")).unwrap();
        assert_eq!(config.services[0].burst(), Some(20));
    }

    #[test]
    fn rejects_configurations_that_cannot_work() {
        assert!(
            Config::from_toml("")
                .unwrap_err()
                .to_string()
                .contains("no [[service]]")
        );

        let bad_hash = "[[service]]\nname = \"a\"\nkey_sha256 = [\"abc\"]";
        assert!(format!("{:#}", Config::from_toml(bad_hash).unwrap_err()).contains("SHA-256"));

        let shared_hash = format!(
            "[[service]]\nname = \"a\"\nkey_sha256 = [\"{HASH_A}\"]\n\
             [[service]]\nname = \"b\"\nkey_sha256 = [\"{HASH_A}\"]"
        );
        assert!(
            Config::from_toml(&shared_hash)
                .unwrap_err()
                .to_string()
                .contains("more than one")
        );

        let twice = format!(
            "[[service]]\nname = \"a\"\nkey_sha256 = [\"{HASH_A}\"]\n\
             [[service]]\nname = \"a\"\nkey_sha256 = [\"{HASH_B}\"]"
        );
        assert!(
            Config::from_toml(&twice)
                .unwrap_err()
                .to_string()
                .contains("twice")
        );

        let bad_name = format!("[[service]]\nname = \"a b\"\nkey_sha256 = [\"{HASH_A}\"]");
        assert!(Config::from_toml(&bad_name).is_err());

        let slow_queue = format!(
            "[server]\nrequest_timeout_ms = 1000\n[[backend]]\nname = \"t\"\nmax_queue_wait_ms = 1000\n{}",
            with_service("")
        );
        assert!(
            Config::from_toml(&slow_queue)
                .unwrap_err()
                .to_string()
                .contains("shorter")
        );

        let typo = format!(
            "[[backend]]\nname = \"t\"\nrequest_per_minute = 10\n{}",
            with_service("")
        );
        assert!(Config::from_toml(&typo).is_err());

        let zero_rate = with_service("requests_per_minute = 0");
        assert!(Config::from_toml(&zero_rate).is_err());
    }

    fn error_of(toml: &str) -> String {
        format!(
            "{:#}",
            Config::from_toml(&format!("{toml}\n{}", with_service(""))).unwrap_err()
        )
    }

    #[test]
    fn a_chat_backend_sits_next_to_typesafe() {
        let config = Config::from_toml(&format!(
            r#"
            [[backend]]
            name = "typesafe"
            api_key_env = "TYPESAFE_API_KEY"
            models = ["jev-*"]

            [[backend]]
            name = "huggingface"
            protocol = "chat"
            base_url = "https://router.huggingface.co/v1"
            api_key_env = "HF_TOKEN"
            models = ["Qwen/*"]
            requests_per_minute = 300
            request_extras = {{ temperature = 0 }}
            {}"#,
            with_service("")
        ))
        .unwrap();
        let hf = &config.backends[1];
        assert_eq!(hf.protocol, Protocol::Chat);
        assert_eq!(hf.base_url(), "https://router.huggingface.co/v1");
        assert_eq!(hf.top_logprobs, 5);
        assert_eq!(hf.burst(), 5);
        assert_eq!(hf.request_extras["temperature"].as_integer(), Some(0));
    }

    #[test]
    fn rejects_backend_setups_that_cannot_work() {
        assert!(error_of("[upstream]\nrequests_per_minute = 10").contains("[[backend]]"));
        assert!(error_of("[[backend]]\nmodels = [\"*\"]").contains("backend name"));
        assert!(
            error_of("[[backend]]\nname = \"a\"\n[[backend]]\nname = \"b\"")
                .contains("listed by backends")
        );
        assert!(
            error_of(
                "[[backend]]\nname = \"a\"\nmodels = [\"x\"]\n\
                 [[backend]]\nname = \"a\"\nmodels = [\"y\"]"
            )
            .contains("twice")
        );
        assert!(error_of("[[backend]]\nname = \"hf\"\nprotocol = \"chat\"").contains("base_url"));
        assert!(
            error_of("[[backend]]\nname = \"t\"\nupstream_model = \"m\"").contains("only applies")
        );
        assert!(
            error_of(
                "[[backend]]\nname = \"hf\"\nprotocol = \"chat\"\nbase_url = \"http://x/v1\"\n\
                 top_logprobs = 0"
            )
            .contains("top_logprobs")
        );
        assert!(error_of("[[backend]]\nname = \"t\"\nmodels = [\"a*b\"]").contains("`*`"));
        assert!(error_of("[[backend]]\nname = \"t\"\nmodels = []").contains("at least one"));
        assert!(error_of("[[backend]]\nname = \"t\"\napi_key_env = \"\"").contains("api_key_env"));
        assert!(
            error_of("[[backend]]\nname = \"t\"\nprotocol = \"grpc\"").contains("unknown variant")
        );
    }
}
