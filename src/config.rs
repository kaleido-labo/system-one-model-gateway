//! Gateway configuration, read from a TOML file.
//!
//! The file holds no secret: services are identified by the SHA-256 of their
//! key, and the TypeSafe key is read from the environment variable the file
//! names. The file can live in git or in a Kubernetes ConfigMap.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub upstream: UpstreamConfig,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UpstreamConfig {
    pub base_url: String,
    /// Environment variable holding the TypeSafe API key.
    pub api_key_env: String,
    /// The account's limits, shared by every service behind the gateway.
    /// Defaults are Jev 1.13's published limits (https://docs.typesafe.ai/models.md).
    pub requests_per_minute: u32,
    /// Requests that may leave back to back. Defaults to one second's worth.
    pub burst: Option<u32>,
    pub tokens_per_second: u32,
    /// Upstream calls in flight at once.
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
    /// How long `GET /v1/models` is served from memory.
    pub models_cache_ttl_ms: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.typesafe.ai".to_owned(),
            api_key_env: "TYPESAFE_API_KEY".to_owned(),
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

impl UpstreamConfig {
    pub fn burst(&self) -> u32 {
        self.burst.unwrap_or(self.requests_per_minute / 60).max(1)
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

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("could not read {}", path.display()))?;
        Self::from_toml(&text)
            .with_context(|| format!("invalid configuration in {}", path.display()))
    }

    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(text)?;
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

        let upstream = &self.upstream;
        let url = reqwest::Url::parse(&upstream.base_url)
            .with_context(|| format!("upstream.base_url {:?} is not a URL", upstream.base_url))?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "upstream.base_url must be an http or https URL"
        );
        ensure!(
            !upstream.api_key_env.is_empty(),
            "upstream.api_key_env must name a variable"
        );
        ensure!(
            upstream.requests_per_minute > 0,
            "upstream.requests_per_minute must be positive"
        );
        ensure!(
            upstream.tokens_per_second > 0,
            "upstream.tokens_per_second must be positive"
        );
        ensure!(
            upstream.max_concurrency > 0,
            "upstream.max_concurrency must be positive"
        );
        ensure!(
            upstream.attempt_timeout_ms > 0,
            "upstream.attempt_timeout_ms must be positive"
        );
        ensure!(
            upstream.max_queue_wait_ms < server.request_timeout_ms,
            "upstream.max_queue_wait_ms ({}) must be shorter than server.request_timeout_ms ({}), \
             or queued calls would time out before they are sent",
            upstream.max_queue_wait_ms,
            server.request_timeout_ms
        );

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
                !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
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
                    !models.is_empty() && models.iter().all(|m| !m.trim().is_empty()),
                    "service {name:?}: allowed_models must list model names"
                );
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
        assert_eq!(config.upstream.requests_per_minute, 1_200);
        assert_eq!(config.upstream.burst(), 20);
        assert_eq!(config.coalescing.window_ms, 10);
        assert_eq!(config.server.listen.port(), 8080);
        assert_eq!(config.services[0].burst(), None);
    }

    #[test]
    fn the_example_file_is_valid() {
        let text = include_str!("../config.example.toml");
        let config = Config::from_toml(text).unwrap();
        assert!(config.services.len() >= 2);
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
            "[server]\nrequest_timeout_ms = 1000\n[upstream]\nmax_queue_wait_ms = 1000\n{}",
            with_service("")
        );
        assert!(
            Config::from_toml(&slow_queue)
                .unwrap_err()
                .to_string()
                .contains("shorter")
        );

        let typo = format!("[upstream]\nrequest_per_minute = 10\n{}", with_service(""));
        assert!(Config::from_toml(&typo).is_err());

        let zero_rate = with_service("requests_per_minute = 0");
        assert!(Config::from_toml(&zero_rate).is_err());
    }
}
