//! The optional `[cluster]` table: sharing the pacing between replicas.

use anyhow::ensure;
use serde::Deserialize;

/// Where replicas keep the rate limits they share. Without this table the
/// gateway paces in memory, per process, and never contacts Redis.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Environment variable holding the Redis URL. The URL can carry a
    /// password, so the file names the variable, as `api_key_env` does for
    /// a backend's key.
    pub redis_url_env: String,
    /// Put in front of every key the gateway writes, so that several
    /// gateways, or several environments, can share one Redis.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
    /// Longest the gateway waits for Redis on one booking. Past it, that
    /// booking is answered from this replica's own share of the limits.
    #[serde(default = "default_redis_timeout_ms")]
    pub redis_timeout_ms: u64,
    /// How many replicas share the limits. Only used while Redis is down:
    /// each replica then paces with its backends' limits divided by this.
    #[serde(default = "default_expected_replicas")]
    pub expected_replicas: u32,
}

fn default_key_prefix() -> String {
    "systemone-gateway".to_owned()
}

fn default_redis_timeout_ms() -> u64 {
    200
}

fn default_expected_replicas() -> u32 {
    1
}

impl ClusterConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.redis_url_env.trim().is_empty(),
            "cluster.redis_url_env must name the environment variable that holds the Redis URL"
        );
        let prefix = &self.key_prefix;
        ensure!(
            !prefix.is_empty()
                && prefix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':')),
            "cluster.key_prefix {prefix:?} must be non-empty and use only letters, digits, '-', '_', '.' or ':'"
        );
        ensure!(
            self.redis_timeout_ms > 0,
            "cluster.redis_timeout_ms must be positive"
        );
        ensure!(
            self.expected_replicas > 0,
            "cluster.expected_replicas must be positive"
        );
        Ok(())
    }
}
