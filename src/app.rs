//! Wiring the configuration into running servers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::info;

use crate::backend::Backends;
use crate::cache::AnswerCache;
use crate::config::{Config, millis};
use crate::http::{AdminToken, AppState, Shared, admin_router, public_router};
use crate::metrics::Metrics;
use crate::services::ServiceRegistry;
use crate::wire::TokenEstimator;

/// Both servers, listening.
pub struct Gateway {
    pub public_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    state: AppState,
    stop: watch::Sender<()>,
    servers: JoinSet<std::io::Result<()>>,
}

impl Gateway {
    /// Binds both listeners and starts serving. Port 0 picks a free port;
    /// the chosen addresses are in `public_addr` and `admin_addr`. `env`
    /// looks up an environment variable by name, for the backends' keys.
    pub async fn start(
        config: &Config,
        env: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let state = build_state(config, &env)?;
        let public = TcpListener::bind(config.server.listen)
            .await
            .with_context(|| format!("could not listen on {}", config.server.listen))?;
        let admin = TcpListener::bind(config.server.admin_listen)
            .await
            .with_context(|| format!("could not listen on {}", config.server.admin_listen))?;
        let public_addr = public.local_addr()?;
        let admin_addr = admin.local_addr()?;

        let (stop, stopped) = watch::channel(());
        let mut servers = JoinSet::new();
        let public_app = public_router(state.clone(), config.server.max_body_bytes);
        let mut signal = stopped.clone();
        servers.spawn(async move {
            axum::serve(public, public_app)
                .with_graceful_shutdown(async move {
                    let _ = signal.changed().await;
                })
                .await
        });
        let admin_app = admin_router(state.clone());
        let mut signal = stopped;
        servers.spawn(async move {
            axum::serve(admin, admin_app)
                .with_graceful_shutdown(async move {
                    let _ = signal.changed().await;
                })
                .await
        });
        state.set_ready(true);
        info!(
            %public_addr,
            %admin_addr,
            services = config.services.len(),
            backends = config.backends.len(),
            "gateway listening"
        );
        Ok(Self {
            public_addr,
            admin_addr,
            state,
            stop,
            servers,
        })
    }

    /// Stops taking connections, lets in-flight calls finish, and returns
    /// once both servers are down.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.state.set_ready(false);
        let _ = self.stop.send(());
        while let Some(joined) = self.servers.join_next().await {
            joined
                .context("a server task panicked")?
                .context("a server failed")?;
        }
        Ok(())
    }
}

fn build_state(config: &Config, env: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<AppState> {
    // Like a backend's key: a variable that is named but empty stops the
    // start, so a typo cannot leave `/metrics` open without anyone noticing.
    let admin_token = match &config.server.admin_token_env {
        Some(variable) => {
            let token = env(variable).unwrap_or_default();
            let token = token.trim();
            anyhow::ensure!(
                !token.is_empty(),
                "server.admin_token_env: the admin token must be in the {variable} environment variable"
            );
            Some(AdminToken::new(token))
        }
        None => None,
    };
    let metrics = Arc::new(Metrics::new());
    let cache = config
        .cache
        .enabled
        .then(|| AnswerCache::new(&config.cache, metrics.cache_entries.clone()));
    Ok(AppState::new(Shared {
        registry: ServiceRegistry::from_config(&config.services),
        backends: Backends::build(config, env, &metrics)?,
        metrics,
        estimator: TokenEstimator::new(config.coalescing.bytes_per_token),
        cache,
        request_timeout: millis(config.server.request_timeout_ms),
        admin_token,
        ready: AtomicBool::new(false),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(server: &str) -> Config {
        Config::from_toml(&format!(
            "[server]\n{server}\n[[service]]\nname = \"s\"\nkey_sha256 = [\"{}\"]\n",
            "0".repeat(64)
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn a_missing_admin_token_stops_the_start() {
        let config = config("admin_token_env = \"ADMIN_TOKEN\"");
        // The typesafe backend's own key is provided; the admin token is not.
        let backend_key = |variable: &str| (variable == "TYPESAFE_API_KEY").then(|| "k".to_owned());
        let err = build_state(&config, &backend_key).err().unwrap();
        assert!(err.to_string().contains("ADMIN_TOKEN"), "{err}");

        // Blank counts as missing.
        let blank =
            |variable: &str| Some(if variable == "ADMIN_TOKEN" { "  " } else { "k" }.to_owned());
        assert!(build_state(&config, &blank).is_err());

        let both = |_: &str| Some("k".to_owned());
        assert!(build_state(&config, &both).is_ok());

        // No admin_token_env: nothing to look up.
        let open = self::config("");
        assert!(build_state(&open, &backend_key).is_ok());
    }
}
