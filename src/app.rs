//! Wiring the configuration into running servers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;
use tracing::info;

use crate::api::{AppState, Shared, admin_router, public_router};
use crate::coalescer::{BatchLimits, Coalescer};
use crate::config::{Config, millis};
use crate::dispatch::Dispatcher;
use crate::limiter::Gcra;
use crate::metrics::Metrics;
use crate::services::ServiceRegistry;
use crate::tokens::TokenEstimator;
use crate::upstream::{ApiKey, RetryPolicy, Upstream};

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
    /// the chosen addresses are in `public_addr` and `admin_addr`.
    pub async fn start(config: &Config, api_key: ApiKey) -> anyhow::Result<Self> {
        let state = build_state(config, api_key)?;
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
        info!(%public_addr, %admin_addr, services = config.services.len(), "gateway listening");
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

fn build_state(config: &Config, api_key: ApiKey) -> anyhow::Result<AppState> {
    let upstream_config = &config.upstream;
    let metrics = Arc::new(Metrics::new());
    let pacer = Arc::new(Gcra::per_minute(
        f64::from(upstream_config.requests_per_minute),
        u64::from(upstream_config.burst()),
    ));
    let upstream = Arc::new(Upstream::new(
        &upstream_config.base_url,
        api_key,
        millis(upstream_config.connect_timeout_ms),
        RetryPolicy {
            max_retries: upstream_config.max_retries,
            backoff_initial: millis(upstream_config.backoff_initial_ms),
            backoff_max: millis(upstream_config.backoff_max_ms),
            attempt_timeout: millis(upstream_config.attempt_timeout_ms),
        },
        Arc::clone(&pacer),
        Arc::clone(&metrics),
    )?);
    // One second of tokens may go at once.
    let tokens_per_second = u64::from(upstream_config.tokens_per_second);
    let token_pacer = Gcra::per_second(tokens_per_second as f64, tokens_per_second);
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::clone(&upstream),
        pacer,
        token_pacer,
        upstream_config.max_concurrency,
        Arc::clone(&metrics),
    ));
    let coalescing = &config.coalescing;
    let coalescer = Arc::new(Coalescer::new(
        BatchLimits {
            window: millis(coalescing.window_ms),
            max_questions: coalescing.max_questions,
            max_request_tokens: coalescing.max_request_tokens,
            max_state_plus_question_tokens: coalescing.max_state_plus_question_tokens,
            max_queue_wait: millis(upstream_config.max_queue_wait_ms),
        },
        dispatcher,
    ));
    Ok(AppState::new(Shared {
        registry: ServiceRegistry::from_config(&config.services),
        coalescer,
        upstream,
        metrics,
        estimator: TokenEstimator::new(coalescing.bytes_per_token),
        request_timeout: millis(config.server.request_timeout_ms),
        max_queue_wait: millis(upstream_config.max_queue_wait_ms),
        models_ttl: millis(upstream_config.models_cache_ttl_ms),
        models_cache: Mutex::new(None),
        ready: AtomicBool::new(false),
    }))
}
