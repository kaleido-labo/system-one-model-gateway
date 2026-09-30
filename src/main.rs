use std::io::{BufRead, IsTerminal};
use std::path::PathBuf;

use anyhow::{Context, ensure};
use clap::{Parser, Subcommand};
use systemone_gateway::{
    Config, Gateway, LogFormat, Protocol, Telemetry, generate_key, hash_key, logs_filter,
};
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

#[derive(Parser)]
#[command(
    version,
    about = "One System One API in front of TypeSafe, Hugging Face and other model backends"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the gateway.
    Serve {
        #[arg(long, env = "SYSTEMONE_GATEWAY_CONFIG", default_value = "gateway.toml")]
        config: PathBuf,
    },
    /// Validate a configuration file and print what it sets up.
    CheckConfig {
        #[arg(long, env = "SYSTEMONE_GATEWAY_CONFIG", default_value = "gateway.toml")]
        config: PathBuf,
    },
    /// Generate a key for a new service, with the hash that goes in the configuration.
    GenKey {
        /// Name to put in the printed [[service]] block.
        #[arg(long, default_value = "my-service")]
        service: String,
    },
    /// Print the SHA-256 of a key read from standard input.
    HashKey,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Serve { config } => serve(config).await,
        Command::CheckConfig { config } => check_config(config),
        Command::GenKey { service } => {
            gen_key(&service);
            Ok(())
        }
        Command::HashKey => hash_stdin_key(),
    }
}

async fn serve(path: PathBuf) -> anyhow::Result<()> {
    let (config, loaded) = Config::load_hashed(&path)?;
    let telemetry = init_logging(&config)?;
    // Before the gateway starts: a SIGHUP that arrives while it binds its
    // ports would otherwise end the process, which is what SIGHUP does when
    // nobody is listening for it.
    #[cfg(unix)]
    let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("could not listen for SIGHUP")?;
    let gateway = Gateway::start(&config, |variable| std::env::var(variable).ok()).await?;
    let reloader = gateway.reloader();
    #[cfg(unix)]
    tokio::spawn({
        let (reloader, path) = (reloader.clone(), path.clone());
        async move {
            while hangup.recv().await.is_some() {
                info!("SIGHUP received; reloading the configuration");
                // Logged and counted by the reloader; the gateway keeps
                // running on the old configuration when this fails.
                let _ = reloader.reload_file(&path);
            }
        }
    });
    // Idle unless server.config_reload_interval_ms is set, now or by a
    // later reload.
    tokio::spawn(async move { reloader.watch_file(path, loaded).await });
    shutdown_signal().await;
    info!("shutting down; letting in-flight calls finish");
    let stopped = gateway.shutdown().await;
    // After the servers: the spans of the last calls are in the queue by now.
    if let Some(telemetry) = telemetry {
        telemetry.shutdown().await;
    }
    stopped
}

fn check_config(path: PathBuf) -> anyhow::Result<()> {
    let config = Config::load(&path)?;
    println!("{} is valid.", path.display());
    for backend in &config.backends {
        let protocol = match backend.protocol {
            Protocol::Systemone => "System One",
            Protocol::Chat => "chat with logprobs",
        };
        let key = backend
            .api_key_env
            .as_ref()
            .map_or("no key".to_owned(), |variable| {
                format!("key from ${variable}")
            });
        let rate = if backend.adaptive_rate {
            format!(
                "{} requests/min at most, adapting down to {}",
                backend.requests_per_minute,
                backend.adaptive_min()
            )
        } else {
            format!("{} requests/min", backend.requests_per_minute)
        };
        println!(
            "backend {}: {protocol} at {} for {}, {rate}, {key}",
            backend.name,
            backend.base_url(),
            backend.models.join(", "),
        );
    }
    match &config.server.admin_token_env {
        Some(variable) => println!("admin port: /metrics needs the bearer token in ${variable}"),
        None => println!("admin port: open"),
    }
    match &config.cluster {
        None => println!("cluster: none, pacing stays in this process"),
        Some(cluster) => println!(
            "cluster: pacing shared through Redis (URL from ${}), key prefix {}, \
             {} replica(s) assumed while Redis is down",
            cluster.redis_url_env, cluster.key_prefix, cluster.expected_replicas
        ),
    }
    match config
        .tracing
        .endpoint(&|variable| std::env::var(variable).ok())
    {
        Some(endpoint) => println!(
            "tracing: spans of service {} exported to {} (from {}), {} of the traces the gateway starts kept",
            config.tracing.service_name, endpoint.url, endpoint.source, config.tracing.sample_ratio,
        ),
        None => println!("tracing: off"),
    }
    match config.server.config_reload_interval_ms {
        0 => println!("config reload: on SIGHUP"),
        every => println!(
            "config reload: on SIGHUP, and when the file changes (checked every {every} ms)"
        ),
    }
    match config.coalescing.window_ms {
        0 => println!("merging: off"),
        window => println!("merging: calls sharing a state within {window} ms go out together"),
    }
    if config.cache.enabled {
        println!(
            "answer cache: on, {} answers for {} ms",
            config.cache.max_entries, config.cache.ttl_ms
        );
    } else {
        println!("answer cache: off");
    }
    for service in &config.services {
        let rate = service
            .requests_per_minute
            .map_or("shared limit only".to_owned(), |rpm| {
                format!("{rpm} requests/min")
            });
        println!(
            "service {}: {} key(s), {rate}",
            service.name,
            service.key_sha256.len()
        );
    }
    Ok(())
}

fn gen_key(service: &str) {
    let key = generate_key();
    let hash = hex::encode(hash_key(&key));
    println!(
        "Key for {service}. Give it to the service as TYPESAFE_API_KEY; it is not stored anywhere:"
    );
    println!();
    println!("    {key}");
    println!();
    println!("Add this to the gateway configuration:");
    println!();
    println!("    [[service]]");
    println!("    name = \"{service}\"");
    println!("    key_sha256 = [\"{hash}\"]");
}

fn hash_stdin_key() -> anyhow::Result<()> {
    let mut key = String::new();
    std::io::stdin().lock().read_line(&mut key)?;
    let key = key.trim();
    ensure!(!key.is_empty(), "no key on standard input");
    println!("{}", hex::encode(hash_key(key)));
    Ok(())
}

/// Sets up logging and, when an OTLP endpoint is configured, trace export.
/// Returns the exporter, to flush when the gateway stops.
fn init_logging(config: &Config) -> anyhow::Result<Option<Telemetry>> {
    let telemetry = config
        .tracing
        .endpoint(&|variable| std::env::var(variable).ok())
        .map(|endpoint| {
            Telemetry::otlp(&config.tracing, &endpoint).map(|telemetry| (telemetry, endpoint))
        })
        .transpose()?;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Colours only on a terminal: in a container log they are noise.
    let logs = tracing_subscriber::fmt::layer().with_ansi(std::io::stdout().is_terminal());
    let logs = match config.server.log_format {
        LogFormat::Json => logs.json().flatten_event(true).boxed(),
        LogFormat::Text => logs.boxed(),
    };
    Registry::default()
        // The trace spans are for the tracing backend only: the log lines
        // stay what they were.
        .with(logs.with_filter(logs_filter(filter)))
        .with(telemetry.as_ref().map(|(telemetry, _)| telemetry.layer()))
        .init();

    if let Some((_, endpoint)) = &telemetry {
        info!(
            url = %endpoint.url,
            from = endpoint.source,
            service_name = %config.tracing.service_name,
            sample_ratio = config.tracing.sample_ratio,
            "exporting traces"
        );
    }
    Ok(telemetry.map(|(telemetry, _)| telemetry))
}

async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
