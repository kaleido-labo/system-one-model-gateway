use std::io::{BufRead, IsTerminal};
use std::path::PathBuf;

use anyhow::ensure;
use clap::{Parser, Subcommand};
use systemone_gateway::{Config, Gateway, LogFormat, Protocol, generate_key, hash_key};
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    let config = Config::load(&path)?;
    init_logging(config.server.log_format);
    let gateway = Gateway::start(&config, |variable| std::env::var(variable).ok()).await?;
    shutdown_signal().await;
    info!("shutting down; letting in-flight calls finish");
    gateway.shutdown().await
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
        println!(
            "backend {}: {protocol} at {} for {}, {} requests/min, {key}",
            backend.name,
            backend.base_url(),
            backend.models.join(", "),
            backend.requests_per_minute,
        );
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

fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Colours only on a terminal: in a container log they are noise.
    let logs = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(std::io::stdout().is_terminal());
    match format {
        LogFormat::Json => logs.json().flatten_event(true).init(),
        LogFormat::Text => logs.init(),
    }
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
