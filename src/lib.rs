//! One gateway speaking TypeSafe's System One API, in front of one or more
//! model backends: TypeSafe's Jev, another System One server, or a chat model
//! such as one hosted on Hugging Face.
//!
//! Services call it exactly as they would call TypeSafe, with a key of their
//! own. The gateway picks the backend from the requested model, merges calls
//! that share a state, keeps the combined traffic inside each backend's
//! limits, retries what can be retried, and hands each caller back its own
//! answers and its share of the token usage.
//!
//! # Where things live
//!
//! A call enters through `http`, is checked against `wire` and `services`,
//! routed by `backend`, merged and paced by `scheduling`, and answered from
//! the backend's reply.
//!
//! - `app`: starts and stops the public and admin servers.
//! - `config`: the TOML configuration, including model name patterns.
//! - `http`: the public API, which copies TypeSafe's, and the admin endpoints.
//! - `services`: who may call the gateway, and each service's own limits.
//! - `wire`: the System One request and response format, and its validation.
//! - `backend`: the providers behind the gateway, how a call is routed to one,
//!   and how each one is spoken to (`upstream` client, `chat` protocol).
//! - `scheduling`: merging calls that share a state, pacing upstream traffic
//!   and splitting each answer and its token usage back between callers.
//! - `error`: the errors the gateway answers itself, in TypeSafe's shape.
//! - `metrics`: the Prometheus metrics served on the admin port.
//! - `telemetry`: OpenTelemetry traces, exported when an OTLP endpoint is
//!   configured.

mod app;
mod backend;
mod config;
mod error;
mod http;
mod metrics;
mod scheduling;
mod services;
mod telemetry;
mod wire;

pub use app::Gateway;
pub use config::{Config, LogFormat, Protocol, TracesEndpoint, TracingConfig};
pub use services::{generate_key, hash_key};
pub use telemetry::{Telemetry, logs_filter};
