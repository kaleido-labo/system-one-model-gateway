//! One gateway in front of TypeSafe's System One API (Jev).
//!
//! Services call it exactly as they would call TypeSafe, with a key of their
//! own. The gateway merges calls that share a state into one upstream call,
//! keeps the combined traffic inside the account's limits, retries what can
//! be retried, and hands each caller back its own answers and its share of
//! the token usage.

mod api;
mod app;
mod batch;
mod coalescer;
mod config;
mod dispatch;
mod error;
mod json;
mod limiter;
mod metrics;
mod protocol;
mod services;
mod tokens;
mod upstream;
mod usage;
mod validate;

pub use app::Gateway;
pub use config::{Config, LogFormat};
pub use services::{generate_key, hash_key};
pub use upstream::ApiKey;
