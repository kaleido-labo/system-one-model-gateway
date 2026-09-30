//! One gateway speaking TypeSafe's System One API, in front of one or more
//! model backends: TypeSafe's Jev, another System One server, or a chat model
//! such as one hosted on Hugging Face.
//!
//! Services call it exactly as they would call TypeSafe, with a key of their
//! own. The gateway picks the backend from the requested model, merges calls
//! that share a state, keeps the combined traffic inside each backend's
//! limits, retries what can be retried, and hands each caller back its own
//! answers and its share of the token usage.

mod api;
mod app;
mod backend;
mod chat;
mod config;
mod error;
mod metrics;
mod pattern;
mod scheduling;
mod services;
mod upstream;
mod wire;

pub use app::Gateway;
pub use config::{Config, LogFormat, Protocol};
pub use services::{generate_key, hash_key};
