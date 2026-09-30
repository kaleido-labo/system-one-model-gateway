//! Keeping the combined traffic of every service inside a backend's limits.
//!
//! A call moves through three stages:
//!
//! - `coalescer`: holds a call in an open batch for its `(model, state)` pair,
//!   so calls that share a state go upstream together.
//! - `dispatch`: sends a sealed batch upstream once the backend's request and
//!   token budgets and a free connection allow it, and hands every caller its
//!   outcome.
//! - `batch`: the plan for one upstream call. It merges the questions of
//!   several calls into one body and splits the answer back into one response
//!   per call, with each caller's share of the token usage.
//!
//! `limiter` holds the pacing all of them book against, in memory or shared
//! between replicas through Redis.

mod batch;
mod coalescer;
mod dispatch;
mod limiter;

pub use coalescer::{BatchLimits, Coalescer, Saturated};
pub use dispatch::{Dispatcher, Outcome};
pub use limiter::{Limiter, Limiters};
