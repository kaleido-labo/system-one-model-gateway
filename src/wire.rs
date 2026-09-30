//! The System One wire format, and the checks that keep one caller's mistake
//! from reaching another caller's call.
//!
//! - `request`: a System One request body, parsed and normalised just enough
//!   to merge it with others (`PreparedRequest`).
//! - `response`: a successful answer, read just enough to split it between
//!   callers (`UpstreamAnswers`).
//! - `validate`: the rules TypeSafe documents for questions, checked before a
//!   question can join a merged call.
//! - `tokens`: estimates of how many tokens a payload costs.
//! - `json`: helpers for JSON the gateway forwards without re-encoding.

mod json;
mod request;
mod response;
mod tokens;
mod validate;

pub use json::{ObjectWriter, minify};
pub use request::{BatchKey, PreparedRequest, QuestionKey, RequestError};
pub use response::{UpstreamAnswers, input_tokens};
pub use tokens::TokenEstimator;
pub use validate::Invalid;
