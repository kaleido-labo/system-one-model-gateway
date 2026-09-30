//! Splitting a successful upstream body into one answer per caller.
//!
//! Every caller gets back its own question ids, the vendor's envelope as it
//! was sent, and its share of the token usage.

use bytes::Bytes;
use serde_json::value::RawValue;

use super::usage::apportion;
use super::{Plan, Routes};
use crate::wire::{ObjectWriter, UpstreamAnswers, input_tokens};

/// One caller's part of an upstream answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub body: Bytes,
    /// Input tokens charged to this caller, when the vendor reported usage.
    pub input_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitError {
    /// The upstream body could not be read.
    Unreadable(String),
    /// The upstream body has no answer for this caller's question id.
    MissingAnswer(String),
}

impl Plan {
    /// Splits a successful upstream body into one answer per caller, in the
    /// order the requests were given to `new`.
    pub fn split(&self, body: &Bytes) -> Vec<Result<Answer, SplitError>> {
        let (routes, input_weights, output_weights) = match &self.routes {
            Routes::Passthrough => {
                return vec![Ok(Answer {
                    body: body.clone(),
                    input_tokens: input_tokens(body),
                })];
            }
            Routes::Merged {
                routes,
                input_weights,
                output_weights,
            } => (routes, input_weights, output_weights),
        };
        let upstream = match UpstreamAnswers::parse(body) {
            Ok(upstream) => upstream,
            Err(reason) => return vec![Err(SplitError::Unreadable(reason)); routes.len()],
        };

        // Integer usage fields are split by weight so that the callers'
        // shares add up to what the vendor bills; anything else is copied.
        let usage: Vec<(&str, Result<Vec<u64>, &RawValue>)> = upstream
            .usage
            .iter()
            .map(|(name, raw)| {
                let weights = if name == "output_tokens" {
                    output_weights
                } else {
                    input_weights
                };
                let split = serde_json::from_str::<u64>(raw.get())
                    .map(|total| apportion(total, weights))
                    .map_err(|_| raw.as_ref());
                (name.as_str(), split)
            })
            .collect();

        routes
            .iter()
            .enumerate()
            .map(|(caller, route)| {
                let mut answers = ObjectWriter::with_capacity(route.len() * 128);
                for (caller_id, upstream_id) in route {
                    let answer = upstream
                        .answers
                        .get(upstream_id)
                        .ok_or_else(|| SplitError::MissingAnswer(caller_id.clone()))?;
                    answers.field(caller_id, answer.get());
                }
                let answers = answers.finish();

                let mut caller_usage = ObjectWriter::with_capacity(64);
                let mut caller_input_tokens = None;
                for (name, split) in &usage {
                    match split {
                        Ok(shares) => {
                            caller_usage.field(name, &shares[caller].to_string());
                            if *name == "input_tokens" {
                                caller_input_tokens = Some(shares[caller]);
                            }
                        }
                        Err(raw) => caller_usage.field(name, raw.get()),
                    }
                }
                let caller_usage = caller_usage.finish();

                let mut out = ObjectWriter::with_capacity(answers.len() + 128);
                for (name, raw) in &upstream.envelope {
                    match name.as_str() {
                        "answers" => out.field(name, &answers),
                        "usage" => out.field(name, &caller_usage),
                        _ => out.field(name, raw.get()),
                    }
                }
                Ok(Answer {
                    body: Bytes::from(out.finish()),
                    input_tokens: caller_input_tokens,
                })
            })
            .collect()
    }
}
