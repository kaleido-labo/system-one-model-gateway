//! Turning a batch of calls into one upstream request, and the upstream
//! answer back into one response per call.
//!
//! Merging is safe because the vendor evaluates every question of a request
//! "independently" against the same state, and never shows question ids to
//! the model (https://docs.typesafe.ai/primitives.md). A question gets the
//! same answer whether it travels alone or next to another service's.

use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::value::RawValue;

use crate::wire::{ObjectWriter, PreparedRequest, QuestionKey, UpstreamAnswers, input_tokens};

mod usage;

use usage::apportion;

/// How a batch of calls goes upstream, and how the answer comes back.
#[derive(Debug)]
pub struct Plan {
    pub body: Bytes,
    pub callers: usize,
    /// Distinct questions in the upstream call.
    pub questions: usize,
    /// Questions left out because an identical one was already in the call.
    pub deduplicated: usize,
    /// Estimated input tokens of the upstream call.
    pub estimated_tokens: u32,
    /// Estimated input tokens had each call gone upstream on its own.
    pub standalone_tokens: u32,
    routes: Routes,
}

#[derive(Debug)]
enum Routes {
    /// One caller: its own ids go upstream, and the vendor's body comes back
    /// unchanged.
    Passthrough,
    /// Several callers. For each: its (question id, upstream id) pairs, and
    /// its weights for splitting input and output tokens.
    Merged {
        routes: Vec<Vec<(String, String)>>,
        input_weights: Vec<f64>,
        output_weights: Vec<f64>,
    },
}

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

struct UniqueQuestion<'a> {
    upstream_id: String,
    raw: &'a RawValue,
    tokens: u32,
    askers: usize,
}

impl Plan {
    /// Plans one upstream call for `requests`, which must share a batch key.
    pub fn new(requests: &[&PreparedRequest]) -> Self {
        match requests {
            [] => panic!("a batch has at least one call"),
            [single] => Self::single(single),
            _ => Self::merged(requests),
        }
    }

    fn single(request: &PreparedRequest) -> Self {
        let tokens = request.standalone_tokens();
        Self {
            body: Bytes::from(request.encode()),
            callers: 1,
            questions: request.questions.len(),
            deduplicated: 0,
            estimated_tokens: tokens,
            standalone_tokens: tokens,
            routes: Routes::Passthrough,
        }
    }

    fn merged(requests: &[&PreparedRequest]) -> Self {
        let first = requests[0];
        debug_assert!(
            requests.iter().all(|r| r.key == first.key),
            "one batch, one key"
        );

        let mut unique: IndexMap<QuestionKey, UniqueQuestion<'_>> = IndexMap::new();
        let mut routes = Vec::with_capacity(requests.len());
        let mut asked = 0;
        for request in requests {
            let mut route = Vec::with_capacity(request.questions.len());
            for question in &request.questions {
                asked += 1;
                let next = unique.len();
                let entry = unique
                    .entry(question.key)
                    .or_insert_with(|| UniqueQuestion {
                        upstream_id: format!("q{next}"),
                        raw: &question.raw,
                        tokens: question.tokens,
                        askers: 0,
                    });
                entry.askers += 1;
                route.push((question.id.clone(), entry.upstream_id.clone()));
            }
            routes.push(route);
        }

        // Each caller pays an even share of the state, and each question is
        // paid by the callers who asked it.
        let state_share = f64::from(first.state_tokens) / requests.len() as f64;
        let mut input_weights = Vec::with_capacity(requests.len());
        let mut output_weights = Vec::with_capacity(requests.len());
        for request in requests {
            let mut input = state_share;
            let mut output = 0.0;
            for question in &request.questions {
                let shared = &unique[&question.key];
                input += f64::from(shared.tokens) / shared.askers as f64;
                output += 1.0 / shared.askers as f64;
            }
            input_weights.push(input);
            output_weights.push(output);
        }

        let estimated_tokens = unique
            .values()
            .fold(first.state_tokens, |sum, q| sum.saturating_add(q.tokens));
        let standalone_tokens = requests
            .iter()
            .fold(0u32, |sum, r| sum.saturating_add(r.standalone_tokens()));
        let body = first.encode_with(unique.values().map(|q| (q.upstream_id.as_str(), q.raw)));
        Self {
            body: Bytes::from(body),
            callers: requests.len(),
            questions: unique.len(),
            deduplicated: asked - unique.len(),
            estimated_tokens,
            standalone_tokens,
            routes: Routes::Merged {
                routes,
                input_weights,
                output_weights,
            },
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::TokenEstimator;
    use serde_json::{Value, json};

    /// A request whose questions keep the given order. `json!` would sort
    /// them, and the order decides which upstream id each question gets.
    fn request(questions: &[(&str, Value)]) -> PreparedRequest {
        let mut encoded = ObjectWriter::with_capacity(256);
        for (id, question) in questions {
            encoded.field(id, &question.to_string());
        }
        let body = format!(
            r#"{{"state":{{"ticket":"Help! My payouts have been failing for 3 days."}},"model":"jev-latest","questions":{}}}"#,
            encoded.finish()
        );
        PreparedRequest::parse(body.as_bytes(), &TokenEstimator::new(3.0)).unwrap()
    }

    fn noul(instructions: &str) -> Value {
        json!({"type": "noul", "instructions": instructions})
    }

    /// The fields of a JSON object, in the order they were written.
    fn ordered(json: &[u8]) -> Vec<(String, Value)> {
        serde_json::from_slice::<IndexMap<String, Value>>(json)
            .unwrap()
            .into_iter()
            .collect()
    }

    /// The fields of the object stored under `name`, in written order.
    fn nested(json: &[u8], name: &str) -> Vec<(String, Value)> {
        let fields: IndexMap<String, Box<RawValue>> = serde_json::from_slice(json).unwrap();
        ordered(fields[name].get().as_bytes())
    }

    fn questions_sent(plan: &Plan) -> Vec<(String, Value)> {
        nested(&plan.body, "questions")
    }

    fn pairs(expected: &[(&str, Value)]) -> Vec<(String, Value)> {
        expected
            .iter()
            .map(|(id, value)| ((*id).to_owned(), value.clone()))
            .collect()
    }

    #[test]
    fn a_single_call_goes_through_unchanged() {
        let only = request(&[("urgent", noul("Urgent?"))]);
        let plan = Plan::new(&[&only]);
        assert_eq!(questions_sent(&plan), pairs(&[("urgent", noul("Urgent?"))]));
        assert_eq!(plan.estimated_tokens, plan.standalone_tokens);

        let body = Bytes::from_static(
            br#"{"model":"jev-1.13.0","answers":{"urgent":{"type":"noul","noul":0.95}},"usage":{"input_tokens":296,"output_tokens":20}}"#,
        );
        let answers = plan.split(&body);
        assert_eq!(
            answers,
            vec![Ok(Answer {
                body: body.clone(),
                input_tokens: Some(296)
            })]
        );
    }

    #[test]
    fn merged_calls_share_the_state_and_get_fresh_ids() {
        let a = request(&[("urgent", noul("Urgent?")), ("angry", noul("Angry?"))]);
        let b = request(&[("urgent", noul("Is it about billing?"))]);
        let plan = Plan::new(&[&a, &b]);
        assert_eq!(
            questions_sent(&plan),
            pairs(&[
                ("q0", noul("Urgent?")),
                ("q1", noul("Angry?")),
                ("q2", noul("Is it about billing?")),
            ])
        );
        let body = ordered(&plan.body);
        assert_eq!(
            body[0],
            (
                "state".to_owned(),
                json!({"ticket": "Help! My payouts have been failing for 3 days."})
            )
        );
        assert_eq!(plan.callers, 2);
        assert_eq!(plan.questions, 3);
        assert_eq!(plan.deduplicated, 0);
        assert_eq!(
            plan.estimated_tokens + a.state_tokens,
            plan.standalone_tokens
        );
    }

    #[test]
    fn identical_questions_are_asked_once() {
        let a = request(&[("urgent", noul("Urgent?"))]);
        let b = request(&[("is_urgent", noul("Urgent?")), ("angry", noul("Angry?"))]);
        let plan = Plan::new(&[&a, &b]);
        assert_eq!(
            questions_sent(&plan),
            pairs(&[("q0", noul("Urgent?")), ("q1", noul("Angry?"))])
        );
        assert_eq!(plan.deduplicated, 1);
    }

    #[test]
    fn answers_go_back_under_each_callers_ids_with_split_usage() {
        let a = request(&[("urgent", noul("Urgent?"))]);
        let b = request(&[("is_urgent", noul("Urgent?")), ("angry", noul("Angry?"))]);
        let plan = Plan::new(&[&a, &b]);
        let upstream = Bytes::from_static(
            br#"{"model":"jev-1.13.0","answers":{"q0":{"type":"noul","noul":0.9},"q1":{"type":"noul","noul":0.1}},"usage":{"input_tokens":1000,"output_tokens":40,"note":"x"}}"#,
        );
        let answers: Vec<_> = plan
            .split(&upstream)
            .into_iter()
            .map(Result::unwrap)
            .collect();

        let first: Value = serde_json::from_slice(&answers[0].body).unwrap();
        let second: Value = serde_json::from_slice(&answers[1].body).unwrap();
        assert_eq!(first["model"], "jev-1.13.0");
        assert_eq!(
            first["answers"],
            json!({"urgent": {"type": "noul", "noul": 0.9}})
        );
        assert_eq!(
            nested(&answers[1].body, "answers"),
            pairs(&[
                ("is_urgent", json!({"type": "noul", "noul": 0.9})),
                ("angry", json!({"type": "noul", "noul": 0.1})),
            ])
        );
        // Shares add up to the vendor's totals; b asked more, so b pays more.
        let input: Vec<u64> = answers.iter().map(|a| a.input_tokens.unwrap()).collect();
        assert_eq!(input.iter().sum::<u64>(), 1000);
        assert!(input[1] > input[0]);
        let output = first["usage"]["output_tokens"].as_u64().unwrap()
            + second["usage"]["output_tokens"].as_u64().unwrap();
        assert_eq!(output, 40);
        assert_eq!(first["usage"]["note"], "x");
        // The vendor's field order is kept.
        let keys: Vec<_> = ordered(&answers[0].body)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, ["model", "answers", "usage"]);
    }

    #[test]
    fn a_missing_answer_fails_only_the_caller_who_asked() {
        let a = request(&[("urgent", noul("Urgent?"))]);
        let b = request(&[("angry", noul("Angry?"))]);
        let plan = Plan::new(&[&a, &b]);
        let upstream =
            Bytes::from_static(br#"{"model":"m","answers":{"q0":{"type":"noul","noul":0.9}}}"#);
        let answers = plan.split(&upstream);
        assert!(answers[0].is_ok());
        assert_eq!(
            answers[1],
            Err(SplitError::MissingAnswer("angry".to_owned()))
        );
        // Without usage from the vendor, callers get none either.
        let first: Value = serde_json::from_slice(&answers[0].as_ref().unwrap().body).unwrap();
        assert!(first.get("usage").is_none());
    }

    #[test]
    fn an_unreadable_body_fails_every_caller() {
        let a = request(&[("urgent", noul("Urgent?"))]);
        let b = request(&[("angry", noul("Angry?"))]);
        let plan = Plan::new(&[&a, &b]);
        let answers = plan.split(&Bytes::from_static(b"<html>"));
        assert!(
            answers
                .iter()
                .all(|a| matches!(a, Err(SplitError::Unreadable(_))))
        );
    }
}
