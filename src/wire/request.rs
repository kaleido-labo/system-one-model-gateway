//! The System One wire format, parsed just enough to merge and split calls.
//!
//! Request: `{ state, model, questions: { id: { type, instructions, criteria } } }`.
//! Response: `{ model, answers: { id: answer }, usage: { input_tokens, output_tokens } }`.
//! See https://docs.typesafe.ai/api.md.
//!
//! Everything the model reads is kept as raw JSON text. Fields the gateway
//! does not know are forwarded untouched, so a future API field keeps working
//! through the gateway, and requests carrying different values for it never
//! share an upstream call.

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::wire::json::{JsonKind, ObjectWriter, minify};
use crate::wire::tokens::TokenEstimator;
use crate::wire::validate::{Invalid, check_question};

/// Requests with the same key carry the same `model`, the same `state` and
/// the same other top-level fields, so their questions can share one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchKey([u8; 32]);

/// Two questions with the same key ask the model exactly the same thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuestionKey([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    /// The body is not a JSON object.
    Malformed(String),
    /// The body is a JSON object that breaks a documented rule.
    Invalid(Invalid),
}

impl From<Invalid> for RequestError {
    fn from(invalid: Invalid) -> Self {
        Self::Invalid(invalid)
    }
}

/// A request that passed validation, normalised for merging.
#[derive(Debug)]
pub struct PreparedRequest {
    pub model: String,
    /// The minified `state`, which is what the model reads.
    pub state: Box<RawValue>,
    /// Top-level fields other than `state`, `model` and `questions`, minified,
    /// in the caller's order.
    pub extras: Vec<(String, Box<RawValue>)>,
    pub questions: Vec<PreparedQuestion>,
    pub key: BatchKey,
    pub state_tokens: u32,
}

#[derive(Debug)]
pub struct PreparedQuestion {
    /// The id the caller chose. The vendor never shows ids to the model, so
    /// the gateway is free to rename questions in a merged call.
    pub id: String,
    /// The minified question object as the caller wrote it.
    pub raw: Box<RawValue>,
    pub key: QuestionKey,
    pub tokens: u32,
}

impl PreparedRequest {
    pub fn parse(body: &[u8], estimator: &TokenEstimator) -> Result<Self, RequestError> {
        let mut fields: IndexMap<String, Box<RawValue>> =
            serde_json::from_slice(body).map_err(|err| {
                RequestError::Malformed(format!("the body must be a JSON object: {err}"))
            })?;

        let state = fields
            .shift_remove("state")
            .ok_or_else(|| Invalid::new("state", "is required"))?;
        let kind = JsonKind::of(state.get());
        if !kind.is_text_like() {
            return Err(Invalid::new(
                "state",
                format!(
                    "must be a string, an object or an array, got {}",
                    kind.name()
                ),
            )
            .into());
        }

        let model = fields
            .shift_remove("model")
            .ok_or_else(|| Invalid::new("model", "is required"))?;
        let model: String = serde_json::from_str(model.get())
            .map_err(|_| Invalid::new("model", "must be a string"))?;
        if model.trim().is_empty() {
            return Err(Invalid::new("model", "must not be empty").into());
        }

        let questions = fields
            .shift_remove("questions")
            .ok_or_else(|| Invalid::new("questions", "is required"))?;
        if JsonKind::of(questions.get()) != JsonKind::Object {
            return Err(Invalid::new(
                "questions",
                "must be an object mapping your question ids to questions",
            )
            .into());
        }
        let questions: IndexMap<String, Box<RawValue>> = serde_json::from_str(questions.get())
            .map_err(|err| Invalid::new("questions", format!("could not be read: {err}")))?;
        if questions.is_empty() {
            return Err(Invalid::new("questions", "must contain at least one question").into());
        }

        let questions = questions
            .into_iter()
            .map(|(id, raw)| prepare_question(id, &raw, estimator))
            .collect::<Result<Vec<_>, _>>()?;
        let state = minified(&state);
        let extras: Vec<_> = fields
            .into_iter()
            .map(|(name, value)| (name, minified(&value)))
            .collect();
        let key = batch_key(&model, &extras, &state);
        let state_tokens = estimator.estimate(state.get().len());
        Ok(Self {
            model,
            state,
            extras,
            questions,
            key,
            state_tokens,
        })
    }

    /// Estimated input tokens if this request went upstream on its own.
    pub fn standalone_tokens(&self) -> u32 {
        self.questions
            .iter()
            .fold(self.state_tokens, |sum, question| {
                sum.saturating_add(question.tokens)
            })
    }

    /// Encodes an upstream body carrying this request's state, model and
    /// extra fields, with the given questions.
    pub fn encode_with<'a>(
        &self,
        questions: impl IntoIterator<Item = (&'a str, &'a RawValue)>,
    ) -> String {
        let mut encoded_questions = ObjectWriter::with_capacity(256);
        for (id, raw) in questions {
            encoded_questions.field(id, raw.get());
        }
        let encoded_questions = encoded_questions.finish();
        let model = serde_json::to_string(&self.model).expect("a String always serializes");
        let mut body =
            ObjectWriter::with_capacity(self.state.get().len() + encoded_questions.len() + 64);
        body.field("state", self.state.get());
        body.field("model", &model);
        body.field("questions", &encoded_questions);
        for (name, value) in &self.extras {
            body.field(name, value.get());
        }
        body.finish()
    }

    /// Encodes this request on its own, under the caller's question ids.
    pub fn encode(&self) -> String {
        self.encode_with(
            self.questions
                .iter()
                .map(|question| (question.id.as_str(), question.raw.as_ref())),
        )
    }
}

fn prepare_question(
    id: String,
    raw: &RawValue,
    estimator: &TokenEstimator,
) -> Result<PreparedQuestion, Invalid> {
    let param = format!("questions.{id}");
    if JsonKind::of(raw.get()) != JsonKind::Object {
        return Err(Invalid::new(param, "must be a question object"));
    }
    let fields: IndexMap<String, Box<RawValue>> = serde_json::from_str(raw.get())
        .map_err(|err| Invalid::new(&param, format!("could not be read: {err}")))?;
    check_question(&param, &fields)?;
    let raw = minified(raw);
    Ok(PreparedQuestion {
        key: question_key(&fields),
        tokens: estimator.question(raw.get().len()),
        id,
        raw,
    })
}

fn minified(raw: &RawValue) -> Box<RawValue> {
    RawValue::from_string(minify(raw.get())).expect("minified JSON is still valid JSON")
}

/// Hashes `model`, the extra fields (sorted by name, since their order carries
/// no meaning) and the minified state. Key order inside the state is kept:
/// the model reads the state as text, so two orderings may not get the same
/// answers, and the gateway only merges requests whose state reads the same.
fn batch_key(model: &str, extras: &[(String, Box<RawValue>)], state: &RawValue) -> BatchKey {
    let mut hasher = Sha256::new();
    feed(&mut hasher, model.as_bytes());
    let mut extras: Vec<_> = extras.iter().collect();
    extras.sort_by(|a, b| a.0.cmp(&b.0));
    hasher.update((extras.len() as u64).to_le_bytes());
    for (name, value) in extras {
        feed(&mut hasher, name.as_bytes());
        feed(&mut hasher, value.get().as_bytes());
    }
    feed(&mut hasher, state.get().as_bytes());
    BatchKey(digest(hasher))
}

/// Hashes a question's fields sorted by name. The field order of the question
/// envelope carries no meaning, but key order inside `instructions` and
/// `criteria` does, and minifying keeps it.
fn question_key(fields: &IndexMap<String, Box<RawValue>>) -> QuestionKey {
    let mut names: Vec<&String> = fields.keys().collect();
    names.sort();
    let mut hasher = Sha256::new();
    for name in names {
        feed(&mut hasher, name.as_bytes());
        feed(&mut hasher, minify(fields[name].get()).as_bytes());
    }
    QuestionKey(digest(hasher))
}

/// Length-prefixes each part so that two different sequences of parts can
/// never hash the same bytes.
fn feed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn digest(hasher: Sha256) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(&hasher.finalize());
    out
}

/// A successful upstream body, parsed just enough to split it between callers.
#[derive(Debug)]
pub struct UpstreamAnswers {
    /// Every top-level field in upstream order, `answers` and `usage`
    /// included, so each caller's body can keep the vendor's layout.
    pub envelope: IndexMap<String, Box<RawValue>>,
    pub answers: IndexMap<String, Box<RawValue>>,
    /// Empty when the vendor sent no `usage`.
    pub usage: IndexMap<String, Box<RawValue>>,
}

impl UpstreamAnswers {
    pub fn parse(body: &[u8]) -> Result<Self, String> {
        let envelope: IndexMap<String, Box<RawValue>> =
            serde_json::from_slice(body).map_err(|err| format!("not a JSON object: {err}"))?;
        let answers = envelope.get("answers").ok_or("no `answers` field")?;
        let answers = serde_json::from_str(answers.get())
            .map_err(|err| format!("`answers` is not an object: {err}"))?;
        let usage = match envelope.get("usage") {
            Some(usage) => serde_json::from_str(usage.get())
                .map_err(|err| format!("`usage` is not an object: {err}"))?,
            None => IndexMap::new(),
        };
        Ok(Self {
            envelope,
            answers,
            usage,
        })
    }
}

/// Reads `usage.input_tokens` from a response body, if it has one.
pub fn input_tokens(body: &[u8]) -> Option<u64> {
    #[derive(Deserialize)]
    struct Envelope {
        usage: Option<Usage>,
    }
    #[derive(Deserialize)]
    struct Usage {
        input_tokens: Option<u64>,
    }
    serde_json::from_slice::<Envelope>(body)
        .ok()?
        .usage?
        .input_tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estimator() -> TokenEstimator {
        TokenEstimator::new(3.0)
    }

    fn parse(json: &str) -> Result<PreparedRequest, RequestError> {
        PreparedRequest::parse(json.as_bytes(), &estimator())
    }

    fn invalid_param(json: &str) -> String {
        match parse(json).unwrap_err() {
            RequestError::Invalid(invalid) => invalid.param,
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    const EXAMPLE: &str = r#"{
        "state": { "ticket": "Help! My payouts have been failing for 3 days.", "amount": 12.50 },
        "model": "jev-latest",
        "questions": {
            "is_urgent": { "type": "noul", "instructions": "Does this convey urgency?" },
            "department": {
                "type": "choice",
                "instructions": "Which team should handle this?",
                "criteria": { "billing": "Payments", "technical": "Bugs" }
            }
        }
    }"#;

    #[test]
    fn parses_and_minifies_the_documented_shape() {
        let request = parse(EXAMPLE).unwrap();
        assert_eq!(request.model, "jev-latest");
        assert_eq!(
            request.state.get(),
            r#"{"ticket":"Help! My payouts have been failing for 3 days.","amount":12.50}"#
        );
        let ids: Vec<_> = request.questions.iter().map(|q| q.id.as_str()).collect();
        assert_eq!(ids, ["is_urgent", "department"]);
        assert_eq!(
            request.questions[0].raw.get(),
            r#"{"type":"noul","instructions":"Does this convey urgency?"}"#
        );
        assert!(request.extras.is_empty());
    }

    #[test]
    fn keeps_unknown_top_level_fields_for_forwarding() {
        let request = parse(
            r#"{"state":"s","model":"m","questions":{"q":{"type":"noul","instructions":"i"}},"future_option": { "x" : 1 }}"#,
        )
        .unwrap();
        assert_eq!(request.extras.len(), 1);
        assert_eq!(request.extras[0].0, "future_option");
        assert_eq!(request.extras[0].1.get(), r#"{"x":1}"#);
        assert!(request.encode().ends_with(r#","future_option":{"x":1}}"#));
    }

    #[test]
    fn rejects_bodies_that_are_not_objects() {
        assert!(matches!(parse("[1,2]"), Err(RequestError::Malformed(_))));
        assert!(matches!(
            parse("{\"state\":"),
            Err(RequestError::Malformed(_))
        ));
    }

    #[test]
    fn reports_the_offending_field() {
        let question = r#"{"q":{"type":"noul","instructions":"i"}}"#;
        assert_eq!(
            invalid_param(&format!(r#"{{"model":"m","questions":{question}}}"#)),
            "state"
        );
        assert_eq!(
            invalid_param(&format!(
                r#"{{"state":3,"model":"m","questions":{question}}}"#
            )),
            "state"
        );
        assert_eq!(
            invalid_param(&format!(r#"{{"state":"s","questions":{question}}}"#)),
            "model"
        );
        assert_eq!(
            invalid_param(&format!(
                r#"{{"state":"s","model":" ","questions":{question}}}"#
            )),
            "model"
        );
        assert_eq!(invalid_param(r#"{"state":"s","model":"m"}"#), "questions");
        assert_eq!(
            invalid_param(r#"{"state":"s","model":"m","questions":{}}"#),
            "questions"
        );
        assert_eq!(
            invalid_param(r#"{"state":"s","model":"m","questions":[]}"#),
            "questions"
        );
        assert_eq!(
            invalid_param(r#"{"state":"s","model":"m","questions":{"q":"is it?"}}"#),
            "questions.q"
        );
        assert_eq!(
            invalid_param(
                r#"{"state":"s","model":"m","questions":{"q":{"type":"score","instructions":"i","criteria":["a"]}}}"#
            ),
            "questions.q.criteria"
        );
    }

    #[test]
    fn whitespace_does_not_change_the_batch_key() {
        let compact = parse(&minify(EXAMPLE)).unwrap();
        let pretty = parse(EXAMPLE).unwrap();
        assert_eq!(compact.key, pretty.key);
    }

    #[test]
    fn state_key_order_model_and_extras_change_the_batch_key() {
        let q = r#""questions":{"q":{"type":"noul","instructions":"i"}}"#;
        let base = parse(&format!(r#"{{"state":{{"a":1,"b":2}},"model":"m",{q}}}"#)).unwrap();
        let reordered = parse(&format!(r#"{{"state":{{"b":2,"a":1}},"model":"m",{q}}}"#)).unwrap();
        let other_model =
            parse(&format!(r#"{{"state":{{"a":1,"b":2}},"model":"n",{q}}}"#)).unwrap();
        let with_extra = parse(&format!(
            r#"{{"state":{{"a":1,"b":2}},"model":"m",{q},"x":1}}"#
        ))
        .unwrap();
        assert_ne!(base.key, reordered.key);
        assert_ne!(base.key, other_model.key);
        assert_ne!(base.key, with_extra.key);
    }

    #[test]
    fn extras_order_does_not_change_the_batch_key() {
        let q = r#""questions":{"q":{"type":"noul","instructions":"i"}}"#;
        let a = parse(&format!(r#"{{"state":"s","model":"m",{q},"x":1,"y":2}}"#)).unwrap();
        let b = parse(&format!(r#"{{"y":2,"state":"s","x":1,"model":"m",{q}}}"#)).unwrap();
        assert_eq!(a.key, b.key);
    }

    #[test]
    fn question_keys_ignore_field_order_but_not_content() {
        let request = parse(
            r#"{"state":"s","model":"m","questions":{
                "a":{"type":"choice","instructions":"i","criteria":{"x":null,"y":null}},
                "b":{"criteria":{"x":null,"y":null},"instructions":"i","type":"choice"},
                "c":{"type":"choice","instructions":"i","criteria":{"y":null,"x":null}},
                "d":{"type":"choice","instructions":"j","criteria":{"x":null,"y":null}}
            }}"#,
        )
        .unwrap();
        let keys: Vec<_> = request.questions.iter().map(|q| q.key).collect();
        assert_eq!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
        assert_ne!(keys[0], keys[3]);
    }

    #[test]
    fn encode_writes_state_model_questions_in_order() {
        let request = parse(EXAMPLE).unwrap();
        let body = request.encode();
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["model"], "jev-latest");
        assert_eq!(
            value["questions"]["department"]["criteria"]["billing"],
            "Payments"
        );
        assert!(body.starts_with(r#"{"state":{"ticket":"#));
        // The number is forwarded exactly as the caller wrote it.
        assert!(body.contains(r#""amount":12.50"#));
    }

    #[test]
    fn standalone_tokens_add_state_and_questions() {
        let request = parse(EXAMPLE).unwrap();
        let questions: u32 = request.questions.iter().map(|q| q.tokens).sum();
        assert_eq!(
            request.standalone_tokens(),
            request.state_tokens + questions
        );
    }

    #[test]
    fn upstream_answers_are_split_into_parts() {
        let body = br#"{"model":"jev-1.13.0","answers":{"a":{"type":"noul","noul":0.9}},"usage":{"input_tokens":10,"output_tokens":2}}"#;
        let parsed = UpstreamAnswers::parse(body).unwrap();
        let keys: Vec<_> = parsed.envelope.keys().map(String::as_str).collect();
        assert_eq!(keys, ["model", "answers", "usage"]);
        assert_eq!(parsed.answers["a"].get(), r#"{"type":"noul","noul":0.9}"#);
        assert_eq!(parsed.usage["input_tokens"].get(), "10");
        assert_eq!(input_tokens(body), Some(10));
        assert!(UpstreamAnswers::parse(br#"{"model":"x"}"#).is_err());
        assert_eq!(input_tokens(b"not json"), None);
    }
}
