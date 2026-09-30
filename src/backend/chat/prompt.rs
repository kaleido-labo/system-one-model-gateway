//! What a chat model reads for one question: the prompt built from a System
//! One question, and the labels its answer is read back from.
//!
//! The state comes first, so every question about it shares the same prefix
//! and a server with prefix caching reads the state once. The prompt ends by
//! asking for one label: Yes or No, a letter per option, or a digit per level.

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::wire::minify;

/// Most options a Choice question may have on a chat backend: one letter each.
pub(super) const MAX_CHOICE_OPTIONS: usize = 26;

#[derive(Deserialize)]
pub(super) struct QuestionFields {
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) instructions: Box<RawValue>,
    pub(super) criteria: Option<Box<RawValue>>,
}

/// What the model reads for one question, and how to read its answer.
#[derive(Debug)]
pub(super) struct Prompt {
    pub(super) user: String,
    pub(super) kind: Kind,
}

#[derive(Debug)]
pub(super) enum Kind {
    Noul,
    /// The option names, which answers use as keys.
    Choice(Vec<String>),
    /// The level descriptions as sent, which answers echo in `legend`.
    Score(Vec<Box<RawValue>>),
}

impl Prompt {
    pub(super) fn new(state: &str, raw: &RawValue) -> Result<Self, String> {
        let fields: QuestionFields =
            serde_json::from_str(raw.get()).map_err(|err| format!("unreadable question: {err}"))?;
        // The state comes first, so every question about it shares the same
        // prefix and a server with prefix caching reads the state once.
        let mut user = format!(
            "State:\n{state}\n\nQuestion: {}\n",
            text_of(&fields.instructions)
        );
        let kind = match fields.kind.as_str() {
            "noul" => {
                let criteria: IndexMap<String, Box<RawValue>> = match &fields.criteria {
                    Some(raw) => serde_json::from_str(raw.get())
                        .map_err(|err| format!("unreadable criteria: {err}"))?,
                    None => IndexMap::new(),
                };
                for (key, label) in [("true", "Yes"), ("false", "No")] {
                    if let Some(description) = criteria.get(key) {
                        user.push_str(&format!("Answer {label} if: {}\n", text_of(description)));
                    }
                }
                user.push_str("\nReply with Yes or No only.");
                Kind::Noul
            }
            "choice" => {
                let criteria: IndexMap<String, Box<RawValue>> = fields
                    .criteria
                    .as_ref()
                    .map(|raw| serde_json::from_str(raw.get()))
                    .transpose()
                    .map_err(|err| format!("unreadable criteria: {err}"))?
                    .unwrap_or_default();
                if criteria.is_empty() || criteria.len() > MAX_CHOICE_OPTIONS {
                    return Err(format!(
                        "a choice needs 1 to {MAX_CHOICE_OPTIONS} options here, got {}",
                        criteria.len()
                    ));
                }
                user.push_str("Options:\n");
                for (label, (option, description)) in letters().zip(&criteria) {
                    if description.get() == "null" {
                        user.push_str(&format!("{label}. {option}\n"));
                    } else {
                        user.push_str(&format!("{label}. {option}: {}\n", text_of(description)));
                    }
                }
                user.push_str("\nReply with the letter of one option only.");
                Kind::Choice(criteria.into_keys().collect())
            }
            "score" => {
                let levels: Vec<Box<RawValue>> = fields
                    .criteria
                    .as_ref()
                    .map(|raw| serde_json::from_str(raw.get()))
                    .transpose()
                    .map_err(|err| format!("unreadable criteria: {err}"))?
                    .unwrap_or_default();
                if !(2..=10).contains(&levels.len()) {
                    return Err(format!(
                        "a score needs 2 to 10 levels, got {}",
                        levels.len()
                    ));
                }
                user.push_str(&format!("Levels, from 0 to {}:\n", levels.len() - 1));
                for (level, description) in levels.iter().enumerate() {
                    user.push_str(&format!("{level}. {}\n", text_of(description)));
                }
                user.push_str("\nReply with the number of one level only.");
                Kind::Score(levels)
            }
            other => return Err(format!("unknown question type {other:?}")),
        };
        Ok(Self { user, kind })
    }
}

pub(super) fn letters() -> impl Iterator<Item = &'static str> {
    const LETTERS: [&str; MAX_CHOICE_OPTIONS] = [
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
        "S", "T", "U", "V", "W", "X", "Y", "Z",
    ];
    LETTERS.into_iter()
}

/// What the model reads for a JSON value: a string's text, or the value's
/// compact JSON for an object or an array.
pub(super) fn text_of(raw: &RawValue) -> String {
    serde_json::from_str::<String>(raw.get()).unwrap_or_else(|_| minify(raw.get()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value, json};

    fn raw(json: &str) -> Box<RawValue> {
        RawValue::from_string(json.to_owned()).unwrap()
    }

    #[test]
    fn prompts_refuse_what_the_labels_cannot_express() {
        let options: Map<String, Value> = (0..27).map(|i| (format!("o{i}"), Value::Null)).collect();
        let question = json!({"type": "choice", "instructions": "q", "criteria": options});
        assert!(Prompt::new("s", &raw(&question.to_string())).is_err());
        assert!(
            Prompt::new(
                "s",
                &raw(r#"{"type":"score","instructions":"q","criteria":["a"]}"#)
            )
            .is_err()
        );
    }
}
