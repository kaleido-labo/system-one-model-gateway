//! Reading an answer from a chat completion.
//!
//! The probability of each label comes from the first generated token's
//! `top_logprobs`, and the answer is written in the shape TypeSafe sends for
//! that question type.

use serde::Deserialize;

use super::prompt::{Kind, Prompt, letters};
use crate::wire::ObjectWriter;

#[derive(Deserialize)]
pub(super) struct ChatResponse {
    pub(super) model: Option<String>,
    #[serde(default)]
    choices: Vec<ChatChoice>,
    pub(super) usage: Option<ChatUsage>,
}

#[derive(Deserialize)]
struct ChatChoice {
    logprobs: Option<ChatLogprobs>,
}

#[derive(Deserialize)]
struct ChatLogprobs {
    content: Option<Vec<TokenLogprobs>>,
}

#[derive(Deserialize)]
struct TokenLogprobs {
    top_logprobs: Option<Vec<Candidate>>,
}

#[derive(Deserialize)]
struct Candidate {
    token: String,
    logprob: f64,
}

#[derive(Deserialize)]
pub(super) struct ChatUsage {
    pub(super) prompt_tokens: Option<u64>,
    pub(super) completion_tokens: Option<u64>,
}

impl Prompt {
    fn labels(&self) -> Vec<String> {
        match &self.kind {
            Kind::Noul => vec!["Yes".to_owned(), "No".to_owned()],
            Kind::Choice(options) => letters().take(options.len()).map(String::from).collect(),
            Kind::Score(levels) => (0..levels.len()).map(|level| level.to_string()).collect(),
        }
    }

    /// Reads the answer from the first token's `top_logprobs` and writes it
    /// as TypeSafe's answer JSON.
    pub(super) fn answer(&self, response: &ChatResponse) -> Result<String, String> {
        let candidates = response
            .choices
            .first()
            .and_then(|choice| choice.logprobs.as_ref())
            .and_then(|logprobs| logprobs.content.as_ref())
            .and_then(|content| content.first())
            .and_then(|token| token.top_logprobs.as_ref())
            .filter(|candidates| !candidates.is_empty())
            .ok_or(
                "the backend sent no top_logprobs; the model or provider must support logprobs",
            )?;
        let probabilities = distribution(&self.labels(), candidates)?;
        let number = |value: f64| serde_json::to_string(&value).expect("a finite f64 serializes");

        let mut answer = ObjectWriter::with_capacity(128);
        match &self.kind {
            Kind::Noul => {
                answer.field("type", r#""noul""#);
                answer.field("noul", &number(probabilities[0]));
            }
            Kind::Choice(options) => {
                let best = most_likely(&probabilities);
                let mut spread = ObjectWriter::with_capacity(16 * options.len());
                for (option, probability) in options.iter().zip(&probabilities) {
                    spread.field(option, &number(*probability));
                }
                answer.field("type", r#""choice""#);
                answer.field(
                    "choice",
                    &serde_json::to_string(&options[best]).expect("a String always serializes"),
                );
                answer.field("probabilities", &spread.finish());
                answer.field("confidence", &number(confidence(&probabilities)));
            }
            Kind::Score(levels) => {
                let score: f64 = probabilities
                    .iter()
                    .enumerate()
                    .map(|(level, probability)| level as f64 * probability)
                    .sum();
                let mut legend = ObjectWriter::with_capacity(32 * levels.len());
                let mut spread = ObjectWriter::with_capacity(16 * levels.len());
                for (level, (description, probability)) in
                    levels.iter().zip(&probabilities).enumerate()
                {
                    legend.field(&level.to_string(), description.get());
                    spread.field(&level.to_string(), &number(*probability));
                }
                answer.field("type", r#""score""#);
                answer.field("score", &number(score));
                answer.field("legend", &legend.finish());
                answer.field("probabilities", &spread.finish());
                answer.field("confidence", &number(confidence(&probabilities)));
            }
        }
        Ok(answer.finish())
    }
}

/// The probability of each label, from the candidates for the first token.
/// Tokens that spell the same label (`Yes`, ` Yes`, `yes`) add up; tokens
/// that are no label are dropped, and the rest is renormalised to sum to 1.
fn distribution(labels: &[String], candidates: &[Candidate]) -> Result<Vec<f64>, String> {
    let mut mass = vec![0.0; labels.len()];
    for candidate in candidates {
        let token = normalise(&candidate.token);
        if let Some(index) = labels
            .iter()
            .position(|label| label.eq_ignore_ascii_case(token))
        {
            let probability = candidate.logprob.exp();
            if probability.is_finite() {
                mass[index] += probability;
            }
        }
    }
    let total: f64 = mass.iter().sum();
    if total.is_nan() || total <= 0.0 {
        let seen: Vec<&str> = candidates.iter().map(|c| c.token.as_str()).collect();
        return Err(format!(
            "none of the most likely first tokens {seen:?} is one of the labels {labels:?}"
        ));
    }
    Ok(mass.into_iter().map(|m| m / total).collect())
}

/// The label a token spells: without surrounding spaces, without the word
/// markers some tokenizers leave (`▁`, `Ġ`) and without a trailing `.`, `)`
/// or `:` as in "A." or "B)".
fn normalise(token: &str) -> &str {
    token
        .trim_matches(|c: char| c.is_whitespace() || c == '▁' || c == 'Ġ')
        .trim_end_matches(['.', ')', ':'])
}

fn most_likely(probabilities: &[f64]) -> usize {
    probabilities
        .iter()
        .enumerate()
        .fold(0, |best, (index, p)| {
            if *p > probabilities[best] {
                index
            } else {
                best
            }
        })
}

/// TypeSafe's confidence: 1 when all the probability sits on one outcome,
/// 0 when it is spread evenly. For n outcomes, `(n * max - 1) / (n - 1)`,
/// which is the formula the docs give for three
/// (https://docs.typesafe.ai/confidence.md).
fn confidence(probabilities: &[f64]) -> f64 {
    let n = probabilities.len() as f64;
    if n < 2.0 {
        return 1.0;
    }
    let max = probabilities.iter().copied().fold(0.0, f64::max);
    ((n * max - 1.0) / (n - 1.0)).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use serde_json::value::RawValue;
    use serde_json::{Value, json};

    fn raw(json: &str) -> Box<RawValue> {
        RawValue::from_string(json.to_owned()).unwrap()
    }

    fn response(candidates: &[(&str, f64)]) -> ChatResponse {
        let top: Vec<Value> = candidates
            .iter()
            .map(|(token, p)| json!({"token": token, "logprob": p.ln()}))
            .collect();
        serde_json::from_value(json!({
            "model": "Qwen/Qwen2.5-7B-Instruct",
            "choices": [{"logprobs": {"content": [{"token": "x", "logprob": -0.1, "top_logprobs": top}]}}],
            "usage": {"prompt_tokens": 40, "completion_tokens": 1},
        }))
        .unwrap()
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn a_noul_adds_spellings_of_the_same_label_and_drops_other_tokens() {
        let prompt = Prompt::new(
            "Parking Saemes - 18,00 EUR",
            &raw(r#"{"type":"noul","instructions":"Is this a parking receipt?","criteria":{"true":"a car park"}}"#),
        )
        .unwrap();
        assert!(prompt.user.starts_with(
            "State:\nParking Saemes - 18,00 EUR\n\nQuestion: Is this a parking receipt?\n"
        ));
        assert!(prompt.user.contains("Answer Yes if: a car park\n"));
        assert!(!prompt.user.contains("Answer No if"));

        let answer = prompt
            .answer(&response(&[
                (" Yes", 0.6),
                ("Yes", 0.2),
                ("No", 0.1),
                ("Maybe", 0.1),
            ]))
            .unwrap();
        let value: Value = serde_json::from_str(&answer).unwrap();
        assert_eq!(value["type"], "noul");
        assert!(
            close(value["noul"].as_f64().unwrap(), 0.8 / 0.9),
            "{answer}"
        );
        assert!(answer.starts_with(r#"{"type":"noul","noul":"#));
    }

    #[test]
    fn a_choice_reads_one_letter_per_option_in_the_documented_shape() {
        let prompt = Prompt::new(
            "ticket",
            &raw(r#"{"type":"choice","instructions":"Which team?","criteria":{"billing":"Payments","technical":null,"sales":"Deals"}}"#),
        )
        .unwrap();
        assert!(
            prompt
                .user
                .contains("A. billing: Payments\nB. technical\nC. sales: Deals\n")
        );

        let answer = prompt
            .answer(&response(&[
                ("B", 0.5),
                (" A", 0.3),
                ("b", 0.1),
                ("The", 0.1),
            ]))
            .unwrap();
        let value: Value = serde_json::from_str(&answer).unwrap();
        assert_eq!(value["choice"], "technical");
        let p = &value["probabilities"];
        assert!(close(p["technical"].as_f64().unwrap(), 0.6 / 0.9));
        assert!(close(p["billing"].as_f64().unwrap(), 0.3 / 0.9));
        assert_eq!(p["sales"].as_f64().unwrap(), 0.0);
        // (3 * max - 1) / 2, the formula TypeSafe documents for 3 options.
        assert!(close(
            value["confidence"].as_f64().unwrap(),
            (3.0 * 0.6 / 0.9 - 1.0) / 2.0
        ));
        // Fields in the order the API reference lists them.
        let fields: IndexMap<String, Value> = serde_json::from_str(&answer).unwrap();
        let keys: Vec<&String> = fields.keys().collect();
        assert_eq!(keys, ["type", "choice", "probabilities", "confidence"]);
    }

    #[test]
    fn a_score_weights_levels_and_echoes_them_in_the_legend() {
        let prompt = Prompt::new(
            r#"{"ticket":"late"}"#,
            &raw(r#"{"type":"score","instructions":"How angry?","criteria":["Calm","Frustrated",{"label":"Very angry"}]}"#),
        )
        .unwrap();
        assert!(prompt.user.contains(
            "Levels, from 0 to 2:\n0. Calm\n1. Frustrated\n2. {\"label\":\"Very angry\"}\n"
        ));

        let answer = prompt
            .answer(&response(&[("1", 0.6), ("2", 0.2), ("Level", 0.2)]))
            .unwrap();
        let value: Value = serde_json::from_str(&answer).unwrap();
        assert!(
            close(value["score"].as_f64().unwrap(), 0.75 + 2.0 * 0.25),
            "{answer}"
        );
        assert_eq!(value["legend"]["2"]["label"], "Very angry");
        assert_eq!(value["probabilities"]["0"].as_f64().unwrap(), 0.0);
        assert!(close(value["probabilities"]["1"].as_f64().unwrap(), 0.75));
    }

    #[test]
    fn an_answer_without_any_label_or_logprobs_is_an_error() {
        let prompt = Prompt::new("s", &raw(r#"{"type":"noul","instructions":"q"}"#)).unwrap();
        let err = prompt
            .answer(&response(&[("Sorry", 0.7), ("I", 0.3)]))
            .unwrap_err();
        assert!(err.contains("Sorry"), "{err}");

        let bare: ChatResponse = serde_json::from_value(json!({
            "choices": [{"message": {"content": "Yes"}}]
        }))
        .unwrap();
        assert!(prompt.answer(&bare).unwrap_err().contains("top_logprobs"));
    }

    #[test]
    fn tokens_are_matched_without_spaces_markers_or_trailing_punctuation() {
        assert_eq!(normalise(" Yes"), "Yes");
        assert_eq!(normalise("▁No"), "No");
        assert_eq!(normalise("ĠA"), "A");
        assert_eq!(normalise("B)"), "B");
        assert_eq!(normalise("C.\n"), "C");
    }

    #[test]
    fn confidence_follows_the_documented_formula() {
        assert!(close(confidence(&[1.0, 0.0, 0.0]), 1.0));
        assert!(close(confidence(&[1.0 / 3.0; 3]), 0.0));
        assert!(close(
            confidence(&[0.88, 0.12, 0.0]),
            (3.0 * 0.88 - 1.0) / 2.0
        ));
        assert!(close(confidence(&[0.5, 0.5]), 0.0));
        assert!(close(confidence(&[1.0]), 1.0));
    }
}
