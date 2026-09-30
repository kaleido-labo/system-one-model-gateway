//! The body of a successful upstream answer, read just enough to split it
//! between the callers of a merged call.

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::value::RawValue;

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
