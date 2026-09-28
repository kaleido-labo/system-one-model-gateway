//! The request rules TypeSafe documents, checked before a question can join a
//! merged call.
//!
//! One malformed question would make the vendor reject a merged call for every
//! service in it. Catching the documented mistakes here keeps that failure with
//! the caller that made it. The checks stop at what the API reference states
//! (https://docs.typesafe.ai/api.md): anything stricter would turn away
//! requests the vendor accepts. What slips through is handled by replaying a
//! rejected merged call one caller at a time (see `dispatch`).

use indexmap::IndexMap;
use serde_json::value::RawValue;

use crate::json::JsonKind;

/// Most options a Choice question accepts.
pub const MAX_CHOICE_OPTIONS: usize = 255;
/// Fewest and most levels a Score question accepts.
pub const SCORE_LEVELS: std::ops::RangeInclusive<usize> = 2..=10;

/// A field that breaks a documented rule, with the path of the field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid {
    pub param: String,
    pub message: String,
}

impl Invalid {
    pub fn new(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            param: param.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuestionType {
    Noul,
    Choice,
    Score,
}

/// Checks one question object, given as its fields, and returns its type.
pub fn check_question(
    param: &str,
    fields: &IndexMap<String, Box<RawValue>>,
) -> Result<QuestionType, Invalid> {
    let question_type = question_type(param, fields.get("type").map(AsRef::as_ref))?;
    let instructions = fields.get("instructions").map(AsRef::as_ref);
    check_text_like(&format!("{param}.instructions"), instructions, true)?;
    let criteria_param = format!("{param}.criteria");
    let criteria = fields.get("criteria").map(AsRef::as_ref);
    match question_type {
        QuestionType::Noul => check_noul_criteria(&criteria_param, criteria)?,
        QuestionType::Choice => check_choice_criteria(&criteria_param, criteria)?,
        QuestionType::Score => check_score_criteria(&criteria_param, criteria)?,
    }
    Ok(question_type)
}

fn question_type(param: &str, raw: Option<&RawValue>) -> Result<QuestionType, Invalid> {
    let param = format!("{param}.type");
    let raw = raw.ok_or_else(|| Invalid::new(&param, "is required"))?;
    match serde_json::from_str::<String>(raw.get()).as_deref() {
        Ok("noul") => Ok(QuestionType::Noul),
        Ok("choice") => Ok(QuestionType::Choice),
        Ok("score") => Ok(QuestionType::Score),
        _ => Err(Invalid::new(
            param,
            format!(
                "must be \"noul\", \"choice\" or \"score\", got {}",
                raw.get()
            ),
        )),
    }
}

/// `string | object | array`, the shape the API accepts for instructions and
/// for every description.
fn check_text_like(param: &str, raw: Option<&RawValue>, required: bool) -> Result<(), Invalid> {
    match raw {
        None if required => Err(Invalid::new(param, "is required")),
        None => Ok(()),
        Some(raw) => {
            let kind = JsonKind::of(raw.get());
            if kind.is_text_like() {
                Ok(())
            } else {
                Err(Invalid::new(
                    param,
                    format!(
                        "must be a string, an object or an array, got {}",
                        kind.name()
                    ),
                ))
            }
        }
    }
}

fn check_noul_criteria(param: &str, raw: Option<&RawValue>) -> Result<(), Invalid> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let criteria: IndexMap<String, &RawValue> = parse_object(param, raw)?;
    for key in ["true", "false"] {
        check_text_like(&format!("{param}.{key}"), criteria.get(key).copied(), false)?;
    }
    Ok(())
}

fn check_choice_criteria(param: &str, raw: Option<&RawValue>) -> Result<(), Invalid> {
    let raw = raw.ok_or_else(|| Invalid::new(param, "is required for a choice question"))?;
    let options: IndexMap<String, &RawValue> = parse_object(param, raw)?;
    if options.is_empty() {
        return Err(Invalid::new(param, "must list at least one option"));
    }
    if options.len() > MAX_CHOICE_OPTIONS {
        return Err(Invalid::new(
            param,
            format!(
                "lists {} options, the maximum is {MAX_CHOICE_OPTIONS}",
                options.len()
            ),
        ));
    }
    for (option, description) in &options {
        if JsonKind::of(description.get()) != JsonKind::Null {
            check_text_like(&format!("{param}.{option}"), Some(description), true)?;
        }
    }
    Ok(())
}

fn check_score_criteria(param: &str, raw: Option<&RawValue>) -> Result<(), Invalid> {
    let raw = raw.ok_or_else(|| Invalid::new(param, "is required for a score question"))?;
    if JsonKind::of(raw.get()) != JsonKind::Array {
        return Err(Invalid::new(
            param,
            "must be an array of level descriptions",
        ));
    }
    let levels: Vec<&RawValue> = serde_json::from_str(raw.get())
        .map_err(|err| Invalid::new(param, format!("could not be read: {err}")))?;
    if !SCORE_LEVELS.contains(&levels.len()) {
        return Err(Invalid::new(
            param,
            format!(
                "lists {} levels, a score needs between {} and {}",
                levels.len(),
                SCORE_LEVELS.start(),
                SCORE_LEVELS.end()
            ),
        ));
    }
    for (index, level) in levels.iter().enumerate() {
        check_text_like(&format!("{param}[{index}]"), Some(level), true)?;
    }
    Ok(())
}

fn parse_object<'a>(
    param: &str,
    raw: &'a RawValue,
) -> Result<IndexMap<String, &'a RawValue>, Invalid> {
    if JsonKind::of(raw.get()) != JsonKind::Object {
        return Err(Invalid::new(
            param,
            format!("must be an object, got {}", JsonKind::of(raw.get()).name()),
        ));
    }
    serde_json::from_str(raw.get())
        .map_err(|err| Invalid::new(param, format!("could not be read: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(json: &str) -> IndexMap<String, Box<RawValue>> {
        serde_json::from_str(json).unwrap()
    }

    fn check(json: &str) -> Result<QuestionType, Invalid> {
        check_question("questions.q", &fields(json))
    }

    #[test]
    fn accepts_the_documented_examples() {
        assert_eq!(
            check(r#"{"type":"noul","instructions":"Does this convey urgency?"}"#),
            Ok(QuestionType::Noul)
        );
        assert_eq!(
            check(
                r#"{"type":"noul","instructions":"Urgent?","criteria":{"true":"Explicitly time-sensitive","false":"No urgency expressed"}}"#
            ),
            Ok(QuestionType::Noul)
        );
        assert_eq!(
            check(
                r#"{"type":"choice","instructions":"Which team?","criteria":{"billing":"Payments","technical":null}}"#
            ),
            Ok(QuestionType::Choice)
        );
        assert_eq!(
            check(
                r#"{"type":"score","instructions":{"question":"How frustrated?"},"criteria":["Calm","Frustrated","Very angry"]}"#
            ),
            Ok(QuestionType::Score)
        );
    }

    #[test]
    fn rejects_an_unknown_or_missing_type() {
        let err = check(r#"{"type":"rank","instructions":"x"}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.type");
        let err = check(r#"{"instructions":"x"}"#).unwrap_err();
        assert_eq!(err, Invalid::new("questions.q.type", "is required"));
    }

    #[test]
    fn rejects_missing_or_numeric_instructions() {
        let err = check(r#"{"type":"noul"}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.instructions");
        let err = check(r#"{"type":"noul","instructions":42}"#).unwrap_err();
        assert!(err.message.contains("a number"), "{err:?}");
    }

    #[test]
    fn choice_needs_between_one_and_255_options() {
        let err = check(r#"{"type":"choice","instructions":"x"}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.criteria");
        let err = check(r#"{"type":"choice","instructions":"x","criteria":{}}"#).unwrap_err();
        assert!(err.message.contains("at least one"));
        let options: Vec<String> = (0..256).map(|i| format!("\"o{i}\":null")).collect();
        let json = format!(
            r#"{{"type":"choice","instructions":"x","criteria":{{{}}}}}"#,
            options.join(",")
        );
        let err = check(&json).unwrap_err();
        assert!(err.message.contains("256 options"), "{err:?}");
    }

    #[test]
    fn choice_option_descriptions_must_be_text_like_or_null() {
        let err = check(r#"{"type":"choice","instructions":"x","criteria":{"a":1}}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.criteria.a");
    }

    #[test]
    fn score_needs_between_two_and_ten_levels() {
        let err = check(r#"{"type":"score","instructions":"x","criteria":["only"]}"#).unwrap_err();
        assert!(err.message.contains("1 levels"), "{err:?}");
        let levels: Vec<String> = (0..11).map(|i| format!("\"l{i}\"")).collect();
        let json = format!(
            r#"{{"type":"score","instructions":"x","criteria":[{}]}}"#,
            levels.join(",")
        );
        assert!(check(&json).is_err());
        let err = check(r#"{"type":"score","instructions":"x","criteria":{"a":"b"}}"#).unwrap_err();
        assert!(err.message.contains("array"));
        let err =
            check(r#"{"type":"score","instructions":"x","criteria":["a",null]}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.criteria[1]");
    }

    #[test]
    fn noul_criteria_must_be_an_object_of_text_like_values() {
        let err = check(r#"{"type":"noul","instructions":"x","criteria":"yes"}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.criteria");
        let err = check(r#"{"type":"noul","instructions":"x","criteria":{"true":1}}"#).unwrap_err();
        assert_eq!(err.param, "questions.q.criteria.true");
    }
}
