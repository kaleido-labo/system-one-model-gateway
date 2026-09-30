//! Chat API errors in TypeSafe's error shape.
//!
//! A chat API rejects a request in its own format: OpenAI's
//! `{"error": {"message": ...}}`, Hugging Face's `{"error": "..."}`, vLLM's
//! flat `{"message": ...}`, or plain text. A service that talks TypeSafe's
//! API expects `{"error": {"type", "message"}}`, so the body is rewritten
//! here, before the failure reaches the dispatcher. The status, the
//! `retry-after` hint and the request id are kept as they came.

use serde_json::Value;

use crate::backend::UpstreamFailure;
use crate::error::GatewayError;

/// Longest provider message passed on, in characters. A provider can put a
/// page of text in an error, and the caller needs only the first lines.
const MAX_MESSAGE_CHARS: usize = 500;

/// Rewrites the body of an error status into TypeSafe's shape. Any other
/// failure is returned as it is: the gateway words those itself.
pub(super) fn translate(failure: UpstreamFailure) -> UpstreamFailure {
    match failure {
        UpstreamFailure::Status {
            status,
            body,
            retry_after,
            request_id,
        } => {
            let message = provider_message(&body).unwrap_or_else(|| {
                format!("the chat backend answered with status {}", status.as_u16())
            });
            UpstreamFailure::Status {
                status,
                body: GatewayError::from_status(status, message).body().into(),
                retry_after,
                request_id,
            }
        }
        other => other,
    }
}

/// The text of a provider's error body, when it has one. A body that is not
/// JSON is never echoed: it may be an HTML page, or repeat part of the
/// request.
fn provider_message(body: &[u8]) -> Option<String> {
    let body: Value = serde_json::from_slice(body).ok()?;
    let text = body
        .pointer("/error/message")
        .or_else(|| body.get("error"))
        .or_else(|| body.get("message"))
        .and_then(Value::as_str)?
        .trim();
    if text.is_empty() {
        return None;
    }
    Some(match text.char_indices().nth(MAX_MESSAGE_CHARS) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use bytes::Bytes;
    use std::time::Duration;

    use super::*;

    fn rewritten(status: u16, body: &str) -> (StatusCode, Value) {
        let failure = UpstreamFailure::Status {
            status: StatusCode::from_u16(status).unwrap(),
            body: Bytes::from(body.to_owned()),
            retry_after: Some(Duration::from_secs(2)),
            request_id: Some("req_1".to_owned()),
        };
        match translate(failure) {
            UpstreamFailure::Status {
                status,
                body,
                retry_after,
                request_id,
            } => {
                assert_eq!(retry_after, Some(Duration::from_secs(2)));
                assert_eq!(request_id.as_deref(), Some("req_1"));
                (status, serde_json::from_slice(&body).unwrap())
            }
            other => panic!("not a status failure: {other:?}"),
        }
    }

    #[test]
    fn an_openai_style_error_keeps_its_message() {
        let (status, body) = rewritten(
            400,
            r#"{"error": {"message": "Invalid value for 'top_logprobs'", "type": "invalid_request_error", "param": "top_logprobs", "code": null}}"#,
        );
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            serde_json::json!({"error": {
                "type": "invalid_request_error",
                "message": "Invalid value for 'top_logprobs'",
            }})
        );
    }

    #[test]
    fn a_hugging_face_style_error_keeps_its_message() {
        let (status, body) = rewritten(429, r#"{"error": "Rate limit reached"}"#);
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert_eq!(body["error"]["message"], "Rate limit reached");
    }

    #[test]
    fn a_flat_message_is_used_too() {
        let (_, body) = rewritten(
            404,
            r#"{"object": "error", "message": "The model does not exist.", "type": "NotFoundError", "code": 404}"#,
        );
        assert_eq!(body["error"]["type"], "not_found_error");
        assert_eq!(body["error"]["message"], "The model does not exist.");
    }

    #[test]
    fn a_body_that_is_not_json_is_not_echoed() {
        let (status, body) = rewritten(503, "<html>upstream connect error: your prompt</html>");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "upstream_error");
        assert_eq!(
            body["error"]["message"],
            "the chat backend answered with status 503"
        );
    }

    #[test]
    fn a_json_body_without_text_gets_the_generic_message() {
        for text in [r#"{}"#, r#"{"error": {"code": 7}}"#, r#"{"error": "  "}"#] {
            let (_, body) = rewritten(422, text);
            assert_eq!(body["error"]["type"], "validation_error");
            assert_eq!(
                body["error"]["message"], "the chat backend answered with status 422",
                "{text}"
            );
        }
    }

    #[test]
    fn a_long_message_is_cut() {
        let long = "é".repeat(800);
        let (_, body) = rewritten(400, &format!(r#"{{"error": "{long}"}}"#));
        let message = body["error"]["message"].as_str().unwrap();
        assert_eq!(message.chars().count(), MAX_MESSAGE_CHARS + 3);
        assert!(message.ends_with("..."));
    }

    #[test]
    fn other_failures_pass_unchanged() {
        assert!(matches!(
            translate(UpstreamFailure::Deadline),
            UpstreamFailure::Deadline
        ));
    }
}
