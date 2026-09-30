//! Errors the gateway answers itself, all in one JSON shape:
//! `{"error": {"type": "...", "message": "...", "param": "..."}}`.
//!
//! Status codes follow the TypeSafe API, so the vendor SDKs raise the same
//! exception types they would against TypeSafe directly, and retry the same
//! ones (429 and 5xx) honouring `retry-after`.

use std::time::Duration;

use axum::http::header::{CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::wire::Invalid;

/// `retry-after-ms`, which the TypeSafe SDKs read before `retry-after`.
pub const RETRY_AFTER_MS: HeaderName = HeaderName::from_static("retry-after-ms");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayError {
    pub status: StatusCode,
    pub kind: &'static str,
    pub message: String,
    pub param: Option<String>,
    pub retry_after: Option<Duration>,
}

impl GatewayError {
    fn new(status: StatusCode, kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            kind,
            message: message.into(),
            param: None,
            retry_after: None,
        }
    }

    pub fn malformed(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request_error", message)
    }

    pub fn invalid(invalid: Invalid) -> Self {
        let message = format!("{} {}", invalid.param, invalid.message);
        Self {
            param: Some(invalid.param),
            ..Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                message,
            )
        }
    }

    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "missing or unknown service key; send the key the gateway issued to your service as `Authorization: Bearer <key>`",
        )
    }

    pub fn model_not_allowed(model: &str) -> Self {
        Self {
            param: Some("model".to_owned()),
            ..Self::new(
                StatusCode::FORBIDDEN,
                "permission_error",
                format!("this service may not use model {model:?}"),
            )
        }
    }

    pub fn rate_limited(message: impl Into<String>, retry_after: Duration) -> Self {
        Self {
            retry_after: Some(retry_after),
            ..Self::new(StatusCode::TOO_MANY_REQUESTS, "rate_limit_error", message)
        }
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "upstream_error", message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "timeout_error", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found_error", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }

    /// An error status that a backend sent and the gateway passes on with
    /// its own status code, under the `type` this status has in the table
    /// above. A backend's 500 is `upstream_error`, not `internal_error`:
    /// `internal_error` means a bug in the gateway.
    pub fn from_status(status: StatusCode, message: impl Into<String>) -> Self {
        let kind = match status {
            StatusCode::FORBIDDEN => "permission_error",
            StatusCode::NOT_FOUND => "not_found_error",
            StatusCode::UNPROCESSABLE_ENTITY => "validation_error",
            StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => "timeout_error",
            StatusCode::UNAUTHORIZED => "authentication_error",
            status if status.is_client_error() => "invalid_request_error",
            _ => "upstream_error",
        };
        Self::new(status, kind, message)
    }

    /// The JSON body of this error.
    pub fn body(&self) -> String {
        serde_json::to_string(&Body {
            error: Detail {
                kind: self.kind,
                message: &self.message,
                param: self.param.as_deref(),
            },
        })
        .expect("the error body always serializes")
    }
}

#[derive(Serialize)]
struct Body<'a> {
    error: Detail<'a>,
}

#[derive(Serialize)]
struct Detail<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    param: Option<&'a str>,
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let body = self.body();
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(retry_after) = self.retry_after {
            insert_retry_after(&mut headers, retry_after);
        }
        (self.status, headers, body).into_response()
    }
}

/// Sets `retry-after` (whole seconds, rounded up) and `retry-after-ms`.
pub fn insert_retry_after(headers: &mut HeaderMap, retry_after: Duration) {
    let millis = retry_after.as_millis().max(1);
    let seconds = millis.div_ceil(1000);
    headers.insert(RETRY_AFTER, HeaderValue::from(seconds as u64));
    headers.insert(RETRY_AFTER_MS, HeaderValue::from(millis as u64));
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn validation_errors_name_the_field() {
        let response = GatewayError::invalid(Invalid::new("questions.q.criteria", "is required"))
            .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body_of(response).await;
        assert_eq!(body["error"]["type"], "validation_error");
        assert_eq!(body["error"]["param"], "questions.q.criteria");
        assert_eq!(body["error"]["message"], "questions.q.criteria is required");
    }

    #[test]
    fn a_backend_status_keeps_its_code_and_gets_the_matching_type() {
        for (status, kind) in [
            (400, "invalid_request_error"),
            (402, "invalid_request_error"),
            (403, "permission_error"),
            (404, "not_found_error"),
            (422, "validation_error"),
            (429, "rate_limit_error"),
            (500, "upstream_error"),
            (503, "upstream_error"),
            (504, "timeout_error"),
            (529, "upstream_error"),
        ] {
            let error = GatewayError::from_status(StatusCode::from_u16(status).unwrap(), "no");
            assert_eq!((error.status.as_u16(), error.kind), (status, kind));
        }
    }

    #[tokio::test]
    async fn rate_limits_carry_both_retry_after_headers() {
        let response =
            GatewayError::rate_limited("slow down", Duration::from_millis(1_250)).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[RETRY_AFTER], "2");
        assert_eq!(response.headers()[RETRY_AFTER_MS], "1250");
        let body = body_of(response).await;
        assert!(body["error"].get("param").is_none());
    }
}
