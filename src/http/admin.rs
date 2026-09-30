//! The optional bearer token of the admin port.
//!
//! Only `/metrics` asks for it. The probes stay open, because a Kubernetes
//! probe cannot easily send a header and the two answers they get (`ok`,
//! `ready`) say nothing worth protecting.

use std::fmt;

use axum::http::HeaderMap;

use crate::services::{bearer_token, hash_key};

/// The token `/metrics` expects. Only its SHA-256 is kept, as with service
/// keys, and Debug output never shows even that.
pub struct AdminToken([u8; 32]);

impl AdminToken {
    pub fn new(token: &str) -> Self {
        Self(hash_key(token))
    }

    /// Whether the `Authorization: Bearer` header carries the token.
    ///
    /// Both sides are hashed first, so the two values compared always have the
    /// same length, and the comparison looks at every byte whatever the
    /// difference: a wrong guess takes as long to refuse as a nearly right one.
    pub fn allows(&self, headers: &HeaderMap) -> bool {
        let Some(presented) = bearer_token(headers) else {
            return false;
        };
        let presented = hash_key(presented);
        self.0
            .iter()
            .zip(presented)
            .fold(0u8, |difference, (expected, got)| {
                difference | (expected ^ got)
            })
            == 0
    }
}

impl fmt::Debug for AdminToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdminToken(redacted)")
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header::AUTHORIZATION;

    use super::*;

    fn headers(value: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(value) = value {
            headers.insert(AUTHORIZATION, value.parse().unwrap());
        }
        headers
    }

    #[test]
    fn only_the_right_bearer_token_is_allowed() {
        let token = AdminToken::new("scrape-me");
        assert!(token.allows(&headers(Some("Bearer scrape-me"))));
        assert!(token.allows(&headers(Some("bearer scrape-me"))));
        assert!(!token.allows(&headers(Some("Bearer scrape-m"))));
        assert!(!token.allows(&headers(Some("Bearer scrape-me-too"))));
        assert!(!token.allows(&headers(Some("Basic scrape-me"))));
        assert!(!token.allows(&headers(Some("scrape-me"))));
        assert!(!token.allows(&headers(Some("Bearer "))));
        assert!(!token.allows(&headers(None)));
    }

    #[test]
    fn debug_output_hides_the_token() {
        assert_eq!(
            format!("{:?}", AdminToken::new("scrape-me")),
            "AdminToken(redacted)"
        );
    }
}
