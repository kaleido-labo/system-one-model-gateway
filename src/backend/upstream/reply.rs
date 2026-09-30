//! What an upstream call hands back: the reply, or how it failed.

use std::time::Duration;

use axum::http::StatusCode;
use bytes::Bytes;

#[derive(Debug)]
pub struct UpstreamReply {
    pub body: Bytes,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum UpstreamFailure {
    /// The backend answered with an error status, after any retries.
    Status {
        status: StatusCode,
        body: Bytes,
        retry_after: Option<Duration>,
        request_id: Option<String>,
    },
    /// No usable response: refused or reset connection, TLS failure, or an
    /// attempt that timed out, after any retries.
    Transport { timed_out: bool, detail: String },
    /// The backend asked everyone to back off past the calls' deadline, or
    /// its request slots are booked past it.
    Paused { retry_after: Duration },
    /// The deadline passed before an attempt could start.
    Deadline,
    /// The backend answered 200 with a body the gateway cannot use.
    Unreadable(String),
}

pub fn describe(failure: &UpstreamFailure) -> String {
    match failure {
        UpstreamFailure::Status { status, .. } => format!("status {}", status.as_u16()),
        UpstreamFailure::Transport {
            timed_out: true, ..
        } => "attempt timed out".to_owned(),
        UpstreamFailure::Transport { detail, .. } => format!("network error: {detail}"),
        UpstreamFailure::Paused { retry_after } => {
            format!("paused for {} ms after a 429", retry_after.as_millis())
        }
        UpstreamFailure::Deadline => "deadline reached".to_owned(),
        UpstreamFailure::Unreadable(reason) => format!("unreadable answer: {reason}"),
    }
}
