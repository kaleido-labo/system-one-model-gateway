//! How a backend answers a System One request: by forwarding it to a System
//! One server, or by asking a chat model one question at a time.

use std::sync::Arc;

use bytes::Bytes;
use tokio::time::Instant;

use super::chat::ChatEngine;
use super::{Upstream, UpstreamFailure, UpstreamReply};
use crate::wire::{Invalid, PreparedRequest};

/// How a backend answers a System One request.
pub enum Engine {
    /// Forwards the body to `POST /v1/systemone`.
    SystemOne {
        upstream: Arc<Upstream>,
        url: reqwest::Url,
    },
    /// Asks a chat model one question at a time (see `chat`).
    Chat(ChatEngine),
}

impl Engine {
    /// Sends a System One request body and returns a System One response
    /// body, retried until `deadline`.
    pub async fn execute(
        &self,
        body: Bytes,
        deadline: Instant,
    ) -> Result<UpstreamReply, UpstreamFailure> {
        match self {
            Self::SystemOne { upstream, url } => upstream.post(url, body, deadline).await,
            Self::Chat(chat) => chat.execute(&body, deadline).await,
        }
    }

    /// Refuses a request this backend cannot answer, before it joins a batch.
    pub fn check(&self, request: &PreparedRequest) -> Result<(), Invalid> {
        match self {
            Self::SystemOne { .. } => Ok(()),
            Self::Chat(chat) => chat.check(request),
        }
    }

    /// Whether a merged call sends the state once. A chat backend sends it
    /// again with every question, so merging there only saves the
    /// questions two services share.
    pub fn sends_state_once(&self) -> bool {
        matches!(self, Self::SystemOne { .. })
    }
}
