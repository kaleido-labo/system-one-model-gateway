//! What a batch holds and what one merged call may carry: the calls that
//! joined it, the distinct questions among them, and the running token totals
//! checked against `BatchLimits`.

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::Instant;

use crate::scheduling::dispatch::Member;
use crate::wire::{PreparedRequest, QuestionKey};

/// What one merged upstream call may carry.
#[derive(Debug, Clone)]
pub struct BatchLimits {
    /// How long a new batch waits for company. Zero disables merging.
    pub window: Duration,
    pub max_questions: usize,
    pub max_request_tokens: u32,
    pub max_state_plus_question_tokens: u32,
    /// Longest a call may wait for upstream capacity, on top of the window.
    pub max_queue_wait: Duration,
}

/// The calls in a batch, and the running totals the limits are checked on.
pub(super) struct Members {
    pub(super) sealed: bool,
    pub(super) calls: Vec<Member>,
    questions: HashSet<QuestionKey>,
    state_tokens: u32,
    question_tokens: u32,
    longest_question: u32,
}

impl Members {
    pub(super) fn new(state_tokens: u32) -> Self {
        Self {
            sealed: false,
            calls: Vec::new(),
            questions: HashSet::new(),
            state_tokens,
            question_tokens: 0,
            longest_question: 0,
        }
    }

    /// Whether `request` can join without the merged call going over a limit.
    /// Questions already in the batch cost nothing: they are sent once.
    pub(super) fn fits(&self, request: &PreparedRequest, limits: &BatchLimits) -> bool {
        let mut added = HashSet::new();
        let mut added_tokens = 0u32;
        let mut longest = self.longest_question;
        for question in &request.questions {
            if self.questions.contains(&question.key) || !added.insert(question.key) {
                continue;
            }
            added_tokens = added_tokens.saturating_add(question.tokens);
            longest = longest.max(question.tokens);
        }
        let total = self
            .state_tokens
            .saturating_add(self.question_tokens)
            .saturating_add(added_tokens);
        self.questions.len() + added.len() <= limits.max_questions
            && total <= limits.max_request_tokens
            && self.state_tokens.saturating_add(longest) <= limits.max_state_plus_question_tokens
    }

    pub(super) fn add(&mut self, member: Member) {
        for question in &member.request.questions {
            if self.questions.insert(question.key) {
                self.question_tokens = self.question_tokens.saturating_add(question.tokens);
                self.longest_question = self.longest_question.max(question.tokens);
            }
        }
        self.calls.push(member);
    }

    /// When the next call runs out of patience.
    pub(super) fn next_expiry(&self) -> Option<Instant> {
        self.calls.iter().map(|call| call.latest_send).min()
    }

    /// Takes out the calls whose latest send time has passed, and recounts
    /// the totals for the calls that stay.
    pub(super) fn take_expired(&mut self, now: Instant) -> Vec<Member> {
        let (expired, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.calls)
            .into_iter()
            .partition(|call| call.latest_send <= now);
        let mut kept = Members::new(self.state_tokens);
        for call in waiting {
            kept.add(call);
        }
        *self = kept;
        expired
    }
}
