//! Grouping calls that share a state into batches.
//!
//! A batch opens with the first call for a `(model, state)` pair and books
//! one upstream request slot. It stays open for `window_ms`, then for as long
//! as it waits for that slot and for a free upstream connection, and every
//! call for the same pair that arrives meanwhile joins it for free. So
//! merging does the most when it matters: when the upstream limit is
//! saturated and calls queue up anyway.
//!
//! Each call carries a latest send time: its arrival plus
//! `max_queue_wait_ms`, and never earlier than the end of the merge window.
//! A call still waiting at that time gets a 429 there and then.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::{Instant, sleep_until};

use crate::dispatch::{Dispatcher, MIN_RETRY_AFTER, Member, Outcome};
use crate::error::GatewayError;
use crate::wire::{BatchKey, PreparedRequest, QuestionKey};

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

/// Every upstream slot within `max_queue_wait` is booked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Saturated {
    pub retry_after: Duration,
}

pub struct Coalescer {
    open: Mutex<HashMap<BatchKey, Arc<Batch>>>,
    limits: BatchLimits,
    dispatcher: Arc<Dispatcher>,
}

struct Batch {
    key: BatchKey,
    opened: Instant,
    send_at: Instant,
    members: Mutex<Members>,
}

/// The calls in a batch, and the running totals the limits are checked on.
struct Members {
    sealed: bool,
    calls: Vec<Member>,
    questions: HashSet<QuestionKey>,
    state_tokens: u32,
    question_tokens: u32,
    longest_question: u32,
}

impl Members {
    fn new(state_tokens: u32) -> Self {
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
    fn fits(&self, request: &PreparedRequest, limits: &BatchLimits) -> bool {
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

    fn add(&mut self, member: Member) {
        for question in &member.request.questions {
            if self.questions.insert(question.key) {
                self.question_tokens = self.question_tokens.saturating_add(question.tokens);
                self.longest_question = self.longest_question.max(question.tokens);
            }
        }
        self.calls.push(member);
    }

    /// When the next call runs out of patience.
    fn next_expiry(&self) -> Option<Instant> {
        self.calls.iter().map(|call| call.latest_send).min()
    }

    /// Takes out the calls whose latest send time has passed, and recounts
    /// the totals for the calls that stay.
    fn take_expired(&mut self, now: Instant) -> Vec<Member> {
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

impl Coalescer {
    pub fn new(limits: BatchLimits, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            open: Mutex::new(HashMap::new()),
            limits,
            dispatcher,
        }
    }

    pub fn engine(&self) -> &crate::backend::Engine {
        self.dispatcher.engine()
    }

    /// Queues a call that arrived at `arrived`. Its outcome is sent on `reply`.
    pub fn submit(
        self: &Arc<Self>,
        request: PreparedRequest,
        arrived: Instant,
        deadline: Instant,
        reply: oneshot::Sender<Outcome>,
    ) -> Result<(), Saturated> {
        let now = Instant::now();
        // The merge window is the gateway's own delay: it never counts
        // against the caller's patience.
        let latest_send = (arrived + self.limits.max_queue_wait).max(now + self.limits.window);
        let member = Member {
            request,
            latest_send,
            deadline,
            reply,
        };
        // Lock order: the open map, then a batch's members, then the pacer.
        // `run` and `shed_expired` take them in the same order.
        let mut open = lock(&self.open);
        if let Some(batch) = open.get(&member.request.key) {
            let mut members = lock(&batch.members);
            if !members.sealed
                && batch.send_at <= latest_send
                && members.fits(&member.request, &self.limits)
            {
                members.add(member);
                return Ok(());
            }
        }

        // A new batch needs its own upstream slot, early enough for this call.
        let slot = self
            .dispatcher
            .pacer()
            .try_book(now, 1, latest_send.saturating_duration_since(now))
            .map_err(|retry_after| Saturated { retry_after })?;
        let mut members = Members::new(member.request.state_tokens);
        let key = member.request.key;
        members.add(member);
        let batch = Arc::new(Batch {
            key,
            opened: now,
            send_at: slot.max(now + self.limits.window),
            members: Mutex::new(members),
        });
        if !self.limits.window.is_zero() {
            // A newer batch replaces a full one for the same key; the full
            // one keeps its members and still goes out on its own schedule.
            open.insert(key, Arc::clone(&batch));
        }
        drop(open);
        tokio::spawn(Arc::clone(self).run(batch));
        Ok(())
    }

    async fn run(self: Arc<Self>, batch: Arc<Batch>) {
        sleep_until(batch.send_at).await;

        // Wait for a free upstream connection, still open to new calls. The
        // acquire future stays pinned across the loop, so the batch keeps its
        // place in the semaphore's queue while callers who run out of
        // patience are answered one by one.
        let acquire = self.dispatcher.acquire();
        tokio::pin!(acquire);
        let permit = loop {
            let expiry = lock(&batch.members)
                .next_expiry()
                .unwrap_or_else(Instant::now);
            tokio::select! {
                // A connection that is already free wins over a deadline that
                // passed while the batch slept until `send_at`.
                biased;
                permit = &mut acquire => break permit,
                () = sleep_until(expiry) => {
                    if self.shed_expired(&batch, Instant::now()) {
                        // Nobody is left: hand the request slot back.
                        self.dispatcher.pacer().adjust(-1);
                        return;
                    }
                }
            }
        };

        let calls = {
            let mut open = lock(&self.open);
            if open
                .get(&batch.key)
                .is_some_and(|current| Arc::ptr_eq(current, &batch))
            {
                open.remove(&batch.key);
            }
            let mut members = lock(&batch.members);
            members.sealed = true;
            std::mem::take(&mut members.calls)
        };
        self.dispatcher.dispatch(calls, permit, batch.opened).await;
    }

    /// Answers 429 to the callers of `batch` whose latest send time has
    /// passed. Returns true if that emptied the batch, which is then closed.
    fn shed_expired(&self, batch: &Arc<Batch>, now: Instant) -> bool {
        let mut open = lock(&self.open);
        let mut members = lock(&batch.members);
        let expired = members.take_expired(now);
        if !expired.is_empty() {
            let retry_after = self.dispatcher.pacer().backlog(now).max(MIN_RETRY_AFTER);
            let outcome = Outcome::Failed(GatewayError::rate_limited(
                "every upstream connection stayed busy past max_queue_wait_ms",
                retry_after,
            ));
            for member in expired {
                let _ = member.reply.send(outcome.clone());
            }
        }
        if !members.calls.is_empty() {
            return false;
        }
        members.sealed = true;
        if open
            .get(&batch.key)
            .is_some_and(|current| Arc::ptr_eq(current, batch))
        {
            open.remove(&batch.key);
        }
        true
    }

    /// Batches still accepting calls.
    #[cfg(test)]
    fn open_batches(&self) -> usize {
        lock(&self.open).len()
    }
}

/// The guarded data stays consistent at every await point, so a panic in
/// another task is no reason to stop serving: take the lock anyway.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Engine;
    use crate::limiter::Gcra;
    use crate::metrics::Metrics;
    use crate::upstream::{ApiKey, RetryPolicy, Upstream};
    use crate::wire::TokenEstimator;
    use serde_json::json;

    fn request(state: &str, questions: serde_json::Value) -> PreparedRequest {
        let body = json!({"state": state, "model": "jev-latest", "questions": questions});
        PreparedRequest::parse(body.to_string().as_bytes(), &TokenEstimator::new(1.0)).unwrap()
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            window: Duration::from_millis(50),
            max_questions: 3,
            max_request_tokens: 10_000,
            max_state_plus_question_tokens: 10_000,
            max_queue_wait: Duration::from_secs(1),
        }
    }

    fn member(request: PreparedRequest) -> (Member, oneshot::Receiver<Outcome>) {
        let (reply, answer) = oneshot::channel();
        let now = Instant::now();
        let member = Member {
            request,
            latest_send: now + Duration::from_secs(1),
            deadline: now + Duration::from_secs(2),
            reply,
        };
        (member, answer)
    }

    /// Submits a call arriving now; keep the receiver alive or the call
    /// counts as abandoned.
    fn submit(
        coalescer: &Arc<Coalescer>,
        request: PreparedRequest,
    ) -> Result<oneshot::Receiver<Outcome>, Saturated> {
        let (reply, answer) = oneshot::channel();
        let now = Instant::now();
        coalescer
            .submit(request, now, now + Duration::from_secs(2), reply)
            .map(|()| answer)
    }

    fn coalescer(limits: BatchLimits, upstream_rpm: f64) -> Arc<Coalescer> {
        let metrics = Arc::new(Metrics::new());
        let pacer = Arc::new(Gcra::per_minute(upstream_rpm, 1));
        // Nothing listens on port 9; the batches below are never sent.
        let upstream = Upstream::new(
            "test",
            "http://127.0.0.1:9",
            Some(ApiKey::new("test")),
            Duration::from_millis(100),
            RetryPolicy {
                max_retries: 0,
                backoff_initial: Duration::from_millis(1),
                backoff_max: Duration::from_millis(1),
                attempt_timeout: Duration::from_millis(100),
            },
            Arc::clone(&pacer),
            Arc::clone(&metrics),
        )
        .unwrap();
        let upstream = Arc::new(upstream);
        let engine = Engine::SystemOne {
            url: upstream.endpoint("v1/systemone").unwrap(),
            upstream,
        };
        let dispatcher = Dispatcher::new(
            "test",
            engine,
            pacer,
            Gcra::per_second(1_000_000.0, 1_000_000),
            4,
            metrics,
        );
        Arc::new(Coalescer::new(limits, Arc::new(dispatcher)))
    }

    fn noul(text: &str) -> serde_json::Value {
        json!({"type": "noul", "instructions": text})
    }

    #[test]
    fn fits_counts_shared_questions_once_and_enforces_limits() {
        let limits = limits();
        let mut members = Members::new(10);
        let (first, _answer) = member(request("s", json!({"a": noul("A"), "b": noul("B")})));
        members.add(first);
        // Same questions again: free.
        assert!(members.fits(
            &request("s", json!({"x": noul("A"), "y": noul("B")})),
            &limits
        ));
        // One new question: 3 distinct, still fits.
        assert!(members.fits(&request("s", json!({"c": noul("C")})), &limits));
        // Two new questions: 4 distinct, over max_questions.
        assert!(!members.fits(
            &request("s", json!({"c": noul("C"), "d": noul("D")})),
            &limits
        ));

        let tight = BatchLimits {
            max_state_plus_question_tokens: 40,
            ..limits
        };
        let long = "x".repeat(100);
        assert!(!members.fits(&request("s", json!({"long": noul(&long)})), &tight));
    }

    #[tokio::test]
    async fn calls_with_the_same_state_join_one_open_batch() {
        let coalescer = coalescer(limits(), 60_000.0);
        let _a = submit(&coalescer, request("same", json!({"a": noul("A")}))).unwrap();
        let _b = submit(&coalescer, request("same", json!({"b": noul("B")}))).unwrap();
        let _c = submit(&coalescer, request("other", json!({"c": noul("C")}))).unwrap();
        assert_eq!(coalescer.open_batches(), 2);
        let open = lock(&coalescer.open);
        let calls: Vec<usize> = open
            .values()
            .map(|b| lock(&b.members).calls.len())
            .collect();
        assert_eq!(calls.iter().sum::<usize>(), 3);
        assert!(calls.contains(&2));
    }

    #[tokio::test]
    async fn a_full_batch_is_replaced_by_a_new_one() {
        let coalescer = coalescer(limits(), 60_000.0);
        let full = request("s", json!({"a": noul("A"), "b": noul("B"), "c": noul("C")}));
        let _a = submit(&coalescer, full).unwrap();
        let _b = submit(&coalescer, request("s", json!({"d": noul("D")}))).unwrap();
        let open = lock(&coalescer.open);
        assert_eq!(open.len(), 1);
        let batch = open.values().next().unwrap();
        // The open batch is the new one, holding only the second call.
        assert_eq!(lock(&batch.members).calls.len(), 1);
    }

    #[tokio::test]
    async fn a_new_batch_is_refused_when_no_slot_is_free_in_time() {
        // One request per second, no burst beyond one.
        let coalescer = coalescer(
            BatchLimits {
                max_queue_wait: Duration::from_millis(200),
                ..limits()
            },
            60.0,
        );
        let _a = submit(&coalescer, request("first", json!({"a": noul("A")}))).unwrap();
        let refused = submit(&coalescer, request("second", json!({"b": noul("B")}))).unwrap_err();
        assert!(
            refused.retry_after > Duration::from_millis(900),
            "{refused:?}"
        );
        // Joining an open batch needs no slot, so it still works.
        let _c = submit(&coalescer, request("first", json!({"c": noul("C")}))).unwrap();
    }

    #[tokio::test]
    async fn with_merging_off_every_call_is_its_own_batch() {
        let coalescer = coalescer(
            BatchLimits {
                window: Duration::ZERO,
                ..limits()
            },
            60_000.0,
        );
        let _a = submit(&coalescer, request("same", json!({"a": noul("A")}))).unwrap();
        let _b = submit(&coalescer, request("same", json!({"b": noul("B")}))).unwrap();
        assert_eq!(coalescer.open_batches(), 0);
    }
}
