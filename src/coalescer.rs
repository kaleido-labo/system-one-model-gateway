//! Grouping calls that share a state into batches.
//!
//! A batch opens with the first call for a `(model, state)` pair and books
//! one upstream request slot. It stays open for `window_ms`, or for as long
//! as it waits for that slot, and every call for the same pair that arrives
//! meanwhile joins it for free. So merging does the most when it matters:
//! when the upstream limit is saturated and calls queue up anyway.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::{Instant, sleep_until};

use crate::dispatch::{Dispatcher, Member};
use crate::protocol::{BatchKey, PreparedRequest, QuestionKey};

/// What one merged upstream call may carry.
#[derive(Debug, Clone)]
pub struct BatchLimits {
    /// How long a new batch waits for company. Zero disables merging.
    pub window: Duration,
    pub max_questions: usize,
    pub max_request_tokens: u32,
    pub max_state_plus_question_tokens: u32,
    /// Longest a new batch may wait for an upstream request slot.
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
}

impl Coalescer {
    pub fn new(limits: BatchLimits, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            open: Mutex::new(HashMap::new()),
            limits,
            dispatcher,
        }
    }

    /// The shared upstream request pacer.
    pub fn pacer(&self) -> &crate::limiter::Gcra {
        self.dispatcher.pacer()
    }

    /// Queues a call. Its outcome arrives on `member.reply`.
    pub fn submit(self: &Arc<Self>, member: Member) -> Result<(), Saturated> {
        let now = Instant::now();
        // Lock order: the open map, then a batch's members. `run` takes them
        // in the same order.
        let mut open = lock(&self.open);
        if let Some(batch) = open.get(&member.request.key) {
            let mut members = lock(&batch.members);
            if !members.sealed
                && batch.send_at <= member.queue_deadline
                && members.fits(&member.request, &self.limits)
            {
                members.add(member);
                return Ok(());
            }
        }

        // A new batch needs its own upstream slot.
        let slot = self
            .dispatcher
            .pacer()
            .try_book(now, 1, self.limits.max_queue_wait)
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
        // Still open while it waits for a free upstream slot.
        let permit = self.dispatcher.acquire().await;
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
    use crate::limiter::Gcra;
    use crate::metrics::Metrics;
    use crate::tokens::TokenEstimator;
    use crate::upstream::{ApiKey, RetryPolicy, Upstream};
    use serde_json::json;
    use tokio::sync::oneshot;

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

    fn member(request: PreparedRequest) -> (Member, oneshot::Receiver<crate::dispatch::Outcome>) {
        let (reply, answer) = oneshot::channel();
        let now = Instant::now();
        let member = Member {
            request,
            queue_deadline: now + Duration::from_secs(1),
            deadline: now + Duration::from_secs(2),
            reply,
        };
        (member, answer)
    }

    fn coalescer(limits: BatchLimits, upstream_rpm: f64) -> Arc<Coalescer> {
        let metrics = Arc::new(Metrics::new());
        let pacer = Arc::new(Gcra::per_minute(upstream_rpm, 1));
        // Nothing listens on port 9; the batches below are never sent.
        let upstream = Upstream::new(
            "http://127.0.0.1:9",
            ApiKey::new("test"),
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
        let dispatcher = Dispatcher::new(
            Arc::new(upstream),
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
        let (a, _ra) = member(request("same", json!({"a": noul("A")})));
        let (b, _rb) = member(request("same", json!({"b": noul("B")})));
        let (c, _rc) = member(request("other", json!({"c": noul("C")})));
        coalescer.submit(a).unwrap();
        coalescer.submit(b).unwrap();
        coalescer.submit(c).unwrap();
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
        let (a, _ra) = member(request(
            "s",
            json!({"a": noul("A"), "b": noul("B"), "c": noul("C")}),
        ));
        let (b, _rb) = member(request("s", json!({"d": noul("D")})));
        coalescer.submit(a).unwrap();
        coalescer.submit(b).unwrap();
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
        let (a, _ra) = member(request("first", json!({"a": noul("A")})));
        let (b, _rb) = member(request("second", json!({"b": noul("B")})));
        let (c, _rc) = member(request("first", json!({"c": noul("C")})));
        coalescer.submit(a).unwrap();
        let refused = coalescer.submit(b).unwrap_err();
        assert!(
            refused.retry_after > Duration::from_millis(900),
            "{refused:?}"
        );
        // Joining an open batch needs no slot, so it still works.
        coalescer.submit(c).unwrap();
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
        let (a, _ra) = member(request("same", json!({"a": noul("A")})));
        let (b, _rb) = member(request("same", json!({"b": noul("B")})));
        coalescer.submit(a).unwrap();
        coalescer.submit(b).unwrap();
        assert_eq!(coalescer.open_batches(), 0);
    }
}
