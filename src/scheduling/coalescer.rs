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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::{Instant, sleep_until};
use tracing::Span;

use crate::error::GatewayError;
use crate::scheduling::dispatch::{Dispatcher, MIN_RETRY_AFTER, Member, Outcome};
use crate::telemetry;
use crate::wire::{BatchKey, PreparedRequest};

mod members;

pub use members::BatchLimits;
use members::Members;

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

/// How far `submit` got without waiting for the pacer.
enum InProcess {
    Done(Result<(), Saturated>),
    /// No batch to join, and the slot for a new one is booked through Redis.
    NeedsSlot(Member),
}

struct Batch {
    key: BatchKey,
    opened: Instant,
    send_at: Instant,
    members: Mutex<Members>,
    /// The batch in the trace, from the first call to the last answer.
    span: Span,
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
    pub async fn submit(
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
            // Called from the handler, inside the call's trace span.
            span: Span::current(),
        };
        let member = match self.submit_in_process(member, now) {
            InProcess::Done(result) => return result,
            InProcess::NeedsSlot(member) => member,
        };

        // The slot is booked through Redis, which must not happen under the
        // lock: a call that finds no batch to join asks for its slot first.
        let slot = self
            .dispatcher
            .pacer()
            .try_book(now, 1, latest_send.saturating_duration_since(now))
            .await
            .map_err(|retry_after| Saturated { retry_after })?;
        // A call for the same state may have opened a batch while this one
        // was waiting for Redis. Better to join it than to open a second
        // batch for the same state, so the slot goes back.
        if self.join_or_open(member, slot, now) {
            self.dispatcher.pacer().adjust(-1).await;
        }
        Ok(())
    }

    /// The part of `submit` that needs no await: joining an open batch, and
    /// opening one when the pacer lives in this process. All of it happens
    /// under one lock, so two calls for the same state never both open a
    /// batch.
    fn submit_in_process(self: &Arc<Self>, member: Member, now: Instant) -> InProcess {
        // Lock order: the open map, then a batch's members, then the pacer.
        // `run` and `shed_expired` take them in the same order.
        let open = lock(&self.open);
        let Some(member) = self.join(&open, member) else {
            return InProcess::Done(Ok(()));
        };

        // A new batch needs its own upstream slot, early enough for this call.
        let wait = member.latest_send.saturating_duration_since(now);
        match self.dispatcher.pacer().try_book_in_process(now, 1, wait) {
            Some(Ok(slot)) => {
                self.open_batch(open, member, slot, now);
                InProcess::Done(Ok(()))
            }
            Some(Err(retry_after)) => InProcess::Done(Err(Saturated { retry_after })),
            None => InProcess::NeedsSlot(member),
        }
    }

    /// Puts a call that holds a fresh slot into an open batch that has
    /// appeared meanwhile, if any, and otherwise opens a batch on the slot.
    /// True when the call joined, which leaves the slot unused.
    fn join_or_open(self: &Arc<Self>, member: Member, slot: Instant, now: Instant) -> bool {
        let open = lock(&self.open);
        match self.join(&open, member) {
            None => true,
            Some(member) => {
                self.open_batch(open, member, slot, now);
                false
            }
        }
    }

    /// Adds the call to the open batch for its state, if that batch can
    /// still take it. Otherwise hands the call back.
    fn join(&self, open: &HashMap<BatchKey, Arc<Batch>>, member: Member) -> Option<Member> {
        if let Some(batch) = open.get(&member.request.key) {
            let mut members = lock(&batch.members);
            if !members.sealed
                && batch.send_at <= member.latest_send
                && members.fits(&member.request, &self.limits)
            {
                telemetry::link_to_batch(&batch.span, &member.span);
                members.add(member);
                return None;
            }
        }
        Some(member)
    }

    /// Opens a batch for the call, to leave when `slot` is reached.
    fn open_batch(
        self: &Arc<Self>,
        mut open: MutexGuard<'_, HashMap<BatchKey, Arc<Batch>>>,
        member: Member,
        slot: Instant,
        now: Instant,
    ) {
        let mut members = Members::new(member.request.state_tokens);
        let key = member.request.key;
        // The first call is the batch's parent (see `telemetry`).
        let span = telemetry::batch(
            &member.span,
            self.dispatcher.backend(),
            &member.request.model,
        );
        members.add(member);
        let batch = Arc::new(Batch {
            key,
            opened: now,
            send_at: slot.max(now + self.limits.window),
            members: Mutex::new(members),
            span,
        });
        if !self.limits.window.is_zero() {
            // A newer batch replaces a full one for the same key; the full
            // one keeps its members and still goes out on its own schedule.
            open.insert(key, Arc::clone(&batch));
        }
        drop(open);
        tokio::spawn(Arc::clone(self).run(batch));
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
                    if self.shed_expired(&batch, Instant::now()).await {
                        // Nobody is left: hand the request slot back.
                        self.dispatcher.pacer().adjust(-1).await;
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
        self.dispatcher
            .dispatch(calls, permit, batch.opened, batch.span.clone())
            .await;
    }

    /// Answers 429 to the callers of `batch` whose latest send time has
    /// passed. Returns true if that emptied the batch, which is then closed.
    async fn shed_expired(&self, batch: &Arc<Batch>, now: Instant) -> bool {
        // Asked before taking the locks, because with a shared pacer it is a
        // round trip to Redis.
        let retry_after = self
            .dispatcher
            .pacer()
            .backlog(now)
            .await
            .max(MIN_RETRY_AFTER);
        let mut open = lock(&self.open);
        let mut members = lock(&batch.members);
        let expired = members.take_expired(now);
        if !expired.is_empty() {
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
    use crate::backend::{ApiKey, Engine, RetryPolicy, Upstream};
    use crate::metrics::Metrics;
    use crate::scheduling::limiter::{Gcra, Limiter};
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
            span: Span::none(),
        };
        (member, answer)
    }

    /// Submits a call arriving now; keep the receiver alive or the call
    /// counts as abandoned.
    async fn submit(
        coalescer: &Arc<Coalescer>,
        request: PreparedRequest,
    ) -> Result<oneshot::Receiver<Outcome>, Saturated> {
        let (reply, answer) = oneshot::channel();
        let now = Instant::now();
        coalescer
            .submit(request, now, now + Duration::from_secs(2), reply)
            .await
            .map(|()| answer)
    }

    fn coalescer(limits: BatchLimits, upstream_rpm: f64) -> Arc<Coalescer> {
        let metrics = Arc::new(Metrics::new());
        let pacer = Arc::new(Limiter::Local(Gcra::per_minute(upstream_rpm, 1)));
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
            Limiter::Local(Gcra::per_second(1_000_000.0, 1_000_000)),
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
        let _a = submit(&coalescer, request("same", json!({"a": noul("A")})))
            .await
            .unwrap();
        let _b = submit(&coalescer, request("same", json!({"b": noul("B")})))
            .await
            .unwrap();
        let _c = submit(&coalescer, request("other", json!({"c": noul("C")})))
            .await
            .unwrap();
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
        let _a = submit(&coalescer, full).await.unwrap();
        let _b = submit(&coalescer, request("s", json!({"d": noul("D")})))
            .await
            .unwrap();
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
        let _a = submit(&coalescer, request("first", json!({"a": noul("A")})))
            .await
            .unwrap();
        let refused = submit(&coalescer, request("second", json!({"b": noul("B")})))
            .await
            .unwrap_err();
        assert!(
            refused.retry_after > Duration::from_millis(900),
            "{refused:?}"
        );
        // Joining an open batch needs no slot, so it still works.
        let _c = submit(&coalescer, request("first", json!({"c": noul("C")})))
            .await
            .unwrap();
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
        let _a = submit(&coalescer, request("same", json!({"a": noul("A")})))
            .await
            .unwrap();
        let _b = submit(&coalescer, request("same", json!({"b": noul("B")})))
            .await
            .unwrap();
        assert_eq!(coalescer.open_batches(), 0);
    }
}
