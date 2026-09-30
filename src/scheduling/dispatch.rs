//! Sending a sealed batch upstream and handing every caller its outcome.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, sleep_until, timeout_at};
use tracing::{debug, error, warn};

use crate::backend::{CircuitBreaker, Engine, UpstreamFailure, Verdict, describe};
use crate::error::GatewayError;
use crate::metrics::Metrics;
use crate::scheduling::batch::{Plan, SplitError};
use crate::scheduling::limiter::Gcra;
use crate::wire::{PreparedRequest, input_tokens};

/// Shortest retry-after the gateway suggests when it sheds a call.
pub const MIN_RETRY_AFTER: Duration = Duration::from_millis(500);

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One service call waiting for its answer.
pub struct Member {
    pub request: PreparedRequest,
    /// Latest instant the call may still leave for its backend. Past it, the
    /// caller is better served by a 429 and a retry-after than by a late send
    /// that burns tokens on an answer it may no longer wait for.
    pub latest_send: Instant,
    /// When the caller stops waiting for an answer.
    pub deadline: Instant,
    pub reply: oneshot::Sender<Outcome>,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Answered {
        body: Bytes,
        request_id: Option<String>,
        input_tokens: Option<u64>,
        /// Service calls answered by the same upstream call.
        batch_callers: usize,
    },
    /// An error the gateway answers itself.
    Failed(GatewayError),
    /// The backend could not serve the call: unreachable, erroring, or
    /// refusing the gateway's key. Holds what the caller gets if no fallback
    /// takes over. A call the backend refused for its own faults (a 400) or
    /// its rate (a 429) is not this: another backend would not do better.
    Unavailable(Box<Outcome>),
    /// An error status from the backend, handed back as it was sent.
    Rejected {
        status: StatusCode,
        body: Bytes,
        retry_after: Option<Duration>,
        request_id: Option<String>,
    },
}

impl Outcome {
    pub fn from_failure(backend: &str, failure: &UpstreamFailure) -> Self {
        let outcome = Self::describe_failure(backend, failure);
        if Verdict::of_failure(failure) == Verdict::Failed {
            Self::Unavailable(Box::new(outcome))
        } else {
            outcome
        }
    }

    /// Whether a fallback backend could do better than this outcome.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }

    fn describe_failure(backend: &str, failure: &UpstreamFailure) -> Self {
        match failure {
            UpstreamFailure::Status { status, .. } if *status == StatusCode::UNAUTHORIZED => {
                // The caller's key was fine; the gateway's own key is not.
                // Passing the 401 through would send the service chasing a
                // problem it does not have.
                Self::Failed(GatewayError::upstream(format!(
                    "backend {backend:?} refused the gateway's API key; the gateway's operator has to fix it"
                )))
            }
            UpstreamFailure::Status {
                status,
                body,
                retry_after,
                request_id,
            } => Self::Rejected {
                status: *status,
                body: body.clone(),
                retry_after: *retry_after,
                request_id: request_id.clone(),
            },
            UpstreamFailure::Transport {
                timed_out: true, ..
            } => Self::Failed(GatewayError::timeout(format!(
                "backend {backend:?} did not answer in time"
            ))),
            UpstreamFailure::Transport { detail, .. } => Self::Failed(GatewayError::upstream(
                format!("could not reach backend {backend:?}: {detail}"),
            )),
            UpstreamFailure::Paused { retry_after } => Self::Failed(GatewayError::rate_limited(
                format!("backend {backend:?} is over its limit for now"),
                *retry_after,
            )),
            UpstreamFailure::Deadline => Self::Failed(GatewayError::timeout(format!(
                "the call's deadline passed before backend {backend:?} could be reached"
            ))),
            UpstreamFailure::Unreadable(reason) => Self::Failed(GatewayError::upstream(format!(
                "backend {backend:?} answered something the gateway cannot read: {reason}"
            ))),
        }
    }
}

pub struct Dispatcher {
    /// The backend's name, for messages and logs.
    backend: String,
    engine: Engine,
    /// Upstream requests per minute, shared by every service.
    pacer: Arc<Gcra>,
    /// Upstream tokens per second, booked on estimates.
    token_pacer: Gcra,
    /// Upstream calls in flight. Tokio's semaphore is fair, so batches go
    /// out in the order they became ready.
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    /// Told how every upstream call went.
    breaker: Arc<CircuitBreaker>,
}

impl Dispatcher {
    pub fn new(
        backend: &str,
        engine: Engine,
        pacer: Arc<Gcra>,
        token_pacer: Gcra,
        max_concurrency: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            backend: backend.to_owned(),
            engine,
            pacer,
            token_pacer,
            permits: Arc::new(Semaphore::new(max_concurrency)),
            metrics,
            breaker: Arc::new(CircuitBreaker::disabled(backend)),
        }
    }

    /// Reports the outcome of every upstream call to `breaker`.
    pub fn with_breaker(mut self, breaker: Arc<CircuitBreaker>) -> Self {
        self.breaker = breaker;
        self
    }

    pub fn pacer(&self) -> &Gcra {
        &self.pacer
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Waits for a free upstream slot.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the permit semaphore is never closed")
    }

    /// Sends one sealed batch upstream and answers every caller in it.
    /// `opened` is when the batch received its first call.
    pub async fn dispatch(
        self: &Arc<Self>,
        members: Vec<Member>,
        permit: OwnedSemaphorePermit,
        opened: Instant,
    ) {
        // Callers who timed out or hung up are not worth paying for. Callers
        // whose latest send time passed were answered while the batch waited
        // for a connection.
        let mut live: Vec<Member> = members
            .into_iter()
            .filter(|member| !member.reply.is_closed())
            .collect();
        if live.is_empty() {
            // Nobody is left to answer: hand the request slot this batch
            // booked back to the others.
            self.pacer.adjust(-1);
            return;
        }
        let mut plan = Plan::new(&live.iter().map(|m| &m.request).collect::<Vec<_>>());

        // Tokens per second, booked on the estimate and corrected with the
        // real count once the backend answers. The batch waits for the budget
        // only as long as its most patient caller allows.
        let now = Instant::now();
        let most_patient = live
            .iter()
            .map(|m| m.latest_send)
            .max()
            .expect("at least one live member");
        let start = match self.token_pacer.try_book(
            now,
            u64::from(plan.estimated_tokens),
            most_patient.saturating_duration_since(now),
        ) {
            Ok(start) => start,
            Err(wait) => {
                self.pacer.adjust(-1);
                let outcome = Outcome::Failed(GatewayError::rate_limited(
                    "the shared tokens_per_second budget is spent for now",
                    wait.max(MIN_RETRY_AFTER),
                ));
                for member in live {
                    let _ = member.reply.send(outcome.clone());
                }
                return;
            }
        };
        if start > now {
            // Callers who cannot wait until `start` get their 429 now, and
            // the others go without them.
            let (patient, impatient): (Vec<_>, Vec<_>) =
                live.into_iter().partition(|m| m.latest_send >= start);
            if !impatient.is_empty() {
                let outcome = Outcome::Failed(GatewayError::rate_limited(
                    "the shared tokens_per_second budget is spent for now",
                    start.saturating_duration_since(now).max(MIN_RETRY_AFTER),
                ));
                for member in impatient {
                    let _ = member.reply.send(outcome.clone());
                }
                let smaller = Plan::new(&patient.iter().map(|m| &m.request).collect::<Vec<_>>());
                self.token_pacer
                    .adjust(i64::from(smaller.estimated_tokens) - i64::from(plan.estimated_tokens));
                plan = smaller;
            }
            live = patient;
            sleep_until(start).await;
        }

        self.metrics.queue_wait.observe(
            Instant::now()
                .saturating_duration_since(opened)
                .as_secs_f64(),
        );
        self.metrics.batch_callers.observe(plan.callers as f64);
        self.metrics.batch_questions.observe(plan.questions as f64);
        self.metrics
            .deduplicated_questions
            .inc_by(plan.deduplicated as u64);
        if self.engine.sends_state_once() {
            self.metrics.estimated_tokens_saved.inc_by(u64::from(
                plan.standalone_tokens.saturating_sub(plan.estimated_tokens),
            ));
        }

        // Serve the most patient caller: callers whose deadline passes first
        // get their 504 from the HTTP handler, the others still get answers.
        let deadline = live
            .iter()
            .map(|m| m.deadline)
            .max()
            .expect("at least one live member");
        let sent = Instant::now();
        let result = self.engine.execute(plan.body.clone(), deadline).await;
        drop(permit);
        // One upstream call is one verdict, however many callers it carried.
        self.breaker
            .record(sent, Instant::now(), Verdict::of(&result));

        match result {
            Ok(reply) => {
                if let Some(actual) = input_tokens(&reply.body) {
                    let actual = i64::try_from(actual).unwrap_or(i64::MAX);
                    self.token_pacer
                        .adjust(actual - i64::from(plan.estimated_tokens));
                }
                debug!(
                    backend = %self.backend,
                    callers = plan.callers,
                    questions = plan.questions,
                    deduplicated = plan.deduplicated,
                    upstream_ms = sent.elapsed().as_millis() as u64,
                    request_id = reply.request_id.as_deref().unwrap_or("-"),
                    "upstream call answered"
                );
                for (member, answer) in live.into_iter().zip(plan.split(&reply.body)) {
                    let outcome = match answer {
                        Ok(answer) => Outcome::Answered {
                            body: answer.body,
                            request_id: reply.request_id.clone(),
                            input_tokens: answer.input_tokens,
                            batch_callers: plan.callers,
                        },
                        Err(err) => {
                            error!(backend = %self.backend, error = ?err, "the answer could not be split");
                            Outcome::Failed(GatewayError::upstream(match err {
                                SplitError::Unreadable(reason) => format!(
                                    "backend {:?}'s response could not be read: {reason}",
                                    self.backend
                                ),
                                SplitError::MissingAnswer(id) => format!(
                                    "backend {:?}'s response has no answer for question {id:?}",
                                    self.backend
                                ),
                            }))
                        }
                    };
                    let _ = member.reply.send(outcome);
                }
            }
            Err(UpstreamFailure::Status { status, .. })
                if live.len() > 1
                    && matches!(
                        status,
                        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                    ) =>
            {
                // A question slipped past local validation and sank the whole
                // merged call. Replaying each call alone costs a few extra
                // requests, and the error then reaches only the caller whose
                // question caused it.
                warn!(
                    backend = %self.backend,
                    callers = live.len(),
                    status = status.as_u16(),
                    "the backend rejected a merged call; replaying each call on its own"
                );
                self.metrics.isolated_replays.inc_by(live.len() as u64);
                for member in live {
                    tokio::spawn(Arc::clone(self).replay_alone(member));
                }
            }
            Err(failure) => {
                match &failure {
                    UpstreamFailure::Status { status, .. }
                        if *status == StatusCode::UNAUTHORIZED =>
                    {
                        error!(backend = %self.backend, "the backend refused the gateway's API key");
                    }
                    _ => {
                        warn!(
                            backend = %self.backend,
                            callers = live.len(),
                            failure = %describe(&failure),
                            "upstream call failed"
                        )
                    }
                }
                let outcome = Outcome::from_failure(&self.backend, &failure);
                for member in live {
                    let _ = member.reply.send(outcome.clone());
                }
            }
        }
    }

    /// Sends a call on its own after the merged call it was in got rejected.
    /// Boxed because it recurses through `dispatch`.
    fn replay_alone(self: Arc<Self>, mut member: Member) -> BoxFuture {
        Box::pin(async move {
            // The call was admitted already, so only its own deadline bounds
            // the wait for a slot and a connection.
            member.latest_send = member.deadline;
            let now = Instant::now();
            let start = self.pacer.book(now, 1);
            if start >= member.deadline {
                self.pacer.adjust(-1);
                let _ = member
                    .reply
                    .send(Outcome::Failed(GatewayError::rate_limited(
                        "the backend's shared quota is booked past this call's deadline",
                        start.saturating_duration_since(now),
                    )));
                return;
            }
            sleep_until(start).await;
            let Ok(permit) = timeout_at(member.deadline, self.acquire()).await else {
                // The handler has answered 504 by now.
                self.pacer.adjust(-1);
                return;
            };
            self.dispatch(vec![member], permit, start).await;
        })
    }
}
