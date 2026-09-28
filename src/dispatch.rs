//! Sending a sealed batch upstream and handing every caller its outcome.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, sleep_until};
use tracing::{debug, error, warn};

use crate::batch::{Plan, SplitError};
use crate::error::GatewayError;
use crate::limiter::Gcra;
use crate::metrics::Metrics;
use crate::protocol::{PreparedRequest, input_tokens};
use crate::upstream::{Upstream, UpstreamFailure, describe};

/// Shortest retry-after the gateway suggests when it sheds a call.
const MIN_RETRY_AFTER: Duration = Duration::from_millis(500);

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One service call waiting for its answer.
pub struct Member {
    pub request: PreparedRequest,
    /// Latest instant the call may still leave for TypeSafe. Past it, the
    /// caller is better served by a 429 and a retry-after than by a late send.
    pub queue_deadline: Instant,
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
    /// An error status from TypeSafe, handed back as the vendor sent it.
    Rejected {
        status: StatusCode,
        body: Bytes,
        retry_after: Option<Duration>,
        request_id: Option<String>,
    },
}

impl Outcome {
    pub fn from_failure(failure: &UpstreamFailure) -> Self {
        match failure {
            UpstreamFailure::Status { status, .. }
                if matches!(*status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) =>
            {
                // The caller's key was fine; the gateway's own key is not.
                // Passing the 401 through would send the service chasing a
                // problem it does not have.
                Self::Failed(GatewayError::upstream(
                    "TypeSafe refused the gateway's API key; the gateway's operator has to fix it",
                ))
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
            } => Self::Failed(GatewayError::timeout("TypeSafe did not answer in time")),
            UpstreamFailure::Transport { detail, .. } => Self::Failed(GatewayError::upstream(
                format!("could not reach TypeSafe: {detail}"),
            )),
            UpstreamFailure::Paused { retry_after } => Self::Failed(GatewayError::rate_limited(
                "TypeSafe asked the gateway to slow down",
                *retry_after,
            )),
            UpstreamFailure::Deadline => Self::Failed(GatewayError::timeout(
                "the call's deadline passed before TypeSafe could be reached",
            )),
        }
    }
}

pub struct Dispatcher {
    upstream: Arc<Upstream>,
    /// Upstream requests per minute, shared by every service.
    pacer: Arc<Gcra>,
    /// Upstream tokens per second, booked on estimates.
    token_pacer: Gcra,
    /// Upstream calls in flight. Tokio's semaphore is fair, so batches go
    /// out in the order they became ready.
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl Dispatcher {
    pub fn new(
        upstream: Arc<Upstream>,
        pacer: Arc<Gcra>,
        token_pacer: Gcra,
        max_concurrency: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            upstream,
            pacer,
            token_pacer,
            permits: Arc::new(Semaphore::new(max_concurrency)),
            metrics,
        }
    }

    pub fn pacer(&self) -> &Gcra {
        &self.pacer
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
        let now = Instant::now();
        let mut live = Vec::with_capacity(members.len());
        for member in members {
            if member.reply.is_closed() {
                // The caller timed out or hung up; don't pay for its questions.
                continue;
            }
            if now > member.queue_deadline {
                let retry_after = self.pacer.backlog(now).max(MIN_RETRY_AFTER);
                let _ = member
                    .reply
                    .send(Outcome::Failed(GatewayError::rate_limited(
                        "every upstream slot stayed busy past max_queue_wait_ms",
                        retry_after,
                    )));
                continue;
            }
            live.push(member);
        }
        if live.is_empty() {
            // Nobody is left to answer: hand the request slot this batch
            // booked back to the others.
            self.pacer.adjust(-1);
            return;
        }

        let plan = Plan::new(&live.iter().map(|m| &m.request).collect::<Vec<_>>());
        self.metrics
            .queue_wait
            .observe(now.saturating_duration_since(opened).as_secs_f64());
        self.metrics.batch_callers.observe(plan.callers as f64);
        self.metrics.batch_questions.observe(plan.questions as f64);
        self.metrics
            .deduplicated_questions
            .inc_by(plan.deduplicated as u64);
        self.metrics.estimated_tokens_saved.inc_by(u64::from(
            plan.standalone_tokens.saturating_sub(plan.estimated_tokens),
        ));

        // Serve the most patient caller: callers whose deadline passes first
        // get their 504 from the HTTP handler, the others still get answers.
        let deadline = live
            .iter()
            .map(|m| m.deadline)
            .max()
            .expect("at least one live member");

        let estimated = u64::from(plan.estimated_tokens);
        let start = self.token_pacer.book(Instant::now(), estimated);
        if start >= deadline {
            // The tokens-per-second budget is spent past every caller's
            // deadline. A 429 lets the callers' SDKs come back when it refills.
            self.token_pacer.adjust(-i64::from(plan.estimated_tokens));
            self.pacer.adjust(-1);
            let outcome = Outcome::Failed(GatewayError::rate_limited(
                "the shared tokens_per_second budget is spent for now",
                start.saturating_duration_since(Instant::now()),
            ));
            for member in live {
                let _ = member.reply.send(outcome.clone());
            }
            return;
        }
        sleep_until(start).await;

        let sent = Instant::now();
        let result = self.upstream.systemone(plan.body.clone(), deadline).await;
        drop(permit);

        match result {
            Ok(reply) => {
                if let Some(actual) = input_tokens(&reply.body) {
                    let actual = i64::try_from(actual).unwrap_or(i64::MAX);
                    self.token_pacer
                        .adjust(actual - i64::from(plan.estimated_tokens));
                }
                debug!(
                    callers = plan.callers,
                    questions = plan.questions,
                    deduplicated = plan.deduplicated,
                    upstream_ms = sent.elapsed().as_millis() as u64,
                    request_id = reply.request_id.as_deref().unwrap_or("-"),
                    "TypeSafe call answered"
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
                            error!(error = ?err, "TypeSafe's answer could not be split");
                            Outcome::Failed(GatewayError::upstream(match err {
                                SplitError::Unreadable(reason) => {
                                    format!("TypeSafe's response could not be read: {reason}")
                                }
                                SplitError::MissingAnswer(id) => {
                                    format!("TypeSafe's response has no answer for question {id:?}")
                                }
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
                    callers = live.len(),
                    status = status.as_u16(),
                    "TypeSafe rejected a merged call; replaying each call on its own"
                );
                self.metrics.isolated_replays.inc_by(live.len() as u64);
                for member in live {
                    tokio::spawn(Arc::clone(self).replay_alone(member));
                }
            }
            Err(failure) => {
                match &failure {
                    UpstreamFailure::Status { status, .. }
                        if matches!(*status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) =>
                    {
                        error!(
                            status = status.as_u16(),
                            "TypeSafe refused the gateway's API key"
                        );
                    }
                    _ => {
                        warn!(callers = live.len(), failure = %describe(&failure), "TypeSafe call failed")
                    }
                }
                let outcome = Outcome::from_failure(&failure);
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
            // The call was admitted already: book a slot however far it is,
            // and let the caller's own deadline decide.
            member.queue_deadline = member.deadline;
            let start = self.pacer.book(Instant::now(), 1);
            sleep_until(start).await;
            let permit = self.acquire().await;
            self.dispatch(vec![member], permit, Instant::now()).await;
        })
    }
}
