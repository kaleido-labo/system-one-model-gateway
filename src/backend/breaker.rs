//! The circuit breaker of one backend.
//!
//! A backend that is down makes every call wait through its retries, then
//! fail. The breaker learns that after a few failed calls in a row and turns
//! calls away at once, so callers get an answer (or a fallback, see
//! `Backends::chain`) in milliseconds instead of a timeout, and the backend
//! is left alone while it recovers.
//!
//! - **Closed**: calls go through. `circuit_breaker_failures` failed calls in a
//!   row open the breaker.
//! - **Open**: calls are turned away for `circuit_breaker_cooldown_ms`.
//! - **Half-open**: once the cool-down is over, one trial call goes through.
//!   If it gets an answer the breaker closes, if it fails the breaker opens
//!   again for another cool-down.
//!
//! What counts is one upstream call, the final outcome after its retries, and
//! not one caller: a merged call that fails for ten callers is one failure.
//! Nothing here sleeps or spawns. Every method takes the current instant, so
//! the state machine can be tested without a clock.

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use tokio::time::Instant;
use tracing::{info, warn};

use super::{UpstreamFailure, UpstreamReply};

/// Longest a call is told to wait while a trial call is in flight. The trial
/// is usually over sooner than a whole cool-down.
const TRIAL_RETRY_AFTER: Duration = Duration::from_secs(1);

/// What one upstream call says about the backend's health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The backend answered. That includes an answer that is a refusal of
    /// the call itself (400, 403, 422) or of its rate (429): the backend is
    /// up, and a 429 is for the pause mechanism to handle.
    Answered,
    /// The backend could not serve the call: no connection, an attempt that
    /// timed out, a 5xx (529 included), or a 401, which means the gateway's
    /// own key is refused.
    Failed,
    /// The call says nothing about the backend: it never started, or the
    /// answer was garbage that may be the call's own fault.
    Unknown,
}

impl Verdict {
    pub fn of(result: &Result<UpstreamReply, UpstreamFailure>) -> Self {
        match result {
            Ok(_) => Self::Answered,
            Err(failure) => Self::of_failure(failure),
        }
    }

    pub fn of_failure(failure: &UpstreamFailure) -> Self {
        match failure {
            UpstreamFailure::Status { status, .. } => {
                if status.is_server_error() || status.as_u16() == 401 {
                    Self::Failed
                } else {
                    Self::Answered
                }
            }
            UpstreamFailure::Transport { .. } => Self::Failed,
            UpstreamFailure::Paused { .. }
            | UpstreamFailure::Deadline
            | UpstreamFailure::Unreadable(_) => Self::Unknown,
        }
    }
}

/// The state of a breaker, as the `circuit_state` gauge reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Circuit {
    Closed = 0,
    HalfOpen = 1,
    Open = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Consecutive failed calls so far.
    Closed {
        failures: u32,
    },
    Open {
        until: Instant,
    },
    /// `trial` is when the trial call was let through, if one was. A trial
    /// that never reports back (its caller hung up before it was sent) must
    /// not block the breaker for good, so a trial older than a cool-down is
    /// replaced by a new one.
    HalfOpen {
        trial: Option<Instant>,
    },
}

struct Inner {
    state: State,
    /// When the state last moved between closed, open and half-open. A call
    /// sent before that is news from another era (it may have been in flight
    /// when the breaker opened) and is ignored.
    changed: Instant,
}

pub struct CircuitBreaker {
    /// The backend's name, for logs.
    backend: String,
    /// Failed calls in a row that open the breaker. 0 turns it off.
    threshold: u32,
    cooldown: Duration,
    inner: Mutex<Inner>,
    gauge: Gauge,
}

impl CircuitBreaker {
    pub fn new(backend: &str, threshold: u32, cooldown: Duration, gauge: Gauge) -> Self {
        gauge.set(Circuit::Closed as i64);
        Self {
            backend: backend.to_owned(),
            threshold,
            cooldown,
            inner: Mutex::new(Inner {
                state: State::Closed { failures: 0 },
                changed: Instant::now(),
            }),
            gauge,
        }
    }

    /// A breaker that never opens.
    pub fn disabled(backend: &str) -> Self {
        Self::new(backend, 0, Duration::ZERO, Gauge::default())
    }

    /// Lets a call through, or says how long to wait before asking again.
    /// The first call after the cool-down becomes the trial call.
    pub fn admit(&self, now: Instant) -> Result<(), Duration> {
        if self.threshold == 0 {
            return Ok(());
        }
        let mut inner = self.lock();
        match inner.state {
            State::Closed { .. } => Ok(()),
            State::Open { until } => {
                if now < until {
                    return Err(until - now);
                }
                self.move_to(&mut inner, State::HalfOpen { trial: Some(now) }, now);
                Ok(())
            }
            State::HalfOpen { trial } => {
                if let Some(since) = trial
                    && now < since + self.cooldown
                {
                    return Err((since + self.cooldown - now).min(TRIAL_RETRY_AFTER));
                }
                inner.state = State::HalfOpen { trial: Some(now) };
                Ok(())
            }
        }
    }

    /// Records the outcome of an upstream call that was sent at `sent`.
    pub fn record(&self, sent: Instant, now: Instant, verdict: Verdict) {
        if self.threshold == 0 {
            return;
        }
        let mut inner = self.lock();
        if sent < inner.changed {
            return;
        }
        match (inner.state, verdict) {
            (State::Closed { .. }, Verdict::Answered) => {
                inner.state = State::Closed { failures: 0 };
            }
            (State::Closed { failures }, Verdict::Failed) => {
                let failures = failures + 1;
                if failures >= self.threshold {
                    warn!(
                        backend = %self.backend,
                        failures,
                        cooldown_ms = self.cooldown.as_millis() as u64,
                        "circuit breaker opened: the backend failed every call lately"
                    );
                    self.move_to(&mut inner, self.open(now), now);
                } else {
                    inner.state = State::Closed { failures };
                }
            }
            (State::HalfOpen { .. }, Verdict::Answered) => {
                info!(backend = %self.backend, "circuit breaker closed: the trial call was answered");
                self.move_to(&mut inner, State::Closed { failures: 0 }, now);
            }
            (State::HalfOpen { .. }, Verdict::Failed) => {
                warn!(
                    backend = %self.backend,
                    cooldown_ms = self.cooldown.as_millis() as u64,
                    "circuit breaker opened again: the trial call failed"
                );
                self.move_to(&mut inner, self.open(now), now);
            }
            // The trial said nothing: let the next call be the trial.
            (State::HalfOpen { .. }, Verdict::Unknown) => {
                inner.state = State::HalfOpen { trial: None };
            }
            (State::Closed { .. }, Verdict::Unknown) | (State::Open { .. }, _) => {}
        }
    }

    #[cfg(test)]
    fn circuit(&self) -> Circuit {
        match self.lock().state {
            State::Closed { .. } => Circuit::Closed,
            State::HalfOpen { .. } => Circuit::HalfOpen,
            State::Open { .. } => Circuit::Open,
        }
    }

    fn open(&self, now: Instant) -> State {
        State::Open {
            until: now + self.cooldown,
        }
    }

    fn move_to(&self, inner: &mut Inner, state: State, now: Instant) {
        inner.state = state;
        inner.changed = now;
        self.gauge.set(match state {
            State::Closed { .. } => Circuit::Closed,
            State::HalfOpen { .. } => Circuit::HalfOpen,
            State::Open { .. } => Circuit::Open,
        } as i64);
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // The state is a value and an instant; a panic elsewhere cannot
        // leave it torn.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use bytes::Bytes;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Opens after 3 failures, for a 1000 ms cool-down.
    fn breaker() -> (CircuitBreaker, Gauge) {
        let gauge = Gauge::default();
        (CircuitBreaker::new("b", 3, ms(1_000), gauge.clone()), gauge)
    }

    fn fail(breaker: &CircuitBreaker, at: Instant) {
        breaker.record(at, at, Verdict::Failed);
    }

    #[test]
    fn enough_failures_in_a_row_open_the_breaker() {
        let (breaker, gauge) = breaker();
        let t0 = Instant::now();
        fail(&breaker, t0);
        fail(&breaker, t0);
        assert_eq!(breaker.circuit(), Circuit::Closed);
        assert_eq!(breaker.admit(t0), Ok(()));
        fail(&breaker, t0);
        assert_eq!(breaker.circuit(), Circuit::Open);
        assert_eq!(gauge.get(), 2);
        // Turned away, told how long is left.
        assert_eq!(breaker.admit(t0 + ms(400)), Err(ms(600)));
    }

    #[test]
    fn an_answer_resets_the_count() {
        let (breaker, _) = breaker();
        let t0 = Instant::now();
        fail(&breaker, t0);
        fail(&breaker, t0);
        breaker.record(t0, t0, Verdict::Answered);
        fail(&breaker, t0);
        fail(&breaker, t0);
        assert_eq!(breaker.circuit(), Circuit::Closed);
        fail(&breaker, t0);
        assert_eq!(breaker.circuit(), Circuit::Open);
    }

    #[test]
    fn calls_that_say_nothing_change_nothing() {
        let (breaker, _) = breaker();
        let t0 = Instant::now();
        fail(&breaker, t0);
        fail(&breaker, t0);
        breaker.record(t0, t0, Verdict::Unknown);
        fail(&breaker, t0);
        assert_eq!(breaker.circuit(), Circuit::Open);
    }

    #[test]
    fn after_the_cool_down_one_trial_goes_through() {
        let (breaker, gauge) = breaker();
        let t0 = Instant::now();
        (0..3).for_each(|_| fail(&breaker, t0));
        assert_eq!(breaker.admit(t0 + ms(999)), Err(ms(1)));

        let t1 = t0 + ms(1_000);
        assert_eq!(breaker.admit(t1), Ok(()));
        assert_eq!(breaker.circuit(), Circuit::HalfOpen);
        assert_eq!(gauge.get(), 1);
        // Everyone else waits for the trial, for less than a second.
        assert_eq!(breaker.admit(t1 + ms(300)), Err(ms(700)));
        assert_eq!(breaker.admit(t1 + ms(1)), Err(ms(999)));
    }

    #[test]
    fn a_call_waiting_for_the_trial_is_never_told_to_wait_long() {
        let breaker = CircuitBreaker::new("b", 1, Duration::from_secs(30), Gauge::default());
        let t0 = Instant::now();
        fail(&breaker, t0);
        let t1 = t0 + Duration::from_secs(30);
        breaker.admit(t1).unwrap();
        assert_eq!(breaker.admit(t1 + ms(200)), Err(TRIAL_RETRY_AFTER));
    }

    #[test]
    fn a_trial_that_is_answered_closes_the_breaker() {
        let (breaker, gauge) = breaker();
        let t0 = Instant::now();
        (0..3).for_each(|_| fail(&breaker, t0));
        let t1 = t0 + ms(1_000);
        breaker.admit(t1).unwrap();
        breaker.record(t1 + ms(10), t1 + ms(50), Verdict::Answered);
        assert_eq!(breaker.circuit(), Circuit::Closed);
        assert_eq!(gauge.get(), 0);
        assert_eq!(breaker.admit(t1 + ms(60)), Ok(()));
        // The count starts from zero.
        fail(&breaker, t1 + ms(60));
        fail(&breaker, t1 + ms(60));
        assert_eq!(breaker.circuit(), Circuit::Closed);
    }

    #[test]
    fn a_trial_that_fails_opens_the_breaker_for_another_cool_down() {
        let (breaker, gauge) = breaker();
        let t0 = Instant::now();
        (0..3).for_each(|_| fail(&breaker, t0));
        let t1 = t0 + ms(1_000);
        breaker.admit(t1).unwrap();
        breaker.record(t1, t1 + ms(200), Verdict::Failed);
        assert_eq!(breaker.circuit(), Circuit::Open);
        assert_eq!(gauge.get(), 2);
        assert_eq!(breaker.admit(t1 + ms(201)), Err(ms(999)));
        assert_eq!(breaker.admit(t1 + ms(1_200)), Ok(()));
        assert_eq!(breaker.circuit(), Circuit::HalfOpen);
    }

    #[test]
    fn a_trial_that_says_nothing_lets_the_next_call_try() {
        let (breaker, _) = breaker();
        let t0 = Instant::now();
        (0..3).for_each(|_| fail(&breaker, t0));
        let t1 = t0 + ms(1_000);
        breaker.admit(t1).unwrap();
        breaker.record(t1, t1 + ms(5), Verdict::Unknown);
        assert_eq!(breaker.circuit(), Circuit::HalfOpen);
        assert_eq!(breaker.admit(t1 + ms(6)), Ok(()));
    }

    #[test]
    fn a_trial_that_never_reports_does_not_block_the_breaker_for_good() {
        let (breaker, _) = breaker();
        let t0 = Instant::now();
        (0..3).for_each(|_| fail(&breaker, t0));
        let t1 = t0 + ms(1_000);
        breaker.admit(t1).unwrap();
        // The trial's caller hung up: nothing is ever recorded. A cool-down
        // later, another call takes over.
        assert!(breaker.admit(t1 + ms(999)).is_err());
        assert_eq!(breaker.admit(t1 + ms(1_000)), Ok(()));
    }

    #[test]
    fn calls_sent_before_the_last_change_are_ignored() {
        let (breaker, _) = breaker();
        let t0 = Instant::now();
        // Three calls were in flight when the breaker opened at t0 + 10.
        (0..3).for_each(|_| fail(&breaker, t0 + ms(10)));
        // They report back late, during the half-open trial. They are not it.
        let t1 = t0 + ms(1_010);
        breaker.admit(t1).unwrap();
        breaker.record(t0 + ms(5), t1 + ms(1), Verdict::Answered);
        assert_eq!(breaker.circuit(), Circuit::HalfOpen);
        breaker.record(t0 + ms(6), t1 + ms(2), Verdict::Failed);
        assert_eq!(breaker.circuit(), Circuit::HalfOpen);
        // The real trial fails: open again, and an answer that arrives while
        // open changes nothing.
        let t2 = t1 + ms(1);
        breaker.record(t2, t2, Verdict::Failed);
        assert_eq!(breaker.circuit(), Circuit::Open);
        breaker.record(t2, t2, Verdict::Answered);
        assert_eq!(breaker.circuit(), Circuit::Open);
    }

    #[test]
    fn zero_failures_turns_the_breaker_off() {
        let breaker = CircuitBreaker::new("b", 0, ms(1_000), Gauge::default());
        let t0 = Instant::now();
        (0..100).for_each(|_| fail(&breaker, t0));
        assert_eq!(breaker.circuit(), Circuit::Closed);
        assert_eq!(breaker.admit(t0), Ok(()));
        assert_eq!(CircuitBreaker::disabled("b").admit(t0), Ok(()));
    }

    #[test]
    fn only_outages_count_as_failures() {
        let status = |code: u16| UpstreamFailure::Status {
            status: StatusCode::from_u16(code).unwrap(),
            body: Bytes::new(),
            retry_after: None,
            request_id: None,
        };
        for code in [500, 502, 503, 504, 529, 401] {
            assert_eq!(
                Verdict::of_failure(&status(code)),
                Verdict::Failed,
                "{code}"
            );
        }
        // A 429 is for the pause mechanism, and a client error is the call's
        // own fault: the backend answered both.
        for code in [429, 400, 403, 404, 422] {
            assert_eq!(
                Verdict::of_failure(&status(code)),
                Verdict::Answered,
                "{code}"
            );
        }
        assert_eq!(
            Verdict::of_failure(&UpstreamFailure::Transport {
                timed_out: true,
                detail: String::new()
            }),
            Verdict::Failed
        );
        for failure in [
            UpstreamFailure::Deadline,
            UpstreamFailure::Paused {
                retry_after: ms(10),
            },
            UpstreamFailure::Unreadable("junk".to_owned()),
        ] {
            assert_eq!(Verdict::of_failure(&failure), Verdict::Unknown);
        }
        let reply = UpstreamReply {
            body: Bytes::new(),
            request_id: None,
        };
        assert_eq!(Verdict::of(&Ok(reply)), Verdict::Answered);
    }
}
