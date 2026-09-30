//! The additive-increase, multiplicative-decrease (AIMD) rule behind a
//! backend's adaptive request rate.
//!
//! A backend's `requests_per_minute` is what the operator believes the
//! provider allows, and providers move their limits without notice. With
//! `adaptive_rate` on, the gateway treats that number as a ceiling and finds
//! the real limit the way TCP finds a link's capacity: a 429 multiplies the
//! rate by `adaptive_decrease`, and every `adaptive_recovery_ms` without one
//! adds `adaptive_increase_per_minute` back, up to the ceiling.
//!
//! This file is the rule alone. It holds a rate and two instants, and it neither
//! sleeps nor touches a limiter: every method takes the current instant, so the
//! rule can be tested without a clock. `backend::adaptive` applies its answers
//! to the pacer, the metrics and the logs.
//!
//! **One decrease per episode.** One overload makes many 429s at once: every
//! batch and retry already in flight hits the provider's limit before the first
//! answer comes back. Dividing the rate once for each of them would send it to
//! the floor on a single hiccup. So a 429 lowers the rate only when at least
//! one recovery interval has passed since the last decrease, and the 429s that
//! arrive in between belong to the same episode: they lower nothing. They do
//! restart the recovery clock, because the provider is still saying no. If 429s
//! go on for longer than one interval at the lowered rate, that rate is still
//! too high, and the next one lowers it again.

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::Instant;

/// What the rule needs to know. Rates are in requests per minute.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Params {
    /// The configured `requests_per_minute`: never exceeded.
    pub ceiling: f64,
    /// The rate never goes below it, whatever the provider says.
    pub floor: f64,
    /// What a decrease multiplies the rate by, between 0 and 1 exclusive.
    pub decrease: f64,
    /// What each recovery step adds.
    pub step: f64,
    /// How long without a 429 before each step, and the length of an episode.
    pub recovery: Duration,
}

/// A change of rate, for the caller to apply and report.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Change {
    pub from: f64,
    pub to: f64,
}

impl Change {
    pub fn is_decrease(&self) -> bool {
        self.to < self.from
    }
}

pub struct Aimd {
    params: Params,
    state: Mutex<State>,
}

struct State {
    rate: f64,
    /// When the rate was last lowered. Starts the current episode.
    last_decrease: Option<Instant>,
    /// The last 429, or the last recovery step if that came later. The next
    /// step is due one recovery interval after it.
    last_event: Instant,
}

impl Aimd {
    /// A rule at its ceiling as of `now`.
    pub fn new(params: Params, now: Instant) -> Self {
        assert!(
            params.floor > 0.0 && params.floor <= params.ceiling,
            "the floor must be positive and at most the ceiling"
        );
        assert!(
            params.decrease > 0.0 && params.decrease < 1.0,
            "the decrease factor must be between 0 and 1"
        );
        assert!(params.step > 0.0, "the increase step must be positive");
        assert!(
            !params.recovery.is_zero(),
            "the recovery time must be positive"
        );
        Self {
            params,
            state: Mutex::new(State {
                rate: params.ceiling,
                last_decrease: None,
                last_event: now,
            }),
        }
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    /// The current rate, in requests per minute.
    pub fn rate(&self) -> f64 {
        self.lock().rate
    }

    /// The provider answered 429 at `now`. Returns the new rate when this 429
    /// starts an episode and there is room to lower the rate, and nothing when
    /// it belongs to an episode already counted or the rate is at its floor.
    pub fn on_rate_limited(&self, now: Instant) -> Option<Change> {
        let mut state = self.lock();
        // Any 429, counted or not, says the provider is not ready for more.
        state.last_event = now;
        let in_episode = state
            .last_decrease
            .is_some_and(|at| now.saturating_duration_since(at) < self.params.recovery);
        if in_episode {
            return None;
        }
        let from = state.rate;
        let to = (from * self.params.decrease).max(self.params.floor);
        if to >= from {
            return None;
        }
        state.rate = to;
        state.last_decrease = Some(now);
        Some(Change { from, to })
    }

    /// Adds back the steps that have fallen due by `now`: one for each
    /// recovery interval since the last 429 or step, up to the ceiling.
    pub fn recover(&self, now: Instant) -> Option<Change> {
        let mut state = self.lock();
        let from = state.rate;
        while state.rate < self.params.ceiling
            && now.saturating_duration_since(state.last_event) >= self.params.recovery
        {
            state.rate = (state.rate + self.params.step).min(self.params.ceiling);
            // Steps are spaced from each other, not from when this was called,
            // so a late caller does not slow the recovery down.
            state.last_event += self.params.recovery;
        }
        (state.rate > from).then_some(Change {
            from,
            to: state.rate,
        })
    }

    /// When the next step is due, or `None` at the ceiling.
    pub fn next_step(&self) -> Option<Instant> {
        let state = self.lock();
        (state.rate < self.params.ceiling).then(|| state.last_event + self.params.recovery)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A rate and two instants; a panic elsewhere cannot leave them torn.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// 1200 requests per minute at most, 120 at least, halved by each
    /// episode, 60 more back after every 10 s without a 429.
    fn rule(t0: Instant) -> Aimd {
        Aimd::new(
            Params {
                ceiling: 1200.0,
                floor: 120.0,
                decrease: 0.5,
                step: 60.0,
                recovery: secs(10),
            },
            t0,
        )
    }

    #[test]
    fn it_starts_at_the_ceiling_with_nothing_to_recover() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        assert_eq!(aimd.rate(), 1200.0);
        assert_eq!(aimd.next_step(), None);
        assert_eq!(aimd.recover(t0 + secs(3600)), None);
    }

    #[test]
    fn a_429_lowers_the_rate_by_the_factor() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        assert_eq!(
            aimd.on_rate_limited(t0),
            Some(Change {
                from: 1200.0,
                to: 600.0
            })
        );
        assert_eq!(aimd.rate(), 600.0);
    }

    #[test]
    fn the_429s_of_one_episode_lower_the_rate_once() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        assert!(aimd.on_rate_limited(t0).is_some());
        // Fifty batches and retries answered 429 in the same second.
        for millis in 0..50 {
            let at = t0 + Duration::from_millis(millis * 20);
            assert_eq!(aimd.on_rate_limited(at), None);
        }
        assert_eq!(aimd.rate(), 600.0);
    }

    #[test]
    fn a_429_after_the_episode_lowers_the_rate_again() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        // Still inside the interval one tick before it ends...
        assert_eq!(
            aimd.on_rate_limited(t0 + secs(10) - Duration::from_millis(1)),
            None
        );
        // ...and a new episode when it is over: the lowered rate is still too high.
        assert_eq!(
            aimd.on_rate_limited(t0 + secs(10)),
            Some(Change {
                from: 600.0,
                to: 300.0
            })
        );
    }

    #[test]
    fn the_rate_never_goes_below_the_floor() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        let mut at = t0;
        let mut rates = Vec::new();
        for _ in 0..6 {
            aimd.on_rate_limited(at);
            rates.push(aimd.rate());
            at += secs(10);
        }
        // 1200 -> 600 -> 300 -> 150 -> floor, and then nothing more to lower.
        assert_eq!(rates, [600.0, 300.0, 150.0, 120.0, 120.0, 120.0]);
        assert_eq!(aimd.on_rate_limited(at), None);
    }

    #[test]
    fn recovery_adds_one_step_per_quiet_interval() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        assert_eq!(aimd.next_step(), Some(t0 + secs(10)));
        assert_eq!(aimd.recover(t0 + secs(9)), None);
        assert_eq!(
            aimd.recover(t0 + secs(10)),
            Some(Change {
                from: 600.0,
                to: 660.0
            })
        );
        assert_eq!(aimd.next_step(), Some(t0 + secs(20)));
        // Nothing more until the next interval is over.
        assert_eq!(aimd.recover(t0 + secs(19)), None);
        assert_eq!(aimd.recover(t0 + secs(20)).map(|c| c.to), Some(720.0));
    }

    #[test]
    fn a_late_call_catches_up_on_every_step_that_fell_due() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        // 35 s without a 429: the steps of 10, 20 and 30 s, not of 35.
        assert_eq!(
            aimd.recover(t0 + secs(35)),
            Some(Change {
                from: 600.0,
                to: 780.0
            })
        );
        assert_eq!(aimd.next_step(), Some(t0 + secs(40)));
    }

    #[test]
    fn recovery_stops_at_the_ceiling() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        // 600 -> 1200 takes ten steps; a day of quiet does not go further.
        let change = aimd.recover(t0 + secs(86_400)).unwrap();
        assert_eq!(change.to, 1200.0);
        assert_eq!(aimd.rate(), 1200.0);
        assert_eq!(aimd.next_step(), None);
        assert_eq!(aimd.recover(t0 + secs(90_000)), None);
    }

    #[test]
    fn the_last_step_is_cut_at_the_ceiling() {
        let t0 = Instant::now();
        let aimd = Aimd::new(
            Params {
                ceiling: 100.0,
                floor: 10.0,
                decrease: 0.9,
                step: 25.0,
                recovery: secs(1),
            },
            t0,
        );
        aimd.on_rate_limited(t0);
        assert_eq!(aimd.rate(), 90.0);
        assert_eq!(aimd.recover(t0 + secs(1)).map(|c| c.to), Some(100.0));
    }

    #[test]
    fn a_429_restarts_the_recovery_clock_even_inside_an_episode() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        // The provider is still refusing at 8 s: no decrease, but no recovery at 10 s.
        assert_eq!(aimd.on_rate_limited(t0 + secs(8)), None);
        assert_eq!(aimd.recover(t0 + secs(10)), None);
        assert_eq!(aimd.next_step(), Some(t0 + secs(18)));
        assert_eq!(aimd.recover(t0 + secs(18)).map(|c| c.to), Some(660.0));
    }

    #[test]
    fn a_429_during_recovery_lowers_the_rate_from_where_it_had_got_to() {
        let t0 = Instant::now();
        let aimd = rule(t0);
        aimd.on_rate_limited(t0);
        aimd.recover(t0 + secs(30));
        assert_eq!(aimd.rate(), 780.0);
        assert_eq!(
            aimd.on_rate_limited(t0 + secs(31)),
            Some(Change {
                from: 780.0,
                to: 390.0
            })
        );
        assert_eq!(aimd.next_step(), Some(t0 + secs(41)));
    }

    #[test]
    fn a_rate_at_the_floor_is_not_a_decrease() {
        let t0 = Instant::now();
        let aimd = Aimd::new(
            Params {
                ceiling: 100.0,
                floor: 100.0,
                decrease: 0.5,
                step: 10.0,
                recovery: secs(1),
            },
            t0,
        );
        assert_eq!(aimd.on_rate_limited(t0), None);
        assert_eq!(aimd.rate(), 100.0);
    }
}
