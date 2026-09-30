//! Pacing against rate limits with the generic cell rate algorithm (GCRA).
//!
//! One type covers the three limits the gateway enforces: upstream requests
//! per minute, upstream tokens per second, and each service's own requests
//! per minute. Booking never sleeps. It returns the instant the caller may
//! start, and the caller decides whether to wait, to join a batch that
//! already holds a slot, or to answer `429` with that instant as retry-after.

use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::Instant;

pub struct Gcra {
    /// Time one unit of cost occupies at the sustained rate.
    emission: Duration,
    /// Units that may go back to back before pacing starts.
    burst: u64,
    state: Mutex<State>,
}

struct State {
    /// Theoretical arrival time: when the bucket would be full again.
    tat: Instant,
    /// Set after the vendor answered 429; nothing may start before it.
    paused_until: Option<Instant>,
}

impl Gcra {
    /// Allows `per_second` units per second, with bursts of up to `burst`.
    pub fn per_second(per_second: f64, burst: u64) -> Self {
        assert!(per_second > 0.0, "rate must be positive");
        Self {
            emission: Duration::from_secs_f64(1.0 / per_second),
            burst: burst.max(1),
            state: Mutex::new(State {
                tat: Instant::now(),
                paused_until: None,
            }),
        }
    }

    pub fn per_minute(per_minute: f64, burst: u64) -> Self {
        Self::per_second(per_minute / 60.0, burst)
    }

    /// Books `cost` units if they can start within `max_wait`, and returns
    /// when they start. Otherwise books nothing and returns how long the
    /// caller would have had to wait.
    pub fn try_book(
        &self,
        now: Instant,
        cost: u64,
        max_wait: Duration,
    ) -> Result<Instant, Duration> {
        let mut state = self.lock();
        let start = self.start_for(&state, now, cost);
        let wait = start.saturating_duration_since(now);
        if wait > max_wait {
            return Err(wait);
        }
        self.commit(&mut state, start, cost);
        Ok(start)
    }

    /// Books `cost` units however long they have to wait.
    pub fn book(&self, now: Instant, cost: u64) -> Instant {
        let mut state = self.lock();
        let start = self.start_for(&state, now, cost);
        self.commit(&mut state, start, cost);
        start
    }

    /// Corrects an earlier booking once the real cost is known: a positive
    /// `delta` charges more units, a negative one gives units back.
    pub fn adjust(&self, delta: i64) {
        let mut state = self.lock();
        let shift = self.emission_for(delta.unsigned_abs());
        state.tat = if delta >= 0 {
            state.tat + shift
        } else {
            state.tat.checked_sub(shift).unwrap_or(state.tat)
        };
    }

    /// Holds every start until `until`, then resumes without a burst. Called
    /// when the vendor answers 429: its limits move without notice, and every
    /// batch backing off together beats each one finding out on its own.
    pub fn pause_until(&self, until: Instant) {
        let mut state = self.lock();
        if state.paused_until.is_none_or(|current| current < until) {
            state.paused_until = Some(until);
        }
        let capacity = self.emission_for(self.burst);
        let no_burst = until + capacity.saturating_sub(self.emission);
        state.tat = state.tat.max(no_burst);
    }

    /// When a pause set by `pause_until` ends, if one is still running.
    pub fn resume_at(&self, now: Instant) -> Option<Instant> {
        self.lock().paused_until.filter(|until| *until > now)
    }

    /// How long a one-unit booking made now would wait.
    pub fn backlog(&self, now: Instant) -> Duration {
        let state = self.lock();
        self.start_for(&state, now, 1)
            .saturating_duration_since(now)
    }

    /// The earliest start for `cost` units: once the bucket has room for
    /// them, and never during a pause. A cost above the burst waits for a
    /// full bucket and then overdraws it.
    fn start_for(&self, state: &State, now: Instant, cost: u64) -> Instant {
        let capacity = self.emission_for(self.burst);
        let room_needed = self.emission_for(cost.min(self.burst));
        let start = (state.tat + room_needed)
            .checked_sub(capacity)
            .map_or(now, |start| start.max(now));
        match state.paused_until {
            Some(until) => start.max(until),
            None => start,
        }
    }

    fn commit(&self, state: &mut State, start: Instant, cost: u64) {
        state.tat = state.tat.max(start) + self.emission_for(cost);
    }

    fn emission_for(&self, units: u64) -> Duration {
        let nanos = self.emission.as_nanos().saturating_mul(u128::from(units));
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // The state is two instants; a panic elsewhere cannot leave it torn.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn a_burst_goes_at_once_then_units_are_spaced() {
        let gcra = Gcra::per_second(20.0, 3);
        let t0 = Instant::now();
        assert_eq!(gcra.book(t0, 1), t0);
        assert_eq!(gcra.book(t0, 1), t0);
        assert_eq!(gcra.book(t0, 1), t0);
        assert_eq!(gcra.book(t0, 1), t0 + ms(50));
        assert_eq!(gcra.book(t0, 1), t0 + ms(100));
    }

    #[test]
    fn idle_time_refills_the_bucket() {
        let gcra = Gcra::per_second(20.0, 2);
        let t0 = Instant::now();
        gcra.book(t0, 1);
        gcra.book(t0, 1);
        let later = t0 + Duration::from_secs(5);
        assert_eq!(gcra.book(later, 1), later);
        assert_eq!(gcra.book(later, 1), later);
        assert_eq!(gcra.book(later, 1), later + ms(50));
    }

    #[test]
    fn try_book_refuses_without_booking() {
        let gcra = Gcra::per_second(10.0, 1);
        let t0 = Instant::now();
        assert_eq!(gcra.try_book(t0, 1, Duration::ZERO), Ok(t0));
        assert_eq!(gcra.try_book(t0, 1, ms(50)), Err(ms(100)));
        // The refused call left no trace: the next slot is still 100 ms away.
        assert_eq!(gcra.try_book(t0, 1, ms(100)), Ok(t0 + ms(100)));
    }

    #[test]
    fn weighted_costs_consume_proportionally() {
        // 1000 units per second, bursts of 500.
        let gcra = Gcra::per_second(1000.0, 500);
        let t0 = Instant::now();
        assert_eq!(gcra.book(t0, 400), t0);
        // 100 units left in the bucket; 300 more need 200 ms of refill.
        assert_eq!(gcra.book(t0, 300), t0 + ms(200));
    }

    #[test]
    fn a_cost_above_the_burst_waits_for_a_full_bucket() {
        let gcra = Gcra::per_second(1000.0, 100);
        let t0 = Instant::now();
        assert_eq!(gcra.book(t0, 50), t0);
        // The bucket is full again 50 ms later; then 250 units overdraw it,
        // and a single unit waits for the debt to fall back under the burst.
        assert_eq!(gcra.book(t0, 250), t0 + ms(50));
        assert_eq!(gcra.backlog(t0), ms(201));
    }

    #[test]
    fn adjust_charges_or_refunds_units() {
        let gcra = Gcra::per_second(1000.0, 1);
        let t0 = Instant::now();
        gcra.book(t0, 1);
        gcra.adjust(99);
        assert_eq!(gcra.backlog(t0), ms(100));
        gcra.adjust(-50);
        assert_eq!(gcra.backlog(t0), ms(50));
    }

    #[test]
    fn a_pause_holds_starts_and_removes_the_burst() {
        let gcra = Gcra::per_second(20.0, 5);
        let t0 = Instant::now();
        gcra.pause_until(t0 + ms(500));
        assert_eq!(gcra.resume_at(t0), Some(t0 + ms(500)));
        assert_eq!(gcra.book(t0, 1), t0 + ms(500));
        assert_eq!(gcra.book(t0, 1), t0 + ms(550));
        assert_eq!(gcra.resume_at(t0 + ms(600)), None);
    }

    #[test]
    fn per_minute_matches_the_upstream_limit() {
        let gcra = Gcra::per_minute(1200.0, 1);
        let t0 = Instant::now();
        gcra.book(t0, 1);
        assert_eq!(gcra.backlog(t0), ms(50));
    }
}
