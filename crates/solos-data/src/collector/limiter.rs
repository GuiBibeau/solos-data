//! One compute-unit bucket, tail priority, bounded idle borrowing and AIMD concurrency. A port
//! of `limiter.ts`; the numbers are the same.

use super::config::{Lane, Lanes};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Mutable limiter state, exposed for tests.
#[derive(Debug)]
pub struct State {
    /// Current admitted rate, CU/s.
    pub rate: f64,
    /// Shared tokens.
    pub tokens: f64,
    /// Tail-reserved tokens.
    pub tail_tokens: f64,
    /// Backfill tokens.
    pub backfill_tokens: f64,
    /// Last refill instant.
    pub last: Instant,
    /// Last tail grant, if any.
    pub last_tail: Option<Instant>,
    /// Tail requests waiting.
    pub waiting_tail: u32,
    /// Requests in flight per lane.
    pub active: Lanes,
    /// Current concurrency windows.
    pub windows: Lanes,
    /// Successes since the last window growth.
    pub successes: Lanes,
    /// Until when no request is admitted after a throttle.
    pub cooldown_until: Option<Instant>,
}

/// The limiter.
pub struct Limiter {
    /// Configured maximum rate.
    pub maximum_rate: f64,
    /// Tail share.
    pub share: f64,
    ceilings: Lanes,
    /// State behind a mutex; the async loop polls every 10 ms like the TypeScript one.
    pub state: Mutex<State>,
}

fn set_lane(lanes: &mut Lanes, lane: Lane, value: u32) {
    match lane {
        Lane::Tail => lanes.tail = value,
        Lane::Backfill => lanes.backfill = value,
    }
}

impl Limiter {
    /// New limiter at `rate` CU/s with `share` reserved for the tail.
    #[must_use]
    pub fn new(rate: f64, share: f64, concurrency: Lanes) -> Self {
        Limiter {
            maximum_rate: rate,
            share,
            ceilings: concurrency.clone(),
            state: Mutex::new(State {
                rate,
                tokens: 0.0,
                tail_tokens: 0.0,
                backfill_tokens: 0.0,
                last: Instant::now(),
                last_tail: None,
                waiting_tail: 0,
                active: Lanes {
                    tail: 0,
                    backfill: 0,
                },
                windows: concurrency,
                successes: Lanes {
                    tail: 0,
                    backfill: 0,
                },
                cooldown_until: None,
            }),
        }
    }

    /// Refill to at most a 100 ms burst.
    pub fn refill(&self) {
        let mut s = self.state.lock().expect("limiter lock");
        Self::refill_state(&mut s, self.share);
    }

    fn refill_state(s: &mut State, share: f64) {
        let now = Instant::now();
        let seconds = now.duration_since(s.last).as_secs_f64();
        s.last = now;
        // Smooth requests to a 100ms burst, leaving headroom in provider rolling windows.
        let capacity = (s.rate / 10.0).max(100.0);
        s.tokens = (s.tokens + seconds * s.rate).min(capacity);
        s.tail_tokens = (s.tail_tokens + seconds * s.rate * share).min(capacity);
        s.backfill_tokens = (s.backfill_tokens + seconds * s.rate * (1.0 - share)).min(capacity);
    }

    /// Current rate.
    #[must_use]
    pub fn rate(&self) -> f64 {
        self.state.lock().expect("limiter lock").rate
    }

    /// Current windows.
    #[must_use]
    pub fn windows(&self) -> Lanes {
        self.state.lock().expect("limiter lock").windows.clone()
    }

    /// Wait for `weight` CU on `lane`. Errors when `shutdown` is set or the weight exceeds the plan.
    pub async fn acquire(
        &self,
        lane: Lane,
        weight: u32,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<(), String> {
        if f64::from(weight) > self.maximum_rate {
            return Err("CU rate is below a method weight".into());
        }
        if lane == Lane::Tail {
            self.state.lock().expect("limiter lock").waiting_tail += 1;
        }
        let result = loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break Err("Shutdown requested".to_owned());
            }
            if self.try_grant(lane, weight) {
                break Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        if lane == Lane::Tail {
            self.state.lock().expect("limiter lock").waiting_tail -= 1;
        }
        result
    }

    fn try_grant(&self, lane: Lane, weight: u32) -> bool {
        let mut s = self.state.lock().expect("limiter lock");
        Self::refill_state(&mut s, self.share);
        let now = Instant::now();
        let weight = f64::from(weight);
        let idle_borrow = lane == Lane::Backfill
            && s.waiting_tail == 0
            && s.last_tail
                .is_none_or(|t| now.duration_since(t) > Duration::from_secs(1));
        let lane_tokens = if lane == Lane::Tail {
            s.tail_tokens
        } else {
            s.backfill_tokens
        };
        let cooled = s.cooldown_until.is_none_or(|until| now >= until);
        let active = s.active.get(lane);
        let window = s.windows.get(lane);
        if cooled && active < window && s.tokens >= weight && (lane_tokens >= weight || idle_borrow)
        {
            s.tokens -= weight;
            match lane {
                Lane::Tail => {
                    s.tail_tokens -= weight;
                    s.last_tail = Some(now);
                    s.active.tail += 1;
                }
                Lane::Backfill => {
                    s.backfill_tokens = (s.backfill_tokens - weight).max(0.0);
                    s.active.backfill += 1;
                }
            }
            return true;
        }
        false
    }

    /// Release a request; a throttle shrinks rate and window and starts a cooldown.
    pub fn release(&self, lane: Lane, throttled: bool, retry_ms: u64) {
        let mut s = self.state.lock().expect("limiter lock");
        let active = s.active.get(lane).saturating_sub(1);
        set_lane(&mut s.active, lane, active);
        if throttled {
            s.rate = (s.rate * 0.7).max(40.0);
            s.tokens = 0.0;
            let window = s.windows.get(lane);
            set_lane(&mut s.windows, lane, (window / 2).max(1));
            s.cooldown_until = Some(Instant::now() + Duration::from_millis(retry_ms.max(1000)));
            set_lane(&mut s.successes, lane, 0);
        } else {
            let successes = s.successes.get(lane) + 1;
            if successes >= 100 {
                let window = (s.windows.get(lane) + 1).min(self.ceilings.get(lane));
                set_lane(&mut s.windows, lane, window);
                s.rate = (s.rate + 10.0).min(self.maximum_rate);
                set_lane(&mut s.successes, lane, 0);
            } else {
                set_lane(&mut s.successes, lane, successes);
            }
        }
    }
}
