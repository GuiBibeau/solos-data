//! Hang-proofing for the lanes that stream the Old Faithful archive (ADR-0014, "Stalls").
//!
//! On 2026-10-24 the spot history stopped for five hours at the batch that crossed into epoch
//! 1065: the first block of that epoch carries reward entries of a type Jetstreamer did not know,
//! its thread failed on the block, backed off and retried it forever, and Jetstreamer reports a
//! retry only through `log` and an optional error callback, neither of which the lanes used. The
//! process looked alive and wrote nothing. The vendored Jetstreamer now skips such entries
//! (`vendor/jetstreamer-firehose`); this module makes the next unknown failure loud and bounded:
//!
//! - [`Retries`]: every firehose error is logged (`archive_retry`), and a slot that fails
//!   [`SLOT_ATTEMPTS`] times stops the call, so the batch fails with the error, is counted in
//!   the status and is retried whole.
//! - [`spawn`]: a watchdog thread exits the process with [`STALL_EXIT`] when nothing moved for
//!   the configured time ([`bump`] marks progress: a block streamed, an RPC answer, a batch
//!   written), so `Restart=on-failure` brings it back from a hang nothing else catches.
//! - [`firehose_turn`]: one `firehose` call at a time per process. Jetstreamer keeps its thread
//!   bookkeeping (activity stamps, finished threads, stream starts, recycle requests) in
//!   process-wide statics keyed by thread index and resets them at the start of each call, so
//!   two calls in flight read and clear each other's state. A batch is one call with the
//!   threads its streams would have had.
//! - [`archive_log`]: `SOLOS_DATA_ARCHIVE_LOG=info` forwards Jetstreamer's own log.

use crate::jsonout::{Obj, log};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The exit code of a process the watchdog stopped (`EX_TEMPFAIL`).
pub const STALL_EXIT: i32 = 75;

/// How often the watchdog looks.
const LOOK: Duration = Duration::from_secs(15);

static PROGRESS: AtomicU64 = AtomicU64::new(0);

/// Mark progress.
pub fn bump() {
    PROGRESS.fetch_add(1, Ordering::Relaxed);
}

/// The progress counter.
#[must_use]
pub fn progress() -> u64 {
    PROGRESS.load(Ordering::Relaxed)
}

/// What the watchdog last saw: the counter and when it last changed.
#[derive(Clone, Copy, Debug)]
pub struct Watch {
    seen: u64,
    since: Instant,
}

impl Watch {
    /// Start watching at `value`.
    #[must_use]
    pub fn new(value: u64, now: Instant) -> Watch {
        Watch {
            seen: value,
            since: now,
        }
    }

    /// Record a look at `value`; returns how long the counter has not moved.
    pub fn look(&mut self, value: u64, now: Instant) -> Duration {
        if value != self.seen {
            self.seen = value;
            self.since = now;
        }
        now.saturating_duration_since(self.since)
    }
}

/// Start the watchdog: when [`progress`] has not moved for `stall`, log `event` (with `lane`)
/// and exit the process with [`STALL_EXIT`]. A zero `stall` starts nothing.
pub fn spawn(lane: &'static str, event: &'static str, stall: Duration) {
    if stall.is_zero() {
        return;
    }
    let started = std::thread::Builder::new()
        .name("stall-watchdog".into())
        .spawn(move || {
            let mut watch = Watch::new(progress(), Instant::now());
            loop {
                std::thread::sleep(LOOK.min(stall));
                let quiet = watch.look(progress(), Instant::now());
                if quiet >= stall {
                    log(
                        event,
                        Obj::new()
                            .with("lane", lane)
                            .with("quietSeconds", quiet.as_secs())
                            .with("exitCode", STALL_EXIT),
                    );
                    std::process::exit(STALL_EXIT);
                }
            }
        });
    if let Err(error) = started {
        log(
            "stall_watchdog_error",
            Obj::new()
                .with("lane", lane)
                .with("error", error.to_string()),
        );
    }
}

/// Attempts at one slot after which a `firehose` call is stopped (Jetstreamer backs off 1, 2,
/// 4, 8, 16 and then 32 s between attempts: about four minutes).
pub const SLOT_ATTEMPTS: u32 = 12;

/// The failures of one `firehose` call, by slot.
#[derive(Debug)]
pub struct Retries {
    lane: &'static str,
    limit: u32,
    state: std::sync::Mutex<RetryState>,
}

#[derive(Debug, Default)]
struct RetryState {
    by_slot: std::collections::HashMap<u64, u32>,
    stuck: Option<(u64, u32, String)>,
}

impl Retries {
    /// No failures yet; `lane` names the logs.
    #[must_use]
    pub fn new(lane: &'static str, limit: u32) -> Retries {
        Retries {
            lane,
            limit: limit.max(1),
            state: std::sync::Mutex::new(RetryState::default()),
        }
    }

    /// Record a failure at `slot` and log it; true once the slot has failed `limit` times (the
    /// caller then stops the call).
    pub fn record(&self, thread: usize, slot: u64, error: &str) -> bool {
        let error = crate::jsonout::safe_error(error);
        let mut state = self.state.lock().expect("retries");
        let attempts = {
            let n = state.by_slot.entry(slot).or_default();
            *n += 1;
            *n
        };
        let give_up = attempts >= self.limit;
        if give_up && state.stuck.is_none() {
            state.stuck = Some((slot, attempts, error.clone()));
        }
        drop(state);
        log(
            if give_up {
                "archive_stuck"
            } else {
                "archive_retry"
            },
            Obj::new()
                .with("lane", self.lane)
                .with("thread", thread)
                .with("slot", slot)
                .with("attempts", attempts)
                .with("error", error),
        );
        give_up
    }

    /// The slot that made the call stop, as an error for the batch.
    #[must_use]
    pub fn stuck(&self) -> Option<String> {
        let state = self.state.lock().expect("retries");
        state.stuck.as_ref().map(|(slot, attempts, error)| {
            format!("archive stream stopped: slot {slot} failed {attempts} times: {error}")
        })
    }
}

/// Wait for this process's turn to run a Jetstreamer `firehose` call; hold the guard for the
/// whole call.
pub async fn firehose_turn() -> tokio::sync::MutexGuard<'static, ()> {
    static TURN: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    TURN.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Forward Jetstreamer's `log` records to stderr as JSON lines (`archive_log`, URLs redacted)
/// when `SOLOS_DATA_ARCHIVE_LOG` names a level (`error`, `warn`, `info`, `debug`); without it,
/// or without the `archive` feature, the library's records are dropped as before.
pub fn archive_log() {
    #[cfg(feature = "archive")]
    enabled::init();
}

/// The error callback and shutdown signal of one `firehose` call: each error goes to
/// `retries`, and the call is shut down once a slot has failed too often (the caller then
/// reads [`Retries::stuck`]). Keep the returned sender alive for the whole call: dropping every
/// sender also shuts it down.
#[cfg(feature = "archive")]
#[must_use]
pub fn on_error(
    retries: std::sync::Arc<Retries>,
) -> (
    impl jetstreamer_firehose::firehose::Handler<jetstreamer_firehose::firehose::FirehoseErrorContext>,
    tokio::sync::broadcast::Sender<()>,
    tokio::sync::broadcast::Receiver<()>,
) {
    use futures_util::FutureExt;
    let (stop, signal) = tokio::sync::broadcast::channel::<()>(4);
    let sender = stop.clone();
    let handler =
        move |thread: usize, error: jetstreamer_firehose::firehose::FirehoseErrorContext| {
            let retries = std::sync::Arc::clone(&retries);
            let stop = sender.clone();
            async move {
                if retries.record(thread, error.slot, &error.error_message) {
                    let _ = stop.send(());
                }
                Ok(())
            }
            .boxed()
        };
    (handler, stop, signal)
}

#[cfg(feature = "archive")]
mod enabled {
    use crate::jsonout::{Obj, safe_error};
    use ::log::{LevelFilter, Log, Metadata, Record};

    struct Forward(LevelFilter);

    impl Log for Forward {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= self.0
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                crate::jsonout::log(
                    "archive_log",
                    Obj::new()
                        .with("level", record.level().as_str())
                        .with("target", record.target())
                        .with("message", safe_error(&record.args().to_string())),
                );
            }
        }

        fn flush(&self) {}
    }

    pub(super) fn init() {
        let Ok(level) = std::env::var("SOLOS_DATA_ARCHIVE_LOG") else {
            return;
        };
        let Ok(filter) = level.trim().parse::<LevelFilter>() else {
            return;
        };
        if ::log::set_boxed_logger(Box::new(Forward(filter))).is_ok() {
            ::log::set_max_level(filter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watch_measures_time_since_the_counter_last_moved() {
        let t0 = Instant::now();
        let mut watch = Watch::new(5, t0);
        assert_eq!(watch.look(5, t0 + Duration::from_secs(10)).as_secs(), 10);
        // The counter moved: the clock starts again.
        assert_eq!(watch.look(6, t0 + Duration::from_secs(12)).as_secs(), 0);
        assert_eq!(watch.look(6, t0 + Duration::from_secs(40)).as_secs(), 28);
    }

    #[test]
    fn a_slot_that_keeps_failing_stops_the_call() {
        let retries = Retries::new("test", 3);
        assert!(!retries.record(0, 10, "timeout"));
        // Another slot has its own count.
        assert!(!retries.record(1, 11, "timeout"));
        assert!(!retries.record(0, 10, "decode rewards (slot 10): unsupported reward type 5"));
        assert_eq!(retries.stuck(), None);
        assert!(retries.record(0, 10, "decode rewards (slot 10): unsupported reward type 5"));
        assert_eq!(
            retries.stuck().unwrap(),
            "archive stream stopped: slot 10 failed 3 times: decode rewards (slot 10): unsupported reward type 5"
        );
        // URLs never reach the logs or the status.
        let retries = Retries::new("test", 1);
        assert!(retries.record(0, 5, "GET https://mirror.example/x?key=secret failed"));
        assert!(!retries.stuck().unwrap().contains("secret"));
    }

    #[test]
    fn firehose_calls_take_turns() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let first = firehose_turn().await;
            // A second call waits while the first holds its turn.
            let second = tokio::time::timeout(Duration::from_millis(50), firehose_turn()).await;
            assert!(second.is_err());
            drop(first);
            let third = tokio::time::timeout(Duration::from_secs(5), firehose_turn()).await;
            assert!(third.is_ok());
        });
    }
}
