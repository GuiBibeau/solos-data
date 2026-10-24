//! Hang-proofing for the lanes that stream the Old Faithful archive (ADR-0014, "Stalls").
//!
//! Jetstreamer keeps its thread bookkeeping (activity stamps, finished threads, stream starts)
//! in process-wide statics keyed by thread index, and resets them at the start of every
//! `firehose` call. Two calls in flight in one process read each other's stamps: a thread that
//! finished its range asks an index the other call has started, but this call has not spawned
//! yet, for half of its work, and waits for an answer that never comes; the waiting thread goes
//! idle, and an idle thread holds back the spawn of the rest, so neither moves again. That is
//! how the spot history stopped on 2026-10-24 at the batch crossing epochs 1064 and 1065, with
//! every task parked and nothing on the network. So one process runs one `firehose` call at a
//! time ([`firehose_turn`]), with the threads its streams would have had.
//!
//! And a process whose lane stops making progress exits, so `Restart=on-failure` brings it
//! back: [`bump`] marks progress (a block streamed, an RPC answer, a batch written), and
//! [`spawn`] starts a watchdog thread that exits the process with [`STALL_EXIT`] once nothing
//! moved for the configured time. A retry inside the process would not do: the stuck call's
//! tasks and Jetstreamer's statics would outlive it.

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
