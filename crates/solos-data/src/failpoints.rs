//! Named crash-safety failpoints (issue #56).
//!
//! A kill point aborts the process — it never returns an error — because a returned error runs
//! destructors and is not a crash. The points are compiled in only under the `failpoints` cargo
//! feature, which release builds leave off. The harness (`tests/failpoints.rs`) spawns the real
//! binary, arms exactly one point, and SIGKILL/SIGABRT lands where the point is placed.

/// The collector publication kill points, in commit order (ADR-0016 steps 2–7).
///
/// - `publish/after-tmp-write`: after the `COPY` wrote the `.tmp` file, before its fsync.
/// - `publish/after-file-fsync`: after the `.tmp` fsync, before the rename.
/// - `publish/after-rename`: after the rename onto the final path.
/// - `publish/before-dir-fsync`: immediately before the directory fsync that follows the rename.
/// - `publish/after-range-check`: after the exported row count read back matched.
/// - `publish/before-watermark-commit`: before the transaction that registers the files and
///   advances the watermark.
/// - `publish/mid-seal`: inside that transaction, after the files and range are registered, before
///   the watermark moves.
pub const KILL_POINTS: &[&str] = &[
    "publish/after-tmp-write",
    "publish/after-file-fsync",
    "publish/after-rename",
    "publish/before-dir-fsync",
    "publish/after-range-check",
    "publish/before-watermark-commit",
    "publish/mid-seal",
];

/// Arm each named failpoint to abort the process. Only the listed points fire; the others stay
/// no-ops. Requires the `failpoints` feature.
#[cfg(feature = "failpoints")]
pub fn arm_crash(names: &[&str]) {
    for name in names {
        let _ = fail::cfg_callback(*name, || std::process::abort());
    }
}

/// Arm each named failpoint to abort the process (no-op without the `failpoints` feature).
#[cfg(not(feature = "failpoints"))]
pub fn arm_crash(_names: &[&str]) {}

/// Arm one failpoint with a custom action for tests (the `fail` crate's callback). The action
/// must not unwind through the process: a returning action continues, an aborting action kills.
#[cfg(feature = "failpoints")]
pub fn arm_callback(name: &str, action: impl Fn() + Send + Sync + 'static) {
    let _ = fail::cfg_callback(name.to_owned(), action);
}

/// Remove a failpoint's armed action.
#[cfg(feature = "failpoints")]
pub fn disarm(name: &str) {
    fail::remove(name);
}

/// Fire the named crash failpoint. A no-op unless the `failpoints` feature is on and the point
/// was armed.
#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! crash_point {
    ($name:expr) => {
        let _ = ::fail::eval($name, |_| ());
    };
}

/// Fire the named crash failpoint (compiled out).
#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! crash_point {
    ($name:expr) => {};
}
