//! Test-only failure injection. This module is compiled only when the `failpoints` feature is
//! enabled, which the test build does through a self dev-dependency. Release builds ship no
//! injection surface (issue #66 item 5).

use std::cell::Cell;

thread_local! {
    /// When set, the next `sync_all` on this thread fails. A lane that hits the failure must
    /// exit and restart from the watermark, not retry it (ADR-0016).
    static FAIL_FSYNC: Cell<bool> = const { Cell::new(false) };
    /// When set, the store's DuckDB config builder asks for an invalid memory limit, so
    /// `config(false)` fails on this thread.
    static FAIL_DB_CONFIG: Cell<bool> = const { Cell::new(false) };
}

/// Fail the next `sync_all` on this thread until cleared.
pub fn inject_fsync_failure(fail: bool) {
    FAIL_FSYNC.with(|flag| flag.set(fail));
}

/// Whether the next `sync_all` on this thread must fail.
pub(crate) fn fsync_fails() -> bool {
    FAIL_FSYNC.with(|flag| flag.get())
}

/// Fail the store's DuckDB config on this thread until cleared.
pub fn inject_db_config_failure(fail: bool) {
    FAIL_DB_CONFIG.with(|flag| flag.set(fail));
}

/// Whether the store's DuckDB config must fail on this thread.
pub(crate) fn db_config_fails() -> bool {
    FAIL_DB_CONFIG.with(|flag| flag.get())
}
