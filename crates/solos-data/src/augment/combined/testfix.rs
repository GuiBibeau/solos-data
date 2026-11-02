//! Shared combined-store test fixtures: the v2 lockbox written to disk with
//! its named hours, and lockbox-intersection asserts for output tables. Every
//! unit's tests (U1 today, the U2 hourly units after their rebase) build
//! lockbox-touching data through this module, so "an hour inside the lockbox"
//! means the same hours everywhere and the in-data case is always testable.
//!
//! The lockbox body itself comes from `lockbox::tests::v2_body`, the same
//! bytes U1's behavior fixtures use: four Monday weeks in 2026 with a 24-hour
//! guard, 864 excluded hours over 36 UTC dates.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::grid;

/// The v2 lockbox fixture: file, sha, and the exclusion set with named hours.
pub(crate) struct LockboxFixture {
    /// Where the lockbox JSON was written.
    pub(crate) path: PathBuf,
    /// SHA-256 of the file bytes (pass as `--lockbox-sha256`).
    pub(crate) sha256: String,
    /// Every excluded unix hour (864 for v2).
    pub(crate) hours: BTreeSet<i64>,
    /// Half-open hour ranges of the exclusion set, one per week.
    pub(crate) ranges: Vec<(i64, i64)>,
}

impl LockboxFixture {
    /// First excluded hour.
    pub(crate) fn first_hour(&self) -> i64 {
        *self.hours.iter().next().expect("v2 excludes hours")
    }

    /// Last excluded hour.
    pub(crate) fn last_hour(&self) -> i64 {
        *self.hours.iter().next_back().expect("v2 excludes hours")
    }

    /// An excluded hour in the middle of the set.
    pub(crate) fn mid_hour(&self) -> i64 {
        let middle = self.hours.len() / 2;
        *self.hours.iter().nth(middle).expect("v2 excludes hours")
    }

    /// The free hour just before the set.
    pub(crate) fn hour_before(&self) -> i64 {
        self.first_hour() - 1
    }

    /// The free hour just after the set.
    pub(crate) fn hour_after(&self) -> i64 {
        self.last_hour() + 1
    }

    /// First second of a fixture hour (timestamps for fixture rows).
    pub(crate) fn secs(&self, hour: i64) -> i64 {
        grid::hour_start(hour)
    }
}

/// Write the v2 lockbox body to `<dir>/lockbox.json` and return its fixture.
pub(crate) fn v2_lockbox(dir: &Path) -> LockboxFixture {
    let body = super::lockbox::tests::v2_body();
    let path = dir.join("lockbox.json");
    std::fs::write(&path, &body).unwrap();
    let lockbox: super::lockbox::Lockbox = serde_json::from_str(&body).unwrap();
    LockboxFixture {
        sha256: crate::fsutil::sha256_hex(body.as_bytes()),
        hours: lockbox.excluded_hours().unwrap(),
        ranges: lockbox.excluded_ranges().unwrap(),
        path,
    }
}

/// The timestamp unit of a table column: unix seconds or unix milliseconds.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TsUnit {
    /// `floor(ts / 3600)` is the hour.
    Secs,
    /// `floor(ts / 3_600_000)` is the hour.
    Millis,
}

impl TsUnit {
    /// The divisor that maps the column to unix hours.
    fn divisor(self) -> i64 {
        match self {
            Self::Secs => grid::HOUR_SECS,
            Self::Millis => grid::HOUR_MS,
        }
    }
}

/// Panic when any row of `table` (a plain table or view name) carries a
/// `ts_column` timestamp inside `hours`, so a unit's sealed output can prove
/// it dropped the lockbox. Fetches the distinct hours first, so the failure
/// names exactly which lockbox hours leaked.
pub(crate) fn assert_no_rows_in_lockbox(
    conn: &duckdb::Connection,
    table: &str,
    ts_column: &str,
    unit: TsUnit,
    hours: &BTreeSet<i64>,
) {
    let sql = format!(
        "SELECT DISTINCT ({ts_column} // {}) AS hour FROM {table}",
        unit.divisor()
    );
    let rows = crate::store::query_rows(conn, &sql, &[]).expect("hour query over the table");
    let leaked: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.int("hour"))
        .filter(|hour| hours.contains(hour))
        .collect();
    assert!(
        leaked.is_empty(),
        "{table} has rows inside the lockbox hours {leaked:?}"
    );
}

/// [`assert_no_rows_in_lockbox`] over a sealed Parquet file.
pub(crate) fn assert_no_parquet_rows_in_lockbox(
    conn: &duckdb::Connection,
    path: &Path,
    ts_column: &str,
    unit: TsUnit,
    hours: &BTreeSet<i64>,
) {
    let sql_table = format!(
        "read_parquet({})",
        crate::store::sql_string(&path.to_string_lossy())
    );
    assert_no_rows_in_lockbox(conn, &sql_table, ts_column, unit, hours);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory_connection;

    #[test]
    fn the_v2_fixture_names_its_hours() {
        let dir = std::env::temp_dir().join(format!(
            "combined-testfix-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let fix = v2_lockbox(&dir);
        assert!(fix.path.is_file());
        assert_eq!(fix.hours.len(), 864);
        assert_eq!(fix.ranges.len(), 4);
        assert_eq!(fix.sha256, crate::fsutil::file_hash(&fix.path).unwrap());
        for hour in [fix.first_hour(), fix.mid_hour(), fix.last_hour()] {
            assert!(fix.hours.contains(&hour), "{hour} must be excluded");
        }
        assert!(!fix.hours.contains(&fix.hour_before()));
        assert!(!fix.hours.contains(&fix.hour_after()));
        assert_eq!(fix.secs(fix.mid_hour()), fix.mid_hour() * 3_600);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_in_lockbox_assert_passes_clean_and_panics_on_leak() {
        let dir = std::env::temp_dir().join(format!(
            "combined-testfix-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let fix = v2_lockbox(&dir);
        let conn = memory_connection("1", "512MB").unwrap();
        // Seconds: one free hour and one inside.
        conn.execute_batch(&format!(
            "CREATE TABLE fills(ts BIGINT);
             INSERT INTO fills VALUES ({}), ({});",
            fix.secs(fix.hour_before()),
            fix.secs(fix.mid_hour()),
        ))
        .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_no_rows_in_lockbox(&conn, "fills", "ts", TsUnit::Secs, &fix.hours);
        }));
        assert!(result.is_err(), "the inside row must trip the assert");
        conn.execute_batch(&format!(
            "CREATE TABLE kept AS SELECT * FROM fills WHERE ts <> {}",
            fix.secs(fix.mid_hour())
        ))
        .unwrap();
        assert_no_rows_in_lockbox(&conn, "kept", "ts", TsUnit::Secs, &fix.hours);
        // Millis: the same hours through a millisecond column.
        conn.execute_batch(&format!(
            "CREATE TABLE fills_ms(ts BIGINT);
             INSERT INTO fills_ms VALUES ({});",
            fix.secs(fix.mid_hour()) * 1_000
        ))
        .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_no_rows_in_lockbox(&conn, "fills_ms", "ts", TsUnit::Millis, &fix.hours);
        }));
        assert!(result.is_err(), "the inside millisecond row must trip it");
        let _ = std::fs::remove_dir_all(dir);
    }
}
