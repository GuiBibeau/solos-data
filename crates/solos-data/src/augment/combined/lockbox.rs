//! Lockbox v2: load, SHA-256 check, and the UTC hours it removes from every
//! combined output table (DESIGN.md 6, PLAN.md).
//!
//! The lockbox is a JSON file of exclusion weeks. Week `start` is inclusive,
//! `end` is exclusive, and every hour from `start - guard_hours` to
//! `end + guard_hours` is dropped. The sha256 is checked against the operator's
//! `--lockbox-sha256` before anything else happens; a mismatch refuses the run.

use crate::store::StoreError;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::grid;

/// Schema of the accepted lockbox files.
pub const LOCKBOX_VERSION: &str = "v2";

/// The v2 lockbox must exclude exactly this many hours: four weeks (7 days each)
/// with a 24-hour guard on both sides is 4 x (168 + 48) = 864.
pub const V2_EXPECTED_HOURS: usize = 864;

/// The v2 lockbox must cover exactly this many UTC calendar dates.
pub const V2_EXPECTED_DAYS: usize = 36;

/// The parsed lockbox file.
#[derive(Clone, Debug, Deserialize)]
pub struct Lockbox {
    /// [`LOCKBOX_VERSION`]; any other version is refused.
    pub version: String,
    /// Guard hours dropped on each side of every week.
    pub guard_hours: i64,
    /// Exclusion weeks, Monday 00:00 UTC to the next Monday 00:00 UTC, half-open.
    pub weeks: Vec<Week>,
}

/// One exclusion week.
#[derive(Clone, Debug, Deserialize)]
pub struct Week {
    /// Week name (`early_stress`, `stress`, `quiet`, `busy`).
    #[allow(dead_code)]
    pub name: String,
    /// Inclusive RFC 3339 start.
    pub start: String,
    /// Exclusive RFC 3339 end.
    pub end: String,
}

/// Load `path`, require its SHA-256 to equal `expected_sha256`, and require the
/// v2 schema. The bytes are hashed exactly as stored.
///
/// # Errors
///
/// The file cannot be read, its sha256 does not match, it is not valid JSON, its
/// version is not [`LOCKBOX_VERSION`], a week is malformed, or `guard_hours` is
/// negative.
pub fn load(path: &Path, expected_sha256: &str) -> Result<Lockbox, StoreError> {
    let bytes = std::fs::read(path)
        .map_err(|error| StoreError::Check(format!("lockbox: cannot read: {error}")))?;
    let actual = crate::fsutil::sha256_hex(&bytes);
    if !actual.eq_ignore_ascii_case(expected_sha256.trim()) {
        return Err(StoreError::Check(format!(
            "lockbox sha256 mismatch: file {actual}, expected {}",
            expected_sha256.trim()
        )));
    }
    let lockbox: Lockbox = serde_json::from_slice(&bytes)
        .map_err(|error| StoreError::Check(format!("lockbox: bad JSON: {error}")))?;
    check(&lockbox)?;
    Ok(lockbox)
}

/// The structural guards of a parsed lockbox: version, guard hours, and the
/// weeks' timestamps. `load` applies them after the sha check; tests call this
/// directly on hand-built values.
pub(crate) fn check(lockbox: &Lockbox) -> Result<(), StoreError> {
    if lockbox.version != LOCKBOX_VERSION {
        return Err(StoreError::Check(format!(
            "lockbox version {} is not {LOCKBOX_VERSION}",
            lockbox.version
        )));
    }
    if lockbox.guard_hours < 0 {
        return Err(StoreError::Check("lockbox guard_hours is negative".into()));
    }
    if lockbox.weeks.is_empty() {
        return Err(StoreError::Check("lockbox has no weeks".into()));
    }
    for week in &lockbox.weeks {
        let start = grid::parse_rfc3339_secs(&week.start)
            .map_err(|error| StoreError::Check(format!("lockbox week {}: {error}", week.name)))?;
        let end = grid::parse_rfc3339_secs(&week.end)
            .map_err(|error| StoreError::Check(format!("lockbox week {}: {error}", week.name)))?;
        if end <= start {
            return Err(StoreError::Check(format!(
                "lockbox week {}: end is not after start",
                week.name
            )));
        }
    }
    Ok(())
}

impl Lockbox {
    /// Half-open unix-hour ranges `[first, last + 1)` of the exclusion set, one
    /// per week (week hours with the guard on both sides). The SQL-side filters
    /// use these ranges: they are the same hours as [`Lockbox::excluded_hours`]
    /// without materialising 864 values per query.
    ///
    /// # Errors
    ///
    /// A week timestamp is malformed.
    pub fn excluded_ranges(&self) -> Result<Vec<(i64, i64)>, StoreError> {
        let mut ranges = Vec::new();
        let guard = self.guard_hours * grid::HOUR_SECS;
        for week in &self.weeks {
            let start = grid::parse_rfc3339_secs(&week.start).map_err(|error| {
                StoreError::Check(format!("lockbox week {}: {error}", week.name))
            })?;
            let end = grid::parse_rfc3339_secs(&week.end).map_err(|error| {
                StoreError::Check(format!("lockbox week {}: {error}", week.name))
            })?;
            let first = grid::hour_of_secs(start - guard);
            // `end - 1 + guard` is the last second the guard keeps.
            let last = grid::hour_of_secs(end - 1 + guard);
            ranges.push((first, last + 1));
        }
        ranges.sort();
        Ok(ranges)
    }

    /// Every excluded unix hour: for each week, the hours that intersect
    /// `[start - guard, end + guard)`.
    ///
    /// # Errors
    ///
    /// A week timestamp is malformed (rechecked here so the set is always in
    /// sync with the parsed weeks).
    pub fn excluded_hours(&self) -> Result<BTreeSet<i64>, StoreError> {
        let mut hours = BTreeSet::new();
        for (first, last_exclusive) in self.excluded_ranges()? {
            for hour in first..last_exclusive {
                hours.insert(hour);
            }
        }
        Ok(hours)
    }

    /// The v2 pin: a v2 lockbox must exclude exactly [`V2_EXPECTED_HOURS`]
    /// hours across exactly [`V2_EXPECTED_DAYS`] UTC dates. Another version is
    /// refused here too: the pin is what the plan assumes, so any other shape
    /// or version makes the run stop rather than drop silently.
    ///
    /// # Errors
    ///
    /// The version is not v2, or the exclusion set has any other size.
    pub fn assert_v2_shape(&self, hours: &BTreeSet<i64>) -> Result<(), StoreError> {
        if self.version != LOCKBOX_VERSION {
            return Err(StoreError::Check(format!(
                "lockbox version {} is not {LOCKBOX_VERSION}",
                self.version
            )));
        }
        let days: BTreeSet<String> = hours.iter().map(|h| grid::date_of_hour(*h)).collect();
        if hours.len() != V2_EXPECTED_HOURS || days.len() != V2_EXPECTED_DAYS {
            return Err(StoreError::Check(format!(
                "lockbox v2 must exclude exactly {V2_EXPECTED_HOURS} hours over {V2_EXPECTED_DAYS} UTC days, got {} hours over {} days",
                hours.len(),
                days.len()
            )));
        }
        Ok(())
    }
}

/// Refuse when any of `days` falls inside the excluded hours.
///
/// # Errors
///
/// A day is not `YYYY-MM-DD`, or one of its 24 hours is excluded.
pub fn refuse_check_days_inside(
    hours: &BTreeSet<i64>,
    days: &[chrono::NaiveDate],
) -> Result<(), StoreError> {
    for day in days {
        for hour in grid::day_hours(*day) {
            if hours.contains(&hour) {
                return Err(StoreError::Check(format!(
                    "check day {day} is inside the lockbox; pick days the price check may read"
                )));
            }
        }
    }
    Ok(())
}

/// `true` when `hour` is excluded.
#[must_use]
pub fn is_excluded(hours: &BTreeSet<i64>, hour: i64) -> bool {
    hours.contains(&hour)
}

/// Distinct UTC dates of an excluded-hour set, oldest first (receipts record this).
#[must_use]
pub fn excluded_dates(hours: &BTreeSet<i64>) -> Vec<String> {
    let mut dates: BTreeMap<String, ()> = BTreeMap::new();
    for hour in hours {
        dates.insert(grid::date_of_hour(*hour), ());
    }
    dates.into_keys().collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A parsed v2 lockbox for helper methods.
    pub fn from_body_for_test() -> super::Lockbox {
        serde_json::from_str::<super::Lockbox>(&v2_body()).unwrap()
    }

    /// The real v2 shape: four Monday weeks in 2026 with a 24-hour guard.
    pub fn v2_body() -> String {
        serde_json::json!({
            "version": "v2",
            "created": "2026-10-22",
            "guard_hours": 24,
            "weeks": [
                {"name": "early_stress", "start": "2026-06-01T00:00:00Z", "end": "2026-06-08T00:00:00Z", "reason": "r"},
                {"name": "stress", "start": "2026-08-17T00:00:00Z", "end": "2026-08-24T00:00:00Z", "reason": "r"},
                {"name": "quiet", "start": "2026-09-07T00:00:00Z", "end": "2026-09-14T00:00:00Z", "reason": "r"},
                {"name": "busy", "start": "2026-09-28T00:00:00Z", "end": "2026-10-05T00:00:00Z", "reason": "r"}
            ]
        })
        .to_string()
    }

    #[test]
    fn v2_weeks_exclude_exactly_864_hours_over_36_days() {
        let lockbox: Lockbox = serde_json::from_str(&v2_body()).unwrap();
        assert_eq!(lockbox.version, "v2");
        let hours = lockbox.excluded_hours().unwrap();
        assert_eq!(hours.len(), 864);
        assert_eq!(excluded_dates(&hours).len(), 36);
        lockbox.assert_v2_shape(&hours).unwrap();
        // The first excluded hour is Sunday 00:00 before the week (guard 24h); the
        // last is Monday 23:00 after it, and Tuesday 00:00 is kept.
        let sunday = grid::day_start_secs(grid::parse_date("2026-05-31").unwrap());
        assert!(hours.contains(&grid::hour_of_secs(sunday)));
        assert!(is_excluded(
            &hours,
            grid::hour_of_secs(sunday + 9 * 86_400 - 1)
        ));
        assert!(!hours.contains(&grid::hour_of_secs(sunday + 9 * 86_400)));
    }

    #[test]
    fn a_wrong_shape_or_version_is_refused() {
        let lockbox: Lockbox = serde_json::from_str(&v2_body()).unwrap();
        let mut hours = lockbox.excluded_hours().unwrap();
        hours.pop_last();
        assert!(lockbox.assert_v2_shape(&hours).is_err());
        let mut body: serde_json::Value = serde_json::from_str(&v2_body()).unwrap();
        body["version"] = serde_json::json!("v1");
        let lockbox: Lockbox = serde_json::from_value(body).unwrap();
        let hours = lockbox.excluded_hours().unwrap();
        assert!(lockbox.assert_v2_shape(&hours).is_err());
    }

    #[test]
    fn ranges_and_hours_agree_and_the_v2_loader_guards_hold() {
        let lockbox: Lockbox = serde_json::from_str(&v2_body()).unwrap();
        let hours = lockbox.excluded_hours().unwrap();
        let ranges = lockbox.excluded_ranges().unwrap();
        assert_eq!(ranges.len(), 4);
        let from_ranges: BTreeSet<i64> = ranges
            .iter()
            .flat_map(|(first, last_exclusive)| *first..*last_exclusive)
            .collect();
        assert_eq!(from_ranges, hours);
        // Each loader guard refuses its own malformed file.
        let negative: Lockbox = serde_json::from_value(serde_json::json!({
            "version": "v2", "guard_hours": -1, "weeks": [{"name": "w", "start": "2026-06-01T00:00:00Z", "end": "2026-06-08T00:00:00Z"}]
        }))
        .unwrap();
        assert!(
            negative.excluded_hours().is_ok(),
            "parsed; load() refuses it"
        );
        let body = serde_json::json!({
            "version": "v2", "guard_hours": 24, "weeks": [{"name": "w", "start": "2026-06-01T00:00:00Z", "end": "2026-06-08T00:00:00Z"}]
        });
        let mut bad = body.clone();
        bad["guard_hours"] = serde_json::json!(-1);
        let bad: Lockbox = serde_json::from_value(bad).unwrap();
        let error = super::check(&bad).unwrap_err();
        assert!(error.to_string().contains("negative"), "{error}");
        let mut bad = body.clone();
        bad["weeks"] = serde_json::json!([]);
        let bad: Lockbox = serde_json::from_value(bad).unwrap();
        let error = super::check(&bad).unwrap_err();
        assert!(error.to_string().contains("no weeks"), "{error}");
        let mut bad = body;
        bad["weeks"][0]["end"] = serde_json::json!("2026-05-31T00:00:00Z");
        let bad: Lockbox = serde_json::from_value(bad).unwrap();
        let error = super::check(&bad).unwrap_err();
        assert!(error.to_string().contains("not after start"), "{error}");
    }

    /// The v2 ranges, for tests outside this module.
    pub fn v2_ranges_for_test() -> Vec<(i64, i64)> {
        from_body_for_test().excluded_ranges().unwrap()
    }

    #[test]
    fn a_check_day_inside_the_lockbox_is_refused() {
        let lockbox: Lockbox = serde_json::from_str(&v2_body()).unwrap();
        let hours = lockbox.excluded_hours().unwrap();
        assert!(
            refuse_check_days_inside(&hours, &[grid::parse_date("2026-09-10").unwrap()]).is_err()
        );
        assert!(
            refuse_check_days_inside(&hours, &[grid::parse_date("2026-08-16").unwrap()]).is_err()
        );
        refuse_check_days_inside(&hours, &[grid::parse_date("2026-10-15").unwrap()]).unwrap();
    }
}
