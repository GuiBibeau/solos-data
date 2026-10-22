//! The `kalshi` section of `solos-data health`, from the sync lane's `status.json`: per series
//! the forward walk's day (behind when it is more than a day old), the backward walk's day
//! and when it last moved (stalled past the threshold while not done), and the run's requests
//! and `429` answers.

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};

/// The section and its warnings. `stall_minutes` bounds a backward walk that has not moved;
/// `throttle_warn` is the count of `429` answers in one run that warns.
#[must_use]
pub fn section(
    sync_status: Option<&Value>,
    now: DateTime<Utc>,
    stall_minutes: i64,
    throttle_warn: i64,
) -> (Value, Vec<String>) {
    let mut warn = Vec::new();
    let Some(block) = sync_status.and_then(|s| s.get("kalshi")) else {
        return (Value::Null, warn);
    };
    let today = now.date_naive();
    let mut behind = Vec::new();
    let mut stalled = Vec::new();
    let mut pending = 0;
    for (ticker, s) in block
        .get("series")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let head = s
            .get("head")
            .and_then(Value::as_str)
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());
        if head.is_some_and(|h| h < today - chrono::Duration::days(1)) {
            behind.push(ticker.clone());
        }
        if s.get("tailDone").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        pending += 1;
        let moved = s
            .get("tailMovedAt")
            .and_then(Value::as_str)
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok());
        if moved.is_some_and(|m| (now - m.with_timezone(&Utc)).num_minutes() >= stall_minutes) {
            stalled.push(ticker.clone());
        }
    }
    if !behind.is_empty() {
        warn.push(format!(
            "Kalshi forward walk more than a day behind: {}",
            behind.join(", ")
        ));
    }
    if !stalled.is_empty() {
        warn.push(format!(
            "Kalshi backfill has not moved for {stall_minutes} min: {}",
            stalled.join(", ")
        ));
    }
    let limited = block
        .get("rateLimited")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if limited >= throttle_warn {
        warn.push(format!(
            "Kalshi answered 429 {limited} times in the last run"
        ));
    }
    let section = json!({
        "cutoff": block.get("cutoff"), "requests": block.get("requests"), "rateLimited": limited,
        "seconds": block.get("seconds"), "series": block.get("series").and_then(Value::as_object).map_or(0, serde_json::Map::len),
        "backfillPending": pending, "behind": behind, "stalled": stalled,
    });
    (section, warn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn behind_stalled_and_throttled_series_warn() {
        let status = json!({"kalshi": {"requests": 900, "rateLimited": 3, "series": {
            "KXBTCD": {"head": "2026-10-22", "tail": "2026-03-01", "tailDone": false, "tailMovedAt": "2026-10-22T11:30:00Z"},
            "KXETHD": {"head": "2026-10-19", "tail": "2026-03-01", "tailDone": false, "tailMovedAt": "2026-10-21T01:00:00Z"},
            "KXFED": {"head": "2026-10-22", "tailDone": true}
        }}});
        let (section, warn) = section(Some(&status), at("2026-10-22T12:00:00Z"), 360, 100);
        assert_eq!(section["behind"], json!(["KXETHD"]));
        assert_eq!(section["stalled"], json!(["KXETHD"]));
        assert_eq!(section["backfillPending"], 2);
        assert_eq!(warn.len(), 2);
        let (_, warn) = section_with_429(&status);
        assert!(warn.iter().any(|w| w.contains("429")));
        assert_eq!(
            super::section(None, at("2026-10-22T12:00:00Z"), 360, 100).0,
            Value::Null
        );
    }

    fn section_with_429(status: &Value) -> (Value, Vec<String>) {
        let mut status = status.clone();
        status["kalshi"]["rateLimited"] = json!(250);
        section(Some(&status), at("2026-10-22T12:00:00Z"), 360, 100)
    }
}
