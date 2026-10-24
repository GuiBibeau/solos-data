//! The forward spot lane as its status file reports it (`status-spot.json`, ADR-0015): the
//! file's age, how far its durable cursor trails the finalized slot, its run of failed batches,
//! and how long since its last batch while a batch is due. Optional: no status file means no
//! finding. While it catches up the lag warns; only a lane that stopped making batches with a
//! batch due, or that keeps failing, is critical.

use super::{Findings, Thresholds, num, parse_time};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// Judge the forward lane; `Null` when it has never run.
pub(super) fn section(
    status: Option<&Value>,
    age_s: Option<i64>,
    t: &Thresholds,
    now: DateTime<Utc>,
    f: &mut Findings,
) -> Value {
    let Some(status) = status else {
        return Value::Null;
    };
    let age = age_s.unwrap_or(0);
    let running = status.get("running").and_then(Value::as_bool) == Some(true);
    let source = &status["sources"]["spot"];
    if age / 60 >= t.venues_status_warn_minutes {
        f.warn.push(format!("spot status {} min old", age / 60));
    }
    if !running {
        f.warn.push("spot lane stopped".into());
    }
    let lag = num(source.get("lagSlots"));
    let batch = num(source.get("batchSlots")).unwrap_or(9_000);
    if let Some(lag) = lag
        && lag > t.spot_lag_warn_slots
    {
        f.warn
            .push(format!("spot {lag} slots behind the finalized slot"));
    }
    // Before its first batch the lane is measured from its start.
    let since = source
        .get("lastBatchAt")
        .and_then(Value::as_str)
        .or_else(|| status.get("startedAt").and_then(Value::as_str))
        .and_then(parse_time)
        .map(|at| (now - at).num_minutes());
    // A batch is due once two batches' worth of finalized slots wait; until then the lane is
    // waiting for the tip, not stalled.
    if running
        && lag.is_some_and(|l| l >= 2 * batch)
        && let Some(minutes) = since
    {
        f.grade(
            minutes as f64,
            (t.history_stall_warn_minutes - 1) as f64,
            (t.history_stall_critical_minutes - 1) as f64,
            true,
            &format!("spot: no batch for {minutes} min with a batch due"),
        );
    }
    let streak = num(source.get("errorStreak")).unwrap_or(0);
    f.grade(
        streak as f64,
        (t.error_streak_warn - 1) as f64,
        (t.error_streak_critical - 1) as f64,
        true,
        &format!("spot: {streak} failed batches in a row"),
    );
    json!({ "ageSeconds": age, "running": running, "cursor": source.get("cursor"),
            "lagSlots": lag, "lastBatchMinutesAgo": since, "errorStreak": streak,
            "lastError": source.get("lastError"),
            "creditsPerDayAtTip": status["rpc"].get("creditsPerDayAtTip") })
}

#[cfg(test)]
mod tests {
    use super::super::tests::at;
    use super::*;

    fn status(lag: i64, last: &str, streak: i64) -> Value {
        json!({ "running": true, "startedAt": "2026-10-25T00:00:00Z",
                "sources": { "spot": { "lagSlots": lag, "batchSlots": 9_000, "cursor": 1,
                                        "lastBatchAt": last, "errorStreak": streak } },
                "rpc": { "creditsPerDayAtTip": 220_000 } })
    }

    #[test]
    fn the_forward_lane_warns_on_lag_and_goes_critical_when_it_stops() {
        let now = at("2026-10-25T12:00:00Z");
        let t = Thresholds::default();
        // At the tip, between batches: quiet even an hour after the last batch.
        let mut f = Findings::default();
        let out = section(
            Some(&status(8_000, "2026-10-25T10:50:00Z", 0)),
            Some(30),
            &t,
            now,
            &mut f,
        );
        assert!(f.warn.is_empty() && f.critical.is_empty(), "{:?}", f.warn);
        assert_eq!(out["lagSlots"], 8_000);
        // Catching up with batches flowing: the lag warns, nothing else.
        let mut f = Findings::default();
        section(
            Some(&status(400_000, "2026-10-25T11:55:00Z", 0)),
            Some(30),
            &t,
            now,
            &mut f,
        );
        assert_eq!(f.warn, ["spot 400000 slots behind the finalized slot"]);
        assert!(f.critical.is_empty());
        // Behind with no batch for three hours, and failing: critical.
        let mut f = Findings::default();
        section(
            Some(&status(40_000, "2026-10-25T09:00:00Z", 10)),
            Some(30),
            &t,
            now,
            &mut f,
        );
        assert_eq!(
            f.critical,
            [
                "spot: no batch for 180 min with a batch due",
                "spot: 10 failed batches in a row"
            ]
        );
        assert_eq!(f.warn, ["spot 40000 slots behind the finalized slot"]);
        let mut f = Findings::default();
        assert_eq!(section(None, None, &t, now, &mut f), Value::Null);
    }
}
