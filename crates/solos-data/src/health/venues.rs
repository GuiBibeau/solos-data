//! The venues lane (ADR-0012) as `status-venues.json` reports it: the file's age, each venue's
//! slot lag behind the finalized slot, its run of failed cycles and its last cycle, and the Pyth
//! poll. The lane is optional: no status file means no finding.

use super::{Findings, Thresholds, at_path, num, parse_time};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// Judge the venues status; `None` when the lane has never run.
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
    if age / 60 >= t.venues_status_warn_minutes {
        f.warn
            .push(format!("venues status-venues.json {} min old", age / 60));
    }
    if status.get("running").and_then(Value::as_bool) == Some(false) {
        f.warn.push("venues lane stopped".into());
    }
    let minutes = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .and_then(parse_time)
            .map(|at| (now - at).num_minutes())
    };
    let mut venues = serde_json::Map::new();
    for (name, venue) in status
        .get("venues")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let lag = num(venue.get("lagSlots"));
        if let Some(lag) = lag {
            f.grade(
                lag as f64,
                t.venues_lag_warn_slots as f64,
                t.venues_lag_critical_slots as f64,
                true,
                &format!("venues {name}: {lag} slots behind finalized"),
            );
        }
        let streak = num(venue.get("errorStreak")).unwrap_or(0);
        f.grade(
            streak as f64,
            (t.error_streak_warn - 1) as f64,
            (t.error_streak_critical - 1) as f64,
            true,
            &format!("venues {name}: {streak} failed cycles in a row"),
        );
        let cycle_minutes = minutes(venue.get("lastCycleAt"));
        if cycle_minutes.is_some_and(|m| m >= t.venues_status_warn_minutes) {
            f.warn.push(format!(
                "venues {name}: last cycle {} min ago",
                cycle_minutes.unwrap_or(0)
            ));
        }
        venues.insert(name.clone(), json!({ "lagSlots": lag, "errorStreak": streak, "lastCycleMinutesAgo": cycle_minutes, "lastError": venue.get("lastError") }));
    }
    let pyth_enabled =
        at_path(Some(status), &["pyth", "enabled"]).and_then(Value::as_bool) == Some(true);
    let pyth_minutes = minutes(at_path(Some(status), &["pyth", "lastPollAt"]));
    if pyth_enabled && pyth_minutes.is_none_or(|m| m >= t.venues_status_warn_minutes) && age < 600 {
        f.warn.push("venues pyth: no recent poll".into());
    }
    let mismatched = num(at_path(Some(status), &["pyth", "mismatched"])).unwrap_or(0);
    if mismatched > 0 {
        f.warn.push(format!(
            "venues pyth: {mismatched} accounts with another feed id or owner"
        ));
    }
    json!({
        "ageSeconds": age,
        "running": status.get("running"),
        "venues": venues,
        "pythPollMinutesAgo": pyth_minutes,
        "creditsPerDay": at_path(Some(status), &["rpc", "creditsPerDay"]),
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::at;
    use super::*;

    #[test]
    fn lag_streaks_and_stale_polls_are_findings() {
        let now = "2026-10-18T12:00:00Z";
        let status = json!({ "running": true,
            "venues": {
                "jupiter": { "lagSlots": 100, "errorStreak": 0, "lastCycleAt": "2026-10-18T11:59:30Z" },
                "flash": { "lagSlots": 20_000, "errorStreak": 4, "lastCycleAt": "2026-10-18T11:30:00Z" } },
            "pyth": { "enabled": true, "lastPollAt": "2026-10-18T11:59:55Z", "mismatched": 0 } });
        let mut f = Findings::default();
        let out = section(
            Some(&status),
            Some(30),
            &Thresholds::default(),
            at(now),
            &mut f,
        );
        assert_eq!(out["venues"]["jupiter"]["lagSlots"], 100);
        assert_eq!(f.critical, ["venues flash: 20000 slots behind finalized"]);
        assert_eq!(
            f.warn,
            [
                "venues flash: 4 failed cycles in a row",
                "venues flash: last cycle 30 min ago"
            ]
        );
        let mut quiet = Findings::default();
        assert!(section(None, None, &Thresholds::default(), at(now), &mut quiet).is_null());
        assert!(quiet.warn.is_empty() && quiet.critical.is_empty());
    }
}
