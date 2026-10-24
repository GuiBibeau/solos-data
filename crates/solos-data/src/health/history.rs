//! A history backfill as its status file reports it (`status-venues-history.json`, ADR-0013;
//! `status-spot-history.json`, ADR-0014): the file's age while it runs, and per source its run of
//! failed batches and how long since its last batch while it has not reached the floor.
//! Optional: no status file means no finding; a finished backfill (every source done) is
//! reported, not judged. `label` names the backfill in the findings.

use super::{Findings, Thresholds, num, parse_time};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// Judge the history status; `Null` when the backfill has never run.
pub(super) fn section(
    label: &str,
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
    let sources = status.get("sources").and_then(Value::as_object);
    let all_done = sources.is_some_and(|s| {
        s.values()
            .all(|v| v.get("done").and_then(Value::as_bool) == Some(true))
    });
    if !all_done && age / 60 >= t.venues_status_warn_minutes {
        f.warn.push(format!("{label} status {} min old", age / 60));
    }
    if !all_done && !running {
        f.warn.push(format!("{label} stopped before the floor"));
    }
    let started = status.get("startedAt").and_then(Value::as_str);
    let mut out = serde_json::Map::new();
    for (name, source) in sources.into_iter().flatten() {
        let done = source.get("done").and_then(Value::as_bool) == Some(true);
        let streak = num(source.get("errorStreak")).unwrap_or(0);
        // Before its first batch a source is measured from the run's start.
        let since = source
            .get("lastBatchAt")
            .and_then(Value::as_str)
            .or(started)
            .and_then(parse_time)
            .map(|at| (now - at).num_minutes());
        if !done {
            f.grade(
                streak as f64,
                (t.error_streak_warn - 1) as f64,
                (t.error_streak_critical - 1) as f64,
                true,
                &format!("{label} {name}: {streak} failed batches in a row"),
            );
            if running && since.is_some_and(|m| m >= t.backfill_stall_minutes) {
                f.warn.push(format!(
                    "{label} {name}: no batch for {} min",
                    since.unwrap_or(0)
                ));
            }
        }
        out.insert(
            name.clone(),
            json!({ "done": done, "percent": source.get("percent"), "etaHours": source.get("etaHours"),
                    "errorStreak": streak, "lastBatchMinutesAgo": since, "lastError": source.get("lastError") }),
        );
    }
    json!({ "ageSeconds": age, "running": running, "done": all_done, "sources": out })
}

#[cfg(test)]
mod tests {
    use super::super::tests::at;
    use super::*;

    #[test]
    fn a_stalled_or_failing_source_warns_and_a_finished_backfill_is_quiet() {
        let now = at("2026-10-23T12:00:00Z");
        let t = Thresholds::default();
        let status = json!({ "running": true, "startedAt": "2026-10-23T09:30:00Z", "sources": {
            "jupiter": { "done": false, "errorStreak": 4, "lastBatchAt": "2026-10-23T11:59:00Z", "percent": 12.5 },
            "pyth": { "done": false, "errorStreak": 0, "lastBatchAt": "2026-10-23T09:00:00Z" },
            "flash": { "done": true, "errorStreak": 12, "lastBatchAt": "2026-10-20T09:00:00Z" },
            "fresh": { "done": false, "errorStreak": 0 }
        }});
        let mut f = Findings::default();
        let out = section("venues history", Some(&status), Some(60), &t, now, &mut f);
        assert_eq!(out["sources"]["jupiter"]["percent"], 12.5);
        assert_eq!(
            f.warn,
            [
                "venues history fresh: no batch for 150 min",
                "venues history jupiter: 4 failed batches in a row",
                "venues history pyth: no batch for 180 min"
            ]
        );
        assert!(f.critical.is_empty());
        let finished = json!({ "running": false, "sources": { "jupiter": { "done": true } } });
        let mut f = Findings::default();
        section(
            "venues history",
            Some(&finished),
            Some(86_400),
            &t,
            now,
            &mut f,
        );
        assert!(f.warn.is_empty() && f.critical.is_empty());
        assert_eq!(
            section("spot history", None, None, &t, now, &mut f),
            Value::Null
        );
    }
}
