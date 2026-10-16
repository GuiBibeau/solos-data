//! The state snapshot lane's checks (ADR-0011). The lane is optional: no `status.json` under the
//! state root means it is not installed and nothing is judged. A status that says the lane
//! stopped cleanly (`running: false`) is reported, not judged; the operator stopped it. While it
//! says it runs, a stale status (a hung or killed process), a stale last order book read and a
//! stale last trader scan warn or go critical, and errors grown since the previous check warn.

use super::{Findings, Inputs, Thresholds, at_path, num, parse_time};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

fn minutes_since(status: &Value, lane: &str, now: DateTime<Utc>) -> Option<i64> {
    at_path(Some(status), &["lanes", lane, "lastAt"])
        .and_then(Value::as_str)
        .and_then(parse_time)
        .map(|at| (now - at).num_minutes())
}

fn errors(status: &Value) -> i64 {
    ["exchange", "books", "markets", "traders"]
        .iter()
        .filter_map(|lane| num(at_path(Some(status), &["lanes", lane, "errors"])))
        .sum()
}

/// The `snapshot` section of `health.json`.
pub(super) fn section(
    inputs: &Inputs,
    t: &Thresholds,
    now: DateTime<Utc>,
    f: &mut Findings,
) -> Value {
    let Some(status) = inputs.snapshot_status.value.as_ref() else {
        return Value::Null;
    };
    let age = inputs.snapshot_status.age_s.unwrap_or(0) / 60;
    let running = status.get("running").and_then(Value::as_bool) == Some(true);
    let books = minutes_since(status, "books", now);
    let traders = minutes_since(status, "traders", now);
    let total = errors(status);
    let started = status.get("startedAt").and_then(Value::as_str);
    let previous = at_path(inputs.previous.as_ref(), &["snapshot"]);
    let same_run = previous
        .and_then(|p| p.get("startedAt"))
        .and_then(Value::as_str)
        == started;
    let before = if same_run {
        previous.and_then(|p| num(p.get("errors"))).unwrap_or(0)
    } else {
        0
    };
    if running {
        f.grade(
            age as f64,
            t.snapshot_status_warn_minutes as f64 - 0.5,
            t.snapshot_status_critical_minutes as f64 - 0.5,
            true,
            &format!("snapshot status.json {age} min old"),
        );
        match books {
            Some(m) => f.grade(
                m as f64,
                t.snapshot_books_stale_warn_minutes as f64 - 0.5,
                t.snapshot_books_stale_critical_minutes as f64 - 0.5,
                true,
                &format!("last order book snapshot {m} min ago"),
            ),
            None if age >= t.snapshot_books_stale_warn_minutes => f
                .warn
                .push("snapshot lane running without an order book snapshot".into()),
            None => {}
        }
        if let Some(m) = traders.filter(|m| *m >= t.snapshot_traders_stale_warn_minutes) {
            f.warn.push(format!("last trader scan {m} min ago"));
        }
        if total > before {
            f.warn
                .push(format!("snapshot lane: {} new errors", total - before));
        }
    }
    json!({
        "ageSeconds": inputs.snapshot_status.age_s,
        "startedAt": started,
        "running": running,
        "errors": total,
        "booksMinutesAgo": books,
        "tradersMinutesAgo": traders,
        "creditsPerDay": status.get("creditsPerDayAtThisRate"),
        "bytesPerDay": status.get("publishedBytesPerDayAtThisRate"),
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{at, healthy, snap};
    use super::super::{Thresholds, evaluate};
    use serde_json::json;

    fn status(running: bool, books: &str, errors: i64) -> serde_json::Value {
        json!({ "startedAt": "2026-10-10T00:00:00Z", "running": running,
            "lanes": { "books": { "lastAt": books, "errors": errors }, "traders": { "lastAt": "2026-10-10T07:55:00Z", "errors": 0 } } })
    }

    #[test]
    fn the_lane_is_judged_only_while_it_runs() {
        let now = "2026-10-10T08:00:00Z";
        let t = Thresholds::default();
        // Not installed: nothing.
        let health = evaluate(&healthy(now), &t, at(now));
        assert_eq!(health["level"], "ok", "{health}");
        assert!(health["snapshot"].is_null());
        // Running and fresh.
        let mut inputs = healthy(now);
        inputs.snapshot_status = snap(status(true, "2026-10-10T07:59:50Z", 0), 20);
        let health = evaluate(&inputs, &t, at(now));
        assert_eq!(health["level"], "ok", "{health}");
        assert_eq!(health["snapshot"]["booksMinutesAgo"], 0);
        // Books stale for 10 minutes: warn; 40 minutes and a dead status: critical.
        inputs.snapshot_status = snap(status(true, "2026-10-10T07:50:00Z", 0), 20);
        let health = evaluate(&inputs, &t, at(now));
        assert_eq!(health["level"], "warn", "{health}");
        assert!(
            health["warn"]
                .to_string()
                .contains("order book snapshot 10 min ago")
        );
        inputs.snapshot_status = snap(status(true, "2026-10-10T07:20:00Z", 0), 40 * 60);
        let health = evaluate(&inputs, &t, at(now));
        assert_eq!(health["level"], "critical", "{health}");
        // Stopped cleanly: reported, not judged.
        inputs.snapshot_status = snap(status(false, "2026-10-09T07:20:00Z", 3), 86_400);
        let health = evaluate(&inputs, &t, at(now));
        assert_eq!(health["level"], "ok", "{health}");
        assert_eq!(health["snapshot"]["running"], false);
        // New errors since the previous check of the same run warn once.
        inputs.snapshot_status = snap(status(true, "2026-10-10T07:59:50Z", 2), 20);
        let health = evaluate(&inputs, &t, at(now));
        assert!(health["warn"].to_string().contains("2 new errors"));
        inputs.previous = Some(health);
        let again = evaluate(&inputs, &t, at(now));
        assert_eq!(again["level"], "ok", "{again}");
    }
}
