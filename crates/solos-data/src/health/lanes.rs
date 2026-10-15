//! The collector's two lanes as raw `status.json` reports them: how long the tail watermark has
//! not advanced, and each lane's run of consecutive errors with no progress in between. Both came
//! out of the 2026-10-14 incident, where DuckDB ran out of memory, a commit failed to roll back and
//! from then on both lanes failed every 30 seconds while the service stayed active (ADR-0008).
//!
//! The collector writes `lanes.{tail,backfill}` since this change; an older collector's status has
//! only `tailHealth.errorStreak` and `lastError`, which the checks fall back to for the tail.

use super::{Findings, Thresholds, at_path, num, parse_time, stamp};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// The class of a collector error message, for the error-streak findings.
#[must_use]
pub fn error_class(message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    if m.contains("failed to rollback") || m.contains("cannot continue operation") {
        "rollback_failed"
    } else if m.contains("out of memory") {
        "oom"
    } else if m.contains("constraint error") || m.contains("duplicate key") {
        "constraint"
    } else if m.contains("(code ") || m.contains("timed out") || m.contains("timeout") {
        "rpc"
    } else {
        "other"
    }
}

/// A lane's progress marker, from the record that moves when the lane publishes a chunk: the
/// tail's watermark `W` (its slot) and the `backfill` record (`next` and `completedChunks`). The
/// collector stores it with each error and health compares it with the current status, so an
/// error streak with a published chunk in between is not a streak.
#[must_use]
pub fn lane_progress(lane: &str, record: &Value) -> Value {
    match lane {
        "tail" => record.get("slot").cloned().unwrap_or(Value::Null),
        _ => json!([record.get("next"), record.get("completedChunks")]),
    }
}

/// The status record a lane's progress marker is read from.
#[must_use]
pub fn progress_record(lane: &str) -> &'static str {
    if lane == "tail" { "W" } else { "backfill" }
}

fn status_record<'a>(status: &'a Value, lane: &str) -> Option<&'a Value> {
    let field = if lane == "tail" {
        "watermark"
    } else {
        "backfill"
    };
    status.get(field)
}

/// When the lane last published a chunk: its record's `completedAt`.
fn published_at(status: &Value, lane: &str) -> Option<DateTime<Utc>> {
    status_record(status, lane)
        .and_then(|r| r.get("completedAt"))
        .and_then(Value::as_str)
        .and_then(parse_time)
}

fn status_progress(status: &Value, lane: &str) -> Value {
    status_record(status, lane)
        .filter(|v| !v.is_null())
        .map_or(Value::Null, |record| lane_progress(lane, record))
}

/// Whether the tail is enabled: `lanes.tail.enabled`, or for an older collector a tail cycle in
/// flight or a non-zero tail error streak (`None` when it cannot tell).
fn tail_enabled(status: &Value) -> Option<bool> {
    if let Some(enabled) = at_path(Some(status), &["lanes", "tail", "enabled"]) {
        return enabled.as_bool();
    }
    let cycle = status.get("tailCycle").is_some_and(|c| !c.is_null());
    let streak = num(at_path(Some(status), &["tailHealth", "errorStreak"])).unwrap_or(0);
    (cycle || streak > 0).then_some(true)
}

/// Tail staleness: minutes since the watermark last advanced (`watermark.completedAt`, written
/// with every published tail chunk). A tail catching up advances it every chunk however far behind
/// the head it is, so only the advance is judged; the lag is reported.
pub(super) fn tail_section(
    status: Option<&Value>,
    t: &Thresholds,
    now: DateTime<Utc>,
    f: &mut Findings,
) -> Value {
    let Some(status) = status else {
        return Value::Null;
    };
    let enabled = tail_enabled(status);
    if enabled != Some(true) {
        return json!({ "enabled": enabled });
    }
    let watermark = status.get("watermark");
    let slot = num(at_path(watermark, &["slot"]));
    let advanced_at = at_path(watermark, &["completedAt"])
        .and_then(Value::as_str)
        .and_then(parse_time);
    let head = [
        num(at_path(Some(status), &["tailCycle", "ceiling"])),
        num(status.get("newest_slot")),
    ]
    .into_iter()
    .flatten()
    .max();
    let lag = match (head, slot) {
        (Some(h), Some(s)) => Some((h - s).max(0)),
        _ => None,
    };
    let minutes = advanced_at.map(|at| (now - at).num_minutes());
    match minutes {
        None => f
            .warn
            .push("tail enabled but its watermark is missing".into()),
        Some(m) if m >= t.tail_stale_warn_minutes => {
            let what = format!(
                "tail watermark not advanced for {m} min (slot {}, {} slots behind the head)",
                slot.map_or("?".into(), |s| s.to_string()),
                lag.map_or("?".into(), |l| l.to_string()),
            );
            if m >= t.tail_stale_critical_minutes {
                f.critical.push(what);
            } else {
                f.warn.push(what);
            }
        }
        Some(_) => {}
    }
    json!({
        "enabled": true,
        "watermarkSlot": slot,
        "watermarkAt": advanced_at.map(stamp),
        "minutesSinceAdvance": minutes,
        "headSlot": head,
        "lagSlots": lag,
    })
}

/// One lane's streak as the status reports it.
struct Streak {
    count: i64,
    since: Option<String>,
    last_at: Option<DateTime<Utc>>,
    class: String,
    progress: Value,
}

fn streak(status: &Value, lane: &str) -> Option<Streak> {
    let last = status
        .get("lastError")
        .filter(|e| e.get("lane").and_then(Value::as_str) == Some(lane));
    let last_message = last.and_then(|e| e.get("error")).and_then(Value::as_str);
    if let Some(entry) = at_path(Some(status), &["lanes", lane]) {
        let text = |key: &str| entry.get(key).and_then(Value::as_str).map(str::to_owned);
        return Some(Streak {
            count: num(entry.get("errorStreak")).unwrap_or(0),
            since: text("streakSince"),
            last_at: text("lastErrorAt").as_deref().and_then(parse_time),
            class: text("lastErrorClass")
                .or_else(|| last_message.map(|m| error_class(m).to_owned()))
                .unwrap_or_else(|| "unknown".into()),
            progress: entry.get("progress").cloned().unwrap_or(Value::Null),
        });
    }
    // An older collector: only the tail keeps a streak, reset by every successful cycle.
    if lane != "tail" {
        return None;
    }
    let count = num(at_path(Some(status), &["tailHealth", "errorStreak"]))?;
    let at = |v: Option<&Value>| {
        v.and_then(|v| v.get("at"))
            .and_then(Value::as_str)
            .and_then(parse_time)
    };
    Some(Streak {
        count,
        since: None,
        last_at: at(last).or_else(|| at(Some(status))),
        class: last_message.map_or("unknown", error_class).to_owned(),
        progress: Value::Null,
    })
}

/// Error streaks: a lane whose last `errorStreak` errors came with no published chunk in between
/// (its progress marker unchanged) and whose last error is recent.
pub(super) fn streak_section(
    status: Option<&Value>,
    t: &Thresholds,
    now: DateTime<Utc>,
    f: &mut Findings,
) -> Value {
    let Some(status) = status else {
        return Value::Null;
    };
    let mut out = serde_json::Map::new();
    for lane in ["tail", "backfill"] {
        let Some(s) = streak(status, lane) else {
            continue;
        };
        // A chunk published since the streak's last error ends it: the marker moved, or the lane's
        // record was completed after that error. The second also covers an older collector,
        // whose `tailHealth.errorStreak` is written only when a tail cycle ends, which during a
        // catch-up is hours after the chunks resumed.
        let moved = (!s.progress.is_null() && status_progress(status, lane) != s.progress)
            || matches!((published_at(status, lane), s.last_at), (Some(p), Some(e)) if p > e);
        let recent = s
            .last_at
            .is_some_and(|at| (now - at).num_minutes() < t.error_streak_window_minutes);
        let current = if moved || !recent { 0 } else { s.count };
        if current >= t.error_streak_warn {
            let what = format!(
                "{lane} lane: {current} consecutive errors ({}) {}with no chunk published",
                s.class,
                s.since
                    .as_deref()
                    .map_or(String::new(), |at| format!("since {at} ")),
            );
            if current >= t.error_streak_critical {
                f.critical.push(what);
            } else {
                f.warn.push(what);
            }
        }
        out.insert(
            lane.to_owned(),
            json!({
                "errorStreak": s.count,
                "current": current,
                "class": s.class,
                "since": s.since,
                "lastErrorAt": s.last_at.map(stamp),
                "progressMoved": moved,
            }),
        );
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::super::evaluate;
    use super::super::tests::{at, healthy, snap};
    use super::*;

    const OOM: &str =
        "Out of Memory Error: failed to allocate data of size 512.0 KiB (29.8 GiB/29.8 GiB used)";
    const ROLLBACK: &str = "TransactionContext Error: Failed to commit: Failed to rollback transaction. Cannot continue operation.\nOriginal Error: Out of Memory Error: failed to pin block";
    const DUPLICATE: &str = "Constraint Error: Duplicate key \"name: bulk/tail/454584552/454584679\" violates primary key constraint.";

    #[test]
    fn errors_are_classified() {
        assert_eq!(error_class(OOM), "oom");
        assert_eq!(error_class(ROLLBACK), "rollback_failed");
        assert_eq!(error_class(DUPLICATE), "constraint");
        assert_eq!(error_class("RPC getSlot failed (code 429)"), "rpc");
        assert_eq!(error_class("Invalid bulk transaction page"), "other");
    }

    /// The raw `status.json` 30 minutes into the incident, as this change's collector writes it.
    fn incident(now: &str) -> Value {
        json!({
            "at": now,
            "newest_slot": 456_379_002_i64,
            "watermark": { "slot": 454_584_551_i64, "completedAt": "2026-10-14T07:22:40.000Z", "signature": "x" },
            "backfill": { "next": 412_113_136_i64, "completedChunks": 3835, "phase": "fetching" },
            "tailCycle": { "floor": 453_617_399_i64, "ceiling": 456_379_002_i64, "next": 454_584_552_i64 },
            "tailHealth": { "errorStreak": 60 },
            "lastError": { "lane": "tail", "error": DUPLICATE, "at": "2026-10-14T07:52:16.000Z" },
            "lanes": {
                "tail": { "enabled": true, "errorStreak": 60, "streakSince": "2026-10-14T07:22:44.217Z",
                    "lastErrorAt": "2026-10-14T07:52:16.000Z", "lastErrorClass": "constraint",
                    "progress": 454_584_551_i64 },
                "backfill": { "enabled": true, "errorStreak": 60, "streakSince": "2026-10-14T07:22:44.467Z",
                    "lastErrorAt": "2026-10-14T07:52:14.000Z", "lastErrorClass": "oom",
                    "progress": [412_113_136_i64, 3835] },
            },
        })
    }

    fn judge(status: Value, now: &str) -> Value {
        let mut inputs = healthy(now);
        inputs.raw_status = snap(status, 5);
        inputs.capture_status.value.as_mut().unwrap()["lastCycles"]["elfaAt"] = json!(now);
        evaluate(&inputs, &Thresholds::default(), at(now))
    }

    #[test]
    fn the_incident_goes_critical_on_both_lanes() {
        let now = "2026-10-14T07:52:30Z";
        let health = judge(incident(now), now);
        assert_eq!(health["level"], "critical", "{health}");
        let critical = health["critical"].to_string();
        assert!(
            critical.contains(
                "tail lane: 60 consecutive errors (constraint) since 2026-10-14T07:22:44.217Z"
            ),
            "{critical}"
        );
        assert!(
            critical.contains("backfill lane: 60 consecutive errors (oom)"),
            "{critical}"
        );
        // Half an hour without a tail chunk: a warning, then critical at an hour.
        let warn = health["warn"].to_string();
        assert!(warn.contains("tail watermark not advanced for 29 min (slot 454584551, 1794451 slots behind the head)"), "{warn}");
        let later = "2026-10-14T08:30:00Z";
        let mut status = incident(later);
        status["lanes"]["tail"]["lastErrorAt"] = json!("2026-10-14T08:29:46.000Z");
        let health = judge(status, later);
        assert!(
            health["critical"]
                .to_string()
                .contains("tail watermark not advanced for 67 min")
        );
        assert_eq!(health["raw"]["tail"]["lagSlots"], 1_794_451);
    }

    #[test]
    fn an_older_collector_status_still_flags_the_tail() {
        // The box's status.json on 2026-10-15 before the restart: no `lanes`.
        let now = "2026-10-15T02:45:00Z";
        let mut status = incident(now);
        status.as_object_mut().unwrap().remove("lanes");
        status["tailHealth"]["errorStreak"] = json!(1255);
        status["lastError"] =
            json!({ "lane": "tail", "error": OOM, "at": "2026-10-15T02:42:37.003Z" });
        let health = judge(status, now);
        let critical = health["critical"].to_string();
        assert!(
            critical.contains("tail lane: 1255 consecutive errors (oom) with no chunk"),
            "{critical}"
        );
        assert!(
            critical.contains("tail watermark not advanced for 1162 min"),
            "{critical}"
        );
    }

    #[test]
    fn a_stale_streak_from_an_older_collector_is_ignored_once_chunks_publish() {
        // The box at 03:09 on 2026-10-15, eighteen minutes after the restart: the tail publishes
        // again, but the old collector's `tailHealth` still holds the streak from before.
        let now = "2026-10-15T03:09:30Z";
        let mut status = incident(now);
        status.as_object_mut().unwrap().remove("lanes");
        status["tailHealth"]["errorStreak"] = json!(1255);
        status["lastError"] =
            json!({ "lane": "tail", "error": OOM, "at": "2026-10-15T02:42:37.003Z" });
        status["watermark"]["completedAt"] = json!("2026-10-15T03:08:11.192Z");
        let health = judge(status, now);
        assert_eq!(health["level"], "ok", "{health}");
        assert_eq!(health["raw"]["errorStreaks"]["tail"]["progressMoved"], true);
    }

    #[test]
    fn a_tail_catching_up_and_a_broken_streak_are_ok() {
        let now = "2026-10-15T03:00:00Z";
        let mut status = incident(now);
        // Two million slots behind but publishing chunks: fine.
        status["watermark"]["completedAt"] = json!("2026-10-15T02:59:50.000Z");
        status["watermark"]["slot"] = json!(454_600_000_i64);
        // The tail has published since its last error; the backfill errors are old.
        status["lanes"]["tail"]["lastErrorAt"] = json!("2026-10-15T02:59:00.000Z");
        status["lanes"]["backfill"]["lastErrorAt"] = json!("2026-10-15T02:20:00.000Z");
        let health = judge(status.clone(), now);
        assert_eq!(health["level"], "ok", "{health}");
        assert_eq!(health["raw"]["errorStreaks"]["tail"]["progressMoved"], true);
        assert_eq!(health["raw"]["errorStreaks"]["backfill"]["current"], 0);
        // Under the critical count: a warning.
        status["lanes"]["backfill"]["lastErrorAt"] = json!("2026-10-15T02:59:30.000Z");
        status["lanes"]["backfill"]["errorStreak"] = json!(4);
        let warn = judge(status.clone(), now)["warn"].to_string();
        assert!(
            warn.contains("backfill lane: 4 consecutive errors (oom)"),
            "{warn}"
        );
        // A disabled tail is not judged.
        status["lanes"]["tail"]["enabled"] = json!(false);
        status["watermark"]["completedAt"] = json!("2026-10-05T00:00:00.000Z");
        assert!(
            !judge(status, now)["warn"]
                .to_string()
                .contains("tail watermark")
        );
    }

    #[test]
    fn progress_markers_match_the_status_records() {
        let status = incident("2026-10-14T07:52:30Z");
        assert_eq!(
            status_progress(&status, "tail"),
            status["lanes"]["tail"]["progress"]
        );
        assert_eq!(
            status_progress(&status, "backfill"),
            status["lanes"]["backfill"]["progress"]
        );
        assert_eq!(progress_record("tail"), "W");
        assert_eq!(progress_record("backfill"), "backfill");
    }
}
