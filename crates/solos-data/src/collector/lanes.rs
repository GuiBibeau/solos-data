//! Each lane's state for `status.json` `lanes`: whether it is enabled and its run of consecutive
//! errors. Kept in memory, not in DuckDB, so it is still counted when the database is the thing
//! failing (the 2026-10-14 incident). A streak resets when the lane succeeds (a tail cycle) or
//! when its progress marker moved since the previous error (a backfill chunk published); health
//! reads the marker to tell a live streak from a stale one.

use crate::health::error_class;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone, Debug, Default)]
struct LaneState {
    enabled: bool,
    streak: u64,
    since: Option<String>,
    last_at: Option<String>,
    class: Option<&'static str>,
    progress: Value,
    last_success_at: Option<String>,
}

/// The lanes of one collector process.
#[derive(Debug, Default)]
pub struct Lanes(Mutex<BTreeMap<&'static str, LaneState>>);

fn key(lane: &str) -> &'static str {
    if lane == "tail" { "tail" } else { "backfill" }
}

impl Lanes {
    /// Both lanes, with their configured switches.
    #[must_use]
    pub fn new(tail: bool, backfill: bool) -> Self {
        let lane = |enabled| LaneState {
            enabled,
            ..LaneState::default()
        };
        Lanes(Mutex::new(BTreeMap::from([
            ("tail", lane(tail)),
            ("backfill", lane(backfill)),
        ])))
    }

    /// Count an error; `progress` is the lane's marker now, when it could be read.
    pub fn error(&self, lane: &str, message: &str, at: &str, progress: Option<Value>) {
        let mut lanes = self.0.lock().expect("lanes");
        let state = lanes.entry(key(lane)).or_default();
        if let Some(progress) = progress {
            if state.streak > 0 && progress != state.progress {
                state.streak = 0;
            }
            state.progress = progress;
        }
        if state.streak == 0 {
            state.since = Some(at.to_owned());
        }
        state.streak += 1;
        state.last_at = Some(at.to_owned());
        state.class = Some(error_class(message));
    }

    /// A successful cycle ends the streak.
    pub fn success(&self, lane: &str, at: &str) {
        let mut lanes = self.0.lock().expect("lanes");
        let state = lanes.entry(key(lane)).or_default();
        state.streak = 0;
        state.since = None;
        state.last_success_at = Some(at.to_owned());
    }

    /// The `lanes` object of `status.json`.
    #[must_use]
    pub fn to_value(&self) -> Value {
        let lanes = self.0.lock().expect("lanes");
        let mut out = serde_json::Map::new();
        for (name, s) in lanes.iter() {
            out.insert(
                (*name).to_owned(),
                json!({
                    "enabled": s.enabled,
                    "errorStreak": s.streak,
                    "streakSince": s.since,
                    "lastErrorAt": s.last_at,
                    "lastErrorClass": s.class,
                    "progress": s.progress,
                    "lastSuccessAt": s.last_success_at,
                }),
            );
        }
        Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaks_count_until_success_or_progress() {
        let lanes = Lanes::new(true, true);
        let oom = "Out of Memory Error: could not allocate block";
        lanes.error("backfill", oom, "t1", Some(json!([412, 3835])));
        lanes.error("backfill", oom, "t2", Some(json!([412, 3835])));
        // Unreadable marker (the database is failing): still the same streak.
        lanes.error("backfill", oom, "t3", None);
        lanes.error(
            "tail",
            "Constraint Error: Duplicate key",
            "t3",
            Some(json!(454)),
        );
        let v = lanes.to_value();
        assert_eq!(v["backfill"]["errorStreak"], 3);
        assert_eq!(v["backfill"]["streakSince"], "t1");
        assert_eq!(v["backfill"]["lastErrorClass"], "oom");
        assert_eq!(v["tail"]["lastErrorClass"], "constraint");
        assert_eq!(v["tail"]["enabled"], true);
        // A chunk was published between two errors: a new streak.
        lanes.error("backfill", oom, "t4", Some(json!([411, 3836])));
        let v = lanes.to_value();
        assert_eq!(v["backfill"]["errorStreak"], 1);
        assert_eq!(v["backfill"]["streakSince"], "t4");
        lanes.success("tail", "t5");
        let v = lanes.to_value();
        assert_eq!(v["tail"]["errorStreak"], 0);
        assert_eq!(v["tail"]["lastSuccessAt"], "t5");
        assert_eq!(v["tail"]["lastErrorAt"], "t3");
    }
}
