//! `solos-data health`: one read-only pass over the JSON snapshots the services already write
//! (the raw collector's `status.json` and `catalog.json`, the decoder's `status.json` and
//! `catalog.json`, the augment lanes' `status.json` and `status-capture.json`) plus the free
//! space of the data volume, judged against the thresholds of `config/health.json`. The result
//! is written to `health.json` (atomically) and summarized in one `health` log line with its
//! `warn` and `critical` lists. Two checks need a previous observation (backfill movement and
//! capture error growth); they read it from the previous `health.json`. Nothing under the raw
//! or decoded roots is opened for writing and no DuckDB file is touched.

use crate::fsutil::write_atomic;
use crate::jsonout::{Obj, log};
use crate::store::StoreError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The thresholds; every field has a default, so the config file may name only some.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Thresholds {
    /// Decoded catalog files above which compaction is behind.
    pub decoded_files_warn: u64,
    /// Decoded catalog files above which it is critical.
    pub decoded_files_critical: u64,
    /// The decoder cadence window, minutes.
    pub decoder_window_minutes: i64,
    /// No decoded batch for this long while the raw catalog is newer is critical, minutes.
    pub decoder_critical_minutes: i64,
    /// The raw collector's `status.json` older than this warns, minutes.
    pub raw_status_warn_minutes: i64,
    /// ... and older than this is critical, minutes.
    pub raw_status_critical_minutes: i64,
    /// `backfill.next` unchanged for this long warns, minutes.
    pub backfill_stall_minutes: i64,
    /// The sync lane's `status.json` older than this warns, minutes (the timer runs hourly).
    pub sync_status_warn_minutes: i64,
    /// The capture lane's `status-capture.json` older than this warns, minutes (written every
    /// minute while it runs).
    pub capture_status_warn_minutes: i64,
    /// The capture lane's last hourly Elfa cycle older than this warns, minutes.
    pub elfa_cycle_warn_minutes: i64,
    /// Free space on the data volume below this warns, GB.
    pub disk_free_warn_gb: u64,
    /// ... and below this is critical, GB.
    pub disk_free_critical_gb: u64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Thresholds {
            decoded_files_warn: 10_000,
            decoded_files_critical: 50_000,
            decoder_window_minutes: 30,
            decoder_critical_minutes: 120,
            raw_status_warn_minutes: 10,
            raw_status_critical_minutes: 60,
            backfill_stall_minutes: 120,
            sync_status_warn_minutes: 150,
            capture_status_warn_minutes: 10,
            elfa_cycle_warn_minutes: 150,
            disk_free_warn_gb: 500,
            disk_free_critical_gb: 100,
        }
    }
}

/// Where the snapshots are.
#[derive(Clone, Debug)]
pub struct Paths {
    /// The raw collector root.
    pub raw: PathBuf,
    /// The decoded root (`.../phoenix_decoded/v1`).
    pub decoded: PathBuf,
    /// The augment root.
    pub augment: PathBuf,
    /// The `health.json` to write.
    pub out: PathBuf,
}

impl Paths {
    /// From `SOLOS_DATA_RAW_DIR`, `SOLOS_DATA_DECODED_DIR`, `SOLOS_DATA_AUGMENT_DIR` and
    /// `SOLOS_DATA_HEALTH_FILE`, relative defaults under `data/` otherwise.
    #[must_use]
    pub fn from_env() -> Paths {
        let var = |name: &str, default: &str| {
            PathBuf::from(std::env::var(name).unwrap_or_else(|_| default.to_owned()))
        };
        Paths {
            raw: var("SOLOS_DATA_RAW_DIR", "data/phoenix_raw"),
            decoded: var("SOLOS_DATA_DECODED_DIR", "data/phoenix_decoded/v1"),
            augment: var("SOLOS_DATA_AUGMENT_DIR", "data/augment"),
            out: var("SOLOS_DATA_HEALTH_FILE", "data/health.json"),
        }
    }
}

/// Load the thresholds from a config file; a missing file means the defaults.
pub fn load_thresholds(path: Option<&str>) -> Result<Thresholds, StoreError> {
    let path = path.unwrap_or("config/health.json");
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let value: Value =
                serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
            let block = value.get("thresholds").cloned().unwrap_or(value);
            serde_json::from_value(block).map_err(|e| StoreError::Check(e.to_string()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Thresholds::default()),
        Err(e) => Err(StoreError::Check(e.to_string())),
    }
}

/// A JSON snapshot and its age.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// The parsed file, when it could be read.
    pub value: Option<Value>,
    /// File size, bytes.
    pub bytes: u64,
    /// Seconds since its `at` field (the modification time when it has none).
    pub age_s: Option<i64>,
}

/// Everything one evaluation reads.
#[derive(Clone, Debug, Default)]
pub struct Inputs {
    /// Raw `status.json`.
    pub raw_status: Snapshot,
    /// Raw `catalog.json`.
    pub raw_catalog: Snapshot,
    /// Decoded `status.json`.
    pub decoded_status: Snapshot,
    /// Decoded `catalog.json`.
    pub decoded_catalog: Snapshot,
    /// Augment `status.json` (sync lane).
    pub sync_status: Snapshot,
    /// Augment `status-capture.json`.
    pub capture_status: Snapshot,
    /// Free bytes on the data volume.
    pub disk_free_bytes: Option<u64>,
    /// The previous `health.json`.
    pub previous: Option<Value>,
}

/// Read a snapshot relative to `now`.
#[must_use]
pub fn read_snapshot(path: &Path, now: DateTime<Utc>) -> Snapshot {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Snapshot::default();
    };
    let value: Option<Value> = serde_json::from_str(&text).ok();
    let at = value
        .as_ref()
        .and_then(|v| v.get("at"))
        .and_then(Value::as_str)
        .and_then(parse_time)
        .or_else(|| {
            let modified = std::fs::metadata(path).ok()?.modified().ok()?;
            Some(DateTime::<Utc>::from(modified))
        });
    Snapshot {
        value,
        bytes: text.len() as u64,
        age_s: at.map(|at| (now - at).num_seconds()),
    }
}

fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Free bytes on the volume holding `path`.
#[must_use]
#[allow(clippy::useless_conversion)] // the field types differ between Linux and macOS
pub fn disk_free(path: &Path) -> Option<u64> {
    let stat = nix::sys::statvfs::statvfs(path).ok()?;
    Some(u64::from(stat.blocks_available()).saturating_mul(u64::from(stat.fragment_size())))
}

/// The findings of one evaluation.
#[derive(Default)]
struct Findings {
    warn: Vec<String>,
    critical: Vec<String>,
}

impl Findings {
    fn grade(&mut self, value: f64, warn: f64, critical: f64, above: bool, what: &str) {
        let (w, c) = if above {
            (value > warn, value > critical)
        } else {
            (value < warn, value < critical)
        };
        if c {
            self.critical.push(what.to_owned());
        } else if w {
            self.warn.push(what.to_owned());
        }
    }
}

fn num(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn at_path<'a>(value: Option<&'a Value>, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value?, |v, k| v.get(*k))
}

/// Judge the inputs at `now`; the result is the `health.json` body.
#[must_use]
pub fn evaluate(inputs: &Inputs, t: &Thresholds, now: DateTime<Utc>) -> Value {
    let mut f = Findings::default();
    let decoded = decoded_section(inputs, t, now, &mut f);
    let raw = raw_section(inputs, t, now, &mut f);
    let augment = augment_section(inputs, t, now, &mut f);
    let disk = match inputs.disk_free_bytes {
        Some(free) => {
            let gb = free as f64 / 1e9;
            f.grade(
                gb,
                t.disk_free_warn_gb as f64,
                t.disk_free_critical_gb as f64,
                false,
                &format!("disk free {gb:.0} GB"),
            );
            json!({ "freeBytes": free, "freeGb": (gb * 10.0).round() / 10.0 })
        }
        None => {
            f.warn.push("disk free unknown".into());
            json!({ "freeBytes": null })
        }
    };
    let level = if !f.critical.is_empty() {
        "critical"
    } else if !f.warn.is_empty() {
        "warn"
    } else {
        "ok"
    };
    json!({
        "at": now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        "level": level,
        "warn": f.warn,
        "critical": f.critical,
        "decoded": decoded,
        "raw": raw,
        "augment": augment,
        "disk": disk,
        "thresholds": t,
    })
}

fn decoded_section(inputs: &Inputs, t: &Thresholds, now: DateTime<Utc>, f: &mut Findings) -> Value {
    let Some(catalog) = inputs.decoded_catalog.value.as_ref() else {
        f.critical.push("decoded catalog unreadable".into());
        return json!({ "files": null });
    };
    let files = catalog.get("files").and_then(Value::as_array);
    let count = files.map_or(0, Vec::len) as u64;
    f.grade(
        count as f64,
        t.decoded_files_warn as f64,
        t.decoded_files_critical as f64,
        true,
        &format!("decoded files {count}: compaction behind"),
    );
    // Batches published by the decoder (compaction rewrites carry old batch ids; skip them).
    let since = now - chrono::Duration::minutes(t.decoder_window_minutes);
    let mut batches = BTreeSet::new();
    let mut newest: Option<DateTime<Utc>> = None;
    for file in files.into_iter().flatten() {
        let path = file.get("path").and_then(Value::as_str).unwrap_or("");
        let Some(created) = file
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(parse_time)
        else {
            continue;
        };
        if path.contains("/compact-") {
            continue;
        }
        newest = newest.max(Some(created));
        if created >= since {
            batches.insert(num(file.get("batch_id")).unwrap_or(-1));
        }
    }
    let raw_at = at_path(inputs.raw_catalog.value.as_ref(), &["at"])
        .and_then(Value::as_str)
        .and_then(parse_time);
    let behind = matches!((raw_at, newest), (Some(r), Some(n)) if r > n) || newest.is_none();
    let idle_minutes = newest.map(|n| (now - n).num_minutes());
    if batches.is_empty() && behind {
        let message = format!(
            "no decoded batch in {} min while the raw catalog is newer",
            idle_minutes.map_or("?".into(), |m| m.to_string())
        );
        if idle_minutes.is_none_or(|m| m >= t.decoder_critical_minutes) {
            f.critical.push(message);
        } else {
            f.warn.push(message);
        }
    }
    let status = inputs.decoded_status.value.as_ref();
    json!({
        "files": count,
        "catalogBytes": inputs.decoded_catalog.bytes,
        "batchesInWindow": batches.len(),
        "windowMinutes": t.decoder_window_minutes,
        "newestBatchAt": newest.map(|n| n.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()),
        "statusAgeSeconds": inputs.decoded_status.age_s,
        "transactionsProcessed": status.and_then(|s| num(s.get("transactions_processed"))),
        "idle": status.and_then(|s| s.get("idle")).cloned(),
    })
}

fn stamp(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn raw_section(inputs: &Inputs, t: &Thresholds, now: DateTime<Utc>, f: &mut Findings) -> Value {
    let status = inputs.raw_status.value.as_ref();
    match inputs.raw_status.age_s {
        Some(age) => f.grade(
            age as f64 / 60.0,
            t.raw_status_warn_minutes as f64,
            t.raw_status_critical_minutes as f64,
            true,
            &format!("raw status.json {} min old: collector stalled", age / 60),
        ),
        None => f.critical.push("raw status.json unreadable".into()),
    }
    let backfill = at_path(status, &["backfill"]);
    let next = backfill.and_then(|b| num(b.get("next")));
    let phase = backfill
        .and_then(|b| b.get("phase"))
        .and_then(Value::as_str);
    // When `next` last moved: now if it differs from the previous run's, else the previous
    // run's stamp (now on the first run, which cannot judge).
    let previous = at_path(inputs.previous.as_ref(), &["raw", "backfill"]);
    let changed_at = match previous {
        Some(p) if num(p.get("next")) == next => p
            .get("nextChangedAt")
            .and_then(Value::as_str)
            .and_then(parse_time)
            .unwrap_or(now),
        _ => now,
    };
    let unchanged_minutes = (now - changed_at).num_minutes();
    if next.is_some() && phase != Some("complete") && unchanged_minutes >= t.backfill_stall_minutes
    {
        f.warn.push(format!(
            "backfill.next unchanged for {unchanged_minutes} min"
        ));
    }
    json!({
        "statusAgeSeconds": inputs.raw_status.age_s,
        "catalogFiles": at_path(inputs.raw_catalog.value.as_ref(), &["files"])
            .and_then(Value::as_array)
            .map(Vec::len),
        "catalogAt": at_path(inputs.raw_catalog.value.as_ref(), &["at"]),
        "backfill": {
            "next": next,
            "phase": phase,
            "nextChangedAt": stamp(changed_at),
            "unchangedMinutes": unchanged_minutes,
        },
    })
}

/// The sum of every `errors` field one or two levels down.
fn error_total(status: &Value, sections: &[&str]) -> i64 {
    sections
        .iter()
        .filter_map(|s| status.get(*s))
        .flat_map(|section| {
            let own = num(section.get("errors"));
            let nested = section
                .as_object()
                .into_iter()
                .flat_map(|o| o.values())
                .filter_map(|v| num(v.get("errors")));
            own.into_iter().chain(nested)
        })
        .sum()
}

fn augment_section(inputs: &Inputs, t: &Thresholds, now: DateTime<Utc>, f: &mut Findings) -> Value {
    let sync = match inputs.sync_status.value.as_ref() {
        Some(status) => {
            let age = inputs.sync_status.age_s.unwrap_or(0);
            if age / 60 >= t.sync_status_warn_minutes {
                f.warn
                    .push(format!("augment sync status.json {} min old", age / 60));
            }
            let errors = error_total(status, &["sources"]);
            if errors > 0 {
                f.warn
                    .push(format!("augment sync: {errors} errors in the last run"));
            }
            json!({ "ageSeconds": age, "errors": errors, "stopped": status.get("stopped") })
        }
        None => {
            f.warn.push("augment status.json unreadable".into());
            Value::Null
        }
    };
    let capture = match inputs.capture_status.value.as_ref() {
        Some(status) => capture_section(status, inputs, t, now, f),
        None => {
            f.warn.push("augment status-capture.json unreadable".into());
            Value::Null
        }
    };
    json!({ "sync": sync, "capture": capture })
}

fn capture_section(
    status: &Value,
    inputs: &Inputs,
    t: &Thresholds,
    now: DateTime<Utc>,
    f: &mut Findings,
) -> Value {
    let age = inputs.capture_status.age_s.unwrap_or(0);
    if age / 60 >= t.capture_status_warn_minutes {
        f.warn
            .push(format!("augment status-capture.json {} min old", age / 60));
    }
    let errors = error_total(
        status,
        &["candles", "assetContexts", "elfa", "elfaEvents", "elfaAuto"],
    );
    let started = status.get("startedAt").and_then(Value::as_str);
    let previous = at_path(inputs.previous.as_ref(), &["augment", "capture"]);
    let same_run = previous
        .and_then(|p| p.get("startedAt"))
        .and_then(Value::as_str)
        == started;
    let before = if same_run {
        previous.and_then(|p| num(p.get("errors"))).unwrap_or(0)
    } else {
        0
    };
    if errors > before {
        f.warn
            .push(format!("augment capture: {} new errors", errors - before));
    }
    let guard = at_path(Some(status), &["elfa", "disabledByCreditGuard"]).and_then(Value::as_bool);
    if guard == Some(true) {
        f.warn
            .push("Elfa lane disabled by the credit guard until restart".into());
    }
    let elfa_at = at_path(Some(status), &["lastCycles", "elfaAt"])
        .and_then(Value::as_str)
        .and_then(parse_time);
    let elfa_enabled = at_path(Some(status), &["elfa", "enabled"]).and_then(Value::as_bool);
    let elfa_minutes = elfa_at.map(|at| (now - at).num_minutes());
    if elfa_enabled == Some(true) && elfa_minutes.is_some_and(|m| m >= t.elfa_cycle_warn_minutes) {
        f.warn.push(format!(
            "last Elfa hourly cycle {} min ago",
            elfa_minutes.unwrap_or(0)
        ));
    }
    json!({
        "ageSeconds": age,
        "startedAt": started,
        "running": status.get("running"),
        "errors": errors,
        "elfaDisabledByCreditGuard": guard,
        "elfaCycleMinutesAgo": elfa_minutes,
    })
}

/// Read everything, evaluate, write `health.json` and log the `health` line.
pub fn run(paths: &Paths, t: &Thresholds) -> Result<Value, StoreError> {
    let now = Utc::now();
    let read = |path: PathBuf| read_snapshot(&path, now);
    let inputs = Inputs {
        raw_status: read(paths.raw.join("status.json")),
        raw_catalog: read(paths.raw.join("catalog.json")),
        decoded_status: read(paths.decoded.join("status.json")),
        decoded_catalog: read(paths.decoded.join("catalog.json")),
        sync_status: read(paths.augment.join("status.json")),
        capture_status: read(paths.augment.join("status-capture.json")),
        disk_free_bytes: disk_free(paths.raw.parent().unwrap_or(&paths.raw)),
        previous: read(paths.out.clone()).value,
    };
    let health = evaluate(&inputs, t, now);
    let body = serde_json::to_vec_pretty(&health).map_err(|e| StoreError::Check(e.to_string()))?;
    write_atomic(&paths.out, &body).map_err(|e| StoreError::Check(e.to_string()))?;
    log(
        "health",
        Obj::new()
            .with("level", health["level"].clone())
            .with("warn", health["warn"].clone())
            .with("critical", health["critical"].clone())
            .with("decodedFiles", health["decoded"]["files"].clone())
            .with("diskFreeGb", health["disk"]["freeGb"].clone()),
    );
    Ok(health)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        parse_time(text).unwrap()
    }

    fn snap(value: Value, age_s: i64) -> Snapshot {
        Snapshot {
            bytes: value.to_string().len() as u64,
            value: Some(value),
            age_s: Some(age_s),
        }
    }

    fn ago(now: &str, minutes: i64) -> String {
        stamp(at(now) - chrono::Duration::minutes(minutes))
    }

    fn healthy(now: &str) -> Inputs {
        let file = |batch: i64, created: &str| json!({ "path": format!("tables/events/epoch=1/b{batch}.parquet"), "batch_id": batch.to_string(), "created_at": created });
        Inputs {
            raw_status: snap(
                json!({ "backfill": { "next": 430_863_136, "phase": "fetching" } }),
                30,
            ),
            raw_catalog: snap(json!({ "at": now, "files": [1, 2, 3] }), 5),
            decoded_status: snap(json!({ "transactions_processed": 7, "idle": false }), 10),
            decoded_catalog: snap(
                json!({ "files": [
                    file(1, &ago(now, 120)),
                    file(2, &ago(now, 10)),
                    file(2, &ago(now, 10)),
                    { "path": "tables/events/epoch=1/compact-x.parquet", "batch_id": "1", "created_at": ago(now, 5) },
                ] }),
                10,
            ),
            sync_status: snap(
                json!({ "sources": { "binance": { "errors": 0 } }, "stopped": false }),
                1_800,
            ),
            capture_status: snap(
                json!({ "startedAt": "2026-10-09T19:32:23.173Z", "running": true,
                    "candles": { "errors": 0 }, "elfa": { "errors": 0, "enabled": true, "disabledByCreditGuard": false },
                    "elfaAuto": { "errors": 0 }, "lastCycles": { "elfaAt": "2026-10-10T07:30:00Z" } }),
                40,
            ),
            disk_free_bytes: Some(3_000_000_000_000),
            previous: None,
        }
    }

    #[test]
    fn a_healthy_dataset_is_ok() {
        let now = "2026-10-10T08:00:00Z";
        let health = evaluate(&healthy(now), &Thresholds::default(), at(now));
        assert_eq!(health["level"], "ok", "{health}");
        assert_eq!(health["decoded"]["files"], 4);
        assert_eq!(health["decoded"]["batchesInWindow"], 1);
        assert_eq!(
            health["decoded"]["newestBatchAt"],
            "2026-10-10T07:50:00.000Z"
        );
        assert_eq!(health["disk"]["freeGb"], 3000.0);
    }

    #[test]
    fn each_check_warns_or_goes_critical() {
        let now = "2026-10-10T08:00:00Z";
        let mut inputs = healthy(now);
        let files: Vec<Value> = (0..10_001)
            .map(|i| json!({ "path": format!("p{i}"), "batch_id": "1", "created_at": "2026-10-10T05:00:00Z" }))
            .collect();
        inputs.decoded_catalog = snap(json!({ "files": files }), 10);
        inputs.disk_free_bytes = Some(400_000_000_000);
        inputs.capture_status.value.as_mut().unwrap()["elfa"]["disabledByCreditGuard"] =
            json!(true);
        inputs.capture_status.value.as_mut().unwrap()["lastCycles"]["elfaAt"] =
            json!("2026-10-10T03:26:09.343Z");
        inputs.capture_status.value.as_mut().unwrap()["elfaAuto"]["errors"] = json!(2);
        inputs.sync_status.age_s = Some(4 * 3600);
        let health = evaluate(&inputs, &Thresholds::default(), at(now));
        assert_eq!(health["level"], "critical");
        let warn = health["warn"].to_string();
        for needle in [
            "decoded files 10001",
            "disk free 400 GB",
            "credit guard",
            "Elfa hourly cycle 273 min",
            "2 new errors",
            "sync status.json 240 min",
        ] {
            assert!(warn.contains(needle), "{needle} missing from {warn}");
        }
        // Three hours without a decoded batch while the raw catalog moved on.
        assert!(
            health["critical"]
                .to_string()
                .contains("no decoded batch in 180 min")
        );
        inputs.disk_free_bytes = Some(50_000_000_000);
        inputs.raw_status.age_s = Some(3_700);
        let health = evaluate(&inputs, &Thresholds::default(), at(now));
        let critical = health["critical"].to_string();
        assert!(critical.contains("disk free 50 GB") && critical.contains("collector stalled"));
    }

    #[test]
    fn backfill_and_capture_errors_compare_with_the_previous_run() {
        let t = Thresholds::default();
        let first = evaluate(
            &healthy("2026-10-10T08:00:00Z"),
            &t,
            at("2026-10-10T08:00:00Z"),
        );
        assert_eq!(
            first["raw"]["backfill"]["nextChangedAt"],
            "2026-10-10T08:00:00.000Z"
        );
        // Same `next` two hours later: stalled.
        let mut later = healthy("2026-10-10T10:00:00Z");
        later.previous = Some(first.clone());
        later.capture_status.value.as_mut().unwrap()["lastCycles"]["elfaAt"] =
            json!("2026-10-10T09:30:00Z");
        let second = evaluate(&later, &t, at("2026-10-10T10:00:00Z"));
        assert!(
            second["warn"]
                .to_string()
                .contains("backfill.next unchanged for 120 min")
        );
        assert_eq!(
            second["raw"]["backfill"]["nextChangedAt"],
            "2026-10-10T08:00:00.000Z"
        );
        // It moved: the stamp resets; capture errors seen before do not warn again.
        later.raw_status.value.as_mut().unwrap()["backfill"]["next"] = json!(430_000_000);
        let mut previous = second.clone();
        previous["augment"]["capture"]["errors"] = json!(3);
        later.previous = Some(previous);
        later.capture_status.value.as_mut().unwrap()["candles"]["errors"] = json!(3);
        let third = evaluate(&later, &t, at("2026-10-10T10:00:00Z"));
        assert_eq!(third["level"], "ok", "{third}");
        // A complete backfill never stalls.
        let mut done = healthy("2026-10-10T12:00:00Z");
        done.raw_status.value.as_mut().unwrap()["backfill"]["phase"] = json!("complete");
        done.previous = Some(first);
        done.capture_status.value.as_mut().unwrap()["lastCycles"]["elfaAt"] =
            json!("2026-10-10T11:30:00Z");
        assert_eq!(
            evaluate(&done, &t, at("2026-10-10T12:00:00Z"))["level"],
            "ok"
        );
    }

    #[test]
    fn run_reads_the_snapshots_and_writes_health_json() {
        let root = std::env::temp_dir().join(format!("solos-health-{}", uuid::Uuid::new_v4()));
        let paths = Paths {
            raw: root.join("phoenix_raw"),
            decoded: root.join("phoenix_decoded/v1"),
            augment: root.join("augment"),
            out: root.join("health.json"),
        };
        for dir in [&paths.raw, &paths.decoded, &paths.augment] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let stamp = crate::jsonout::now();
        std::fs::write(
            paths.raw.join("status.json"),
            json!({ "at": stamp, "backfill": { "next": 5 } }).to_string(),
        )
        .unwrap();
        std::fs::write(
            paths.raw.join("catalog.json"),
            json!({ "at": stamp, "files": [] }).to_string(),
        )
        .unwrap();
        std::fs::write(
            paths.decoded.join("catalog.json"),
            json!({ "files": [] }).to_string(),
        )
        .unwrap();
        let health = run(&paths, &Thresholds::default()).unwrap();
        let written: Value = serde_json::from_slice(&std::fs::read(&paths.out).unwrap()).unwrap();
        assert_eq!(written, health);
        assert_eq!(health["decoded"]["files"], 0);
        assert!(health["disk"]["freeBytes"].as_u64().is_some());
        // The augment snapshots are missing: warned, not fatal.
        assert!(
            health["warn"]
                .to_string()
                .contains("status-capture.json unreadable")
        );
        let again = run(&paths, &Thresholds::default()).unwrap();
        assert_eq!(
            again["raw"]["backfill"]["nextChangedAt"],
            health["raw"]["backfill"]["nextChangedAt"]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn thresholds_load_from_a_block_with_defaults() {
        let t = load_thresholds(Some(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/health.json"
        )))
        .unwrap();
        assert_eq!(t, Thresholds::default());
        assert_eq!(
            load_thresholds(Some("/nonexistent/health.json")).unwrap(),
            Thresholds::default()
        );
    }
}
