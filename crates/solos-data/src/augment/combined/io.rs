//! Shared input/output helpers for the U2 hourly units (`phoenix-hours`,
//! `solana-perp-hours`, `pyth-hours`): input file walks, explicit Parquet list
//! references, the pinned in-memory DuckDB reader, durable hour-partitioned
//! writes, and per-table unit receipts.
//!
//! Copies of the small helpers `mapping.rs` already carries (walk, lock,
//! reader, provenance, memory cap) so the mapping unit stays untouched; a
//! follow-up can move both onto this module.

use crate::fsutil::{
    create_dir_all_durable, file_hash, sync_dir_after_rename, sync_tmp_file, tmp_path,
    write_durable,
};
use crate::store::{StoreError, sql_string};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// A walked input file, stored relative to its root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    /// Path relative to the walked root, `/` separators.
    pub relative: String,
    /// File size in bytes.
    pub size: u64,
}

/// Walk `root` recursively for Parquet files, sorted by relative path.
///
/// # Errors
///
/// A directory cannot be read or a file metadata fails.
pub fn walk_parquet(root: &Path) -> Result<Vec<FileRef>, StoreError> {
    let mut out = Vec::new();
    walk_dir(root, root, &mut out)?;
    out.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(out)
}

fn walk_dir(root: &Path, dir: &Path, out: &mut Vec<FileRef>) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| StoreError::Check(format!("cannot read {}: {error}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| StoreError::Check(format!("walk: {e}")))?;
        let path = entry.path();
        if path.is_dir() {
            walk_dir(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            let size = std::fs::metadata(&path)?.len();
            out.push(FileRef {
                relative: path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/"),
                size,
            });
        }
    }
    Ok(())
}

/// SHA-256 of the sorted `relative size` lines of a file list.
pub fn file_list_sha256(files: &[FileRef]) -> String {
    let mut text = String::new();
    for file in files {
        text.push_str(&file.relative);
        text.push(' ');
        text.push_str(&file.size.to_string());
        text.push('\n');
    }
    crate::fsutil::sha256_hex(text.as_bytes())
}

/// Absolute paths of a file list, for `read_parquet`.
pub fn absolute(root: &Path, files: &[FileRef]) -> Vec<String> {
    files
        .iter()
        .map(|file| root.join(&file.relative).to_string_lossy().to_string())
        .collect()
}

/// A `read_parquet([...])` table reference with an explicit file list. An
/// explicit list (not a glob) is the point: only files the run walked are read.
///
/// # Errors
///
/// The list is empty.
pub fn parquet_list(files: &[String]) -> Result<String, StoreError> {
    if files.is_empty() {
        return Err(StoreError::Check(
            "no parquet files to read (empty input list)".into(),
        ));
    }
    let list = files
        .iter()
        .map(|path| sql_string(path))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("read_parquet([{list}])"))
}

/// A single-file table reference rooted at `root`.
pub fn parquet_one(root: &Path, file: &FileRef) -> Result<String, StoreError> {
    parquet_list(&[root.join(&file.relative).to_string_lossy().to_string()])
}

/// The provenance object is required and must be a JSON object.
///
/// # Errors
///
/// The file cannot be read or is not a JSON object.
pub fn load_provenance(path: &Path) -> Result<Value, StoreError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| StoreError::Check(format!("--provenance: cannot read: {error}")))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| StoreError::Check(format!("--provenance: bad JSON: {error}")))?;
    if !value.is_object() {
        return Err(StoreError::Check(
            "--provenance must be a JSON object".into(),
        ));
    }
    Ok(value)
}

/// DuckDB memory cap: the shared parser, plus the plan's 32GB ceiling.
///
/// # Errors
///
/// The text is malformed or above the ceiling.
pub fn parse_memory_cap(text: &str) -> Result<String, StoreError> {
    let limit = crate::augment::hlfills::parse_memory_limit(text)?;
    let (digits, unit) = limit.split_at(limit.len().saturating_sub(2));
    let ok = match unit {
        "GB" => digits.parse::<u64>().is_ok_and(|gb| gb <= 32),
        "MB" => digits.parse::<u64>().is_ok_and(|mb| mb <= 32_768),
        _ => false,
    };
    if !ok {
        return Err(StoreError::Check("--memory must be at most 32GB".into()));
    }
    Ok(limit)
}

/// An exclusive process lock on the combined root (one writer per root).
///
/// # Errors
///
/// The lock file cannot be opened, or another process holds the lock.
pub fn lock_output(out: &Path) -> Result<nix::fcntl::Flock<std::fs::File>, StoreError> {
    let path = out.join("combined.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)?;
    match nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
        Ok(file) => Ok(file),
        Err((_, errno))
            if errno == nix::errno::Errno::EWOULDBLOCK || errno == nix::errno::Errno::EAGAIN =>
        {
            Err(StoreError::Check("combined output already owned".into()))
        }
        Err((_, errno)) => Err(std::io::Error::from(errno).into()),
    }
}

/// Open the pinned in-memory reader: the caller's thread and memory caps, spill
/// dir under the output root with a 20GB temp ceiling.
///
/// # Errors
///
/// The pinned DuckDB version check fails or a setting is refused.
pub fn open_reader(
    out: &Path,
    threads: u32,
    memory_limit: &str,
) -> Result<duckdb::Connection, StoreError> {
    let conn = crate::store::memory_connection(&threads.to_string(), memory_limit)?;
    crate::decoder::parquet::require_connection_duckdb(&conn)?;
    let temp = out.join(".tmp-duckdb");
    let _ = std::fs::remove_dir_all(&temp);
    create_dir_all_durable(&temp)?;
    conn.execute_batch(&format!(
        "SET threads = {threads}; SET memory_limit = {}; SET temp_directory = {};
         SET max_temp_directory_size = '20GB'",
        sql_string(memory_limit),
        sql_string(&temp.to_string_lossy())
    ))?;
    Ok(conn)
}

/// The `.tmp-duckdb` spill dir is removed when the run ends. Declared after the
/// connection's users so the connection closes first.
pub struct TempClean(PathBuf);

impl TempClean {
    /// Remove `out/.tmp-duckdb` when the value drops.
    pub fn new(out: &Path) -> Self {
        Self(out.join(".tmp-duckdb"))
    }
}

impl Drop for TempClean {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Parse the decoder catalog's `created_at`/`at` stamps:
/// `YYYY-MM-DDTHH:MM:SS[.mmm]Z` as UTC seconds.
///
/// # Errors
///
/// The stamp is malformed.
pub fn parse_catalog_stamp(text: &str) -> Result<i64, StoreError> {
    let trimmed = text.trim();
    let stamp = trimmed.strip_suffix('Z').unwrap_or(trimmed);
    let secs = stamp.split('.').next().unwrap_or(stamp);
    let dt = chrono::NaiveDateTime::parse_from_str(secs, "%Y-%m-%dT%H:%M:%S")
        .map_err(|error| StoreError::Check(format!("bad catalog stamp '{text}': {error}")))?;
    Ok(dt.and_utc().timestamp())
}

/// Write one hour's Parquet durably: `COPY` to a tmp file, fsync, rename, then
/// read the row count back. Returns (rows, bytes, sha256).
///
/// # Errors
///
/// The `COPY`, the durable write, or the read-back fails.
pub fn write_hour_parquet(
    conn: &duckdb::Connection,
    select: &str,
    final_path: &Path,
) -> Result<(u64, u64, String), StoreError> {
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent)?;
    }
    let tmp = tmp_path(final_path);
    let copy = crate::decoder::parquet::copy_statement(select, &tmp);
    conn.execute_batch(&copy)
        .map_err(|error| StoreError::Check(format!("parquet write: {error}")))?;
    sync_tmp_file(&tmp)?;
    let rows = count_parquet_rows(conn, &tmp)?;
    std::fs::rename(&tmp, final_path)?;
    sync_dir_after_rename(final_path)?;
    let bytes = std::fs::metadata(final_path)?.len();
    let sha256 = file_hash(final_path)?;
    Ok((rows, bytes, sha256))
}

/// Row count of a Parquet file, read back through the same connection.
///
/// # Errors
///
/// The file cannot be read as Parquet.
pub fn count_parquet_rows(conn: &duckdb::Connection, path: &Path) -> Result<u64, StoreError> {
    let sql = format!(
        "SELECT count(*) AS n FROM {}",
        parquet_list(&[path.to_string_lossy().to_string()])?
    );
    let rows = crate::store::query_rows(conn, &sql, &[])?;
    rows.first()
        .and_then(|row| row.int("n"))
        .map(|value| value.max(0) as u64)
        .ok_or_else(|| StoreError::Check("parquet read-back: no count".into()))
}

/// One sealed hour's facts, as the per-hour receipt and the run report carry them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HourOutput {
    /// Unix hour (its first second).
    pub hour: i64,
    /// Data rows in the file.
    pub rows: u64,
    /// File size in bytes.
    pub bytes: u64,
    /// SHA-256 of the file bytes.
    pub sha256: String,
    /// Path of the file relative to the output root.
    pub relative: String,
}

/// The per-hour receipt: what one hour wrote, over which inputs, with which
/// unit version. Idempotent reruns compare this against the sealed file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HourReceipt {
    /// The unit that ran.
    pub unit: String,
    /// Bumped on an output-shape change.
    pub unit_version: u32,
    /// The hour this file covers (unix hour, its first second).
    pub hour: i64,
    /// Table the file belongs to.
    pub table: String,
    /// Data rows in the file.
    pub rows: u64,
    /// File size in bytes.
    pub bytes: u64,
    /// SHA-256 of the file bytes.
    pub sha256: String,
    /// SHA-256 over the run's input facts (file list + watermark); equal inputs
    /// produce equal bytes, so a matching receipt skips the hour.
    pub inputs_sha256: String,
    /// UTC time the hour was sealed.
    pub sealed_utc: String,
}

/// The per-hour receipt path: `<root>/<table>/receipts/<YYYYMMDD>-<H>.json`.
pub fn hour_receipt_path(root: &Path, table: &str, hour: i64) -> PathBuf {
    root.join(table)
        .join("receipts")
        .join(format!("{}.json", hour_file_key(hour)))
}

/// `YYYYMMDD-H`, the hour file key used by file names and receipts.
#[must_use]
pub fn hour_file_key(hour: i64) -> String {
    let secs = crate::augment::combined::grid::hour_start(hour);
    let dt = chrono::DateTime::from_timestamp(secs, 0).expect("hour start is in range");
    dt.format("%Y%m%d-%H").to_string()
}

/// Write one hour's receipt durably.
///
/// # Errors
///
/// Serialization or the durable write fails.
pub fn write_hour_receipt(root: &Path, receipt: &HourReceipt) -> Result<(), StoreError> {
    let mut body =
        serde_json::to_vec_pretty(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    let path = hour_receipt_path(root, &receipt.table, receipt.hour);
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

/// Read one hour's receipt, when it exists.
pub fn read_hour_receipt(root: &Path, table: &str, hour: i64) -> Option<HourReceipt> {
    let text = std::fs::read_to_string(hour_receipt_path(root, table, hour)).ok()?;
    serde_json::from_str(&text).ok()
}

/// The build plan of one run: the hours to build (closed by the watermark,
/// minus the lockbox) and the counters the receipts record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HourPlan {
    /// Hours that may be written (locked for the first and last built hour).
    pub build_hours: Vec<i64>,
    /// Lockbox hours inside the requested range (never written).
    pub lockbox_dropped: u64,
    /// First unix hour of the requested range.
    pub first_hour: i64,
    /// Last unix hour of the requested range.
    pub requested_last: i64,
}

/// Plan the hours of one run: `[first, requested_last]` minus the lockbox.
/// The caller enforces watermark closure before this (a refusal, not a skip).
///
/// # Errors
///
/// The range is empty or ends before it starts.
pub fn plan_hours(
    first_hour: i64,
    requested_last: i64,
    lockbox_hours: &std::collections::BTreeSet<i64>,
) -> Result<HourPlan, StoreError> {
    if requested_last < first_hour {
        return Err(StoreError::Check("hour range ends before it starts".into()));
    }
    let build_hours: Vec<i64> = (first_hour..=requested_last)
        .filter(|hour| !lockbox_hours.contains(hour))
        .collect();
    let lockbox_dropped = (first_hour..=requested_last)
        .filter(|hour| lockbox_hours.contains(hour))
        .count() as u64;
    Ok(HourPlan {
        build_hours,
        lockbox_dropped,
        first_hour,
        requested_last,
    })
}

/// Split the plan into already-sealed hours (matching receipt + file hash) and
/// the pending hours this run must write.
pub fn skip_sealed_hours(
    out: &Path,
    table: &str,
    unit: &str,
    unit_version: u32,
    inputs_sha: &str,
    build_hours: &[i64],
) -> (Vec<HourOutput>, Vec<i64>) {
    let mut outputs = Vec::new();
    let mut pending = Vec::new();
    for hour in build_hours {
        let relative = hour_partition(*hour, table);
        let path = out.join(&relative);
        let receipt = read_hour_receipt(out, table, *hour).filter(|receipt| {
            receipt.unit == unit
                && receipt.unit_version == unit_version
                && receipt.inputs_sha256 == inputs_sha
                && path.is_file()
                && file_hash(&path).is_ok_and(|sha| sha == receipt.sha256)
        });
        match receipt {
            Some(receipt) => outputs.push(HourOutput {
                hour: *hour,
                rows: receipt.rows,
                bytes: receipt.bytes,
                sha256: receipt.sha256,
                relative,
            }),
            None => pending.push(*hour),
        }
    }
    (outputs, pending)
}

/// `<table>/date=YYYY-MM-DD/hour=YYYYMMDD-H.parquet`, relative to the root.
pub fn hour_partition(hour: i64, table: &str) -> String {
    let date = crate::augment::combined::grid::date_of_hour(hour);
    format!("{table}/date={date}/hour={}.parquet", hour_file_key(hour))
}

/// The facts of one finished run, as `finish_run` records them.
pub struct RunSummary<'a> {
    /// The unit that ran.
    pub unit: &'a str,
    /// Bumped on an output-shape change.
    pub unit_version: u32,
    /// The table the run wrote.
    pub table: &'a str,
    /// Sealed hour outputs of the run (written and verified hours).
    pub outputs: &'a [HourOutput],
    /// Hours skipped because a matching receipt and file already exist.
    pub hours_skipped: u64,
    /// Lockbox hours inside the requested range (never written).
    pub lockbox_dropped: u64,
    /// Distinct UTC hours in the lockbox exclusion set (864 for v2).
    pub lockbox_excluded: u64,
    /// SHA-256 of the lockbox bytes, as checked against `--lockbox-sha256`.
    pub lockbox_sha256: &'a str,
    /// Input watermark facts (file lists, max timestamp, closure).
    pub watermark: &'a Value,
    /// Digest of the input facts; equal inputs produce equal outputs.
    pub inputs_sha256: &'a str,
    /// SHA-256 over the unit's semantic config.
    pub config_sha256: &'a str,
    /// The operator's provenance object, verbatim.
    pub provenance: &'a Value,
    /// Run start, UTC.
    pub started_utc: &'a str,
}

/// The run summary every unit writes after its outputs are durable: the
/// per-run receipt under `<table>/` and one catalog entry per output file.
///
/// # Errors
///
/// The receipt write or the catalog rewrite fails.
pub fn finish_run(out: &Path, summary: &RunSummary<'_>) -> Result<(), StoreError> {
    let rows: u64 = summary.outputs.iter().map(|file| file.rows).sum();
    let receipt = crate::augment::combined::receipt::Receipt {
        unit: summary.unit.to_owned(),
        unit_version: summary.unit_version,
        rows,
        outputs: summary
            .outputs
            .iter()
            .map(|file| crate::augment::combined::receipt::OutputFile {
                path: file.relative.clone(),
                rows: file.rows,
                bytes: file.bytes,
                sha256: file.sha256.clone(),
            })
            .collect(),
        inputs: json!({
            "watermark": summary.watermark,
            "inputs_sha256": summary.inputs_sha256,
            "hours_written": summary.outputs.len(),
            "hours_skipped": summary.hours_skipped,
            "table": summary.table,
        }),
        lockbox_sha256: summary.lockbox_sha256.to_ascii_lowercase(),
        lockbox_hours_dropped: summary.lockbox_dropped,
        lockbox_hours_excluded: summary.lockbox_excluded,
        config_sha256: summary.config_sha256.to_owned(),
        provenance: summary.provenance.clone(),
        started_utc: summary.started_utc.to_owned(),
        finished_utc: crate::augment::combined::grid::now_rfc3339(),
    };
    crate::augment::combined::receipt::write(out, summary.table, &receipt)?;
    let mut cat = crate::augment::combined::catalog::load(out)?;
    for file in summary.outputs {
        cat.upsert(
            crate::augment::combined::catalog::FileEntry {
                path: file.relative.clone(),
                rows: file.rows,
                bytes: file.bytes,
                sha256: file.sha256.clone(),
                unit: summary.unit.to_owned(),
                unit_version: summary.unit_version,
            },
            summary.unit,
            summary.watermark.clone(),
        );
    }
    cat.save(out)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_stamps_parse_with_and_without_millis() {
        assert_eq!(
            parse_catalog_stamp("2026-11-01T18:58:17.164Z").unwrap(),
            1_793_559_497
        );
        assert_eq!(
            parse_catalog_stamp("2026-11-01T18:58:17Z").unwrap(),
            1_793_559_497
        );
        assert!(parse_catalog_stamp("nope").is_err());
    }

    #[test]
    fn hour_file_keys_match_the_partition_names() {
        assert_eq!(hour_file_key(1_792_022_400 / 3_600), "20261015-00");
        assert_eq!(hour_file_key(1_792_058_400 / 3_600), "20261015-10");
    }

    #[test]
    fn file_list_sha_is_order_stable_and_content_sensitive() {
        let files = vec![
            FileRef {
                relative: "a.parquet".into(),
                size: 3,
            },
            FileRef {
                relative: "b.parquet".into(),
                size: 5,
            },
        ];
        let mut other = files.clone();
        other.reverse();
        other.sort_by(|a, b| a.relative.cmp(&b.relative));
        assert_eq!(file_list_sha256(&files), file_list_sha256(&other));
        let mut bigger = files.clone();
        bigger[0].size = 4;
        assert_ne!(file_list_sha256(&files), file_list_sha256(&bigger));
    }

    #[test]
    fn parquet_list_refuses_an_empty_list() {
        assert!(parquet_list(&[]).is_err());
    }
}
