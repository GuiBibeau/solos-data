//! The batch: disk floor, checksum, decode, receipt, watermark.
//!
//! Hours run one at a time on one in-memory DuckDB connection. The watermark gains an hour
//! only after that hour's Parquet files and receipt are durable and the checks in
//! [`super::publish`] passed. A rerun of a verified hour leaves the files in place.

use super::checksum;
use super::publish::{self, Receipt};
use super::{SCHEMA, config_hash};
use crate::fsutil::{create_dir_all_durable, file_hash, write_durable};
use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// One offline run.
pub struct Request<'a> {
    /// Directory of `YYYYMMDD/H.lz4` (`node_fills_by_block/hourly`).
    pub input: &'a Path,
    /// Output root. `train/` and `forward/` are created under it.
    pub out: &'a Path,
    /// `SHA256SUMS` or a JSON manifest covering the hourly files.
    pub checksums: &'a Path,
    /// Refuse to start, or stop before the next hour, when free space is below this many bytes.
    pub min_free_bytes: u64,
    /// DuckDB threads. `1..=256`.
    pub threads: u32,
    /// DuckDB `memory_limit` (`8GB`).
    pub memory_limit: String,
    /// Recorded in each new receipt. [`super::git_sha`] is the CLI's value.
    pub git_sha: String,
}

/// Why `run` returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// Every discovered hour is verified or was already verified.
    Done,
    /// Free space is below [`Request::min_free_bytes`]. Hours already verified stay verified.
    DiskFloor,
}

/// The run's outcome. Printed by the CLI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// [`Stop::Done`] or [`Stop::DiskFloor`].
    pub stop: Stop,
    /// Hours newly verified on this call.
    pub hours_verified: u64,
    /// Hours already verified and left unchanged.
    pub hours_skipped: u64,
    /// Rows in the hours verified on this call.
    pub rows: u64,
    /// Free bytes observed on the last check.
    pub free_bytes: Option<u64>,
    /// The floor this run used.
    pub floor_bytes: u64,
    /// DuckDB `threads` actually set, or the request when no hour was opened.
    pub threads: String,
    /// DuckDB `memory_limit` actually set, or the request when no hour was opened.
    pub memory_limit: String,
}

impl Report {
    /// Process exit code: `0` finished, `2` stopped or refused on the disk floor.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        match self.stop {
            Stop::Done => 0,
            Stop::DiskFloor => 2,
        }
    }

    /// JSON object for stdout.
    #[must_use]
    pub fn to_json(&self) -> String {
        crate::jsonout::Obj::new()
            .with("command", "augment hl-fills")
            .with(
                "stop",
                match self.stop {
                    Stop::Done => "done",
                    Stop::DiskFloor => "disk floor",
                },
            )
            .with("hoursVerified", self.hours_verified)
            .with("hoursSkipped", self.hours_skipped)
            .with("rows", self.rows)
            .with("freeBytes", json_u64(self.free_bytes))
            .with("floorBytes", self.floor_bytes)
            .with("threads", self.threads.clone())
            .with("memoryLimit", self.memory_limit.clone())
            .to_json()
    }
}

fn json_u64(value: Option<u64>) -> serde_json::Value {
    value.map_or(serde_json::Value::Null, serde_json::Value::from)
}

/// Decode every hourly file under `request.input`.
///
/// `free_bytes` is called once before any hour and once before each hour. The CLI passes the
/// free space of the output filesystem. A value below the floor stops the run without failing
/// the hours already written.
///
/// # Errors
///
/// A checksum is missing or does not match, a file is short or corrupt, a count or range check
/// fails, or the output lock is already held. The watermark does not gain that hour.
pub fn run(
    request: &Request<'_>,
    mut free_bytes: impl FnMut(&Path) -> Result<u64, String>,
) -> Result<Report, StoreError> {
    if !(1..=256).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=256".into()));
    }
    let memory_limit = super::parse_memory_limit(&request.memory_limit)?;
    if !request.input.is_dir() {
        return Err(StoreError::Check(
            "--input is not a directory of YYYYMMDD/H.lz4".into(),
        ));
    }
    if !request.checksums.is_file() {
        return Err(StoreError::Check(
            "--checksums is not a SHA256SUMS file or a JSON manifest".into(),
        ));
    }
    create_dir_all_durable(request.out)?;
    let _lock = lock_output(request.out)?;
    let _staging = ClearStaging(request.out);
    let mut report = Report {
        stop: Stop::Done,
        hours_verified: 0,
        hours_skipped: 0,
        rows: 0,
        free_bytes: None,
        floor_bytes: request.min_free_bytes,
        threads: request.threads.to_string(),
        memory_limit: memory_limit.clone(),
    };
    match space(request.out, request.min_free_bytes, &mut free_bytes)? {
        Space::Floor(free) => {
            report.stop = Stop::DiskFloor;
            report.free_bytes = Some(free);
            return Ok(report);
        }
        Space::Ok(free) => report.free_bytes = Some(free),
    }
    let checksums = checksum::load_checksums(request.checksums)?;
    let hours = list_hours(request.input)?;
    let mut watermark = load_watermark(request.out)?;
    let hash = config_hash();
    let git = if request.git_sha.trim().is_empty() {
        "unknown".to_owned()
    } else {
        request.git_sha.trim().to_owned()
    };
    let conn = if hours.is_empty() {
        None
    } else {
        let conn = publish::open_writer(request.out, request.threads, &memory_limit)?;
        report.threads = publish::current_setting(&conn, "threads")?;
        report.memory_limit = publish::current_setting(&conn, "memory_limit")?;
        Some(conn)
    };
    for hour in &hours {
        match space(request.out, request.min_free_bytes, &mut free_bytes)? {
            Space::Floor(free) => {
                report.stop = Stop::DiskFloor;
                report.free_bytes = Some(free);
                return Ok(report);
            }
            Space::Ok(free) => report.free_bytes = Some(free),
        }
        let key = hour.key();
        let Some(expected) = checksums.get(&key) else {
            return Err(StoreError::Check(format!("{key}: no checksum")));
        };
        let bytes = std::fs::read(&hour.path)?;
        let actual = crate::fsutil::sha256_hex(&bytes);
        if actual != *expected {
            return Err(StoreError::Check(format!(
                "{key}: sha256 {actual} != manifest {expected}"
            )));
        }
        if already_done(request.out, &key, &actual, &hash, &watermark)? {
            report.hours_skipped += 1;
            continue;
        }
        let Some(conn) = conn.as_ref() else {
            return Err(StoreError::Check(
                "internal: writer connection missing".into(),
            ));
        };
        let output = publish::publish_hour(conn, request.out, &key, &bytes, &actual, &git, &hash)
            .map_err(|error| StoreError::Check(format!("{key}: {error}")))?;
        watermark.insert(key);
        write_watermark(request.out, &watermark)?;
        report.hours_verified += 1;
        report.rows += output.output_rows;
    }
    Ok(report)
}

enum Space {
    Ok(u64),
    Floor(u64),
}

fn space(
    out: &Path,
    floor: u64,
    free_bytes: &mut impl FnMut(&Path) -> Result<u64, String>,
) -> Result<Space, StoreError> {
    let free = free_bytes(out)
        .map_err(|error| StoreError::Check(format!("free disk unknown: {error}")))?;
    if free < floor {
        Ok(Space::Floor(free))
    } else {
        Ok(Space::Ok(free))
    }
}

struct ClearStaging<'a>(&'a Path);

impl Drop for ClearStaging<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.join(".staging"));
    }
}

fn lock_output(out: &Path) -> Result<nix::fcntl::Flock<std::fs::File>, StoreError> {
    let path = out.join("hl-fills.lock");
    let file = OpenOptions::new()
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
            Err(StoreError::Check("hl-fills output already owned".into()))
        }
        Err((_, errno)) => Err(std::io::Error::from(errno).into()),
    }
}

struct HourRef {
    date: String,
    hour: u32,
    path: PathBuf,
}

impl HourRef {
    fn key(&self) -> String {
        format!("{}/{}", self.date, self.hour)
    }
}

fn list_hours(input: &Path) -> Result<Vec<HourRef>, StoreError> {
    let mut hours = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in std::fs::read_dir(input)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(date) = name.to_str() else { continue };
        if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) || !entry.path().is_dir() {
            continue;
        }
        for file in std::fs::read_dir(entry.path())? {
            let file = file?;
            let path = file.path();
            if !path.is_file() {
                continue;
            }
            let Some(stem) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(key) = checksum::hour_key_of_path(&format!("{date}/{stem}")) else {
                continue;
            };
            let hour: u32 = key
                .rsplit('/')
                .next()
                .and_then(|h| h.parse().ok())
                .ok_or_else(|| StoreError::Check(format!("bad hour {key}")))?;
            if !seen.insert(key.clone()) {
                return Err(StoreError::Check(format!("duplicate input hour {key}")));
            }
            hours.push(HourRef {
                date: date.to_owned(),
                hour,
                path,
            });
        }
    }
    hours.sort_by(|a, b| a.date.cmp(&b.date).then(a.hour.cmp(&b.hour)));
    Ok(hours)
}

#[derive(Serialize, Deserialize)]
struct Watermark {
    schema: String,
    hours: BTreeSet<String>,
}

fn watermark_path(out: &Path) -> PathBuf {
    out.join("watermark.json")
}

fn load_watermark(out: &Path) -> Result<BTreeSet<String>, StoreError> {
    let path = watermark_path(out);
    if !path.is_file() {
        return Ok(BTreeSet::new());
    }
    let text = std::fs::read_to_string(&path)?;
    let watermark: Watermark =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(format!("watermark: {e}")))?;
    if watermark.schema != SCHEMA {
        return Err(StoreError::Check(format!(
            "watermark schema is {}, this command writes {SCHEMA}",
            watermark.schema
        )));
    }
    Ok(watermark.hours)
}

fn write_watermark(out: &Path, hours: &BTreeSet<String>) -> Result<(), StoreError> {
    let watermark = Watermark {
        schema: SCHEMA.to_owned(),
        hours: hours.clone(),
    };
    let mut body = serde_json::to_vec(&watermark).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    #[cfg(test)]
    fire_before_watermark();
    write_durable(&watermark_path(out), &body)?;
    Ok(())
}

#[cfg(test)]
thread_local! {
    static BEFORE_WATERMARK: std::cell::Cell<Option<fn()>> = const { std::cell::Cell::new(None) };
}

/// Test hook: run `hook` immediately before the watermark's durable write.
#[cfg(test)]
pub(crate) fn on_before_watermark(hook: fn()) {
    BEFORE_WATERMARK.with(|slot| slot.set(Some(hook)));
}

#[cfg(test)]
fn fire_before_watermark() {
    if let Some(hook) = BEFORE_WATERMARK.with(|slot| slot.take()) {
        hook();
    }
}

#[cfg(test)]
pub(crate) fn clear_before_watermark() {
    BEFORE_WATERMARK.with(|slot| slot.set(None));
}

fn already_done(
    out: &Path,
    key: &str,
    sha: &str,
    config: &str,
    watermark: &BTreeSet<String>,
) -> Result<bool, StoreError> {
    if !watermark.contains(key) {
        return Ok(false);
    }
    let Some(receipt) = publish::read_receipt(out, key) else {
        return Ok(false);
    };
    if !receipt_matches(&receipt, key, sha, config) {
        return Ok(false);
    }
    let mut rows = 0u64;
    for output in &receipt.outputs {
        let path = out.join(&output.path);
        if output
            .path
            .split('/')
            .any(|part| part.is_empty() || part == "..")
        {
            return Ok(false);
        }
        if !path.is_file() || file_hash(&path)? != output.sha256 {
            return Ok(false);
        }
        rows += output.rows;
    }
    Ok(rows == receipt.output_rows && rows == receipt.input_fills)
}

fn receipt_matches(receipt: &Receipt, key: &str, sha: &str, config: &str) -> bool {
    receipt.schema == SCHEMA
        && receipt.hour == key
        && receipt.input_sha256 == sha
        && receipt.config_hash == config
        && receipt.output_rows == receipt.input_fills
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hours_sort_by_number_not_lexicographically() {
        let root = std::env::temp_dir().join(format!(
            "hl-hours-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("20261022")).unwrap();
        std::fs::write(root.join("20261022/10.lz4"), b"x").unwrap();
        std::fs::write(root.join("20261022/2.lz4"), b"y").unwrap();
        let hours = list_hours(&root).unwrap();
        assert_eq!(
            hours.iter().map(HourRef::key).collect::<Vec<_>>(),
            vec!["20261022/2".to_owned(), "20261022/10".to_owned()]
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
