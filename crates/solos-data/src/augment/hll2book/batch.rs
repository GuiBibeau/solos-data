//! The batch: disk floor, manifest, decode, receipt, watermark, then optional raw delete.
//!
//! Hours run one at a time on one in-memory DuckDB connection. The watermark gains an hour
//! only after that hour's Parquet files and receipt are durable and the checks in
//! [`super::publish`] passed. Raw `.lz4` files are deleted only after that write, and only
//! when the request asked for it. A file whose size does not match the manifest is left
//! untouched: the downloader has not finished it.

use super::checksum::{self, Entry};
use super::publish::{self, FileBytes, HourJob, InputFile, Receipt};
use super::{DROP_SCHEMA, SCHEMA, config_hash, s3_url};
use crate::fsutil::{create_dir_all_durable, file_hash, write_durable};
use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// One offline run.
pub struct Request<'a> {
    /// Directory of `YYYYMMDD/H/<COIN>.lz4` (or the `market_data/.../l2Book/` layout).
    pub input: &'a Path,
    /// Output root. `train/` and `forward/` are created under it.
    pub out: &'a Path,
    /// JSON manifest or a `sha256  size  path` listing. Every book entry needs a size.
    pub checksums: &'a Path,
    /// Stop before the next hour when free space is below this many bytes.
    pub min_free_bytes: u64,
    /// DuckDB threads. `1..=256`.
    pub threads: u32,
    /// DuckDB `memory_limit` (`8GB`).
    pub memory_limit: String,
    /// Recorded in each new receipt.
    pub git_sha: String,
    /// When set, only these coins. Other coins are ignored, including on disk.
    pub coins: Option<BTreeSet<String>>,
    /// First source date, `YYYYMMDD`, inclusive.
    pub since: Option<String>,
    /// Last source date, `YYYYMMDD`, inclusive.
    pub until: Option<String>,
    /// Delete verified raw files after the watermark entry is durable.
    pub drop_raw: bool,
}

/// Why `run` returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// Every complete hour is verified or was already verified. Incomplete hours are pending.
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
    /// Hours skipped as incomplete: a manifest file is missing on disk or the wrong size, or
    /// an in-scope `.lz4` on disk has no manifest row yet (those are also in
    /// [`Report::hours_uncovered`]).
    pub hours_pending: u64,
    /// Hours skipped because an in-scope `.lz4` on disk has no manifest row (a subset of
    /// [`Report::hours_pending`]). The manifest builder covers those hours; the run does not
    /// abort on them.
    pub hours_uncovered: u64,
    /// Rows in the hours verified on this call.
    pub rows: u64,
    /// Identical re-sends dropped by the duplicate rule in the verified hours.
    pub dup_identical: u64,
    /// Extra rows kept for a distinct book at a repeated `time_ms`.
    pub dup_distinct: u64,
    /// Raw bytes deleted on this call.
    pub raw_bytes_dropped: u64,
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
    /// Process exit code: `0` finished, `2` stopped on the disk floor.
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
            .with("command", "augment hl-l2book")
            .with(
                "stop",
                match self.stop {
                    Stop::Done => "done",
                    Stop::DiskFloor => "disk floor",
                },
            )
            .with("hoursVerified", self.hours_verified)
            .with("hoursSkipped", self.hours_skipped)
            .with("hoursPending", self.hours_pending)
            .with("hoursUncovered", self.hours_uncovered)
            .with("rows", self.rows)
            .with("dupIdentical", self.dup_identical)
            .with("dupDistinct", self.dup_distinct)
            .with("rawBytesDropped", self.raw_bytes_dropped)
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

/// Decode every complete hour under `request.input`.
///
/// # Errors
///
/// A size-matched file's sha256 does not match the manifest, a frame is short or corrupt, a
/// count or sample check fails, or the output lock is already held. The watermark does not
/// gain that hour. An incomplete hour is not an error: a short or missing file, or a finished
/// `.lz4` with no manifest row, is counted as pending and the run continues.
pub fn run(
    request: &Request<'_>,
    mut free_bytes: impl FnMut(&Path) -> Result<u64, String>,
) -> Result<Report, StoreError> {
    if !(1..=256).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=256".into()));
    }
    let memory_limit = super::parse_memory_limit(&request.memory_limit)?;
    if let (Some(since), Some(until)) = (&request.since, &request.until)
        && since > until
    {
        return Err(StoreError::Check("--since is after --until".into()));
    }
    if !request.input.is_dir() {
        return Err(StoreError::Check(
            "--input is not a directory of YYYYMMDD/H/<COIN>.lz4".into(),
        ));
    }
    if !request.checksums.is_file() {
        return Err(StoreError::Check(
            "--checksums is not a manifest (JSON with path, sha256, and size)".into(),
        ));
    }
    create_dir_all_durable(request.out)?;
    let _lock = lock_output(request.out)?;
    let _staging = ClearStaging(request.out);
    let mut report = Report {
        stop: Stop::Done,
        hours_verified: 0,
        hours_skipped: 0,
        hours_pending: 0,
        hours_uncovered: 0,
        rows: 0,
        dup_identical: 0,
        dup_distinct: 0,
        raw_bytes_dropped: 0,
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
    let entries = checksum::load_entries(request.checksums)?;
    let on_disk = scan_books(request.input)?;
    // A finished `.lz4` in scope with no manifest row makes that hour pending, never an abort:
    // the sha256 manifest lags the downloader, and the builder subcommand closes the gap.
    // `.part` files never match a book path, so a downloading file is invisible here.
    let covered: BTreeSet<(String, u32, String)> = entries
        .iter()
        .map(|entry| (entry.date.clone(), entry.hour, entry.coin.clone()))
        .collect();
    let mut uncovered: BTreeSet<String> = BTreeSet::new();
    for (date, hour, coin) in &on_disk {
        if !in_scope(request, date, coin) {
            continue;
        }
        if !covered.contains(&(date.clone(), *hour, coin.clone())) {
            uncovered.insert(format!("{date}/{hour}"));
        }
    }
    let hours = group_hours(&entries, request);
    let hour_keys: BTreeSet<String> = hours.iter().map(|hour| hour.key.clone()).collect();
    let mut watermark = load_watermark(request.out)?;
    let hash = config_hash();
    let git = if request.git_sha.trim().is_empty() {
        "unknown".to_owned()
    } else {
        request.git_sha.trim().to_owned()
    };
    let mut conn: Option<duckdb::Connection> = None;
    for hour in hours {
        let key = hour.key.clone();
        if uncovered.contains(&key) {
            // Pending, not failed: the hour is re-run once the manifest covers its coins.
            report.hours_pending += 1;
            report.hours_uncovered += 1;
            continue;
        }
        let ready = match locate_hour(request.input, &hour.entries)? {
            HourFiles::Partial => {
                if watermark.contains(&key) {
                    let identity = identity_of(&hour.entries);
                    if already_done(request.out, &key, &identity, &hash, &watermark)? {
                        if request.drop_raw {
                            report.raw_bytes_dropped += drop_raw(request.input, request.out, &key)?;
                        }
                        report.hours_skipped += 1;
                        continue;
                    }
                    return Err(StoreError::Check(format!(
                        "{key}: verified hour is incomplete and its output does not match the receipt"
                    )));
                }
                report.hours_pending += 1;
                continue;
            }
            HourFiles::Ready(ready) => ready,
        };
        match space(request.out, request.min_free_bytes, &mut free_bytes)? {
            Space::Floor(free) => {
                report.stop = Stop::DiskFloor;
                report.free_bytes = Some(free);
                return Ok(report);
            }
            Space::Ok(free) => report.free_bytes = Some(free),
        }
        let loaded = match read_ready(&ready)? {
            ReadReady::Partial => {
                report.hours_pending += 1;
                continue;
            }
            ReadReady::Ready(loaded) => loaded,
        };
        let identity = identity_of_inputs(&loaded);
        if already_done(request.out, &key, &identity, &hash, &watermark)? {
            if request.drop_raw {
                report.raw_bytes_dropped += drop_raw(request.input, request.out, &key)?;
            }
            report.hours_skipped += 1;
            continue;
        }
        if conn.is_none() {
            let opened = publish::open_writer(request.out, request.threads, &memory_limit)?;
            report.threads = publish::current_setting(&opened, "threads")?;
            report.memory_limit = publish::current_setting(&opened, "memory_limit")?;
            conn = Some(opened);
        }
        let Some(conn) = conn.as_ref() else {
            return Err(StoreError::Check(
                "internal: writer connection missing".into(),
            ));
        };
        let files: Vec<FileBytes<'_>> = loaded
            .iter()
            .map(|file| FileBytes {
                coin: file.coin.as_str(),
                relative: file.relative.as_str(),
                sha256: file.sha256.as_str(),
                bytes: file.bytes.as_slice(),
            })
            .collect();
        let job = HourJob {
            hour_key: &key,
            files: &files,
            git_sha: &git,
            config_hash: &hash,
        };
        let output = publish::publish_hour(conn, request.out, &job)
            .map_err(|error| StoreError::Check(format!("{key}: {error}")))?;
        if output.input_snapshots != output.output_rows + output.dup_identical {
            return Err(StoreError::Check(format!(
                "{key}: hour total {} != kept {} + identical re-sends {}",
                output.input_snapshots, output.output_rows, output.dup_identical
            )));
        }
        if output.inputs.is_empty() && output.input_snapshots != 0 {
            return Err(StoreError::Check(format!(
                "{key}: snapshots were counted but no input was recorded"
            )));
        }
        watermark.insert(key.clone());
        write_watermark(request.out, &watermark)?;
        report.hours_verified += 1;
        report.rows += output.output_rows;
        report.dup_identical += output.dup_identical;
        report.dup_distinct += output.dup_distinct;
        if request.drop_raw {
            report.raw_bytes_dropped += drop_raw(request.input, request.out, &key)?;
        }
    }
    // An hour whose on-disk coins have no manifest row at all never entered the loop above.
    for key in &uncovered {
        if !hour_keys.contains(key) {
            report.hours_pending += 1;
            report.hours_uncovered += 1;
        }
    }
    Ok(report)
}

struct LoadedFile {
    coin: String,
    relative: String,
    sha256: String,
    bytes: Vec<u8>,
}

enum ReadReady {
    Partial,
    Ready(Vec<LoadedFile>),
}

fn read_ready(ready: &[ReadyFile]) -> Result<ReadReady, StoreError> {
    let mut loaded = Vec::with_capacity(ready.len());
    for file in ready {
        let body = std::fs::read(&file.path)?;
        if u64::try_from(body.len()).ok() != Some(file.entry.bytes) {
            return Ok(ReadReady::Partial);
        }
        let actual = crate::fsutil::sha256_hex(&body);
        if actual != file.entry.sha256 {
            return Err(StoreError::Check(format!(
                "{}/{}/{}: sha256 {actual} != manifest {}",
                file.entry.date, file.entry.hour, file.entry.coin, file.entry.sha256
            )));
        }
        loaded.push(LoadedFile {
            coin: file.entry.coin.clone(),
            relative: file.relative.clone(),
            sha256: actual,
            bytes: body,
        });
    }
    Ok(ReadReady::Ready(loaded))
}

fn identity_of(entries: &[Entry]) -> String {
    let inputs: Vec<InputFile> = entries
        .iter()
        .map(|entry| InputFile {
            coin: entry.coin.clone(),
            path: String::new(),
            sha256: entry.sha256.clone(),
            bytes: entry.bytes,
            snapshots: 0,
        })
        .collect();
    publish::input_identity(&inputs)
}

fn identity_of_inputs(files: &[LoadedFile]) -> String {
    let inputs: Vec<InputFile> = files
        .iter()
        .map(|file| InputFile {
            coin: file.coin.clone(),
            path: file.relative.clone(),
            sha256: file.sha256.clone(),
            bytes: u64::try_from(file.bytes.len()).unwrap_or(0),
            snapshots: 0,
        })
        .collect();
    publish::input_identity(&inputs)
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
        let _ = std::fs::remove_dir_all(self.0.join(".tmp-duckdb"));
    }
}

fn lock_output(out: &Path) -> Result<nix::fcntl::Flock<std::fs::File>, StoreError> {
    let path = out.join("hl-l2book.lock");
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
            Err(StoreError::Check("hl-l2book output already owned".into()))
        }
        Err((_, errno)) => Err(std::io::Error::from(errno).into()),
    }
}

fn in_scope(request: &Request<'_>, date: &str, coin: &str) -> bool {
    if request
        .since
        .as_ref()
        .is_some_and(|since| date < since.as_str())
    {
        return false;
    }
    if request
        .until
        .as_ref()
        .is_some_and(|until| date > until.as_str())
    {
        return false;
    }
    if request
        .coins
        .as_ref()
        .is_some_and(|coins| !coins.contains(coin))
    {
        return false;
    }
    true
}

struct HourGroup {
    key: String,
    date: String,
    hour: u32,
    entries: Vec<Entry>,
}

fn group_hours(entries: &[Entry], request: &Request<'_>) -> Vec<HourGroup> {
    let mut map: BTreeMap<(String, u32), Vec<Entry>> = BTreeMap::new();
    for entry in entries {
        if !in_scope(request, &entry.date, &entry.coin) {
            continue;
        }
        map.entry((entry.date.clone(), entry.hour))
            .or_default()
            .push(entry.clone());
    }
    let mut hours: Vec<HourGroup> = map
        .into_iter()
        .map(|((date, hour), mut entries)| {
            entries.sort_by(|a, b| a.coin.cmp(&b.coin));
            let key = entries
                .first()
                .map(Entry::hour_key)
                .unwrap_or_else(|| format!("{date}/{hour}"));
            HourGroup {
                key,
                date,
                hour,
                entries,
            }
        })
        .collect();
    hours.sort_by(|a, b| a.date.cmp(&b.date).then(a.hour.cmp(&b.hour)));
    hours
}

struct ReadyFile {
    entry: Entry,
    path: PathBuf,
    relative: String,
}

enum HourFiles {
    Partial,
    Ready(Vec<ReadyFile>),
}

fn locate_hour(input: &Path, entries: &[Entry]) -> Result<HourFiles, StoreError> {
    let mut ready = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(path) = locate(input, entry)? else {
            return Ok(HourFiles::Partial);
        };
        let len = std::fs::metadata(&path)?.len();
        if len != entry.bytes {
            return Ok(HourFiles::Partial);
        }
        let relative = relative_to(input, &path)?;
        ready.push(ReadyFile {
            entry: entry.clone(),
            path,
            relative,
        });
    }
    Ok(HourFiles::Ready(ready))
}

fn locate(input: &Path, entry: &Entry) -> Result<Option<PathBuf>, StoreError> {
    let mut found = Vec::new();
    if let Some(direct) = safe_join(input, &entry.manifest_path)
        && direct.is_file()
    {
        found.push(direct);
    }
    for candidate in checksum::candidate_paths(input, &entry.date, entry.hour, &entry.coin) {
        if candidate.is_file() && !found.iter().any(|path| path == &candidate) {
            found.push(candidate);
        }
    }
    match found.len() {
        0 => Ok(None),
        1 => Ok(Some(found.remove(0))),
        _ => {
            if let Some(direct) = safe_join(input, &entry.manifest_path)
                && found.iter().any(|path| path == &direct)
            {
                return Ok(Some(direct));
            }
            Err(StoreError::Check(format!(
                "{}/{}/{}: more than one file matches the manifest",
                entry.date, entry.hour, entry.coin
            )))
        }
    }
}

fn safe_join(input: &Path, relative: &str) -> Option<PathBuf> {
    if relative.is_empty()
        || relative.starts_with('/')
        || relative
            .split(['/', '\\'])
            .any(|part| part.is_empty() || part == "..")
    {
        return None;
    }
    let path = input.join(relative);
    crate::fsutil::inside(input, &path).then_some(path)
}

fn relative_to(input: &Path, path: &Path) -> Result<String, StoreError> {
    let relative = path
        .strip_prefix(input)
        .map_err(|_| StoreError::Check(format!("{} is outside the input dir", path.display())))?;
    let text = relative.to_string_lossy().replace('\\', "/");
    if text
        .split('/')
        .any(|part| part.is_empty() || part == ".." || part == ".")
    {
        return Err(StoreError::Check(format!("{text} escapes the input dir")));
    }
    Ok(text)
}

fn scan_books(input: &Path) -> Result<Vec<(String, u32, String)>, StoreError> {
    let mut out = Vec::new();
    walk_books(input, input, &mut out)?;
    Ok(out)
}

fn walk_books(
    input: &Path,
    dir: &Path,
    out: &mut Vec<(String, u32, String)>,
) -> Result<(), StoreError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') {
                continue;
            }
            walk_books(input, &path, out)?;
            continue;
        }
        if !kind.is_file() && !kind.is_symlink() {
            continue;
        }
        let Ok(relative) = path.strip_prefix(input) else {
            continue;
        };
        if let Some(book) = checksum::book_of_path(&relative.to_string_lossy()) {
            out.push(book);
        }
    }
    Ok(())
}

/// Canonical path of `path` when it is inside `input`.
///
/// The returned path is what a delete removes. A symlink whose target leaves `input` is refused,
/// so the target is not deleted.
///
/// # Errors
///
/// The path cannot be canonicalized, or its canonical path is outside `input`.
pub(crate) fn checked_input_file(input: &Path, path: &Path) -> Result<PathBuf, StoreError> {
    let root = input
        .canonicalize()
        .map_err(|error| StoreError::Check(format!("input dir: {error}")))?;
    let file = path
        .canonicalize()
        .map_err(|error| StoreError::Check(format!("{}: {error}", path.display())))?;
    if !file.starts_with(&root) {
        return Err(StoreError::Check(format!(
            "refusing to delete {}: outside the input dir",
            file.display()
        )));
    }
    Ok(file)
}

fn drop_raw(input: &Path, out: &Path, hour_key: &str) -> Result<u64, StoreError> {
    let Some(receipt) = publish::read_receipt(out, hour_key) else {
        return Err(StoreError::Check(format!(
            "{hour_key}: receipt missing, raw files were not deleted"
        )));
    };
    let (date, hour) = hour_parts(hour_key)?;
    let mut pending = Vec::new();
    for file in &receipt.inputs {
        let path = under_input(input, &file.path)?;
        if !path.exists() {
            continue;
        }
        let len = std::fs::metadata(&path)?.len();
        if len != file.bytes {
            return Err(StoreError::Check(format!(
                "refusing to delete {}: size {len} != receipt {}",
                file.path, file.bytes
            )));
        }
        let actual = file_hash(&path)?;
        if actual != file.sha256 {
            return Err(StoreError::Check(format!(
                "refusing to delete {}: sha256 changed",
                file.path
            )));
        }
        let target = checked_input_file(input, &path)?;
        pending.push((target, len));
    }
    // The re-download record is durable before any unlink. A crash while
    // deleting still leaves `drops/YYYYMMDD/H.json`, and a retry skips files
    // that are already gone.
    write_drop_record(out, hour_key, date, hour, &receipt)?;
    let mut removed = 0u64;
    for (target, len) in pending {
        std::fs::remove_file(&target)?;
        removed += len;
        #[cfg(test)]
        if take_drop_fault() {
            return Err(StoreError::Check("injected drop stop".into()));
        }
    }
    Ok(removed)
}

fn hour_parts(hour_key: &str) -> Result<(&str, u32), StoreError> {
    let (date, hour) = hour_key
        .split_once('/')
        .ok_or_else(|| StoreError::Check(format!("bad hour {hour_key}")))?;
    let hour: u32 = hour
        .parse()
        .map_err(|_| StoreError::Check(format!("bad hour {hour_key}")))?;
    Ok((date, hour))
}

fn under_input(input: &Path, relative: &str) -> Result<PathBuf, StoreError> {
    if relative.is_empty()
        || relative.starts_with('/')
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == ".." || part == ".")
    {
        return Err(StoreError::Check(format!(
            "refusing to delete {relative}: outside the input dir"
        )));
    }
    Ok(input.join(relative))
}

fn write_drop_record(
    out: &Path,
    hour_key: &str,
    date: &str,
    hour: u32,
    receipt: &Receipt,
) -> Result<(), StoreError> {
    let files: Vec<DropFile> = receipt
        .inputs
        .iter()
        .map(|file| DropFile {
            path: file.path.clone(),
            bytes: file.bytes,
            sha256: file.sha256.clone(),
            s3: s3_url(date, hour, &file.coin),
        })
        .collect();
    let record = DropRecord {
        schema: DROP_SCHEMA.to_owned(),
        hour: hour_key.to_owned(),
        files,
    };
    let mut body = serde_json::to_vec(&record).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    let path = under_out_drop(out, &format!("drops/{hour_key}.json"))?;
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

#[derive(Serialize)]
struct DropRecord {
    schema: String,
    hour: String,
    files: Vec<DropFile>,
}

#[derive(Serialize)]
struct DropFile {
    path: String,
    bytes: u64,
    sha256: String,
    s3: String,
}

fn under_out_drop(out: &Path, relative: &str) -> Result<PathBuf, StoreError> {
    if relative.is_empty()
        || relative.starts_with('/')
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == "..")
    {
        return Err(StoreError::Check(format!(
            "path escapes the output root: {relative}"
        )));
    }
    Ok(out.join(relative))
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
    /// When true, `drop_raw` errors after the next successful unlink. The drop
    /// record has already been written.
    static DROP_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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

/// Test hook: the next unlink in `drop_raw` succeeds, then the function returns
/// an error. The drop record is already durable.
#[cfg(test)]
pub(crate) fn fail_after_next_unlink() {
    DROP_FAULT.with(|slot| slot.set(true));
}

#[cfg(test)]
pub(crate) fn clear_drop_fault() {
    DROP_FAULT.with(|slot| slot.set(false));
}

#[cfg(test)]
fn take_drop_fault() -> bool {
    DROP_FAULT.with(|slot| {
        if slot.get() {
            slot.set(false);
            true
        } else {
            false
        }
    })
}

fn already_done(
    out: &Path,
    key: &str,
    identity: &str,
    config: &str,
    watermark: &BTreeSet<String>,
) -> Result<bool, StoreError> {
    if !watermark.contains(key) {
        return Ok(false);
    }
    let Some(receipt) = publish::read_receipt(out, key) else {
        return Ok(false);
    };
    if receipt.schema != SCHEMA
        || receipt.hour != key
        || receipt.input_sha256 != identity
        || receipt.config_hash != config
        || receipt.output_rows != receipt.input_snapshots
    {
        return Ok(false);
    }
    let mut rows = 0u64;
    for output in &receipt.outputs {
        if output
            .path
            .split('/')
            .any(|part| part.is_empty() || part == "..")
        {
            return Ok(false);
        }
        let path = out.join(&output.path);
        if !path.is_file() || file_hash(&path)? != output.sha256 {
            return Ok(false);
        }
        rows += output.rows;
    }
    Ok(rows == receipt.output_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hours_sort_by_number_not_lexicographically() {
        let entries = vec![
            Entry {
                manifest_path: "20261022/10/BTC.lz4".into(),
                date: "20261022".into(),
                hour: 10,
                coin: "BTC".into(),
                sha256: "ab".repeat(32),
                bytes: 1,
            },
            Entry {
                manifest_path: "20261022/2/BTC.lz4".into(),
                date: "20261022".into(),
                hour: 2,
                coin: "BTC".into(),
                sha256: "ab".repeat(32),
                bytes: 1,
            },
        ];
        let request = Request {
            input: Path::new("."),
            out: Path::new("."),
            checksums: Path::new("."),
            min_free_bytes: 0,
            threads: 1,
            memory_limit: "1GB".into(),
            git_sha: "x".into(),
            coins: None,
            since: None,
            until: None,
            drop_raw: false,
        };
        let hours = group_hours(&entries, &request);
        assert_eq!(
            hours
                .iter()
                .map(|hour| format!("{}/{}", hour.date, hour.hour))
                .collect::<Vec<_>>(),
            vec!["20261022/2".to_owned(), "20261022/10".to_owned()]
        );
    }
}
