//! `solos-data augment hl-l2book-manifest`: build the sha256 manifest from the downloader TSV
//! and the local bytes (ADR-0021).
//!
//! The downloader's TSV has the object's byte size and its MD5 etag, never a sha256. For every
//! finished `.lz4` on disk whose size equals the TSV size and whose local MD5 equals the etag,
//! the builder writes `sha256 size path`, where the sha256 is of the local bytes: the MD5 etag
//! is a gate that ties the local file to the TSV row, never the checksum. The converter reads
//! the same three-field listing, so its manifest can be this file. MD5 itself is the `md-5`
//! crate: a vetted implementation for a gate that is not a checksum.
//!
//! Resume: the manifest and its sidecar state file are written whole through
//! `write_durable` (temp, fsync, rename, directory fsync), so a crash leaves the previous
//! complete file or the new complete file, never a torn one. An entry is not re-hashed when
//! the manifest already has the path with the same byte size and the sidecar still records
//! those bytes and an unchanged mtime; anything else is hashed again. A rerun appends only
//! new finished files. A size or MD5 mismatch is counted and not written, and it drops any
//! entry the manifest had for that path. `.part` files are never read.

use super::checksum;
use crate::fsutil::write_durable;
use crate::store::StoreError;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

/// One builder run.
pub struct Request<'a> {
    /// Directory of `YYYYMMDD/H/<COIN>.lz4` (or the `market_data/.../l2Book/` layout).
    pub input: &'a Path,
    /// Downloader TSV, gzip or plain, with `key`, `size`, and `etag` columns.
    pub tsv: &'a Path,
    /// Manifest path to write: `<sha256> <size> <path>` lines. A `<path>.state.json`
    /// sidecar beside it records the resume state.
    pub out: &'a Path,
    /// Hashing threads, `1..=256`.
    pub jobs: u32,
    /// Test seam: fail once after this many hashed files, like a kill mid-batch, before any
    /// write of it. The CLI always passes `None`.
    pub fail_after: Option<u64>,
}

/// How many new hashed bytes trigger an intermediate durable write, so a crash costs at most
/// this much re-hashing. The full archive re-hashes about 8 GiB per flush at worst.
const FLUSH_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// How many files a hashing batch handles before the merge step runs.
const BATCH: usize = 1024;

/// The builder's outcome. Printed by the CLI.
#[derive(Clone, Debug, PartialEq)]
pub struct Report {
    /// Finished `.lz4` book files found on disk.
    pub present: u64,
    /// Of those, the files the manifest covers after this run.
    pub covered: u64,
    /// Files hashed this run (MD5 gate passed, sha256 written).
    pub newly_hashed: u64,
    /// Present files already covered, not re-hashed (size and mtime unchanged).
    pub reused: u64,
    /// `.part` files seen and never read.
    pub skipped_part: u64,
    /// Files whose size or MD5 differs from the TSV row, or whose etag is not an MD5.
    pub mismatched: u64,
    /// Finished `.lz4` files with no TSV row.
    pub not_in_tsv: u64,
    /// Hashing threads used.
    pub jobs: u32,
}

impl Report {
    /// JSON object for stdout.
    #[must_use]
    pub fn to_json(&self) -> String {
        crate::jsonout::Obj::new()
            .with("command", "augment hl-l2book-manifest")
            .with("present", self.present)
            .with("covered", self.covered)
            .with("newlyHashed", self.newly_hashed)
            .with("reused", self.reused)
            .with("skippedPart", self.skipped_part)
            .with("mismatched", self.mismatched)
            .with("notInTsv", self.not_in_tsv)
            .with("jobs", self.jobs)
            .to_json()
    }
}

/// One manifest line, `sha256 size path`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Line {
    sha256: String,
    bytes: u64,
    path: String,
}

impl Line {
    fn render(&self) -> String {
        format!("{} {} {}\n", self.sha256, self.bytes, self.path)
    }
}

/// The sidecar's per-file resume state: what the hashed bytes and mtime were.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FileState {
    bytes: u64,
    #[serde(rename = "mtimeMs")]
    mtime_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    schema: String,
    files: std::collections::BTreeMap<String, FileState>,
}

const STATE_SCHEMA: &str = "hl-l2book-manifest-state-v1";

/// Build or extend the manifest.
///
/// # Errors
///
/// The TSV cannot be read or has no `key`, `size`, and `etag` header, the input walk fails,
/// an existing manifest or state file is malformed, or a durable write fails. Mismatched and
/// not-in-TSV files are counts, not errors.
pub fn run(request: &Request<'_>) -> Result<Report, StoreError> {
    if !(1..=256).contains(&request.jobs) {
        return Err(StoreError::Check("--jobs must be 1..=256".into()));
    }
    if !request.input.is_dir() {
        return Err(StoreError::Check(
            "--input is not a directory of YYYYMMDD/H/<COIN>.lz4".into(),
        ));
    }
    if !request.tsv.is_file() {
        return Err(StoreError::Check("--tsv is not a downloader TSV".into()));
    }
    let tsv = load_tsv(request.tsv)?;
    let walked = walk(request.input)?;
    let mut report = Report {
        present: walked.books.len() as u64,
        covered: 0,
        newly_hashed: 0,
        reused: 0,
        skipped_part: walked.parts,
        mismatched: 0,
        not_in_tsv: 0,
        jobs: request.jobs,
    };
    let mut manifest = load_manifest(request.out)?;
    let mut state = load_state(&sidecar(request.out))?;
    let mut hashed_since_flush: u64 = 0;
    // Counts down hashed files to the injected stop; 0 means off. Atomic because hashing
    // threads share it, and a request field rather than a global so concurrent tests (and
    // runs) cannot trip each other's stop.
    let fail_after = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
        request.fail_after.map_or(0, |files| files + 1),
    ));
    for batch in walked.books.chunks(BATCH) {
        for outcome in hash_batch(request, batch, &tsv, &manifest, &state, &fail_after)? {
            match outcome.body {
                OutcomeBody::Covered { sha256, reused } => {
                    report.covered += 1;
                    if reused {
                        report.reused += 1;
                    } else {
                        report.newly_hashed += 1;
                        hashed_since_flush += outcome.bytes;
                        manifest.insert(
                            outcome.path.clone(),
                            Line {
                                sha256,
                                bytes: outcome.bytes,
                                path: outcome.path.clone(),
                            },
                        );
                    }
                    state.insert(
                        outcome.path.clone(),
                        FileState {
                            bytes: outcome.bytes,
                            mtime_ms: outcome.mtime_ms,
                        },
                    );
                }
                OutcomeBody::Mismatch => {
                    report.mismatched += 1;
                    // The file no longer matches the TSV row, so any earlier entry for it
                    // cannot be trusted either.
                    manifest.remove(&outcome.path);
                    state.remove(&outcome.path);
                }
                OutcomeBody::NotInTsv => report.not_in_tsv += 1,
            }
        }
        if hashed_since_flush >= FLUSH_BYTES {
            write_manifest(request.out, &manifest)?;
            write_state(&sidecar(request.out), &state)?;
            hashed_since_flush = 0;
        }
    }
    write_manifest(request.out, &manifest)?;
    write_state(&sidecar(request.out), &state)?;
    Ok(report)
}

fn sidecar(out: &Path) -> PathBuf {
    let mut name = out.as_os_str().to_os_string();
    name.push(".state.json");
    PathBuf::from(name)
}

struct Outcome {
    path: String,
    bytes: u64,
    mtime_ms: u64,
    body: OutcomeBody,
}

enum OutcomeBody {
    Covered { sha256: String, reused: bool },
    Mismatch,
    NotInTsv,
}

/// Classify and, when needed, hash one batch of files across `jobs` threads. Each worker
/// owns its read buffer and pulls file paths from a shared index; the caller merges the
/// channel's outcomes into its maps.
fn hash_batch(
    request: &Request<'_>,
    batch: &[PathBuf],
    tsv: &HashMap<String, TsvRow>,
    manifest: &HashMap<String, Line>,
    state: &std::collections::BTreeMap<String, FileState>,
    fail_after: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> Result<Vec<Outcome>, StoreError> {
    let relative: Vec<String> = batch
        .iter()
        .map(|path| {
            path.strip_prefix(request.input)
                .map_err(|_| StoreError::Check(format!("{} escapes the input dir", path.display())))
                .map(|rest| rest.to_string_lossy().replace('\\', "/"))
        })
        .collect::<Result<_, _>>()?;
    let next = AtomicUsize::new(0);
    let (sender, receiver) = mpsc::channel::<Outcome>();
    std::thread::scope(|scope| -> Result<(), StoreError> {
        let mut handles = Vec::new();
        for _ in 0..request.jobs {
            let sender = sender.clone();
            let next = &next;
            let relative = &relative;
            let fail_after = std::sync::Arc::clone(fail_after);
            handles.push(scope.spawn(move || -> Result<(), StoreError> {
                let mut buffer: Vec<u8> = Vec::new();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(path) = batch.get(index) else {
                        return Ok(());
                    };
                    let outcome =
                        classify(path, &relative[index], tsv, manifest, state, &mut buffer)?;
                    if matches!(outcome.body, OutcomeBody::Covered { .. }) {
                        let remaining = fail_after.fetch_sub(1, Ordering::SeqCst);
                        if remaining == 1 {
                            fail_after.store(0, Ordering::SeqCst);
                            return Err(StoreError::Check("injected manifest-builder stop".into()));
                        }
                        if remaining <= 1 {
                            fail_after.store(0, Ordering::SeqCst);
                        }
                    }
                    if sender.send(outcome).is_err() {
                        return Ok(());
                    }
                }
            }));
        }
        drop(sender);
        for handle in handles {
            handle
                .join()
                .map_err(|_| StoreError::Check("hashing thread panicked".into()))??;
        }
        Ok(())
    })?;
    let mut outcomes = Vec::with_capacity(batch.len());
    while let Ok(outcome) = receiver.recv() {
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

/// The per-file decision: reuse, hash, mismatch, or not in the TSV.
fn classify(
    path: &Path,
    relative: &str,
    tsv: &HashMap<String, TsvRow>,
    manifest: &HashMap<String, Line>,
    state: &std::collections::BTreeMap<String, FileState>,
    buffer: &mut Vec<u8>,
) -> Result<Outcome, StoreError> {
    let metadata = std::fs::metadata(path)?;
    let bytes = metadata.len();
    let mtime_ms = mtime_millis(&metadata)?;
    let Some(row) = tsv.get(relative).or_else(|| {
        tsv.get(&format!("market_data/{relative}"))
            .or_else(|| tsv.get(relative.trim_start_matches("market_data/")))
    }) else {
        return Ok(Outcome {
            path: relative.to_owned(),
            bytes,
            mtime_ms,
            body: OutcomeBody::NotInTsv,
        });
    };
    // An etag that is not a plain MD5 (multipart objects are `md5-N`) can never be gated.
    if row.etag.len() != 32 || !row.etag.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(Outcome {
            path: relative.to_owned(),
            bytes,
            mtime_ms,
            body: OutcomeBody::Mismatch,
        });
    }
    if bytes != row.size {
        return Ok(Outcome {
            path: relative.to_owned(),
            bytes,
            mtime_ms,
            body: OutcomeBody::Mismatch,
        });
    }
    // Resume rule: same path and byte size in the manifest, and the sidecar still records
    // those bytes with an unchanged mtime. Any doubt re-hashes.
    if let Some(line) = manifest.get(relative)
        && line.bytes == bytes
        && let Some(saved) = state.get(relative)
        && saved.bytes == bytes
        && saved.mtime_ms == mtime_ms
    {
        return Ok(Outcome {
            path: relative.to_owned(),
            bytes,
            mtime_ms,
            body: OutcomeBody::Covered {
                sha256: line.sha256.clone(),
                reused: true,
            },
        });
    }
    buffer.clear();
    std::fs::File::open(path)?.read_to_end(buffer)?;
    let gate = md5_hex(buffer);
    if gate != row.etag {
        return Ok(Outcome {
            path: relative.to_owned(),
            bytes,
            mtime_ms,
            body: OutcomeBody::Mismatch,
        });
    }
    // The checksum is of the local bytes, computed here, never the MD5 etag.
    let sha256 = crate::fsutil::sha256_hex(buffer);
    Ok(Outcome {
        path: relative.to_owned(),
        bytes,
        mtime_ms,
        body: OutcomeBody::Covered {
            sha256,
            reused: false,
        },
    })
}

fn mtime_millis(metadata: &std::fs::Metadata) -> Result<u64, StoreError> {
    let modified = metadata.modified()?;
    let millis = modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StoreError::Check("file mtime is before the epoch".into()))?;
    Ok(u64::try_from(millis.as_millis()).unwrap_or(u64::MAX))
}

#[derive(Clone, Debug)]
struct TsvRow {
    size: u64,
    etag: String,
}

/// The TSV: header names locate `key`, `size`, and `etag`, so column order can move.
fn load_tsv(path: &Path) -> Result<HashMap<String, TsvRow>, StoreError> {
    let raw = std::fs::read(path)?;
    let reader: Box<dyn Read> = if raw.starts_with(b"\x1f\x8b") {
        Box::new(flate2::read::GzDecoder::new(&raw[..]))
    } else {
        Box::new(&raw[..])
    };
    let mut lines = BufReader::new(reader).lines();
    let header = lines
        .next()
        .ok_or_else(|| StoreError::Check("tsv has no header".into()))??
        .split('\t')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let column = |name: &str| -> Result<usize, StoreError> {
        header
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| {
                StoreError::Check(format!(
                    "tsv header has no {name} column: {}",
                    header.join(",")
                ))
            })
    };
    let (key_at, size_at, etag_at) = (column("key")?, column("size")?, column("etag")?);
    let mut rows: HashMap<String, TsvRow> = HashMap::new();
    for line in lines {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        let (Some(key), Some(size), Some(etag)) = (
            parts.get(key_at).copied(),
            parts.get(size_at).copied(),
            parts.get(etag_at).copied(),
        ) else {
            return Err(StoreError::Check("tsv row is short".into()));
        };
        if key.is_empty() {
            continue;
        }
        let size: u64 = size
            .parse()
            .map_err(|_| StoreError::Check(format!("tsv size '{size}' is not an integer")))?;
        let row = TsvRow {
            size,
            etag: etag.trim().to_ascii_lowercase(),
        };
        match rows.get(key) {
            Some(existing) if existing.size != row.size || existing.etag != row.etag => {
                return Err(StoreError::Check(format!(
                    "tsv has conflicting rows for {key}"
                )));
            }
            _ => {
                rows.insert(key.to_owned(), row);
            }
        }
    }
    Ok(rows)
}

struct Walked {
    books: Vec<PathBuf>,
    parts: u64,
}

/// Every finished book `.lz4` under `input`, sorted by relative path, plus the `.part` count.
fn walk(input: &Path) -> Result<Walked, StoreError> {
    let mut books = Vec::new();
    let mut parts = 0u64;
    let mut stack = vec![input.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_dir() {
                let name = entry.file_name();
                if !name.to_string_lossy().starts_with('.') {
                    stack.push(path);
                }
                continue;
            }
            if !kind.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".part") {
                parts += 1;
                continue;
            }
            let Ok(relative) = path.strip_prefix(input) else {
                continue;
            };
            if checksum::book_of_path(&relative.to_string_lossy()).is_some() {
                books.push(path);
            }
        }
    }
    books.sort();
    Ok(Walked { books, parts })
}

fn load_manifest(out: &Path) -> Result<HashMap<String, Line>, StoreError> {
    let text = match std::fs::read_to_string(out) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut lines = HashMap::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let (Some(sha256), Some(size), Some(path)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(StoreError::Check(format!(
                "manifest line {}: expected sha256, size, and path",
                number + 1
            )));
        };
        if parts.next().is_some() {
            return Err(StoreError::Check(format!(
                "manifest line {}: extra fields",
                number + 1
            )));
        }
        let bytes: u64 = size
            .parse()
            .map_err(|_| StoreError::Check(format!("manifest line {}: bad size", number + 1)))?;
        if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(StoreError::Check(format!(
                "manifest line {}: not a sha256",
                number + 1
            )));
        }
        lines.insert(
            path.to_owned(),
            Line {
                sha256: sha256.to_ascii_lowercase(),
                bytes,
                path: path.to_owned(),
            },
        );
    }
    Ok(lines)
}

fn load_state(path: &Path) -> Result<std::collections::BTreeMap<String, FileState>, StoreError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(std::collections::BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    };
    let state: StateFile = serde_json::from_str(&text)
        .map_err(|e| StoreError::Check(format!("manifest state: {e}")))?;
    if state.schema != STATE_SCHEMA {
        return Err(StoreError::Check(format!(
            "manifest state schema is {}, this command writes {STATE_SCHEMA}",
            state.schema
        )));
    }
    Ok(state.files)
}

/// The whole manifest, sorted by path, through a durable write. A crash leaves the previous
/// file or this file, never a torn line.
fn write_manifest(out: &Path, lines: &HashMap<String, Line>) -> Result<(), StoreError> {
    let mut sorted: Vec<&Line> = lines.values().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut body = String::new();
    for line in sorted {
        body.push_str(&line.render());
    }
    write_durable(out, body.as_bytes())?;
    Ok(())
}

fn write_state(
    path: &Path,
    files: &std::collections::BTreeMap<String, FileState>,
) -> Result<(), StoreError> {
    let state = StateFile {
        schema: STATE_SCHEMA.to_owned(),
        files: files.clone(),
    };
    let mut body = serde_json::to_vec(&state).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    write_durable(path, &body)?;
    Ok(())
}

/// Lowercase MD5 hex of the bytes, through the `md-5` crate: the downloader etag gate only.
#[must_use]
pub(crate) fn md5_hex(bytes: &[u8]) -> String {
    hex::encode(Md5::digest(bytes))
}
