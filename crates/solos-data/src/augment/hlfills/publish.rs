//! Write one hour: stream rows to staging, `COPY` Parquet, check, then rename.
//!
//! Nothing under `train/` or `forward/` is replaced until every output of the hour has been
//! counted back and checked. A failure deletes the temporary files and leaves the watermark
//! alone. The caller writes the receipt and the watermark after this returns.

use super::decode::{self, FillRow, select_list};
use super::{Corpus, FILL_COLUMNS, FORWARD_START_MS, SCHEMA, output_relative, utc_date};
use crate::decoder::parquet::{self, copy_statement};
use crate::fsutil::{
    create_dir_all_durable, file_hash, sync_dir_after_rename, sync_tmp_file, tmp_path,
    write_durable,
};
use crate::store::{StoreError, query_rows, sql_string};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// One sealed Parquet file, paths relative to the output root.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputFile {
    /// `train/...` or `forward/...`.
    pub path: String,
    /// Rows in the file.
    pub rows: u64,
    /// File size.
    pub bytes: u64,
    /// SHA-256 of the file bytes.
    pub sha256: String,
}

/// What one verified hour wrote. The batch totals `output_rows`; the other fields are the
/// receipt's breakdown, kept on the value so a caller can log them without re-reading it.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct HourOutput {
    /// Events counted from `events` arrays, not from the row builder.
    pub input_fills: u64,
    /// Rows read back from Parquet.
    pub output_rows: u64,
    /// Rows under `train/`.
    pub train_rows: u64,
    /// Rows under `forward/`.
    pub forward_rows: u64,
    /// Sealed files.
    pub outputs: Vec<OutputFile>,
}

/// Decode `bytes` (the `.lz4` body, already hashed) and seal its Parquet files.
///
/// # Errors
///
/// The frame is short or corrupt, a line does not parse, the event count disagrees with the
/// row count, or a sealed file fails a range check. No final path is replaced in those cases
/// unless a rename itself fails after an earlier rename in the same hour; the watermark is
/// still not moved, and a rerun rewrites the hour.
pub fn publish_hour(
    conn: &Connection,
    out: &Path,
    hour_key: &str,
    bytes: &[u8],
    input_sha: &str,
    git_sha: &str,
    config_hash: &str,
) -> Result<HourOutput, StoreError> {
    if !bytes.starts_with(&crate::augment::hls3::LZ4_MAGIC) {
        return Err(StoreError::Check("lz4: not an lz4 frame".into()));
    }
    let staging = out.join(".staging").join(hour_key.replace('/', "-"));
    let _ = std::fs::remove_dir_all(&staging);
    create_dir_all_durable(&staging)?;
    let cleaner = DirClean(staging.clone());
    let mut spill = Spill::new(staging);
    let input_fills = stream_rows(bytes, &mut spill)?;
    let parts = spill.finish()?;
    #[cfg(test)]
    let mut parts = parts;
    #[cfg(test)]
    if seams::take_if(seams::CountSeam::Staged)
        && let Some(part) = parts.first_mut()
    {
        // The staged counter claims a row the JSONL does not hold, so only the
        // Parquet-versus-staged check can see it.
        part.rows = part.rows.saturating_add(1);
    }
    let mut pending = Vec::with_capacity(parts.len());
    for part in &parts {
        pending.push(write_tmp(conn, out, hour_key, part)?);
    }
    let mut output_rows = 0u64;
    for file in &pending {
        output_rows += check_tmp(conn, file)?;
    }
    if output_rows != input_fills {
        drop_tmps(&pending);
        return Err(StoreError::Check(format!(
            "event count {input_fills} != parquet rows {output_rows}"
        )));
    }
    let outputs = rename_all(&pending)?;
    drop(cleaner);
    let stale = stale_paths(out, hour_key, &outputs);
    for relative in stale {
        let path = under_out(out, &relative)?;
        if path.is_file() {
            std::fs::remove_file(&path)?;
            if let Some(parent) = path.parent() {
                sync_dir_after_rename(parent)?;
            }
        }
    }
    let receipt = Receipt {
        schema: SCHEMA.to_owned(),
        hour: hour_key.to_owned(),
        input_sha256: input_sha.to_owned(),
        input_fills,
        output_rows: outputs.iter().map(|file| file.rows).sum(),
        train_rows: parts.iter().map(|part| part.train_rows).sum(),
        forward_rows: parts.iter().map(|part| part.forward_rows).sum(),
        outputs: outputs.clone(),
        git_sha: git_sha.to_owned(),
        config_hash: config_hash.to_owned(),
    };
    #[cfg(test)]
    seams::fire_before_receipt();
    write_receipt(out, &receipt)?;
    Ok(HourOutput {
        input_fills,
        output_rows,
        train_rows: parts.iter().map(|p| p.train_rows).sum(),
        forward_rows: parts.iter().map(|p| p.forward_rows).sum(),
        outputs,
    })
}

struct DirClean(PathBuf);

impl Drop for DirClean {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn stream_rows(bytes: &[u8], spill: &mut Spill) -> Result<u64, StoreError> {
    let cursor = std::io::Cursor::new(bytes);
    let decoder = lz4::Decoder::new(cursor).map_err(|e| StoreError::Check(format!("lz4: {e}")))?;
    let reader = BufReader::new(decoder);
    let mut counted = 0u64;
    let mut written = 0u64;
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| StoreError::Check(format!("line {}: {e}", index + 1)))?;
        if line.is_empty() {
            continue;
        }
        let events = decode::count_events(&line)
            .map_err(|e| StoreError::Check(format!("line {}: {e}", index + 1)))?;
        let rows = decode::rows_of_line(&line)
            .map_err(|e| StoreError::Check(format!("line {}: {e}", index + 1)))?;
        #[cfg(test)]
        let mut rows = rows;
        #[cfg(test)]
        if seams::take_if(seams::CountSeam::Line) {
            rows.pop();
        }
        let got =
            u64::try_from(rows.len()).map_err(|_| StoreError::Check("too many rows".into()))?;
        if got != events {
            return Err(StoreError::Check(format!(
                "line {}: event count {events} diverged from {got} rows",
                index + 1
            )));
        }
        for row in rows {
            #[cfg(test)]
            if seams::take_if(seams::CountSeam::Hour) {
                // Count the event, but do not stage the row. The hour total is then short
                // while the per-line check and the staged-versus-Parquet check still agree.
                written += 1;
                continue;
            }
            spill.write(&row)?;
            written += 1;
        }
        counted += events;
    }
    if counted != written {
        return Err(StoreError::Check(format!(
            "event count {counted} diverged from {written} rows"
        )));
    }
    Ok(counted)
}

struct Spill {
    dir: PathBuf,
    files: BTreeMap<(Corpus, String), SpillFile>,
}

struct SpillFile {
    path: PathBuf,
    writer: std::io::BufWriter<std::fs::File>,
    rows: u64,
    train_rows: u64,
    forward_rows: u64,
}

struct Part {
    corpus: Corpus,
    date: String,
    path: PathBuf,
    rows: u64,
    train_rows: u64,
    forward_rows: u64,
}

impl Spill {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            files: BTreeMap::new(),
        }
    }

    fn write(&mut self, row: &FillRow) -> Result<(), StoreError> {
        let time_ms = row.time_ms();
        let date =
            utc_date(time_ms).ok_or_else(|| StoreError::Check("time is out of range".into()))?;
        let corpus = Corpus::of_time_ms(time_ms);
        if !self.files.contains_key(&(corpus, date.clone())) {
            let path = self.dir.join(format!("{}-{date}.jsonl", corpus.as_str()));
            let file = std::fs::File::create(&path)?;
            self.files.insert(
                (corpus, date.clone()),
                SpillFile {
                    path,
                    writer: std::io::BufWriter::new(file),
                    rows: 0,
                    train_rows: 0,
                    forward_rows: 0,
                },
            );
        }
        let entry = self
            .files
            .get_mut(&(corpus, date))
            .expect("spill file inserted");
        serde_json::to_writer(&mut entry.writer, row)
            .map_err(|e| StoreError::Check(format!("row json: {e}")))?;
        entry.writer.write_all(b"\n")?;
        entry.rows += 1;
        match corpus {
            Corpus::Train => entry.train_rows += 1,
            Corpus::Forward => entry.forward_rows += 1,
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<Part>, StoreError> {
        let mut out = Vec::new();
        for ((corpus, date), mut file) in self.files {
            file.writer.flush()?;
            out.push(Part {
                corpus,
                date,
                path: file.path,
                rows: file.rows,
                train_rows: file.train_rows,
                forward_rows: file.forward_rows,
            });
        }
        Ok(out)
    }
}

struct Pending {
    tmp: PathBuf,
    final_path: PathBuf,
    relative: String,
    corpus: Corpus,
    date: String,
    rows: u64,
}

fn write_tmp(
    conn: &Connection,
    out: &Path,
    hour_key: &str,
    part: &Part,
) -> Result<Pending, StoreError> {
    let relative = output_relative(part.corpus, &part.date, hour_key);
    let final_path = under_out(out, &relative)?;
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent)?;
    }
    let tmp = tmp_path(&final_path);
    let _ = std::fs::remove_file(&tmp);
    let select = format!(
        "SELECT {} FROM {} ORDER BY coin, time_ms NULLS LAST, tid NULLS LAST",
        select_list(),
        ndjson(&part.path)
    );
    if let Err(error) = conn.execute_batch(&copy_statement(&select, &tmp)) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    Ok(Pending {
        tmp,
        final_path,
        relative,
        corpus: part.corpus,
        date: part.date.clone(),
        rows: part.rows,
    })
}

fn ndjson(path: &Path) -> String {
    let columns = FILL_COLUMNS
        .iter()
        .map(|(name, kind)| format!("{}: '{kind}'", sql_string(name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "read_json({}, format='newline_delimited', columns={{{}}})",
        sql_string(&path.to_string_lossy()),
        columns
    )
}

fn check_tmp(conn: &Connection, file: &Pending) -> Result<u64, StoreError> {
    let rows = count_where(conn, &file.tmp, "")?;
    if rows != file.rows {
        return Err(StoreError::Check(format!(
            "{}: staged {} rows, parquet has {rows}",
            file.relative, file.rows
        )));
    }
    let wrong_time = match file.corpus {
        Corpus::Train => count_where(
            conn,
            &file.tmp,
            &format!("WHERE time_ms >= {FORWARD_START_MS}"),
        )?,
        Corpus::Forward => count_where(
            conn,
            &file.tmp,
            &format!("WHERE time_ms < {FORWARD_START_MS}"),
        )?,
    };
    if wrong_time != 0 {
        return Err(StoreError::Check(format!(
            "{}: {wrong_time} rows are in the wrong corpus",
            file.relative
        )));
    }
    let (start_ms, end_ms) = utc_day_bounds(&file.date)?;
    let wrong_date = count_where(
        conn,
        &file.tmp,
        &format!("WHERE time_ms < {start_ms} OR time_ms >= {end_ms}"),
    )?;
    if wrong_date != 0 {
        return Err(StoreError::Check(format!(
            "{}: {wrong_date} rows are not dated {}",
            file.relative, file.date
        )));
    }
    let wrong_class = count_where(conn, &file.tmp, &format!("WHERE {CLASS_MISMATCH}"))?;
    if wrong_class != 0 {
        return Err(StoreError::Check(format!(
            "{}: {wrong_class} rows have the wrong coin_class",
            file.relative
        )));
    }
    Ok(rows)
}

const CLASS_MISMATCH: &str = "NOT (\
    (starts_with(coin, '@') AND coin_class = 'spot') OR \
    (starts_with(coin, '#') AND coin_class = 'outcome') OR \
    (NOT starts_with(coin, '@') AND NOT starts_with(coin, '#') AND contains(coin, ':') AND coin_class = 'hip3') OR \
    (NOT starts_with(coin, '@') AND NOT starts_with(coin, '#') AND NOT contains(coin, ':') AND coin_class = 'perp'))";

/// Inclusive start and exclusive end, in milliseconds, of one UTC calendar day.
fn utc_day_bounds(date: &str) -> Result<(i64, i64), StoreError> {
    let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|error| StoreError::Check(format!("bad partition date {date}: {error}")))?;
    let start = day
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| StoreError::Check(format!("bad partition date {date}")))?
        .and_utc()
        .timestamp_millis();
    Ok((start, start.saturating_add(86_400_000)))
}

fn count_where(conn: &Connection, path: &Path, filter: &str) -> Result<u64, StoreError> {
    let sql = format!(
        "SELECT count(*) AS n FROM read_parquet({}) {filter}",
        sql_string(&path.to_string_lossy())
    );
    let rows = query_rows(conn, &sql, &[])?;
    let n = rows
        .first()
        .and_then(|row| row.int("n"))
        .ok_or_else(|| StoreError::Check("parquet count missing".into()))?;
    u64::try_from(n).map_err(|_| StoreError::Check("parquet count overflow".into()))
}

fn rename_all(pending: &[Pending]) -> Result<Vec<OutputFile>, StoreError> {
    let mut sealed = Vec::with_capacity(pending.len());
    for file in pending {
        sync_tmp_file(&file.tmp)?;
        std::fs::rename(&file.tmp, &file.final_path)?;
        if let Some(parent) = file.final_path.parent() {
            sync_dir_after_rename(parent)?;
        }
        let bytes = std::fs::metadata(&file.final_path)?.len();
        sealed.push(OutputFile {
            path: file.relative.clone(),
            rows: file.rows,
            bytes,
            sha256: file_hash(&file.final_path)?,
        });
    }
    Ok(sealed)
}

fn drop_tmps(pending: &[Pending]) {
    for file in pending {
        let _ = std::fs::remove_file(&file.tmp);
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp);
    }
}

/// Receipt on disk. `written` fields are the verification record for one hour.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    /// [`SCHEMA`].
    pub schema: String,
    /// `YYYYMMDD/H`.
    pub hour: String,
    /// SHA-256 of the `.lz4` bytes.
    pub input_sha256: String,
    /// Independent event count.
    pub input_fills: u64,
    /// Rows read back from Parquet.
    pub output_rows: u64,
    /// Rows under `train/`.
    pub train_rows: u64,
    /// Rows under `forward/`.
    pub forward_rows: u64,
    /// Sealed files.
    pub outputs: Vec<OutputFile>,
    /// Git commit that wrote the files, or `unknown`.
    pub git_sha: String,
    /// [`super::config_hash`].
    pub config_hash: String,
}

fn write_receipt(out: &Path, receipt: &Receipt) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
    let mut body = bytes;
    body.push(b'\n');
    let path = receipt_path(out, &receipt.hour)?;
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

pub fn receipt_path(out: &Path, hour_key: &str) -> Result<PathBuf, StoreError> {
    under_out(out, &format!("receipts/{hour_key}.json"))
}

pub fn read_receipt(out: &Path, hour_key: &str) -> Option<Receipt> {
    let path = receipt_path(out, hour_key).ok()?;
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn stale_paths(out: &Path, hour_key: &str, outputs: &[OutputFile]) -> Vec<String> {
    let Some(previous) = read_receipt(out, hour_key) else {
        return Vec::new();
    };
    let keep: BTreeSet<&str> = outputs.iter().map(|file| file.path.as_str()).collect();
    previous
        .outputs
        .into_iter()
        .map(|file| file.path)
        .filter(|path| !keep.contains(path.as_str()))
        .collect()
}

fn under_out(out: &Path, relative: &str) -> Result<PathBuf, StoreError> {
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

/// Open the in-memory writer connection: pinned DuckDB, UTC, the caller's thread and memory caps.
///
/// # Errors
///
/// DuckDB is not 1.4.5, or a setting is rejected.
pub fn open_writer(out: &Path, threads: u32, memory_limit: &str) -> Result<Connection, StoreError> {
    let conn = crate::store::memory_connection(&threads.to_string(), memory_limit)?;
    parquet::require_connection_duckdb(&conn)?;
    let temp = out.join(".tmp-duckdb");
    let _ = std::fs::remove_dir_all(&temp);
    create_dir_all_durable(&temp)?;
    conn.execute_batch(&format!(
        "SET threads = {threads}; SET memory_limit = {}; SET temp_directory = {}",
        sql_string(memory_limit),
        sql_string(&temp.to_string_lossy())
    ))?;
    Ok(conn)
}

/// `current_setting` value.
///
/// # Errors
///
/// The setting cannot be read.
pub fn current_setting(conn: &Connection, name: &str) -> Result<String, StoreError> {
    let rows = query_rows(
        conn,
        &format!("SELECT current_setting({}) AS v", sql_string(name)),
        &[],
    )?;
    rows.first()
        .and_then(|row| row.str("v").map(str::to_owned))
        .ok_or_else(|| StoreError::Check(format!("missing setting {name}")))
}

/// Test-only ways to force one count check, or to arm an fsync immediately before the receipt
/// write. Production builds do not include this.
#[cfg(test)]
pub(crate) mod seams {
    use std::cell::Cell;

    /// Which count guard a test is pinning.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CountSeam {
        /// Drop one decoded row before the per-line length check.
        Line,
        /// Count an event that is never staged.
        Hour,
        /// Report one more staged row than the JSONL contains.
        Staged,
    }

    thread_local! {
        static COUNT: Cell<Option<CountSeam>> = const { Cell::new(None) };
        static BEFORE_RECEIPT: Cell<Option<fn()>> = const { Cell::new(None) };
    }

    pub fn set_count_seam(seam: CountSeam) {
        COUNT.with(|slot| slot.set(Some(seam)));
    }

    pub fn take_if(want: CountSeam) -> bool {
        COUNT.with(|slot| {
            if slot.get() == Some(want) {
                slot.set(None);
                true
            } else {
                false
            }
        })
    }

    pub fn clear_count_seam() {
        COUNT.with(|slot| slot.set(None));
    }

    /// `hook` runs once, immediately before the receipt's durable write.
    pub fn on_before_receipt(hook: fn()) {
        BEFORE_RECEIPT.with(|slot| slot.set(Some(hook)));
    }

    pub fn fire_before_receipt() {
        if let Some(hook) = BEFORE_RECEIPT.with(|slot| slot.take()) {
            hook();
        }
    }

    pub fn clear_before_receipt() {
        BEFORE_RECEIPT.with(|slot| slot.set(None));
    }
}
