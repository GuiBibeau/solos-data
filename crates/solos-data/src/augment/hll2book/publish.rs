//! Write one hour: stream rows to staging, `COPY` Parquet, check, then rename.
//!
//! Nothing under `train/` or `forward/` is replaced until every output of the hour has been
//! counted back, sampled, and checked. A failure deletes the temporary files and leaves the
//! watermark alone. The caller writes the watermark after this returns, and only then may
//! delete raw inputs.

use super::decode::{self, Snapshot};
use super::{
    BOOK_LEVELS, COPY_OPTIONS, Corpus, FORWARD_START_MS, SAMPLE_CAP, SCHEMA, column_specs,
    output_relative, utc_date,
};
use crate::fsutil::{
    create_dir_all_durable, file_hash, sync_dir_after_rename, sync_tmp_file, tmp_path,
    write_durable,
};
use crate::store::{StoreError, query_rows, sql_string};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// One coin file already read into memory. The sha is of these bytes.
pub struct FileBytes<'a> {
    /// Coin, also the file stem.
    pub coin: &'a str,
    /// Path relative to the input root, using `/`.
    pub relative: &'a str,
    /// SHA-256 of `bytes`.
    pub sha256: &'a str,
    /// The `.lz4` body.
    pub bytes: &'a [u8],
}

/// What the batch asked to publish.
pub struct HourJob<'a> {
    /// `YYYYMMDD/H`.
    pub hour_key: &'a str,
    /// Coin files of the hour, size-checked and hashed.
    pub files: &'a [FileBytes<'a>],
    /// Recorded on the receipt.
    pub git_sha: &'a str,
    /// [`super::config_hash`].
    pub config_hash: &'a str,
}

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

/// One verified input, so a later run can delete it and so the drop log can name the archive key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputFile {
    /// Coin.
    pub coin: String,
    /// Path relative to the input root.
    pub path: String,
    /// SHA-256 of the `.lz4` bytes.
    pub sha256: String,
    /// Byte size.
    pub bytes: u64,
    /// Snapshots counted in the file.
    pub snapshots: u64,
}

/// What one verified hour wrote.
#[derive(Clone, Debug)]
pub struct HourOutput {
    /// Independent snapshot count.
    pub input_snapshots: u64,
    /// Rows read back from Parquet.
    pub output_rows: u64,
    /// Inputs the receipt recorded.
    pub inputs: Vec<InputFile>,
}

/// Receipt on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    /// [`SCHEMA`].
    pub schema: String,
    /// `YYYYMMDD/H`.
    pub hour: String,
    /// SHA-256 of the sorted `coin sha256` lines. Resume compares this, not a single file.
    pub input_sha256: String,
    /// Independent snapshot count.
    pub input_snapshots: u64,
    /// Rows read back from Parquet.
    pub output_rows: u64,
    /// Rows under `train/`.
    pub train_rows: u64,
    /// Rows under `forward/`.
    pub forward_rows: u64,
    /// Sealed files.
    pub outputs: Vec<OutputFile>,
    /// The coin files this hour verified.
    pub inputs: Vec<InputFile>,
    /// Git commit that wrote the files, or `unknown`.
    pub git_sha: String,
    /// [`super::config_hash`].
    pub config_hash: String,
}

/// Decode `job` and seal its Parquet files.
///
/// # Errors
///
/// A frame is short or corrupt, a line does not parse, a count or sample check fails, or a
/// sealed file fails a range check. No final path is replaced in those cases unless a rename
/// itself fails after an earlier rename in the same hour; the watermark is still not moved.
pub fn publish_hour(
    conn: &Connection,
    out: &Path,
    job: &HourJob<'_>,
) -> Result<HourOutput, StoreError> {
    let staging = out.join(".staging").join(job.hour_key.replace('/', "-"));
    let _ = std::fs::remove_dir_all(&staging);
    create_dir_all_durable(&staging)?;
    let cleaner = DirClean(staging.clone());
    let mut spill = Spill::new(staging);
    let mut samples = Vec::new();
    let mut tallies = BTreeMap::new();
    let input_snapshots = stream_hour(job.files, &mut spill, &mut samples, &mut tallies)?;
    let parts = spill.finish()?;
    #[cfg(test)]
    let mut parts = parts;
    #[cfg(test)]
    if seams::take_if(seams::CountSeam::Staged)
        && let Some(part) = parts.first_mut()
    {
        part.rows = part.rows.saturating_add(1);
    }
    let mut pending = Vec::with_capacity(parts.len());
    for part in &parts {
        pending.push(write_tmp(conn, out, job.hour_key, part)?);
    }
    let mut output_rows = 0u64;
    for file in &pending {
        output_rows += check_tmp(conn, file)?;
    }
    if output_rows != input_snapshots {
        drop_tmps(&pending);
        return Err(StoreError::Check(format!(
            "hour total {input_snapshots} != parquet rows {output_rows}"
        )));
    }
    check_coins(conn, &pending, &tallies)?;
    check_samples(conn, &pending, &samples)?;
    let outputs = rename_all(&pending)?;
    drop(cleaner);
    let stale = stale_paths(out, job.hour_key, &outputs);
    for relative in stale {
        let path = under_out(out, &relative)?;
        if path.is_file() {
            std::fs::remove_file(&path)?;
            if let Some(parent) = path.parent() {
                sync_dir_after_rename(parent)?;
            }
        }
    }
    let inputs = input_files(job, &tallies);
    let receipt = Receipt {
        schema: SCHEMA.to_owned(),
        hour: job.hour_key.to_owned(),
        input_sha256: input_identity(&inputs),
        input_snapshots,
        output_rows: outputs.iter().map(|file| file.rows).sum(),
        train_rows: parts.iter().map(|part| part.train_rows).sum(),
        forward_rows: parts.iter().map(|part| part.forward_rows).sum(),
        outputs: outputs.clone(),
        inputs: inputs.clone(),
        git_sha: job.git_sha.to_owned(),
        config_hash: job.config_hash.to_owned(),
    };
    #[cfg(test)]
    seams::fire_before_receipt();
    write_receipt(out, &receipt)?;
    Ok(HourOutput {
        input_snapshots,
        output_rows,
        inputs,
    })
}

/// SHA-256 of sorted `coin sha256` lines.
#[must_use]
pub fn input_identity(inputs: &[InputFile]) -> String {
    let mut lines: Vec<String> = inputs
        .iter()
        .map(|file| format!("{} {}", file.coin, file.sha256))
        .collect();
    lines.sort();
    crate::fsutil::sha256_hex(lines.join("\n").as_bytes())
}

fn input_files(job: &HourJob<'_>, tallies: &BTreeMap<String, u64>) -> Vec<InputFile> {
    job.files
        .iter()
        .map(|file| InputFile {
            coin: file.coin.to_owned(),
            path: file.relative.to_owned(),
            sha256: file.sha256.to_owned(),
            bytes: u64::try_from(file.bytes.len()).unwrap_or(u64::MAX),
            snapshots: tallies.get(file.coin).copied().unwrap_or(0),
        })
        .collect()
}

struct DirClean(PathBuf);

impl Drop for DirClean {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn stream_hour(
    files: &[FileBytes<'_>],
    spill: &mut Spill,
    samples: &mut Vec<Snapshot>,
    tallies: &mut BTreeMap<String, u64>,
) -> Result<u64, StoreError> {
    let mut counted = 0u64;
    let mut written = 0u64;
    let mut seen = BTreeSet::new();
    for file in files {
        if !file.bytes.starts_with(&crate::augment::hls3::LZ4_MAGIC) {
            return Err(StoreError::Check(format!(
                "{}: lz4: not an lz4 frame",
                file.coin
            )));
        }
        let decoder = lz4::Decoder::new(std::io::Cursor::new(file.bytes))
            .map_err(|e| StoreError::Check(format!("{}: lz4: {e}", file.coin)))?;
        let reader = BufReader::new(decoder);
        for (index, line) in reader.lines().enumerate() {
            let line = line
                .map_err(|e| StoreError::Check(format!("{} line {}: {e}", file.coin, index + 1)))?;
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            let events = decode::count_snapshot(line)
                .map_err(|e| StoreError::Check(format!("{} line {}: {e}", file.coin, index + 1)))?;
            let snapshot = decode::snapshot_of_line(line, file.coin)
                .map_err(|e| StoreError::Check(format!("{} line {}: {e}", file.coin, index + 1)))?;
            let rows = vec![snapshot];
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
                    "{} line {}: snapshot count {events} diverged from {got} rows",
                    file.coin,
                    index + 1
                )));
            }
            for row in rows {
                if !seen.insert((row.coin.clone(), row.time_ms)) {
                    return Err(StoreError::Check(format!(
                        "{} {} duplicate time_ms",
                        row.coin, row.time_ms
                    )));
                }
                *tallies.entry(file.coin.to_owned()).or_default() += 1;
                #[cfg(test)]
                if seams::take_if(seams::CountSeam::Hour) {
                    written += 1;
                    continue;
                }
                consider_sample(samples, &row, written);
                let row = stage_mutation(row);
                spill.write(&row)?;
                written += 1;
            }
            counted += events;
        }
    }
    if counted != written {
        return Err(StoreError::Check(format!(
            "snapshot count {counted} diverged from {written} rows"
        )));
    }
    Ok(counted)
}

/// Test seams that change the staged row after the sample and the file tally are recorded.
#[cfg(test)]
fn stage_mutation(mut row: Snapshot) -> Snapshot {
    if seams::take_if(seams::CountSeam::PerFile) {
        row.coin = "mutated".to_owned();
    }
    if seams::take_if(seams::CountSeam::Sample) {
        row.bid_px[0] = Some(match row.bid_px[0] {
            Some(0.0) => 1.0,
            _ => 0.0,
        });
    }
    row
}

#[cfg(not(test))]
fn stage_mutation(row: Snapshot) -> Snapshot {
    row
}

fn consider_sample(samples: &mut Vec<Snapshot>, row: &Snapshot, index: u64) {
    if samples.len() < SAMPLE_CAP {
        samples.push(row.clone());
        return;
    }
    let slot = mix(index) % (index + 1);
    if slot < u64::try_from(SAMPLE_CAP).unwrap_or(0) {
        samples[usize::try_from(slot).unwrap_or(0)] = row.clone();
    }
}

fn mix(mut n: u64) -> u64 {
    n = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    n ^= n >> 30;
    n = n.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    n ^ (n >> 27)
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

    fn write(&mut self, row: &Snapshot) -> Result<(), StoreError> {
        let date = utc_date(row.time_ms)
            .ok_or_else(|| StoreError::Check("time is out of range".into()))?;
        let corpus = Corpus::of_time_ms(row.time_ms);
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
            .ok_or_else(|| StoreError::Check("spill file missing".into()))?;
        let line =
            decode::snapshot_line(row).map_err(|e| StoreError::Check(format!("row json: {e}")))?;
        std::io::Write::write_all(&mut entry.writer, &line)?;
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
            std::io::Write::flush(&mut file.writer)?;
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
        "SELECT {cols} FROM {src} ORDER BY coin, time_ms",
        cols = select_list(),
        src = ndjson(&part.path)
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

fn select_list() -> String {
    column_specs()
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Project fixed arrays back to `bid_px_0` names. DuckDB arrays are 1-indexed.
fn sample_select() -> String {
    let mut cols = vec![
        "coin".to_owned(),
        "time_ms".to_owned(),
        "recv_time_ms".to_owned(),
    ];
    for side in ["bid", "ask"] {
        for kind in ["px", "sz", "n"] {
            for level in 0..BOOK_LEVELS {
                cols.push(format!(
                    "{side}_{kind}[{}] AS {side}_{kind}_{level}",
                    level + 1
                ));
            }
        }
    }
    cols.join(", ")
}

fn ndjson(path: &Path) -> String {
    let columns = column_specs()
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

fn copy_statement(select: &str, path: &Path) -> String {
    format!(
        "COPY ({select}) TO {} ({COPY_OPTIONS})",
        sql_string(&path.to_string_lossy())
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
    Ok(rows)
}

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

fn check_coins(
    conn: &Connection,
    pending: &[Pending],
    tallies: &BTreeMap<String, u64>,
) -> Result<(), StoreError> {
    let mut got: BTreeMap<String, u64> = BTreeMap::new();
    for file in pending {
        let sql = format!(
            "SELECT coin, count(*) AS n FROM read_parquet({}) GROUP BY coin",
            sql_string(&file.tmp.to_string_lossy())
        );
        for row in query_rows(conn, &sql, &[])? {
            let coin = row
                .str("coin")
                .ok_or_else(|| StoreError::Check("parquet coin missing".into()))?
                .to_owned();
            let n = row
                .int("n")
                .ok_or_else(|| StoreError::Check("parquet coin count missing".into()))?;
            let n = u64::try_from(n)
                .map_err(|_| StoreError::Check("parquet coin count overflow".into()))?;
            *got.entry(coin).or_default() += n;
        }
    }
    for (coin, expected) in tallies {
        let actual = got.get(coin).copied().unwrap_or(0);
        if actual != *expected {
            return Err(StoreError::Check(format!(
                "{coin}: file snapshots {expected} != {actual}"
            )));
        }
    }
    for coin in got.keys() {
        if !tallies.contains_key(coin) {
            return Err(StoreError::Check(format!(
                "{coin}: file snapshots 0 != {}",
                got[coin]
            )));
        }
    }
    Ok(())
}

fn check_samples(
    conn: &Connection,
    pending: &[Pending],
    samples: &[Snapshot],
) -> Result<(), StoreError> {
    for sample in samples {
        let row = find_sample(conn, pending, sample)?;
        if let Err(field) = row_matches(sample, &row) {
            return Err(StoreError::Check(format!(
                "sample mismatch {field} for {} {}",
                sample.coin, sample.time_ms
            )));
        }
    }
    Ok(())
}

fn find_sample(
    conn: &Connection,
    pending: &[Pending],
    sample: &Snapshot,
) -> Result<crate::jsonout::Obj, StoreError> {
    let mut found = Vec::new();
    for file in pending {
        let sql = format!(
            "SELECT {cols} FROM read_parquet({}) WHERE coin = {} AND time_ms = {}",
            sql_string(&file.tmp.to_string_lossy()),
            sql_string(&sample.coin),
            sample.time_ms,
            cols = sample_select()
        );
        found.extend(query_rows(conn, &sql, &[])?);
    }
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => Err(StoreError::Check(format!(
            "sample mismatch missing row for {} {}",
            sample.coin, sample.time_ms
        ))),
        _ => Err(StoreError::Check(format!(
            "sample mismatch ambiguous row for {} {}",
            sample.coin, sample.time_ms
        ))),
    }
}

fn row_matches(sample: &Snapshot, row: &crate::jsonout::Obj) -> Result<(), String> {
    if row.str("coin") != Some(sample.coin.as_str()) {
        return Err("coin".into());
    }
    if row.int("time_ms") != Some(sample.time_ms) {
        return Err("time_ms".into());
    }
    if row.int("recv_time_ms") != sample.recv_time_ms {
        return Err("recv_time_ms".into());
    }
    for level in 0..BOOK_LEVELS {
        check_f64(row, &format!("bid_px_{level}"), sample.bid_px[level])?;
        check_f64(row, &format!("bid_sz_{level}"), sample.bid_sz[level])?;
        check_i64(row, &format!("bid_n_{level}"), sample.bid_n[level])?;
        check_f64(row, &format!("ask_px_{level}"), sample.ask_px[level])?;
        check_f64(row, &format!("ask_sz_{level}"), sample.ask_sz[level])?;
        check_i64(row, &format!("ask_n_{level}"), sample.ask_n[level])?;
    }
    Ok(())
}

fn check_f64(row: &crate::jsonout::Obj, name: &str, expected: Option<f64>) -> Result<(), String> {
    let actual = match row.get(name) {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(text)) => text.parse().ok(),
        Some(_) => return Err(name.to_owned()),
    };
    match (actual, expected) {
        (None, None) => Ok(()),
        (Some(actual), Some(expected)) if actual.to_bits() == expected.to_bits() => Ok(()),
        _ => Err(name.to_owned()),
    }
}

fn check_i64(row: &crate::jsonout::Obj, name: &str, expected: Option<i64>) -> Result<(), String> {
    if row.int(name) == expected {
        Ok(())
    } else {
        Err(name.to_owned())
    }
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

fn write_receipt(out: &Path, receipt: &Receipt) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
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

/// Open the in-memory writer: pinned DuckDB, the caller's thread and memory caps.
///
/// # Errors
///
/// DuckDB is not 1.4.5, or a setting is rejected.
pub fn open_writer(out: &Path, threads: u32, memory_limit: &str) -> Result<Connection, StoreError> {
    let conn = crate::store::memory_connection(&threads.to_string(), memory_limit)?;
    crate::decoder::parquet::require_connection_duckdb(&conn)?;
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

/// Test-only count faults and the receipt fsync hook.
#[cfg(test)]
pub(crate) mod seams {
    use std::cell::Cell;

    /// Which guard a test is pinning.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CountSeam {
        /// Drop the decoded row before the per-line length check.
        Line,
        /// Count a snapshot that is never staged.
        Hour,
        /// Report one more staged row than the JSONL contains.
        Staged,
        /// Stage the row under a different coin than the file tally.
        PerFile,
        /// Change a sampled price after the sample is recorded.
        Sample,
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
