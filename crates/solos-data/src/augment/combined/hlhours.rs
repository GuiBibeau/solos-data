//! Unit U2a `hl-hours`: the HL per-coin hourly table `hl_coin_hour` (DESIGN 4.1).
//!
//! Port of the frozen hltrain stage 1 (STAGE1_SQL, version `s1-v1`, code sha
//! `5acea2048146fc5fb9e8af47dc0cbba7e02ae3f6`) over hl-fills-v1 receipted hours, plus
//! the asset_ctxs block (funding, premium, open interest, mark, oracle, mid) and the
//! asset_id join from the mapping unit's `mapping/assets.csv` when it exists. One
//! output file per UTC hour bucket, one receipt per built hour, one run receipt and a
//! catalog entry per sealed file.
//!
//! Source hour files can hold fills whose exchange timestamp falls in an adjacent hour
//! (block-hour files, second-level strays), so a bucket's aggregate receives partials
//! from source files h-1, h and h+1. The unit runs the frozen stage 1 per source file
//! into a parts table, then merges each bucket with hltrain's merge expressions, so the
//! emitted table is unique per (coin, hour) and matches the frozen pipeline exactly.
//! `first_ms`/`last_ms` survive the merge as min/max (hltrain's coin_hour_sparse drops
//! them; this table keeps them per the unit spec). The `hour` column stays unix
//! seconds, the frozen stage-1 convention.
//!
//! Refusal rules: a bucket is sealed only when source files h-1, h, h+1 are all
//! receipted and the input watermark reaches `h + 1h + 2h guard`. The run range is
//! clamped to that closed watermark. Lockbox v2 hours (common `lockbox`) are never
//! written; the receipt records the drop. The fills watermark is read once per run
//! and never written. The aggregate runs single-threaded so repeated runs produce
//! identical bytes (the frozen smoke config also used one DuckDB thread).
//!
//! asset_ctxs: the s3 daily lz4 CSV is the primary source (perp-style names, 1-minute
//! snapshots); a coin missing there falls back per coin to the augment capture lane
//! parquet (which also carries HIP-3 builder names); otherwise the block is null with
//! `ctx_present = false` and `ctx_source = 'none'`. The snapshot for bucket h is the
//! last one in [h, h+1h); `ctx_d_oi` subtracts the same-source snapshot of h-1h (the
//! previous day's file is loaded on demand for hour 0).

use super::{catalog, grid, lockbox, mapping, receipt};
use crate::fsutil::{
    create_dir_all_durable, file_hash, sha256_hex, sync_dir_after_rename, sync_tmp_file, tmp_path,
    write_durable,
};
use crate::store::{StoreError, sql_string};
use duckdb::Connection;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Schema name recorded in every receipt.
pub const SCHEMA: &str = "combined-hl-coin-hour-v1";

/// CLI unit name and receipt unit.
pub const UNIT: &str = "hl-hours";

/// Version of this unit's writer; bump on any change that moves output bytes.
pub const UNIT_VERSION: u32 = 1;

/// Output table directory under the combined root.
pub const TABLE: &str = "hl_coin_hour";

/// Frozen hltrain stage-1 version this unit ports.
pub const STAGE1_VERSION: &str = "s1-v1";

/// hltrain code sha the frozen SQL was taken from, recorded in receipts.
pub const HL_TRAIN_CODE_SHA: &str = "5acea2048146fc5fb9e8af47dc0cbba7e02ae3f6";

/// A bucket seals only when the input watermark reaches `hour + 1 + GUARD_HOURS`.
pub const GUARD_HOURS: i64 = 2;

/// The frozen hltrain stage-1 SQL, copied verbatim; only `{files}` is substituted.
/// Do not edit anything inside this string.
pub const STAGE1_SQL: &str = r#"
WITH raw AS (
  SELECT coin, coin_class, time_ms, tid, px, sz, side, crossed, liquidation, start_position
  FROM read_parquet({files})
),
t AS (
  SELECT coin, coin_class, (time_ms // 3600000) * 3600 AS hour, time_ms // 60000 AS minute,
         time_ms, time_ms::HUGEINT * 1000000000000000000 + coalesce(tid, 0) AS k,
         px, sz, side, liquidation
  FROM raw WHERE crossed AND px > 0 AND sz > 0
),
m AS (SELECT coin, hour, minute, arg_max(px, k) AS lpx FROM t GROUP BY ALL),
r AS (SELECT coin, hour,
             ln(lpx) - lag(ln(lpx)) OVER (PARTITION BY coin, hour ORDER BY minute) AS dr
      FROM m),
rv AS (SELECT coin, hour, coalesce(sum(dr * dr), 0) AS rv2, count(*) AS n_minutes
       FROM r GROUP BY ALL),
h AS (SELECT coin, any_value(coin_class) AS coin_class, hour, count(*) AS n_trades,
             sum(px * sz) AS vol_usd,
             coalesce(sum(px * sz) FILTER (WHERE side = 'B'), 0) AS buy_usd,
             coalesce(sum(px * sz) FILTER (WHERE liquidation IS NOT NULL), 0) AS liq_usd,
             arg_min(px, k) AS first_px, arg_max(px, k) AS last_px,
             min(time_ms) AS first_ms, max(time_ms) AS last_ms
      FROM t GROUP BY coin, hour),
oi AS (SELECT coin, (time_ms // 3600000) * 3600 AS hour,
              sum(px * (abs(start_position + CASE WHEN side = 'B' THEN sz ELSE -sz END)
                        - abs(start_position))) / 2 AS d_oi_usd
       FROM raw
       WHERE coin_class IN ('perp', 'hip3') AND start_position IS NOT NULL AND px > 0 AND sz > 0
       GROUP BY ALL)
SELECT h.*, rv.rv2, rv.n_minutes, coalesce(oi.d_oi_usd, 0) AS d_oi_usd
FROM h LEFT JOIN rv USING (coin, hour) LEFT JOIN oi USING (coin, hour)
"#;

/// hltrain `merge_stage1` expressions applied to one bucket's partials. One addition
/// versus hltrain: `first_ms`/`last_ms` survive the merge as min/max. `{bucket}` is
/// the bucket in unix seconds (the `hour` column's unit).
pub const MERGE_SQL: &str = r#"
SELECT coin, any_value(coin_class) AS coin_class, hour,
       sum(n_trades) AS n_trades, sum(vol_usd) AS vol_usd, sum(buy_usd) AS buy_usd,
       sum(liq_usd) AS liq_usd, arg_min(first_px, first_ms) AS first_px,
       arg_max(last_px, last_ms) AS last_px, min(first_ms) AS first_ms,
       max(last_ms) AS last_ms, sum(rv2) AS rv2,
       sum(n_minutes) AS n_minutes, sum(d_oi_usd) AS d_oi_usd
FROM parts WHERE hour = {bucket}
GROUP BY coin, hour
"#;

/// Final select: the merged stage-1 columns in frozen order, then the added block.
/// ctxrows holds one row per coin that has a snapshot, so a LEFT JOIN miss is a
/// null block with `ctx_present = false` and `ctx_source = 'none'`. The asset_id
/// join is separate from the ctx block: every coin the mapping knows keeps its
/// asset_id even in hours where no snapshot exists.
pub const FINAL_SELECT: &str = r#"
SELECT s1.*,
       am.asset_id AS asset_id,
       ctx.funding AS ctx_funding,
       ctx.premium AS ctx_premium,
       ctx.open_interest AS ctx_open_interest,
       ctx.mark_px AS ctx_mark_px,
       ctx.oracle_px AS ctx_oracle_px,
       ctx.mid_px AS ctx_mid_px,
       CASE WHEN ctx.oracle_px IS NOT NULL AND ctx.oracle_px <> 0
            THEN (ctx.mark_px - ctx.oracle_px) / ctx.oracle_px END AS ctx_oracle_basis,
       CASE WHEN ctx.open_interest IS NOT NULL AND ctx.prev_open_interest IS NOT NULL
            THEN ctx.open_interest - ctx.prev_open_interest END AS ctx_d_oi,
       (ctx.coin IS NOT NULL) AS ctx_present,
       coalesce(ctx.src, 'none') AS ctx_source
FROM s1
LEFT JOIN ctxrows ctx USING (coin)
LEFT JOIN (SELECT venue_symbol, min(asset_id) AS asset_id FROM asset_map
           GROUP BY venue_symbol) am ON am.venue_symbol = s1.coin
ORDER BY coin, hour
"#;

/// Columns of the s3 daily asset_ctxs CSV, in file order. `time` stays VARCHAR: the
/// pinned connection has no icu extension, and the cast to TIMESTAMPTZ happens in the
/// select (Z-suffixed ISO strings only, so the result is unambiguous).
pub const S3_CSV_COLUMNS: &str = "{'time': 'VARCHAR', 'coin': 'VARCHAR', \
     'funding': 'DOUBLE', 'open_interest': 'DOUBLE', 'prev_day_px': 'DOUBLE', \
     'day_ntl_vlm': 'DOUBLE', 'premium': 'DOUBLE', 'oracle_px': 'DOUBLE', \
     'mark_px': 'DOUBLE', 'mid_px': 'DOUBLE', 'impact_bid_px': 'DOUBLE', \
     'impact_ask_px': 'DOUBLE'}";

/// Columns of the `hl_coin_hour` table, in file order.
pub const TABLE_COLUMNS: &[&str] = &[
    "coin",
    "coin_class",
    "hour",
    "n_trades",
    "vol_usd",
    "buy_usd",
    "liq_usd",
    "first_px",
    "last_px",
    "first_ms",
    "last_ms",
    "rv2",
    "n_minutes",
    "d_oi_usd",
    "asset_id",
    "ctx_funding",
    "ctx_premium",
    "ctx_open_interest",
    "ctx_mark_px",
    "ctx_oracle_px",
    "ctx_mid_px",
    "ctx_oracle_basis",
    "ctx_d_oi",
    "ctx_present",
    "ctx_source",
];

/// SHA-256 over the writer settings that change output bytes.
#[must_use]
pub fn config_hash() -> String {
    let body = json!({
        "schema": SCHEMA,
        "unitVersion": UNIT_VERSION,
        "stage1Version": STAGE1_VERSION,
        "hlTrainCodeSha": HL_TRAIN_CODE_SHA,
        "columns": TABLE_COLUMNS,
        "sort": ["coin", "hour"],
        "guardHours": GUARD_HOURS,
        "ctxSnapshot": "last in [h, h+1h), s3 then capture per coin",
        "ctxDoi": "same-source previous hour snapshot",
        "aggregateThreads": 1,
    });
    sha256_hex(body.to_string().as_bytes())
}

/// One run of the unit.
#[derive(Clone)]
pub struct Request {
    /// Combined store root (`<root>/hl_coin_hour/...`, `<root>/catalog.json`).
    pub out: PathBuf,
    /// hl-fills-v1 root with `watermark.json` and `receipts/YYYYMMDD/H.json`.
    pub fills: PathBuf,
    /// Directory of s3 daily `YYYYMMDD.csv.lz4` asset_ctxs files.
    pub ctx_s3: PathBuf,
    /// Directory of augment capture `YYYY-MM-DD.parquet` asset_contexts files.
    pub ctx_capture: PathBuf,
    /// Lockbox v2 JSON.
    pub lockbox: PathBuf,
    /// Required SHA-256 of the lockbox file; refuse on mismatch.
    /// Lockbox bytes sha256, as verified.
    pub lockbox_sha256: String,
    /// Operator `--provenance` JSON file, copied verbatim into every receipt.
    pub provenance: PathBuf,
    /// Inclusive lower bound for emitted buckets (unix hour), or run to the data floor.
    pub since: Option<i64>,
    /// Inclusive upper bound for emitted buckets (unix hour), clamped to the watermark.
    pub until: Option<i64>,
    /// DuckDB threads for scans (the aggregate always runs on one thread), 1..=8.
    pub threads: u32,
    /// DuckDB memory limit.
    pub memory_limit: String,
    /// Git sha recorded in receipts, or `unknown`.
    pub git_sha: String,
}

/// Run summary printed as one JSON object.
pub struct Report {
    /// The unit name (`hl-hours`).
    pub unit: &'static str,
    /// The writer version.
    pub unit_version: u32,
    /// Buckets the run considered, after the watermark clamp.
    pub emit_buckets: usize,
    /// Buckets newly sealed this run.
    pub built: usize,
    /// Buckets whose matching receipt sealed them before.
    pub skipped: usize,
    /// Considered buckets inside the lockbox exclusion.
    pub lockbox_hours_dropped: usize,
    /// Buckets the input watermark cannot close yet.
    pub refused_hours: usize,
    /// The first refused bucket tags, capped at 20.
    pub refused_examples: Vec<String>,
    /// Rows across built and skipped buckets.
    pub coin_hour_rows: u64,
    /// Source hour files stage 1 ran over.
    pub source_hours_read: usize,
    /// Fills read across those files, all checked against receipts.
    pub source_rows_read: u64,
    /// Coin-hours whose ctx block came from each lane.
    pub ctx_rows_by_source: BTreeMap<String, u64>,
    /// Hours in the input watermark.
    pub watermark_hours: usize,
    /// SHA-256 of the watermark file bytes (read once).
    pub watermark_sha256: String,
    /// Newest watermark hour, source key form.
    pub watermark_max_hour: String,
    /// Lockbox bytes sha256, as verified.
    pub lockbox_sha256: String,
    /// HL rows in `mapping/assets.csv`, when the mapping unit has run.
    pub mapping_rows: Option<u64>,
    /// Wall seconds.
    pub seconds: f64,
}

impl Report {
    /// JSON object for stdout.
    #[must_use]
    pub fn to_json(&self) -> String {
        let ctx: serde_json::Map<String, Value> = self
            .ctx_rows_by_source
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        json!({
            "unit": self.unit,
            "unitVersion": self.unit_version,
            "stage1Version": STAGE1_VERSION,
            "schema": SCHEMA,
            "emitBuckets": self.emit_buckets,
            "built": self.built,
            "skipped": self.skipped,
            "lockboxHoursDropped": self.lockbox_hours_dropped,
            "refusedHours": self.refused_hours,
            "refusedExamples": self.refused_examples,
            "coinHourRows": self.coin_hour_rows,
            "sourceHoursRead": self.source_hours_read,
            "sourceRowsRead": self.source_rows_read,
            "ctxRowsBySource": ctx,
            "watermarkHours": self.watermark_hours,
            "watermarkSha256": self.watermark_sha256,
            "watermarkMaxHour": self.watermark_max_hour,
            "lockboxSha256": self.lockbox_sha256,
            "mappingRows": self.mapping_rows,
            "configSha256": config_hash(),
            "seconds": self.seconds,
            "ok": true,
        })
        .to_string()
    }
}

/// The hl-fills-v1 input watermark, read once per run.
struct Watermark {
    hours: BTreeSet<i64>,
    keys: BTreeMap<i64, String>,
    sha256: String,
    max_hour: i64,
}

impl Watermark {
    fn load(path: &Path) -> Result<Self, StoreError> {
        let bytes = std::fs::read(path)
            .map_err(|e| StoreError::Check(format!("input watermark {}: {e}", path.display())))?;
        let sha = sha256_hex(&bytes);
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|e| StoreError::Check(format!("input watermark: {e}")))?;
        let hours_value = value
            .get("hours")
            .and_then(Value::as_array)
            .ok_or_else(|| StoreError::Check("input watermark has no hours array".into()))?;
        let mut hours = BTreeSet::new();
        let mut keys = BTreeMap::new();
        for h in hours_value {
            let key = h
                .as_str()
                .ok_or_else(|| StoreError::Check("watermark hour is not a string".into()))?;
            let unix = source_hour_to_hour(key)?;
            hours.insert(unix);
            keys.insert(unix, key.to_owned());
        }
        let max_hour = *hours
            .iter()
            .next_back()
            .ok_or_else(|| StoreError::Check("input watermark is empty".into()))?;
        Ok(Self {
            hours,
            keys,
            sha256: sha,
            max_hour,
        })
    }

    fn key(&self, hour: i64) -> Option<&str> {
        self.keys.get(&hour).map(String::as_str)
    }
}

/// `YYYYMMDD/H` (the hl-fills-v1 hour key) to a unix hour.
fn source_hour_to_hour(key: &str) -> Result<i64, StoreError> {
    let (day, hour) = key
        .split_once('/')
        .ok_or_else(|| StoreError::Check(format!("watermark hour {key:?} is not YYYYMMDD/H")))?;
    let d = grid::parse_date(&format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]))
        .map_err(|e| StoreError::Check(format!("watermark hour {key:?}: {e}")))?;
    let h: i64 = hour
        .parse()
        .map_err(|_| StoreError::Check(format!("watermark hour {key:?}: bad hour")))?;
    if !(0..24).contains(&h) {
        return Err(StoreError::Check(format!(
            "watermark hour {key:?}: hour out of range"
        )));
    }
    Ok(grid::hour_of_secs(super::grid::day_start_secs(d)) + h)
}

/// `YYYYMMDD` of a unix hour, for the s3 daily file names.
#[must_use]
fn compact_day(hour: i64) -> String {
    grid::date_of_hour(hour).replace('-', "")
}

/// One source hour's hl-fills-v1 receipt.
#[derive(Clone)]
struct SourceReceipt {
    input_sha256: String,
    output_rows: i64,
    files: Vec<String>,
}

impl SourceReceipt {
    fn load(root: &Path, key: &str) -> Result<Self, StoreError> {
        let (day, hour) = key
            .split_once('/')
            .ok_or_else(|| StoreError::Check(format!("source hour {key:?} is not YYYYMMDD/H")))?;
        let path = root.join("receipts").join(day).join(format!("{hour}.json"));
        let text = std::fs::read_to_string(&path)
            .map_err(|e| StoreError::Check(format!("source receipt {}: {e}", path.display())))?;
        let value: Value =
            serde_json::from_str(&text).map_err(|e| StoreError::Check(format!("{path:?}: {e}")))?;
        let input_sha256 = value
            .get("input_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::Check(format!("{key}: no input_sha256")))?
            .to_owned();
        let output_rows = value
            .get("output_rows")
            .and_then(Value::as_i64)
            .ok_or_else(|| StoreError::Check(format!("{key}: no output_rows")))?;
        let mut files = Vec::new();
        for out in value
            .get("outputs")
            .and_then(Value::as_array)
            .ok_or_else(|| StoreError::Check(format!("{key}: no outputs")))?
        {
            let rel = out
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| StoreError::Check(format!("{key}: output without path")))?;
            files.push(root.join(rel).to_string_lossy().to_string());
        }
        if files.is_empty() {
            return Err(StoreError::Check(format!(
                "{key}: receipt lists no output files"
            )));
        }
        Ok(Self {
            input_sha256,
            output_rows,
            files,
        })
    }
}

fn files_list_sql(files: &[String]) -> String {
    let inner: Vec<String> = files.iter().map(|f| sql_string(f)).collect();
    format!("[{}]", inner.join(", "))
}

fn count_rows(conn: &Connection, sql: &str) -> Result<u64, StoreError> {
    let n: i64 = conn
        .query_row(sql, [], |row| row.get(0))
        .map_err(|e| StoreError::Check(format!("count query: {e}")))?;
    u64::try_from(n).map_err(|_| StoreError::Check("negative count".into()))
}

/// The asset_ctxs block for one bucket: one row per coin that has a snapshot.
#[derive(Clone)]
struct CtxRow {
    coin: String,
    src: &'static str,
    funding: Option<f64>,
    open_interest: Option<f64>,
    premium: Option<f64>,
    oracle_px: Option<f64>,
    mark_px: Option<f64>,
    mid_px: Option<f64>,
    prev_open_interest: Option<f64>,
}

/// Per-day s3 lz4 CSV and capture parquet loader over one connection.
struct CtxSource {
    con: Connection,
    s3_dir: PathBuf,
    capture_dir: PathBuf,
    loaded_s3_day: Option<String>,
    capture_days: Option<BTreeSet<String>>,
}

impl CtxSource {
    fn new(con: Connection, s3_dir: PathBuf, capture_dir: PathBuf) -> Self {
        Self {
            con,
            s3_dir,
            capture_dir,
            loaded_s3_day: None,
            capture_days: None,
        }
    }

    fn load_s3_day(&mut self, day: &str, alias: &str) -> Result<bool, StoreError> {
        self.con
            .execute_batch(&format!("DROP TABLE IF EXISTS {alias}"))
            .map_err(|e| StoreError::Check(format!("drop {alias}: {e}")))?;
        let src = self.s3_dir.join(format!("{day}.csv.lz4"));
        if !src.is_file() {
            return Ok(false);
        }
        let raw = decode_lz4(&src)?;
        let csv_path = self.con_dir()?.join(format!("asset_ctxs_{day}.csv"));
        {
            let mut file = std::fs::File::create(&csv_path)
                .map_err(|e| StoreError::Check(format!("ctx temp csv: {e}")))?;
            std::io::Write::write_all(&mut file, &raw)
                .map_err(|e| StoreError::Check(format!("ctx temp csv: {e}")))?;
        }
        // The pinned connection cannot load the icu extension, so the CSV reader
        // cannot convert TIMESTAMPTZ itself; read the column as VARCHAR and cast
        // (Z-suffixed ISO timestamps cast fine, and epoch_ms is absolute).
        let sql = format!(
            "CREATE TEMP TABLE {alias} AS SELECT coin, \
             epoch_ms(time::TIMESTAMPTZ) AS at_ms, funding, \
             open_interest, premium, oracle_px, mark_px, mid_px FROM read_csv({}, \
             header = true, columns = {S3_CSV_COLUMNS})",
            sql_string(&csv_path.to_string_lossy())
        );
        self.con
            .execute_batch(&sql)
            .map_err(|e| StoreError::Check(format!("ctx csv load {day}: {e}")))?;
        let _ = std::fs::remove_file(&csv_path);
        Ok(true)
    }

    /// The spill dir the connection was opened with (`<out>/.tmp-duckdb`).
    fn con_dir(&self) -> Result<PathBuf, StoreError> {
        let rows = crate::store::query_rows(
            &self.con,
            "SELECT current_setting('temp_directory') AS dir",
            &[],
        )?;
        rows.first()
            .and_then(|row| row.str("dir").map(PathBuf::from))
            .ok_or_else(|| StoreError::Check("temp_directory setting missing".into()))
    }

    fn capture_day_set(&mut self) -> Result<BTreeSet<String>, StoreError> {
        if let Some(set) = &self.capture_days {
            return Ok(set.clone());
        }
        let mut set = BTreeSet::new();
        let entries = std::fs::read_dir(&self.capture_dir)
            .map_err(|e| StoreError::Check(format!("capture dir: {e}")))?;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".parquet") {
                set.insert(stem.replace('-', ""));
            }
        }
        self.capture_days = Some(set.clone());
        Ok(set)
    }

    /// Last snapshot per coin with `at_ms` in `[w0, w1)`.
    fn window(&self, table: &str, w0: i64, w1: i64) -> Result<Vec<CtxRow>, StoreError> {
        let sql = format!(
            "SELECT coin, funding, open_interest, premium, oracle_px, mark_px, mid_px FROM (\
             SELECT *, row_number() OVER (PARTITION BY coin ORDER BY at_ms DESC) AS rn \
             FROM {table} WHERE at_ms >= {w0} AND at_ms < {w1}) WHERE rn = 1"
        );
        let mut statement = self
            .con
            .prepare(&sql)
            .map_err(|e| StoreError::Check(format!("ctx window: {e}")))?;
        let mut rows = statement
            .query([])
            .map_err(|e| StoreError::Check(format!("ctx window: {e}")))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| StoreError::Check(format!("ctx window: {e}")))?
        {
            let coin: String = row
                .get(0)
                .map_err(|e| StoreError::Check(format!("ctx window: {e}")))?;
            out.push(CtxRow {
                coin,
                src: "",
                funding: row.get(1).ok(),
                open_interest: row.get(2).ok(),
                premium: row.get(3).ok(),
                oracle_px: row.get(4).ok(),
                mark_px: row.get(5).ok(),
                mid_px: row.get(6).ok(),
                prev_open_interest: None,
            });
        }
        Ok(out)
    }

    /// Per-coin snapshots for the bucket: s3 first, capture per-coin fallback.
    ///
    /// `ctx_d_oi` baselines come from the same source's previous-hour window; for
    /// hour 0 that is the previous day's file, loaded on demand.
    fn hour_ctx(&mut self, bucket: i64) -> Result<Vec<CtxRow>, StoreError> {
        let w0 = grid::hour_start(bucket) * 1000;
        let w1 = w0 + grid::HOUR_MS;
        let pw0 = w0 - grid::HOUR_MS;
        let pw1 = w0;
        let day = compact_day(bucket);
        let prev_day = compact_day(bucket - 1);

        let mut s3_cur: BTreeMap<String, CtxRow> = BTreeMap::new();
        let mut s3_prev: BTreeMap<String, f64> = BTreeMap::new();
        if self.loaded_s3_day.as_deref() != Some(day.as_str()) && self.load_s3_day(&day, "ctxd")? {
            self.loaded_s3_day = Some(day.clone());
        }
        if self.loaded_s3_day.as_deref() == Some(day.as_str()) {
            for row in self.window("ctxd", w0, w1)? {
                s3_cur.insert(row.coin.clone(), row);
            }
            if !s3_cur.is_empty() {
                let prev_rows = if prev_day == day {
                    self.window("ctxd", pw0, pw1)?
                } else if self.load_s3_day(&prev_day, "ctxd_prev")? {
                    self.window("ctxd_prev", pw0, pw1)?
                } else {
                    Vec::new()
                };
                for row in prev_rows {
                    if let Some(oi) = row.open_interest {
                        s3_prev.insert(row.coin, oi);
                    }
                }
                if self.loaded_s3_day.as_deref() != Some(day.as_str()) {
                    self.load_s3_day(&day, "ctxd")?;
                    self.loaded_s3_day = Some(day.clone());
                }
            }
        }

        let mut cap_cur: BTreeMap<String, CtxRow> = BTreeMap::new();
        let mut cap_prev: BTreeMap<String, f64> = BTreeMap::new();
        if self.capture_day_set()?.contains(&day) {
            let capture_dir = self.capture_dir.clone();
            let capture_table = move |d: &str| {
                let path =
                    capture_dir.join(format!("{}-{}-{}.parquet", &d[0..4], &d[4..6], &d[6..8]));
                format!(
                    "(SELECT symbol AS coin, funding, open_interest, premium, oracle_px, \
                     mark_px, mid_px, at_ms FROM read_parquet({}))",
                    sql_string(&path.to_string_lossy())
                )
            };
            for row in self.window(&capture_table(&day), w0, w1)? {
                cap_cur.insert(row.coin.clone(), row);
            }
            if !cap_cur.is_empty() && self.capture_day_set()?.contains(&prev_day) {
                for row in self.window(&capture_table(&prev_day), pw0, pw1)? {
                    if let Some(oi) = row.open_interest {
                        cap_prev.insert(row.coin, oi);
                    }
                }
            }
        }

        let mut out = Vec::new();
        for (coin, row) in &s3_cur {
            let mut row = row.clone();
            row.prev_open_interest = s3_prev.get(coin).copied();
            row.src = "s3";
            out.push(row);
        }
        for (coin, row) in &cap_cur {
            if s3_cur.contains_key(coin) {
                continue;
            }
            let mut row = row.clone();
            row.prev_open_interest = cap_prev.get(coin).copied();
            row.src = "capture";
            out.push(row);
        }
        Ok(out)
    }
}

/// Decode an lz4 frame file to bytes.
fn decode_lz4(path: &Path) -> Result<Vec<u8>, StoreError> {
    let file = std::fs::File::open(path)
        .map_err(|e| StoreError::Check(format!("open {}: {e}", path.display())))?;
    let mut decoder = lz4::Decoder::new(file)
        .map_err(|e| StoreError::Check(format!("lz4 {}: {e}", path.display())))?;
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Check(format!("lz4 {}: {e}", path.display())))?;
    Ok(out)
}

/// Register `<root>/mapping/assets.csv` (written by the mapping unit) when it exists.
///
/// Every ctxrows row carries a null asset_id until the UPDATE fills it from the
/// mapping, so the final select is the same whether or not the file exists.
fn load_mapping(conn: &Connection, out: &Path) -> Result<Option<u64>, StoreError> {
    let path = out.join("mapping").join("assets.csv");
    if !path.is_file() {
        // The final select joins asset_map unconditionally; keep an empty relation
        // so the query binds either way.
        conn.execute_batch(
            "CREATE OR REPLACE TEMP TABLE asset_map AS \
             SELECT cast(null as varchar) AS venue_symbol, \
                    cast(null as varchar) AS asset_id \
             WHERE false",
        )
        .map_err(|e| StoreError::Check(format!("empty asset_map: {e}")))?;
        return Ok(None);
    }
    // PLAN column order: asset_id, venue, venue_symbol, builder, coin_class,
    // venue_market_id, first_ts, last_ts, source_of_truth, mapped, reason,
    // median_abs_log_ratio, n_hours.
    let columns = "{'asset_id': 'VARCHAR', 'venue': 'VARCHAR', 'venue_symbol': 'VARCHAR', \
                   'builder': 'VARCHAR', 'coin_class': 'VARCHAR', 'venue_market_id': 'VARCHAR', \
                   'first_ts': 'VARCHAR', 'last_ts': 'VARCHAR', 'source_of_truth': 'VARCHAR', \
                   'mapped': 'VARCHAR', 'reason': 'VARCHAR', \
                   'median_abs_log_ratio': 'DOUBLE', 'n_hours': 'BIGINT'}";
    let sql = format!(
        "CREATE OR REPLACE TEMP TABLE asset_map AS SELECT asset_id, venue_symbol \
         FROM read_csv({}, header = true, columns = {columns}) WHERE venue = 'hl'",
        sql_string(&path.to_string_lossy())
    );
    conn.execute_batch(&sql)
        .map_err(|e| StoreError::Check(format!("mapping load: {e}")))?;
    Ok(Some(count_rows(conn, "SELECT count(*) FROM asset_map")?))
}

/// Run the unit: stage 1 per receipted source file, merge per bucket, ctx, lockbox,
/// durable write, receipts, catalog. See the module doc for the refusal and drop
/// rules.
///
/// # Errors
///
/// Fails on a lockbox sha mismatch or wrong v2 shape, a missing source receipt, a
/// source row count that disagrees with the receipt, a combined root already owned by
/// another run, or any DuckDB or I/O error. Hours already sealed stay sealed; a rerun
/// recomputes the failing hour.
#[allow(clippy::too_many_lines)]
pub fn run(request: &Request) -> Result<Report, StoreError> {
    let started = grid::now_rfc3339();
    let clock = std::time::Instant::now();
    if !(1..=8).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=8".into()));
    }
    let memory_limit = crate::augment::hlfills::parse_memory_limit(&request.memory_limit)?;
    let provenance = mapping::load_provenance(&request.provenance)?;
    let lockbox = lockbox::load(&request.lockbox, &request.lockbox_sha256)?;
    let lockbox_hours = lockbox.excluded_hours()?;
    lockbox.assert_v2_shape(&lockbox_hours)?;

    let watermark = Watermark::load(&request.fills.join("watermark.json"))?;
    let first_receipted = *watermark
        .hours
        .iter()
        .next()
        .ok_or_else(|| StoreError::Check("input watermark is empty".into()))?;
    let last_closed = watermark.max_hour - (1 + GUARD_HOURS);
    let first_bucket = request.since.unwrap_or(first_receipted);
    let last_bucket = request.until.unwrap_or(last_closed).min(last_closed);
    if last_bucket < first_bucket {
        return Err(StoreError::Check(format!(
            "empty bucket range: since {} is after the closed watermark {}",
            grid::hour_key(first_bucket),
            grid::hour_key(last_bucket)
        )));
    }
    let buckets: Vec<i64> = std::iter::successors(Some(first_bucket), |b| {
        Some(b + 1).filter(|b| *b <= last_bucket)
    })
    .collect();

    create_dir_all_durable(&request.out)?;
    let _guard = mapping::lock_output(&request.out)?;
    let _temp = mapping::TempClean::new(&request.out);
    let conn = mapping::open_reader(&request.out, request.threads, &memory_limit)?;

    let mut report = Report {
        unit: UNIT,
        unit_version: UNIT_VERSION,
        emit_buckets: buckets.len(),
        built: 0,
        skipped: 0,
        lockbox_hours_dropped: 0,
        refused_hours: 0,
        refused_examples: Vec::new(),
        coin_hour_rows: 0,
        source_hours_read: 0,
        source_rows_read: 0,
        ctx_rows_by_source: BTreeMap::from([
            ("s3".to_string(), 0u64),
            ("capture".to_string(), 0u64),
            ("none".to_string(), 0u64),
        ]),
        watermark_hours: watermark.hours.len(),
        watermark_sha256: watermark.sha256.clone(),
        watermark_max_hour: watermark
            .key(watermark.max_hour)
            .unwrap_or_default()
            .to_owned(),
        lockbox_sha256: request.lockbox_sha256.trim().to_ascii_lowercase(),
        mapping_rows: None,
        seconds: 0.0,
    };

    let mut built_files: Vec<receipt::OutputFile> = Vec::new();
    execute(
        request,
        &conn,
        &provenance,
        &watermark,
        &lockbox_hours,
        &buckets,
        &mut report,
        &mut built_files,
    )?;
    let rows_total = report.coin_hour_rows;

    let finished = grid::now_rfc3339();
    receipt::write(
        &request.out,
        TABLE,
        &receipt::Receipt {
            unit: UNIT.to_owned(),
            unit_version: UNIT_VERSION,
            rows: rows_total,
            outputs: built_files.clone(),
            inputs: json!({
                "fills_watermark": {
                    "sha256": watermark.sha256,
                    "hours": watermark.hours.len(),
                    "max_hour": watermark.key(watermark.max_hour),
                },
                "lockbox_sha256": request.lockbox_sha256.trim().to_ascii_lowercase(),
            }),
            lockbox_sha256: request.lockbox_sha256.trim().to_ascii_lowercase(),
            // Dropped = buckets this run considered that sit in the exclusion set;
            // excluded = the set's own v2 size.
            lockbox_hours_dropped: report.lockbox_hours_dropped as u64,
            lockbox_hours_excluded: lockbox_hours.len() as u64,
            config_sha256: config_hash(),
            provenance,
            started_utc: started,
            finished_utc: finished,
        },
    )?;
    let mut cat = catalog::load(&request.out)?;
    for file in &built_files {
        cat.upsert(
            catalog::FileEntry {
                path: file.path.clone(),
                rows: file.rows,
                bytes: file.bytes,
                sha256: file.sha256.clone(),
                unit: UNIT.to_owned(),
                unit_version: UNIT_VERSION,
            },
            UNIT,
            json!({
                "fills_watermark_sha256": watermark.sha256,
                "fills_watermark_hours": watermark.hours.len(),
                "stage1_version": STAGE1_VERSION,
                "hl_train_code_sha": HL_TRAIN_CODE_SHA,
                "lockbox_sha256": request.lockbox_sha256.trim().to_ascii_lowercase(),
            }),
        );
    }
    cat.save(&request.out)?;

    report.seconds = clock.elapsed().as_secs_f64();
    Ok(report)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn execute(
    request: &Request,
    conn: &Connection,
    provenance: &Value,
    watermark: &Watermark,
    lockbox_hours: &BTreeSet<i64>,
    buckets: &[i64],
    report: &mut Report,
    built_files: &mut Vec<receipt::OutputFile>,
) -> Result<(), StoreError> {
    // Source files: every bucket needs h-1, h, h+1 receipted, so stage 1 runs over the
    // union, one file at a time, each verified against its receipt first.
    conn.execute_batch(
        "CREATE TEMP TABLE parts (coin VARCHAR, coin_class VARCHAR, hour BIGINT, \
         n_trades BIGINT, vol_usd DOUBLE, buy_usd DOUBLE, liq_usd DOUBLE, \
         first_px DOUBLE, last_px DOUBLE, first_ms BIGINT, last_ms BIGINT, \
         rv2 DOUBLE, n_minutes BIGINT, d_oi_usd DOUBLE)",
    )
    .map_err(|e| StoreError::Check(format!("parts table: {e}")))?;
    report.mapping_rows = load_mapping(conn, &request.out)?;

    let mut src_cache: BTreeMap<i64, SourceReceipt> = BTreeMap::new();
    let mut src_files: BTreeSet<i64> = BTreeSet::new();
    for b in buckets {
        src_files.insert(b - 1);
        src_files.insert(*b);
        src_files.insert(b + 1);
    }
    for u in &src_files {
        let Some(key) = watermark.key(*u) else {
            continue;
        };
        let outcome = (|| -> Result<SourceReceipt, StoreError> {
            let receipt = SourceReceipt::load(&request.fills, key)?;
            let files = files_list_sql(&receipt.files);
            let n = count_rows(conn, &format!("SELECT count(*) FROM read_parquet({files})"))?;
            if n != u64::try_from(receipt.output_rows).unwrap_or(u64::MAX) {
                return Err(StoreError::Check(format!(
                    "read {n} rows, receipt says {}",
                    receipt.output_rows
                )));
            }
            // The frozen aggregate runs single-threaded: parallel float sums are not
            // reproducible, and the frozen smoke config also used one thread.
            conn.execute_batch("SET threads TO 1")
                .map_err(|e| StoreError::Check(format!("threads: {e}")))?;
            conn.execute_batch(&format!(
                "CREATE TEMP TABLE stage1_tmp AS {}",
                STAGE1_SQL.replace("{files}", &files)
            ))
            .map_err(|e| StoreError::Check(format!("stage 1: {e}")))?;
            conn.execute_batch(&format!("SET threads TO {}", request.threads))
                .map_err(|e| StoreError::Check(format!("threads: {e}")))?;
            conn.execute_batch(
                "INSERT INTO parts SELECT * FROM stage1_tmp; DROP TABLE stage1_tmp;",
            )
            .map_err(|e| StoreError::Check(format!("stage 1: {e}")))?;
            Ok(receipt)
        })();
        match outcome {
            Ok(receipt) => {
                report.source_hours_read += 1;
                report.source_rows_read += count_of(&receipt);
                src_cache.insert(*u, receipt);
            }
            Err(e) => return Err(StoreError::Check(format!("{key}: {e}"))),
        }
    }

    let mut ctx = CtxSource::new(
        conn.try_clone()
            .map_err(|e| StoreError::Check(format!("conn: {e}")))?,
        request.ctx_s3.clone(),
        request.ctx_capture.clone(),
    );

    for b in buckets {
        let complete = [b - 1, *b, b + 1].iter().all(|u| src_cache.contains_key(u));
        let closed = watermark.max_hour >= b + (1 + GUARD_HOURS);
        if !complete || !closed {
            report.refused_hours += 1;
            if report.refused_examples.len() < 20 {
                report.refused_examples.push(grid::hour_key(*b));
            }
            continue;
        }
        if lockbox::is_excluded(lockbox_hours, *b) {
            report.lockbox_hours_dropped += 1;
            continue;
        }
        let facts: Vec<Value> = [b - 1, *b, b + 1]
            .iter()
            .filter_map(|u| src_cache.get(u).map(|r| (u, r)))
            .map(|(u, r)| {
                json!({
                    "hour": watermark.key(*u).unwrap_or_default(),
                    "sourceInputSha256": r.input_sha256,
                    "sourceOutputRows": r.output_rows,
                })
            })
            .collect();
        let file = emit_bucket(
            request,
            conn,
            &mut ctx,
            provenance,
            *b,
            &facts,
            watermark,
            lockbox_hours,
            report,
        )?;
        built_files.push(file);
    }
    Ok(())
}

fn count_of(receipt: &SourceReceipt) -> u64 {
    u64::try_from(receipt.output_rows).unwrap_or(0)
}

/// Merge partials for one time bucket, join ctx, write the Parquet file durably, and
/// seal its receipt. Returns the sealed file for the catalog.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn emit_bucket(
    request: &Request,
    conn: &Connection,
    ctx: &mut CtxSource,
    provenance: &Value,
    bucket: i64,
    facts: &[Value],
    watermark: &Watermark,
    lockbox_hours: &BTreeSet<i64>,
    report: &mut Report,
) -> Result<receipt::OutputFile, StoreError> {
    let date = grid::date_of_hour(bucket);
    let tag = grid::hour_key(bucket);
    let final_path = request
        .out
        .join(TABLE)
        .join(format!("date={date}"))
        .join(format!("hour={tag}.parquet"));
    let relative = format!("{TABLE}/date={date}/hour={tag}.parquet");
    let receipt_path = request
        .out
        .join(TABLE)
        .join("receipts")
        .join(format!("hour={tag}.json"));

    // Idempotent skip: the receipt was sealed for the same unit version and the same
    // source input facts, and the sealed file is still there.
    if let Ok(old) = std::fs::read_to_string(&receipt_path).and_then(|text| {
        serde_json::from_str::<Value>(&text).map_err(|_| std::io::Error::other("bad receipt json"))
    }) {
        {
            let same_version =
                old.get("unitVersion").and_then(Value::as_u64) == Some(u64::from(UNIT_VERSION));
            let same_sources =
                old.get("input").and_then(|i| i.get("sources")) == Some(&json!(facts));
            if same_version && same_sources && final_path.is_file() {
                report.skipped += 1;
                if let Some(rows) = old.get("rows").and_then(Value::as_i64) {
                    report.coin_hour_rows += u64::try_from(rows).unwrap_or(0);
                }
                let bytes = std::fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
                let sha = file_hash(&final_path).unwrap_or_default();
                return Ok(receipt::OutputFile {
                    path: relative,
                    rows: u64::try_from(old.get("rows").and_then(Value::as_i64).unwrap_or(0))
                        .unwrap_or(0),
                    bytes,
                    sha256: sha,
                });
            }
        }
    }

    // parts.hour is unix seconds (the frozen stage-1 convention); `bucket` is an
    // hour count.
    conn.execute_batch("DROP TABLE IF EXISTS s1")
        .map_err(|e| StoreError::Check(format!("drop s1: {e}")))?;
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE s1 AS {}",
        MERGE_SQL.replace("{bucket}", &(bucket * grid::HOUR_SECS).to_string())
    ))
    .map_err(|e| StoreError::Check(format!("merge bucket {tag}: {e}")))?;
    conn.execute_batch("DROP TABLE IF EXISTS ctxrows")
        .map_err(|e| StoreError::Check(format!("drop ctxrows: {e}")))?;
    conn.execute_batch(
        "CREATE TEMP TABLE ctxrows (coin VARCHAR, src VARCHAR, \
         funding DOUBLE, open_interest DOUBLE, premium DOUBLE, oracle_px DOUBLE, \
         mark_px DOUBLE, mid_px DOUBLE, prev_open_interest DOUBLE)",
    )
    .map_err(|e| StoreError::Check(format!("ctxrows: {e}")))?;

    let ctx_rows = ctx.hour_ctx(bucket)?;
    if !ctx_rows.is_empty() {
        let mut sql = String::from("INSERT INTO ctxrows VALUES ");
        for (i, row) in ctx_rows.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({}, {}, {}, {}, {}, {}, {}, {}, {})",
                sql_string(&row.coin),
                sql_string(row.src),
                opt_f64(row.funding),
                opt_f64(row.open_interest),
                opt_f64(row.premium),
                opt_f64(row.oracle_px),
                opt_f64(row.mark_px),
                opt_f64(row.mid_px),
                opt_f64(row.prev_open_interest),
            ));
        }
        conn.execute_batch(&sql)
            .map_err(|e| StoreError::Check(format!("ctxrows insert: {e}")))?;
    }
    let io = |step: &str, e: std::io::Error| StoreError::Check(format!("{tag}: {step}: {e}"));
    if let Some(parent) = receipt_path.parent() {
        create_dir_all_durable(parent).map_err(|e| io("receipt dir", e))?;
    }
    let tmp = tmp_path(&final_path);
    let _ = std::fs::remove_file(&tmp);
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent).map_err(|e| io("out dir", e))?;
    }
    let copy = crate::decoder::parquet::copy_statement(FINAL_SELECT, &tmp);
    conn.execute_batch(&copy)
        .map_err(|e| StoreError::Check(format!("copy {tag}: {e}")))?;
    let rows = count_rows(
        conn,
        &format!(
            "SELECT count(*) FROM read_parquet({})",
            sql_string(&tmp.to_string_lossy())
        ),
    )?;
    let expected = count_rows(conn, "SELECT count(*) FROM s1")?;
    if rows != expected {
        let _ = std::fs::remove_file(&tmp);
        return Err(StoreError::Check(format!(
            "{tag}: parquet has {rows} rows, merge produced {expected}"
        )));
    }
    sync_tmp_file(&tmp).map_err(|e| io("fsync tmp", e))?;
    std::fs::rename(&tmp, &final_path).map_err(|e| io("rename", e))?;
    if let Some(parent) = final_path.parent() {
        sync_dir_after_rename(parent).map_err(|e| io("fsync dir", e))?;
    }
    let sha = file_hash(&final_path).map_err(|e| io("hash", e))?;
    let bytes = std::fs::metadata(&final_path)
        .map_err(|e| io("stat", e))?
        .len();

    let none_n = count_rows(
        conn,
        "SELECT count(*) FROM (SELECT coin FROM s1 \
         WHERE coin NOT IN (SELECT coin FROM ctxrows))",
    )?;
    let s3_n = count_rows(
        conn,
        "SELECT count(*) FROM s1 WHERE coin IN \
         (SELECT coin FROM ctxrows WHERE src = 's3')",
    )?;
    let cap_n = count_rows(
        conn,
        "SELECT count(*) FROM s1 WHERE coin IN \
         (SELECT coin FROM ctxrows WHERE src = 'capture')",
    )?;
    *report
        .ctx_rows_by_source
        .entry("s3".to_string())
        .or_insert(0) += s3_n;
    *report
        .ctx_rows_by_source
        .entry("capture".to_string())
        .or_insert(0) += cap_n;
    *report
        .ctx_rows_by_source
        .entry("none".to_string())
        .or_insert(0) += none_n;

    let receipt = json!({
        "schema": SCHEMA,
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "stage1Version": STAGE1_VERSION,
        "hlTrainCodeSha": HL_TRAIN_CODE_SHA,
        "hour": tag,
        "bucketHour": bucket,
        "rows": rows,
        "input": {
            "watermarkSha256": watermark.sha256,
            "sources": facts,
        },
        "output": {
            "path": relative,
            "rows": rows,
            "bytes": bytes,
            "sha256": sha,
        },
        "ctx": {"s3": s3_n, "capture": cap_n, "none": none_n},
        "lockbox": {
            "sha256": request.lockbox_sha256.trim().to_ascii_lowercase(),
            "dropped": false,
            "excludedHours": lockbox_hours.len(),
        },
        "configSha256": config_hash(),
        "gitSha": request.git_sha,
        "provenance": provenance,
    });
    let mut body =
        serde_json::to_vec(&receipt).map_err(|e| StoreError::Check(format!("receipt: {e}")))?;
    body.push(b'\n');
    write_durable(&receipt_path, &body).map_err(|e| io("receipt", e))?;
    report.built += 1;
    report.coin_hour_rows += rows;
    Ok(receipt::OutputFile {
        path: relative,
        rows,
        bytes,
        sha256: sha,
    })
}

fn opt_f64(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v}"),
        _ => "null".to_string(),
    }
}

#[cfg(test)]
mod tests;
