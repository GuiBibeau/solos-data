//! U2c: `solos-data augment combined pyth-hours` builds
//! `<out>/pyth_coin_hour/` from the Pyth price lanes (ADR-0023, DESIGN.md
//! 4.3): per (feed, hour) the update count, the last price in the hour, the
//! in-hour log return, the mean relative confidence, and the staleness at the
//! hour end.
//!
//! Inputs are plain Parquet lanes, each argument one lane read in full: the
//! venues-history dumps first, then the live capture (2026-10-18 onward).
//! Identical `(feed_id, publish_time)` rows across lanes dedupe to one; the
//! later lane wins, then the later `at_ms`.
//!
//! `price_f` and `conf_f` are the decoder's float projections of the scaled
//! on-chain integers. Leakage rule: ordering uses `publish_time` (the Pythnet
//! clock), never `at_ms` (the collector's arrival). The asset join reads
//! `<out>/mapping/assets.csv` (U1): `venue = pyth`, `venue_market_id =
//! feed_id`.

use crate::fsutil::create_dir_all_durable;
use crate::store::{StoreError, query_rows};
use serde_json::json;
use std::path::{Path, PathBuf};

use super::grid;
use super::io::{
    self, FileRef, HourOutput, TempClean, file_list_sha256, finish_run, load_provenance,
    lock_output, open_reader, parquet_list, parse_memory_cap, plan_hours, skip_sealed_hours,
};

/// The unit this module runs.
pub const UNIT: &str = "pyth-hours";

/// The table this unit writes under the output root.
pub const TABLE: &str = "pyth_coin_hour";

/// Bumped on an output-shape change.
pub const UNIT_VERSION: u32 = 1;

/// An output hour is sealed only when the input watermark covers the hour end
/// plus this guard (DESIGN.md 3).
pub const HOUR_GUARD_SECS: i64 = 2 * grid::HOUR_SECS;

/// Output columns, in file order.
pub const COLUMNS: &[(&str, &str)] = &[
    ("hour", "BIGINT"),
    ("feed_id", "VARCHAR"),
    ("symbol", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("py_n", "BIGINT"),
    ("py_last", "DOUBLE"),
    ("py_ret", "DOUBLE"),
    ("py_conf_rel", "DOUBLE"),
    ("py_stale_s", "BIGINT"),
];

/// One offline pyth-hours run.
pub struct Request<'a> {
    /// Combined store root; `pyth_coin_hour/` is written under it.
    pub out: &'a Path,
    /// Lockbox v2 JSON; its hours are dropped from the table.
    pub lockbox: &'a Path,
    /// Required sha256 of the lockbox bytes.
    pub lockbox_sha256: &'a str,
    /// Operator provenance object, copied verbatim into the receipts.
    pub provenance: &'a Path,
    /// Pyth price directories, in lane order (history first, live last).
    pub pyth_prices: &'a [PathBuf],
    /// Inclusive UTC date of the first hour built (required, bounded runs).
    pub since: &'a str,
    /// Inclusive UTC date of the last hour built (required, bounded runs).
    pub until: &'a str,
    /// DuckDB threads (1..=8).
    pub threads: u32,
    /// DuckDB memory limit (at most 32GB).
    pub memory_limit: String,
}

/// The run's outcome, printed by the CLI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// Data rows across the built and verified hours.
    pub rows: u64,
    /// Hours sealed by this run.
    pub hours_written: u64,
    /// Hours skipped because a matching receipt and file already exist.
    pub hours_skipped: u64,
    /// Requested hours with no source rows (no file, left to assembly).
    pub hours_empty: u64,
    /// Lockbox hours inside the requested range (never written).
    pub lockbox_hours_dropped: u64,
    /// Distinct feeds across the built hours.
    pub feeds: u64,
    /// The input watermark (max publish_time over all lane files).
    pub watermark_max_publish_time: i64,
    /// Sealed hour outputs.
    pub outputs: Vec<HourOutput>,
}

impl Report {
    /// Process exit code: 0.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        0
    }

    /// JSON object for stdout.
    #[must_use]
    pub fn to_json(&self) -> String {
        crate::jsonout::Obj::new()
            .with("command", format!("augment combined {UNIT}"))
            .with("unit", UNIT)
            .with("unitVersion", UNIT_VERSION)
            .with("table", TABLE)
            .with("rows", self.rows)
            .with("hoursWritten", self.hours_written)
            .with("hoursSkipped", self.hours_skipped)
            .with("hoursEmpty", self.hours_empty)
            .with("lockboxHoursDropped", self.lockbox_hours_dropped)
            .with("feeds", self.feeds)
            .with("watermarkMaxPublishTime", self.watermark_max_publish_time)
            .with(
                "outputs",
                self.outputs
                    .iter()
                    .map(|file| {
                        json!({
                            "path": file.relative, "rows": file.rows,
                            "bytes": file.bytes, "sha256": file.sha256
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .to_json()
    }
}

/// SHA-256 over the unit's semantic config: the pin that must move on purpose.
fn config_hash() -> String {
    let body = json!({
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "columns": COLUMNS,
        "sort": ["feed_id"],
        "dedupe": "(feed_id, publish_time), later lane then later at_ms wins",
        "ordering": "publish_time (Pythnet clock), never at_ms",
        "pyLast": "price_f at max publish_time in the hour",
        "pyRet": "ln(last/first price_f in hour)",
        "pyConfRel": "mean of conf_f/price_f over the hour",
        "pyStaleS": "hour end minus max publish_time",
        "hourGuardSecs": HOUR_GUARD_SECS,
    });
    crate::fsutil::sha256_hex(body.to_string().as_bytes())
}

/// The pyth rows of `<out>/mapping/assets.csv` (U1): feed_id -> asset_id.
fn load_mapping(out: &Path) -> Result<Vec<(String, String)>, StoreError> {
    let path = out.join("mapping").join("assets.csv");
    let text = std::fs::read_to_string(&path).map_err(|error| {
        StoreError::Check(format!(
            "mapping/assets.csv: {error} (run 'augment combined mapping' first)"
        ))
    })?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    let expected: Vec<&str> = super::mapping::COLUMNS.to_vec();
    if header.split(',').collect::<Vec<_>>() != expected {
        return Err(StoreError::Check(
            "mapping/assets.csv header does not match the mapping schema".into(),
        ));
    }
    let mut pairs = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let mut venue = "";
        let mut asset_id = "";
        let mut market_id = "";
        let fields = split_csv_line(line);
        if fields.len() != expected.len() {
            return Err(StoreError::Check(
                "mapping/assets.csv row has the wrong field count".into(),
            ));
        }
        for (index, field) in fields.iter().enumerate() {
            match expected[index] {
                "venue" => venue = field,
                "asset_id" => asset_id = field,
                "venue_market_id" => market_id = field,
                _ => {}
            }
        }
        if venue != "pyth" || market_id.is_empty() {
            continue;
        }
        pairs.push((market_id.to_owned(), asset_id.to_owned()));
    }
    if pairs.is_empty() {
        return Err(StoreError::Check(
            "mapping/assets.csv has no pyth rows; run 'augment combined mapping' first".into(),
        ));
    }
    Ok(pairs)
}

/// Split one CSV line into fields (handles quoted commas).
fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => fields.push(std::mem::take(&mut field)),
            c => field.push(c),
        }
    }
    fields.push(field);
    fields
}

/// Run the pyth-hours unit.
///
/// # Errors
///
/// Any input is missing or malformed, the lockbox sha does not match, the
/// requested range extends past the watermark's closure, or an output fails
/// its read-back check. A failure never seals the failed hour.
pub fn run(request: &Request<'_>) -> Result<Report, StoreError> {
    let started = grid::now_rfc3339();
    if !(1..=8).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=8".into()));
    }
    let memory_limit = parse_memory_cap(&request.memory_limit)?;
    let provenance = load_provenance(request.provenance)?;
    let lockbox = super::lockbox::load(request.lockbox, request.lockbox_sha256)?;
    let lockbox_hours = lockbox.excluded_hours()?;
    lockbox.assert_v2_shape(&lockbox_hours)?;
    let mapping = load_mapping(request.out)?;
    if request.pyth_prices.is_empty() {
        return Err(StoreError::Check(
            "--pyth-prices needs at least one directory".into(),
        ));
    }

    create_dir_all_durable(request.out)?;
    let _guard = lock_output(request.out)?;
    let _temp = TempClean::new(request.out);
    let conn = open_reader(request.out, request.threads, &memory_limit)?;

    // Snapshot the lanes once: file lists are fixed before any query.
    let mut lanes: Vec<(PathBuf, Vec<FileRef>)> = Vec::new();
    for dir in request.pyth_prices {
        lanes.push((dir.clone(), io::walk_parquet(dir)?));
    }
    let file_count: usize = lanes.iter().map(|(_, files)| files.len()).sum();
    if file_count == 0 {
        return Err(StoreError::Check("no pyth price parquet files".into()));
    }
    let mut all_refs: Vec<FileRef> = Vec::new();
    for (_, files) in &lanes {
        all_refs.extend(files.iter().cloned());
    }
    let watermark = json!({
        "source": "augment venues-history + live pyth price lanes (walked parquet, snapshot at run start)",
        "files": file_count,
        "files_sha256": file_list_sha256(&all_refs),
        "lanes": lanes.iter().map(|(dir, files)| json!({
            "files": files.len(),
            "files_sha256": file_list_sha256(files),
            "root": dir.file_name().map_or_else(String::new, |name| name.to_string_lossy().to_string()),
        })).collect::<Vec<_>>(),
    });
    let inputs_sha = crate::fsutil::sha256_hex(watermark.to_string().as_bytes());

    let first_day = grid::parse_date(request.since)?;
    let last_day = grid::parse_date(request.until)?;
    let first_hour = grid::hour_of_secs(grid::day_start_secs(first_day));
    let requested_last = grid::hour_of_secs(grid::day_start_secs(last_day)) + 23;

    // Closure: the watermark is the max publish_time over all lane files,
    // read from Parquet statistics (no data pages).
    let watermark_max_publish_time = lane_watermark(&conn, &lanes)?;
    let last_closed_hour = grid::floor_div(
        watermark_max_publish_time - grid::HOUR_SECS - HOUR_GUARD_SECS,
        grid::HOUR_SECS,
    );
    if requested_last > last_closed_hour {
        return Err(StoreError::Check(format!(
            "requested range extends past the input watermark: hour {requested_last} ends at {} but the watermark {watermark_max_publish_time} closes only hour {last_closed_hour} (+2h guard)",
            grid::hour_start(requested_last) + grid::HOUR_SECS
        )));
    }

    let plan = plan_hours(first_hour, requested_last, &lockbox_hours)?;
    let lockbox_dropped = plan.lockbox_dropped;
    let (mut outputs, pending) = skip_sealed_hours(
        request.out,
        TABLE,
        UNIT,
        UNIT_VERSION,
        &inputs_sha,
        &plan.build_hours,
    );
    let hours_skipped = outputs.len() as u64;

    let mut hours_written = 0u64;
    let mut hours_empty = 0u64;
    if !pending.is_empty() {
        build_price_view(
            &conn,
            &lanes,
            grid::hour_start(first_hour),
            grid::hour_start(requested_last) + grid::HOUR_SECS,
        )?;
        build_aggregates(&conn)?;
        create_mapping_table(&conn, &mapping)?;

        for hour in &pending {
            let n_rows = scalar(
                &conn,
                &format!(
                    "SELECT count(*) AS n FROM pyth_coin_hour_agg WHERE hour = {}",
                    grid::hour_start(*hour)
                ),
                "n",
            )?;
            if n_rows == 0 {
                hours_empty += 1;
                continue;
            }
            let select = format!(
                "SELECT hour, feed_id, symbol, asset_id, py_n, py_last, py_ret,
                        py_conf_rel, py_stale_s
                 FROM pyth_coin_hour_agg
                 WHERE hour = {}
                 ORDER BY feed_id",
                grid::hour_start(*hour)
            );
            let relative = io::hour_partition(*hour, TABLE);
            let final_path = request.out.join(&relative);
            let (rows, bytes, sha256) = io::write_hour_parquet(&conn, &select, &final_path)?;
            io::write_hour_receipt(
                request.out,
                &io::HourReceipt {
                    unit: UNIT.to_owned(),
                    unit_version: UNIT_VERSION,
                    hour: *hour,
                    table: TABLE.to_owned(),
                    rows,
                    bytes,
                    sha256: sha256.clone(),
                    inputs_sha256: inputs_sha.clone(),
                    sealed_utc: grid::now_rfc3339(),
                },
            )?;
            outputs.push(HourOutput {
                hour: *hour,
                rows,
                bytes,
                sha256,
                relative,
            });
            hours_written += 1;
        }
    }

    let rows: u64 = outputs.iter().map(|file| file.rows).sum();
    let feeds = if outputs.is_empty() {
        0
    } else {
        query_rows(
            &conn,
            "SELECT count(DISTINCT feed_id) AS n FROM pyth_coin_hour_agg",
            &[],
        )?
        .first()
        .and_then(|row| row.int("n"))
        .unwrap_or(0) as u64
    };
    let mut watermark_facts = watermark.clone();
    watermark_facts["max_publish_time"] = json!(watermark_max_publish_time);
    watermark_facts["last_closed_hour"] = json!(last_closed_hour);
    finish_run(
        request.out,
        &io::RunSummary {
            unit: UNIT,
            unit_version: UNIT_VERSION,
            table: TABLE,
            outputs: &outputs,
            hours_skipped,
            lockbox_dropped,
            lockbox_excluded: lockbox_hours.len() as u64,
            lockbox_sha256: request.lockbox_sha256,
            watermark: &watermark_facts,
            inputs_sha256: &inputs_sha,
            config_sha256: &config_hash(),
            provenance: &provenance,
            started_utc: &started,
        },
    )?;

    Ok(Report {
        rows,
        hours_written,
        hours_skipped,
        hours_empty,
        lockbox_hours_dropped: lockbox_dropped,
        feeds,
        watermark_max_publish_time,
        outputs,
    })
}

/// One scalar from a single-row query.
fn scalar(conn: &duckdb::Connection, sql: &str, column: &str) -> Result<u64, StoreError> {
    query_rows(conn, sql, &[])?
        .first()
        .and_then(|row| row.int(column))
        .map(|value| value as u64)
        .ok_or_else(|| StoreError::Check(format!("query has no {column}: {sql}")))
}

/// Max publish_time over every lane file, from Parquet statistics.
fn lane_watermark(
    conn: &duckdb::Connection,
    lanes: &[(PathBuf, Vec<FileRef>)],
) -> Result<i64, StoreError> {
    let mut max: Option<i64> = None;
    for (dir, files) in lanes {
        for file in files {
            let table = parquet_list(&[dir.join(&file.relative).to_string_lossy().to_string()])?;
            let rows = query_rows(
                conn,
                &format!("SELECT max(publish_time) AS hi FROM {table}"),
                &[],
            )?;
            if let Some(hi) = rows.first().and_then(|row| row.int("hi")) {
                max = Some(max.map_or(hi, |current| current.max(hi)));
            }
        }
    }
    max.ok_or_else(|| StoreError::Check("lane files have no publish_time".into()))
}

/// Build `pyth_all` (all lane rows of the window with the lane index) and
/// `pyth_dedup` (identical (feed_id, publish_time) rows collapse to one; the
/// later lane, then the later at_ms, wins).
fn build_price_view(
    conn: &duckdb::Connection,
    lanes: &[(PathBuf, Vec<FileRef>)],
    window_lo: i64,
    window_hi: i64,
) -> Result<(), StoreError> {
    let mut parts = Vec::new();
    for (lane, (dir, files)) in lanes.iter().enumerate() {
        if files.is_empty() {
            continue;
        }
        let paths: Vec<String> = files
            .iter()
            .map(|file| dir.join(&file.relative).to_string_lossy().to_string())
            .collect();
        parts.push(format!(
            "SELECT feed_id, symbol, publish_time, price_f, conf_f, at_ms, {lane} AS lane
             FROM {} WHERE publish_time >= {window_lo} AND publish_time < {window_hi}",
            parquet_list(&paths)?,
        ));
    }
    if parts.is_empty() {
        return Err(StoreError::Check(
            "no pyth price rows in the requested window".into(),
        ));
    }
    conn.execute_batch(&format!(
        "CREATE TABLE pyth_all AS {}",
        parts.join(" UNION ALL BY NAME ")
    ))
    .map_err(|error| StoreError::Check(format!("pyth scan: {error}")))?;
    conn.execute_batch(
        "CREATE TABLE pyth_dedup AS
         SELECT feed_id, symbol, publish_time, price_f, conf_f
         FROM pyth_all
         QUALIFY row_number() OVER
             (PARTITION BY feed_id, publish_time ORDER BY lane DESC, at_ms DESC) = 1",
    )
    .map_err(|error| StoreError::Check(format!("pyth dedupe: {error}")))?;
    Ok(())
}

/// The per-(feed, hour) aggregates over the deduplicated prices.
fn build_aggregates(conn: &duckdb::Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE pyth_coin_hour_stage AS
         SELECT (publish_time // 3600) * 3600 AS hour, feed_id, symbol,
                count(*) AS py_n,
                arg_max(price_f, publish_time) AS py_last,
                ln(arg_max(price_f, publish_time)) - ln(arg_min(price_f, publish_time)) AS py_ret,
                avg(conf_f / nullif(price_f, 0)) AS py_conf_rel,
                ((publish_time // 3600) * 3600 + 3600 - max(publish_time)) AS py_stale_s
         FROM pyth_dedup
         WHERE price_f > 0
         GROUP BY (publish_time // 3600) * 3600, feed_id, symbol",
    )
    .map_err(|error| StoreError::Check(format!("pyth aggregates: {error}")))?;
    Ok(())
}

/// Join the mapping (U1 `mapping/assets.csv`, pyth rows) on feed_id.
fn create_mapping_table(
    conn: &duckdb::Connection,
    mapping: &[(String, String)],
) -> Result<(), StoreError> {
    conn.execute_batch("CREATE TABLE pyth_map(feed_id VARCHAR, asset_id VARCHAR)")
        .map_err(|error| StoreError::Check(format!("map table: {error}")))?;
    let mut stmt = conn
        .prepare("INSERT INTO pyth_map VALUES (?, ?)")
        .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    for (feed_id, asset_id) in mapping {
        stmt.execute(duckdb::params![feed_id, asset_id])
            .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    }
    conn.execute_batch(
        "CREATE TABLE pyth_coin_hour_agg AS
         SELECT s.hour, s.feed_id, s.symbol, m.asset_id,
                s.py_n, s.py_last, s.py_ret, s.py_conf_rel, s.py_stale_s
         FROM pyth_coin_hour_stage s
         LEFT JOIN pyth_map m ON m.feed_id = s.feed_id",
    )
    .map_err(|error| StoreError::Check(format!("map join: {error}")))?;
    Ok(())
}
