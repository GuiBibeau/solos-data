//! U2b: `solos-data augment combined phoenix-hours` builds
//! `<out>/phoenix_coin_hour/` from the Phoenix decoded fills (ADR-0023,
//! DESIGN.md 4.3): per (asset, hour) the frozen hltrain stage-1 trade
//! aggregates on decoded Parquet, hour-partitioned, lockbox hours dropped.
//!
//! Inputs are read as Parquet only; the decoder's `catalog.json` (a JSON file,
//! never the checkpoint database) provides the registered file list. The
//! decoder rewrites files live, so the list is snapshotted once per run and
//! files created after the snapshot are ignored. Dedupe replicates the
//! decoder reader exactly: fills rows join the deduplicated
//! `decoded_transactions` parents (latest revision per signature by batch id;
//! revisions share the slot, so the epoch range of the selected fills files is
//! exact), then `row_number() OVER (PARTITION BY event_id ORDER BY batch_id
//! DESC) = 1`.
//!
//! Price is `price_ticks * tick_size * 10^(base_lot_decimals -
//! quote_lot_decimals)` and quantity `base_lots / 10^base_lot_decimals`
//! (slice 0, verified against HL). The stored `taker_side` column is a
//! constant `Bid` (a documented parity quirk of the decoder port), so the
//! taker direction is derived from `maker_side`: an `ask` maker was hit by a
//! buying taker, so `buy_usd` sums fills whose `maker_side = 'ask'`.
//! Phoenix fills carry no liquidation flag: `ph_liq_usd` is NULL everywhere.

use crate::fsutil::create_dir_all_durable;
use crate::store::{StoreError, query_rows};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::grid;
use super::io::{
    self, HourOutput, TempClean, finish_run, load_provenance, lock_output, open_reader,
    parquet_list, parse_memory_cap, plan_hours, skip_sealed_hours,
};

/// The unit this module runs.
pub const UNIT: &str = "phoenix-hours";

/// The table this unit writes under the output root.
pub const TABLE: &str = "phoenix_coin_hour";

/// Bumped on an output-shape change.
pub const UNIT_VERSION: u32 = 1;

/// An output hour is sealed only when the input watermark covers the hour end
/// plus this guard (DESIGN.md 3).
pub const HOUR_GUARD_SECS: i64 = 2 * grid::HOUR_SECS;

/// Slots per decoder epoch (`epoch=<slot / 432000>` path partitions); child
/// revisions share the transaction's slot, so an epoch range is exact.
pub const EPOCH_SLOTS: i64 = 432_000;

/// Output columns, in file order.
pub const COLUMNS: &[(&str, &str)] = &[
    ("hour", "BIGINT"),
    ("asset_symbol", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("mapped", "BOOLEAN"),
    ("n_trades", "BIGINT"),
    ("vol_usd", "DOUBLE"),
    ("buy_usd", "DOUBLE"),
    ("first_px", "DOUBLE"),
    ("last_px", "DOUBLE"),
    ("first_ts", "BIGINT"),
    ("last_ts", "BIGINT"),
    ("rv2", "DOUBLE"),
    ("n_minutes", "BIGINT"),
    ("ph_liq_usd", "DOUBLE"),
];

/// One offline phoenix-hours run.
pub struct Request<'a> {
    /// Combined store root; `phoenix_coin_hour/` is written under it.
    pub out: &'a Path,
    /// Lockbox v2 JSON; its hours are dropped from the table.
    pub lockbox: &'a Path,
    /// Required sha256 of the lockbox bytes.
    pub lockbox_sha256: &'a str,
    /// Operator provenance object, copied verbatim into the receipts.
    pub provenance: &'a Path,
    /// Phoenix decoded root (`catalog.json` + `tables/fills` + `tables/decoded_transactions`).
    pub phoenix_decoded: &'a Path,
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
    /// Fills rows the deduplicated view kept.
    pub fills_kept: u64,
    /// Fills rows dropped as superseded or duplicate revisions.
    pub fills_dropped: u64,
    /// Distinct assets across the built hours.
    pub assets: u64,
    /// The input watermark facts recorded in the receipt.
    pub watermark_max_block_time: i64,
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
            .with("lockboxHoursDropped", self.lockbox_hours_dropped)
            .with("fillsKept", self.fills_kept)
            .with("fillsDropped", self.fills_dropped)
            .with("assets", self.assets)
            .with("watermarkMaxBlockTime", self.watermark_max_block_time)
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

/// One registered file of the decoder catalog.
#[derive(Clone, Debug, Deserialize)]
struct CatalogFile {
    path: String,
    #[allow(dead_code)]
    #[serde(rename = "table_name")]
    table_name: String,
    sha256: String,
    batch_id: String,
    created_at: String,
}

/// The decoder catalog: registered files only, snapshotted once per run.
#[derive(Deserialize)]
struct Catalog {
    at: String,
    files: Vec<CatalogFile>,
}

/// SHA-256 over the sorted `path sha256 batch_id` lines of a catalog file list.
fn catalog_list_sha256(files: &[CatalogFile]) -> String {
    let mut sorted: Vec<&CatalogFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut text = String::new();
    for file in sorted {
        text.push_str(&file.path);
        text.push(' ');
        text.push_str(&file.sha256);
        text.push(' ');
        text.push_str(&file.batch_id);
        text.push('\n');
    }
    crate::fsutil::sha256_hex(text.as_bytes())
}

/// The epoch a registered path belongs to, when its directory says so.
fn path_epoch(path: &str) -> Option<i64> {
    let start = path.find("epoch=")? + "epoch=".len();
    let digits: String = path[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// SHA-256 over the unit's semantic config: the pin that must move on purpose.
fn config_hash() -> String {
    let body = json!({
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "columns": COLUMNS,
        "sort": ["asset_id nulls last", "asset_symbol"],
        "price": "price_ticks * tick_size * 10^(base_lot_decimals - quote_lot_decimals)",
        "quantity": "base_lots / 10^base_lot_decimals",
        "buy": "sum(px*qty) where maker_side = 'ask' (stored taker_side is a constant Bid parity quirk)",
        "rv": "sum of squared 1-minute last-price log diffs inside the hour",
        "hourGuardSecs": HOUR_GUARD_SECS,
        "dedupe": "decoder reader: latest parent revision per signature, latest event_id per batch_id",
        "liq": "null (Phoenix fills carry no liquidation flag)",
    });
    crate::fsutil::sha256_hex(body.to_string().as_bytes())
}

/// The snapshot of registered files this run reads.
struct Inputs {
    watermark: Value,
    inputs_sha: String,
    fills: Vec<CatalogFile>,
    txs: Vec<CatalogFile>,
}

/// Snapshot `catalog.json`, keep files created no later than the snapshot, and
/// split the registered list by table.
fn snapshot_inputs(decoded_root: &Path) -> Result<Inputs, StoreError> {
    let path = decoded_root.join("catalog.json");
    let text = std::fs::read_to_string(&path)
        .map_err(|error| StoreError::Check(format!("phoenix catalog: {error}")))?;
    let catalog: Catalog = serde_json::from_str(&text)
        .map_err(|error| StoreError::Check(format!("phoenix catalog: bad JSON: {error}")))?;
    let run_start = io::parse_catalog_stamp(&catalog.at)?;
    let mut fills = Vec::new();
    let mut txs = Vec::new();
    for file in catalog.files {
        let created = io::parse_catalog_stamp(&file.created_at)?;
        if created > run_start {
            continue;
        }
        match file.table_name.as_str() {
            "fills" => fills.push(file),
            "decoded_transactions" => txs.push(file),
            _ => {}
        }
    }
    if fills.is_empty() {
        return Err(StoreError::Check(
            "phoenix catalog registers no fills files".into(),
        ));
    }
    let watermark = json!({
        "source": "phoenix_decoded catalog.json (registered files only, created_at <= snapshot)",
        "catalog_at": catalog.at,
        "fills_files": fills.len(),
        "fills_files_sha256": catalog_list_sha256(&fills),
        "decoded_transactions_files": txs.len(),
        "decoded_transactions_files_sha256": catalog_list_sha256(&txs),
    });
    // Canonical JSON text of the watermark: serde_json object order here is
    // insertion order of json!, so the same facts hash the same.
    let inputs_sha = crate::fsutil::sha256_hex(watermark.to_string().as_bytes());
    Ok(Inputs {
        watermark,
        inputs_sha,
        fills,
        txs,
    })
}

/// Per-file block_time range, answered from Parquet statistics.
fn block_time_range(conn: &duckdb::Connection, path: &Path) -> Result<(i64, i64), StoreError> {
    let table = parquet_list(&[path.to_string_lossy().to_string()])?;
    let rows = query_rows(
        conn,
        &format!("SELECT min(block_time) AS lo, max(block_time) AS hi FROM {table}"),
        &[],
    )?;
    let row = rows
        .first()
        .ok_or_else(|| StoreError::Check("empty block_time range".into()))?;
    let lo = row
        .int("lo")
        .ok_or_else(|| StoreError::Check("null min(block_time)".into()))?;
    let hi = row
        .int("hi")
        .ok_or_else(|| StoreError::Check("null max(block_time)".into()))?;
    Ok((lo, hi))
}

/// The Phoenix rows of `<out>/mapping/assets.csv` (U1), for the asset join.
struct PhoenixMap {
    /// venue_symbol -> (asset_id, mapped).
    by_symbol: BTreeMap<String, (String, bool)>,
}

/// Load `<out>/mapping/assets.csv` and keep its Phoenix rows.
fn load_mapping(out: &Path) -> Result<PhoenixMap, StoreError> {
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
    let mut by_symbol = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let fields = split_csv_line(line);
        if fields.len() != expected.len() {
            return Err(StoreError::Check(
                "mapping/assets.csv row has the wrong field count".into(),
            ));
        }
        let mut venue = "";
        let mut symbol = "";
        let mut asset_id = "";
        let mut mapped = "";
        for (index, field) in fields.iter().enumerate() {
            match expected[index] {
                "venue" => venue = field,
                "venue_symbol" => symbol = field,
                "asset_id" => asset_id = field,
                "mapped" => mapped = field,
                _ => {}
            }
        }
        if venue != "phoenix" || symbol.is_empty() {
            continue;
        }
        by_symbol.insert(symbol.to_owned(), (asset_id.to_owned(), mapped == "true"));
    }
    if by_symbol.is_empty() {
        return Err(StoreError::Check(
            "mapping/assets.csv has no phoenix rows; run 'augment combined mapping' first".into(),
        ));
    }
    Ok(PhoenixMap { by_symbol })
}

/// Split one CSV line into fields (handles quoted commas; the mapping writer
/// quotes only when needed).
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

/// Run the phoenix-hours unit.
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

    create_dir_all_durable(request.out)?;
    let _guard = lock_output(request.out)?;
    let _temp = TempClean::new(request.out);
    let conn = open_reader(request.out, request.threads, &memory_limit)?;

    let inputs = snapshot_inputs(request.phoenix_decoded)?;

    let first_day = grid::parse_date(request.since)?;
    let last_day = grid::parse_date(request.until)?;
    let first_hour = grid::hour_of_secs(grid::day_start_secs(first_day));
    let requested_last = grid::hour_of_secs(grid::day_start_secs(last_day)) + 23;
    if requested_last < first_hour {
        return Err(StoreError::Check("--until is before --since".into()));
    }

    // Keep the registered fills files whose block_time range overlaps the
    // requested window (statistics read, no data pages); the maximum over the
    // kept files is the run's input watermark.
    let window_lo = grid::hour_start(first_hour);
    let window_hi = grid::hour_start(requested_last) + grid::HOUR_SECS;
    let mut selected: Vec<(&CatalogFile, i64, i64)> = Vec::new();
    let mut watermark_max: Option<i64> = None;
    for file in &inputs.fills {
        let path = request.phoenix_decoded.join(&file.path);
        if !path.is_file() {
            return Err(StoreError::Check(format!(
                "registered fills file is missing on disk: {}",
                file.path
            )));
        }
        let (lo, hi) = block_time_range(&conn, &path)?;
        if hi < window_lo || lo >= window_hi {
            continue;
        }
        watermark_max = Some(watermark_max.map_or(hi, |current| current.max(hi)));
        selected.push((file, lo, hi));
    }
    let watermark_max_block_time = watermark_max
        .ok_or_else(|| StoreError::Check("no fills files overlap the requested range".into()))?;
    let last_closed_hour = grid::floor_div(
        watermark_max_block_time - grid::HOUR_SECS - HOUR_GUARD_SECS,
        grid::HOUR_SECS,
    );
    if requested_last > last_closed_hour {
        return Err(StoreError::Check(format!(
            "requested range extends past the input watermark: hour {requested_last} ends at {} but the watermark {watermark_max_block_time} closes only hour {last_closed_hour} (+2h guard)",
            grid::hour_start(requested_last) + grid::HOUR_SECS
        )));
    }

    // Lockbox hours inside the requested range are dropped and counted; the
    // rest is the build plan. Closure of the range was just enforced.
    let plan = plan_hours(first_hour, requested_last, &lockbox_hours)?;
    let lockbox_dropped = plan.lockbox_dropped;

    // Idempotent skip: an existing receipt over the same inputs with a file
    // that still hashes the same means the hour is already sealed.
    let (mut outputs, pending) = skip_sealed_hours(
        request.out,
        TABLE,
        UNIT,
        UNIT_VERSION,
        &inputs.inputs_sha,
        &plan.build_hours,
    );
    let hours_skipped = outputs.len() as u64;

    let mut fills_kept = 0u64;
    let mut fills_dropped = 0u64;
    let mut hours_written = 0u64;
    let mut hours_empty = 0u64;
    if !pending.is_empty() {
        // The deduplicated fills view over the selected files (projected
        // columns only; `event_json` is big and not needed here).
        build_fills_view(
            &conn,
            request.phoenix_decoded,
            &inputs,
            &selected,
            window_lo,
            window_hi,
        )?;
        fills_kept = scalar(&conn, "SELECT count(*) AS n FROM fills_dedup", "n")?;
        let raw = scalar(&conn, "SELECT count(*) AS n FROM fills_raw_selected", "n")?;
        fills_dropped = raw.saturating_sub(fills_kept);

        // The frozen stage-1 aggregates over the whole range in one pass,
        // then the mapping join.
        build_aggregates(&conn)?;
        create_mapping_table(&conn, &mapping)?;

        for hour in &pending {
            // Sparse hours are the assembly's job (U3): an hour with no rows
            // gets no file and no receipt.
            let n_rows = scalar(
                &conn,
                &format!(
                    "SELECT count(*) AS n FROM phoenix_coin_hour_agg WHERE hour = {}",
                    grid::hour_start(*hour)
                ),
                "n",
            )?;
            if n_rows == 0 {
                hours_empty += 1;
                continue;
            }
            let select = format!(
                "SELECT hour, asset_symbol, asset_id, mapped, n_trades, vol_usd, buy_usd,
                        first_px, last_px, first_ts, last_ts, rv2, n_minutes, ph_liq_usd
                 FROM phoenix_coin_hour_agg
                 WHERE hour = {}
                 ORDER BY asset_id NULLS LAST, asset_symbol",
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
                    inputs_sha256: inputs.inputs_sha.clone(),
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

    // Per-run receipt and catalog registration, after every output is durable.
    let rows: u64 = outputs.iter().map(|file| file.rows).sum();
    let assets = if outputs.is_empty() {
        0
    } else {
        scalar(
            &conn,
            "SELECT count(DISTINCT asset_symbol) AS n FROM phoenix_coin_hour_agg",
            "n",
        )?
    };
    let mut watermark = inputs.watermark.clone();
    watermark["max_block_time"] = json!(watermark_max_block_time);
    watermark["last_closed_hour"] = json!(last_closed_hour);
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
            watermark: &watermark,
            inputs_sha256: &inputs.inputs_sha,
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
        fills_kept,
        fills_dropped,
        assets,
        watermark_max_block_time,
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

/// Create `fills_raw_selected` (the window's rows with registration batch ids),
/// `tx_parents` (latest revision per signature), and `fills_dedup` (the decoder
/// reader's deduplicated fills view).
fn build_fills_view(
    conn: &duckdb::Connection,
    decoded_root: &Path,
    inputs: &Inputs,
    selected: &[(&CatalogFile, i64, i64)],
    window_lo: i64,
    window_hi: i64,
) -> Result<(), StoreError> {
    let fill_paths: Vec<String> = selected
        .iter()
        .map(|(file, _, _)| decoded_root.join(&file.path).to_string_lossy().to_string())
        .collect();
    let fill_entries: Vec<(PathBuf, i64)> = selected
        .iter()
        .map(|(file, _, _)| {
            (
                decoded_root.join(&file.path),
                file.batch_id.parse::<i64>().unwrap_or(0),
            )
        })
        .collect();
    create_registrations(conn, "fill_registrations", &fill_entries)?;

    // Parent revisions of the selected rows share their slots, so the epoch
    // range of the selected fills files is exact for the parent check.
    let epochs: Vec<i64> = selected
        .iter()
        .filter_map(|(file, _, _)| path_epoch(&file.path))
        .collect();
    let epoch_min = epochs.iter().min().copied().unwrap_or(0);
    let epoch_max = epochs.iter().max().copied().unwrap_or(0);
    let slot_lo = epoch_min * EPOCH_SLOTS;
    let slot_hi = (epoch_max + 1) * EPOCH_SLOTS - 1;
    let tx_files: Vec<CatalogFile> = inputs
        .txs
        .iter()
        .filter(|file| {
            path_epoch(&file.path).is_some_and(|epoch| (epoch_min..=epoch_max).contains(&epoch))
        })
        .cloned()
        .collect();
    if tx_files.is_empty() {
        return Err(StoreError::Check(
            "no registered decoded_transactions files cover the selected fills epochs".into(),
        ));
    }
    let tx_paths: Vec<String> = tx_files
        .iter()
        .map(|file| decoded_root.join(&file.path).to_string_lossy().to_string())
        .collect();
    let tx_entries: Vec<(PathBuf, i64)> = tx_files
        .iter()
        .map(|file| {
            (
                decoded_root.join(&file.path),
                file.batch_id.parse::<i64>().unwrap_or(0),
            )
        })
        .collect();
    create_registrations(conn, "tx_registrations", &tx_entries)?;

    conn.execute_batch(&format!(
        "CREATE TABLE fills_raw_selected AS
         SELECT f.event_id, f.signature, f.source_hash, f.slot, f.tx_index, f.event_ordinal,
                f.block_time, f.asset_symbol, f.tick_size, f.base_lot_decimals,
                f.quote_lot_decimals, f.price_ticks, f.base_lots, f.maker_side, r.batch_id
         FROM {} f
         JOIN fill_registrations r ON r.path = f.filename
         WHERE f.block_time >= {window_lo} AND f.block_time < {window_hi}",
        parquet_list(&fill_paths)?,
    ))
    .map_err(|error| StoreError::Check(format!("fills scan: {error}")))?;

    conn.execute_batch(&format!(
        "CREATE TABLE tx_parents AS
         SELECT signature, source_hash FROM (
             SELECT t.signature, t.source_hash, r.batch_id
             FROM {} t
             JOIN tx_registrations r ON r.path = t.filename
             WHERE t.slot >= {slot_lo} AND t.slot <= {slot_hi}
         )
         QUALIFY row_number() OVER (PARTITION BY signature ORDER BY batch_id DESC) = 1",
        parquet_list(&tx_paths)?,
    ))
    .map_err(|error| StoreError::Check(format!("parent scan: {error}")))?;

    conn.execute_batch(
        "CREATE TABLE fills_dedup AS
         SELECT f.* EXCLUDE (batch_id)
         FROM fills_raw_selected f
         SEMI JOIN tx_parents p USING (signature, source_hash)
         QUALIFY row_number() OVER (PARTITION BY f.event_id ORDER BY f.batch_id DESC) = 1",
    )
    .map_err(|error| StoreError::Check(format!("fills dedupe: {error}")))?;
    Ok(())
}

/// A `(path, batch_id)` registration table from a fixed list.
fn create_registrations(
    conn: &duckdb::Connection,
    name: &str,
    entries: &[(PathBuf, i64)],
) -> Result<(), StoreError> {
    let list: Vec<Value> = entries
        .iter()
        .map(|(path, batch_id)| {
            json!({
                "path": path.to_string_lossy(),
                "batch_id": batch_id,
            })
        })
        .collect();
    conn.execute(
        &format!(
            "CREATE TABLE {name} AS SELECT value->>'path' AS path,
             (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?)"
        ),
        [Value::Array(list).to_string()],
    )
    .map_err(|error| StoreError::Check(format!("{name}: {error}")))?;
    Ok(())
}

/// The frozen hltrain stage-1 aggregates over `fills_dedup`, one pass over the
/// whole range: per (asset_symbol, hour) trade count, taker notional, taker-buy
/// notional, first/last price, realised variance from 1-minute last prices, and
/// the minute count.
fn build_aggregates(conn: &duckdb::Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE phoenix_agg_stage AS
         WITH t AS (
             SELECT asset_symbol,
                    (block_time // 3600) * 3600 AS hour,
                    block_time // 60 AS minute,
                    block_time::HUGEINT * 1000000000000
                        + tx_index::HUGEINT * 100000
                        + event_ordinal::HUGEINT AS k,
                    block_time,
                    price_ticks::DOUBLE * tick_size::DOUBLE
                        * power(10::DOUBLE, (base_lot_decimals - quote_lot_decimals)::DOUBLE) AS px,
                    base_lots::DOUBLE / power(10::DOUBLE, base_lot_decimals::DOUBLE) AS qty,
                    maker_side
             FROM fills_dedup
             WHERE price_ticks > 0 AND base_lots > 0 AND asset_symbol IS NOT NULL
         ),
         m AS (SELECT asset_symbol, hour, minute, arg_max(px, k) AS lpx FROM t GROUP BY ALL),
         r AS (SELECT asset_symbol, hour,
                      ln(lpx) - lag(ln(lpx)) OVER (PARTITION BY asset_symbol, hour ORDER BY minute) AS dr
               FROM m),
         rv AS (SELECT asset_symbol, hour, coalesce(sum(dr * dr), 0) AS rv2, count(*) AS n_minutes
                FROM r GROUP BY ALL),
         h AS (SELECT asset_symbol, hour, count(*) AS n_trades,
                      sum(px * qty) AS vol_usd,
                      coalesce(sum(px * qty) FILTER (WHERE maker_side = 'ask'), 0) AS buy_usd,
                      arg_min(px, k) AS first_px, arg_max(px, k) AS last_px,
                      min(block_time) AS first_ts, max(block_time) AS last_ts
               FROM t GROUP BY asset_symbol, hour)
         SELECT h.*, rv.rv2, rv.n_minutes,
                CAST(NULL AS DOUBLE) AS ph_liq_usd
         FROM h LEFT JOIN rv USING (asset_symbol, hour)",
    )
    .map_err(|error| StoreError::Check(format!("stage-1 aggregates: {error}")))?;
    Ok(())
}

/// Join the mapping (U1 `mapping/assets.csv`, Phoenix rows) onto the aggregate.
fn create_mapping_table(conn: &duckdb::Connection, mapping: &PhoenixMap) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE phoenix_map(asset_symbol VARCHAR, asset_id VARCHAR, mapped BOOLEAN)",
    )
    .map_err(|error| StoreError::Check(format!("map table: {error}")))?;
    let mut stmt = conn
        .prepare("INSERT INTO phoenix_map VALUES (?, ?, ?)")
        .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    for (symbol, (asset_id, mapped)) in &mapping.by_symbol {
        stmt.execute(duckdb::params![symbol, asset_id, mapped])
            .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    }
    conn.execute_batch(
        "CREATE TABLE phoenix_coin_hour_agg AS
         SELECT a.hour, a.asset_symbol, m.asset_id, m.mapped,
                a.n_trades, a.vol_usd, a.buy_usd, a.first_px, a.last_px,
                a.first_ts, a.last_ts, a.rv2, a.n_minutes, a.ph_liq_usd
         FROM phoenix_agg_stage a
         LEFT JOIN phoenix_map m USING (asset_symbol)",
    )
    .map_err(|error| StoreError::Check(format!("map join: {error}")))?;
    Ok(())
}
