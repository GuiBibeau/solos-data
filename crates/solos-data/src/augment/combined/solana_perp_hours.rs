//! U2c: `solos-data augment combined solana-perp-hours` builds
//! `<out>/solana_perp_coin_hour/` from the Jupiter Perps and Flash Trade event
//! lanes (ADR-0023, DESIGN.md 4.3): per (asset, venue, hour) position-event
//! counts, traded size, buy share, funding and liquidation dollars, and the
//! in-hour log return of the event price.
//!
//! Inputs are plain Parquet lanes, each argument one lane read in full: the
//! venues-history dumps first, then the live capture (2026-10-18 onward), so a
//! later lane wins the `(signature, ordinal)` dedupe across lanes.
//!
//! Scaling, learned from the data and verified against Pyth (prototype: median
//! |ln ratio| 0.0009 for SOL over 94 hours): every Jupiter USD and price
//! integer in `fields` is 1e6-scaled. Flash carries its own exponent field
//! (`entry_price_exponent`, typically -8) next to each price. Jupiter's
//! `positionSide` is 1 = long, 2 = short; Flash events carry no side and no
//! funding, so `buy_share` and `funding_fee_usd` are NULL for flash rows.
//! Flash perp events end around 2026-06-28; later hours simply have no rows.
//!
//! The asset join reads `<out>/mapping/assets.csv` (U1): Jupiter rows by
//! `venue = jup` and `venue_market_id = custody`, Flash rows by
//! `venue = flash`. Flash markets are not in the venues config, so flash rows
//! carry a NULL `asset_id` until the mapping covers them.

use crate::fsutil::create_dir_all_durable;
use crate::store::{StoreError, query_rows};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::grid;
use super::io::{
    self, FileRef, HourOutput, TempClean, file_list_sha256, finish_run, load_provenance,
    lock_output, open_reader, parquet_list, parse_memory_cap, plan_hours, skip_sealed_hours,
};

/// The unit this module runs.
pub const UNIT: &str = "solana-perp-hours";

/// The table this unit writes under the output root.
pub const TABLE: &str = "solana_perp_coin_hour";

/// Bumped on an output-shape change.
pub const UNIT_VERSION: u32 = 1;

/// An output hour is sealed only when the input watermark covers the hour end
/// plus this guard (DESIGN.md 3).
pub const HOUR_GUARD_SECS: i64 = 2 * grid::HOUR_SECS;

/// Jupiter USD and price integers in `fields` are 1e6-scaled.
pub const JUP_USD_SCALE: f64 = 1_000_000.0;

/// Jupiter `positionSide` value for a long position.
pub const SIDE_LONG: i64 = 1;

/// The position events that count, per venue. Jupiter has instant and
/// request-follow-up variants of every action; Flash's families are the
/// USDv1 logs and the older V2/V3/V4 logs.
pub const JUP_EVENTS: &[&str] = &[
    "InstantIncreasePositionEvent",
    "IncreasePositionEvent",
    "InstantDecreasePositionEvent",
    "DecreasePositionEvent",
    "LiquidateFullPositionEvent",
    "LiquidateBorrowPositionEvent",
];

/// Output columns, in file order.
pub const COLUMNS: &[(&str, &str)] = &[
    ("hour", "BIGINT"),
    ("venue", "VARCHAR"),
    ("venue_key", "VARCHAR"),
    ("asset_id", "VARCHAR"),
    ("mapped", "BOOLEAN"),
    ("n_inc", "BIGINT"),
    ("n_dec", "BIGINT"),
    ("n_liq", "BIGINT"),
    ("size_usd", "DOUBLE"),
    ("buy_share", "DOUBLE"),
    ("funding_fee_usd", "DOUBLE"),
    ("liq_usd", "DOUBLE"),
    ("ret", "DOUBLE"),
    ("present", "BOOLEAN"),
];

/// One offline solana-perp-hours run.
pub struct Request<'a> {
    /// Combined store root; `solana_perp_coin_hour/` is written under it.
    pub out: &'a Path,
    /// Lockbox v2 JSON; its hours are dropped from the table.
    pub lockbox: &'a Path,
    /// Required sha256 of the lockbox bytes.
    pub lockbox_sha256: &'a str,
    /// Operator provenance object, copied verbatim into the receipts.
    pub provenance: &'a Path,
    /// Jupiter event directories, in lane order (history first, live last).
    pub jupiter_events: &'a [PathBuf],
    /// Flash event directories, in lane order (history first, live last).
    pub flash_events: &'a [PathBuf],
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
    /// Jupiter position events after the lane dedupe.
    pub jupiter_events: u64,
    /// Flash position events after the lane dedupe.
    pub flash_events: u64,
    /// Distinct (venue, key) pairs across the built hours.
    pub pairs: u64,
    /// The input watermark (max block_time over all lane files).
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
            .with("hoursEmpty", self.hours_empty)
            .with("lockboxHoursDropped", self.lockbox_hours_dropped)
            .with("jupiterEvents", self.jupiter_events)
            .with("flashEvents", self.flash_events)
            .with("pairs", self.pairs)
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

/// One walked lane: the directory (the lane's identity) and its files.
type Lane = (PathBuf, Vec<FileRef>);

/// Walk every lane directory; empty lanes are allowed (flash live has almost
/// no rows) but at least one lane per venue must have files.
fn walk_lanes(dirs: &[PathBuf]) -> Result<Vec<Lane>, StoreError> {
    let mut lanes = Vec::new();
    for dir in dirs {
        lanes.push((dir.clone(), io::walk_parquet(dir)?));
    }
    Ok(lanes)
}

/// SHA-256 over the unit's semantic config: the pin that must move on purpose.
fn config_hash() -> String {
    let body = json!({
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "columns": COLUMNS,
        "sort": ["venue", "venue_key"],
        "jupiterEvents": JUP_EVENTS,
        "flashEventMatch": "name matches Open/Increase/Decrease/Close/CloseAndSwap/Liquidate/SwapAndOpen/Execute and fields.market is not null",
        "jupUsdScale": JUP_USD_SCALE,
        "jupSideLong": SIDE_LONG,
        "buyShare": "long increase size over all increase size (jupiter only; flash has no side)",
        "ret": "ln(last/first event price in hour), price>0 rows only",
        "flashPrice": "coalesce(exit, entry, oracle_account_price) * 10^exponent",
        "dedupe": "(signature, ordinal), later lane wins",
        "hourGuardSecs": HOUR_GUARD_SECS,
    });
    crate::fsutil::sha256_hex(body.to_string().as_bytes())
}

/// The perp mapping rows of `<out>/mapping/assets.csv` (U1).
struct PerpMap {
    /// (venue, venue_key) -> (asset_id, mapped).
    by_key: BTreeMap<(String, String), (String, bool)>,
}

/// Load `<out>/mapping/assets.csv` and keep its jupiter and flash rows.
fn load_mapping(out: &Path) -> Result<PerpMap, StoreError> {
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
    let mut by_key = BTreeMap::new();
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
        let mut asset_id = "";
        let mut market_id = "";
        let mut mapped = "";
        for (index, field) in fields.iter().enumerate() {
            match expected[index] {
                "venue" => venue = field,
                "asset_id" => asset_id = field,
                "venue_market_id" => market_id = field,
                "mapped" => mapped = field,
                _ => {}
            }
        }
        if (venue != "jup" && venue != "flash") || market_id.is_empty() {
            continue;
        }
        by_key.insert(
            (venue.to_owned(), market_id.to_owned()),
            (asset_id.to_owned(), mapped == "true"),
        );
    }
    if by_key.is_empty() {
        return Err(StoreError::Check(
            "mapping/assets.csv has no jupiter or flash rows; run 'augment combined mapping' first"
                .into(),
        ));
    }
    Ok(PerpMap { by_key })
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

/// Run the solana-perp-hours unit.
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
    if request.jupiter_events.is_empty() || request.flash_events.is_empty() {
        return Err(StoreError::Check(
            "both --jupiter-events and --flash-events need at least one directory".into(),
        ));
    }

    create_dir_all_durable(request.out)?;
    let _guard = lock_output(request.out)?;
    let _temp = TempClean::new(request.out);
    let conn = open_reader(request.out, request.threads, &memory_limit)?;

    // Snapshot the lanes once: file lists are fixed before any query, so the
    // receipt's watermark describes exactly what the queries read.
    let jupiter_lanes = walk_lanes(request.jupiter_events)?;
    let flash_lanes = walk_lanes(request.flash_events)?;
    let jupiter_files: Vec<(&Path, &FileRef)> = jupiter_lanes
        .iter()
        .flat_map(|(dir, files)| files.iter().map(move |file| (dir.as_path(), file)))
        .collect();
    let flash_files: Vec<(&Path, &FileRef)> = flash_lanes
        .iter()
        .flat_map(|(dir, files)| files.iter().map(move |file| (dir.as_path(), file)))
        .collect();
    if jupiter_files.is_empty() {
        return Err(StoreError::Check("no jupiter event parquet files".into()));
    }
    if flash_files.is_empty() {
        return Err(StoreError::Check("no flash event parquet files".into()));
    }
    let files_sha = |files: &[(&Path, &FileRef)]| -> String {
        let refs: Vec<FileRef> = files.iter().map(|(_, file)| (*file).clone()).collect();
        file_list_sha256(&refs)
    };
    let watermark = json!({
        "source": "augment venues-history + live event lanes (walked parquet, snapshot at run start)",
        "jupiter_files": jupiter_files.len(),
        "jupiter_files_sha256": files_sha(&jupiter_files),
        "flash_files": flash_files.len(),
        "flash_files_sha256": files_sha(&flash_files),
    });
    let inputs_sha = crate::fsutil::sha256_hex(watermark.to_string().as_bytes());

    let first_day = grid::parse_date(request.since)?;
    let last_day = grid::parse_date(request.until)?;
    let first_hour = grid::hour_of_secs(grid::day_start_secs(first_day));
    let requested_last = grid::hour_of_secs(grid::day_start_secs(last_day)) + 23;

    // Closure: the watermark is the max block_time over all lane files, read
    // from Parquet statistics (no data pages).
    let mut all_lanes = jupiter_lanes.clone();
    all_lanes.extend(flash_lanes.iter().cloned());
    let watermark_max_block_time = lane_watermark(&conn, &all_lanes)?;
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
    let mut jupiter_events = 0u64;
    let mut flash_events = 0u64;
    if !pending.is_empty() {
        build_event_views(
            &conn,
            &jupiter_lanes,
            &flash_lanes,
            grid::hour_start(first_hour),
            grid::hour_start(requested_last) + grid::HOUR_SECS,
        )?;
        jupiter_events = scalar(&conn, "SELECT count(*) AS n FROM jup_pos", "n")?;
        flash_events = scalar(&conn, "SELECT count(*) AS n FROM flash_pos", "n")?;
        build_aggregates(&conn)?;
        create_mapping_table(&conn, &mapping)?;

        for hour in &pending {
            let n_rows = scalar(
                &conn,
                &format!(
                    "SELECT count(*) AS n FROM solana_perp_hour_agg WHERE hour = {}",
                    grid::hour_start(*hour)
                ),
                "n",
            )?;
            if n_rows == 0 {
                hours_empty += 1;
                continue;
            }
            let select = format!(
                "SELECT hour, venue, venue_key, asset_id, mapped, n_inc, n_dec, n_liq,
                        size_usd, buy_share, funding_fee_usd, liq_usd, ret, present
                 FROM solana_perp_hour_agg
                 WHERE hour = {}
                 ORDER BY venue, venue_key",
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
    let pairs = if outputs.is_empty() {
        0
    } else {
        scalar(
            &conn,
            "SELECT count(*) AS n FROM (SELECT venue, venue_key FROM solana_perp_hour_agg
             GROUP BY venue, venue_key) pairs",
            "n",
        )?
    };
    let mut watermark_facts = watermark.clone();
    watermark_facts["max_block_time"] = json!(watermark_max_block_time);
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
        jupiter_events,
        flash_events,
        pairs,
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

/// Max block_time over every lane file, from Parquet statistics.
fn lane_watermark(conn: &duckdb::Connection, lanes: &[Lane]) -> Result<i64, StoreError> {
    let mut max: Option<i64> = None;
    for (dir, files) in lanes {
        for file in files {
            let table = parquet_list(&[dir.join(&file.relative).to_string_lossy().to_string()])?;
            let rows = query_rows(
                conn,
                &format!("SELECT max(block_time) AS hi FROM {table}"),
                &[],
            )?;
            if let Some(hi) = rows.first().and_then(|row| row.int("hi")) {
                max = Some(max.map_or(hi, |current| current.max(hi)));
            }
        }
    }
    max.ok_or_else(|| StoreError::Check("lane files have no block_time".into()))
}

/// Build `events_all` (all lane rows of the window with the lane index),
/// `events_dedup` ((signature, ordinal), later lane wins), then the projected
/// position-event tables `jup_pos` and `flash_pos`.
fn build_event_views(
    conn: &duckdb::Connection,
    jupiter: &[Lane],
    flash: &[Lane],
    window_lo: i64,
    window_hi: i64,
) -> Result<(), StoreError> {
    // One union arm per lane (never per file: thousands of union arms exceed
    // DuckDB's expression depth). The lane index is the position in the
    // argument list, so a later (live) lane outranks an earlier one.
    let union_of = |lanes: &[Lane]| -> Result<String, StoreError> {
        let mut parts = Vec::new();
        for (lane, (dir, files)) in lanes.iter().enumerate() {
            if files.is_empty() {
                continue;
            }
            let list: Vec<String> = files
                .iter()
                .map(|file| dir.join(&file.relative).to_string_lossy().to_string())
                .collect();
            let table = parquet_list(&list)?;
            parts.push(format!(
                "SELECT *, {lane} AS lane FROM {table}
                 WHERE block_time >= {window_lo} AND block_time < {window_hi}"
            ));
        }
        if parts.is_empty() {
            return Err(StoreError::Check("all lanes are empty".into()));
        }
        Ok(parts.join(" UNION ALL BY NAME "))
    };
    conn.execute_batch(&format!("CREATE TABLE jup_all AS {}", union_of(jupiter)?))
        .map_err(|error| StoreError::Check(format!("jupiter scan: {error}")))?;
    conn.execute_batch(&format!("CREATE TABLE flash_all AS {}", union_of(flash)?))
        .map_err(|error| StoreError::Check(format!("flash scan: {error}")))?;
    conn.execute_batch(
        "CREATE TABLE jup_dedup AS
         SELECT * EXCLUDE (lane) FROM jup_all
         QUALIFY row_number() OVER (PARTITION BY signature, ordinal ORDER BY lane DESC) = 1",
    )
    .map_err(|error| StoreError::Check(format!("jupiter dedupe: {error}")))?;
    conn.execute_batch(
        "CREATE TABLE flash_dedup AS
         SELECT * EXCLUDE (lane) FROM flash_all
         QUALIFY row_number() OVER (PARTITION BY signature, ordinal ORDER BY lane DESC) = 1",
    )
    .map_err(|error| StoreError::Check(format!("flash dedupe: {error}")))?;

    let jupiter_names = JUP_EVENTS
        .iter()
        .map(|name| format!("'{}'", name.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TABLE jup_pos AS
         SELECT (block_time // 3600) * 3600 AS hour, block_time, ordinal,
                json_extract_string(fields, '$.positionCustody') AS custody,
                name,
                try_cast(json_extract_string(fields, '$.sizeUsdDelta') AS HUGEINT) AS size_usd_delta,
                try_cast(json_extract_string(fields, '$.positionSizeUsd') AS HUGEINT) AS position_size_usd,
                try_cast(json_extract_string(fields, '$.fundingFeeUsd') AS HUGEINT) AS funding_fee_usd,
                try_cast(json_extract_string(fields, '$.positionSide') AS INTEGER) AS side,
                try_cast(json_extract_string(fields, '$.price') AS HUGEINT) AS price,
                block_time::BIGINT * 100 + ordinal AS ord_key
         FROM jup_dedup
         WHERE name IN ({jupiter_names})"
    ))
    .map_err(|error| StoreError::Check(format!("jupiter projection: {error}")))?;

    // Flash: the USDv1 family and the older V2/V3/V4 family, both keyed by the
    // market address; names repeat suffixes, so the match is by substring.
    conn.execute_batch(
        "CREATE TABLE flash_pos AS
         SELECT (block_time // 3600) * 3600 AS hour, block_time, ordinal, name,
                json_extract_string(fields, '$.market') AS market,
                coalesce(try_cast(json_extract_string(fields,'$.delta_size_usd') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.deltaSizeUsd') AS HUGEINT)) AS size_usd_delta,
                coalesce(try_cast(json_extract_string(fields,'$.size_usd') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.sizeUsd') AS HUGEINT)) AS size_usd,
                coalesce(try_cast(json_extract_string(fields,'$.exit_price') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.exitPrice') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.entry_price') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.entryPrice') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.oracle_account_price') AS HUGEINT),
                         try_cast(json_extract_string(fields,'$.oracleAccountPrice') AS HUGEINT)) AS price,
                coalesce(try_cast(json_extract_string(fields,'$.exit_price_exponent') AS INTEGER),
                         try_cast(json_extract_string(fields,'$.exitPriceExponent') AS INTEGER),
                         try_cast(json_extract_string(fields,'$.entry_price_exponent') AS INTEGER),
                         try_cast(json_extract_string(fields,'$.entryPriceExponent') AS INTEGER),
                         -8) AS price_exp,
                block_time::BIGINT * 100 + ordinal AS ord_key
         FROM flash_dedup
         WHERE (name LIKE '%OpenPosition%' OR name LIKE '%IncreaseSize%'
                OR name LIKE '%DecreaseSize%' OR name LIKE '%ClosePosition%'
                OR name LIKE '%CloseAndSwap%' OR name LIKE '%Liquidate%'
                OR name LIKE '%SwapAndOpen%' OR name LIKE '%ExecuteTrigger%'
                OR name LIKE '%ExecuteLimit%')
           AND json_extract_string(fields, '$.market') IS NOT NULL",
    )
    .map_err(|error| StoreError::Check(format!("flash projection: {error}")))?;
    Ok(())
}

/// The per-(key, hour) aggregates of both venues over the projected events.
fn build_aggregates(conn: &duckdb::Connection) -> Result<(), StoreError> {
    conn.execute_batch(&format!(
        "CREATE TABLE solana_perp_stage AS
         SELECT hour, 'jupiter' AS venue, custody AS venue_key,
                count(*) FILTER (WHERE name LIKE '%Increase%') AS n_inc,
                count(*) FILTER (WHERE name LIKE '%Decrease%') AS n_dec,
                count(*) FILTER (WHERE name LIKE '%Liquidate%') AS n_liq,
                (sum(abs(size_usd_delta)) FILTER (WHERE name NOT LIKE '%Liquidate%'))::DOUBLE
                    / {JUP_USD_SCALE} AS size_usd,
                (sum(abs(size_usd_delta)) FILTER (WHERE name LIKE '%Increase%' AND side = {SIDE_LONG}))::DOUBLE
                    / nullif(sum(abs(size_usd_delta)) FILTER (WHERE name LIKE '%Increase%')::DOUBLE, 0)
                    AS buy_share,
                (coalesce(sum(funding_fee_usd), 0))::DOUBLE / {JUP_USD_SCALE} AS funding_fee_usd,
                (sum(abs(position_size_usd)) FILTER (WHERE name LIKE '%Liquidate%'))::DOUBLE
                    / {JUP_USD_SCALE} AS liq_usd,
                ln(arg_max(price, ord_key) FILTER (WHERE price > 0)::DOUBLE)
                    - ln(arg_min(price, ord_key) FILTER (WHERE price > 0)::DOUBLE) AS ret
         FROM jup_pos GROUP BY hour, custody
         UNION ALL
         SELECT hour, 'flash' AS venue, market AS venue_key,
                count(*) FILTER (WHERE name LIKE '%Open%' OR name LIKE '%Increase%' OR name LIKE '%SwapAndOpen%') AS n_inc,
                count(*) FILTER (WHERE name LIKE '%Decrease%' OR name LIKE '%Close%' OR name LIKE '%Execute%') AS n_dec,
                count(*) FILTER (WHERE name LIKE '%Liquidate%') AS n_liq,
                (sum(abs(coalesce(size_usd_delta, size_usd))) FILTER (WHERE name NOT LIKE '%Liquidate%'))::DOUBLE
                    / {JUP_USD_SCALE} AS size_usd,
                NULL::DOUBLE AS buy_share,
                NULL::DOUBLE AS funding_fee_usd,
                (sum(abs(coalesce(size_usd_delta, size_usd))) FILTER (WHERE name LIKE '%Liquidate%'))::DOUBLE
                    / {JUP_USD_SCALE} AS liq_usd,
                ln(arg_max(price * power(10::DOUBLE, price_exp::DOUBLE), ord_key) FILTER (WHERE price > 0)::DOUBLE)
                    - ln(arg_min(price * power(10::DOUBLE, price_exp::DOUBLE), ord_key) FILTER (WHERE price > 0)::DOUBLE) AS ret
         FROM flash_pos GROUP BY hour, market",
    ))
    .map_err(|error| StoreError::Check(format!("per-venue aggregates: {error}")))?;
    conn.execute_batch(
        "CREATE TABLE solana_perp_hour_stage AS
         SELECT *, true AS present FROM solana_perp_stage",
    )
    .map_err(|error| StoreError::Check(format!("present flag: {error}")))?;
    Ok(())
}

/// Join the mapping (U1 `mapping/assets.csv`, jupiter + flash rows) on
/// (venue, venue_market_id).
fn create_mapping_table(conn: &duckdb::Connection, mapping: &PerpMap) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE perp_map(venue VARCHAR, venue_key VARCHAR, asset_id VARCHAR, mapped BOOLEAN)",
    )
    .map_err(|error| StoreError::Check(format!("map table: {error}")))?;
    let mut stmt = conn
        .prepare("INSERT INTO perp_map VALUES (?, ?, ?, ?)")
        .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    for ((venue, key), (asset_id, mapped)) in &mapping.by_key {
        stmt.execute(duckdb::params![venue, key, asset_id, mapped])
            .map_err(|error| StoreError::Check(format!("map insert: {error}")))?;
    }
    conn.execute_batch(
        "CREATE TABLE solana_perp_hour_agg AS
         SELECT s.hour, s.venue, s.venue_key, m.asset_id, m.mapped,
                s.n_inc, s.n_dec, s.n_liq, s.size_usd, s.buy_share,
                s.funding_fee_usd, s.liq_usd, s.ret, s.present
         FROM solana_perp_hour_stage s
         LEFT JOIN perp_map m
           ON m.venue_key = s.venue_key
          AND m.venue = CASE WHEN s.venue = 'jupiter' THEN 'jup' ELSE s.venue END",
    )
    .map_err(|error| StoreError::Check(format!("map join: {error}")))?;
    Ok(())
}
