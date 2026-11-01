//! U2d: spot DEX hourly aggregates per mint (DESIGN.md 4.4, slice-0 layout).
//!
//! `solos-data augment combined spotdex-hours` aggregates the spot-history swap
//! batches into two hour tables under the combined root:
//!
//! * `spotdex_mint_hour/date=YYYY-MM-DD/hour=YYYYMMDD-H.parquet` - one row per
//!   (mint, unix hour) for every mint that traded: `n_swaps`, `n_buy`, `n_sell`
//!   (buy = the mint is the output side), `n_pools`, `n_programs`,
//!   `usd_volume` (winsorized at USD 10M per swap; NULL when nothing priced),
//!   `usd_priced` (share of the hour's swaps whose USD was computable). Sorted
//!   by `mint`, then hour.
//! * `spotdex_venue_hour/month=YYYY-MM.parquet` - one row per hour:
//!   `sd_log_volume` (ln of 1 + total winsorized USD), `sd_swaps`,
//!   `sd_mints_active`, `sd_program_hhi` (Herfindahl of the swap programs' USD
//!   shares), `sd_new_pools` (pools whose first swap falls in the hour; NULL
//!   unless the run reads the whole input span). Sorted by `hour`.
//!
//! Pricing follows the slice-0 decision: a swap's USD is the stable side
//! (USDC/USDT) amount directly, else the wrapped-SOL side amount times the Pyth
//! SOL last price of the hour (carried forward at most
//! [`MAX_PRICE_STALENESS_HOURS`], NULL beyond); other pairs are unpriced and
//! only counted. Per-swap USD is winsorized at [`USD_WINSOR`] because the raw
//! data contains artifact swaps above USD 1B.
//!
//! Swap batches span 6-22 wall-clock hours, so aggregation keys off
//! `block_time`, never off file boundaries: every batch is scanned per hour
//! with a zonemap-prunable `block_time` range, and per-(batch, hour) partials
//! under `<root>/.spotdex-partials/` are merged per hour, so an hour spanning
//! several files is counted once. Partials are range-independent, which makes a
//! run resumable: a rerun hashes each input against the spot-history catalog
//! and skips batches it already processed, then re-merges only hours whose
//! receipt does not match.
//!
//! USD sums are carried as integer micro-dollars (BIGINT) end to end. Integer
//! sums do not depend on accumulation order, so sealed bytes are a pure
//! function of the input bytes; only the final divisions to doubles happen at
//! merge time, on exact integer operands. The SOL hourly price itself is a
//! deterministic max over (publish_time, price), not an order-dependent argmax.
//!
//! Hours are published only when the input watermark closes them: hour `h`
//! needs `max(block_time) >= h + 1h + 2h` (the +2h guard). Requested hours
//! beyond that refuse the run; trailing unclosed hours are dropped and counted.
//! Lockbox v2 hours are dropped from both tables; receipts record how many.

use crate::fsutil::{
    create_dir_all_durable, file_hash, sync_dir_after_rename, sync_tmp_file, tmp_path,
    write_atomic, write_durable,
};
use crate::store::{StoreError, memory_connection, query_rows, query_scalar, sql_string};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::decoder::parquet;
use std::os::unix::fs::OpenOptionsExt;

use super::catalog;
use super::grid;
use super::lockbox;
use super::receipt;

// ---------------------------------------------------------------------------
// Constants

/// The unit this module runs.
pub const UNIT: &str = "spotdex-hours";

/// Bumped on an output-shape change.
pub const UNIT_VERSION: u32 = 1;

/// Schema name in the receipts.
pub const SCHEMA: &str = "spotdex-mint-hour-v1";

/// Per-mint table under the root.
pub const MINT_TABLE: &str = "spotdex_mint_hour";

/// Venue-wide table under the root.
pub const VENUE_TABLE: &str = "spotdex_venue_hour";

/// The wrapped SOL mint.
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// The USDC mint.
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

/// The USDT mint.
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

/// Per-swap USD values at or above this are clipped to it (slice 0: artifact
/// swaps above USD 1B exist in the raw data).
pub const USD_WINSOR: f64 = 10_000_000.0;

/// An hour is published only when the input watermark reaches its end plus this
/// guard (DESIGN.md 3).
pub const GUARD_SECS: i64 = 2 * 3_600;

/// A wSOL-side price older than this many hours prices nothing (the carried
/// Pyth SOL price would be too stale to be a fair USD conversion).
pub const MAX_PRICE_STALENESS_HOURS: i64 = 48;

/// The 19 spot DEX programs of the spot-history lane, one bit each in the
/// partial `programs_mask`. Any other program ORs [`PROGRAM_BIT_OTHER`].
pub const PROGRAMS: &[(&str, u64)] = &[
    ("raydium_amm", 1 << 0),
    ("raydium_clmm", 1 << 1),
    ("raydium_cpmm", 1 << 2),
    ("raydium_launchlab", 1 << 3),
    ("orca_whirlpool", 1 << 4),
    ("meteora_dlmm", 1 << 5),
    ("meteora_damm", 1 << 6),
    ("meteora_damm_v2", 1 << 7),
    ("meteora_dbc", 1 << 8),
    ("pumpswap", 1 << 9),
    ("pump", 1 << 10),
    ("humidifi", 1 << 11),
    ("tessera_v", 1 << 12),
    ("zerofi", 1 << 13),
    ("alphaq", 1 << 14),
    ("solfi", 1 << 15),
    ("solfi_v2", 1 << 16),
    ("goonfi", 1 << 17),
    ("pancake_clmm", 1 << 18),
];

/// Mask bit for a program outside [`PROGRAMS`]. Distinct unknown programs
/// collapse into this one bit; the spot-history lane is done, so this only
/// matters if the lane ever grows a twentieth program.
pub const PROGRAM_BIT_OTHER: u64 = 1 << 40;

/// Columns of one `spotdex_mint_hour` file, in file order.
pub const MINT_COLUMNS: &[(&str, &str)] = &[
    ("mint", "VARCHAR"),
    ("hour", "BIGINT"),
    ("n_swaps", "BIGINT"),
    ("n_buy", "BIGINT"),
    ("n_sell", "BIGINT"),
    ("n_pools", "BIGINT"),
    ("n_programs", "BIGINT"),
    ("usd_volume", "DOUBLE"),
    ("usd_priced", "DOUBLE"),
];

/// Columns of one `spotdex_venue_hour` file, in file order.
pub const VENUE_COLUMNS: &[(&str, &str)] = &[
    ("hour", "BIGINT"),
    ("sd_log_volume", "DOUBLE"),
    ("sd_swaps", "BIGINT"),
    ("sd_mints_active", "BIGINT"),
    ("sd_program_hhi", "DOUBLE"),
    ("sd_new_pools", "BIGINT"),
];

// ---------------------------------------------------------------------------
// Request / Report

/// One offline run.
pub struct Request<'a> {
    /// Combined store root. `spotdex_mint_hour/`, `spotdex_venue_hour/`,
    /// `.spotdex-partials/` and `catalog.json` live under it.
    pub out: &'a Path,
    /// Directory of spot-history swap batches (`tables/spot/swaps/ALL`).
    pub swaps_dir: &'a Path,
    /// The spot-history catalog JSON describing the swap batches.
    pub catalog: &'a Path,
    /// Pyth prices directories (venues-history and/or live capture).
    pub pyth_prices: &'a [PathBuf],
    /// Lockbox v2 JSON.
    pub lockbox: &'a Path,
    /// Required sha256 of the lockbox bytes.
    pub lockbox_sha256: &'a str,
    /// Operator provenance object, copied verbatim into every receipt.
    pub provenance: &'a Path,
    /// Inclusive first UTC day to publish (partials still cover whole files).
    pub since: Option<&'a str>,
    /// Inclusive last UTC day to publish. Every hour of this day must be
    /// closed by the input watermark, or the run refuses.
    pub until: Option<&'a str>,
    /// DuckDB threads (1..=8).
    pub threads: u32,
    /// DuckDB memory limit (at most 32GB).
    pub memory_limit: String,
}

/// The run's outcome, printed by the CLI.
#[derive(Clone, Debug, PartialEq)]
pub struct Report {
    /// Hours newly published on this run.
    pub hours_written: u64,
    /// Hours already published and left unchanged.
    pub hours_skipped: u64,
    /// Hours in range with no swaps at all.
    pub hours_empty: u64,
    /// Lockbox hours inside the published range (dropped, never written).
    pub lockbox_hours_dropped: u64,
    /// Trailing hours the watermark had not closed (dropped).
    pub hours_unclosed: u64,
    /// Rows in `spotdex_mint_hour` files sealed by this run.
    pub mint_rows: u64,
    /// Venue hours (re)written by this run.
    pub venue_hours: u64,
    /// Swap rows read from the input batches this run processed.
    pub swaps_read: u64,
    /// Winsorized USD across the hours this run sealed (0 when skipped).
    pub usd_volume: f64,
    /// Max input `block_time` (the input watermark).
    pub watermark_block_time: i64,
    /// Output files sealed by this run.
    pub outputs: u64,
}

impl Report {
    /// Process exit code: 0.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        0
    }

    /// JSON object for stdout.
    pub fn to_json(&self) -> String {
        crate::jsonout::Obj::new()
            .with("command", "augment combined spotdex-hours")
            .with("unit", UNIT)
            .with("unitVersion", UNIT_VERSION)
            .with("hoursWritten", self.hours_written)
            .with("hoursSkipped", self.hours_skipped)
            .with("hoursEmpty", self.hours_empty)
            .with("lockboxHoursDropped", self.lockbox_hours_dropped)
            .with("hoursUnclosed", self.hours_unclosed)
            .with("mintRows", self.mint_rows)
            .with("venueHours", self.venue_hours)
            .with("swapsRead", self.swaps_read)
            .with("usdVolume", self.usd_volume)
            .with("watermarkBlockTime", self.watermark_block_time)
            .with("outputs", self.outputs)
            .to_json()
    }
}

// ---------------------------------------------------------------------------
// Receipts

/// Receipt for one published hour.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HourReceipt {
    /// [`SCHEMA`].
    pub schema: String,
    /// [`UNIT`].
    pub unit: String,
    /// [`UNIT_VERSION`].
    pub unit_version: u32,
    /// Unix hour the file covers (its first second).
    pub hour: i64,
    /// `YYYYMMDD-H` key.
    pub hour_key: String,
    /// Rows in the mint file.
    pub rows: u64,
    /// Winsorized USD micro-dollars of the hour (NULL when nothing priced).
    pub usd_micro: Option<i64>,
    /// Sealed mint file.
    pub output: receipt::OutputFile,
    /// SHA-256 of the sorted `stem sha256` lines of the input batches that
    /// cover the hour.
    pub input_files_sha256: String,
    /// Lockbox sha256 the run checked.
    pub lockbox_sha256: String,
    /// [`config_hash`].
    pub config_sha256: String,
    /// Git commit that wrote the file, or `unknown`.
    pub git_sha: String,
    /// Operator provenance, verbatim.
    pub provenance: Value,
    /// Write time, UTC.
    pub written_utc: String,
}

/// Receipt for one venue month file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonthReceipt {
    /// [`SCHEMA`].
    pub schema: String,
    /// [`UNIT`].
    pub unit: String,
    /// [`UNIT_VERSION`].
    pub unit_version: u32,
    /// `YYYY-MM`.
    pub month: String,
    /// Rows in the month file.
    pub rows: u64,
    /// Sealed month file.
    pub output: receipt::OutputFile,
    /// Hours of this month this run (re)wrote.
    pub hours: Vec<i64>,
    /// Lockbox sha256 the run checked.
    pub lockbox_sha256: String,
    /// [`config_hash`].
    pub config_sha256: String,
    /// Operator provenance, verbatim.
    pub provenance: Value,
    /// Write time, UTC.
    pub written_utc: String,
}

// ---------------------------------------------------------------------------
// Run

/// Run the spotdex-hours unit.
///
/// # Errors
///
/// Any input is missing or malformed, a catalog fact contradicts a file, the
/// lockbox sha does not match, a requested hour is not closed by the input
/// watermark, or a sealed file fails its read-back checks. A failure leaves the
/// receipts, the catalog and every previously sealed hour in place.
pub fn run(request: &Request<'_>) -> Result<Report, StoreError> {
    if !(1..=8).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=8".into()));
    }
    let memory_limit = parse_memory_cap(&request.memory_limit)?;
    let since = request.since.map(grid::parse_date).transpose()?;
    let until = request.until.map(grid::parse_date).transpose()?;
    if let (Some(since), Some(until)) = (since, until)
        && until < since
    {
        return Err(StoreError::Check("--until is before --since".into()));
    }
    let started = grid::now_rfc3339();
    let provenance = load_provenance(request.provenance)?;
    let checked_lockbox_sha = request.lockbox_sha256.trim().to_ascii_lowercase();
    let lock = lockbox::load(request.lockbox, &checked_lockbox_sha)?;
    let lockbox_hours = lock.excluded_hours()?;
    lock.assert_v2_shape(&lockbox_hours)?;
    let config = config_hash();
    let git = git_sha();

    create_dir_all_durable(request.out)?;
    let _guard = lock_output(request.out)?;
    let temp_dir = request.out.join(".tmp-duckdb-spotdex");
    let _ = std::fs::remove_dir_all(&temp_dir);
    create_dir_all_durable(&temp_dir)?;
    let conn = memory_connection(&request.threads.to_string(), &memory_limit)?;
    parquet::require_connection_duckdb(&conn)?;
    conn.execute_batch(&format!(
        "SET temp_directory = {}; SET max_temp_directory_size = '20GB'",
        sql_string(&temp_dir.to_string_lossy())
    ))?;

    // Inputs: the swap batches with their catalog facts and time spans.
    let catalog_path_text = std::fs::read_to_string(request.catalog)
        .map_err(|error| StoreError::Check(format!("--catalog: cannot read: {error}")))?;
    let spot_catalog: SpotCatalog = serde_json::from_str(&catalog_path_text)
        .map_err(|error| StoreError::Check(format!("--catalog: bad JSON: {error}")))?;
    let swaps = scan_swaps(request.swaps_dir, &spot_catalog)?;
    let catalog_sha256 = file_hash(request.catalog)?;

    // The input watermark and the closed-hour cutoff.
    let watermark = swaps.last().expect("nonempty").bt_max;
    let first_data_hour = grid::hour_of_secs(swaps.first().expect("nonempty").bt_min);
    let last_data_hour = grid::hour_of_secs(watermark);
    // Hour h is closed once the watermark reaches its end plus the guard:
    // watermark >= (h + 1) * 3600 + GUARD_SECS.
    let last_closed_hour = grid::hour_of_secs(watermark - GUARD_SECS) - 1;

    // The published hour range: requested days clipped to closed hours.
    let mut range_lo = first_data_hour;
    let mut range_hi = last_closed_hour;
    if let Some(day) = since {
        range_lo = range_lo.max(grid::hour_of_secs(grid::day_start_secs(day)));
    }
    if let Some(day) = until {
        let last_hour_of_day = grid::hour_of_secs(grid::day_start_secs(day) + 86_399);
        if last_hour_of_day > last_closed_hour {
            return Err(StoreError::Check(format!(
                "--until {day}: hour {last_hour_of_day} is not closed by the input \
                 watermark {watermark} (guard {GUARD_SECS}s)"
            )));
        }
        range_hi = range_hi.min(last_hour_of_day);
    }
    if range_hi < range_lo {
        return Err(StoreError::Check(
            "no closed hour in the requested range".into(),
        ));
    }
    let lockbox_in_range = lockbox_hours
        .iter()
        .filter(|hour| **hour >= range_lo && **hour <= range_hi)
        .count() as u64;
    let hours_unclosed = last_data_hour.saturating_sub(last_closed_hour) as u64;

    // Pyth SOL hourly last prices.
    if load_sol_px(&conn, request.pyth_prices)? == 0 {
        return Err(StoreError::Check(
            "no Pyth SOL prices in --pyth-prices (wSOL pricing impossible)".into(),
        ));
    }

    // Partials for every batch that overlaps the range; resumable via state.
    let parts = request.out.join(".spotdex-partials");
    create_dir_all_durable(&parts)?;
    let state_path = parts.join("state.json");
    let mut state: State = std::fs::read(&state_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    if state.config_sha256.as_str() != config {
        state = State::default();
    }
    let range_start_secs = range_lo * grid::HOUR_SECS;
    let range_end_secs = (range_hi + 1) * grid::HOUR_SECS;
    let mut swaps_read = 0u64;
    for file in swaps
        .iter()
        .filter(|file| file.bt_max >= range_start_secs && file.bt_min < range_end_secs)
    {
        let done = state
            .files
            .get(&file.stem)
            .is_some_and(|sha| sha == &file.sha256)
            && partials_look_written(&parts, file);
        if done {
            continue;
        }
        let actual = file_hash(&file.path)?;
        if actual != file.sha256 {
            return Err(StoreError::Check(format!(
                "{}: sha256 {actual} != catalog {}",
                file.stem, file.sha256
            )));
        }
        let rows = row_count(&conn, &file.path)?;
        if rows != file.rows {
            return Err(StoreError::Check(format!(
                "{}: {rows} rows != catalog {}",
                file.stem, file.rows
            )));
        }
        swaps_read += rows;
        write_partials(&conn, &parts, file)?;
        state.files.insert(file.stem.clone(), file.sha256.clone());
        state.config_sha256 = config.clone();
        let mut body = serde_json::to_vec(&state).map_err(|e| StoreError::Check(e.to_string()))?;
        body.push(b'\n');
        write_atomic(&state_path, &body)?;
    }

    // Per-pool first hour: exact only when the run reads the whole span.
    let covers_floor = swaps
        .first()
        .is_some_and(|first| state.files.contains_key(&first.stem));
    let new_pools = if covers_floor {
        Some(load_new_pools(&conn, &parts)?)
    } else {
        None
    };

    // Merge, check, seal, receipt - one hour at a time.
    let mut report = Report {
        hours_written: 0,
        hours_skipped: 0,
        hours_empty: 0,
        lockbox_hours_dropped: lockbox_in_range,
        hours_unclosed,
        mint_rows: 0,
        venue_hours: 0,
        swaps_read,
        usd_volume: 0.0,
        watermark_block_time: watermark,
        outputs: 0,
    };
    let mut sealed: Vec<receipt::OutputFile> = Vec::new();
    let mut usd_micro_total = 0i64;
    let mut venue_months: BTreeMap<String, Vec<(i64, VenueRow)>> = BTreeMap::new();
    for hour in range_lo..=range_hi {
        if lockbox_hours.contains(&hour) {
            continue;
        }
        let covering: Vec<&SwapFile> = swaps
            .iter()
            .filter(|file| {
                file.bt_min < hour * grid::HOUR_SECS + grid::HOUR_SECS
                    && file.bt_max >= hour * grid::HOUR_SECS
            })
            .collect();
        let inputs_sha = file_list_sha256(&covering);
        let key = grid::hour_key(hour);
        if let Some(existing) = already_published(
            &conn,
            request.out,
            hour,
            &key,
            &inputs_sha,
            &config,
            &checked_lockbox_sha,
        )? {
            sealed.push(existing);
            report.hours_skipped += 1;
            continue;
        }
        let Some(mint) = merge_mint_hour(&conn, &parts, hour)? else {
            report.hours_empty += 1;
            continue;
        };
        if mint.rows.is_empty() {
            report.hours_empty += 1;
            continue;
        }
        let output = seal_mint_hour(&conn, request.out, hour, &mint)?;
        let venue = merge_venue_hour(&conn, &parts, hour, mint.rows.len(), new_pools.as_ref())?;
        venue_months
            .entry(month_of_hour(hour))
            .or_default()
            .push((hour, venue));
        write_hour_receipt(
            request.out,
            &HourReceipt {
                schema: SCHEMA.to_owned(),
                unit: UNIT.to_owned(),
                unit_version: UNIT_VERSION,
                hour,
                hour_key: key.clone(),
                rows: output.rows,
                usd_micro: mint.usd_micro,
                output: output.clone(),
                input_files_sha256: inputs_sha,
                lockbox_sha256: checked_lockbox_sha.clone(),
                config_sha256: config.clone(),
                git_sha: git.clone(),
                provenance: provenance.clone(),
                written_utc: grid::now_rfc3339(),
            },
        )?;
        report.hours_written += 1;
        report.mint_rows += output.rows;
        report.outputs += 1;
        usd_micro_total = usd_micro_total.saturating_add(mint.usd_micro.unwrap_or(0));
        sealed.push(output);
    }
    report.usd_volume = usd_micro_total as f64 / 1e6;

    // Venue months: union with the existing file, drop lockbox hours, seal.
    for (month, mut rows) in venue_months {
        rows.sort_by_key(|(hour, _)| *hour);
        let written_hours: BTreeSet<i64> = rows.iter().map(|(hour, _)| *hour).collect();
        let output = seal_venue_month(&conn, request.out, &month, &rows, &lockbox_hours)?;
        write_month_receipt(
            request.out,
            &MonthReceipt {
                schema: SCHEMA.to_owned(),
                unit: UNIT.to_owned(),
                unit_version: UNIT_VERSION,
                month: month.clone(),
                rows: output.rows,
                hours: written_hours.iter().copied().collect(),
                output: output.clone(),
                lockbox_sha256: checked_lockbox_sha.clone(),
                config_sha256: config.clone(),
                provenance: provenance.clone(),
                written_utc: grid::now_rfc3339(),
            },
        )?;
        report.venue_hours += written_hours.len() as u64;
        report.outputs += 1;
        sealed.push(output);
    }

    // Run receipt: everything this unit has published for the requested range.
    let rows: u64 = sealed.iter().map(|file| file.rows).sum();
    receipt::write(
        request.out,
        MINT_TABLE,
        &receipt::Receipt {
            unit: UNIT.to_owned(),
            unit_version: UNIT_VERSION,
            rows,
            outputs: sealed.clone(),
            inputs: json!({
                "swaps": {
                    "files": swaps.len(),
                    "files_sha256": swap_list_sha256(&swaps),
                    "max_block_time": watermark,
                    "top_slot": swaps.last().expect("nonempty").slot_end,
                    "catalog_at": spot_catalog.at,
                    "catalog_sha256": catalog_sha256,
                },
                "pyth_prices": {
                    "files_sha256": pyth_list_sha256(request.pyth_prices)?,
                },
                "since": since.map(|day| day.to_string()),
                "until": until.map(|day| day.to_string()),
                "hours_written": report.hours_written,
                "hours_skipped": report.hours_skipped,
                "hours_empty": report.hours_empty,
                "hours_unclosed": report.hours_unclosed,
                "lockbox_hours_dropped": report.lockbox_hours_dropped,
            }),
            lockbox_sha256: checked_lockbox_sha.clone(),
            lockbox_hours_dropped: lockbox_hours.len() as u64,
            config_sha256: config.clone(),
            provenance,
            started_utc: started,
            finished_utc: grid::now_rfc3339(),
        },
    )?;

    // Catalog: one entry per sealed file, then the unit's watermark.
    let mut cat = catalog::load(request.out)?;
    for file in &sealed {
        cat.upsert_file(catalog::FileEntry {
            path: file.path.clone(),
            rows: file.rows,
            bytes: file.bytes,
            sha256: file.sha256.clone(),
            unit: UNIT.to_owned(),
            unit_version: UNIT_VERSION,
        });
    }
    cat.set_watermarks(
        UNIT,
        json!({
            "swaps_files": swaps.len(),
            "swaps_files_sha256": swap_list_sha256(&swaps),
            "max_block_time": watermark,
            "catalog_at": spot_catalog.at,
            "catalog_sha256": catalog_sha256,
            "since": since.map(|day| day.to_string()),
            "until": until.map(|day| day.to_string()),
            "lockbox_sha256": checked_lockbox_sha,
            "lockbox_hours_dropped": lockbox_hours.len() as u64,
        }),
    );
    cat.save(request.out)?;

    Ok(report)
}

// ---------------------------------------------------------------------------
// Inputs

/// A swap batch file on disk with its catalog facts and time span.
struct SwapFile {
    /// File name without `.parquet` (`0370364680-0370455999`).
    stem: String,
    /// Full path.
    path: PathBuf,
    /// First slot of the batch, from the file name.
    slot_start: i64,
    /// Last slot of the batch, from the file name.
    slot_end: i64,
    /// sha256 expected by the catalog.
    sha256: String,
    /// Row count expected by the catalog.
    rows: u64,
    /// Min `block_time` from the Parquet statistics.
    bt_min: i64,
    /// Max `block_time` from the Parquet statistics.
    bt_max: i64,
}

/// One entry of the spot-history catalog.
#[derive(Deserialize)]
struct CatalogEntry {
    path: String,
    dataset: String,
    complete: bool,
    row_count: String,
    sha256: String,
}

/// The spot-history catalog file.
#[derive(Deserialize)]
struct SpotCatalog {
    at: String,
    files: Vec<CatalogEntry>,
}

/// Resumable per-batch state under `.spotdex-partials/state.json`.
#[derive(Serialize, Deserialize, Default)]
struct State {
    /// [`config_hash`] of the run that wrote the partials.
    config_sha256: String,
    /// Processed file stems with the catalog sha they were processed under.
    files: BTreeMap<String, String>,
}

/// `YYYY-MM` of a unix hour.
#[must_use]
fn month_of_hour(hour: i64) -> String {
    let secs =
        chrono::DateTime::from_timestamp(hour * grid::HOUR_SECS, 0).expect("hour is in range");
    secs.format("%Y-%m").to_string()
}

/// Sealed output paths must stay under the root.
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

/// SHA-256 of the sorted `stem sha256` lines of a covering-file list.
fn file_list_sha256(files: &[&SwapFile]) -> String {
    let mut stems: Vec<&str> = files.iter().map(|file| file.stem.as_str()).collect();
    stems.sort_unstable();
    let mut text = String::new();
    for stem in stems {
        let file = files
            .iter()
            .find(|file| file.stem == stem)
            .expect("same list");
        text.push_str(stem);
        text.push(' ');
        text.push_str(&file.sha256);
        text.push('\n');
    }
    crate::fsutil::sha256_hex(text.as_bytes())
}

/// SHA-256 of the sorted `stem sha256` lines of the whole input set.
fn swap_list_sha256(swaps: &[SwapFile]) -> String {
    let mut text = String::new();
    for file in swaps {
        text.push_str(&file.stem);
        text.push(' ');
        text.push_str(&file.sha256);
        text.push('\n');
    }
    crate::fsutil::sha256_hex(text.as_bytes())
}

/// SHA-256 of the sorted absolute Pyth file paths the run can read.
fn pyth_list_sha256(dirs: &[PathBuf]) -> Result<String, StoreError> {
    let mut files: Vec<String> = Vec::new();
    for dir in dirs {
        collect_parquet(dir, &mut files)?;
    }
    files.sort();
    let text = files.join("\n");
    Ok(crate::fsutil::sha256_hex(text.as_bytes()))
}

/// SHA-256 over the unit's semantic config: the pin that must move on purpose.
fn config_hash() -> String {
    let body = json!({
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "mintColumns": MINT_COLUMNS,
        "venueColumns": VENUE_COLUMNS,
        "mintSort": ["mint", "hour"],
        "venueSort": ["hour"],
        "usdWinsor": USD_WINSOR,
        "usdUnit": "micro-dollars, integer sums",
        "guardSecs": GUARD_SECS,
        "maxPriceStalenessHours": MAX_PRICE_STALENESS_HOURS,
        "stableMints": [USDC_MINT, USDT_MINT],
        "wsolMint": WSOL_MINT,
        "priceSource": "pyth SOL hourly last by publish_time, carried forward",
        "programs": PROGRAMS,
        "newPools": "global first hour when the run covers the input floor, else null",
        "lockbox": "v2 weeks with guard hours, dropped from every table",
    });
    crate::fsutil::sha256_hex(body.to_string().as_bytes())
}

/// `git rev-parse HEAD`, or `SOLOS_DATA_GIT_SHA` when set (same rule as the
/// other lanes).
fn git_sha() -> String {
    if let Ok(value) = std::env::var("SOLOS_DATA_GIT_SHA") {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_owned();
        }
    }
    let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    else {
        return "unknown".to_owned();
    };
    if !output.status.success() {
        return "unknown".to_owned();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let text = text.trim();
    if text.len() == 40 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
        text.to_owned()
    } else {
        "unknown".to_owned()
    }
}

// ---------------------------------------------------------------------------
// Input scans

/// `0370364680-0370455999` -> (start, end).
fn parse_stem(stem: &str) -> Option<(i64, i64)> {
    let (left, right) = stem.split_once('-')?;
    if left.len() < 9 || right.len() < 9 {
        return None;
    }
    Some((left.parse().ok()?, right.parse().ok()?))
}

/// List the swap batches in `dir`, cross-checked against the catalog entries.
fn scan_swaps(dir: &Path, catalog: &SpotCatalog) -> Result<Vec<SwapFile>, StoreError> {
    let mut by_stem: BTreeMap<&str, &CatalogEntry> = BTreeMap::new();
    for entry in &catalog.files {
        if entry.dataset != "swaps" {
            continue;
        }
        let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
        if let Some(stem) = name.strip_suffix(".parquet") {
            by_stem.insert(stem, entry);
        }
    }
    let mut swaps = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|error| StoreError::Check(format!("--swaps: cannot read: {error}")))?
    {
        let entry = entry.map_err(|e| StoreError::Check(format!("--swaps: {e}")))?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned();
        let Some(stem) = name.strip_suffix(".parquet") else {
            continue;
        };
        let Some((slot_start, slot_end)) = parse_stem(stem) else {
            continue;
        };
        if slot_end < slot_start {
            return Err(StoreError::Check(format!("{stem}: slot range is reversed")));
        }
        let Some(entry) = by_stem.get(stem) else {
            return Err(StoreError::Check(format!(
                "{stem}: no swaps catalog entry (catalog is stale or the directory is wrong)"
            )));
        };
        if !entry.complete {
            return Err(StoreError::Check(format!(
                "{stem}: catalog entry is incomplete"
            )));
        }
        let (bt_min, bt_max) = block_time_span(&path)?;
        swaps.push(SwapFile {
            stem: stem.to_owned(),
            path,
            slot_start,
            slot_end,
            sha256: entry.sha256.to_ascii_lowercase(),
            rows: entry
                .row_count
                .parse()
                .map_err(|_| StoreError::Check(format!("{stem}: bad catalog row_count")))?,
            bt_min,
            bt_max,
        });
    }
    if swaps.is_empty() {
        return Err(StoreError::Check("--swaps has no parquet batches".into()));
    }
    swaps.sort_by_key(|file| file.slot_start);
    Ok(swaps)
}

/// Min and max `block_time` of a batch, from the Parquet row-group statistics
/// (footer only, no data read).
fn block_time_span(path: &Path) -> Result<(i64, i64), StoreError> {
    let conn = stats_connection()?;
    let rows = query_rows(
        &conn,
        &format!(
            "SELECT min(try_cast(stats_min_value AS BIGINT)) AS lo,
                    max(try_cast(stats_max_value AS BIGINT)) AS hi
             FROM parquet_metadata({})
             WHERE path_in_schema = 'block_time'
               AND stats_min_value IS NOT NULL AND stats_max_value IS NOT NULL",
            sql_string(&path.to_string_lossy())
        ),
        &[],
    )?;
    let row = rows.first().ok_or_else(|| {
        StoreError::Check(format!("{}: no block_time statistics", path.display()))
    })?;
    let lo = row
        .int("lo")
        .ok_or_else(|| StoreError::Check(format!("{}: block_time min missing", path.display())))?;
    let hi = row
        .int("hi")
        .ok_or_else(|| StoreError::Check(format!("{}: block_time max missing", path.display())))?;
    if hi < lo {
        return Err(StoreError::Check(format!(
            "{}: block_time span is reversed",
            path.display()
        )));
    }
    Ok((lo, hi))
}

/// A throwaway in-memory connection for Parquet footer reads and file counts.
fn stats_connection() -> Result<duckdb::Connection, StoreError> {
    let conn = memory_connection("2", "1GB")?;
    parquet::require_connection_duckdb(&conn)?;
    Ok(conn)
}

/// Row count of a Parquet file.
fn row_count(conn: &duckdb::Connection, path: &Path) -> Result<u64, StoreError> {
    let value = query_scalar(
        conn,
        &format!(
            "SELECT count(*) FROM read_parquet({})",
            sql_string(&path.to_string_lossy())
        ),
        &[],
    )?;
    let n = value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| StoreError::Check(format!("{}: count unreadable", path.display())))?;
    u64::try_from(n).map_err(|_| StoreError::Check(format!("{}: negative count", path.display())))
}

/// Walk the Pyth directories, keep the SOL feed's hourly last price by
/// `publish_time`, and load it into the `sol_px` temp table. The price of an
/// hour is `max(price_f)` over the prints at that hour's max `publish_time`,
/// which is order-independent (no argmax tie-breaking).
fn load_sol_px(conn: &duckdb::Connection, dirs: &[PathBuf]) -> Result<usize, StoreError> {
    let mut files: Vec<String> = Vec::new();
    for dir in dirs {
        collect_parquet(dir, &mut files)?;
    }
    if files.is_empty() {
        return Err(StoreError::Check(
            "--pyth-prices has no parquet files".into(),
        ));
    }
    files.sort();
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE sol_px AS
         SELECT px_hour, max(px) AS px
         FROM (
           SELECT CAST(floor(publish_time / 3600) * 3600 AS BIGINT) AS px_hour,
                  publish_time AS pt,
                  max(price_f) AS px
           FROM {} WHERE symbol = 'SOL' AND price_f IS NOT NULL AND price_f > 0
           GROUP BY 1, 2
         )
         GROUP BY 1",
        parquet_list(&files)?
    ))?;
    Ok(scalar_i64(conn, "SELECT count(*) FROM sol_px")?.max(0) as usize)
}

/// Recursively collect `*.parquet` paths under `root`.
fn collect_parquet(dir: &Path, out: &mut Vec<String>) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| StoreError::Check(format!("cannot read {}: {error}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| StoreError::Check(format!("walk: {e}")))?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path.to_string_lossy().to_string());
        }
    }
    Ok(())
}

/// A `read_parquet([...])` reference over an explicit sorted file list.
fn parquet_list(files: &[String]) -> Result<String, StoreError> {
    if files.is_empty() {
        return Err(StoreError::Check("no parquet files to read".into()));
    }
    let list = files
        .iter()
        .map(|path| sql_string(path))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("read_parquet([{list}])"))
}

/// The partial `programs_mask` expression.
fn program_mask_sql() -> String {
    let branches: Vec<String> = PROGRAMS
        .iter()
        .map(|(name, bit)| format!("WHEN {} THEN {}", sql_string(name), bit))
        .collect();
    format!(
        "CASE program {} ELSE {PROGRAM_BIT_OTHER} END",
        branches.join(" ")
    )
}

// ---------------------------------------------------------------------------
// Partials

/// The row-level priced projection of one hour of one batch.
///
/// `amount_in`/`amount_out` are always the swap's input and output amounts;
/// only the mints flip with `a_to_b`. The USD is the stable side amount
/// directly, else the wSOL side amount times the Pyth SOL price of the hour
/// (NULL when the carried price is too stale), winsorized and converted to
/// integer micro-dollars so sums are order-independent.
fn priced_cte(file: &SwapFile, hour: i64) -> String {
    let lo = hour * grid::HOUR_SECS;
    let hi = lo + grid::HOUR_SECS;
    let stale_before = lo - MAX_PRICE_STALENESS_HOURS * grid::HOUR_SECS;
    let usdc = sql_string(USDC_MINT);
    let usdt = sql_string(USDT_MINT);
    let wsol = sql_string(WSOL_MINT);
    format!(
        "SELECT program, pool, in_mint, out_mint,
           CASE WHEN raw_usd IS NULL THEN NULL
                ELSE CAST(round(least(raw_usd, {USD_WINSOR}) * 1e6) AS BIGINT) END AS usd_micro
         FROM (
           SELECT program, pool, in_mint, out_mint,
             CASE
               WHEN in_mint IN ({usdc}, {usdt}) AND out_mint IN ({usdc}, {usdt})
                 THEN in_amt / pow(10, in_dec)
               WHEN in_mint IN ({usdc}, {usdt}) THEN in_amt / pow(10, in_dec)
               WHEN out_mint IN ({usdc}, {usdt}) THEN out_amt / pow(10, out_dec)
               WHEN in_mint = {wsol} AND out_mint = {wsol} THEN NULL
               WHEN in_mint = {wsol} AND sol_px_hour >= {stale_before}
                 THEN in_amt / 1e9 * sol_px
               WHEN out_mint = {wsol} AND sol_px_hour >= {stale_before}
                 THEN out_amt / 1e9 * sol_px
               ELSE NULL
             END AS raw_usd
           FROM (
             SELECT s.program, s.pool, s.hour,
               IF(a_to_b, mint_a, mint_b) AS in_mint,
               amount_in AS in_amt,
               IF(a_to_b, decimals_a, decimals_b) AS in_dec,
               IF(a_to_b, mint_b, mint_a) AS out_mint,
               amount_out AS out_amt,
               IF(a_to_b, decimals_b, decimals_a) AS out_dec,
               p.px AS sol_px, p.px_hour AS sol_px_hour
             FROM (
               SELECT program, pool, {lo} AS hour, mint_a, mint_b, decimals_a, decimals_b,
                      a_to_b, amount_in, amount_out
               FROM read_parquet({})
               WHERE block_time >= {lo} AND block_time < {hi}
             ) s
             ASOF JOIN sol_px p ON s.hour >= p.px_hour
           )
         )",
        sql_string(&file.path.to_string_lossy()),
    )
}

/// Write the partials of one batch: per hour the `a1` per-(mint, hour) sums,
/// `a2` per-(mint, hour) pool hashes and `c` per-(hour, program) sums, then the
/// `b` per-pool first hours over the whole file. The `b` write is last, so its
/// existence is the resumability probe.
fn write_partials(
    conn: &duckdb::Connection,
    parts: &Path,
    file: &SwapFile,
) -> Result<(), StoreError> {
    let first = grid::hour_of_secs(file.bt_min);
    let last = grid::hour_of_secs(file.bt_max);
    for hour in first..=last {
        let priced = priced_cte(file, hour);
        let key = grid::hour_key(hour);
        let priced_rows = hour_row_count(conn, file, hour)?;
        // a1: per-(mint, hour) sums over both swap sides. An hour with no rows
        // keeps an empty file so the merge can tell "no swaps" from "missing
        // partials".
        let a1_path = parts
            .join("a1")
            .join(format!("hk={key}"))
            .join(format!("{}.parquet", file.stem));
        create_dir_all_durable(a1_path.parent().expect("a1 parent"))?;
        let sql = format!(
            "COPY (
               WITH proj AS (
                 SELECT in_mint AS mint, 0 AS bought, program, usd_micro FROM ({priced})
                 UNION ALL
                 SELECT out_mint AS mint, 1 AS bought, program, usd_micro FROM ({priced})
               )
               SELECT mint, {hour} AS hour,
                 count(*) FILTER (WHERE bought = 1) AS n_buy,
                 count(*) FILTER (WHERE bought = 0) AS n_sell,
                 CAST(sum(usd_micro) AS BIGINT) AS usd_micro_sum,
                 count(usd_micro) AS usd_n,
                 bit_or({mask}) AS programs_mask
               FROM proj GROUP BY mint ORDER BY mint
             ) TO {} ({})",
            sql_string(&a1_path.to_string_lossy()),
            parquet::COPY_OPTIONS,
            mask = program_mask_sql(),
        );
        conn.execute_batch(&sql)?;
        if priced_rows == 0 {
            continue;
        }
        // a2: per-(mint, hour) pool hashes.
        let a2_path = parts
            .join("a2")
            .join(format!("hk={key}"))
            .join(format!("{}.parquet", file.stem));
        create_dir_all_durable(a2_path.parent().expect("a2 parent"))?;
        let sql = format!(
            "COPY (
               SELECT DISTINCT mint, hour, pool_hash FROM (
                 SELECT in_mint AS mint, {hour} AS hour, hash(pool) AS pool_hash FROM ({priced})
                 UNION ALL
                 SELECT out_mint AS mint, {hour} AS hour, hash(pool) AS pool_hash FROM ({priced})
               ) ORDER BY mint, pool_hash
             ) TO {} ({})",
            sql_string(&a2_path.to_string_lossy()),
            parquet::COPY_OPTIONS,
        );
        conn.execute_batch(&sql)?;
        // c: per-(hour, program) sums.
        let c_path = parts
            .join("c")
            .join(format!("hk={key}"))
            .join(format!("{}.parquet", file.stem));
        create_dir_all_durable(c_path.parent().expect("c parent"))?;
        let sql = format!(
            "COPY (
               SELECT {hour} AS hour, program,
                 count(*) AS n_swaps,
                 CAST(sum(usd_micro) AS BIGINT) AS usd_micro_sum,
                 count(usd_micro) AS usd_n
               FROM ({priced})
               GROUP BY program ORDER BY program
             ) TO {} ({})",
            sql_string(&c_path.to_string_lossy()),
            parquet::COPY_OPTIONS,
        );
        conn.execute_batch(&sql)?;
    }
    // b: pool first hours over the whole file (the resumability probe).
    let b_path = parts.join("b").join(format!("{}.parquet", file.stem));
    create_dir_all_durable(b_path.parent().expect("b parent"))?;
    let sql = format!(
        "COPY (
           SELECT pool, min(CAST(floor(block_time / 3600) * 3600 AS BIGINT)) AS first_hour
           FROM read_parquet({})
           GROUP BY pool ORDER BY pool
         ) TO {} ({})",
        sql_string(&file.path.to_string_lossy()),
        sql_string(&b_path.to_string_lossy()),
        parquet::COPY_OPTIONS
    );
    conn.execute_batch(&sql)?;
    Ok(())
}

/// Rows of one batch in one hour (a real count, pruned by the `block_time`
/// zonemaps).
fn hour_row_count(
    conn: &duckdb::Connection,
    file: &SwapFile,
    hour: i64,
) -> Result<u64, StoreError> {
    let lo = hour * grid::HOUR_SECS;
    let sql = format!(
        "SELECT count(*) FROM read_parquet({})
         WHERE block_time >= {lo} AND block_time < {}",
        sql_string(&file.path.to_string_lossy()),
        lo + grid::HOUR_SECS
    );
    Ok(scalar_i64(conn, &sql)?.max(0) as u64)
}

/// Cheap resumability probe: the batch's `b` partial exists and is non-empty.
/// `write_partials` writes it last, so its presence implies the hour partials
/// of the same pass completed.
fn partials_look_written(parts: &Path, file: &SwapFile) -> bool {
    let b_path = parts.join("b").join(format!("{}.parquet", file.stem));
    b_path.is_file()
        && std::fs::metadata(&b_path)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false)
}

/// Per-pool first hour across every `b` partial: (hour, pool count).
fn load_new_pools(
    conn: &duckdb::Connection,
    parts: &Path,
) -> Result<BTreeMap<i64, i64>, StoreError> {
    let mut files: Vec<String> = Vec::new();
    collect_parquet(&parts.join("b"), &mut files)?;
    if files.is_empty() {
        return Err(StoreError::Check("no b partials for new pools".into()));
    }
    files.sort();
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE new_pools AS
         SELECT CAST(first_hour / 3600 AS BIGINT) AS hour, count(*) AS n
         FROM (
           SELECT pool, min(first_hour) AS first_hour
           FROM {} GROUP BY pool
         )
         GROUP BY 1",
        parquet_list(&files)?
    ))?;
    let rows = query_rows(conn, "SELECT hour, n FROM new_pools", &[])?;
    let mut out = BTreeMap::new();
    for row in rows {
        if let (Some(hour), Some(n)) = (row.int("hour"), row.int("n")) {
            out.insert(hour, n);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Merge and seal

/// Aggregated rows of one hour, the merge result before it is written.
struct MintHour {
    rows: Vec<MintRow>,
    usd_micro: Option<i64>,
}

/// One (mint, hour) row.
struct MintRow {
    mint: String,
    n_swaps: i64,
    n_buy: i64,
    n_sell: i64,
    n_pools: i64,
    n_programs: i64,
    usd_micro: Option<i64>,
    usd_n: i64,
}

/// One venue hour row.
struct VenueRow {
    sd_log_volume: f64,
    sd_swaps: i64,
    sd_mints_active: i64,
    sd_program_hhi: Option<f64>,
    sd_new_pools: Option<i64>,
}

/// Sorted `*.parquet` paths of one partial hour partition.
fn partial_files(dir: &Path) -> Result<Vec<String>, StoreError> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    collect_parquet(dir, &mut out)?;
    out.sort();
    Ok(out)
}

/// Merge the partials of one hour. `None` when the hour has no partials at all.
fn merge_mint_hour(
    conn: &duckdb::Connection,
    parts: &Path,
    hour: i64,
) -> Result<Option<MintHour>, StoreError> {
    let key = grid::hour_key(hour);
    let a1 = partial_files(&parts.join("a1").join(format!("hk={key}")))?;
    if a1.is_empty() {
        return Ok(None);
    }
    let a2 = partial_files(&parts.join("a2").join(format!("hk={key}")))?;
    if a2.is_empty() {
        // The a1 partition only holds the empty-hour marker file.
        return Ok(None);
    }
    let rows = query_rows(
        conn,
        &format!(
            "WITH s AS (
               SELECT mint,
                 sum(n_buy) AS n_buy,
                 sum(n_sell) AS n_sell,
                 sum(usd_micro_sum) AS usd_micro,
                 sum(usd_n) AS usd_n,
                 bit_or(programs_mask) AS pmask
               FROM {s_table} GROUP BY mint
             ),
             p AS (
               SELECT mint, count(DISTINCT pool_hash) AS n_pools
               FROM {p_table} GROUP BY mint
             )
             SELECT s.mint AS mint,
               s.n_buy + s.n_sell AS n_swaps,
               s.n_buy AS n_buy,
               s.n_sell AS n_sell,
               p.n_pools AS n_pools,
               bit_count(s.pmask) AS n_programs,
               s.usd_micro AS usd_micro,
               s.usd_n AS usd_n
             FROM s JOIN p USING (mint) ORDER BY s.mint",
            s_table = parquet_list(&a1)?,
            p_table = parquet_list(&a2)?,
        ),
        &[],
    )?;
    let mut out = Vec::with_capacity(rows.len());
    let mut usd_micro = 0i64;
    let mut any_usd = false;
    for row in rows {
        let row_usd = row.int("usd_micro");
        let row = MintRow {
            mint: row.str("mint").unwrap_or_default().to_owned(),
            n_swaps: row.int("n_swaps").unwrap_or_default(),
            n_buy: row.int("n_buy").unwrap_or_default(),
            n_sell: row.int("n_sell").unwrap_or_default(),
            n_pools: row.int("n_pools").unwrap_or_default(),
            n_programs: row.int("n_programs").unwrap_or_default(),
            usd_micro: row_usd,
            usd_n: row.int("usd_n").unwrap_or_default(),
        };
        if let Some(value) = row_usd {
            any_usd = true;
            usd_micro = usd_micro.saturating_add(value);
        }
        out.push(row);
    }
    Ok(Some(MintHour {
        rows: out,
        usd_micro: any_usd.then_some(usd_micro),
    }))
}

/// Seal one hour's mint file through a checked staging write.
fn seal_mint_hour(
    conn: &duckdb::Connection,
    out: &Path,
    hour: i64,
    mint: &MintHour,
) -> Result<receipt::OutputFile, StoreError> {
    let key = grid::hour_key(hour);
    let date = grid::date_of_hour(hour);
    let relative = format!("{MINT_TABLE}/date={date}/hour={key}.parquet");
    let final_path = under_out(out, &relative)?;
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent)?;
    }
    let rows_json: Vec<String> = mint
        .rows
        .iter()
        .map(|row| {
            format!(
                "({}, {}, {}, {}, {}, {}, {}, {}, {})",
                sql_string(&row.mint),
                hour,
                row.n_swaps,
                row.n_buy,
                row.n_sell,
                row.n_pools,
                row.n_programs,
                row.usd_micro
                    .map_or_else(|| "NULL".to_owned(), |value| value.to_string()),
                row.usd_n,
            )
        })
        .collect();
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE merged (
           mint VARCHAR, hour BIGINT, n_swaps BIGINT, n_buy BIGINT, n_sell BIGINT,
           n_pools BIGINT, n_programs BIGINT, usd_micro BIGINT, usd_n BIGINT
         );
         INSERT INTO merged VALUES {}",
        rows_json.join(", ")
    ))?;
    let tmp = tmp_path(&final_path);
    let _ = std::fs::remove_file(&tmp);
    let sql = format!(
        "COPY (
           SELECT mint, hour, n_swaps, n_buy, n_sell, n_pools, n_programs,
             usd_micro / 1e6 AS usd_volume,
             usd_n::DOUBLE / (n_buy + n_sell) AS usd_priced
           FROM merged ORDER BY mint, hour
         ) TO {} ({})",
        sql_string(&tmp.to_string_lossy()),
        parquet::COPY_OPTIONS
    );
    if let Err(error) = conn.execute_batch(&sql) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    check_mint_file(conn, &tmp, hour, mint.rows.len())?;
    sync_tmp_file(&tmp)?;
    std::fs::rename(&tmp, &final_path)?;
    sync_dir_after_rename(
        final_path
            .parent()
            .ok_or_else(|| StoreError::Check("hour file has no parent".into()))?,
    )?;
    let bytes = std::fs::metadata(&final_path)?.len();
    Ok(receipt::OutputFile {
        path: relative,
        rows: mint.rows.len() as u64,
        bytes,
        sha256: file_hash(&final_path)?,
    })
}

/// Independent read-back checks of one sealed-hour staging file.
fn check_mint_file(
    conn: &duckdb::Connection,
    path: &Path,
    hour: i64,
    expected_rows: usize,
) -> Result<(), StoreError> {
    let quoted = sql_string(&path.to_string_lossy());
    let count = scalar_i64(
        conn,
        &format!("SELECT count(*) AS n FROM read_parquet({quoted})"),
    )?;
    if count != expected_rows as i64 {
        return Err(StoreError::Check(format!(
            "hour {hour}: staged {expected_rows} rows, parquet has {count}"
        )));
    }
    let bad = scalar_i64(
        conn,
        &format!(
            "SELECT count(*) AS n FROM read_parquet({quoted})
             WHERE hour IS DISTINCT FROM {hour} OR mint IS NULL
                OR n_swaps != n_buy + n_sell OR n_swaps <= 0
                OR n_buy < 0 OR n_sell < 0 OR n_pools <= 0 OR n_programs <= 0
                OR usd_priced IS NULL OR NOT (usd_priced >= 0 AND usd_priced <= 1)
                OR (usd_volume IS NOT NULL AND usd_volume < 0)"
        ),
    )?;
    if bad != 0 {
        return Err(StoreError::Check(format!(
            "hour {hour}: {bad} rows failed the invariant checks"
        )));
    }
    Ok(())
}

/// Merge one hour's venue row from the `c` partials. The Herfindahl squares
/// are exact `HUGEINT`s rendered as strings; only the final division is float.
fn merge_venue_hour(
    conn: &duckdb::Connection,
    parts: &Path,
    hour: i64,
    mints_active: usize,
    new_pools: Option<&BTreeMap<i64, i64>>,
) -> Result<VenueRow, StoreError> {
    let key = grid::hour_key(hour);
    let c = partial_files(&parts.join("c").join(format!("hk={key}")))?;
    if c.is_empty() {
        return Err(StoreError::Check(format!(
            "hour {hour}: c partials missing for a non-empty hour"
        )));
    }
    let rows = query_rows(
        conn,
        &format!(
            "WITH c AS (
               SELECT program, sum(n_swaps) AS n, sum(usd_micro_sum) AS u
               FROM {} GROUP BY program
             ),
             t AS (
               SELECT sum(n) AS n, sum(u) AS u, sum(u::HUGEINT * u) AS uu FROM c
             )
             SELECT t.n AS n, t.u AS u, t.uu::VARCHAR AS uu FROM t",
            parquet_list(&c)?
        ),
        &[],
    )?;
    let row = rows
        .first()
        .ok_or_else(|| StoreError::Check(format!("hour {hour}: venue merge produced no row")))?;
    let n = row.int("n").unwrap_or_default();
    let u = row.int("u").unwrap_or_default();
    let uu: i128 = row
        .str("uu")
        .and_then(|text| text.parse().ok())
        .unwrap_or_default();
    let total = u as f64;
    let hhi = if u > 0 && total > 0.0 {
        Some(uu as f64 / (total * total))
    } else {
        None
    };
    Ok(VenueRow {
        sd_log_volume: (1.0 + total / 1e6).ln(),
        sd_swaps: n,
        sd_mints_active: mints_active as i64,
        sd_program_hhi: hhi,
        sd_new_pools: new_pools.and_then(|pools| pools.get(&hour).copied()),
    })
}

/// Union this run's venue rows with the existing month file, drop lockbox
/// hours, and seal through a checked staging write.
fn seal_venue_month(
    conn: &duckdb::Connection,
    out: &Path,
    month: &str,
    rows: &[(i64, VenueRow)],
    lockbox_hours: &BTreeSet<i64>,
) -> Result<receipt::OutputFile, StoreError> {
    let relative = format!("{VENUE_TABLE}/month={month}.parquet");
    let final_path = under_out(out, &relative)?;
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent)?;
    }
    let excluded = lockbox_hours
        .iter()
        .map(|hour| hour.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let existing_rows = if final_path.is_file() {
        let quoted = sql_string(&final_path.to_string_lossy());
        query_rows(
            conn,
            &format!(
                "SELECT hour, sd_log_volume, sd_swaps, sd_mints_active, sd_program_hhi, sd_new_pools
                 FROM read_parquet({quoted})
                 WHERE hour NOT IN (SELECT unnest([{excluded}]))
                 ORDER BY hour"
            ),
            &[],
        )?
    } else {
        Vec::new()
    };
    let replaced: BTreeSet<i64> = rows.iter().map(|(hour, _)| *hour).collect();
    let kept = existing_rows
        .iter()
        .filter(|row| !replaced.contains(&row.int("hour").unwrap_or_default()))
        .count();
    let expected = kept + rows.len();
    let mut values: Vec<String> = Vec::with_capacity(expected);
    for row in &existing_rows {
        let hour = row.int("hour").unwrap_or_default();
        if replaced.contains(&hour) {
            continue;
        }
        values.push(format!(
            "({}, {}, {}, {}, {}, {})",
            hour,
            double_sql(row.get("sd_log_volume")),
            row.int("sd_swaps").unwrap_or_default(),
            row.int("sd_mints_active").unwrap_or_default(),
            double_sql(row.get("sd_program_hhi")),
            double_sql(row.get("sd_new_pools")),
        ));
    }
    for (hour, row) in rows {
        if lockbox_hours.contains(hour) {
            return Err(StoreError::Check(format!(
                "hour {hour}: lockbox hour reached the venue seal"
            )));
        }
        values.push(format!(
            "({}, {}, {}, {}, {}, {})",
            hour,
            sql_f64(row.sd_log_volume),
            row.sd_swaps,
            row.sd_mints_active,
            row.sd_program_hhi
                .map_or_else(|| "NULL".to_owned(), sql_f64),
            row.sd_new_pools
                .map_or_else(|| "NULL".to_owned(), |value| value.to_string()),
        ));
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE venue (
           hour BIGINT, sd_log_volume DOUBLE, sd_swaps BIGINT, sd_mints_active BIGINT,
           sd_program_hhi DOUBLE, sd_new_pools BIGINT
         );
         INSERT INTO venue VALUES {}",
        values.join(", ")
    ))?;
    let tmp = tmp_path(&final_path);
    let _ = std::fs::remove_file(&tmp);
    let sql = format!(
        "COPY (SELECT * FROM venue ORDER BY hour) TO {} ({})",
        sql_string(&tmp.to_string_lossy()),
        parquet::COPY_OPTIONS
    );
    if let Err(error) = conn.execute_batch(&sql) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    check_venue_file(conn, &tmp, expected, lockbox_hours)?;
    sync_tmp_file(&tmp)?;
    std::fs::rename(&tmp, &final_path)?;
    sync_dir_after_rename(
        final_path
            .parent()
            .ok_or_else(|| StoreError::Check("month file has no parent".into()))?,
    )?;
    let bytes = std::fs::metadata(&final_path)?.len();
    Ok(receipt::OutputFile {
        path: relative,
        rows: expected as u64,
        bytes,
        sha256: file_hash(&final_path)?,
    })
}

/// Independent read-back checks of one sealed month staging file.
fn check_venue_file(
    conn: &duckdb::Connection,
    path: &Path,
    expected_rows: usize,
    lockbox_hours: &BTreeSet<i64>,
) -> Result<(), StoreError> {
    let quoted = sql_string(&path.to_string_lossy());
    let count = scalar_i64(
        conn,
        &format!("SELECT count(*) AS n FROM read_parquet({quoted})"),
    )?;
    if count != expected_rows as i64 {
        return Err(StoreError::Check(format!(
            "venue month: staged {expected_rows} rows, parquet has {count}"
        )));
    }
    let hours = lockbox_hours
        .iter()
        .map(|hour| hour.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let bad = scalar_i64(
        conn,
        &format!(
            "SELECT count(*) AS n FROM read_parquet({quoted})
             WHERE hour IS NULL OR sd_swaps IS NULL OR sd_swaps <= 0
                OR sd_mints_active IS NULL OR sd_mints_active <= 0
                OR sd_log_volume IS NULL
                OR (sd_program_hhi IS NOT NULL AND NOT (sd_program_hhi > 0 AND sd_program_hhi <= 1))
                OR hour IN (SELECT unnest([{hours}]))"
        ),
    )?;
    if bad != 0 {
        return Err(StoreError::Check(format!(
            "venue month: {bad} rows failed the invariant checks"
        )));
    }
    Ok(())
}

/// A finite f64 as a SQL literal.
fn sql_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value}")
    } else {
        "NULL".to_owned()
    }
}

/// A JSON number (or null) as a SQL literal.
fn double_sql(value: Option<&Value>) -> String {
    match value {
        Some(Value::Number(n)) => sql_f64(n.as_f64().unwrap_or_default()),
        Some(Value::String(text)) => text.clone(),
        _ => "NULL".to_owned(),
    }
}

/// One scalar BIGINT from a query. DuckDB counts may arrive as JSON numbers
/// or as strings, depending on the width of the value.
fn scalar_i64(conn: &duckdb::Connection, sql: &str) -> Result<i64, StoreError> {
    let value = query_scalar(conn, sql, &[])?;
    Ok(value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Receipts and idempotence

/// Path of one hour receipt.
fn hour_receipt_path(out: &Path, key: &str) -> PathBuf {
    out.join(MINT_TABLE)
        .join("receipts")
        .join(format!("{key}.json"))
}

/// Write one hour receipt durably.
fn write_hour_receipt(out: &Path, receipt: &HourReceipt) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    let path = hour_receipt_path(out, &receipt.hour_key);
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

/// Read one hour receipt, if it is there and parses.
fn read_hour_receipt(out: &Path, key: &str) -> Option<HourReceipt> {
    let text = std::fs::read_to_string(hour_receipt_path(out, key)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Path of one month receipt.
fn month_receipt_path(out: &Path, month: &str) -> PathBuf {
    out.join(VENUE_TABLE)
        .join("receipts")
        .join(format!("month={month}.json"))
}

/// Write one month receipt durably.
fn write_month_receipt(out: &Path, receipt: &MonthReceipt) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec(receipt).map_err(|e| StoreError::Check(e.to_string()))?;
    body.push(b'\n');
    let path = month_receipt_path(out, &receipt.month);
    if let Some(parent) = path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&path, &body)?;
    Ok(())
}

/// Whether an hour is already published exactly as this run would publish it:
/// the receipt matches the inputs, config and lockbox, and the sealed file
/// still hashes and counts to the receipt's values. Returns the recorded
/// output so the run receipt and the catalog can list it again.
fn already_published(
    conn: &duckdb::Connection,
    out: &Path,
    hour: i64,
    key: &str,
    inputs_sha: &str,
    config: &str,
    lockbox_request_sha: &str,
) -> Result<Option<receipt::OutputFile>, StoreError> {
    let Some(r) = read_hour_receipt(out, key) else {
        return Ok(None);
    };
    if r.schema != SCHEMA
        || r.unit != UNIT
        || r.unit_version != UNIT_VERSION
        || r.hour != hour
        || r.hour_key != key
        || r.input_files_sha256 != inputs_sha
        || r.config_sha256 != config
        || r.lockbox_sha256 != lockbox_request_sha
    {
        return Ok(None);
    }
    let path = under_out(out, &r.output.path)?;
    if !path.is_file() || file_hash(&path)? != r.output.sha256 {
        return Ok(None);
    }
    if row_count(conn, &path)? != r.rows {
        return Ok(None);
    }
    Ok(Some(r.output))
}

/// Exclusive `.spotdex-hours.lock` beside the output root.
fn lock_output(out: &Path) -> Result<nix::fcntl::Flock<std::fs::File>, StoreError> {
    let path = out.join(".spotdex-hours.lock");
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
            Err(StoreError::Check(
                "spotdex-hours output already owned".into(),
            ))
        }
        Err((_, errno)) => Err(std::io::Error::from(errno).into()),
    }
}

/// DuckDB memory cap: the shared parser, plus the plan's 32GB ceiling.
fn parse_memory_cap(text: &str) -> Result<String, StoreError> {
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

/// The provenance object is required and must be a JSON object.
fn load_provenance(path: &Path) -> Result<Value, StoreError> {
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

// ---------------------------------------------------------------------------
// Tests: offline fixtures, hand-computed formulas, lockbox drop, unclosed-hour
// refusal, idempotent rerun.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonout::Obj;

    /// `2026-10-23T00:00:00Z`, outside every lockbox week.
    const H_SEC: i64 = 1_792_713_600;
    /// The unix hour number of [`H_SEC`].
    const H: i64 = H_SEC / 3_600;

    fn temp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "spotdex-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp root");
        dir
    }

    /// One swap row of a fixture batch.
    struct Swap {
        block_time: i64,
        program: &'static str,
        pool: &'static str,
        mint_a: &'static str,
        mint_b: &'static str,
        decimals_a: i64,
        decimals_b: i64,
        a_to_b: bool,
        amount_in: i64,
        amount_out: i64,
    }

    fn swap(
        block_time: i64,
        pool: &'static str,
        a_to_b: bool,
        amount_in: i64,
        amount_out: i64,
    ) -> Swap {
        Swap {
            block_time,
            program: "pumpswap",
            pool,
            mint_a: WSOL_MINT,
            mint_b: USDC_MINT,
            decimals_a: 9,
            decimals_b: 6,
            a_to_b,
            amount_in,
            amount_out,
        }
    }

    /// Write one batch file, return (stem, row count).
    fn write_batch(dir: &Path, stem: &str, swaps: &[Swap]) -> (String, u64) {
        let conn = duckdb::Connection::open_in_memory().expect("fixture conn");
        let values: Vec<String> = swaps
            .iter()
            .map(|s| {
                format!(
                    "({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    s.block_time,
                    sql_string(s.program),
                    sql_string(s.pool),
                    sql_string(s.mint_a),
                    sql_string(s.mint_b),
                    s.decimals_a,
                    s.decimals_b,
                    s.a_to_b,
                    s.amount_in,
                    s.amount_out,
                )
            })
            .collect();
        let path = dir.join(format!("{stem}.parquet"));
        let sql = format!(
            "COPY (
               SELECT * FROM (VALUES {}) t(
                 block_time, program, pool, mint_a, mint_b,
                 decimals_a, decimals_b, a_to_b, amount_in, amount_out
               )
             ) TO {} ({})",
            values.join(", "),
            sql_string(&path.to_string_lossy()),
            parquet::COPY_OPTIONS
        );
        conn.execute_batch(&sql).expect("fixture batch");
        (stem.to_owned(), swaps.len() as u64)
    }

    /// Write the Pyth prices fixture: SOL prints and optional other symbols.
    fn write_pyth(dir: &Path, prints: &[(i64, &str, f64)]) {
        std::fs::create_dir_all(dir).expect("pyth dir");
        let conn = duckdb::Connection::open_in_memory().expect("fixture conn");
        let values: Vec<String> = prints
            .iter()
            .map(|(t, symbol, px)| format!("({}, {}, {})", t, sql_string(symbol), px))
            .collect();
        let path = dir.join("2026-10-23T00.parquet");
        let sql = format!(
            "COPY (
               SELECT * FROM (VALUES {}) t(publish_time, symbol, price_f)
             ) TO {} ({})",
            values.join(", "),
            sql_string(&path.to_string_lossy()),
            parquet::COPY_OPTIONS
        );
        conn.execute_batch(&sql).expect("fixture pyth");
    }

    /// Catalog JSON for the given batches, hashing the files as written.
    fn write_catalog(path: &Path, batches: &[(String, u64)]) -> PathBuf {
        let dir = path.parent().expect("catalog parent").to_path_buf();
        let mut entries: Vec<Value> = Vec::new();
        for (stem, rows) in batches {
            let sha = file_hash(dir.join(format!("{stem}.parquet")).as_path()).expect("batch sha");
            entries.push(json!({
                "path": format!("tables/spot/swaps/ALL/{stem}.parquet"),
                "source": "spot",
                "dataset": "swaps",
                "symbol": "ALL",
                "complete": true,
                "row_count": rows.to_string(),
                "sha256": sha,
            }));
        }
        let body = json!({"at": "2026-11-01T00:00:00.000Z", "files": entries});
        std::fs::write(path, serde_json::to_vec(&body).expect("catalog json")).expect("catalog");
        path.to_path_buf()
    }

    /// The v2 lockbox: the four real exclusion weeks with a 24-hour guard.
    fn write_lockbox(dir: &Path) -> (PathBuf, String) {
        let body = json!({
            "version": "v2",
            "created": "2026-10-22",
            "guard_hours": 24,
            "weeks": [
                {"name": "early_stress", "start": "2026-06-01T00:00:00Z", "end": "2026-06-08T00:00:00Z"},
                {"name": "stress", "start": "2026-08-17T00:00:00Z", "end": "2026-08-24T00:00:00Z"},
                {"name": "quiet", "start": "2026-09-07T00:00:00Z", "end": "2026-09-14T00:00:00Z"},
                {"name": "busy", "start": "2026-09-28T00:00:00Z", "end": "2026-10-05T00:00:00Z"}
            ]
        });
        let path = dir.join("lockbox.json");
        let bytes = serde_json::to_vec(&body).expect("lockbox json");
        let sha = crate::fsutil::sha256_hex(&bytes);
        std::fs::write(&path, &bytes).expect("lockbox");
        (path, sha)
    }

    fn write_provenance(dir: &Path) -> PathBuf {
        let path = dir.join("provenance.json");
        std::fs::write(&path, br#"{"solos_data_git_sha":"test"}"#).expect("provenance");
        path
    }

    struct Fixture {
        root: PathBuf,
        swaps_dir: PathBuf,
        pyth_dir: PathBuf,
        catalog: PathBuf,
        lockbox: PathBuf,
        lockbox_sha: String,
        provenance: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let root = temp_root(tag);
            std::fs::create_dir_all(root.join("swaps")).expect("swaps dir");
            std::fs::create_dir_all(root.join("pyth")).expect("pyth dir");
            let (lockbox, lockbox_sha) = write_lockbox(&root);
            Self {
                swaps_dir: root.join("swaps"),
                pyth_dir: root.join("pyth"),
                catalog: root.join("swaps").join("catalog-spot-history.json"),
                lockbox,
                lockbox_sha,
                provenance: write_provenance(&root),
                root,
            }
        }

        fn request<'a>(&'a self, since: Option<&'a str>, until: Option<&'a str>) -> Request<'a> {
            Request {
                out: &self.root,
                swaps_dir: &self.swaps_dir,
                catalog: &self.catalog,
                pyth_prices: std::slice::from_ref(&self.pyth_dir),
                lockbox: &self.lockbox,
                lockbox_sha256: &self.lockbox_sha,
                provenance: &self.provenance,
                since,
                until,
                threads: 2,
                memory_limit: "1GB".to_owned(),
            }
        }
    }

    /// SOL at USD 100 during the fixture hours.
    fn sol_at_100(pyth: &Path, hour: i64) {
        write_pyth(pyth, &[(hour * 3_600 + 10, "SOL", 100.0)]);
    }

    fn hour_file(out: &Path, hour: i64) -> PathBuf {
        out.join(MINT_TABLE)
            .join(format!("date={}", grid::date_of_hour(hour)))
            .join(format!("hour={}.parquet", grid::hour_key(hour)))
    }

    fn read_rows(path: &Path) -> Vec<Obj> {
        let conn = duckdb::Connection::open_in_memory().expect("read conn");
        query_rows(
            &conn,
            &format!(
                "SELECT * FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )
        .expect("read rows")
    }

    #[test]
    fn formulas_match_hand_computed_rows() {
        let fx = Fixture::new("formulas");
        sol_at_100(&fx.pyth_dir, H);
        // One hour of swaps with every pricing branch, plus a watermark swap
        // 50,000 s later (hour H_SEC + 13, which the +2h guard leaves unclosed).
        let swaps = vec![
            // r1: 2 SOL in, 250 USDC out. USDC bought, wSOL sold, USD 250.
            swap(H_SEC + 100, "P1", true, 2_000_000_000, 250_000_000),
            // r2: a_to_b=false, TOKEN_B in, USDC out: USDC bought (output
            // side), TOKEN_B sold, USD 8.
            Swap {
                block_time: H_SEC + 200,
                program: "raydium_clmm",
                pool: "P2",
                mint_a: USDC_MINT,
                mint_b: "TokenB11111111111111111111111111111111111111",
                decimals_a: 6,
                decimals_b: 2,
                a_to_b: false,
                amount_in: 5_000,
                amount_out: 8_000_000,
            },
            // r3: TOKEN_A in, 3 wSOL out: wSOL bought, USD 3 x 100 = 300.
            Swap {
                block_time: H_SEC + 300,
                program: "pumpswap",
                pool: "P1",
                mint_a: "TokenA11111111111111111111111111111111111111",
                mint_b: WSOL_MINT,
                decimals_a: 6,
                decimals_b: 9,
                a_to_b: true,
                amount_in: 1_000_000,
                amount_out: 3_000_000_000,
            },
            // r4: 20,000 USDC in (USD 20M, winsorized to 10M).
            Swap {
                block_time: H_SEC + 400,
                program: "pumpswap",
                pool: "P2",
                mint_a: USDC_MINT,
                mint_b: "TokenB11111111111111111111111111111111111111",
                decimals_a: 6,
                decimals_b: 2,
                a_to_b: true,
                amount_in: 20_000_000_000_000,
                amount_out: 1,
            },
            // r5: no stable or wSOL side: unpriced.
            Swap {
                block_time: H_SEC + 500,
                program: "pumpswap",
                pool: "P4",
                mint_a: "TokenA11111111111111111111111111111111111111",
                mint_b: "TokenB11111111111111111111111111111111111111",
                decimals_a: 6,
                decimals_b: 6,
                a_to_b: true,
                amount_in: 1,
                amount_out: 2,
            },
            // r6: the watermark swap in hour H_SEC + 13.
            swap(H_SEC + 50_000, "P9", true, 1_000_000_000, 10_000_000),
        ];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        write_catalog(&fx.catalog, &batches);

        let report = run(&fx.request(None, None)).expect("run");
        assert_eq!(report.hours_written, 1, "hour H only: {report:?}");
        assert_eq!(report.hours_empty, 10, "H+1..H+10 are empty");
        assert_eq!(report.hours_unclosed, 3, "H+11..H+13 are unclosed");
        assert!(
            !hour_file(&fx.root, H + 13).exists(),
            "unclosed hour written"
        );

        let rows = read_rows(&hour_file(&fx.root, H));
        assert_eq!(rows.len(), 4, "USDC, wSOL, TOKEN_A, TOKEN_B");
        let by_mint: BTreeMap<String, &Obj> = rows
            .iter()
            .map(|row| (row.str("mint").expect("mint").to_owned(), row))
            .collect();

        let usdc = by_mint.get(USDC_MINT).expect("usdc row");
        assert_eq!(usdc.int("n_swaps"), Some(3));
        assert_eq!(usdc.int("n_buy"), Some(2), "r1 and r2 pay out USDC");
        assert_eq!(usdc.int("n_sell"), Some(1), "r4 pays USDC in");
        assert_eq!(usdc.int("n_pools"), Some(2), "P1, P2");
        assert_eq!(
            usdc.int("n_programs"),
            Some(2),
            "pumpswap (r1, r4) + raydium_clmm (r2)"
        );
        let usdc_usd = usdc.get("usd_volume").and_then(Value::as_f64).expect("usd");
        assert!((usdc_usd - (250.0 + 8.0 + USD_WINSOR)).abs() < 1e-6);
        assert!(
            (usdc
                .get("usd_priced")
                .and_then(Value::as_f64)
                .expect("priced")
                - 1.0)
                .abs()
                < 1e-12
        );

        let wsol = by_mint.get(WSOL_MINT).expect("wsol row");
        assert_eq!(wsol.int("n_swaps"), Some(2));
        assert_eq!(wsol.int("n_buy"), Some(1), "r3 pays wSOL out");
        assert_eq!(wsol.int("n_sell"), Some(1), "r1 pays wSOL in");
        let wsol_usd = wsol.get("usd_volume").and_then(Value::as_f64).expect("usd");
        assert!(
            (wsol_usd - (250.0 + 300.0)).abs() < 1e-6,
            "stable direct + 3 SOL at 100"
        );

        let token_a = by_mint
            .get("TokenA11111111111111111111111111111111111111")
            .expect("token a");
        assert_eq!(token_a.int("n_swaps"), Some(2));
        assert_eq!(token_a.int("n_buy"), Some(0));
        assert_eq!(token_a.int("n_sell"), Some(2));
        // r3 shares its row USD (3 SOL at 100) with the TOKEN_A side; r5 is
        // unpriced, so half the swaps priced.
        let token_a_usd = token_a
            .get("usd_volume")
            .and_then(Value::as_f64)
            .expect("usd");
        assert!((token_a_usd - 300.0).abs() < 1e-9);
        assert!(
            (token_a
                .get("usd_priced")
                .and_then(Value::as_f64)
                .expect("priced")
                - 0.5)
                .abs()
                < 1e-12
        );

        let token_b = by_mint
            .get("TokenB11111111111111111111111111111111111111")
            .expect("token b");
        assert_eq!(token_b.int("n_swaps"), Some(3));
        assert_eq!(token_b.int("n_buy"), Some(2), "r4 and r5 pay out TOKEN_B");
        assert_eq!(token_b.int("n_pools"), Some(2));
        let priced = token_b
            .get("usd_priced")
            .and_then(Value::as_f64)
            .expect("priced");
        assert!(
            (priced - 2.0 / 3.0).abs() < 1e-12,
            "r2 and r4 priced, r5 not"
        );

        // Venue row: 5 swaps, USD 10,000,558 total, three pools new, hhi over
        // the three priced programs.
        let month_path = fx.root.join(VENUE_TABLE).join("month=2026-10.parquet");
        let venue = read_rows(&month_path);
        assert_eq!(venue.len(), 1);
        let v = &venue[0];
        assert_eq!(v.int("hour"), Some(H));
        assert_eq!(v.int("sd_swaps"), Some(5));
        assert_eq!(v.int("sd_mints_active"), Some(4));
        let total = 250.0 + 8.0 + 300.0 + USD_WINSOR;
        let logv = v
            .get("sd_log_volume")
            .and_then(Value::as_f64)
            .expect("logv");
        assert!((logv - (1.0 + total).ln()).abs() < 1e-9);
        let hhi = v
            .get("sd_program_hhi")
            .and_then(Value::as_f64)
            .expect("hhi");
        let sq = |usd: f64| (usd * 1e6) * (usd * 1e6);
        let want_hhi = (sq(250.0 + 300.0 + USD_WINSOR) + sq(8.0)) / (total * 1e6 * (total * 1e6));
        assert!((hhi - want_hhi).abs() < 1e-9, "hhi {hhi} vs {want_hhi}");
        assert_eq!(v.int("sd_new_pools"), Some(3), "P1, P2, P4 first swap at H");

        // The hour receipt records the hour, rows and lockbox.
        let receipt: HourReceipt = serde_json::from_str(
            &std::fs::read_to_string(
                fx.root
                    .join(MINT_TABLE)
                    .join("receipts")
                    .join(format!("{}.json", grid::hour_key(H))),
            )
            .expect("hour receipt"),
        )
        .expect("receipt json");
        assert_eq!(receipt.rows, 4);
        assert_eq!(receipt.lockbox_sha256, fx.lockbox_sha);
    }

    #[test]
    fn an_hour_spanning_files_merges_once() {
        let fx = Fixture::new("crossfile");
        sol_at_100(&fx.pyth_dir, H);
        // Pool PX trades in both batches of the same hour; PY only in the
        // second; programs differ per batch.
        let mut first = vec![swap(H_SEC + 10, "PX", true, 1_000_000_000, 10_000_000)];
        first[0].program = "humidifi";
        let second = vec![
            Swap {
                program: "zerofi",
                ..swap(H_SEC + 20, "PX", false, 20_000_000, 2_000_000_000)
            },
            swap(H_SEC + 30, "PY", true, 1_000_000_000, 5_000_000),
            // Watermark swap in hour H_SEC + 13, unclosed.
            swap(H_SEC + 50_000, "PZ", true, 1_000_000_000, 10_000_000),
        ];
        let mut batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &first)];
        batches.push(write_batch(&fx.swaps_dir, "0370090000-0370179999", &second));
        write_catalog(&fx.catalog, &batches);

        let report = run(&fx.request(None, None)).expect("run");
        assert_eq!(report.hours_written, 1);
        let rows = read_rows(&hour_file(&fx.root, H));
        assert_eq!(rows.len(), 2, "wSOL and USDC");
        let row = rows
            .iter()
            .find(|row| row.str("mint") == Some(WSOL_MINT))
            .expect("wsol row");
        assert_eq!(row.int("n_swaps"), Some(3));
        assert_eq!(row.int("n_pools"), Some(2), "PX counted once across files");
        assert_eq!(
            row.int("n_programs"),
            Some(3),
            "humidifi + zerofi + pumpswap"
        );
        let usd = row.get("usd_volume").and_then(Value::as_f64).expect("usd");
        assert!((usd - 35.0).abs() < 1e-9);
        let month_path = fx.root.join(VENUE_TABLE).join("month=2026-10.parquet");
        let venue = read_rows(&month_path);
        assert_eq!(
            venue[0].int("sd_new_pools"),
            Some(2),
            "PX and PY start at H; PZ is unclosed"
        );
    }

    #[test]
    fn lockbox_hours_are_dropped() {
        let fx = Fixture::new("lockbox");
        // 2026-09-30T12:00 = 1_790_769_600 sits inside the busy week plus its
        // 24h guard (2026-09-27T00:00 to 2026-10-06T00:00).
        let hour = 1_790_769_600 / 3_600;
        write_pyth(&fx.pyth_dir, &[(hour * 3_600 + 10, "SOL", 100.0)]);
        let swaps = vec![
            swap(hour * 3_600 + 100, "PX", true, 1_000_000_000, 10_000_000),
            // Watermark swap at 2026-10-02T00:00 closes the requested day.
            swap(
                hour * 3_600 + 129_600,
                "PY",
                true,
                1_000_000_000,
                10_000_000,
            ),
        ];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        write_catalog(&fx.catalog, &batches);

        let report = run(&fx.request(Some("2026-09-30"), Some("2026-09-30"))).expect("run");
        assert_eq!(
            report.hours_written, 0,
            "every hour with data is a lockbox hour"
        );
        assert_eq!(report.lockbox_hours_dropped, 12, "12:00 to 23:00 UTC");
        assert_eq!(
            report.hours_empty, 0,
            "every in-range hour is a lockbox hour"
        );
        assert!(!fx.root.join(MINT_TABLE).join("date=2026-09-30").exists());
        assert!(
            !fx.root.join(VENUE_TABLE).exists(),
            "no venue month with only lockbox hours"
        );
        let run_receipt =
            std::fs::read_to_string(fx.root.join(MINT_TABLE).join("receipt-spotdex-hours.json"))
                .expect("run receipt");
        assert!(
            run_receipt.contains("\"rows\":0"),
            "no rows published: {run_receipt}"
        );
    }

    #[test]
    fn requested_unclosed_hours_refuse_the_run() {
        let fx = Fixture::new("unclosed");
        sol_at_100(&fx.pyth_dir, H);
        let swaps = vec![swap(H_SEC + 100, "PX", true, 1_000_000_000, 10_000_000)];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        write_catalog(&fx.catalog, &batches);
        // 2026-10-23T23:00 is 13 hours past the last closed hour.
        let error = run(&fx.request(Some("2026-10-23"), Some("2026-10-23")))
            .expect_err("unclosed requested day must refuse");
        assert!(error.to_string().contains("not closed"), "{error}");
    }

    #[test]
    fn a_rerun_of_the_same_input_skips_and_keeps_bytes() {
        let fx = Fixture::new("idempotent");
        sol_at_100(&fx.pyth_dir, H);
        let swaps = vec![
            swap(H_SEC + 100, "PX", true, 1_000_000_000, 10_000_000),
            swap(H_SEC + 50_000, "PY", true, 1_000_000_000, 10_000_000),
        ];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        write_catalog(&fx.catalog, &batches);

        let first = run(&fx.request(None, None)).expect("first run");
        assert_eq!(first.hours_written, 1);
        let sha = file_hash(&hour_file(&fx.root, H)).expect("hour sha");

        let second = run(&fx.request(None, None)).expect("second run");
        assert_eq!(second.hours_written, 0);
        assert_eq!(second.hours_skipped, 1);
        assert_eq!(
            file_hash(&hour_file(&fx.root, H)).expect("hour sha"),
            sha,
            "same input must keep the same bytes"
        );
        // A third run that drops the receipt rebuilds the hour byte-identically.
        std::fs::remove_file(
            fx.root
                .join(MINT_TABLE)
                .join("receipts")
                .join(format!("{}.json", grid::hour_key(H))),
        )
        .expect("remove receipt");
        let third = run(&fx.request(None, None)).expect("third run");
        assert_eq!(third.hours_written, 1);
        assert_eq!(
            file_hash(&hour_file(&fx.root, H)).expect("hour sha"),
            sha,
            "rebuild must be byte-identical"
        );
    }

    #[test]
    fn a_catalog_sha_mismatch_fails_the_run() {
        let fx = Fixture::new("catalog");
        sol_at_100(&fx.pyth_dir, H);
        let swaps = vec![
            swap(H_SEC + 100, "PX", true, 1_000_000_000, 10_000_000),
            swap(H_SEC + 50_000, "PY", true, 1_000_000_000, 10_000_000),
        ];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        let catalog = write_catalog(&fx.catalog, &batches);
        // Tamper with the recorded sha.
        let mut body: Value =
            serde_json::from_str(&std::fs::read_to_string(&catalog).expect("catalog"))
                .expect("json");
        body["files"][0]["sha256"] = Value::String("0".repeat(64));
        std::fs::write(&catalog, serde_json::to_vec(&body).expect("json")).expect("rewrite");
        let error = run(&fx.request(None, None)).expect_err("tampered catalog must fail");
        assert!(error.to_string().contains("sha256"), "{error}");
    }

    #[test]
    fn a_stale_sol_price_prices_nothing() {
        let fx = Fixture::new("stale");
        // One SOL print 60 hours before the swap hour: beyond the staleness cap.
        write_pyth(&fx.pyth_dir, &[((H - 60) * 3_600 + 10, "SOL", 100.0)]);
        let swaps = vec![
            Swap {
                block_time: H_SEC + 100,
                program: "pumpswap",
                pool: "PX",
                mint_a: "TokenA11111111111111111111111111111111111111",
                mint_b: WSOL_MINT,
                decimals_a: 6,
                decimals_b: 9,
                a_to_b: true,
                amount_in: 1_000_000,
                amount_out: 1_000_000_000,
            },
            swap(H_SEC + 50_000, "PY", true, 1_000_000_000, 10_000_000),
        ];
        let batches = vec![write_batch(&fx.swaps_dir, "0370000000-0370089999", &swaps)];
        write_catalog(&fx.catalog, &batches);
        let report = run(&fx.request(None, None)).expect("run");
        assert_eq!(report.hours_written, 1);
        let rows = read_rows(&hour_file(&fx.root, H));
        assert_eq!(rows.len(), 2, "wSOL and TOKEN_A");
        let row = rows
            .iter()
            .find(|row| row.str("mint") == Some(WSOL_MINT))
            .expect("wsol row");
        assert!(
            row.get("usd_volume").expect("usd").is_null(),
            "stale price prices nothing"
        );
        assert!(
            (row.get("usd_priced")
                .and_then(Value::as_f64)
                .expect("priced"))
            .abs()
                < 1e-12
        );
        let token_a = rows
            .iter()
            .find(|row| row.str("mint") == Some("TokenA11111111111111111111111111111111111111"))
            .expect("token a row");
        assert!(
            token_a.get("usd_volume").expect("usd").is_null(),
            "the input side of a wSOL pair shares the row USD"
        );
        let venue = read_rows(&fx.root.join(VENUE_TABLE).join("month=2026-10.parquet"));
        assert!(
            venue[0].get("sd_program_hhi").expect("hhi").is_null(),
            "no priced USD, no hhi"
        );
        assert!(
            (venue[0]
                .get("sd_log_volume")
                .and_then(Value::as_f64)
                .expect("logv"))
            .abs()
                < 1e-12
        );
    }

    #[test]
    fn memory_caps_are_enforced() {
        assert!(parse_memory_cap("8gb").is_ok());
        assert!(parse_memory_cap("33GB").is_err());
        assert!(parse_memory_cap("8TB").is_err());
    }
}
