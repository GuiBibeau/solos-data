//! U1: the combined store's asset mapping (PLAN.md, inventory/MAPPING.md).
//!
//! `solos-data augment combined mapping` builds `<out>/mapping/assets.csv`, one
//! row per (venue, venue symbol), plus `<out>/mapping/receipt-mapping.json`. It
//! reads local Parquet only (hl-fills-v1 receipted hours, Phoenix fills, Pyth
//! prices, augment asset_contexts) and `config/venues.json`; it never opens a
//! DuckDB database file and never touches the network.
//!
//! Rules (inventory/MAPPING.md): every HL perp and HIP-3 coin is its own row with
//! the `builder:` prefix stripped and kept in `builder`; `@N` spot stays unmapped
//! (`spot_unnamed`); `#N` outcomes never join (`outcome`). The Phoenix candidate
//! of an HL coin is the asset_contexts `phoenix_symbol` when that asset trades on
//! Phoenix, else the equal base name. Every candidate pair gets a price-tracking
//! check over the operator's check days: hourly last price on both venues,
//! median |ln(hl/ph)| over hours both traded; `mapped=true` iff the median is
//! under [`PRICE_THRESHOLD`] and at least [`MIN_HOURS`] hours traded, else
//! `price_divergence` or `thin`. Pyth feeds and Jupiter custodies come from the
//! venues config. Output rows are sorted, so a rerun writes the same bytes.

use crate::augment::hlfills;
use crate::fsutil::{create_dir_all_durable, file_hash, write_durable};
use crate::store::{StoreError, query_rows, sql_string};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use super::catalog;
use super::grid;
use super::lockbox;
use super::receipt;

/// The unit this module runs.
pub const UNIT: &str = "mapping";

/// Bumped on an output-shape change.
pub const UNIT_VERSION: u32 = 1;

/// A pair maps when the median |ln(hl/ph)| over common hours is below this.
pub const PRICE_THRESHOLD: f64 = 0.01;

/// A pair needs at least this many common trading hours for the median to count.
pub const MIN_HOURS: u64 = 24;

/// Unmapped reason for `@N` HL spot coins: no spotMeta names on this box yet.
pub const REASON_SPOT: &str = "spot_unnamed";

/// Unmapped reason for `#N` HL outcome coins.
pub const REASON_OUTCOME: &str = "outcome";

/// Unmapped reason when the price check fails the threshold.
pub const REASON_DIVERGENT: &str = "price_divergence";

/// Unmapped reason when fewer than [`MIN_HOURS`] common hours traded.
pub const REASON_THIN: &str = "thin";

/// Column order of `mapping/assets.csv` (PLAN.md).
pub const COLUMNS: &[&str] = &[
    "asset_id",
    "venue",
    "venue_symbol",
    "builder",
    "coin_class",
    "venue_market_id",
    "first_ts",
    "last_ts",
    "source_of_truth",
    "mapped",
    "reason",
    "median_abs_log_ratio",
    "n_hours",
];

/// One offline mapping run.
pub struct Request<'a> {
    /// Combined store root. `mapping/` and `catalog.json` are written under it.
    pub out: &'a Path,
    /// Lockbox v2 JSON.
    pub lockbox: &'a Path,
    /// Required sha256 of the lockbox bytes.
    pub lockbox_sha256: &'a str,
    /// Operator provenance object, copied verbatim into the receipt.
    pub provenance: &'a Path,
    /// hl-fills-v1 root (watermark.json, train/, forward/).
    pub hl_fills: &'a Path,
    /// Phoenix fills directory (`tables/fills` with `epoch=*/...parquet`).
    pub phoenix_fills: &'a Path,
    /// Pyth prices directories (venues-history and/or live capture).
    pub pyth_prices: &'a [PathBuf],
    /// `config/venues.json` (Pyth feeds, Jupiter custodies).
    pub venues_config: &'a Path,
    /// augment asset_contexts directory (daily `YYYY-MM-DD.parquet`).
    pub asset_contexts: &'a Path,
    /// Three UTC check days, comma separated, each outside the lockbox.
    pub check_days: &'a str,
    /// DuckDB threads (1..=8).
    pub threads: u32,
    /// DuckDB memory limit (at most 32GB).
    pub memory_limit: String,
}

/// The run's outcome, printed by the CLI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// Data rows in assets.csv.
    pub rows: u64,
    /// Rows per venue, alphabetical.
    pub per_venue: BTreeMap<String, u64>,
    /// HL perp + HIP-3 pairs that had a Phoenix candidate.
    pub pairs_checked: u64,
    /// Pairs that mapped.
    pub pairs_mapped: u64,
    /// Pairs demoted for price divergence.
    pub price_divergence: u64,
    /// Pairs demoted as thin.
    pub thin: u64,
    /// `@N` spot rows.
    pub spot_unnamed: u64,
    /// `#N` outcome rows.
    pub outcome: u64,
    /// Excluded lockbox hours recorded in the receipt.
    pub lockbox_hours_dropped: u64,
    /// Sealed outputs.
    pub outputs: Vec<receipt::OutputFile>,
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
        let mut venues = crate::jsonout::Obj::new();
        for (venue, rows) in &self.per_venue {
            venues.set(venue, *rows);
        }
        crate::jsonout::Obj::new()
            .with("command", "augment combined mapping")
            .with("unit", UNIT)
            .with("unitVersion", UNIT_VERSION)
            .with("rows", self.rows)
            .with_obj("venues", venues)
            .with("hlPairsChecked", self.pairs_checked)
            .with("hlPairsMapped", self.pairs_mapped)
            .with("priceDivergence", self.price_divergence)
            .with("thin", self.thin)
            .with("spotUnnamed", self.spot_unnamed)
            .with("outcome", self.outcome)
            .with("lockboxHoursDropped", self.lockbox_hours_dropped)
            .with(
                "outputs",
                self.outputs
                    .iter()
                    .map(|file| {
                        json!({"path": file.path, "rows": file.rows, "bytes": file.bytes, "sha256": file.sha256})
                    })
                    .collect::<Vec<_>>(),
            )
            .to_json()
    }
}

/// One row of `mapping/assets.csv`.
#[derive(Clone, Debug)]
struct AssetRow {
    asset_id: String,
    venue: &'static str,
    venue_symbol: String,
    builder: String,
    coin_class: String,
    venue_market_id: String,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    source_of_truth: String,
    mapped: bool,
    reason: String,
    median_abs_log_ratio: Option<f64>,
    n_hours: Option<u64>,
}

impl AssetRow {
    /// The row's fields in [`COLUMNS`] order.
    fn fields(&self) -> [String; COLUMNS.len()] {
        [
            self.asset_id.clone(),
            self.venue.to_owned(),
            self.venue_symbol.clone(),
            self.builder.clone(),
            self.coin_class.clone(),
            self.venue_market_id.clone(),
            self.first_ts.map_or(String::new(), |v| v.to_string()),
            self.last_ts.map_or(String::new(), |v| v.to_string()),
            self.source_of_truth.clone(),
            if self.mapped { "true" } else { "false" }.to_owned(),
            self.reason.clone(),
            self.median_abs_log_ratio
                .map_or(String::new(), |v| format!("{v:.6}")),
            self.n_hours.map_or(String::new(), |v| v.to_string()),
        ]
    }

    /// The row's CSV line, fields quoted only when needed.
    fn csv(&self) -> String {
        self.fields()
            .iter()
            .map(|field| csv_field(field))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Quote a CSV field only when it has to be quoted (RFC 4180).
fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// Serialize rows to the CSV body: header plus one line per row, `\n` line ends.
fn csv_body(rows: &[AssetRow]) -> String {
    let mut out = String::new();
    out.push_str(&COLUMNS.join(","));
    out.push('\n');
    for row in rows {
        out.push_str(&row.csv());
        out.push('\n');
    }
    out
}

#[derive(Deserialize)]
struct VenuesConfig {
    #[serde(default)]
    venues: Vec<VenueEntry>,
    #[serde(default)]
    pyth: PythSection,
}

#[derive(Deserialize)]
struct VenueEntry {
    name: String,
    #[serde(default)]
    accounts: Vec<VenueAccount>,
}

#[derive(Deserialize)]
struct VenueAccount {
    #[allow(dead_code)]
    address: String,
    kind: String,
    label: String,
}

#[derive(Default, Deserialize)]
struct PythSection {
    #[serde(default)]
    feeds: Vec<PythFeed>,
}

#[derive(Deserialize)]
struct PythFeed {
    symbol: String,
    /// Display market name; 27 of the 39 feeds lack it and fall back to the
    /// base symbol (slice 0).
    #[serde(default)]
    pyth: Option<String>,
    #[serde(rename = "feedId")]
    feed_id: String,
    #[allow(dead_code)]
    account: String,
}

impl PythFeed {
    /// The venue symbol: the Pyth market name when the config has one, else
    /// the base symbol.
    fn venue_symbol(&self) -> &str {
        self.pyth.as_deref().unwrap_or(self.symbol.as_str())
    }
}

#[derive(Deserialize)]
struct HlWatermark {
    schema: String,
    hours: BTreeSet<String>,
}

/// Universe entry of one venue symbol.
#[derive(Clone, Debug)]
struct Universe {
    first_ts: i64,
    last_ts: i64,
}

/// Run the mapping unit.
///
/// # Errors
///
/// Any input is missing or malformed, the lockbox sha does not match, a check
/// day falls inside the lockbox, or an output fails its read-back check. A
/// failure never writes the receipt or the catalog.
pub fn run(request: &Request<'_>) -> Result<Report, StoreError> {
    let started = grid::now_rfc3339();
    if !(1..=8).contains(&request.threads) {
        return Err(StoreError::Check("--threads must be 1..=8".into()));
    }
    let memory_limit = parse_memory_cap(&request.memory_limit)?;
    let check_days = parse_check_days(request.check_days)?;
    let provenance = load_provenance(request.provenance)?;
    let config = load_venues_config(request.venues_config)?;
    let lockbox = lockbox::load(request.lockbox, request.lockbox_sha256)?;
    let lockbox_hours = lockbox.excluded_hours()?;
    lockbox.assert_v2_shape(&lockbox_hours)?;
    lockbox::refuse_check_days_inside(&lockbox_hours, &check_days)?;

    create_dir_all_durable(request.out)?;
    let _guard = lock_output(request.out)?;
    let _temp = TempClean::new(request.out);
    let conn = open_reader(request.out, request.threads, &memory_limit)?;

    // Inputs: file lists are walked before any query, so the receipt's watermarks
    // describe exactly what the queries read.
    let hl = HlInput::scan(&conn, request.hl_fills, &check_days)?;
    let phoenix_files = walk_parquet(request.phoenix_fills)?;
    if phoenix_files.is_empty() {
        return Err(StoreError::Check(
            "--phoenix-fills has no parquet files".into(),
        ));
    }
    let phoenix = scan_phoenix(&conn, request.phoenix_fills, &phoenix_files, &check_days)?;
    let (context_files, contexts) = scan_asset_contexts(&conn, request.asset_contexts)?;
    let (pyth_files, pyth) = scan_pyth(&conn, request.pyth_prices)?;
    if config.pyth.feeds.is_empty() {
        return Err(StoreError::Check("venues config has no pyth feeds".into()));
    }

    let mut rows = build_rows(&hl, &phoenix, &contexts, &pyth, &config);
    rows.sort_by(|a, b| (a.venue, &a.venue_symbol).cmp(&(b.venue, &b.venue_symbol)));
    let body = csv_body(&rows);

    // Seal assets.csv before anything else: the receipt and the catalog only
    // follow when the output is durable and its read-back check passed.
    let final_path = request.out.join("mapping").join("assets.csv");
    if let Some(parent) = final_path.parent() {
        create_dir_all_durable(parent)?;
    }
    write_durable(&final_path, body.as_bytes())?;
    let bytes = std::fs::metadata(&final_path)?.len();
    let sha = file_hash(&final_path)?;
    verify_written(&final_path, rows.len())?;
    let output = receipt::OutputFile {
        path: "mapping/assets.csv".to_owned(),
        rows: rows.len() as u64,
        bytes,
        sha256: sha,
    };

    let inputs = inputs_value(&hl, &phoenix_files, &context_files, &pyth_files, request);
    let finished = grid::now_rfc3339();
    let counts = count_per_venue(&rows);
    let counters = PairCounters::of(&rows);
    receipt::write(
        request.out,
        &receipt::Receipt {
            unit: UNIT.to_owned(),
            unit_version: UNIT_VERSION,
            rows: output.rows,
            outputs: vec![output.clone()],
            inputs,
            lockbox_sha256: request.lockbox_sha256.trim().to_ascii_lowercase(),
            lockbox_hours_dropped: lockbox_hours.len() as u64,
            config_sha256: config_hash(&check_days),
            provenance,
            started_utc: started,
            finished_utc: finished,
        },
    )?;

    let mut cat = catalog::load(request.out)?;
    cat.upsert(
        catalog::FileEntry {
            path: output.path.clone(),
            rows: output.rows,
            bytes: output.bytes,
            sha256: output.sha256.clone(),
            unit: UNIT.to_owned(),
            unit_version: UNIT_VERSION,
        },
        UNIT,
        json!({
            "hl_fills_hours": hl.hours.len(),
            "hl_fills_hours_sha256": hl.hours_sha256,
            "check_days": check_days.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "lockbox_sha256": request.lockbox_sha256.trim().to_ascii_lowercase(),
            "lockbox_hours_dropped": lockbox_hours.len() as u64,
        }),
    );
    cat.save(request.out)?;

    Ok(Report {
        rows: output.rows,
        per_venue: counts,
        pairs_checked: counters.pairs_checked,
        pairs_mapped: counters.pairs_mapped,
        price_divergence: counters.price_divergence,
        thin: counters.thin,
        spot_unnamed: counters.spot_unnamed,
        outcome: counters.outcome,
        lockbox_hours_dropped: lockbox_hours.len() as u64,
        outputs: vec![output],
    })
}

/// DuckDB memory cap: the shared parser, plus the plan's 32GB ceiling.
fn parse_memory_cap(text: &str) -> Result<String, StoreError> {
    let limit = hlfills::parse_memory_limit(text)?;
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

/// Outcome counters over the HL rows.
#[derive(Default)]
struct PairCounters {
    pairs_checked: u64,
    pairs_mapped: u64,
    price_divergence: u64,
    thin: u64,
    spot_unnamed: u64,
    outcome: u64,
}

impl PairCounters {
    fn of(rows: &[AssetRow]) -> Self {
        let mut counters = Self::default();
        for row in rows {
            if row.venue != "hl" {
                continue;
            }
            match row.reason.as_str() {
                REASON_SPOT => counters.spot_unnamed += 1,
                REASON_OUTCOME => counters.outcome += 1,
                REASON_DIVERGENT => {
                    counters.pairs_checked += 1;
                    counters.price_divergence += 1;
                }
                REASON_THIN => {
                    counters.pairs_checked += 1;
                    counters.thin += 1;
                }
                _ => {
                    if row.mapped {
                        counters.pairs_checked += 1;
                        counters.pairs_mapped += 1;
                    }
                }
            }
        }
        counters
    }
}

fn count_per_venue(rows: &[AssetRow]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for row in rows {
        *counts.entry(row.venue.to_owned()).or_default() += 1;
    }
    counts
}

/// Parse `YYYY-MM-DD,YYYY-MM-DD,YYYY-MM-DD`: exactly three distinct UTC dates.
fn parse_check_days(text: &str) -> Result<Vec<NaiveDate>, StoreError> {
    let mut days = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        days.push(grid::parse_date(part)?);
    }
    days.sort();
    let unique = days.iter().collect::<BTreeSet<_>>();
    if days.len() != 3 || unique.len() != 3 {
        return Err(StoreError::Check(
            "--check-days needs exactly three distinct UTC days, e.g. 2026-10-15,2026-10-16,2026-10-17"
                .into(),
        ));
    }
    Ok(days)
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

fn load_venues_config(path: &Path) -> Result<VenuesConfig, StoreError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| StoreError::Check(format!("--venues-config: cannot read: {error}")))?;
    serde_json::from_str(&text)
        .map_err(|error| StoreError::Check(format!("--venues-config: bad JSON: {error}")))
}

/// SHA-256 over the unit's semantic config: the pin that must move on purpose.
fn config_hash(check_days: &[NaiveDate]) -> String {
    let body = json!({
        "unit": UNIT,
        "unitVersion": UNIT_VERSION,
        "columns": COLUMNS,
        "sort": ["venue", "venue_symbol"],
        "priceThreshold": PRICE_THRESHOLD,
        "minHours": MIN_HOURS,
        "checkDays": check_days.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
        "lockbox": "v2 weeks with guard hours, dropped from every table",
        "seeds": ["asset_contexts phoenix_symbol", "base name"],
    });
    crate::fsutil::sha256_hex(body.to_string().as_bytes())
}

/// Everything the run read, for the receipt. Relative paths and sizes only; no
/// absolute machine paths.
fn inputs_value(
    hl: &HlInput,
    phoenix_files: &[FileRef],
    contexts: &[FileRef],
    pyth_files: &[FileRef],
    request: &Request<'_>,
) -> Value {
    json!({
        "check_days": hl.check_days.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
        "hl_fills": {
            "schema": hl.schema,
            "hours": hl.hours.len(),
            "hours_sha256": hl.hours_sha256,
            "files": hl.files.len(),
            "files_sha256": file_list_sha256(&hl.files),
        },
        "phoenix_fills": {
            "files": phoenix_files.len(),
            "files_sha256": file_list_sha256(phoenix_files),
            "root": root_name(request.phoenix_fills),
        },
        "pyth_prices": {
            "files": pyth_files.len(),
            "files_sha256": file_list_sha256(pyth_files),
        },
        "asset_contexts": {
            "files": contexts.len(),
            "files_sha256": file_list_sha256(contexts),
            "root": root_name(request.asset_contexts),
        },
        "venues_config_sha256": file_hash(request.venues_config).unwrap_or_default(),
    })
}

/// The last path component of an input root, so receipts can tell inputs apart
/// without carrying machine paths.
fn root_name(path: &Path) -> String {
    path.file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().to_string())
}

/// SHA-256 of the sorted `relative size` lines of a file list.
fn file_list_sha256(files: &[FileRef]) -> String {
    let mut text = String::new();
    for file in files {
        text.push_str(&file.relative);
        text.push(' ');
        text.push_str(&file.size.to_string());
        text.push('\n');
    }
    crate::fsutil::sha256_hex(text.as_bytes())
}

/// A walked input file.
#[derive(Clone, Debug)]
struct FileRef {
    relative: String,
    size: u64,
}

/// Walk `root` recursively for Parquet files, sorted by relative path.
fn walk_parquet(root: &Path) -> Result<Vec<FileRef>, StoreError> {
    let mut out = Vec::new();
    walk_dir(root, root, &mut out)?;
    out.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(out)
}

fn walk_dir(root: &Path, dir: &Path, out: &mut Vec<FileRef>) -> Result<(), StoreError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| StoreError::Check(format!("cannot read {}: {error}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| StoreError::Check(format!("walk: {e}")))?;
        let path = entry.path();
        if path.is_dir() {
            walk_dir(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            let size = std::fs::metadata(&path)?.len();
            out.push(FileRef {
                relative: path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/"),
                size,
            });
        }
    }
    Ok(())
}

/// Absolute paths of a file list, for `read_parquet`.
fn absolute(root: &Path, files: &[FileRef]) -> Vec<String> {
    files
        .iter()
        .map(|file| root.join(&file.relative).to_string_lossy().to_string())
        .collect()
}

/// A `read_parquet([...])` table reference with an explicit file list. An
/// explicit list (not a glob) is the point: only files the run walked are read.
#[track_caller]
fn parquet_list(files: &[String]) -> Result<String, StoreError> {
    if files.is_empty() {
        let location = std::panic::Location::caller();
        return Err(StoreError::Check(format!(
            "no parquet files to read at {location}"
        )));
    }
    let list = files
        .iter()
        .map(|path| sql_string(path))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("read_parquet([{list}])"))
}

/// A single-file table reference.
fn parquet_one(root: &Path, file: &FileRef) -> Result<String, StoreError> {
    parquet_list(&[root.join(&file.relative).to_string_lossy().to_string()])
}

/// The hl-fills-v1 input: watermark, receipted files, universe, check-day prices.
struct HlInput {
    schema: String,
    check_days: Vec<NaiveDate>,
    hours: BTreeSet<String>,
    hours_sha256: String,
    files: Vec<FileRef>,
    /// coin, (coin_class, first/last fill second).
    universe: BTreeMap<String, (String, Universe)>,
    /// coin, hour start, last price of the hour (check days only).
    prices: BTreeMap<String, BTreeMap<i64, f64>>,
}

impl HlInput {
    /// Read the watermark, check every receipted hour's files exist, and scan
    /// the universe plus the check-day hourly last prices.
    fn scan(
        conn: &duckdb::Connection,
        root: &Path,
        check_days: &[NaiveDate],
    ) -> Result<Self, StoreError> {
        let path = root.join("watermark.json");
        let text = std::fs::read_to_string(&path)
            .map_err(|error| StoreError::Check(format!("hl-fills watermark: {error}")))?;
        let watermark: HlWatermark = serde_json::from_str(&text)
            .map_err(|error| StoreError::Check(format!("hl-fills watermark: {error}")))?;
        if watermark.schema != hlfills::SCHEMA {
            return Err(StoreError::Check(format!(
                "hl-fills watermark schema is {}, expected {}",
                watermark.schema,
                hlfills::SCHEMA
            )));
        }
        let mut hours_sha = String::new();
        for hour in &watermark.hours {
            hours_sha.push_str(hour);
            hours_sha.push('\n');
        }
        let hours_sha256 = crate::fsutil::sha256_hex(hours_sha.as_bytes());
        let files = watermark_files(root, &watermark.hours)?;
        let table = parquet_list(&absolute(root, &files))?;
        let rows = query_rows(
            conn,
            &format!(
                "SELECT coin, coin_class, min(time_ms) AS first_ms, max(time_ms) AS last_ms
                 FROM {table} WHERE coin IS NOT NULL GROUP BY 1, 2"
            ),
            &[],
        )?;
        let mut universe = BTreeMap::new();
        for row in rows {
            let coin = row
                .str("coin")
                .ok_or_else(|| StoreError::Check("hl universe: null coin".into()))?
                .to_owned();
            let class = row.str("coin_class").unwrap_or_default().to_owned();
            let first_ms = row
                .int("first_ms")
                .ok_or_else(|| StoreError::Check(format!("hl universe {coin}: null first")))?;
            let last_ms = row
                .int("last_ms")
                .ok_or_else(|| StoreError::Check(format!("hl universe {coin}: null last")))?;
            if universe.contains_key(&coin) {
                return Err(StoreError::Check(format!(
                    "hl universe: coin {coin} has two classes"
                )));
            }
            universe.insert(
                coin.clone(),
                (
                    class,
                    Universe {
                        first_ts: first_ms.div_euclid(1000),
                        last_ts: last_ms.div_euclid(1000),
                    },
                ),
            );
        }
        // Hourly last price over the check days only: those hours' files are a
        // small subset of the tree.
        let check_keys: BTreeSet<String> = check_days
            .iter()
            .map(|day| day.format("%Y%m%d").to_string())
            .collect();
        let check_files: Vec<FileRef> = files
            .iter()
            .filter(|file| {
                let name = file.relative.rsplit('/').next().unwrap_or_default();
                let stem = name.strip_suffix(".parquet").unwrap_or(name);
                let stem = stem.strip_prefix("hour=").unwrap_or(stem);
                stem.len() >= 8 && check_keys.contains(&stem[..8])
            })
            .cloned()
            .collect();
        let prices = hourly_prices(
            conn,
            &parquet_list(&absolute(root, &check_files))?,
            "coin IS NOT NULL AND px IS NOT NULL AND px > 0",
            "coin",
            "time_ms",
            "px",
            "(time_ms // 3600000) * 3600",
        )?;
        Ok(Self {
            schema: watermark.schema,
            check_days: check_days.to_vec(),
            hours: watermark.hours,
            hours_sha256,
            files,
            universe,
            prices,
        })
    }
}

/// The receipted Parquet files of the watermark hours: `train/` and `forward/`
/// candidates for every hour; a missing file is an input-tree error.
fn watermark_files(root: &Path, hours: &BTreeSet<String>) -> Result<Vec<FileRef>, StoreError> {
    let mut files = Vec::new();
    for hour in hours {
        let (date, hour_of_day) = hour
            .split_once('/')
            .ok_or_else(|| StoreError::Check(format!("hl watermark hour '{hour}' is malformed")))?;
        if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(StoreError::Check(format!(
                "hl watermark hour '{hour}' is malformed"
            )));
        }
        let date_text = format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..]);
        for corpus in ["train", "forward"] {
            let relative = format!("{corpus}/date={date_text}/hour={date}-{hour_of_day}.parquet");
            let path = root.join(&relative);
            if path.is_file() {
                let size = std::fs::metadata(&path)?.len();
                files.push(FileRef { relative, size });
            }
        }
    }
    if files.is_empty() {
        return Err(StoreError::Check(
            "hl-fills watermark has hours but no Parquet files exist under train/ or forward/"
                .into(),
        ));
    }
    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
}

/// Hourly last price per symbol. The struct max makes the pick deterministic:
/// the later timestamp wins, and at equal timestamps the larger price does; the
/// filter already removed nulls, so no comparison field can be null.
fn hourly_prices(
    conn: &duckdb::Connection,
    table: &str,
    filter: &str,
    symbol_column: &str,
    time_column: &str,
    price_expression: &str,
    hour_expression: &str,
) -> Result<BTreeMap<String, BTreeMap<i64, f64>>, StoreError> {
    let rows = query_rows(
        conn,
        &format!(
            "SELECT {symbol_column} AS symbol, {hour_expression} AS hour_s,
                    max({{'t': {time_column}, 'p': {price_expression}}})['p'] AS px
             FROM {table}
             WHERE {filter}
             GROUP BY 1, 2"
        ),
        &[],
    )?;
    let mut prices: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    for row in rows {
        let symbol = row
            .str("symbol")
            .ok_or_else(|| StoreError::Check("prices: null symbol".into()))?
            .to_owned();
        let hour = row
            .int("hour_s")
            .ok_or_else(|| StoreError::Check("prices: null hour".into()))?;
        let px = row
            .get("px")
            .and_then(Value::as_f64)
            .ok_or_else(|| StoreError::Check("prices: null px".into()))?;
        prices.entry(symbol).or_default().insert(hour, px);
    }
    Ok(prices)
}

/// The Phoenix fills input: universe (per asset symbol) and check-day prices.
/// Price is `price_ticks * tick_size * 10^(base_lot_decimals - quote_lot_decimals)`
/// (slice 0 measured; validated against HL for BTC and SOL).
struct PhoenixInput {
    universe: BTreeMap<String, Universe>,
    /// asset symbol, Phoenix numeric market id when every fill agrees.
    market_id: BTreeMap<String, Option<String>>,
    prices: BTreeMap<String, BTreeMap<i64, f64>>,
}

fn scan_phoenix(
    conn: &duckdb::Connection,
    root: &Path,
    files: &[FileRef],
    check_days: &[NaiveDate],
) -> Result<PhoenixInput, StoreError> {
    let table = parquet_list(&absolute(root, files))?;
    let rows = query_rows(
        conn,
        &format!(
            "SELECT asset_symbol, min(block_time) AS first_s, max(block_time) AS last_s,
                    min(asset_id) AS id_min, max(asset_id) AS id_max
             FROM {table} WHERE asset_symbol IS NOT NULL GROUP BY 1"
        ),
        &[],
    )?;
    let mut universe = BTreeMap::new();
    let mut market_id = BTreeMap::new();
    for row in rows {
        let symbol = row
            .str("asset_symbol")
            .ok_or_else(|| StoreError::Check("phoenix universe: null symbol".into()))?
            .to_owned();
        let first = row
            .int("first_s")
            .ok_or_else(|| StoreError::Check(format!("phoenix universe {symbol}: null first")))?;
        let last = row
            .int("last_s")
            .ok_or_else(|| StoreError::Check(format!("phoenix universe {symbol}: null last")))?;
        let id = match (row.int("id_min"), row.int("id_max")) {
            (Some(min), Some(max)) if min == max => Some(min.to_string()),
            _ => None,
        };
        universe.insert(
            symbol.clone(),
            Universe {
                first_ts: first,
                last_ts: last,
            },
        );
        market_id.insert(symbol, id);
    }
    let first_day = grid::day_start_secs(check_days[0]);
    let last_day = grid::day_start_secs(check_days[check_days.len() - 1]) + 86_400;
    let price_expression = "price_ticks::DOUBLE * tick_size::DOUBLE
        * power(10::DOUBLE, (base_lot_decimals - quote_lot_decimals)::DOUBLE)";
    let prices = hourly_prices(
        conn,
        &table,
        &format!(
            "asset_symbol IS NOT NULL AND block_time >= {first_day} AND block_time < {last_day}
             AND {price_expression} IS NOT NULL AND {price_expression} > 0"
        ),
        "asset_symbol",
        "block_time",
        price_expression,
        "(block_time // 3600) * 3600",
    )?;
    Ok(PhoenixInput {
        universe,
        market_id,
        prices,
    })
}

/// The asset_contexts seed: latest non-null `phoenix_symbol` per HL symbol,
/// files processed in ascending date order so a later day wins.
fn scan_asset_contexts(
    conn: &duckdb::Connection,
    root: &Path,
) -> Result<(Vec<FileRef>, BTreeMap<String, String>), StoreError> {
    let files = walk_parquet(root)?;
    let mut seeds: BTreeMap<String, String> = BTreeMap::new();
    for file in &files {
        let table = parquet_one(root, file)?;
        let rows = query_rows(
            conn,
            &format!(
                "SELECT symbol, min(phoenix_symbol) AS phoenix_symbol
                 FROM {table}
                 WHERE symbol IS NOT NULL AND phoenix_symbol IS NOT NULL
                 GROUP BY 1"
            ),
            &[],
        )?;
        for row in rows {
            let symbol = row.str("symbol").unwrap_or_default().to_owned();
            let phoenix = row.str("phoenix_symbol").unwrap_or_default().to_owned();
            if !symbol.is_empty() && !phoenix.is_empty() {
                seeds.insert(symbol, phoenix);
            }
        }
    }
    Ok((files, seeds))
}

/// Pyth price history across all given directories, merged per feed symbol.
struct PythInput {
    universe: BTreeMap<String, Universe>,
}

fn scan_pyth(
    conn: &duckdb::Connection,
    dirs: &[PathBuf],
) -> Result<(Vec<FileRef>, PythInput), StoreError> {
    if dirs.is_empty() {
        return Err(StoreError::Check(
            "--pyth-prices needs at least one directory".into(),
        ));
    }
    let mut all: Vec<FileRef> = Vec::new();
    let mut universe: BTreeMap<String, Universe> = BTreeMap::new();
    for (index, dir) in dirs.iter().enumerate() {
        let root_files = walk_parquet(dir)?;
        if root_files.is_empty() {
            continue;
        }
        for file in &root_files {
            all.push(FileRef {
                relative: format!("pyth{}/{}", index, file.relative),
                size: file.size,
            });
        }
        let table = parquet_list(&absolute(dir, &root_files))?;
        let rows = query_rows(
            conn,
            &format!(
                "SELECT symbol, min(publish_time) AS first_s, max(publish_time) AS last_s
                 FROM {table} WHERE symbol IS NOT NULL GROUP BY 1"
            ),
            &[],
        )?;
        for row in rows {
            let symbol = row.str("symbol").unwrap_or_default().to_owned();
            let (Some(first), Some(last)) = (row.int("first_s"), row.int("last_s")) else {
                continue;
            };
            let entry = universe.entry(symbol).or_insert(Universe {
                first_ts: i64::MAX,
                last_ts: i64::MIN,
            });
            entry.first_ts = entry.first_ts.min(first);
            entry.last_ts = entry.last_ts.max(last);
        }
    }
    all.sort_by(|a, b| a.relative.cmp(&b.relative));
    if all.is_empty() {
        return Err(StoreError::Check(
            "--pyth-prices has no parquet files".into(),
        ));
    }
    Ok((all, PythInput { universe }))
}

/// Build every CSV row: HL coins (with pair checks), Phoenix assets, Pyth feeds,
/// Jupiter custodies.
fn build_rows(
    hl: &HlInput,
    phoenix: &PhoenixInput,
    contexts: &BTreeMap<String, String>,
    pyth: &PythInput,
    config: &VenuesConfig,
) -> Vec<AssetRow> {
    let mut rows = Vec::new();
    // Outcome of each checked pair, kept per Phoenix asset for the Phoenix rows.
    let mut pair_outcomes: BTreeMap<String, Vec<PairOutcome>> = BTreeMap::new();
    let empty_prices: BTreeMap<i64, f64> = BTreeMap::new();

    for (coin, (class, span)) in &hl.universe {
        let (builder, base) = split_builder(coin, class);
        let row = match class.as_str() {
            "spot" => AssetRow {
                asset_id: String::new(),
                venue: "hl",
                venue_symbol: coin.clone(),
                builder: String::new(),
                coin_class: class.clone(),
                venue_market_id: String::new(),
                first_ts: Some(span.first_ts),
                last_ts: Some(span.last_ts),
                source_of_truth: String::new(),
                mapped: false,
                reason: REASON_SPOT.to_owned(),
                median_abs_log_ratio: None,
                n_hours: None,
            },
            "outcome" => AssetRow {
                asset_id: String::new(),
                venue: "hl",
                venue_symbol: coin.clone(),
                builder: String::new(),
                coin_class: class.clone(),
                venue_market_id: String::new(),
                first_ts: Some(span.first_ts),
                last_ts: Some(span.last_ts),
                source_of_truth: String::new(),
                mapped: false,
                reason: REASON_OUTCOME.to_owned(),
                median_abs_log_ratio: None,
                n_hours: None,
            },
            _ => {
                let candidate = phoenix_candidate(coin, &base, contexts, phoenix);
                let seed_column = candidate
                    .as_ref()
                    .map_or(String::new(), |(_, seed)| seed_text(seed));
                let (mapped, reason, median, n_hours) = match &candidate {
                    None => (false, String::new(), None, None),
                    Some((asset, seed)) => {
                        let (n, med) = price_check(
                            hl.prices.get(coin).unwrap_or(&empty_prices),
                            phoenix.prices.get(asset).unwrap_or(&empty_prices),
                        );
                        let (mapped, reason) = if n < MIN_HOURS {
                            (false, REASON_THIN.to_owned())
                        } else if med.is_some_and(|value| value >= PRICE_THRESHOLD) {
                            (false, REASON_DIVERGENT.to_owned())
                        } else {
                            (true, String::new())
                        };
                        pair_outcomes
                            .entry(asset.clone())
                            .or_default()
                            .push(PairOutcome {
                                hl_coin: coin.clone(),
                                seed: *seed,
                                mapped,
                                reason: reason.clone(),
                                median: med,
                                n_hours: n,
                            });
                        (mapped, reason, med, Some(n))
                    }
                };
                AssetRow {
                    asset_id: base.clone(),
                    venue: "hl",
                    venue_symbol: coin.clone(),
                    builder,
                    coin_class: class.clone(),
                    venue_market_id: String::new(),
                    first_ts: Some(span.first_ts),
                    last_ts: Some(span.last_ts),
                    source_of_truth: seed_column,
                    mapped,
                    reason,
                    median_abs_log_ratio: median,
                    n_hours,
                }
            }
        };
        rows.push(row);
    }

    // Phoenix assets: mapped when any HL pair for the asset passed.
    for (symbol, span) in &phoenix.universe {
        let outcomes = pair_outcomes.get(symbol);
        let mapped = outcomes.is_some_and(|pairs| pairs.iter().any(|pair| pair.mapped));
        let reason = if mapped {
            String::new()
        } else {
            outcomes
                .map(|pairs| {
                    pairs
                        .iter()
                        .filter(|pair| !pair.mapped && !pair.reason.is_empty())
                        .map(|pair| pair.reason.clone())
                        .min()
                        .unwrap_or_default()
                })
                .unwrap_or_default()
        };
        // Representative pair: the one with the most common hours, ties by the
        // lexicographically smallest HL coin.
        let representative = outcomes.and_then(|pairs| {
            pairs
                .iter()
                .max_by(|a, b| a.n_hours.cmp(&b.n_hours).then(b.hl_coin.cmp(&a.hl_coin)))
        });
        rows.push(AssetRow {
            asset_id: symbol.clone(),
            venue: "phoenix",
            venue_symbol: symbol.clone(),
            builder: String::new(),
            coin_class: "spot".to_owned(),
            venue_market_id: phoenix
                .market_id
                .get(symbol)
                .cloned()
                .flatten()
                .unwrap_or_default(),
            first_ts: Some(span.first_ts),
            last_ts: Some(span.last_ts),
            source_of_truth: representative.map_or(String::new(), |pair| seed_text(&pair.seed)),
            mapped,
            reason,
            median_abs_log_ratio: representative.and_then(|pair| pair.median),
            n_hours: representative.map(|pair| pair.n_hours),
        });
    }

    // Pyth feeds: venue_symbol is the Pyth market symbol when the config has
    // one (else the base symbol), asset_id the config's base symbol, market id
    // the feed id.
    for feed in &config.pyth.feeds {
        let span = pyth.universe.get(&feed.symbol);
        rows.push(AssetRow {
            asset_id: feed.symbol.clone(),
            venue: "pyth",
            venue_symbol: feed.venue_symbol().to_owned(),
            builder: String::new(),
            coin_class: "price".to_owned(),
            venue_market_id: feed.feed_id.clone(),
            first_ts: span.map(|s| s.first_ts),
            last_ts: span.map(|s| s.last_ts),
            source_of_truth: "venues_config".to_owned(),
            mapped: true,
            reason: String::new(),
            median_abs_log_ratio: None,
            n_hours: None,
        });
    }

    // Jupiter custodies: the config's custody label is the asset.
    for venue in &config.venues {
        if venue.name != "jupiter" {
            continue;
        }
        for account in &venue.accounts {
            if account.kind != "custody" {
                continue;
            }
            rows.push(AssetRow {
                asset_id: account.label.clone(),
                venue: "jup",
                venue_symbol: account.label.clone(),
                builder: String::new(),
                coin_class: "perp".to_owned(),
                venue_market_id: account.address.clone(),
                first_ts: None,
                last_ts: None,
                source_of_truth: "venues_config".to_owned(),
                mapped: true,
                reason: String::new(),
                median_abs_log_ratio: None,
                n_hours: None,
            });
        }
    }
    rows
}

/// One HL coin against one Phoenix asset.
struct PairOutcome {
    hl_coin: String,
    seed: Seed,
    mapped: bool,
    reason: String,
    median: Option<f64>,
    n_hours: u64,
}

/// Where a pair came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seed {
    /// The augment asset_contexts `phoenix_symbol` column.
    Contexts,
    /// Equal base name on both venues.
    BaseName,
}

fn seed_text(seed: &Seed) -> String {
    match seed {
        Seed::Contexts => "asset_contexts".to_owned(),
        Seed::BaseName => "base_name".to_owned(),
    }
}

/// Split an HL coin into (builder, base). Only HIP-3 (`builder:SYMBOL`) has a
/// builder; `@`/`#` classes never reach here.
fn split_builder(coin: &str, class: &str) -> (String, String) {
    if class == "hip3"
        && let Some((builder, base)) = coin.split_once(':')
    {
        return (builder.to_owned(), base.to_owned());
    }
    (String::new(), coin.to_owned())
}

/// The Phoenix candidate of an HL coin: the seeded `phoenix_symbol` when that
/// asset trades on Phoenix, else the equal base name.
fn phoenix_candidate(
    coin: &str,
    base: &str,
    contexts: &BTreeMap<String, String>,
    phoenix: &PhoenixInput,
) -> Option<(String, Seed)> {
    if let Some(seeded) = contexts.get(coin)
        && phoenix.universe.contains_key(seeded.as_str())
    {
        return Some((seeded.clone(), Seed::Contexts));
    }
    if phoenix.universe.contains_key(base) {
        return Some((base.to_owned(), Seed::BaseName));
    }
    let _ = coin;
    None
}

/// Median |ln(hl/ph)| over the hours where both traded, with the hour count.
fn price_check(hl: &BTreeMap<i64, f64>, ph: &BTreeMap<i64, f64>) -> (u64, Option<f64>) {
    let mut ratios = Vec::new();
    for (hour, hpx) in hl {
        if let Some(ppx) = ph.get(hour)
            && *hpx > 0.0
            && *ppx > 0.0
        {
            let ratio = (hpx / ppx).ln().abs();
            if ratio.is_finite() {
                ratios.push(ratio);
            }
        }
    }
    let n = ratios.len() as u64;
    (n, median(&mut ratios))
}

/// Median of a sample; the sample is sorted in place.
fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    Some(if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    })
}

/// Read the sealed CSV back: the header must match and the data-row count must
/// equal what was written.
fn verify_written(path: &Path, rows: usize) -> Result<(), StoreError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| StoreError::Check(format!("assets.csv read-back: {error}")))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != COLUMNS.join(",") {
        return Err(StoreError::Check(
            "assets.csv header does not match the schema".into(),
        ));
    }
    let count = lines.count();
    if count != rows {
        return Err(StoreError::Check(format!(
            "assets.csv read-back has {count} data rows, wrote {rows}"
        )));
    }
    Ok(())
}

/// An exclusive process lock on the combined root.
pub(crate) fn lock_output(out: &Path) -> Result<nix::fcntl::Flock<std::fs::File>, StoreError> {
    let path = out.join("combined.lock");
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
            Err(StoreError::Check("combined output already owned".into()))
        }
        Err((_, errno)) => Err(std::io::Error::from(errno).into()),
    }
}

/// Open the pinned in-memory reader: the caller's thread and memory caps, spill
/// dir under the output root.
fn open_reader(
    out: &Path,
    threads: u32,
    memory_limit: &str,
) -> Result<duckdb::Connection, StoreError> {
    let conn = crate::store::memory_connection(&threads.to_string(), memory_limit)?;
    crate::decoder::parquet::require_connection_duckdb(&conn)?;
    let temp = out.join(".tmp-duckdb");
    let _ = std::fs::remove_dir_all(&temp);
    create_dir_all_durable(&temp)?;
    conn.execute_batch(&format!(
        "SET threads = {threads}; SET memory_limit = {}; SET temp_directory = {};
         SET max_temp_directory_size = '20GB'",
        sql_string(memory_limit),
        sql_string(&temp.to_string_lossy())
    ))?;
    Ok(conn)
}

/// The `.tmp-duckdb` spill dir is removed when the run ends.
struct TempClean(PathBuf);

impl TempClean {
    fn new(out: &Path) -> Self {
        Self(out.join(".tmp-duckdb"))
    }
}

impl Drop for TempClean {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
