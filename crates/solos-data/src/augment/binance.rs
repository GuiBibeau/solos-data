//! Binance USD-M futures public dumps (`data.binance.vision`): one zip of one CSV per symbol and
//! day (or month for funding), listed through the bucket's S3 index, verified against the
//! published `.CHECKSUM`, converted to Parquet and deleted. A file is final once published, so
//! the ledger's `files` table is the progress record and the listing starts after the newest
//! registered file. A period the venue skipped before that file is a hole the listing never
//! revisits, so each run also asks directly for the periods missing between the oldest and the
//! newest registered file, marks the ones still missing after three days as absent, and asks
//! again on the schedule of `absentReprobe` (ADR-0024).

use super::absent::{self, AbsentSet, Counts, Due};
use super::config::{AbsentReprobe, Binance, Symbol};
use super::dumps::maybe_unpublished;
use super::http::{HttpError, with_query};
use super::ledger::{get_progress, register, set_progress};
use super::parquet::{Target, read_csv, staging_path, write_parquet};
use super::periods::{Granularity, Period};
use super::series::{Ctx, Outcome, progress_key};
use crate::fsutil::sha256_hex;
use crate::jsonout::{Obj, log, safe_error};
use crate::store::StoreError;
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::io::Read;

/// One dump dataset: its path shape and its CSV columns.
pub struct Dataset {
    /// Name as Binance spells it.
    pub name: &'static str,
    /// Day or month files.
    pub granularity: Granularity,
    /// Kline datasets carry the interval as one more path element.
    pub interval: Option<&'static str>,
    /// CSV columns in order, with DuckDB types.
    pub columns: &'static [(&'static str, &'static str)],
    /// The typed projection written to Parquet.
    pub select: &'static str,
}

const KLINE_COLUMNS: &[(&str, &str)] = &[
    ("open_time", "BIGINT"),
    ("open", "DOUBLE"),
    ("high", "DOUBLE"),
    ("low", "DOUBLE"),
    ("close", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("close_time", "BIGINT"),
    ("quote_volume", "DOUBLE"),
    ("count", "BIGINT"),
    ("taker_buy_volume", "DOUBLE"),
    ("taker_buy_quote_volume", "DOUBLE"),
    ("ignore", "VARCHAR"),
];
const KLINE_SELECT: &str = "open_time AS open_time_ms, make_timestamp(open_time * 1000) AS ts, open, high, low, close, volume,
    close_time AS close_time_ms, quote_volume, count AS trades, taker_buy_volume, taker_buy_quote_volume";

const METRICS_COLUMNS: &[(&str, &str)] = &[
    ("create_time", "TIMESTAMP"),
    ("symbol", "VARCHAR"),
    ("sum_open_interest", "DOUBLE"),
    ("sum_open_interest_value", "DOUBLE"),
    ("count_toptrader_long_short_ratio", "DOUBLE"),
    ("sum_toptrader_long_short_ratio", "DOUBLE"),
    ("count_long_short_ratio", "DOUBLE"),
    ("sum_taker_long_short_vol_ratio", "DOUBLE"),
];
const METRICS_SELECT: &str = "create_time AS ts, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio,
    sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio";

const FUNDING_COLUMNS: &[(&str, &str)] = &[
    ("calc_time", "BIGINT"),
    ("funding_interval_hours", "BIGINT"),
    ("last_funding_rate", "DOUBLE"),
];
const FUNDING_SELECT: &str = "calc_time AS calc_time_ms, make_timestamp(calc_time * 1000) AS ts, funding_interval_hours, last_funding_rate";

const AGG_TRADES_COLUMNS: &[(&str, &str)] = &[
    ("agg_trade_id", "BIGINT"),
    ("price", "DOUBLE"),
    ("quantity", "DOUBLE"),
    ("first_trade_id", "BIGINT"),
    ("last_trade_id", "BIGINT"),
    ("transact_time", "BIGINT"),
    ("is_buyer_maker", "BOOLEAN"),
];
const AGG_TRADES_SELECT: &str = "agg_trade_id, transact_time AS transact_time_ms, make_timestamp(transact_time * 1000) AS ts, price, quantity,
    first_trade_id, last_trade_id, is_buyer_maker";

const BOOK_DEPTH_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("percentage", "DOUBLE"),
    ("depth", "DOUBLE"),
    ("notional", "DOUBLE"),
];
const BOOK_DEPTH_SELECT: &str = "timestamp AS ts, percentage, depth, notional";

/// The dataset definition for a name.
#[must_use]
pub fn dataset(name: &str) -> Option<Dataset> {
    let kline = |name: &'static str| Dataset {
        name,
        granularity: Granularity::Day,
        interval: Some("1m"),
        columns: KLINE_COLUMNS,
        select: KLINE_SELECT,
    };
    Some(match name {
        "klines" => kline("klines"),
        "premiumIndexKlines" => kline("premiumIndexKlines"),
        "markPriceKlines" => kline("markPriceKlines"),
        "indexPriceKlines" => kline("indexPriceKlines"),
        "metrics" => Dataset {
            name: "metrics",
            granularity: Granularity::Day,
            interval: None,
            columns: METRICS_COLUMNS,
            select: METRICS_SELECT,
        },
        "fundingRate" => Dataset {
            name: "fundingRate",
            granularity: Granularity::Month,
            interval: None,
            columns: FUNDING_COLUMNS,
            select: FUNDING_SELECT,
        },
        "aggTrades" => Dataset {
            name: "aggTrades",
            granularity: Granularity::Day,
            interval: None,
            columns: AGG_TRADES_COLUMNS,
            select: AGG_TRADES_SELECT,
        },
        "bookDepth" => Dataset {
            name: "bookDepth",
            granularity: Granularity::Day,
            interval: None,
            columns: BOOK_DEPTH_COLUMNS,
            select: BOOK_DEPTH_SELECT,
        },
        _ => return None,
    })
}

impl Dataset {
    /// Key prefix of a symbol's files.
    #[must_use]
    pub fn prefix(&self, symbol: &str) -> String {
        let cadence = match self.granularity {
            Granularity::Day => "daily",
            Granularity::Month | Granularity::Year => "monthly",
        };
        match self.interval {
            Some(interval) => format!(
                "data/futures/um/{cadence}/{}/{symbol}/{interval}/",
                self.name
            ),
            None => format!("data/futures/um/{cadence}/{}/{symbol}/", self.name),
        }
    }

    /// File name for a period label.
    #[must_use]
    pub fn file_name(&self, symbol: &str, label: &str) -> String {
        match self.interval {
            Some(interval) => format!("{symbol}-{interval}-{label}.zip"),
            None => format!("{symbol}-{}-{label}.zip", self.name),
        }
    }

    /// The period label inside a zip key, when the key is one of this dataset's zips.
    #[must_use]
    pub fn label_of(&self, symbol: &str, key: &str) -> Option<String> {
        let name = key.rsplit('/').next()?;
        let stem = name.strip_suffix(".zip")?;
        let lead = match self.interval {
            Some(interval) => format!("{symbol}-{interval}-"),
            None => format!("{symbol}-{}-", self.name),
        };
        let label = stem.strip_prefix(lead.as_str())?;
        super::periods::Period::parse(label).map(|_| label.to_owned())
    }
}

/// One page of the bucket listing.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Listing {
    /// Keys in order.
    pub keys: Vec<String>,
    /// Whether more keys follow.
    pub truncated: bool,
    /// Marker for the next page, when the bucket names one.
    pub next_marker: Option<String>,
}

/// Extract keys, truncation and the next marker from a `ListBucketResult`.
#[must_use]
pub fn parse_listing(xml: &str) -> Listing {
    let mut listing = Listing {
        keys: elements(xml, "Key"),
        truncated: elements(xml, "IsTruncated").first().map(String::as_str) == Some("true"),
        next_marker: elements(xml, "NextMarker").into_iter().next(),
    };
    if listing.truncated && listing.next_marker.is_none() {
        listing.next_marker = listing.keys.last().cloned();
    }
    listing
}

fn elements(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(after[..end].to_owned());
        rest = &after[end + close.len()..];
    }
    out
}

/// The zip keys worth fetching: at or after `start_label`, not yet registered.
#[must_use]
pub fn plan(
    keys: &[String],
    dataset: &Dataset,
    symbol: &str,
    start_label: &str,
    registered: &HashSet<String>,
) -> Vec<(String, String)> {
    keys.iter()
        .filter_map(|key| {
            dataset
                .label_of(symbol, key)
                .map(|label| (key.clone(), label))
        })
        .filter(|(_, label)| label.as_str() >= start_label && !registered.contains(label))
        .collect()
}

/// The SHA-256 named by a `.CHECKSUM` file (`<hex>  <file>`).
#[must_use]
pub fn parse_checksum(text: &str) -> Option<String> {
    let token = text.split_whitespace().next()?;
    (token.len() == 64 && token.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| token.to_lowercase())
}

/// The one CSV inside a dump zip.
pub fn unzip_single(bytes: &[u8]) -> Result<Vec<u8>, StoreError> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| StoreError::Check(format!("zip: {e}")))?;
    if archive.is_empty() {
        return Err(StoreError::Check("zip has no entry".into()));
    }
    let mut entry = archive
        .by_index(0)
        .map_err(|e| StoreError::Check(format!("zip: {e}")))?;
    let mut out = Vec::with_capacity(usize::try_from(entry.size()).unwrap_or(0));
    entry
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Check(format!("zip: {e}")))?;
    Ok(out)
}

/// Whether the CSV starts with a header line (older dumps have none).
#[must_use]
pub fn has_header(csv: &[u8]) -> bool {
    !csv.first().is_some_and(u8::is_ascii_digit)
}

/// Label of the period just before the start date, in the dataset's granularity.
#[must_use]
pub fn label_before(dataset: &Dataset, start: chrono::NaiveDate) -> String {
    let period = super::periods::Period::containing(start, dataset.granularity);
    let previous = match dataset.granularity {
        Granularity::Day => start.pred_opt().unwrap_or(start),
        Granularity::Month | Granularity::Year => period.start.pred_opt().unwrap_or(start),
    };
    super::periods::Period::containing(previous, dataset.granularity).label()
}

/// The large datasets (`aggTrades`, `bookDepth`) as dump series for the newest-first walk
/// (ADR-0009, exchange dumps): one per dataset and symbol of the walk's kinds.
#[must_use]
pub fn dump_kinds(
    cfg: &Binance,
    symbols: &[Symbol],
    only_symbol: Option<&str>,
) -> Vec<std::sync::Arc<dyn super::dumps::Kind>> {
    let mut out: Vec<std::sync::Arc<dyn super::dumps::Kind>> = Vec::new();
    for name in &cfg.large_datasets {
        let Some(dataset) = dataset_static(name) else {
            continue;
        };
        for symbol in symbols {
            let Some(venue) = symbol.binance.as_deref() else {
                continue;
            };
            if !cfg.walk.kinds.contains(&symbol.kind)
                || only_symbol.is_some_and(|s| s != symbol.phoenix && s != venue)
            {
                continue;
            }
            out.push(std::sync::Arc::new(DumpSeries {
                files_url: cfg.files_url.trim_end_matches('/').to_owned(),
                verify_checksums: cfg.verify_checksums,
                dataset,
                venue: venue.to_owned(),
                phoenix: symbol.phoenix.clone(),
            }));
        }
    }
    out
}

/// The large datasets the dump walk knows; any other name is not walked (and the config
/// validation refuses it).
fn dataset_static(name: &str) -> Option<&'static Dataset> {
    static AGG: std::sync::OnceLock<Option<Dataset>> = std::sync::OnceLock::new();
    static DEPTH: std::sync::OnceLock<Option<Dataset>> = std::sync::OnceLock::new();
    match name {
        "aggTrades" => AGG.get_or_init(|| dataset("aggTrades")).as_ref(),
        "bookDepth" => DEPTH.get_or_init(|| dataset("bookDepth")).as_ref(),
        _ => None,
    }
}

/// One large dataset of one symbol, as daily dump files.
pub struct DumpSeries {
    files_url: String,
    verify_checksums: bool,
    dataset: &'static Dataset,
    venue: String,
    phoenix: String,
}

impl super::dumps::Kind for DumpSeries {
    fn source(&self) -> &str {
        "binance"
    }
    fn dataset(&self) -> &str {
        self.dataset.name
    }
    fn symbol(&self) -> &str {
        &self.venue
    }
    fn phoenix(&self) -> &str {
        &self.phoenix
    }
    fn granularity(&self) -> Granularity {
        self.dataset.granularity
    }
    fn url(&self, label: &str) -> String {
        format!(
            "{}/{}{}",
            self.files_url,
            self.dataset.prefix(&self.venue),
            self.dataset.file_name(&self.venue, label)
        )
    }
    fn checksum_url(&self, label: &str) -> Option<String> {
        self.verify_checksums
            .then(|| format!("{}.CHECKSUM", self.url(label)))
    }
    fn container(&self) -> super::dumps::Container {
        super::dumps::Container::Zip
    }
    fn select(
        &self,
        _store: &mut crate::store::Store,
        staged: &std::path::Path,
        constants: &str,
    ) -> Result<String, StoreError> {
        let header = super::dumps::staged_has_header(staged)?;
        Ok(format!(
            "SELECT {}, {constants} FROM {} ORDER BY 1",
            self.dataset.select,
            read_csv(staged, self.dataset.columns, header)
        ))
    }
}

/// Sync every phase-1 dataset for every symbol with a Binance market (the large ones are walked
/// by `dumps`). `reprobe` is the schedule on which a missing period between published ones is
/// asked for again.
pub async fn sync(
    ctx: &Ctx,
    cfg: &Binance,
    symbols: &[Symbol],
    only_symbol: Option<&str>,
    reprobe: &AbsentReprobe,
) -> Outcome {
    let mut outcome = Outcome::default();
    let mut asked = 0;
    for name in &cfg.datasets {
        let Some(dataset) = dataset(name) else {
            continue;
        };
        for symbol in symbols {
            if ctx.stopping() {
                return outcome;
            }
            let Some(venue) = symbol.binance.as_deref() else {
                continue;
            };
            if only_symbol.is_some_and(|s| s != symbol.phoenix && s != venue) {
                continue;
            }
            let pair = (venue, symbol.phoenix.as_str());
            let result = sync_pair(ctx, cfg, &dataset, pair, (reprobe, &mut asked)).await;
            outcome.add(&result);
        }
    }
    outcome
}

/// One dataset of one symbol; `asked` counts the holes the source has asked for in this run.
async fn sync_pair(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    (venue, phoenix): (&str, &str),
    (reprobe, asked): (&AbsentReprobe, &mut u32),
) -> Outcome {
    let mut outcome = Outcome::default();
    let (source, name, venue_owned) = (
        "binance".to_owned(),
        dataset.name.to_owned(),
        venue.to_owned(),
    );
    let mut registered: HashSet<String> = match ctx
        .db
        .run(move |store| super::ledger::complete_periods(store, &source, &name, &venue_owned))
        .await
    {
        Ok(labels) => labels.into_iter().collect(),
        Err(error) => {
            log_error(dataset, venue, "ledger", &error.to_string());
            outcome.errors += 1;
            return outcome;
        }
    };
    forward(
        ctx,
        cfg,
        dataset,
        (venue, phoenix),
        &mut registered,
        &mut outcome,
    )
    .await;
    holes(
        ctx,
        cfg,
        dataset,
        (venue, phoenix),
        &registered,
        (reprobe, asked),
        &mut outcome,
    )
    .await;
    outcome
}

/// List after the newest registered file and fetch what is new, oldest first.
async fn forward(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    (venue, phoenix): (&str, &str),
    registered: &mut HashSet<String>,
    outcome: &mut Outcome,
) {
    let start_label = super::periods::Period::containing(ctx.start, dataset.granularity).label();
    let newest = registered
        .iter()
        .max()
        .cloned()
        .unwrap_or_else(|| label_before(dataset, ctx.start));
    let prefix = dataset.prefix(venue);
    let mut marker = format!("{prefix}{}.CHECKSUM", dataset.file_name(venue, &newest));
    let mut items = Vec::new();
    loop {
        let url = with_query(
            cfg.list_url.trim_end_matches('/'),
            &[
                ("delimiter", "/".into()),
                ("prefix", prefix.clone()),
                ("marker", marker.clone()),
            ],
        );
        let page = match ctx.http.get_bytes(&url, &[]).await {
            Ok(page) => page,
            Err(error) => {
                log_error(dataset, venue, "listing", &error.to_string());
                outcome.errors += 1;
                return;
            }
        };
        let listing = parse_listing(&String::from_utf8_lossy(&page.body));
        items.extend(plan(
            &listing.keys,
            dataset,
            venue,
            &start_label,
            registered,
        ));
        match listing.next_marker {
            Some(next) if listing.truncated && next != marker => marker = next,
            _ => break,
        }
    }
    for (key, label) in items {
        if ctx.stopping() {
            break;
        }
        match fetch_item(ctx, cfg, dataset, (venue, phoenix), &key, &label, None).await {
            Ok(rows) => {
                registered.insert(label);
                outcome.files += 1;
                outcome.rows += rows;
            }
            Err(error) => {
                // Later days would move the listing marker past this one; stop here and let
                // the next run retry it.
                log_error(dataset, venue, &label, &error.to_string());
                outcome.errors += 1;
                break;
            }
        }
    }
}

/// The periods missing between the oldest registered one at or after `start_label` and the
/// newest, newest first: holes in a continuous series. Periods before the oldest are a market
/// listed later and are not holes.
fn missing_periods(held: &BTreeSet<String>, start_label: &str) -> Vec<Period> {
    let first = held.iter().find(|label| label.as_str() >= start_label);
    let (Some(first), Some(last)) = (first, held.last()) else {
        return Vec::new();
    };
    let (Some(mut period), Some(last)) = (Period::parse(first), Period::parse(last)) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    while period < last {
        if !held.contains(&period.label()) {
            out.push(period);
        }
        period = period.next();
    }
    out.reverse();
    out
}

/// Ask for every period missing between published ones, directly and not through the listing
/// (which starts after the newest file): a hole is marked absent once three days old (and
/// logged and counted), asked again on the schedule, and filled when the venue publishes it. A
/// failed forward pass skips the asking, not the counting.
async fn holes(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    (venue, phoenix): (&str, &str),
    registered: &HashSet<String>,
    (reprobe, asked): (&AbsentReprobe, &mut u32),
    outcome: &mut Outcome,
) {
    let key = progress_key("binance", dataset.name, venue);
    let lookup = key.clone();
    let mut absent = match ctx.db.run(move |store| get_progress(store, &lookup)).await {
        Ok(progress) => AbsentSet::from_progress(progress.as_ref()),
        Err(error) => {
            log_error(dataset, venue, "progress", &error.to_string());
            outcome.errors += 1;
            return;
        }
    };
    let mut held: BTreeSet<String> = registered.iter().cloned().collect();
    let mut counts = Counts::default();
    if outcome.errors == 0 {
        let start_label =
            super::periods::Period::containing(ctx.start, dataset.granularity).label();
        for period in missing_periods(&held, &start_label) {
            if ctx.stopping() {
                break;
            }
            let label = period.label();
            let marked = absent.contains(&label);
            if marked && absent.due(&label, ctx.now_ms, reprobe) != Due::Now {
                continue;
            }
            // The holes left over wait for the next run.
            if *asked >= reprobe.max_per_run {
                break;
            }
            *asked += 1;
            let file = format!(
                "{}{}",
                dataset.prefix(venue),
                dataset.file_name(venue, &label)
            );
            let mut after = absent.clone();
            after.clear(&label);
            let progress = Some((key.clone(), after.to_progress()));
            match fetch_item(ctx, cfg, dataset, (venue, phoenix), &file, &label, progress).await {
                Ok(rows) => {
                    if marked {
                        counts.filled += 1;
                        absent::log_filled(
                            "binance",
                            dataset.name,
                            venue,
                            &label,
                            absent.misses(&label),
                        );
                    }
                    absent = after;
                    held.insert(label);
                    outcome.files += 1;
                    outcome.rows += rows;
                }
                Err(ItemError::NotFound) => {
                    let saved = if marked {
                        let spent = absent.missed(&label, ctx.now_ms, reprobe);
                        let saved = save(ctx, &key, &absent).await;
                        if spent && saved.is_ok() {
                            absent::log_given_up(
                                "binance",
                                dataset.name,
                                venue,
                                &label,
                                absent.misses(&label),
                            );
                        }
                        saved
                    } else if maybe_unpublished(&period, ctx.now_ms) {
                        Ok(())
                    } else {
                        let position = absent::position(&label, &held);
                        absent.mark(&label, ctx.now_ms);
                        let saved = save(ctx, &key, &absent).await;
                        if saved.is_ok() {
                            counts.marked += 1;
                            absent::log_marked("binance", dataset.name, venue, &label, position);
                        } else {
                            absent.clear(&label);
                        }
                        saved
                    };
                    if let Err(error) = saved {
                        log_error(dataset, venue, &label, &error.to_string());
                        outcome.errors += 1;
                    }
                }
                Err(ItemError::Other(error)) => {
                    log_error(dataset, venue, &label, &error.to_string());
                    outcome.errors += 1;
                    if marked {
                        absent.attempted(&label, ctx.now_ms);
                        let _ = save(ctx, &key, &absent).await;
                    }
                    // The host is likely in trouble: stop asking, count what is standing.
                    break;
                }
            }
        }
    }
    (counts.interior, counts.given_up) = absent.interior(&held, reprobe);
    outcome.absent = Some(counts);
}

async fn save(ctx: &Ctx, key: &str, absent: &AbsentSet) -> Result<(), StoreError> {
    let (key, value) = (key.to_owned(), absent.to_progress());
    ctx.db
        .run(move |store| set_progress(store, &key, &value))
        .await
}

/// Why one file could not be stored: the venue does not serve it, or something else went wrong.
enum ItemError {
    NotFound,
    Other(StoreError),
}

impl std::fmt::Display for ItemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemError::NotFound => f.write_str("not found"),
            ItemError::Other(error) => write!(f, "{error}"),
        }
    }
}

impl From<HttpError> for ItemError {
    fn from(error: HttpError) -> Self {
        match error {
            HttpError::NotFound => ItemError::NotFound,
            other => ItemError::Other(other.into()),
        }
    }
}

impl From<StoreError> for ItemError {
    fn from(error: StoreError) -> Self {
        ItemError::Other(error)
    }
}

/// Fetch, verify, convert and register one file; with `progress`, that progress record is
/// written in the same transaction as the file's registration.
async fn fetch_item(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    (venue, phoenix): (&str, &str),
    key: &str,
    label: &str,
    progress: Option<(String, Value)>,
) -> Result<u64, ItemError> {
    let zip = download(ctx, cfg, key).await?;
    Ok(store_item(ctx, dataset, (venue, phoenix), label, &zip, progress).await?)
}

async fn download(ctx: &Ctx, cfg: &Binance, key: &str) -> Result<Vec<u8>, ItemError> {
    let url = format!("{}/{key}", cfg.files_url.trim_end_matches('/'));
    // The zip and its checksum are two origin round trips of about 0.7 s each from the box;
    // issued together (still spaced by the host limiter) they overlap instead of adding up.
    let checksum_url = format!("{url}.CHECKSUM");
    let (zip, checksum) = tokio::join!(ctx.http.get_bytes(&url, &[]), async {
        if cfg.verify_checksums {
            ctx.http.get_bytes(&checksum_url, &[]).await.map(Some)
        } else {
            Ok(None)
        }
    });
    let zip = zip?;
    if let Some(checksum) = checksum? {
        let expected = parse_checksum(&String::from_utf8_lossy(&checksum.body))
            .ok_or_else(|| StoreError::Check("unreadable CHECKSUM".into()))?;
        if sha256_hex(&zip.body) != expected {
            return Err(StoreError::Check("checksum mismatch".into()).into());
        }
    }
    Ok(zip.body)
}

async fn store_item(
    ctx: &Ctx,
    dataset: &Dataset,
    (venue, phoenix): (&str, &str),
    label: &str,
    zip: &[u8],
    progress: Option<(String, Value)>,
) -> Result<u64, StoreError> {
    let csv = unzip_single(zip)?;
    let header = has_header(&csv);
    let staged = staging_path(&ctx.staging(), "csv");
    std::fs::write(&staged, &csv)?;
    let target = Target {
        source: "binance".into(),
        dataset: dataset.name.into(),
        symbol: venue.into(),
        phoenix_symbol: Some(phoenix.into()),
        period: label.into(),
        complete: true,
    };
    let select = format!(
        "SELECT {}, {} FROM {} ORDER BY 1",
        dataset.select,
        target.constant_columns(),
        read_csv(&staged, dataset.columns, header)
    );
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_parquet(store, &root, &select, &target)?;
            let progress = progress.as_ref().map(|(key, value)| (key.as_str(), value));
            register(store, &record, progress)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    let rows = result?;
    log(
        "augment_file",
        Obj::new()
            .with("source", "binance")
            .with("dataset", dataset.name)
            .with("symbol", venue)
            .with("period", label)
            .with("rows", rows)
            .with("complete", true),
    );
    Ok(u64::try_from(rows).unwrap_or(0))
}

fn log_error(dataset: &Dataset, venue: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "binance")
            .with("dataset", dataset.name)
            .with("symbol", venue)
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = include_str!("../../../../tests/data/augment/s3-list-truncated.xml");

    #[test]
    fn paths_and_labels() {
        let klines = dataset("klines").unwrap();
        assert_eq!(
            klines.prefix("SOLUSDT"),
            "data/futures/um/daily/klines/SOLUSDT/1m/"
        );
        assert_eq!(
            klines.file_name("SOLUSDT", "2026-10-07"),
            "SOLUSDT-1m-2026-10-07.zip"
        );
        assert_eq!(
            klines.label_of(
                "SOLUSDT",
                "data/futures/um/daily/klines/SOLUSDT/1m/SOLUSDT-1m-2026-10-07.zip"
            ),
            Some("2026-10-07".into())
        );
        assert_eq!(
            klines.label_of("SOLUSDT", "x/SOLUSDT-1m-2026-10-07.zip.CHECKSUM"),
            None
        );
        let funding = dataset("fundingRate").unwrap();
        assert_eq!(
            funding.prefix("SOLUSDT"),
            "data/futures/um/monthly/fundingRate/SOLUSDT/"
        );
        assert_eq!(
            funding.file_name("SOLUSDT", "2026-09"),
            "SOLUSDT-fundingRate-2026-09.zip"
        );
        let start = super::super::periods::parse_date("2026-01-01").unwrap();
        assert_eq!(label_before(&klines, start), "2025-12-31");
        assert_eq!(label_before(&funding, start), "2025-12");
        assert!(dataset("bookTicker").is_none());
    }

    #[test]
    fn listing_and_plan() {
        let listing = parse_listing(LISTING);
        assert!(listing.truncated);
        assert_eq!(
            listing.next_marker.as_deref(),
            Some("data/futures/um/daily/klines/SOLUSDT/1m/SOLUSDT-1m-2020-09-16.zip")
        );
        assert!(listing.keys.len() >= 4);
        assert!(listing.keys[1].ends_with(".CHECKSUM"));
        let klines = dataset("klines").unwrap();
        let registered: HashSet<String> = ["2020-09-14".to_owned()].into_iter().collect();
        let items = plan(&listing.keys, &klines, "SOLUSDT", "2020-09-14", &registered);
        let labels: Vec<&str> = items.iter().map(|(_, l)| l.as_str()).collect();
        assert!(labels.contains(&"2020-09-15"));
        assert!(!labels.contains(&"2020-09-14"));
        assert!(plan(&listing.keys, &klines, "SOLUSDT", "2026-01-01", &registered).is_empty());
        let plain =
            parse_listing("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
        assert_eq!(plain, Listing::default());
    }

    #[test]
    fn checksums_zips_and_headers() {
        assert_eq!(
            parse_checksum(
                "56967f56743382c46b6c98ac49ebb09f2c31a2dd6680f273e75da3cb9db70b66  SOLUSDT-1m-2026-10-07.zip\n"
            ),
            Some("56967f56743382c46b6c98ac49ebb09f2c31a2dd6680f273e75da3cb9db70b66".into())
        );
        assert_eq!(parse_checksum("nope"), None);
        let csv = b"open_time,open\n1,2\n";
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buffer);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            writer.start_file("a.csv", options).unwrap();
            std::io::Write::write_all(&mut writer, csv).unwrap();
            writer.finish().unwrap();
        }
        assert_eq!(unzip_single(buffer.get_ref()).unwrap(), csv);
        assert!(unzip_single(b"not a zip").is_err());
        assert!(has_header(csv));
        assert!(!has_header(b"1791331200000,120.66\n"));
    }
}
