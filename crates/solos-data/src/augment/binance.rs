//! Binance USD-M futures public dumps (`data.binance.vision`): one zip of one CSV per symbol and
//! day (or month for funding), listed through the bucket's S3 index, verified against the
//! published `.CHECKSUM`, converted to Parquet and deleted. A file is final once published, so
//! the ledger's `files` table is the progress record and the listing starts after the newest
//! registered file.

use super::config::{Binance, Symbol};
use super::http::with_query;
use super::ledger::register;
use super::parquet::{Target, read_csv, staging_path, write_parquet};
use super::periods::Granularity;
use super::series::{Ctx, Outcome};
use crate::fsutil::sha256_hex;
use crate::jsonout::{Obj, log, safe_error};
use crate::store::StoreError;
use std::collections::HashSet;
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
            Granularity::Month => "monthly",
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
        Granularity::Month => period.start.pred_opt().unwrap_or(start),
    };
    super::periods::Period::containing(previous, dataset.granularity).label()
}

/// Sync every enabled dataset for every symbol with a Binance market.
pub async fn sync(
    ctx: &Ctx,
    cfg: &Binance,
    symbols: &[Symbol],
    only_symbol: Option<&str>,
) -> Outcome {
    let mut outcome = Outcome::default();
    let names: Vec<&String> = cfg.datasets.iter().chain(&cfg.large_datasets).collect();
    for name in names {
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
            let result = sync_pair(ctx, cfg, &dataset, venue, &symbol.phoenix).await;
            outcome.add(&result);
        }
    }
    outcome
}

async fn sync_pair(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    venue: &str,
    phoenix: &str,
) -> Outcome {
    let mut outcome = Outcome::default();
    let (source, name, venue_owned) = (
        "binance".to_owned(),
        dataset.name.to_owned(),
        venue.to_owned(),
    );
    let registered: HashSet<String> = match ctx
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
                return outcome;
            }
        };
        let listing = parse_listing(&String::from_utf8_lossy(&page.body));
        items.extend(plan(
            &listing.keys,
            dataset,
            venue,
            &start_label,
            &registered,
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
        match fetch_item(ctx, cfg, dataset, venue, phoenix, &key, &label).await {
            Ok(rows) => {
                outcome.files += 1;
                outcome.rows += rows;
            }
            Err(error) => {
                log_error(dataset, venue, &label, &error.to_string());
                outcome.errors += 1;
            }
        }
    }
    outcome
}

async fn fetch_item(
    ctx: &Ctx,
    cfg: &Binance,
    dataset: &Dataset,
    venue: &str,
    phoenix: &str,
    key: &str,
    label: &str,
) -> Result<u64, StoreError> {
    let url = format!("{}/{key}", cfg.files_url.trim_end_matches('/'));
    let zip = ctx.http.get_bytes(&url, &[]).await?;
    if cfg.verify_checksums {
        let checksum = ctx.http.get_bytes(&format!("{url}.CHECKSUM"), &[]).await?;
        let expected = parse_checksum(&String::from_utf8_lossy(&checksum.body))
            .ok_or_else(|| StoreError::Check("unreadable CHECKSUM".into()))?;
        if sha256_hex(&zip.body) != expected {
            return Err(StoreError::Check("checksum mismatch".into()));
        }
    }
    let csv = unzip_single(&zip.body)?;
    let header = has_header(&csv);
    let staged = staging_path(&ctx.root, "csv");
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
    let result = ctx
        .db
        .run(move |store| {
            let record = write_parquet(store, &select, &target)?;
            register(store, &record, None)?;
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
