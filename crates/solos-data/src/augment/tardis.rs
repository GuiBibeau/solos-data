//! Tardis.dev downloadable CSV datasets (ADR-0009, Tardis.dev section): one gzip CSV per
//! exchange, data type, symbol and UTC day at
//! `<datasetsUrl>/<exchange>/<dataType>/<YYYY>/<MM>/<DD>/<symbol>.csv.gz`. The first day of every
//! month is served without a key; every other day needs `TARDIS_API_KEY` (a paid plan) and is
//! asked for only when the key is set. Each file becomes one Parquet day under
//! `tables/tardis/<exchange>_<dataType>/<symbol>/<YYYY-MM-DD>.parquet`; order book snapshots are
//! thinned to the last snapshot of each second (local time), ten levels a side plus the 25-level
//! totals. Days are walked newest first and recorded one by one, so a stopped run resumes.

use super::config::{ApiKey, Symbol, Tardis, TardisExchange};
use super::http::HttpError;
use super::ledger::{complete_periods, get_progress, register, set_progress};
use super::parquet::{Target, staging_path, write_parquet};
use super::periods::{date_of_ms, ms_of, parse_date};
use super::series::{Ctx, Outcome, budget_bytes};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::{StoreError, sql_string};
use chrono::{Datelike, NaiveDate};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashSet};
use std::io::Read;
use std::path::Path;

/// Source name in the ledger, the catalog and the query views (`tardis_<exchange>_<type>`).
pub const SOURCE: &str = "tardis";
/// The data types the source knows how to type.
pub const DATA_TYPES: [&str; 4] = [
    "trades",
    "derivative_ticker",
    "liquidations",
    "book_snapshot_25",
];
/// Symbol-map fields an exchange can name its markets from.
pub const SYMBOL_FIELDS: [&str; 4] = ["binance", "bybit", "okx", "hyperliquid"];
/// A day's files are exported around 06:00 UTC the next day; ask from 08:00.
pub const LAG_MS: i64 = 8 * 3_600_000;
/// A day older than this that the API does not serve (HTTP 400 or 404) is a day before the
/// symbol's listing and is not asked for again; a younger one may not be exported yet.
pub const UNPUBLISHED_DAYS: i64 = 3;
/// Levels in a `book_snapshot_25` row, per side.
pub const BOOK_LEVELS: usize = 25;
/// Levels a side kept in the thinned snapshot.
pub const BOOK_KEPT_LEVELS: usize = 10;
/// The note `augment status` shows while no key is set.
pub const SAMPLES_ONLY: &str = "TARDIS_API_KEY not set: first-of-month samples only";
/// The note while the host's transfer limit holds (HTTP 429 with a long `Retry-After`).
pub const QUOTA_REACHED: &str =
    "transfer limit reached (HTTP 429); the next runs resume when it resets";

/// One (exchange, data type, symbol) series.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    /// Tardis.dev exchange id.
    pub exchange: String,
    /// Data type.
    pub data_type: String,
    /// The dataset symbol (uppercase, `:` and `/` as `-`).
    pub symbol: String,
    /// Phoenix symbol.
    pub phoenix: String,
}

impl Item {
    /// Dataset name in the ledger: `binance_futures_trades`.
    #[must_use]
    pub fn dataset(&self) -> String {
        dataset_name(&self.exchange, &self.data_type)
    }

    /// Progress key: the days the API does not serve for this series.
    #[must_use]
    pub fn progress_key(&self) -> String {
        format!("{SOURCE}/{}/{}", self.dataset(), self.symbol)
    }
}

/// Check the source's settings.
pub fn validate(cfg: &Tardis) -> Result<(), StoreError> {
    let bad = |what: &str| Err(StoreError::Check(format!("invalid tardis.{what}")));
    if parse_date(&cfg.sample_start).is_none() {
        return bad("sampleStart");
    }
    if parse_date(&cfg.keyed_start).is_none() {
        return bad("keyedStart");
    }
    if !(cfg.budget_gb >= 0.0 && cfg.budget_gb.is_finite()) {
        return bad("budgetGb");
    }
    if cfg
        .kinds
        .iter()
        .any(|k| !["crypto", "equity", "commodity"].contains(&k.as_str()))
    {
        return bad("kinds");
    }
    for exchange in &cfg.exchanges {
        let id_ok = !exchange.id.is_empty()
            && exchange
                .id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        let field_ok = match exchange.symbol_field.as_deref() {
            Some(field) => SYMBOL_FIELDS.contains(&field) && exchange.symbols.is_empty(),
            None => !exchange.symbols.is_empty(),
        };
        let types_ok = !exchange.data_types.is_empty()
            && exchange
                .data_types
                .iter()
                .all(|t| DATA_TYPES.contains(&t.as_str()));
        if !(id_ok && field_ok && types_ok) {
            return bad(&format!("exchanges.{}", exchange.id));
        }
    }
    Ok(())
}

/// The dataset symbol Tardis.dev uses for a venue symbol: uppercase, `:` and `/` as `-`.
#[must_use]
pub fn dataset_symbol(venue: &str) -> String {
    venue.to_uppercase().replace([':', '/'], "-")
}

/// `binance-futures` + `trades` → `binance_futures_trades`.
#[must_use]
pub fn dataset_name(exchange: &str, data_type: &str) -> String {
    format!("{}_{data_type}", exchange.replace('-', "_"))
}

/// URL of one day's file.
#[must_use]
pub fn dataset_url(base: &str, item: &Item, day: NaiveDate) -> String {
    format!(
        "{}/{}/{}/{:04}/{:02}/{:02}/{}.csv.gz",
        base.trim_end_matches('/'),
        item.exchange,
        item.data_type,
        day.year(),
        day.month(),
        day.day(),
        item.symbol
    )
}

/// The days every series wants, newest first: each first of the month from `sampleStart`, and
/// with a key each day from `keyedStart`; only days whose files have been exported.
#[must_use]
pub fn wanted_days(cfg: &Tardis, keyed: bool, now_ms: i64) -> Vec<NaiveDate> {
    let Some(last) = date_of_ms(now_ms - LAG_MS).pred_opt() else {
        return Vec::new();
    };
    let mut days: BTreeSet<NaiveDate> = BTreeSet::new();
    if let Some(start) = parse_date(&cfg.sample_start) {
        let mut month = start.with_day(1).unwrap_or(start);
        if month < start {
            month = next_month(month);
        }
        while month <= last {
            days.insert(month);
            month = next_month(month);
        }
    }
    if keyed && let Some(mut day) = parse_date(&cfg.keyed_start) {
        while day <= last {
            days.insert(day);
            let Some(next) = day.succ_opt() else { break };
            day = next;
        }
    }
    days.into_iter().rev().collect()
}

fn next_month(first: NaiveDate) -> NaiveDate {
    first
        .checked_add_months(chrono::Months::new(1))
        .unwrap_or(NaiveDate::MAX)
}

/// Whether a day is served without a key.
#[must_use]
pub fn is_sample_day(day: NaiveDate) -> bool {
    day.day() == 1
}

/// Every series the configuration names, filtered to one Phoenix or venue symbol when given.
#[must_use]
pub fn items(cfg: &Tardis, symbols: &[Symbol], only: Option<&str>) -> Vec<Item> {
    let mut out = Vec::new();
    for exchange in &cfg.exchanges {
        for symbol in symbols {
            if !cfg.kinds.contains(&symbol.kind) {
                continue;
            }
            let Some(venue) = venue_symbol(exchange, symbol) else {
                continue;
            };
            let dataset = dataset_symbol(&venue);
            if only.is_some_and(|s| s != symbol.phoenix && s != venue && s != dataset) {
                continue;
            }
            for data_type in &exchange.data_types {
                if data_type.starts_with("book_snapshot")
                    && !cfg.book_symbols.contains(&symbol.phoenix)
                {
                    continue;
                }
                out.push(Item {
                    exchange: exchange.id.clone(),
                    data_type: data_type.clone(),
                    symbol: dataset.clone(),
                    phoenix: symbol.phoenix.clone(),
                });
            }
        }
    }
    out
}

fn venue_symbol(exchange: &TardisExchange, symbol: &Symbol) -> Option<String> {
    match exchange.symbol_field.as_deref() {
        Some("binance") => symbol.binance.clone(),
        Some("bybit") => symbol.bybit.clone(),
        Some("okx") => symbol.okx.clone(),
        Some("hyperliquid") => symbol.hyperliquid.clone(),
        Some(_) => None,
        None => exchange.symbols.get(&symbol.phoenix).cloned(),
    }
    .filter(|v| {
        !v.is_empty()
            && v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_:/.".contains(&b))
    })
}

/// The CSV's columns and DuckDB types, in file order.
#[must_use]
pub fn columns(data_type: &str) -> Vec<(String, &'static str)> {
    let mut cols: Vec<(String, &'static str)> = vec![
        ("exchange".into(), "VARCHAR"),
        ("symbol".into(), "VARCHAR"),
        ("timestamp".into(), "BIGINT"),
        ("local_timestamp".into(), "BIGINT"),
    ];
    match data_type {
        "derivative_ticker" => {
            cols.push(("funding_timestamp".into(), "BIGINT"));
            for name in [
                "funding_rate",
                "predicted_funding_rate",
                "open_interest",
                "last_price",
                "index_price",
                "mark_price",
            ] {
                cols.push((name.into(), "DOUBLE"));
            }
        }
        "book_snapshot_25" => {
            for level in 0..BOOK_LEVELS {
                for (side, field) in [
                    ("asks", "price"),
                    ("asks", "amount"),
                    ("bids", "price"),
                    ("bids", "amount"),
                ] {
                    cols.push((format!("{side}[{level}].{field}"), "DOUBLE"));
                }
            }
        }
        _ => {
            cols.push(("id".into(), "VARCHAR"));
            cols.push(("side".into(), "VARCHAR"));
            cols.push(("price".into(), "DOUBLE"));
            cols.push(("amount".into(), "DOUBLE"));
        }
    }
    cols
}

/// `read_csv` of a staged gzip file with the type's explicit columns.
#[must_use]
pub fn read_source(path: &Path, data_type: &str) -> String {
    let map: Vec<String> = columns(data_type)
        .iter()
        .map(|(name, kind)| format!("{}: '{kind}'", sql_string(name)))
        .collect();
    format!(
        "read_csv({}, header=true, compression='gzip', columns={{{}}}, auto_detect=false, delim=',', quote='\"')",
        sql_string(&path.to_string_lossy()),
        map.join(", ")
    )
}

/// A typed source with no rows, for the empty files the API returns for days without data.
#[must_use]
pub fn empty_source(data_type: &str) -> String {
    let cols: Vec<String> = columns(data_type)
        .iter()
        .map(|(name, kind)| format!("NULL::{kind} AS {}", ident(name)))
        .collect();
    format!("(SELECT {} WHERE false)", cols.join(", "))
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The typed projection written to Parquet; `constants` are the symbol columns.
#[must_use]
pub fn select(data_type: &str, from: &str, exchange: &str, constants: &str) -> String {
    let head = "\"timestamp\" AS time_us, local_timestamp AS local_time_us, make_timestamp(\"timestamp\") AS ts";
    let tail = format!("{} AS exchange, {constants}", sql_string(exchange));
    match data_type {
        "derivative_ticker" => format!(
            "SELECT {head}, funding_timestamp AS funding_time_us, funding_rate, predicted_funding_rate,
                    open_interest, last_price, index_price, mark_price, {tail} FROM {from}"
        ),
        "book_snapshot_25" => {
            let mut fields = Vec::new();
            for level in 0..BOOK_KEPT_LEVELS {
                for (side, short) in [("bids", "bid"), ("asks", "ask")] {
                    fields.push(format!(
                        "{} AS {short}_price_{level}",
                        ident(&format!("{side}[{level}].price"))
                    ));
                    fields.push(format!(
                        "{} AS {short}_amount_{level}",
                        ident(&format!("{side}[{level}].amount"))
                    ));
                }
            }
            for (side, short) in [("bids", "bid"), ("asks", "ask")] {
                let amounts: Vec<String> = (0..BOOK_LEVELS)
                    .map(|l| format!("coalesce({}, 0)", ident(&format!("{side}[{l}].amount"))))
                    .collect();
                let notionals: Vec<String> = (0..BOOK_LEVELS)
                    .map(|l| {
                        format!(
                            "coalesce({} * {}, 0)",
                            ident(&format!("{side}[{l}].price")),
                            ident(&format!("{side}[{l}].amount"))
                        )
                    })
                    .collect();
                fields.push(format!("{} AS {short}_amount_25", amounts.join(" + ")));
                fields.push(format!("{} AS {short}_notional_25", notionals.join(" + ")));
            }
            format!(
                "SELECT {head}, {}, {tail} FROM {from}
                 QUALIFY row_number() OVER (PARTITION BY local_timestamp // 1000000 ORDER BY local_timestamp DESC) = 1
                 ORDER BY local_timestamp",
                fields.join(", ")
            )
        }
        "liquidations" => format!(
            "SELECT {head}, id AS liquidation_id, side, price, amount, {tail} FROM {from}"
        ),
        _ => format!("SELECT {head}, id AS trade_id, side, price, amount, {tail} FROM {from}"),
    }
}

/// Whether a gzip body holds no data (the API's answer for a day without rows).
#[must_use]
pub fn gzip_is_empty(body: &[u8]) -> bool {
    if body.is_empty() {
        return true;
    }
    let mut byte = [0u8; 1];
    matches!(flate2::read::GzDecoder::new(body).read(&mut byte), Ok(0))
}

/// State shared by the series of one run.
#[derive(Default)]
struct Run {
    outcome: Outcome,
    /// The API refused the key before any keyed day succeeded this run; no other keyed day is
    /// asked for.
    keyed_refused: bool,
    /// A keyed day was served this run, so the key works.
    keyed_served: bool,
    /// The budget is spent; no other keyed day is asked for.
    budget_spent: bool,
    /// The host answered 429 with a long `Retry-After` (the transfer limit): the run stops.
    quota_reached: bool,
}

/// One series and what the ledger knows of it.
struct Walk {
    item: Item,
    done: HashSet<String>,
    absent: BTreeSet<String>,
    /// Keyed days older than a refused one are not asked for this run.
    refused: bool,
    /// A failed request stops the series for this run.
    failed: bool,
}

/// Sync every configured series, day by day (newest first) across all series, so whatever a
/// run manages to fetch before a stop or the transfer limit is complete cross-sections.
pub async fn sync(ctx: &Ctx, cfg: &Tardis, symbols: &[Symbol], only: Option<&str>) -> Outcome {
    let key = cfg.api_key.as_ref();
    log(
        "augment_tardis_mode",
        Obj::new()
            .with("keyed", key.is_some())
            .with("note", key.is_none().then_some(SAMPLES_ONLY)),
    );
    let days = wanted_days(cfg, key.is_some(), ctx.now_ms);
    let mut run = Run::default();
    let mut walks = Vec::new();
    for item in items(cfg, symbols, only) {
        match load(ctx, &item).await {
            Ok(walk) => walks.push(walk),
            Err(error) => {
                run.outcome.errors += 1;
                log_error(&item, "ledger", &error.to_string());
            }
        }
    }
    'days: for &day in &days {
        for walk in &mut walks {
            if ctx.stopping() || run.quota_reached {
                break 'days;
            }
            if let Err(error) = sync_day(ctx, cfg, key, walk, day, &mut run).await {
                run.outcome.errors += 1;
                walk.failed = true;
                log_error(&walk.item, "write", &error.to_string());
            }
        }
    }
    let mut notes = Vec::new();
    if key.is_none() {
        notes.push(SAMPLES_ONLY);
    }
    if run.quota_reached {
        notes.push(QUOTA_REACHED);
    }
    let mut outcome = run.outcome;
    outcome.note = (!notes.is_empty()).then(|| notes.join("; "));
    outcome
}

async fn load(ctx: &Ctx, item: &Item) -> Result<Walk, StoreError> {
    let (dataset, symbol, key) = (item.dataset(), item.symbol.clone(), item.progress_key());
    let (done, progress) = ctx
        .db
        .run(move |store| {
            let done = complete_periods(store, SOURCE, &dataset, &symbol)?;
            Ok((done, get_progress(store, &key)?))
        })
        .await?;
    let absent = progress
        .as_ref()
        .and_then(|p| p.get("absent"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    Ok(Walk {
        item: item.clone(),
        done: done.into_iter().collect(),
        absent,
        refused: false,
        failed: false,
    })
}

/// Fetch one day of one series unless the ledger has it or the run's state rules it out.
async fn sync_day(
    ctx: &Ctx,
    cfg: &Tardis,
    key: Option<&ApiKey>,
    walk: &mut Walk,
    day: NaiveDate,
    run: &mut Run,
) -> Result<(), StoreError> {
    let label = day.format("%Y-%m-%d").to_string();
    if walk.failed || walk.done.contains(&label) || walk.absent.contains(&label) {
        return Ok(());
    }
    let sample = is_sample_day(day);
    let auth = match (sample, key) {
        (true, _) => None,
        (false, None) => return Ok(()),
        (false, Some(key)) => {
            if run.keyed_refused || walk.refused || run.budget_spent {
                return Ok(());
            }
            if !within_budget(ctx, cfg).await {
                run.budget_spent = true;
                return Ok(());
            }
            Some(format!("Bearer {}", key.0))
        }
    };
    let headers: Vec<(&str, &str)> = auth
        .as_deref()
        .map(|value| vec![("Authorization", value)])
        .unwrap_or_default();
    let item = walk.item.clone();
    let url = dataset_url(&cfg.datasets_url, &item, day);
    match ctx.http.get_bytes(&url, &headers).await {
        Ok(fetched) => {
            let rows = write_day(ctx, &item, &label, &fetched.body).await?;
            walk.done.insert(label.clone());
            run.keyed_served |= !sample;
            run.outcome.files += 1;
            run.outcome.rows += u64::try_from(rows).unwrap_or(0);
            log(
                "augment_file",
                Obj::new()
                    .with("source", SOURCE)
                    .with("dataset", item.dataset())
                    .with("symbol", item.symbol.as_str())
                    .with("period", label.as_str())
                    .with("rows", rows)
                    .with("keyed", !sample),
            );
        }
        Err(HttpError::NotFound | HttpError::Status(400)) => {
            if ctx.now_ms - ms_of(day) < (UNPUBLISHED_DAYS + 1) * 86_400_000 {
                return Ok(());
            }
            walk.absent.insert(label);
            let value =
                json!({ "absent": walk.absent.iter().collect::<Vec<_>>(), "updatedAt": now() });
            let key = item.progress_key();
            ctx.db
                .run(move |store| set_progress(store, &key, &value))
                .await?;
        }
        Err(HttpError::Status(429)) => {
            run.quota_reached = true;
            log(
                "augment_tardis_quota",
                Obj::new()
                    .with("dataset", item.dataset())
                    .with("symbol", item.symbol.as_str())
                    .with("period", label.as_str())
                    .with("note", QUOTA_REACHED),
            );
        }
        Err(HttpError::Status(status @ (401 | 403))) => {
            run.outcome.errors += 1;
            if sample {
                walk.failed = true;
                log_error(&item, &label, &format!("HTTP {status}: sample day refused"));
                return Ok(());
            }
            // Refused before any keyed day was served this run (the first keyed request is the
            // newest pending day, whatever the ledger already holds): the key or the plan does
            // not work, so no series asks again this run. Refused after others were served: the
            // plan's history does not reach this day (keyedStart is too early); this series asks
            // for no older keyed day.
            walk.refused = true;
            let what = if !run.keyed_served {
                run.keyed_refused = true;
                "key refused before any keyed day was served; no keyed day is asked for again this run"
            } else {
                "key refused for this day: the plan's history does not reach keyedStart"
            };
            log_error(&item, &label, &format!("HTTP {status}: {what}"));
        }
        Err(error) => {
            run.outcome.errors += 1;
            walk.failed = true;
            log_error(&item, &label, &error.to_string());
        }
    }
    Ok(())
}

/// Whether the source's registered files are under `budgetGb` (keyed days only).
async fn within_budget(ctx: &Ctx, cfg: &Tardis) -> bool {
    let budget = budget_bytes(cfg.budget_gb);
    if budget <= 0 {
        return false;
    }
    let used = ctx
        .db
        .run(|store| {
            let rows = store.rows(
                "SELECT coalesce(sum(bytes), 0) AS bytes FROM files WHERE source = ?",
                &[&SOURCE],
            )?;
            Ok(rows.first().and_then(|row| row.int("bytes")).unwrap_or(0))
        })
        .await
        .unwrap_or(i64::MAX);
    if used < budget {
        return true;
    }
    log(
        "augment_budget_reached",
        Obj::new()
            .with("source", SOURCE)
            .with("usedBytes", used)
            .with("budgetBytes", budget),
    );
    false
}

async fn write_day(ctx: &Ctx, item: &Item, label: &str, body: &[u8]) -> Result<i64, StoreError> {
    let staged = staging_path(&ctx.staging(), "csv.gz");
    std::fs::write(&staged, body)?;
    let from = if gzip_is_empty(body) {
        empty_source(&item.data_type)
    } else {
        read_source(&staged, &item.data_type)
    };
    let target = Target {
        source: SOURCE.into(),
        dataset: item.dataset(),
        symbol: item.symbol.clone(),
        phoenix_symbol: Some(item.phoenix.clone()),
        period: label.to_owned(),
        complete: true,
    };
    let sql = select(
        &item.data_type,
        &from,
        &item.exchange,
        &target.constant_columns(),
    );
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_parquet(store, &root, &sql, &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

fn log_error(item: &Item, at: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", SOURCE)
            .with("dataset", item.dataset())
            .with("symbol", item.symbol.as_str())
            .with("item", at)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Tardis {
        Tardis {
            enabled: true,
            sample_start: "2026-07-15".into(),
            keyed_start: "2026-09-29".into(),
            ..Tardis::default()
        }
    }

    #[test]
    fn days_are_samples_then_keyed_newest_first() {
        // 2026-10-02T07:00Z: October 1st is not exported yet (lag to 08:00 on the 2nd).
        let now = 1_790_924_400_000;
        let days: Vec<String> = wanted_days(&cfg(), false, now)
            .iter()
            .map(|d| d.to_string())
            .collect();
        assert_eq!(days, ["2026-09-01", "2026-08-01"]);
        let later = now + 2 * 3_600_000;
        let keyed: Vec<String> = wanted_days(&cfg(), true, later)
            .iter()
            .map(|d| d.to_string())
            .collect();
        assert_eq!(
            keyed,
            [
                "2026-10-01",
                "2026-09-30",
                "2026-09-29",
                "2026-09-01",
                "2026-08-01"
            ]
        );
        assert!(is_sample_day(parse_date("2026-08-01").unwrap()));
    }

    #[test]
    fn urls_symbols_and_names() {
        let item = Item {
            exchange: "binance-futures".into(),
            data_type: "trades".into(),
            symbol: dataset_symbol("solusdt"),
            phoenix: "SOL".into(),
        };
        assert_eq!(
            dataset_url(
                "https://datasets.tardis.dev/v1/",
                &item,
                parse_date("2026-09-01").unwrap()
            ),
            "https://datasets.tardis.dev/v1/binance-futures/trades/2026/09/01/SOLUSDT.csv.gz"
        );
        assert_eq!(item.dataset(), "binance_futures_trades");
        assert_eq!(item.progress_key(), "tardis/binance_futures_trades/SOLUSDT");
        assert_eq!(dataset_symbol("xyz:NVDA"), "XYZ-NVDA");
        assert_eq!(dataset_symbol("kBONK"), "KBONK");
    }

    #[test]
    fn book_select_thins_and_totals() {
        let sql = select("book_snapshot_25", "src", "bybit", "'X' AS symbol");
        assert!(sql.contains("\"bids[9].amount\" AS bid_amount_9"));
        assert!(!sql.contains("AS bid_amount_10"));
        assert!(sql.contains("coalesce(\"asks[24].amount\", 0) AS ask_amount_25"));
        assert!(sql.contains("PARTITION BY local_timestamp // 1000000"));
        assert_eq!(columns("book_snapshot_25").len(), 4 + 4 * BOOK_LEVELS);
        assert_eq!(columns("trades")[4].0, "id");
        assert!(empty_source("liquidations").contains("NULL::DOUBLE AS \"amount\""));
    }

    #[test]
    fn empty_gzip_is_detected() {
        use std::io::Write;
        let mut empty = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        empty.write_all(b"").unwrap();
        assert!(gzip_is_empty(&empty.finish().unwrap()));
        let mut full = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        full.write_all(b"exchange,symbol\n").unwrap();
        assert!(!gzip_is_empty(&full.finish().unwrap()));
        assert!(gzip_is_empty(b""));
    }

    #[test]
    fn validation_rejects_bad_settings() {
        let mut bad = cfg();
        bad.exchanges.push(TardisExchange {
            id: "binance-futures".into(),
            symbol_field: Some("binance".into()),
            symbols: Default::default(),
            data_types: vec!["incremental_book_L2".into()],
        });
        assert!(validate(&bad).is_err());
        bad.exchanges[0].data_types = vec!["trades".into()];
        assert!(validate(&bad).is_ok());
        bad.exchanges[0].symbol_field = Some("kraken".into());
        assert!(validate(&bad).is_err());
        bad.sample_start = "x".into();
        assert!(validate(&cfg()).is_ok() && validate(&bad).is_err());
        assert_eq!(
            format!("{:?}", ApiKey("secret".into())),
            "ApiKey(<6 chars>)"
        );
    }
}
