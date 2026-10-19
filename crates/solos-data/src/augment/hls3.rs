//! Hyperliquid's requester-pays S3 archive (ADR-0009, exchange dumps 2026-10-19). The requester
//! pays the transfer, so the source is off unless `HL_AWS_ACCESS_KEY_ID` and
//! `HL_AWS_SECRET_ACCESS_KEY` are set, and with them it first runs in `estimateOnly`: it lists
//! the window day by day (`ListObjectsV2`, a fraction of a cent) and reports the bytes and the
//! transfer price per dataset in the status note, downloading nothing. With `estimateOnly` off it
//! downloads, newest UTC day first, bounded by `transferGbPerRun`, `walk.runMinutes` and
//! `walk.budgetGb`:
//!
//! - `l2Book`: `hyperliquid-archive/market_data/<YYYYMMDD>/<H>/l2Book/<COIN>.lz4`, the hour's
//!   book snapshots (JSON lines) of `bookSymbols`, one Parquet day per coin, ten levels a side
//!   plus the level count and size totals.
//! - `asset_ctxs`: `hyperliquid-archive/asset_ctxs/<YYYYMMDD>.csv.lz4`, every coin's context
//!   (funding, open interest, prices), one Parquet day.
//! - `fills`: `hl-mainnet-node-data/node_fills_by_block/hourly/<YYYYMMDD>/<H>.lz4`, every fill of
//!   the chain by block, kept for the mapped coins, one Parquet day per coin.
//!
//! Hyperliquid uploads the archive about once a month with no guarantee, so a day without files
//! younger than `UPLOAD_LAG_DAYS` is asked for again; an older one is recorded as absent.

use super::config::{AwsKeys, HyperliquidS3, Symbol};
use super::http::{HttpError, host_of};
use super::ledger::{complete_periods, get_progress, register, set_progress};
use super::parquet::{Target, staging_path, write_parquet};
use super::periods::{Granularity, Period, parse_date};
use super::series::{Ctx, Outcome, budget_bytes};
use super::sigv4;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::{StoreError, sql_string};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Source name.
pub const SOURCE: &str = "hyperliquid_s3";
/// Days the archive may lag before a day without files counts as absent.
pub const UPLOAD_LAG_DAYS: i64 = 45;
/// Transfer price, USD per GB, out of `us-east-1` (`hyperliquid-archive`) to the internet.
pub const ARCHIVE_USD_PER_GB: f64 = 0.09;
/// Transfer price, USD per GB, out of `ap-northeast-1` (`hl-mainnet-node-data`).
pub const NODE_USD_PER_GB: f64 = 0.114;
/// Book levels kept per side.
pub const BOOK_LEVELS: usize = 10;
/// The status note while the credentials are missing.
pub const NO_KEYS: &str = "HL_AWS_ACCESS_KEY_ID / HL_AWS_SECRET_ACCESS_KEY not set";

/// One bucket.
#[derive(Clone, Debug)]
pub struct Bucket {
    /// Endpoint (`https://<bucket>.s3.<region>.amazonaws.com`).
    pub url: String,
    /// Region, for signing.
    pub region: String,
}

/// One listed object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    /// Key.
    pub key: String,
    /// Size in bytes.
    pub size: i64,
}

/// One page of a `ListObjectsV2` answer.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    /// Objects in order.
    pub objects: Vec<Object>,
    /// The token of the next page, when truncated.
    pub next: Option<String>,
}

/// Parse a `ListBucketResult` (V2).
#[must_use]
pub fn parse_list(xml: &str) -> ListPage {
    let mut page = ListPage::default();
    let mut rest = xml;
    while let Some(start) = rest.find("<Contents>") {
        let after = &rest[start + 10..];
        let Some(end) = after.find("</Contents>") else {
            break;
        };
        let block = &after[..end];
        if let (Some(key), Some(size)) = (tag(block, "Key"), tag(block, "Size")) {
            page.objects.push(Object {
                key: unescape(&key),
                size: size.parse().unwrap_or(0),
            });
        }
        rest = &after[end..];
    }
    if tag(xml, "IsTruncated").as_deref() == Some("true") {
        page.next = tag(xml, "NextContinuationToken").map(|t| unescape(&t));
    }
    page
}

fn tag(xml: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))?;
    Some(xml[start..start + end].to_owned())
}

fn unescape(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// The headers of a signed requester-pays GET of `path` with `query` on `bucket`.
#[must_use]
pub fn signed_headers(
    keys: &AwsKeys,
    bucket: &Bucket,
    path: &str,
    query: &[(String, String)],
    amz_date: &str,
) -> Vec<(String, String)> {
    let mut headers = vec![
        (
            "x-amz-content-sha256".to_owned(),
            sigv4::EMPTY_SHA256.to_owned(),
        ),
        ("x-amz-date".to_owned(), amz_date.to_owned()),
        ("x-amz-request-payer".to_owned(), "requester".to_owned()),
    ];
    if let Some(token) = &keys.session_token {
        headers.push(("x-amz-security-token".to_owned(), token.clone()));
    }
    let request = sigv4::Request {
        method: "GET",
        host: host_of(&bucket.url),
        path,
        query,
        headers: &headers,
        amz_date,
        region: &bucket.region,
    };
    let authorization =
        sigv4::authorization(&keys.access_key_id, &keys.secret_access_key, &request);
    headers.push(("authorization".to_owned(), authorization));
    headers
}

async fn signed_get(
    ctx: &Ctx,
    keys: &AwsKeys,
    bucket: &Bucket,
    path: &str,
    query: &[(String, String)],
) -> Result<Vec<u8>, HttpError> {
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let headers = signed_headers(keys, bucket, path, query, &amz_date);
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut url = format!(
        "{}{}",
        bucket.url.trim_end_matches('/'),
        sigv4::uri_encode(path, true)
    );
    if !query.is_empty() {
        url.push('?');
        url.push_str(&sigv4::canonical_query(query));
    }
    Ok(ctx.http.get_bytes(&url, &pairs).await?.body)
}

/// Every object under `prefix`.
pub async fn list(
    ctx: &Ctx,
    keys: &AwsKeys,
    bucket: &Bucket,
    prefix: &str,
) -> Result<Vec<Object>, HttpError> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut query = vec![
            ("list-type".to_owned(), "2".to_owned()),
            ("prefix".to_owned(), prefix.to_owned()),
        ];
        if let Some(t) = &token {
            query.push(("continuation-token".to_owned(), t.clone()));
        }
        let body = signed_get(ctx, keys, bucket, "/", &query).await?;
        let page = parse_list(&String::from_utf8_lossy(&body));
        out.extend(page.objects);
        match page.next {
            Some(next) if Some(&next) != token.as_ref() => token = Some(next),
            _ => return Ok(out),
        }
    }
}

/// The magic number every lz4 frame starts with.
pub const LZ4_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

/// Decompress an lz4 frame.
pub fn unlz4(bytes: &[u8]) -> Result<Vec<u8>, StoreError> {
    if !bytes.starts_with(&LZ4_MAGIC) {
        return Err(StoreError::Check("lz4: not an lz4 frame".into()));
    }
    let mut decoder =
        lz4::Decoder::new(bytes).map_err(|e| StoreError::Check(format!("lz4: {e}")))?;
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Check(format!("lz4: {e}")))?;
    Ok(out)
}

/// What one day holds, from its listings.
#[derive(Clone, Debug, Default)]
pub struct Day {
    /// `l2Book` objects per coin.
    pub books: BTreeMap<String, Vec<Object>>,
    /// The day's asset-context object.
    pub contexts: Option<Object>,
    /// Hourly fill objects.
    pub fills: Vec<Object>,
}

impl Day {
    /// Bytes per dataset (`l2Book` for the wanted coins only).
    #[must_use]
    pub fn sizes(&self) -> (i64, i64, i64) {
        (
            self.books.values().flatten().map(|o| o.size).sum(),
            self.contexts.as_ref().map_or(0, |o| o.size),
            self.fills.iter().map(|o| o.size).sum(),
        )
    }

    /// Whether the archive has nothing for the day.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.books.is_empty() && self.contexts.is_none() && self.fills.is_empty()
    }
}

/// Sort a day's listings: `l2Book` keys of the wanted coins, the asset contexts, the fills.
#[must_use]
pub fn classify(
    compact: &str,
    market: &[Object],
    contexts: &[Object],
    fills: &[Object],
    coins: &BTreeSet<String>,
) -> Day {
    let mut day = Day::default();
    for object in market {
        let parts: Vec<&str> = object.key.split('/').collect();
        if let ["market_data", date, _hour, "l2Book", file] = parts.as_slice()
            && *date == compact
            && let Some(coin) = file.strip_suffix(".lz4")
            && coins.contains(coin)
        {
            day.books
                .entry(coin.to_owned())
                .or_default()
                .push(object.clone());
        }
    }
    day.contexts = contexts
        .iter()
        .find(|o| o.key == format!("asset_ctxs/{compact}.csv.lz4"))
        .cloned();
    day.fills = fills
        .iter()
        .filter(|o| o.key.ends_with(".lz4"))
        .cloned()
        .collect();
    day
}

/// One `l2Book` line as a flat row: `time_ms`, ten levels a side, level counts and size totals.
/// Accepts the archive's `{"raw": {"data": {...}}}` envelope or a bare `data` object.
#[must_use]
pub fn book_row(line: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(line).ok()?;
    let data = value
        .pointer("/raw/data")
        .or_else(|| value.get("data"))
        .unwrap_or(&value);
    let time = data.get("time").and_then(Value::as_i64)?;
    let levels = data.get("levels").and_then(Value::as_array)?;
    let mut row = serde_json::Map::new();
    row.insert("time_ms".into(), json!(time));
    for (side, short) in [(0, "bid"), (1, "ask")] {
        let side_levels = levels.get(side).and_then(Value::as_array)?;
        let mut total = 0.0;
        for (i, level) in side_levels.iter().enumerate() {
            let px = number(level.get("px"));
            let sz = number(level.get("sz"));
            total += sz.unwrap_or(0.0);
            if i < BOOK_LEVELS {
                row.insert(format!("{short}_px_{i}"), json!(px));
                row.insert(format!("{short}_sz_{i}"), json!(sz));
                row.insert(
                    format!("{short}_n_{i}"),
                    json!(level.get("n").and_then(Value::as_i64)),
                );
            }
        }
        row.insert(format!("{short}_levels"), json!(side_levels.len()));
        row.insert(format!("{short}_sz_total"), json!(total));
    }
    Some(Value::Object(row))
}

fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// The DuckDB columns of a flattened book row.
#[must_use]
pub fn book_columns() -> Vec<(String, &'static str)> {
    let mut cols = vec![("time_ms".to_owned(), "BIGINT")];
    for short in ["bid", "ask"] {
        for i in 0..BOOK_LEVELS {
            cols.push((format!("{short}_px_{i}"), "DOUBLE"));
            cols.push((format!("{short}_sz_{i}"), "DOUBLE"));
            cols.push((format!("{short}_n_{i}"), "BIGINT"));
        }
        cols.push((format!("{short}_levels"), "BIGINT"));
        cols.push((format!("{short}_sz_total"), "DOUBLE"));
    }
    cols
}

/// The fills of one node line kept for `coins`, flattened. A line is a block
/// (`{"block_number", "block_time", "events": [[user, fill], …]}`) or, in the older format, one
/// `[user, fill]` pair.
#[must_use]
pub fn fill_rows(line: &str, coins: &BTreeSet<String>) -> Vec<Value> {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return Vec::new();
    };
    let (events, block_number, block_time) = match &value {
        Value::Object(block) => (
            block
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            block.get("block_number").and_then(Value::as_i64),
            block
                .get("block_time")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ),
        Value::Array(_) => (vec![value.clone()], None, None),
        _ => return Vec::new(),
    };
    events
        .iter()
        .filter_map(|event| {
            let user = event.get(0).and_then(Value::as_str)?;
            let fill = event.get(1)?;
            let coin = fill.get("coin").and_then(Value::as_str)?;
            if !coins.contains(coin) {
                return None;
            }
            let s = |k: &str| fill.get(k).and_then(Value::as_str);
            Some(json!({
                "time_ms": fill.get("time").and_then(Value::as_i64),
                "coin": coin,
                "user": user,
                "px": number(fill.get("px")),
                "sz": number(fill.get("sz")),
                "side": s("side"),
                "dir": s("dir"),
                "start_position": number(fill.get("startPosition")),
                "closed_pnl": number(fill.get("closedPnl")),
                "fee": number(fill.get("fee")),
                "fee_token": s("feeToken"),
                "crossed": fill.get("crossed").and_then(Value::as_bool),
                "oid": fill.get("oid").and_then(Value::as_i64),
                "tid": fill.get("tid").and_then(Value::as_i64),
                "hash": s("hash"),
                "liquidation": fill.get("liquidation").filter(|v| !v.is_null()).map(Value::to_string),
                "block_number": block_number,
                "block_time": block_time,
            }))
        })
        .collect()
}

/// The DuckDB columns of a flattened fill row.
pub const FILL_COLUMNS: &[(&str, &str)] = &[
    ("time_ms", "BIGINT"),
    ("coin", "VARCHAR"),
    ("user", "VARCHAR"),
    ("px", "DOUBLE"),
    ("sz", "DOUBLE"),
    ("side", "VARCHAR"),
    ("dir", "VARCHAR"),
    ("start_position", "DOUBLE"),
    ("closed_pnl", "DOUBLE"),
    ("fee", "DOUBLE"),
    ("fee_token", "VARCHAR"),
    ("crossed", "BOOLEAN"),
    ("oid", "BIGINT"),
    ("tid", "BIGINT"),
    ("hash", "VARCHAR"),
    ("liquidation", "VARCHAR"),
    ("block_number", "BIGINT"),
    ("block_time", "VARCHAR"),
];

fn ndjson_source(path: &Path, columns: &[(String, &str)]) -> String {
    let map: Vec<String> = columns
        .iter()
        .map(|(name, kind)| format!("{}: '{kind}'", sql_string(name)))
        .collect();
    format!(
        "read_json({}, format='newline_delimited', columns={{{}}})",
        sql_string(&path.to_string_lossy()),
        map.join(", ")
    )
}

/// The cost of `bytes` out of a bucket at `usd_per_gb`.
#[must_use]
pub fn transfer_usd(bytes: i64, usd_per_gb: f64) -> f64 {
    bytes as f64 / 1e9 * usd_per_gb
}

struct Plan<'a> {
    cfg: &'a HyperliquidS3,
    keys: &'a AwsKeys,
    archive: Bucket,
    node: Bucket,
    /// Hyperliquid coins of the mapped markets of the walk's kinds, with their Phoenix symbols.
    coins: BTreeMap<String, String>,
    /// The `bookSymbols` coins.
    book_coins: BTreeSet<String>,
}

/// Sync the source.
pub async fn sync(
    ctx: &Ctx,
    cfg: &HyperliquidS3,
    symbols: &[Symbol],
    only: Option<&str>,
) -> Outcome {
    let Some(keys) = cfg.keys.as_ref() else {
        log(
            "augment_source_disabled",
            Obj::new().with("source", SOURCE).with("reason", NO_KEYS),
        );
        return Outcome::disabled(NO_KEYS);
    };
    let mut coins = BTreeMap::new();
    for symbol in symbols {
        let Some(coin) = symbol.hyperliquid.as_deref() else {
            continue;
        };
        if cfg.walk.kinds.contains(&symbol.kind)
            && only.is_none_or(|s| s == symbol.phoenix || s == coin)
        {
            coins.insert(coin.to_owned(), symbol.phoenix.clone());
        }
    }
    let book_coins = symbols
        .iter()
        .filter(|s| cfg.book_symbols.contains(&s.phoenix))
        .filter_map(|s| s.hyperliquid.clone())
        .filter(|c| coins.contains_key(c))
        .collect();
    let plan = Plan {
        cfg,
        keys,
        archive: Bucket {
            url: cfg.archive_url.clone(),
            region: cfg.archive_region.clone(),
        },
        node: Bucket {
            url: cfg.node_url.clone(),
            region: cfg.node_region.clone(),
        },
        coins,
        book_coins,
    };
    if cfg.estimate_only {
        estimate(ctx, &plan).await
    } else {
        download(ctx, &plan).await
    }
}

fn days(cfg: &HyperliquidS3, now_ms: i64) -> Vec<Period> {
    parse_date(&cfg.walk.start_date)
        .map(|start| super::dumps::wanted(start, Granularity::Day, now_ms))
        .unwrap_or_default()
}

async fn list_day(ctx: &Ctx, plan: &Plan<'_>, period: &Period) -> Result<Day, HttpError> {
    let compact = period.start.format("%Y%m%d").to_string();
    let market = if plan.book_coins.is_empty() {
        Vec::new()
    } else {
        list(
            ctx,
            plan.keys,
            &plan.archive,
            &format!("market_data/{compact}/"),
        )
        .await?
    };
    let contexts = list(
        ctx,
        plan.keys,
        &plan.archive,
        &format!("asset_ctxs/{compact}"),
    )
    .await?;
    let fills = list(
        ctx,
        plan.keys,
        &plan.node,
        &format!("node_fills_by_block/hourly/{compact}/"),
    )
    .await?;
    Ok(classify(
        &compact,
        &market,
        &contexts,
        &fills,
        &plan.book_coins,
    ))
}

/// List the window (days not listed before) and price it; download nothing.
async fn estimate(ctx: &Ctx, plan: &Plan<'_>) -> Outcome {
    let mut outcome = Outcome::default();
    let deadline = Instant::now() + Duration::from_secs(plan.cfg.walk.run_minutes * 60);
    let key = format!("{SOURCE}/estimate");
    let lookup = key.clone();
    let mut sizes: BTreeMap<String, Value> =
        match ctx.db.run(move |s| get_progress(s, &lookup)).await {
            Ok(p) => p
                .and_then(|p| p.get("days").and_then(Value::as_object).cloned())
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            Err(error) => {
                outcome.errors += 1;
                log_error("estimate", "ledger", &error.to_string());
                return outcome;
            }
        };
    let books: Vec<String> = plan.book_coins.iter().cloned().collect();
    for period in days(plan.cfg, ctx.now_ms) {
        let label = period.label();
        if sizes.get(&label).is_some_and(|v| {
            v.get("books").and_then(Value::as_array).is_some_and(|b| {
                b.iter()
                    .filter_map(Value::as_str)
                    .eq(books.iter().map(String::as_str))
            })
        }) {
            continue;
        }
        if ctx.stopping() || Instant::now() >= deadline {
            break;
        }
        match list_day(ctx, plan, &period).await {
            Ok(day) if day.is_empty() && recent(&period, ctx.now_ms) => {}
            Ok(day) => {
                let (book, contexts, fills) = day.sizes();
                outcome.requests += 1;
                sizes.insert(
                    label,
                    json!({ "l2Book": book, "asset_ctxs": contexts, "fills": fills, "books": books }),
                );
            }
            Err(error) => {
                outcome.errors += 1;
                log_error(&label, "listing", &error.to_string());
                break;
            }
        }
    }
    let total = |name: &str| -> i64 {
        sizes
            .values()
            .filter_map(|v| v.get(name).and_then(Value::as_i64))
            .sum()
    };
    let (book, contexts, fills) = (total("l2Book"), total("asset_ctxs"), total("fills"));
    let enabled = (
        if plan.cfg.l2_book { book } else { 0 },
        if plan.cfg.asset_ctxs { contexts } else { 0 },
        if plan.cfg.fills { fills } else { 0 },
    );
    let usd = transfer_usd(enabled.0 + enabled.1, ARCHIVE_USD_PER_GB)
        + transfer_usd(enabled.2, NODE_USD_PER_GB);
    let note = format!(
        "estimateOnly: {} days listed; l2Book {:.1} GB, asset_ctxs {:.1} GB, fills {:.1} GB; enabled datasets about {:.1} GB, ${usd:.2} transfer",
        sizes.len(),
        book as f64 / 1e9,
        contexts as f64 / 1e9,
        fills as f64 / 1e9,
        (enabled.0 + enabled.1 + enabled.2) as f64 / 1e9,
    );
    log(
        "augment_hl_s3_estimate",
        Obj::new()
            .with("days", sizes.len())
            .with("l2BookBytes", book)
            .with("assetCtxsBytes", contexts)
            .with("fillsBytes", fills)
            .with("enabledUsd", usd),
    );
    let value = json!({ "days": sizes, "updatedAt": now() });
    if let Err(error) = ctx.db.run(move |s| set_progress(s, &key, &value)).await {
        outcome.errors += 1;
        log_error("estimate", "ledger", &error.to_string());
    }
    outcome.note = Some(note);
    outcome
}

fn recent(period: &Period, now_ms: i64) -> bool {
    now_ms - period.end_ms() < UPLOAD_LAG_DAYS * 86_400_000
}

struct Progress {
    done: BTreeMap<String, HashSet<String>>,
    absent: BTreeSet<String>,
    fills_done: BTreeSet<String>,
    transferred: i64,
    used: i64,
    stop: Option<&'static str>,
}

/// Download the window newest day first.
async fn download(ctx: &Ctx, plan: &Plan<'_>) -> Outcome {
    let mut outcome = Outcome::default();
    let mut state = match load(ctx, plan).await {
        Ok(state) => state,
        Err(error) => {
            outcome.errors += 1;
            log_error("download", "ledger", &error.to_string());
            return outcome;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(plan.cfg.walk.run_minutes * 60);
    let cap = budget_bytes(plan.cfg.transfer_gb_per_run);
    let budget = budget_bytes(plan.cfg.walk.budget_gb);
    for period in days(plan.cfg, ctx.now_ms) {
        let label = period.label();
        if state.absent.contains(&label) || !wants_day(plan, &state, &label) {
            continue;
        }
        if ctx.stopping() {
            break;
        }
        if Instant::now() >= deadline {
            state.stop = Some(super::dumps::YIELDED);
            break;
        }
        if state.used >= budget {
            state.stop = Some(super::dumps::BUDGET_REACHED);
            break;
        }
        let day = match list_day(ctx, plan, &period).await {
            Ok(day) => day,
            Err(error) => {
                outcome.errors += 1;
                log_error(&label, "listing", &error.to_string());
                break;
            }
        };
        if day.is_empty() {
            if !recent(&period, ctx.now_ms) {
                state.absent.insert(label.clone());
                if let Err(error) = save_absent(ctx, &state.absent).await {
                    outcome.errors += 1;
                    log_error(&label, "ledger", &error.to_string());
                    break;
                }
            }
            continue;
        }
        let (book, contexts, fills) = day.sizes();
        let need = if plan.cfg.l2_book { book } else { 0 }
            + if plan.cfg.asset_ctxs { contexts } else { 0 }
            + if plan.cfg.fills { fills } else { 0 };
        if state.transferred + need > cap {
            state.stop = Some("transferGbPerRun reached");
            break;
        }
        if let Err(error) = fetch_day(ctx, plan, &period, &day, &mut state, &mut outcome).await {
            outcome.errors += 1;
            log_error(&label, "day", &error.to_string());
            break;
        }
    }
    outcome.note = state.stop.map(str::to_owned);
    outcome
}

fn wants_day(plan: &Plan<'_>, state: &Progress, label: &str) -> bool {
    let missing = |dataset: &str, symbol: &str| {
        !state
            .done
            .get(&format!("{dataset}/{symbol}"))
            .is_some_and(|d| d.contains(label))
    };
    (plan.cfg.l2_book && plan.book_coins.iter().any(|c| missing("l2Book", c)))
        || (plan.cfg.asset_ctxs && missing("asset_ctxs", "ALL"))
        || (plan.cfg.fills && !state.fills_done.contains(label))
}

async fn load(ctx: &Ctx, plan: &Plan<'_>) -> Result<Progress, StoreError> {
    let mut series: Vec<(String, String)> = plan
        .book_coins
        .iter()
        .map(|c| ("l2Book".to_owned(), c.clone()))
        .collect();
    series.push(("asset_ctxs".into(), "ALL".into()));
    ctx.db
        .run(move |store| {
            let mut done = BTreeMap::new();
            for (dataset, symbol) in &series {
                let periods = complete_periods(store, SOURCE, dataset, symbol)?;
                done.insert(format!("{dataset}/{symbol}"), periods.into_iter().collect());
            }
            let list = |key: &str, field: &str| -> Result<BTreeSet<String>, StoreError> {
                Ok(get_progress(store, key)?
                    .and_then(|p| p.get(field).and_then(Value::as_array).cloned())
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect())
            };
            let used = store
                .rows(
                    "SELECT coalesce(sum(bytes), 0) AS bytes FROM files WHERE source = ?",
                    &[&SOURCE],
                )?
                .first()
                .and_then(|row| row.int("bytes"))
                .unwrap_or(0);
            Ok(Progress {
                done,
                absent: list(&format!("{SOURCE}/days"), "absent")?,
                fills_done: list(&format!("{SOURCE}/fills"), "done")?,
                transferred: 0,
                used,
                stop: None,
            })
        })
        .await
}

async fn save_absent(ctx: &Ctx, absent: &BTreeSet<String>) -> Result<(), StoreError> {
    let value = json!({ "absent": absent.iter().collect::<Vec<_>>(), "updatedAt": now() });
    ctx.db
        .run(move |store| set_progress(store, &format!("{SOURCE}/days"), &value))
        .await
}

async fn fetch_day(
    ctx: &Ctx,
    plan: &Plan<'_>,
    period: &Period,
    day: &Day,
    state: &mut Progress,
    outcome: &mut Outcome,
) -> Result<(), StoreError> {
    let label = period.label();
    if plan.cfg.l2_book {
        for (coin, objects) in &day.books {
            let key = format!("l2Book/{coin}");
            if state.done.get(&key).is_some_and(|d| d.contains(&label)) {
                continue;
            }
            let staged = staging_path(&ctx.staging(), "jsonl");
            let mut file = std::fs::File::create(&staged)?;
            let mut lines = 0usize;
            for object in objects {
                let body = get_object(ctx, plan, false, object, state).await?;
                for line in String::from_utf8_lossy(&body).lines() {
                    if let Some(row) = book_row(line) {
                        writeln!(file, "{row}")?;
                        lines += 1;
                    }
                }
            }
            drop(file);
            if lines == 0 {
                let _ = std::fs::remove_file(&staged);
                return Err(StoreError::Check(format!(
                    "{coin} {label}: no l2Book row parsed"
                )));
            }
            let select = format!(
                "SELECT time_ms, make_timestamp(time_ms * 1000) AS ts, * EXCLUDE (time_ms) FROM {} ORDER BY time_ms",
                ndjson_source(&staged, &book_columns())
            );
            let phoenix = plan.coins.get(coin).cloned();
            write(
                ctx, &staged, "l2Book", coin, phoenix, &label, &select, state, outcome,
            )
            .await?;
            state.done.entry(key).or_default().insert(label.clone());
        }
    }
    if plan.cfg.asset_ctxs
        && let Some(object) = &day.contexts
        && !state
            .done
            .get("asset_ctxs/ALL")
            .is_some_and(|d| d.contains(&label))
    {
        let body = get_object(ctx, plan, false, object, state).await?;
        let staged = staging_path(&ctx.staging(), "csv");
        std::fs::write(&staged, body)?;
        let select = format!(
            "SELECT * FROM read_csv({}, header=true, auto_detect=true)",
            sql_string(&staged.to_string_lossy())
        );
        write(
            ctx,
            &staged,
            "asset_ctxs",
            "ALL",
            None,
            &label,
            &select,
            state,
            outcome,
        )
        .await?;
        state
            .done
            .entry("asset_ctxs/ALL".into())
            .or_default()
            .insert(label.clone());
    }
    if plan.cfg.fills && !day.fills.is_empty() && !state.fills_done.contains(&label) {
        fetch_fills(ctx, plan, &label, &day.fills, state, outcome).await?;
    }
    Ok(())
}

async fn get_object(
    ctx: &Ctx,
    plan: &Plan<'_>,
    node: bool,
    object: &Object,
    state: &mut Progress,
) -> Result<Vec<u8>, StoreError> {
    let bucket = if node { &plan.node } else { &plan.archive };
    let body = signed_get(ctx, plan.keys, bucket, &format!("/{}", object.key), &[]).await?;
    state.transferred += i64::try_from(body.len()).unwrap_or(i64::MAX);
    tokio::task::spawn_blocking(move || unlz4(&body))
        .await
        .map_err(|e| StoreError::Check(format!("lz4 task: {e}")))?
}

async fn fetch_fills(
    ctx: &Ctx,
    plan: &Plan<'_>,
    label: &str,
    objects: &[Object],
    state: &mut Progress,
    outcome: &mut Outcome,
) -> Result<(), StoreError> {
    let coins: BTreeSet<String> = plan.coins.keys().cloned().collect();
    let staged = staging_path(&ctx.staging(), "jsonl");
    let mut file = std::io::BufWriter::new(std::fs::File::create(&staged)?);
    let mut kept: BTreeSet<String> = BTreeSet::new();
    for object in objects {
        let body = get_object(ctx, plan, true, object, state).await?;
        for line in String::from_utf8_lossy(&body).lines() {
            for row in fill_rows(line, &coins) {
                if let Some(coin) = row.get("coin").and_then(Value::as_str) {
                    kept.insert(coin.to_owned());
                }
                writeln!(file, "{row}")?;
            }
        }
    }
    file.flush()?;
    drop(file);
    let columns: Vec<(String, &str)> = FILL_COLUMNS
        .iter()
        .map(|(n, k)| ((*n).to_owned(), *k))
        .collect();
    let source = ndjson_source(&staged, &columns);
    for coin in &kept {
        let select = format!(
            "SELECT time_ms, make_timestamp(time_ms * 1000) AS ts, * EXCLUDE (time_ms) FROM {source} WHERE coin = {} ORDER BY time_ms, tid",
            sql_string(coin)
        );
        let phoenix = plan.coins.get(coin).cloned();
        write_keep(ctx, "fills", coin, phoenix, label, &select, state, outcome).await?;
    }
    let _ = std::fs::remove_file(&staged);
    state.fills_done.insert(label.to_owned());
    let value = json!({ "done": state.fills_done.iter().collect::<Vec<_>>(), "updatedAt": now() });
    ctx.db
        .run(move |store| set_progress(store, &format!("{SOURCE}/fills"), &value))
        .await
}

#[allow(clippy::too_many_arguments)]
async fn write(
    ctx: &Ctx,
    staged: &PathBuf,
    dataset: &str,
    symbol: &str,
    phoenix: Option<String>,
    label: &str,
    select: &str,
    state: &mut Progress,
    outcome: &mut Outcome,
) -> Result<(), StoreError> {
    let result = write_keep(ctx, dataset, symbol, phoenix, label, select, state, outcome).await;
    let _ = std::fs::remove_file(staged);
    result
}

#[allow(clippy::too_many_arguments)]
async fn write_keep(
    ctx: &Ctx,
    dataset: &str,
    symbol: &str,
    phoenix: Option<String>,
    label: &str,
    select: &str,
    state: &mut Progress,
    outcome: &mut Outcome,
) -> Result<(), StoreError> {
    let target = Target {
        source: SOURCE.into(),
        dataset: dataset.into(),
        symbol: symbol.into(),
        phoenix_symbol: phoenix,
        period: label.into(),
        complete: true,
    };
    let sql = format!("SELECT *, {} FROM ({select})", target.constant_columns());
    let root = ctx.root.clone();
    let (rows, bytes) = ctx
        .db
        .run(move |store| {
            let record = write_parquet(store, &root, &sql, &target)?;
            register(store, &record, None)?;
            Ok((record.row_count, record.bytes))
        })
        .await?;
    state.used += bytes;
    outcome.files += 1;
    outcome.rows += u64::try_from(rows).unwrap_or(0);
    log(
        "augment_file",
        Obj::new()
            .with("source", SOURCE)
            .with("dataset", dataset)
            .with("symbol", symbol)
            .with("period", label)
            .with("rows", rows)
            .with("complete", true),
    );
    Ok(())
}

fn log_error(item: &str, what: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", SOURCE)
            .with("dataset", what)
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_and_classification() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>a&amp;b</NextContinuationToken>\
            <Contents><Key>market_data/20251001/0/l2Book/BTC.lz4</Key><Size>100</Size></Contents>\
            <Contents><Key>market_data/20251001/1/l2Book/BTC.lz4</Key><Size>50</Size></Contents>\
            <Contents><Key>market_data/20251001/0/l2Book/DOGE.lz4</Key><Size>7</Size></Contents>\
            <Contents><Key>market_data/20251001/0/trades/BTC.lz4</Key><Size>9</Size></Contents></ListBucketResult>";
        let page = parse_list(xml);
        assert_eq!(page.objects.len(), 4);
        assert_eq!(page.next.as_deref(), Some("a&b"));
        let coins: BTreeSet<String> = ["BTC".to_owned()].into_iter().collect();
        let contexts = vec![Object {
            key: "asset_ctxs/20251001.csv.lz4".into(),
            size: 3,
        }];
        let fills = vec![Object {
            key: "node_fills_by_block/hourly/20251001/0.lz4".into(),
            size: 11,
        }];
        let day = classify("20251001", &page.objects, &contexts, &fills, &coins);
        assert_eq!(day.books.get("BTC").map(Vec::len), Some(2));
        assert_eq!(day.sizes(), (150, 3, 11));
        assert!(!day.is_empty());
        assert!(
            parse_list("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
                .next
                .is_none()
        );
    }

    #[test]
    fn book_lines_flatten() {
        let line = r#"{"time":"2025-10-01T00:00:00.5","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"BTC","time":1759276800500,"levels":[[{"px":"100.5","sz":"2","n":3},{"px":"100","sz":"1.5","n":1}],[{"px":"101","sz":"4","n":2}]]}}}"#;
        let row = book_row(line).unwrap();
        assert_eq!(row["time_ms"], 1_759_276_800_500_i64);
        assert_eq!(row["bid_px_0"], 100.5);
        assert_eq!(row["bid_sz_total"], 3.5);
        assert_eq!(row["ask_levels"], 1);
        assert!(row["ask_px_1"].is_null() || row.get("ask_px_1").is_none());
        assert!(book_row("not json").is_none());
        assert_eq!(book_columns().len(), 1 + 2 * (BOOK_LEVELS * 3 + 2));
    }

    #[test]
    fn fill_lines_flatten_and_filter() {
        let coins: BTreeSet<String> = ["SOL".to_owned()].into_iter().collect();
        let block = r#"{"local_time":"x","block_time":"2025-10-01T00:00:01","block_number":7,"events":[["0xa",{"coin":"SOL","px":"200.1","sz":"3","side":"B","time":1759276801000,"startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"0xh","oid":1,"crossed":true,"fee":"0.01","tid":9,"feeToken":"USDC"}],["0xb",{"coin":"BTC","px":"1","sz":"1","side":"A","time":1}]]}"#;
        let rows = fill_rows(block, &coins);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["px"], 200.1);
        assert_eq!(rows[0]["block_number"], 7);
        assert_eq!(rows[0]["user"], "0xa");
        let old = r#"["0xc",{"coin":"SOL","px":"1","sz":"1","side":"A","time":5,"tid":1}]"#;
        assert_eq!(fill_rows(old, &coins).len(), 1);
        assert!(fill_rows("{}", &coins).is_empty());
    }

    #[test]
    fn lz4_and_prices() {
        let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
        encoder.write_all(b"hello\n").unwrap();
        let (frame, result) = encoder.finish();
        result.unwrap();
        assert_eq!(unlz4(&frame).unwrap(), b"hello\n");
        assert!(unlz4(b"nope").is_err());
        assert!((transfer_usd(100_000_000_000, ARCHIVE_USD_PER_GB) - 9.0).abs() < 1e-9);
    }

    #[test]
    fn signed_headers_carry_the_payer() {
        let keys = AwsKeys {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        };
        let bucket = Bucket {
            url: "https://hyperliquid-archive.s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
        };
        let headers = signed_headers(
            &keys,
            &bucket,
            "/asset_ctxs/20251001.csv.lz4",
            &[],
            "20251002T000000Z",
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "x-amz-request-payer" && v == "requester")
        );
        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        assert!(
            auth.contains(
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-request-payer,"
            )
        );
        assert!(!auth.contains("secret"));
    }
}
