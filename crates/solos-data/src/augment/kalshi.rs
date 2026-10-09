//! Kalshi's public read endpoints (no key): the curated series are expanded into their markets
//! since the start date; each market is a paged history of hourly candlesticks cut into month
//! files, and the market catalogue is merged into the day's `markets` file.

use super::config::Kalshi;
use super::http::{Http, with_query};
use super::ledger::register;
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome, RowsFuture, Series, finished, sync_series};
use crate::jsonout::{Obj, log, safe_error};
use crate::store::{StoreError, sql_string};
use serde_json::{Value, json};
use std::path::Path;

/// Markets per listing page.
pub const PAGE: usize = 200;

/// The hourly candlesticks of one market.
pub struct Candles {
    /// API base.
    pub base_url: String,
    /// Series ticker (`KXFED`).
    pub series_ticker: String,
    /// Market ticker (`KXFED-27APR-T6.00`), the directory name.
    pub ticker: String,
    /// Event ticker and market title.
    pub event_ticker: String,
    /// Market title.
    pub title: String,
    /// Whether the market is past trading, and when it closed.
    pub settled: bool,
    /// `close_time` in milliseconds, when parseable.
    pub close_ms: Option<i64>,
}

impl Candles {
    /// A market from its listing object.
    #[must_use]
    pub fn from_market(base_url: &str, series_ticker: &str, market: &Value) -> Option<Candles> {
        let status = market.get("status").and_then(Value::as_str).unwrap_or("");
        Some(Candles {
            base_url: base_url.trim_end_matches('/').to_owned(),
            series_ticker: series_ticker.to_owned(),
            ticker: market.get("ticker")?.as_str()?.to_owned(),
            event_ticker: market
                .get("event_ticker")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            title: market
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            settled: !matches!(status, "active" | "open" | "initialized"),
            close_ms: market
                .get("close_time")
                .and_then(Value::as_str)
                .and_then(|d| chrono::DateTime::parse_from_rfc3339(d).ok())
                .map(|d| d.timestamp_millis()),
        })
    }
}

fn dollars(value: Option<&Value>, key: &str) -> Value {
    value
        .and_then(|v| v.get(key))
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .map_or(Value::Null, Value::from)
}

/// One candlestick as a flat row keyed by the hour it covers (`end_period_ts` minus one hour).
#[must_use]
pub fn candle_row(candle: &Value) -> Option<Value> {
    let end = candle.get("end_period_ts")?.as_i64()?;
    let side = |name: &str, key: &str| dollars(candle.get(name), key);
    let number = |name: &str| {
        candle
            .get(name)
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
    };
    Some(json!({
        "t": end - 3600, "end_t": end,
        "yes_bid_open": side("yes_bid", "open_dollars"), "yes_bid_high": side("yes_bid", "high_dollars"),
        "yes_bid_low": side("yes_bid", "low_dollars"), "yes_bid_close": side("yes_bid", "close_dollars"),
        "yes_ask_open": side("yes_ask", "open_dollars"), "yes_ask_high": side("yes_ask", "high_dollars"),
        "yes_ask_low": side("yes_ask", "low_dollars"), "yes_ask_close": side("yes_ask", "close_dollars"),
        "price_open": side("price", "open_dollars"), "price_high": side("price", "high_dollars"),
        "price_low": side("price", "low_dollars"), "price_close": side("price", "close_dollars"),
        "volume": number("volume_fp"), "open_interest": number("open_interest_fp"),
    }))
}

impl Series for Candles {
    fn source(&self) -> &str {
        "kalshi"
    }
    fn dataset(&self) -> &str {
        "candles_1h"
    }
    fn symbol(&self) -> &str {
        &self.ticker
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        None
    }
    fn granularity(&self) -> Granularity {
        Granularity::Month
    }
    fn lag_ms(&self) -> i64 {
        3_600_000
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let url = with_query(
                &format!(
                    "{}/series/{}/markets/{}/candlesticks",
                    self.base_url, self.series_ticker, self.ticker
                ),
                &[
                    ("start_ts", (start_ms / 1000).to_string()),
                    ("end_ts", (end_ms / 1000).to_string()),
                    ("period_interval", "60".into()),
                ],
            );
            let answer = http.get_json(&url, &[]).await?;
            let candles = answer
                .get("candlesticks")
                .and_then(Value::as_array)
                .ok_or_else(|| StoreError::Check("answer has no candlesticks".into()))?;
            let mut rows: Vec<Value> = candles
                .iter()
                .filter_map(candle_row)
                .filter(|row| {
                    let ms = row.get("t").and_then(Value::as_i64).unwrap_or(0) * 1000;
                    ms >= start_ms && ms < end_ms
                })
                .collect();
            rows.sort_by_key(|r| r.get("t").and_then(Value::as_i64).unwrap_or(0));
            rows.dedup_by_key(|r| r.get("t").and_then(Value::as_i64).unwrap_or(0));
            Ok(rows)
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT t AS time_s, make_timestamp(t * 1000000) AS ts, end_t AS end_time_s,
                    yes_bid_open, yes_bid_high, yes_bid_low, yes_bid_close, yes_ask_open, yes_ask_high, yes_ask_low, yes_ask_close,
                    price_open, price_high, price_low, price_close, volume, open_interest,
                    {} AS series_ticker, {} AS event_ticker, {} AS title, {constants}
             FROM {} ORDER BY t",
            sql_string(&self.series_ticker),
            sql_string(&self.event_ticker),
            sql_string(&self.title),
            read_json_array(
                staged,
                &[
                    ("t", "BIGINT"), ("end_t", "BIGINT"),
                    ("yes_bid_open", "DOUBLE"), ("yes_bid_high", "DOUBLE"), ("yes_bid_low", "DOUBLE"), ("yes_bid_close", "DOUBLE"),
                    ("yes_ask_open", "DOUBLE"), ("yes_ask_high", "DOUBLE"), ("yes_ask_low", "DOUBLE"), ("yes_ask_close", "DOUBLE"),
                    ("price_open", "DOUBLE"), ("price_high", "DOUBLE"), ("price_low", "DOUBLE"), ("price_close", "DOUBLE"),
                    ("volume", "DOUBLE"), ("open_interest", "DOUBLE")
                ]
            )
        )
    }
}

/// Every market of a series closing at or after `since_ms`, through the cursor.
pub async fn list_markets(
    http: &Http,
    base_url: &str,
    series_ticker: &str,
    since_ms: i64,
) -> Result<Vec<Value>, StoreError> {
    let mut markets = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut pairs = vec![
            ("series_ticker", series_ticker.to_owned()),
            ("limit", PAGE.to_string()),
            ("min_close_ts", (since_ms / 1000).to_string()),
        ];
        if let Some(c) = &cursor {
            pairs.push(("cursor", c.clone()));
        }
        let url = with_query(
            &format!("{}/markets", base_url.trim_end_matches('/')),
            &pairs,
        );
        let answer = http.get_json(&url, &[]).await?;
        let page = answer
            .get("markets")
            .and_then(Value::as_array)
            .ok_or_else(|| StoreError::Check("answer has no markets".into()))?;
        markets.extend(page.iter().cloned());
        cursor = answer
            .get("cursor")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
            .map(str::to_owned);
        if cursor.is_none() || page.is_empty() {
            return Ok(markets);
        }
    }
}

/// The catalogue rows of a series' markets.
#[must_use]
pub fn market_rows(series_ticker: &str, markets: &[Value], fetched_at_ms: i64) -> Vec<Value> {
    markets
        .iter()
        .filter_map(|m| {
            let text = |key: &str| m.get(key).and_then(Value::as_str).map(str::to_owned);
            let number = |key: &str| m.get(key).and_then(Value::as_str).and_then(|s| s.parse::<f64>().ok());
            Some(json!({
                "ticker": m.get("ticker")?.as_str()?, "series_ticker": series_ticker, "event_ticker": text("event_ticker"),
                "title": text("title"), "subtitle": text("subtitle"), "yes_sub_title": text("yes_sub_title"),
                "status": text("status"), "result": text("result"), "strike_type": text("strike_type"),
                "floor_strike": m.get("floor_strike"), "cap_strike": m.get("cap_strike"),
                "open_time": text("open_time"), "close_time": text("close_time"), "expiration_time": text("expiration_time"),
                "last_price": number("last_price_dollars"), "volume": number("volume_fp"),
                "open_interest": number("open_interest_fp"), "fetched_at": fetched_at_ms,
            }))
        })
        .collect()
}

/// The typed `SELECT` of staged catalogue rows.
#[must_use]
pub fn markets_select(staged: &Path) -> String {
    format!(
        "SELECT ticker, series_ticker, event_ticker, title, subtitle, yes_sub_title, status, result, strike_type, floor_strike,
                cap_strike, open_time, close_time, expiration_time, last_price, volume, open_interest,
                fetched_at AS fetched_at_ms, make_timestamp(fetched_at * 1000) AS ts, 'ALL' AS symbol
         FROM {}",
        read_json_array(
            staged,
            &[
                ("ticker", "VARCHAR"), ("series_ticker", "VARCHAR"), ("event_ticker", "VARCHAR"), ("title", "VARCHAR"),
                ("subtitle", "VARCHAR"), ("yes_sub_title", "VARCHAR"), ("status", "VARCHAR"), ("result", "VARCHAR"),
                ("strike_type", "VARCHAR"), ("floor_strike", "DOUBLE"), ("cap_strike", "DOUBLE"), ("open_time", "VARCHAR"),
                ("close_time", "VARCHAR"), ("expiration_time", "VARCHAR"), ("last_price", "DOUBLE"), ("volume", "DOUBLE"),
                ("open_interest", "DOUBLE"), ("fetched_at", "BIGINT")
            ]
        )
    )
}

/// Sync every curated series: its catalogue rows and every market's candles.
pub async fn sync(ctx: &Ctx, cfg: &Kalshi, only_symbol: Option<&str>) -> Outcome {
    let mut outcome = Outcome::default();
    let since_ms = super::periods::ms_of(ctx.start);
    for series_ticker in &cfg.series {
        if ctx.stopping() {
            break;
        }
        let markets = match list_markets(&ctx.http, &cfg.base_url, series_ticker, since_ms).await {
            Ok(markets) => markets,
            Err(error) => {
                outcome.errors += 1;
                log_error("markets", series_ticker, &error.to_string());
                continue;
            }
        };
        match write_markets(ctx, &market_rows(series_ticker, &markets, ctx.now_ms)).await {
            Ok(_) => outcome.files += 1,
            Err(error) => {
                outcome.errors += 1;
                log_error("markets", series_ticker, &error.to_string());
            }
        }
        for market in &markets {
            let Some(series) = Candles::from_market(&cfg.base_url, series_ticker, market) else {
                continue;
            };
            if only_symbol.is_some_and(|s| s != series.ticker && s != *series_ticker)
                || ctx.stopping()
            {
                continue;
            }
            if series.settled
                && let Some(close) = series.close_ms
                && finished(ctx, &series, close).await
            {
                outcome.skipped += 1;
                continue;
            }
            outcome.add(&sync_series(ctx, &series).await);
        }
    }
    outcome
}

async fn write_markets(ctx: &Ctx, rows: &[Value]) -> Result<i64, StoreError> {
    if rows.is_empty() {
        return Err(StoreError::Check(
            "series has no markets since the start date".into(),
        ));
    }
    let staged = stage_json(&ctx.staging(), rows)?;
    let day = Period::containing(date_of_ms(ctx.now_ms), Granularity::Day);
    let target = Target {
        source: "kalshi".into(),
        dataset: "markets".into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete: false,
    };
    let select = markets_select(&staged);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, "ticker", &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

fn log_error(dataset: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "kalshi")
            .with("dataset", dataset)
            .with("symbol", "ALL")
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const MARKETS: &str = include_str!("../../../../tests/data/augment/kalshi-markets.json");
    const CANDLES: &str = include_str!("../../../../tests/data/augment/kalshi-candles.json");

    #[test]
    fn markets_and_candles_flatten() {
        let listing: Value = serde_json::from_str(MARKETS).unwrap();
        let markets = listing["markets"].as_array().unwrap();
        let rows = market_rows("KXFEDDECISION", markets, 9);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["ticker"], "KXFEDDECISION-28JAN-H26");
        assert_eq!(rows[0]["status"], "active");
        let series = Candles::from_market("https://k/", "KXFEDDECISION", &markets[0]).unwrap();
        assert!(!series.settled);
        assert_eq!(series.event_ticker, "KXFEDDECISION-28JAN");
        assert!(series.close_ms.unwrap() > 1_800_000_000_000);
        let answer: Value = serde_json::from_str(CANDLES).unwrap();
        let candles = answer["candlesticks"].as_array().unwrap();
        let rows: Vec<Value> = candles.iter().filter_map(candle_row).collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["end_t"], 1_759_276_800);
        assert_eq!(rows[2]["t"], 1_759_273_200);
        assert_eq!(rows[2]["yes_ask_close"], 0.57);
        assert_eq!(rows[2]["price_close"], Value::Null, "no trade in that hour");
        assert!(rows[0]["price_close"].as_f64().is_some());
    }
}
