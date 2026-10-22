//! Kalshi answers as flat rows, and the typed `SELECT`s that turn staged rows into Parquet.
//! The live tier writes prices as `*_dollars` strings and sizes as `*_fp`; the archive writes
//! the same values under the bare names. Every row carries `fetched_at` (milliseconds), the
//! instant the lane received it.

use crate::augment::parquet::read_json_array;
use crate::augment::periods::date_of_ms;
use serde_json::{Value, json};
use std::path::Path;

/// RFC 3339 to milliseconds.
#[must_use]
pub fn parse_ms(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|d| d.timestamp_millis())
}

fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.replace(',', "").trim().parse().ok(),
        _ => None,
    }
}

/// The first of `keys` that holds a number.
fn first_number(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| number(value.get(*k)))
}

fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn time_ms(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_str).and_then(parse_ms)
}

/// The level a market's payoff turns on: the floor of a `greater` market, the cap of a `less`
/// one, the middle of a `between` range.
#[must_use]
pub fn strike_of(market: &Value) -> Option<f64> {
    let floor = number(market.get("floor_strike"));
    let cap = number(market.get("cap_strike"));
    match (floor, cap) {
        (Some(f), Some(c)) => Some((f + c) / 2.0),
        (Some(f), None) => Some(f),
        (None, Some(c)) => Some(c),
        (None, None) => None,
    }
}

/// One market's metadata row; `tier` is `live` or `historical`.
#[must_use]
pub fn market_row(
    market: &Value,
    series: &str,
    asset: Option<&str>,
    tier: &str,
    fetched_at: i64,
) -> Option<Value> {
    let ticker = market.get("ticker")?.as_str()?;
    let close_ms = time_ms(market, "close_time")?;
    Some(json!({
        "ticker": ticker, "series_ticker": series, "event_ticker": text(market, "event_ticker"),
        "asset": asset, "title": text(market, "title"), "yes_sub_title": text(market, "yes_sub_title"),
        "status": text(market, "status"), "result": text(market, "result"),
        "strike_type": text(market, "strike_type"),
        "floor_strike": number(market.get("floor_strike")), "cap_strike": number(market.get("cap_strike")),
        "strike": strike_of(market),
        "created_ms": time_ms(market, "created_time"), "open_ms": time_ms(market, "open_time"),
        "close_ms": close_ms, "expiration_ms": time_ms(market, "expiration_time"),
        "settle_ms": time_ms(market, "settlement_ts"),
        "expiration_value_text": text(market, "expiration_value"),
        "expiration_value": number(market.get("expiration_value")),
        "settlement_value": first_number(market, &["settlement_value_dollars", "settlement_value"]),
        "last_price": first_number(market, &["last_price_dollars", "last_price"]),
        "volume": first_number(market, &["volume_fp", "volume"]),
        "open_interest": first_number(market, &["open_interest_fp", "open_interest"]),
        "tier": tier, "close_day": date_of_ms(close_ms).to_string(), "fetched_at": fetched_at,
    }))
}

fn side(candle: &Value, block: &str, field: &str) -> Option<f64> {
    let block = candle.get(block)?;
    number(block.get(format!("{field}_dollars").as_str())).or_else(|| number(block.get(field)))
}

/// One candle as a flat row; `t` is its first second, `end_t` the API's `end_period_ts`.
#[must_use]
pub fn candle_row(candle: &Value, interval: u32) -> Option<Value> {
    let end_t = candle.get("end_period_ts")?.as_i64()?;
    let mut row = json!({ "t": end_t - i64::from(interval) * 60, "end_t": end_t });
    for block in ["yes_bid", "yes_ask", "price"] {
        for field in ["open", "high", "low", "close"] {
            row[format!("{block}_{field}")] = json!(side(candle, block, field));
        }
    }
    row["price_mean"] = json!(side(candle, "price", "mean"));
    row["price_previous"] = json!(side(candle, "price", "previous"));
    row["volume"] = json!(first_number(candle, &["volume_fp", "volume"]));
    row["open_interest"] = json!(first_number(candle, &["open_interest_fp", "open_interest"]));
    Some(row)
}

/// One trade row.
#[must_use]
pub fn trade_row(trade: &Value, series: &str, close_day: &str, fetched_at: i64) -> Option<Value> {
    let created = trade.get("created_time")?.as_str()?;
    Some(json!({
        "trade_id": trade.get("trade_id")?.as_str()?, "ticker": text(trade, "ticker"),
        "series_ticker": series, "close_day": close_day, "created_ms": parse_ms(created)?,
        "yes_price": first_number(trade, &["yes_price_dollars", "yes_price"]),
        "no_price": first_number(trade, &["no_price_dollars", "no_price"]),
        "count": first_number(trade, &["count_fp", "count"]),
        "taker_side": text(trade, "taker_side"), "taker_outcome_side": text(trade, "taker_outcome_side"),
        "taker_book_side": text(trade, "taker_book_side"),
        "is_block_trade": trade.get("is_block_trade").and_then(Value::as_bool),
        "fetched_at": fetched_at,
    }))
}

const MARKET_COLUMNS: &[(&str, &str)] = &[
    ("ticker", "VARCHAR"),
    ("series_ticker", "VARCHAR"),
    ("event_ticker", "VARCHAR"),
    ("asset", "VARCHAR"),
    ("title", "VARCHAR"),
    ("yes_sub_title", "VARCHAR"),
    ("status", "VARCHAR"),
    ("result", "VARCHAR"),
    ("strike_type", "VARCHAR"),
    ("floor_strike", "DOUBLE"),
    ("cap_strike", "DOUBLE"),
    ("strike", "DOUBLE"),
    ("created_ms", "BIGINT"),
    ("open_ms", "BIGINT"),
    ("close_ms", "BIGINT"),
    ("expiration_ms", "BIGINT"),
    ("settle_ms", "BIGINT"),
    ("expiration_value_text", "VARCHAR"),
    ("expiration_value", "DOUBLE"),
    ("settlement_value", "DOUBLE"),
    ("last_price", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("open_interest", "DOUBLE"),
    ("tier", "VARCHAR"),
    ("close_day", "VARCHAR"),
    ("fetched_at", "BIGINT"),
];

const CANDLE_COLUMNS: &[(&str, &str)] = &[
    ("ticker", "VARCHAR"),
    ("series_ticker", "VARCHAR"),
    ("event_ticker", "VARCHAR"),
    ("t", "BIGINT"),
    ("end_t", "BIGINT"),
    ("yes_bid_open", "DOUBLE"),
    ("yes_bid_high", "DOUBLE"),
    ("yes_bid_low", "DOUBLE"),
    ("yes_bid_close", "DOUBLE"),
    ("yes_ask_open", "DOUBLE"),
    ("yes_ask_high", "DOUBLE"),
    ("yes_ask_low", "DOUBLE"),
    ("yes_ask_close", "DOUBLE"),
    ("price_open", "DOUBLE"),
    ("price_high", "DOUBLE"),
    ("price_low", "DOUBLE"),
    ("price_close", "DOUBLE"),
    ("price_mean", "DOUBLE"),
    ("price_previous", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("open_interest", "DOUBLE"),
    ("strike", "DOUBLE"),
    ("centre", "DOUBLE"),
    ("centre_source", "VARCHAR"),
    ("tier", "VARCHAR"),
    ("fetched_at", "BIGINT"),
];

const TRADE_COLUMNS: &[(&str, &str)] = &[
    ("trade_id", "VARCHAR"),
    ("ticker", "VARCHAR"),
    ("series_ticker", "VARCHAR"),
    ("close_day", "VARCHAR"),
    ("created_ms", "BIGINT"),
    ("yes_price", "DOUBLE"),
    ("no_price", "DOUBLE"),
    ("count", "DOUBLE"),
    ("taker_side", "VARCHAR"),
    ("taker_outcome_side", "VARCHAR"),
    ("taker_book_side", "VARCHAR"),
    ("is_block_trade", "BOOLEAN"),
    ("fetched_at", "BIGINT"),
];

/// The datasets this source writes, their merge keys and columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `contracts`: market metadata, one row per market.
    Contracts,
    /// `candles_1m` / `candles_60m`: one row per market and period.
    Candles,
    /// `trades`: one row per fill.
    Trades,
}

impl Kind {
    /// The merge key.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Kind::Contracts => "ticker",
            Kind::Candles => "ticker, t",
            Kind::Trades => "trade_id",
        }
    }

    /// The typed `SELECT` of rows staged at `staged`; `constants` are the file's symbol columns.
    #[must_use]
    pub fn select(self, staged: &Path, constants: &str) -> String {
        let (columns, extra) = match self {
            Kind::Contracts => (MARKET_COLUMNS, "make_timestamp(close_ms * 1000) AS ts"),
            Kind::Candles => (CANDLE_COLUMNS, "make_timestamp(t * 1000000) AS ts"),
            Kind::Trades => (TRADE_COLUMNS, "make_timestamp(created_ms * 1000) AS ts"),
        };
        let names: Vec<String> = columns
            .iter()
            .map(|(name, _)| match *name {
                "fetched_at" => "fetched_at AS fetched_at_ms".to_owned(),
                other => other.to_owned(),
            })
            .collect();
        format!(
            "SELECT {}, {extra}, {constants} FROM {}",
            names.join(", "),
            read_json_array(staged, columns)
        )
    }
}

/// The candle dataset of an interval.
#[must_use]
pub fn candle_dataset(interval: u32) -> &'static str {
    if interval == 60 {
        "candles_60m"
    } else {
        "candles_1m"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVE: &str = include_str!("../../../../../tests/data/augment/kalshi-live-markets.json");
    const ARCHIVE: &str =
        include_str!("../../../../../tests/data/augment/kalshi-historical-candles.json");

    #[test]
    fn markets_flatten_in_both_tiers() {
        let page: Value = serde_json::from_str(LIVE).unwrap();
        let market = &page["markets"][0];
        let row = market_row(market, "KXBTCD", Some("BTC"), "live", 7).unwrap();
        assert_eq!(row["ticker"], "KXBTCD-26OCT2112-T81499.99");
        assert_eq!(row["strike"], 81_499.99);
        assert_eq!(row["close_day"], "2026-10-21");
        assert_eq!(row["volume"], 12.0);
        assert_eq!(row["expiration_value"], 81_620.55);
        assert_eq!(row["fetched_at"], 7);
        let between = json!({"floor_strike": 100.0, "cap_strike": 102.0});
        assert_eq!(strike_of(&between), Some(101.0));
        assert_eq!(strike_of(&json!({"cap_strike": 5.5})), Some(5.5));
        assert!(
            market_row(&json!({"ticker": "X"}), "S", None, "live", 0).is_none(),
            "no close time"
        );
    }

    #[test]
    fn candles_read_live_and_archive_fields() {
        let answer: Value = serde_json::from_str(ARCHIVE).unwrap();
        let row = candle_row(&answer["candlesticks"][0], 1).unwrap();
        assert_eq!(row["end_t"], 1_759_348_860);
        assert_eq!(row["t"], 1_759_348_800);
        assert_eq!(row["yes_ask_close"], 0.07);
        assert_eq!(row["yes_bid_open"], 0.0);
        assert_eq!(row["price_close"], Value::Null);
        assert_eq!(row["price_previous"], 0.05);
        assert_eq!(row["open_interest"], 2.0);
        let live = json!({"end_period_ts": 7200, "yes_bid": {"close_dollars": "0.41"}, "volume_fp": "3.00"});
        let row = candle_row(&live, 60).unwrap();
        assert_eq!(row["t"], 3600);
        assert_eq!(row["yes_bid_close"], 0.41);
        assert_eq!(row["volume"], 3.0);
    }

    #[test]
    fn trades_flatten() {
        let trade = json!({"trade_id": "a", "ticker": "T", "created_time": "2025-10-01T15:20:24.686277Z",
            "yes_price_dollars": "0.0400", "no_price_dollars": "0.9600", "count_fp": "1.00",
            "taker_side": "yes", "is_block_trade": false});
        let row = trade_row(&trade, "KXBTCD", "2025-10-01", 9).unwrap();
        assert_eq!(row["created_ms"], 1_759_332_024_686_i64);
        assert_eq!(row["yes_price"], 0.04);
        assert_eq!(row["count"], 1.0);
        assert_eq!(row["close_day"], "2025-10-01");
    }

    #[test]
    fn selects_name_every_column() {
        let sql = Kind::Candles.select(Path::new("/s/x.json"), "'K' AS symbol");
        assert!(sql.contains("fetched_at AS fetched_at_ms"));
        assert!(sql.contains("centre_source"));
        assert!(sql.ends_with("maximum_object_size=67108864)"));
        assert_eq!(candle_dataset(60), "candles_60m");
    }
}
