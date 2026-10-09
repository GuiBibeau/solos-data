//! Hyperliquid's info API: the hourly funding history is a paged history (sync); 1-minute
//! candles and asset contexts only exist for the recent past and are captured continuously.

use super::http::Http;
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::{StoreError, sql_string};
use serde_json::{Value, json};
use std::path::Path;

/// Rows per `fundingHistory` page.
pub const FUNDING_PAGE: usize = 500;

/// The hourly funding history of one coin.
pub struct FundingHistory {
    /// `/info` URL.
    pub url: String,
    /// Hyperliquid coin (`SOL`, `xyz:NVDA`).
    pub coin: String,
    /// Phoenix symbol.
    pub phoenix: String,
}

impl Series for FundingHistory {
    fn source(&self) -> &str {
        "hyperliquid"
    }
    fn dataset(&self) -> &str {
        "funding"
    }
    fn symbol(&self) -> &str {
        &self.coin
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        Some(&self.phoenix)
    }
    fn granularity(&self) -> Granularity {
        Granularity::Month
    }
    fn lag_ms(&self) -> i64 {
        3_600_000
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let mut rows = Vec::new();
            let mut cursor = start_ms;
            loop {
                let body = json!({ "type": "fundingHistory", "coin": self.coin, "startTime": cursor, "endTime": end_ms - 1 });
                let page = http.post_json(&self.url, &body, &[]).await?;
                let page = page
                    .as_array()
                    .ok_or_else(|| StoreError::Check("fundingHistory is not an array".into()))?;
                let (count, last) = append_page(&mut rows, page, start_ms, end_ms);
                match last {
                    Some(last) if count == FUNDING_PAGE && last + 1 < end_ms => cursor = last + 1,
                    _ => break,
                }
            }
            Ok(rows)
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT time AS time_ms, make_timestamp(time * 1000) AS ts, CAST(fundingRate AS DOUBLE) AS funding_rate,
                    CAST(premium AS DOUBLE) AS premium, {constants}
             FROM {} ORDER BY time",
            read_json_array(
                staged,
                &[("coin", "VARCHAR"), ("fundingRate", "VARCHAR"), ("premium", "VARCHAR"), ("time", "BIGINT")]
            )
        )
    }
}

/// Keep the page's rows inside `[start, end)`, deduplicated on `time`; returns the page size
/// and its last `time`.
#[must_use]
pub fn append_page(
    rows: &mut Vec<Value>,
    page: &[Value],
    start_ms: i64,
    end_ms: i64,
) -> (usize, Option<i64>) {
    let mut last = None;
    for row in page {
        let Some(time) = row.get("time").and_then(Value::as_i64) else {
            continue;
        };
        last = Some(time);
        if time < start_ms || time >= end_ms {
            continue;
        }
        if rows
            .last()
            .and_then(|r| r.get("time"))
            .and_then(Value::as_i64)
            == Some(time)
        {
            continue;
        }
        rows.push(row.clone());
    }
    (page.len(), last)
}

/// The request body of a `candleSnapshot` for `[start, end]`.
#[must_use]
pub fn candle_request(coin: &str, start_ms: i64, end_ms: i64) -> Value {
    json!({ "type": "candleSnapshot", "req": { "coin": coin, "interval": "1m", "startTime": start_ms, "endTime": end_ms } })
}

/// The request body of `metaAndAssetCtxs`, for the main dex (`None`) or a HIP-3 dex.
#[must_use]
pub fn contexts_request(dex: Option<&str>) -> Value {
    match dex {
        Some(dex) => json!({ "type": "metaAndAssetCtxs", "dex": dex }),
        None => json!({ "type": "metaAndAssetCtxs" }),
    }
}

/// The dex of a coin name: `xyz:NVDA` lives on `xyz`, `SOL` on the main dex.
#[must_use]
pub fn dex_of(coin: &str) -> Option<&str> {
    coin.split_once(':').map(|(dex, _)| dex)
}

/// Flatten one `metaAndAssetCtxs` answer into rows: the universe's names zipped with the
/// contexts, keeping only `coins` (every coin when the filter is empty).
#[must_use]
pub fn context_rows(answer: &Value, coins: &[String], at_ms: i64) -> Vec<Value> {
    let universe = answer
        .get(0)
        .and_then(|m| m.get("universe"))
        .and_then(Value::as_array);
    let contexts = answer.get(1).and_then(Value::as_array);
    let (Some(universe), Some(contexts)) = (universe, contexts) else {
        return Vec::new();
    };
    universe
        .iter()
        .zip(contexts)
        .filter_map(|(asset, ctx)| {
            let name = asset.get("name").and_then(Value::as_str)?;
            if !coins.is_empty() && !coins.iter().any(|c| c == name) {
                return None;
            }
            let mut row = ctx.clone();
            let object = row.as_object_mut()?;
            object.insert("coin".into(), Value::String(name.to_owned()));
            object.insert("at".into(), Value::from(at_ms));
            Some(row)
        })
        .collect()
}

/// `SELECT` typing staged context rows.
#[must_use]
pub fn contexts_select(staged: &Path, phoenix_case: &str) -> String {
    format!(
        "SELECT at AS at_ms, make_timestamp(at * 1000) AS ts, coin AS symbol, {phoenix_case} AS phoenix_symbol,
                CAST(funding AS DOUBLE) AS funding, CAST(openInterest AS DOUBLE) AS open_interest,
                CAST(markPx AS DOUBLE) AS mark_px, CAST(oraclePx AS DOUBLE) AS oracle_px, CAST(midPx AS DOUBLE) AS mid_px,
                CAST(premium AS DOUBLE) AS premium, CAST(prevDayPx AS DOUBLE) AS prev_day_px,
                CAST(dayNtlVlm AS DOUBLE) AS day_notional_volume, CAST(dayBaseVlm AS DOUBLE) AS day_base_volume,
                impactPxs AS impact_pxs
         FROM {} ORDER BY at, coin",
        read_json_array(
            staged,
            &[
                ("at", "BIGINT"), ("coin", "VARCHAR"), ("funding", "VARCHAR"), ("openInterest", "VARCHAR"),
                ("markPx", "VARCHAR"), ("oraclePx", "VARCHAR"), ("midPx", "VARCHAR"), ("premium", "VARCHAR"),
                ("prevDayPx", "VARCHAR"), ("dayNtlVlm", "VARCHAR"), ("dayBaseVlm", "VARCHAR"), ("impactPxs", "VARCHAR[]")
            ]
        )
    )
}

/// `SELECT` typing staged candles.
#[must_use]
pub fn candles_select(staged: &Path, constants: &str) -> String {
    format!(
        "SELECT t AS open_time_ms, make_timestamp(t * 1000) AS ts, T AS close_time_ms, CAST(o AS DOUBLE) AS open,
                CAST(h AS DOUBLE) AS high, CAST(l AS DOUBLE) AS low, CAST(c AS DOUBLE) AS close, CAST(v AS DOUBLE) AS volume,
                n AS trades, {constants}
         FROM {} ORDER BY t",
        read_json_array(
            staged,
            &[("t", "BIGINT"), ("T", "BIGINT"), ("o", "VARCHAR"), ("h", "VARCHAR"), ("l", "VARCHAR"), ("c", "VARCHAR"), ("v", "VARCHAR"), ("n", "BIGINT")]
        )
    )
}

/// A `CASE coin WHEN 'x' THEN 'X' ... END` mapping venue coins to Phoenix symbols.
#[must_use]
pub fn phoenix_case(pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return "NULL::VARCHAR".into();
    }
    let mut out = String::from("CASE coin");
    for (coin, phoenix) in pairs {
        out.push_str(&format!(
            " WHEN {} THEN {}",
            sql_string(coin),
            sql_string(phoenix)
        ));
    }
    out.push_str(" END");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/hl-funding-page.json");

    #[test]
    fn pages_are_bounded_and_deduplicated() {
        let sample: Vec<Value> = serde_json::from_str(SAMPLE).unwrap();
        let mut rows = Vec::new();
        let (count, last) = append_page(&mut rows, &sample, 1_767_225_600_000, 1_767_232_800_000);
        assert_eq!((count, last), (3, Some(1_767_232_800_024)));
        assert_eq!(rows.len(), 2, "the third row starts at the window's end");
        let page: Vec<Value> = (0..3)
            .map(|i| json!({ "coin": "SOL", "time": 1000 + i * 100, "fundingRate": "0.1" }))
            .collect();
        let mut rows = Vec::new();
        let (count, last) = append_page(&mut rows, &page, 1050, 1200);
        assert_eq!((count, last), (3, Some(1200)));
        assert_eq!(rows.len(), 1);
        let _ = append_page(&mut rows, &page, 1050, 1300);
        assert_eq!(rows.len(), 2);
        assert_eq!(dex_of("xyz:NVDA"), Some("xyz"));
        assert_eq!(dex_of("SOL"), None);
    }

    #[test]
    fn contexts_flatten_against_the_universe() {
        let answer = json!([
            { "universe": [{ "name": "BTC" }, { "name": "SOL" }] },
            [{ "funding": "0.1", "markPx": "1" }, { "funding": "0.2", "markPx": "2" }]
        ]);
        let rows = context_rows(&answer, &["SOL".into()], 5);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["coin"], "SOL");
        assert_eq!(rows[0]["at"], 5);
        assert_eq!(context_rows(&answer, &[], 5).len(), 2);
        assert_eq!(
            phoenix_case(&[("xyz:NVDA".into(), "NVDA".into())]),
            "CASE coin WHEN 'xyz:NVDA' THEN 'NVDA' END"
        );
    }
}
