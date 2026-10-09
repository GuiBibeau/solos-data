//! Deribit's volatility index (DVOL) for BTC and ETH: `get_volatility_index_data` answers the
//! newest 1,000 bars of a window with a `continuation` timestamp for the older part.

use super::http::{Http, with_query};
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::path::Path;

/// One currency at one resolution.
pub struct Dvol {
    /// API base.
    pub base_url: String,
    /// `BTC` or `ETH`.
    pub currency: String,
    /// Bar length in seconds (60 or 3600).
    pub resolution: u64,
    /// Dataset name (`dvol_60s`).
    pub dataset: String,
}

impl Dvol {
    /// A series for `currency` at `resolution` seconds.
    #[must_use]
    pub fn new(base_url: &str, currency: &str, resolution: u64) -> Dvol {
        Dvol {
            base_url: base_url.trim_end_matches('/').to_owned(),
            currency: currency.to_owned(),
            resolution,
            dataset: format!("dvol_{resolution}s"),
        }
    }
}

impl Series for Dvol {
    fn source(&self) -> &str {
        "deribit"
    }
    fn dataset(&self) -> &str {
        &self.dataset
    }
    fn symbol(&self) -> &str {
        &self.currency
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        Some(&self.currency)
    }
    fn granularity(&self) -> Granularity {
        if self.resolution < 3600 {
            Granularity::Day
        } else {
            Granularity::Month
        }
    }
    fn lag_ms(&self) -> i64 {
        i64::try_from(self.resolution * 1000).unwrap_or(0)
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let mut pages: Vec<Vec<Value>> = Vec::new();
            let mut end = end_ms - 1;
            loop {
                let url = with_query(
                    &format!("{}/api/v2/public/get_volatility_index_data", self.base_url),
                    &[
                        ("currency", self.currency.clone()),
                        ("start_timestamp", start_ms.to_string()),
                        ("end_timestamp", end.to_string()),
                        ("resolution", self.resolution.to_string()),
                    ],
                );
                let answer = http.get_json(&url, &[]).await?;
                let (rows, continuation) = parse_answer(&answer)?;
                let empty = rows.is_empty();
                pages.push(rows);
                match continuation {
                    Some(next) if !empty && next >= start_ms => end = next,
                    _ => break,
                }
            }
            pages.reverse();
            Ok(merge_pages(pages, start_ms, end_ms))
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT t AS time_ms, make_timestamp(t * 1000) AS ts, o AS open, h AS high, l AS low, c AS close, {constants}
             FROM {} ORDER BY t",
            read_json_array(staged, &[("t", "BIGINT"), ("o", "DOUBLE"), ("h", "DOUBLE"), ("l", "DOUBLE"), ("c", "DOUBLE")])
        )
    }
}

/// `result.data` rows as objects and `result.continuation`.
pub fn parse_answer(answer: &Value) -> Result<(Vec<Value>, Option<i64>), StoreError> {
    let result = answer
        .get("result")
        .ok_or_else(|| StoreError::Check("DVOL answer has no result".into()))?;
    let data = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| StoreError::Check("DVOL answer has no data".into()))?;
    let rows = data
        .iter()
        .filter_map(|bar| {
            let bar = bar.as_array()?;
            Some(json!({ "t": bar.first()?.as_i64()?, "o": bar.get(1)?, "h": bar.get(2)?, "l": bar.get(3)?, "c": bar.get(4)? }))
        })
        .collect();
    Ok((rows, result.get("continuation").and_then(Value::as_i64)))
}

/// Oldest page first; keep `[start, end)`, drop duplicate timestamps, sort ascending.
#[must_use]
pub fn merge_pages(pages: Vec<Vec<Value>>, start_ms: i64, end_ms: i64) -> Vec<Value> {
    let mut rows: Vec<Value> = pages.into_iter().flatten().collect();
    rows.sort_by_key(|r| r.get("t").and_then(Value::as_i64).unwrap_or(0));
    rows.dedup_by_key(|r| r.get("t").and_then(Value::as_i64).unwrap_or(0));
    rows.retain(|r| {
        let t = r.get("t").and_then(Value::as_i64).unwrap_or(0);
        t >= start_ms && t < end_ms
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/deribit.json");

    #[test]
    fn parses_bars_and_merges_pages() {
        let sample: Value = serde_json::from_str(SAMPLE).unwrap();
        let (bars, continuation) = parse_answer(&sample).unwrap();
        assert_eq!(bars.len(), 3);
        assert_eq!(bars[0]["t"], 1_767_225_600_000_i64);
        assert_eq!(bars[0]["o"], 43.01);
        assert_eq!(continuation, None);
        let answer = json!({ "result": { "data": [[1000, 1.0, 2.0, 0.5, 1.5], [2000, 1.5, 1.6, 1.4, 1.5]], "continuation": 940 } });
        let (rows, continuation) = parse_answer(&answer).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["t"], 1000);
        assert_eq!(continuation, Some(940));
        let older = vec![json!({ "t": 500 }), json!({ "t": 1000 })];
        let merged = merge_pages(vec![older, rows], 500, 2000);
        let times: Vec<i64> = merged.iter().map(|r| r["t"].as_i64().unwrap()).collect();
        assert_eq!(times, [500, 1000]);
        let dvol = Dvol::new("https://x/", "BTC", 60);
        assert_eq!(dvol.dataset(), "dvol_60s");
        assert_eq!(dvol.granularity(), Granularity::Day);
        assert_eq!(
            Dvol::new("https://x", "ETH", 3600).granularity(),
            Granularity::Month
        );
    }
}
