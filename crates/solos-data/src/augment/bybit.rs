//! Bybit's v5 market API: the funding history of a linear perpetual is a paged history, newest
//! first, 200 rows per page. Tick-trade dumps (`public.bybit.com`) are the opt-in large dataset.

use super::http::{Http, with_query};
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::StoreError;
use serde_json::Value;
use std::path::Path;

/// Rows per page.
pub const PAGE: usize = 200;

/// The funding history of one symbol.
pub struct FundingHistory {
    /// API base.
    pub base_url: String,
    /// Bybit symbol (`SOLUSDT`).
    pub symbol: String,
    /// Phoenix symbol.
    pub phoenix: String,
}

impl Series for FundingHistory {
    fn source(&self) -> &str {
        "bybit"
    }
    fn dataset(&self) -> &str {
        "funding"
    }
    fn symbol(&self) -> &str {
        &self.symbol
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
            let mut rows: Vec<Value> = Vec::new();
            let mut end = end_ms - 1;
            loop {
                let url = with_query(
                    &format!(
                        "{}/v5/market/funding/history",
                        self.base_url.trim_end_matches('/')
                    ),
                    &[
                        ("category", "linear".into()),
                        ("symbol", self.symbol.clone()),
                        ("startTime", start_ms.to_string()),
                        ("endTime", end.to_string()),
                        ("limit", PAGE.to_string()),
                    ],
                );
                let answer = http.get_json(&url, &[]).await?;
                let page = parse_answer(&answer)?;
                let oldest = page.iter().filter_map(time_of).min();
                let count = page.len();
                rows.extend(page);
                match oldest {
                    Some(oldest) if count == PAGE && oldest > start_ms => end = oldest - 1,
                    _ => break,
                }
            }
            rows.sort_by_key(|r| time_of(r).unwrap_or(0));
            rows.dedup_by_key(|r| time_of(r).unwrap_or(0));
            rows.retain(|r| time_of(r).is_some_and(|t| t >= start_ms && t < end_ms));
            Ok(rows)
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT CAST(fundingRateTimestamp AS BIGINT) AS time_ms,
                    make_timestamp(CAST(fundingRateTimestamp AS BIGINT) * 1000) AS ts,
                    CAST(fundingRate AS DOUBLE) AS funding_rate, {constants}
             FROM {} ORDER BY 1",
            read_json_array(
                staged,
                &[
                    ("symbol", "VARCHAR"),
                    ("fundingRate", "VARCHAR"),
                    ("fundingRateTimestamp", "VARCHAR")
                ]
            )
        )
    }
}

/// `result.list` of a successful answer.
pub fn parse_answer(answer: &Value) -> Result<Vec<Value>, StoreError> {
    if answer.get("retCode").and_then(Value::as_i64) != Some(0) {
        return Err(StoreError::Check(format!(
            "Bybit retCode {}",
            answer.get("retCode").cloned().unwrap_or(Value::Null)
        )));
    }
    Ok(answer
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

/// `fundingRateTimestamp` (a decimal string) as milliseconds.
#[must_use]
pub fn time_of(row: &Value) -> Option<i64> {
    match row.get("fundingRateTimestamp")? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/bybit-funding.json");

    #[test]
    fn parses_pages() {
        let answer: Value = serde_json::from_str(SAMPLE).unwrap();
        let rows = parse_answer(&answer).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(time_of(&rows[0]), Some(1_768_435_200_000));
        assert!(parse_answer(&serde_json::json!({ "retCode": 10001 })).is_err());
    }
}
