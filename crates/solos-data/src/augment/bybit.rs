//! Bybit's v5 market API: the funding history of a linear perpetual is a paged history, newest
//! first, 200 rows per page. Tick-trade dumps (`public.bybit.com`, one gzip CSV per symbol and
//! day) are the opt-in large dataset, walked newest first by `dumps`.

use super::config::{Bybit, Symbol};
use super::http::{Http, with_query};
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{RowsFuture, Series};
use crate::store::{StoreError, sql_string};
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

/// URL of one day's tick dump.
#[must_use]
pub fn trades_url(files_url: &str, symbol: &str, label: &str) -> String {
    format!(
        "{}/trading/{symbol}/{symbol}{label}.csv.gz",
        files_url.trim_end_matches('/')
    )
}

/// The typed projection of a dump's CSV; `columns` are the header names DuckDB found.
#[must_use]
pub fn trades_select(from: &str, columns: &[String], constants: &str) -> String {
    let rpi = if columns.iter().any(|c| c == "RPI") {
        "CAST(RPI AS BIGINT) AS rpi"
    } else {
        "NULL::BIGINT AS rpi"
    };
    format!(
        "SELECT CAST(CAST(timestamp AS DOUBLE) * 1000000 AS BIGINT) AS time_us,
                make_timestamp(CAST(CAST(timestamp AS DOUBLE) * 1000000 AS BIGINT)) AS ts,
                side, CAST(size AS DOUBLE) AS size, CAST(price AS DOUBLE) AS price, tickDirection AS tick_direction,
                trdMatchID AS trade_id, CAST(grossValue AS DOUBLE) AS gross_value, CAST(homeNotional AS DOUBLE) AS home_notional,
                CAST(foreignNotional AS DOUBLE) AS foreign_notional, {rpi}, {constants}
         FROM {from} ORDER BY CAST(timestamp AS DOUBLE), trdMatchID"
    )
}

/// The tick dumps of every symbol of the walk's kinds, as dump series for the newest-first walk
/// (ADR-0009, exchange dumps).
#[must_use]
pub fn trade_kinds(
    cfg: &Bybit,
    symbols: &[Symbol],
    only_symbol: Option<&str>,
) -> Vec<std::sync::Arc<dyn super::dumps::Kind>> {
    let mut out: Vec<std::sync::Arc<dyn super::dumps::Kind>> = Vec::new();
    for symbol in symbols {
        let Some(venue) = symbol.bybit.as_deref() else {
            continue;
        };
        if !cfg.walk.kinds.contains(&symbol.kind)
            || only_symbol.is_some_and(|s| s != symbol.phoenix && s != venue)
        {
            continue;
        }
        out.push(std::sync::Arc::new(TradeDumps {
            files_url: cfg.files_url.clone(),
            venue: venue.to_owned(),
            phoenix: symbol.phoenix.clone(),
        }));
    }
    out
}

/// One symbol's daily tick dumps (`public.bybit.com/trading/<SYMBOL>/<SYMBOL><day>.csv.gz`).
pub struct TradeDumps {
    files_url: String,
    venue: String,
    phoenix: String,
}

impl super::dumps::Kind for TradeDumps {
    fn source(&self) -> &str {
        "bybit"
    }
    fn dataset(&self) -> &str {
        "trades"
    }
    fn symbol(&self) -> &str {
        &self.venue
    }
    fn phoenix(&self) -> &str {
        &self.phoenix
    }
    fn granularity(&self) -> Granularity {
        Granularity::Day
    }
    fn url(&self, label: &str) -> String {
        trades_url(&self.files_url, &self.venue, label)
    }
    fn container(&self) -> super::dumps::Container {
        super::dumps::Container::Gzip
    }
    fn select(
        &self,
        store: &mut crate::store::Store,
        staged: &Path,
        constants: &str,
    ) -> Result<String, StoreError> {
        let from = format!(
            "read_csv({}, header=true, compression='gzip', auto_detect=true, all_varchar=true)",
            sql_string(&staged.to_string_lossy())
        );
        let columns: Vec<String> = store
            .rows(
                &format!("SELECT column_name FROM (DESCRIBE SELECT * FROM {from})"),
                &[],
            )?
            .iter()
            .filter_map(|row| row.str("column_name").map(str::to_owned))
            .collect();
        Ok(trades_select(&from, &columns, constants))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/bybit-funding.json");

    #[test]
    fn trade_dump_paths_and_selects() {
        assert_eq!(
            trades_url("https://public.bybit.com/", "SOLUSDT", "2026-10-07"),
            "https://public.bybit.com/trading/SOLUSDT/SOLUSDT2026-10-07.csv.gz"
        );
        let with_rpi = trades_select(
            "read_csv('x')",
            &["timestamp".into(), "RPI".into()],
            "'a' AS symbol",
        );
        assert!(with_rpi.contains("CAST(RPI AS BIGINT) AS rpi"));
        let without = trades_select("read_csv('x')", &["timestamp".into()], "'a' AS symbol");
        assert!(without.contains("NULL::BIGINT AS rpi"));
    }

    #[test]
    fn parses_pages() {
        let answer: Value = serde_json::from_str(SAMPLE).unwrap();
        let rows = parse_answer(&answer).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(time_of(&rows[0]), Some(1_768_435_200_000));
        assert!(parse_answer(&serde_json::json!({ "retCode": 10001 })).is_err());
    }
}
