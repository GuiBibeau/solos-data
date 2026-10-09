//! Bybit's v5 market API: the funding history of a linear perpetual is a paged history, newest
//! first, 200 rows per page. Tick-trade dumps (`public.bybit.com`, one gzip CSV per symbol and
//! day) are the opt-in large dataset, walked day by day from the progress record.

use super::config::{Bybit, Symbol};
use super::http::{Http, HttpError, with_query};
use super::ledger::{get_progress, register, set_progress};
use super::parquet::{Target, read_json_array, staging_path, write_parquet};
use super::periods::{Granularity, Period, date_of_ms, periods_through};
use super::series::{Ctx, Outcome, RowsFuture, Series};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::{StoreError, sql_string};
use serde_json::{Value, json};
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

/// A day this old with no dump is a day without trades, not a day not yet published.
pub const UNPUBLISHED_DAYS: i64 = 3;

/// URL of one day's tick dump.
#[must_use]
pub fn trades_url(files_url: &str, symbol: &str, label: &str) -> String {
    format!(
        "{}/trading/{symbol}/{symbol}{label}.csv.gz",
        files_url.trim_end_matches('/')
    )
}

/// Progress key of a symbol's tick dumps.
#[must_use]
pub fn trades_key(symbol: &str) -> String {
    format!("bybit/trades/{symbol}")
}

/// Which day to try first and which is the last one that can exist (the day before yesterday
/// is always published; yesterday may be).
#[must_use]
pub fn trades_window(
    progress: Option<&Value>,
    start: chrono::NaiveDate,
    now_ms: i64,
) -> (Period, Period) {
    let first = progress
        .and_then(|p| p.get("completeThrough"))
        .and_then(Value::as_str)
        .and_then(Period::parse)
        .map_or_else(|| Period::containing(start, Granularity::Day), Period::next);
    let yesterday = Period::containing(date_of_ms(now_ms - 86_400_000), Granularity::Day);
    (first, yesterday)
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

/// Sync the tick dumps of every symbol with a Bybit market, under the disk budget.
pub async fn sync_trades(
    ctx: &Ctx,
    cfg: &Bybit,
    symbols: &[Symbol],
    only_symbol: Option<&str>,
) -> Outcome {
    let mut outcome = Outcome::default();
    for symbol in symbols {
        let Some(venue) = symbol.bybit.as_deref() else {
            continue;
        };
        if only_symbol.is_some_and(|s| s != symbol.phoenix && s != venue) {
            continue;
        }
        if ctx.stopping() {
            break;
        }
        match sync_symbol_trades(ctx, cfg, venue, &symbol.phoenix, &mut outcome).await {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => {
                outcome.errors += 1;
                log_trades_error(venue, "walk", &error.to_string());
            }
        }
    }
    outcome
}

/// Walk one symbol's days; `Ok(false)` means the budget is spent and the walk should stop.
async fn sync_symbol_trades(
    ctx: &Ctx,
    cfg: &Bybit,
    venue: &str,
    phoenix: &str,
    outcome: &mut Outcome,
) -> Result<bool, StoreError> {
    let key = trades_key(venue);
    let lookup = key.clone();
    let progress = ctx
        .db
        .run(move |store| get_progress(store, &lookup))
        .await?;
    let (first, last) = trades_window(progress.as_ref(), ctx.start, ctx.now_ms);
    for day in periods_through(first, last.start_ms()) {
        if ctx.stopping() {
            return Ok(true);
        }
        if !ctx.within_budget("bybit/trades").await {
            return Ok(false);
        }
        let url = trades_url(&cfg.files_url, venue, &day.label());
        let fetched = match ctx.http.get_bytes(&url, &[]).await {
            Ok(fetched) => fetched,
            Err(HttpError::NotFound) => {
                if ctx.now_ms - day.end_ms() < UNPUBLISHED_DAYS * 86_400_000 {
                    return Ok(true);
                }
                let value = json!({ "completeThrough": day.label(), "updatedAt": now() });
                let key = key.clone();
                ctx.db
                    .run(move |store| set_progress(store, &key, &value))
                    .await?;
                continue;
            }
            Err(error) => {
                outcome.errors += 1;
                log_trades_error(venue, &day.label(), &error.to_string());
                return Ok(true);
            }
        };
        let staged = staging_path(&ctx.staging(), "csv.gz");
        std::fs::write(&staged, &fetched.body)?;
        let target = Target {
            source: "bybit".into(),
            dataset: "trades".into(),
            symbol: venue.into(),
            phoenix_symbol: Some(phoenix.into()),
            period: day.label(),
            complete: true,
        };
        let from = format!(
            "read_csv({}, header=true, compression='gzip', auto_detect=true, all_varchar=true)",
            sql_string(&staged.to_string_lossy())
        );
        let progress = json!({ "completeThrough": day.label(), "updatedAt": now() });
        let (root, key_for_run) = (ctx.root.clone(), key.clone());
        let result = ctx
            .db
            .run(move |store| {
                let columns: Vec<String> = store
                    .rows(
                        &format!("SELECT column_name FROM (DESCRIBE SELECT * FROM {from})"),
                        &[],
                    )?
                    .iter()
                    .filter_map(|row| row.str("column_name").map(str::to_owned))
                    .collect();
                let select = trades_select(&from, &columns, &target.constant_columns());
                let record = write_parquet(store, &root, &select, &target)?;
                register(store, &record, Some((&key_for_run, &progress)))?;
                Ok(record.row_count)
            })
            .await;
        let _ = std::fs::remove_file(&staged);
        let rows = result?;
        outcome.files += 1;
        outcome.rows += u64::try_from(rows).unwrap_or(0);
        log(
            "augment_file",
            Obj::new()
                .with("source", "bybit")
                .with("dataset", "trades")
                .with("symbol", venue)
                .with("period", day.label())
                .with("rows", rows)
                .with("complete", true),
        );
    }
    Ok(true)
}

fn log_trades_error(venue: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", "bybit")
            .with("dataset", "trades")
            .with("symbol", venue)
            .with("item", item)
            .with("error", safe_error(error)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/bybit-funding.json");

    #[test]
    fn trade_dump_paths_and_windows() {
        assert_eq!(
            trades_url("https://public.bybit.com/", "SOLUSDT", "2026-10-07"),
            "https://public.bybit.com/trading/SOLUSDT/SOLUSDT2026-10-07.csv.gz"
        );
        let start = super::super::periods::parse_date("2026-01-01").unwrap();
        let now_ms = 1_791_547_200_000; // 2026-10-09T12:00Z
        let (first, last) = trades_window(None, start, now_ms);
        assert_eq!(
            (first.label(), last.label()),
            ("2026-01-01".into(), "2026-10-08".into())
        );
        let progress = json!({ "completeThrough": "2026-10-06" });
        let (first, _) = trades_window(Some(&progress), start, now_ms);
        assert_eq!(first.label(), "2026-10-07");
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
