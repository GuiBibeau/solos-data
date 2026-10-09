//! Phoenix's own market list: each equity market's `metadata.earningsDates`, captured every
//! sync into the day's file so the calendar's changes stay visible.

use super::config::Phoenix;
use super::ledger::register;
use super::parquet::{Target, read_json_array, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::path::Path;

/// One row per (market, earnings date) with the capture instant.
#[must_use]
pub fn earnings_rows(markets: &Value, fetched_at_ms: i64) -> Vec<Value> {
    markets
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|market| {
            let symbol = market.get("symbol").and_then(Value::as_str).unwrap_or("");
            let asset_id = market.get("assetId").cloned().unwrap_or(Value::Null);
            let status = market.get("marketStatus").cloned().unwrap_or(Value::Null);
            let dates = market
                .get("metadata")
                .and_then(|m| m.get("earningsDates"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            dates
                .into_iter()
                .filter_map(move |date| {
                    let text = date.as_str()?;
                    let at = chrono::DateTime::parse_from_rfc3339(text)
                        .ok()?
                        .timestamp_millis();
                    Some(json!({
                        "phoenix_symbol": symbol, "asset_id": asset_id, "market_status": status,
                        "earnings_at": at, "earnings_text": text, "fetched_at": fetched_at_ms,
                    }))
                })
                .collect::<Vec<Value>>()
        })
        .collect()
}

/// The typed `SELECT` of staged rows.
#[must_use]
pub fn select(staged: &Path) -> String {
    format!(
        "SELECT phoenix_symbol, asset_id, market_status, earnings_at AS earnings_at_ms,
                make_timestamp(earnings_at * 1000) AS earnings_at, CAST(make_timestamp(earnings_at * 1000) AS DATE) AS earnings_date,
                earnings_text, fetched_at AS fetched_at_ms, make_timestamp(fetched_at * 1000) AS ts, 'ALL' AS symbol
         FROM {}",
        read_json_array(
            staged,
            &[
                ("phoenix_symbol", "VARCHAR"), ("asset_id", "BIGINT"), ("market_status", "VARCHAR"),
                ("earnings_at", "BIGINT"), ("earnings_text", "VARCHAR"), ("fetched_at", "BIGINT")
            ]
        )
    )
}

/// Fetch the market list and merge today's earnings dates into the day's file.
pub async fn sync(ctx: &Ctx, cfg: &Phoenix, now_ms: i64) -> Outcome {
    let mut outcome = Outcome::default();
    match capture(ctx, cfg, now_ms).await {
        Ok((count, added)) => {
            outcome.files += 1;
            outcome.rows += added;
            log(
                "augment_file",
                Obj::new()
                    .with("source", "phoenix")
                    .with("dataset", "earnings_dates")
                    .with("symbol", "ALL")
                    .with("period", date_of_ms(now_ms).to_string())
                    .with("rows", count)
                    .with("added", added),
            );
        }
        Err(error) => {
            outcome.errors += 1;
            log(
                "augment_series_error",
                Obj::new()
                    .with("source", "phoenix")
                    .with("dataset", "earnings_dates")
                    .with("symbol", "ALL")
                    .with("item", "markets")
                    .with("error", safe_error(&error.to_string())),
            );
        }
    }
    outcome
}

async fn capture(ctx: &Ctx, cfg: &Phoenix, now_ms: i64) -> Result<(i64, u64), StoreError> {
    let markets = ctx.http.get_json(&cfg.markets_url, &[]).await?;
    let rows = earnings_rows(&markets, now_ms);
    if rows.is_empty() {
        return Err(StoreError::Check(
            "markets list has no earnings dates".into(),
        ));
    }
    let staged = stage_json(&ctx.staging(), &rows)?;
    let day = Period::containing(date_of_ms(now_ms), Granularity::Day);
    let target = Target {
        source: "phoenix".into(),
        dataset: "earnings_dates".into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete: false,
    };
    let select = select(&staged);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(
                store,
                &root,
                &select,
                "phoenix_symbol, earnings_at_ms",
                &target,
            )?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    Ok((result?, u64::try_from(rows.len()).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/phoenix-markets.json");

    #[test]
    fn one_row_per_market_and_date() {
        let markets: Value = serde_json::from_str(SAMPLE).unwrap();
        let rows = earnings_rows(&markets, 5);
        assert_eq!(rows.len(), 1, "MET has no earnings dates");
        assert_eq!(rows[0]["phoenix_symbol"], "QCOM");
        assert_eq!(rows[0]["asset_id"], 59);
        assert_eq!(rows[0]["earnings_at"], 1_793_750_400_000_i64);
        assert_eq!(rows[0]["fetched_at"], 5);
    }
}
