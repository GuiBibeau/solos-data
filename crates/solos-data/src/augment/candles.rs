//! Hyperliquid 1-minute candles, captured while they exist: `candleSnapshot` returns only the
//! most recent few thousand candles, so every cycle asks for the candles since the last one
//! stored and merges the closed ones into the day's file.

use super::hyperliquid::{candle_request, candle_row, candles_select};
use super::ledger::{get_progress, register};
use super::parquet::{Target, stage_json, write_merged};
use super::periods::{Period, date_of_ms};
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// The candles the API still serves on a first run: about 5,000 at one minute.
pub const LOOKBACK_MS: i64 = 5_000 * 60_000;

/// The lane's settings.
pub struct CandleLane {
    /// `/info` URL.
    pub url: String,
    /// `(coin, phoenix)` pairs.
    pub coins: Vec<(String, String)>,
}

/// One pass over every coin.
pub async fn cycle(ctx: &Ctx, lane: &CandleLane, now_ms: i64) -> Outcome {
    let mut outcome = Outcome::default();
    for (coin, phoenix) in &lane.coins {
        if ctx.stopping() {
            break;
        }
        match capture_coin(ctx, lane, coin, phoenix, now_ms).await {
            Ok(result) => outcome.add(&result),
            Err(error) => {
                outcome.errors += 1;
                log(
                    "augment_series_error",
                    Obj::new()
                        .with("source", "hyperliquid")
                        .with("dataset", "candles_1m")
                        .with("symbol", coin.as_str())
                        .with("item", "cycle")
                        .with("error", safe_error(&error.to_string())),
                );
            }
        }
    }
    outcome
}

/// Progress key of a coin.
#[must_use]
pub fn progress_key(coin: &str) -> String {
    format!("hyperliquid/candles_1m/{coin}")
}

async fn capture_coin(
    ctx: &Ctx,
    lane: &CandleLane,
    coin: &str,
    phoenix: &str,
    now_ms: i64,
) -> Result<Outcome, StoreError> {
    let key = progress_key(coin);
    let lookup = key.clone();
    let last = ctx
        .db
        .run(move |store| get_progress(store, &lookup))
        .await?
        .and_then(|p| p.get("lastOpenTime").and_then(Value::as_i64));
    let start = last.map_or(now_ms - LOOKBACK_MS, |t| t + 60_000);
    let answer = ctx
        .http
        .post_json(&lane.url, &candle_request(coin, start, now_ms), &[])
        .await?;
    let candles = answer
        .as_array()
        .ok_or_else(|| StoreError::Check("candleSnapshot is not an array".into()))?;
    let by_day = closed_by_day(candles, start, now_ms);
    let mut outcome = Outcome::default();
    let today = date_of_ms(now_ms);
    for (day, rows) in by_day {
        let newest = rows
            .iter()
            .filter_map(|r| r.get("open_time").and_then(Value::as_i64))
            .max()
            .unwrap_or(start);
        let staged = stage_json(&ctx.staging(), &rows)?;
        let target = Target {
            source: "hyperliquid".into(),
            dataset: "candles_1m".into(),
            symbol: coin.into(),
            phoenix_symbol: Some(phoenix.into()),
            period: day.label(),
            complete: day.start < today,
        };
        let select = candles_select(&staged, &target.constant_columns());
        let progress = json!({ "lastOpenTime": newest, "updatedAt": now() });
        let (root, key_for_run) = (ctx.root.clone(), key.clone());
        let result = ctx
            .db
            .run(move |store| {
                let record = write_merged(store, &root, &select, "open_time_ms", &target)?;
                register(store, &record, Some((&key_for_run, &progress)))?;
                Ok(record.row_count)
            })
            .await;
        let _ = std::fs::remove_file(&staged);
        let count = result?;
        outcome.files += 1;
        outcome.rows += u64::try_from(rows.len()).unwrap_or(0);
        log(
            "augment_file",
            Obj::new()
                .with("source", "hyperliquid")
                .with("dataset", "candles_1m")
                .with("symbol", coin)
                .with("period", day.label())
                .with("rows", count)
                .with("added", rows.len())
                .with("complete", day.start < today),
        );
    }
    Ok(outcome)
}

/// Closed candles (`T` before now) at or after `start`, grouped by the UTC day of their open,
/// as staged rows.
#[must_use]
pub fn closed_by_day(
    candles: &[Value],
    start_ms: i64,
    now_ms: i64,
) -> BTreeMap<Period, Vec<Value>> {
    let mut by_day: BTreeMap<Period, Vec<Value>> = BTreeMap::new();
    for candle in candles {
        let Some(row) = candle_row(candle) else {
            continue;
        };
        let open = row["open_time"].as_i64().unwrap_or(0);
        let close = row["close_time"].as_i64().unwrap_or(0);
        if open < start_ms || close >= now_ms {
            continue;
        }
        let day = Period::containing(date_of_ms(open), super::periods::Granularity::Day);
        let rows = by_day.entry(day).or_default();
        if rows
            .last()
            .and_then(|r| r.get("open_time"))
            .and_then(Value::as_i64)
            != Some(open)
        {
            rows.push(row);
        }
    }
    by_day
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_closed_candles_by_day() {
        let sample: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../tests/data/augment/hl-candles.json"
        ))
        .unwrap();
        let first = sample[0]["t"].as_i64().unwrap();
        let last_close = sample[2]["T"].as_i64().unwrap();
        // The last candle is still open at its own close time.
        let by_day = closed_by_day(&sample, first, last_close);
        let rows: Vec<&Value> = by_day.values().flatten().collect();
        assert_eq!(rows.len(), 2);
        let by_day = closed_by_day(&sample, first + 60_000, last_close + 1);
        assert_eq!(by_day.values().flatten().count(), 2);
        assert_eq!(by_day.keys().next().unwrap().label(), "2026-10-09");
        assert_eq!(progress_key("xyz:NVDA"), "hyperliquid/candles_1m/xyz:NVDA");
    }
}
