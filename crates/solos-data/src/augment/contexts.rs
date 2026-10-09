//! Hyperliquid asset contexts: `metaAndAssetCtxs` per dex, every minute, flattened for the
//! mapped coins and buffered in memory, then merged into the day's file every few minutes and
//! at shutdown. A crash loses at most one buffer.

use super::hyperliquid::{context_rows, contexts_request, contexts_select, dex_of, phoenix_case};
use super::ledger::register;
use super::parquet::{Target, stage_json, write_merged};
use super::periods::{Granularity, Period, date_of_ms};
use super::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, safe_error};
use crate::store::StoreError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// The lane's settings and buffer.
pub struct ContextLane {
    /// `/info` URL.
    pub url: String,
    /// `(coin, phoenix)` pairs.
    pub coins: Vec<(String, String)>,
    /// The dexes the coins live on (`None` is the main dex).
    pub dexes: Vec<Option<String>>,
    buffer: Mutex<Vec<Value>>,
}

impl ContextLane {
    /// A lane for the given coins; the dexes follow from their prefixes.
    #[must_use]
    pub fn new(url: &str, coins: Vec<(String, String)>) -> ContextLane {
        let mut dexes: BTreeSet<Option<String>> = BTreeSet::new();
        for (coin, _) in &coins {
            dexes.insert(dex_of(coin).map(str::to_owned));
        }
        ContextLane {
            url: url.to_owned(),
            coins,
            dexes: dexes.into_iter().collect(),
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Rows waiting to be written.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buffer.lock().expect("context buffer").len()
    }
}

/// One snapshot of every dex; returns the rows buffered.
pub async fn snapshot(ctx: &Ctx, lane: &ContextLane, now_ms: i64) -> Result<usize, StoreError> {
    let coins: Vec<String> = lane.coins.iter().map(|(coin, _)| coin.clone()).collect();
    let mut added = 0;
    for dex in &lane.dexes {
        if ctx.stopping() {
            break;
        }
        let answer = ctx
            .http
            .post_json(&lane.url, &contexts_request(dex.as_deref()), &[])
            .await?;
        let rows = context_rows(&answer, &coins, now_ms);
        added += rows.len();
        lane.buffer.lock().expect("context buffer").extend(rows);
    }
    Ok(added)
}

/// Merge the buffer into the day files; `today` keeps the current day's file open.
pub async fn flush(ctx: &Ctx, lane: &ContextLane, now_ms: i64) -> Outcome {
    let rows: Vec<Value> = std::mem::take(&mut *lane.buffer.lock().expect("context buffer"));
    let mut outcome = Outcome::default();
    if rows.is_empty() {
        return outcome;
    }
    let mut by_day: BTreeMap<Period, Vec<Value>> = BTreeMap::new();
    for row in rows {
        let at = row
            .get("captured_at")
            .and_then(Value::as_i64)
            .unwrap_or(now_ms);
        by_day
            .entry(Period::containing(date_of_ms(at), Granularity::Day))
            .or_default()
            .push(row);
    }
    let today = date_of_ms(now_ms);
    let case = phoenix_case(&lane.coins);
    for (day, rows) in by_day {
        match write_day(ctx, &case, day, &rows, day.start < today).await {
            Ok(count) => {
                outcome.files += 1;
                outcome.rows += u64::try_from(rows.len()).unwrap_or(0);
                log(
                    "augment_file",
                    Obj::new()
                        .with("source", "hyperliquid")
                        .with("dataset", "asset_contexts")
                        .with("symbol", "ALL")
                        .with("period", day.label())
                        .with("rows", count)
                        .with("added", rows.len())
                        .with("complete", day.start < today),
                );
            }
            Err(error) => {
                outcome.errors += 1;
                // Keep the rows for the next flush rather than lose them.
                lane.buffer.lock().expect("context buffer").extend(rows);
                log(
                    "augment_series_error",
                    Obj::new()
                        .with("source", "hyperliquid")
                        .with("dataset", "asset_contexts")
                        .with("symbol", "ALL")
                        .with("item", day.label())
                        .with("error", safe_error(&error.to_string())),
                );
            }
        }
    }
    outcome
}

async fn write_day(
    ctx: &Ctx,
    case: &str,
    day: Period,
    rows: &[Value],
    complete: bool,
) -> Result<i64, StoreError> {
    let staged = stage_json(&ctx.staging(), rows)?;
    let target = Target {
        source: "hyperliquid".into(),
        dataset: "asset_contexts".into(),
        symbol: "ALL".into(),
        phoenix_symbol: None,
        period: day.label(),
        complete,
    };
    let select = contexts_select(&staged, case);
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, "at_ms, symbol", &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dexes_follow_the_coins() {
        let lane = ContextLane::new(
            "https://x/info",
            vec![
                ("SOL".into(), "SOL".into()),
                ("xyz:NVDA".into(), "NVDA".into()),
                ("xyz:GOLD".into(), "GOLD".into()),
                ("flx:OIL".into(), "WTIOIL".into()),
            ],
        );
        assert_eq!(lane.dexes, [None, Some("flx".into()), Some("xyz".into())]);
        assert_eq!(lane.buffered(), 0);
    }
}
