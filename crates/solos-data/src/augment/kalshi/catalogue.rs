//! The contract catalogue of one series, from both tiers. The live tier is listed every run
//! from a frontier (the earliest close among markets not yet final, else the latest close, less
//! a day); the archive is walked once, newest close first, one page a step, by cursor. Each
//! page is merged into the close-day files before its cursor is saved, so a stopped run resumes
//! without a gap and a repeated page only rewrites the same rows. Pages are written open; a day
//! is sealed (marked complete) only once the listing proves it whole: in the live tier, after the
//! full listing, a past day after the frontier day whose markets are all final; in the archive, a
//! day newer than the page's oldest close (or any day once the walk ends) and at least two days
//! before the cutoff day (a market closing near the cutoff can settle after it, in the live tier).

use super::api::Api;
use super::rows::{Kind, market_row};
use super::store::{Dest, seal, write};
use crate::augment::config::KalshiSeries;
use crate::augment::ledger::{get_progress, set_progress};
use crate::augment::periods::date_of_ms;
use crate::augment::series::Ctx;
use crate::store::StoreError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

const DAY_MS: i64 = 86_400_000;

/// What the ledger keeps per series (`kalshi/<SERIES>`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    /// Where the next live listing starts, milliseconds.
    #[serde(default)]
    pub live_from_ms: Option<i64>,
    /// The archive walk's next page.
    #[serde(default)]
    pub hist_cursor: Option<String>,
    /// The oldest close the archive walk has listed, milliseconds.
    #[serde(default)]
    pub hist_oldest_ms: Option<i64>,
    /// The archive walk reached the start day (or the archive's end).
    #[serde(default)]
    pub hist_done: bool,
    /// The next day the forward walk stores (it reaches today, partially).
    #[serde(default)]
    pub head: Option<String>,
    /// Events of the head day already stored while the day was open.
    #[serde(default)]
    pub head_events: Vec<String>,
    /// The next day the backward walk stores.
    #[serde(default)]
    pub tail: Option<String>,
    /// The backward walk passed the start day.
    #[serde(default)]
    pub tail_done: bool,
    /// When the backward walk last stored a day.
    #[serde(default)]
    pub tail_moved_at: Option<String>,
}

fn key(series: &str) -> String {
    format!("kalshi/{series}")
}

/// The series' state.
pub async fn load(ctx: &Ctx, series: &str) -> Result<State, StoreError> {
    let key = key(series);
    let value = ctx.db.run(move |store| get_progress(store, &key)).await?;
    Ok(value
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default())
}

/// Save the series' state.
pub async fn save(ctx: &Ctx, series: &str, state: &State) -> Result<(), StoreError> {
    let key = key(series);
    let value = serde_json::to_value(state).map_err(|e| StoreError::Check(e.to_string()))?;
    ctx.db
        .run(move |store| set_progress(store, &key, &value))
        .await
}

fn is_final(market: &Value) -> bool {
    matches!(
        market.get("status").and_then(Value::as_str),
        Some("finalized" | "settled")
    )
}

/// What one page wrote: files, rows, and per close day whether every market on it is final.
type Written = (u64, u64, BTreeMap<String, bool>);

/// Merge one page into its close-day files, all left open.
async fn store_page(
    ctx: &Ctx,
    spec: &KalshiSeries,
    page: &[Value],
    tier: &str,
    start_ms: i64,
) -> Result<Written, StoreError> {
    let fetched_at = chrono::Utc::now().timestamp_millis();
    let mut days: BTreeMap<String, (Vec<Value>, bool)> = BTreeMap::new();
    for market in page {
        let Some(row) = market_row(
            market,
            &spec.ticker,
            spec.asset.as_deref(),
            tier,
            fetched_at,
        ) else {
            continue;
        };
        if row["close_ms"].as_i64().unwrap_or(0) < start_ms {
            continue;
        }
        let day = row["close_day"].as_str().unwrap_or("").to_owned();
        let entry = days.entry(day).or_insert_with(|| (Vec::new(), true));
        entry.1 &= is_final(market);
        entry.0.push(row);
    }
    let (mut files, mut rows) = (0, 0);
    let mut finality = BTreeMap::new();
    for (day, (batch, all_final)) in days {
        let dest = Dest {
            kind: Kind::Contracts,
            dataset: "contracts",
            series: &spec.ticker,
            day: &day,
            complete: false,
        };
        write(ctx, &dest, &batch).await?;
        files += 1;
        rows += batch.len() as u64;
        finality.insert(day, all_final);
    }
    Ok((files, rows, finality))
}

/// The days of one listing that may be sealed: final, and inside `(after, before)`.
#[must_use]
pub fn sealable(finality: &BTreeMap<String, bool>, after: &str, before: &str) -> Vec<String> {
    finality
        .iter()
        .filter(|(day, all_final)| **all_final && day.as_str() > after && day.as_str() < before)
        .map(|(day, _)| day.clone())
        .collect()
}

/// The next live frontier from one listing: a day before the earliest close of a market not
/// yet final, else before the latest close seen.
#[must_use]
pub fn next_live_from(markets: &[Value], previous_ms: i64) -> i64 {
    let close = |m: &Value| {
        m.get("close_time")
            .and_then(Value::as_str)
            .and_then(super::rows::parse_ms)
    };
    let pending = markets
        .iter()
        .filter(|m| !is_final(m))
        .filter_map(close)
        .min();
    let latest = markets.iter().filter_map(close).max();
    pending
        .or(latest)
        .map_or(previous_ms, |ms| (ms - DAY_MS).max(previous_ms))
}

/// List the live tier from the frontier; files and rows written.
pub async fn live(
    api: &Api<'_>,
    ctx: &Ctx,
    spec: &KalshiSeries,
    state: &mut State,
    floor_ms: i64,
    start_ms: i64,
) -> Result<(u64, u64), StoreError> {
    let from = state.live_from_ms.unwrap_or(floor_ms).max(start_ms);
    let mut cursor: Option<String> = None;
    let mut seen = Vec::new();
    let (mut files, mut rows) = (0, 0);
    let mut finality: BTreeMap<String, bool> = BTreeMap::new();
    loop {
        let (page, next) = api
            .live_markets(&spec.ticker, from / 1000, cursor.as_deref())
            .await?;
        let (f, r, days) = store_page(ctx, spec, &page, "live", start_ms).await?;
        files += f;
        rows += r;
        for (day, all_final) in days {
            *finality.entry(day).or_insert(true) &= all_final;
        }
        seen.extend(page);
        cursor = next;
        if cursor.is_none() || ctx.stopping() {
            break;
        }
    }
    if cursor.is_none() {
        state.live_from_ms = Some(next_live_from(&seen, from));
        let today = date_of_ms(ctx.now_ms).to_string();
        seal(
            ctx,
            &spec.ticker,
            sealable(&finality, &date_of_ms(from).to_string(), &today),
        )
        .await?;
    }
    Ok((files, rows))
}

/// One page of the archive walk; files and rows written.
pub async fn historical_step(
    api: &Api<'_>,
    ctx: &Ctx,
    spec: &KalshiSeries,
    state: &mut State,
    start_ms: i64,
    cutoff_ms: i64,
) -> Result<(u64, u64), StoreError> {
    let (page, next) = api
        .historical_markets(&spec.ticker, state.hist_cursor.as_deref())
        .await?;
    let (files, rows, finality) = store_page(ctx, spec, &page, "historical", start_ms).await?;
    let oldest = page
        .iter()
        .filter_map(|m| {
            m.get("close_time")
                .and_then(Value::as_str)
                .and_then(super::rows::parse_ms)
        })
        .min();
    if let Some(oldest) = oldest {
        state.hist_oldest_ms = Some(state.hist_oldest_ms.map_or(oldest, |o| o.min(oldest)));
    }
    state.hist_done = next.is_none() || page.is_empty() || oldest.is_some_and(|o| o < start_ms);
    state.hist_cursor = if state.hist_done { None } else { next };
    let after = match (state.hist_done, oldest) {
        (false, Some(ms)) => date_of_ms(ms).to_string(),
        _ => String::new(),
    };
    let before = (date_of_ms(cutoff_ms) - chrono::Duration::days(1)).to_string();
    seal(ctx, &spec.ticker, sealable(&finality, &after, &before)).await?;
    Ok((files, rows))
}

/// Whether the catalogue holds every contract closing on `day` and the two days before it (the
/// centre looks back up to two days): the live tier lists every day after the cutoff day; the
/// archive walk, every day after the oldest close it has reached.
#[must_use]
pub fn covers(state: &State, day: chrono::NaiveDate, cutoff_ms: i64) -> bool {
    let earliest = day - chrono::Duration::days(2);
    if state.hist_done || earliest > date_of_ms(cutoff_ms) {
        return true;
    }
    state
        .hist_oldest_ms
        .is_some_and(|ms| earliest > date_of_ms(ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::augment::periods::{ms_of, parse_date};
    use serde_json::json;

    fn day(text: &str) -> chrono::NaiveDate {
        parse_date(text).unwrap()
    }

    #[test]
    fn coverage_needs_both_tiers_below_the_cutoff() {
        let cutoff = ms_of(day("2026-08-23"));
        let mut state = State::default();
        assert!(covers(&state, day("2026-08-26"), cutoff));
        assert!(
            !covers(&state, day("2026-08-25"), cutoff),
            "the cutoff day is split across tiers"
        );
        state.hist_oldest_ms = Some(ms_of(day("2026-08-20")) + 3_600_000);
        assert!(covers(&state, day("2026-08-23"), cutoff));
        assert!(
            !covers(&state, day("2026-08-22"), cutoff),
            "the oldest day may be partly listed"
        );
        state.hist_done = true;
        assert!(covers(&state, day("2025-10-01"), cutoff));
    }

    #[test]
    fn the_live_frontier_waits_for_the_earliest_pending_market() {
        let m = |close: &str, status: &str| json!({"close_time": close, "status": status});
        let ms = |t: &str| super::super::rows::parse_ms(t).unwrap();
        let page = [
            m("2026-10-20T10:00:00Z", "finalized"),
            m("2026-10-21T10:00:00Z", "determined"),
            m("2026-10-23T10:00:00Z", "active"),
        ];
        assert_eq!(next_live_from(&page, 0), ms("2026-10-20T10:00:00Z"));
        let settled = [
            m("2026-10-20T10:00:00Z", "finalized"),
            m("2026-10-21T10:00:00Z", "settled"),
        ];
        assert_eq!(next_live_from(&settled, 0), ms("2026-10-20T10:00:00Z"));
        assert_eq!(next_live_from(&[], 5), 5);
        assert_eq!(
            next_live_from(&settled, ms("2026-10-22T00:00:00Z")),
            ms("2026-10-22T00:00:00Z"),
            "never moves back"
        );
    }

    #[test]
    fn only_whole_final_days_are_sealed() {
        let finality: BTreeMap<String, bool> = [
            ("2026-08-19", true),
            ("2026-08-20", true),
            ("2026-08-21", false),
            ("2026-08-22", true),
        ]
        .into_iter()
        .map(|(d, f)| (d.to_owned(), f))
        .collect();
        // An archive page whose oldest close is on the 19th, cutoff on the 23rd.
        assert_eq!(
            sealable(&finality, "2026-08-19", "2026-08-22"),
            ["2026-08-20"]
        );
        // The walk ended: the oldest day is whole too.
        assert_eq!(
            sealable(&finality, "", "2026-08-22"),
            ["2026-08-19", "2026-08-20"]
        );
    }

    #[test]
    fn state_round_trips_and_tolerates_missing_fields() {
        let state: State = serde_json::from_value(json!({"head": "2026-10-21"})).unwrap();
        assert_eq!(state.head.as_deref(), Some("2026-10-21"));
        assert!(!state.hist_done);
        let back: State = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert_eq!(back, state);
    }
}
