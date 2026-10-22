//! One close day of one series: its events' kept strikes, their candles over the stored life,
//! and the trades of the kept markets that traded. A market the archive listed is fetched one
//! market a call from `/historical`; a market the live tier listed goes through the live batch
//! endpoint (even when it closed before the cutoff: it settled after it), and any market the
//! batch answer leaves out (archived since it was listed) is retried in the archive. The day's rows are written once, after every event is fetched: a run stopped
//! by its budget mid-day writes nothing for that day and the day is redone next run.

use super::api::{self, Api};
use super::ladder::{self, Contract, Event};
use super::rows::{Kind, candle_dataset, candle_row, trade_row};
use super::store::{Dest, write};
use crate::augment::config::KalshiSeries;
use crate::augment::series::Ctx;
use crate::store::StoreError;
use serde_json::{Value, json};
use std::collections::HashMap;

const HOUR_MS: i64 = 3_600_000;
/// Minutes after an event's close before its candles are fetched.
pub const SETTLE_LAG_MS: i64 = 10 * 60_000;

/// Why a day stopped short.
#[derive(Debug)]
pub enum Stop {
    /// The run's budget or a stop request.
    Budget,
    /// A request or a write failed.
    Failed(StoreError),
}

impl From<StoreError> for Stop {
    fn from(error: StoreError) -> Stop {
        Stop::Failed(error)
    }
}

/// What a day stored.
#[derive(Debug, Default)]
pub struct Stored {
    /// Events whose rows were written.
    pub events: Vec<String>,
    /// Files written.
    pub files: u64,
    /// Rows written.
    pub rows: u64,
}

/// The shared inputs of a walk step.
pub struct Walk<'a> {
    /// The API.
    pub api: &'a Api<'a>,
    /// The run's context.
    pub ctx: &'a Ctx,
    /// The series.
    pub spec: &'a KalshiSeries,
    /// Whether the run may send another request.
    pub may_continue: &'a (dyn Fn() -> bool + Sync),
}

impl Walk<'_> {
    fn check(&self) -> Result<(), Stop> {
        if (self.may_continue)() {
            Ok(())
        } else {
            Err(Stop::Budget)
        }
    }

    /// Store `day`'s closed events not in `skip`; `complete` marks the files final.
    pub async fn day(
        &self,
        day: &str,
        contracts: Vec<Contract>,
        skip: &[String],
        complete: bool,
    ) -> Result<Stored, Stop> {
        let all = ladder::events(contracts);
        let ready = self.ctx.now_ms - SETTLE_LAG_MS;
        let mut candles = Vec::new();
        let mut trades = Vec::new();
        let mut stored = Stored::default();
        for event in all.iter().filter(|e| super::rows_day(e.close_ms) == day) {
            if event.close_ms > ready || skip.contains(&event.ticker) {
                continue;
            }
            let (centre, source) = ladder::centre(event, &all);
            let kept = ladder::select(event, centre, self.spec);
            let mut rows = self.event_candles(event, &kept).await?;
            for row in &mut rows {
                let strike = kept
                    .iter()
                    .find(|m| Some(m.ticker.as_str()) == row["ticker"].as_str())
                    .and_then(|m| m.strike);
                row["strike"] = json!(strike);
                row["series_ticker"] = json!(self.spec.ticker);
                row["centre"] = json!(centre);
                row["centre_source"] = json!(source);
            }
            candles.extend(rows);
            if self.spec.trades {
                trades.extend(self.event_trades(day, &kept).await?);
            }
            stored.events.push(event.ticker.clone());
        }
        let dataset = candle_dataset(self.spec.interval);
        for (kind, name, rows) in [
            (Kind::Candles, dataset, &candles),
            (Kind::Trades, "trades", &trades),
        ] {
            // An empty day writes nothing, unless it closes a file the open day started.
            if rows.is_empty() && !(complete && self.exists(name, day)) {
                continue;
            }
            let dest = Dest {
                kind,
                dataset: name,
                series: &self.spec.ticker,
                day,
                complete,
            };
            write(self.ctx, &dest, rows).await?;
            stored.files += 1;
            stored.rows += rows.len() as u64;
        }
        Ok(stored)
    }

    fn exists(&self, dataset: &str, day: &str) -> bool {
        let path = format!(
            "tables/{}/{dataset}/{}/{day}.parquet",
            super::store::SOURCE,
            self.spec.ticker
        );
        self.ctx.root.join(path).is_file()
    }

    /// The window of candles stored for an event: its life, at most `maxLifeHours`.
    fn window(&self, event: &Event) -> (i64, i64) {
        let life = self.spec.max_life_hours.max(1) * HOUR_MS;
        (
            event.open_ms.max(event.close_ms - life) / 1000,
            event.close_ms / 1000,
        )
    }

    async fn event_candles(&self, event: &Event, kept: &[&Contract]) -> Result<Vec<Value>, Stop> {
        let (start, end) = self.window(event);
        let interval = self.spec.interval;
        let mut out = Vec::new();
        let (archived, live): (Vec<&&Contract>, Vec<&&Contract>) =
            kept.iter().partition(|m| m.historical);
        let mut missing: Vec<&Contract> = archived.into_iter().copied().collect();
        let tickers: Vec<String> = live.iter().map(|m| m.ticker.clone()).collect();
        let mut answered: HashMap<String, usize> = HashMap::new();
        for (from, until) in api::windows(start, end, interval, api::SINGLE_PERIODS) {
            for group in tickers.chunks(api::batch_width(until - from, interval)) {
                self.check()?;
                for (ticker, candles) in
                    self.api.batch_candles(group, from, until, interval).await?
                {
                    *answered.entry(ticker.clone()).or_default() += 1;
                    out.extend(tagged(&ticker, &event.ticker, &candles, interval, "live"));
                }
            }
        }
        missing.extend(
            live.iter()
                .filter(|m| !answered.contains_key(&m.ticker))
                .map(|m| **m),
        );
        for market in missing {
            for (from, until) in api::windows(start, end, interval, api::SINGLE_PERIODS) {
                self.check()?;
                let candles = self
                    .api
                    .historical_candles(&market.ticker, from, until, interval)
                    .await?;
                out.extend(tagged(
                    &market.ticker,
                    &event.ticker,
                    &candles,
                    interval,
                    "historical",
                ));
            }
        }
        out.sort_by(|a, b| {
            (a["ticker"].as_str(), a["t"].as_i64()).cmp(&(b["ticker"].as_str(), b["t"].as_i64()))
        });
        out.dedup_by(|a, b| a["ticker"] == b["ticker"] && a["t"] == b["t"]);
        Ok(out)
    }

    async fn event_trades(&self, day: &str, kept: &[&Contract]) -> Result<Vec<Value>, Stop> {
        let mut out = Vec::new();
        for market in kept.iter().filter(|m| m.volume > 0.0) {
            let mut historical = market.historical;
            let mut cursor: Option<String> = None;
            let mut fell_back = false;
            loop {
                self.check()?;
                let (page, next) = self
                    .api
                    .trades(&market.ticker, historical, cursor.as_deref())
                    .await?;
                if page.is_empty() && cursor.is_none() && !historical && !fell_back {
                    // Archived since it was listed: ask the archive once.
                    historical = true;
                    fell_back = true;
                    continue;
                }
                let fetched_at = chrono::Utc::now().timestamp_millis();
                out.extend(
                    page.iter()
                        .filter_map(|t| trade_row(t, &self.spec.ticker, day, fetched_at)),
                );
                cursor = next;
                if cursor.is_none() || page.is_empty() {
                    break;
                }
            }
        }
        Ok(out)
    }
}

fn tagged(ticker: &str, event: &str, candles: &[Value], interval: u32, tier: &str) -> Vec<Value> {
    let fetched_at = chrono::Utc::now().timestamp_millis();
    candles
        .iter()
        .filter_map(|c| candle_row(c, interval))
        .map(|mut row| {
            row["ticker"] = json!(ticker);
            row["event_ticker"] = json!(event);
            row["tier"] = json!(tier);
            row["fetched_at"] = json!(fetched_at);
            row
        })
        .collect()
}
