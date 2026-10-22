//! Kalshi's public market data (ADR-0009, "Kalshi ladders", 2026-10-22): the curated series'
//! contracts from both tiers, one-minute (or hourly) candles of each event's kept strikes, and
//! their trades. Collected under the operator's written permission
//! (`docs/licences/kalshi-consent.md`): internal storage and backup only, never published.
//!
//! A run lists each series' live tier, then walks forward from the newest stored day to today
//! (today's closed events are stored as they close), then spends what is left of its budget
//! (`runMinutes`, `maxRequests`, Kalshi requests only) round-robin over the series: one day of
//! the backward walk, or one page of the archive listing when the next day is not catalogued
//! yet. Every step is saved in the ledger before the next, so a stopped run resumes.

pub mod api;
pub mod catalogue;
pub mod health;
pub mod ladder;
pub mod rows;
pub mod store;
pub mod walk;

use crate::augment::config::{Kalshi, KalshiSeries};
use crate::augment::periods::{date_of_ms, ms_of, parse_date};
use crate::augment::series::{Ctx, Outcome};
use crate::jsonout::{Obj, log, now, safe_error};
use api::Api;
use catalogue::State;
use chrono::{Duration, NaiveDate};
use std::time::Instant;
use walk::{Stop, Walk};

/// The close day of an instant, as a period label.
#[must_use]
pub fn rows_day(ms: i64) -> String {
    date_of_ms(ms).to_string()
}

/// The run's budget on Kalshi's host.
struct Budget<'a> {
    ctx: &'a Ctx,
    url: String,
    started: Instant,
    first: u64,
    cfg: &'a Kalshi,
}

impl Budget<'_> {
    fn sent(&self) -> u64 {
        self.ctx.http.counts(&self.url).0.saturating_sub(self.first)
    }

    fn ok(&self) -> bool {
        let minutes = self.cfg.run_minutes;
        !self.ctx.stopping()
            && (minutes == 0 || self.started.elapsed().as_secs() < minutes * 60)
            && (self.cfg.max_requests == 0 || self.sent() < self.cfg.max_requests)
    }
}

/// One series through a run.
struct Run<'a> {
    spec: &'a KalshiSeries,
    state: State,
    outcome: Outcome,
}

/// Sync the curated series; the outcome and the `kalshi` status block.
pub async fn sync(ctx: &Ctx, cfg: &Kalshi, only: Option<&str>) -> (Outcome, Obj) {
    let api = Api::new(&ctx.http, &cfg.base_url);
    let budget = Budget {
        ctx,
        url: cfg.base_url.clone(),
        started: Instant::now(),
        first: ctx.http.counts(&cfg.base_url).0,
        cfg,
    };
    let start = cfg
        .start_date
        .as_deref()
        .and_then(parse_date)
        .unwrap_or(ctx.start);
    let cutoff_ms = match api.cutoff_ms().await {
        Ok(ms) => ms,
        Err(error) => {
            log_error("cutoff", "ALL", &error.to_string());
            let outcome = Outcome {
                errors: 1,
                ..Outcome::default()
            };
            return (outcome, Obj::new().with("error", "cutoff unavailable"));
        }
    };
    let mut runs = Vec::new();
    for spec in cfg
        .series
        .iter()
        .filter(|s| only.is_none_or(|o| o == s.ticker || s.asset.as_deref() == Some(o)))
    {
        let state = catalogue::load(ctx, &spec.ticker).await.unwrap_or_default();
        runs.push(Run {
            spec,
            state,
            outcome: Outcome::default(),
        });
    }
    let may_continue = || budget.ok();
    for run in &mut runs {
        if !budget.ok() {
            break;
        }
        forward(&api, ctx, run, start, cutoff_ms, &may_continue).await;
    }
    loop {
        let mut progressed = false;
        for run in &mut runs {
            if !budget.ok() {
                break;
            }
            progressed |= backward(&api, ctx, run, start, cutoff_ms, &may_continue).await;
        }
        if !progressed || !budget.ok() {
            break;
        }
    }
    let mut outcome = Outcome::default();
    let mut series = Obj::new();
    for run in &runs {
        outcome.add(&run.outcome);
        series.set_obj(&run.spec.ticker, series_status(run));
    }
    let (sent, limited) = (budget.sent(), ctx.http.counts(&cfg.base_url).1);
    outcome.requests = sent;
    let detail = Obj::new()
        .with("start", start.to_string())
        .with("cutoff", rows_day(cutoff_ms))
        .with("requests", sent)
        .with("rateLimited", limited)
        .with("seconds", budget.started.elapsed().as_secs())
        .with("budgetSpent", !budget.ok() && !ctx.stopping())
        .with_obj("series", series);
    (outcome, detail)
}

fn series_status(run: &Run<'_>) -> Obj {
    let s = &run.state;
    Obj::new()
        .with("head", s.head.clone())
        .with("tail", s.tail.clone())
        .with("tailDone", s.tail_done)
        .with("tailMovedAt", s.tail_moved_at.clone())
        .with("histDone", s.hist_done)
        .with("histOldest", s.hist_oldest_ms.map(rows_day))
        .with("files", run.outcome.files)
        .with("rows", run.outcome.rows)
        .with("errors", run.outcome.errors)
}

async fn save(ctx: &Ctx, run: &mut Run<'_>) {
    if let Err(error) = catalogue::save(ctx, &run.spec.ticker, &run.state).await {
        run.outcome.errors += 1;
        log_error("state", &run.spec.ticker, &error.to_string());
    }
}

/// The live listing, then the forward walk from the head day through today.
async fn forward(
    api: &Api<'_>,
    ctx: &Ctx,
    run: &mut Run<'_>,
    start: NaiveDate,
    cutoff_ms: i64,
    may: &(dyn Fn() -> bool + Sync),
) {
    let floor = cutoff_ms - 86_400_000;
    match catalogue::live(api, ctx, run.spec, &mut run.state, floor, ms_of(start)).await {
        Ok((files, rows)) => {
            run.outcome.files += files;
            run.outcome.rows += rows;
        }
        Err(error) => {
            run.outcome.errors += 1;
            log_error("contracts", &run.spec.ticker, &error.to_string());
            return;
        }
    }
    let today = date_of_ms(ctx.now_ms);
    let mut head = run
        .state
        .head
        .as_deref()
        .and_then(parse_date)
        .unwrap_or(today - Duration::days(1))
        .max(start);
    if run.state.tail.is_none() {
        run.state.tail = Some((head - Duration::days(1)).to_string());
    }
    save(ctx, run).await;
    while head <= today && may() && catalogue::covers(&run.state, head, cutoff_ms) {
        let complete = ctx.now_ms >= ms_of(head + Duration::days(1)) + walk::SETTLE_LAG_MS;
        let skip = if run.state.head.as_deref() == Some(&head.to_string()) {
            run.state.head_events.clone()
        } else {
            Vec::new()
        };
        let Some(stored) = day(api, ctx, run, head, &skip, complete, may).await else {
            return;
        };
        run.state.head = Some(head.to_string());
        if complete {
            run.state.head_events.clear();
            head += Duration::days(1);
            run.state.head = Some(head.to_string());
        } else {
            run.state.head_events.extend(stored);
        }
        save(ctx, run).await;
        if !complete {
            return;
        }
    }
}

/// One step of the backward walk, or one archive page; whether anything moved.
async fn backward(
    api: &Api<'_>,
    ctx: &Ctx,
    run: &mut Run<'_>,
    start: NaiveDate,
    cutoff_ms: i64,
    may: &(dyn Fn() -> bool + Sync),
) -> bool {
    if run.state.tail_done || run.outcome.errors > 0 {
        return false;
    }
    let Some(tail) = run.state.tail.as_deref().and_then(parse_date) else {
        return false;
    };
    if tail < start {
        run.state.tail_done = true;
        save(ctx, run).await;
        return true;
    }
    if !catalogue::covers(&run.state, tail, cutoff_ms) {
        if run.state.hist_done {
            return false;
        }
        return match catalogue::historical_step(
            api,
            ctx,
            run.spec,
            &mut run.state,
            ms_of(start),
            cutoff_ms,
        )
        .await
        {
            Ok((files, rows)) => {
                run.outcome.files += files;
                run.outcome.rows += rows;
                save(ctx, run).await;
                true
            }
            Err(error) => {
                run.outcome.errors += 1;
                log_error("contracts", &run.spec.ticker, &error.to_string());
                false
            }
        };
    }
    if day(api, ctx, run, tail, &[], true, may).await.is_none() {
        return false;
    }
    run.state.tail = Some((tail - Duration::days(1)).to_string());
    run.state.tail_moved_at = Some(now());
    save(ctx, run).await;
    true
}

/// Store one day; the events stored, or `None` when the day stopped short.
#[allow(clippy::too_many_arguments)]
async fn day(
    api: &Api<'_>,
    ctx: &Ctx,
    run: &mut Run<'_>,
    day: NaiveDate,
    skip: &[String],
    complete: bool,
    may: &(dyn Fn() -> bool + Sync),
) -> Option<Vec<String>> {
    let days: Vec<String> = (0..=2)
        .rev()
        .map(|d| (day - Duration::days(d)).to_string())
        .collect();
    let contracts = match store::contracts(ctx, &run.spec.ticker, &days).await {
        Ok(c) => c,
        Err(error) => {
            run.outcome.errors += 1;
            log_error("contracts", &run.spec.ticker, &error.to_string());
            return None;
        }
    };
    let walk = Walk {
        api,
        ctx,
        spec: run.spec,
        may_continue: may,
    };
    match walk.day(&day.to_string(), contracts, skip, complete).await {
        Ok(stored) => {
            run.outcome.files += stored.files;
            run.outcome.rows += stored.rows;
            Some(stored.events)
        }
        Err(Stop::Budget) => None,
        Err(Stop::Failed(error)) => {
            run.outcome.errors += 1;
            log_error(
                "candles",
                &format!("{}/{day}", run.spec.ticker),
                &error.to_string(),
            );
            None
        }
    }
}

fn log_error(dataset: &str, item: &str, error: &str) {
    log(
        "augment_series_error",
        Obj::new()
            .with("source", store::SOURCE)
            .with("dataset", dataset)
            .with("item", item)
            .with("error", safe_error(error)),
    );
}
