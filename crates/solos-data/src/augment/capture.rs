//! `augment capture`: the long-running lane. Hyperliquid asset contexts every minute (flushed to
//! the day's file every ten), Hyperliquid 1-minute candles every half hour, Elfa every hour under
//! its credit guard. Each loop runs on its own cadence; SIGTERM ends them at their next check,
//! the buffers are flushed, and the catalog and status are written.

use super::candles::{self, CandleLane};
use super::config::AugmentConfig;
use super::contexts::{self, ContextLane};
use super::elfa::{self, ElfaLane};
use super::http::Http;
use super::ledger::{self, Lane};
use super::series::{Ctx, Outcome, budget_bytes};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Seconds between context flushes.
pub const FLUSH_SECONDS: u64 = 600;

/// What the lanes have done so far.
#[derive(Default)]
struct Totals {
    candles: Outcome,
    contexts: Outcome,
    snapshots: u64,
    elfa: Outcome,
    cycles: Obj,
}

/// Run the capture lane until the stop flag is set; returns the final status object.
pub fn run_capture(config: &AugmentConfig, stop: Arc<AtomicBool>) -> Result<Obj, StoreError> {
    let started_at = now();
    let started = std::time::Instant::now();
    let start = super::periods::parse_date(&config.start_date)
        .ok_or_else(|| StoreError::Check("Invalid startDate".into()))?;
    let mut store = ledger::open(&config.data_dir, Lane::Capture)?;
    let recovered = ledger::recover(&mut store, &config.data_dir, Lane::Capture)?;
    let (db, thread) = Db::spawn(store);
    let http = Http::new(config.requests_per_second)?;
    http.set_host_rate(
        &config.sources.hyperliquid.base_url,
        config.sources.hyperliquid.requests_per_second,
    );
    let elfa_key = std::env::var("ELFA_API_KEY").ok().filter(|k| !k.is_empty());
    let elfa_cfg = &config.sources.elfa;
    if elfa_cfg.enabled && elfa_key.is_some() {
        http.set_host_rate(
            &elfa_cfg.base_url,
            elfa_cfg.requests_per_minute as f64 / 60.0,
        );
    }
    let ctx = Arc::new(Ctx {
        http,
        db: db.clone(),
        root: config.data_dir.clone(),
        lane: Lane::Capture,
        start,
        now_ms: 0,
        stop,
        disk_budget_bytes: budget_bytes(config.disk_budget_gb),
    });
    let coins: Vec<(String, String)> = config
        .symbols
        .iter()
        .filter_map(|s| s.hyperliquid.clone().map(|coin| (coin, s.phoenix.clone())))
        .collect();
    let hl = &config.sources.hyperliquid;
    let info_url = format!("{}/info", hl.base_url.trim_end_matches('/'));
    let candle_lane = (hl.enabled && hl.candles).then(|| CandleLane {
        url: info_url.clone(),
        coins: coins.clone(),
    });
    let context_lane =
        (hl.enabled && hl.asset_contexts).then(|| ContextLane::new(&info_url, coins.clone()));
    let elfa_lane = match (&elfa_key, elfa_cfg.enabled) {
        (Some(key), true) => Some(ElfaLane::new(
            &elfa_cfg.base_url,
            key,
            super::periods::ms_of(start) / 1000,
        )),
        _ => {
            log(
                "augment_elfa_skipped",
                Obj::new().with(
                    "reason",
                    if elfa_cfg.enabled {
                        "ELFA_API_KEY unset"
                    } else {
                        "disabled"
                    },
                ),
            );
            None
        }
    };
    log(
        "augment_capture_start",
        Obj::new()
            .with("coins", coins.len())
            .with("candles", candle_lane.is_some())
            .with("assetContexts", context_lane.is_some())
            .with("elfa", elfa_lane.is_some())
            .with("recovered", recovered),
    );
    let totals = Arc::new(Mutex::new(Totals::default()));
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        tokio::join!(
            candle_loop(
                &ctx,
                candle_lane.as_ref(),
                hl.candle_interval_seconds,
                &totals
            ),
            context_loop(
                &ctx,
                context_lane.as_ref(),
                hl.asset_context_interval_seconds,
                &totals
            ),
            elfa_loop(&ctx, elfa_lane.as_ref(), elfa_cfg.interval_seconds, &totals),
            status_loop(&ctx, &totals, &started_at, elfa_lane.as_ref()),
        );
    });
    let status = write_status(&ctx, &totals, &started_at, elfa_lane.as_ref(), true)?;
    let store = thread.join(db);
    store.close()?;
    log(
        "augment_capture_done",
        Obj::new().with("durationSeconds", started.elapsed().as_secs_f64()),
    );
    Ok(status)
}

/// Sleep `seconds`, waking early on stop.
async fn pause(ctx: &Ctx, seconds: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline && !ctx.stopping() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn candle_loop(ctx: &Ctx, lane: Option<&CandleLane>, interval: u64, totals: &Mutex<Totals>) {
    let Some(lane) = lane else { return };
    while !ctx.stopping() {
        let outcome = candles::cycle(ctx, lane, now_ms()).await;
        {
            let mut guard = totals.lock().expect("totals");
            guard.candles.add(&outcome);
            guard.cycles.set("candlesAt", now());
        }
        pause(ctx, interval.max(60)).await;
    }
}

async fn context_loop(
    ctx: &Ctx,
    lane: Option<&ContextLane>,
    interval: u64,
    totals: &Mutex<Totals>,
) {
    let Some(lane) = lane else { return };
    let mut last_flush = tokio::time::Instant::now();
    while !ctx.stopping() {
        match contexts::snapshot(ctx, lane, now_ms()).await {
            Ok(_) => {
                let mut guard = totals.lock().expect("totals");
                guard.snapshots += 1;
                guard.cycles.set("contextsAt", now());
            }
            Err(error) => {
                totals.lock().expect("totals").contexts.errors += 1;
                log(
                    "augment_series_error",
                    Obj::new()
                        .with("source", "hyperliquid")
                        .with("dataset", "asset_contexts")
                        .with("symbol", "ALL")
                        .with("item", "snapshot")
                        .with("error", crate::jsonout::safe_error(&error.to_string())),
                );
            }
        }
        if last_flush.elapsed() >= Duration::from_secs(FLUSH_SECONDS) {
            let outcome = contexts::flush(ctx, lane, now_ms()).await;
            totals.lock().expect("totals").contexts.add(&outcome);
            last_flush = tokio::time::Instant::now();
        }
        pause(ctx, interval.max(10)).await;
    }
    let outcome = contexts::flush(ctx, lane, now_ms()).await;
    totals.lock().expect("totals").contexts.add(&outcome);
}

async fn elfa_loop(ctx: &Ctx, lane: Option<&ElfaLane>, interval: u64, totals: &Mutex<Totals>) {
    let Some(lane) = lane else { return };
    while !ctx.stopping() && !lane.disabled() {
        let outcome = elfa::cycle(ctx, lane, now_ms() / 1000).await;
        {
            let mut guard = totals.lock().expect("totals");
            guard.elfa.add(&outcome);
            guard.cycles.set("elfaAt", now());
        }
        pause(ctx, interval.max(60)).await;
    }
}

async fn status_loop(ctx: &Ctx, totals: &Mutex<Totals>, started_at: &str, elfa: Option<&ElfaLane>) {
    while !ctx.stopping() {
        pause(ctx, 60).await;
        if let Err(error) = write_status(ctx, totals, started_at, elfa, false) {
            log(
                "augment_status_error",
                Obj::new().with("error", crate::jsonout::safe_error(&error.to_string())),
            );
        }
    }
}

fn write_status(
    ctx: &Ctx,
    totals: &Mutex<Totals>,
    started_at: &str,
    elfa: Option<&ElfaLane>,
    final_write: bool,
) -> Result<Obj, StoreError> {
    let root = ctx.root.clone();
    let summary = ctx.db.run_blocking(move |store| {
        ledger::write_catalog(store, &root, Lane::Capture)?;
        ledger::summary(store)
    })?;
    let guard = totals.lock().expect("totals");
    let status = Obj::new()
        .with("at", now())
        .with("lane", "capture")
        .with("startedAt", started_at)
        .with("running", !final_write)
        .with_obj("candles", guard.candles.to_obj())
        .with_obj(
            "assetContexts",
            guard.contexts.to_obj().with("snapshots", guard.snapshots),
        )
        .with_obj(
            "elfa",
            guard.elfa.to_obj().with("enabled", elfa.is_some()).with(
                "disabledByCreditGuard",
                elfa.is_some_and(ElfaLane::disabled),
            ),
        )
        .with_obj("lastCycles", guard.cycles.clone())
        .with_rows("datasets", summary);
    drop(guard);
    ledger::write_status(&ctx.root, Lane::Capture, &status)?;
    Ok(status)
}
