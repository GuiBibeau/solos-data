//! The supervised collector: the historical lane, the tail follower and the status observer run
//! concurrently; the writer thread serializes database work. A port of `service.ts`.

use super::config::Config;
use super::exchange::{Exchange, confirm_mainnet, refresh_exchange};
use super::history::backfill;
use super::maintenance::maintain;
use super::pipeline::tail_cycle;
use super::rpc::{Provider, Rpc};
use super::status::write_status;
use super::writer::recover_files;
use crate::db::Db;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// `(code N)` from an RPC failure message, if any.
fn rpc_code(message: &str) -> Option<i64> {
    let start = message.find("(code ")? + 6;
    let rest = &message[start..];
    let end = rest.find(')')?;
    rest[..end].parse().ok()
}

async fn record_error(db: &Db, provider: &Provider, lane: &str, error: &StoreError) {
    let detail = Obj::new()
        .with("lane", lane)
        .with("error", safe_error(&error.to_string()))
        .with("at", now());
    let value = detail.to_value();
    let provider_name = provider.provider_name().to_owned();
    let lane = lane.to_owned();
    let text = detail.str("error").unwrap_or("").to_owned();
    let at = detail.str("at").unwrap_or("").to_owned();
    let _ = db
        .run(move |store| {
            store.set("last-error", &value)?;
            store.exec(
                "INSERT INTO errors VALUES (?, ?, ?, ?, ?)",
                &[&"service", &lane, &provider_name, &text, &at],
            )?;
            Ok(())
        })
        .await;
    log("collector_error", detail);
}

/// Run until a signal arrives or a lane fails fatally.
pub async fn run(provider: Arc<Provider>, db: Db, config: Config) -> Result<(), StoreError> {
    let data_dir = config.data_dir.clone();
    db.run(move |store| recover_files(store, &data_dir)).await?;
    confirm_mainnet(provider.as_ref()).await?;
    let http = reqwest::Client::new();
    let exchange: Arc<std::sync::Mutex<Exchange>> = Arc::new(std::sync::Mutex::new(
        refresh_exchange(provider.as_ref(), &db, &config, &http).await?,
    ));
    if db.get("H0").await?.is_none() {
        let recent = super::fetcher::value(
            provider.as_ref(),
            "getSignaturesForAddress",
            json!([config.program_id, { "limit": 1, "commitment": "finalized" }]),
            super::config::Lane::Tail,
        )
        .await?;
        let Some(first) = recent.as_array().and_then(|a| a.first()) else {
            return Err(StoreError::Check(
                "Program has no finalized signatures".into(),
            ));
        };
        db.set(
            "H0",
            json!({ "signature": first["signature"], "slot": first["slot"], "recordedAt": now() }),
        )
        .await?;
    }
    let stopped: Arc<AtomicBool> = Arc::clone(&provider.shutdown);
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stopped))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stopped))?;
    let markets = exchange.lock().expect("exchange").markets;
    log(
        "collector_started",
        Obj::new()
            .with("program", config.program_id.clone())
            .with("markets", markets)
            .with("backfill", config.backfill_enabled),
    );

    let announcer = {
        let stopped = Arc::clone(&stopped);
        async move {
            while !stopped.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            log("shutdown_requested", Obj::new());
        }
    };

    let history = {
        let (provider, db, config, exchange, stopped) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&exchange),
            Arc::clone(&stopped),
        );
        async move {
            if !config.backfill_enabled {
                return Ok::<(), StoreError>(());
            }
            let archive_enabled = config.archive_backfill_enabled && cfg!(feature = "archive");
            if config.archive_backfill_enabled && !archive_enabled {
                log("archive_lane_unavailable", Obj::new());
            }
            // The tip is probed in the background every ten minutes; a throttled probe keeps the
            // last known value, which also survives restarts through the `archive-tip` record.
            let known: Option<i64> = db
                .get("archive-tip")
                .await
                .ok()
                .flatten()
                .and_then(|v| v.get("slot").and_then(Value::as_i64));
            let tip: Arc<std::sync::Mutex<Option<i64>>> = Arc::new(std::sync::Mutex::new(known));
            if archive_enabled {
                let (tip, db, config, stopped) = (
                    Arc::clone(&tip),
                    db.clone(),
                    config.clone(),
                    Arc::clone(&stopped),
                );
                tokio::spawn(async move {
                    while !stopped.load(Ordering::Relaxed) {
                        let hint = db
                            .get("W")
                            .await
                            .ok()
                            .flatten()
                            .and_then(|w| w.get("slot").and_then(Value::as_i64))
                            .map(|s| (s / 432_000) as u64);
                        let probe = super::archive::probe_tip(&config, hint).await;
                        let current = *tip.lock().expect("tip");
                        let next = super::archive::apply_probe(probe, current);
                        if probe == super::archive::TipProbe::Unknown {
                            log(
                                "archive_tip_unknown",
                                Obj::new().with("kept", current.map_or(Value::Null, Value::from)),
                            );
                        } else if next != current || current.is_none() {
                            log(
                                "archive_tip",
                                Obj::new().with("slot", next.map_or(Value::Null, Value::from)),
                            );
                        }
                        *tip.lock().expect("tip") = next;
                        if let Some(slot) = next {
                            let _ = db
                                .set("archive-tip", json!({ "slot": slot, "at": now() }))
                                .await;
                        }
                        tokio::time::sleep(Duration::from_secs(600)).await;
                    }
                });
            }
            while !stopped.load(Ordering::Relaxed) {
                let program_data = exchange.lock().expect("exchange").program_data.clone();
                let tip_reader = Arc::clone(&tip);
                let rpc: Arc<dyn super::rpc::Rpc> = provider.clone();
                match backfill(
                    rpc,
                    &db,
                    &config,
                    &program_data,
                    &move || *tip_reader.lock().expect("tip"),
                    archive_enabled,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        if stopped.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                        record_error(&db, &provider, "backfill", &error).await;
                        if matches!(rpc_code(&error.to_string()), Some(401 | 403)) {
                            stopped.store(true, Ordering::Relaxed);
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
            Ok(())
        }
    };

    let follower = {
        let (provider, db, config, exchange, stopped, http) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&exchange),
            Arc::clone(&stopped),
            http.clone(),
        );
        async move {
            let mut last_refresh = Instant::now();
            let mut last_maintenance = Instant::now();
            let mut error_streak = 0u64;
            if !config.tail_enabled {
                log("tail_lane_disabled", Obj::new());
            }
            while !stopped.load(Ordering::Relaxed) {
                let started = Instant::now();
                let outcome: Result<(), StoreError> = async {
                    if last_refresh.elapsed() > Duration::from_secs(config.exchange_refresh_seconds)
                    {
                        let refreshed =
                            refresh_exchange(provider.as_ref(), &db, &config, &http).await?;
                        *exchange.lock().expect("exchange") = refreshed;
                        last_refresh = Instant::now();
                    }
                    let program_data = exchange.lock().expect("exchange").program_data.clone();
                    let rpc: Arc<dyn super::rpc::Rpc> = provider.clone();
                    if config.tail_enabled {
                        tail_cycle(rpc, &db, &config, &program_data).await?;
                    }
                    error_streak = 0;
                    if last_maintenance.elapsed()
                        > Duration::from_secs(config.maintenance_interval_seconds)
                    {
                        let config2 = config.clone();
                        let result = db
                            .run(move |store| maintain(store, &config2, false, false))
                            .await?;
                        log("storage_maintenance", result);
                        last_maintenance = Instant::now();
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = outcome {
                    if stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    error_streak += 1;
                    record_error(&db, &provider, "tail", &error).await;
                }
                let _ = db.set("tail-health", json!({ "errorStreak": error_streak, "cycleDurationSeconds": started.elapsed().as_secs_f64() })).await;
                let delay = Duration::from_secs(config.tail_interval_seconds)
                    .saturating_sub(started.elapsed())
                    .max(Duration::from_secs(1));
                tokio::time::sleep(delay).await;
            }
            Ok::<(), StoreError>(())
        }
    };

    let observer = {
        let (provider, db, config, stopped) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&stopped),
        );
        async move {
            while !stopped.load(Ordering::Relaxed) {
                let (provider2, root) = (Arc::clone(&provider), config.data_dir.clone());
                let _ = db
                    .run(move |store| write_status(store, &root, Some(provider2.as_ref())))
                    .await;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    };

    let (history_result, follower_result, (), ()) =
        tokio::join!(history, follower, observer, announcer);
    history_result?;
    follower_result?;
    let (provider2, root) = (Arc::clone(&provider), config.data_dir.clone());
    db.run(move |store| write_status(store, &root, Some(provider2.as_ref())))
        .await?;
    log(
        "collector_stopped",
        Obj::new().with("metrics", provider.counters().to_json()),
    );
    Ok(())
}
