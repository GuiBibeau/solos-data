//! The supervised collector: the historical lane, the tail follower and the status observer run
//! concurrently; the writer thread serializes database work. A port of `service.ts`.

use super::config::Config;
use super::exchange::{Exchange, confirm_mainnet, refresh_exchange};
use super::history::backfill;
use super::lanes::Lanes;
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

pub use crate::db::fatal_class;

/// The exit status of a fatal error, so systemd's `Restart=always` restarts the collector.
pub const FATAL_EXIT_CODE: i32 = 70;

/// What the service does after logging `collector_fatal`, given the lane and the class.
pub type FatalHook = Arc<dyn Fn(&str, &'static str) + Send + Sync>;

/// The production hook: exit at once, before any lane runs another statement. Reopening the
/// database replays its write-ahead log; startup recovery removes files written for a range whose
/// registration never committed.
#[must_use]
pub fn exit_on_fatal() -> FatalHook {
    Arc::new(|_lane, _class| {
        use std::io::Write;
        let _ = std::io::stdout().flush();
        let _ = std::io::stderr().flush();
        std::process::exit(FATAL_EXIT_CODE);
    })
}

/// Record a lane's error and count it in its streak. A fatal error (see [`fatal_class`]) skips
/// the database, logs `collector_fatal` and calls `fatal`; the result says whether it was fatal.
pub async fn lane_error(
    db: &Db,
    provider_name: &str,
    lanes: &Lanes,
    lane: &str,
    error: &StoreError,
    fatal: &FatalHook,
) -> bool {
    let message = error.to_string();
    let detail = Obj::new()
        .with("lane", lane)
        .with("error", safe_error(&message))
        .with("at", now());
    let text = detail.str("error").unwrap_or("").to_owned();
    let at = detail.str("at").unwrap_or("").to_owned();
    if let Some(class) = fatal_class(&message) {
        lanes.error(lane, &text, &at, None);
        log("collector_error", detail);
        log(
            "collector_fatal",
            Obj::new()
                .with("lane", lane)
                .with("class", class)
                .with("exitCode", FATAL_EXIT_CODE),
        );
        fatal(lane, class);
        return true;
    }
    let value = detail.to_value();
    let provider_name = provider_name.to_owned();
    let lane = lane.to_owned();
    // Best effort: when the database is failing the marker is unreadable and the streak goes on.
    // Maintenance has no progress marker; its streak ends with a successful pass.
    let progress = if lane == "maintenance" {
        None
    } else {
        db.get(crate::health::progress_record(&lane))
            .await
            .ok()
            .flatten()
            .map(|record| crate::health::lane_progress(&lane, &record))
    };
    lanes.error(&lane, &text, &at, progress);
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
    false
}

/// Storage maintenance on its own schedule, so that a tail cycle catching up for days cannot hold
/// back checkpoint trimming (2026-10-17, ADR-0008). A pass that trimmed a range is followed by
/// another after `busy`, so a backlog drains between the lanes' writes; otherwise after `idle`.
async fn maintenance_loop(
    db: Db,
    config: Config,
    lanes: Arc<Lanes>,
    report: impl Fn(StoreError) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
    stopped: Arc<AtomicBool>,
) {
    let idle = Duration::from_secs(config.maintenance_interval_seconds);
    let busy = Duration::from_secs(5).min(idle);
    let mut delay = idle;
    loop {
        let started = Instant::now();
        while started.elapsed() < delay {
            if stopped.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let config2 = config.clone();
        match db
            .run(move |store| maintain(store, &config2, false, false))
            .await
        {
            Ok(result) => {
                lanes.success("maintenance", &now());
                delay = if result.int("ranges").unwrap_or(0) > 0 {
                    busy
                } else {
                    idle
                };
                log("storage_maintenance", result);
            }
            Err(error) => {
                if stopped.load(Ordering::Relaxed) || report(error).await {
                    return;
                }
                delay = idle;
            }
        }
    }
}

/// Run until a signal arrives or a lane fails fatally; a fatal error exits the process.
pub async fn run(provider: Arc<Provider>, db: Db, config: Config) -> Result<(), StoreError> {
    run_with(provider, db, config, exit_on_fatal()).await
}

/// [`run`] with the fatal-error hook chosen by the caller.
pub async fn run_with(
    provider: Arc<Provider>,
    db: Db,
    config: Config,
    fatal: FatalHook,
) -> Result<(), StoreError> {
    db.guard_fatal();
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
    let lanes = Arc::new(Lanes::new(config.tail_enabled, config.backfill_enabled));
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
        let (provider, db, config, exchange, stopped, lanes, fatal) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&exchange),
            Arc::clone(&stopped),
            Arc::clone(&lanes),
            Arc::clone(&fatal),
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
                        let name = provider.provider_name().to_owned();
                        if lane_error(&db, &name, &lanes, "backfill", &error, &fatal).await {
                            stopped.store(true, Ordering::Relaxed);
                            return Ok(());
                        }
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
        let (provider, db, config, exchange, stopped, http, lanes, fatal) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&exchange),
            Arc::clone(&stopped),
            http.clone(),
            Arc::clone(&lanes),
            Arc::clone(&fatal),
        );
        async move {
            let mut last_refresh = Instant::now();
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
                        lanes.success("tail", &now());
                    }
                    error_streak = 0;
                    Ok(())
                }
                .await;
                if let Err(error) = outcome {
                    if stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    error_streak += 1;
                    let name = provider.provider_name().to_owned();
                    if lane_error(&db, &name, &lanes, "tail", &error, &fatal).await {
                        stopped.store(true, Ordering::Relaxed);
                        break;
                    }
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
        let (provider, db, config, stopped, lanes) = (
            Arc::clone(&provider),
            db.clone(),
            config.clone(),
            Arc::clone(&stopped),
            Arc::clone(&lanes),
        );
        async move {
            while !stopped.load(Ordering::Relaxed) {
                let (provider2, root) = (Arc::clone(&provider), config.data_dir.clone());
                let state = lanes.to_value();
                let _ = db
                    .run(move |store| write_status(store, &root, Some(provider2.as_ref()), state))
                    .await;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    };

    let maintainer = {
        let (provider2, db2, lanes2, fatal2, stopped2) = (
            Arc::clone(&provider),
            db.clone(),
            Arc::clone(&lanes),
            Arc::clone(&fatal),
            Arc::clone(&stopped),
        );
        let report = move |error: StoreError| {
            let (provider, db, lanes, fatal, stopped) = (
                Arc::clone(&provider2),
                db2.clone(),
                Arc::clone(&lanes2),
                Arc::clone(&fatal2),
                Arc::clone(&stopped2),
            );
            Box::pin(async move {
                let name = provider.provider_name().to_owned();
                let fatal = lane_error(&db, &name, &lanes, "maintenance", &error, &fatal).await;
                if fatal {
                    stopped.store(true, Ordering::Relaxed);
                }
                fatal
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
        };
        maintenance_loop(
            db.clone(),
            config.clone(),
            Arc::clone(&lanes),
            report,
            Arc::clone(&stopped),
        )
    };

    let (history_result, follower_result, (), (), ()) =
        tokio::join!(history, follower, observer, announcer, maintainer);
    history_result?;
    follower_result?;
    let (provider2, root) = (Arc::clone(&provider), config.data_dir.clone());
    let state = lanes.to_value();
    db.run(move |store| write_status(store, &root, Some(provider2.as_ref()), state))
        .await?;
    log(
        "collector_stopped",
        Obj::new().with("metrics", provider.counters().to_json()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::fatal_class;

    #[test]
    fn errors_that_leave_duckdb_unusable_are_fatal_others_are_retried() {
        let cases = [
            (
                "TransactionContext Error: Failed to commit: Failed to rollback transaction. Cannot continue operation",
                Some("rollback_failed"),
            ),
            (
                "TransactionContext Error: Failed to commit: Out of Memory Error: could not allocate block",
                Some("commit_failed"),
            ),
            (
                "Out of Memory Error: failed to allocate data of size 32.0 KiB (59.6 GiB/59.6 GiB used)",
                Some("oom"),
            ),
            ("Constraint Error: Duplicate key \"signature: x\"", None),
            ("getBlock failed (code 429)", None),
            ("archive does not match checkpoint rows", None),
        ];
        for (message, class) in cases {
            assert_eq!(fatal_class(message), class, "{message}");
        }
    }
}
