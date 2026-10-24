//! `spot live run` (ADR-0015): the spot tables from the history's top forward, oldest first,
//! one batch of `batchSlots` finalized slots at a time, from the RPC.
//!
//! A batch lists its produced blocks with `getBlocks`, fetches every one with `getBlock`
//! ([`super::blocks`]) while each program's `getSignaturesForAddress` manifest is walked over
//! one `manifestSlots` window of it, and is accepted on the history's checks (ADR-0014): every
//! listed block fetched with all its transactions, every successful signature of each
//! program's manifest window fetched. Its `swaps`, `liquidity` and `batches` files are then
//! written and registered in the lane's own ledger (`Lane::SpotLive`), the cursor last, so a
//! restart redoes at most the batch in flight. A batch never crosses an epoch boundary and
//! waits until its last slot is finalized; the first starts at `start`, the history's top plus
//! one, so the two lanes tile the chain with no gap and no slot twice.

use super::blocks::{self, Fetched};
use super::config::LiveConfig;
use super::dex::{ALL, Counts, Programs};
use super::run::{
    Done, EPOCH_SLOTS, Target, check_blocks, compare_manifests, manifest, manifest_window,
    produced_blocks, register_batch_to, write_batch_to,
};
use crate::augment::ledger::{self, Lane, get_progress, set_progress};
use crate::collector::rpc::{Provider, Rpc};
use crate::db::Db;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use crate::venues::rpc::{credit_provider, credits};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Seconds a failed batch waits before its retry.
const RETRY_SECONDS: u64 = 30;
/// Slots a day at 400 ms a slot, for the steady-state cost estimates.
pub const SLOTS_PER_DAY: f64 = 216_000.0;
/// The cursor record: the next slot to collect.
pub const CURSOR_KEY: &str = "spot-live/cursor";
/// The forward lane's batches.
pub const LIVE: Target = Target {
    lane: Lane::SpotLive,
    cursor_key: CURSOR_KEY,
};

/// The last slot of the batch starting at `lo`: `batch` slots, but not past the end of `lo`'s
/// epoch.
#[must_use]
pub fn batch_hi(lo: u64, batch: u64) -> u64 {
    let epoch_end = (lo / EPOCH_SLOTS + 1) * EPOCH_SLOTS - 1;
    lo.saturating_add(batch.max(1) - 1).min(epoch_end)
}

/// Running totals for the status.
#[derive(Default)]
struct Totals {
    batches: u64,
    slots: u64,
    blocks: u64,
    transactions: u64,
    swaps: u64,
    liquidity: u64,
    truncated: u64,
    manifest: u64,
    unlisted: u64,
    bytes: u64,
    counts: Vec<Counts>,
    gaps: std::collections::BTreeMap<String, u64>,
    errors: u64,
    error_streak: u64,
    last_error: Option<String>,
    last_error_at: Option<String>,
    last_batch_at: Option<String>,
    newest_block_time: Option<i64>,
}

struct Live {
    config: LiveConfig,
    programs: Programs,
    root: PathBuf,
    rpc: Arc<Provider>,
    db: Db,
    stop: Arc<AtomicBool>,
    started_at: String,
    started: Instant,
    finalized: Option<u64>,
    totals: Totals,
}

/// Fetch, list and walk one batch, and accept it or say why not.
///
/// # Errors
///
/// An RPC failure or a failed check; nothing of the batch is kept.
pub async fn batch(
    rpc: &dyn Rpc,
    programs: &Programs,
    config: &LiveConfig,
    range: (u64, u64),
) -> Result<(Done, Fetched), String> {
    let (lo, hi) = range;
    let started = Instant::now();
    let window = manifest_window(lo, hi, config.manifest_slots);
    let listed = produced_blocks(rpc, lo, hi).await?;
    let windows = [window];
    let fetch = blocks::fetch(
        rpc,
        &listed,
        config.concurrency as usize,
        programs,
        &windows,
    );
    let manifests = futures_util::future::join_all(
        programs
            .list()
            .iter()
            .map(|&dex| async move { (dex, manifest(rpc, dex, window).await) }),
    );
    let (fetched, manifests) = tokio::join!(fetch, manifests);
    let (stream, fetched) = fetched?;
    let blocks = check_blocks(&stream, &listed)?;
    let compared = compare_manifests(&stream, manifests, window)?;
    Ok((
        Done {
            stream,
            blocks,
            window,
            manifests: compared,
            seconds: started.elapsed().as_secs_f64(),
        },
        fetched,
    ))
}

impl Live {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    async fn pause(&self, seconds: u64) {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline && !self.stopping() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn db_run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut crate::store::Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        self.db.run_blocking(job)
    }

    fn record_error(&mut self, error: &str) {
        let t = &mut self.totals;
        t.errors += 1;
        t.error_streak += 1;
        t.last_error = Some(safe_error(error));
        t.last_error_at = Some(now());
        log(
            "spot_live_error",
            Obj::new().with("error", safe_error(error)),
        );
    }

    /// Write and register a batch's files and then its cursor.
    fn commit(
        &mut self,
        range: (u64, u64),
        done: &Done,
        fetched: Fetched,
    ) -> Result<(), StoreError> {
        let (lo, hi) = range;
        let files = write_batch_to(
            Lane::SpotLive,
            &self.config.writer_memory,
            &self.root,
            range,
            done,
        )?;
        let cursor = json!({ "next": hi + 1, "updatedAt": now() });
        let root = self.root.clone();
        self.db_run(move |store| register_batch_to(LIVE, store, &root, range, &files, &cursor))?;
        crate::stall::bump();
        let rows = &done.stream.rows;
        let t = &mut self.totals;
        t.batches += 1;
        t.slots += hi - lo + 1;
        t.blocks += done.blocks.streamed;
        t.transactions += done.blocks.transactions;
        t.swaps += rows.swaps.len() as u64;
        t.liquidity += rows.liquidity.len() as u64;
        t.truncated += rows.truncated;
        t.bytes += fetched.bytes;
        if t.counts.len() < ALL.len() {
            t.counts.resize(ALL.len(), Counts::default());
        }
        for (mine, theirs) in t.counts.iter_mut().zip(&rows.counts) {
            mine.add(theirs);
        }
        for (k, n) in &rows.gaps {
            *t.gaps.entry(k.clone()).or_default() += n;
        }
        for (_, m) in &done.manifests {
            t.manifest += m.listed;
            t.unlisted += m.unlisted;
        }
        t.error_streak = 0;
        t.last_batch_at = Some(now());
        if let Some(newest) = done.stream.blocks.values().filter_map(|b| b.time).max() {
            t.newest_block_time = Some(t.newest_block_time.map_or(newest, |n| n.max(newest)));
        }
        log(
            "spot_live_batch",
            Obj::new()
                .with("from", lo)
                .with("to", hi)
                .with("blocks", done.blocks.streamed)
                .with("transactions", done.blocks.transactions)
                .with("swaps", rows.swaps.len())
                .with("liquidity", rows.liquidity.len())
                .with("truncated", rows.truncated)
                .with("manifestWindow", json!([done.window.0, done.window.1]))
                .with(
                    "manifest",
                    done.manifests.iter().map(|(_, m)| m.listed).sum::<u64>(),
                )
                .with(
                    "unlisted",
                    done.manifests.iter().map(|(_, m)| m.unlisted).sum::<u64>(),
                )
                .with("bytes", fetched.bytes)
                .with("seconds", done.seconds),
        );
        Ok(())
    }

    fn status(&self, next: u64, running: bool) -> Result<Obj, StoreError> {
        let root = self.root.clone();
        let summary = self.db_run(move |store| {
            ledger::write_catalog(store, &root, Lane::SpotLive)?;
            ledger::summary(store)
        })?;
        let t = &self.totals;
        let hours = self.started.elapsed().as_secs_f64() / 3600.0;
        let mut programs = Obj::new();
        for dex in self.programs.list() {
            let c = t.counts.get(*dex as usize).copied().unwrap_or_default();
            programs.set(
                dex.name(),
                json!({ "txs": c.txs, "failed": c.failed, "swapIxs": c.swap_ixs, "swaps": c.swaps,
                        "liquidity": c.liquidity, "missingEvent": c.missing_event,
                        "undecoded": c.undecoded, "unresolved": c.unresolved }),
            );
        }
        let counters = self.rpc.counters().to_value();
        let spent = credits(&counters, &self.config.credit_costs);
        let per_slot = |total: f64| (t.slots > 0).then(|| total / t.slots as f64);
        let source = Obj::new()
            .with("start", self.config.start)
            .with("cursor", next)
            .with("finalized", self.finalized)
            .with(
                "lagSlots",
                self.finalized.map(|f| (f + 1).saturating_sub(next)),
            )
            .with("batchSlots", self.config.batch_slots)
            .with("done", false)
            .with(
                "newestBlockTime",
                t.newest_block_time.and_then(|s| {
                    chrono::DateTime::from_timestamp(s, 0)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                }),
            )
            .with("batches", t.batches)
            .with("slots", t.slots)
            .with("blocks", t.blocks)
            .with("transactions", t.transactions)
            .with("swaps", t.swaps)
            .with("liquidity", t.liquidity)
            .with("truncatedLogs", t.truncated)
            .with("manifest", t.manifest)
            .with("unlisted", t.unlisted)
            .with("errors", t.errors)
            .with("errorStreak", t.error_streak)
            .with("lastBatchAt", t.last_batch_at.clone())
            .with("lastError", t.last_error.clone())
            .with("lastErrorAt", t.last_error_at.clone());
        let status = Obj::new()
            .with("at", now())
            .with("lane", Lane::SpotLive.name())
            .with("startedAt", self.started_at.as_str())
            .with("running", running)
            .with_obj("sources", Obj::new().with_obj("spot", source))
            .with_obj("programs", programs)
            .with("gaps", serde_json::to_value(&t.gaps).unwrap_or(Value::Null))
            .with_obj(
                "rpc",
                Obj::new()
                    .with("creditsPerSecond", self.config.credits_per_second)
                    .with("credits", spent)
                    .with("responseBytes", t.bytes)
                    .with("creditsPerSlot", per_slot(spent))
                    .with("bytesPerSlot", per_slot(t.bytes as f64))
                    .with(
                        "creditsPerDayAtTip",
                        per_slot(spent).map(|c| (c * SLOTS_PER_DAY).round()),
                    )
                    .with(
                        "bytesPerDayAtTip",
                        per_slot(t.bytes as f64).map(|b| (b * SLOTS_PER_DAY).round()),
                    )
                    .with("hours", hours)
                    .with("counters", counters),
            )
            .with_rows("datasets", summary);
        ledger::write_status(&self.root, Lane::SpotLive, &status)?;
        Ok(status)
    }
}

/// Run the lane until `stop` is set; returns the final status.
///
/// # Errors
///
/// When the ledger cannot be opened or the RPC URL is missing.
pub fn run(config: LiveConfig, stop: Arc<AtomicBool>) -> Result<Obj, StoreError> {
    let programs = config.programs()?;
    let root = config.data_dir.clone();
    let mut store = ledger::open(&root, Lane::SpotLive)?;
    let recovered = ledger::recover(&mut store, &root, Lane::SpotLive)?;
    let (db, thread) = Db::spawn(store);
    let result = (|| {
        let rpc = credit_provider(
            config.credits_per_second,
            &config.credit_costs,
            config.concurrency,
            Arc::clone(&stop),
        )?;
        let runtime = tokio::runtime::Runtime::new()?;
        let saved = runtime.block_on(db.run(|s| get_progress(s, CURSOR_KEY)))?;
        let mut next = match saved
            .as_ref()
            .and_then(|v| v.get("next"))
            .and_then(Value::as_u64)
        {
            Some(next) => next,
            None => {
                // A first run records where it starts before collecting anything.
                let value = json!({ "next": config.start, "updatedAt": now() });
                runtime.block_on(db.run(move |s| set_progress(s, CURSOR_KEY, &value)))?;
                config.start
            }
        };
        log(
            "spot_live_start",
            Obj::new()
                .with("start", config.start)
                .with("next", next)
                .with(
                    "programs",
                    programs.list().iter().map(|d| d.name()).collect::<Vec<_>>(),
                )
                .with("batchSlots", config.batch_slots)
                .with("recovered", recovered),
        );
        let mut live = Live {
            programs,
            root: root.clone(),
            rpc,
            db: db.clone(),
            stop: Arc::clone(&stop),
            started_at: now(),
            started: Instant::now(),
            finalized: None,
            totals: Totals::default(),
            config,
        };
        live.status(next, true)?;
        crate::stall::spawn(
            "spot",
            "spot_live_stalled",
            Duration::from_secs(live.config.stall_minutes.saturating_mul(60)),
        );
        let mut last_status = Instant::now();
        while !live.stopping() {
            let rpc: &dyn Rpc = live.rpc.as_ref();
            let finalized = match runtime.block_on(crate::venues::walk::finalized_slot(rpc)) {
                Ok(slot) => u64::try_from(slot).unwrap_or(0),
                Err(error) => {
                    live.record_error(&error);
                    runtime.block_on(live.pause(RETRY_SECONDS));
                    continue;
                }
            };
            live.finalized = Some(finalized);
            let hi = batch_hi(next, live.config.batch_slots);
            if hi > finalized {
                if last_status.elapsed() >= Duration::from_secs(60) {
                    if let Err(error) = live.status(next, true) {
                        live.record_error(&error.to_string());
                    }
                    last_status = Instant::now();
                }
                runtime.block_on(live.pause(live.config.cycle_seconds));
                continue;
            }
            let outcome = runtime.block_on(batch(rpc, &live.programs, &live.config, (next, hi)));
            match outcome {
                Ok((done, fetched)) => match live.commit((next, hi), &done, fetched) {
                    Ok(()) => next = hi + 1,
                    Err(error) => {
                        live.record_error(&error.to_string());
                        runtime.block_on(live.pause(RETRY_SECONDS));
                    }
                },
                Err(error) => {
                    live.record_error(&error);
                    runtime.block_on(live.pause(RETRY_SECONDS));
                }
            }
            if let Err(error) = live.status(next, true) {
                log(
                    "spot_live_status_error",
                    Obj::new().with("error", safe_error(&error.to_string())),
                );
            }
            last_status = Instant::now();
        }
        live.status(next, false)
    })();
    let store = thread.join(db);
    store.close()?;
    let status = result?;
    log("spot_live_exit", Obj::new().with("at", now()));
    Ok(status)
}

/// `spot live trial`: fetch and decode `[from, to]` the way a batch does, without files or
/// ledger, and report rows, decode coverage, bytes, time and the RPC credits it spent, with
/// what that costs a day at the tip.
///
/// # Errors
///
/// An RPC failure or a failed check.
pub async fn trial(rpc: &Provider, config: &LiveConfig, range: (u64, u64)) -> Result<Obj, String> {
    let programs = config.programs().map_err(|e| e.to_string())?;
    let started = Instant::now();
    let (done, fetched) = batch(rpc, &programs, config, range).await?;
    let seconds = started.elapsed().as_secs_f64();
    let counters = rpc.counters().to_value();
    let spent = credits(&counters, &config.credit_costs);
    let slots = (range.1 - range.0 + 1) as f64;
    let rows = &done.stream.rows;
    let mut per_program = Obj::new();
    for dex in ALL {
        let c = rows.counts.get(dex as usize).copied().unwrap_or_default();
        if c != Counts::default() {
            per_program.set(
                dex.name(),
                json!({ "txs": c.txs, "failed": c.failed, "swapIxs": c.swap_ixs, "swaps": c.swaps,
                        "liquidity": c.liquidity, "missingEvent": c.missing_event,
                        "undecoded": c.undecoded, "unresolved": c.unresolved }),
            );
        }
    }
    Ok(Obj::new()
        .with("from", range.0)
        .with("to", range.1)
        .with("slots", slots)
        .with("blocks", done.blocks.streamed)
        .with("transactions", done.blocks.transactions)
        .with("swaps", rows.swaps.len())
        .with("liquidity", rows.liquidity.len())
        .with("truncatedLogs", rows.truncated)
        .with(
            "gaps",
            serde_json::to_value(&rows.gaps).unwrap_or(Value::Null),
        )
        .with(
            "manifest",
            done.manifests.iter().map(|(_, m)| m.listed).sum::<u64>(),
        )
        .with(
            "unlisted",
            done.manifests.iter().map(|(_, m)| m.unlisted).sum::<u64>(),
        )
        .with_obj("programs", per_program)
        .with("seconds", seconds)
        .with("slotsPerSecond", slots / seconds.max(1e-9))
        .with("responseBytes", fetched.bytes)
        .with("credits", spent)
        .with(
            "creditsPerDayAtTip",
            (spent / slots * SLOTS_PER_DAY).round(),
        )
        .with(
            "bytesPerDayAtTip",
            (fetched.bytes as f64 / slots * SLOTS_PER_DAY).round(),
        )
        .with("counters", counters))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_ends_at_its_size_or_its_epoch() {
        // The history ends at 460,511,999; the lane starts on epoch 1066's first slot and its
        // batches tile the epoch (432,000 = 48 × 9,000).
        assert_eq!(batch_hi(460_512_000, 9_000), 460_520_999);
        assert_eq!(batch_hi(460_935_000, 9_000), 460_943_999);
        // A start off the grid ends at the epoch's last slot, and the next batch starts the next
        // epoch.
        assert_eq!(batch_hi(460_940_000, 9_000), 460_943_999);
        assert_eq!(batch_hi(460_944_000, 9_000), 460_952_999);
        assert_eq!(460_944_000 % EPOCH_SLOTS, 0);
    }
}
