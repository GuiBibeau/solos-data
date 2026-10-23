//! `venues history run` (ADR-0013): the venues lane's transactions, events, instructions and Pyth
//! prices from the slot before the live lane's first window back to the floor, newest first.
//!
//! Per source, one batch at a time: `batchWindows` windows of the source's `windowSlots`, below
//! the source's cursor. The manifest of the batch is one stretch of each address's continuous
//! `getSignaturesForAddress` walk ([`Walker`]); each window with a successful signature is then
//! fetched in bulk with `getTransactionsForAddress` and checked against its manifest exactly as
//! the live cycle checks it ([`walk::fetch_listed`]: a listed signature neither path serves fails
//! the batch). A failed batch is retried whole with the walks rebuilt from the last cursors
//! handed to the buffer. Rows, and one `batches` row per batch with what its manifest listed and
//! what it stored, are flushed into the history lane's hour files; the cursor (next slot down and
//! each walk's cursor) is written after them, so a restart refetches at most the batches whose
//! files were not registered.

use super::config::{HistoryConfig, Kind, Source, floor_seconds};
use super::floor::slot_at;
use super::walker::{Cursor, Entry, Walker};
use crate::augment::http::Http;
use crate::augment::ledger::{self, Lane, get_progress, set_progress};
use crate::augment::series::Ctx;
use crate::collector::rpc::{Provider, Rpc};
use crate::db::Db;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use crate::venues::events::TxMeta;
use crate::venues::idl::Idl;
use crate::venues::pyth_tx::{self, PythTx};
use crate::venues::rpc::{credit_provider, credits};
use crate::venues::store::{self, Buffer};
use crate::venues::walk::{self, Cycle, CycleRows, Manifest, Tx};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Seconds a failed batch waits before its retry.
const RETRY_SECONDS: u64 = 30;

/// Counters of one source.
#[derive(Default)]
struct Stats {
    /// Next slot down handed to the buffer.
    collected: Option<i64>,
    /// The durable cursor at start, for the rate.
    start_next: Option<i64>,
    batches: u64,
    totals: Cycle,
    pyth: PythTx,
    /// Distinct transactions turned into rows.
    txs: u64,
    rows: u64,
    oldest_block_time: Option<i64>,
    done: bool,
    errors: u64,
    error_streak: u64,
    last_batch_at: Option<String>,
    last_error: Option<String>,
    last_error_at: Option<String>,
}

struct SourceState {
    source: Source,
    idl: Idl,
    stats: Mutex<Stats>,
}

struct History {
    ctx: Ctx,
    rpc: Arc<Provider>,
    config: HistoryConfig,
    costs: BTreeMap<String, f64>,
    floor: i64,
    sources: Vec<SourceState>,
    buffer: Buffer,
    flushes: Mutex<store::Flushed>,
    started_at: String,
    started: Instant,
}

/// The progress record of a source's cursor.
#[must_use]
pub fn cursor_key(source: &str) -> String {
    format!("history/{source}/cursor")
}

/// The floor record.
pub const FLOOR_KEY: &str = "history/floor";

fn cursor_json(cursor: &Cursor) -> Value {
    json!({ "before": cursor.before, "fallback": cursor.fallback })
}

fn cursor_of(value: Option<&Value>) -> Cursor {
    let text = |k: &str| {
        value
            .and_then(|v| v.get(k))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    Cursor {
        before: text("before"),
        fallback: text("fallback"),
    }
}

/// A source's cursor record: the next slot to collect going down, each walk's cursor, and
/// whether the source reached the floor.
#[must_use]
pub fn cursor_value(next: i64, addresses: &[String], cursors: &[Cursor], done: bool) -> Value {
    let walkers: serde_json::Map<String, Value> = addresses
        .iter()
        .zip(cursors)
        .map(|(a, c)| (a.clone(), cursor_json(c)))
        .collect();
    json!({ "next": next, "done": done, "walkers": walkers, "updatedAt": now() })
}

/// The windows of `[lo, hi]`, newest first, each at most `size` slots.
#[must_use]
pub fn windows(lo: i64, hi: i64, size: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let mut to = hi;
    while to >= lo {
        let from = (to - size + 1).max(lo);
        out.push((from, to));
        to = from - 1;
    }
    out
}

/// The manifest of one window from a stretch of the walk.
#[must_use]
pub fn window_manifest(entries: &[Entry], from: i64, to: i64) -> Manifest {
    let mut out = Manifest::default();
    for e in entries.iter().filter(|e| e.slot >= from && e.slot <= to) {
        if e.ok {
            out.ok.insert(e.signature.clone(), e.slot);
        } else {
            out.failed += 1;
        }
    }
    out
}

fn add_cycle(total: &mut Cycle, c: &Cycle) {
    total.manifest += c.manifest;
    total.failed += c.failed;
    total.bulk += c.bulk;
    total.single += c.single;
    total.unlisted += c.unlisted;
    total.missing += c.missing;
    total.events += c.events;
    total.undecoded += c.undecoded;
    total.unknown += c.unknown;
    total.instructions += c.instructions;
    total.unparsed += c.unparsed;
}

async fn pause(ctx: &Ctx, seconds: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline && !ctx.stopping() {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// What one batch produced.
struct Batch {
    cycle: Cycle,
    pyth: PythTx,
    rows: CycleRows,
    prices: Vec<Value>,
    txs: u64,
    oldest_block_time: Option<i64>,
}

impl History {
    fn rpc(&self) -> &dyn Rpc {
        self.rpc.as_ref()
    }

    /// Fetch `[lo, hi]` for a source; `walkers` advance only on success of their `take`.
    async fn batch(
        &self,
        st: &SourceState,
        walkers: &mut [Walker],
        lo: i64,
        hi: i64,
    ) -> Result<Batch, String> {
        let rpc = self.rpc();
        let takes =
            futures_util::future::join_all(walkers.iter_mut().map(|w| w.take(rpc, lo, hi))).await;
        let mut stretches = Vec::with_capacity(takes.len());
        for take in takes {
            stretches.push(take?);
        }
        let windows = windows(lo, hi, st.source.window_slots);
        let mut jobs = Vec::new();
        let mut cycle = Cycle {
            from: lo,
            to: hi,
            ..Cycle::default()
        };
        for (address, entries) in st.source.addresses.iter().zip(&stretches) {
            for &window in &windows {
                let manifest = window_manifest(entries, window.0, window.1);
                cycle.failed += manifest.failed;
                // No successful signature listed: the bulk method has nothing to return.
                if manifest.ok.is_empty() {
                    continue;
                }
                jobs.push((address.as_str(), window, manifest));
            }
        }
        let fetched: Vec<Result<(Cycle, Vec<Tx>), String>> = futures_util::stream::iter(
            jobs.into_iter()
                .map(|(address, window, manifest)| async move {
                    walk::fetch_listed(rpc, address, window, &manifest).await
                }),
        )
        .buffer_unordered(self.config.concurrency as usize)
        .collect()
        .await;
        let mut seen: HashSet<String> = HashSet::new();
        let mut txs: Vec<Tx> = Vec::new();
        for result in fetched {
            let (c, list) = result?;
            cycle.manifest += c.manifest;
            cycle.bulk += c.bulk;
            cycle.single += c.single;
            cycle.unlisted += c.unlisted;
            txs.extend(
                list.into_iter()
                    .filter(|t| seen.insert(t.signature.clone())),
            );
        }
        txs.sort_by_key(|t| std::cmp::Reverse(t.slot));
        let mut out = Batch {
            txs: txs.len() as u64,
            oldest_block_time: txs.iter().filter_map(|t| t.block_time).min(),
            cycle,
            pyth: PythTx::default(),
            rows: CycleRows::default(),
            prices: Vec::new(),
        };
        match &st.source.kind {
            Kind::Venue { store_transactions } => {
                let program = &st.source.addresses[0];
                for tx in &txs {
                    walk::to_rows(
                        tx,
                        program,
                        &st.idl,
                        *store_transactions,
                        &mut out.cycle,
                        &mut out.rows,
                    );
                }
            }
            Kind::Pyth { feeds } => {
                let accounts: HashSet<String> = st.source.addresses.iter().cloned().collect();
                for tx in &txs {
                    let meta: Value = serde_json::from_str(&tx.meta_json).unwrap_or(Value::Null);
                    let at = TxMeta {
                        signature: &tx.signature,
                        slot: tx.slot,
                        block_time: tx.block_time,
                    };
                    match pyth_tx::rows(&tx.tx_b64, &meta, &at, feeds, &accounts) {
                        Ok((rows, counts)) => {
                            out.prices.extend(rows);
                            out.pyth.add(&counts);
                        }
                        Err(_) => out.cycle.unparsed += 1,
                    }
                }
            }
        }
        Ok(out)
    }

    fn record_error(&self, st: &SourceState, error: &str) {
        let mut stats = st.stats.lock().expect("history stats");
        stats.errors += 1;
        stats.error_streak += 1;
        stats.last_error = Some(safe_error(error));
        stats.last_error_at = Some(now());
        drop(stats);
        log(
            "history_error",
            Obj::new()
                .with("source", st.source.name.as_str())
                .with("error", safe_error(error)),
        );
    }

    async fn saved_cursor(&self, st: &SourceState) -> Option<Value> {
        loop {
            if self.ctx.stopping() {
                return None;
            }
            let key = cursor_key(&st.source.name);
            match self.ctx.db.run(move |s| get_progress(s, &key)).await {
                Ok(value) => return Some(value.unwrap_or(Value::Null)),
                Err(error) => {
                    self.record_error(st, &error.to_string());
                    pause(&self.ctx, RETRY_SECONDS).await;
                }
            }
        }
    }

    async fn source_loop(&self, st: &SourceState) {
        let Some(saved) = self.saved_cursor(st).await else {
            return;
        };
        let name = st.source.name.clone();
        let addresses = &st.source.addresses;
        let mut next = saved
            .get("next")
            .and_then(Value::as_i64)
            .unwrap_or(st.source.top);
        let mut committed: Vec<Cursor> = addresses
            .iter()
            .map(|a| cursor_of(saved.get("walkers").and_then(|w| w.get(a))))
            .collect();
        {
            let mut stats = st.stats.lock().expect("history stats");
            stats.start_next = Some(next);
            stats.collected = Some(next);
            stats.done = next < self.floor;
        }
        let rebuild = |cursors: &[Cursor]| -> Vec<Walker> {
            addresses
                .iter()
                .zip(cursors)
                .map(|(a, c)| Walker::resume(a, c.clone()).allowing_end(st.source.may_start_late))
                .collect()
        };
        let mut walkers = rebuild(&committed);
        let span = st.source.window_slots * self.config.batch_windows as i64;
        while !self.ctx.stopping() && next >= self.floor {
            // Back-pressure: the flush loop drains the buffer before more is fetched.
            while self.buffer.len() >= self.config.flush_rows.saturating_mul(2)
                && !self.ctx.stopping()
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let hi = next;
            let lo = (hi - span + 1).max(self.floor);
            let started = Instant::now();
            match self.batch(st, &mut walkers, lo, hi).await {
                Ok(batch) => {
                    committed = walkers.iter().map(Walker::cursor).collect();
                    let record = batch_record(&batch, lo, hi);
                    let sets = vec![
                        (store::BATCHES.dataset, vec![record]),
                        (store::TRANSACTIONS.dataset, batch.rows.transactions),
                        (store::EVENTS.dataset, batch.rows.events),
                        (store::INSTRUCTIONS.dataset, batch.rows.instructions),
                        (store::PRICES.dataset, batch.prices),
                    ];
                    let rows: u64 = sets.iter().map(|(_, r)| r.len() as u64).sum();
                    next = lo - 1;
                    let done = next < self.floor;
                    self.buffer.push_cycle(
                        &name,
                        sets,
                        &cursor_key(&name),
                        cursor_value(next, addresses, &committed, done),
                    );
                    let mut stats = st.stats.lock().expect("history stats");
                    stats.collected = Some(next);
                    stats.batches += 1;
                    stats.error_streak = 0;
                    stats.done = done;
                    stats.txs += batch.txs;
                    stats.rows += rows;
                    stats.last_batch_at = Some(now());
                    add_cycle(&mut stats.totals, &batch.cycle);
                    stats.pyth.add(&batch.pyth);
                    if let Some(t) = batch.oldest_block_time {
                        stats.oldest_block_time =
                            Some(stats.oldest_block_time.map_or(t, |o| o.min(t)));
                    }
                    drop(stats);
                    log(
                        "history_batch",
                        Obj::new()
                            .with("source", name.as_str())
                            .with("from", lo)
                            .with("to", hi)
                            .with("manifest", batch.cycle.manifest)
                            .with("txs", batch.txs)
                            .with("rows", rows)
                            .with("undecoded", batch.cycle.undecoded)
                            .with("unknown", batch.cycle.unknown)
                            .with("unlisted", batch.cycle.unlisted)
                            .with("oldestBlockTime", batch.oldest_block_time)
                            .with("ms", started.elapsed().as_millis() as u64),
                    );
                }
                Err(error) => {
                    self.record_error(st, &error);
                    walkers = rebuild(&committed);
                    pause(&self.ctx, RETRY_SECONDS).await;
                }
            }
        }
        if next < self.floor {
            log(
                "history_source_done",
                Obj::new()
                    .with("source", name.as_str())
                    .with("floor", self.floor),
            );
        }
    }

    async fn flush_loop(&self) {
        let mut last = Instant::now();
        loop {
            let stopping = self.ctx.stopping();
            let all_done = self
                .sources
                .iter()
                .all(|s| s.stats.lock().expect("history stats").done);
            let due = last.elapsed().as_secs() >= self.config.flush_seconds
                || self.buffer.len() >= self.config.flush_rows;
            if stopping || all_done {
                // The final flush runs after the loops end.
                return;
            }
            if due {
                let flushed = store::flush(&self.ctx, &self.buffer, now_ms()).await;
                self.add_flush(flushed);
                last = Instant::now();
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    fn add_flush(&self, flushed: store::Flushed) {
        let mut total = self.flushes.lock().expect("flushes");
        total.files += flushed.files;
        total.rows += flushed.rows;
        total.errors += flushed.errors;
    }

    async fn status_loop(&self) {
        while !self.ctx.stopping() {
            pause(&self.ctx, 60).await;
            if let Err(error) = self.write_status(true).await {
                log(
                    "history_status_error",
                    Obj::new().with("error", safe_error(&error.to_string())),
                );
            }
            if self
                .sources
                .iter()
                .all(|s| s.stats.lock().expect("history stats").done)
            {
                return;
            }
        }
    }

    fn source_obj(&self, st: &SourceState, durable: Option<i64>, done: bool) -> Obj {
        let s = st.stats.lock().expect("history stats");
        let t = &s.totals;
        let hours = self.started.elapsed().as_secs_f64() / 3600.0;
        let covered = s.start_next.zip(durable).map(|(a, b)| (a - b).max(0));
        let rate = covered
            .filter(|_| hours > 0.05)
            .map(|c| c as f64 / hours)
            .filter(|r| *r > 0.0);
        let remaining = durable.map(|d| (d - self.floor + 1).max(0));
        let total = st.source.top - self.floor + 1;
        let mut obj = Obj::new()
            .with("addresses", st.source.addresses.len())
            .with("top", st.source.top)
            .with("floor", self.floor)
            .with("cursor", durable)
            .with("collectedThrough", s.collected)
            .with("done", done)
            .with(
                "percent",
                remaining.map(|r| ((1.0 - r as f64 / total as f64) * 1000.0).round() / 10.0),
            )
            .with("slotsPerHour", rate.map(f64::round))
            .with(
                "etaHours",
                remaining
                    .zip(rate)
                    .map(|(r, rate)| (r as f64 / rate * 10.0).round() / 10.0),
            )
            .with(
                "oldestBlockTime",
                s.oldest_block_time.and_then(|t| {
                    chrono::DateTime::from_timestamp(t, 0)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                }),
            )
            .with("batches", s.batches)
            .with("txs", s.txs)
            .with("rows", s.rows)
            .with("manifest", t.manifest)
            .with("failedSkipped", t.failed)
            .with("bulk", t.bulk)
            .with("single", t.single)
            .with("unlisted", t.unlisted)
            .with("events", t.events)
            .with("undecoded", t.undecoded)
            .with("unknown", t.unknown)
            .with("instructions", t.instructions)
            .with("unparsed", t.unparsed)
            .with("errors", s.errors)
            .with("errorStreak", s.error_streak)
            .with("lastBatchAt", s.last_batch_at.clone())
            .with("lastError", s.last_error.clone())
            .with("lastErrorAt", s.last_error_at.clone());
        if matches!(st.source.kind, Kind::Pyth { .. }) {
            let p = &s.pyth;
            obj = obj.with_obj(
                "pyth",
                Obj::new()
                    .with("updates", p.updates)
                    .with("applied", p.applied)
                    .with("skipped", p.skipped)
                    .with("other", p.other)
                    .with("undecoded", p.undecoded)
                    .with("mismatched", p.mismatched),
            );
        } else {
            obj = obj.with("idlFallbacks", st.idl.fallback_count());
        }
        obj
    }

    async fn write_status(&self, running: bool) -> Result<Obj, StoreError> {
        let root = self.ctx.root.clone();
        let keys: Vec<String> = self
            .sources
            .iter()
            .map(|s| cursor_key(&s.source.name))
            .collect();
        let (summary, durable) = self
            .ctx
            .db
            .run(move |store| {
                ledger::write_catalog(store, &root, Lane::History)?;
                let mut durable = Vec::new();
                for key in &keys {
                    durable.push(get_progress(store, key)?);
                }
                Ok((ledger::summary(store)?, durable))
            })
            .await?;
        let counters = self.rpc.counters().to_value();
        let spent = credits(&counters, &self.costs);
        let hours = self.started.elapsed().as_secs_f64() / 3600.0;
        let mut sources = Obj::new();
        for (st, saved) in self.sources.iter().zip(durable) {
            let next = saved
                .as_ref()
                .and_then(|v| v.get("next"))
                .and_then(Value::as_i64)
                .or(Some(st.source.top));
            let done = saved
                .as_ref()
                .and_then(|v| v.get("done"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            sources.set_obj(&st.source.name, self.source_obj(st, next, done));
        }
        let flushed = *self.flushes.lock().expect("flushes");
        let status = Obj::new()
            .with("at", now())
            .with("lane", Lane::History.name())
            .with("startedAt", self.started_at.as_str())
            .with("running", running)
            .with("floorTime", self.config.floor_time.as_str())
            .with("floorSlot", self.floor)
            .with_obj("sources", sources)
            .with_obj(
                "rpc",
                Obj::new()
                    .with("creditsPerSecond", self.config.credits_per_second)
                    .with("credits", spent)
                    .with(
                        "creditsPerDay",
                        if hours > 0.0 {
                            json!((spent / hours * 24.0).round())
                        } else {
                            Value::Null
                        },
                    )
                    .with("hours", hours)
                    .with("counters", counters),
            )
            .with_obj(
                "flushes",
                Obj::new()
                    .with("files", flushed.files)
                    .with("rows", flushed.rows)
                    .with("errors", flushed.errors)
                    .with("buffered", self.buffer.len()),
            )
            .with_rows("datasets", summary);
        ledger::write_status(&self.ctx.root, Lane::History, &status)?;
        Ok(status)
    }
}

fn batch_record(batch: &Batch, from: i64, to: i64) -> Value {
    let c = &batch.cycle;
    json!({
        "from_slot": from, "to_slot": to, "manifest": c.manifest, "failed": c.failed,
        "bulk": c.bulk, "single": c.single, "unlisted": c.unlisted, "txs": batch.txs,
        "events": c.events, "undecoded": c.undecoded, "unknown": c.unknown,
        "instructions": c.instructions, "unparsed": c.unparsed, "prices": batch.prices.len(),
        "pyth_applied": batch.pyth.applied, "oldest_block_time": batch.oldest_block_time,
        "at_ms": now_ms(),
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The floor slot: recorded once per `floorTime`, found from the chain otherwise.
async fn floor_slot(ctx: &Ctx, rpc: &dyn Rpc, floor_time: &str) -> Result<i64, StoreError> {
    let saved = ctx
        .db
        .run(|s| get_progress(s, FLOOR_KEY))
        .await?
        .filter(|v| v.get("time").and_then(Value::as_str) == Some(floor_time));
    if let Some(slot) = saved.and_then(|v| v.get("slot").and_then(Value::as_i64)) {
        return Ok(slot);
    }
    let seconds = floor_seconds(floor_time)?;
    let slot = slot_at(rpc, seconds).await.map_err(StoreError::Check)?;
    let value = json!({ "time": floor_time, "slot": slot, "resolvedAt": now() });
    ctx.db
        .run(move |s| set_progress(s, FLOOR_KEY, &value))
        .await?;
    log(
        "history_floor",
        Obj::new().with("time", floor_time).with("slot", slot),
    );
    Ok(slot)
}

/// Run the backfill until every source reaches the floor or the stop flag is set; returns the
/// final status.
///
/// # Errors
///
/// When the ledger cannot be opened, the IDLs or the RPC URL are missing, or the floor cannot
/// be resolved.
pub fn run(config: HistoryConfig, stop: Arc<AtomicBool>) -> Result<Obj, StoreError> {
    let venues = config.venues()?;
    let sources = config.resolve(&venues)?;
    let costs = HistoryConfig::costs(&venues);
    let mut states = Vec::new();
    for source in sources {
        states.push(SourceState {
            idl: source.idl()?,
            source,
            stats: Mutex::new(Stats::default()),
        });
    }
    let root = venues.data_dir.clone();
    let mut store = ledger::open(&root, Lane::History)?;
    let recovered = ledger::recover(&mut store, &root, Lane::History)?;
    let (db, thread) = Db::spawn(store);
    let rpc = credit_provider(
        config.credits_per_second,
        &costs,
        config.concurrency,
        Arc::clone(&stop),
    )?;
    let ctx = Ctx {
        http: Http::new(1.0)?,
        db: db.clone(),
        root,
        lane: Lane::History,
        start: chrono::Utc::now().date_naive(),
        now_ms: 0,
        stop,
        disk_budget_bytes: 0,
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let floor = match runtime.block_on(floor_slot(&ctx, rpc.as_ref(), &config.floor_time)) {
        Ok(slot) => slot,
        Err(error) => {
            drop(ctx);
            let store = thread.join(db);
            store.close()?;
            return Err(error);
        }
    };
    let history = History {
        ctx,
        rpc,
        costs,
        floor,
        sources: states,
        buffer: Buffer::default(),
        flushes: Mutex::new(store::Flushed::default()),
        started_at: now(),
        started: Instant::now(),
        config,
    };
    log(
        "history_start",
        Obj::new()
            .with(
                "sources",
                history
                    .sources
                    .iter()
                    .map(|s| {
                        json!({ "name": s.source.name, "top": s.source.top, "addresses": s.source.addresses.len(), "windowSlots": s.source.window_slots })
                    })
                    .collect::<Vec<_>>(),
            )
            .with("floorSlot", floor)
            .with("creditsPerSecond", history.config.credits_per_second)
            .with("recovered", recovered),
    );
    runtime.block_on(async {
        let loops =
            futures_util::future::join_all(history.sources.iter().map(|s| history.source_loop(s)));
        tokio::join!(loops, history.flush_loop(), history.status_loop());
    });
    let flushed = runtime.block_on(store::flush(&history.ctx, &history.buffer, now_ms()));
    history.add_flush(flushed);
    let status = runtime.block_on(history.write_status(false))?;
    drop(runtime);
    let History { ctx, .. } = history;
    drop(ctx);
    let store = thread.join(db);
    store.close()?;
    log("history_done", Obj::new().with("buffered", 0));
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_cover_the_batch_newest_first() {
        assert_eq!(
            windows(1, 25, 10),
            vec![(16, 25), (6, 15), (1, 5)],
            "the last window is cut at the batch floor"
        );
        assert_eq!(windows(5, 5, 10), vec![(5, 5)]);
        assert!(windows(6, 5, 10).is_empty());
    }

    #[test]
    fn a_window_manifest_keeps_its_slots_and_counts_failures() {
        let e = |s: &str, slot: i64, ok: bool| Entry {
            signature: s.into(),
            slot,
            ok,
        };
        let entries = vec![
            e("a", 30, true),
            e("b", 20, false),
            e("c", 20, true),
            e("d", 9, true),
        ];
        let m = window_manifest(&entries, 10, 20);
        assert_eq!(m.ok.keys().collect::<Vec<_>>(), ["c"]);
        assert_eq!(m.failed, 1);
    }

    #[test]
    fn a_cursor_record_round_trips() {
        let addresses = vec!["A".to_owned(), "B".to_owned()];
        let cursors = vec![
            Cursor {
                before: Some("s1".into()),
                fallback: Some("s0".into()),
            },
            Cursor::default(),
        ];
        let value = cursor_value(41, &addresses, &cursors, false);
        assert_eq!(value["next"], 41);
        assert_eq!(cursor_of(value["walkers"].get("A")), cursors[0]);
        assert_eq!(cursor_of(value["walkers"].get("B")), Cursor::default());
        assert_eq!(cursor_of(None), Cursor::default());
    }
}
