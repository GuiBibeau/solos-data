//! `spot run` (ADR-0014): the spot tables from `top` down to the floor, newest first, one batch
//! of `chunkSlots × parallel` slots at a time.
//!
//! A batch streams its chunks from the archive (decoding in the stream), and meanwhile lists the
//! range's produced blocks with `getBlocks` and walks each program's `getSignaturesForAddress`
//! manifest over one `manifestSlots` window of the batch. It is accepted only when the archive
//! gave exactly the blocks the RPC lists, every block delivered as many transactions as its
//! entries name, and every successful signature a program's manifest lists in the window was
//! streamed; otherwise it is retried whole. The batch's `swaps`, `liquidity` and `batches`
//! files are then written and registered, the cursor last, so a restart redoes at most the batch
//! whose cursor was not recorded (its files are rewritten under the same names).

use super::archive::{Options, Stream};
use super::config::SpotConfig;
use super::dex::{ALL, Counts, Dex, Programs};
use super::trial::stream_range;
use super::write::{Names, connection, copy_to, load_liquidity, load_swaps};
use crate::augment::ledger::{self, FileRecord, Lane, get_progress, register, set_progress};
use crate::collector::rpc::{Provider, Rpc};
use crate::db::Db;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use crate::venues::history::walker::{Entry, Walker, seed};
use crate::venues::rpc::{call, credit_provider, credits};
use duckdb::params;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Seconds a failed batch waits before its retry.
const RETRY_SECONDS: u64 = 30;
/// The widest range one `getBlocks` call may ask for.
const GET_BLOCKS_SPAN: u64 = 500_000;
/// The cursor record.
pub const CURSOR_KEY: &str = "spot/cursor";
/// The floor record.
pub const FLOOR_KEY: &str = "spot/floor";

/// The manifest window of a batch: `size` slots at an offset that varies with the batch, so
/// the samples spread over the whole history instead of always the top of a batch.
#[must_use]
pub fn manifest_window(lo: u64, hi: u64, size: u64) -> (u64, u64) {
    let span = hi - lo + 1;
    if span <= size {
        return (lo, hi);
    }
    let room = span - size + 1;
    let offset = lo.wrapping_mul(0x9E37_79B9_7F4A_7C15) % room;
    (lo + offset, lo + offset + size - 1)
}

/// What the block check found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Blocks {
    /// Blocks the archive gave.
    pub streamed: u64,
    /// Blocks the RPC lists.
    pub listed: u64,
    /// Transactions the archive delivered (votes included).
    pub transactions: u64,
}

/// Compare the archive's blocks with the RPC's list and each block's delivered transactions
/// with its entries.
///
/// # Errors
///
/// A block one side has and the other lacks, or a block short of transactions.
pub fn check_blocks(stream: &Stream, listed: &[u64]) -> Result<Blocks, String> {
    let streamed: BTreeSet<u64> = stream.blocks.keys().copied().collect();
    let listed_set: BTreeSet<u64> = listed.iter().copied().collect();
    let only_archive: Vec<u64> = streamed.difference(&listed_set).take(5).copied().collect();
    let only_rpc: Vec<u64> = listed_set.difference(&streamed).take(5).copied().collect();
    if !only_archive.is_empty() || !only_rpc.is_empty() {
        return Err(format!(
            "blocks differ: archive only {only_archive:?}, RPC only {only_rpc:?}"
        ));
    }
    let short: Vec<u64> = stream
        .blocks
        .iter()
        .filter(|(slot, b)| stream.delivered.get(slot).copied().unwrap_or(0) != b.executed)
        .map(|(slot, _)| *slot)
        .take(5)
        .collect();
    if !short.is_empty() {
        return Err(format!(
            "blocks whose delivered transactions differ from their entries: {short:?}"
        ));
    }
    Ok(Blocks {
        streamed: streamed.len() as u64,
        listed: listed_set.len() as u64,
        transactions: stream.delivered.values().sum(),
    })
}

/// A program's manifest comparison over the window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    /// Successful signatures the RPC lists.
    pub listed: u64,
    /// Listed but not streamed.
    pub missing: u64,
    /// Streamed but not listed.
    pub unlisted: u64,
}

/// Compare one program's manifest rows (already cut to the window) with the streamed
/// signatures of that program.
#[must_use]
pub fn compare(entries: &[Entry], streamed: &HashSet<&str>) -> Manifest {
    let listed: HashSet<&str> = entries
        .iter()
        .filter(|e| e.ok)
        .map(|e| e.signature.as_str())
        .collect();
    Manifest {
        listed: listed.len() as u64,
        missing: listed.difference(streamed).count() as u64,
        unlisted: streamed.difference(&listed).count() as u64,
    }
}

async fn produced_blocks(rpc: &dyn Rpc, lo: u64, hi: u64) -> Result<Vec<u64>, String> {
    let mut out = Vec::new();
    let mut from = lo;
    while from <= hi {
        let to = (from + GET_BLOCKS_SPAN - 1).min(hi);
        let reply = call(
            rpc,
            "getBlocks",
            json!([from, to, { "commitment": "finalized" }]),
        )
        .await
        .map_err(|e| e.to_string())?;
        let Some(list) = reply.result.as_array() else {
            return Err("getBlocks answered no list".into());
        };
        out.extend(list.iter().filter_map(Value::as_u64));
        from = to + 1;
    }
    Ok(out)
}

async fn manifest(rpc: &dyn Rpc, dex: Dex, window: (u64, u64)) -> Result<Vec<Entry>, String> {
    let (lo, hi) = (
        i64::try_from(window.0).unwrap_or(i64::MAX),
        i64::try_from(window.1).unwrap_or(i64::MAX),
    );
    let cursor = seed(rpc, dex.program_id(), hi).await?;
    Walker::resume(dex.program_id(), cursor)
        .take(rpc, lo, hi)
        .await
        .map_err(|e| format!("{} manifest: {e}", dex.name()))
}

/// One accepted batch.
pub struct Done {
    /// What the archive gave.
    pub stream: Stream,
    /// The block check.
    pub blocks: Blocks,
    /// The manifest window.
    pub window: (u64, u64),
    /// Each program's manifest comparison.
    pub manifests: Vec<(Dex, Manifest)>,
    /// Seconds the batch took.
    pub seconds: f64,
}

/// Running totals for the status.
#[derive(Default)]
struct Totals {
    batches: u64,
    swaps: u64,
    liquidity: u64,
    transactions: u64,
    blocks: u64,
    truncated: u64,
    manifest: u64,
    unlisted: u64,
    counts: Vec<Counts>,
    gaps: std::collections::BTreeMap<String, u64>,
    errors: u64,
    error_streak: u64,
    last_error: Option<String>,
    last_error_at: Option<String>,
    last_batch_at: Option<String>,
    oldest_block_time: Option<i64>,
}

struct Spot {
    config: SpotConfig,
    programs: Programs,
    root: PathBuf,
    rpc: Arc<Provider>,
    db: Db,
    stop: Arc<AtomicBool>,
    floor: u64,
    started_at: String,
    started: Instant,
    start_next: u64,
    totals: Totals,
}

fn period(lo: u64, hi: u64) -> String {
    super::check::period(
        i64::try_from(lo).unwrap_or(i64::MAX),
        i64::try_from(hi).unwrap_or(i64::MAX),
    )
}

/// Unregister and delete files of an earlier batch that overlaps `[lo, hi]` under another
/// range (a batch redone after `chunkSlots` or `parallel` changed, before its cursor was
/// recorded), so no slot is counted twice.
fn retire_overlapping(
    store: &mut crate::store::Store,
    files_root: &std::path::Path,
    lo: u64,
    hi: u64,
) -> Result<u64, StoreError> {
    let keep = period(lo, hi);
    let rows = store.rows("SELECT path, period FROM files WHERE source = 'spot'", &[])?;
    let mut retired = 0;
    for row in rows {
        let (Some(path), Some(p)) = (row.str("path"), row.str("period")) else {
            continue;
        };
        let Some((a, b)) = p.split_once('-') else {
            continue;
        };
        let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>()) else {
            continue;
        };
        if p != keep && a <= hi && b >= lo {
            let path = path.to_owned();
            store.exec("DELETE FROM files WHERE path = ?", &[&path])?;
            let _ = std::fs::remove_file(files_root.join(&path));
            retired += 1;
        }
    }
    Ok(retired)
}

impl Spot {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    async fn pause(&self, seconds: u64) {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline && !self.stopping() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Stream, list and walk one batch, and accept it or say why not.
    async fn batch(&self, lo: u64, hi: u64) -> Result<Done, String> {
        let started = Instant::now();
        let window = manifest_window(lo, hi, self.config.manifest_slots);
        let options = Options {
            windows: vec![window],
            capture: 0,
        };
        let rpc: &dyn Rpc = self.rpc.as_ref();
        let stream = stream_range(
            lo,
            hi,
            self.config.chunk_slots,
            self.config.parallel,
            &self.programs,
            &options,
        );
        let blocks = produced_blocks(rpc, lo, hi);
        let manifests = futures_util::future::join_all(
            self.programs
                .list()
                .iter()
                .map(|&dex| async move { (dex, manifest(rpc, dex, window).await) }),
        );
        let (stream, listed, manifests) = tokio::join!(stream, blocks, manifests);
        let stream = stream?;
        let blocks = check_blocks(&stream, &listed?)?;
        let mut by_dex: HashMap<Dex, HashSet<&str>> = HashMap::new();
        for (dex, _, signature) in &stream.sampled {
            by_dex.entry(*dex).or_default().insert(signature.as_str());
        }
        let empty = HashSet::new();
        let mut compared = Vec::new();
        for (dex, entries) in manifests {
            let m = compare(&entries?, by_dex.get(&dex).unwrap_or(&empty));
            if m.missing > 0 {
                return Err(format!(
                    "{}: {} of {} manifest signatures in slots {}..={} were not streamed",
                    dex.name(),
                    m.missing,
                    m.listed,
                    window.0,
                    window.1
                ));
            }
            compared.push((dex, m));
        }
        drop(by_dex);
        Ok(Done {
            stream,
            blocks,
            window,
            manifests: compared,
            seconds: started.elapsed().as_secs_f64(),
        })
    }

    /// Write and register a batch's files and then its cursor.
    fn commit(&mut self, lo: u64, hi: u64, done: Done) -> Result<(), StoreError> {
        let next = lo.saturating_sub(1);
        let finished = lo <= self.floor;
        let files = write_batch(&self.config.writer_memory, &self.root, lo, hi, &done)?;
        let cursor = json!({ "next": next, "done": finished, "updatedAt": now() });
        let root = self.root.clone();
        let retired =
            self.db_run(move |store| register_batch(store, &root, lo, hi, &files, &cursor))?;
        if retired > 0 {
            log(
                "spot_retired",
                Obj::new()
                    .with("from", lo)
                    .with("to", hi)
                    .with("files", retired),
            );
        }
        let rows = &done.stream.rows;
        let t = &mut self.totals;
        t.batches += 1;
        t.swaps += rows.swaps.len() as u64;
        t.liquidity += rows.liquidity.len() as u64;
        t.transactions += done.blocks.transactions;
        t.blocks += done.blocks.streamed;
        t.truncated += rows.truncated;
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
        if let Some(oldest) = done.stream.blocks.values().filter_map(|b| b.time).min() {
            t.oldest_block_time = Some(t.oldest_block_time.map_or(oldest, |o| o.min(oldest)));
        }
        log(
            "spot_batch",
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
                .with("seconds", done.seconds),
        );
        if finished {
            log("spot_done", Obj::new().with("floor", self.floor));
        }
        Ok(())
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
        log("spot_error", Obj::new().with("error", safe_error(error)));
    }

    fn status(&self, next: u64, done: bool, running: bool) -> Result<Obj, StoreError> {
        let root = self.root.clone();
        let summary = self.db_run(move |store| {
            ledger::write_catalog(store, &root, Lane::Spot)?;
            ledger::summary(store)
        })?;
        let t = &self.totals;
        let hours = self.started.elapsed().as_secs_f64() / 3600.0;
        let covered = self.start_next.saturating_sub(next);
        let rate = (hours > 0.05 && covered > 0).then(|| covered as f64 / hours);
        let remaining = if done {
            0
        } else {
            next + 1 - self.floor.min(next + 1)
        };
        let total = (self.config.top as u64).saturating_sub(self.floor) + 1;
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
        let source = Obj::new()
            .with("top", self.config.top)
            .with("floor", self.floor)
            .with("cursor", next)
            .with("done", done)
            .with(
                "percent",
                ((1.0 - remaining as f64 / total as f64) * 1000.0).round() / 10.0,
            )
            .with("slotsPerHour", rate.map(f64::round))
            .with(
                "etaHours",
                rate.map(|r| (remaining as f64 / r * 10.0).round() / 10.0),
            )
            .with(
                "oldestBlockTime",
                t.oldest_block_time.and_then(|s| {
                    chrono::DateTime::from_timestamp(s, 0)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                }),
            )
            .with("batches", t.batches)
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
            .with("lane", Lane::Spot.name())
            .with("startedAt", self.started_at.as_str())
            .with("running", running)
            .with("floorTime", self.config.floor_time.as_str())
            .with("floorSlot", self.floor)
            .with_obj("sources", Obj::new().with_obj("spot", source))
            .with_obj("programs", programs)
            .with("gaps", serde_json::to_value(&t.gaps).unwrap_or(Value::Null))
            .with_obj(
                "rpc",
                Obj::new()
                    .with("creditsPerSecond", self.config.credits_per_second)
                    .with("credits", spent)
                    .with("hours", hours)
                    .with("counters", counters),
            )
            .with_rows("datasets", summary);
        ledger::write_status(&self.root, Lane::Spot, &status)?;
        Ok(status)
    }
}

/// The batch's per-program and total row for the `batches` table.
fn batch_rows(done: &Done) -> Vec<(String, Counts, Manifest)> {
    let mut out = vec![(
        "ALL".to_owned(),
        done.stream
            .rows
            .counts
            .iter()
            .fold(Counts::default(), |mut a, c| {
                a.add(c);
                a
            }),
        done.manifests
            .iter()
            .fold(Manifest::default(), |a, (_, m)| Manifest {
                listed: a.listed + m.listed,
                missing: a.missing + m.missing,
                unlisted: a.unlisted + m.unlisted,
            }),
    )];
    for (dex, m) in &done.manifests {
        let c = done
            .stream
            .rows
            .counts
            .get(*dex as usize)
            .copied()
            .unwrap_or_default();
        out.push((dex.name().to_owned(), c, *m));
    }
    out
}

/// Register a batch's files (retiring overlapping ones first) and, with the last, its cursor;
/// returns the files retired.
///
/// # Errors
///
/// A ledger failure.
pub fn register_batch(
    store: &mut crate::store::Store,
    root: &std::path::Path,
    lo: u64,
    hi: u64,
    files: &[FileRecord],
    cursor: &Value,
) -> Result<u64, StoreError> {
    let retired = retire_overlapping(store, &Lane::Spot.files_root(root), lo, hi)?;
    for (i, file) in files.iter().enumerate() {
        let progress = (i + 1 == files.len()).then_some((CURSOR_KEY, cursor));
        register(store, file, progress)?;
    }
    Ok(retired)
}

/// Write the batch's three files under the lane's tree.
///
/// # Errors
///
/// A DuckDB or file failure.
pub fn write_batch(
    writer_memory: &str,
    root: &std::path::Path,
    lo: u64,
    hi: u64,
    done: &Done,
) -> Result<Vec<FileRecord>, StoreError> {
    let files_root = Lane::Spot.files_root(root);
    let conn = connection("8", writer_memory)?;
    let blocks = &done.stream.blocks;
    let time = |slot: u64| blocks.get(&slot).and_then(|b| b.time);
    let mut names = Names::default();
    load_swaps(&conn, &done.stream.rows.swaps, &time, &mut names)?;
    load_liquidity(&conn, &done.stream.rows.liquidity, &time, &mut names)?;
    drop(names);
    conn.execute_batch(super::write::BATCHES_DDL)?;
    {
        let mut app = conn.appender("batches")?;
        let at_ms = chrono::Utc::now().timestamp_millis();
        for (program, c, m) in batch_rows(done) {
            let all = program == "ALL";
            app.append_row(params![
                i64::try_from(lo).unwrap_or(i64::MAX),
                i64::try_from(hi).unwrap_or(i64::MAX),
                program,
                all.then_some(done.blocks.streamed),
                all.then_some(done.blocks.listed),
                all.then_some(done.blocks.transactions),
                all.then_some(done.stream.rows.truncated),
                c.txs,
                c.failed,
                c.frames,
                c.swap_ixs,
                c.swaps,
                c.liquidity,
                c.missing_event,
                c.undecoded,
                c.unresolved,
                i64::try_from(done.window.0).unwrap_or(i64::MAX),
                i64::try_from(done.window.1).unwrap_or(i64::MAX),
                m.listed,
                m.missing,
                m.unlisted,
                done.seconds,
                at_ms,
            ])?;
        }
    }
    let period = period(lo, hi);
    let mut out = Vec::new();
    for (dataset, select) in [
        (
            "swaps",
            format!("SELECT * FROM swaps ORDER BY {}", super::write::ORDER),
        ),
        (
            "liquidity",
            format!("SELECT * FROM liquidity ORDER BY {}", super::write::ORDER),
        ),
        (
            "batches",
            "SELECT * FROM batches ORDER BY program".to_owned(),
        ),
    ] {
        let relative = format!("tables/spot/{dataset}/ALL/{period}.parquet");
        out.push(copy_to(
            &conn,
            &select,
            &files_root,
            &relative,
            ("spot", dataset, &period),
        )?);
    }
    Ok(out)
}

/// The floor slot: recorded once per `floorTime`, found from the chain otherwise.
async fn floor_slot(db: &Db, rpc: &dyn Rpc, floor_time: &str) -> Result<u64, StoreError> {
    let saved = db
        .run(|s| get_progress(s, FLOOR_KEY))
        .await?
        .filter(|v| v.get("time").and_then(Value::as_str) == Some(floor_time));
    if let Some(slot) = saved.and_then(|v| v.get("slot").and_then(Value::as_u64)) {
        return Ok(slot);
    }
    let seconds = crate::venues::history::config::floor_seconds(floor_time)?;
    let slot = crate::venues::history::floor::slot_at(rpc, seconds)
        .await
        .map_err(StoreError::Check)?;
    let value = json!({ "time": floor_time, "slot": slot, "resolvedAt": now() });
    db.run(move |s| set_progress(s, FLOOR_KEY, &value)).await?;
    log(
        "spot_floor",
        Obj::new().with("time", floor_time).with("slot", slot),
    );
    u64::try_from(slot).map_err(|_| StoreError::Check("negative floor slot".into()))
}

/// Run the backfill until it reaches the floor or `stop` is set; returns the final status.
///
/// # Errors
///
/// When the ledger cannot be opened, the binary cannot stream the archive, the RPC URL is
/// missing, or the floor cannot be resolved.
pub fn run(config: SpotConfig, stop: Arc<AtomicBool>) -> Result<Obj, StoreError> {
    if !super::archive::available() {
        return Err(StoreError::Check(
            "spot run reads the archive, but this binary was built without the archive feature (cargo build --features archive)".into(),
        ));
    }
    let programs = config.programs()?;
    let root = config.data_dir.clone();
    let mut store = ledger::open(&root, Lane::Spot)?;
    let recovered = ledger::recover(&mut store, &root, Lane::Spot)?;
    let (db, thread) = Db::spawn(store);
    let result = (|| {
        let rpc = credit_provider(
            config.credits_per_second,
            &config.credit_costs,
            config.concurrency,
            Arc::clone(&stop),
        )?;
        let runtime = tokio::runtime::Runtime::new()?;
        let floor = runtime.block_on(floor_slot(&db, rpc.as_ref(), &config.floor_time))?;
        let saved = runtime.block_on(db.run(|s| get_progress(s, CURSOR_KEY)))?;
        let mut next = saved
            .as_ref()
            .and_then(|v| v.get("next"))
            .and_then(Value::as_u64)
            .unwrap_or(u64::try_from(config.top).unwrap_or(0));
        let mut done = saved
            .as_ref()
            .and_then(|v| v.get("done"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        log(
            "spot_start",
            Obj::new()
                .with("top", config.top)
                .with("next", next)
                .with("floor", floor)
                .with(
                    "programs",
                    programs.list().iter().map(|d| d.name()).collect::<Vec<_>>(),
                )
                .with("batchSlots", config.batch_slots())
                .with("recovered", recovered),
        );
        let mut spot = Spot {
            programs,
            root: root.clone(),
            rpc,
            db: db.clone(),
            stop: Arc::clone(&stop),
            floor,
            started_at: now(),
            started: Instant::now(),
            start_next: next,
            totals: Totals::default(),
            config,
        };
        spot.status(next, done, true)?;
        let mut last_status = Instant::now();
        while !done && !spot.stopping() {
            let hi = next;
            let lo = hi
                .saturating_sub(spot.config.batch_slots() - 1)
                .max(spot.floor);
            match runtime.block_on(spot.batch(lo, hi)) {
                Ok(batch) => match spot.commit(lo, hi, batch) {
                    Ok(()) => {
                        next = lo.saturating_sub(1);
                        done = lo <= spot.floor;
                    }
                    Err(error) => {
                        spot.record_error(&error.to_string());
                        runtime.block_on(spot.pause(RETRY_SECONDS));
                    }
                },
                Err(error) => {
                    spot.record_error(&error);
                    runtime.block_on(spot.pause(RETRY_SECONDS));
                }
            }
            if last_status.elapsed() >= Duration::from_secs(30) || done {
                if let Err(error) = spot.status(next, done, true) {
                    log(
                        "spot_status_error",
                        Obj::new().with("error", safe_error(&error.to_string())),
                    );
                }
                last_status = Instant::now();
            }
        }
        spot.status(next, done, false)
    })();
    let store = thread.join(db);
    store.close()?;
    let status = result?;
    log("spot_exit", Obj::new().with("at", now()));
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spot::archive::Block;

    #[test]
    fn a_manifest_window_stays_inside_its_batch() {
        assert_eq!(manifest_window(10, 19, 50), (10, 19));
        for lo in [0u64, 1, 77, 400_000_000, 459_999_999] {
            let (a, b) = manifest_window(lo, lo + 199_999, 1_000);
            assert_eq!(b - a + 1, 1_000);
            assert!(a >= lo && b <= lo + 199_999);
        }
        // The offset moves from batch to batch.
        let first = manifest_window(0, 199_999, 1_000).0;
        let second = manifest_window(200_000, 399_999, 1_000).0 - 200_000;
        assert_ne!(first, second);
    }

    fn stream(blocks: &[(u64, u64, u64)]) -> Stream {
        let mut s = Stream::default();
        for &(slot, executed, delivered) in blocks {
            s.blocks.insert(
                slot,
                Block {
                    time: Some(1),
                    executed,
                },
            );
            s.delivered.insert(slot, delivered);
        }
        s
    }

    #[test]
    fn the_block_check_needs_every_listed_block_with_all_its_transactions() {
        let ok = stream(&[(10, 3, 3), (12, 0, 0)]);
        assert_eq!(
            check_blocks(&ok, &[10, 12]).unwrap(),
            Blocks {
                streamed: 2,
                listed: 2,
                transactions: 3
            }
        );
        assert!(
            check_blocks(&ok, &[10, 11, 12])
                .unwrap_err()
                .contains("RPC only [11]")
        );
        assert!(
            check_blocks(&ok, &[10])
                .unwrap_err()
                .contains("archive only [12]")
        );
        let short = stream(&[(10, 3, 2)]);
        assert!(check_blocks(&short, &[10]).unwrap_err().contains("[10]"));
    }

    #[test]
    fn the_manifest_compares_successful_signatures_both_ways() {
        let e = |s: &str, ok: bool| Entry {
            signature: s.into(),
            slot: 1,
            ok,
        };
        let entries = vec![e("a", true), e("b", true), e("f", false)];
        let streamed: HashSet<&str> = ["a", "c"].into_iter().collect();
        assert_eq!(
            compare(&entries, &streamed),
            Manifest {
                listed: 2,
                missing: 1,
                unlisted: 1
            }
        );
    }
}
