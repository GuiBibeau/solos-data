//! The decoder loop: pick the next raw sources, decode a bounded batch in-process, publish, run
//! maintenance on its own clocks, write `status.json`. A port of `decode/service.ts` with the
//! codec linked instead of spawned.

use super::compact::{CompactionPolicy, compact_decoded_with};
use super::extract::extract_groups;
use super::normalize::{Rows, normalize, quarantine};
use super::publish::{SourceProgress, publish_sources, recover, write_catalog};
use super::schema::{VERSION, schema_static};
use super::source::{NextSource, SourceRow, attach_previous, pending_sources, read_source_rows};
use crate::fsutil::write_atomic;
use crate::gc::collect_retired;
use crate::jsonout::{Obj, log, now};
use crate::lease::with_read_lease;
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Default wall time per maintenance pass during which compaction keeps starting merges.
pub const COMPACTION_BUDGET: Duration = Duration::from_secs(15);

/// Decoder settings from `config/decoded.json` and the environment.
#[derive(Clone, Debug)]
pub struct DecoderConfig {
    /// Raw collector root (read-only, except for `.readers`).
    pub raw_dir: PathBuf,
    /// Decoded root.
    pub data_dir: PathBuf,
    /// Transactions per batch, across all of its sources.
    pub batch_size: u64,
    /// Raw source files one batch may span (`maxBatchSources`, default 32; 1 is the old
    /// one-file batch).
    pub max_batch_sources: usize,
    /// Idle poll interval, milliseconds.
    pub poll_ms: u64,
    /// Seconds between explicit checkpoints of the decoded store (`checkpointIntervalSeconds`,
    /// default 900).
    pub checkpoint_interval_seconds: u64,
    /// Seconds between compaction and garbage passes (`compactionIntervalSeconds`, default 60).
    pub compaction_interval_seconds: u64,
    /// Wall time a pass keeps starting merges (`compactionBudgetSeconds`, default 15).
    pub compaction_budget_seconds: u64,
    /// Batches after which a group that receives none is settled and merges its head with two
    /// files (`compactionSettledBatches`, default 100; 0 turns the rule off).
    pub compaction_settled_batches: u64,
}

impl DecoderConfig {
    /// Defaults for everything but the two roots.
    #[must_use]
    pub fn new(raw_dir: PathBuf, data_dir: PathBuf) -> Self {
        DecoderConfig {
            raw_dir,
            data_dir,
            batch_size: 10_000,
            max_batch_sources: 32,
            poll_ms: 2_000,
            checkpoint_interval_seconds: 900,
            compaction_interval_seconds: 60,
            compaction_budget_seconds: COMPACTION_BUDGET.as_secs(),
            compaction_settled_batches: 100,
        }
    }
}

/// `(signature, content hash)` pairs one source saw, in source order.
type Seen = Vec<(String, String)>;

/// One source's share of a batch.
struct Part {
    source: NextSource,
    rows: Vec<SourceRow>,
}

/// Read the next batch: the pending sources in consumption order, from each one's offset, until
/// `batch_size` transactions or `max_batch_sources` files. The batch ends before a source that
/// repeats a signature already in it (two revisions must not share a batch number) and before a
/// raw merge (its parents may be consumed by this very batch, which decides whether it is read at
/// all), so the published result is the one-source-per-batch loop's.
fn read_batch(
    store: &mut Store,
    config: &DecoderConfig,
    verified: &mut HashSet<String>,
) -> Result<Vec<Part>, StoreError> {
    let mut parts: Vec<Part> = Vec::new();
    let mut signatures: HashSet<String> = HashSet::new();
    let mut remaining = config.batch_size;
    for source in pending_sources(store, &config.raw_dir)? {
        if remaining == 0 || parts.len() >= config.max_batch_sources.max(1) {
            break;
        }
        if !parts.is_empty() && !source.file.parents.is_empty() {
            break;
        }
        let rows = read_source_rows(
            store,
            &config.raw_dir,
            &source.file,
            source.offset,
            remaining,
            verified,
        )?;
        if !parts.is_empty()
            && rows
                .iter()
                .any(|row| signatures.contains(&row.tx.signature))
        {
            break;
        }
        signatures.extend(rows.iter().map(|row| row.tx.signature.clone()));
        let exhausted = source.offset + rows.len() as u64 >= source.file.row_count;
        remaining = remaining.saturating_sub(rows.len() as u64);
        parts.push(Part { source, rows });
        if !exhausted {
            break;
        }
    }
    Ok(parts)
}

/// Run until stopped, or for one batch when `once`.
pub fn run_decoder(
    config: &DecoderConfig,
    once: bool,
    stopping: Arc<AtomicBool>,
) -> Result<(), StoreError> {
    let mut store = Store::open(&config.data_dir, schema_static())?;
    let mut verified: HashSet<String> = HashSet::new();
    let mut last_maintenance = Instant::now() - Duration::from_secs(3600);
    let mut last_checkpoint = Instant::now();
    let policy = CompactionPolicy {
        budget: Duration::from_secs(config.compaction_budget_seconds),
        settled_batches: (config.compaction_settled_batches > 0)
            .then(|| i64::try_from(config.compaction_settled_batches).unwrap_or(i64::MAX)),
    };
    recover(&mut store)?;
    while !stopping.load(Ordering::Relaxed) {
        let started = Instant::now();
        let parts = with_read_lease(&config.raw_dir, || -> Result<_, StoreError> {
            let mut parts = read_batch(&mut store, config, &mut verified)?;
            let mut all: Vec<SourceRow> = Vec::new();
            let mut lengths = Vec::with_capacity(parts.len());
            for part in &mut parts {
                lengths.push(part.rows.len());
                all.append(&mut part.rows);
            }
            attach_previous(&mut store, &mut all)?;
            let mut all = all.into_iter();
            for (part, length) in parts.iter_mut().zip(lengths) {
                part.rows = all.by_ref().take(length).collect();
            }
            Ok(parts)
        })?;
        if parts.is_empty() {
            snapshot(&mut store, Obj::new().with("idle", true))?;
            if once {
                break;
            }
            std::thread::sleep(Duration::from_millis(config.poll_ms));
            continue;
        }
        let read_seconds = started.elapsed().as_secs_f64();
        let decoded_at = Instant::now();
        let (rows, seen) = decode_rows(&parts)?;
        let decode_seconds = decoded_at.elapsed().as_secs_f64();
        let known: HashSet<String> = parts
            .iter()
            .flat_map(|part| part.rows.iter())
            .filter(|row| row.known)
            .map(|row| row.tx.signature.clone())
            .collect();
        let progress: Vec<SourceProgress> = parts
            .iter()
            .zip(seen)
            .map(|(part, seen)| SourceProgress {
                hash: part.source.file.sha256.clone(),
                path: part.source.file.path.clone(),
                offset: part.source.offset + part.rows.len() as u64,
                at: Some(part.source.file.created_at.clone()),
                seen: Some(seen),
            })
            .collect();
        let published_at = Instant::now();
        publish_sources(&mut store, &rows, &progress, Some(&known))?;
        let publish_seconds = published_at.elapsed().as_secs_f64();
        if last_maintenance.elapsed().as_secs() >= config.compaction_interval_seconds {
            maintain(&mut store, config, policy, &mut last_checkpoint)?;
            last_maintenance = Instant::now();
        }
        let quarantined = rows
            .get("decoded_transactions")
            .iter()
            .filter(|row| row.str("status") == Some("quarantined"))
            .count();
        let transactions: usize = parts.iter().map(|part| part.rows.len()).sum();
        snapshot(
            &mut store,
            Obj::new()
                .with("idle", false)
                .with("sourceCatalogAt", parts[0].source.catalog_at.clone())
                .with("batchSources", parts.len())
                .with("batchRaw", transactions)
                .with("batchTransactions", rows.get("decoded_transactions").len())
                .with("batchEvents", rows.get("events").len())
                .with("batchFills", rows.get("fills").len())
                .with("batchQuarantined", quarantined)
                .with("batchNewestSlot", slot_bound(&parts, true))
                .with("batchOldestSlot", slot_bound(&parts, false))
                .with("readSeconds", round(read_seconds))
                .with("decodeSeconds", round(decode_seconds))
                .with("publishSeconds", round(publish_seconds))
                .with("loopSeconds", round(started.elapsed().as_secs_f64())),
        )?;
        if once {
            break;
        }
    }
    store.close()
}

/// Compaction, garbage collection and, on its own clock, an explicit checkpoint; one
/// `decoded_maintenance` line with the time each took.
fn maintain(
    store: &mut Store,
    config: &DecoderConfig,
    policy: CompactionPolicy,
    last_checkpoint: &mut Instant,
) -> Result<(), StoreError> {
    let started = Instant::now();
    // Compaction starts merges for at most the budget of each pass so the backlog drains without
    // halving decode throughput; one merge may overrun it.
    let compaction = compact_decoded_with(store, policy)?;
    let compaction_seconds = started.elapsed().as_secs_f64();
    let collected_at = Instant::now();
    let garbage = collect_retired(store, &config.data_dir.clone(), 600, &write_catalog)?;
    let garbage_seconds = collected_at.elapsed().as_secs_f64();
    // A checkpoint rewrites the `processed` table's index whatever changed, so it runs on its
    // own clock; durability is the write-ahead log's job in between (ADR-0007).
    let checkpoint = last_checkpoint.elapsed().as_secs() >= config.checkpoint_interval_seconds;
    let checkpointed_at = Instant::now();
    if checkpoint {
        store.exec_batch("CHECKPOINT")?;
        *last_checkpoint = Instant::now();
    }
    let mut fields = compaction;
    fields.extend(garbage);
    fields.set("checkpoint", checkpoint);
    fields.set("compactionSeconds", round(compaction_seconds));
    fields.set("garbageSeconds", round(garbage_seconds));
    fields.set(
        "checkpointSeconds",
        round(checkpointed_at.elapsed().as_secs_f64()),
    );
    fields.set_obj("engineMemory", engine_memory(store));
    log("decoded_maintenance", fields);
    Ok(())
}

/// The storage engine's memory by tag (`duckdb_memory()`), in bytes, for the tags holding any:
/// what fills the buffer when the decoder nears its memory limit.
fn engine_memory(store: &Store) -> Obj {
    let mut out = Obj::new();
    let rows = store
        .rows(
            "SELECT tag, memory_usage_bytes AS bytes, temporary_storage_bytes AS spilled
    FROM duckdb_memory() WHERE memory_usage_bytes>0 OR temporary_storage_bytes>0",
            &[],
        )
        .unwrap_or_default();
    for row in rows {
        if let Some(tag) = row.str("tag") {
            out.set(
                tag,
                serde_json::json!([
                    row.int("bytes").unwrap_or(0),
                    row.int("spilled").unwrap_or(0)
                ]),
            );
        }
    }
    out
}

fn round(seconds: f64) -> f64 {
    (seconds * 1000.0).round() / 1000.0
}

/// The newest (or oldest) slot in the batch, for pace measurements.
fn slot_bound(parts: &[Part], newest: bool) -> Value {
    let slots = parts
        .iter()
        .flat_map(|part| part.rows.iter())
        .filter_map(|row| {
            row.tx
                .slot
                .as_i64()
                .or_else(|| row.tx.slot.as_str()?.parse().ok())
        });
    let bound = if newest { slots.max() } else { slots.min() };
    bound.map_or(Value::Null, Value::from)
}

/// Per-transaction result of the parallel phase, in source order.
struct Outcome {
    /// `(signature, content hash)` when the row counts as seen.
    seen: Option<(String, String)>,
    /// Normalized rows when the revision changed; `None` when unchanged or skipped.
    rows: Option<Result<Rows, String>>,
}

/// Decode and normalize one source row; pure, so rows decode in parallel.
fn decode_row(row: &SourceRow, created_at: &str, source_hash: &str) -> Outcome {
    let tx = &row.tx;
    let hash = tx.content_hash();
    if let Some(previous_at) = &row.previous_at
        && !previous_at.is_empty()
        && previous_at.as_str() > created_at
    {
        return Outcome {
            seen: None,
            rows: None,
        };
    }
    let seen = Some((tx.signature.clone(), hash.clone()));
    if row.previous_hash.as_deref() == Some(hash.as_str()) {
        return Outcome { seen, rows: None };
    }
    let decoded = match (&tx.tx_b64, &tx.meta_json, &tx.terminal_error) {
        (Some(wire), Some(meta_text), None) => serde_json::from_str::<Value>(meta_text)
            .map_err(|e| e.to_string())
            .and_then(|meta| extract_groups(wire, &meta))
            .and_then(phoenix_codec::decode_all),
        _ => Err("missing raw transaction or terminal fetch error".to_owned()),
    }
    .unwrap_or_else(|_| {
        vec![quarantine(
            "extraction",
            "transaction instruction extraction failed",
            tx.tx_b64.as_deref().unwrap_or(""),
        )]
    });
    let normalized = normalize(tx, &hash, &decoded, source_hash).or_else(|_| {
        normalize(
            tx,
            &hash,
            &[quarantine(
                "validation",
                "event context validation failed",
                tx.tx_b64.as_deref().unwrap_or(""),
            )],
            source_hash,
        )
    });
    Outcome {
        seen,
        rows: Some(normalized),
    }
}

/// Decode threads: `SOLOS_DATA_DECODE_THREADS`, else the available parallelism, at most 32.
fn decode_threads() -> usize {
    std::env::var("SOLOS_DATA_DECODE_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
                .min(32)
        })
}

/// Decode a batch in parallel, then assemble `(rows, seen per part)` in source order so the
/// published tables and the progress records are identical to the sequential loop.
fn decode_rows(parts: &[Part]) -> Result<(Rows, Vec<Seen>), StoreError> {
    let items: Vec<(&SourceRow, &str, &str)> = parts
        .iter()
        .flat_map(|part| {
            let created_at = part.source.file.created_at.as_str();
            let hash = part.source.file.sha256.as_str();
            part.rows.iter().map(move |row| (row, created_at, hash))
        })
        .collect();
    let threads = decode_threads().min(items.len().max(1));
    let chunk = items.len().div_ceil(threads).max(1);
    let outcomes: Vec<Outcome> = std::thread::scope(|scope| {
        let handles: Vec<_> = items
            .chunks(chunk)
            .map(|part| {
                scope.spawn(move || {
                    part.iter()
                        .map(|(row, created_at, hash)| decode_row(row, created_at, hash))
                        .collect::<Vec<Outcome>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("decode thread"))
            .collect()
    });
    let mut rows = Rows::default();
    let mut seen: Vec<Seen> = Vec::with_capacity(parts.len());
    let mut outcomes = outcomes.into_iter();
    for part in parts {
        let mut part_seen = Vec::with_capacity(part.rows.len());
        for outcome in outcomes.by_ref().take(part.rows.len()) {
            if let Some(pair) = outcome.seen {
                part_seen.push(pair);
            }
            if let Some(normalized) = outcome.rows {
                rows.extend(normalized.map_err(StoreError::Check)?);
            }
        }
        seen.push(part_seen);
    }
    Ok((rows, seen))
}

fn snapshot(store: &mut Store, progress: Obj) -> Result<(), StoreError> {
    let counts = store
        .rows(
            "SELECT count(*) AS transactions_processed FROM processed",
            &[],
        )?
        .into_iter()
        .next()
        .unwrap_or_default();
    let files = store.rows("SELECT table_name, sum(row_count) AS published_rows, count(*) AS files FROM files GROUP BY table_name", &[])?;
    let mut value = Obj::new().with("at", now()).with("decoderVersion", VERSION);
    value.extend(counts.clone());
    value.extend(progress.clone());
    value.set_rows("tables", files);
    value.set_obj("storageTimings", store.timings());
    write_atomic(
        &store.root.join("status.json"),
        format!("{}\n", value.to_json()).as_bytes(),
    )?;
    if progress.get("idle") != Some(&Value::Bool(true)) {
        let mut fields = progress;
        fields.extend(counts);
        log("decoded_batch", fields);
    }
    Ok(())
}

/// Install SIGTERM and SIGINT handlers that set the flag.
pub fn stop_flag() -> std::io::Result<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&flag))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&flag))?;
    Ok(flag)
}
