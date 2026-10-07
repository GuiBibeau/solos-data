//! The decoder loop: pick the next raw source, decode a bounded batch in-process, publish, run
//! maintenance once a minute, write `status.json`. A port of `decode/service.ts` with the codec
//! linked instead of spawned.

use super::compact::compact_decoded;
use super::extract::extract_groups;
use super::normalize::{Rows, normalize, quarantine};
use super::publish::{SourceProgress, publish, recover, write_catalog};
use super::schema::{VERSION, schema_static};
use super::source::{next_source, source_rows};
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

/// Decoder settings from `config/decoded.json` and the environment.
#[derive(Clone, Debug)]
pub struct DecoderConfig {
    /// Raw collector root (read-only, except for `.readers`).
    pub raw_dir: PathBuf,
    /// Decoded root.
    pub data_dir: PathBuf,
    /// Transactions per batch.
    pub batch_size: u64,
    /// Idle poll interval, milliseconds.
    pub poll_ms: u64,
}

/// Run until stopped, or for one batch when `once`.
pub fn run_decoder(
    config: &DecoderConfig,
    once: bool,
    stopping: Arc<AtomicBool>,
) -> Result<(), StoreError> {
    let mut store = Store::open(&config.data_dir, schema_static())?;
    let mut verified: HashSet<String> = HashSet::new();
    let mut last_maintenance = std::time::Instant::now() - std::time::Duration::from_secs(3600);
    recover(&mut store)?;
    while !stopping.load(Ordering::Relaxed) {
        let input = with_read_lease(&config.raw_dir, || -> Result<_, StoreError> {
            let Some(source) = next_source(&mut store, &config.raw_dir)? else {
                return Ok(None);
            };
            let raw = source_rows(
                &mut store,
                &config.raw_dir,
                &source.file,
                source.offset,
                config.batch_size,
                &mut verified,
            )?;
            Ok(Some((source, raw)))
        })?;
        let Some((source, raw)) = input else {
            snapshot(&mut store, Obj::new().with("idle", true))?;
            if once {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(config.poll_ms));
            continue;
        };
        let (rows, seen) = decode_rows(&raw, &source.file.created_at, &source.file.sha256)?;
        let progress = SourceProgress {
            hash: source.file.sha256.clone(),
            path: source.file.path.clone(),
            offset: source.offset + raw.len() as u64,
            at: Some(source.file.created_at.clone()),
            seen: Some(seen),
        };
        publish(&mut store, &rows, &progress)?;
        if last_maintenance.elapsed().as_secs() > 60 {
            let compaction = compact_decoded(&mut store)?;
            let garbage =
                collect_retired(&mut store, &config.data_dir.clone(), 600, &write_catalog)?;
            store.exec_batch("CHECKPOINT")?;
            let mut fields = compaction;
            fields.extend(garbage);
            log("decoded_maintenance", fields);
            last_maintenance = std::time::Instant::now();
        }
        let quarantined = rows
            .get("decoded_transactions")
            .iter()
            .filter(|row| row.str("status") == Some("quarantined"))
            .count();
        snapshot(
            &mut store,
            Obj::new()
                .with("idle", false)
                .with("sourceCatalogAt", source.catalog_at.clone())
                .with("batchTransactions", rows.get("decoded_transactions").len())
                .with("batchEvents", rows.get("events").len())
                .with("batchFills", rows.get("fills").len())
                .with("batchQuarantined", quarantined),
        )?;
        if once {
            break;
        }
    }
    store.close()
}

/// Per-transaction result of the parallel phase, in source order.
struct Outcome {
    /// `(signature, content hash)` when the row counts as seen.
    seen: Option<(String, String)>,
    /// Normalized rows when the revision changed; `None` when unchanged or skipped.
    rows: Option<Result<Rows, String>>,
}

/// Decode and normalize one source row; pure, so rows decode in parallel.
fn decode_row(row: &super::source::SourceRow, created_at: &str, source_hash: &str) -> Outcome {
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

/// Decode a batch in parallel, then assemble `(rows, seen)` in source order so the published
/// tables and the progress record are identical to the sequential loop.
fn decode_rows(
    raw: &[super::source::SourceRow],
    created_at: &str,
    source_hash: &str,
) -> Result<(Rows, Vec<(String, String)>), StoreError> {
    let threads = decode_threads().min(raw.len().max(1));
    let chunk = raw.len().div_ceil(threads).max(1);
    let outcomes: Vec<Outcome> = std::thread::scope(|scope| {
        let handles: Vec<_> = raw
            .chunks(chunk)
            .map(|part| {
                scope.spawn(move || {
                    part.iter()
                        .map(|row| decode_row(row, created_at, source_hash))
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
    let mut seen = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        if let Some(pair) = outcome.seen {
            seen.push(pair);
        }
        if let Some(normalized) = outcome.rows {
            rows.extend(normalized.map_err(StoreError::Check)?);
        }
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
