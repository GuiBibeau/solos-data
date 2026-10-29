//! Offline keep-hot rewrite (`collector repack --keep-hot`): build a new checkpoint holding only
//! the rows the collector still needs, then swap it in. Trimming in service deletes published
//! copies 100,000 slots at a time and checkpoints after each pass, which took the single writer
//! for about 20 hours on a 1.2 TB checkpoint (2026-10-18, ADR-0008). This does the same proof once
//! and copies the remainder instead of deleting the bulk.
//!
//! Dropped: rows of [`TRIMMED_TABLES`] inside the published coverage up to the trimming cutoff
//! ([`trimmable_coverage`], the same rule as in-service trimming), checked chunk by chunk against
//! their newest published copy with [`verify_archived`]. A chunk that fails the check keeps its
//! rows and is reported. Kept: everything else, row for row: rows above the cutoff or outside the
//! coverage, rows without a slot, and every other table (kv state, walks, progress, files,
//! catalog registrations, exchange metadata). The new file is proven equal to the original minus
//! the dropped rows (counts per table, and whole-row digest multisets of the kept rows in both
//! directions) before it replaces the original, which stays as `checkpoint.prev.duckdb`.

use super::catalog::merge_coverage;
use super::retention::{TRIMMED_TABLES, trimmable_coverage, verify_archived};
use crate::jsonout::{Obj, log, now};
use crate::repack::{digest_mismatch, swap_in};
use crate::store::{Store, StoreError, memory_connection, query_rows, query_scalar, sql_string};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// The candidate file, next to the checkpoint.
pub const CANDIDATE: &str = "checkpoint.keep.duckdb";

/// What may be dropped, proven against the archive.
#[derive(Debug, Default)]
pub struct Plan {
    /// Trimming cutoff (slot).
    pub cutoff: i64,
    /// Verified slot ranges whose trimmed-table rows are dropped, merged.
    pub drops: Vec<(i64, i64)>,
    /// Rows dropped per table.
    pub dropped: BTreeMap<String, i64>,
    /// Chunks that failed the archive check, kept whole: `{from, to, table, error}`.
    pub unverified: Vec<Value>,
}

/// Split `ranges` into chunks of at most `size` slots.
#[must_use]
pub fn chunks(ranges: &[(i64, i64)], size: i64) -> Vec<(i64, i64)> {
    let size = size.max(1);
    let mut out = Vec::new();
    for &(from, to) in ranges {
        let mut start = from;
        while start <= to {
            let end = to.min(start.saturating_add(size - 1));
            out.push((start, end));
            start = end + 1;
        }
    }
    out
}

/// SQL predicate that is true for rows to keep: outside every drop range, or without a slot.
#[must_use]
pub fn keep_predicate(drops: &[(i64, i64)]) -> String {
    if drops.is_empty() {
        return "TRUE".into();
    }
    let any = drops
        .iter()
        .map(|(from, to)| format!("slot BETWEEN {from} AND {to}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("NOT coalesce({any}, false)")
}

/// Verify every chunk of the trimmable coverage against the archive. Changes no rows; with a
/// `root` it first registers missing file bounds and rewrites `catalog.json`, as trimming does,
/// and without one (the dry run) it writes nothing at all.
pub fn plan(
    store: &mut Store,
    root: Option<&Path>,
    hot_slots: i64,
    chunk_slots: i64,
) -> Result<Plan, StoreError> {
    let Some((cutoff, coverage)) = trimmable_coverage(store, root, hot_slots)? else {
        return Err(StoreError::Check(
            "no watermark; nothing is published yet".into(),
        ));
    };
    let mut plan = Plan {
        cutoff,
        ..Plan::default()
    };
    let mut verified_files: HashSet<String> = HashSet::new();
    let mut drops = Vec::new();
    for (from, to) in chunks(&coverage, chunk_slots) {
        let mut counts: Vec<(&str, i64)> = Vec::new();
        let mut failure = None;
        for table in TRIMMED_TABLES {
            match verify_archived(store, table, from, to, &mut verified_files) {
                Ok(n) => counts.push((table, n)),
                Err(StoreError::Check(error)) => {
                    failure =
                        Some(json!({ "from": from, "to": to, "table": table, "error": error }));
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(failure) = failure {
            log(
                "checkpoint_keep_hot_unverified",
                Obj::new().with("chunk", failure.clone()),
            );
            plan.unverified.push(failure);
            continue;
        }
        let rows: i64 = counts.iter().map(|c| c.1).sum();
        if rows == 0 {
            continue;
        }
        for (table, n) in counts {
            *plan.dropped.entry(table.to_owned()).or_default() += n;
        }
        drops.push((from, to));
        log(
            "checkpoint_keep_hot_verified",
            Obj::new()
                .with("from", from)
                .with("to", to)
                .with("rows", rows),
        );
    }
    plan.drops = merge_coverage(drops);
    Ok(plan)
}

/// Plan, then (unless `dry_run`, which writes nothing) rewrite and swap. The store is closed during the rewrite and
/// reopened on the new file, where the result is recorded as `checkpoint-repack`.
pub fn repack_keep_hot(
    store: &mut Store,
    hot_slots: i64,
    chunk_slots: i64,
    dry_run: bool,
) -> Result<Obj, StoreError> {
    let started = std::time::Instant::now();
    let root = store.root.clone();
    let plan = plan(
        store,
        (!dry_run).then_some(root.as_path()),
        hot_slots,
        chunk_slots,
    )?;
    let mut report = Obj::new()
        .with("at", now())
        .with("mode", "keep-hot")
        .with("cutoff", plan.cutoff)
        .with("drops", super::catalog::coverage_json(&plan.drops))
        .with("dropped", json!(plan.dropped))
        .with("unverified", Value::Array(plan.unverified.clone()))
        .with("verifySeconds", started.elapsed().as_secs_f64());
    if dry_run {
        return Ok(report.with("dryRun", true));
    }
    super::writer::require_storage_space(&root)?;
    store.exec_batch("CHECKPOINT")?;
    let rewrite = store.while_closed(|root, _lock| rewrite(root, &plan))?;
    report.extend(rewrite);
    report.set("seconds", started.elapsed().as_secs_f64());
    store.set("checkpoint-repack", &report.to_value())?;
    Ok(report)
}

/// Copy the closed checkpoint minus `plan.drops` into [`CANDIDATE`], prove it, swap it in.
pub fn rewrite(root: &Path, plan: &Plan) -> Result<Obj, StoreError> {
    let source = root.join("checkpoint.duckdb");
    let candidate = root.join(CANDIDATE);
    let bytes_before = std::fs::metadata(&source)?.len();
    let memory = std::env::var("SOLOS_DATA_DB_MEMORY").unwrap_or_else(|_| "4GB".into());
    let threads = std::env::var("SOLOS_DATA_DB_THREADS").unwrap_or_else(|_| "4".into());
    let keep = keep_predicate(&plan.drops);
    let mut kept = Obj::new();
    {
        let conn = memory_connection(&threads, &memory)?;
        // Read-write: holds DuckDB's process lock on the original throughout.
        conn.execute_batch(&format!(
            "ATTACH {} AS original",
            sql_string(&source.to_string_lossy())
        ))?;
        conn.execute_batch("CHECKPOINT original")?;
        for stale in [candidate.clone(), candidate.with_extension("duckdb.wal")] {
            match std::fs::remove_file(&stale) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(error.into());
                }
                _ => {}
            }
        }
        conn.execute_batch(&format!(
            "ATTACH {} AS packed",
            sql_string(&candidate.to_string_lossy())
        ))?;
        conn.execute_batch("COPY FROM DATABASE original TO packed (SCHEMA)")?;
        let tables = query_rows(
            &conn,
            "SELECT table_name FROM duckdb_tables() WHERE database_name='original' AND NOT temporary ORDER BY table_name",
            &[],
        )?;
        for table in tables {
            let name = table.str("table_name").unwrap_or("").to_owned();
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            let trimmed = TRIMMED_TABLES.contains(&name.as_str());
            let filter = if trimmed { keep.as_str() } else { "TRUE" };
            let rows = conn.execute(
                &format!(
                    "INSERT INTO packed.{quoted} SELECT * FROM original.{quoted} WHERE {filter}"
                ),
                [],
            )?;
            let expected = if trimmed {
                plan.dropped.get(&name).copied().unwrap_or(0)
            } else {
                0
            };
            let dropped = int(&query_scalar(
                &conn,
                &format!("SELECT count(*) FROM original.{quoted} WHERE NOT ({filter})"),
                &[],
            )?);
            if dropped != expected {
                return Err(StoreError::Check(format!(
                    "{name}: {dropped} rows outside the kept set, {expected} verified against the archive"
                )));
            }
            let mismatch = digest_mismatch(
                &conn,
                &format!("SELECT * FROM original.{quoted} WHERE {filter}"),
                &format!("SELECT * FROM packed.{quoted}"),
            )?;
            if mismatch != 0 {
                return Err(StoreError::Check(format!(
                    "{name}: kept rows differ in the rewrite"
                )));
            }
            log(
                "checkpoint_keep_hot_copied",
                Obj::new()
                    .with("table", name.as_str())
                    .with("kept", rows)
                    .with("dropped", dropped),
            );
            kept.set(&name, rows);
        }
        conn.execute_batch("CHECKPOINT packed")?;
        conn.close().map_err(|(_, e)| e)?;
    }
    let bytes_after = swap_in(root, &candidate)?;
    Ok(Obj::new()
        .with("ok", true)
        .with_obj("kept", kept)
        .with("bytesBefore", bytes_before)
        .with("bytesAfter", bytes_after)
        .with(
            "reclaimedBytes",
            i64::try_from(bytes_before).unwrap_or(i64::MAX)
                - i64::try_from(bytes_after).unwrap_or(i64::MAX),
        ))
}

fn int(value: &Value) -> i64 {
    value
        .as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| value.as_i64())
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::{chunks, keep_predicate};

    #[test]
    fn chunks_cover_each_range_exactly() {
        assert_eq!(
            chunks(&[(1, 10), (20, 20)], 4),
            vec![(1, 4), (5, 8), (9, 10), (20, 20)]
        );
        assert_eq!(chunks(&[(5, 5)], 0), vec![(5, 5)]);
        assert!(chunks(&[], 10).is_empty());
    }

    #[test]
    fn the_keep_predicate_keeps_rows_without_a_slot() {
        assert_eq!(keep_predicate(&[]), "TRUE");
        assert_eq!(
            keep_predicate(&[(1, 2), (5, 9)]),
            "NOT coalesce(slot BETWEEN 1 AND 2 OR slot BETWEEN 5 AND 9, false)"
        );
    }
}
