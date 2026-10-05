//! Periodic and offline storage maintenance: compaction, verified checkpoint trimming, retired
//! file cleanup, optional legacy cleanup, CHECKPOINT, and the automatic checkpoint rewrite. A
//! port of `maintenance.ts`, `checkpoint-maintenance.ts` and `relocate.ts`.

use super::catalog::write_catalog;
use super::compactor::compact;
use super::config::Config;
use super::retention::{collect_legacy, prune_published};
use super::writer::TABLES;
use crate::fsutil::file_hash;
use crate::gc::collect_retired;
use crate::jsonout::{Obj, log, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::path::Path;

/// Checkpoint index churn can grow the file even when archived rows have been trimmed.
pub fn reclaim_checkpoint(
    store: &mut Store,
    minimum_bytes: u64,
) -> Result<Option<Obj>, StoreError> {
    let previous = store.get("checkpoint-repack")?;
    let size = std::fs::metadata(store.root.join("checkpoint.duckdb"))?.len();
    let baseline = previous
        .as_ref()
        .and_then(|p| p.get("bytesAfter"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
        * 2;
    if size < minimum_bytes.max(baseline) {
        return Ok(None);
    }
    let started = std::time::Instant::now();
    super::writer::require_storage_space(&store.root.clone())?;
    let result = store.while_closed(crate::repack::repack_checkpoint)?;
    let mut value = Obj::new()
        .with("at", now())
        .with("seconds", started.elapsed().as_secs_f64());
    value.extend(result);
    store.set("checkpoint-repack", &value.to_value())?;
    Ok(Some(value))
}

/// One maintenance pass.
pub fn maintain(
    store: &mut Store,
    config: &Config,
    all: bool,
    legacy: bool,
) -> Result<Obj, StoreError> {
    compact(store, &config.data_dir)?;
    log("checkpoint_trim_started", Obj::new().with("all", all));
    let (mut trimmed, mut ranges) = (0i64, 0i64);
    loop {
        let result = prune_published(
            store,
            &config.data_dir,
            config.checkpoint_hot_slots,
            if all { 1_000_000_000 } else { 16_000 },
        )?;
        let removed = result.int("removedRows").unwrap_or(0);
        let count = result.int("ranges").unwrap_or(0);
        trimmed += removed;
        ranges += count;
        if count > 0 {
            log(
                "checkpoint_trim_progress",
                Obj::new()
                    .with("removedRows", removed)
                    .with("ranges", count),
            );
        }
        if !all || count == 0 {
            break;
        }
    }
    let data_dir = config.data_dir.clone();
    let catalog_dir = data_dir.clone();
    let garbage = collect_retired(
        store,
        &data_dir,
        config.garbage_grace_seconds,
        &move |store: &mut Store| write_catalog(store, &catalog_dir).map(|_| ()),
    )?;
    if legacy {
        log("legacy_cleanup_started", Obj::new());
    }
    let legacy_result = if legacy {
        Some(collect_legacy(store, &config.data_dir)?)
    } else {
        None
    };
    store.exec_batch("CHECKPOINT")?;
    let checkpoint = reclaim_checkpoint(store, 16 * 1024u64.pow(3))?;
    let mut result = Obj::new()
        .with("at", now())
        .with("trimmedRows", trimmed)
        .with("ranges", ranges);
    result.extend(garbage);
    match legacy_result {
        Some(value) => result.set_obj("legacy", value),
        None => result.set("legacy", Value::Null),
    }
    match checkpoint {
        Some(value) => result.set_obj("checkpoint", value),
        None => result.set("checkpoint", Value::Null),
    }
    store.set("maintenance", &result.to_value())?;
    Ok(result)
}

/// Offline only: rebase registered absolute paths after the data root moved.
pub fn relocate(store: &mut Store) -> Result<Obj, StoreError> {
    let root = store.root.clone();
    let files = store.rows("SELECT path, sha256, row_count,status FROM files", &[])?;
    let removed: std::collections::HashSet<String> = store
        .rows("SELECT path FROM garbage_removed", &[])?
        .iter()
        .filter_map(|r| r.str("path").map(str::to_owned))
        .collect();
    let mut replacements: Vec<(String, String)> = Vec::new();
    for file in &files {
        let old = file.str("path").unwrap_or("").to_owned();
        let mut path = old.clone();
        if !crate::fsutil::inside(&root, Path::new(&path)) {
            let markers: Vec<String> = std::iter::once("/staging/".to_owned())
                .chain(TABLES.iter().map(|t| format!("/{t}/")))
                .collect();
            let Some(marker) = markers.iter().find(|m| path.contains(m.as_str())) else {
                return Err(StoreError::Check(
                    "unrecognized registered path; relocation held".into(),
                ));
            };
            let index = path.find(marker.as_str()).unwrap_or(0);
            path = root.join(&path[index + 1..]).to_string_lossy().into_owned();
        }
        if file.str("status") != Some("deleted") && !removed.contains(&old) {
            if file_hash(Path::new(&path))? != file.str("sha256").unwrap_or("") {
                return Err(StoreError::Check("relocation checksum mismatch".into()));
            }
            let count = store.rows(
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    sql_string(&path)
                ),
                &[],
            )?;
            if count.first().and_then(|r| r.int("n")) != file.int("row_count") {
                return Err(StoreError::Check("relocation row count mismatch".into()));
            }
        }
        replacements.push((old, path));
    }
    store.transaction(|store| {
        for (old, path) in &replacements {
            for table in [
                "files",
                "file_bounds",
                "retired_files",
                "compaction_inputs",
                "garbage_removed",
            ] {
                store.exec(
                    &format!("UPDATE {table} SET path=? WHERE path=?"),
                    &[path, old],
                )?;
            }
            store.exec(
                "UPDATE retired_files SET replacement_path=? WHERE replacement_path=?",
                &[path, old],
            )?;
        }
        Ok(())
    })?;
    write_catalog(store, &root)?;
    let relocated = replacements
        .iter()
        .filter(|(old, path)| old != path)
        .count();
    Ok(Obj::new()
        .with("ok", true)
        .with("checkedFiles", files.len())
        .with("relocated", relocated))
}
