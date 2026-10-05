//! Checkpoint retention against the permanent raw archive (ADR-0006): bounds per file, digest
//! comparison before deletion, a round-robin cursor so cold ranges all get trimmed, and the
//! one-time legacy cleanup. A port of `archive.ts`, `retention.ts` and `legacy-gc.ts`.

use super::catalog::{merge_coverage, write_catalog};
use super::writer::TABLES;
use crate::fsutil::file_hash;
use crate::jsonout::{Obj, now};
use crate::lease::has_readers;
use crate::store::{Store, StoreError, sql_string};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::Path;

/// Register slot bounds for active files that lack them.
pub fn register_bounds(store: &mut Store) -> Result<(), StoreError> {
    let missing = store.rows("SELECT f.path FROM files f LEFT JOIN file_bounds b USING(path) WHERE f.status='active' AND b.path IS NULL", &[])?;
    for file in missing {
        let path = file.str("path").unwrap_or("").to_owned();
        let bounds = store
            .rows(
                &format!(
                    "SELECT min(slot) AS first,max(slot) AS last FROM read_parquet({})",
                    sql_string(&path)
                ),
                &[],
            )?
            .into_iter()
            .next()
            .unwrap_or_default();
        store.exec(
            "INSERT OR IGNORE INTO file_bounds VALUES (?, ?, ?)",
            &[
                &path,
                &bounds.int("first").unwrap_or(0),
                &bounds.int("last").unwrap_or(0),
            ],
        )?;
    }
    Ok(())
}

/// `sha256(to_json(struct_pack(col := col::VARCHAR, ...)))` over a table's columns.
pub fn checkpoint_digest(
    store: &mut Store,
    table: &str,
    archive: bool,
) -> Result<String, StoreError> {
    let columns = store.rows(
        &format!("SELECT name FROM pragma_table_info('{table}') ORDER BY cid"),
        &[],
    )?;
    let fields: Vec<String> = columns
        .iter()
        .filter_map(|c| c.str("name"))
        .map(|name| {
            let source = if archive && table == "rpc_pages" && name == "page_id" {
                "signature"
            } else {
                name
            };
            format!("{name}:={source}::VARCHAR")
        })
        .collect();
    Ok(format!(
        "sha256(to_json(struct_pack({})))",
        fields.join(",")
    ))
}

/// SQL for the canonical archive rows of `table` in `[from, to]`, or `None` when no file covers
/// the range. With `digests`, only `(signature, digest)` pairs are produced.
pub fn archive_relation(
    store: &mut Store,
    table: &str,
    from: i64,
    to: i64,
    verified: &mut HashSet<String>,
    digests: bool,
) -> Result<Option<String>, StoreError> {
    let files = store.rows(
        "SELECT f.* FROM files f JOIN file_bounds b USING(path) WHERE f.status='active' AND f.table_name=? AND b.slot_from<=? AND b.slot_to>=?",
        &[&table, &to, &from],
    )?;
    for file in &files {
        let sha = file.str("sha256").unwrap_or("").to_owned();
        if verified.contains(&sha) {
            continue;
        }
        let path = file.str("path").unwrap_or("");
        if file_hash(Path::new(path))? != sha {
            return Err(StoreError::Check("archive checksum mismatch".into()));
        }
        let count = store.rows(
            &format!(
                "SELECT count(*) AS n FROM read_parquet({})",
                sql_string(path)
            ),
            &[],
        )?;
        if count.first().and_then(|r| r.int("n")) != file.int("row_count") {
            return Err(StoreError::Check("archive row count mismatch".into()));
        }
        verified.insert(sha);
    }
    if files.is_empty() {
        return Ok(None);
    }
    let paths = format!(
        "[{}]",
        files
            .iter()
            .filter_map(|f| f.str("path"))
            .map(sql_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    if digests {
        let digest = checkpoint_digest(store, table, true)?;
        // Compute hashes before the revision window: its working set has only keys/hashes, never
        // the transaction or RPC payload strings for the entire historical range.
        return Ok(Some(format!(
            "SELECT p.signature,p.digest FROM (SELECT signature,{digest} AS digest,filename
      FROM read_parquet({paths},filename=true,union_by_name=true) WHERE slot BETWEEN {from} AND {to}) p
      JOIN files f ON f.path=p.filename QUALIFY row_number() OVER
      (PARTITION BY p.signature ORDER BY f.created_at DESC,p.filename DESC)=1"
        )));
    }
    let columns = store.rows(
        &format!("SELECT name FROM pragma_table_info('{table}') ORDER BY cid"),
        &[],
    )?;
    let projection: Vec<String> = columns
        .iter()
        .filter_map(|c| c.str("name"))
        .map(|name| {
            if table == "rpc_pages" && name == "page_id" {
                "p.signature AS page_id".to_owned()
            } else {
                format!("p.{name}")
            }
        })
        .collect();
    Ok(Some(format!(
        "SELECT {} FROM read_parquet({paths},filename=true,union_by_name=true) p
    JOIN files f ON f.path=p.filename WHERE p.slot BETWEEN {from} AND {to}
    QUALIFY row_number() OVER(PARTITION BY p.signature ORDER BY f.created_at DESC,p.filename DESC)=1",
        projection.join(",")
    )))
}

/// Delete only checkpoint copies; every source field remains in the verified raw archive.
pub fn prune_published(
    store: &mut Store,
    root: &Path,
    hot_slots: i64,
    max_slots: i64,
) -> Result<Obj, StoreError> {
    let Some(watermark) = store.get("W")? else {
        return Ok(Obj::new().with("ranges", 0).with("removedRows", 0));
    };
    let tail = store.get("tail-active")?;
    let w_slot = watermark.get("slot").and_then(Value::as_i64).unwrap_or(0);
    let cutoff = match tail
        .as_ref()
        .and_then(|t| t.get("floor"))
        .and_then(Value::as_i64)
    {
        Some(floor) => (w_slot - hot_slots).min(floor - 1),
        None => w_slot - hot_slots,
    };
    register_bounds(store)?;
    write_catalog(store, root)?;
    let ranges_rows = store.rows("SELECT slot_from,slot_to FROM published_ranges", &[])?;
    let coverage = merge_coverage(
        ranges_rows
            .iter()
            .map(|r| {
                (
                    r.int("slot_from").unwrap_or(0),
                    r.int("slot_to").unwrap_or(0).min(cutoff),
                )
            })
            .filter(|r| r.0 <= r.1)
            .collect(),
    );
    let cursor = store.get("retention-cursor")?.map_or(0, |v| {
        v.as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| v.as_i64())
            .unwrap_or(0)
    });
    let mut candidates: Vec<(i64, i64)> = coverage
        .iter()
        .map(|c| (c.0.max(cursor), c.1))
        .filter(|c| c.0 <= c.1)
        .collect();
    candidates.extend(coverage.iter().copied());
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    for covered in candidates {
        let hot = store
            .rows(
                "SELECT min(slot) AS first FROM transactions WHERE slot BETWEEN ? AND ?",
                &[&covered.0, &covered.1],
            )?
            .into_iter()
            .next()
            .unwrap_or_default();
        let Some(first) = hot.int("first") else {
            continue;
        };
        ranges.push((first, covered.1.min(first + max_slots - 1)));
        break;
    }
    let mut verified: HashSet<String> = HashSet::new();
    let mut removed_rows = 0i64;
    for (first, last) in &ranges {
        let (from, to) = (*first, *last);
        store.exec_batch("BEGIN")?;
        let result: Result<(), StoreError> = (|| {
            for table in TABLES.iter().filter(|t| **t != "program_versions") {
                let count = store
                    .rows(
                        &format!("SELECT count(*) AS n FROM {table} WHERE slot BETWEEN ? AND ?"),
                        &[&from, &to],
                    )?
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                let n = count.int("n").unwrap_or(0);
                if n == 0 {
                    continue;
                }
                let Some(archive) = archive_relation(store, table, from, to, &mut verified, true)?
                else {
                    return Err(StoreError::Check("unarchived checkpoint rows".into()));
                };
                let digest = checkpoint_digest(store, table, false)?;
                let key = if *table == "rpc_pages" {
                    "page_id"
                } else {
                    "signature"
                };
                let missing = store
                    .rows(&format!("SELECT count(*) AS n FROM (SELECT {key} AS signature,{digest} AS digest FROM {table} WHERE slot BETWEEN {from} AND {to} EXCEPT ({archive}))"), &[])?
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                if missing.int("n").unwrap_or(1) != 0 {
                    return Err(StoreError::Check(
                        "archive does not match checkpoint rows".into(),
                    ));
                }
                store.exec(
                    &format!("DELETE FROM {table} WHERE slot BETWEEN ? AND ?"),
                    &[&from, &to],
                )?;
                removed_rows += n;
            }
            store.exec(
                "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
                &[
                    &format!("retention/{from}-{to}"),
                    &json!({ "at": now(), "archiveVerified": true }).to_string(),
                ],
            )?;
            store.exec(
                "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
                &[&"retention-cursor", &(to + 1).to_string()],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => store.exec_batch("COMMIT")?,
            Err(error) => {
                let _ = store.exec_batch("ROLLBACK");
                return Err(error);
            }
        }
    }
    let value = Obj::new()
        .with("at", now())
        .with("hotSlots", hot_slots)
        .with("cutoff", cutoff)
        .with("ranges", ranges.len())
        .with("removedRows", removed_rows)
        .with(
            "policy",
            "raw history retained in Parquet; only published checkpoint copies trimmed",
        );
    store.set("retention", &value.to_value())?;
    Ok(value)
}

/// One-time migration of old superseded files that predate replacement lineage.
pub fn collect_legacy(store: &mut Store, root: &Path) -> Result<Obj, StoreError> {
    register_bounds(store)?;
    write_catalog(store, root)?;
    if has_readers(root)? {
        return Ok(Obj::new()
            .with("removedFiles", 0)
            .with("removedBytes", 0)
            .with("retainedFiles", 0));
    }
    let files = store.rows("SELECT * FROM files WHERE status='superseded' AND path NOT IN (SELECT path FROM retired_files)", &[])?;
    let mut verified: HashSet<String> = HashSet::new();
    let (mut removed_files, mut removed_bytes, mut retained) = (0u64, 0u64, 0u64);
    for file in &files {
        let path = file.str("path").unwrap_or("").to_owned();
        let table = file.str("table_name").unwrap_or("").to_owned();
        let bytes = match std::fs::metadata(&path) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        if bytes == 0 {
            store.exec("UPDATE files SET status='deleted' WHERE path=?", &[&path])?;
            continue;
        }
        if file_hash(Path::new(&path))? != file.str("sha256").unwrap_or("") {
            return Err(StoreError::Check("legacy source checksum mismatch".into()));
        }
        let bounds = store
            .rows(
                &format!(
                    "SELECT min(slot) AS first,max(slot) AS last FROM read_parquet({})",
                    sql_string(&path)
                ),
                &[],
            )?
            .into_iter()
            .next()
            .unwrap_or_default();
        let Some(archive) = archive_relation(
            store,
            &table,
            bounds.int("first").unwrap_or(0),
            bounds.int("last").unwrap_or(0),
            &mut verified,
            true,
        )?
        else {
            retained += 1;
            continue;
        };
        let digest = checkpoint_digest(store, &table, true)?;
        let n = store.rows(&format!("SELECT count(*) AS n FROM (SELECT signature,{digest} AS digest FROM read_parquet({}) EXCEPT ({archive}))", sql_string(&path)), &[])?.into_iter().next().unwrap_or_default();
        if n.int("n").unwrap_or(1) != 0 {
            retained += 1;
            continue;
        }
        std::fs::remove_file(&path)?;
        store.exec("UPDATE files SET status='deleted' WHERE path=?", &[&path])?;
        removed_files += 1;
        removed_bytes += bytes;
    }
    Ok(Obj::new()
        .with("removedFiles", removed_files)
        .with("removedBytes", removed_bytes)
        .with("retainedFiles", retained))
}
