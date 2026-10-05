//! Offline performance reproductions kept for the ADR-0005 record. The `legacy` mode exercised
//! TypeScript write paths that no longer exist; it reports as not applicable.

use crate::jsonout::Obj;
use crate::store::{Store, StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::path::Path;

fn legacy(mode: &str) -> Option<Obj> {
    (mode == "legacy").then(|| Obj::new().with("mode", "legacy").with("applicable", false))
}

/// Resume-read benchmark on the largest published transactions file.
pub fn benchmark_decoder(root: &Path, mode: &str) -> Result<Obj, StoreError> {
    if let Some(report) = legacy(mode) {
        return Ok(report);
    }
    let catalog: Value = serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
        .map_err(|e| StoreError::Check(e.to_string()))?;
    let file = catalog
        .get("files")
        .and_then(Value::as_array)
        .and_then(|files| {
            files
                .iter()
                .filter(|f| f.get("table_name").and_then(Value::as_str) == Some("transactions"))
                .max_by_key(|f| {
                    f.get("row_count").map(|r| {
                        r.as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                            .or_else(|| r.as_i64())
                            .unwrap_or(0)
                    })
                })
        })
        .cloned()
        .ok_or_else(|| StoreError::Check("no published transactions".into()))?;
    let rows = file
        .get("row_count")
        .map(|r| {
            r.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| r.as_i64())
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let path = sql_string(file.get("path").and_then(Value::as_str).unwrap_or(""));
    let conn = memory_connection("4", "4GB")?;
    let offset = rows * 3 / 4;
    let started = std::time::Instant::now();
    let keys: Vec<String> = query_rows(&conn, &format!("SELECT file_row_number FROM (SELECT file_row_number, row_number() OVER (ORDER BY slot DESC, signature DESC) AS ordinal FROM read_parquet({path}, file_row_number=true)) WHERE ordinal>{offset} AND ordinal<={}", offset + 250), &[])?
        .iter()
        .filter_map(|r| r.int("file_row_number"))
        .map(|n| n.to_string())
        .collect();
    let batch = if keys.is_empty() {
        0
    } else {
        query_rows(&conn, &format!("SELECT * FROM read_parquet({path}, file_row_number=true) WHERE file_row_number IN ({}) ORDER BY slot DESC, signature DESC", keys.join(",")), &[])?.len()
    };
    Ok(Obj::new()
        .with("mode", mode)
        .with("sourceRows", rows)
        .with("offset", offset)
        .with("batchRows", batch)
        .with("seconds", started.elapsed().as_secs_f64()))
}

/// Ordering update benchmark; rolled back.
pub fn benchmark_ordering(store: &mut Store, mode: &str) -> Result<Obj, StoreError> {
    if let Some(report) = legacy(mode) {
        return Ok(report);
    }
    let progress = store
        .get("backfill")?
        .ok_or_else(|| StoreError::Check("no backfill cursor".into()))?;
    let to = progress.get("next").and_then(Value::as_i64).unwrap_or(0) + 1000;
    let from = to - 63;
    store.exec_batch("BEGIN")?;
    let started = std::time::Instant::now();
    let plan = store.rows(&format!("EXPLAIN ANALYZE UPDATE transactions SET tx_index=o.tx_index, single_in_slot=false FROM slot_order o WHERE transactions.signature=o.signature AND o.slot BETWEEN {from} AND {to} AND transactions.slot BETWEEN {from} AND {to}"), &[]);
    let seconds = started.elapsed().as_secs_f64();
    let _ = store.exec_batch("ROLLBACK");
    let plan = plan?;
    Ok(Obj::new()
        .with("mode", mode)
        .with("from", from)
        .with("to", to)
        .with("seconds", seconds)
        .with(
            "plan",
            Value::Array(
                plan.iter()
                    .filter_map(|r| r.get("explain_value").cloned())
                    .collect(),
            ),
        ))
}

/// Duplicate-page insert benchmark; rolled back.
pub fn benchmark_ingest(store: &mut Store, mode: &str) -> Result<Obj, StoreError> {
    if let Some(report) = legacy(mode) {
        return Ok(report);
    }
    let progress = store
        .get("backfill")?
        .ok_or_else(|| StoreError::Check("no backfill cursor".into()))?;
    let next = progress.get("next").and_then(Value::as_i64).unwrap_or(0);
    store.exec_batch("BEGIN")?;
    let started = std::time::Instant::now();
    let plan = store.rows(
        &format!("EXPLAIN ANALYZE INSERT INTO transactions SELECT * FROM transactions WHERE slot BETWEEN {} AND {} AND signature NOT IN (SELECT signature FROM transactions WHERE slot BETWEEN {} AND {}) LIMIT 100", next + 1, next + 1000, next + 1, next + 1000),
        &[],
    );
    let seconds = started.elapsed().as_secs_f64();
    let _ = store.exec_batch("ROLLBACK");
    let plan = plan?;
    Ok(Obj::new().with("seconds", seconds).with(
        "plan",
        Value::Array(
            plan.iter()
                .filter_map(|r| r.get("explain_value").cloned())
                .collect(),
        ),
    ))
}

/// Storage statistics for the offline `storage-stats` command.
pub fn storage_stats(store: &mut Store) -> Result<Obj, StoreError> {
    let allocation = store.rows("SELECT * FROM pragma_database_size()", &[])?;
    let tables = store.rows(
        "SELECT table_name,estimated_size FROM duckdb_tables() WHERE NOT temporary",
        &[],
    )?;
    let hot = store.rows(
        "SELECT count(*) AS n,min(slot) AS first,max(slot) AS last FROM transactions",
        &[],
    )?;
    Ok(Obj::new()
        .with_rows("allocation", allocation)
        .with_rows("tables", tables)
        .with_rows("hotTransactions", hot))
}
