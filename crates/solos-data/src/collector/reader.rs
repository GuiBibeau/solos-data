//! Read-only SQL over the raw archive: registered Parquet with the newest publication per
//! signature, plus a `coverage` view. A port of `reader.ts`.

use super::writer::TABLES;
use crate::jsonout::Obj;
use crate::lease::with_read_lease;
use crate::store::{StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::path::Path;

/// Run one `SELECT`/`WITH` statement over a raw root.
pub fn query_dataset(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    with_read_lease(root, || read_dataset(root, sql))
}

fn read_dataset(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    let head = sql.trim_start().to_lowercase();
    let is_select = ["select", "with"].iter().any(|kw| {
        head.starts_with(kw)
            && !head[kw.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
    });
    if !is_select || sql.contains(';') {
        return Err(StoreError::Check(
            "query accepts one SELECT or WITH statement".into(),
        ));
    }
    let catalog: Value = serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
        .map_err(|e| StoreError::Check(e.to_string()))?;
    let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
    let conn = memory_connection("4", &memory)?;
    let files = catalog
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    conn.execute(
        "CREATE TABLE registrations AS SELECT value->>'path' AS path, value->>'created_at' AS created_at FROM json_each(?::JSON)",
        [Value::Array(files.clone()).to_string()],
    )?;
    conn.execute_batch(&format!(
        "CREATE VIEW coverage AS SELECT (value->>'from')::BIGINT AS slot_from, (value->>'to')::BIGINT AS slot_to FROM json_each({}::JSON)",
        sql_string(&catalog.get("coverage").cloned().unwrap_or(Value::Array(vec![])).to_string())
    ))?;
    let mut available = Vec::new();
    for table in TABLES {
        let paths: Vec<String> = files
            .iter()
            .filter(|f| f.get("table_name").and_then(Value::as_str) == Some(table))
            .filter_map(|f| f.get("path").and_then(Value::as_str))
            .map(sql_string)
            .collect();
        if paths.is_empty() {
            continue;
        }
        conn.execute_batch(&format!(
            "CREATE VIEW {table} AS SELECT p.* EXCLUDE(filename) FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN registrations r ON r.path=p.filename QUALIFY row_number() OVER (PARTITION BY p.signature ORDER BY r.created_at DESC, p.filename DESC)=1",
            paths.join(",")
        ))?;
        available.push(Value::String(table.to_owned()));
    }
    let rows = query_rows(&conn, sql, &[])?;
    Ok(Obj::new()
        .with(
            "catalogAt",
            catalog.get("at").cloned().unwrap_or(Value::Null),
        )
        .with(
            "coverage",
            catalog
                .get("coverage")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        )
        .with("availableTables", Value::Array(available))
        .with(
            "acceptance",
            catalog.get("acceptance").cloned().unwrap_or(Value::Null),
        )
        .with_rows("rows", rows))
}

/// Rows of `table` with `slot` in `[from, to]` and the newest publication per signature, read
/// only from the registered files that can hold the range: ranged staging files whose name
/// covers it and compacted files of the same epochs. The slot filter sits inside the scan, so
/// Parquet row groups prune and the dedupe window sees only the range.
pub fn range_rows(
    root: &Path,
    table: &str,
    columns: &str,
    from: i64,
    to: i64,
) -> Result<Vec<Obj>, StoreError> {
    with_read_lease(root, || {
        let catalog: Value =
            serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
                .map_err(|e| StoreError::Check(e.to_string()))?;
        let epochs: std::collections::HashSet<i64> =
            (from.div_euclid(432_000)..=to.div_euclid(432_000)).collect();
        let files = catalog
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let selected: Vec<&Value> = files
            .iter()
            .filter(|f| f.get("table_name").and_then(Value::as_str) == Some(table))
            .filter(|f| {
                let path = f.get("path").and_then(Value::as_str).unwrap_or("");
                let in_epoch = epochs.iter().any(|e| path.contains(&format!("epoch={e}/")));
                match path_range(path) {
                    Some((a, b)) => in_epoch && a <= to && b >= from,
                    None => in_epoch,
                }
            })
            .collect();
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
        let conn = memory_connection("4", &memory)?;
        conn.execute(
            "CREATE TABLE registrations AS SELECT value->>'path' AS path, value->>'created_at' AS created_at FROM json_each(?::JSON)",
            [Value::Array(selected.iter().map(|f| (*f).clone()).collect()).to_string()],
        )?;
        let paths: Vec<String> = selected
            .iter()
            .filter_map(|f| f.get("path").and_then(Value::as_str))
            .map(sql_string)
            .collect();
        query_rows(
            &conn,
            &format!(
                "SELECT {columns} FROM (SELECT p.* EXCLUDE(filename), r.created_at, p.filename FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN registrations r ON r.path=p.filename WHERE p.slot BETWEEN {from} AND {to}) p QUALIFY row_number() OVER (PARTITION BY p.signature ORDER BY p.created_at DESC, p.filename DESC)=1 ORDER BY slot, signature",
                paths.join(",")
            ),
            &[],
        )
    })
}

/// `<from>-<to>-<name>.parquet` → `(from, to)`.
fn path_range(path: &str) -> Option<(i64, i64)> {
    let name = path.rsplit('/').next()?;
    let stem = name.strip_suffix(".parquet")?;
    let mut parts = stem.splitn(3, '-');
    let from = parts.next()?.parse::<i64>().ok()?;
    let to = parts.next()?.parse::<i64>().ok()?;
    parts.next()?;
    Some((from, to))
}
