//! Primary-key uniqueness. A repeated key in one batch is not a total order: the reader would
//! keep one of the copies by an unstable tie, and the content digest would not be a function of
//! the rows. The publisher and the digest both refuse it. That is the no-duplicate-primary-key
//! check crash-safety re-runs use.

use super::schema::{TABLES, key_column};
use super::source::source_path;
use crate::store::{Store, StoreError, query_rows, sql_string};
use serde_json::Value;
use std::path::Path;

fn reject(table: &str, key: &str, value: &str) -> StoreError {
    StoreError::Check(format!("duplicate {key} {value} in {table}"))
}

/// Refuse a repeated `(primary key, batch_id)` in `relation`.
///
/// `relation` is a `SELECT` of `key` and `batch_id` over compactor inputs before `QUALIFY`.
/// Two files that share a key and a batch would otherwise collapse by filename.
///
/// # Errors
///
/// A duplicate, or the query fails.
pub fn assert_key_batch_unique(
    store: &Store,
    table: &str,
    relation: &str,
) -> Result<(), StoreError> {
    let key = key_column(table);
    let sql = format!(
        "SELECT key, batch_id FROM ({relation}) AS inputs GROUP BY key, batch_id HAVING count(*) > 1 ORDER BY key LIMIT 1"
    );
    let rows = store.rows(&sql, &[])?;
    if let Some(row) = rows.first() {
        return Err(StoreError::Check(format!(
            "duplicate {key} {} in {table} batch {}",
            row.str("key").unwrap_or(""),
            row.int("batch_id").unwrap_or(0)
        )));
    }
    Ok(())
}

/// Refuse a repeated primary key in `relation` (`events`, or `(SELECT ...) AS keyed`).
///
/// # Errors
///
/// Two rows share the key, or the query fails.
pub fn assert_relation_keys_unique(
    store: &Store,
    table: &str,
    relation: &str,
) -> Result<(), StoreError> {
    let key = key_column(table);
    let sql = format!(
        "SELECT {key}::VARCHAR AS key FROM {relation} GROUP BY {key} HAVING count(*) > 1 ORDER BY 1 LIMIT 1"
    );
    let rows = store.rows(&sql, &[])?;
    if let Some(row) = rows.first() {
        return Err(reject(table, key, row.str("key").unwrap_or("")));
    }
    Ok(())
}

fn batch_id(file: &Value) -> i64 {
    file.get("batch_id")
        .map(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

/// Refuse a repeated primary key inside one `(key, batch_id)` of the registered files.
///
/// Revisions in a later batch are a different batch id and are kept. Two copies in the same
/// batch are not.
///
/// # Errors
///
/// A duplicate, a bad catalog, or a Parquet read failure.
pub fn assert_registered_keys_unique(root: &Path) -> Result<(), StoreError> {
    let text = std::fs::read_to_string(root.join("catalog.json"))?;
    let catalog: Value =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    assert_registered_keys_unique_catalog(root, &catalog)
}

/// The same check against a catalog the caller already read under its lease, so the check sees
/// the same catalog as the rows (issue #66 follow-up 6).
///
/// # Errors
///
/// A duplicate, a bad catalog, or a Parquet read failure.
pub fn assert_registered_keys_unique_catalog(
    root: &Path,
    catalog: &Value,
) -> Result<(), StoreError> {
    let files = catalog
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let conn = duckdb::Connection::open_in_memory()?;
    for table in TABLES {
        let registered: Vec<&Value> = files
            .iter()
            .filter(|file| file.get("table_name").and_then(Value::as_str) == Some(table))
            .collect();
        if registered.is_empty() {
            continue;
        }
        let key = key_column(table);
        let mut paths = Vec::new();
        let mut regs = Vec::new();
        for file in registered {
            let relative = file.get("path").and_then(Value::as_str).unwrap_or("");
            let full = source_path(root, relative)?.to_string_lossy().into_owned();
            regs.push(serde_json::json!({"path": full, "batch_id": batch_id(file)}));
            paths.push(sql_string(&full));
        }
        let list = paths.join(",");
        let payload = Value::Array(regs).to_string();
        let sql = format!(
            "SELECT key FROM (
               SELECT p.{key}::VARCHAR AS key, r.batch_id AS batch_id
               FROM read_parquet([{list}], filename=true, union_by_name=true) p
               JOIN (SELECT value->>'path' AS path, (value->>'batch_id')::BIGINT AS batch_id
                     FROM json_each(?::JSON)) r ON r.path = p.filename
             ) AS rows GROUP BY key, batch_id HAVING count(*) > 1 ORDER BY key LIMIT 1"
        );
        let rows = query_rows(&conn, &sql, &[&payload])?;
        if let Some(row) = rows.first() {
            return Err(reject(table, key, row.str("key").unwrap_or("")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_primary_key_is_rejected() {
        let dir = std::env::temp_dir().join(format!("dup-key-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.parquet");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let listed = path.to_string_lossy().into_owned();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ('same'), ('same')) t(event_id)) TO {} (FORMAT PARQUET)",
            sql_string(&listed)
        ))
        .unwrap();
        let catalog = serde_json::json!({
            "files": [{
                "path": "events.parquet",
                "table_name": "events",
                "batch_id": 1
            }]
        });
        // The catalog path is relative. `source_path` joins it to the root, so the file has to
        // live at that relative path. Copy the bytes there.
        std::fs::write(dir.join("catalog.json"), catalog.to_string()).unwrap();
        let err = assert_registered_keys_unique(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate event_id same in events"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn distinct_primary_keys_pass() {
        let dir = std::env::temp_dir().join(format!("uniq-key-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.parquet");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ('a'), ('b')) t(event_id)) TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
        let catalog = serde_json::json!({
            "files": [{ "path": "events.parquet", "table_name": "events", "batch_id": 1 }]
        });
        std::fs::write(dir.join("catalog.json"), catalog.to_string()).unwrap();
        assert_registered_keys_unique(&dir).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
