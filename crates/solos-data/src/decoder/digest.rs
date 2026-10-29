//! Canonical content digest of a decoded root.
//!
//! `files.sha256` stays the raw file hash and is what detects bit-rot. This digest is the
//! equivalence check: two runs of the same rows produce it, including across a DuckDB build
//! change. It hashes the schema (column name, type, and order) and the reader rows in a total
//! order. Each field is tagged and length-prefixed. `decoded_at` keeps its schema slot and
//! contributes an omitted tag, not the clock. Footer `created_by` is not read.
//!
//! All six tables are read under one lease, from one catalog snapshot. The rows are still
//! materialized as JSON before they are encoded. That holds the fixture day and must not be
//! how a production day is hashed; see ADR-0017.

use super::canonical::{put_row, put_schema};
use super::keys::assert_registered_keys_unique;
use super::reader::query_decoded_many;
use super::schema::{TABLES, column_pairs, sort_columns};
use crate::jsonout::Obj;
use crate::store::StoreError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// The row list of a digest query. A missing list is a shape error, not an empty table.
fn row_list<'a>(result: &'a Obj, table: &str) -> Result<&'a [Obj], StoreError> {
    result
        .rows("rows")
        .ok_or_else(|| StoreError::Check(format!("digest query for {table} returned no row list")))
}

/// SHA-256, hex, of the schema and decoded rows under `root`.
///
/// # Errors
///
/// The catalog or a Parquet file cannot be read, a primary key is repeated in one batch, the
/// DuckDB build is not 1.4.5, a query result has no row list, or a value cannot be encoded.
pub fn content_digest(root: &Path) -> Result<String, StoreError> {
    assert_registered_keys_unique(root)?;
    let statements: Vec<String> = TABLES
        .iter()
        .map(|table| {
            let projection = column_pairs(table)
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {projection} FROM {table} ORDER BY {}",
                sort_columns(table)
            )
        })
        .collect();
    let refs: Vec<&str> = statements.iter().map(String::as_str).collect();
    // One lease for every table, so the six results are one snapshot.
    let results = query_decoded_many(root, &refs)?;
    if results.len() != TABLES.len() {
        return Err(StoreError::Check(
            "digest did not read every decoded table".into(),
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"SDD2");
    for (table, result) in TABLES.iter().zip(results) {
        let columns = column_pairs(table);
        let mut record = Vec::new();
        put_schema(&mut record, table, &columns).map_err(StoreError::Check)?;
        let rows = row_list(&result, table)?;
        let count = u64::try_from(rows.len()).map_err(|_| StoreError::Check("row count".into()))?;
        record.extend_from_slice(&count.to_be_bytes());
        hasher.update(&record);
        for row in rows {
            let mut encoded = Vec::new();
            put_row(&mut encoded, &columns, row).map_err(StoreError::Check)?;
            hasher.update(&encoded);
        }
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sql_string;

    fn transactions(path: &std::path::Path, rows: &[(&str, i64)]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let values = rows
            .iter()
            .map(|(signature, slot)| {
                format!(
                    "('{signature}', 'h', {slot}, 0, false, 1, true, 'decoded', 0, 0, 'src', '2026-10-04T00:00:00.000Z', 'v')"
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES {values}) t(signature, source_hash, slot, tx_index, single_in_slot, block_time, committed, status, event_count, error_count, source_file, decoded_at, decoder_version)) TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    }

    struct Sample<'a> {
        path: &'a str,
        batch: i64,
        rows: &'a [(&'a str, i64)],
    }

    fn root_with(files: &[Sample<'_>]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("digest-rows-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut listed = Vec::new();
        for file in files {
            let path = dir.join(file.path);
            transactions(&path, file.rows);
            listed.push(serde_json::json!({
                "path": file.path,
                "table_name": "decoded_transactions",
                "batch_id": file.batch
            }));
        }
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": listed }).to_string(),
        )
        .unwrap();
        dir
    }

    #[test]
    fn the_same_rows_in_a_different_order_share_a_digest_and_one_value_changes_it() {
        let forward = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-b", 20), ("sig-a", 10)],
        }]);
        let reverse = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 10), ("sig-b", 20)],
        }]);
        let changed = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 1,
            rows: &[("sig-a", 11), ("sig-b", 20)],
        }]);
        let left = content_digest(&forward).unwrap();
        let right = content_digest(&reverse).unwrap();
        let other = content_digest(&changed).unwrap();
        assert_eq!(left, right);
        assert_ne!(left, other);
        std::fs::remove_dir_all(forward).unwrap();
        std::fs::remove_dir_all(reverse).unwrap();
        std::fs::remove_dir_all(changed).unwrap();
    }

    #[test]
    fn an_injected_duplicate_key_is_an_error() {
        let dir = root_with(&[
            Sample {
                path: "tables/decoded_transactions/epoch=0/a.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
            Sample {
                path: "tables/decoded_transactions/epoch=0/b.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
        ]);
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate signature sig-a in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_digest_result_without_a_row_list_is_an_error() {
        let err = row_list(&Obj::new(), "events").unwrap_err();
        assert!(err.to_string().contains("no row list"), "{err}");
        assert!(err.to_string().contains("events"), "{err}");
        assert!(
            row_list(&Obj::new().with_rows("rows", Vec::new()), "events")
                .unwrap()
                .is_empty()
        );
    }
}
