//! Canonical content digest of a decoded root.
//!
//! `files.sha256` stays the raw file hash and is what detects bit-rot. This digest is the
//! equivalence check: two runs of the same rows produce it, including across a DuckDB build
//! change. It hashes the schema (column name, type, and order) and the reader rows in a total
//! order. Each field is tagged and length-prefixed. `decoded_at` keeps its schema slot and
//! contributes an omitted tag, not the clock. Footer `created_by` is not read.

use super::canonical::{put_row, put_schema};
use super::keys::assert_registered_keys_unique;
use super::reader::query_decoded;
use super::schema::{TABLES, column_pairs, sort_columns};
use crate::store::StoreError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// SHA-256, hex, of the schema and decoded rows under `root`.
///
/// # Errors
///
/// The catalog or a Parquet file cannot be read, a primary key is repeated in one batch, or a
/// value cannot be encoded.
pub fn content_digest(root: &Path) -> Result<String, StoreError> {
    assert_registered_keys_unique(root)?;
    let mut hasher = Sha256::new();
    hasher.update(b"SDD2");
    for table in TABLES {
        let columns = column_pairs(table);
        let mut record = Vec::new();
        put_schema(&mut record, table, &columns).map_err(StoreError::Check)?;
        let projection = columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {projection} FROM {table} ORDER BY {}",
            sort_columns(table)
        );
        let result = query_decoded(root, &sql)?;
        let rows = result.rows("rows").unwrap_or(&[]);
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
