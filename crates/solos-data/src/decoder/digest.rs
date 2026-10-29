//! Canonical content digest of a decoded root.
//!
//! The hash is not the Parquet file SHA-256. Two things in the file are not a function of the
//! decoded rows: DuckDB's footer `created_by` (library version and build id) and
//! `decoded_transactions.decoded_at` (the wall clock). Catalog `at` and `files.created_at` are
//! wall clocks too and are not read. The digest hashes the reader's rows, latest revision of
//! each key, in definition order, excluding `decoded_at`, sorted by [`sort_columns`](super::schema::sort_columns).

use super::reader::query_decoded;
use super::schema::{TABLES, digest_projection, sort_columns};
use crate::store::StoreError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// SHA-256, hex, of the decoded rows under `root`.
///
/// # Errors
///
/// The catalog or a Parquet file cannot be read.
pub fn content_digest(root: &Path) -> Result<String, StoreError> {
    let mut hasher = Sha256::new();
    hasher.update(b"solos-decoded-digest-v1\n");
    for table in TABLES {
        hasher.update(table.as_bytes());
        hasher.update(b"\n");
        let projection = digest_projection(table);
        let order = sort_columns(table);
        let sql = format!("SELECT {projection} FROM {table} ORDER BY {order}");
        let result = query_decoded(root, &sql)?;
        for row in result.rows("rows").unwrap_or(&[]) {
            hasher.update(row.to_json().as_bytes());
            hasher.update(b"\n");
        }
    }
    Ok(hex::encode(hasher.finalize()))
}
