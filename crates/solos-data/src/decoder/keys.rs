//! Primary-key uniqueness. A repeated key in one batch is not a total order: the reader would
//! keep one of the copies by an unstable tie, and the content digest would not be a function of
//! the rows. The publisher and the digest both refuse it. That is the no-duplicate-primary-key
//! check crash-safety re-runs use.

use super::schema::{TABLES, key_column};
use crate::store::{Store, StoreError, query_rows, sql_string};
use duckdb::Connection;

pub(crate) fn reject(table: &str, key: &str, value: &str) -> StoreError {
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

/// Refuse a repeated `(primary key, batch_id)` in any registered file attached to `conn` as a
/// `{table}_registrations` table (created by the reader's `attach_views`).
///
/// This is the full registered-file check: it reads each table's files directly and does not
/// join child tables to the deduplicated `decoded_transactions`, so a duplicate `event_id` or
/// `error_id` under a superseded or missing parent is still refused. Revisions in a later batch
/// are a different batch id and are kept.
///
/// # Errors
///
/// A duplicate, or the query fails.
pub fn assert_registered_keys_unique(conn: &Connection) -> Result<(), StoreError> {
    // Failpoint for the digest lease test. `content_digest` must reach this check while the
    // read lease is held (issue #66). The session that owns `conn` was opened from one catalog
    // read under that lease.
    crate::crash_point!("digest/duplicate-key-check");
    for table in TABLES {
        let key = key_column(table);
        let registered = query_rows(
            conn,
            &format!("SELECT path FROM {table}_registrations"),
            &[],
        )?;
        if registered.is_empty() {
            continue;
        }
        let list = registered
            .iter()
            .filter_map(|row| row.str("path"))
            .map(sql_string)
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT key FROM (
               SELECT p.{key}::VARCHAR AS key, r.batch_id AS batch_id
               FROM read_parquet([{list}], filename=true, union_by_name=true) p
               JOIN {table}_registrations r ON r.path = p.filename
             ) AS rows GROUP BY key, batch_id HAVING count(*) > 1 ORDER BY key LIMIT 1"
        );
        let rows = query_rows(conn, &sql, &[])?;
        if let Some(row) = rows.first() {
            return Err(reject(table, key, row.str("key").unwrap_or("")));
        }
    }
    Ok(())
}
