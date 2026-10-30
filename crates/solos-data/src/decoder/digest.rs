//! Canonical content digest of a decoded root.
//!
//! `files.sha256` stays the raw file hash and is what detects bit-rot. This digest is the
//! equivalence check: two runs of the same rows produce it, including across a DuckDB build
//! change. It hashes the schema (column name, type, and order) and the reader rows in a total
//! order. Each field is tagged and length-prefixed. `decoded_at` and `source_file` keep their
//! schema slots and contribute omitted tags, not the clock or the raw-file byte hash. Footer
//! `created_by` is not read.
//!
//! All six tables are read under one lease, from one catalog snapshot. Rows stream through a
//! cursor one at a time: memory is bounded by one row, not the day (ADR-0017). The raw view
//! keeps `batch_id`, so the digest itself drops older revisions and refuses a repeated primary
//! key in one batch, across cursor boundaries.

use super::canonical::{put_row, put_schema};
use super::keys::reject;
use super::reader::{DecodedSession, with_decoded_session};
use super::schema::{TABLES, column_pairs, key_column, sort_columns};
use crate::jsonout::Obj;
use crate::store::StoreError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// Stream one table's rows in canonical order (`sort_columns`, `batch_id DESC`) and pass the
/// newest revision of each primary key to `on_new`. Older revisions are skipped, not hashed. A
/// repeated primary key in one batch is refused, including when the two copies arrive in
/// different cursor steps. Returns how many rows were passed (one per distinct key).
fn stream_newest(
    session: &DecodedSession,
    sql: &str,
    key: &str,
    table: &str,
    mut on_new: impl FnMut(&Obj) -> Result<(), StoreError>,
) -> Result<u64, StoreError> {
    let mut count = 0u64;
    let mut prev_key: Option<String> = None;
    let mut prev_batch: i64 = 0;
    session.stream(sql, |row| {
        let value = row.str(key).unwrap_or("").to_owned();
        let batch = row.int("batch_id").unwrap_or(0);
        let repeated = prev_key.as_deref() == Some(value.as_str());
        if repeated && batch == prev_batch {
            // A repeated key in one batch is not a total order; the digest refuses it.
            return Err(reject(table, key, &value));
        }
        if !repeated {
            on_new(row)?;
            count += 1;
        }
        prev_key = Some(value);
        prev_batch = batch;
        Ok(())
    })?;
    Ok(count)
}

/// SHA-256, hex, of the schema and decoded rows under `root`.
///
/// # Errors
///
/// The catalog or a Parquet file cannot be read, a primary key is repeated in one batch, the
/// DuckDB build is not 1.4.5, or a value cannot be encoded.
pub fn content_digest(root: &Path) -> Result<String, StoreError> {
    with_decoded_session(root, None, true, |session| {
        let mut hasher = Sha256::new();
        hasher.update(b"SDD2");
        for table in TABLES {
            let columns = column_pairs(table);
            let key = key_column(table);
            let mut record = Vec::new();
            put_schema(&mut record, table, &columns).map_err(StoreError::Check)?;
            let projection = columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {projection}, batch_id FROM {table}_raw ORDER BY {}, batch_id DESC",
                sort_columns(table)
            );
            // First pass counts the distinct keys (the rows the digest hashes) and refuses a
            // duplicate; the count must precede the rows in the record, and a cursor cannot
            // rewind, so the rows are streamed twice. Both passes hold one row, never the day.
            let count = stream_newest(session, &sql, key, table, |_| Ok(()))?;
            record.extend_from_slice(&count.to_be_bytes());
            hasher.update(&record);
            if count == 0 {
                continue;
            }
            let hashed = stream_newest(session, &sql, key, table, |row| {
                let mut encoded = Vec::new();
                put_row(&mut encoded, &columns, row).map_err(StoreError::Check)?;
                hasher.update(&encoded);
                Ok(())
            })?;
            if hashed != count {
                return Err(StoreError::Check(format!(
                    "digest for {table} hashed {hashed} rows, expected {count}"
                )));
            }
        }
        Ok(hex::encode(hasher.finalize()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sql_string;

    fn transactions(path: &std::path::Path, rows: &[(&str, i64)], source_file: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let values = rows
            .iter()
            .map(|(signature, slot)| {
                format!(
                    "('{signature}', 'h', {slot}, 0, false, 1, true, 'decoded', 0, 0, '{source_file}', '2026-10-04T00:00:00.000Z', 'v')"
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
            transactions(&path, file.rows, "src");
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

    /// One decoded root with a single `decoded_transactions` row whose `source_file` is the
    /// given raw-file byte hash.
    fn root_with_source_file(source_file: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("digest-source-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tables/decoded_transactions/epoch=0/a.parquet");
        transactions(&path, &[("sig-a", 10)], source_file);
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [{
                "path": "tables/decoded_transactions/epoch=0/a.parquet",
                "table_name": "decoded_transactions",
                "batch_id": 1
            }] })
            .to_string(),
        )
        .unwrap();
        dir
    }

    #[test]
    fn raw_file_provenance_does_not_move_the_digest() {
        // A DuckDB build bump rewrites the raw Parquet `created_by` footer, so the raw file's
        // byte hash in `decoded_transactions.source_file` changes while the decoded rows do not.
        // The content digest must not move with that provenance.
        let before = root_with_source_file("raw-sha-before-toolchain-bump");
        let after = root_with_source_file("raw-sha-after-toolchain-bump");
        assert_eq!(
            content_digest(&before).unwrap(),
            content_digest(&after).unwrap(),
            "source_file provenance (raw Parquet byte hash) moved the content digest"
        );
        std::fs::remove_dir_all(before).unwrap();
        std::fs::remove_dir_all(after).unwrap();
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
    fn a_duplicate_key_separated_by_other_rows_is_still_refused_while_streaming() {
        // The two copies of `sig-a` sit in different files and are separated by many other rows;
        // the streamed refusal has to carry the previous key across every cursor step, not just
        // within one file or one in-memory batch.
        fn write_rows(path: &std::path::Path, rows: &[(String, i64)]) {
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

        let dir = std::env::temp_dir().join(format!("digest-cross-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut many: Vec<(String, i64)> = (0..2_000).map(|n| (format!("sig-{n:06}"), n)).collect();
        many.push(("sig-a".to_owned(), 10_000));
        write_rows(
            &dir.join("tables/decoded_transactions/epoch=0/a.parquet"),
            &many,
        );
        write_rows(
            &dir.join("tables/decoded_transactions/epoch=0/b.parquet"),
            &[("sig-a".to_owned(), 10_000)],
        );
        std::fs::write(
            dir.join("catalog.json"),
            serde_json::json!({ "files": [
                { "path": "tables/decoded_transactions/epoch=0/a.parquet", "table_name": "decoded_transactions", "batch_id": 1 },
                { "path": "tables/decoded_transactions/epoch=0/b.parquet", "table_name": "decoded_transactions", "batch_id": 1 }
            ] })
            .to_string(),
        )
        .unwrap();
        let err = content_digest(&dir).unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate signature sig-a in decoded_transactions"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_revision_in_a_later_batch_is_kept_and_the_older_one_is_not_hashed() {
        let latest = root_with(&[Sample {
            path: "tables/decoded_transactions/epoch=0/a.parquet",
            batch: 2,
            rows: &[("sig-a", 10)],
        }]);
        let revised = root_with(&[
            Sample {
                path: "tables/decoded_transactions/epoch=0/a.parquet",
                batch: 1,
                rows: &[("sig-a", 10)],
            },
            Sample {
                path: "tables/decoded_transactions/epoch=0/b.parquet",
                batch: 2,
                rows: &[("sig-a", 10)],
            },
        ]);
        assert_eq!(
            content_digest(&latest).unwrap(),
            content_digest(&revised).unwrap(),
            "a later batch must supersede an earlier revision, not be hashed twice"
        );
        std::fs::remove_dir_all(latest).unwrap();
        std::fs::remove_dir_all(revised).unwrap();
    }
}
