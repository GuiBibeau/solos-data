//! Pinned Parquet writer settings for decoded partitions.
//!
//! DuckDB 1.4.5 (`duckdb = "=1.4.5"`) writes a footer `created_by` of
//! `DuckDB version <version> (build <source id>)`. That string changes when the library build
//! changes, so a SHA-256 of the file bytes is not a stable golden hash. These options are that
//! version's defaults written out, so a later default cannot move them: ZSTD at level 3,
//! 122880-row groups, Parquet V1, bloom filters on. `ROW_GROUP_SIZE_BYTES` stays unset. In 1.4.5
//! it is unlimited while insertion order is preserved, and setting it is rejected in that mode.

use crate::store::sql_string;
use std::path::Path;

/// `COPY` options for every decoded Parquet file.
pub const COPY_OPTIONS: &str = concat!(
    "FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ",
    "ROW_GROUP_SIZE 122880, PARQUET_VERSION 'V1', WRITE_BLOOM_FILTER true"
);

/// `COPY (<select>) TO <path>` with [`COPY_OPTIONS`]. `select` must already be ordered.
#[must_use]
pub fn copy_statement(select: &str, path: &Path) -> String {
    format!(
        "COPY ({select}) TO {} ({COPY_OPTIONS})",
        sql_string(&path.to_string_lossy())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::file_hash;

    fn write(options: &str) -> String {
        let dir = std::env::temp_dir().join(format!("parquet-pin-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.parquet");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sql = format!(
            "COPY (SELECT * FROM (VALUES (1::BIGINT, 'b'), (1::BIGINT, 'a'), (2::BIGINT, 'a')) \
             t(slot, signature) ORDER BY slot, signature) TO {} ({options})",
            sql_string(&path.to_string_lossy())
        );
        conn.execute_batch(&sql).unwrap();
        let hash = file_hash(&path).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        hash
    }

    #[test]
    fn pinned_options_repeat_and_match_the_previous_clause() {
        let first = write(COPY_OPTIONS);
        let second = write(COPY_OPTIONS);
        assert_eq!(first, second, "pinned writer bytes differ across writes");
        let previous = write("FORMAT PARQUET, COMPRESSION ZSTD");
        assert_eq!(
            first, previous,
            "explicit 1.4.5 settings change bytes versus FORMAT PARQUET, COMPRESSION ZSTD"
        );
    }

    /// Rows already in `(slot, signature, event_ordinal)` order. The decoder inserts events that
    /// way. The total key must not move them relative to `ORDER BY slot, signature`.
    fn write_presorted(order: &str) -> String {
        let dir = std::env::temp_dir().join(format!("parquet-order-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.parquet");
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sql = format!(
            "COPY (SELECT * FROM (VALUES (1::BIGINT, 'a', 1::INTEGER), (1::BIGINT, 'a', 2::INTEGER)) \
             t(slot, signature, event_ordinal) ORDER BY {order}) TO {} ({COPY_OPTIONS})",
            sql_string(&path.to_string_lossy())
        );
        conn.execute_batch(&sql).unwrap();
        let hash = file_hash(&path).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        hash
    }

    #[test]
    fn total_key_keeps_bytes_when_tied_rows_are_already_in_order() {
        assert_eq!(
            write_presorted("slot, signature"),
            write_presorted("slot, signature, event_ordinal")
        );
    }
}
