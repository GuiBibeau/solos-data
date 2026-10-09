//! Writing one Parquet file from a `SELECT` on the ledger's connection: `COPY` to a `.tmp`
//! sibling, fsync, rename, fsync the directory, then count the rows back and hash the file.
//! Downloaded CSV and JSON pages are read through DuckDB's readers from the `staging/` directory
//! and deleted once converted.

use super::ledger::FileRecord;
use crate::fsutil::{file_hash, sync_path, tmp_path};
use crate::store::{Store, StoreError, sql_string};
use std::path::{Path, PathBuf};

/// Where a file goes and what it is.
#[derive(Clone, Debug)]
pub struct Target {
    /// Source name.
    pub source: String,
    /// Dataset name.
    pub dataset: String,
    /// The venue's symbol, verbatim.
    pub symbol: String,
    /// Phoenix symbol, when the series is a Phoenix market.
    pub phoenix_symbol: Option<String>,
    /// Period label.
    pub period: String,
    /// Whether the period is closed.
    pub complete: bool,
}

impl Target {
    /// `tables/<source>/<dataset>/<symbol>/<period>.parquet`; a `:` in a symbol (Hyperliquid
    /// dex prefix) becomes `_` in the directory name only.
    #[must_use]
    pub fn relative_path(&self) -> String {
        format!(
            "tables/{}/{}/{}/{}.parquet",
            self.source,
            self.dataset,
            self.symbol.replace(':', "_"),
            self.period
        )
    }

    /// SQL for the two constant columns every file carries.
    #[must_use]
    pub fn constant_columns(&self) -> String {
        let phoenix = self
            .phoenix_symbol
            .as_deref()
            .map_or_else(|| "NULL::VARCHAR".to_owned(), sql_string);
        format!(
            "{} AS symbol, {phoenix} AS phoenix_symbol",
            sql_string(&self.symbol)
        )
    }
}

/// A fresh path under `<root>/staging/` with the given extension.
#[must_use]
pub fn staging_path(root: &Path, extension: &str) -> PathBuf {
    root.join("staging")
        .join(format!("{}.{extension}", uuid::Uuid::new_v4()))
}

/// Run `COPY (<select>) TO <target>` durably and describe the result.
pub fn write_parquet(
    store: &mut Store,
    select: &str,
    target: &Target,
) -> Result<FileRecord, StoreError> {
    let relative = target.relative_path();
    let path = store.root.join(&relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_path(&path);
    store.exec_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
        sql_string(&tmp.to_string_lossy())
    ))?;
    sync_path(&tmp)?;
    std::fs::rename(&tmp, &path)?;
    if let Some(parent) = path.parent() {
        sync_path(parent)?;
    }
    let rows = store.rows(
        &format!(
            "SELECT count(*) AS n FROM read_parquet({})",
            sql_string(&path.to_string_lossy())
        ),
        &[],
    )?;
    let row_count = rows.first().and_then(|row| row.int("n")).unwrap_or(0);
    let bytes = i64::try_from(std::fs::metadata(&path)?.len()).unwrap_or(i64::MAX);
    Ok(FileRecord {
        path: relative,
        source: target.source.clone(),
        dataset: target.dataset.clone(),
        symbol: target.symbol.clone(),
        phoenix_symbol: target.phoenix_symbol.clone(),
        period: target.period.clone(),
        complete: target.complete,
        row_count,
        bytes,
        sha256: file_hash(&path)?,
    })
}

/// `read_csv(...)` with explicit names and types; `header` says whether the first line is one.
#[must_use]
pub fn read_csv(path: &Path, columns: &[(&str, &str)], header: bool) -> String {
    format!(
        "read_csv({}, header={header}, columns={}, auto_detect=false, delim=',', quote='\"')",
        sql_string(&path.to_string_lossy()),
        column_map(columns)
    )
}

/// `read_json(...)` over a JSON array of objects with explicit names and types.
#[must_use]
pub fn read_json_array(path: &Path, columns: &[(&str, &str)]) -> String {
    format!(
        "read_json({}, format='array', columns={}, maximum_object_size=67108864)",
        sql_string(&path.to_string_lossy()),
        column_map(columns)
    )
}

fn column_map(columns: &[(&str, &str)]) -> String {
    let entries: Vec<String> = columns
        .iter()
        .map(|(name, kind)| format!("{}: '{kind}'", sql_string(name)))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// Write a JSON array of rows to staging for `read_json_array`.
pub fn stage_json(root: &Path, rows: &[serde_json::Value]) -> Result<PathBuf, StoreError> {
    let path = staging_path(root, "json");
    let text = serde_json::to_vec(rows).map_err(|e| StoreError::Check(e.to_string()))?;
    std::fs::write(&path, text)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_and_readers() {
        let target = Target {
            source: "hyperliquid".into(),
            dataset: "funding".into(),
            symbol: "xyz:NVDA".into(),
            phoenix_symbol: Some("NVDA".into()),
            period: "2026-01".into(),
            complete: true,
        };
        assert_eq!(
            target.relative_path(),
            "tables/hyperliquid/funding/xyz_NVDA/2026-01.parquet"
        );
        assert_eq!(
            target.constant_columns(),
            "'xyz:NVDA' AS symbol, 'NVDA' AS phoenix_symbol"
        );
        assert_eq!(
            read_csv(
                Path::new("/s/a.csv"),
                &[("t", "BIGINT"), ("p", "DOUBLE")],
                true
            ),
            "read_csv('/s/a.csv', header=true, columns={'t': 'BIGINT', 'p': 'DOUBLE'}, auto_detect=false, delim=',', quote='\"')"
        );
    }
}
