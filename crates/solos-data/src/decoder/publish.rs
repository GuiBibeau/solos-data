//! Durable publication of one decoded batch: Parquet per table and epoch, then one transaction
//! registering the files, the processed revisions, the source offset and the batch number. A
//! port of `decode/publish.ts`.

use super::normalize::Rows;
use super::schema::{TABLES, VERSION, definition, type_map};
use crate::fsutil::{create_dir_all_durable, file_hash, sync_path, write_durable};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;

/// How far the source was consumed and which signatures this batch saw.
pub struct SourceProgress {
    /// Source file hash.
    pub hash: String,
    /// Source file path as registered.
    pub path: String,
    /// Rows consumed after this batch.
    pub offset: u64,
    /// Source publication instant.
    pub at: Option<String>,
    /// `(signature, source_hash)` pairs seen, including unchanged revisions.
    pub seen: Option<Vec<(String, String)>>,
}

struct Published {
    path: String,
    table: &'static str,
    count: usize,
    hash: String,
}

/// Write the batch and commit its registrations.
pub fn publish(store: &mut Store, rows: &Rows, source: &SourceProgress) -> Result<(), StoreError> {
    publish_sources(store, rows, std::slice::from_ref(source), None)
}

/// Write one batch read from several sources and commit its registrations, every source's
/// offset and every seen revision in one transaction under one batch number.
///
/// No signature may be seen by two sources of one batch (the caller ends a batch before a
/// repeat), so the batch's revisions never tie on batch number. `known`, when given, is every
/// seen signature `processed` already held when the batch was read: only those can be updated,
/// so the update (a pass over the whole table) is skipped when none is. `None` keeps the
/// unrestricted update.
pub fn publish_sources(
    store: &mut Store,
    rows: &Rows,
    sources: &[SourceProgress],
    known: Option<&HashSet<String>>,
) -> Result<(), StoreError> {
    if sources.len() > 1 && sources.iter().any(|source| source.seen.is_none()) {
        return Err(StoreError::Check(
            "a batch of several sources needs every source's seen revisions".into(),
        ));
    }
    let batch = store.get("batch")?.map_or(0, |v| {
        v.as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| v.as_i64())
            .unwrap_or(0)
    }) + 1;
    // Files are durable before the transaction that registers them and progress.
    let mut files: Vec<Published> = Vec::new();
    for table in TABLES {
        store.exec_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {table}({})",
            definition(table)
        ))?;
        let table_rows = rows.get(table);
        if table_rows.is_empty() {
            continue;
        }
        let payload =
            serde_json::to_string(table_rows).map_err(|e| StoreError::Check(e.to_string()))?;
        store.exec(&format!("INSERT INTO {table} SELECT unnest(json_transform(?::JSON, ?::JSON), recursive:=true)"), &[&payload, &type_map(table)])?;
        let epochs = store.rows(
            &format!("SELECT DISTINCT slot // 432000 AS epoch FROM {table}"),
            &[],
        )?;
        for item in epochs {
            let epoch = item.int("epoch").unwrap_or(0);
            let relative = format!("tables/{table}/epoch={epoch}/{batch:012}.parquet");
            let path = store.root.join(&relative);
            if let Some(parent) = path.parent() {
                create_dir_all_durable(parent)?;
            }
            let tmp = crate::fsutil::tmp_path(&path);
            store.exec_batch(&format!(
                "COPY (SELECT * FROM {table} WHERE slot // 432000={epoch}
          ORDER BY slot, signature) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
                sql_string(&tmp.to_string_lossy())
            ))?;
            sync_path(&tmp)?;
            std::fs::rename(&tmp, &path)?;
            if let Some(parent) = path.parent() {
                sync_path(parent)?;
            }
            let count = store.rows(
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    sql_string(&path.to_string_lossy())
                ),
                &[],
            )?;
            let expected = table_rows
                .iter()
                .filter(|row| row.int("slot").map(|s| s.div_euclid(432_000)) == Some(epoch))
                .count();
            if count.first().and_then(|row| row.int("n"))
                != Some(i64::try_from(expected).unwrap_or(i64::MAX))
            {
                return Err(StoreError::Check("decoded file row count mismatch".into()));
            }
            files.push(Published {
                path: relative,
                table,
                count: expected,
                hash: file_hash(&path)?,
            });
        }
    }
    store.transaction(|store| {
        for file in &files {
            let count = i64::try_from(file.count).unwrap_or(i64::MAX);
            store.exec("INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)", &[&file.path, &file.table, &count, &file.hash, &batch, &now()])?;
        }
        let mut seen: Vec<(String, String, String)> = Vec::new();
        for source in sources {
            let at = source.at.clone().unwrap_or_default();
            match &source.seen {
                Some(pairs) => seen.extend(pairs.iter().map(|(signature, hash)| (signature.clone(), hash.clone(), at.clone()))),
                None => seen.extend(rows
                    .get("decoded_transactions")
                    .iter()
                    .map(|row| (row.str("signature").unwrap_or("").to_owned(), row.str("source_hash").unwrap_or("").to_owned(), at.clone()))),
            }
        }
        if !seen.is_empty() {
            let keys = seen.iter().map(|(signature, _, _)| sql_string(signature)).collect::<Vec<_>>().join(",");
            let payload = Value::Array(
                seen.iter().map(|(signature, hash, at)| Value::Object([("signature".to_owned(), Value::String(signature.clone())), ("source_hash".to_owned(), Value::String(hash.clone())), ("publication_at".to_owned(), Value::String(at.clone()))].into_iter().collect())).collect(),
            )
            .to_string();
            store.exec_batch("CREATE OR REPLACE TEMP TABLE processed_batch(signature VARCHAR, source_hash VARCHAR, publication_at VARCHAR)")?;
            store.exec("INSERT INTO processed_batch SELECT value->>'signature', value->>'source_hash', value->>'publication_at' FROM json_each(?::JSON)", &[&payload])?;
            let update_keys = match known {
                Some(known) => seen
                    .iter()
                    .filter(|(signature, _, _)| known.contains(signature))
                    .map(|(signature, _, _)| sql_string(signature))
                    .collect::<Vec<_>>()
                    .join(","),
                None => keys.clone(),
            };
            if !update_keys.is_empty() {
                store.exec_batch(&format!(
                    "UPDATE processed SET source_hash=b.source_hash, publication_at=b.publication_at
          FROM processed_batch b WHERE processed.signature=b.signature AND processed.signature IN ({update_keys})
          AND coalesce(processed.publication_at,'')<=b.publication_at"
                ))?;
            }
            store.exec_batch(&format!(
                "INSERT INTO processed SELECT * FROM processed_batch
          WHERE signature NOT IN (SELECT signature FROM processed WHERE signature IN ({keys}))"
            ))?;
        }
        for source in sources {
            let offset = i64::try_from(source.offset).unwrap_or(i64::MAX);
            store.exec("INSERT OR REPLACE INTO sources VALUES (?, ?, ?)", &[&source.hash, &source.path, &offset])?;
        }
        store.exec("INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)", &[&"batch", &batch.to_string()])?;
        Ok(())
    })?;
    if !rows.is_empty() {
        write_catalog(store)?;
    }
    Ok(())
}

/// `catalog.json`: every registered file in batch order, durably written.
pub fn write_catalog(store: &mut Store) -> Result<(), StoreError> {
    let files = store.rows("SELECT * FROM files ORDER BY batch_id", &[])?;
    let catalog = Obj::new()
        .with("at", now())
        .with("schemaVersion", 1)
        .with("decoderVersion", VERSION)
        .with_rows("files", files)
        .with(
            "acceptance",
            "decoded events; source range validation/sealing and state reconstruction pending",
        );
    write_durable(
        &store.root.join("catalog.json"),
        format!("{}\n", catalog.to_json()).as_bytes(),
    )?;
    Ok(())
}

/// Remove `.tmp` files and unregistered Parquet, then rewrite the catalog.
pub fn recover(store: &mut Store) -> Result<(), StoreError> {
    let registered: std::collections::HashSet<std::path::PathBuf> = store
        .rows(
            "SELECT path FROM files UNION SELECT path FROM retired_files",
            &[],
        )?
        .iter()
        .filter_map(|row| row.str("path"))
        .map(|p| store.root.join(p))
        .collect();
    fn walk(
        root: &Path,
        registered: &std::collections::HashSet<std::path::PathBuf>,
    ) -> std::io::Result<()> {
        for entry in std::fs::read_dir(root)? {
            let path = entry?.path();
            if path.is_dir() {
                walk(&path, registered)?;
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.ends_with(".tmp")
                    || (name.ends_with(".parquet") && !registered.contains(&path))
                {
                    std::fs::remove_file(&path)?;
                }
            }
        }
        Ok(())
    }
    walk(&store.root, &registered)?;
    write_catalog(store)
}
