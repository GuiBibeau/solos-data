//! Raw publication: Parquet per table and epoch into `staging/`, registered with the range,
//! bounds and the lane checkpoint in one transaction. A port of `writer.ts` and
//! `storage-space.ts`.

use crate::fsutil::{file_hash, sync_path, tmp_path};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::path::Path;

/// Raw tables in publication order.
pub const TABLES: [&str; 5] = [
    "signatures",
    "transactions",
    "slot_order",
    "program_versions",
    "rpc_pages",
];

/// Keep room for DuckDB recovery; a full disk must not turn into silent data loss.
pub fn require_storage_space(root: &Path) -> Result<(), StoreError> {
    let stats = nix::sys::statvfs::statvfs(root).map_err(|e| StoreError::Check(e.to_string()))?;
    let available = u128::from(stats.blocks_available()) * u128::from(stats.fragment_size());
    if available < 20 * 1024u128.pow(3) {
        return Err(StoreError::Check(
            "Storage below 20 GiB reserve; collection paused".into(),
        ));
    }
    Ok(())
}

/// Remove `.tmp` files and unregistered Parquet under `root`.
pub fn recover_files(store: &mut Store, root: &Path) -> Result<(), StoreError> {
    let registered: std::collections::HashSet<String> = store
        .rows("SELECT path FROM files", &[])?
        .iter()
        .filter_map(|r| r.str("path").map(str::to_owned))
        .collect();
    fn visit(
        directory: &Path,
        registered: &std::collections::HashSet<String>,
    ) -> std::io::Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(&path, registered)?;
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.ends_with(".tmp")
                    || (name.ends_with(".parquet")
                        && !registered.contains(&path.to_string_lossy().into_owned()))
                {
                    std::fs::remove_file(&path)?;
                }
            }
        }
        Ok(())
    }
    visit(root, &registered)?;
    Ok(())
}

/// The lane checkpoint written with a publication.
pub struct Checkpoint {
    /// `kv` key.
    pub key: String,
    /// `kv` value.
    pub value: Value,
    /// Cycle to mark finished, if any.
    pub cycle_id: Option<String>,
}

/// Publish `[from, to]` of every raw table and register the files, range and checkpoint.
pub fn publish_range(
    store: &mut Store,
    root: &Path,
    from: i64,
    to: i64,
    checkpoint: Option<Checkpoint>,
) -> Result<(), StoreError> {
    require_storage_space(root)?;
    struct Published {
        path: String,
        table: &'static str,
        epoch: i64,
        count: i64,
        hash: String,
    }
    let mut files: Vec<Published> = Vec::new();
    for table in TABLES {
        let groups = store.rows(&format!("SELECT slot // 432000 AS epoch, count(*) AS n FROM {table} WHERE slot BETWEEN ? AND ? GROUP BY epoch"), &[&from, &to])?;
        for item in groups {
            let epoch = item.int("epoch").unwrap_or(0);
            let count = item.int("n").unwrap_or(0);
            let directory = root
                .join("staging")
                .join(table)
                .join(format!("epoch={epoch}"));
            std::fs::create_dir_all(&directory)?;
            let path = directory.join(format!("{from}-{to}-{}.parquet", uuid::Uuid::new_v4()));
            let projection = if table == "rpc_pages" {
                "page_id AS signature, * EXCLUDE(page_id)"
            } else {
                "*"
            };
            let query = format!(
                "SELECT {projection} FROM {table} WHERE slot BETWEEN {from} AND {to} AND slot // 432000 = {epoch} ORDER BY slot, signature"
            );
            let tmp = tmp_path(&path);
            store.exec_batch(&format!(
                "COPY ({query}) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
                sql_string(&tmp.to_string_lossy())
            ))?;
            sync_path(&tmp)?;
            std::fs::rename(&tmp, &path)?;
            sync_path(&directory)?;
            let actual = store.rows(
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    sql_string(&path.to_string_lossy())
                ),
                &[],
            )?;
            if actual.first().and_then(|r| r.int("n")) != Some(count) {
                return Err(StoreError::Check("V7: exported row count mismatch".into()));
            }
            files.push(Published {
                path: path.to_string_lossy().into_owned(),
                table,
                epoch,
                count,
                hash: file_hash(&path)?,
            });
        }
    }
    store.transaction(|store| {
        for file in &files {
            store.exec(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)",
                &[
                    &file.path,
                    &file.table,
                    &file.epoch,
                    &file.count,
                    &file.hash,
                    &now(),
                    &"active",
                ],
            )?;
        }
        for file in &files {
            store.exec(
                "INSERT INTO file_bounds VALUES (?, ?, ?)",
                &[
                    &file.path,
                    &from.max(file.epoch * 432_000),
                    &to.min((file.epoch + 1) * 432_000 - 1),
                ],
            )?;
        }
        store.exec(
            "INSERT OR IGNORE INTO published_ranges VALUES (?, ?)",
            &[&from, &to],
        )?;
        if let Some(checkpoint) = &checkpoint {
            store.exec(
                "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
                &[&checkpoint.key, &checkpoint.value.to_string()],
            )?;
            if let Some(cycle) = &checkpoint.cycle_id {
                store.exec(
                    "UPDATE cycles SET finished_at=?, status='ok' WHERE cycle_id=?",
                    &[&now(), cycle],
                )?;
            }
        }
        Ok(())
    })
}

/// Hash and count every active file.
pub fn verify_files(store: &mut Store) -> Result<Obj, StoreError> {
    let files = store.rows("SELECT * FROM files WHERE status=?", &[&"active"])?;
    for file in &files {
        let path = Path::new(file.str("path").unwrap_or(""));
        if file_hash(path)? != file.str("sha256").unwrap_or("") {
            return Err(StoreError::Check("V7: file hash mismatch".into()));
        }
        let count = store.rows(
            &format!(
                "SELECT count(*) AS n FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )?;
        if count.first().and_then(|r| r.int("n")) != file.int("row_count") {
            return Err(StoreError::Check("V7: file count mismatch".into()));
        }
    }
    Ok(Obj::new().with("files", files.len()).with("ok", true))
}
