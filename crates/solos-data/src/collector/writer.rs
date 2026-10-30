//! Raw publication: Parquet per table and epoch into `staging/`, registered with the range,
//! bounds and the lane checkpoint in one transaction. A port of `writer.ts` and
//! `storage-space.ts`.

use crate::fsutil::{
    create_dir_all_durable, file_hash, manifest_path, sync_dir_after_rename, sync_tmp_file,
    tmp_path,
};
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
    let registered: std::collections::HashSet<std::path::PathBuf> = store
        .rows("SELECT path FROM files", &[])?
        .iter()
        .filter_map(|r| r.str("path"))
        .map(|path| manifest_path(root, Path::new(path)))
        .collect();
    fn visit(
        root: &Path,
        directory: &Path,
        registered: &std::collections::HashSet<std::path::PathBuf>,
    ) -> std::io::Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(root, &path, registered)?;
            } else {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.ends_with(".tmp")
                    || (name.ends_with(".parquet")
                        && !registered.contains(&manifest_path(root, &path)))
                {
                    std::fs::remove_file(&path)?;
                }
            }
        }
        Ok(())
    }
    visit(root, root, &registered)?;
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

/// One publication progress point (issue #61): a step boundary the random-kill fixture prints
/// immediately before running the step, so a seeded byte offset plus a seeded jitter can land the
/// SIGKILL inside that step's window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The step about to run.
    pub stage: ProgressStage,
    /// Raw table being published (empty for the range-level commit step).
    pub table: &'static str,
    /// Epoch of the file being published (zero for the range-level commit step).
    pub epoch: i64,
}

/// The publication steps the random-kill fixture can kill inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressStage {
    /// The Parquet `COPY` into the `.tmp` file.
    TmpWrite,
    /// `sync_tmp_file` (fsync of the `.tmp` file).
    TmpFsync,
    /// The rename into the final path.
    Rename,
    /// `sync_dir_after_rename` (fsync of the epoch directory).
    DirFsync,
    /// The readback range check and hash.
    Readback,
    /// The watermark transaction (file rows, bounds, range and `W`).
    Commit,
}

impl ProgressStage {
    /// The label `dev publish-fixture` prints on the progress line.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            ProgressStage::TmpWrite => "tmp-write",
            ProgressStage::TmpFsync => "tmp-fsync",
            ProgressStage::Rename => "rename",
            ProgressStage::DirFsync => "dir-fsync",
            ProgressStage::Readback => "readback",
            ProgressStage::Commit => "commit",
        }
    }
}

/// Publish `[from, to]` of every raw table and register the files, range and checkpoint.
pub fn publish_range(
    store: &mut Store,
    root: &Path,
    from: i64,
    to: i64,
    checkpoint: Option<Checkpoint>,
) -> Result<(), StoreError> {
    publish_range_with(store, root, from, to, checkpoint, &mut |_| {})
}

/// [`publish_range`] with a progress callback, invoked immediately before each publication step
/// (ADR-0016 steps 3-7). The random-kill fixture (`dev publish-fixture`, issue #61) prints one
/// line per step so the harness can kill the process at a seeded byte offset of that output; a
/// seeded jitter then lands the kill inside the step's window instead of at its boundary.
pub fn publish_range_with(
    store: &mut Store,
    root: &Path,
    from: i64,
    to: i64,
    checkpoint: Option<Checkpoint>,
    mut on_progress: impl FnMut(Progress),
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
        let groups = store.rows(&format!("SELECT slot // 432000 AS epoch, count(*) AS n FROM {table} WHERE slot BETWEEN ? AND ? GROUP BY epoch ORDER BY epoch"), &[&from, &to])?;
        for item in groups {
            let epoch = item.int("epoch").unwrap_or(0);
            let count = item.int("n").unwrap_or(0);
            let directory = root
                .join("staging")
                .join(table)
                .join(format!("epoch={epoch}"));
            create_dir_all_durable(&directory)?;
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
            on_progress(Progress {
                stage: ProgressStage::TmpWrite,
                table,
                epoch,
            });
            store.exec_batch(&format!(
                "COPY ({query}) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
                sql_string(&tmp.to_string_lossy())
            ))?;
            crate::crash_point!("publish/after-tmp-write");
            on_progress(Progress {
                stage: ProgressStage::TmpFsync,
                table,
                epoch,
            });
            sync_tmp_file(&tmp)?;
            crate::crash_point!("publish/after-file-fsync");
            on_progress(Progress {
                stage: ProgressStage::Rename,
                table,
                epoch,
            });
            std::fs::rename(&tmp, &path)?;
            crate::crash_point!("publish/before-dir-fsync");
            on_progress(Progress {
                stage: ProgressStage::DirFsync,
                table,
                epoch,
            });
            sync_dir_after_rename(&directory)?;
            crate::crash_point!("publish/after-dir-fsync");
            on_progress(Progress {
                stage: ProgressStage::Readback,
                table,
                epoch,
            });
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
            crate::crash_point!("publish/after-range-check");
            files.push(Published {
                path: path.to_string_lossy().into_owned(),
                table,
                epoch,
                count,
                hash: file_hash(&path)?,
            });
        }
    }
    on_progress(Progress {
        stage: ProgressStage::Commit,
        table: "",
        epoch: 0,
    });
    crate::crash_point!("publish/before-watermark-commit");
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
        crate::crash_point!("publish/before-watermark-write");
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
