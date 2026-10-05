//! Raw compaction: merge a newest prefix of at least ten files per table and epoch, bounded to
//! 250k rows and 256 MiB, keeping revision precedence. A port of `compactor.ts`.

use super::catalog::write_catalog;
use crate::fsutil::{file_hash, sync_path, tmp_path};
use crate::jsonout::now;
use crate::store::{Store, StoreError, sql_string};
use std::path::Path;

/// Consolidate registered revisions; the newest publication wins per signature.
pub fn compact(store: &mut Store, root: &Path) -> Result<(), StoreError> {
    let groups = store.rows("SELECT table_name, epoch FROM files WHERE status='active' GROUP BY table_name, epoch HAVING count(*)>=10", &[])?;
    for group in groups {
        let table = group.str("table_name").unwrap_or("").to_owned();
        let epoch = group.int("epoch").unwrap_or(0);
        let candidates = store.rows("SELECT path, row_count,sha256 FROM files WHERE status='active' AND table_name=? AND epoch=? ORDER BY created_at DESC, path DESC", &[&table, &epoch])?;
        let mut paths: Vec<String> = Vec::new();
        let mut inputs = Vec::new();
        let (mut rows, mut bytes) = (0i64, 0u64);
        // Only a newest prefix is safe: assigning a new publication time must not promote an old
        // revision above a newer file that was excluded from this merge.
        for file in &candidates {
            let count = file.int("row_count").unwrap_or(0);
            if rows + count > 250_000 {
                break;
            }
            let path = file.str("path").unwrap_or("").to_owned();
            let size = std::fs::metadata(&path)?.len();
            if bytes + size > 256 * 1024 * 1024 {
                break;
            }
            bytes += size;
            rows += count;
            paths.push(path);
            inputs.push((file.str("sha256").unwrap_or("").to_owned(), count));
        }
        if paths.len() < 10 {
            continue;
        }
        let list = format!(
            "[{}]",
            paths
                .iter()
                .map(|p| sql_string(p))
                .collect::<Vec<_>>()
                .join(",")
        );
        let query = format!(
            "SELECT p.* EXCLUDE(filename) FROM read_parquet({list}, filename=true) p
      JOIN files f ON f.path=p.filename QUALIFY row_number() OVER
      (PARTITION BY p.signature ORDER BY f.created_at DESC, p.filename DESC)=1"
        );
        let directory = root
            .join(&table)
            .join(format!("epoch={epoch}"))
            .join("open");
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(format!("compact-{}.parquet", uuid::Uuid::new_v4()));
        let tmp = tmp_path(&path);
        store.exec_batch(&format!(
            "COPY ({query} ORDER BY slot, signature) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
            sql_string(&tmp.to_string_lossy())
        ))?;
        sync_path(&tmp)?;
        std::fs::rename(&tmp, &path)?;
        sync_path(&directory)?;
        let bounds = store.rows(&format!("SELECT count(*) AS n,min(slot) AS first,max(slot) AS last FROM read_parquet({})", sql_string(&path.to_string_lossy())), &[])?.into_iter().next().unwrap_or_default();
        let expected = store
            .rows(&format!("SELECT count(*) AS n FROM ({query})"), &[])?
            .into_iter()
            .next()
            .unwrap_or_default();
        let n = bounds.int("n").unwrap_or(-1);
        if n != expected.int("n").unwrap_or(-2) {
            return Err(StoreError::Check("V7: compaction mismatch".into()));
        }
        let hash = file_hash(&path)?;
        let path_text = path.to_string_lossy().into_owned();
        let (first, last) = (
            bounds.int("first").unwrap_or(0),
            bounds.int("last").unwrap_or(0),
        );
        store.transaction(|store| {
            for (old, (sha, count)) in paths.iter().zip(&inputs) {
                store.exec("UPDATE files SET status='superseded' WHERE path=?", &[old])?;
                store.exec(
                    "INSERT INTO retired_files VALUES (?, ?, ?)",
                    &[old, &path_text, &now()],
                )?;
                store.exec(
                    "INSERT INTO compaction_inputs VALUES (?, ?, ?)",
                    &[&path_text, sha, count],
                )?;
            }
            store.exec(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)",
                &[&path_text, &table, &epoch, &n, &hash, &now(), &"active"],
            )?;
            store.exec(
                "INSERT INTO file_bounds VALUES (?, ?, ?)",
                &[&path_text, &first, &last],
            )?;
            Ok(())
        })?;
    }
    write_catalog(store, root)?;
    Ok(())
}
