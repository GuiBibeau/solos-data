//! Decoded compaction: merge a newest prefix of small files per table and epoch into one, keeping
//! revision precedence, and retire the inputs for the garbage collector. A port of
//! `decode/compactor.ts`.

use super::publish::write_catalog;
use super::schema::key_column;
use crate::fsutil::{file_hash, sync_path, tmp_path};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;

/// Merge newest prefixes of at least 32 files, bounded to 128 MiB and 500k rows per merge.
pub fn compact_decoded(store: &mut Store) -> Result<Obj, StoreError> {
    let groups = store.rows(
        "SELECT table_name,regexp_extract(path,'epoch=([0-9]+)',1) AS epoch
    FROM files WHERE regexp_matches(path,'epoch=[0-9]+') GROUP BY table_name,epoch HAVING count(*)>=32",
        &[],
    )?;
    let mut merges = 0u64;
    for group in groups {
        let table = group.str("table_name").unwrap_or("").to_owned();
        let epoch = group.str("epoch").unwrap_or("").to_owned();
        let input = store.rows(
            "SELECT * FROM files WHERE table_name=?
      AND regexp_extract(path,'epoch=([0-9]+)',1)=? ORDER BY batch_id DESC,path DESC",
            &[&table, &epoch],
        )?;
        let mut files: Vec<Obj> = Vec::new();
        let (mut bytes, mut rows) = (0u64, 0i64);
        for file in input {
            let path = store.root.join(file.str("path").unwrap_or(""));
            let size = std::fs::metadata(&path)?.len();
            let count = file.int("row_count").unwrap_or(0);
            if bytes + size > 128 * 1024 * 1024 || rows + count > 500_000 {
                break;
            }
            bytes += size;
            rows += count;
            files.push(file);
        }
        if files.len() < 32 {
            continue;
        }
        let paths = files
            .iter()
            .map(|f| {
                sql_string(
                    &store
                        .root
                        .join(f.str("path").unwrap_or(""))
                        .to_string_lossy(),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let registrations = Value::Array(
            files
                .iter()
                .map(|f| {
                    Value::Object(
                        [
                            (
                                "path".to_owned(),
                                Value::String(
                                    store
                                        .root
                                        .join(f.str("path").unwrap_or(""))
                                        .to_string_lossy()
                                        .into_owned(),
                                ),
                            ),
                            (
                                "batch".to_owned(),
                                Value::from(f.int("batch_id").unwrap_or(0)),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect(),
        )
        .to_string();
        let key = key_column(&table);
        let query = format!(
            "SELECT p.* EXCLUDE(filename,epoch) FROM read_parquet([{paths}],filename=true,union_by_name=true) p
      JOIN (SELECT value->>'path' AS path,(value->>'batch')::BIGINT AS batch FROM json_each({}::JSON)) r
      ON r.path=p.filename QUALIFY row_number() OVER(PARTITION BY p.{key} ORDER BY r.batch DESC,p.filename DESC)=1",
            sql_string(&registrations)
        );
        let relative = format!(
            "tables/{table}/epoch={epoch}/compact-{}.parquet",
            uuid::Uuid::new_v4()
        );
        let path = store.root.join(&relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = tmp_path(&path);
        store.exec_batch(&format!(
            "COPY ({query} ORDER BY slot,signature) TO {} (FORMAT PARQUET,COMPRESSION ZSTD)",
            sql_string(&tmp.to_string_lossy())
        ))?;
        sync_path(&tmp)?;
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            sync_path(parent)?;
        }
        let n = store
            .rows(
                &format!(
                    "SELECT count(*) AS n FROM read_parquet({})",
                    sql_string(&path.to_string_lossy())
                ),
                &[],
            )?
            .first()
            .and_then(|r| r.int("n"))
            .unwrap_or(-1);
        let expected = store
            .rows(&format!("SELECT count(*) AS n FROM ({query})"), &[])?
            .first()
            .and_then(|r| r.int("n"))
            .unwrap_or(-2);
        if n != expected {
            return Err(StoreError::Check(
                "decoded compaction row count mismatch".into(),
            ));
        }
        let hash = file_hash(&path)?;
        let max_batch = files
            .iter()
            .filter_map(|f| f.int("batch_id"))
            .max()
            .unwrap_or(0);
        store.transaction(|store| {
            for file in &files {
                let old = file.str("path").unwrap_or("");
                store.exec("DELETE FROM files WHERE path=?", &[&old])?;
                store.exec(
                    "INSERT INTO retired_files VALUES (?, ?, ?)",
                    &[&old, &relative, &now()],
                )?;
            }
            store.exec(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)",
                &[&relative, &table, &n, &hash, &max_batch, &now()],
            )?;
            Ok(())
        })?;
        merges += 1;
    }
    if merges > 0 {
        write_catalog(store)?;
    }
    Ok(Obj::new().with("merges", merges))
}
