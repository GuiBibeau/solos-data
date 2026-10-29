//! Decoded compaction: merge one contiguous run of files per table and epoch into one file,
//! keeping revision precedence, and retire the inputs for the garbage collector. A port of
//! `decode/compactor.ts` whose selection rule changed on 2026-10-08 (ADR-0006): the port merged
//! only the newest prefix of a group and needed 32 files inside 500k rows, which an `events`
//! file of 20–50k rows can never satisfy, so the table never compacted.

use super::keys::assert_relation_keys_unique;
use super::parquet::copy_statement;
use super::publish::write_catalog;
use super::schema::{key_column, sort_columns};
use crate::fsutil::{file_hash, sync_path, tmp_path};
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Files the run at the head of a group needs before it merges. New batches land there, so
/// waiting bounds how often the growing head file is rewritten.
pub const HEAD_MIN_FILES: usize = 32;
/// Files a run below the head needs. Nothing new ever lands below the head, so a stranded pair
/// is worth merging.
pub const INNER_MIN_FILES: usize = 2;
/// Row bound of one merge.
pub const MAX_ROWS: i64 = 4_000_000;
/// Compressed input bound of one merge.
pub const MAX_BYTES: u64 = 512 * 1024 * 1024;

/// One registered file with its size on disk.
#[derive(Clone, Debug)]
pub struct Candidate {
    /// The `files` row.
    pub file: Obj,
    /// Compressed bytes on disk.
    pub bytes: u64,
    /// Registered row count.
    pub rows: i64,
}

/// The run to merge in one group whose files are ordered newest first, or `None`.
///
/// Files are walked from the newest. A run grows while it stays inside [`MAX_ROWS`] and
/// [`MAX_BYTES`]; the file that would overflow it closes the run. A closed run merges when it has
/// `head_minimum` files at the head of the group ([`HEAD_MIN_FILES`] while batches still land
/// there, [`INNER_MIN_FILES`] once the group is settled) or [`INNER_MIN_FILES`] below it;
/// otherwise the walk continues from the closing file. Runs are contiguous in batch order and a merged file keeps
/// the run's highest batch id, so no older row is ever promoted above an excluded newer revision.
#[must_use]
pub fn select_run(files: Vec<Candidate>, head_minimum: usize) -> Option<Vec<Candidate>> {
    let mut run: Vec<Candidate> = Vec::new();
    let (mut bytes, mut rows) = (0u64, 0i64);
    let mut head = true;
    for file in files {
        if bytes + file.bytes > MAX_BYTES || rows + file.rows > MAX_ROWS {
            if run.len() >= minimum(head, head_minimum) {
                return Some(run);
            }
            head = false;
            run.clear();
            (bytes, rows) = (0, 0);
            if file.bytes > MAX_BYTES || file.rows > MAX_ROWS {
                continue;
            }
        }
        bytes += file.bytes;
        rows += file.rows;
        run.push(file);
    }
    (run.len() >= minimum(head, head_minimum)).then_some(run)
}

fn minimum(head: bool, head_minimum: usize) -> usize {
    if head {
        head_minimum.max(INNER_MIN_FILES)
    } else {
        INNER_MIN_FILES
    }
}

/// How one maintenance pass compacts.
#[derive(Clone, Copy, Debug)]
pub struct CompactionPolicy {
    /// Wall time during which the pass keeps starting merges; a running merge finishes.
    pub budget: Duration,
    /// A group whose newest batch is at least this many batches behind the decoder's latest is
    /// settled: no batch lands at its head any more (a backfill moved past its epoch), so its
    /// head run merges with [`INNER_MIN_FILES`] like any other. `None` never settles a group.
    pub settled_batches: Option<i64>,
}

/// Merge at most one run per `(table, epoch)` group, largest groups first, until `budget` is
/// spent; a merge that is running finishes. Returns `merges`, `mergedFiles` and `deferred`
/// (groups not examined this pass). No group is ever settled.
pub fn compact_decoded(store: &mut Store, budget: Duration) -> Result<Obj, StoreError> {
    compact_decoded_with(
        store,
        CompactionPolicy {
            budget,
            settled_batches: None,
        },
    )
}

/// [`compact_decoded`] under `policy`; also reports `settledGroups` (groups merged under the
/// settled rule this pass).
pub fn compact_decoded_with(
    store: &mut Store,
    policy: CompactionPolicy,
) -> Result<Obj, StoreError> {
    let started = Instant::now();
    let latest = store.get("batch")?.map_or(0, |v| {
        v.as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| v.as_i64())
            .unwrap_or(0)
    });
    // One read of the registrations, grouped here, instead of one query per group.
    let mut groups: BTreeMap<(String, String), Vec<Obj>> = BTreeMap::new();
    for file in store.rows(
        "SELECT *,regexp_extract(path,'epoch=([0-9]+)',1) AS epoch FROM files
    WHERE regexp_matches(path,'epoch=[0-9]+') ORDER BY batch_id DESC,path DESC",
        &[],
    )? {
        let key = (
            file.str("table_name").unwrap_or("").to_owned(),
            file.str("epoch").unwrap_or("").to_owned(),
        );
        groups.entry(key).or_default().push(file);
    }
    let mut ordered: Vec<((String, String), Vec<Obj>)> = groups
        .into_iter()
        .filter(|(_, files)| files.len() >= INNER_MIN_FILES)
        .collect();
    // Largest groups first, then table and epoch, as the per-group query ordered them.
    ordered.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));
    let (mut merges, mut merged_files, mut deferred, mut settled) = (0u64, 0u64, 0u64, 0u64);
    for ((table, epoch), input) in ordered {
        if started.elapsed() >= policy.budget {
            deferred += 1;
            continue;
        }
        let newest = input
            .iter()
            .filter_map(|f| f.int("batch_id"))
            .max()
            .unwrap_or(0);
        let is_settled = policy
            .settled_batches
            .is_some_and(|gap| latest - newest >= gap);
        let head_minimum = if is_settled {
            INNER_MIN_FILES
        } else {
            HEAD_MIN_FILES
        };
        let mut candidates = Vec::with_capacity(input.len());
        for file in input {
            let path = store.root.join(file.str("path").unwrap_or(""));
            let bytes = std::fs::metadata(&path)?.len();
            let rows = file.int("row_count").unwrap_or(0);
            candidates.push(Candidate { file, bytes, rows });
        }
        let Some(run) = select_run(candidates, head_minimum) else {
            continue;
        };
        let files: Vec<Obj> = run.into_iter().map(|c| c.file).collect();
        merge(store, &table, &epoch, &files)?;
        merges += 1;
        merged_files += files.len() as u64;
        settled += u64::from(is_settled);
    }
    if merges > 0 {
        write_catalog(store)?;
    }
    Ok(Obj::new()
        .with("merges", merges)
        .with("mergedFiles", merged_files)
        .with("settledGroups", settled)
        .with("deferred", deferred))
}

/// Write one file holding the latest revision of every key in `files`, register it with the
/// run's highest batch id and retire the inputs.
fn merge(store: &mut Store, table: &str, epoch: &str, files: &[Obj]) -> Result<(), StoreError> {
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
    let key = key_column(table);
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
    let order = sort_columns(table);
    assert_relation_keys_unique(store, table, &format!("({query}) AS keyed"))?;
    store.exec_batch(&copy_statement(&format!("{query} ORDER BY {order}"), &tmp))?;
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
        for file in files {
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(batch: i64, rows: i64) -> Candidate {
        Candidate {
            file: Obj::new().with("batch_id", batch),
            bytes: 1024,
            rows,
        }
    }

    fn batches(run: &[Candidate]) -> Vec<i64> {
        run.iter().filter_map(|c| c.file.int("batch_id")).collect()
    }

    #[test]
    fn a_short_head_waits_but_a_stranded_run_below_an_oversized_file_merges() {
        // Newest first: five fresh files, a file at the bound, then a long stranded run.
        let mut files: Vec<Candidate> = (0..5).map(|i| candidate(100 - i, 10_000)).collect();
        files.push(candidate(90, MAX_ROWS));
        files.extend((0..20).map(|i| candidate(80 - i, 30_000)));
        let run = select_run(files, HEAD_MIN_FILES).expect("the stranded run merges");
        assert_eq!(batches(&run), (61..=80).rev().collect::<Vec<_>>());
    }

    #[test]
    fn the_head_merges_once_it_has_enough_files_and_stops_at_the_bound() {
        let files: Vec<Candidate> = (0..200).map(|i| candidate(1000 - i, 30_000)).collect();
        let run = select_run(files, HEAD_MIN_FILES).expect("a full head run merges");
        assert_eq!(run.len(), 133);
        assert_eq!(batches(&run)[0], 1000);
        let short: Vec<Candidate> = (0..HEAD_MIN_FILES - 1)
            .map(|i| candidate(100 - i as i64, 1_000))
            .collect();
        assert!(select_run(short.clone(), HEAD_MIN_FILES).is_none());
        // A settled group no longer waits: its short head run merges.
        assert_eq!(
            select_run(short, INNER_MIN_FILES).map(|r| r.len()),
            Some(HEAD_MIN_FILES - 1)
        );
    }

    #[test]
    fn a_lone_full_file_never_merges_with_itself() {
        let files = vec![candidate(10, MAX_ROWS), candidate(9, MAX_ROWS - 1)];
        assert!(select_run(files, HEAD_MIN_FILES).is_none());
        let files = vec![
            candidate(10, MAX_ROWS),
            candidate(9, 100),
            candidate(8, 100),
        ];
        let run = select_run(files, HEAD_MIN_FILES).expect("the two small files below merge");
        assert_eq!(batches(&run), vec![9, 8]);
    }
}
