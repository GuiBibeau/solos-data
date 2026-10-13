//! Independent re-verification of published ranges (ADR-0008, 2026-10-13): re-walk the program
//! and ProgramData manifests over a slot range into a separate output root, then compare each
//! published range's raw transactions, read from the Parquet catalog under a reader lease, with
//! that manifest. The live checkpoint is never opened, and requests go through a limiter of their
//! own so the running collector keeps its headroom.

use super::config::{Config, Lane};
use super::reader::range_files;
use super::rpc::{Rpc, call_value};
use super::walker::{Walk, make_walk, walk_page};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::lease::with_read_lease;
use crate::store::{Store, StoreError, memory_connection, query_rows, sql_string};
use duckdb::Connection;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What `collector reverify` was asked to do.
#[derive(Clone, Debug)]
pub struct Options {
    /// Lowest slot.
    pub from: i64,
    /// Highest slot.
    pub to: i64,
    /// Output root for the walked manifests and reports; never inside the raw root.
    pub out: PathBuf,
    /// Request ceiling, `getSignaturesForAddress` calls per second.
    pub rps: f64,
    /// Ranges walked at once.
    pub parallel: usize,
    /// DuckDB memory limit of the comparison connection.
    pub memory: String,
    /// DuckDB threads of the comparison connection.
    pub threads: String,
}

/// Slots above a range searched for the published transactions a walk starts from.
const ANCHOR_WINDOW: i64 = 1_000;
/// Mismatching signatures listed inline per range; the full lists go to CSV files.
const SAMPLE: i64 = 20;
/// The walk state key inside each output store.
const WALK_KEY: &str = "walk/reverify";

/// The published ranges that intersect `[from, to]`, from the `<from>-<to>-<id>.parquet` names
/// of the catalog's transaction files, sorted. Slots no ranged file names but the catalog's
/// published `coverage` holds (their files were compacted into unranged ones) become a range of
/// their own. Errors when a slot of `[from, to]` is not published at all, since the comparison
/// would then miss it.
pub fn published_ranges(
    files: &[Value],
    coverage: &[(i64, i64)],
    from: i64,
    to: i64,
) -> Result<Vec<(i64, i64)>, StoreError> {
    let mut named: Vec<(i64, i64)> = files
        .iter()
        .filter(|f| f.get("table_name").and_then(Value::as_str) == Some("transactions"))
        .filter_map(|f| f.get("path").and_then(Value::as_str))
        .filter_map(super::reader::path_range)
        .filter(|(a, b)| *a <= to && *b >= from)
        .collect();
    named.sort_unstable();
    named.dedup();
    let covered = |a: i64, b: i64| coverage.iter().any(|(x, y)| *x <= a && b <= *y);
    let mut ranges = Vec::new();
    let mut expected = from;
    for (a, b) in named {
        if expected > to {
            break;
        }
        // Tail cycles republish `overlapSlots` below the watermark, so ranged files overlap;
        // each slot goes to one range and the newest publication per signature wins anyway.
        if b < expected {
            continue;
        }
        if a > expected {
            if !covered(expected, a - 1) {
                return Err(StoreError::Check(format!(
                    "slots {expected} to {} are not published",
                    a - 1
                )));
            }
            ranges.push((expected, a - 1));
        }
        // The first range keeps its published start; later ones start after their predecessor.
        ranges.push((
            if ranges.is_empty() {
                a
            } else {
                a.max(expected)
            },
            b,
        ));
        expected = b + 1;
    }
    if expected <= to {
        if !covered(expected, to) {
            return Err(StoreError::Check(format!(
                "slots {expected} to {to} are not published"
            )));
        }
        ranges.push((expected, to));
    }
    Ok(ranges)
}

/// The catalog's published coverage as `(from, to)` pairs.
#[must_use]
pub fn catalog_coverage(catalog: &Value) -> Vec<(i64, i64)> {
    let int = |v: &Value, k: &str| {
        v.get(k).and_then(|x| {
            x.as_i64()
                .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
        })
    };
    catalog
        .get("coverage")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|c| Some((int(c, "from")?, int(c, "to")?)))
                .collect()
        })
        .unwrap_or_default()
}

/// The cursor a walk over a range starts from: the oldest published transaction above it (lowest
/// slot, then lowest index), with the next oldest as the fallback cursor, so a cursor the
/// provider cannot resolve is confirmed from the row above it like any other walk (ADR-0008).
/// Rows the ProgramData walk listed are skipped: the archive keeps transactions that touch only
/// ProgramData, which the program's own listing cannot resolve as a cursor.
#[must_use]
pub fn start_cursor(
    rows: &[Obj],
    programdata: &std::collections::HashSet<String>,
) -> Option<(String, i64, Option<String>)> {
    let mut keyed: Vec<(i64, i64, String)> = rows
        .iter()
        .filter(|row| {
            row.str("signature")
                .is_none_or(|s| !programdata.contains(s))
        })
        .filter_map(|row| {
            Some((
                row.int("slot")?,
                row.int("tx_index").unwrap_or(-1),
                row.str("signature")?.to_owned(),
            ))
        })
        .collect();
    keyed.sort_unstable();
    let mut keyed = keyed.into_iter();
    let (slot, _, signature) = keyed.next()?;
    Some((signature, slot, keyed.next().map(|(_, _, s)| s)))
}

/// Walk to the end, resuming from the stored state, logging every 250 pages.
pub async fn walk_segment(rpc: &dyn Rpc, db: &Db, initial: Walk) -> Result<Walk, StoreError> {
    let mut walk = match db.get(WALK_KEY).await? {
        Some(value) => {
            serde_json::from_value(value).map_err(|e| StoreError::Check(e.to_string()))?
        }
        None => initial,
    };
    while !walk.done {
        walk = walk_page(rpc, db, WALK_KEY, walk).await?;
        if walk.pages.is_multiple_of(250) {
            log(
                "reverify_walk_progress",
                Obj::new()
                    .with("address", walk.address.clone())
                    .with("floor", walk.floor)
                    .with("pages", walk.pages)
                    .with("cursorSlot", walk.last_slot),
            );
        }
    }
    Ok(walk)
}

/// Whether a finished walk listed everything down to its floor: its oldest row lies below the
/// floor; or it listed rows down into its window and then ended on an empty page the fallback
/// cursor confirmed (the address has no older history); or, started from the tip, the provider
/// listed nothing at all. A walk that ended on an unconfirmed empty page above the floor is the
/// 2026-10-10 failure, and a start cursor whose own page and fallback page are both empty (a
/// cursor the provider does not list for the address) never counts as a confirmed end.
#[must_use]
pub fn reached_floor(walk: &Walk) -> bool {
    let below = |bound: i64| walk.oldest_slot.is_some_and(|slot| slot < bound);
    let confirmed_end = walk.done
        && walk.fallback.is_some()
        && walk.oldest_slot.is_some_and(|slot| slot <= walk.ceiling);
    let empty_from_tip = walk.done && walk.before.is_none() && walk.oldest_slot.is_none();
    below(walk.floor) || confirmed_end || empty_from_tip
}

/// Compare the `manifest(signature, slot)` and `archive(signature, slot, tx_index,
/// single_in_slot)` tables of `conn`: V1 in both directions and V6 exactly as range validation
/// runs them, plus slot agreement and samples of every mismatch.
pub fn compare(conn: &Connection) -> Result<Obj, StoreError> {
    let counts = query_rows(
        conn,
        "SELECT (SELECT count(*) FROM manifest) AS manifest,
  (SELECT count(*) FROM archive) AS archive,
  (SELECT count(DISTINCT slot) FROM manifest) AS manifest_slots,
  (SELECT count(DISTINCT slot) FROM archive) AS archive_slots,
  (SELECT count(*) FROM manifest m WHERE NOT EXISTS (SELECT 1 FROM archive a WHERE a.signature=m.signature)) AS missing,
  (SELECT count(*) FROM archive a WHERE NOT EXISTS (SELECT 1 FROM manifest m WHERE m.signature=a.signature)) AS unlisted,
  (SELECT count(*) FROM manifest m JOIN archive a USING(signature) WHERE m.slot<>a.slot) AS slot_mismatch,
  (SELECT count(*) FROM manifest m JOIN archive a USING(signature)
    WHERE a.single_in_slot IS NULL OR (NOT a.single_in_slot AND a.tx_index IS NULL)) AS unordered",
        &[],
    )?
    .into_iter()
    .next()
    .unwrap_or_default();
    let bad_slots = "SELECT m.slot, count(*) AS n, count(a.tx_index) AS indexed,
    count(DISTINCT a.tx_index) AS distinct_index, count(*) FILTER (WHERE a.single_in_slot) AS singles
    FROM manifest m LEFT JOIN archive a USING(signature) GROUP BY m.slot
    HAVING (n>1 AND (indexed<>n OR distinct_index<>n OR singles>0)) OR (n=1 AND singles<>1)";
    let bad = query_rows(
        conn,
        &format!("SELECT count(*) AS bad FROM ({bad_slots})"),
        &[],
    )?
    .first()
    .and_then(|r| r.int("bad"))
    .unwrap_or(0);
    let int = |key: &str| counts.int(key).unwrap_or(0);
    let (missing, unlisted, mismatch, unordered) = (
        int("missing"),
        int("unlisted"),
        int("slot_mismatch"),
        int("unordered"),
    );
    let mut report = Obj::new()
        .with(
            "ok",
            missing == 0 && unlisted == 0 && mismatch == 0 && unordered == 0 && bad == 0,
        )
        .with("V1", missing == 0 && unlisted == 0)
        .with("V6", bad == 0 && unordered == 0)
        .with("manifest", int("manifest"))
        .with("archive", int("archive"))
        .with("manifestSlots", int("manifest_slots"))
        .with("archiveSlots", int("archive_slots"))
        .with("missing", missing)
        .with("unlisted", unlisted)
        .with("slotMismatch", mismatch)
        .with("unordered", unordered)
        .with("badOrderingSlots", bad);
    if missing > 0 {
        report.set(
            "missingSample",
            sample(conn, &format!("{MISSING} LIMIT {SAMPLE}"))?,
        );
    }
    if unlisted > 0 {
        report.set(
            "unlistedSample",
            sample(conn, &format!("{UNLISTED} LIMIT {SAMPLE}"))?,
        );
    }
    if bad > 0 {
        report.set(
            "badOrderingSample",
            sample(conn, &format!("{bad_slots} ORDER BY m.slot LIMIT {SAMPLE}"))?,
        );
    }
    Ok(report)
}

/// Rows as a plain JSON array.
fn sample(conn: &Connection, sql: &str) -> Result<Value, StoreError> {
    Ok(Value::Array(
        query_rows(conn, sql, &[])?
            .iter()
            .map(Obj::to_value)
            .collect(),
    ))
}

/// A stored report back as an ordered object.
fn obj_of(value: Value) -> Obj {
    let mut obj = Obj::new();
    if let Value::Object(map) = value {
        for (key, value) in map {
            obj.set(&key, value);
        }
    }
    obj
}

const MISSING: &str = "SELECT m.signature, m.slot FROM manifest m
  WHERE NOT EXISTS (SELECT 1 FROM archive a WHERE a.signature=m.signature) ORDER BY m.slot, m.signature";
const UNLISTED: &str = "SELECT a.signature, a.slot, a.tx_index FROM archive a
  WHERE NOT EXISTS (SELECT 1 FROM manifest m WHERE m.signature=a.signature) ORDER BY a.slot, a.signature";

/// A digest of the catalog entries a comparison read (path and content hash), so a stored report
/// is reused only while the published files behind it are the same.
#[must_use]
pub fn files_digest(files: &[Value]) -> String {
    use sha2::{Digest, Sha256};
    let field = |f: &Value, k: &str| f.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
    let mut lines: Vec<String> = files
        .iter()
        .map(|f| format!("{}\t{}\n", field(f, "path"), field(f, "sha256")))
        .collect();
    lines.sort_unstable();
    hex::encode(Sha256::digest(lines.concat().as_bytes()))
}

/// The newest publication per signature of the published transactions in `[from, to]`, into
/// `archive`, read from the files `range_files` selects; returns their [`files_digest`]. Call it
/// under a reader lease.
pub fn load_archive(
    conn: &Connection,
    root: &Path,
    from: i64,
    to: i64,
) -> Result<String, StoreError> {
    let files = range_files(root, "transactions", from, to)?;
    let digest = files_digest(&files);
    conn.execute_batch(
        "CREATE TABLE archive (signature VARCHAR, slot BIGINT, tx_index INTEGER, single_in_slot BOOLEAN)",
    )?;
    if files.is_empty() {
        return Ok(digest);
    }
    conn.execute(
        "CREATE TABLE registrations AS SELECT value->>'path' AS path, value->>'created_at' AS created_at FROM json_each(?::JSON)",
        [Value::Array(files.clone()).to_string()],
    )?;
    let paths: Vec<String> = files
        .iter()
        .filter_map(|f| f.get("path").and_then(Value::as_str))
        .map(sql_string)
        .collect();
    conn.execute_batch(&format!(
        "INSERT INTO archive SELECT signature, slot, tx_index, single_in_slot FROM (
  SELECT p.signature, p.slot, p.tx_index, p.single_in_slot, r.created_at, p.filename
  FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN registrations r ON r.path=p.filename
  WHERE p.slot BETWEEN {from} AND {to}) p
  QUALIFY row_number() OVER (PARTITION BY p.signature ORDER BY p.created_at DESC, p.filename DESC)=1",
        paths.join(",")
    ))?;
    Ok(digest)
}

/// One range: manifest from its walked store plus the ProgramData rows, archive from the
/// catalog, compared; full mismatch lists are written next to the store as CSV.
fn compare_range(
    raw: &Path,
    dir: &Path,
    programdata: &str,
    window: (i64, i64),
    options: &Options,
) -> Result<Obj, StoreError> {
    let (from, to) = window;
    let conn = memory_connection(&options.threads, &options.memory)?;
    let walked = sql_string(&dir.join("checkpoint.duckdb").to_string_lossy());
    conn.execute_batch(&format!(
        "ATTACH {walked} AS walked (READ_ONLY);
CREATE TABLE manifest AS SELECT signature, slot FROM walked.signatures WHERE slot BETWEEN {from} AND {to};
DETACH walked;"
    ))?;
    conn.execute(
        &format!(
            "INSERT INTO manifest SELECT value->>'signature', (value->>'slot')::BIGINT FROM json_each(?::JSON)
  WHERE (value->>'slot')::BIGINT BETWEEN {from} AND {to}
  AND value->>'signature' NOT IN (SELECT signature FROM manifest)"
        ),
        [programdata],
    )?;
    let digest = with_read_lease(raw, || load_archive(&conn, raw, from, to))?;
    let mut report = compare(&conn)?;
    report.set("archiveFiles", digest);
    for (name, sql, key) in [
        ("missing", MISSING, "missing"),
        ("unlisted", UNLISTED, "unlisted"),
    ] {
        if report.int(key).unwrap_or(0) > 0 {
            let path = sql_string(&dir.join(format!("{name}.csv")).to_string_lossy());
            conn.execute_batch(&format!("COPY ({sql}) TO {path} (HEADER)"))?;
        }
    }
    Ok(report)
}

/// The program's ProgramData address, from the program account.
async fn program_data(rpc: &dyn Rpc, program: &str) -> Result<String, StoreError> {
    let account = call_value(
        rpc,
        "getAccountInfo",
        json!([program, { "encoding": "jsonParsed", "commitment": "finalized" }]),
        Lane::Backfill,
    )
    .await
    .map_err(|e| StoreError::Check(e.to_string()))?;
    account
        .pointer("/value/data/parsed/info/programData")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| StoreError::Check("ProgramData address is missing".into()))
}

/// Walk `initial` into the store at `dir` and close it.
async fn walk_into(rpc: &dyn Rpc, dir: &Path, initial: Walk) -> Result<Walk, StoreError> {
    let store = Store::open(dir, super::schema::SCHEMA)?;
    let (db, thread) = Db::spawn(store);
    let walk = walk_segment(rpc, &db, initial).await;
    thread.join(db).close()?;
    walk
}

fn write_json(path: &Path, value: &Obj) -> Result<(), StoreError> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, value.to_json())?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Walk and compare one published range, or read its finished report.
async fn verify_range(
    rpc: Arc<dyn Rpc>,
    config: &Config,
    options: &Options,
    programdata: Arc<String>,
    programdata_signatures: Arc<std::collections::HashSet<String>>,
    compare_lock: Arc<tokio::sync::Mutex<()>>,
    range: (i64, i64),
) -> Result<Obj, StoreError> {
    let window = (range.0.max(options.from), range.1.min(options.to));
    let dir = options
        .out
        .join("ranges")
        .join(format!("{}-{}", window.0, window.1));
    let report_path = dir.join("report.json");
    let pd_digest = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(programdata.as_bytes()))
    };
    if let Ok(text) = std::fs::read_to_string(&report_path) {
        let value: Value =
            serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
        let current = with_read_lease(&config.data_dir, || {
            range_files(&config.data_dir, "transactions", window.0, window.1)
                .map(|files| files_digest(&files))
        })?;
        let same = |key: &str, digest: &str| value.get(key).and_then(Value::as_str) == Some(digest);
        if same("archiveFiles", &current) && same("programDataRows", &pd_digest) {
            return Ok(obj_of(value));
        }
        // The catalog or the ProgramData manifest changed since this report: compare again
        // (the finished walk is kept).
        log(
            "reverify_inputs_changed",
            Obj::new().with("from", window.0).with("to", window.1),
        );
    }
    let above = super::reader::range_rows(
        &config.data_dir,
        "transactions",
        "signature, slot, tx_index",
        range.1 + 1,
        range.1 + ANCHOR_WINDOW,
    )?;
    let mut initial = make_walk(
        &config.program_id,
        Lane::Backfill,
        "reverify",
        window.0,
        window.1,
    );
    let start = start_cursor(&above, &programdata_signatures);
    if let Some((signature, slot, fallback)) = &start {
        initial.before = Some(signature.clone());
        initial.last_slot = Some(*slot);
        initial.fallback.clone_from(fallback);
    }
    let walk = walk_into(rpc.as_ref(), &dir, initial).await?;
    let _guard = compare_lock.lock().await;
    let raw = config.data_dir.clone();
    let opts = options.clone();
    let compare_dir = dir.clone();
    let mut report = tokio::task::spawn_blocking(move || {
        compare_range(&raw, &compare_dir, &programdata, window, &opts)
    })
    .await
    .map_err(|e| StoreError::Check(e.to_string()))??;
    let reached = reached_floor(&walk);
    let ok = report.get("ok") == Some(&Value::Bool(true)) && reached;
    let mut out = Obj::new()
        .with("ok", ok)
        .with("range", json!([range.0, range.1]))
        .with("from", window.0)
        .with("to", window.1)
        .with("walkPages", walk.pages)
        .with("walkReachedFloor", reached)
        .with("startSlot", start.map(|(_, slot, _)| slot));
    report.set("ok", ok);
    report.set("programDataRows", pd_digest);
    out.extend(report);
    out.set("verifiedAt", now());
    write_json(&report_path, &out)?;
    log("reverify_range", out.clone());
    Ok(out)
}

/// Re-walk the manifest over `[from, to]` and compare every published range there.
pub async fn reverify(
    rpc: Arc<dyn Rpc>,
    config: &Config,
    options: &Options,
) -> Result<Obj, StoreError> {
    let raw = std::fs::canonicalize(&config.data_dir)?;
    std::fs::create_dir_all(&options.out)?;
    if std::fs::canonicalize(&options.out)?.starts_with(&raw) {
        return Err(StoreError::Check(
            "--out must lie outside the raw data root".into(),
        ));
    }
    let catalog: Value = with_read_lease(&config.data_dir, || {
        serde_json::from_str(&std::fs::read_to_string(
            config.data_dir.join("catalog.json"),
        )?)
        .map_err(|e| StoreError::Check(e.to_string()))
    })?;
    let files = catalog
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let ranges = published_ranges(
        &files,
        &catalog_coverage(&catalog),
        options.from,
        options.to,
    )?;
    // Stores and reports are keyed by the compared window, so a later run over other bounds
    // never reuses a walk or report that covered less.
    let pd_dir = options
        .out
        .join(format!("programdata-{}-{}", options.from, options.to));
    let address = program_data(rpc.as_ref(), &config.program_id).await?;
    let pd_walk = walk_into(
        rpc.as_ref(),
        &pd_dir,
        // No ceiling: ProgramData signatures above the request are what keeps the program walks'
        // start cursors on program transactions.
        make_walk(&address, Lane::Backfill, "reverify", options.from, i64::MAX),
    )
    .await?;
    let pd_store = Store::open(&pd_dir, super::schema::SCHEMA)?;
    let pd_rows = pd_store.rows("SELECT signature, slot FROM signatures ORDER BY slot", &[])?;
    pd_store.close()?;
    let programdata =
        Arc::new(Value::Array(pd_rows.iter().map(Obj::to_value).collect()).to_string());
    let pd_signatures: Arc<std::collections::HashSet<String>> = Arc::new(
        pd_rows
            .iter()
            .filter_map(|r| r.str("signature").map(str::to_owned))
            .collect(),
    );
    let pd_in_window = pd_rows
        .iter()
        .filter(|r| {
            r.int("slot")
                .is_some_and(|s| (options.from..=options.to).contains(&s))
        })
        .count();
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let results: Vec<Result<Obj, StoreError>> = futures_util::stream::iter(ranges.clone())
        .map(|range| {
            verify_range(
                Arc::clone(&rpc),
                config,
                options,
                Arc::clone(&programdata),
                Arc::clone(&pd_signatures),
                Arc::clone(&lock),
                range,
            )
        })
        .buffer_unordered(options.parallel.max(1))
        .collect()
        .await;
    let mut reports = Vec::new();
    for result in results {
        reports.push(result?);
    }
    reports.sort_by_key(|r| r.int("from"));
    let sum = |key: &str| reports.iter().map(|r| r.int(key).unwrap_or(0)).sum::<i64>();
    let failing: Vec<Value> = reports
        .iter()
        .filter(|r| r.get("ok") != Some(&Value::Bool(true)))
        .map(|r| r.get("range").cloned().unwrap_or(Value::Null))
        .collect();
    let pd_reached = reached_floor(&pd_walk);
    let summary = Obj::new()
        .with("ok", failing.is_empty() && pd_reached)
        .with("from", options.from)
        .with("to", options.to)
        .with("ranges", reports.len())
        .with("failingRanges", Value::Array(failing))
        .with("manifest", sum("manifest"))
        .with("archive", sum("archive"))
        .with("missing", sum("missing"))
        .with("unlisted", sum("unlisted"))
        .with("slotMismatch", sum("slotMismatch"))
        .with("unordered", sum("unordered"))
        .with("badOrderingSlots", sum("badOrderingSlots"))
        .with("walkPages", sum("walkPages"))
        .with(
            "programData",
            json!({"rows": pd_in_window, "rowsWalked": pd_rows.len(), "pages": pd_walk.pages, "walkReachedFloor": pd_reached}),
        )
        .with_rows("perRange", reports)
        .with("verifiedAt", now());
    write_json(&options.out.join("report.json"), &summary)?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let conn = memory_connection("1", "1GB").unwrap();
        conn.execute_batch(
            "CREATE TABLE manifest (signature VARCHAR, slot BIGINT);
CREATE TABLE archive (signature VARCHAR, slot BIGINT, tx_index INTEGER, single_in_slot BOOLEAN);",
        )
        .unwrap();
        conn
    }

    fn rows(
        conn: &Connection,
        manifest: &[(&str, i64)],
        archive: &[(&str, i64, Option<i32>, bool)],
    ) {
        for (signature, slot) in manifest {
            conn.execute(
                "INSERT INTO manifest VALUES (?, ?)",
                duckdb::params![signature, slot],
            )
            .unwrap();
        }
        for (signature, slot, index, single) in archive {
            conn.execute(
                "INSERT INTO archive VALUES (?, ?, ?, ?)",
                duckdb::params![signature, slot, index, single],
            )
            .unwrap();
        }
    }

    #[test]
    fn identical_manifest_and_archive_pass() {
        let conn = conn();
        rows(
            &conn,
            &[("a", 10), ("b", 10), ("c", 11)],
            &[
                ("a", 10, Some(4), false),
                ("b", 10, Some(9), false),
                ("c", 11, None, true),
            ],
        );
        let report = compare(&conn).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
        assert_eq!(report.int("manifest"), Some(3));
        assert_eq!(report.int("archive"), Some(3));
        assert_eq!(report.int("manifestSlots"), Some(2));
        assert!(report.get("missingSample").is_none());
    }

    /// The 2026-10-10 shape: the archive holds a whole busy slot, the manifest one row of it.
    #[test]
    fn a_short_manifest_reports_unlisted_rows_and_their_slots() {
        let conn = conn();
        rows(
            &conn,
            &[("pd-touch", 10)],
            &[
                ("pd-touch", 10, Some(311), false),
                ("p-a", 10, Some(172), false),
                ("p-b", 12, None, true),
            ],
        );
        let report = compare(&conn).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(false)));
        assert_eq!(report.get("V1"), Some(&Value::Bool(false)));
        assert_eq!(report.int("unlisted"), Some(2));
        assert_eq!(report.int("missing"), Some(0));
        // One manifest row in a slot the archive saw as multi-transaction fails V6 too.
        assert_eq!(report.int("badOrderingSlots"), Some(1));
        let sample = report.get("unlistedSample").unwrap().as_array().unwrap();
        assert_eq!(sample[0]["signature"], "p-a");
        assert_eq!(sample[1]["slot"], "12");
    }

    #[test]
    fn missing_rows_slot_disagreements_and_bad_ordering_are_reported() {
        let conn = conn();
        rows(
            &conn,
            &[("a", 10), ("b", 10), ("c", 11), ("gone", 13)],
            // b shares a's index; c was published at another slot.
            &[
                ("a", 10, Some(4), false),
                ("b", 10, Some(4), false),
                ("c", 12, None, true),
            ],
        );
        let report = compare(&conn).unwrap();
        assert_eq!(report.int("missing"), Some(1));
        assert_eq!(report.int("unlisted"), Some(0));
        assert_eq!(report.int("slotMismatch"), Some(1));
        // Slot 10: duplicate index; slot 13: the missing row has no single flag.
        assert_eq!(report.int("badOrderingSlots"), Some(2));
        assert_eq!(report.get("missingSample").unwrap()[0]["signature"], "gone");
        // A multi-transaction row without an index is unordered.
        let conn = super::tests::conn();
        rows(
            &conn,
            &[("a", 10), ("b", 10)],
            &[("a", 10, Some(1), false), ("b", 10, None, false)],
        );
        let report = compare(&conn).unwrap();
        assert_eq!(report.int("unordered"), Some(1));
        assert_eq!(report.get("V6"), Some(&Value::Bool(false)));
    }

    fn file(path: &str) -> Value {
        json!({"path": path, "table_name": "transactions"})
    }

    #[test]
    fn published_ranges_cover_the_request_without_gaps() {
        let files = [
            file("/r/staging/transactions/epoch=989/200-299-b.parquet"),
            file("/r/staging/transactions/epoch=990/200-299-c.parquet"),
            file("/r/staging/transactions/epoch=989/100-199-a.parquet"),
            file("/r/staging/transactions/epoch=990/300-399-d.parquet"),
            file("/r/open/compact-0f.parquet"),
            json!({"path": "/r/staging/signatures/epoch=989/0-99-x.parquet", "table_name": "signatures"}),
        ];
        let none: [(i64, i64); 0] = [];
        assert_eq!(
            published_ranges(&files, &none, 150, 250).unwrap(),
            [(100, 199), (200, 299)]
        );
        assert_eq!(published_ranges(&files, &none, 100, 399).unwrap().len(), 3);
        // Nothing past the request, even when an overlapping range extends beyond it.
        let tail = [
            file("/r/t/epoch=1/100-199-a.parquet"),
            file("/r/t/epoch=1/150-299-a.parquet"),
        ];
        assert_eq!(
            published_ranges(&tail, &none, 160, 170).unwrap(),
            [(100, 199)]
        );
        let error = |files: &[Value], coverage: &[(i64, i64)], from, to| {
            published_ranges(files, coverage, from, to)
                .unwrap_err()
                .to_string()
        };
        assert!(error(&files, &none, 50, 250).contains("50 to 99"));
        assert!(error(&files, &none, 100, 450).contains("400 to 450"));
        // Slots whose ranged files were compacted away are published coverage: their own range.
        let coverage = [(0, 420)];
        assert_eq!(
            published_ranges(&files, &coverage, 50, 410).unwrap(),
            [(50, 99), (100, 199), (200, 299), (300, 399), (400, 410)]
        );
        assert!(error(&files, &coverage, 50, 450).contains("400 to 450"));
        let gap = [
            file("/r/t/epoch=1/100-199-a.parquet"),
            file("/r/t/epoch=1/250-299-a.parquet"),
        ];
        assert!(error(&gap, &none, 100, 299).contains("200 to 249"));
        // Tail republications overlap; every slot lands in exactly one range.
        let overlap = [
            file("/r/t/epoch=1/100-199-a.parquet"),
            file("/r/t/epoch=1/150-299-a.parquet"),
            file("/r/t/epoch=1/160-180-a.parquet"),
            file("/r/t/epoch=1/290-310-a.parquet"),
        ];
        assert_eq!(
            published_ranges(&overlap, &none, 100, 305).unwrap(),
            [(100, 199), (200, 299), (300, 310)]
        );
        assert_eq!(
            catalog_coverage(&json!({"coverage": [{"from": 1, "to": "9"}]})),
            [(1, 9)]
        );
    }

    #[test]
    fn the_walk_starts_from_the_oldest_row_above_the_range_with_its_neighbour_as_fallback() {
        let row = |s: &str, slot: i64, index: Option<i64>| {
            Obj::new()
                .with("signature", s)
                .with("slot", slot)
                .with("tx_index", index)
        };
        let rows = [
            row("later", 201, Some(0)),
            row("high", 200, Some(9)),
            row("low", 200, Some(3)),
        ];
        let none = std::collections::HashSet::new();
        assert_eq!(
            start_cursor(&rows, &none),
            Some(("low".into(), 200, Some("high".into())))
        );
        assert_eq!(
            start_cursor(&rows[..1], &none),
            Some(("later".into(), 201, None))
        );
        assert_eq!(start_cursor(&[], &none), None);
        // A ProgramData-only transaction is never a program cursor.
        let pd = std::collections::HashSet::from(["low".to_owned()]);
        assert_eq!(
            start_cursor(&rows, &pd),
            Some(("high".into(), 200, Some("later".into())))
        );
    }

    #[test]
    fn only_a_walk_below_its_floor_counts_as_complete() {
        let mut walk = make_walk("p", Lane::Backfill, "reverify", 100, 199);
        walk.done = true;
        // A walk from the tip that listed nothing: the address has no history.
        assert!(reached_floor(&walk));
        // From a start cursor, an unconfirmed empty page is the 2026-10-10 failure.
        walk.before = Some("anchor".into());
        assert!(!reached_floor(&walk));
        walk.oldest_slot = Some(100);
        assert!(!reached_floor(&walk));
        walk.oldest_slot = Some(99);
        assert!(reached_floor(&walk));
        // An empty page confirmed from the fallback is the address's first signature.
        walk.oldest_slot = Some(150);
        assert!(!reached_floor(&walk));
        walk.fallback = Some("second".into());
        assert!(reached_floor(&walk));
        walk.done = false;
        assert!(!reached_floor(&walk));
        // A start cursor whose page and fallback page were both empty listed nothing: not an end.
        walk.done = true;
        walk.oldest_slot = None;
        assert!(!reached_floor(&walk));
        // Nor is an end confirmed above the window, before the walk reached it.
        walk.oldest_slot = Some(250);
        assert!(!reached_floor(&walk));
    }

    /// Pages keyed by the `before` cursor; every other call fails.
    struct Pages(std::collections::HashMap<String, Value>);

    impl Rpc for Pages {
        fn call<'a>(
            &'a self,
            _method: &'a str,
            params: Value,
            _lane: Lane,
        ) -> super::super::rpc::RpcFuture<'a> {
            let before = params[1]["before"].as_str().unwrap_or("").to_owned();
            Box::pin(async move {
                match self.0.get(&before) {
                    Some(page) => Ok(super::super::rpc::RpcResponse {
                        result: page.clone(),
                        raw: String::new(),
                    }),
                    None => Err(super::super::rpc::RpcFailure {
                        method: before,
                        code: -1,
                        throttled: false,
                    }),
                }
            })
        }

        fn provider_name(&self) -> &str {
            "pages"
        }
    }

    /// The start cursor is the cursor the provider could not resolve on 2026-10-10: the walk
    /// confirms it from the fallback row and lists the range below it.
    #[test]
    fn a_dead_start_cursor_resumes_from_its_fallback_and_reaches_the_floor() {
        let sig = |s: &str, slot: i64| json!({"signature": s, "slot": slot, "err": null});
        let rpc = Pages(
            [
                ("dead".to_owned(), json!([])),
                (
                    "high".to_owned(),
                    json!([sig("dead", 200), sig("x", 150), sig("y", 120), sig("z", 99)]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let dir =
            std::env::temp_dir().join(format!("solos-reverify-walk-{}", uuid::Uuid::new_v4()));
        let mut initial = make_walk("p", Lane::Backfill, "reverify", 100, 199);
        initial.before = Some("dead".into());
        initial.last_slot = Some(200);
        initial.fallback = Some("high".into());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let walk = runtime.block_on(walk_into(&rpc, &dir, initial)).unwrap();
        assert!(walk.done && reached_floor(&walk));
        let store = Store::open(&dir, super::super::schema::SCHEMA).unwrap();
        let rows = store
            .rows("SELECT signature FROM signatures ORDER BY slot", &[])
            .unwrap();
        assert_eq!(
            rows.iter()
                .filter_map(|r| r.str("signature"))
                .collect::<Vec<_>>(),
            ["y", "x"]
        );
        store.close().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The archive side reads the newest publication per signature from the catalog's files.
    #[test]
    fn the_archive_is_read_from_registered_parquet_with_the_newest_publication_winning() {
        let root = std::env::temp_dir().join(format!("solos-reverify-{}", uuid::Uuid::new_v4()));
        let dir = root.join("staging/transactions/epoch=0");
        std::fs::create_dir_all(&dir).unwrap();
        let writer = memory_connection("1", "1GB").unwrap();
        let old = dir.join("100-199-old.parquet");
        let new = dir.join("100-199-new.parquet");
        for (path, sql) in [
            (
                &old,
                "SELECT * FROM (VALUES ('a', 100::BIGINT, 1, false), ('b', 100::BIGINT, NULL, false), ('z', 250::BIGINT, NULL, true)) t(signature, slot, tx_index, single_in_slot)",
            ),
            (
                &new,
                "SELECT * FROM (VALUES ('b', 100::BIGINT, 2, false)) t(signature, slot, tx_index, single_in_slot)",
            ),
        ] {
            writer
                .execute_batch(&format!(
                    "COPY ({sql}) TO {} (FORMAT parquet)",
                    sql_string(&path.to_string_lossy())
                ))
                .unwrap();
        }
        let catalog = json!({"files": [
            {"path": old.to_string_lossy(), "table_name": "transactions", "created_at": "2026-10-10T00:00:00Z"},
            {"path": new.to_string_lossy(), "table_name": "transactions", "created_at": "2026-10-11T00:00:00Z"},
        ]});
        std::fs::write(root.join("catalog.json"), catalog.to_string()).unwrap();
        let conn = conn();
        conn.execute_batch("DROP TABLE archive").unwrap();
        let digest = load_archive(&conn, &root, 100, 199).unwrap();
        assert_eq!(digest.len(), 64);
        let files = range_files(&root, "transactions", 100, 199).unwrap();
        assert_eq!(digest, files_digest(&files));
        // Order does not matter; a changed content hash does.
        let mut reversed = files.clone();
        reversed.reverse();
        assert_eq!(digest, files_digest(&reversed));
        let mut changed = files.clone();
        changed[0]["sha256"] = json!("other");
        assert_ne!(digest, files_digest(&changed));
        rows(&conn, &[("a", 100), ("b", 100)], &[]);
        let report = compare(&conn).unwrap();
        assert_eq!(report.get("ok"), Some(&Value::Bool(true)), "{report:?}");
        assert_eq!(report.int("archive"), Some(2));
        std::fs::remove_dir_all(root).unwrap();
    }
}
