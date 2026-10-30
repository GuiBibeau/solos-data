//! Crash-safety matrix (issue #56): the real binary publishes a fixture range with one kill
//! point armed, aborts, restarts, and the restart predicate (ADR-0016) passes. Re-running the
//! same range to completion is byte-identical to a golden uncrashed run, and no primary key is
//! duplicated. The matrix runs with DuckDB's default checkpointing and with `wal_autocheckpoint`
//! forced tiny so kills land on both sides of a checkpoint.
//!
//! Offline, on temporary directories. `std::process::abort` (SIGABRT) is process death, not
//! power loss; power loss is out of scope (see ADR-0016).

use solos_data::collector::schema::SCHEMA;
use solos_data::collector::writer::{publish_range, recover_files};
use solos_data::failpoints::{KILL_POINTS, arm_callback, disarm};
use solos_data::restart::{restart_predicate, tail_watermark};
use solos_data::store::Store;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const FROM: i64 = 100;
const TO: i64 = 101;

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("solos-fp-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(root: &Path) -> Store {
    Store::open(root, SCHEMA).unwrap()
}

fn insert_fixture(store: &mut Store) {
    for (signature, slot) in [("a", 100i64), ("b", 101)] {
        store
            .exec_batch(&format!(
                "INSERT INTO transactions VALUES ('{signature}', {slot}, {slot}, NULL, NULL, \
                 'null', 5000, 10, 'AA==', '{{}}', '{{}}', 'tail', 'fixture', \
                 '2026-10-29T00:00:00Z', NULL)"
            ))
            .unwrap();
    }
}

/// Spawn the real binary to publish `[FROM, TO]` with one kill point armed (`None` for a normal
/// run). With `tiny_wal`, the WAL autocheckpoint is forced tiny.
fn spawn(root: &Path, name: Option<&str>, tiny_wal: bool) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_solos-data"));
    command
        .args(["dev", "failpoint-writer", "--root"])
        .arg(root)
        .arg("--failpoint")
        .arg(name.unwrap_or("publish/none"))
        .arg("--from")
        .arg(FROM.to_string())
        .arg("--to")
        .arg(TO.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if tiny_wal {
        command.env("SOLOS_DATA_WAL_AUTOCHECKPOINT", "1KB");
    }
    command.spawn().unwrap()
}

/// Wait for a child, bounded so a hang fails the test instead of hanging CI.
fn wait(mut child: Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("binary did not exit within 60s");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// `(table, epoch, row_count, sha256, slot_from, slot_to)` of every active file, ordered.
type Manifest = Vec<(String, i64, i64, String, i64, i64)>;

fn manifest(store: &Store) -> Manifest {
    store
        .rows(
            "SELECT f.table_name, f.epoch, f.row_count, f.sha256, b.slot_from, b.slot_to
             FROM files f LEFT JOIN file_bounds b ON b.path = f.path
             WHERE f.status = 'active'
             ORDER BY f.table_name",
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.str("table_name").unwrap_or("").to_owned(),
                row.int("epoch").unwrap_or(0),
                row.int("row_count").unwrap_or(0),
                row.str("sha256").unwrap_or("").to_owned(),
                row.int("slot_from").unwrap_or(0),
                row.int("slot_to").unwrap_or(0),
            )
        })
        .collect()
}

/// After a kill, the restart predicate must pass and the watermark must not have moved.
fn assert_restart_ok(root: &Path) {
    let mut store = open(root);
    recover_files(&mut store, root).unwrap();
    let watermark = tail_watermark(&store).unwrap();
    assert_eq!(
        watermark, None,
        "a kill before the commit must not advance the watermark"
    );
    restart_predicate(&store, watermark).unwrap();
    store.close().unwrap();
}

/// Complete a range from `root` and assert the files are byte-identical to `golden` and that no
/// primary key was duplicated.
fn complete_and_compare(root: &Path, golden: &Manifest, tiny_wal: bool) {
    let status = wait(spawn(root, None, tiny_wal));
    assert!(status.success(), "completion run failed: {status}");
    let store = open(root);
    assert_eq!(tail_watermark(&store).unwrap(), Some(TO));
    let got = manifest(&store);
    assert_eq!(got, *golden, "partition hashes and manifest must match");
    // No duplicate primary keys: one `files` row (unique path), one range, distinct signatures.
    assert_eq!(
        store
            .rows("SELECT count(*) AS n FROM files", &[])
            .unwrap()[0]
            .int("n"),
        Some(i64::try_from(golden.len()).unwrap_or(0))
    );
    assert_eq!(
        store
            .rows("SELECT count(DISTINCT path) AS n FROM files", &[])
            .unwrap()[0]
            .int("n"),
        store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].int("n")
    );
    assert_eq!(
        store
            .rows("SELECT count(*) AS n FROM transactions", &[])
            .unwrap()[0]
            .int("n"),
        store
            .rows("SELECT count(DISTINCT signature) AS n FROM transactions", &[])
            .unwrap()[0]
            .int("n")
    );
    restart_predicate(&store, tail_watermark(&store).unwrap()).unwrap();
    store.close().unwrap();
}

fn golden() -> Manifest {
    let root = TempDir::new("golden");
    {
        let mut store = open(&root.0);
        insert_fixture(&mut store);
        store.close().unwrap();
    }
    let status = wait(spawn(&root.0, None, false));
    assert!(status.success(), "golden run failed: {status}");
    let store = open(&root.0);
    let golden = manifest(&store);
    assert!(!golden.is_empty(), "the fixture must publish at least one file");
    restart_predicate(&store, tail_watermark(&store).unwrap()).unwrap();
    store.close().unwrap();
    golden
}

#[test]
fn the_kill_point_matrix_is_green_and_re_runs_are_byte_identical() {
    let golden = golden();
    for tiny_wal in [false, true] {
        for name in KILL_POINTS {
            let root = TempDir::new(name);
            {
                let mut store = open(&root.0);
                insert_fixture(&mut store);
                store.close().unwrap();
            }
            let status = wait(spawn(&root.0, Some(name), tiny_wal));
            assert!(
                !status.success() && status.signal().is_some(),
                "kill point {name} (tiny_wal={tiny_wal}) must die by signal, got {status}"
            );
            assert_restart_ok(&root.0);
            complete_and_compare(&root.0, &golden, tiny_wal);
        }
    }
}

#[test]
fn the_range_check_catches_an_export_whose_row_count_changed() {
    let root = TempDir::new("range-check");
    let mut store = open(&root.0);
    insert_fixture(&mut store);
    store.close().unwrap();

    // A valid one-row Parquet file, used to corrupt the export between COPY and the readback.
    let corrupt = {
        let conn = solos_data::store::memory_connection("1", "64MB").unwrap();
        conn.execute_batch("CREATE TABLE t(n BIGINT)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let path = std::env::temp_dir().join(format!("solos-corrupt-{}.parquet", uuid::Uuid::new_v4()));
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM t) TO '{}' (FORMAT PARQUET)",
            path.to_string_lossy()
        ))
        .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        bytes
    };
    static CORRUPT: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    static ROOT: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
    let _ = CORRUPT.set(corrupt);
    *ROOT.lock().unwrap() = Some(root.0.clone());

    arm_callback("publish/after-tmp-write", || {
        let staging = ROOT.lock().unwrap().clone().unwrap().join("staging");
        overwrite_tmp_files(&staging, CORRUPT.get().unwrap());
    });

    let mut store = open(&root.0);
    let error = publish_range(&mut store, &root.0, FROM, TO, None).unwrap_err();
    disarm("publish/after-tmp-write");
    *ROOT.lock().unwrap() = None;
    let message = error.to_string();
    assert!(
        message.contains("row count mismatch"),
        "the range check must reject a changed row count: {message}"
    );
    assert_eq!(
        store
            .rows("SELECT count(*) AS n FROM files", &[])
            .unwrap()[0]
            .int("n"),
        Some(0),
        "nothing may be registered when the range check fails"
    );
    store.close().unwrap();
}

/// Overwrite every `.tmp` file under `dir` with `bytes` (the corruption callback).
fn overwrite_tmp_files(dir: &Path, bytes: &[u8]) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            overwrite_tmp_files(&path, bytes);
        } else if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
            let _ = std::fs::write(&path, bytes);
        }
    }
}
