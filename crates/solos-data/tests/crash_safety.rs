//! Single-owner lock and the restart predicate (ADR-0016).
//!
//! Offline, on temporary directories. The holder is `solos-data dev crash-writer`, which opens the
//! checkpoint and waits. `Child::kill` sends SIGKILL. That releases the kernel lock; it is not a
//! power-loss test.

use serde_json::json;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::writer::{Checkpoint, publish_range, recover_files};
use solos_data::fsutil::{file_hash, manifest_path, tmp_path};
use solos_data::restart::{RestartError, restart_predicate, tail_watermark};
use solos_data::store::{Store, StoreError, sql_string};
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("solos-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Holder(Option<Child>);

impl Holder {
    fn spawn(root: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_solos-data"))
            .args(["dev", "crash-writer", "--root"])
            .arg(root)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self(Some(child))
    }

    fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_ready(child: &mut Child) {
    use std::io::Read;
    let mut stdout = child.stdout.take().unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 64];
    while !got.windows(5).any(|window| window == b"ready") {
        let n = stdout.read(&mut buf).unwrap();
        assert!(n > 0, "holder closed stdout before ready");
        got.extend_from_slice(&buf[..n]);
    }
}

fn assert_already_owned(root: &Path, opened: Result<Store, StoreError>) {
    match opened {
        Err(StoreError::AlreadyOwned { path }) => {
            assert_eq!(path, root.join("checkpoint.duckdb.lock"));
        }
        Err(other) => panic!("expected AlreadyOwned, got {other}"),
        Ok(_) => panic!("second open must fail before DuckDB"),
    }
}

#[test]
fn second_owner_gets_the_typed_lock_error() {
    let root = TempDir::new("lock");
    let mut holder = Holder::spawn(&root.0);
    wait_ready(holder.child());
    assert_already_owned(&root.0, Store::open(&root.0, SCHEMA));
    assert_already_owned(&root.0, Store::open_read_only(&root.0));
    let mode = std::fs::metadata(root.0.join("checkpoint.duckdb.lock"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[test]
fn kernel_releases_the_lock_after_sigkill_and_the_restart_acquires_it() {
    let root = TempDir::new("lock-kill");
    let mut holder = Holder::spawn(&root.0);
    wait_ready(holder.child());
    // SIGKILL. The kernel drops the flock with the process.
    holder.child().kill().unwrap();
    let status = holder.child().wait().unwrap();
    assert!(!status.success());
    holder.0.take();
    let store = Store::open(&root.0, SCHEMA).unwrap();
    assert_eq!(store.get("durable").unwrap(), Some(json!(true)));
    store.close().unwrap();
}

#[test]
fn a_second_open_in_this_process_is_already_owned() {
    let root = TempDir::new("lock-same");
    let ddl = "CREATE TABLE IF NOT EXISTS kv(name VARCHAR PRIMARY KEY, value JSON);";
    let first = Store::open(&root.0, ddl).unwrap();
    assert_already_owned(&root.0, Store::open(&root.0, ddl));
    drop(first);
    let again = Store::open(&root.0, ddl).unwrap();
    again.close().unwrap();
}

#[test]
fn store_open_rejects_an_invalid_wal_size() {
    // `Store::open` validates `SOLOS_DATA_WAL_AUTOCHECKPOINT` (failpoints feature) with
    // `is_wal_size` before interpolating it into SQL. `-1` is rejected by that validator but
    // DuckDB's own size parser accepts it ("infinite"), so without the guard the same process
    // would open and publish successfully.
    let root = TempDir::new("wal-size");
    let output = Command::new(env!("CARGO_BIN_EXE_solos-data"))
        .args(["dev", "failpoint-writer", "--root"])
        .arg(&root.0)
        .args(["--failpoint", "publish/none"])
        .env("SOLOS_DATA_WAL_AUTOCHECKPOINT", "-1")
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "Store::open must reject an invalid WAL size"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("SOLOS_DATA_WAL_AUTOCHECKPOINT is not a size"),
        "stderr must name the invalid WAL size, got: {stderr}"
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("published"),
        "the invalid WAL size must fail before any range is published"
    );
}

/// One Parquet row registered in `files` and `file_bounds`.
fn publish_partition(store: &mut Store, path: &Path, slot_from: i64, slot_to: i64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    store
        .exec_batch("CREATE OR REPLACE TEMP TABLE restart_row(n BIGINT)")
        .unwrap();
    store
        .exec_batch("INSERT INTO restart_row VALUES (1)")
        .unwrap();
    let tmp = tmp_path(path);
    store
        .exec_batch(&format!(
            "COPY (SELECT * FROM restart_row) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
            sql_string(&tmp.to_string_lossy())
        ))
        .unwrap();
    std::fs::rename(&tmp, path).unwrap();
    let count = store
        .rows(
            &format!(
                "SELECT count(*) AS n FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )
        .unwrap()[0]
        .int("n")
        .unwrap();
    let hash = file_hash(path).unwrap();
    let path_text = path.to_string_lossy().into_owned();
    store
        .exec(
            "INSERT INTO files VALUES (?, 'transactions', 0, ?, ?, '2026-10-29T00:00:00Z', 'active')",
            &[&path_text, &count, &hash],
        )
        .unwrap();
    store
        .exec(
            "INSERT INTO file_bounds VALUES (?, ?, ?)",
            &[&path_text, &slot_from, &slot_to],
        )
        .unwrap();
}

fn open_collector(root: &Path) -> Store {
    Store::open(root, SCHEMA).unwrap()
}

fn predicate(store: &Store) -> Result<(), RestartError> {
    restart_predicate(store, tail_watermark(store).unwrap())
}

#[test]
fn restart_predicate_is_green_on_a_clean_temp_run_and_red_when_a_covered_partition_is_mutated() {
    let root = TempDir::new("restart");
    let covered = root
        .0
        .join("staging/transactions/epoch=0/100-100-covered.parquet");
    let above = root
        .0
        .join("staging/transactions/epoch=0/200-210-above.parquet");
    let unregistered = root.0.join("staging/transactions/epoch=0/orphan.parquet");
    {
        let mut store = open_collector(&root.0);
        publish_partition(&mut store, &covered, 100, 100);
        publish_partition(&mut store, &above, 200, 210);
        std::fs::copy(&covered, &unregistered).unwrap();
        store
            .set(
                "W",
                &json!({"signature": "sig", "slot": 100, "completedAt": "2026-10-29T00:00:00Z"}),
            )
            .unwrap();
        store.close().unwrap();
    }

    let mut store = open_collector(&root.0);
    assert_eq!(tail_watermark(&store).unwrap(), Some(100));
    // W and the file rows commit together, so a registered file above W is not an orphan.
    match predicate(&store).unwrap_err() {
        RestartError::AboveWatermark {
            path,
            slot_to,
            watermark,
        } => {
            assert_eq!(path, manifest_path(&root.0, &above));
            assert_eq!(slot_to, 210);
            assert_eq!(watermark, 100);
        }
        other => panic!("a file above W must fail the predicate, got {other}"),
    }
    std::fs::remove_file(&above).unwrap();
    store
        .exec(
            "DELETE FROM files WHERE path=?",
            &[&above.to_string_lossy().into_owned()],
        )
        .unwrap();
    store
        .exec(
            "DELETE FROM file_bounds WHERE path=?",
            &[&above.to_string_lossy().into_owned()],
        )
        .unwrap();
    predicate(&store).unwrap();
    recover_files(&mut store, &root.0).unwrap();
    assert!(!unregistered.exists());
    assert!(covered.exists());
    predicate(&store).unwrap();
    recover_files(&mut store, &root.0).unwrap();
    predicate(&store).unwrap();
    store.close().unwrap();

    std::fs::remove_file(&covered).unwrap();
    let store = open_collector(&root.0);
    match predicate(&store).unwrap_err() {
        RestartError::Missing { path } => assert_eq!(path, manifest_path(&root.0, &covered)),
        other => panic!("delete must fail the predicate, got {other}"),
    }
    store.close().unwrap();

    let truncated_root = TempDir::new("restart-truncate");
    let truncated = truncated_root
        .0
        .join("staging/transactions/epoch=0/100-100-covered.parquet");
    {
        let mut store = open_collector(&truncated_root.0);
        publish_partition(&mut store, &truncated, 100, 100);
        store
            .set(
                "W",
                &json!({"signature": "sig", "slot": 100, "completedAt": "2026-10-29T00:00:00Z"}),
            )
            .unwrap();
        store.close().unwrap();
    }
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&truncated)
        .unwrap()
        .set_len(8)
        .unwrap();
    let store = open_collector(&truncated_root.0);
    match predicate(&store).unwrap_err() {
        RestartError::Parse { path, .. }
        | RestartError::RowCount { path, .. }
        | RestartError::Hash { path, .. } => {
            assert_eq!(path, manifest_path(&truncated_root.0, &truncated));
        }
        other => panic!("truncate must fail the predicate, got {other}"),
    }
    store.close().unwrap();
}

/// Copy the checkpoint files (the DuckDB database and its write-ahead log) of a killed run to
/// `to`. The WAL holds the committed data a crash left uncheckpointed, so a faithful checkpoint
/// copy takes both files.
fn copy_checkpoint(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for name in ["checkpoint.duckdb", "checkpoint.duckdb.wal"] {
        let source = from.join(name);
        if source.is_file() {
            std::fs::copy(&source, to.join(name)).unwrap();
        }
    }
}

/// Spawn the real `dev failpoint-writer` binary to publish `[from, to]` with one kill point
/// armed. `SOLOS_DATA_WAL_AUTOCHECKPOINT` is forced huge so the committed transaction stays in
/// the WAL instead of being checkpointed into the database file.
fn failpoint_writer(root: &Path, name: &str, from: i64, to: i64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_solos-data"))
        .args(["dev", "failpoint-writer", "--root"])
        .arg(root)
        .arg("--failpoint")
        .arg(name)
        .arg("--from")
        .arg(from.to_string())
        .arg("--to")
        .arg(to.to_string())
        .env("SOLOS_DATA_FAILPOINT_MARKER", root.join("failpoint-marker"))
        .env("SOLOS_DATA_WAL_AUTOCHECKPOINT", "10GB")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Wait for a child, bounded so a hang fails the test instead of hanging CI.
fn wait_exit(mut child: Child) -> std::process::ExitStatus {
    use std::time::{Duration, Instant};
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

#[test]
fn above_watermark_fires_on_a_checkpoint_copied_from_a_killed_run() {
    // A real failpoint-writer commits W and its file rows, then aborts at
    // publish/after-watermark-commit, before DuckDB checkpoints them into the database file. The
    // committed data survives only in the WAL.
    let killed = TempDir::new("above-killed");
    {
        let mut store = open_collector(&killed.0);
        insert_transaction(&mut store, "sig", 100);
        store.close().unwrap();
    }
    let status = wait_exit(failpoint_writer(
        &killed.0,
        "publish/after-watermark-commit",
        100,
        101,
    ));
    assert!(
        !status.success() && status.signal().is_some(),
        "the kill point must die by signal, got {status}"
    );
    let marker = std::fs::read_to_string(killed.0.join("failpoint-marker"))
        .unwrap_or_else(|_| panic!("the kill point must write its marker file"));
    assert_eq!(marker, "publish/after-watermark-commit");

    // Copy the checkpoint (the DuckDB file plus its WAL) to a fresh root and recover it: the
    // committed W and file rows survive the copy only if the WAL was included.
    let copy = TempDir::new("above-copy");
    copy_checkpoint(&killed.0, &copy.0);
    let mut store = Store::open(&copy.0, SCHEMA).unwrap();
    let watermark = tail_watermark(&store).unwrap();
    assert_eq!(
        watermark,
        Some(101),
        "the killed run's committed W must survive the copy"
    );
    predicate(&store).unwrap();

    // Register a partition past the watermark: the corrupt state the check defends against.
    let above = copy
        .0
        .join("staging/transactions/epoch=0/200-210-above.parquet");
    publish_partition(&mut store, &above, 200, 210);
    store.close().unwrap();

    let store = open_collector(&copy.0);
    match predicate(&store).unwrap_err() {
        RestartError::AboveWatermark {
            path,
            slot_to,
            watermark,
        } => {
            assert_eq!(path, manifest_path(&copy.0, &above));
            assert_eq!(slot_to, 210);
            assert_eq!(watermark, 101);
        }
        other => panic!("a file above W must fail the predicate, got {other}"),
    }
    store.close().unwrap();
}

fn insert_transaction(store: &mut Store, signature: &str, slot: i64) {
    store
        .exec_batch(&format!(
            "INSERT INTO transactions VALUES ('{signature}', {slot}, {slot}, NULL, NULL, 'null', 5000, 10, 'AA==', '{{}}', '{{}}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)"
        ))
        .unwrap();
}

fn publish_watermark(store: &mut Store, root: &Path, slot: i64) {
    publish_range(
        store,
        root,
        slot,
        slot,
        Some(Checkpoint {
            key: "W".into(),
            value: json!({
                "signature": "sig",
                "slot": slot,
                "completedAt": "2026-10-29T00:00:00Z"
            }),
            cycle_id: None,
        }),
    )
    .unwrap();
}

#[test]
fn restart_predicate_passes_a_publish_range_and_fails_when_that_partition_is_deleted() {
    let root = TempDir::new("publish-range");
    let mut store = open_collector(&root.0);
    insert_transaction(&mut store, "sig", 100);
    publish_watermark(&mut store, &root.0, 100);
    assert_eq!(tail_watermark(&store).unwrap(), Some(100));
    predicate(&store).unwrap();
    let path = store
        .rows("SELECT path FROM files WHERE status='active'", &[])
        .unwrap()[0]
        .str("path")
        .unwrap()
        .to_owned();
    store.close().unwrap();
    std::fs::remove_file(&path).unwrap();
    let store = open_collector(&root.0);
    match predicate(&store).unwrap_err() {
        RestartError::Missing { path: missing } => {
            assert_eq!(missing, manifest_path(&root.0, Path::new(&path)));
        }
        other => panic!("deleting a publish_range partition must fail, got {other}"),
    }
    store.close().unwrap();
}

#[test]
fn a_same_size_bit_flip_of_a_published_partition_is_a_hash_mismatch() {
    let root = TempDir::new("bit-flip");
    let mut store = open_collector(&root.0);
    insert_transaction(&mut store, "sig", 100);
    publish_watermark(&mut store, &root.0, 100);
    predicate(&store).unwrap();
    let path = store
        .rows("SELECT path FROM files WHERE status='active'", &[])
        .unwrap()[0]
        .str("path")
        .unwrap()
        .to_owned();
    let mut bytes = std::fs::read(&path).unwrap();
    let index = bytes.len() / 2;
    bytes[index] ^= 0x01;
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        std::fs::metadata(&path).unwrap().len()
    );
    std::fs::write(&path, &bytes).unwrap();
    match predicate(&store).unwrap_err() {
        RestartError::Hash {
            path: got,
            expected,
            actual,
        } => {
            assert_eq!(got, manifest_path(&root.0, Path::new(&path)));
            assert_ne!(expected, actual);
        }
        other => panic!("a same-size bit flip must be a hash mismatch, got {other}"),
    }
    store.close().unwrap();
}

#[test]
fn recover_files_keeps_registered_files_through_a_symlink_and_a_relative_path() {
    let real = TempDir::new("recover-real");
    let link = std::env::temp_dir().join(format!("solos-recover-link-{}", uuid::Uuid::new_v4()));
    std::os::unix::fs::symlink(&real.0, &link).unwrap();
    let mut store = open_collector(&link);
    let relative = real.0.join("staging/rel.parquet");
    let absolute = real.0.join("staging/abs.parquet");
    let orphan = real.0.join("staging/orphan.parquet");
    std::fs::create_dir_all(relative.parent().unwrap()).unwrap();
    std::fs::write(&relative, b"relative").unwrap();
    std::fs::write(&absolute, b"absolute").unwrap();
    std::fs::write(&orphan, b"orphan").unwrap();
    let absolute_path = absolute.to_string_lossy().into_owned();
    let count = 1i64;
    let hash = "hash";
    for path in ["staging/rel.parquet", absolute_path.as_str()] {
        store
            .exec(
                "INSERT INTO files VALUES (?, 'transactions', 0, ?, ?, 't', 'active')",
                &[&path, &count, &hash],
            )
            .unwrap();
    }
    recover_files(&mut store, &link).unwrap();
    assert!(relative.exists(), "relative registration must survive");
    assert!(absolute.exists(), "absolute registration must survive");
    assert!(!orphan.exists(), "unregistered parquet is removed");
    recover_files(&mut store, &link).unwrap();
    assert!(relative.exists());
    store.close().unwrap();
    let _ = std::fs::remove_file(&link);
}

#[test]
fn offline_repack_holds_the_checkpoint_lock() {
    let root = TempDir::new("repack-lock");
    let mut store = open_collector(&root.0);
    store.set("k", &json!({"a": 1})).unwrap();
    store.close().unwrap();
    let lock = solos_data::store::acquire_lock(&root.0).unwrap();
    assert_already_owned(&root.0, Store::open(&root.0, SCHEMA));
    let report = solos_data::repack::repack_checkpoint(&root.0, &lock).unwrap();
    assert_eq!(report.get("ok"), Some(&json!(true)));
    drop(lock);
    let store = open_collector(&root.0);
    assert_eq!(store.get("k").unwrap(), Some(json!({"a": 1})));
    store.close().unwrap();
}

#[test]
fn repack_refuses_a_lock_taken_for_a_different_root() {
    let root_a = TempDir::new("lock-root-a");
    let root_b = TempDir::new("lock-root-b");
    let lock_b = solos_data::store::acquire_lock(&root_b.0).unwrap();
    let error = solos_data::repack::repack_checkpoint(&root_a.0, &lock_b).unwrap_err();
    assert!(error.to_string().contains("root mismatch"), "{error}");
    drop(lock_b);
}
