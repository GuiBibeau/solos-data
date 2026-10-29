//! Single-owner lock and the restart predicate (ADR-0016).
//!
//! Offline, on temporary directories. The holder is `solos-data dev crash-writer`, which opens the
//! checkpoint and waits. `Child::kill` sends SIGKILL. That releases the kernel lock; it is not a
//! power-loss test.

use serde_json::json;
use solos_data::collector::schema::SCHEMA;
use solos_data::collector::writer::recover_files;
use solos_data::fsutil::{file_hash, tmp_path};
use solos_data::restart::{RestartError, restart_predicate, tail_watermark};
use solos_data::store::{Store, StoreError, sql_string};
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
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
        // Registered, but strictly above W: an orphan, not trusted.
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&above)
            .unwrap()
            .set_len(0)
            .unwrap();
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
    predicate(&store).unwrap();
    recover_files(&mut store, &root.0).unwrap();
    assert!(!unregistered.exists());
    assert!(above.exists());
    assert_eq!(std::fs::metadata(&above).unwrap().len(), 0);
    predicate(&store).unwrap();
    recover_files(&mut store, &root.0).unwrap();
    predicate(&store).unwrap();
    store.close().unwrap();

    std::fs::remove_file(&covered).unwrap();
    let store = open_collector(&root.0);
    match predicate(&store).unwrap_err() {
        RestartError::Missing { path } => assert_eq!(path, covered),
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
        | RestartError::Hash { path, .. } => assert_eq!(path, truncated),
        other => panic!("truncate must fail the predicate, got {other}"),
    }
    store.close().unwrap();
}
