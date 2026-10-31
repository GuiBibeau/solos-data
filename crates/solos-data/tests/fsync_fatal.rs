//! The fsync-is-fatal rule (ADR-0016) traced through the decoder, augment and spot write paths:
//! an injected fsync failure on the temporary file or on the directory after rename must end the
//! lane without a retry: no file registered, no error row, no partial publication. The
//! collector's halves live in `collector_service.rs`; the crash matrix is `failpoints.rs`.

use serde_json::json;
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::parquet::{Target, write_parquet};
use solos_data::augment::periods::Granularity;
use solos_data::augment::series::{Ctx, RowsFuture, Series, sync_series};
use solos_data::db::Db;
use solos_data::decoder::normalize::Rows;
use solos_data::decoder::publish::{SourceProgress, publish};
use solos_data::decoder::schema::schema_static;
use solos_data::fsutil::{FsyncStep, clear_fsync_failure, inject_fsync_failure};
use solos_data::jsonout::Obj;
use solos_data::spot::write::copy_to;
use solos_data::store::{Store, memory_connection, sql_string};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn tempdir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn decoder_transaction(signature: &str, slot: i64) -> Obj {
    Obj::new()
        .with("signature", signature)
        .with("source_hash", "h")
        .with("slot", slot)
        .with("tx_index", 0)
        .with("single_in_slot", false)
        .with("block_time", 100)
        .with("committed", true)
        .with("status", "decoded")
        .with("event_count", 1)
        .with("error_count", 0)
        .with("source_file", "fixture")
        .with("decoded_at", "fixture")
        .with("decoder_version", "fixture")
}

#[test]
fn decoder_publication_exits_without_retry_on_an_injected_fsync_failure() {
    for step in [FsyncStep::TempFile, FsyncStep::DirAfterRename] {
        let root = tempdir("decoder-fsync");
        let mut store = Store::open(&root, schema_static()).unwrap();
        let mut rows = Rows::default();
        rows.get_mut("decoded_transactions")
            .push(decoder_transaction("sig", 100));
        inject_fsync_failure(step);
        let result = publish(
            &mut store,
            &rows,
            &SourceProgress {
                hash: "file".into(),
                path: "raw".into(),
                offset: 1,
                at: Some("0".into()),
                seen: None,
            },
        );
        clear_fsync_failure();
        let message = result.unwrap_err().to_string();
        assert!(message.contains("fsync failed"), "{step:?}: {message}");
        assert_eq!(
            store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].int("n"),
            Some(0),
            "no file may be registered when the fsync failed"
        );
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}

fn target() -> Target {
    Target {
        source: "fixture".into(),
        dataset: "funding".into(),
        symbol: "SOL".into(),
        phoenix_symbol: None,
        period: "2026-10".into(),
        complete: true,
    }
}

#[test]
fn augment_write_parquet_is_fatal_on_an_injected_fsync_failure() {
    for step in [FsyncStep::TempFile, FsyncStep::DirAfterRename] {
        let root = tempdir("augment-fsync");
        let mut store = ledger::open(&root, Lane::Sync).unwrap();
        inject_fsync_failure(step);
        let result = write_parquet(&mut store, &root, "SELECT 1 AS n", &target());
        clear_fsync_failure();
        let message = result.unwrap_err().to_string();
        assert!(message.contains("fsync failed"), "{step:?}: {message}");
        assert_eq!(
            store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0].int("n"),
            Some(0)
        );
        store.close().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A one-period source with a single row, so `sync_series` writes exactly one file.
struct MockSeries;

impl Series for MockSeries {
    fn source(&self) -> &str {
        "fixture"
    }
    fn dataset(&self) -> &str {
        "funding"
    }
    fn symbol(&self) -> &str {
        "SOL"
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        None
    }
    fn granularity(&self) -> Granularity {
        Granularity::Day
    }
    fn lag_ms(&self) -> i64 {
        0
    }
    fn fetch<'a>(&'a self, _http: &'a Http, _start_ms: i64, _end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move { Ok(vec![json!({ "t": 1, "p": 2.5 })]) })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT t, p, {constants} FROM read_json({}, format='array', \
             columns={{'t': 'BIGINT', 'p': 'DOUBLE'}}, maximum_object_size=67108864)",
            sql_string(&staged.to_string_lossy())
        )
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn augment_sync_series_stops_and_flags_a_fatal_fsync() {
    let root = tempdir("augment-series-fsync");
    std::fs::create_dir_all(root.join("staging")).unwrap();
    let store = ledger::open(&root, Lane::Sync).unwrap();
    let (db, thread) = Db::spawn(store);
    let ctx = Ctx {
        http: Http::new(1.0).unwrap(),
        db,
        root: root.clone(),
        lane: Lane::Sync,
        start: chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
        now_ms: 1_791_547_200_000,
        stop: Arc::new(AtomicBool::new(false)),
        disk_budget_bytes: 0,
    };
    // The fsync runs on the writer thread, so the injection is set there.
    ctx.db
        .run_blocking(|_store| {
            inject_fsync_failure(FsyncStep::TempFile);
            Ok(())
        })
        .unwrap();
    let outcome = sync_series(&ctx, &MockSeries).await;
    let store = thread.join(ctx.db);
    let files = store.rows("SELECT count(*) AS n FROM files", &[]).unwrap()[0]
        .int("n")
        .unwrap_or(0);
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(
        outcome.files, 0,
        "the failed file must not be counted as written"
    );
    let fatal = outcome.fatal.expect("an fsync failure must be fatal");
    assert!(fatal.contains("fsync failed"), "{fatal}");
    assert_eq!(files, 0, "no file may be registered");
}

#[test]
fn spot_copy_to_is_fatal_on_an_injected_fsync_failure() {
    for step in [FsyncStep::TempFile, FsyncStep::DirAfterRename] {
        let root = tempdir("spot-fsync");
        let conn = memory_connection("1", "64MB").unwrap();
        conn.execute_batch("CREATE TABLE src(n BIGINT)").unwrap();
        conn.execute_batch("INSERT INTO src VALUES (1)").unwrap();
        inject_fsync_failure(step);
        let result = copy_to(
            &conn,
            "SELECT * FROM src",
            &root,
            "tables/spot/swaps/ALL/2026-10.parquet",
            ("spot", "swaps", "2026-10"),
        );
        clear_fsync_failure();
        let message = result.unwrap_err().to_string();
        assert!(message.contains("fsync failed"), "{step:?}: {message}");
        assert_eq!(
            solos_data::db::fatal_class(&message),
            Some("fsync"),
            "the spot lane must classify an fsync failure as fatal"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
