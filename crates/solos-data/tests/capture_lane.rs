//! The whole capture lane's fatal path (issue #77): `run_capture` must return the recorded
//! `lane_fatal` error (`capture.rs:170`) instead of writing a final status when a lane's write
//! fails fatally. The fsync failure is injected process-wide because `run_capture` spawns its
//! own writer thread the thread-local injection cannot reach. Offline, against an in-process
//! HTTP server.

mod augment_common;

use augment_common::{Response, Server, tempdir};
use serde_json::json;
use solos_data::augment::capture::run_capture;
use solos_data::augment::config::{load_augment_config, repository_config_path};
use solos_data::fsutil::{FsyncStep, clear_fsync_failure_process, inject_fsync_failure_process};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[test]
fn run_capture_returns_the_lane_fatal_error() {
    let server = Server::start();
    let real_now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    server.route("/info", move |request| {
        let body = request.json();
        assert_eq!(body["type"], "candleSnapshot");
        let coin = body["req"]["coin"].as_str().unwrap().to_owned();
        let candle = |open: i64| {
            json!({ "t": open, "T": open + 59_999, "s": coin, "i": "1m", "o": "100",
                    "c": "101", "h": "102", "l": "99", "v": "5", "n": 3 })
        };
        Response::json(&json!([
            candle(real_now_ms - 120_000),
            candle(real_now_ms - 60_000),
        ]))
    });
    let root = tempdir("solos-run-capture-fatal");
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.clone();
    config.requests_per_second = 50.0;
    config.symbols.retain(|s| s.phoenix == "SOL");
    config.sources.hyperliquid.base_url = server.base.clone();
    config.sources.hyperliquid.enabled = true;
    config.sources.hyperliquid.candles = true;
    config.sources.hyperliquid.asset_contexts = false;
    config.sources.elfa.enabled = false;
    config.sources.elfa.auto.enabled = false;

    // The first Hyperliquid candle write fails its parent-directory fsync (creating
    // `tables/` under the fresh root), the candle loop records the fatal and sets the stop
    // flag, and `run_capture` returns that error instead of a status. `ParentDir` is chosen
    // because `run_capture`'s own `recover` step fsyncs the catalog's temp file and directory
    // before the loop starts; a parent-directory failure is only reached by the candle write.
    inject_fsync_failure_process(FsyncStep::ParentDir);
    // A missed fsync arm, or an fsync error that is no longer classified as fatal, leaves the
    // lane in its interval sleep instead of returning. Bound the wait so that hang fails the
    // test. The fatal path returns on the first candle write, well inside this limit.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_lane = Arc::clone(&stop);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(run_capture(&config, stop_for_lane));
    });
    let result = match rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            panic!(
                "run_capture did not return within 30s; a missed fsync arm or a non-fatal fsync leaves the lane running"
            );
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("run_capture thread ended without a result");
        }
    };
    clear_fsync_failure_process();
    let error = result.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("fsync failed"), "{message}");
    // The status loop may have written a running status, but the fatal path must not write the
    // final status (the one marked `running: false`).
    if let Ok(body) = std::fs::read_to_string(root.join("status-capture.json")) {
        assert!(
            !body.contains("\"running\":false"),
            "a fatal lane must not write a final status: {body}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
