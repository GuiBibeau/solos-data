//! Peak-RSS benchmark for the streamed content digest (issue #67).
//!
//! `digest_peak_rss_on_10m_rows` is ignored in the normal suite: it builds a synthetic day of
//! 10M `decoded_transactions` rows and re-runs this test binary in a child process that only
//! computes the digest, so the child's peak RSS is the digest's own, not the generator's.
//!
//! Run it with:
//!
//! ```sh
//! CC=gcc CXX=g++ cargo test --release --test digest_rss -- --ignored digest_peak_rss_on_10m_rows --nocapture
//! ```
//!
//! The child bounds DuckDB's sort buffer with `SOLOS_DATA_QUERY_MEMORY` (default 4GB, set to
//! 512MB here so the benchmark stays cheap and shows the spill path) while the digest itself
//! holds one row at a time. The reported peak RSS is the child's `VmHWM`.

mod common;

use common::tempdir;
use serde_json::json;
use solos_data::decoder::digest::content_digest;
use solos_data::store::sql_string;
use std::path::PathBuf;
use std::process::Command;

const ROWS_ENV: &str = "SOLOS_DIGEST_RSS_ROWS";
const DIR_ENV: &str = "SOLOS_DIGEST_RSS_DIR";
const DEFAULT_ROWS: u64 = 10_000_000;

/// Peak resident set size of this process, in kilobytes, from `/proc/self/status`.
fn peak_rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .expect("VmHWM is an integer");
        }
    }
    panic!("VmHWM not found in /proc/self/status");
}

/// A synthetic decoded root with `rows` unique `decoded_transactions` rows.
fn synthetic_day(rows: u64) -> PathBuf {
    let root = tempdir("digest-rss");
    let dir = root.join("tables/decoded_transactions/epoch=0");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("day.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT 'sig-' || n::VARCHAR AS signature, 'src-hash' AS source_hash, n::BIGINT AS slot,
            0::INTEGER AS tx_index, false AS single_in_slot, n::BIGINT AS block_time, true AS committed,
            'ok' AS status, 0::INTEGER AS event_count, 0::INTEGER AS error_count, 'src' AS source_file,
            '2026-10-04T00:00:00.000Z' AS decoded_at, 'v' AS decoder_version
        FROM range({rows}) r(n))
        TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
    std::fs::write(
        root.join("catalog.json"),
        json!({ "at": "fixture", "files": [{
            "path": "tables/decoded_transactions/epoch=0/day.parquet",
            "table_name": "decoded_transactions",
            "batch_id": 1
        }] })
        .to_string(),
    )
    .unwrap();
    root
}

/// Child entry point: compute the digest on the directory named by `SOLOS_DIGEST_RSS_DIR` and
/// print one `digest=<hex> peak_rss_kb=<n>` line. Run under `--exact digest_rss_child`.
#[test]
fn digest_rss_child() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        // The parent benchmark spawns this test with `SOLOS_DIGEST_RSS_DIR`; in the normal
        // suite it is a no-op.
        return;
    };
    let digest = content_digest(std::path::Path::new(&dir)).unwrap();
    assert_eq!(digest.len(), 64);
    println!("digest={digest} peak_rss_kb={}", peak_rss_kb());
}

#[test]
#[ignore = "10M-row synthetic day; run explicitly with the command in this file's header"]
fn digest_peak_rss_on_10m_rows() {
    let rows: u64 = std::env::var(ROWS_ENV)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(DEFAULT_ROWS);
    assert!(
        rows >= DEFAULT_ROWS,
        "the benchmark needs at least 10M rows"
    );
    let dir = synthetic_day(rows);
    let exe = std::env::current_exe().unwrap();
    let memory = std::env::var("SOLOS_DIGEST_RSS_MEMORY").unwrap_or_else(|_| "512MB".into());
    let output = Command::new(exe)
        .arg("--exact")
        .arg("digest_rss_child")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(DIR_ENV, &dir)
        .env("SOLOS_DATA_QUERY_MEMORY", &memory)
        .output()
        .expect("spawn the digest child");
    assert!(
        output.status.success(),
        "child failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find(|line| line.contains("digest="))
        .expect("child printed a digest line");
    let (digest, rss) = line
        .split_once("digest=")
        .and_then(|(_, rest)| rest.split_once(" peak_rss_kb="))
        .expect("digest line has both fields");
    println!("SYNTHETIC_DAY_ROWS={rows}");
    println!("QUERY_MEMORY_LIMIT={memory}");
    println!("CONTENT_DIGEST={digest}");
    println!("PEAK_RSS_KB={rss}");
    std::fs::remove_dir_all(dir).unwrap();
}
