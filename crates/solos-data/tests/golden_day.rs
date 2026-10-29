//! Golden day hash of the checked-in Phoenix fixture (ADR-0016).
//!
//! Decodes `tests/data/phoenix-events.json` through the real decoder into a fresh temp directory
//! and compares the canonical content digest to `tests/data/golden-day.sha256`. A second fresh
//! directory must produce the same digest. The test publishes into the temp directory; it does
//! not seal a partition.
//!
//! After a decoder or writer change that should move the digest,
//! `SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day` rewrites the committed hash.

mod common;

use common::{golden_fixtures, raw_fixture, raw_json, tempdir, write_raw_parquet};
use solos_data::decoder::digest::content_digest;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::file_hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/data/golden-day.sha256"
);

/// Far enough past the maintenance clock (`Instant::now() - 3600s` at start) that one batch
/// does not compact. Compaction would still be deterministic in the digest, and it names files
/// with a random id; the golden path is the publisher.
const NO_MAINTENANCE_SECS: u64 = 86_400 * 365;

fn raw_fixture_bytes() -> (Vec<u8>, String, u64) {
    let fixtures = golden_fixtures();
    let dir = tempdir("golden-raw");
    let path = dir.join("day.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let rows: Vec<_> = fixtures.iter().map(|f| raw_json(&raw_fixture(f))).collect();
    let count = u64::try_from(rows.len()).unwrap();
    write_raw_parquet(&conn, &path, &rows);
    let bytes = std::fs::read(&path).unwrap();
    let hash = file_hash(&path).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    (bytes, hash, count)
}

fn decode_day(bytes: &[u8], hash: &str, count: u64) -> (PathBuf, String) {
    let root = tempdir("golden-day");
    let raw = root.join("raw");
    let decoded = root.join("decoded");
    std::fs::create_dir_all(raw.join("staging")).unwrap();
    std::fs::write(raw.join("staging/day.parquet"), bytes).unwrap();
    let catalog = serde_json::json!({
        "at": "fixture",
        "files": [{
            "path": "/old/root/staging/day.parquet",
            "table_name": "transactions",
            "row_count": count,
            "sha256": hash,
            "created_at": "2026-10-04T00:00:00.000Z"
        }]
    });
    std::fs::write(raw.join("catalog.json"), catalog.to_string()).unwrap();
    let config = DecoderConfig {
        batch_size: 10_000,
        poll_ms: 1,
        checkpoint_interval_seconds: NO_MAINTENANCE_SECS,
        compaction_interval_seconds: NO_MAINTENANCE_SECS,
        ..DecoderConfig::new(raw, decoded.clone())
    };
    run_decoder(&config, true, Arc::new(AtomicBool::new(false))).unwrap();
    let digest = content_digest(&decoded).unwrap();
    (root, digest)
}

fn committed_digest() -> String {
    std::fs::read_to_string(GOLDEN)
        .unwrap_or_else(|error| panic!("read {GOLDEN}: {error}"))
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap_or_else(|| panic!("{GOLDEN} has no digest line"))
        .trim()
        .to_owned()
}

fn write_golden(digest: &str) {
    let body = format!(
        "# Canonical content digest of tests/data/phoenix-events.json decoded by the pinned writer.\n\
         # Excludes decoded_at and Parquet created_by. Not a file SHA-256.\n\
         # Refresh: SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day\n\
         {digest}\n"
    );
    std::fs::write(GOLDEN, body).unwrap();
}

#[test]
fn golden_day_digest_matches_across_fresh_roots() {
    let (bytes, hash, count) = raw_fixture_bytes();
    let (left_root, left) = decode_day(&bytes, &hash, count);
    let (right_root, right) = decode_day(&bytes, &hash, count);
    std::fs::remove_dir_all(&left_root).unwrap();
    std::fs::remove_dir_all(&right_root).unwrap();
    assert_eq!(
        left, right,
        "two fresh decodes of the fixture day produced different content digests"
    );
    assert_eq!(left.len(), 64);
    if std::env::var("SOLOS_DATA_REFRESH_GOLDEN_DAY").is_ok() {
        write_golden(&left);
        return;
    }
    let committed = committed_digest();
    assert_eq!(
        left, committed,
        "golden day digest changed; refresh with SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day"
    );
}
