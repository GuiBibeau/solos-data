//! Golden day hash of the checked-in Phoenix fixture (ADR-0017).
//!
//! Decodes `tests/data/phoenix-events.json` through the real decoder into a fresh temp directory
//! with a fixed `decoded_at`. Two fresh directories must share the content digest and every
//! registered file's SHA-256. The digest is row equivalence and omits the fixed `decoded_at`
//! clock, `source_file` provenance and `created_by`. `files.sha256` is the pinned
//! DuckDB 1.4.5 byte check, so a writer-option change fails even when the rows do not. The test
//! publishes into the temp directory; it does not seal a partition.
//!
//! After a decoder or writer change that should move either hash,
//! `SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day` rewrites the committed file.

mod common;

use common::{golden_fixtures, raw_fixture, raw_json, tempdir, write_raw_parquet};
use solos_data::decoder::digest::content_digest;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::decoder::source::source_path;
use solos_data::fsutil::file_hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/data/golden-day.sha256"
);

/// Far enough past the maintenance clock (`Instant::now() - 3600s` at start) that one batch
/// does not compact. Compaction names files with a random id; the golden path is the publisher.
const NO_MAINTENANCE_SECS: u64 = 86_400 * 365;

/// Fixture clock written into `decoded_at`. Production does not set this.
const FIXTURE_DECODED_AT: &str = "2026-10-04T00:00:00.000Z";

struct Day {
    root: PathBuf,
    digest: String,
    files: Vec<(String, String)>,
}

struct Golden {
    digest: String,
    files: Vec<(String, String)>,
}

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

fn registered_file_hashes(root: &Path) -> Vec<(String, String)> {
    let catalog: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json")).unwrap()).unwrap();
    let mut files: Vec<(String, String)> = catalog["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| {
            let relative = file["path"].as_str().unwrap().to_owned();
            let path = source_path(root, &relative).unwrap();
            (relative, file_hash(&path).unwrap())
        })
        .collect();
    files.sort();
    files
}

fn decode_day(bytes: &[u8], hash: &str, count: u64) -> Day {
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
        decoded_at: Some(FIXTURE_DECODED_AT.into()),
        ..DecoderConfig::new(raw, decoded.clone())
    };
    run_decoder(&config, true, Arc::new(AtomicBool::new(false))).unwrap();
    let digest = content_digest(&decoded).unwrap();
    let files = registered_file_hashes(&decoded);
    Day {
        root,
        digest,
        files,
    }
}

fn committed() -> Golden {
    let text =
        std::fs::read_to_string(GOLDEN).unwrap_or_else(|error| panic!("read {GOLDEN}: {error}"));
    let mut digest = None;
    let mut files = Vec::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("digest") => digest = Some(parts.next().unwrap().to_owned()),
            Some("file") => files.push((
                parts.next().unwrap().to_owned(),
                parts.next().unwrap().to_owned(),
            )),
            other => panic!("bad golden line {other:?}"),
        }
    }
    files.sort();
    Golden {
        digest: digest.unwrap_or_else(|| panic!("{GOLDEN} has no digest")),
        files,
    }
}

fn write_golden(day: &Day) {
    let mut body = format!(
        "# Content digest v2 and pinned DuckDB 1.4.5 file hashes of tests/data/phoenix-events.json.\n\
         # decoded_at is fixed at {FIXTURE_DECODED_AT}. The digest omits that clock, source_file and created_by.\n\
         # files.sha256 is the raw file hash. Refresh: SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day\n\
         digest {}\n",
        day.digest
    );
    for (path, hash) in &day.files {
        body.push_str(&format!("file {path} {hash}\n"));
    }
    std::fs::write(GOLDEN, body).unwrap();
}

#[test]
fn golden_day_digest_matches_across_fresh_roots() {
    let (bytes, hash, count) = raw_fixture_bytes();
    let left = decode_day(&bytes, &hash, count);
    let right = decode_day(&bytes, &hash, count);
    assert_eq!(
        left.digest, right.digest,
        "two fresh decodes of the fixture day produced different content digests"
    );
    assert_eq!(
        left.files, right.files,
        "files.sha256 differ across fresh roots on the pinned writer"
    );
    assert_eq!(left.digest.len(), 64);
    assert!(!left.files.is_empty());
    if std::env::var("SOLOS_DATA_REFRESH_GOLDEN_DAY").is_ok() {
        write_golden(&left);
        std::fs::remove_dir_all(&left.root).unwrap();
        std::fs::remove_dir_all(&right.root).unwrap();
        return;
    }
    let committed = committed();
    assert_eq!(
        left.digest, committed.digest,
        "golden day digest changed; refresh with SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day"
    );
    assert_eq!(
        left.files, committed.files,
        "files.sha256 changed under the pinned DuckDB build; writer options drifted. Refresh with SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day"
    );
    std::fs::remove_dir_all(&left.root).unwrap();
    std::fs::remove_dir_all(&right.root).unwrap();
}
