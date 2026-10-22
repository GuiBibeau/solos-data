//! Batches that span several raw sources publish what the one-source-per-batch loop publishes.

mod common;

use common::*;
use serde_json::Value;
use solos_data::decoder::schema::schema_static;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::file_hash;
use solos_data::store::Store;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// One raw file per entry of `files` (fixture indexes), registered newest first by name.
fn raw_root(root: &Path, files: &[&[usize]]) {
    let fixtures = golden_fixtures();
    let raw_dir = root.join("raw");
    std::fs::create_dir_all(raw_dir.join("staging")).unwrap();
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let mut entries = Vec::new();
    for (i, indexes) in files.iter().enumerate() {
        let rows: Vec<Value> = indexes
            .iter()
            .map(|&n| raw_json(&raw_fixture(&fixtures[n])))
            .collect();
        // Ranges in the name give the consumption order: file 0 holds the newest range.
        let end = 1_000 - i as i64 * 10;
        let relative = format!("staging/{}-{end}-f{i}.parquet", end - 5);
        let path = raw_dir.join(&relative);
        write_raw_parquet(&conn, &path, &rows);
        entries.push(serde_json::json!({ "path": relative, "table_name": "transactions", "row_count": rows.len(), "sha256": file_hash(&path).unwrap(), "created_at": format!("2026-10-04T0{i}:00:00Z") }));
    }
    let catalog = serde_json::json!({ "at": "fixture", "files": entries });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
}

fn config(root: &Path, batch_size: u64, max_batch_sources: usize) -> DecoderConfig {
    DecoderConfig {
        batch_size,
        max_batch_sources,
        poll_ms: 100,
        checkpoint_interval_seconds: 0,
        ..DecoderConfig::new(root.join("raw"), root.join("decoded"))
    }
}

fn once(config: &DecoderConfig) {
    run_decoder(config, true, Arc::new(AtomicBool::new(false))).unwrap();
}

/// Everything a reader and the next decoder run see: latest revisions of every table, the
/// processed revisions and the source offsets.
fn published(root: &Path) -> Vec<String> {
    let decoded = root.join("decoded");
    let mut out = Vec::new();
    for sql in [
        "SELECT signature, source_hash, slot, status, event_count, error_count FROM decoded_transactions ORDER BY ALL",
        "SELECT event_id, signature, source_hash, event_type, committed FROM events ORDER BY ALL",
        "SELECT event_id, signature, price_ticks::VARCHAR AS p FROM fills ORDER BY ALL",
        "SELECT event_id, signature FROM order_events ORDER BY ALL",
        "SELECT event_id, signature FROM funding_events ORDER BY ALL",
    ] {
        out.push(
            solos_data::decoder::reader::query_decoded(&decoded, sql)
                .unwrap()
                .to_json(),
        );
    }
    let store = Store::open(&decoded, schema_static()).unwrap();
    for sql in [
        "SELECT * FROM processed ORDER BY ALL",
        "SELECT source_hash, row_offset FROM sources ORDER BY ALL",
    ] {
        let rows = store.rows(sql, &[]).unwrap();
        out.push(Value::Array(rows.into_iter().map(|r| r.to_value()).collect()).to_string());
    }
    store.close().unwrap();
    // `catalogAt` differs between roots; the rows do not.
    out.iter()
        .map(|text| {
            let mut value: Value = serde_json::from_str(text).unwrap();
            if let Some(obj) = value.as_object_mut() {
                obj.remove("catalogAt");
            }
            value.to_string()
        })
        .collect()
}

fn batch_number(root: &Path) -> i64 {
    let store = Store::open(&root.join("decoded"), schema_static()).unwrap();
    let batch = store.get("batch").unwrap().unwrap();
    store.close().unwrap();
    batch
        .as_str()
        .map(|s| s.parse().unwrap())
        .or_else(|| batch.as_i64())
        .unwrap()
}

#[test]
fn one_batch_over_several_sources_publishes_what_one_batch_per_source_does() {
    let files: &[&[usize]] = &[&[0, 1], &[2, 3, 4], &[5, 6]];
    let batched = tempdir("decode-batched");
    raw_root(&batched, files);
    once(&config(&batched, 100, 32));
    assert_eq!(batch_number(&batched), 1, "all three sources in one batch");
    let single = tempdir("decode-single");
    raw_root(&single, files);
    let one_by_one = config(&single, 100, 1);
    for _ in 0..3 {
        once(&one_by_one);
    }
    assert_eq!(batch_number(&single), 3);
    once(&one_by_one);
    once(&config(&batched, 100, 32));
    assert_eq!(published(&batched), published(&single));
    std::fs::remove_dir_all(batched).unwrap();
    std::fs::remove_dir_all(single).unwrap();
}

#[test]
fn a_batch_stops_at_the_transaction_bound_inside_a_source_and_resumes_there() {
    let files: &[&[usize]] = &[&[0, 1], &[2, 3, 4], &[5, 6]];
    let capped = tempdir("decode-capped");
    raw_root(&capped, files);
    let small = config(&capped, 4, 32);
    once(&small);
    let store = Store::open(&capped.join("decoded"), schema_static()).unwrap();
    let offsets: Vec<i64> = store
        .rows(
            "SELECT row_offset FROM sources ORDER BY row_offset DESC",
            &[],
        )
        .unwrap()
        .iter()
        .filter_map(|r| r.int("row_offset"))
        .collect();
    store.close().unwrap();
    assert_eq!(offsets, vec![2, 2], "file 0 whole, two rows of file 1");
    once(&small);
    once(&small);
    let single = tempdir("decode-capped-single");
    raw_root(&single, files);
    for _ in 0..4 {
        once(&config(&single, 100, 1));
    }
    assert_eq!(published(&capped), published(&single));
    std::fs::remove_dir_all(capped).unwrap();
    std::fs::remove_dir_all(single).unwrap();
}

#[test]
fn a_signature_seen_twice_ends_the_batch_and_the_newer_publication_still_wins() {
    let root = tempdir("decode-repeat");
    raw_root(&root, &[&[0, 1], &[2]]);
    // A third, older source repeats fixture 0 with another tx_index; it is consumed last.
    let raw_dir = root.join("raw");
    let first = raw_dir.join("staging/995-1000-f0.parquet");
    let older = raw_dir.join("staging/975-980-f2.parquet");
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT * REPLACE(7 AS tx_index) FROM read_parquet('{}') LIMIT 1) TO '{}' (FORMAT PARQUET)",
        first.display(),
        older.display()
    ))
    .unwrap();
    let mut catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(raw_dir.join("catalog.json")).unwrap())
            .unwrap();
    catalog["files"].as_array_mut().unwrap().push(serde_json::json!({ "path": "staging/975-980-f2.parquet", "table_name": "transactions", "row_count": 1, "sha256": file_hash(&older).unwrap(), "created_at": "2026-10-03T00:00:00Z" }));
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
    let config = config(&root, 100, 32);
    once(&config);
    let store = Store::open(&root.join("decoded"), schema_static()).unwrap();
    let consumed = store.rows("SELECT * FROM sources", &[]).unwrap().len();
    store.close().unwrap();
    assert_eq!(consumed, 2, "the repeating source waits for the next batch");
    once(&config);
    assert_eq!(batch_number(&root), 2);
    let result: Value = serde_json::from_str(
        &solos_data::decoder::reader::query_decoded(
            &root.join("decoded"),
            "SELECT count(*) AS n, max(tx_index) AS t FROM decoded_transactions",
        )
        .unwrap()
        .to_json(),
    )
    .unwrap();
    assert_eq!(result["rows"][0]["n"], Value::String("3".into()));
    assert_eq!(
        result["rows"][0]["t"], 0,
        "the older publication is skipped"
    );
    std::fs::remove_dir_all(root).unwrap();
}
