//! Ported from `tests/decode.test.ts`.

mod common;

use base64::{Engine, engine::general_purpose::STANDARD};
use common::*;
use serde_json::Value;
use solos_data::decoder::extract::extract_groups;
use solos_data::decoder::normalize::normalize;
use solos_data::decoder::publish::{SourceProgress, publish, recover};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::schema::schema_static;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::file_hash;
use solos_data::store::Store;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[test]
fn official_phoenix_golden_events_instruction_attribution_and_failed_transaction_isolation() {
    let fixtures = golden_fixtures();
    for fixture in &fixtures {
        let tx = raw_fixture(fixture);
        let meta: Value = serde_json::from_str(tx.meta_json.as_deref().unwrap()).unwrap();
        let groups = phoenix_codec::decode_all(
            extract_groups(tx.tx_b64.as_deref().unwrap(), &meta).unwrap(),
        )
        .unwrap();
        let expected = fixture["eventsCount"].as_u64().unwrap() as usize;
        assert_eq!(
            groups.iter().map(|g| g.events.len()).sum::<usize>(),
            expected,
            "{}",
            fixture["signature"]
        );
        assert!(
            groups
                .iter()
                .all(|g| g.errors.is_empty() && g.attribution == "stack_height")
        );
        let rows = normalize(&tx, "h", &groups, "source").unwrap();
        assert_eq!(rows.get("events").len(), expected);
        let failed_rows = normalize(&failed(&tx), "h", &groups, "source").unwrap();
        assert!(failed_rows.get("fills").is_empty());
        assert!(failed_rows.get("order_events").is_empty());
        assert!(failed_rows.get("funding_events").is_empty());
        assert!(
            failed_rows
                .get("events")
                .iter()
                .all(|e| e.get("committed") == Some(&Value::Bool(false)))
        );
    }
    let tx = raw_fixture(&fixtures[0]);
    let meta: Value = serde_json::from_str(tx.meta_json.as_deref().unwrap()).unwrap();
    let mut group = extract_groups(tx.tx_b64.as_deref().unwrap(), &meta)
        .unwrap()
        .remove(0);
    group.logs[1] = STANDARD.encode(hex::decode("8de6d6f209d1cfaa0000000001000000ff").unwrap());
    let bad = phoenix_codec::decode(group).unwrap();
    assert!(bad.events.is_empty());
    assert!(!bad.errors.is_empty());
}

#[test]
fn atomic_publication_survives_restart_relocation_and_corrected_revisions_without_duplicate_fills()
{
    let fixtures = golden_fixtures();
    let root = tempdir("phoenix-decode");
    let mut store = Store::open(&root, schema_static()).unwrap();
    let tx = raw_fixture(&fixtures[0]);
    let meta: Value = serde_json::from_str(tx.meta_json.as_deref().unwrap()).unwrap();
    let groups =
        phoenix_codec::decode_all(extract_groups(tx.tx_b64.as_deref().unwrap(), &meta).unwrap())
            .unwrap();
    let mut rows = normalize(&tx, "hash1", &groups, "source").unwrap();
    let fills = rows.get("fills").len();
    assert!(fills > 0);
    rows.get_mut("fills")[0].set("price_ticks", "18446744073709551615");
    publish(
        &mut store,
        &rows,
        &SourceProgress {
            hash: "source".into(),
            path: "raw".into(),
            offset: 1,
            at: None,
            seen: None,
        },
    )
    .unwrap();
    let result: Value = serde_json::from_str(
        &query_decoded(
            &root,
            "SELECT count(*) AS n, max(price_ticks)::VARCHAR AS price FROM fills",
        )
        .unwrap()
        .to_json(),
    )
    .unwrap();
    assert_eq!(result["rows"][0]["n"], Value::String(fills.to_string()));
    assert_eq!(result["rows"][0]["price"], "18446744073709551615");
    std::fs::write(root.join("orphan.parquet"), "incomplete").unwrap();
    store.close().unwrap();
    let mut store = Store::open(&root, schema_static()).unwrap();
    recover(&mut store).unwrap();
    assert_eq!(
        store.rows("SELECT row_offset FROM sources", &[]).unwrap()[0].get("row_offset"),
        Some(&Value::String("1".into()))
    );
    assert!(!root.join("orphan.parquet").exists());
    let correction = normalize(&failed(&tx), "hash2", &groups, "source2").unwrap();
    publish(
        &mut store,
        &correction,
        &SourceProgress {
            hash: "source2".into(),
            path: "raw2".into(),
            offset: 1,
            at: None,
            seen: None,
        },
    )
    .unwrap();
    assert_eq!(count(&root, "SELECT count(*) AS n FROM fills"), 0);
    store.close().unwrap();
    let moved = root.with_file_name(format!(
        "{}-moved",
        root.file_name().unwrap().to_string_lossy()
    ));
    std::fs::rename(&root, &moved).unwrap();
    assert_eq!(
        count(&moved, "SELECT count(*) AS n FROM decoded_transactions"),
        1
    );
    std::fs::remove_dir_all(moved).unwrap();
}

#[test]
fn decoder_consumes_raw_publications_once_across_restarts_and_rebases_the_raw_catalog() {
    let fixtures = golden_fixtures();
    let root = tempdir("phoenix-consumer");
    let raw_dir = root.join("raw");
    let data_dir = root.join("decoded");
    std::fs::create_dir_all(raw_dir.join("staging")).unwrap();
    let fixture = raw_fixture(&fixtures[0]);
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let path = raw_dir.join("staging/tx.parquet");
    write_raw_parquet(&conn, &path, &[raw_json(&fixture)]);
    let catalog = serde_json::json!({ "at": "fixture", "files": [{ "path": "/old/root/staging/tx.parquet", "table_name": "transactions", "row_count": 1, "sha256": file_hash(&path).unwrap(), "created_at": "2026-10-04T03:00:00Z" }] });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
    let config = DecoderConfig {
        raw_dir: raw_dir.clone(),
        data_dir: data_dir.clone(),
        batch_size: 100,
        poll_ms: 100,
    };
    let stop = Arc::new(AtomicBool::new(false));
    run_decoder(&config, true, Arc::clone(&stop)).unwrap();
    run_decoder(&config, true, Arc::clone(&stop)).unwrap();
    assert_eq!(
        count(&data_dir, "SELECT count(*) AS n FROM decoded_transactions"),
        1
    );
    assert_eq!(
        count(&data_dir, "SELECT count(*) AS n FROM events"),
        fixtures[0]["eventsCount"].as_i64().unwrap()
    );
    // Newest-first traversal must never undo a newer correction with old files.
    let older = raw_dir.join("staging/older.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT * REPLACE(7 AS tx_index) FROM read_parquet('{}')) TO '{}' (FORMAT PARQUET)",
        path.display(),
        older.display()
    ))
    .unwrap();
    let catalog = serde_json::json!({ "at": "fixture2", "files": [{ "path": older.to_string_lossy(), "table_name": "transactions", "row_count": 1, "sha256": file_hash(&older).unwrap(), "created_at": "2026-10-04T02:00:00Z" }] });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
    run_decoder(&config, true, stop).unwrap();
    let result: Value = serde_json::from_str(
        &query_decoded(&data_dir, "SELECT tx_index FROM decoded_transactions")
            .unwrap()
            .to_json(),
    )
    .unwrap();
    assert_eq!(result["rows"][0]["tx_index"], 0);
    std::fs::remove_dir_all(root).unwrap();
}
