//! Codec audit across a program-upgrade boundary (issue #58).
//!
//! `tests/data/codec-audit.json` carries real captured Phoenix Rise instructions on both sides
//! of an example upgrade boundary, including the new-market (`MarketAdded`) transaction. The
//! decoder follows a codec version schedule: every decoded row is stamped with the codec version
//! that produced it, resolved from the row's slot. A range spanning the upgrade decodes each
//! side under its own version, and forcing the wrong version on the post-upgrade side is
//! observable (and turns the audit assertion red).

mod common;

use common::{raw_fixture, raw_json, tempdir, write_raw_parquet};
use serde_json::Value;
use solos_data::decoder::codec::{Upgrade, codec_version_for};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::schema::TABLES;
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::file_hash;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/data/codec-audit.json"
);

struct Fixture {
    pre: Value,
    post: Value,
    upgrade_slot: i64,
    pre_version: &'static str,
    post_version: &'static str,
}

fn leak(text: &str) -> &'static str {
    Box::leak(text.to_owned().into_boxed_str())
}

fn fixture() -> Fixture {
    let text = std::fs::read_to_string(FIXTURE).unwrap_or_else(|e| panic!("read {FIXTURE}: {e}"));
    let doc: Value = serde_json::from_str(&text).unwrap();
    let upgrade = &doc["upgrade"];
    let transactions = doc["transactions"].as_array().unwrap();
    let mut pre = None;
    let mut post = None;
    for tx in transactions {
        let slot = tx["slot"].as_i64().unwrap();
        let boundary = upgrade["slot"].as_i64().unwrap();
        if slot < boundary {
            assert!(pre.replace(tx.clone()).is_none(), "one pre-upgrade tx");
        } else {
            assert!(post.replace(tx.clone()).is_none(), "one post-upgrade tx");
        }
    }
    Fixture {
        pre: pre.expect("a pre-upgrade transaction"),
        post: post.expect("a post-upgrade transaction"),
        upgrade_slot: upgrade["slot"].as_i64().unwrap(),
        pre_version: leak(upgrade["preVersion"].as_str().unwrap()),
        post_version: leak(upgrade["postVersion"].as_str().unwrap()),
    }
}

/// A decoded root holding the two fixture transactions, decoded under `schedule`.
fn decode(schedule: &[Upgrade]) -> std::path::PathBuf {
    let fx = fixture();
    let root = tempdir("codec-audit");
    let raw_dir = root.join("raw");
    let data_dir = root.join("decoded");
    std::fs::create_dir_all(raw_dir.join("staging")).unwrap();
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let pre = raw_fixture(&fx.pre);
    let post = raw_fixture(&fx.post);
    let path = raw_dir.join("staging/tx.parquet");
    write_raw_parquet(&conn, &path, &[raw_json(&pre), raw_json(&post)]);
    let catalog = serde_json::json!({
        "at": "fixture",
        "files": [{
            "path": "/old/root/staging/tx.parquet",
            "table_name": "transactions",
            "row_count": 2,
            "sha256": file_hash(&path).unwrap(),
            "created_at": "2026-10-04T03:00:00Z"
        }]
    });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string()).unwrap();
    let config = DecoderConfig {
        batch_size: 100,
        poll_ms: 100,
        checkpoint_interval_seconds: 0,
        codec_schedule: schedule.to_vec(),
        ..DecoderConfig::new(raw_dir, data_dir.clone())
    };
    run_decoder(&config, true, Arc::new(AtomicBool::new(false))).unwrap();
    data_dir
}

/// `(slot, decoder_version)` pairs of every row of `table`, parsed from the reader.
fn stamped(decoded: &Path, table: &str) -> Vec<(i64, String)> {
    let result = query_decoded(
        decoded,
        &format!("SELECT slot, decoder_version FROM {table}"),
    )
    .unwrap()
    .to_json();
    let value: Value = serde_json::from_str(&result).unwrap();
    value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let slot = row["slot"]
                .as_str()
                .map(|s| s.parse().unwrap())
                .or_else(|| row["slot"].as_i64())
                .unwrap();
            (slot, row["decoder_version"].as_str().unwrap().to_owned())
        })
        .collect()
}

fn signature_versions(decoded: &Path) -> Vec<(String, String)> {
    let result = query_decoded(
        decoded,
        "SELECT signature, decoder_version FROM decoded_transactions ORDER BY signature",
    )
    .unwrap()
    .to_json();
    let value: Value = serde_json::from_str(&result).unwrap();
    value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["signature"].as_str().unwrap().to_owned(),
                row["decoder_version"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[test]
fn an_upgrade_boundary_decodes_each_side_with_its_own_codec_version() {
    let fx = fixture();
    let schedule = [
        Upgrade {
            slot: 0,
            version: fx.pre_version,
        },
        Upgrade {
            slot: fx.upgrade_slot,
            version: fx.post_version,
        },
    ];
    let decoded = decode(&schedule);

    // Every decoded row of every table carries the codec version its slot resolves to.
    for table in TABLES {
        for (slot, version) in stamped(&decoded, table) {
            assert_eq!(
                version,
                codec_version_for(&schedule, slot),
                "{table} slot {slot}"
            );
        }
    }

    let versions = signature_versions(&decoded);
    let pre_sig = fx.pre["signature"].as_str().unwrap();
    let post_sig = fx.post["signature"].as_str().unwrap();
    assert_eq!(
        versions
            .iter()
            .find(|(sig, _)| sig == pre_sig)
            .map(|(_, v)| v.as_str()),
        Some(fx.pre_version)
    );
    assert_eq!(
        versions
            .iter()
            .find(|(sig, _)| sig == post_sig)
            .map(|(_, v)| v.as_str()),
        Some(fx.post_version)
    );

    // The new-market transaction on the post side publishes its `MarketAdded` event.
    let result = query_decoded(&decoded, "SELECT event_type, decoder_version FROM events")
        .unwrap()
        .to_json();
    let events: Value = serde_json::from_str(&result).unwrap();
    let added: Vec<&str> = events["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["event_type"].as_str() == Some("MarketAdded"))
        .map(|row| row["decoder_version"].as_str().unwrap())
        .collect();
    assert_eq!(added, vec![fx.post_version]);

    std::fs::remove_dir_all(decoded.parent().unwrap()).unwrap();
}

#[test]
fn forcing_the_wrong_post_upgrade_version_changes_the_stamp_and_is_observable() {
    let fx = fixture();
    // The mutation: the post-upgrade side is forced onto the pre-upgrade codec version.
    let wrong = [
        Upgrade {
            slot: 0,
            version: fx.pre_version,
        },
        Upgrade {
            slot: fx.upgrade_slot,
            version: fx.pre_version,
        },
    ];
    let decoded = decode(&wrong);
    let versions = signature_versions(&decoded);
    let post_sig = fx.post["signature"].as_str().unwrap();
    let stamped = versions
        .iter()
        .find(|(sig, _)| sig == post_sig)
        .map(|(_, v)| v.as_str())
        .unwrap();
    assert_eq!(stamped, fx.pre_version, "the forced version is stamped");
    assert_ne!(stamped, fx.post_version, "and differs from the right one");
    std::fs::remove_dir_all(decoded.parent().unwrap()).unwrap();
}
