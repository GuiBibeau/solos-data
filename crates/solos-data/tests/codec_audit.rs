//! Codec audit across a program-upgrade boundary (issue #58).
//!
//! `tests/data/codec-audit.json` carries two real captured Phoenix Rise transactions copied from
//! the existing `tests/data/phoenix-events.json` capture, placed on either side of an *example*
//! upgrade boundary. The boundary slot (428000000) and the pre-upgrade version (0.6.11) are
//! invented for the exercise; the linked codec (0.6.12) decodes both sides, so the per-slot
//! `decoder_version` is only a stamp, not a different decoder. The decoder follows a codec
//! version schedule: every decoded row is stamped with the codec version that produced it,
//! resolved from the row's slot. A range spanning the upgrade decodes each side under its own
//! stamp, and forcing the wrong stamp on the post-upgrade side is observable (and turns the
//! audit assertion red). The fixture's `expected` block (instruction discriminators, event
//! counts, event variants) is asserted here against the linked codec, including a borsh layout
//! size check per event.

mod common;

use base64::{Engine, engine::general_purpose::STANDARD};
use borsh::BorshDeserialize;
use common::{golden_fixtures, raw_fixture, raw_json, tempdir, write_raw_parquet};
use phoenix_codec::phoenix_rise_events::market_events::{
    MarketEventHeader, SlotContextEvent, Symbol,
};
use phoenix_codec::phoenix_rise_events::{
    LOG_EVENT_LENGTHS_INSTRUCTION_TAG, LOG_INSTRUCTION_TAG, MarketEvent, PhoenixLogInstruction,
    PhoenixLogInstructionKind,
};
use serde_json::Value;
use solos_data::decoder::codec::{Upgrade, codec_version_for};
use solos_data::decoder::normalize::{RawTransaction, normalize_at_version};
use solos_data::decoder::reader::query_decoded;
use solos_data::decoder::schema::{TABLES, VERSION};
use solos_data::decoder::service::{DecoderConfig, run_decoder};
use solos_data::fsutil::file_hash;
use std::collections::{BTreeMap, VecDeque};
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

/// `(stack_path, data_base64)` for every instruction of one fixture transaction.
fn instructions_of(tx: &Value) -> Vec<(String, String)> {
    tx["instructions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ix| {
            let path = ix["stackPath"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_u64().unwrap().to_string())
                .collect::<Vec<_>>()
                .join(".");
            (path, ix["dataBase64"].as_str().unwrap().to_owned())
        })
        .collect()
}

/// The 8-byte little-endian instruction discriminant, rendered like the fixture (`0x…`).
fn discriminant_hex(data_base64: &str) -> String {
    let bytes = STANDARD.decode(data_base64).unwrap();
    let tag = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    format!("0x{tag:016x}")
}

/// The Rust variant name of a decoded [`MarketEvent`].
fn variant_name(event: &MarketEvent) -> String {
    format!("{event:?}").split('(').next().unwrap().to_owned()
}

/// Decode every Phoenix log group of one fixture transaction through the linked codec,
/// exactly like the extractor does (log instructions grouped under their parent instruction).
fn decode_groups(tx: &Value) -> Vec<phoenix_codec::DecodedGroup> {
    let mut groups: Vec<phoenix_codec::Group> = Vec::new();
    for (path, data_base64) in instructions_of(tx) {
        let depth = path.split('.').count();
        if depth < 2 {
            // The top-level Phoenix instruction is not a log payload; only its CPI children are.
            continue;
        }
        let parent = path.rsplit_once('.').unwrap().0.to_owned();
        let index = match groups.iter().rposition(|g| g.path == parent) {
            Some(index) => index,
            None => {
                groups.push(phoenix_codec::Group {
                    path: parent.clone(),
                    logs: Vec::new(),
                    attribution: "stack_height".into(),
                });
                groups.len() - 1
            }
        };
        groups[index].logs.push(data_base64);
    }
    phoenix_codec::decode_all(groups).unwrap()
}

/// Split the length-prefixed log batches, decode every event back into the linked codec's
/// `MarketEvent`, and return `(variant, wire_len)` per event. Re-serializing each event with
/// borsh must reproduce the length prefix recorded on the wire: that is the layout check, so a
/// codec bump that changes a field's size (or the enum discriminant) turns the audit red.
fn event_layout(tx: &Value) -> Vec<(String, usize)> {
    let mut lengths_by_batch = BTreeMap::<u32, VecDeque<Vec<usize>>>::new();
    let mut logs = Vec::<(u32, u32, Vec<u8>)>::new();
    for (_, data_base64) in instructions_of(tx) {
        let bytes = STANDARD.decode(data_base64).unwrap();
        let Some(ix) = PhoenixLogInstruction::from_instruction_data(&bytes) else {
            continue;
        };
        let batch = u32::from_le_bytes(ix.data[0..4].try_into().unwrap());
        let count = u32::from_le_bytes(ix.data[4..8].try_into().unwrap());
        match ix.kind {
            PhoenixLogInstructionKind::LogEventLengths => {
                let lengths = ix.data[8..]
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()) as usize)
                    .collect();
                lengths_by_batch
                    .entry(batch)
                    .or_default()
                    .push_back(lengths);
            }
            PhoenixLogInstructionKind::Log => logs.push((batch, count, ix.data.to_vec())),
        }
    }
    let mut out = Vec::new();
    for (batch, count, data) in logs {
        let lengths = lengths_by_batch
            .get_mut(&batch)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| panic!("batch {batch} has no length envelope"));
        assert_eq!(
            lengths.len(),
            count as usize,
            "batch {batch} length count matches the log header"
        );
        let mut offset = 8usize;
        for len in lengths {
            let slice = &data[offset..offset + len];
            let event = MarketEvent::try_from_slice(slice)
                .unwrap_or_else(|e| panic!("batch {batch}: decode event: {e}"));
            let serialized = borsh::to_vec(&event).unwrap();
            assert_eq!(
                serialized.len(),
                len,
                "batch {batch}: {} wire size changed",
                variant_name(&event)
            );
            out.push((variant_name(&event), len));
            offset += len;
        }
        assert_eq!(offset, data.len(), "batch {batch} consumed the whole log");
    }
    out
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

    // The new market's fields (carried by the accompanying Header event) land on the
    // MarketAdded row: symbol, asset id, tick size and both lot decimals.
    let market = query_decoded(
        &decoded,
        "SELECT asset_symbol, asset_id::VARCHAR AS asset_id, tick_size::VARCHAR AS tick_size,
         base_lot_decimals::VARCHAR AS base_lot_decimals,
         quote_lot_decimals::VARCHAR AS quote_lot_decimals
         FROM events WHERE event_type='MarketAdded'",
    )
    .unwrap()
    .to_json();
    let market: Value = serde_json::from_str(&market).unwrap();
    let market = &market["rows"][0];
    assert_eq!(market["asset_symbol"], "AMD");
    assert_eq!(market["asset_id"], "45");
    assert_eq!(market["tick_size"], "10");
    assert_eq!(market["base_lot_decimals"], "3");
    assert_eq!(market["quote_lot_decimals"], "6");

    std::fs::remove_dir_all(decoded.parent().unwrap()).unwrap();
}

/// The audit reads the fixture's `expected` block and checks it against the linked codec:
/// instruction discriminators, per-instruction event counts and variants, and a borsh layout
/// size per event variant (a codec bump that changes a field's size turns this red).
#[test]
fn the_fixture_expected_block_matches_the_linked_codec() {
    let text = std::fs::read_to_string(FIXTURE).unwrap();
    let doc: Value = serde_json::from_str(&text).unwrap();
    for tx in doc["transactions"].as_array().unwrap() {
        let sig = tx["signature"].as_str().unwrap();
        for expected in tx["expected"]["instructions"].as_array().unwrap() {
            let want_tag = expected["instructionTagHex"].as_str().unwrap();
            let stack_path = &expected["stackPath"];
            let got_tag = tx["instructions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|ix| &ix["stackPath"] == stack_path)
                .map(|ix| discriminant_hex(ix["dataBase64"].as_str().unwrap()))
                .unwrap_or_else(|| panic!("{sig}: fixture lacks instruction {stack_path}"));
            assert_eq!(got_tag, want_tag, "{sig}: instruction discriminant");

            let decoded = decode_groups(tx);
            assert!(
                decoded.iter().all(|group| group.errors.is_empty()),
                "{sig}: codec errors"
            );
            let events: Vec<&Value> = decoded
                .iter()
                .flat_map(|group| group.events.iter())
                .collect();
            let want_count = expected["eventsCount"].as_u64().unwrap() as usize;
            assert_eq!(events.len(), want_count, "{sig}: event count");

            let mut variants: Vec<&str> = events
                .iter()
                .map(|event| event.as_object().unwrap().keys().next().unwrap().as_str())
                .collect();
            variants.sort_unstable();
            variants.dedup();
            let mut want_variants: Vec<&str> = expected["eventVariants"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            want_variants.sort_unstable();
            want_variants.dedup();
            assert_eq!(variants, want_variants, "{sig}: event variants");
        }

        // Layout sizes: every event re-serializes with the codec to the wire's length prefix,
        // and each variant's size must match the size pinned in the fixture's `eventLayout`, so
        // a codec bump that changes a field's size turns the audit red.
        let layout = event_layout(tx);
        assert_eq!(layout.len(), tx["eventsCount"].as_u64().unwrap() as usize);
        for entry in tx["expected"]["instructions"][0]["eventLayout"]
            .as_array()
            .unwrap()
        {
            let variant = entry["variant"].as_str().unwrap();
            let want = entry["size"].as_u64().unwrap() as usize;
            let sizes: Vec<usize> = layout
                .iter()
                .filter(|(v, _)| v == variant)
                .map(|(_, size)| *size)
                .collect();
            assert!(!sizes.is_empty(), "{sig}: {variant} has no decoded events");
            for size in sizes {
                assert_eq!(size, want, "{sig}: {variant} layout size");
            }
        }
        let mut layout_variants: Vec<&str> = layout.iter().map(|(v, _)| v.as_str()).collect();
        layout_variants.sort_unstable();
        layout_variants.dedup();
        let mut want_variants: Vec<&str> = tx["eventVariants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        want_variants.sort_unstable();
        want_variants.dedup();
        assert_eq!(layout_variants, want_variants, "{sig}: layout variants");
    }
}

/// A codec version bump changes the revision content hash, so a bumped codec re-decodes rows
/// downstream instead of silently keeping their old revision. This is the observable effect the
/// version stamp has on the pipeline (the hash keys `processed` and the revision dedupe).
#[test]
fn a_codec_version_bump_changes_the_revision_content_hash() {
    let fx = fixture();
    for tx in [&fx.pre, &fx.post] {
        let raw = raw_fixture(tx);
        let old = raw.content_hash_with_version(fx.pre_version);
        let new = raw.content_hash_with_version(fx.post_version);
        assert_ne!(old, new, "a version bump must change the revision identity");
    }
}

/// The raw wire slice of the first event whose variant tag is `tag` in one golden capture.
fn event_slice(fixture_tx: &Value, tag: u8) -> Vec<u8> {
    let instructions = instructions_of(fixture_tx);
    let mut lengths_by_batch = BTreeMap::<u32, VecDeque<Vec<usize>>>::new();
    let mut logs = Vec::<(u32, u32, Vec<u8>)>::new();
    for (_, data_base64) in instructions {
        let bytes = STANDARD.decode(data_base64).unwrap();
        let Some(ix) = PhoenixLogInstruction::from_instruction_data(&bytes) else {
            continue;
        };
        let batch = u32::from_le_bytes(ix.data[0..4].try_into().unwrap());
        let count = u32::from_le_bytes(ix.data[4..8].try_into().unwrap());
        match ix.kind {
            PhoenixLogInstructionKind::LogEventLengths => {
                let lengths = ix.data[8..]
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()) as usize)
                    .collect();
                lengths_by_batch
                    .entry(batch)
                    .or_default()
                    .push_back(lengths);
            }
            PhoenixLogInstructionKind::Log => logs.push((batch, count, ix.data.to_vec())),
        }
    }
    for (batch, count, data) in logs {
        let lengths = lengths_by_batch
            .get_mut(&batch)
            .unwrap()
            .pop_front()
            .unwrap();
        let mut offset = 8usize;
        for len in lengths.into_iter().take(count as usize) {
            let slice = data[offset..offset + len].to_vec();
            offset += len;
            if slice.first() == Some(&tag) {
                return slice;
            }
        }
    }
    panic!("capture has no event with variant tag {tag}");
}

/// A fill on the new market (asset 45, "AMD") decodes from wire bytes and normalizes into the
/// `fills` table with the market fields from its header. The fill payload is the captured
/// `SplineFilled` event; the header and slot context are synthetic for the exercise.
#[test]
fn a_fill_on_the_new_market_decodes_and_normalizes() {
    let slot = 428_400_000u64;
    let spline = event_slice(&golden_fixtures()[0], 5);
    let slot_context = borsh::to_vec(&MarketEvent::SlotContext(SlotContextEvent {
        slot,
        timestamp: 1_782_225_000,
    }))
    .unwrap();
    let header = borsh::to_vec(&MarketEvent::Header(MarketEventHeader {
        sequence_number: 1,
        prev_sequence_number_slot: slot,
        asset_symbol: Symbol::new("AMD").unwrap(),
        asset_id: 45,
        tick_size: 10,
        base_lot_decimals: 3,
        quote_lot_decimals: 6,
        signer: [0; 32],
        trader_account: [0; 32],
    }))
    .unwrap();
    let events = [
        slot_context.as_slice(),
        header.as_slice(),
        spline.as_slice(),
    ]
    .concat();
    let lengths = [
        (slot_context.len() as u16).to_le_bytes(),
        (header.len() as u16).to_le_bytes(),
        (spline.len() as u16).to_le_bytes(),
    ]
    .concat();
    let mut log = LOG_INSTRUCTION_TAG.to_le_bytes().to_vec();
    log.extend(0u32.to_le_bytes());
    log.extend(3u32.to_le_bytes());
    log.extend(events);
    let mut lengths_ix = LOG_EVENT_LENGTHS_INSTRUCTION_TAG.to_le_bytes().to_vec();
    lengths_ix.extend(0u32.to_le_bytes());
    lengths_ix.extend(3u32.to_le_bytes());
    lengths_ix.extend(lengths);
    let group = phoenix_codec::Group {
        path: "1".into(),
        logs: vec![STANDARD.encode(&lengths_ix), STANDARD.encode(&log)],
        attribution: "stack_height".into(),
    };
    let decoded = phoenix_codec::decode(group).unwrap();
    assert!(decoded.errors.is_empty(), "{:?}", decoded.errors);
    assert_eq!(decoded.events.len(), 3);

    let tx = RawTransaction {
        signature: "fill-on-new-market".into(),
        slot: Value::String(slot.to_string()),
        block_time: Value::String("1782225000".into()),
        tx_index: Value::from(0),
        single_in_slot: Value::Bool(false),
        tx_b64: Some("AA==".into()),
        meta_json: Some("{\"err\":null}".into()),
        terminal_error: None,
        err: Value::Null,
    };
    let rows = normalize_at_version(
        &tx,
        "h",
        &[decoded],
        "source",
        "2026-01-01T00:00:00Z",
        VERSION,
    )
    .unwrap();
    let fills = rows.get("fills");
    assert_eq!(fills.len(), 1, "one fill on the new market");
    let fill = &fills[0];
    assert_eq!(
        fill.get("event_type"),
        Some(&Value::String("SplineFilled".into()))
    );
    assert_eq!(fill.get("asset_symbol"), Some(&Value::String("AMD".into())));
    assert_eq!(fill.get("asset_id"), Some(&Value::String("45".into())));
    assert_eq!(fill.get("tick_size"), Some(&Value::String("10".into())));
    assert_eq!(
        fill.get("base_lot_decimals"),
        Some(&Value::String("3".into()))
    );
    assert_eq!(
        fill.get("quote_lot_decimals"),
        Some(&Value::String("6".into()))
    );
    assert!(fill.get("price_ticks").is_some_and(|v| !v.is_null()));
    assert!(fill.get("base_lots").is_some_and(|v| !v.is_null()));
}
