//! Offline fixtures for the verification skill (`.cursor/skills/verify-solos-data`): the same
//! golden transactions the integration tests use, materialised as the raw inputs the real
//! decoder and range check consume. Building a fixture is setup only; decoding and
//! range-checking run the production code paths unchanged.

use crate::decoder::extract::PROGRAM;
use crate::decoder::normalize::RawTransaction;
use crate::fsutil::file_hash;
use crate::store::{Store, StoreError};
use base64::{Engine, engine::general_purpose::STANDARD};
use duckdb::Connection;
use serde_json::Value;
use std::path::Path;

/// Compact-u16 (shortvec) encoding.
#[must_use]
pub fn short(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        out.push(if value > 0 { byte | 128 } else { byte });
        if value == 0 {
            return out;
        }
    }
}

/// Top-level instruction data from a golden fixture, indexed by top-level position.
#[must_use]
pub fn top_level(fixture: &Value) -> Vec<Option<Vec<u8>>> {
    let instructions = fixture["instructions"].as_array().unwrap();
    let count = instructions
        .iter()
        .map(|ix| ix["stackPath"][0].as_u64().unwrap() as usize)
        .max()
        .unwrap()
        + 1;
    (0..count)
        .map(|i| {
            instructions
                .iter()
                .find(|ix| {
                    ix["stackPath"].as_array().unwrap().len() == 1
                        && ix["stackPath"][0].as_u64() == Some(i as u64)
                })
                .map(|ix| {
                    STANDARD
                        .decode(ix["dataBase64"].as_str().unwrap_or(""))
                        .unwrap()
                })
        })
        .collect()
}

/// The legacy wire `rawFixture` builds: zero signature, fee payer and the program as static keys.
#[must_use]
pub fn legacy_wire(fixture: &Value) -> Vec<u8> {
    let top = top_level(fixture);
    let mut wire = vec![1];
    wire.extend([0u8; 64]);
    wire.extend(legacy_message(&top));
    wire
}

/// Legacy message body (header, two keys, blockhash, instructions).
#[must_use]
pub fn legacy_message(top: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut message = vec![1, 0, 1, 2];
    message.extend([0u8; 32]);
    message.extend(bs58::decode(PROGRAM).into_vec().unwrap());
    message.extend([0u8; 32]);
    message.extend(short(top.len()));
    for ix in top {
        let bytes = ix.clone().unwrap_or_default();
        message.extend([u8::from(ix.is_some()), 0]);
        message.extend(short(bytes.len()));
        message.extend(bytes);
    }
    message
}

/// The RPC `meta` of a golden fixture: inner instructions by top-level index.
#[must_use]
pub fn meta(fixture: &Value) -> Value {
    let top = top_level(fixture);
    let inner: Vec<Value> = (0..top.len())
        .map(|i| {
            let instructions: Vec<Value> = fixture["instructions"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|ix| {
                    ix["stackPath"].as_array().unwrap().len() > 1
                        && ix["stackPath"][0].as_u64() == Some(i as u64)
                })
                .map(|ix| {
                    let bytes = STANDARD.decode(ix["dataBase64"].as_str().unwrap()).unwrap();
                    serde_json::json!({
                        "programIdIndex": 1,
                        "accounts": [],
                        "data": bs58::encode(bytes).into_string(),
                        "stackHeight": ix["stackPath"].as_array().unwrap().len()
                    })
                })
                .collect();
            serde_json::json!({ "index": i, "instructions": instructions })
        })
        .collect();
    serde_json::json!({ "err": null, "innerInstructions": inner })
}

/// `rawFixture(fixture)`.
#[must_use]
pub fn raw_fixture(fixture: &Value) -> RawTransaction {
    RawTransaction {
        signature: fixture["signature"].as_str().unwrap().to_owned(),
        slot: fixture["slot"].clone(),
        block_time: fixture["blockTime"].clone(),
        tx_index: Value::from(0),
        single_in_slot: Value::Bool(false),
        tx_b64: Some(STANDARD.encode(legacy_wire(fixture))),
        meta_json: Some(meta(fixture).to_string()),
        terminal_error: None,
        err: Value::Null,
    }
}

/// The same transaction as a failed attempt (`err` set in both places).
#[must_use]
pub fn failed(tx: &RawTransaction) -> RawTransaction {
    let mut failed = tx.clone();
    failed.err = Value::String("{}".into());
    failed.meta_json = Some(serde_json::json!({ "err": {} }).to_string());
    failed
}

/// A raw transaction as JSON for [`write_raw_parquet`].
#[must_use]
pub fn raw_json(tx: &RawTransaction) -> Value {
    serde_json::json!({
        "signature": tx.signature,
        "slot": tx.slot,
        "block_time": tx.block_time,
        "tx_b64": tx.tx_b64,
        "meta_json": tx.meta_json
    })
}

/// A raw transactions Parquet file for the decoder, written through DuckDB from JSON rows.
pub fn write_raw_parquet(conn: &Connection, path: &Path, rows: &[Value]) {
    let data = Value::Array(rows.to_vec()).to_string();
    conn.execute(
        &format!(
            "COPY (SELECT value->>'signature' AS signature, (value->>'slot')::BIGINT AS slot,
      (value->>'block_time')::BIGINT AS block_time, 0 AS tx_index, false AS single_in_slot,
      value->>'tx_b64' AS tx_b64, value->>'meta_json' AS meta_json, NULL::VARCHAR AS terminal_error,
      'null' AS err FROM json_each(?::JSON)) TO '{}' (FORMAT PARQUET)",
            path.display()
        ),
        [data],
    )
    .unwrap();
}

/// Write `<dir>/raw/{catalog.json, staging/tx.parquet}` for the real decoder from the golden
/// Phoenix fixtures, one raw file holding every fixture transaction.
pub fn write_decoder_raw(dir: &Path, fixtures: &[Value]) -> Result<(), StoreError> {
    let raw_dir = dir.join("raw");
    let staging = raw_dir.join("staging");
    std::fs::create_dir_all(&staging)?;
    let path = staging.join("tx.parquet");
    let conn = Connection::open_in_memory()?;
    let rows: Vec<Value> = fixtures
        .iter()
        .map(|fixture| raw_json(&raw_fixture(fixture)))
        .collect();
    write_raw_parquet(&conn, &path, &rows);
    let catalog = serde_json::json!({
        "at": "fixture",
        "files": [{
            "path": "staging/tx.parquet",
            "table_name": "transactions",
            "row_count": rows.len(),
            "sha256": file_hash(&path)?,
            "created_at": "2026-10-04T03:00:00Z"
        }]
    });
    std::fs::write(raw_dir.join("catalog.json"), catalog.to_string())?;
    Ok(())
}

/// Write a collector checkpoint at `dir` covering `[next - chunk + 1, next]`: one signature and
/// one ordered transaction per slot, plus the `backfill` cursor. With `gap`, the transaction for
/// the middle slot is left out, so the range check reports `missing=1` and refuses to publish.
pub fn write_range_fixture(dir: &Path, chunk: i64, next: i64, gap: bool) -> Result<(), StoreError> {
    let mut store = Store::open(dir, crate::collector::schema::SCHEMA)?;
    let from = next - chunk + 1;
    let missing_slot = from + chunk / 2;
    for slot in from..=next {
        let signature = format!("sig-{slot}");
        store.exec(
            "INSERT INTO signatures (signature, slot, block_time) VALUES (?, ?, ?)",
            &[&signature, &slot, &slot],
        )?;
        if gap && slot == missing_slot {
            continue;
        }
        store.exec(
            "INSERT INTO transactions (signature, slot, block_time, single_in_slot, err, fee,
                compute_units_consumed, tx_b64, meta_json, raw_rpc_json, mode, provider, fetched_at)
            VALUES (?, ?, ?, true, 'null', 5000, 10, 'AA==', '{}', '{}', 'tail', 'fixture',
                '2026-10-04T00:00:00Z')",
            &[&signature, &slot, &slot],
        )?;
    }
    store.set("backfill", &serde_json::json!({ "next": next }))?;
    store.close()
}
