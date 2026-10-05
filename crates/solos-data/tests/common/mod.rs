//! Shared fixtures for the decoder tests: the official Phoenix golden transactions and the
//! synthetic raw rows `tests/decode-fixtures.ts` builds from them.

#![allow(dead_code)]

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use solos_data::decoder::extract::PROGRAM;
use solos_data::decoder::normalize::RawTransaction;
use std::path::{Path, PathBuf};

/// A fresh temporary directory.
pub fn tempdir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The repository's `tests/data/phoenix-events.json` transactions.
pub fn golden_fixtures() -> Vec<Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data/phoenix-events.json");
    let text = std::fs::read_to_string(path).unwrap();
    serde_json::from_str::<Value>(&text).unwrap()["transactions"]
        .as_array()
        .unwrap()
        .clone()
}

/// Compact-u16 (shortvec) encoding.
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

/// Top-level instruction data from a golden fixture, index by top-level position.
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
pub fn legacy_wire(fixture: &Value) -> Vec<u8> {
    let top = top_level(fixture);
    let mut wire = vec![1];
    wire.extend([0u8; 64]);
    wire.extend(legacy_message(&top));
    wire
}

/// Legacy message body (header, two keys, blockhash, instructions).
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
pub fn meta(fixture: &Value) -> Value {
    let top = top_level(fixture);
    let inner: Vec<Value> = (0..top.len())
        .map(|i| {
            let instructions: Vec<Value> = fixture["instructions"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|ix| ix["stackPath"].as_array().unwrap().len() > 1 && ix["stackPath"][0].as_u64() == Some(i as u64))
                .map(|ix| {
                    let bytes = STANDARD.decode(ix["dataBase64"].as_str().unwrap()).unwrap();
                    serde_json::json!({ "programIdIndex": 1, "accounts": [], "data": bs58::encode(bytes).into_string(), "stackHeight": ix["stackPath"].as_array().unwrap().len() })
                })
                .collect();
            serde_json::json!({ "index": i, "instructions": instructions })
        })
        .collect();
    serde_json::json!({ "err": null, "innerInstructions": inner })
}

/// `rawFixture(fixture)`.
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
pub fn failed(tx: &RawTransaction) -> RawTransaction {
    let mut failed = tx.clone();
    failed.err = Value::String("{}".into());
    failed.meta_json = Some(serde_json::json!({ "err": {} }).to_string());
    failed
}

/// A raw transactions Parquet file for the decoder, written through DuckDB from JSON rows.
pub fn write_raw_parquet(conn: &duckdb::Connection, path: &Path, rows: &[Value]) {
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

/// A raw transaction as JSON for [`write_raw_parquet`].
pub fn raw_json(tx: &RawTransaction) -> Value {
    serde_json::json!({ "signature": tx.signature, "slot": tx.slot, "block_time": tx.block_time, "tx_b64": tx.tx_b64, "meta_json": tx.meta_json })
}

/// Count rows of a decoded root through the reader.
pub fn count(root: &Path, sql: &str) -> i64 {
    let result = solos_data::decoder::reader::query_decoded(root, sql).unwrap();
    let text = result.to_json();
    let value: Value = serde_json::from_str(&text).unwrap();
    let n = &value["rows"][0]["n"];
    n.as_str()
        .map(|s| s.parse().unwrap())
        .or_else(|| n.as_i64())
        .unwrap()
}
