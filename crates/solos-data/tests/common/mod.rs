//! Shared fixtures for the decoder tests: the official Phoenix golden transactions and the
//! synthetic raw rows `tests/decode-fixtures.ts` builds from them. The wire/meta encoding lives
//! in `solos_data::fixtures` (single source, also used by the verification skill).

#![allow(dead_code)]

use serde_json::Value;
use std::path::{Path, PathBuf};

#[allow(unused_imports)]
pub use solos_data::fixtures::{
    failed, legacy_message, legacy_wire, meta, raw_fixture, raw_json, short, top_level,
    write_raw_parquet,
};

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
