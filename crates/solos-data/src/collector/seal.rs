//! Seal a settled raw epoch in the catalog after the independent checks pass.
//!
//! An epoch is sealed only when it is strictly older than the current epoch by `lag`, its slot
//! span is covered by `published_ranges` (those ranges were published only after V1/V6), every
//! active file's bytes match the registered SHA-256 and row count, and every transactions file
//! is fully consumed in the decoder checkpoint. The current epoch is never sealed. Sealing does
//! not delete anything.

use super::catalog::{merge_coverage, write_catalog};
use crate::fsutil::file_hash;
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError, sql_string};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Slots per raw epoch. The publisher writes `epoch=<slot / 432000>`.
pub const EPOCH_SLOTS: i64 = 432_000;

/// What `maintain seal` was asked to do.
pub struct SealRequest<'a> {
    /// Epochs strictly older than `current - lag` may seal. `0` still refuses the current epoch.
    pub lag: i64,
    /// Decoder checkpoint. Opened only for the consumption read, so the decoder must be stopped.
    pub decoded_root: &'a Path,
}

/// Epochs recorded in `sealed_epochs`.
pub fn sealed_epoch_ids(store: &Store) -> Result<HashSet<i64>, StoreError> {
    Ok(store
        .rows("SELECT epoch FROM sealed_epochs", &[])?
        .iter()
        .filter_map(|row| row.int("epoch"))
        .collect())
}

/// Epoch numbers under `sealedEpochs` in a raw catalog.
#[must_use]
pub fn sealed_epochs_in_catalog(catalog: &Value) -> HashSet<i64> {
    catalog
        .get("sealedEpochs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| json_i64(row.get("epoch")))
        .collect()
}

/// The epoch a catalog file belongs to, from its `epoch` field or its `epoch=` directory.
#[must_use]
pub fn epoch_of_file(file: &Value) -> Option<i64> {
    json_i64(file.get("epoch")).or_else(|| {
        file.get("path")
            .and_then(Value::as_str)
            .and_then(epoch_of_path)
    })
}

/// `epoch=N` in a published path.
#[must_use]
pub fn epoch_of_path(path: &str) -> Option<i64> {
    let start = path.find("epoch=")? + "epoch=".len();
    let digits: String = path[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn json_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

/// Mark every epoch that passes the checks. Already-sealed epochs stay sealed.
///
/// # Errors
///
/// The decoder checkpoint is owned, a file cannot be read, or the catalog cannot be written.
/// An epoch that fails a check is listed in `refused` and is not an error.
pub fn seal_epochs(store: &mut Store, request: &SealRequest<'_>) -> Result<Obj, StoreError> {
    if request.lag < 0 {
        return Err(StoreError::Check("seal lag must be >= 0".into()));
    }
    let consumed = decoder_progress(request.decoded_root)?;
    let files = store.rows(
        "SELECT path, table_name, epoch, row_count, sha256 FROM files WHERE status='active' ORDER BY epoch, path",
        &[],
    )?;
    let current = files.iter().filter_map(|row| row.int("epoch")).max();
    let Some(current) = current else {
        return Ok(Obj::new()
            .with("at", now())
            .with("sealedEpochs", Value::Array(vec![]))
            .with_rows("refused", Vec::new())
            .with("alreadySealed", 0));
    };
    let cutoff = current.saturating_sub(request.lag);
    let ranges = published(store)?;
    let already = sealed_epoch_ids(store)?;
    let epochs: Vec<i64> = {
        let mut seen = Vec::new();
        for row in &files {
            if let Some(epoch) = row.int("epoch")
                && !seen.contains(&epoch)
            {
                seen.push(epoch);
            }
        }
        seen
    };
    let mut sealed_now = Vec::new();
    let mut refused = Vec::new();
    let mut already_sealed = 0i64;
    for epoch in epochs {
        if already.contains(&epoch) {
            already_sealed += 1;
            continue;
        }
        let reason = refusal(store, &files, &ranges, &consumed, epoch, cutoff)?;
        match reason {
            Some(reason) => {
                refused.push(Obj::new().with("epoch", epoch).with("reason", reason));
            }
            None => {
                let checks = json!({
                    "settled": true,
                    "covered": true,
                    "verified": true,
                    "decoded": true
                })
                .to_string();
                store.exec(
                    "INSERT INTO sealed_epochs VALUES (?, ?, ?::JSON)",
                    &[&epoch, &now(), &checks],
                )?;
                sealed_now.push(epoch);
            }
        }
    }
    let root = store.root.clone();
    write_catalog(store, &root)?;
    Ok(Obj::new()
        .with("at", now())
        .with(
            "sealedEpochs",
            Value::Array(sealed_now.iter().copied().map(Value::from).collect()),
        )
        .with_rows("refused", refused)
        .with("alreadySealed", already_sealed)
        .with("currentEpoch", current)
        .with("cutoff", cutoff))
}

fn decoder_progress(root: &Path) -> Result<HashMap<String, i64>, StoreError> {
    let decoded =
        Store::open(root, crate::decoder::schema::schema_static()).map_err(
            |error| {
                match error {
            StoreError::AlreadyOwned { .. } => StoreError::Check(
                "decoder checkpoint is owned; stop solos-data-phoenix-decoder.service before seal"
                    .into(),
            ),
            other => other,
        }
            },
        )?;
    let mut consumed = HashMap::new();
    for row in decoded.rows("SELECT source_hash, row_offset FROM sources", &[])? {
        if let (Some(hash), Some(offset)) = (row.str("source_hash"), row.int("row_offset")) {
            consumed.insert(hash.to_owned(), offset);
        }
    }
    decoded.close()?;
    Ok(consumed)
}

fn published(store: &Store) -> Result<Vec<(i64, i64)>, StoreError> {
    let ranges = store
        .rows("SELECT slot_from, slot_to FROM published_ranges", &[])?
        .iter()
        .map(|row| {
            (
                row.int("slot_from").unwrap_or(0),
                row.int("slot_to").unwrap_or(0),
            )
        })
        .collect();
    Ok(merge_coverage(ranges))
}

fn refusal(
    store: &Store,
    files: &[Obj],
    ranges: &[(i64, i64)],
    consumed: &HashMap<String, i64>,
    epoch: i64,
    cutoff: i64,
) -> Result<Option<&'static str>, StoreError> {
    // Settled is first so the live epoch is never hashed, and so removing this comparison
    // is what lets the current epoch through the later checks.
    if epoch >= cutoff {
        return Ok(Some("not settled"));
    }
    let start = epoch.saturating_mul(EPOCH_SLOTS);
    let end = start + EPOCH_SLOTS - 1;
    if !ranges.iter().any(|(from, to)| *from <= start && *to >= end) {
        return Ok(Some("not covered"));
    }
    let epoch_files: Vec<&Obj> = files
        .iter()
        .filter(|row| row.int("epoch") == Some(epoch))
        .collect();
    let transactions: Vec<&Obj> = epoch_files
        .iter()
        .copied()
        .filter(|row| row.str("table_name") == Some("transactions"))
        .collect();
    if transactions.is_empty() {
        return Ok(Some("no transactions"));
    }
    for file in &epoch_files {
        let path = Path::new(file.str("path").unwrap_or(""));
        if file_hash(path)? != file.str("sha256").unwrap_or("") {
            return Ok(Some("hash mismatch"));
        }
        let count = store.rows(
            &format!(
                "SELECT count(*) AS n FROM read_parquet({})",
                sql_string(&path.to_string_lossy())
            ),
            &[],
        )?;
        if count.first().and_then(|row| row.int("n")) != file.int("row_count") {
            return Ok(Some("count mismatch"));
        }
    }
    for file in &transactions {
        let hash = file.str("sha256").unwrap_or("");
        let rows = file.int("row_count").unwrap_or(0);
        let offset = consumed.get(hash).copied().unwrap_or(0);
        if offset < rows {
            return Ok(Some("not decoded"));
        }
    }
    Ok(None)
}
