//! The raw `catalog.json`: active files with compaction lineage and merged published coverage,
//! written durably. A port of `catalog.ts`.

use crate::fsutil::write_durable;
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError};
use serde_json::{Value, json};
use std::path::Path;

/// Merge overlapping or adjacent `(from, to)` ranges.
#[must_use]
pub fn merge_coverage(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_by_key(|r| r.0);
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.0 <= last.1 + 1 => last.1 = last.1.max(range.1),
            _ => merged.push(range),
        }
    }
    merged
}

/// Coverage as JSON `[{from, to}]`.
#[must_use]
pub fn coverage_json(ranges: &[(i64, i64)]) -> Value {
    Value::Array(
        ranges
            .iter()
            .map(|(from, to)| json!({ "from": from, "to": to }))
            .collect(),
    )
}

/// Write `<root>/catalog.json` and return it.
pub fn write_catalog(store: &mut Store, root: &Path) -> Result<Obj, StoreError> {
    let mut files = store.rows(
        "SELECT * EXCLUDE(status) FROM files WHERE status='active'",
        &[],
    )?;
    let ranges: Vec<(i64, i64)> = store
        .rows("SELECT slot_from, slot_to FROM published_ranges", &[])?
        .iter()
        .map(|r| {
            (
                r.int("slot_from").unwrap_or(0),
                r.int("slot_to").unwrap_or(0),
            )
        })
        .collect();
    let inputs = store.rows(
        "SELECT i.* FROM compaction_inputs i JOIN files f ON f.path=i.path WHERE f.status='active'",
        &[],
    )?;
    for file in &mut files {
        let path = file.str("path").unwrap_or("").to_owned();
        let parents: Vec<Value> = inputs.iter().filter(|i| i.str("path") == Some(path.as_str())).map(|i| json!({ "sha256": i.str("source_hash").unwrap_or(""), "row_count": i.int("row_count").unwrap_or(0) })).collect();
        if !parents.is_empty() {
            file.set("parents", Value::Array(parents));
        }
    }
    let catalog = Obj::new()
        .with("at", now())
        .with_rows("files", files)
        .with("coverage", coverage_json(&merge_coverage(ranges)))
        .with(
            "acceptance",
            "published with fetch/order checks; independent validation and sealing pending",
        );
    write_durable(
        &root.join("catalog.json"),
        format!("{}\n", catalog.to_json()).as_bytes(),
    )?;
    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_adjacent_and_overlapping_ranges() {
        assert_eq!(
            merge_coverage(vec![(3, 4), (1, 3), (10, 12)]),
            vec![(1, 4), (10, 12)]
        );
    }
}
