//! `spot check` (ADR-0014): the spot lane's files against its own `batches` rows, from the
//! catalog only (never the running lane's ledger). The batches must tile one slot range with no
//! hole or overlap; every batch must have streamed exactly the blocks the RPC listed and missed
//! no manifest signature; and each batch's `swaps` and `liquidity` files must hold the rows the
//! batch counted, in the catalog and on disk (Parquet metadata). Read-only.

use crate::augment::ledger::Lane;
use crate::augment::query::query_lanes;
use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, sql_string};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;

const BATCHES: &str = "SELECT from_slot, to_slot, blocks, rpc_blocks, swaps, liquidity,
    (SELECT sum(manifest_missing) FROM spot_batches p WHERE p.from_slot = b.from_slot
       AND p.to_slot = b.to_slot AND p.program <> 'ALL') AS missing,
    manifest, unlisted
  FROM spot_batches b WHERE program = 'ALL' ORDER BY from_slot DESC";

fn int(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// The batch's file period, as the run names it.
#[must_use]
pub fn period(from: i64, to: i64) -> String {
    format!("{from:010}-{to:010}")
}

/// Check the lane; `ok` is false on any hole, overlap or mismatch.
///
/// # Errors
///
/// No spot catalog, or a query failure.
pub fn check(root: &Path) -> Result<Obj, StoreError> {
    let text = std::fs::read_to_string(Lane::Spot.catalog_path(root))
        .map_err(|e| StoreError::Check(format!("no spot catalog: {e}")))?;
    let catalog: Value =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let mut files: HashMap<(String, String), (String, i64)> = HashMap::new();
    for f in catalog["files"].as_array().into_iter().flatten() {
        let key = (
            f["dataset"].as_str().unwrap_or_default().to_owned(),
            f["period"].as_str().unwrap_or_default().to_owned(),
        );
        files.insert(
            key,
            (
                f["path"].as_str().unwrap_or_default().to_owned(),
                int(f.get("row_count")),
            ),
        );
    }
    if files.is_empty() {
        return Ok(Obj::new().with("ok", true).with("batches", 0));
    }
    let result = query_lanes(root, &[Lane::Spot], BATCHES)?;
    let rows = result.to_value()["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let files_root = Lane::Spot.files_root(root);
    let conn = memory_connection("4", "2GB")?;
    let mut holes = Vec::new();
    let mut bad_blocks = Vec::new();
    let mut missing = Vec::new();
    let mut bad_files = Vec::new();
    let (mut swaps, mut liquidity, mut manifest, mut unlisted, mut slots) = (0, 0, 0, 0, 0);
    let mut above: Option<i64> = None;
    for b in &rows {
        let (from, to) = (int(b.get("from_slot")), int(b.get("to_slot")));
        slots += to - from + 1;
        if let Some(above) = above
            && to != above - 1
        {
            holes.push(json!([from, to]));
        }
        above = Some(from);
        if int(b.get("blocks")) != int(b.get("rpc_blocks")) {
            bad_blocks.push(from);
        }
        if int(b.get("missing")) != 0 {
            missing.push(from);
        }
        swaps += int(b.get("swaps"));
        liquidity += int(b.get("liquidity"));
        manifest += int(b.get("manifest"));
        unlisted += int(b.get("unlisted"));
        for dataset in ["swaps", "liquidity"] {
            let expected = int(b.get(dataset));
            let Some((path, catalogued)) = files.get(&(dataset.to_owned(), period(from, to)))
            else {
                bad_files.push(json!({ "dataset": dataset, "from": from, "problem": "no file" }));
                continue;
            };
            let on_disk: i64 = conn.query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet({})",
                    sql_string(&files_root.join(path).to_string_lossy())
                ),
                [],
                |r| r.get(0),
            )?;
            if *catalogued != expected || on_disk != expected {
                bad_files.push(json!({ "dataset": dataset, "from": from, "batch": expected,
                                        "catalog": catalogued, "file": on_disk }));
            }
        }
    }
    let ok =
        holes.is_empty() && bad_blocks.is_empty() && missing.is_empty() && bad_files.is_empty();
    let first = |v: &Vec<Value>| v.iter().take(20).cloned().collect::<Vec<_>>();
    Ok(Obj::new()
        .with("ok", ok)
        .with("batches", rows.len())
        .with("slots", slots)
        .with("from", rows.last().map(|b| int(b.get("from_slot"))))
        .with("to", rows.first().map(|b| int(b.get("to_slot"))))
        .with("swaps", swaps)
        .with("liquidity", liquidity)
        .with("manifest", manifest)
        .with("unlisted", unlisted)
        .with("holesOrOverlaps", first(&holes))
        .with(
            "blockMismatches",
            bad_blocks.iter().take(20).copied().collect::<Vec<_>>(),
        )
        .with(
            "manifestMissing",
            missing.iter().take(20).copied().collect::<Vec<_>>(),
        )
        .with("fileMismatches", first(&bad_files)))
}
