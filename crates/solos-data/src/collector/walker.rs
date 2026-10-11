//! Signature manifest walks over `getSignaturesForAddress`, one durable page at a time. A port
//! of `walker.ts`; the `kv` JSON shape is unchanged so cursors continue across the cutover.

use super::config::Lane;
use super::rpc::{Rpc, call_value};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::{Param, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// A signature row as the provider lists it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Signature {
    /// Base58 signature.
    pub signature: String,
    /// Slot.
    pub slot: i64,
    /// Block time, if known.
    #[serde(default)]
    pub block_time: Option<i64>,
    /// Error, if any.
    #[serde(default)]
    pub err: Value,
    /// Extra provider fields are kept so `newest` round-trips.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// Walk state stored in `kv`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Walk {
    /// Address walked.
    pub address: String,
    /// Lane.
    pub mode: Lane,
    /// Cycle id.
    pub cycle_id: String,
    /// Cursor: the oldest signature seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Stop at this signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// Slot of the cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_slot: Option<i64>,
    /// Lowest slot of interest.
    pub floor: i64,
    /// Highest slot of interest.
    pub ceiling: i64,
    /// Whether the walk reached its end.
    pub done: bool,
    /// Pages fetched.
    pub pages: u64,
    /// Oldest slot seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_slot: Option<i64>,
    /// First row of the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest: Option<Signature>,
    /// A second cursor, one row newer than `before`: the provider can answer an empty page for a
    /// cursor it cannot resolve while history continues below it (2026-10-11, ADR-0008), so an
    /// empty page is confirmed from here before the walk counts as ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
}

/// V2: slots never increase across the cursor and nothing repeats.
pub fn check_page(page: &[Signature], walk: &Walk) -> Result<(), StoreError> {
    let mut previous = walk.last_slot.unwrap_or(i64::MAX);
    let mut seen = std::collections::HashSet::new();
    for row in page {
        if row.slot < 0 || row.signature.is_empty() {
            return Err(StoreError::Check("Invalid signature row".into()));
        }
        if row.slot > previous {
            return Err(StoreError::Check(
                "V2: slots increased across cursor".into(),
            ));
        }
        if seen.contains(&row.signature) || walk.before.as_deref() == Some(row.signature.as_str()) {
            return Err(StoreError::Check("V2: repeated cursor/signature".into()));
        }
        previous = row.slot;
        seen.insert(row.signature.clone());
    }
    Ok(())
}

/// Fetch one page, commit the selected rows and the cursor together.
pub async fn walk_page(rpc: &dyn Rpc, db: &Db, key: &str, walk: Walk) -> Result<Walk, StoreError> {
    if walk.done {
        return Ok(walk);
    }
    let mut page = fetch_page(rpc, &walk, walk.before.as_deref()).await?;
    if page.is_empty()
        && let (Some(dead), Some(fallback)) = (walk.before.as_deref(), walk.fallback.as_deref())
    {
        // Confirm the end of history from the row before the cursor: a resolvable cursor yields
        // the cursor itself and whatever lies below it.
        let retry = fetch_page(rpc, &walk, Some(fallback)).await?;
        page = rows_after_cursor(retry, dead)?;
        if !page.is_empty() {
            log(
                "walk_cursor_unresolved",
                Obj::new()
                    .with("address", walk.address.clone())
                    .with("cycleId", walk.cycle_id.clone())
                    .with("cursorSlot", walk.last_slot)
                    .with("resumedRows", page.len()),
            );
        }
    }
    check_page(&page, &walk)?;
    let oldest = page.last().cloned();
    let next = Walk {
        before: oldest
            .as_ref()
            .map(|o| o.signature.clone())
            .or(walk.before.clone()),
        last_slot: oldest.as_ref().map(|o| o.slot).or(walk.last_slot),
        oldest_slot: oldest.as_ref().map(|o| o.slot).or(walk.oldest_slot),
        newest: walk.newest.clone().or_else(|| page.first().cloned()),
        done: page.is_empty() || oldest.as_ref().is_some_and(|o| o.slot < walk.floor),
        fallback: penultimate(&page).or_else(|| walk.fallback.clone()),
        pages: walk.pages + 1,
        ..walk.clone()
    };
    let selected: Vec<Signature> = page
        .into_iter()
        .filter(|row| row.slot >= walk.floor && row.slot <= walk.ceiling)
        .collect();
    let key = key.to_owned();
    let address = walk.address.clone();
    let mode = walk.mode.as_str().to_owned();
    let cycle = walk.cycle_id.clone();
    let next_value = serde_json::to_value(&next).map_err(|e| StoreError::Check(e.to_string()))?;
    db.run(move |store| {
        store.transaction(|store| {
            if !selected.is_empty() {
                let from = selected.last().map_or(0, |s| s.slot);
                let to = selected.first().map_or(0, |s| s.slot);
                let payload = serde_json::to_string(&selected).map_err(|e| StoreError::Check(e.to_string()))?;
                store.exec(
                    "UPDATE signatures SET source_addresses=list_append(source_addresses, ?)
        WHERE slot BETWEEN ? AND ? AND NOT list_contains(source_addresses, ?)
        AND signature IN (SELECT value->>'signature' FROM json_each(?::JSON))",
                    &[&address, &from, &to, &address, &payload],
                )?;
                store.exec(
                    "INSERT INTO signatures
      SELECT value->>'signature', (value->>'slot')::BIGINT, (value->>'blockTime')::BIGINT,
        value->'err', [?]::VARCHAR[], ?, ?, ? FROM json_each(?::JSON)
      WHERE value->>'signature' NOT IN (SELECT signature FROM signatures WHERE slot BETWEEN ? AND ?)",
                    &[&address, &mode, &cycle, &now(), &payload, &from, &to],
                )?;
            }
            store.exec("INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)", &[&key, &next_value.to_string()])?;
            Ok(())
        })
    })
    .await?;
    Ok(next)
}

/// One `getSignaturesForAddress` page below `before`.
async fn fetch_page(
    rpc: &dyn Rpc,
    walk: &Walk,
    before: Option<&str>,
) -> Result<Vec<Signature>, StoreError> {
    let mut options = json!({ "limit": 1000, "commitment": "finalized" });
    if let Some(before) = before {
        options["before"] = Value::String(before.to_owned());
    }
    if let Some(until) = &walk.until {
        options["until"] = Value::String(until.clone());
    }
    let page = call_value(
        rpc,
        "getSignaturesForAddress",
        json!([walk.address, options]),
        walk.mode,
    )
    .await
    .map_err(|e| StoreError::Check(e.to_string()))?;
    let Some(rows) = page.as_array() else {
        return Err(StoreError::Check("V2: invalid page".into()));
    };
    rows.iter()
        .map(|row| {
            serde_json::from_value(row.clone())
                .map_err(|_| StoreError::Check("Invalid signature row".into()))
        })
        .collect()
}

/// The second-to-last signature of a page: the fallback cursor for the page's last row.
#[must_use]
pub fn penultimate(page: &[Signature]) -> Option<String> {
    page.len()
        .checked_sub(2)
        .and_then(|i| page.get(i))
        .map(|row| row.signature.clone())
}

/// The rows strictly below `dead` in a page fetched from the fallback cursor. An empty page agrees
/// with the cursor's empty page; a non-empty page without the dead cursor cannot confirm
/// anything, so the walk stops with an error instead of ending.
pub fn rows_after_cursor(page: Vec<Signature>, dead: &str) -> Result<Vec<Signature>, StoreError> {
    if page.is_empty() {
        return Ok(page);
    }
    let Some(at) = page.iter().position(|row| row.signature == dead) else {
        return Err(StoreError::Check(
            "V2: the walk cursor returned an empty page and the fallback page does not contain it"
                .into(),
        ));
    };
    Ok(page.into_iter().skip(at + 1).collect())
}

/// Resume or start a walk and run it to its end.
pub async fn walk_to_end(
    rpc: &dyn Rpc,
    db: &Db,
    key: &str,
    initial: Walk,
) -> Result<Walk, StoreError> {
    let mut walk = match db.get(key).await? {
        Some(value) => {
            serde_json::from_value(value).map_err(|e| StoreError::Check(e.to_string()))?
        }
        None => initial,
    };
    while !walk.done {
        walk = walk_page(rpc, db, key, walk).await?;
    }
    Ok(walk)
}

/// A fresh walk.
#[must_use]
pub fn make_walk(address: &str, mode: Lane, cycle_id: &str, floor: i64, ceiling: i64) -> Walk {
    Walk {
        address: address.into(),
        mode,
        cycle_id: cycle_id.into(),
        before: None,
        until: None,
        last_slot: None,
        floor,
        ceiling,
        done: false,
        pages: 0,
        oldest_slot: None,
        newest: None,
        fallback: None,
    }
}

/// Params helper.
#[must_use]
pub fn p(value: impl Into<Param>) -> Param {
    value.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(signature: &str, slot: i64) -> Signature {
        Signature {
            signature: signature.into(),
            slot,
            block_time: None,
            err: Value::Null,
            extra: serde_json::Map::new(),
        }
    }

    /// 2026-10-11: `before=<cursor>` answered `[]` while `before=<the row above it>` listed the
    /// cursor and a full page below it.
    #[test]
    fn a_dead_cursor_resumes_from_the_row_above_it() {
        let page = vec![
            sig("a", 428_785_357),
            sig("b", 428_785_356),
            sig("dead", 428_785_356),
        ];
        assert_eq!(penultimate(&page).as_deref(), Some("b"));
        assert_eq!(penultimate(&page[..1]), None);
        let retry = vec![
            sig("dead", 428_785_356),
            sig("c", 428_785_355),
            sig("d", 428_785_331),
        ];
        let resumed = rows_after_cursor(retry, "dead").unwrap();
        assert_eq!(
            resumed
                .iter()
                .map(|r| r.signature.as_str())
                .collect::<Vec<_>>(),
            ["c", "d"]
        );
        // The true end of history: the fallback lists only the cursor.
        assert!(
            rows_after_cursor(vec![sig("dead", 1)], "dead")
                .unwrap()
                .is_empty()
        );
        assert!(rows_after_cursor(Vec::new(), "dead").unwrap().is_empty());
        // A fallback page without the cursor proves nothing.
        assert!(rows_after_cursor(vec![sig("c", 1)], "dead").is_err());
    }

    #[test]
    fn walks_stored_before_the_fallback_still_load() {
        let stored = json!({"address": "p", "mode": "backfill", "cycleId": "backfill", "floor": 0,
            "ceiling": 9, "done": true, "pages": 3, "before": "dead", "lastSlot": 5});
        let walk: Walk = serde_json::from_value(stored).unwrap();
        assert_eq!(walk.fallback, None);
        assert!(
            serde_json::to_value(&walk)
                .unwrap()
                .get("fallback")
                .is_none()
        );
    }
}
