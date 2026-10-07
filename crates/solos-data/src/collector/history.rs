//! Historical collection, newest first from H0: the RPC lane walks the manifest strictly below
//! the chunk, fetches, orders, validates and publishes; publication atomically checkpoints the
//! next lower slot. A port of `history.ts`, with the archive lane (ADR-0008) chosen per range when
//! it is enabled and the range lies below the archive tip.

use super::config::{Config, Lane};
use super::fetcher::fetch_range;
use super::ordering::order_range;
use super::rpc::Rpc;
use super::validation::{assert_publishable, validate_range};
use super::walker::{Walk, make_walk, walk_page};
use super::writer::{Checkpoint, publish_range};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::sync::Arc;

/// Backfill progress in `kv`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BackfillProgress {
    /// Always `newest-first`.
    pub direction: String,
    /// `fetching` or `complete`.
    pub phase: String,
    /// Next (highest) slot to collect.
    pub next: i64,
    /// H0 slot.
    pub ceiling: i64,
    /// Chunks published.
    pub completed_chunks: u64,
    /// Transactions published.
    pub published_transactions: i64,
    /// Oldest published slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_published_slot: Option<i64>,
    /// Newest published slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_published_slot: Option<i64>,
    /// Last completion instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    /// Stage timings of the last chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_seconds: Option<Value>,
    /// Which lane produced the last chunk (`rpc` or `archive`); absent before the archive lane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

async fn progress(db: &Db) -> Result<BackfillProgress, StoreError> {
    let previous = db.get("backfill").await?;
    if let Some(value) = &previous
        && value.get("direction").and_then(Value::as_str) == Some("newest-first")
    {
        return serde_json::from_value(value.clone()).map_err(|e| StoreError::Check(e.to_string()));
    }
    let Some(h0) = db.get("H0").await? else {
        return Err(StoreError::Check("H0 is missing".into()));
    };
    // Preserve old checkpoints and all collected rows; changing direction is additive.
    if let Some(value) = previous {
        db.set("backfill/oldest-first", value).await?;
    }
    let slot = h0.get("slot").and_then(Value::as_i64).unwrap_or(0);
    let state = BackfillProgress {
        direction: "newest-first".into(),
        phase: "fetching".into(),
        next: slot,
        ceiling: slot,
        completed_chunks: 0,
        published_transactions: 0,
        oldest_published_slot: None,
        newest_published_slot: None,
        completed_at: None,
        timing_seconds: None,
        source: None,
    };
    db.set(
        "backfill",
        serde_json::to_value(&state).map_err(|e| StoreError::Check(e.to_string()))?,
    )
    .await?;
    Ok(state)
}

/// Walk until the cursor is strictly below `floor` (a page can split a slot's signature set).
pub async fn manifest_through(
    rpc: &dyn Rpc,
    db: &Db,
    key: &str,
    initial: Walk,
    floor: i64,
) -> Result<Walk, StoreError> {
    let mut walk = match db.get(key).await? {
        Some(value) => {
            serde_json::from_value(value).map_err(|e| StoreError::Check(e.to_string()))?
        }
        None => initial,
    };
    while !walk.done && walk.last_slot.is_none_or(|s| s >= floor) {
        walk = walk_page(rpc, db, key, walk).await?;
    }
    Ok(walk)
}

/// Which lane collects `[from, to]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Alchemy JSON-RPC.
    Rpc,
    /// Old Faithful through Jetstreamer.
    Archive,
}

/// Publish one recent-to-old batch; publication atomically checkpoints the next lower slot.
pub async fn backfill_step(
    rpc: Arc<dyn Rpc>,
    db: &Db,
    config: &Config,
    program_data: &str,
    archive_tip: Option<i64>,
) -> Result<BackfillProgress, StoreError> {
    let state = progress(db).await?;
    if state.phase == "complete" {
        return Ok(state);
    }
    let started = std::time::Instant::now();
    let to = state.next;
    let source = match archive_tip {
        Some(tip) if config.archive_backfill_enabled && to <= tip => Source::Archive,
        _ => Source::Rpc,
    };
    let chunk = if source == Source::Archive {
        config.archive_chunk_slots
    } else {
        config.backfill_chunk_slots
    };
    let mut from = (to - chunk + 1).max(0);
    let mut walks = Vec::new();
    for (key, address) in [
        ("walk/backfill", config.program_id.as_str()),
        ("walk/programdata", program_data),
    ] {
        walks.push(
            manifest_through(
                rpc.as_ref(),
                db,
                key,
                make_walk(address, Lane::Backfill, key, 0, state.ceiling),
                from,
            )
            .await?,
        );
    }
    let ended = walks.iter().all(|w| w.done);
    let oldest = walks
        .iter()
        .filter_map(|w| w.oldest_slot)
        .min()
        .unwrap_or(i64::MAX);
    if ended && walks[0].oldest_slot.is_none() {
        return Err(StoreError::Check(
            "History walk returned no program transactions".into(),
        ));
    }
    if ended {
        from = from.max(oldest);
        db.set("S_start", Value::from(walks[0].oldest_slot.unwrap_or(0)))
            .await?;
    }
    if from > to {
        let mut complete = state.clone();
        complete.phase = "complete".into();
        db.set(
            "backfill",
            serde_json::to_value(&complete).map_err(|e| StoreError::Check(e.to_string()))?,
        )
        .await?;
        return Ok(complete);
    }
    super::pipeline::record_versions(db, program_data, from, to).await?;
    let fetch_started = std::time::Instant::now();
    let order_started;
    match source {
        Source::Rpc => {
            fetch_range(Arc::clone(&rpc), db, config, Lane::Backfill, from, to).await?;
            order_started = std::time::Instant::now();
            order_range(Arc::clone(&rpc), db, Lane::Backfill, from, to, 256).await?;
        }
        Source::Archive => {
            super::archive::collect_range(Arc::clone(&rpc), db, config, program_data, from, to)
                .await?;
            order_started = std::time::Instant::now();
        }
    }
    let validation_started = std::time::Instant::now();
    let (report, next) = {
        let state = state.clone();
        let data_dir = config.data_dir.clone();
        let source_name = if source == Source::Archive {
            "archive"
        } else {
            "rpc"
        };
        db.run(move |store| {
            let report = validate_range(store, from, to)?;
            assert_publishable(&report)?;
            let next = BackfillProgress {
                next: from - 1,
                phase: if ended && from <= oldest {
                    "complete".into()
                } else {
                    "fetching".into()
                },
                oldest_published_slot: Some(from),
                newest_published_slot: Some(state.ceiling),
                completed_at: Some(now()),
                completed_chunks: state.completed_chunks + 1,
                published_transactions: state.published_transactions
                    + report.int("fetched").unwrap_or(0),
                timing_seconds: Some(json!({
                    "manifest": (fetch_started - started).as_secs_f64(),
                    "fetch": (order_started - fetch_started).as_secs_f64(),
                    "ordering": (validation_started - order_started).as_secs_f64(),
                    "validation": validation_started.elapsed().as_secs_f64(),
                })),
                source: Some(source_name.into()),
                ..state
            };
            let value =
                serde_json::to_value(&next).map_err(|e| StoreError::Check(e.to_string()))?;
            publish_range(
                store,
                &data_dir,
                from,
                to,
                Some(Checkpoint {
                    key: "backfill".into(),
                    value,
                    cycle_id: None,
                }),
            )?;
            Ok((report, next))
        })
        .await?
    };
    let mut fields = report;
    fields.set("direction", next.direction.clone());
    fields.set("oldestPublishedSlot", from);
    fields.set(
        "timingSeconds",
        next.timing_seconds.clone().unwrap_or(Value::Null),
    );
    fields.set("durationSeconds", started.elapsed().as_secs_f64());
    fields.set(
        "source",
        next.source.clone().map_or(Value::Null, Value::String),
    );
    log("backfill_chunk", fields);
    Ok(next)
}

/// Run the historical lane to completion.
pub async fn backfill(
    rpc: Arc<dyn Rpc>,
    db: &Db,
    config: &Config,
    program_data: &str,
    archive_tip: &dyn Fn() -> Option<i64>,
    wait_for_tip: bool,
) -> Result<(), StoreError> {
    let mut waits = 0u32;
    loop {
        let tip = archive_tip();
        if wait_for_tip && tip.is_none() {
            // The archive lane is on but the mirror has not told us its tip yet: waiting costs
            // nothing, while falling back to RPC below the tip costs forty times the compute
            // units per slot (ADR-0008).
            if waits.is_multiple_of(20) {
                log("archive_tip_unknown", Obj::new().with("waitSeconds", 30));
            }
            waits += 1;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            continue;
        }
        waits = 0;
        let state = backfill_step(Arc::clone(&rpc), db, config, program_data, tip).await?;
        if state.phase == "complete" {
            break;
        }
    }
    log(
        "backfill_collection_complete",
        Obj::new()
            .with("direction", "newest-first")
            .with("independentValidation", "pending"),
    );
    Ok(())
}
