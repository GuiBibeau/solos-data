//! The live tail cycle: walk both addresses from the overlap floor to the head, record program
//! upgrades, then fetch, order, validate and publish chunk by chunk, advancing the watermark. A
//! port of `pipeline.ts`.

use super::config::{Config, Lane};
use super::fetcher::fetch_range;
use super::ordering::order_range;
use super::rpc::{Rpc, call_value};
use super::validation::{assert_publishable, chunk_end, validate_range};
use super::walker::{make_walk, walk_to_end};
use super::writer::{Checkpoint, publish_range};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::sync::Arc;

/// Record ProgramData transactions as unclassified attention events.
pub async fn record_versions(
    db: &Db,
    program_data: &str,
    from: i64,
    to: i64,
) -> Result<(), StoreError> {
    let added = db
        .rows(
            "SELECT s.signature, s.slot FROM signatures s
    LEFT JOIN program_versions p USING(signature) WHERE p.signature IS NULL
    AND list_contains(s.source_addresses, ?) AND s.slot BETWEEN ? AND ?",
            vec![program_data.into(), from.into(), to.into()],
        )
        .await?;
    for row in added {
        let signature = row.str("signature").unwrap_or("").to_owned();
        let slot = row.int("slot").unwrap_or(0);
        db.exec(
            "INSERT OR IGNORE INTO program_versions VALUES (?, ?, ?, ?)",
            vec![
                signature.clone().into(),
                slot.into(),
                "programdata_transaction_unclassified".into(),
                now().into(),
            ],
        )
        .await?;
        log(
            "program_version_attention",
            Obj::new().with("signature", signature).with("slot", slot),
        );
    }
    Ok(())
}

/// One tail cycle; returns early when the provider head is behind the watermark.
pub async fn tail_cycle(
    rpc: Arc<dyn Rpc>,
    db: &Db,
    config: &Config,
    program_data: &str,
) -> Result<(), StoreError> {
    let mut active = db.get("tail-active").await?;
    if active.is_none() {
        let previous = db.get("W").await?;
        let anchor = match &previous {
            Some(w) => Some(w.clone()),
            None => db.get("H0").await?,
        };
        let Some(anchor) = anchor else {
            return Err(StoreError::Check("H0 is missing".into()));
        };
        let head = call_value(
            rpc.as_ref(),
            "getSlot",
            json!([{ "commitment": "finalized" }]),
            Lane::Tail,
        )
        .await
        .map_err(|e| StoreError::Check(e.to_string()))?
        .as_i64()
        .unwrap_or(0);
        let anchor_slot = anchor.get("slot").and_then(Value::as_i64).unwrap_or(0);
        if let Some(previous) = &previous
            && head < previous.get("slot").and_then(Value::as_i64).unwrap_or(0)
        {
            log(
                "provider_head_behind_watermark",
                Obj::new()
                    .with("head", head)
                    .with("watermark", previous["slot"].clone()),
            );
            return Ok(());
        }
        let floor = (anchor_slot - config.overlap_slots).max(0);
        let cycle = json!({ "id": uuid::Uuid::new_v4().to_string(), "floor": floor, "ceiling": head, "next": floor, "programData": program_data });
        let cycle2 = cycle.clone();
        db.run(move |store| {
            store.transaction(|store| {
                store.exec(
                    "INSERT OR REPLACE INTO kv VALUES (?, ?::JSON)",
                    &[&"tail-active", &cycle2.to_string()],
                )?;
                store.exec(
                    "INSERT INTO cycles VALUES (?, ?, NULL, ?, ?, ?, NULL)",
                    &[
                        &cycle2["id"].as_str().unwrap_or(""),
                        &now(),
                        &floor,
                        &head,
                        &"walking",
                    ],
                )?;
                Ok(())
            })
        })
        .await?;
        active = Some(cycle);
    }
    let mut active = active.unwrap_or(Value::Null);
    let id = active["id"].as_str().unwrap_or("").to_owned();
    let floor = active["floor"].as_i64().unwrap_or(0);
    let ceiling = active["ceiling"].as_i64().unwrap_or(0);
    let cycle_program_data = active["programData"]
        .as_str()
        .unwrap_or(program_data)
        .to_owned();
    for address in [config.program_id.clone(), cycle_program_data.clone()] {
        let mut walk = make_walk(&address, Lane::Tail, &id, floor, ceiling);
        let anchor = db.rows("SELECT signature FROM signatures WHERE slot<? AND list_contains(source_addresses, ?) ORDER BY slot DESC, signature LIMIT 1", vec![floor.into(), address.clone().into()]).await?;
        if let Some(row) = anchor.first() {
            walk.until = row.str("signature").map(str::to_owned);
        }
        walk_to_end(rpc.as_ref(), db, &format!("walk/tail/{id}/{address}"), walk).await?;
    }
    record_versions(db, &cycle_program_data, floor, ceiling).await?;
    while active["next"].as_i64().unwrap_or(i64::MAX) <= ceiling {
        let next = active["next"].as_i64().unwrap_or(0);
        let to = chunk_end(next, ceiling, config.max_slots_per_chunk);
        fetch_range(Arc::clone(&rpc), db, config, Lane::Tail, next, to).await?;
        order_range(Arc::clone(&rpc), db, Lane::Tail, next, to, 256).await?;
        let program_id = config.program_id.clone();
        let data_dir = config.data_dir.clone();
        let report = db
            .run(move |store| {
                let report = validate_range(store, next, to)?;
                assert_publishable(&report)?;
                let latest = store.rows("SELECT signature FROM signatures WHERE slot<=? AND list_contains(source_addresses, ?) ORDER BY slot DESC, signature LIMIT 1", &[&to, &program_id])?;
                let watermark = json!({ "signature": latest.first().and_then(|r| r.str("signature")).unwrap_or(""), "slot": to, "completedAt": now() });
                publish_range(store, &data_dir, next, to, Some(Checkpoint { key: "W".into(), value: watermark, cycle_id: None }))?;
                Ok(report)
            })
            .await?;
        active["next"] = Value::from(to + 1);
        // A crash before this checkpoint safely republishes an overlap; W is already durable.
        db.set("tail-active", active.clone()).await?;
        let report_json = report.to_value();
        let id2 = id.clone();
        db.run(move |store| {
            store.exec(
                "UPDATE cycles SET validation_report=?::JSON WHERE cycle_id=?",
                &[&report_json.to_string(), &id2],
            )?;
            Ok(())
        })
        .await?;
        let mut fields = report;
        fields.set("watermark", to);
        log("tail_chunk", fields);
    }
    let id2 = id.clone();
    db.run(move |store| {
        store.transaction(|store| {
            store.exec(
                "UPDATE cycles SET finished_at=?, status='ok' WHERE cycle_id=?",
                &[&now(), &id2],
            )?;
            store.exec(
                "DELETE FROM kv WHERE name=? OR name LIKE ?",
                &[&"tail-active", &format!("walk/tail/{id2}/%")],
            )?;
            Ok(())
        })
    })
    .await
}
