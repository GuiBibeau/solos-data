//! The collector's `status.json`, same keys in the same order as `status.ts`.

use super::catalog::write_catalog;
use super::rpc::Provider;
use crate::fsutil::write_atomic;
use crate::jsonout::{Obj, now};
use crate::store::{Store, StoreError};
use serde_json::Value;
use std::path::Path;

/// Build the status object.
pub fn status(store: &mut Store, provider: Option<&Provider>) -> Result<Obj, StoreError> {
    let counts = store
        .rows(
            "SELECT
    (SELECT count(*) FROM signatures) AS signatures,
    (SELECT count(*) FROM transactions) AS transactions,
    (SELECT count(*) FROM files WHERE status='active') AS active_files,
    (SELECT count(*) FROM errors) AS errors,
    (SELECT min(slot) FROM signatures) AS oldest_slot,
    (SELECT max(slot) FROM signatures) AS newest_slot",
            &[],
        )?
        .into_iter()
        .next()
        .unwrap_or_default();
    let kv = |store: &mut Store, name: &str| -> Result<Value, StoreError> {
        Ok(store.get(name)?.unwrap_or(Value::Null))
    };
    let mut value = Obj::new().with("at", now());
    value.extend(counts.clone());
    value.set("H0", kv(store, "H0")?);
    value.set("watermark", kv(store, "W")?);
    value.set("backfill", kv(store, "backfill")?);
    value.set("walk", kv(store, "walk/backfill")?);
    value.set("retention", kv(store, "retention")?);
    value.set("checkpointRepack", kv(store, "checkpoint-repack")?);
    value.set("maintenance", kv(store, "maintenance")?);
    value.set_obj("checkpointCounts", counts);
    value.set("tailCycle", kv(store, "tail-active")?);
    value.set("tailHealth", kv(store, "tail-health")?);
    value.set("exchange", kv(store, "exchange")?);
    match provider {
        Some(provider) => {
            value.set_obj("metrics", provider.counters());
            value.set_obj("storageTimings", store.timings());
            value.set("effectiveCuPerSecond", provider.limiter.rate());
            value.set("maximumCuPerSecond", provider.limiter.maximum_rate);
            value.set(
                "concurrency",
                serde_json::to_value(provider.limiter.windows()).unwrap_or(Value::Null),
            );
        }
        None => {
            value.set("metrics", Value::Null);
            value.set_obj("storageTimings", store.timings());
            value.set("effectiveCuPerSecond", Value::Null);
            value.set("maximumCuPerSecond", Value::Null);
            value.set("concurrency", Value::Null);
        }
    }
    value.set("lastError", kv(store, "last-error")?);
    value.set(
        "acceptance",
        "collecting; independent validation and sealing pending",
    );
    Ok(value)
}

/// Write the catalog, then `status.json`.
pub fn write_status(
    store: &mut Store,
    root: &Path,
    provider: Option<&Provider>,
) -> Result<(), StoreError> {
    write_catalog(store, root)?;
    let body = status(store, provider)?;
    write_atomic(
        &root.join("status.json"),
        format!("{}\n", body.to_json()).as_bytes(),
    )?;
    Ok(())
}
