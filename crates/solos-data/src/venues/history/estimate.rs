//! `venues history estimate` (ADR-0013): how many transactions each history address has, sampled
//! at a few points between the floor and the top, from `getTransactionsForAddress` in
//! `signatures` mode (one page of up to 1,000 signatures from a slot, all statuses), so the
//! backfill's credits and time can be planned before it runs. Read-only; a few dozen requests.

use super::config::{HistoryConfig, floor_seconds};
use super::floor::slot_at;
use crate::collector::rpc::Rpc;
use crate::jsonout::Obj;
use crate::venues::rpc::call;
use serde_json::{Value, json};

/// Slots one sample may span.
const SAMPLE_SLOTS: i64 = 20_000;

/// One density sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    /// First slot.
    pub slot: i64,
    /// Slots the counted signatures span.
    pub slots: i64,
    /// Signatures counted.
    pub signatures: u64,
    /// Of which failed.
    pub failed: u64,
}

impl Sample {
    /// Signatures per slot.
    #[must_use]
    pub fn per_slot(&self) -> f64 {
        self.signatures as f64 / self.slots.max(1) as f64
    }
}

/// The sample of a page that started at `slot`: a full page spans to its last slot, a short one
/// the whole range.
#[must_use]
pub fn sample_of(slot: i64, rows: &[Value], limit: usize) -> Sample {
    let last = rows
        .iter()
        .filter_map(|r| r.get("slot").and_then(Value::as_i64))
        .max();
    let slots = match last {
        Some(last) if rows.len() >= limit => last - slot + 1,
        _ => SAMPLE_SLOTS,
    };
    Sample {
        slot,
        slots,
        signatures: rows.len() as u64,
        failed: rows
            .iter()
            .filter(|r| r.get("err").is_some_and(|e| !e.is_null()))
            .count() as u64,
    }
}

async fn sample(rpc: &dyn Rpc, address: &str, slot: i64) -> Result<Sample, String> {
    let options = json!({
        "transactionDetails": "signatures", "sortOrder": "asc", "limit": 1000,
        "commitment": "finalized",
        "filters": { "slot": { "gte": slot, "lte": slot + SAMPLE_SLOTS - 1 }, "tokenAccounts": "none" }
    });
    let reply = call(rpc, "getTransactionsForAddress", json!([address, options]))
        .await
        .map_err(|e| e.to_string())?;
    let rows = reply
        .result
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(sample_of(slot, &rows, 1000))
}

/// Sample every address of every enabled source at `points` evenly spaced slots.
///
/// # Errors
///
/// RPC failure or an invalid config.
pub async fn estimate(rpc: &dyn Rpc, config: &HistoryConfig, points: usize) -> Result<Obj, String> {
    let venues = config.venues().map_err(|e| e.to_string())?;
    let sources = config.resolve(&venues).map_err(|e| e.to_string())?;
    let floor_s = floor_seconds(&config.floor_time).map_err(|e| e.to_string())?;
    let floor = slot_at(rpc, floor_s).await?;
    let mut out = Obj::new().with("floorSlot", floor);
    for source in &sources {
        let span = source.top - floor;
        let days = span as f64 / 216_000.0;
        let mut per_address = Vec::new();
        let mut total_per_slot = 0.0;
        let mut total_failed = 0.0;
        for address in &source.addresses {
            let mut samples = Vec::new();
            for i in 0..points.max(1) {
                let slot = floor + span * i as i64 / points.max(1) as i64;
                samples.push(sample(rpc, address, slot).await?);
            }
            let mean = samples.iter().map(Sample::per_slot).sum::<f64>() / samples.len() as f64;
            let failed = samples.iter().map(|s| s.failed as f64).sum::<f64>()
                / samples
                    .iter()
                    .map(|s| s.signatures as f64)
                    .sum::<f64>()
                    .max(1.0);
            total_per_slot += mean;
            total_failed += mean * failed;
            per_address.push(json!({
                "address": address, "perDay": (mean * 216_000.0).round(), "failedShare": failed,
                "samples": samples.iter().map(|s| json!({ "slot": s.slot, "slots": s.slots, "signatures": s.signatures, "failed": s.failed })).collect::<Vec<_>>(),
            }));
        }
        let signatures = total_per_slot * span as f64;
        let ok = (total_per_slot - total_failed) * span as f64;
        // Credits: a bulk page of up to 100 successful transactions costs 10, a manifest page of
        // 1,000 signatures 1 (Helius prices in the venues config).
        let credits = ok / 100.0 * 10.0 + signatures / 1000.0;
        out.set_obj(
            &source.name,
            Obj::new()
                .with("top", source.top)
                .with("days", (days * 10.0).round() / 10.0)
                .with("signatures", signatures.round())
                .with("successful", ok.round())
                .with("credits", credits.round())
                .with("addresses", per_address),
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_page_spans_to_its_last_slot_and_a_short_one_the_range() {
        let rows: Vec<Value> = (0..1000)
            .map(|i| json!({ "slot": 100 + i / 10, "err": if i % 4 == 0 { json!({"x": 1}) } else { Value::Null } }))
            .collect();
        let full = sample_of(100, &rows, 1000);
        assert_eq!((full.slots, full.signatures, full.failed), (100, 1000, 250));
        assert!((full.per_slot() - 10.0).abs() < 1e-9);
        let short = sample_of(100, &rows[..10], 1000);
        assert_eq!(short.slots, SAMPLE_SLOTS);
    }
}
