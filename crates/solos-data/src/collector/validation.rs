//! Range validation (V1 completeness, V6 ordering coverage) before publication. A port of
//! `validation.ts`; the report object keeps its field order.

use crate::jsonout::Obj;
use crate::store::{Store, StoreError};
use serde_json::json;

/// Validate `[from, to]`.
pub fn validate_range(store: &mut Store, from: i64, to: i64) -> Result<Obj, StoreError> {
    let counts = store
        .rows(
            "SELECT count(*) AS manifest,
    count(t.signature) AS fetched,
    count(*) FILTER (WHERE t.tx_b64 IS NULL) AS missing,
    count(*) FILTER (WHERE t.single_in_slot IS NULL OR (NOT t.single_in_slot AND t.tx_index IS NULL)) AS unordered
    FROM signatures s LEFT JOIN (SELECT signature, tx_b64, single_in_slot, tx_index FROM transactions
      WHERE slot BETWEEN ? AND ?) t USING(signature) WHERE s.slot BETWEEN ? AND ?",
            &[&from, &to, &from, &to],
        )?
        .into_iter()
        .next()
        .unwrap_or_default();
    let coverage = store
        .rows(
            "SELECT count(*) AS bad FROM (
    SELECT s.slot, count(*) AS n, count(t.tx_index) AS indexed,
      count(DISTINCT t.tx_index) AS distinct_index,
      count(*) FILTER (WHERE t.single_in_slot) AS singles
    FROM signatures s LEFT JOIN (SELECT signature, single_in_slot, tx_index FROM transactions
      WHERE slot BETWEEN ? AND ?) t USING(signature) WHERE s.slot BETWEEN ? AND ?
    GROUP BY s.slot HAVING (n>1 AND (indexed<>n OR distinct_index<>n OR singles>0)) OR (n=1 AND singles<>1)
    )",
            &[&from, &to, &from, &to],
        )?
        .into_iter()
        .next()
        .unwrap_or_default();
    let missing = counts.int("missing").unwrap_or(0);
    let unordered = counts.int("unordered").unwrap_or(0);
    let bad = coverage.int("bad").unwrap_or(0);
    let ok = missing == 0 && unordered == 0 && bad == 0;
    Ok(Obj::new()
        .with("ok", ok)
        .with("V1", missing == 0)
        .with("V6", bad == 0 && unordered == 0)
        .with("missing", missing)
        .with("unordered", unordered)
        .with("badOrderingSlots", bad)
        .with("from", from)
        .with("to", to)
        .with("manifest", counts.int("manifest").unwrap_or(0))
        .with("fetched", counts.int("fetched").unwrap_or(0))
        .with(
            "independentChecks",
            json!({ "V3": "pending", "V4": "credentials_required", "V5": "provider_required" }),
        ))
}

/// The last slot of a chunk starting at `from`.
#[must_use]
pub fn chunk_end(from: i64, ceiling: i64, size: i64) -> i64 {
    ceiling.min(from + size - 1)
}

/// Refuse to publish a failed report.
pub fn assert_publishable(report: &Obj) -> Result<(), StoreError> {
    if report.get("ok") == Some(&serde_json::Value::Bool(true)) {
        return Ok(());
    }
    let show = |key: &str| {
        report
            .int(key)
            .map_or("unknown".to_owned(), |v| v.to_string())
    };
    Err(StoreError::Check(format!(
        "Range validation failed; watermark will not advance (missing={}, unordered={}, badSlots={})",
        show("missing"),
        show("unordered"),
        show("badOrderingSlots")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ends_and_refusals() {
        assert_eq!(chunk_end(100, 50_000, 20_000), 20_099);
        assert_eq!(chunk_end(49_999, 50_000, 20_000), 50_000);
        let error = assert_publishable(&Obj::new().with("ok", false))
            .unwrap_err()
            .to_string();
        assert!(error.contains("watermark"));
    }
}
