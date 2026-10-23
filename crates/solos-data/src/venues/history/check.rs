//! `venues history check` (ADR-0013): the history lane's files against its own `batches` rows,
//! read from the catalog only (never the running lane's ledger). Per source: the batches cover
//! one contiguous slot range with no overlap; for a venue, each batch stored exactly the
//! transactions it fetched (distinct signatures in the batch's slots) and fetched exactly its
//! manifest plus what the bulk pages added (`unlisted`); for Pyth, each batch's price rows are in
//! the files. Read-only.

use super::config::{Kind, Source};
use crate::augment::ledger::Lane;
use crate::augment::query::query_lanes;
use crate::jsonout::Obj;
use crate::store::StoreError;
use serde_json::Value;
use std::path::Path;

/// The batches of a source, one row per batch (the newest collection of a refetched one).
fn batches(source: &str) -> String {
    format!(
        "SELECT * FROM {source}_batches QUALIFY row_number() OVER (PARTITION BY from_slot ORDER BY at_ms DESC) = 1"
    )
}

/// Coverage: batches, slots, range, overlaps (a batch starting at or below the previous end).
fn coverage_sql(source: &str) -> String {
    format!(
        "WITH b AS ({}), o AS (SELECT from_slot, to_slot, lag(from_slot) OVER (ORDER BY from_slot DESC) AS above FROM b)
         SELECT count(*) AS batches, sum(to_slot - from_slot + 1) AS slots, min(from_slot) AS from_slot,
                max(to_slot) AS to_slot, count(*) FILTER (WHERE above IS NOT NULL AND to_slot <> above - 1) AS gaps_or_overlaps,
                (SELECT sum(manifest) FROM b) AS manifest, (SELECT sum(txs) FROM b) AS txs,
                (SELECT sum(undecoded) FROM b) AS undecoded, (SELECT sum(unknown) FROM b) AS unknown,
                (SELECT sum(events) FROM b) AS events, (SELECT sum(prices) FROM b) AS prices
         FROM o",
        batches(source)
    )
}

/// Per-batch comparison of stored rows with the batch's counts.
fn venue_sql(source: &str) -> String {
    format!(
        "WITH b AS ({}), t AS (SELECT DISTINCT signature, slot FROM {source}_transactions),
         per AS (SELECT b.from_slot, any_value(b.txs) AS txs, any_value(b.manifest + b.unlisted) AS listed,
                        count(t.signature) AS stored
                 FROM b LEFT JOIN t ON t.slot BETWEEN b.from_slot AND b.to_slot GROUP BY b.from_slot)
         SELECT sum(stored) AS stored, count(*) FILTER (WHERE stored <> txs OR txs <> listed) AS mismatched,
                list(from_slot ORDER BY from_slot DESC) FILTER (WHERE stored <> txs OR txs <> listed)[1:20] AS sample
         FROM per",
        batches(source)
    )
}

fn pyth_sql(source: &str) -> String {
    format!(
        "WITH b AS ({}), p AS (SELECT context_slot FROM {source}_prices WHERE signature IS NOT NULL),
         per AS (SELECT b.from_slot, any_value(b.prices) AS prices, count(p.context_slot) AS stored
                 FROM b LEFT JOIN p ON p.context_slot BETWEEN b.from_slot AND b.to_slot GROUP BY b.from_slot)
         SELECT sum(stored) AS stored, count(*) FILTER (WHERE stored <> prices) AS mismatched,
                list(from_slot ORDER BY from_slot DESC) FILTER (WHERE stored <> prices)[1:20] AS sample
         FROM per",
        batches(source)
    )
}

/// An integer that DuckDB may have rendered as a string (wide sums and counts).
fn int(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn first(result: &Obj) -> Value {
    result
        .to_value()
        .get("rows")
        .and_then(|r| r.get(0))
        .cloned()
        .unwrap_or(Value::Null)
}

/// Check every source; `ok` is false when any batch mismatches or the coverage has a hole.
///
/// # Errors
///
/// No history catalog, or a query failure other than a source without files yet.
pub fn check(root: &Path, sources: &[Source]) -> Result<Obj, StoreError> {
    let mut out = Obj::new();
    let mut ok = true;
    for source in sources {
        let name = source.name.as_str();
        let coverage = match query_lanes(root, &[Lane::History], &coverage_sql(name)) {
            Ok(result) => first(&result),
            Err(error) if error.to_string().contains("does not exist") => {
                out.set_obj(name, Obj::new().with("files", false));
                continue;
            }
            Err(error) => return Err(error),
        };
        let sql = match &source.kind {
            Kind::Venue {
                store_transactions: true,
            } => Some(venue_sql(name)),
            Kind::Venue { .. } => None,
            Kind::Pyth { .. } => Some(pyth_sql(name)),
        };
        let compare = match sql.map(|sql| query_lanes(root, &[Lane::History], &sql)) {
            None => Value::Null,
            Some(Ok(result)) => first(&result),
            // No rows file yet: every batch must have stored nothing.
            Some(Err(error)) if error.to_string().contains("does not exist") => {
                let expected = ["txs", "prices"]
                    .iter()
                    .map(|k| int(coverage.get(*k)))
                    .sum::<i64>();
                serde_json::json!({ "stored": 0, "mismatched": i64::from(expected > 0) })
            }
            Some(Err(error)) => return Err(error),
        };
        let holes = int(coverage.get("gaps_or_overlaps"));
        let mismatched = int(compare.get("mismatched"));
        let source_ok = holes == 0 && mismatched == 0;
        ok &= source_ok;
        out.set_obj(
            name,
            Obj::new()
                .with("ok", source_ok)
                .with("coverage", coverage)
                .with("stored", compare),
        );
    }
    Ok(Obj::new().with("ok", ok).with_obj("sources", out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_read_as_numbers_or_strings() {
        assert_eq!(int(Some(&serde_json::json!(3))), 3);
        assert_eq!(int(Some(&serde_json::json!("12345678901"))), 12_345_678_901);
        assert_eq!(int(Some(&Value::Null)), 0);
        assert_eq!(int(None), 0);
    }
}
