//! The source's files: `tables/kalshi/<dataset>/<SERIES>/<close day>.parquet`, each merged on
//! its key so a rerun replaces rows instead of duplicating them, and read back for the walk.

use super::ladder::Contract;
use super::rows::Kind;
use crate::augment::ledger::register;
use crate::augment::parquet::{Target, stage_json, write_merged};
use crate::augment::series::Ctx;
use crate::jsonout::Obj;
use crate::store::{StoreError, sql_string};
use serde_json::Value;

/// Source name in the catalog.
pub const SOURCE: &str = "kalshi";

/// Where one write goes.
pub struct Dest<'a> {
    /// What the rows are.
    pub kind: Kind,
    /// Dataset directory.
    pub dataset: &'a str,
    /// Series ticker (the file's `symbol`).
    pub series: &'a str,
    /// Close day (the period label).
    pub day: &'a str,
    /// Whether the day's rows are final.
    pub complete: bool,
}

/// Merge `rows` into the destination's file; the row count of the file afterwards.
pub async fn write(ctx: &Ctx, dest: &Dest<'_>, rows: &[Value]) -> Result<i64, StoreError> {
    let staged = stage_json(&ctx.staging(), rows)?;
    let target = Target {
        source: SOURCE.into(),
        dataset: dest.dataset.into(),
        symbol: dest.series.into(),
        phoenix_symbol: None,
        period: dest.day.into(),
        complete: dest.complete,
    };
    let select = dest.kind.select(&staged, &target.constant_columns());
    let key = dest.kind.key();
    let root = ctx.root.clone();
    let result = ctx
        .db
        .run(move |store| {
            let record = write_merged(store, &root, &select, key, &target)?;
            register(store, &record, None)?;
            Ok(record.row_count)
        })
        .await;
    let _ = std::fs::remove_file(&staged);
    result
}

/// The contracts of a series closing on `days`, from the files that exist.
pub async fn contracts(
    ctx: &Ctx,
    series: &str,
    days: &[String],
) -> Result<Vec<Contract>, StoreError> {
    let dir = ctx
        .root
        .join("tables")
        .join(SOURCE)
        .join("contracts")
        .join(series);
    let files: Vec<String> = days
        .iter()
        .map(|d| dir.join(format!("{d}.parquet")))
        .filter(|p| p.is_file())
        .map(|p| sql_string(&p.to_string_lossy()))
        .collect();
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT ticker, event_ticker, strike::VARCHAR AS strike, open_ms, close_ms,
                expiration_value::VARCHAR AS expiration_value, volume::VARCHAR AS volume, tier
         FROM read_parquet([{}], union_by_name=true)",
        files.join(", ")
    );
    let rows = ctx.db.run(move |store| store.rows(&sql, &[])).await?;
    Ok(rows.iter().filter_map(contract).collect())
}

fn float(row: &Obj, key: &str) -> Option<f64> {
    row.str(key).and_then(|s| s.parse().ok())
}

fn contract(row: &Obj) -> Option<Contract> {
    let close_ms = row.int("close_ms")?;
    Some(Contract {
        ticker: row.str("ticker")?.to_owned(),
        event: row.str("event_ticker").unwrap_or("").to_owned(),
        strike: float(row, "strike"),
        open_ms: row.int("open_ms").unwrap_or(close_ms),
        close_ms,
        expiration_value: float(row, "expiration_value"),
        volume: float(row, "volume").unwrap_or(0.0),
        historical: row.str("tier") == Some("historical"),
    })
}
