//! `snapshot query --sql`: read-only SQL over the catalogued files, one view per dataset, in an
//! independent in-memory DuckDB under a reader lease, so it runs while the lane writes.

use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// Run `sql` with a view per dataset (`raw_accounts`, `book_snapshots`, `book_orders`,
/// `splines`, `market_state`, `trader_index`, `traders`, `trader_positions`, `active_positions`).
pub fn query_state(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    let head = sql.trim_start().to_lowercase();
    if !["select", "with", "describe", "show", "summarize"]
        .iter()
        .any(|w| head.starts_with(w))
    {
        return Err(StoreError::Check(
            "query must be read-only (SELECT, WITH, DESCRIBE, SHOW or SUMMARIZE)".into(),
        ));
    }
    crate::lease::with_read_lease(root, || {
        let text = std::fs::read_to_string(root.join("catalog.json"))
            .map_err(|_| StoreError::Check("no catalog under the state root".into()))?;
        let catalog: Value =
            serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
        let mut views: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for file in catalog
            .get("files")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let (Some(dataset), Some(path)) = (
                file.get("dataset").and_then(Value::as_str),
                file.get("path").and_then(Value::as_str),
            ) {
                views
                    .entry(dataset.to_owned())
                    .or_default()
                    .push(sql_string(&root.join(path).to_string_lossy()));
            }
        }
        let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
        let conn = memory_connection("4", &memory)?;
        for (view, paths) in &views {
            conn.execute_batch(&format!(
                "CREATE VIEW {view} AS SELECT * FROM read_parquet([{}], union_by_name=true)",
                paths.join(",")
            ))?;
        }
        let rows = query_rows(&conn, sql, &[])?;
        Ok(Obj::new()
            .with("views", views.keys().cloned().collect::<Vec<_>>())
            .with("rowCount", rows.len())
            .with_rows("rows", rows))
    })
}
