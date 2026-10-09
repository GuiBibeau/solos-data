//! Read-only SQL over the augment root: one view per (source, dataset) over the catalog's
//! Parquet files in an independent in-memory DuckDB, so queries run while a sync or capture
//! writes.

use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// Run `sql` with views `<source>_<dataset>` over the catalogued files.
pub fn query_augment(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    let head = sql.trim_start().to_lowercase();
    if !(head.starts_with("select")
        || head.starts_with("with")
        || head.starts_with("describe")
        || head.starts_with("show"))
    {
        return Err(StoreError::Check(
            "query must be read-only (SELECT, WITH, DESCRIBE or SHOW)".into(),
        ));
    }
    let text = std::fs::read_to_string(root.join("catalog.json"))?;
    let catalog: Value =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let views = view_paths(&catalog);
    let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
    let conn = memory_connection("4", &memory)?;
    for (view, paths) in &views {
        let list: Vec<String> = paths
            .iter()
            .map(|p| sql_string(&root.join(p).to_string_lossy()))
            .collect();
        conn.execute_batch(&format!(
            "CREATE VIEW {view} AS SELECT * FROM read_parquet([{}], union_by_name=true)",
            list.join(",")
        ))?;
    }
    let rows = query_rows(&conn, sql, &[])?;
    Ok(Obj::new()
        .with("views", views.keys().cloned().collect::<Vec<_>>())
        .with("rowCount", rows.len())
        .with_rows("rows", rows))
}

/// `<source>_<dataset>` to its files, from a catalog.
#[must_use]
pub fn view_paths(catalog: &Value) -> BTreeMap<String, Vec<String>> {
    let mut views: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for file in catalog
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(source), Some(dataset), Some(path)) = (
            file.get("source").and_then(Value::as_str),
            file.get("dataset").and_then(Value::as_str),
            file.get("path").and_then(Value::as_str),
        ) else {
            continue;
        };
        views
            .entry(format!("{source}_{dataset}"))
            .or_default()
            .push(path.to_owned());
    }
    views
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_files_into_views() {
        let catalog = serde_json::json!({ "files": [
            { "source": "binance", "dataset": "klines", "path": "a" },
            { "source": "binance", "dataset": "klines", "path": "b" },
            { "source": "deribit", "dataset": "dvol_60s", "path": "c" }
        ]});
        let views = view_paths(&catalog);
        assert_eq!(views["binance_klines"], ["a", "b"]);
        assert_eq!(views["deribit_dvol_60s"], ["c"]);
    }
}
