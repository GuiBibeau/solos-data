//! Read-only SQL over the registered decoded Parquet, with the latest revision of every row. A
//! port of `decode/reader.ts`.

use super::schema::{TABLES, definition, key_column};
use super::source::source_path;
use crate::jsonout::Obj;
use crate::lease::with_read_lease;
use crate::store::{StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::path::Path;

/// Run one `SELECT` or `WITH` statement against a decoded root.
pub fn query_decoded(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    with_read_lease(root, || read_decoded(root, sql))
}

fn read_decoded(root: &Path, sql: &str) -> Result<Obj, StoreError> {
    let head = sql.trim_start().to_lowercase();
    let is_select = ["select", "with"].iter().any(|kw| {
        head.starts_with(kw)
            && !head[kw.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
    });
    if !is_select || sql.contains(';') {
        return Err(StoreError::Check(
            "query accepts one SELECT or WITH statement".into(),
        ));
    }
    let catalog: Value = serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
        .map_err(|e| StoreError::Check(e.to_string()))?;
    let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
    let conn = memory_connection("4", &memory)?;
    let files = catalog
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for table in TABLES {
        let registered: Vec<&Value> = files
            .iter()
            .filter(|f| f.get("table_name").and_then(Value::as_str) == Some(table))
            .collect();
        if registered.is_empty() {
            conn.execute_batch(&format!("CREATE TABLE {table}({})", definition(table)))?;
            continue;
        }
        let mut paths = Vec::new();
        let mut registrations = Vec::new();
        for file in registered {
            let path = source_path(root, file.get("path").and_then(Value::as_str).unwrap_or(""))?
                .to_string_lossy()
                .into_owned();
            let batch = file
                .get("batch_id")
                .map(|b| {
                    b.as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .or_else(|| b.as_i64())
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            registrations.push(Value::Object(
                [
                    ("path".to_owned(), Value::String(path.clone())),
                    ("batch_id".to_owned(), Value::from(batch)),
                ]
                .into_iter()
                .collect(),
            ));
            paths.push(sql_string(&path));
        }
        conn.execute(
            &format!("CREATE TABLE {table}_registrations AS SELECT value->>'path' AS path, (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?::JSON)"),
            [Value::Array(registrations).to_string()],
        )?;
        let base = format!(
            "SELECT p.* EXCLUDE(filename), r.batch_id FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN {table}_registrations r ON r.path=p.filename",
            paths.join(",")
        );
        let key = key_column(table);
        let relation = if table == "decoded_transactions" {
            format!("({base}) p")
        } else {
            format!(
                "({base}) p JOIN decoded_transactions d ON d.signature=p.signature AND d.source_hash=p.source_hash"
            )
        };
        conn.execute_batch(&format!("CREATE VIEW {table} AS SELECT p.* EXCLUDE(batch_id) FROM {relation} QUALIFY row_number() OVER(PARTITION BY p.{key} ORDER BY p.batch_id DESC)=1"))?;
    }
    let rows = query_rows(&conn, sql, &[])?;
    Ok(Obj::new()
        .with(
            "catalogAt",
            catalog.get("at").cloned().unwrap_or(Value::Null),
        )
        .with(
            "decoderVersion",
            catalog
                .get("decoderVersion")
                .cloned()
                .unwrap_or(Value::Null),
        )
        .with(
            "acceptance",
            catalog.get("acceptance").cloned().unwrap_or(Value::Null),
        )
        .with_rows("rows", rows))
}
