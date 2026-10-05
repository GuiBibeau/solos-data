//! Development and migration tooling that the TypeScript version did not need: differential
//! comparison of two decoded roots for the cutover proof.

use crate::decoder::schema::{TABLES, definition, key_column};
use crate::decoder::source::source_path;
use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, query_rows, sql_string};
use serde_json::Value;
use std::path::Path;

/// Compare the latest-revision rows of every table in two decoded roots. `decoded_at` is
/// excluded because it is a wall-clock stamp. Reports counts and the first mismatching keys.
pub fn compare_decoded(left: &Path, right: &Path) -> Result<Obj, StoreError> {
    let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
    let conn = memory_connection("4", &memory)?;
    let mut report = Obj::new();
    let mut identical = true;
    for table in TABLES {
        let mut sides = Vec::new();
        for (side, root) in [("l", left), ("r", right)] {
            let view = format!("{side}_{table}");
            let catalog: Value =
                serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
                    .map_err(|e| StoreError::Check(e.to_string()))?;
            let files: Vec<Value> = catalog
                .get("files")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut paths = Vec::new();
            let mut registrations = Vec::new();
            for file in files
                .iter()
                .filter(|f| f.get("table_name").and_then(Value::as_str) == Some(table))
            {
                let path =
                    source_path(root, file.get("path").and_then(Value::as_str).unwrap_or(""))?
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
            if paths.is_empty() {
                conn.execute_batch(&format!(
                    "CREATE VIEW {view} AS SELECT * FROM (SELECT 1) WHERE false"
                ))?;
                conn.execute_batch(&format!(
                    "DROP VIEW {view}; CREATE TABLE {view}({})",
                    definition(table)
                ))?;
            } else {
                conn.execute(
                    &format!("CREATE TABLE {view}_reg AS SELECT value->>'path' AS path, (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?::JSON)"),
                    [Value::Array(registrations).to_string()],
                )?;
                conn.execute_batch(&format!(
                    "CREATE VIEW {view} AS SELECT p.* EXCLUDE(filename, batch_id) FROM (SELECT p.* EXCLUDE(filename), r.batch_id, p.filename FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN {view}_reg r ON r.path=p.filename) p QUALIFY row_number() OVER(PARTITION BY p.{} ORDER BY p.batch_id DESC)=1",
                    paths.join(","),
                    key_column(table)
                ))?;
            }
            sides.push(view);
        }
        let exclude = if table == "decoded_transactions" {
            " EXCLUDE(decoded_at)"
        } else {
            ""
        };
        let key = key_column(table);
        let digest = |view: &str| {
            format!(
                "SELECT {key} AS key, sha256(to_json(struct_pack(*COLUMNS(* EXCLUDE({key}))))) AS digest FROM (SELECT *{exclude} FROM {view})"
            )
        };
        let only = |a: &str, b: &str| {
            format!(
                "SELECT key FROM (({}) EXCEPT ({})) ORDER BY key LIMIT 5",
                digest(a),
                digest(b)
            )
        };
        let count = |view: &str| -> Result<Value, StoreError> {
            Ok(
                query_rows(&conn, &format!("SELECT count(*) AS n FROM {view}"), &[])?
                    .first()
                    .and_then(|r| r.get("n").cloned())
                    .unwrap_or(Value::Null),
            )
        };
        let left_only: Vec<Value> = query_rows(&conn, &only(&sides[0], &sides[1]), &[])?
            .into_iter()
            .filter_map(|r| r.get("key").cloned())
            .collect();
        let right_only: Vec<Value> = query_rows(&conn, &only(&sides[1], &sides[0]), &[])?
            .into_iter()
            .filter_map(|r| r.get("key").cloned())
            .collect();
        let same = left_only.is_empty() && right_only.is_empty();
        identical &= same;
        report.set_obj(
            table,
            Obj::new()
                .with("left", count(&sides[0])?)
                .with("right", count(&sides[1])?)
                .with("identical", same)
                .with("onlyLeft", Value::Array(left_only))
                .with("onlyRight", Value::Array(right_only)),
        );
    }
    Ok(Obj::new()
        .with("identical", identical)
        .with_obj("tables", report))
}

/// Test fixture for crash recovery: commit one row and one key, leave a second transaction
/// open, print `ready`, then wait to be killed.
pub fn crash_writer(root: &Path) -> Result<(), StoreError> {
    let mut store = crate::store::Store::open(root, crate::collector::schema::SCHEMA)?;
    store.transaction(|store| {
        store.exec_batch("INSERT INTO kv VALUES ('durable','true')")?;
        store.exec_batch(
            "INSERT INTO signatures VALUES ('durable',100,100,'null',[],'tail','c','fixture')",
        )?;
        Ok(())
    })?;
    store.exec_batch("BEGIN")?;
    store.exec_batch("INSERT INTO kv VALUES ('unfinished','true')")?;
    println!("ready");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
