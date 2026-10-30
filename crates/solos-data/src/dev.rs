//! Development and migration tooling: differential comparison of two decoded roots for the
//! cutover proof, and the offline fixture builders and checks the verification skill
//! (`.cursor/skills/verify-solos-data`) drives through the real binary.

use crate::decoder::schema::{TABLES, definition, key_column};
use crate::decoder::source::source_path;
use crate::jsonout::Obj;
use crate::store::{StoreError, memory_connection, query_rows, query_scalar, sql_string};
use duckdb::Connection;
use serde_json::Value;
use std::path::Path;

/// Register the latest-revision view of one decoded table as `alias`, reading `root`'s
/// `catalog.json`. The deduplication is the reader's: the highest batch number wins per key.
fn register_decoded_view(
    conn: &Connection,
    root: &Path,
    table: &str,
    alias: &str,
) -> Result<(), StoreError> {
    let catalog: Value = serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json"))?)
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
    if paths.is_empty() {
        conn.execute_batch(&format!("CREATE TABLE {alias}({})", definition(table)))?;
        return Ok(());
    }
    conn.execute(
        &format!("CREATE TABLE {alias}_reg AS SELECT value->>'path' AS path, (value->>'batch_id')::BIGINT AS batch_id FROM json_each(?::JSON)"),
        [Value::Array(registrations).to_string()],
    )?;
    conn.execute_batch(&format!(
        "CREATE VIEW {alias} AS SELECT p.* EXCLUDE(filename, batch_id) FROM (SELECT p.* EXCLUDE(filename), r.batch_id, p.filename FROM read_parquet([{}], filename=true, union_by_name=true) p JOIN {alias}_reg r ON r.path=p.filename) p QUALIFY row_number() OVER(PARTITION BY p.{} ORDER BY p.batch_id DESC)=1",
        paths.join(","),
        key_column(table)
    ))?;
    Ok(())
}

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
            register_decoded_view(&conn, root, table, &view)?;
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

/// Per-table row counts and one content hash over every latest-revision row except the
/// wall-clock `decoded_at`, for the verification skill's golden comparison. Row counts render
/// as decimal strings, the store's convention.
pub fn decoded_digest(root: &Path) -> Result<Obj, StoreError> {
    let memory = std::env::var("SOLOS_DATA_QUERY_MEMORY").unwrap_or_else(|_| "4GB".into());
    let conn = memory_connection("4", &memory)?;
    let mut rows_report = Obj::new();
    let mut content = String::new();
    for table in TABLES {
        register_decoded_view(&conn, root, table, table)?;
        rows_report.set(
            table,
            query_scalar(
                &conn,
                &format!("SELECT count(*)::VARCHAR AS n FROM {table}"),
                &[],
            )?,
        );
        let exclude = if table == "decoded_transactions" {
            " EXCLUDE(decoded_at)"
        } else {
            ""
        };
        let key = key_column(table);
        let entries = query_rows(
            &conn,
            &format!(
                "SELECT {key} AS key, to_json(struct_pack(*COLUMNS(* EXCLUDE({key})))) AS row_json
                FROM (SELECT *{exclude} FROM {table}) ORDER BY {key}"
            ),
            &[],
        )?;
        for row in &entries {
            content.push_str(row.str("key").unwrap_or(""));
            content.push('\u{1f}');
            content.push_str(row.str("row_json").unwrap_or("null"));
            content.push('\n');
        }
    }
    Ok(Obj::new().with_obj("rows", rows_report).with(
        "contentSha256",
        crate::fsutil::sha256_hex(content.as_bytes()),
    ))
}

/// Compare a decoded root against a golden JSON `{rows: {...}, contentSha256}` and report.
pub fn decoder_verify(root: &Path, golden: &Path) -> Result<Obj, StoreError> {
    let golden_text = std::fs::read_to_string(golden)?;
    let golden: Value =
        serde_json::from_str(&golden_text).map_err(|e| StoreError::Check(e.to_string()))?;
    let actual = decoded_digest(root)?.to_value();
    let expected_rows = golden.get("rows").cloned().unwrap_or(Value::Null);
    let actual_rows = actual.get("rows").cloned().unwrap_or(Value::Null);
    let expected_sha = golden
        .get("contentSha256")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let actual_sha = actual
        .get("contentSha256")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let rows_match = actual_rows == expected_rows;
    let sha_match = actual_sha == expected_sha;
    Ok(Obj::new()
        .with("ok", rows_match && sha_match)
        .with("rowsMatch", rows_match)
        .with("contentSha256Match", sha_match)
        .with("expected", expected_rows)
        .with("actual", actual_rows)
        .with("expectedContentSha256", expected_sha)
        .with("actualContentSha256", actual_sha))
}

/// The binary and bundled DuckDB versions, for the skill's doctor step.
pub fn version() -> Result<Obj, StoreError> {
    let conn = Connection::open_in_memory()?;
    let duckdb_version = conn.version()?;
    Ok(Obj::new()
        .with("binary", env!("CARGO_PKG_VERSION"))
        .with("duckdb", duckdb_version))
}

/// Build the raw fixture (`raw/catalog.json` plus `raw/staging/tx.parquet`) the real decoder
/// consumes, from the repository's golden Phoenix transactions.
pub fn make_decoder_fixture(out: &Path, fixtures_path: Option<&str>) -> Result<Obj, StoreError> {
    let text = std::fs::read_to_string(fixtures_path.unwrap_or("tests/data/phoenix-events.json"))?;
    let doc: Value = serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let fixtures = doc
        .get("transactions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    crate::fixtures::write_decoder_raw(out, &fixtures)?;
    Ok(Obj::new()
        .with("ok", true)
        .with("out", out.to_string_lossy().into_owned())
        .with("transactions", fixtures.len()))
}

/// Build the collector checkpoint the range-check journey validates, plus a `config.json` that
/// points the repository collector config at it with a matching backfill chunk.
pub fn make_range_fixture(out: &Path, chunk: i64, next: i64, gap: bool) -> Result<Obj, StoreError> {
    crate::fixtures::write_range_fixture(out, chunk, next, gap)?;
    let base_text = std::fs::read_to_string("config/phoenix.json")?;
    let mut config: Value =
        serde_json::from_str(&base_text).map_err(|e| StoreError::Check(e.to_string()))?;
    let data_dir = std::fs::canonicalize(out)?;
    config["dataDir"] = Value::String(data_dir.to_string_lossy().into_owned());
    config["backfillChunkSlots"] = Value::from(chunk);
    std::fs::write(
        out.join("config.json"),
        serde_json::to_string_pretty(&config).map_err(|e| StoreError::Check(e.to_string()))?,
    )?;
    Ok(Obj::new()
        .with("ok", true)
        .with("out", data_dir.to_string_lossy().into_owned())
        .with("chunk", chunk)
        .with("next", next)
        .with("gap", gap))
}

/// Decode every spot fixture through the RPC-shape decoder and compare the rows with the
/// fixture's recorded rows. Exit non-zero (via `ok`) on any mismatch.
pub fn spot_fixture_decode(fixtures_path: Option<&str>) -> Result<Obj, StoreError> {
    let text = std::fs::read_to_string(fixtures_path.unwrap_or("tests/data/spot-fixtures.json"))?;
    let mut doc: Value =
        serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let started = std::time::Instant::now();
    let programs = crate::spot::dex::Programs::all();
    let mut transactions = 0usize;
    let mut swaps = 0usize;
    let mut liquidity = 0usize;
    let mut mismatches: Vec<String> = Vec::new();
    let fixtures = doc
        .get_mut("transactions")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| StoreError::Check("spot fixtures have no transactions".into()))?;
    for t in fixtures {
        transactions += 1;
        let tx = crate::spot::tx::from_rpc(
            t.get("tx_b64").and_then(Value::as_str).unwrap_or(""),
            &t["meta"],
            t.get("slot").and_then(Value::as_u64).unwrap_or(0),
            u32::try_from(t.get("index").and_then(Value::as_u64).unwrap_or(0)).unwrap_or(0),
        )
        .map_err(StoreError::Check)?;
        let mut rows = crate::spot::rows::Rows::new();
        rows.add_tx(
            &tx,
            &programs,
            &crate::spot::rows::named(&tx.keys, &programs),
        );
        swaps += rows.swaps.len();
        liquidity += rows.liquidity.len();
        let got_swaps: Vec<Value> = rows
            .swaps
            .iter()
            .map(crate::spot::rows::swap_json)
            .collect();
        let got_liquidity: Vec<Value> = rows
            .liquidity
            .iter()
            .map(crate::spot::rows::liquidity_json)
            .collect();
        if Value::Array(got_swaps) != t["swaps"] || Value::Array(got_liquidity) != t["liquidity"] {
            mismatches.push(
                t.get("signature")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            );
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let rows = swaps + liquidity;
    let rows_per_second = if seconds > 0.0 {
        rows as f64 / seconds
    } else {
        0.0
    };
    Ok(Obj::new()
        .with("ok", mismatches.is_empty())
        .with("transactions", transactions)
        .with("swaps", swaps)
        .with("liquidity", liquidity)
        .with(
            "mismatches",
            Value::Array(
                mismatches
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        )
        .with("decodeSeconds", (seconds * 1000.0).round() / 1000.0)
        .with("rowsPerSecond", (rows_per_second * 100.0).round() / 100.0))
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

/// Test fixture for the crash-safety matrix (issue #56): publish `[from, to]` once with the
/// named kill point armed. When the point fires the process aborts; otherwise the range and its
/// watermark commit normally and the command prints `published`. Only compiled with the
/// `failpoints` feature; release builds have no kill points, so this command would otherwise
/// publish a range into any root it is given.
#[cfg(feature = "failpoints")]
pub fn failpoint_writer(root: &Path, name: &str, from: i64, to: i64) -> Result<(), StoreError> {
    crate::failpoints::arm_crash(&[name]);
    let mut store = crate::store::Store::open(root, crate::collector::schema::SCHEMA)?;
    let watermark = serde_json::json!({
        "signature": "",
        "slot": to,
        "completedAt": crate::jsonout::now(),
    });
    crate::collector::writer::publish_range(
        &mut store,
        root,
        from,
        to,
        Some(crate::collector::writer::Checkpoint {
            key: "W".into(),
            value: watermark,
            cycle_id: None,
        }),
    )?;
    store.close()?;
    println!("published");
    Ok(())
}
