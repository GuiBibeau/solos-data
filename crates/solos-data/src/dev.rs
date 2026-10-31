//! Development and migration tooling: differential comparison of two decoded roots for the
//! cutover proof, and the offline fixture builders and checks the verification skill
//! (`.cursor/skills/verify-solos-data`) drives through the real binary.

use crate::augment::ledger::{self, Lane};
use crate::decoder::schema::{TABLES, definition, key_column};
use crate::decoder::source::source_path;
use crate::jsonout::Obj;
use crate::spot::archive::{Block, Stream};
use crate::spot::run::{Blocks, Done, HISTORY, register_batch_to, write_batch_to};
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

/// Per-table row counts over the latest-revision views, for the verification skill's golden
/// comparison. Row counts render as decimal strings, the store's convention.
fn decoded_rows(root: &Path) -> Result<Obj, StoreError> {
    let conn = memory_connection("4", "4GB")?;
    let mut rows_report = Obj::new();
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
    }
    Ok(rows_report)
}

/// Compare a decoded root against a golden JSON `{rows: {...}, contentSha256}` and report. The
/// content hash is the canonical [`crate::decoder::digest::content_digest`] (ADR-0017), which
/// hashes the schema and rows but omits `decoded_at` and `source_file` provenance.
pub fn decoder_verify(root: &Path, golden: &Path) -> Result<Obj, StoreError> {
    let golden_text = std::fs::read_to_string(golden)?;
    let golden: Value =
        serde_json::from_str(&golden_text).map_err(|e| StoreError::Check(e.to_string()))?;
    let expected_rows = golden.get("rows").cloned().unwrap_or(Value::Null);
    let actual_rows = decoded_rows(root)?.to_value();
    let expected_sha = golden
        .get("contentSha256")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let actual_sha = crate::decoder::digest::content_digest(root)?;
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
#[cfg(any(debug_assertions, test))]
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
#[cfg(any(debug_assertions, test))]
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

/// Decode every spot fixture through the RPC-shape decoder, compare the rows with the fixture's
/// recorded rows, and publish them through the production durable write and watermark: the
/// swaps, liquidity and batches Parquet files are written and registered in the spot-history
/// ledger and the cursor is advanced. Exit non-zero (via `ok`) on any mismatch.
pub fn spot_fixture_publish(fixtures_path: Option<&str>, root: &Path) -> Result<Obj, StoreError> {
    let text = std::fs::read_to_string(fixtures_path.unwrap_or("tests/data/spot-fixtures.json"))?;
    let doc: Value = serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
    let started = std::time::Instant::now();
    let programs = crate::spot::dex::Programs::all();
    let mut rows = crate::spot::rows::Rows::new();
    let mut transactions = 0usize;
    let mut mismatches: Vec<String> = Vec::new();
    let mut blocks = std::collections::HashMap::new();
    let mut delivered = std::collections::HashMap::new();
    let mut min_slot = u64::MAX;
    let mut max_slot = 0u64;
    let fixtures = doc
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| StoreError::Check("spot fixtures have no transactions".into()))?;
    for t in fixtures {
        let slot = t.get("slot").and_then(Value::as_u64).unwrap_or(0);
        let index = u32::try_from(t.get("index").and_then(Value::as_u64).unwrap_or(0)).unwrap_or(0);
        transactions += 1;
        let tx = crate::spot::tx::from_rpc(
            t.get("tx_b64").and_then(Value::as_str).unwrap_or(""),
            &t["meta"],
            slot,
            index,
        )
        .map_err(StoreError::Check)?;
        let mut tx_rows = crate::spot::rows::Rows::new();
        tx_rows.add_tx(
            &tx,
            &programs,
            &crate::spot::rows::named(&tx.keys, &programs),
        );
        let got_swaps: Vec<Value> = tx_rows
            .swaps
            .iter()
            .map(crate::spot::rows::swap_json)
            .collect();
        let got_liquidity: Vec<Value> = tx_rows
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
        blocks
            .entry(slot)
            .or_insert(Block {
                time: None,
                executed: 0,
            })
            .executed += 1;
        *delivered.entry(slot).or_default() += 1;
        min_slot = min_slot.min(slot);
        max_slot = max_slot.max(slot);
        rows.append(tx_rows);
    }
    let swaps = rows.swaps.len();
    let liquidity = rows.liquidity.len();
    let listed = blocks.len() as u64;
    let done = Done {
        stream: Stream {
            rows,
            blocks,
            delivered,
            sampled: Vec::new(),
            captured: Vec::new(),
        },
        blocks: Blocks {
            streamed: listed,
            listed,
            transactions: transactions as u64,
        },
        window: (min_slot, max_slot),
        manifests: Vec::new(),
        seconds: started.elapsed().as_secs_f64(),
    };
    let files = write_batch_to(Lane::Spot, "2GB", root, (min_slot, max_slot), &done)?;
    let next = min_slot.saturating_sub(1);
    let cursor =
        serde_json::json!({ "next": next, "done": true, "updatedAt": crate::jsonout::now() });
    let mut store = ledger::open(root, Lane::Spot)?;
    register_batch_to(
        HISTORY,
        &mut store,
        root,
        (min_slot, max_slot),
        &files,
        &cursor,
    )?;
    ledger::write_catalog(&mut store, root, Lane::Spot)?;
    let status = Obj::new()
        .with("at", crate::jsonout::now())
        .with("lane", Lane::Spot.name())
        .with("from", min_slot)
        .with("to", max_slot)
        .with("transactions", transactions)
        .with("swaps", swaps)
        .with("liquidity", liquidity)
        .with("watermarkNext", next)
        .with("files", files.len());
    ledger::write_status(root, Lane::Spot, &status)?;
    store.close()?;
    let base_text = std::fs::read_to_string("config/spot-history.json")?;
    let mut spot_config: Value =
        serde_json::from_str(&base_text).map_err(|e| StoreError::Check(e.to_string()))?;
    spot_config["dataDir"] =
        Value::String(std::fs::canonicalize(root)?.to_string_lossy().into_owned());
    std::fs::write(
        root.join("config.json"),
        serde_json::to_string_pretty(&spot_config).map_err(|e| StoreError::Check(e.to_string()))?,
    )?;
    Ok(Obj::new()
        .with("ok", mismatches.is_empty())
        .with("transactions", transactions)
        .with("swaps", swaps)
        .with("liquidity", liquidity)
        .with("watermarkNext", next)
        .with("from", min_slot)
        .with("to", max_slot)
        .with("files", files.len())
        .with(
            "mismatches",
            Value::Array(
                mismatches
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        )
        .with(
            "decodeSeconds",
            (started.elapsed().as_secs_f64() * 1000.0).round() / 1000.0,
        ))
}

/// The collector checkpoint's backfill watermark (`backfill.next`), for the gap journey's
/// assertion that a refused range check leaves it unchanged.
pub fn collector_watermark(config_path: Option<&str>) -> Result<Obj, StoreError> {
    let config = crate::collector::config::load_config(config_path)?;
    let store = crate::store::Store::open_read_only(&config.data_dir)?;
    let backfill = store.get("backfill")?;
    let next = backfill
        .as_ref()
        .and_then(|v| v.get("next"))
        .and_then(Value::as_i64);
    Ok(Obj::new()
        .with("next", next)
        .with("backfill", backfill.unwrap_or(Value::Null)))
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

/// Test fixture for the random-kill loop (issue #61): generate a deterministic fixture source
/// (one row per raw table at one slot per epoch inside `[from, to]`) and publish the range,
/// printing one progress line per publication step and `published` at the end. `INSERT OR IGNORE`
/// makes a re-run after a kill idempotent. With `recover_only`, it only runs the production
/// startup recovery ([`crate::collector::service::recover_startup`]) and exits, so the harness's
/// restart exercises the real recovery path. Only compiled with the `failpoints` feature, like
/// [`failpoint_writer`], because it advances the tail watermark into any root it is given.
#[cfg(feature = "failpoints")]
pub fn publish_fixture(
    root: &Path,
    from: i64,
    to: i64,
    recover_only: bool,
) -> Result<(), StoreError> {
    let mut store = crate::store::Store::open(root, crate::collector::schema::SCHEMA)?;
    if recover_only {
        crate::collector::service::recover_startup(&mut store, root)?;
        store.close()?;
        return Ok(());
    }
    // One row per raw table at one slot per epoch inside `[from, to]`, so a multi-epoch range
    // writes several files per table without inserting millions of rows.
    let epoch_size = 432_000i64;
    let mut slots = Vec::new();
    for epoch in from.div_euclid(epoch_size)..=to.div_euclid(epoch_size) {
        let slot = (epoch * epoch_size + 100).clamp(from, to);
        if slots.last() != Some(&slot) {
            slots.push(slot);
        }
    }
    for slot in slots {
        let signature = format!("fixture-{slot}");
        store.exec_batch(&format!(
            "INSERT OR IGNORE INTO signatures VALUES ('{signature}', {slot}, {slot}, 'null', [], 'tail', 'fixture', '2026-10-04T00:00:00Z')"
        ))?;
        store.exec_batch(&format!(
            "INSERT OR IGNORE INTO transactions VALUES ('{signature}', {slot}, {slot}, NULL, NULL, 'null', 5000, 10, 'AA==', '{{}}', '{{}}', 'tail', 'fixture', '2026-10-04T00:00:00Z', NULL)"
        ))?;
        store.exec_batch(&format!(
            "INSERT OR IGNORE INTO slot_order VALUES ('{signature}', {slot}, 0, 1)"
        ))?;
        store.exec_batch(&format!(
            "INSERT OR IGNORE INTO program_versions VALUES ('{signature}', {slot}, 'fixture', '2026-10-04T00:00:00Z')"
        ))?;
        store.exec_batch(&format!(
            "INSERT OR IGNORE INTO rpc_pages VALUES ('page-{slot}', 'getTransaction', 'fixture', '{{}}', '2026-10-04T00:00:00Z', {slot})"
        ))?;
    }
    let watermark = serde_json::json!({
        "signature": "",
        "slot": to,
        "completedAt": crate::jsonout::now(),
    });
    crate::collector::writer::publish_range_with(
        &mut store,
        root,
        from,
        to,
        Some(crate::collector::writer::Checkpoint {
            key: "W".into(),
            value: watermark,
            cycle_id: None,
        }),
        &mut |progress: crate::collector::writer::Progress| match progress.stage {
            crate::collector::writer::ProgressStage::Commit => println!("commit"),
            _ => println!(
                "{} {} epoch={}",
                progress.stage.label(),
                progress.table,
                progress.epoch
            ),
        },
    )?;
    println!("committed");
    store.close()?;
    println!("published");
    Ok(())
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
    // The watermark and the file rows just committed live in the WAL until DuckDB checkpoints
    // them into the database file. This point is not in the failpoints matrix (the matrix
    // asserts no watermark was committed); a test kills the process here so a copied checkpoint
    // plus WAL is the only place the committed W survives.
    crate::crash_point!("publish/after-watermark-commit");
    store.close()?;
    println!("published");
    Ok(())
}
