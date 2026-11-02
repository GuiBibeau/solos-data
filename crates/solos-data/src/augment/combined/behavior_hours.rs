//! Behavior of the combined hourly units (U2b phoenix-hours, U2c
//! solana-perp-hours and pyth-hours), offline: fixture Parquet trees built
//! in-test with DuckDB, then the frozen formulas on hand-computed rows, the
//! decoder-reader dedupe, the lockbox drop, the watermark closure refusal, and
//! rerun idempotence.

use super::grid;
use super::io;
use super::pyth_hours::Request as PythRequest;
use super::solana_perp_hours::Request as PerpRequest;
use crate::store::{memory_connection, sql_string};
use std::path::{Path, PathBuf};

/// The fixture day: outside every v2 lockbox week (the busy week's guard ends
/// 2026-10-06 23:00).
const DAY: &str = "2026-10-15";

struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "combined-hours-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn dir(&self) -> &Path {
        &self.0
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn day_start() -> i64 {
    grid::day_start_secs(grid::parse_date(DAY).unwrap())
}

/// The v2 lockbox body (four Monday weeks, guard 24h) and its sha256.
fn lockbox_file(dir: &Path) -> (PathBuf, String) {
    let body = super::lockbox::tests::v2_body();
    let path = dir.join("lockbox.json");
    std::fs::write(&path, &body).unwrap();
    let sha = crate::fsutil::sha256_hex(body.as_bytes());
    (path, sha)
}

/// A v2-shaped lockbox body with a different week set (still valid JSON and a
/// real v2 file, but not 864 hours), and its sha256.
fn wrong_shape_lockbox_file(dir: &Path) -> (PathBuf, String) {
    let body = serde_json::json!({
        "version": "v2",
        "created": "2026-10-22",
        "guard_hours": 24,
        "weeks": [
            {"name": "early_stress", "start": "2026-06-01T00:00:00Z", "end": "2026-06-08T00:00:00Z", "reason": "r"},
            {"name": "stress", "start": "2026-08-17T00:00:00Z", "end": "2026-08-24T00:00:00Z", "reason": "r"},
            {"name": "quiet", "start": "2026-09-07T00:00:00Z", "end": "2026-09-14T00:00:00Z", "reason": "r"}
        ]
    })
    .to_string();
    let path = dir.join("lockbox-wrong.json");
    std::fs::write(&path, &body).unwrap();
    let sha = crate::fsutil::sha256_hex(body.as_bytes());
    (path, sha)
}

fn provenance_file(dir: &Path) -> PathBuf {
    let path = dir.join("provenance.json");
    std::fs::write(
        &path,
        r#"{"solos_data_git_sha":"0123456789abcdef0123456789abcdef01234567","pstack_sha":"1ba91fc"}"#,
    )
    .unwrap();
    path
}

/// The mapping rows the units join on: Phoenix SOL, the Jupiter SOL custody,
/// and the Pyth SOL feed. `ZZZ` and flash markets are deliberately absent.
fn write_mapping(out: &Path) {
    let dir = out.join("mapping");
    std::fs::create_dir_all(&dir).unwrap();
    let header = super::mapping::COLUMNS.join(",");
    let body = [
        "SOL,phoenix,SOL,,,77,0,0,price_check,true,,0.001000,73",
        "SOL,jup,SOL,,,Cust1,,,,true,,,",
        "SOL,pyth,SOL/USD,,,feed-sol,,,,true,,,",
    ]
    .join("\n");
    std::fs::write(dir.join("assets.csv"), format!("{header}\n{body}\n")).unwrap();
}

/// One fills row of the Phoenix fixture, as a SQL VALUES tuple.
struct Fill {
    event_id: &'static str,
    signature: &'static str,
    source_hash: &'static str,
    slot: i64,
    tx_index: i64,
    ordinal: i64,
    block_time: i64,
    symbol: &'static str,
    ticks: i64,
    base_lots: i64,
    maker_side: &'static str,
}

impl Fill {
    fn values(&self, tick_size: i64, base_dec: i64, quote_dec: i64) -> String {
        format!(
            "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            sql_string(self.event_id),
            sql_string(self.signature),
            sql_string(self.source_hash),
            self.slot,
            self.tx_index,
            self.ordinal,
            self.block_time,
            sql_string(self.symbol),
            tick_size,
            base_dec,
            quote_dec,
            self.ticks,
            self.base_lots,
            sql_string(self.maker_side),
        )
    }
}

/// Write a Parquet file with the fills columns the unit reads.
fn write_fills_parquet(conn: &duckdb::Connection, path: &Path, rows: &[String]) {
    let select = if rows.is_empty() {
        "SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS signature, NULL::VARCHAR AS source_hash,
                NULL::BIGINT AS slot, NULL::BIGINT AS tx_index, NULL::BIGINT AS event_ordinal,
                NULL::BIGINT AS block_time, NULL::VARCHAR AS asset_symbol, NULL::BIGINT AS tick_size,
                NULL::BIGINT AS base_lot_decimals, NULL::BIGINT AS quote_lot_decimals,
                NULL::BIGINT AS price_ticks, NULL::BIGINT AS base_lots, NULL::VARCHAR AS maker_side
         WHERE false"
            .to_owned()
    } else {
        format!(
            "SELECT * FROM (VALUES {}) t(event_id, signature, source_hash, slot, tx_index,
             event_ordinal, block_time, asset_symbol, tick_size, base_lot_decimals,
             quote_lot_decimals, price_ticks, base_lots, maker_side)",
            rows.join(", ")
        )
    };
    conn.execute_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET)",
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
}

/// The tx columns the unit reads: signature, source_hash, slot.
fn write_tx_parquet(conn: &duckdb::Connection, path: &Path, rows: &[String]) {
    let select = if rows.is_empty() {
        "SELECT NULL::VARCHAR AS signature, NULL::VARCHAR AS source_hash, NULL::BIGINT AS slot
         WHERE false"
            .to_owned()
    } else {
        format!(
            "SELECT * FROM (VALUES {}) t(signature, source_hash, slot)",
            rows.join(", ")
        )
    };
    conn.execute_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET)",
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
}

/// Write a decoded root: `catalog.json` plus the registered Parquet files.
/// `created_at` stamps every registered file (the catalog snapshot drops files
/// created after it).
fn write_decoded_root(
    conn: &duckdb::Connection,
    root: &Path,
    fills_files: &[(&str, i64, Vec<String>)],
    tx_files: &[(&str, i64, Vec<String>)],
    fills_writer: &dyn Fn(&duckdb::Connection, &Path, &[String]),
    tx_writer: &dyn Fn(&duckdb::Connection, &Path, &[String]),
    created_at: &str,
) {
    let mut entries: Vec<(&str, &str, i64, PathBuf)> = Vec::new();
    for (relative, batch_id, rows) in fills_files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        fills_writer(conn, &path, rows);
        entries.push((relative, "fills", *batch_id, path));
    }
    for (relative, batch_id, rows) in tx_files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        tx_writer(conn, &path, rows);
        entries.push((relative, "decoded_transactions", *batch_id, path));
    }
    let files: Vec<String> = entries
        .iter()
        .map(|(relative, table, batch_id, path)| {
            let bytes = std::fs::read(path).unwrap();
            let sha = crate::fsutil::sha256_hex(&bytes);
            format!(
                r#"{{"path": {}, "table_name": {}, "row_count": "0", "sha256": {}, "batch_id": {}, "created_at": {}}}"#,
                serde_json::json!(relative),
                serde_json::json!(table),
                serde_json::json!(sha),
                serde_json::json!(batch_id.to_string()),
                serde_json::json!(created_at),
            )
        })
        .collect();
    let catalog = format!(
        r#"{{"at": "2026-11-01T00:00:00Z", "schemaVersion": 1, "files": [{}]}}"#,
        files.join(",")
    );
    std::fs::write(root.join("catalog.json"), catalog).unwrap();
}

fn tx_row(signature: &str, source_hash: &str, slot: i64) -> String {
    format!(
        "({}, {}, {slot})",
        sql_string(signature),
        sql_string(source_hash)
    )
}

/// The unit receipt of `unit`, read back from `<out>/<table>/receipt-<unit>.json`
/// (the shared receipt path builder is the thing under test here).
fn unit_receipt(out: &Path, table: &str, unit: &str) -> serde_json::Value {
    let path = super::receipt::receipt_path(out, table, unit);
    assert!(path.is_file(), "unit receipt missing: {}", path.display());
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// The main Phoenix fixture: three hand-computed SOL fills in hour 0 (two
/// minutes of last prices), an unmapped ZZZ fill, and a sentinel fill the next
/// morning that closes the whole requested day.
fn build_main_decoded(dir: &Path) -> PathBuf {
    let conn = memory_connection("1", "512MB").unwrap();
    let root = dir.join("decoded");
    let h0 = day_start();
    let fills = vec![
        Fill {
            event_id: "e-sol-1",
            signature: "sigA",
            source_hash: "hA",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: h0 + 60,
            symbol: "SOL",
            ticks: 100,
            base_lots: 1500,
            maker_side: "ask",
        }
        .values(1, 2, 2),
        Fill {
            event_id: "e-sol-2",
            signature: "sigA",
            source_hash: "hA",
            slot: 432_100_000,
            tx_index: 2,
            ordinal: 0,
            block_time: h0 + 120,
            symbol: "SOL",
            ticks: 200,
            base_lots: 1000,
            maker_side: "bid",
        }
        .values(1, 2, 2),
        Fill {
            event_id: "e-sol-3",
            signature: "sigA",
            source_hash: "hA",
            slot: 432_100_000,
            tx_index: 3,
            ordinal: 0,
            block_time: h0 + 130,
            symbol: "SOL",
            ticks: 200,
            base_lots: 500,
            maker_side: "ask",
        }
        .values(1, 2, 2),
        Fill {
            event_id: "e-zzz-1",
            signature: "sigB",
            source_hash: "hB",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: h0 + 70,
            symbol: "ZZZ",
            ticks: 5,
            base_lots: 100,
            maker_side: "bid",
        }
        .values(10, 0, 0),
    ];
    let sentinel = Fill {
        event_id: "e-sol-s",
        signature: "sigC",
        source_hash: "hC",
        slot: 432_100_000,
        tx_index: 1,
        ordinal: 0,
        block_time: day_start() + 86_400 + 3 * 3600,
        symbol: "SOL",
        ticks: 250,
        base_lots: 100,
        maker_side: "ask",
    }
    .values(1, 0, 0);
    write_decoded_root(
        &conn,
        &root,
        &[(
            "tables/fills/epoch=1000/fills.parquet",
            1,
            [fills, vec![sentinel]].concat(),
        )],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigA", "hA", 432_100_000),
                tx_row("sigB", "hB", 432_100_001),
                tx_row("sigC", "hC", 432_100_002),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    root
}

/// The corrections fixture: signature s1 is corrected in batch 2 (new content
/// hash), so batch 1's e1 revision and e3 (which only exists under the
/// superseded parent) must vanish; s2 keeps its single revision.
fn build_corrections_decoded(dir: &Path) -> PathBuf {
    let conn = memory_connection("1", "512MB").unwrap();
    let root = dir.join("decoded-corrections");
    let h0 = day_start();
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                tx_index: i64,
                block_time: i64,
                ticks: i64,
                maker: &'static str| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot: 432_100_000,
            tx_index,
            ordinal: 0,
            block_time,
            symbol: "SOL",
            ticks,
            base_lots: 100,
            maker_side: maker,
        }
        .values(1, 0, 0)
    };
    let batch1 = vec![
        fill("e1", "s1", "h1", 1, h0 + 60, 100, "ask"),
        fill("e3", "s1", "h1", 2, h0 + 61, 999, "ask"),
        fill("e2", "s2", "h2", 3, h0 + 10, 400, "bid"),
    ];
    let batch2 = vec![
        fill("e1", "s1", "h1n", 1, h0 + 120, 300, "ask"),
        // sentinel the next morning closes the requested day
        fill("e4", "s3", "h3", 1, h0 + 86_400 + 3 * 3600, 300, "ask"),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[
            ("tables/fills/epoch=1000/b1.parquet", 1, batch1),
            ("tables/fills/epoch=1000/b2.parquet", 2, batch2),
        ],
        &[
            (
                "tables/decoded_transactions/epoch=1000/t1.parquet",
                1,
                vec![
                    tx_row("s1", "h1", 432_100_000),
                    tx_row("s2", "h2", 432_100_001),
                ],
            ),
            (
                "tables/decoded_transactions/epoch=1000/t2.parquet",
                2,
                vec![
                    tx_row("s1", "h1n", 432_100_000),
                    tx_row("s3", "h3", 432_100_002),
                ],
            ),
        ],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    root
}

/// The unclosed fixture: no sentinel, so the watermark stops mid-day and the
/// full-day request must be refused.
fn build_unclosed_decoded(dir: &Path) -> PathBuf {
    let conn = memory_connection("1", "512MB").unwrap();
    let root = dir.join("decoded-unclosed");
    let h0 = day_start();
    let fills = vec![
        Fill {
            event_id: "e-early",
            signature: "sigE",
            source_hash: "hE",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: h0 + 130,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![tx_row("sigE", "hE", 432_100_003)],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    root
}

/// A phoenix request against one decoded root.
fn phoenix_request<'a>(
    out: &'a Path,
    decoded: &'a Path,
    lockbox: &'a Path,
    lockbox_sha: &'a str,
    provenance: &'a Path,
    since: &'a str,
    until: &'a str,
) -> super::phoenix_hours::Request<'a> {
    super::phoenix_hours::Request {
        out,
        lockbox,
        lockbox_sha256: lockbox_sha,
        provenance,
        phoenix_decoded: decoded,
        since,
        until,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    }
}

/// Read one output hour file into (asset_symbol, asset_id, mapped, n_trades,
/// vol_usd, buy_usd, first_px, last_px, rv2, n_minutes) tuples.
#[allow(clippy::type_complexity)]
fn read_phoenix_hour(
    conn: &duckdb::Connection,
    out: &Path,
    hour: i64,
) -> Vec<(
    String,
    Option<String>,
    bool,
    i64,
    f64,
    f64,
    f64,
    f64,
    f64,
    i64,
)> {
    let path = out.join(io::hour_partition(hour, super::phoenix_hours::TABLE));
    if !path.is_file() {
        return Vec::new();
    }
    let rows = crate::store::query_rows(
        conn,
        &format!(
            "SELECT asset_symbol, asset_id, mapped, n_trades, vol_usd, buy_usd,
                    first_px, last_px, rv2, n_minutes
             FROM read_parquet('{}') ORDER BY asset_symbol",
            path.to_string_lossy()
        ),
        &[],
    )
    .unwrap();
    rows.iter()
        .map(|row| {
            (
                row.str("asset_symbol").unwrap().to_owned(),
                row.str("asset_id").map(str::to_owned),
                row.get("mapped")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                row.int("n_trades").unwrap(),
                row.get("vol_usd")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap(),
                row.get("buy_usd")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap(),
                row.get("first_px")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap(),
                row.get("last_px")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap(),
                row.get("rv2").and_then(serde_json::Value::as_f64).unwrap(),
                row.int("n_minutes").unwrap(),
            )
        })
        .collect()
}

/// The frozen stage-1 formulas on hand-computed rows: 3 trades, vol 4500,
/// buy 2500 (ask makers), prices 100/200, rv from two 1-minute last prices.
#[test]
fn phoenix_formulas_on_hand_computed_rows() {
    let tmp = Tmp::new("phoenix-formulas");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    let hour0 = grid::hour_of_secs(day_start());
    let conn = memory_connection("1", "512MB").unwrap();
    let rows = read_phoenix_hour(&conn, tmp.dir(), hour0);
    assert_eq!(rows.len(), 2, "SOL and ZZZ: {rows:?}");
    let sol = rows.iter().find(|r| r.0 == "SOL").unwrap();
    assert_eq!(sol.1.as_deref(), Some("SOL"), "asset_id from the mapping");
    assert!(sol.2, "mapped flag from the mapping");
    assert_eq!(sol.3, 3);
    assert_eq!(sol.4, 4500.0);
    assert_eq!(sol.5, 2500.0, "buy = ask-maker fills (taker buys)");
    assert_eq!(sol.6, 100.0);
    assert_eq!(sol.7, 200.0);
    assert!(
        (sol.8 - 2.0_f64.ln().powi(2)).abs() < 1e-12,
        "rv2 {}",
        sol.8
    );
    assert_eq!(sol.9, 2, "two minutes of last prices");
    let zzz = rows.iter().find(|r| r.0 == "ZZZ").unwrap();
    assert_eq!(zzz.1, None, "ZZZ is not in the mapping");
    assert!(!zzz.2);
    assert_eq!(zzz.4, 50.0 * 100.0);
    // The sentinel's hour is the next day: outside the requested range.
    let next_morning = grid::hour_of_secs(day_start() + 86_400 + 3 * 3600);
    assert!(report.outputs.iter().all(|o| o.hour < next_morning));
    assert!(read_phoenix_hour(&conn, tmp.dir(), next_morning).is_empty());
    assert_eq!(
        report.fills_kept, 4,
        "window rows; the sentinel is next day"
    );
    assert_eq!(report.fills_dropped, 0);
    assert_eq!(report.lockbox_hours_dropped, 0);
}

/// The decoder-reader dedupe: a corrected signature loses its old child rows
/// (including events that only existed under the superseded parent), and the
/// surviving event_id revision is the higher batch.
#[test]
fn phoenix_dedupe_drops_superseded_revisions() {
    let tmp = Tmp::new("phoenix-corrections");
    let decoded = build_corrections_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(
        report.fills_kept, 2,
        "e1 (batch 2) and e2; the sentinel is outside the window"
    );
    assert_eq!(report.fills_dropped, 2, "old e1 and parent-orphaned e3");
    let conn = memory_connection("1", "512MB").unwrap();
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(day_start()));
    assert_eq!(rows.len(), 1);
    let sol = &rows[0];
    assert_eq!(sol.3, 2, "two surviving fills");
    assert_eq!(
        sol.4,
        300.0 * 100.0 + 400.0 * 100.0,
        "300 and 400, not 100 or 999"
    );
    assert_eq!(sol.5, 300.0 * 100.0, "only the ask-maker corrected fill");
    assert_eq!(
        sol.6, 300.0,
        "the corrected e1 (tx 1) executes before e2 (tx 3) in the slot"
    );
    assert_eq!(sol.7, 400.0, "e2 is the last event in the slot's tx order");
    assert_eq!(sol.9, 2, "minute 0 and minute 2");
    assert!(
        (sol.8 - (300.0_f64 / 400.0).ln().powi(2)).abs() < 1e-12,
        "rv2 over the two minutes: {}",
        sol.8
    );
}

/// A requested day the watermark does not close (+2h guard) is refused.
#[test]
fn phoenix_refuses_an_unclosed_day() {
    let tmp = Tmp::new("phoenix-unclosed");
    let decoded = build_unclosed_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// A rerun over the same inputs skips the sealed hours and reports the same
/// bytes.
#[test]
fn phoenix_rerun_skips_sealed_hours() {
    let tmp = Tmp::new("phoenix-rerun");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let first = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(first.hours_written, 1, "only hour 0 has rows");
    assert_eq!(first.hours_empty, 23);
    let hashes: Vec<String> = first
        .outputs
        .iter()
        .map(|output| output.sha256.clone())
        .collect();

    let second = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(second.hours_written, 0, "nothing left to write");
    assert_eq!(second.hours_skipped, first.hours_written);
    let again: Vec<String> = second
        .outputs
        .iter()
        .map(|output| output.sha256.clone())
        .collect();
    assert_eq!(hashes, again, "same inputs, same bytes");
}

/// A lockbox week inside the requested range, with data inside it: the 10-03
/// fills are read (they are in the window) but their hours are never written;
/// 2026-10-07 is built. The unit receipt pins the shared receipt path, the
/// exclusion-set size (864) and the dropped count (120).
#[test]
fn the_lockbox_week_is_dropped_from_the_range() {
    let tmp = Tmp::new("phoenix-lockbox");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded-lockbox");
    // Fills inside the lockbox (2026-10-03, busy-week guard), fills on
    // 2026-10-07 hour 0, plus a sentinel on 10-08 03:00.
    let oct3 = grid::day_start_secs(grid::parse_date("2026-10-03").unwrap());
    let start = grid::day_start_secs(grid::parse_date("2026-10-07").unwrap());
    let fills = vec![
        Fill {
            event_id: "e-oct3",
            signature: "sigK",
            source_hash: "hK",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: oct3 + 60,
            symbol: "SOL",
            ticks: 777,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
        Fill {
            event_id: "e-oct7",
            signature: "sigL",
            source_hash: "hL",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: start + 60,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
        Fill {
            event_id: "e-oct8",
            signature: "sigL2",
            source_hash: "hL2",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: start + 86_400 + 3 * 3600,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigK", "hK", 432_100_006),
                tx_row("sigL", "hL", 432_100_004),
                tx_row("sigL2", "hL2", 432_100_005),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        "2026-10-01",
        "2026-10-07",
    ))
    .unwrap();
    assert_eq!(
        report.lockbox_hours_dropped,
        5 * 24,
        "guard covers 10-01..10-05"
    );
    for day in 1..=6 {
        let date = format!("2026-10-0{day}");
        assert!(
            !tmp.dir()
                .join(super::phoenix_hours::TABLE)
                .join(format!("date={date}"))
                .exists(),
            "no files in lockbox day {date}"
        );
    }
    assert!(
        tmp.dir()
            .join(super::phoenix_hours::TABLE)
            .join("date=2026-10-07")
            .join("hour=20261007-00.parquet")
            .is_file()
    );
    assert_eq!(report.hours_written, 1);
    // The 10-03 fill was read with the rest of the window; its hour is just
    // never written.
    assert_eq!(
        report.fills_kept, 2,
        "the 10-03 and 10-07 fills; the sentinel is outside the window"
    );
    let conn = memory_connection("1", "512MB").unwrap();
    let hour0 = grid::hour_of_secs(oct3);
    assert!(read_phoenix_hour(&conn, tmp.dir(), hour0).is_empty());
    // The shared unit receipt: its path sits in the unit's own directory and
    // it pins the lockbox numbers (864 is the set, 120 the dropped hours).
    let receipt = unit_receipt(
        tmp.dir(),
        super::phoenix_hours::TABLE,
        super::phoenix_hours::UNIT,
    );
    assert_eq!(
        receipt["lockbox_hours_excluded"], 864,
        "the exclusion set's size, never the dropped count"
    );
    assert_eq!(receipt["lockbox_hours_dropped"], 120);
}

/// A Jupiter event lane fixture: fields JSON with 1e6-scaled USD and prices,
/// one signature present in both jupiter lanes (live wins), plus flash events
/// across two flash lanes (live wins): an increase, a second increase, and a
/// liquidation whose market equals the jupiter custody (a distinct venue must
/// never inherit another venue's mapping row).
fn build_perp_lanes(dir: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let conn = memory_connection("1", "512MB").unwrap();
    let h0 = day_start();
    let event = |signature: &str, ordinal: i64, block_time: i64, name: &str, fields: &str| {
        format!(
            "({}, {}, {block_time}, {ordinal}, {}, {})",
            sql_string(signature),
            432_100_010i64,
            sql_string(name),
            sql_string(fields),
        )
    };
    let jup_hist = dir.join("jup-hist");
    std::fs::create_dir_all(&jup_hist).unwrap();
    let hist_rows = [
        event(
            "j1",
            0,
            h0 + 60,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "2000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        ),
        event(
            "j2",
            0,
            h0 + 100,
            "InstantDecreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 2, "price": "100000000", "positionCustody": "Cust1"}"#,
        ),
        event(
            "j3",
            0,
            h0 + 200,
            "LiquidateFullPositionEvent",
            r#"{"positionSizeUsd": "5000000", "sizeUsdDelta": "9000000", "fundingFeeUsd": "100000", "price": "100000000", "positionCustody": "Cust1"}"#,
        ),
        event(
            "j4",
            0,
            day_start() + 86_400 + 3 * 3600,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        ),
    ];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        hist_rows.join(", "),
        sql_string(&jup_hist.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    let jup_live = dir.join("jup-live");
    std::fs::create_dir_all(&jup_live).unwrap();
    let live_rows = [
        event(
            "j1",
            0,
            h0 + 600,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "3000000", "positionSide": 1, "price": "150000000", "positionCustody": "Cust1"}"#,
        ),
        // A live-lane sentinel outside the window closes the day (the
        // watermark is the most-stalled lane's max).
        event(
            "j5",
            0,
            day_start() + 86_400 + 3 * 3600,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        ),
    ];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        live_rows.join(", "),
        sql_string(&jup_live.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    let flash_hist = dir.join("flash-hist");
    std::fs::create_dir_all(&flash_hist).unwrap();
    let flash_rows = [
        // (f1, 0) is duplicated in the live lane, which must win.
        event(
            "f1",
            0,
            h0 + 500,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "250000000", "entry_price": "2000000000", "entry_price_exponent": -8}"#,
        ),
        event(
            "f2",
            0,
            h0 + 700,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "15000000", "entry_price_exponent": -6}"#,
        ),
        // The flash market equals the jupiter custody: the venue-qualified
        // join must leave this row unmapped.
        event(
            "f3",
            0,
            h0 + 900,
            "LiquidateLogUSDv1",
            r#"{"market": "Cust1", "delta_size_usd": "50000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
        ),
        // A sentinel outside the window closes the day.
        event(
            "f4",
            0,
            day_start() + 86_400 + 3 * 3600,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
        ),
    ];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        flash_rows.join(", "),
        sql_string(&flash_hist.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    let flash_live = dir.join("flash-live");
    std::fs::create_dir_all(&flash_live).unwrap();
    let flash_live_rows = [
        event(
            "f1",
            0,
            h0 + 800,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "350000000", "entry_price": "1750000000", "entry_price_exponent": -8}"#,
        ),
        // A sentinel outside the window closes the day.
        event(
            "f5",
            0,
            day_start() + 86_400 + 3 * 3600,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
        ),
    ];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        flash_live_rows.join(", "),
        sql_string(&flash_live.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    (jup_hist, jup_live, flash_hist, flash_live)
}

/// The perp unit: lane dedupe (live wins, jupiter and flash), the 1e6 scaling,
/// buy_share over long increases, funding and liquidation dollars, the flash
/// price exponent, the venue-qualified mapping join, and flash rows with a
/// NULL asset when the mapping has no market row.
#[test]
fn solana_perp_scales_dedupes_and_maps() {
    let tmp = Tmp::new("perp");
    let (jup_hist, jup_live, flash_hist, flash_live) = build_perp_lanes(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let jupiter_events = vec![jup_hist.clone(), jup_live.clone()];
    let flash_events = vec![flash_hist.clone(), flash_live.clone()];
    let request = PerpRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        jupiter_events: &jupiter_events,
        flash_events: &flash_events,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let report = super::solana_perp_hours::run(&request).unwrap();
    let h0 = grid::hour_of_secs(day_start());
    let path = tmp
        .dir()
        .join(io::hour_partition(h0, super::solana_perp_hours::TABLE));
    let conn = memory_connection("1", "512MB").unwrap();
    let rows = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT venue, venue_key, asset_id, mapped, n_inc, n_dec, n_liq, size_usd,
                    buy_share, funding_fee_usd, liq_usd, ret
             FROM read_parquet('{}') ORDER BY venue",
            path.to_string_lossy()
        ),
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 3, "{rows:?}");
    let jup = rows
        .iter()
        .find(|r| r.str("venue") == Some("jupiter"))
        .unwrap();
    assert_eq!(jup.str("venue_key"), Some("Cust1"));
    assert_eq!(jup.str("asset_id"), Some("SOL"));
    assert_eq!(
        jup.int("n_inc"),
        Some(1),
        "the live lane's j1 wins the dedupe"
    );
    assert_eq!(jup.int("n_dec"), Some(1));
    assert_eq!(jup.int("n_liq"), Some(1));
    let size = jup
        .get("size_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!(
        (size - 4.0).abs() < 1e-9,
        "3 (live) + 1 decrease, liquidation excluded: {size}"
    );
    let buy = jup
        .get("buy_share")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((buy - 1.0).abs() < 1e-9, "the only increase is long: {buy}");
    let funding = jup
        .get("funding_fee_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((funding - 0.1).abs() < 1e-12);
    let liq = jup
        .get("liq_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((liq - 5.0).abs() < 1e-9);
    let ret = jup.get("ret").and_then(serde_json::Value::as_f64).unwrap();
    assert!(
        (ret - (100.0_f64 / 150.0).ln()).abs() < 1e-12,
        "event order (slot, signature, ordinal): first j1 150, last j3 100: {ret}"
    );
    let flash_row = rows
        .iter()
        .find(|r| r.str("venue") == Some("flash") && r.str("venue_key") == Some("mkt1"))
        .unwrap();
    assert_eq!(flash_row.str("venue_key"), Some("mkt1"));
    assert_eq!(
        flash_row.str("asset_id"),
        None,
        "flash markets are unmapped"
    );
    assert_eq!(
        flash_row.get("mapped"),
        Some(&serde_json::Value::Bool(false)),
        "unmapped is false, never NULL"
    );
    let flash_size = flash_row
        .get("size_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!(
        (flash_size - 450.0).abs() < 1e-9,
        "live f1 350 + f2 100, the liquidation is excluded: {flash_size}"
    );
    assert_eq!(flash_row.int("n_liq"), Some(0));
    let flash_ret = flash_row
        .get("ret")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!(
        (flash_ret - (15.0_f64 / 17.5).ln()).abs() < 1e-12,
        "prices 17.5 (live f1, exp -8) to 15 (f2, exp -6, 150000000 scaled): {flash_ret}"
    );
    assert_eq!(
        flash_row.get("buy_share"),
        Some(&serde_json::Value::Null),
        "flash events carry no side"
    );
    let flash_liq_row = rows
        .iter()
        .find(|r| r.str("venue") == Some("flash") && r.str("venue_key") == Some("Cust1"))
        .unwrap();
    assert_eq!(
        flash_liq_row.str("asset_id"),
        None,
        "the jupiter custody row must not leak into the flash venue"
    );
    assert_eq!(
        flash_liq_row.get("mapped"),
        Some(&serde_json::Value::Bool(false))
    );
    let flash_liq = flash_liq_row
        .get("liq_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((flash_liq - 50.0).abs() < 1e-9);
    assert_eq!(
        report.jupiter_events, 3,
        "j1 (deduped), j2, j3; j4 is outside the window"
    );
    assert_eq!(report.flash_events, 3, "f1 (live lane wins), f2, f3");
    assert_eq!(report.pairs, 3);
    assert_eq!(report.unmapped, 2, "mkt1 and the Cust1 flash market");
}

/// A Pyth price lane fixture: duplicate (feed_id, publish_time) rows collapse
/// (later at_ms wins), staleness is measured from the hour end, and the
/// mapping supplies the asset.
#[test]
fn pyth_dedupes_and_measures_staleness() {
    let tmp = Tmp::new("pyth");
    let conn = memory_connection("1", "512MB").unwrap();
    let lane = tmp.dir().join("pyth-hist");
    std::fs::create_dir_all(&lane).unwrap();
    let t0 = day_start();
    let rows = [
        format!(
            "('feed-sol', 'SOL', {}, 100.0, 0.5, {})",
            t0 + 60,
            t0 * 1000 + 60_000
        ),
        format!(
            "('feed-sol', 'SOL', {}, 110.0, 1.11, {})",
            t0 + 1800,
            t0 * 1000 + 1_800_001
        ),
        format!(
            "('feed-sol', 'SOL', {}, 111.0, 1.11, {})",
            t0 + 1800,
            t0 * 1000 + 1_800_002
        ),
        format!(
            "('feed-sol', 'SOL', {}, 111.0, 1.11, {})",
            day_start() + 86_400 + 3 * 3600,
            (day_start() + 86_400 + 3 * 3600) * 1000
        ),
    ];
    conn.execute_batch(&format!(
        "COPY (SELECT feed_id, symbol, publish_time, price_f::DOUBLE AS price_f,
                      conf_f::DOUBLE AS conf_f, at_ms
               FROM (VALUES {}) t(feed_id, symbol, publish_time, price_f, conf_f, at_ms))
         TO {} (FORMAT PARQUET)",
        rows.join(", "),
        sql_string(&lane.join("prices.parquet").to_string_lossy())
    ))
    .unwrap();
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let pyth_lanes = vec![lane.clone()];
    let request = PythRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let report = super::pyth_hours::run(&request).unwrap();
    let h0 = grid::hour_of_secs(day_start());
    let path = tmp
        .dir()
        .join(io::hour_partition(h0, super::pyth_hours::TABLE));
    let rows = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT feed_id, symbol, asset_id, py_n, py_last, py_ret, py_conf_rel, py_stale_s
             FROM read_parquet('{}')",
            path.to_string_lossy()
        ),
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row.str("feed_id"), Some("feed-sol"));
    assert_eq!(row.str("symbol"), Some("SOL"));
    assert_eq!(row.str("asset_id"), Some("SOL"));
    assert_eq!(
        row.int("py_n"),
        Some(2),
        "the duplicate publish_time collapsed"
    );
    assert_eq!(
        row.get("py_last").and_then(serde_json::Value::as_f64),
        Some(111.0),
        "the later at_ms wins"
    );
    let ret = row
        .get("py_ret")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((ret - (111.0_f64 / 100.0).ln()).abs() < 1e-12);
    let conf = row
        .get("py_conf_rel")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((conf - (0.5 / 100.0 + 1.11 / 111.0) / 2.0).abs() < 1e-12);
    assert_eq!(
        row.int("py_stale_s"),
        Some(1800),
        "hour end minus last publish"
    );
    assert_eq!(report.feeds, 1);
}

/// One jupiter/flash event row of a lane fixture, as a SQL VALUES tuple.
fn perp_event(
    signature: &str,
    slot: i64,
    block_time: i64,
    ordinal: i64,
    name: &str,
    fields: &str,
) -> String {
    format!(
        "({}, {}, {block_time}, {ordinal}, {}, {})",
        sql_string(signature),
        slot,
        sql_string(name),
        sql_string(fields),
    )
}

/// Write one event lane directory with a single Parquet file.
fn write_event_lane(dir: &Path, rows: &[String]) {
    std::fs::create_dir_all(dir).unwrap();
    let conn = memory_connection("1", "512MB").unwrap();
    let select = if rows.is_empty() {
        "SELECT NULL::VARCHAR AS signature, NULL::BIGINT AS slot, NULL::BIGINT AS block_time,
                NULL::BIGINT AS ordinal, NULL::VARCHAR AS name, NULL::VARCHAR AS fields
         WHERE false"
            .to_owned()
    } else {
        format!(
            "SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields)",
            rows.join(", ")
        )
    };
    conn.execute_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET)",
        sql_string(&dir.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
}

/// One pyth price row of a lane fixture, as a SQL VALUES tuple.
#[allow(clippy::too_many_arguments)]
fn pyth_price(
    feed: &str,
    symbol: &str,
    publish_time: i64,
    price: f64,
    conf: f64,
    at_ms: i64,
) -> String {
    format!(
        "({}, {}, {publish_time}, {price}, {conf}, {at_ms})",
        sql_string(feed),
        sql_string(symbol),
    )
}

/// Write one pyth price lane directory with a single Parquet file.
fn write_pyth_lane(dir: &Path, rows: &[String]) {
    std::fs::create_dir_all(dir).unwrap();
    let conn = memory_connection("1", "512MB").unwrap();
    let select = if rows.is_empty() {
        "SELECT NULL::VARCHAR AS feed_id, NULL::VARCHAR AS symbol, NULL::BIGINT AS publish_time,
                NULL::DOUBLE AS price_f, NULL::DOUBLE AS conf_f, NULL::BIGINT AS at_ms
         WHERE false"
            .to_owned()
    } else {
        format!(
            "SELECT feed_id, symbol, publish_time, price_f::DOUBLE AS price_f,
                    conf_f::DOUBLE AS conf_f, at_ms
             FROM (VALUES {}) t(feed_id, symbol, publish_time, price_f, conf_f, at_ms)",
            rows.join(", ")
        )
    };
    conn.execute_batch(&format!(
        "COPY ({select}) TO {} (FORMAT PARQUET)",
        sql_string(&dir.join("prices.parquet").to_string_lossy())
    ))
    .unwrap();
}

/// The first/last prices follow the (slot, tx_index, ordinal) tuple, never a
/// packed float of the second: three fills in the SAME second, two slots, and
/// a tx order inside the first slot that contradicts the ordinal order. The
/// correct first is the tx-2 fill (price 200), the correct last the
/// later-slot fill (price 300); a packed float prices first 300 (the original
/// bug) and dropping tx_index prices first 100 (M14).
#[test]
fn phoenix_first_and_last_px_follow_the_slot_order() {
    let tmp = Tmp::new("phoenix-slot-order");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let t = day_start() + 60;
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                slot: i64,
                tx_index: i64,
                ordinal: i64,
                ticks: i64,
                maker: &'static str| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot,
            tx_index,
            ordinal,
            block_time: t,
            symbol: "SOL",
            ticks,
            base_lots: 1000,
            maker_side: maker,
        }
        .values(1, 0, 0)
    };
    let fills = vec![
        fill("e-a", "sigA", "hA", 432_100_000, 5, 0, 100, "ask"),
        fill("e-b", "sigB", "hB", 432_100_000, 2, 1, 200, "bid"),
        fill("e-c", "sigC", "hC", 432_200_000, 1, 0, 300, "ask"),
        Fill {
            event_id: "e-s",
            signature: "sigS",
            source_hash: "hS",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: day_start() + 86_400 + 3 * 3600,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigA", "hA", 432_100_000),
                tx_row("sigB", "hB", 432_100_001),
                tx_row("sigC", "hC", 432_200_000),
                tx_row("sigS", "hS", 432_100_002),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.hours_written, 1);
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(day_start()));
    assert_eq!(rows.len(), 1, "{rows:?}");
    let sol = &rows[0];
    assert_eq!(sol.3, 3);
    assert_eq!(sol.4, 600_000.0, "all three fills, qty 1000 each");
    assert_eq!(sol.5, 400_000.0, "the ask makers are e-a and e-c");
    assert_eq!(sol.6, 200.0, "tx 2 before tx 5 inside the slot");
    assert_eq!(sol.7, 300.0, "the later slot is last");
}

/// The watermark boundary is exact: an input one second short of
/// `hour end + HOUR_GUARD_SECS` refuses the day, an input exactly there
/// closes it. A zeroed guard (M10) accepts the short input.
#[test]
fn phoenix_boundary_watermark_closes_the_last_requested_hour() {
    let build = |label: &str, sentinel: i64| -> PathBuf {
        let tmp_path = std::env::temp_dir().join(format!(
            "combined-hours-boundary-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp_path).unwrap();
        let conn = memory_connection("1", "512MB").unwrap();
        let fills = vec![
            Fill {
                event_id: "e-h0",
                signature: "sigH",
                source_hash: "hH",
                slot: 432_100_000,
                tx_index: 1,
                ordinal: 0,
                block_time: day_start() + 60,
                symbol: "SOL",
                ticks: 100,
                base_lots: 100,
                maker_side: "ask",
            }
            .values(1, 0, 0),
            Fill {
                event_id: "e-boundary",
                signature: "sigT",
                source_hash: "hT",
                slot: 432_100_000,
                tx_index: 1,
                ordinal: 0,
                block_time: sentinel,
                symbol: "SOL",
                ticks: 100,
                base_lots: 100,
                maker_side: "ask",
            }
            .values(1, 0, 0),
        ];
        let root = tmp_path.join("decoded");
        write_decoded_root(
            &conn,
            &root,
            &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
            &[(
                "tables/decoded_transactions/epoch=1000/tx.parquet",
                1,
                vec![
                    tx_row("sigH", "hH", 432_100_000),
                    tx_row("sigT", "hT", 432_100_001),
                ],
            )],
            &write_fills_parquet,
            &write_tx_parquet,
            "2026-10-30T00:00:00Z",
        );
        root
    };
    let tmp = Tmp::new("phoenix-boundary");
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    // One second short of the boundary: hour 23 is not closed yet.
    let short = build("short", day_start() + 86_400 + 2 * 3600 - 1);
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &short,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
    // Exactly the boundary (next-day 02:00 = hour end + 2h guard): closed.
    let exact = build("exact", day_start() + 86_400 + 2 * 3600);
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &exact,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.hours_written, 1);
}

/// The aggregates refuse non-positive prices, non-positive sizes and NULL
/// symbols, and the refusals are counted as `fills_filtered` (M16/M17).
#[test]
fn phoenix_drops_and_counts_filtered_rows() {
    let tmp = Tmp::new("phoenix-filtered");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let h0 = day_start();
    let mut fills = vec![
        Fill {
            event_id: "e-ok",
            signature: "sigA",
            source_hash: "hA",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: h0 + 60,
            symbol: "SOL",
            ticks: 100,
            base_lots: 1000,
            maker_side: "ask",
        }
        .values(1, 0, 0),
        // price_ticks = -5: a non-positive price.
        Fill {
            event_id: "e-neg",
            signature: "sigB",
            source_hash: "hB",
            slot: 432_100_000,
            tx_index: 2,
            ordinal: 0,
            block_time: h0 + 120,
            symbol: "SOL",
            ticks: -5,
            base_lots: 1000,
            maker_side: "bid",
        }
        .values(1, 0, 0),
        // base_lots = 0: a non-positive size.
        Fill {
            event_id: "e-zlots",
            signature: "sigC",
            source_hash: "hC",
            slot: 432_100_000,
            tx_index: 3,
            ordinal: 0,
            block_time: h0 + 130,
            symbol: "SOL",
            ticks: 200,
            base_lots: 0,
            maker_side: "ask",
        }
        .values(1, 0, 0),
        // NULL symbol: written as a raw tuple (Fill carries &str).
        format!(
            "('e-null', 'sigD', 'hD', 432_100_000, 4, 0, {}, NULL, 1, 0, 0, 300, 1000, 'ask')",
            h0 + 140
        ),
        Fill {
            event_id: "e-s",
            signature: "sigS",
            source_hash: "hS",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: day_start() + 86_400 + 3 * 3600,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
    ];
    fills.retain(|_| true);
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigA", "hA", 432_100_000),
                tx_row("sigB", "hB", 432_100_001),
                tx_row("sigC", "hC", 432_100_002),
                tx_row("sigD", "hD", 432_100_003),
                tx_row("sigS", "hS", 432_100_004),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.fills_kept, 4, "every window row survives the dedupe");
    assert_eq!(report.fills_dropped, 0);
    assert_eq!(
        report.fills_filtered, 3,
        "the negative price, the zero size and the NULL symbol"
    );
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(day_start()));
    assert_eq!(rows.len(), 1);
    let sol = &rows[0];
    assert_eq!(sol.3, 1, "only the good fill is aggregated");
    assert_eq!(sol.4, 100_000.0);
    assert_eq!(sol.6, 100.0);
    assert_eq!(sol.7, 100.0);
}

/// Realised variance never chains across the hour boundary: the first minute
/// of hour 1 has no in-hour lag, so its rv2 is 0 (M18 partitions without the
/// hour and leaks hour 0's last price in).
#[test]
fn phoenix_rv_does_not_leak_across_hours() {
    let tmp = Tmp::new("phoenix-rv");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let h0 = day_start();
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                t: i64,
                ticks: i64| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: t,
            symbol: "SOL",
            ticks,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0)
    };
    let fills = vec![
        fill("e-a", "sigA", "hA", h0 + 60, 100),
        fill("e-b", "sigB", "hB", h0 + 300, 200),
        fill("e-c", "sigC", "hC", h0 + 3_660, 150),
        fill("e-s", "sigS", "hS", h0 + 86_400 + 3 * 3600, 100),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigA", "hA", 432_100_000),
                tx_row("sigB", "hB", 432_100_001),
                tx_row("sigC", "hC", 432_100_002),
                tx_row("sigS", "hS", 432_100_003),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.hours_written, 2);
    let hour0 = grid::hour_of_secs(h0);
    let rows = read_phoenix_hour(&conn, tmp.dir(), hour0);
    assert!(
        (rows[0].8 - 2.0_f64.ln().powi(2)).abs() < 1e-9,
        "{}",
        rows[0].8
    );
    assert_eq!(rows[0].9, 2);
    let hour1 = read_phoenix_hour(&conn, tmp.dir(), hour0 + 1);
    assert_eq!(hour1[0].7, 150.0, "hour 1 has only the 150 fill");
    assert_eq!(
        hour1[0].8, 0.0,
        "one minute, no lag, no leak from hour 0's 200"
    );
    assert_eq!(hour1[0].9, 1);
}

/// The price formula uses the fill's own asymmetric decimals:
/// px = ticks * tick_size * 10^(base - quote), qty = lots / 10^base (M13).
#[test]
fn phoenix_price_uses_asymmetric_decimals() {
    let tmp = Tmp::new("phoenix-decimals");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let fills = vec![
        Fill {
            event_id: "e-a",
            signature: "sigA",
            source_hash: "hA",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: day_start() + 60,
            symbol: "SOL",
            ticks: 25,
            base_lots: 2500,
            maker_side: "ask",
        }
        .values(10, 3, 1),
        Fill {
            event_id: "e-s",
            signature: "sigS",
            source_hash: "hS",
            slot: 432_100_000,
            tx_index: 1,
            ordinal: 0,
            block_time: day_start() + 86_400 + 3 * 3600,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[("tables/fills/epoch=1000/fills.parquet", 1, fills)],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigA", "hA", 432_100_000),
                tx_row("sigS", "hS", 432_100_001),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(day_start()));
    assert_eq!(rows.len(), 1);
    let sol = &rows[0];
    assert_eq!(sol.3, 1);
    assert_eq!(sol.4, 62_500.0, "px 25*10*10^2 = 25000, qty 2.5");
    assert_eq!(sol.6, 25_000.0, "10^(3-1), not 10^(1-3)");
    assert_eq!(sol.7, 25_000.0);
}

/// The event_id revision pick is by registration batch, newest batch wins,
/// even when the parent (signature, source_hash) is unchanged between
/// revisions (M25 orders the batches ascending).
#[test]
fn phoenix_event_dedupe_keeps_the_latest_batch_revision() {
    let tmp = Tmp::new("phoenix-batches");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let h0 = day_start();
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                tx_index: i64,
                t: i64,
                ticks: i64| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot: 432_100_000,
            tx_index,
            ordinal: 0,
            block_time: t,
            symbol: "SOL",
            ticks,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0)
    };
    // Same event_id, same signature, same source hash; only the ticks and the
    // time differ. Batch 2 is the correction.
    let b1 = vec![fill("e1", "s1", "h1", 1, h0 + 60, 100)];
    let b2 = vec![
        fill("e1", "s1", "h1", 1, h0 + 120, 300),
        fill("e-s", "s3", "h3", 2, h0 + 86_400 + 3 * 3600, 100),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[
            ("tables/fills/epoch=1000/b1.parquet", 1, b1),
            ("tables/fills/epoch=1000/b2.parquet", 2, b2),
        ],
        &[
            (
                "tables/decoded_transactions/epoch=1000/t1.parquet",
                1,
                vec![
                    tx_row("s1", "h1", 432_100_000),
                    tx_row("s3", "h3", 432_100_002),
                ],
            ),
            (
                "tables/decoded_transactions/epoch=1000/t2.parquet",
                2,
                vec![
                    tx_row("s1", "h1", 432_100_000),
                    tx_row("s3", "h3", 432_100_003),
                ],
            ),
        ],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.fills_kept, 1, "one deduplicated event");
    assert_eq!(report.fills_dropped, 1, "the batch-1 revision");
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(h0));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].3, 1);
    assert_eq!(
        rows[0].4, 30_000.0,
        "ticks 300 (batch 2), not 100 (batch 1)"
    );
    assert_eq!(rows[0].7, 300.0);
}

/// Files the catalog registered after the run's snapshot never contribute
/// rows or watermark (M27 keeps only the too-young files).
#[test]
fn phoenix_ignores_files_created_after_the_snapshot() {
    let tmp = Tmp::new("phoenix-snapshot");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let h0 = day_start();
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                tx_index: i64,
                t: i64,
                symbol: &'static str,
                ticks: i64| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot: 432_100_000,
            tx_index,
            ordinal: 0,
            block_time: t,
            symbol,
            ticks,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0)
    };
    let f1 = vec![
        fill("e-keep", "sigK", "hK", 1, h0 + 60, "SOL", 100),
        fill(
            "e-ks",
            "sigKS",
            "hKS",
            2,
            h0 + 86_400 + 3 * 3600,
            "SOL",
            100,
        ),
    ];
    let f2 = vec![
        fill("e-late", "sigL", "hL", 3, h0 + 90, "SOL", 500),
        fill("e-qqq", "sigQ", "hQ", 4, h0 + 95, "QQQ", 42),
        fill(
            "e-ls",
            "sigLS",
            "hLS",
            5,
            h0 + 86_400 + 3 * 3600,
            "SOL",
            100,
        ),
    ];
    write_decoded_root(
        &conn,
        &root,
        &[
            ("tables/fills/epoch=1000/f1.parquet", 1, f1),
            ("tables/fills/epoch=1000/f2.parquet", 2, f2),
        ],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigK", "hK", 432_100_000),
                tx_row("sigKS", "hKS", 432_100_001),
                tx_row("sigL", "hL", 432_100_002),
                tx_row("sigQ", "hQ", 432_100_003),
                tx_row("sigLS", "hLS", 432_100_004),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    // f2 registered itself two hours after the snapshot stamp.
    let catalog_path = root.join("catalog.json");
    let mut catalog: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&catalog_path).unwrap()).unwrap();
    for file in catalog["files"].as_array_mut().unwrap().iter_mut() {
        if file["path"].as_str() == Some("tables/fills/epoch=1000/f2.parquet") {
            file["created_at"] = serde_json::json!("2026-11-01T02:00:00Z");
        }
    }
    std::fs::write(&catalog_path, catalog.to_string()).unwrap();
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    let rows = read_phoenix_hour(&conn, tmp.dir(), grid::hour_of_secs(h0));
    assert_eq!(rows.len(), 1, "only SOL: the QQQ row is in the late file");
    assert_eq!(rows[0].4, 10_000.0, "the kept file's 100 fill, not the 500");
    let receipt = unit_receipt(
        tmp.dir(),
        super::phoenix_hours::TABLE,
        super::phoenix_hours::UNIT,
    );
    assert_eq!(
        receipt["inputs"]["watermark"]["fills_files"], 1,
        "only the file created before the snapshot is selected"
    );
    assert_eq!(report.hours_written, 1);
}

/// The per-file overlap selection reads only files whose block_time range
/// intersects the requested window: a far-future file never inflates the
/// watermark (M29 selects everything, so the day looks closed).
#[test]
fn phoenix_watermark_ignores_files_outside_the_window() {
    let tmp = Tmp::new("phoenix-overlap");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded");
    let h0 = day_start();
    let fill = |event_id: &'static str,
                signature: &'static str,
                hash: &'static str,
                tx_index: i64,
                t: i64| {
        Fill {
            event_id,
            signature,
            source_hash: hash,
            slot: 432_100_000,
            tx_index,
            ordinal: 0,
            block_time: t,
            symbol: "SOL",
            ticks: 100,
            base_lots: 100,
            maker_side: "ask",
        }
        .values(1, 0, 0)
    };
    let f1 = vec![fill("e-in", "sigW", "hW", 1, h0 + 60)];
    let f2 = vec![fill("e-far", "sigF", "hF", 1, h0 + 30 * 86_400)];
    write_decoded_root(
        &conn,
        &root,
        &[
            ("tables/fills/epoch=1000/f1.parquet", 1, f1),
            ("tables/fills/epoch=1000/f2.parquet", 2, f2),
        ],
        &[(
            "tables/decoded_transactions/epoch=1000/tx.parquet",
            1,
            vec![
                tx_row("sigW", "hW", 432_100_000),
                tx_row("sigF", "hF", 432_100_001),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
        "2026-10-30T00:00:00Z",
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &root,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// A v2 lockbox whose exclusion set is not 864 hours over 36 dates is refused
/// before any input is read, by every unit (M21 drops the assert).
#[test]
fn a_wrong_shape_lockbox_is_refused_before_any_read() {
    let tmp = Tmp::new("phoenix-shape");
    let (lockbox, lockbox_sha) = wrong_shape_lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let decoded = tmp.dir().join("no-such-decoded");
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap_err();
    assert!(error.to_string().contains("864"), "{error}");
    let jupiter_events: Vec<PathBuf> = Vec::new();
    let flash_events: Vec<PathBuf> = Vec::new();
    let perp = super::solana_perp_hours::Request {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        jupiter_events: &jupiter_events,
        flash_events: &flash_events,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::solana_perp_hours::run(&perp).unwrap_err();
    assert!(error.to_string().contains("864"), "{error}");
    let pyth_lanes: Vec<PathBuf> = Vec::new();
    let pyth = super::pyth_hours::Request {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::pyth_hours::run(&pyth).unwrap_err();
    assert!(error.to_string().contains("864"), "{error}");
}

/// An injected fsync failure at the hour Parquet's temp file fails the run,
/// leaves no hour file, no hour receipt and no unit receipt; the fault hit the
/// Parquet temp fsync, not a later one (M05 removes that fsync).
#[test]
fn a_failed_hour_parquet_write_leaves_no_hour_receipt() {
    let tmp = Tmp::new("fsync-hour-parquet");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    crate::fsutil::inject_fsync_eio(crate::fsutil::FsyncStep::TempFile);
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ));
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the injected fsync failure must fail the run"
    );
    assert_eq!(
        crate::fsutil::last_fsync_step(),
        Some(crate::fsutil::FsyncStep::TempFile),
        "the fault hit a temp-file fsync"
    );
    let synced = crate::fsutil::last_fsync_path().unwrap_or_default();
    assert!(
        synced.to_string_lossy().ends_with(".parquet.tmp"),
        "the hour Parquet's temp file was the faulted write: {synced:?}"
    );
    assert!(
        !tmp.dir()
            .join(io::hour_partition(
                grid::hour_of_secs(day_start()),
                super::phoenix_hours::TABLE
            ))
            .exists(),
        "no sealed hour file"
    );
    assert!(
        !super::receipt::receipt_path(
            tmp.dir(),
            super::phoenix_hours::TABLE,
            super::phoenix_hours::UNIT
        )
        .exists()
    );
    assert!(!tmp.dir().join("catalog.json").exists());
    // A rerun without the fault writes the hour.
    let report = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ))
    .unwrap();
    assert_eq!(report.hours_written, 1);
}

/// The hour Parquet write fsyncs its directory after the rename: an injected
/// failure at exactly that step fails the run with the partition directory as
/// the synced path (M07 removes the call and the fault drifts elsewhere).
#[test]
fn the_hour_parquet_write_syncs_the_directory_after_the_rename() {
    let tmp = Tmp::new("fsync-hour-dir");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    crate::fsutil::inject_fsync_eio(crate::fsutil::FsyncStep::DirAfterRename);
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ));
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the injected fsync failure must fail the run"
    );
    let synced = crate::fsutil::last_fsync_path().unwrap_or_default();
    assert!(
        synced.to_string_lossy().ends_with(&format!("date={DAY}")),
        "the fault hit the hour partition's directory fsync: {synced:?}"
    );
    assert!(
        !super::receipt::receipt_path(
            tmp.dir(),
            super::phoenix_hours::TABLE,
            super::phoenix_hours::UNIT
        )
        .exists(),
        "no unit receipt after a failed hour write"
    );
}

/// The hour receipt is written through the durable pattern: an armed temp-file
/// fault fires on the receipt's tmp fsync and no receipt lands (M06 swaps in a
/// plain write, which never fsyncs and never fires the fault).
#[test]
fn a_failed_hour_receipt_write_leaves_the_parquet_but_no_receipt() {
    let tmp = Tmp::new("fsync-hour-receipt");
    let conn = memory_connection("1", "512MB").unwrap();
    let out = tmp.dir();
    let parquet = out.join("t/date=2026-10-15/hour=20261015-00.parquet");
    io::write_hour_parquet(&conn, "SELECT 1::BIGINT AS a WHERE false", &parquet).unwrap();
    assert!(parquet.is_file(), "the hour file itself is durable");
    let receipt = io::HourReceipt {
        unit: "phoenix-hours".into(),
        unit_version: 1,
        hour: 0,
        table: "t".into(),
        rows: 0,
        bytes: 0,
        sha256: "abc".into(),
        inputs_sha256: "def".into(),
        sealed_utc: grid::now_rfc3339(),
    };
    crate::fsutil::inject_fsync_eio(crate::fsutil::FsyncStep::TempFile);
    let error = io::write_hour_receipt(out, &receipt);
    crate::fsutil::clear_fsync_failure();
    let error = error.unwrap_err();
    assert!(error.to_string().contains("fsync failed"), "{error}");
    let synced = crate::fsutil::last_fsync_path().unwrap_or_default();
    assert!(
        synced
            .to_string_lossy()
            .ends_with("receipts/19700101-00.json.tmp"),
        "the receipt's temp file was the faulted write: {synced:?}"
    );
    assert!(!io::hour_receipt_path(out, "t", 0).exists());
}

/// An injected failure at the unit receipt's durable write fails the run but
/// leaves every hour output and hour receipt in place (the seam is the
/// receipt::on_before_write hook, so a plain write never fires it).
#[test]
fn a_failed_unit_receipt_write_leaves_outputs_but_no_unit_receipt() {
    let tmp = Tmp::new("fsync-unit-receipt");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    super::receipt::on_before_write(|| {
        crate::fsutil::inject_fsync_eio(crate::fsutil::FsyncStep::TempFile);
    });
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ));
    super::receipt::clear_before_write();
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the faulted receipt write must fail the run"
    );
    assert!(
        tmp.dir()
            .join(io::hour_partition(
                grid::hour_of_secs(day_start()),
                super::phoenix_hours::TABLE
            ))
            .is_file(),
        "the hour file is durable"
    );
    assert!(
        io::read_hour_receipt(
            tmp.dir(),
            super::phoenix_hours::TABLE,
            grid::hour_of_secs(day_start())
        )
        .is_some(),
        "the hour receipt is durable"
    );
    assert!(
        !super::receipt::receipt_path(
            tmp.dir(),
            super::phoenix_hours::TABLE,
            super::phoenix_hours::UNIT
        )
        .exists()
    );
    assert!(
        !tmp.dir().join("catalog.json").exists(),
        "no catalog after a failed unit receipt"
    );
}

/// An injected failure at the catalog's durable write fails the run but leaves
/// the outputs and both receipts (M19 removes the catalog save entirely, so
/// the hook never fires and the run would succeed).
#[test]
fn a_failed_catalog_write_leaves_outputs_and_receipts() {
    let tmp = Tmp::new("fsync-catalog-hours");
    let decoded = build_main_decoded(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    super::catalog::on_before_save(|| {
        crate::fsutil::inject_fsync_eio(crate::fsutil::FsyncStep::TempFile);
    });
    let error = super::phoenix_hours::run(&phoenix_request(
        tmp.dir(),
        &decoded,
        &lockbox,
        &lockbox_sha,
        &provenance,
        DAY,
        DAY,
    ));
    super::catalog::clear_before_save();
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the faulted catalog write must fail the run"
    );
    assert!(
        super::receipt::receipt_path(
            tmp.dir(),
            super::phoenix_hours::TABLE,
            super::phoenix_hours::UNIT
        )
        .exists()
    );
    assert!(!tmp.dir().join("catalog.json").exists());
}

/// The perp unit refuses a day its watermark does not close (M08 removes the
/// refusal and seals the half-informed hour).
#[test]
fn solana_perp_refuses_an_unclosed_day() {
    let tmp = Tmp::new("perp-unclosed");
    let h0 = day_start();
    let jup = tmp.dir().join("jup");
    write_event_lane(
        &jup,
        &[perp_event(
            "j1",
            432_100_010,
            h0 + 60,
            0,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        )],
    );
    let flash = tmp.dir().join("flash");
    write_event_lane(
        &flash,
        &[perp_event(
            "f1",
            432_100_010,
            h0 + 70,
            0,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
        )],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let jupiter_events = vec![jup];
    let flash_events = vec![flash];
    let request = PerpRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        jupiter_events: &jupiter_events,
        flash_events: &flash_events,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::solana_perp_hours::run(&request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// The perp watermark is the most-stalled lane, not the most-advanced one: a
/// history lane far ahead and a live lane stuck an hour in, the day is not
/// closed (B106-2 takes the max across lanes and seals it).
#[test]
fn solana_perp_watermark_is_the_most_stalled_lane() {
    let tmp = Tmp::new("perp-stall");
    let h0 = day_start();
    let jup_hist = tmp.dir().join("jup-hist");
    write_event_lane(
        &jup_hist,
        &[perp_event(
            "jH",
            432_100_010,
            h0 + 2 * 86_400,
            0,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        )],
    );
    let jup_live = tmp.dir().join("jup-live");
    write_event_lane(
        &jup_live,
        &[perp_event(
            "jL",
            432_100_010,
            h0 + 60,
            0,
            "InstantIncreasePositionEvent",
            r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
        )],
    );
    let flash = tmp.dir().join("flash");
    write_event_lane(
        &flash,
        &[perp_event(
            "fH",
            432_100_010,
            h0 + 2 * 86_400,
            0,
            "IncreaseSizeLogUSDv1",
            r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
        )],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let jupiter_events = vec![jup_hist, jup_live];
    let flash_events = vec![flash];
    let request = PerpRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        jupiter_events: &jupiter_events,
        flash_events: &flash_events,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::solana_perp_hours::run(&request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// The ret order is a total order, so the hour is identical at every thread
/// count (B106-1 packs block_time*100+ordinal; two events in the same second
/// tie there and the tie breaks differently under parallel aggregation).
#[test]
fn solana_perp_ret_is_identical_across_thread_counts() {
    let h0 = day_start();
    let run = |threads: u32| -> f64 {
        let tmp = Tmp::new(&format!("perp-threads-{threads}"));
        let jup = tmp.dir().join("jup");
        write_event_lane(
            &jup,
            &[
                perp_event(
                    "sigA",
                    432_100_010,
                    h0 + 60,
                    0,
                    "InstantIncreasePositionEvent",
                    r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
                ),
                perp_event(
                    "sigB",
                    432_100_010,
                    h0 + 60,
                    0,
                    "InstantIncreasePositionEvent",
                    r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "110000000", "positionCustody": "Cust1"}"#,
                ),
                perp_event(
                    "sigS",
                    432_100_010,
                    h0 + 86_400 + 3 * 3600,
                    0,
                    "InstantIncreasePositionEvent",
                    r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
                ),
            ],
        );
        let flash = tmp.dir().join("flash");
        write_event_lane(
            &flash,
            &[perp_event(
                "fS",
                432_100_010,
                h0 + 86_400 + 3 * 3600,
                0,
                "IncreaseSizeLogUSDv1",
                r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
            )],
        );
        let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
        let provenance = provenance_file(tmp.dir());
        write_mapping(tmp.dir());
        let jupiter_events = vec![jup];
        let flash_events = vec![flash];
        let request = PerpRequest {
            out: tmp.dir(),
            lockbox: &lockbox,
            lockbox_sha256: &lockbox_sha,
            provenance: &provenance,
            jupiter_events: &jupiter_events,
            flash_events: &flash_events,
            since: DAY,
            until: DAY,
            threads,
            memory_limit: "512MB".to_owned(),
        };
        let report = super::solana_perp_hours::run(&request).unwrap();
        assert_eq!(report.hours_written, 1);
        let path = tmp.dir().join(io::hour_partition(
            grid::hour_of_secs(h0),
            super::solana_perp_hours::TABLE,
        ));
        let conn = memory_connection("1", "512MB").unwrap();
        let rows = crate::store::query_rows(
            &conn,
            &format!(
                "SELECT ret FROM read_parquet('{}') WHERE venue = 'jupiter'",
                path.to_string_lossy()
            ),
            &[],
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        rows[0]
            .get("ret")
            .and_then(serde_json::Value::as_f64)
            .unwrap()
    };
    let rets = [run(1), run(2), run(4), run(8)];
    for ret in &rets {
        assert!(
            (ret - (110.0_f64 / 100.0).ln()).abs() < 1e-12,
            "sigB (price 110) is the last event: {ret}"
        );
    }
}

/// Perp lockbox hours with data inside them: the 10-03 events are read (they
/// are inside the window) but never written; the receipt pins the set size
/// and the dropped hours (M33/M34).
#[test]
fn solana_perp_lockbox_hours_are_read_but_never_written() {
    let tmp = Tmp::new("perp-lockbox");
    let oct3 = grid::day_start_secs(grid::parse_date("2026-10-03").unwrap());
    let oct7 = grid::day_start_secs(grid::parse_date("2026-10-07").unwrap());
    let jup = tmp.dir().join("jup");
    write_event_lane(
        &jup,
        &[
            perp_event(
                "jL1",
                432_100_010,
                oct3 + 60,
                0,
                "InstantIncreasePositionEvent",
                r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
            ),
            perp_event(
                "jL2",
                432_100_010,
                oct3 + 120,
                0,
                "InstantDecreasePositionEvent",
                r#"{"sizeUsdDelta": "500000", "positionSide": 2, "price": "100000000", "positionCustody": "Cust1"}"#,
            ),
            perp_event(
                "jK",
                432_100_010,
                oct7 + 60,
                0,
                "InstantIncreasePositionEvent",
                r#"{"sizeUsdDelta": "2000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
            ),
            perp_event(
                "jS",
                432_100_010,
                oct7 + 86_400 + 3 * 3600,
                0,
                "InstantIncreasePositionEvent",
                r#"{"sizeUsdDelta": "1000000", "positionSide": 1, "price": "100000000", "positionCustody": "Cust1"}"#,
            ),
        ],
    );
    let flash = tmp.dir().join("flash");
    write_event_lane(
        &flash,
        &[
            perp_event(
                "fK",
                432_100_010,
                oct7 + 70,
                0,
                "IncreaseSizeLogUSDv1",
                r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
            ),
            // A sentinel outside the window closes the day.
            perp_event(
                "fS",
                432_100_010,
                oct7 + 86_400 + 3 * 3600,
                0,
                "IncreaseSizeLogUSDv1",
                r#"{"market": "mkt1", "delta_size_usd": "100000000", "entry_price": "1000000000", "entry_price_exponent": -8}"#,
            ),
        ],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let jupiter_events = vec![jup];
    let flash_events = vec![flash];
    let request = PerpRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        jupiter_events: &jupiter_events,
        flash_events: &flash_events,
        since: "2026-10-01",
        until: "2026-10-07",
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let report = super::solana_perp_hours::run(&request).unwrap();
    assert_eq!(report.lockbox_hours_dropped, 5 * 24);
    assert!(
        !tmp.dir()
            .join(super::solana_perp_hours::TABLE)
            .join("date=2026-10-03")
            .exists(),
        "no files in the lockbox day"
    );
    assert_eq!(report.hours_written, 1, "only 2026-10-07 has written hours");
    let receipt = unit_receipt(
        tmp.dir(),
        super::solana_perp_hours::TABLE,
        super::solana_perp_hours::UNIT,
    );
    assert_eq!(receipt["lockbox_hours_excluded"], 864);
    assert_eq!(receipt["lockbox_hours_dropped"], 120);
    let conn = memory_connection("1", "512MB").unwrap();
    let path = tmp.dir().join(io::hour_partition(
        grid::hour_of_secs(oct7),
        super::solana_perp_hours::TABLE,
    ));
    let rows = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT venue, n_inc FROM read_parquet('{}')",
            path.to_string_lossy()
        ),
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 2, "jupiter and flash on 10-07");
}

/// The pyth unit refuses a day its watermark does not close (M09).
#[test]
fn pyth_refuses_an_unclosed_day() {
    let tmp = Tmp::new("pyth-unclosed");
    let lane = tmp.dir().join("pyth");
    write_pyth_lane(
        &lane,
        &[pyth_price(
            "feed-sol",
            "SOL",
            day_start() + 60,
            100.0,
            0.5,
            day_start() * 1000,
        )],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let pyth_lanes = vec![lane];
    let request = PythRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::pyth_hours::run(&request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// The pyth watermark is the most-stalled lane (B106-2 takes the max).
#[test]
fn pyth_watermark_is_the_most_stalled_lane() {
    let tmp = Tmp::new("pyth-stall");
    let ahead = tmp.dir().join("pyth-ahead");
    write_pyth_lane(
        &ahead,
        &[pyth_price(
            "feed-sol",
            "SOL",
            day_start() + 2 * 86_400,
            100.0,
            0.5,
            day_start() * 1000,
        )],
    );
    let stalled = tmp.dir().join("pyth-stalled");
    write_pyth_lane(
        &stalled,
        &[pyth_price(
            "feed-sol",
            "SOL",
            day_start() + 60,
            100.0,
            0.5,
            day_start() * 1000,
        )],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let pyth_lanes = vec![ahead, stalled];
    let request = PythRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let error = super::pyth_hours::run(&request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extends past the input watermark"),
        "{error}"
    );
}

/// Lane dedupe: the later lane wins a (feed_id, publish_time) duplicate and
/// the later at_ms wins inside one lane (M15 orders the lanes ascending).
#[test]
fn pyth_later_lane_then_later_at_ms_wins() {
    let tmp = Tmp::new("pyth-lanes");
    let t0 = day_start();
    let hist = tmp.dir().join("pyth-hist");
    write_pyth_lane(
        &hist,
        &[
            pyth_price("feed-sol", "SOL", t0 + 60, 100.0, 0.5, t0 * 1000 + 60_000),
            pyth_price("feed-sol", "SOL", t0 + 120, 90.0, 0.5, t0 * 1000 + 120_000),
            // A sentinel outside the window closes the day.
            pyth_price(
                "feed-sol",
                "SOL",
                t0 + 86_400 + 3 * 3600,
                100.0,
                0.5,
                (t0 + 86_400 + 3 * 3600) * 1000,
            ),
        ],
    );
    let live = tmp.dir().join("pyth-live");
    write_pyth_lane(
        &live,
        &[
            pyth_price("feed-sol", "SOL", t0 + 60, 120.0, 0.5, t0 * 1000 + 61_000),
            pyth_price("feed-sol", "SOL", t0 + 120, 92.0, 0.5, t0 * 1000 + 120_500),
            pyth_price("feed-sol", "SOL", t0 + 120, 95.0, 0.5, t0 * 1000 + 121_000),
            pyth_price(
                "feed-sol",
                "SOL",
                t0 + 86_400 + 3 * 3600,
                100.0,
                0.5,
                (t0 + 86_400 + 3 * 3600) * 1000,
            ),
        ],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let pyth_lanes = vec![hist, live];
    let request = PythRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: DAY,
        until: DAY,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let report = super::pyth_hours::run(&request).unwrap();
    assert_eq!(report.hours_written, 1);
    let path = tmp.dir().join(io::hour_partition(
        grid::hour_of_secs(t0),
        super::pyth_hours::TABLE,
    ));
    let conn = memory_connection("1", "512MB").unwrap();
    let rows = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT py_n, py_last, py_ret, py_conf_rel FROM read_parquet('{}')",
            path.to_string_lossy()
        ),
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.int("py_n"), Some(2), "the duplicates collapsed");
    assert_eq!(
        row.get("py_last").and_then(serde_json::Value::as_f64),
        Some(95.0),
        "the live lane's price wins the duplicate; the last publish_time is 95"
    );
    let ret = row
        .get("py_ret")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((ret - (95.0_f64 / 120.0).ln()).abs() < 1e-12, "{ret}");
    let conf = row
        .get("py_conf_rel")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((conf - (0.5 / 120.0 + 0.5 / 95.0) / 2.0).abs() < 1e-12);
}

/// Pyth lockbox hours with data inside them: prices on 10-03 are read but
/// never written; the receipt pins the set size and the dropped hours
/// (M35/M36).
#[test]
fn pyth_lockbox_hours_are_read_but_never_written() {
    let tmp = Tmp::new("pyth-lockbox");
    let oct3 = grid::day_start_secs(grid::parse_date("2026-10-03").unwrap());
    let oct7 = grid::day_start_secs(grid::parse_date("2026-10-07").unwrap());
    let lane = tmp.dir().join("pyth");
    write_pyth_lane(
        &lane,
        &[
            pyth_price(
                "feed-sol",
                "SOL",
                oct3 + 60,
                100.0,
                0.5,
                oct3 * 1000 + 60_000,
            ),
            pyth_price(
                "feed-sol",
                "SOL",
                oct3 + 120,
                110.0,
                0.5,
                oct3 * 1000 + 120_000,
            ),
            pyth_price(
                "feed-sol",
                "SOL",
                oct7 + 60,
                120.0,
                0.5,
                oct7 * 1000 + 60_000,
            ),
            pyth_price(
                "feed-sol",
                "SOL",
                oct7 + 86_400 + 3 * 3600,
                100.0,
                0.5,
                (oct7 + 86_400 + 3 * 3600) * 1000,
            ),
        ],
    );
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());
    let pyth_lanes = vec![lane];
    let request = PythRequest {
        out: tmp.dir(),
        lockbox: &lockbox,
        lockbox_sha256: &lockbox_sha,
        provenance: &provenance,
        pyth_prices: &pyth_lanes,
        since: "2026-10-01",
        until: "2026-10-07",
        threads: 1,
        memory_limit: "512MB".to_owned(),
    };
    let report = super::pyth_hours::run(&request).unwrap();
    assert_eq!(report.lockbox_hours_dropped, 5 * 24);
    assert!(
        !tmp.dir()
            .join(super::pyth_hours::TABLE)
            .join("date=2026-10-03")
            .exists(),
        "no files in the lockbox day"
    );
    assert_eq!(report.hours_written, 1, "only 2026-10-07 has written hours");
    let receipt = unit_receipt(tmp.dir(), super::pyth_hours::TABLE, super::pyth_hours::UNIT);
    assert_eq!(receipt["lockbox_hours_excluded"], 864);
    assert_eq!(receipt["lockbox_hours_dropped"], 120);
}
