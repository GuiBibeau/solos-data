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
            432_100_000i64,
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
fn write_decoded_root(
    conn: &duckdb::Connection,
    root: &Path,
    fills_files: &[(&str, i64, Vec<String>)],
    tx_files: &[(&str, i64, Vec<String>)],
    fills_writer: &dyn Fn(&duckdb::Connection, &Path, &[String]),
    tx_writer: &dyn Fn(&duckdb::Connection, &Path, &[String]),
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
                r#"{{"path": {}, "table_name": {}, "row_count": "0", "sha256": {}, "batch_id": {}, "created_at": "2026-10-30T00:00:00Z"}}"#,
                serde_json::json!(relative),
                serde_json::json!(table),
                serde_json::json!(sha),
                serde_json::json!(batch_id.to_string()),
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
        sol.6, 400.0,
        "e2 is the earliest by (block_time, tx, ordinal)"
    );
    assert_eq!(sol.7, 300.0, "the corrected e1 is the latest");
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

/// A lockbox week inside the requested range: its 144 hours (busy week guard
/// over 2026-10-01..06) are counted and never written; 2026-10-07 is built.
#[test]
fn the_lockbox_week_is_dropped_from_the_range() {
    let tmp = Tmp::new("phoenix-lockbox");
    let conn = memory_connection("1", "512MB").unwrap();
    let root = tmp.dir().join("decoded-lockbox");
    // Fills on 2026-10-07 hour 0 plus a sentinel on 10-08 03:00.
    let start = grid::day_start_secs(grid::parse_date("2026-10-07").unwrap());
    let fills = vec![
        Fill {
            event_id: "e-oct7",
            signature: "sigL",
            source_hash: "hL",
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
                tx_row("sigL", "hL", 432_100_004),
                tx_row("sigL2", "hL2", 432_100_005),
            ],
        )],
        &write_fills_parquet,
        &write_tx_parquet,
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
    for day in 1..=5 {
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
}

/// A Jupiter event lane fixture: fields JSON with 1e6-scaled USD and prices,
/// one signature present in both lanes (live wins), plus one flash event whose
/// market is not in the mapping.
fn build_perp_lanes(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
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
            r#"{"positionSizeUsd": "5000000", "fundingFeeUsd": "100000", "price": "100000000", "positionCustody": "Cust1"}"#,
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
    let live_rows = [event(
        "j1",
        0,
        h0 + 600,
        "InstantIncreasePositionEvent",
        r#"{"sizeUsdDelta": "3000000", "positionSide": 1, "price": "150000000", "positionCustody": "Cust1"}"#,
    )];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        live_rows.join(", "),
        sql_string(&jup_live.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    let flash = dir.join("flash-hist");
    std::fs::create_dir_all(&flash).unwrap();
    let flash_rows = [event(
        "f1",
        0,
        h0 + 500,
        "IncreaseSizeLogUSDv1",
        r#"{"market": "mkt1", "delta_size_usd": "250000000", "entry_price": "2000000000", "entry_price_exponent": -8}"#,
    )];
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {}) t(signature, slot, block_time, ordinal, name, fields))
         TO {} (FORMAT PARQUET)",
        flash_rows.join(", "),
        sql_string(&flash.join("events.parquet").to_string_lossy())
    ))
    .unwrap();
    (jup_hist, jup_live, flash)
}

/// The perp unit: lane dedupe (live wins), the 1e6 scaling, buy_share over
/// long increases, funding and liquidation dollars, and flash rows with a NULL
/// asset when the mapping has no market row.
#[test]
fn solana_perp_scales_dedupes_and_maps() {
    let tmp = Tmp::new("perp");
    let (jup_hist, jup_live, flash) = build_perp_lanes(tmp.dir());
    let (lockbox, lockbox_sha) = lockbox_file(tmp.dir());
    let provenance = provenance_file(tmp.dir());
    write_mapping(tmp.dir());

    let jupiter_events = vec![jup_hist.clone(), jup_live.clone()];
    let flash_events = vec![flash.clone()];
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
    assert_eq!(rows.len(), 2, "{rows:?}");
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
        (ret - (150.0_f64 / 100.0).ln()).abs() < 1e-12,
        "prices 100 to 150"
    );
    let flash_row = rows
        .iter()
        .find(|r| r.str("venue") == Some("flash"))
        .unwrap();
    assert_eq!(flash_row.str("venue_key"), Some("mkt1"));
    assert_eq!(
        flash_row.str("asset_id"),
        None,
        "flash markets are unmapped"
    );
    assert_eq!(flash_row.get("mapped"), Some(&serde_json::Value::Null));
    let flash_size = flash_row
        .get("size_usd")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((flash_size - 250.0).abs() < 1e-9);
    assert_eq!(
        flash_row.get("buy_share"),
        Some(&serde_json::Value::Null),
        "flash events carry no side"
    );
    assert_eq!(
        report.jupiter_events, 3,
        "j1 (deduped), j2, j3; j4 is outside the window"
    );
    assert_eq!(report.flash_events, 1);
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
