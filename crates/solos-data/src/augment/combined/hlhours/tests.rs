//! Offline behavior of the `hl-hours` unit: frozen stage-1 formulas on hand-computed
//! fills, the hour-boundary merge, the asset_ctxs two-lane fallback, lockbox drops,
//! refusal on unclosed hours, idempotent reruns, and the receipt count guard.
//!
//! Fixtures are tiny Parquet files DuckDB writes into a temp dir; the s3 ctx fixture is
//! a real lz4-framed CSV. No provider URLs, no shared state.

use super::{
    GUARD_HOURS, HL_TRAIN_CODE_SHA, Request, SCHEMA, STAGE1_VERSION, TABLE, UNIT, UNIT_VERSION,
    grid, lockbox, mapping, run,
};
use crate::augment::combined::{receipt, testfix};

/// Seconds in one hour (mirror of `grid::HOUR_SECS`).
const HOUR: i64 = 3_600;

/// `YYYYMMDD/H`, the hl-fills-v1 source hour key.
fn src_key(unix: i64) -> String {
    let secs = unix * HOUR;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| format!("{}/{}", dt.format("%Y%m%d"), dt.format("%-H")))
        .unwrap_or_default()
}

/// `YYYYMMDD`, the s3 daily ctx file key.
fn compact_day(unix: i64) -> String {
    grid::date_of_hour(unix).replace('-', "")
}
use crate::jsonout::Obj;
use serde_json::{Value, json};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// One temp root per test.
struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "combined-hlhours-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A test DuckDB connection: pinned version, UTC, spill under `root`.
fn conn(root: &Path) -> duckdb::Connection {
    let conn = crate::store::memory_connection("2", "2GB").unwrap();
    let temp = root.join(".tmp-duckdb-test");
    std::fs::create_dir_all(&temp).unwrap();
    conn.execute_batch(&format!(
        "SET temp_directory = {};",
        crate::store::sql_string(&temp.to_string_lossy())
    ))
    .unwrap();
    conn
}

/// Write one source hour's fills Parquet from SQL VALUES rows; returns the row count.
/// An empty `rows` writes a schema-correct empty file.
fn write_fills(c: &duckdb::Connection, path: &Path, rows: &str) -> u64 {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let empty = rows.trim().is_empty();
    let inner = if empty {
        "(null, null, null, null, null, null, null, null, null, null)".to_string()
    } else {
        rows.to_string()
    };
    let select = format!(
        "SELECT coin::VARCHAR AS coin, coin_class::VARCHAR AS coin_class, \
         time_ms::BIGINT AS time_ms, tid::BIGINT AS tid, px::DOUBLE AS px, \
         sz::DOUBLE AS sz, side::VARCHAR AS side, crossed::BOOLEAN AS crossed, \
         liquidation::VARCHAR AS liquidation, start_position::DOUBLE AS start_position \
         FROM (VALUES {inner}) t(coin, coin_class, time_ms, tid, px, sz, side, crossed, \
         liquidation, start_position) WHERE {}",
        !empty
    );
    let copy = crate::decoder::parquet::copy_statement(&select, path);
    c.execute_batch(&copy).unwrap();
    let n: i64 = c
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet({})",
                crate::store::sql_string(&path.to_string_lossy())
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    u64::try_from(n).unwrap()
}

/// Seal every source hour in `from..=to` with empty fill files (or `real` at `real_at`).
/// Receipts carry the files' real row counts; callers overwrite receipts with real files
/// via [`seal_hour_file`] afterwards when needed.
fn seal_range(
    root: &Path,
    c: &duckdb::Connection,
    from: i64,
    to: i64,
    real_at: Option<i64>,
    real: Option<&Path>,
) {
    for u in from..=to {
        let step = u - from;
        let path = if Some(u) == real_at {
            real.expect("real file required with real_at").to_path_buf()
        } else {
            let e = root.join("fills").join(format!("e{step}-{u}.parquet"));
            write_fills(c, &e, "");
            e
        };
        let key = src_key(u);
        let (day, hour) = key.split_once('/').unwrap();
        let rows: i64 = c
            .query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet({})",
                    crate::store::sql_string(&path.to_string_lossy())
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let receipt = json!({
            "schema": "hl-fills-v1",
            "hour": key,
            "input_sha256": crate::fsutil::sha256_hex(&bytes),
            "output_rows": rows,
            "outputs": [{"path": path.strip_prefix(root.join("fills")).unwrap().to_string_lossy(), "rows": rows}],
        });
        let dir = root.join("fills").join("receipts").join(day);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{hour}.json")), receipt.to_string()).unwrap();
    }
}

fn write_watermark(root: &Path, hours: &[i64]) {
    let keys: Vec<String> = hours.iter().map(|h| src_key(*h)).collect();
    let wm = json!({ "schema": "hl-fills-v1", "hours": keys });
    std::fs::write(
        root.join("watermark.json"),
        serde_json::to_vec(&wm).unwrap(),
    )
    .unwrap();
}

/// The canonical fixture hour: 2025-07-20 04:00 UTC, far from the fixture lockbox.
fn h0() -> i64 {
    chrono::NaiveDate::from_ymd_opt(2025, 7, 20)
        .unwrap()
        .and_hms_opt(4, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp()
        / HOUR
}

struct Fixture {
    root: Tmp,
    fills: PathBuf,
    lockbox: PathBuf,
    lockbox_sha: String,
    ctx_s3: PathBuf,
    ctx_capture: PathBuf,
    provenance: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = Tmp::new(label);
        let fills = root.path().join("fills");
        std::fs::create_dir_all(&fills).unwrap();
        // The shared v2 lockbox fixture (same bytes everywhere, named hours).
        let lockbox = testfix::v2_lockbox(root.path());
        let ctx_s3 = root.path().join("ctx-s3");
        let ctx_capture = root.path().join("ctx-capture");
        std::fs::create_dir_all(&ctx_s3).unwrap();
        std::fs::create_dir_all(&ctx_capture).unwrap();
        let provenance = root.path().join("provenance.json");
        std::fs::write(
            &provenance,
            json!({"unit": "hl-hours-test", "command": "cargo test"}).to_string(),
        )
        .unwrap();
        Self {
            root,
            fills,
            lockbox: lockbox.path,
            lockbox_sha: lockbox.sha256,
            ctx_s3,
            ctx_capture,
            provenance,
        }
    }

    fn request(&self, since: i64, until: i64) -> Request {
        Request {
            out: self.root.path().to_path_buf(),
            fills: self.fills.clone(),
            ctx_s3: self.ctx_s3.clone(),
            ctx_capture: self.ctx_capture.clone(),
            lockbox: self.lockbox.clone(),
            lockbox_sha256: self.lockbox_sha.clone(),
            provenance: self.provenance.clone(),
            since: Some(since),
            until: Some(until),
            threads: 2,
            memory_limit: "2GB".to_string(),
            git_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
        }
    }

    /// S3 day CSV rows are "time,coin,funding,open_interest"; the loader reads the full
    /// 12-column schema, so the trailing columns are constants (oracle and mark 1.0).
    fn write_s3_day(&self, day: &str, rows: &[String]) {
        let full: Vec<String> = rows
            .iter()
            .map(|row| format!("{row},1,1,1,1,1,1,1,1"))
            .collect();
        self.write_s3_day_exact(day, &full);
    }

    /// S3 day CSV from full 12-column rows (time,coin,funding,open_interest,
    /// prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,
    /// impact_ask_px), for tests that need oracle != mark.
    fn write_s3_day_exact(&self, day: &str, rows: &[String]) {
        let mut csv = String::from(
            "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,\
             mark_px,mid_px,impact_bid_px,impact_ask_px\n",
        );
        for row in rows {
            csv.push_str(row);
            csv.push('\n');
        }
        let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
        encoder.write_all(csv.as_bytes()).unwrap();
        let (frame, result) = encoder.finish();
        result.unwrap();
        std::fs::write(self.ctx_s3.join(format!("{day}.csv.lz4")), frame).unwrap();
    }

    /// Capture parquet for one day from (symbol, at_ms, funding, open_interest) rows;
    /// the remaining snapshot columns are constants. `day` is the compact `YYYYMMDD`;
    /// the file is named `YYYY-MM-DD.parquet`.
    fn write_capture_day(&self, c: &duckdb::Connection, day: &str, rows: &str) {
        let path = self.ctx_capture.join(format!(
            "{}-{}-{}.parquet",
            &day[0..4],
            &day[4..6],
            &day[6..8]
        ));
        let copy = crate::decoder::parquet::copy_statement(
            &format!(
                "SELECT symbol::VARCHAR AS symbol, at_ms::BIGINT AS at_ms, \
                 funding::DOUBLE AS funding, open_interest::DOUBLE AS open_interest, \
                 1.0::DOUBLE AS premium, 1.0::DOUBLE AS oracle_px, \
                 1.0::DOUBLE AS mark_px, 1.0::DOUBLE AS mid_px \
                 FROM (VALUES {rows}) t(symbol, at_ms, funding, open_interest)"
            ),
            &path,
        );
        c.execute_batch(&copy).unwrap();
    }
}

fn output_path(root: &Path, unix: i64) -> PathBuf {
    root.join(super::TABLE)
        .join(format!("date={}", grid::date_of_hour(unix)))
        .join(format!("hour={}.parquet", grid::hour_key(unix)))
}

/// Every sealed output file under the store root (the two-level layout).
fn outputs_glob(root: &Path) -> PathBuf {
    root.join(super::TABLE)
        .join("date=*")
        .join("hour=*.parquet")
}

fn hour_receipt_path(root: &Path, unix: i64) -> PathBuf {
    root.join(super::TABLE)
        .join("receipts")
        .join(format!("hour={}.json", grid::hour_key(unix)))
}

fn receipt_json(root: &Path, unix: i64) -> Value {
    serde_json::from_str(&std::fs::read_to_string(hour_receipt_path(root, unix)).unwrap()).unwrap()
}

/// Write `mapping/assets.csv` (the mapping unit's output shape: PLAN column order).
fn write_assets_csv(root: &Path, rows: &[&str]) {
    let dir = root.join("mapping");
    std::fs::create_dir_all(&dir).unwrap();
    let mut csv = String::from(
        "asset_id,venue,venue_symbol,builder,coin_class,venue_market_id,first_ts,last_ts,\
         source_of_truth,mapped,reason,median_abs_log_ratio,n_hours\n",
    );
    for row in rows {
        csv.push_str(row);
        csv.push('\n');
    }
    std::fs::write(dir.join("assets.csv"), csv).unwrap();
}

fn rows_of(path: &Path) -> Vec<Obj> {
    let c = conn(Path::new("/tmp"));
    crate::store::query_rows(
        &c,
        &format!(
            "SELECT * FROM read_parquet({}) ORDER BY coin",
            crate::store::sql_string(&path.to_string_lossy())
        ),
        &[],
    )
    .unwrap()
}

fn f(row: &Obj, key: &str) -> f64 {
    match row.get(key) {
        Some(Value::Number(n)) => n.as_f64().unwrap(),
        Some(Value::String(s)) => s.parse().unwrap(),
        other => panic!("expected number for {key}, got {other:?}"),
    }
}

fn opt_f(row: &Obj, key: &str) -> Option<f64> {
    if row.get(key).is_none() {
        panic!("key {key} missing in row {row:?}");
    }
    match row.get(key) {
        Some(Value::Number(n)) => Some(n.as_f64().unwrap()),
        Some(Value::Null) | None => None,
        other => panic!("expected number or null for {key}, got {other:?}"),
    }
}

fn s<'a>(row: &'a Obj, key: &str) -> &'a str {
    row.str(key).unwrap()
}

fn i(row: &Obj, key: &str) -> i64 {
    match row.get(key) {
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or_else(|| panic!("key {key} not a number: {row:?}")),
        Some(Value::String(s)) => s
            .parse()
            .unwrap_or_else(|_| panic!("key {key} not parseable: {row:?}")),
        other => panic!("key {key} missing or wrong type: {other:?} in {row:?}"),
    }
}

fn b(row: &Obj, key: &str) -> bool {
    match row.get(key) {
        Some(Value::Bool(v)) => *v,
        other => panic!("expected bool for {key}, got {other:?}"),
    }
}

/// Stage 1 over one hour of hand-computed fills: counts, notionals, liquidation,
/// first/last by tid order, rv from 1-minute last prices, and the open-interest delta
/// across perp, HIP-3 and spot rows.
#[test]
fn stage1_formulas_match_hand_computed_values() {
    let fx = Fixture::new("formulas");
    let h = h0();
    let c = conn(fx.root.path());
    let p1 = fx.fills.join("p1.parquet");
    let rows = format!(
        "('BTC', 'perp', {}, 1, 100.0, 2.0, 'B', true, null, 1.0), \
         ('BTC', 'perp', {}, 2, 110.0, 1.0, 'S', true, null, 2.0), \
         ('BTC', 'perp', {}, 3, 90.0, 3.0, 'B', true, 'liquidated', 0.0), \
         ('@1', 'spot', {}, 4, 5.0, 1.0, 'B', true, null, null), \
         ('xyz:HIP', 'hip3', {}, 5, 10.0, 1.0, 'B', true, null, 0.0)",
        h * HOUR * 1000 + 60_000,
        h * HOUR * 1000 + 120_000,
        h * HOUR * 1000 + 660_000,
        h * HOUR * 1000 + 30_000,
        h * HOUR * 1000 + 90_000
    );
    let n = write_fills(&c, &p1, &rows);
    assert_eq!(n, 5);
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p1));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    assert_eq!(report.coin_hour_rows, 3);

    let files = rows_of(&output_path(fx.root.path(), h));
    let btc = files.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(i(btc, "hour"), h * HOUR, "hour column is unix seconds");
    assert_eq!(i(btc, "n_trades"), 3);
    assert!((f(btc, "vol_usd") - 580.0).abs() < 1e-9);
    assert!((f(btc, "buy_usd") - 470.0).abs() < 1e-9);
    assert!((f(btc, "liq_usd") - 270.0).abs() < 1e-9);
    assert!((f(btc, "first_px") - 100.0).abs() < 1e-9);
    assert!((f(btc, "last_px") - 90.0).abs() < 1e-9);
    assert_eq!(i(btc, "first_ms"), h * HOUR * 1000 + 60_000);
    assert_eq!(i(btc, "last_ms"), h * HOUR * 1000 + 660_000);
    let dr1 = 110f64.ln() - 100f64.ln();
    let dr2 = 90f64.ln() - 110f64.ln();
    assert!((f(btc, "rv2") - (dr1 * dr1 + dr2 * dr2)).abs() < 1e-12);
    assert_eq!(i(btc, "n_minutes"), 3);
    // perp OI: +100 -55 +135 over half the signed size change
    assert!((f(btc, "d_oi_usd") - 180.0).abs() < 1e-9);

    let spot = files.iter().find(|r| s(r, "coin") == "@1").unwrap();
    assert_eq!(i(spot, "n_trades"), 1);
    assert!((f(spot, "d_oi_usd") - 0.0).abs() < 1e-12);

    let hip = files.iter().find(|r| s(r, "coin") == "xyz:HIP").unwrap();
    assert!((f(hip, "d_oi_usd") - 5.0).abs() < 1e-9);
    assert_eq!(i(hip, "n_minutes"), 1);
    assert!((f(hip, "rv2") - 0.0).abs() < 1e-12);
}

/// Fills whose exchange timestamp falls in an adjacent source hour are merged into
/// their true bucket, and a bucket without its h+1 source file stays refused.
#[test]
fn hour_boundary_strays_merge_and_unclosed_buckets_refuse() {
    let fx = Fixture::new("strays");
    let h = h0();
    let c = conn(fx.root.path());
    // file h: two SOL fills in bucket h; one stray BTC fill with time in bucket h+1
    let p_h = fx.fills.join("h.parquet");
    write_fills(
        &c,
        &p_h,
        &format!(
            "('SOL', 'perp', {}, 1, 50.0, 1.0, 'B', true, null, 0.0), \
             ('SOL', 'perp', {}, 2, 51.0, 1.0, 'B', true, null, 1.0), \
             ('BTC', 'perp', {}, 7, 80000.0, 0.1, 'B', true, null, 0.0)",
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 120_000,
            (h + 1) * HOUR * 1000 + 60_000
        ),
    );
    // file h+1: one SOL fill in bucket h+1 and one stray back into bucket h
    let p_h1 = fx.fills.join("h1.parquet");
    write_fills(
        &c,
        &p_h1,
        &format!(
            "('SOL', 'perp', {}, 3, 52.0, 1.0, 'B', true, null, 2.0), \
             ('SOL', 'perp', {}, 4, 49.0, 1.0, 'B', true, null, 3.0)",
            (h + 1) * HOUR * 1000 + 120_000,
            h * HOUR * 1000 + 660_000
        ),
    );
    // file h+2 holds a fill for bucket h+2 so the bucket would build if it could
    let p_h2 = fx.fills.join("h2.parquet");
    write_fills(
        &c,
        &p_h2,
        &format!(
            "('SOL', 'perp', {}, 9, 53.0, 1.0, 'B', true, null, 0.0)",
            (h + 2) * HOUR * 1000 + 60_000
        ),
    );

    // Everything through h + 4h is receipted (the watermark is the receipted set), so
    // buckets h and h+1 pass the guard while h+2 (which needs the watermark at h+5h)
    // stays refused.
    // Watermark with a hole: h+2 and h+3 are not receipted, so the unit runs stage 1
    // over {h-1, h, h+1, h+4} only. Bucket h builds; bucket h+1 lacks its h+2 file and
    // is refused; h+2 is beyond the guard so the range clamps to it.
    seal_range(fx.root.path(), &c, h - 1, h + 1, None, None);
    seal_hour_file(&fx, &c, h, &p_h);
    seal_hour_file(&fx, &c, h + 1, &p_h1);
    let e4 = fx.fills.join("e4.parquet");
    write_fills(&c, &e4, "");
    seal_hour_file(&fx, &c, h + 4, &e4);
    write_watermark(&fx.fills, &[h - 1, h, h + 1, h + 4]);

    let report = run(&fx.request(h, h + 2)).unwrap();
    assert_eq!(report.emit_buckets, 2, "the guard clamps until to h+1");
    assert_eq!(report.refused_hours, 1, "bucket h+1 lacks its h+2 file");
    assert_eq!(report.refused_examples, vec![grid::hour_key(h + 1)]);
    assert_eq!(report.built, 1, "report: {}", report.to_json());

    let bucket0 = rows_of(&output_path(fx.root.path(), h));
    let sol = bucket0.iter().find(|r| s(r, "coin") == "SOL").unwrap();
    assert_eq!(
        i(sol, "n_trades"),
        3,
        "2 from file h + 1 stray back from h+1"
    );
    assert!((f(sol, "vol_usd") - 150.0).abs() < 1e-9);
    assert_eq!(i(sol, "first_ms"), h * HOUR * 1000 + 60_000);
    assert_eq!(i(sol, "last_ms"), h * HOUR * 1000 + 660_000);
    // d_oi sums across files: 50*1/2 + 51*1/2 + 49*1/2 = 75.0
    assert!((f(sol, "d_oi_usd") - 75.0).abs() < 1e-9);
    // first/last px survive the cross-file merge in first_ms/last_ms order
    assert!((f(sol, "first_px") - 50.0).abs() < 1e-9);
    assert!((f(sol, "last_px") - 49.0).abs() < 1e-9);
    assert!(
        !output_path(fx.root.path(), h + 1).exists(),
        "a refused bucket writes nothing"
    );

    // Second run with the hole filled: bucket h+1 now builds and picks up both its own
    // fills and file h's forward stray; bucket h is skipped via its receipt.
    seal_hour_file(&fx, &c, h + 2, &p_h2);
    write_watermark(&fx.fills, &[h - 1, h, h + 1, h + 2, h + 4]);
    let second = run(&fx.request(h, h + 1)).unwrap();
    assert_eq!(second.skipped, 1, "bucket h is sealed already");
    assert_eq!(second.built, 1, "report: {}", second.to_json());
    let bucket1 = rows_of(&output_path(fx.root.path(), h + 1));
    let sol1 = bucket1.iter().find(|r| s(r, "coin") == "SOL").unwrap();
    assert_eq!(i(sol1, "n_trades"), 1, "only its own file fills the bucket");
    let btc1 = bucket1.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(
        i(btc1, "hour"),
        (h + 1) * HOUR,
        "the stray lives in its own bucket"
    );
    assert_eq!(i(btc1, "n_trades"), 1, "file h's forward stray lands here");
}

/// Seal one hour with a specific (non-empty) file, overwriting the empty placeholder.
fn seal_hour_file(fx: &Fixture, c: &duckdb::Connection, unix: i64, file: &Path) {
    let key = src_key(unix);
    let (day, hour) = key.split_once('/').unwrap();
    let rows: i64 = c
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet({})",
                crate::store::sql_string(&file.to_string_lossy())
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    let bytes = std::fs::read(file).unwrap();
    let receipt = json!({
        "schema": "hl-fills-v1",
        "hour": key,
        "input_sha256": crate::fsutil::sha256_hex(&bytes),
        "output_rows": rows,
        "outputs": [{"path": file.strip_prefix(fx.fills.as_path()).unwrap().to_string_lossy(), "rows": rows}],
    });
    let dir = fx.fills.join("receipts").join(day);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{hour}.json")), receipt.to_string()).unwrap();
}

/// The ctx block: s3 snapshot pick, previous-hour d_oi, the hour-0 previous-day file,
/// the per-coin capture fallback for HIP-3, and the none rows.
#[test]
fn ctx_two_lane_fallback_and_doi() {
    let fx = Fixture::new("ctx");
    let h = h0();
    let midnight = h - 4; // same-day 00:00 UTC
    let day = compact_day(h);
    // midnight's previous-hour window falls in the previous day's file
    let prev_day = compact_day(midnight - 1);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let prev_day_iso = format!(
        "{}-{}-{}",
        &prev_day[0..4],
        &prev_day[4..6],
        &prev_day[6..8]
    );
    let c = conn(fx.root.path());

    // BTC s3 snapshots: 00:59 (oi 460), 03:59 (oi 480), 04:59 (oi 500); the previous
    // day carries 23:59 (oi 440) for the midnight bucket's baseline.
    fx.write_s3_day(
        &day,
        &[
            format!("{day_iso}T00:59:00Z,BTC,0.0001,460"),
            format!("{day_iso}T03:59:00Z,BTC,0.0001,480"),
            format!("{day_iso}T04:59:00Z,BTC,0.0002,500"),
        ],
    );
    fx.write_s3_day(
        &prev_day,
        &[format!("{}T23:59:00Z,BTC,0.0001,440", prev_day_iso)],
    );
    // HIP-3 only exists in the capture lane: 03:59 (oi 990) and 04:59 (oi 1000)
    fx.write_capture_day(
        &c,
        &day,
        &format!(
            "('xyz:HIP', {}, 0.0001, 990.0), ('xyz:HIP', {}, 0.0002, 1000.0)",
            h * HOUR * 1000 - 60_000,
            h * HOUR * 1000 + 3_540_000
        ),
    );
    fx.write_capture_day(
        &c,
        &prev_day,
        &format!(
            "('xyz:HIP', {}, 0.0001, 980.0)",
            (midnight - 1) * HOUR * 1000 + 3_540_000
        ),
    );

    let p = fx.fills.join("p.parquet");
    let p_mid = fx.fills.join("p-mid.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null), \
             ('xyz:HIP', 'hip3', {}, 2, 10.0, 1.0, 'B', true, null, null), \
             ('@2', 'spot', {}, 3, 3.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 60_000
        ),
    );
    write_fills(
        &c,
        &p_mid,
        &format!(
            "('BTC', 'perp', {}, 11, 99.0, 1.0, 'B', true, null, null)",
            midnight * HOUR * 1000 + 60_000
        ),
    );
    seal_range(fx.root.path(), &c, midnight - 2, h + 3, Some(h), Some(&p));
    seal_hour_file(&fx, &c, midnight, &p_mid);
    write_watermark(&fx.fills, &(midnight - 2..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(midnight, h)).unwrap();
    assert_eq!(report.built, 5, "report: {}", report.to_json());

    // bucket h (04:00): s3 04:59 snapshot, d_oi against 03:59
    let bucket = rows_of(&output_path(fx.root.path(), h));
    let btc = bucket.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(s(btc, "ctx_source"), "s3");
    assert!(b(btc, "ctx_present"));
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 500.0).abs() < 1e-9);
    assert!((opt_f(btc, "ctx_d_oi").unwrap() - 20.0).abs() < 1e-9);
    assert!((f(btc, "ctx_oracle_px") - 1.0).abs() < 1e-12);
    assert!((f(btc, "ctx_mark_px") - 1.0).abs() < 1e-12);
    assert!((f(btc, "ctx_oracle_basis") - 0.0).abs() < 1e-12);
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0002).abs() < 1e-12);
    // HIP-3 falls back per coin to the capture lane
    let hip = bucket.iter().find(|r| s(r, "coin") == "xyz:HIP").unwrap();
    assert_eq!(s(hip, "ctx_source"), "capture");
    assert!(b(hip, "ctx_present"));
    assert!((opt_f(hip, "ctx_open_interest").unwrap() - 1000.0).abs() < 1e-9);
    assert!((opt_f(hip, "ctx_d_oi").unwrap() - 10.0).abs() < 1e-9);
    // a coin with nothing anywhere
    let spot = bucket.iter().find(|r| s(r, "coin") == "@2").unwrap();
    assert_eq!(s(spot, "ctx_source"), "none");
    assert!(!b(spot, "ctx_present"));
    assert!(opt_f(spot, "ctx_funding").is_none());

    // midnight bucket: the 00:59 snapshot with the previous day's 23:59 baseline
    let mid = rows_of(&output_path(fx.root.path(), midnight));
    let mbtc = mid.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(s(mbtc, "ctx_source"), "s3");
    assert!(b(mbtc, "ctx_present"));
    assert!((opt_f(mbtc, "ctx_open_interest").unwrap() - 460.0).abs() < 1e-9);
    assert!((opt_f(mbtc, "ctx_d_oi").unwrap() - 20.0).abs() < 1e-9);
}

/// The ctx snapshot window is half-open: a snapshot stamped exactly at h+1h
/// belongs to bucket h+1, not to h (the review's R7 boundary).
#[test]
fn ctx_snapshot_at_the_next_hour_boundary_belongs_to_the_next_bucket() {
    let fx = Fixture::new("ctx-boundary");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    // One in-window snapshot for bucket h (04:30) and one stamped exactly at the
    // h+1h edge (05:00:00.000), the first instant of bucket h+1.
    fx.write_s3_day(
        &day,
        &[
            format!("{day_iso}T04:30:00Z,BTC,0.0001,100"),
            format!("{day_iso}T05:00:00Z,BTC,0.0002,200"),
        ],
    );
    let p_h = fx.fills.join("h.parquet");
    write_fills(
        &c,
        &p_h,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000 + 60_000
        ),
    );
    let p_h1 = fx.fills.join("h1.parquet");
    write_fills(
        &c,
        &p_h1,
        &format!(
            "('BTC', 'perp', {}, 2, 101.0, 1.0, 'B', true, null, null)",
            (h + 1) * HOUR * 1000 + 60_000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 4, None, None);
    seal_hour_file(&fx, &c, h, &p_h);
    seal_hour_file(&fx, &c, h + 1, &p_h1);
    write_watermark(&fx.fills, &(h - 1..=h + 4).collect::<Vec<_>>());

    let report = run(&fx.request(h, h + 1)).unwrap();
    assert_eq!(report.built, 2, "report: {}", report.to_json());

    // Bucket h picks the 04:30 snapshot; the 05:00 edge instant is outside [h, h+1h).
    let bucket = rows_of(&output_path(fx.root.path(), h));
    let btc = bucket.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(s(btc, "ctx_source"), "s3");
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 100.0).abs() < 1e-9);
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0001).abs() < 1e-12);
    assert!(
        opt_f(btc, "ctx_d_oi").is_none(),
        "no snapshot in [h-1h, h), so the baseline is missing"
    );

    // Bucket h+1 owns the edge snapshot and baselines its d_oi on bucket h's.
    let next = rows_of(&output_path(fx.root.path(), h + 1));
    let nbtc = next.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(s(nbtc, "ctx_source"), "s3");
    assert!((opt_f(nbtc, "ctx_open_interest").unwrap() - 200.0).abs() < 1e-9);
    assert!((opt_f(nbtc, "ctx_funding").unwrap() - 0.0002).abs() < 1e-12);
    assert!((opt_f(nbtc, "ctx_d_oi").unwrap() - 100.0).abs() < 1e-9);
}

/// The ctx snapshot for a bucket is the LAST one in [h, h+1h): two snapshots in
/// the hour, and the bucket takes the later one (the review's B25).
#[test]
fn the_last_snapshot_in_the_hour_wins() {
    let fx = Fixture::new("ctx-last");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    fx.write_s3_day(
        &day,
        &[
            format!("{day_iso}T04:10:00Z,BTC,0.0001,500"),
            format!("{day_iso}T04:59:00Z,BTC,0.0002,510"),
        ],
    );
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert_eq!(s(btc, "ctx_source"), "s3");
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 510.0).abs() < 1e-9);
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0002).abs() < 1e-12);
}

/// A ctx snapshot carrying a NaN or infinite field is not present at all
/// (ADR-0023): the coin falls to the next lane (capture after s3) or to
/// `ctx_present = false`, and a non-finite previous-hour baseline leaves the
/// d_oi NULL. A non-finite value must never reach the sealed table as a
/// silently-nulled field with `ctx_present` still true (the review's R14).
#[test]
fn a_non_finite_ctx_snapshot_is_not_present() {
    let fx = Fixture::new("ctx-nonfinite");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    // BTC: its only s3 snapshot carries a NaN open interest and it has no
    // capture row, so it reads none. SOL: a finite 04:59 snapshot whose 03:59
    // baseline carries an infinite funding stays present with a NULL d_oi.
    // CAP: the s3 snapshot is NaN, but a finite capture snapshot exists, so it
    // falls to the capture lane. BAD: a capture-only coin with an infinite
    // funding reads none.
    fx.write_s3_day(
        &day,
        &[
            format!("{day_iso}T03:59:00Z,SOL,inf,550"),
            format!("{day_iso}T04:59:00Z,BTC,0.0001,nan"),
            format!("{day_iso}T04:59:00Z,SOL,0.0003,600"),
            format!("{day_iso}T04:59:00Z,CAP,nan,1"),
        ],
    );
    fx.write_capture_day(
        &c,
        &day,
        &format!(
            "('CAP', {}, 0.0004, 700.0), ('BAD', {}, CAST('inf' AS DOUBLE), 1.0)",
            h * HOUR * 1000 + 3_540_000,
            h * HOUR * 1000 + 3_540_000
        ),
    );
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null), \
             ('SOL', 'perp', {}, 2, 50.0, 1.0, 'B', true, null, null), \
             ('CAP', 'perp', {}, 3, 70.0, 1.0, 'B', true, null, null), \
             ('BAD', 'perp', {}, 4, 80.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 60_000,
            h * HOUR * 1000 + 60_000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    assert_eq!(report.ctx_rows_by_source["none"], 2, "BTC and BAD");
    assert_eq!(report.ctx_rows_by_source["s3"], 1, "SOL");
    assert_eq!(report.ctx_rows_by_source["capture"], 1, "CAP");

    let bucket = rows_of(&output_path(fx.root.path(), h));
    let btc = bucket.iter().find(|r| s(r, "coin") == "BTC").unwrap();
    assert_eq!(
        s(btc, "ctx_source"),
        "none",
        "the NaN s3 snapshot is not present"
    );
    assert!(!b(btc, "ctx_present"));
    assert!(opt_f(btc, "ctx_open_interest").is_none());
    assert!(opt_f(btc, "ctx_funding").is_none());

    let sol = bucket.iter().find(|r| s(r, "coin") == "SOL").unwrap();
    assert_eq!(s(sol, "ctx_source"), "s3");
    assert!(b(sol, "ctx_present"));
    assert!((opt_f(sol, "ctx_open_interest").unwrap() - 600.0).abs() < 1e-9);
    assert!(
        opt_f(sol, "ctx_d_oi").is_none(),
        "the infinite baseline snapshot is not present"
    );

    let cap = bucket.iter().find(|r| s(r, "coin") == "CAP").unwrap();
    assert_eq!(
        s(cap, "ctx_source"),
        "capture",
        "the NaN s3 row falls to the capture lane"
    );
    assert!(b(cap, "ctx_present"));
    assert!((opt_f(cap, "ctx_open_interest").unwrap() - 700.0).abs() < 1e-9);

    let bad = bucket.iter().find(|r| s(r, "coin") == "BAD").unwrap();
    assert_eq!(
        s(bad, "ctx_source"),
        "none",
        "the infinite capture snapshot is not present"
    );
    assert!(!b(bad, "ctx_present"));
}

/// Changing the previous-day ctx file rebuilds the midnight bucket: the skip key
/// hashes both days' files, and the midnight d_oi baselines on the previous day
/// (the review's S21).
#[test]
fn a_changed_previous_day_ctx_file_rebuilds_the_midnight_bucket() {
    let fx = Fixture::new("ctx-prevday");
    let midnight = h0() - 4; // 00:00 UTC of the fixture day
    let day = compact_day(midnight);
    let prev_day = compact_day(midnight - 1);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let prev_iso = format!(
        "{}-{}-{}",
        &prev_day[0..4],
        &prev_day[4..6],
        &prev_day[6..8]
    );
    let c = conn(fx.root.path());
    fx.write_s3_day(&prev_day, &[format!("{prev_iso}T23:59:00Z,BTC,0.0001,440")]);
    fx.write_s3_day(&day, &[format!("{day_iso}T00:59:00Z,BTC,0.0002,460")]);
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            midnight * HOUR * 1000 + 60_000
        ),
    );
    seal_range(
        fx.root.path(),
        &c,
        midnight - 2,
        midnight + 3,
        Some(midnight),
        Some(&p),
    );
    write_watermark(
        &fx.fills,
        &(midnight - 2..=midnight + 3).collect::<Vec<_>>(),
    );

    let first = run(&fx.request(midnight, midnight)).unwrap();
    assert_eq!(first.built, 1, "report: {}", first.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), midnight))[0];
    assert!(
        (opt_f(btc, "ctx_d_oi").unwrap() - 20.0).abs() < 1e-9,
        "460 - 440"
    );

    // The previous day's last snapshot moves: the key must change and the
    // midnight bucket must rebuild onto the new baseline.
    fx.write_s3_day(&prev_day, &[format!("{prev_iso}T23:59:00Z,BTC,0.0001,450")]);
    let second = run(&fx.request(midnight, midnight)).unwrap();
    assert_eq!(
        second.built, 1,
        "a changed previous-day ctx file must rebuild"
    );
    assert_eq!(second.skipped, 0);
    let btc = &rows_of(&output_path(fx.root.path(), midnight))[0];
    assert!(
        (opt_f(btc, "ctx_d_oi").unwrap() - 10.0).abs() < 1e-9,
        "460 - 450"
    );
}

/// Hours inside a lockbox week (including the guard band) are dropped unwritten,
/// and the dropped count is the covered input hours inside the set (not the
/// requested range): the watermark covers five lockbox hours, the requested
/// range only one.
#[test]
fn lockbox_hours_are_dropped() {
    let fx = Fixture::new("lockbox");
    let c = conn(fx.root.path());
    // The early_stress week 2026-06-01..08 plus guard covers 2026-05-31..2026-06-09
    let h = chrono::NaiveDate::from_ymd_opt(2026, 6, 3)
        .unwrap()
        .and_hms_opt(5, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp()
        / HOUR;
    // A free hour days after the week, so the second run's catalog entry exists.
    let h_out = chrono::NaiveDate::from_ymd_opt(2026, 6, 10)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp()
        / HOUR;
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 3, h + 1, Some(h), Some(&p));
    seal_range(fx.root.path(), &c, h_out - 1, h_out + 1, None, None);
    let watermark: Vec<i64> = (h - 3..=h + 1).chain(h_out - 1..=h_out + 3).collect();
    write_watermark(&fx.fills, &watermark);

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 0);
    assert_eq!(report.lockbox_hours_dropped, 5, "the whole covered span");
    assert!(!output_path(fx.root.path(), h).exists());
    // The receipt keeps the two apart: dropped is the covered input hours the
    // lockbox removed, excluded is the v2 set's own size.
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(fx.root.path(), TABLE, UNIT)).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["lockbox_hours_dropped"], 5);
    assert_eq!(receipt["lockbox_hours_excluded"], 864);

    // A later run over the free hour keeps counting the covered lockbox hours
    // (the watermark, not the requested range) and records them in the catalog.
    let second = run(&fx.request(h_out, h_out)).unwrap();
    assert_eq!(second.built, 1, "report: {}", second.to_json());
    assert_eq!(second.lockbox_hours_dropped, 5);
    let cat: Value = serde_json::from_str(
        &std::fs::read_to_string(fx.root.path().join("catalog.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(cat["watermarks"][UNIT]["lockbox_hours_dropped"], 5);
    assert_eq!(cat["watermarks"][UNIT]["lockbox_hours_excluded"], 864);
    // The one sealed output (the free hour) stays outside the lockbox hours.
    let lb = testfix::v2_lockbox(fx.root.path());
    testfix::assert_no_parquet_rows_in_lockbox(
        &c,
        &outputs_glob(fx.root.path()),
        "hour",
        testfix::TsUnit::Secs,
        &lb.hours,
    );
}

/// A lockbox sha mismatch refuses the whole run.
#[test]
fn lockbox_sha_mismatch_refuses() {
    let fx = Fixture::new("locksha");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());
    let mut request = fx.request(h, h);
    request.lockbox_sha256 = "0".repeat(64);
    assert!(run(&request).is_err(), "sha mismatch refuses the run");
}

/// A rerun with the same inputs skips every already-sealed hour and the bytes match.
#[test]
fn rerun_is_idempotent() {
    let fx = Fixture::new("idempotent");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let first = run(&fx.request(h, h)).unwrap();
    assert_eq!(first.built, 1, "report: {}", first.to_json());
    assert_eq!(first.skipped, 0);
    assert_eq!(first.coin_hour_rows, 1);
    let path = output_path(fx.root.path(), h);
    let bytes1 = std::fs::read(&path).unwrap();
    let receipt1 = serde_json::to_string(&receipt_json(fx.root.path(), h)).unwrap();
    // The catalog watermark carries the two lockbox counts with U1's meaning:
    // dropped = input hours this run actually lost, excluded = the v2 set size.
    let cat: Value = serde_json::from_str(
        &std::fs::read_to_string(fx.root.path().join("catalog.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(cat["watermarks"][UNIT]["lockbox_hours_dropped"], 0);
    assert_eq!(cat["watermarks"][UNIT]["lockbox_hours_excluded"], 864);
    // The sealed outputs stay outside the lockbox fixture's hours.
    let lb = testfix::v2_lockbox(fx.root.path());
    testfix::assert_no_parquet_rows_in_lockbox(
        &c,
        &outputs_glob(fx.root.path()),
        "hour",
        testfix::TsUnit::Secs,
        &lb.hours,
    );

    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 0, "nothing rebuilt");
    assert_eq!(second.skipped, 1);
    let bytes2 = std::fs::read(&path).unwrap();
    assert_eq!(bytes1, bytes2, "same inputs must produce the same bytes");
    let receipt2 = serde_json::to_string(&receipt_json(fx.root.path(), h)).unwrap();
    assert_eq!(receipt1, receipt2);
    // The skip path still reports the sealed rows: the run report's coinHourRows
    // and the rewritten run receipt's rows both carry them across a full-skip
    // rerun (the review's R12).
    assert_eq!(
        second.coin_hour_rows, first.coin_hour_rows,
        "the skip path keeps counting the sealed rows"
    );
    let run_receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(fx.root.path(), TABLE, UNIT)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        run_receipt["rows"], first.coin_hour_rows,
        "the run receipt's rows survive a full-skip rerun"
    );
}

/// A source row count that disagrees with its receipt fails the run loudly.
#[test]
fn receipt_count_mismatch_fails() {
    let fx = Fixture::new("count");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    // tamper with the receipt after sealing
    let key = src_key(h);
    let (day, hour) = key.split_once('/').unwrap();
    let receipt_path = fx
        .fills
        .join("receipts")
        .join(day)
        .join(format!("{hour}.json"));
    let mut receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    receipt["output_rows"] = json!(999);
    std::fs::write(&receipt_path, receipt.to_string()).unwrap();
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    assert!(
        run(&fx.request(h, h)).is_err(),
        "count mismatch must fail the run"
    );
    assert!(!output_path(fx.root.path(), h).exists());
}

/// A source receipt listing no output files is refused: stage 1 could never run
/// over it, and the refusal names the hour (the review's B30).
#[test]
fn a_source_receipt_without_outputs_is_refused() {
    let fx = Fixture::new("no-outputs");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    // Strip the outputs from source hour h's receipt after sealing.
    let key = src_key(h);
    let (day, hour) = key.split_once('/').unwrap();
    let receipt_path = fx
        .fills
        .join("receipts")
        .join(day)
        .join(format!("{hour}.json"));
    let mut receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    receipt["outputs"] = json!([]);
    std::fs::write(&receipt_path, receipt.to_string()).unwrap();
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let Err(error) = run(&fx.request(h, h)) else {
        panic!("a receipt without outputs must refuse");
    };
    assert!(
        error.to_string().contains("no output files"),
        "the refusal names the receipt: {error}"
    );
    assert!(!output_path(fx.root.path(), h).exists());
}

/// The watermark hour parser refuses `YYYYMMDD/24`: hours run 0..=23, and a
/// 24th hour would silently alias the next day's midnight (the review's B24).
#[test]
fn watermark_hour_24_is_refused() {
    let fx = Fixture::new("hour-24");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    let mut keys: Vec<String> = (h - 1..=h + 3).map(src_key).collect();
    keys.push("20250720/24".to_string());
    let wm = json!({ "schema": "hl-fills-v1", "hours": keys });
    std::fs::write(
        fx.fills.join("watermark.json"),
        serde_json::to_vec(&wm).unwrap(),
    )
    .unwrap();

    let Err(error) = run(&fx.request(h, h)) else {
        panic!("hour 24 must refuse the watermark");
    };
    assert!(
        error.to_string().contains("out of range"),
        "the parser names the bad hour: {error}"
    );
}

/// The hour key conversions round-trip and the schema/version pins hold.
#[test]
fn hour_keys_and_pins() {
    let h = h0();
    let key = src_key(h);
    assert_eq!(key, "20250720/4");
    assert_eq!(grid::hour_key(h), "20250720-04", "the common padded key");
    assert_eq!(grid::date_of_hour(h), "2025-07-20");
    assert_eq!(SCHEMA, "combined-hl-coin-hour-v1");
    assert_eq!(STAGE1_VERSION, "s1-v1");
    assert_eq!(GUARD_HOURS, 2);
    assert_eq!(HL_TRAIN_CODE_SHA.len(), 40);
    assert_eq!(UNIT_VERSION, super::UNIT_VERSION);
}

/// The hour receipt's skipKey names this unit and writer version, so a receipt
/// from another unit or an older writer can never skip this unit's hours (the
/// review's B31).
#[test]
fn the_hour_receipt_skip_key_names_the_unit_and_version() {
    let fx = Fixture::new("skipkey-pin");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    let receipt = receipt_json(fx.root.path(), h);
    assert_eq!(receipt["skipKey"]["unit"], json!(UNIT));
    assert_eq!(receipt["skipKey"]["unitVersion"], json!(UNIT_VERSION));
    // And a rerun still skips with the pinned key in place.
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.skipped, 1);
    assert_eq!(second.built, 0);
}

/// A mapping written after a first run rebuilds the sealed hour with its asset_id:
/// the skip key pins the assets.csv content. A third run with nothing changed skips.
#[test]
fn adding_assets_csv_rebuilds_and_populates_asset_id() {
    let fx = Fixture::new("assets-rebuild");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let first = run(&fx.request(h, h)).unwrap();
    assert_eq!(first.built, 1, "report: {}", first.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!(
        matches!(btc.get("asset_id"), None | Some(Value::Null)),
        "no mapping yet: {btc:?}"
    );

    write_assets_csv(
        fx.root.path(),
        &["A-BTC,hl,BTC,BTC,perp,,,2025-07-20T04:59:59Z,paired,mapped,,0.001,24"],
    );
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(
        second.built,
        1,
        "the new assets.csv must rebuild: {}",
        second.to_json()
    );
    assert_eq!(second.skipped, 0);
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert_eq!(s(btc, "asset_id"), "A-BTC");

    let third = run(&fx.request(h, h)).unwrap();
    assert_eq!(third.built, 0, "nothing changed since the rebuild");
    assert_eq!(third.skipped, 1);
}

/// The asset_id join scopes to venue='hl' rows: another venue's row for the same
/// venue_symbol must not win the min() in the final select.
#[test]
fn the_asset_id_join_scopes_to_hl_rows() {
    let fx = Fixture::new("venue-filter");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());
    write_assets_csv(
        fx.root.path(),
        &[
            "Z-HL,hl,BTC,BTC,perp,,,2025-07-20T04:59:59Z,paired,mapped,,0.001,24",
            "A-OTH,hlx,BTC,BTC,perp,,,2025-07-20T04:59:59Z,paired,mapped,,0.001,24",
        ],
    );

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert_eq!(s(btc, "asset_id"), "Z-HL", "only the hl row joins");
}

/// A receipt sealed under an older writer config (different configSha256) rebuilds.
#[test]
fn a_stale_config_sha_in_the_hour_receipt_forces_a_rebuild() {
    let fx = Fixture::new("stale-config");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());
    run(&fx.request(h, h)).unwrap();

    let path = hour_receipt_path(fx.root.path(), h);
    let mut old: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    old["skipKey"]["configSha256"] = json!("0".repeat(64));
    std::fs::write(&path, old.to_string()).unwrap();

    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a stale config sha must rebuild");
    assert_eq!(second.skipped, 0);
    let sealed: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        sealed["skipKey"]["configSha256"],
        json!(super::config_hash())
    );
}

/// A different lockbox file (same weeks, different bytes and sha) rebuilds.
#[test]
fn a_changed_lockbox_file_forces_a_rebuild() {
    let fx = Fixture::new("lockbox-change");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());
    run(&fx.request(h, h)).unwrap();

    let mut body: Value = serde_json::from_str(&lockbox::tests::v2_body()).unwrap();
    body["created"] = json!("2026-10-23");
    let path2 = fx.root.path().join("lockbox2.json");
    std::fs::write(&path2, body.to_string()).unwrap();
    let sha2 = crate::fsutil::sha256_hex(&std::fs::read(&path2).unwrap());
    let mut request = fx.request(h, h);
    request.lockbox = path2;
    request.lockbox_sha256 = sha2;

    let second = run(&request).unwrap();
    assert_eq!(second.built, 1, "a changed lockbox file must rebuild");
    assert_eq!(second.skipped, 0);
}

/// A lockbox with the right sha but one week of hours dropped is not v2: the
/// shape pin refuses the run even though the bytes hash to the operator's sha
/// (the review's B22).
#[test]
fn a_three_week_lockbox_refuses_even_with_a_matching_sha() {
    let fx = Fixture::new("lockbox-shape");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    // One week (with its guard band) dropped from the shared v2 body: 648
    // hours over 27 dates, so the shape pin must fire.
    let mut body: Value = serde_json::from_str(&lockbox::tests::v2_body()).unwrap();
    body["weeks"].as_array_mut().unwrap().remove(0);
    let path = fx.root.path().join("lockbox-short.json");
    std::fs::write(&path, body.to_string()).unwrap();
    let sha = crate::fsutil::sha256_hex(&std::fs::read(&path).unwrap());
    let mut request = fx.request(h, h);
    request.lockbox = path;
    request.lockbox_sha256 = sha;

    let Err(error) = run(&request) else {
        panic!("a dropped week must refuse the run");
    };
    assert!(
        error.to_string().contains("864"),
        "the shape error names the pin: {error}"
    );
    assert!(!output_path(fx.root.path(), h).exists());
}

/// Changed ctx input files rebuild the sealed hour and the new values land.
#[test]
fn changed_ctx_inputs_force_a_rebuild() {
    let fx = Fixture::new("ctx-change");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    fx.write_s3_day(&day, &[format!("{day_iso}T04:59:00Z,BTC,0.0001,500")]);
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let first = run(&fx.request(h, h)).unwrap();
    assert_eq!(first.built, 1, "report: {}", first.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0001).abs() < 1e-12);

    fx.write_s3_day(&day, &[format!("{day_iso}T04:59:00Z,BTC,0.0002,510")]);
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "changed ctx inputs must rebuild");
    assert_eq!(second.skipped, 0);
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0002).abs() < 1e-12);
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 510.0).abs() < 1e-9);
}

/// Re-sealing a source hour with different fills must rebuild its bucket: the
/// skip key carries the source facts (input sha, rows) per hour, so the rerun
/// cannot skip on the old receipt (the review's B14).
#[test]
fn a_resealed_source_hour_rebuilds_its_bucket() {
    let fx = Fixture::new("reseal");
    let h = h0();
    let c = conn(fx.root.path());
    let p1 = fx.fills.join("p1.parquet");
    write_fills(
        &c,
        &p1,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000 + 60_000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p1));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let first = run(&fx.request(h, h)).unwrap();
    assert_eq!(first.built, 1, "report: {}", first.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!((f(btc, "first_px") - 100.0).abs() < 1e-9);

    // The same source hour, re-sealed from a new file at px 200: a new input
    // sha and new receipt facts. The bucket must rebuild and carry the price.
    let p2 = fx.fills.join("p2.parquet");
    write_fills(
        &c,
        &p2,
        &format!(
            "('BTC', 'perp', {}, 2, 200.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000 + 60_000
        ),
    );
    seal_hour_file(&fx, &c, h, &p2);

    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a re-sealed source hour must rebuild");
    assert_eq!(second.skipped, 0);
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!((f(btc, "first_px") - 200.0).abs() < 1e-9);
}

/// A sealed file tampered after its receipt is rebuilt, and the fresh file matches
/// the receipt's sha256 again.
#[test]
fn a_tampered_sealed_file_is_rebuilt() {
    let fx = Fixture::new("tamper");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());
    run(&fx.request(h, h)).unwrap();

    let out = output_path(fx.root.path(), h);
    let mut bytes = std::fs::read(&out).unwrap();
    bytes.extend_from_slice(b"tampered");
    std::fs::write(&out, bytes).unwrap();

    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a tampered sealed file must rebuild");
    assert_eq!(second.skipped, 0);
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(hour_receipt_path(fx.root.path(), h)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        crate::fsutil::file_hash(&out).unwrap(),
        receipt["output"]["sha256"],
        "the rebuilt file matches the receipt again"
    );
}

/// The oracle basis divides by the oracle price, not the mark.
#[test]
fn oracle_basis_uses_the_oracle_as_denominator() {
    let fx = Fixture::new("oracle-basis");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    fx.write_s3_day_exact(
        &day,
        &[format!(
            "{day_iso}T04:59:00Z,BTC,0.0001,500.0,1.0,1.0,0.0002,100.0,110.0,105.0,99.0,101.0"
        )],
    );
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert!((f(btc, "ctx_oracle_px") - 100.0).abs() < 1e-9);
    assert!((f(btc, "ctx_mark_px") - 110.0).abs() < 1e-9);
    assert!((f(btc, "ctx_oracle_basis") - 0.1).abs() < 1e-12);
}

/// A coin covered by both ctx lanes reads s3: the capture row must not win.
#[test]
fn s3_wins_over_capture_for_a_shared_coin() {
    let fx = Fixture::new("s3-priority");
    let h = h0();
    let day = compact_day(h);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let c = conn(fx.root.path());
    fx.write_s3_day(&day, &[format!("{day_iso}T04:59:00Z,BTC,0.0001,500")]);
    fx.write_capture_day(
        &c,
        &day,
        &format!("('BTC', {}, 9.75, 1000.0)", h * HOUR * 1000 + 3_540_000),
    );
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert_eq!(s(btc, "ctx_source"), "s3");
    assert!((opt_f(btc, "ctx_funding").unwrap() - 0.0001).abs() < 1e-12);
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 500.0).abs() < 1e-9);
}

/// A run refuses while another process holds the combined root lock.
#[test]
fn a_second_run_refuses_while_the_root_is_locked() {
    let fx = Fixture::new("locked");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let _held = mapping::lock_output(fx.root.path()).unwrap();
    assert!(
        run(&fx.request(h, h)).is_err(),
        "the held lock must refuse the run"
    );
}

/// The scan-threads setting is pinned to 1..=8: values outside the bound
/// refuse the run before DuckDB is opened (the review's B28).
#[test]
fn the_scan_threads_bound_is_pinned() {
    let fx = Fixture::new("threads");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(&c, &p, "");
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let mut zero = fx.request(h, h);
    zero.threads = 0;
    assert!(run(&zero).is_err(), "threads = 0 must refuse");
    let mut nine = fx.request(h, h);
    nine.threads = 9;
    assert!(run(&nine).is_err(), "threads = 9 must refuse");
    assert!(!output_path(fx.root.path(), h).exists());
}

/// A failed fsync of the sealed file's temp leaves no output and no receipt; the
/// fault must hit the sealed file's temp fsync, not a later one.
#[test]
fn a_failed_sealed_file_fsync_leaves_no_output() {
    let fx = Fixture::new("fsync-sealed");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    crate::fsutil::inject_fsync_failure(crate::fsutil::FsyncStep::TempFile);
    let error = run(&fx.request(h, h));
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
        "the fault hit the sealed file's temp fsync, got {synced:?}"
    );
    assert!(!output_path(fx.root.path(), h).exists(), "no sealed output");
    assert!(
        !hour_receipt_path(fx.root.path(), h).exists(),
        "no receipt after a failed output"
    );
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a rerun without the fault builds the hour");
}

/// A failed directory fsync after the sealed Parquet rename fails the run, and
/// the fault must hit the sealed file's partition directory, not a later
/// durable write's: the hour receipt's own dir fsync would otherwise consume
/// the one-shot arm and hide the missing call (the review's R11).
#[test]
fn a_failed_dir_fsync_after_the_sealed_rename_fails_the_run() {
    let fx = Fixture::new("fsync-dir");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    crate::fsutil::inject_fsync_failure(crate::fsutil::FsyncStep::DirAfterRename);
    let error = run(&fx.request(h, h));
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the injected dir-fsync failure must fail the run"
    );
    assert_eq!(
        crate::fsutil::last_fsync_step(),
        Some(crate::fsutil::FsyncStep::DirAfterRename),
        "the fault hit a directory fsync after a rename"
    );
    let synced = crate::fsutil::last_fsync_path().unwrap_or_default();
    assert_eq!(
        synced.file_name(),
        Some(std::ffi::OsStr::new(&format!(
            "date={}",
            grid::date_of_hour(h)
        ))),
        "the fault hit the sealed file's partition dir fsync, got {synced:?}"
    );
    assert!(
        output_path(fx.root.path(), h).exists(),
        "the rename happened before the failed fsync: the file is on disk, its \\
         directory entry not yet durable"
    );
    assert!(
        !hour_receipt_path(fx.root.path(), h).exists(),
        "no hour receipt after the failed dir fsync"
    );
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a rerun without the fault builds the hour");
}

/// A failed durable write of the hour receipt leaves the sealed file but no receipt.
#[test]
fn a_failed_hour_receipt_write_leaves_the_sealed_file_but_no_receipt() {
    let fx = Fixture::new("fsync-receipt");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    crate::fsutil::inject_write_enospc();
    let error = run(&fx.request(h, h));
    crate::fsutil::clear_write_failure();
    assert!(
        error.is_err(),
        "the faulted receipt write must fail the run"
    );
    assert!(
        output_path(fx.root.path(), h).exists(),
        "the sealed file is durable"
    );
    assert!(
        !hour_receipt_path(fx.root.path(), h).exists(),
        "no receipt after the failed write"
    );
    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 1, "a rerun seals the unreceipted hour");
}

/// The first hour after the lockbox does not baseline its d_oi on a snapshot from
/// inside the exclusion: its previous-hour OI counts as missing there.
#[test]
fn ctx_d_oi_is_missing_after_a_lockbox_gap() {
    let fx = Fixture::new("lockgap");
    let lb = testfix::v2_lockbox(fx.root.path());
    let h = lb.hour_after();
    assert!(
        lb.hours.contains(&(h - 1)),
        "the baseline hour sits inside the exclusion"
    );
    let day = compact_day(h);
    let prev_day = compact_day(h - 1);
    let day_iso = format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]);
    let prev_iso = format!(
        "{}-{}-{}",
        &prev_day[0..4],
        &prev_day[4..6],
        &prev_day[6..8]
    );
    fx.write_s3_day(&prev_day, &[format!("{prev_iso}T23:59:00Z,BTC,0.0001,440")]);
    fx.write_s3_day(&day, &[format!("{day_iso}T00:59:00Z,BTC,0.0002,460")]);
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 1, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 1..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    // The watermark covers h-1 (inside the set) and three free hours: dropped
    // counts the covered inside hour even though the requested range does not.
    assert_eq!(report.lockbox_hours_dropped, 1);
    let btc = &rows_of(&output_path(fx.root.path(), h))[0];
    assert_eq!(s(btc, "ctx_source"), "s3");
    assert!((opt_f(btc, "ctx_open_interest").unwrap() - 460.0).abs() < 1e-9);
    assert!(
        opt_f(btc, "ctx_d_oi").is_none(),
        "the baseline snapshot is inside the lockbox"
    );
}

/// The CLI exits 0 when every requested hour closed, and nonzero when hours were
/// refused, so a scheduler can see the refused tail.
#[test]
fn the_cli_exits_nonzero_when_hours_are_refused() {
    let fx = Fixture::new("cli-exit");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 5, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 5..=h + 3).collect::<Vec<_>>());
    let day = "2025-07-20";
    let args = |fx: &Fixture| -> Vec<String> {
        vec![
            "hl-hours".into(),
            "--out".into(),
            fx.root.path().to_string_lossy().into_owned(),
            "--fills".into(),
            fx.fills.to_string_lossy().into_owned(),
            "--ctx-s3".into(),
            fx.ctx_s3.to_string_lossy().into_owned(),
            "--ctx-capture".into(),
            fx.ctx_capture.to_string_lossy().into_owned(),
            "--lockbox".into(),
            fx.lockbox.to_string_lossy().into_owned(),
            "--lockbox-sha256".into(),
            fx.lockbox_sha.clone(),
            "--provenance".into(),
            fx.provenance.to_string_lossy().into_owned(),
            "--since".into(),
            day.into(),
            "--until".into(),
            day.into(),
        ]
    };
    let code = crate::cli::combined_hl_hours_run(&args(&fx)).unwrap();
    assert_eq!(code, 0, "every requested hour is closed and receipted");

    // A watermark with a hole: the buckets before h keep refusing, and the exit
    // code says so.
    write_watermark(&fx.fills, &[h - 1, h, h + 1, h + 2, h + 3]);
    let code = crate::cli::combined_hl_hours_run(&args(&fx)).unwrap();
    assert_ne!(code, 0, "refused hours must be visible in the exit code");
}

/// Real fills inside the lockbox are dropped unwritten: no output row carries a
/// lockbox hour, and dropped counts the covered hours the set removed.
#[test]
fn fills_inside_the_lockbox_are_dropped_and_absent_from_outputs() {
    let fx = Fixture::new("lockbox-data");
    let lb = testfix::v2_lockbox(fx.root.path());
    let h_in = lb.mid_hour();
    let h_out = lb.hour_after();
    let c = conn(fx.root.path());
    // A real fill inside the lockbox and one in the first free hour after it.
    let p_in = fx.fills.join("in.parquet");
    write_fills(
        &c,
        &p_in,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h_in * HOUR * 1000
        ),
    );
    let p_out = fx.fills.join("out.parquet");
    write_fills(
        &c,
        &p_out,
        &format!(
            "('BTC', 'perp', {}, 2, 90.0, 1.0, 'B', true, null, null)",
            h_out * HOUR * 1000
        ),
    );
    // Sources: the inside hour's bucket needs h_in-1, h_in, h_in+1 receipted
    // and the free hour needs h_out-1 (the last lockbox hour), h_out, h_out+1;
    // the lockbox never gates input receipts. The watermark reaches h_out+3,
    // so both requested buckets are closed, not refused.
    let e1 = fx.fills.join("e1.parquet");
    write_fills(&c, &e1, "");
    seal_hour_file(&fx, &c, h_in - 1, &e1);
    seal_hour_file(&fx, &c, h_in, &p_in);
    seal_hour_file(&fx, &c, h_in + 1, &e1);
    seal_hour_file(&fx, &c, h_out - 1, &e1);
    seal_hour_file(&fx, &c, h_out, &p_out);
    seal_hour_file(&fx, &c, h_out + 1, &e1);
    write_watermark(
        &fx.fills,
        &[
            h_in - 1,
            h_in,
            h_in + 1,
            h_out - 1,
            h_out,
            h_out + 1,
            h_out + 2,
            h_out + 3,
        ],
    );

    // The inside hour itself is requested: complete and closed, so only the
    // lockbox can stop it. It is dropped, never refused, and stays unwritten.
    // (Dropped counts the whole covered watermark, so run 1 already reports 3.)
    let first = run(&fx.request(h_in, h_in)).unwrap();
    assert_eq!(first.built, 0, "report: {}", first.to_json());
    assert_eq!(
        first.refused_hours, 0,
        "the inside hour is closed, not refused"
    );
    assert_eq!(first.lockbox_hours_dropped, 3, "the covered inside hours");
    assert!(
        !output_path(fx.root.path(), h_in).exists(),
        "no file for the inside hour"
    );

    let report = run(&fx.request(h_out, h_out)).unwrap();
    assert_eq!(report.built, 1, "report: {}", report.to_json());
    assert_eq!(
        report.lockbox_hours_dropped, 3,
        "covered inside hours: h_in, h_in+1 (in week 3) and h_out-1; h_in-1 sits \
         in the gap between weeks 2 and 3"
    );
    // No row of any sealed output carries a lockbox hour.
    let glob = outputs_glob(fx.root.path());
    testfix::assert_no_parquet_rows_in_lockbox(&c, &glob, "hour", testfix::TsUnit::Secs, &lb.hours);
    let run_receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(fx.root.path(), TABLE, UNIT)).unwrap(),
    )
    .unwrap();
    assert_eq!(run_receipt["lockbox_hours_dropped"], 3);
    assert_eq!(run_receipt["lockbox_hours_excluded"], 864);
}

/// A second run over the same inputs skips every sealed hour, and the skip
/// survives a differently-formed store root (an embedded "./" segment): root
/// and sealed file share one form at the `seal::check` call.
#[test]
fn a_rerun_skips_every_sealed_hour_in_any_root_form() {
    let fx = Fixture::new("root-form");
    let h = h0();
    let c = conn(fx.root.path());
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 2, h + 4, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 2..=h + 4).collect::<Vec<_>>());

    let first = run(&fx.request(h - 1, h)).unwrap();
    assert_eq!(first.built, 2, "report: {}", first.to_json());
    let paths: Vec<PathBuf> = [h - 1, h]
        .iter()
        .map(|u| output_path(fx.root.path(), *u))
        .collect();
    let before: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();

    let second = run(&fx.request(h - 1, h)).unwrap();
    assert_eq!(second.skipped, 2, "report: {}", second.to_json());
    assert_eq!(second.built, 0);
    let after: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    assert_eq!(before, after, "sealed bytes unchanged by the skip");

    // The same store root, spelled through an embedded "./" segment: root and
    // sealed file still share one form at the check, so every hour skips and
    // the sealed bytes stay identical.
    let parent = fx.root.path().parent().unwrap().to_path_buf();
    let name = fx.root.path().file_name().unwrap().to_os_string();
    let mut dotted = fx.request(h - 1, h);
    dotted.out = parent.join(Path::new(".")).join(name);
    let third = run(&dotted).unwrap();
    assert_eq!(third.skipped, 2, "report: {}", third.to_json());
    assert_eq!(third.built, 0, "report: {}", third.to_json());
    let after_dotted: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    assert_eq!(before, after_dotted, "sealed bytes unchanged by the skip");

    // The hard case: a symlinked store root. canonicalize resolves the link,
    // so a canonical-vs-raw mismatch (or any other form change) must not turn
    // the skip into a rebuild; both linked runs reuse the sealed bytes. The
    // link lives beside the store with a unique name and is removed on the
    // way out, even when an assertion fails mid-test.
    let link = parent.join(format!(
        "store-link-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::os::unix::fs::symlink(fx.root.path(), &link).unwrap();
    struct RemoveLink(PathBuf);
    impl Drop for RemoveLink {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _guard = RemoveLink(link.clone());
    let mut linked = fx.request(h - 1, h);
    linked.out = link.clone();
    let via_link = run(&linked).unwrap();
    assert_eq!(via_link.skipped, 2, "report: {}", via_link.to_json());
    assert_eq!(via_link.built, 0, "report: {}", via_link.to_json());
    let via_link_again = run(&linked).unwrap();
    assert_eq!(
        via_link_again.skipped,
        2,
        "report: {}",
        via_link_again.to_json()
    );
    assert_eq!(via_link_again.built, 0);
    let after_link: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    assert_eq!(before, after_link, "sealed bytes unchanged by the skip");
}
