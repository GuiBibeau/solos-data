//! Offline behavior of the `hl-hours` unit: frozen stage-1 formulas on hand-computed
//! fills, the hour-boundary merge, the asset_ctxs two-lane fallback, lockbox drops,
//! refusal on unclosed hours, idempotent reruns, and the receipt count guard.
//!
//! Fixtures are tiny Parquet files DuckDB writes into a temp dir; the s3 ctx fixture is
//! a real lz4-framed CSV. No provider URLs, no shared state.

use super::{
    GUARD_HOURS, HL_TRAIN_CODE_SHA, Request, SCHEMA, STAGE1_VERSION, TABLE, UNIT, UNIT_VERSION,
    grid, lockbox, run,
};
use crate::augment::combined::receipt;

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

/// A v2 lockbox fixture with the real shape (the common lockbox test body): four
/// 2026 Monday weeks with a 24 h guard, 864 excluded hours over 36 UTC days.
fn write_lockbox(root: &Path) -> (PathBuf, String) {
    let path = root.join("lockbox.json");
    std::fs::write(&path, lockbox::tests::v2_body()).unwrap();
    let sha = crate::fsutil::sha256_hex(&std::fs::read(&path).unwrap());
    (path, sha)
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
        let (lockbox, lockbox_sha) = write_lockbox(root.path());
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
            lockbox,
            lockbox_sha,
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
        let mut csv = String::from(
            "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,\
             mark_px,mid_px,impact_bid_px,impact_ask_px\n",
        );
        for row in rows {
            csv.push_str(row);
            csv.push_str(",1,1,1,1,1,1,1,1\n");
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

fn receipt_json(root: &Path, unix: i64) -> Value {
    let path = root
        .join(super::TABLE)
        .join("receipts")
        .join(format!("hour={}.json", grid::hour_key(unix)));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
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

/// Every hour inside a lockbox week (including the guard band) is dropped unwritten.
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
    let p = fx.fills.join("p.parquet");
    write_fills(
        &c,
        &p,
        &format!(
            "('BTC', 'perp', {}, 1, 100.0, 1.0, 'B', true, null, null)",
            h * HOUR * 1000
        ),
    );
    seal_range(fx.root.path(), &c, h - 3, h + 3, Some(h), Some(&p));
    write_watermark(&fx.fills, &(h - 3..=h + 3).collect::<Vec<_>>());

    let report = run(&fx.request(h, h)).unwrap();
    assert_eq!(report.built, 0);
    assert_eq!(report.lockbox_hours_dropped, 1);
    assert!(!output_path(fx.root.path(), h).exists());
    // The receipt keeps the two apart: dropped is the one input hour this run
    // actually lost to the lockbox, excluded is the v2 set's own size.
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(fx.root.path(), TABLE, UNIT)).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["lockbox_hours_dropped"], 1);
    assert_eq!(receipt["lockbox_hours_excluded"], 864);
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
    let path = output_path(fx.root.path(), h);
    let bytes1 = std::fs::read(&path).unwrap();
    let receipt1 = serde_json::to_string(&receipt_json(fx.root.path(), h)).unwrap();

    let second = run(&fx.request(h, h)).unwrap();
    assert_eq!(second.built, 0, "nothing rebuilt");
    assert_eq!(second.skipped, 1);
    let bytes2 = std::fs::read(&path).unwrap();
    assert_eq!(bytes1, bytes2, "same inputs must produce the same bytes");
    let receipt2 = serde_json::to_string(&receipt_json(fx.root.path(), h)).unwrap();
    assert_eq!(receipt1, receipt2);
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
