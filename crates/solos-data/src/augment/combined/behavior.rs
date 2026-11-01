//! Behavior of the combined mapping unit, offline: fixture HL/Phoenix/Pyth
//! Parquet trees built in-test with DuckDB, then the lockbox checks, the mapping
//! rules, the receipt ordering, and rerun idempotence.

use super::mapping::{self, Request};
use super::receipt;
use crate::fsutil::FsyncStep;
use crate::store::{memory_connection, sql_string};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Check days the fixtures have data for (outside the lockbox weeks).
const CHECK_DAYS: &str = "2026-10-15,2026-10-16,2026-10-17";

/// HL coins of the fixture: (coin, class, px, hours with fills).
const HL_COINS: &[(&str, &str, f64, usize)] = &[
    ("SOL", "perp", 100.0, 72),
    ("kBONK", "perp", 1.0, 72),
    ("xyz:CL", "hip3", 50.0, 72),
    ("DIV", "perp", 200.0, 72),
    ("THIN", "perp", 100.0, 5),
    ("RENAMED", "perp", 100.0, 72),
    ("ONLYHL", "perp", 10.0, 72),
    ("@1", "spot", 5.0, 3),
    ("#9", "outcome", 3.0, 3),
];

/// Phoenix assets of the fixture: (symbol, price_ticks, hours with fills). The
/// tick/lot scaling is the real one: price = ticks * 100 * 10^(2-6).
const PHOENIX_ASSETS: &[(&str, i64, usize)] = &[
    ("SOL", 10_000, 72),        // 100.0
    ("kBONK", 100, 72),         // 1.0
    ("CL", 5_000, 72),          // 50.0
    ("DIV", 10_000, 72),        // 100.0 against HL 200.0
    ("THIN", 10_000, 5),        // 100.0, only 5 hours
    ("OLDNAME", 10_000, 72),    // 100.0 against HL RENAMED 100.0
    ("PHOENIXONLY", 7_000, 72), // 70.0, no HL pair
];

struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "combined-{label}-{}-{}",
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

/// The v2 lockbox body the fixtures use (four Monday weeks, guard 24h).
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

fn venues_file(dir: &Path) -> PathBuf {
    let path = dir.join("venues.json");
    std::fs::write(
        &path,
        r#"{
            "venues": [
                {"name": "jupiter", "accounts": [
                    {"address": "Cust1", "kind": "custody", "label": "SOL"},
                    {"address": "Pool1", "kind": "pool", "label": "JLP"}
                ]},
                {"name": "flash", "accounts": [
                    {"address": "Pool2", "kind": "pool", "label": "Crypto.1"}
                ]}
            ],
            "pyth": {"feeds": [
                {"symbol": "SOL", "pyth": "SOL/USD", "feedId": "feed-sol", "account": "a"},
                {"symbol": "BTC", "pyth": "BTC/USD", "feedId": "feed-btc", "account": "b"}
            ]}
        }"#,
    )
    .unwrap();
    path
}

struct Fixtures {
    hl: PathBuf,
    phoenix: PathBuf,
    pyth_dirs: Vec<PathBuf>,
    contexts: PathBuf,
    lockbox: PathBuf,
    lockbox_sha: String,
    venues: PathBuf,
    provenance: PathBuf,
}

/// Build the fixture trees: one HL hour file per check-day hour (per-coin fills
/// 60 s into the hour), one Phoenix fills file, one Pyth prices file, one
/// asset_contexts seed file.
fn build_fixtures(dir: &Path) -> Fixtures {
    let conn = memory_connection("1", "512MB").unwrap();
    let hl = dir.join("hl-fills-v1");
    std::fs::create_dir_all(&hl).unwrap();
    let base_hour = super::grid::hour_of_secs(super::grid::day_start_secs(
        super::grid::parse_date("2026-10-15").unwrap(),
    ));
    let mut watermark_hours = Vec::new();
    for (index, hour) in (base_hour..base_hour + 72).enumerate() {
        let start = hour * 3600;
        let dt = chrono::DateTime::from_timestamp(start, 0).unwrap();
        let ymd = dt.format("%Y%m%d");
        let dashed = dt.format("%Y-%m-%d");
        let hour_of_day = dt.format("%H");
        watermark_hours.push(format!("{ymd}/{hour_of_day}"));
        let dir_path = hl.join(format!("train/date={dashed}"));
        std::fs::create_dir_all(&dir_path).unwrap();
        let path = dir_path.join(format!("hour={ymd}-{hour_of_day}.parquet"));
        let mut selects = Vec::new();
        for (coin, class, px, trade_hours) in HL_COINS {
            if index >= *trade_hours {
                continue;
            }
            selects.push(format!(
                "SELECT {} AS coin, {} AS coin_class, {} AS time_ms, {hour} AS tid, {px} AS px",
                sql_string(coin),
                sql_string(class),
                start * 1000 + 60_000
            ));
        }
        let union = selects.join(" UNION ALL ");
        conn.execute_batch(&format!(
            "COPY ({union}) TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    }
    std::fs::write(
        hl.join("watermark.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema": "hl-fills-v1",
            "hours": watermark_hours
        }))
        .unwrap(),
    )
    .unwrap();

    // Phoenix fills: one file, one fill per (asset, traded hour) at +120 s.
    let phoenix = dir.join("phoenix-fills");
    std::fs::create_dir_all(&phoenix).unwrap();
    let mut values = Vec::new();
    for (symbol, ticks, trade_hours) in PHOENIX_ASSETS {
        let market = symbol.bytes().map(u64::from).sum::<u64>() % 97 + 1;
        for hour in base_hour..base_hour + i64::try_from(*trade_hours).unwrap() {
            values.push(format!(
                "({}, {}, {ticks}, 100, 2, 6, {market})",
                sql_string(symbol),
                hour * 3600 + 120
            ));
        }
    }
    let phoenix_path = phoenix.join("fills.parquet");
    conn.execute_batch(&format!(
        "CREATE TABLE pf(asset_symbol VARCHAR, block_time BIGINT, price_ticks UBIGINT,
                          tick_size UBIGINT, base_lot_decimals SMALLINT, quote_lot_decimals SMALLINT,
                          asset_id UBIGINT);
         INSERT INTO pf VALUES {};
         COPY pf TO {} (FORMAT PARQUET);",
        values.join(", "),
        sql_string(&phoenix_path.to_string_lossy())
    ))
    .unwrap();

    // Pyth prices: one file, one row per feed.
    let pyth = dir.join("pyth-prices");
    std::fs::create_dir_all(&pyth).unwrap();
    let pyth_path = pyth.join("prices.parquet");
    let first_publish = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    conn.execute_batch(&format!(
        "COPY (SELECT 'SOL' AS symbol, {first_publish} AS publish_time UNION ALL
               SELECT 'BTC' AS symbol, {} AS publish_time) TO {} (FORMAT PARQUET)",
        first_publish + 3600,
        sql_string(&pyth_path.to_string_lossy())
    ))
    .unwrap();

    // asset_contexts: the seed that renames RENAMED to OLDNAME.
    let contexts = dir.join("asset_contexts");
    std::fs::create_dir_all(&contexts).unwrap();
    let contexts_path = contexts.join("2026-10-17.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT 'RENAMED' AS symbol, 'OLDNAME' AS phoenix_symbol UNION ALL
               SELECT 'SOL' AS symbol, 'SOL' AS phoenix_symbol) TO {} (FORMAT PARQUET)",
        sql_string(&contexts_path.to_string_lossy())
    ))
    .unwrap();

    let (lockbox_path, lockbox_sha) = lockbox_file(dir);
    Fixtures {
        hl,
        phoenix,
        pyth_dirs: vec![pyth],
        contexts,
        lockbox: lockbox_path,
        lockbox_sha,
        venues: venues_file(dir),
        provenance: provenance_file(dir),
    }
}

fn request<'a>(
    out: &'a Path,
    fixtures: &'a Fixtures,
    lockbox_sha: &'a str,
    check_days: &'a str,
) -> Request<'a> {
    Request {
        out,
        lockbox: &fixtures.lockbox,
        lockbox_sha256: lockbox_sha,
        provenance: &fixtures.provenance,
        hl_fills: &fixtures.hl,
        phoenix_fills: &fixtures.phoenix,
        pyth_prices: &fixtures.pyth_dirs,
        venues_config: &fixtures.venues,
        asset_contexts: &fixtures.contexts,
        check_days,
        threads: 1,
        memory_limit: "512MB".to_owned(),
    }
}

fn assets_lines(out: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(out.join("mapping/assets.csv")).unwrap();
    text.lines().map(ToOwned::to_owned).collect()
}

/// The one row whose venue and venue symbol match (`DIV,hl,` vs `DIV,phoenix,`).
fn line_of(lines: &[String], venue: &str, symbol: &str) -> String {
    let prefix = format!("{symbol},{venue},{symbol},");
    lines
        .iter()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no {venue} row for {symbol} in {lines:?}"))
        .clone()
}

#[test]
fn lockbox_sha_mismatch_refuses_and_writes_nothing() {
    let tmp = Tmp::new("sha");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let error = mapping::run(&request(&out, &fixtures, "00", CHECK_DAYS)).unwrap_err();
    assert!(error.to_string().contains("sha256 mismatch"), "{error}");
    assert!(!out.join("mapping").exists(), "no mapping dir on refusal");
}

#[test]
fn a_check_day_inside_the_lockbox_refuses() {
    let tmp = Tmp::new("checkday");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let error = mapping::run(&request(
        &out,
        &fixtures,
        &fixtures.lockbox_sha,
        "2026-09-10,2026-10-16,2026-10-17",
    ))
    .unwrap_err();
    assert!(error.to_string().contains("inside the lockbox"), "{error}");
}

#[test]
fn three_check_days_are_required() {
    let tmp = Tmp::new("days");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let error = mapping::run(&request(
        &out,
        &fixtures,
        &fixtures.lockbox_sha,
        "2026-10-15,2026-10-16",
    ))
    .unwrap_err();
    assert!(error.to_string().contains("exactly three"), "{error}");
}

#[test]
fn threads_and_memory_caps_are_enforced() {
    let tmp = Tmp::new("caps");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let mut req = request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS);
    req.threads = 9;
    let error = mapping::run(&req).unwrap_err();
    assert!(error.to_string().contains("--threads"), "{error}");
    let mut req = request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS);
    req.memory_limit = "64GB".to_owned();
    let error = mapping::run(&req).unwrap_err();
    assert!(error.to_string().contains("32GB"), "{error}");
}

#[test]
fn mapping_rules_and_receipt() {
    let tmp = Tmp::new("rules");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let report =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    assert_eq!(report.exit_code(), 0);
    // 9 HL + 7 phoenix + 2 pyth + 1 jupiter custody; the flash pool is skipped.
    assert_eq!(report.rows, 19);
    assert_eq!(report.per_venue["hl"], 9);
    assert_eq!(report.per_venue["phoenix"], 7);
    assert_eq!(report.per_venue["pyth"], 2);
    assert_eq!(report.per_venue["jup"], 1);
    assert_eq!(report.pairs_checked, 6);
    assert_eq!(report.pairs_mapped, 4);
    assert_eq!(report.price_divergence, 1);
    assert_eq!(report.thin, 1);
    assert_eq!(report.spot_unnamed, 1);
    assert_eq!(report.outcome, 1);
    assert_eq!(report.lockbox_hours_dropped, 864);

    let lines = assets_lines(&out);
    assert_eq!(lines.len(), 20, "header plus 19 rows: {:?}", &lines[..3]);
    let first = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap()) + 60;
    let last = first + 71 * 3600;
    // Direct pair, equal prices: mapped. The contexts file seeds SOL to itself,
    // so the source of truth is the seed table.
    assert_eq!(
        line_of(&lines, "hl", "SOL"),
        format!("SOL,hl,SOL,,perp,,{first},{last},asset_contexts,true,,0.000000,72")
    );
    // The k-class is kept as its own base name.
    assert_eq!(
        line_of(&lines, "hl", "kBONK"),
        format!("kBONK,hl,kBONK,,perp,,{first},{last},base_name,true,,0.000000,72")
    );
    // HIP-3: the builder is stripped and kept; the coin is its own row.
    let cl = lines
        .iter()
        .find(|line| line.contains(",hl,xyz:CL,"))
        .expect("xyz:CL row");
    assert_eq!(
        cl,
        &format!("CL,hl,xyz:CL,xyz,hip3,,{first},{last},base_name,true,,0.000000,72")
    );
    // The asset_contexts seed renames RENAMED to OLDNAME.
    assert_eq!(
        line_of(&lines, "hl", "RENAMED"),
        format!("RENAMED,hl,RENAMED,,perp,,{first},{last},asset_contexts,true,,0.000000,72")
    );
    // Divergent prices demote the pair; the median is |ln(200/100)|.
    assert_eq!(
        line_of(&lines, "hl", "DIV"),
        format!("DIV,hl,DIV,,perp,,{first},{last},base_name,false,price_divergence,0.693147,72")
    );
    // Fewer than 24 common hours is thin even when prices agree.
    assert_eq!(
        line_of(&lines, "hl", "THIN"),
        format!(
            "THIN,hl,THIN,,perp,,{first},{},base_name,false,thin,0.000000,5",
            first + 4 * 3600
        )
    );
    // No Phoenix candidate: unmapped with no reason.
    assert_eq!(
        line_of(&lines, "hl", "ONLYHL"),
        format!("ONLYHL,hl,ONLYHL,,perp,,{first},{last},,false,,,")
    );
    // HL spot and outcomes never map; their asset_id is empty.
    let spot = lines
        .iter()
        .find(|line| line.contains(",hl,@1,"))
        .expect("@1 row");
    assert_eq!(
        spot,
        &format!(
            ",hl,@1,,spot,,{first},{},,false,spot_unnamed,,",
            first + 2 * 3600
        )
    );
    let outcome = lines
        .iter()
        .find(|line| line.contains(",hl,#9,"))
        .expect("#9 row");
    assert_eq!(
        outcome,
        &format!(
            ",hl,#9,,outcome,,{first},{},,false,outcome,,",
            first + 2 * 3600
        )
    );
    // Phoenix rows: mapped when their HL pair passed; unmapped assets carry no
    // reason when nothing was checked.
    assert!(line_of(&lines, "phoenix", "OLDNAME").starts_with("OLDNAME,phoenix,OLDNAME,,spot,"));
    assert!(line_of(&lines, "phoenix", "OLDNAME").contains(",asset_contexts,true,,0.000000,72"));
    assert!(line_of(&lines, "phoenix", "PHOENIXONLY").ends_with(",,false,,,"));
    assert!(line_of(&lines, "phoenix", "DIV").ends_with(",false,price_divergence,0.693147,72"));
    assert!(line_of(&lines, "phoenix", "SOL").contains(",asset_contexts,true,,0.000000,72"));
    // Pyth feed row: venue symbol is the market, market id the feed id.
    let publish0 = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    let sol_feed = lines
        .iter()
        .find(|line| line.contains(",pyth,SOL/USD,"))
        .expect("SOL feed row");
    assert_eq!(
        sol_feed,
        &format!("SOL,pyth,SOL/USD,,price,feed-sol,{publish0},{publish0},venues_config,true,,,")
    );
    // Jupiter custody row: the label is the asset, the address the market id.
    assert_eq!(
        lines
            .iter()
            .find(|line| line.starts_with("SOL,jup,"))
            .expect("jupiter row"),
        "SOL,jup,SOL,,perp,Cust1,,,venues_config,true,,,",
    );

    // The receipt: written after the durable output, with its sha, the lockbox
    // fields, and the provenance passthrough.
    let receipt_text = std::fs::read_to_string(receipt::receipt_path(&out, "mapping")).unwrap();
    let receipt: Value = serde_json::from_str(&receipt_text).unwrap();
    assert_eq!(receipt["unit"], "mapping");
    assert_eq!(receipt["unit_version"], 1);
    assert_eq!(receipt["rows"], 19);
    assert_eq!(receipt["lockbox_hours_dropped"], 864);
    assert_eq!(
        receipt["lockbox_sha256"], fixtures.lockbox_sha,
        "the receipt records the checked sha"
    );
    let file_sha = crate::fsutil::file_hash(&out.join("mapping/assets.csv")).unwrap();
    assert_eq!(receipt["outputs"][0]["sha256"], file_sha.as_str());
    assert_eq!(receipt["outputs"][0]["path"], "mapping/assets.csv");
    assert_eq!(
        receipt["provenance"]["solos_data_git_sha"],
        "0123456789abcdef0123456789abcdef01234567"
    );
    assert_eq!(receipt["inputs"]["hl_fills"]["hours"], 72);
    assert_eq!(
        receipt["inputs"]["check_days"],
        serde_json::json!(["2026-10-15", "2026-10-16", "2026-10-17"])
    );
    assert_eq!(receipt["started_utc"].as_str().unwrap().len(), 20);
    assert!(
        receipt["finished_utc"].as_str().unwrap() >= receipt["started_utc"].as_str().unwrap(),
        "finish after start"
    );

    // The catalog registered the sealed file, and the run's watermarks.
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(catalog["schema"], "combined-v1");
    assert_eq!(catalog["files"][0]["path"], "mapping/assets.csv");
    assert_eq!(catalog["files"][0]["sha256"], file_sha.as_str());
    assert_eq!(
        catalog["watermarks"]["mapping"]["lockbox_hours_dropped"],
        864
    );
}

#[test]
fn a_failed_output_write_leaves_no_receipt() {
    let tmp = Tmp::new("fsync");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    crate::fsutil::inject_fsync_eio(FsyncStep::TempFile);
    let error = mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS));
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the injected fsync failure must fail the run"
    );
    assert!(!receipt::receipt_path(&out, "mapping").exists());
    assert!(!out.join("catalog.json").exists());
    // A rerun without the fault succeeds and writes everything.
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    assert!(receipt::receipt_path(&out, "mapping").exists());
}

#[test]
fn a_rerun_writes_the_same_bytes() {
    let tmp = Tmp::new("rerun");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let first = std::fs::read(out.join("mapping/assets.csv")).unwrap();
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let second = std::fs::read(out.join("mapping/assets.csv")).unwrap();
    assert_eq!(first, second, "the mapping output is byte-identical");
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(
        catalog["files"].as_array().unwrap().len(),
        1,
        "no duplicate catalog entries"
    );
}

#[test]
fn a_second_process_cannot_take_the_output_lock() {
    let tmp = Tmp::new("lock");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    std::fs::create_dir_all(&out).unwrap();
    let held = mapping::lock_output(&out).expect("the lock is free");
    let error =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap_err();
    assert!(error.to_string().contains("already owned"), "{error}");
    drop(held);
}

#[test]
fn the_lockbox_loader_rejects_a_wrong_version_and_sha() {
    let tmp = Tmp::new("version");
    let body = r#"{"version":"v1","guard_hours":24,"weeks":[]}"#;
    let path = tmp.dir().join("lockbox.json");
    std::fs::write(&path, body).unwrap();
    let sha = crate::fsutil::sha256_hex(body.as_bytes());
    let error = super::lockbox::load(&path, &sha).unwrap_err();
    assert!(error.to_string().contains("not v2"), "{error}");
    let v2 = super::lockbox::tests::v2_body();
    std::fs::write(&path, &v2).unwrap();
    let error = super::lockbox::load(&path, &sha).unwrap_err();
    assert!(error.to_string().contains("sha256 mismatch"), "{error}");
}
