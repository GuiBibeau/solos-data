//! Behavior of the combined mapping unit, offline: fixture HL/Phoenix/Pyth
//! Parquet trees built in-test with DuckDB, then the lockbox checks, the mapping
//! rules, the durable-write ordering, and rerun idempotence. The fixture puts
//! fills inside a lockbox week and a Phoenix asset outside the check days, so
//! every filter is load-bearing: removing one changes the pinned output.

use super::mapping::{self, Request};
use super::receipt;
use crate::fsutil::FsyncStep;
use crate::store::{memory_connection, sql_string};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Check days the fixtures have data for (outside the lockbox weeks).
const CHECK_DAYS: &str = "2026-10-15,2026-10-16,2026-10-17";

/// Boundary prices: 0.9% and 1.01% log gaps, and a 20% gap.
fn eq99_px() -> f64 {
    100.0 * 0.0099_f64.exp()
}

fn eq101_px() -> f64 {
    100.0 * 0.0101_f64.exp()
}

fn mid02_px() -> f64 {
    100.0 * 0.2_f64.exp()
}

/// HL coins of the fixture: (coin, class, px on even hours, px on odd hours,
/// hours with fills). Prices alternate so even-count medians are averages.
fn hl_coins() -> Vec<(String, String, f64, f64, usize)> {
    vec![
        ("SOL".into(), "perp".into(), 100.0, 100.0, 72),
        ("kBONK".into(), "perp".into(), 1.0, 1.0, 72),
        ("xyz:CL".into(), "hip3".into(), 50.0, 50.0, 72),
        ("DIV".into(), "perp".into(), 200.0, 201.0, 72),
        ("THIN".into(), "perp".into(), 100.0, 100.0, 5),
        ("RENAMED".into(), "perp".into(), 100.0, 100.0, 72),
        ("ONLYHL".into(), "perp".into(), 10.0, 10.0, 72),
        ("@1".into(), "spot".into(), 5.0, 5.0, 3),
        ("#9".into(), "outcome".into(), 3.0, 3.0, 3),
        ("EQ99".into(), "perp".into(), eq99_px(), eq99_px(), 48),
        ("EQ101".into(), "perp".into(), eq101_px(), eq101_px(), 24),
        ("MID02".into(), "perp".into(), mid02_px(), mid02_px(), 72),
        ("TENH".into(), "perp".into(), 100.0, 100.0, 10),
        ("TWO".into(), "perp".into(), 100.0, 100.0, 72),
        ("xyz:TWO".into(), "hip3".into(), mid02_px(), mid02_px(), 72),
    ]
}

/// Phoenix assets of the fixture: (symbol, price_ticks, hours with fills). The
/// tick/lot scaling is the real one: price = ticks * 100 * 10^(2-6), so 10_000
/// ticks is 100.0.
const PHOENIX_ASSETS: &[(&str, i64, usize)] = &[
    ("SOL", 10_000, 72),
    ("kBONK", 100, 72),
    ("CL", 5_000, 72),
    ("DIV", 10_000, 72),
    ("THIN", 10_000, 5),
    ("OLDNAME", 10_000, 72),
    ("PHOENIXONLY", 7_000, 72),
    ("EQ99", 10_000, 48),
    ("EQ101", 10_000, 24),
    ("MID02", 10_000, 72),
    ("TENH", 10_000, 10),
    ("TWO", 10_000, 72),
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

#[derive(Clone)]
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

/// The check-day hour base and the lockbox-week hour (fixture constants).
fn hour_bases() -> (i64, i64) {
    let base = super::grid::hour_of_secs(super::grid::day_start_secs(
        super::grid::parse_date("2026-10-15").unwrap(),
    ));
    // 2026-06-02 is inside the early_stress week with its guard.
    let june = super::grid::hour_of_secs(super::grid::day_start_secs(
        super::grid::parse_date("2026-06-02").unwrap(),
    ));
    (base, june)
}

/// Build the fixture trees: one HL hour file per check-day hour (plus three
/// SOL hours inside the lockbox week), one Phoenix fills file (plus SOL fills
/// in the lockbox week and PHOENIXONLY fills outside the check days), one Pyth
/// prices file, one asset_contexts seed file.
fn build_fixtures(dir: &Path) -> Fixtures {
    let conn = memory_connection("1", "512MB").unwrap();
    let hl = dir.join("hl-fills-v1");
    std::fs::create_dir_all(&hl).unwrap();
    let (base_hour, june_hour) = hour_bases();
    let mut watermark_hours = Vec::new();
    let write_hl_hour = |conn: &duckdb::Connection,
                         hour: i64,
                         watermark_hours: &mut Vec<String>,
                         rows: &Vec<(String, String, f64)>| {
        let start = hour * 3600;
        let dt = chrono::DateTime::from_timestamp(start, 0).unwrap();
        let ymd = dt.format("%Y%m%d");
        let dashed = dt.format("%Y-%m-%d");
        let hour_of_day = dt.format("%H");
        watermark_hours.push(format!("{ymd}/{hour_of_day}"));
        let dir_path = hl.join(format!("train/date={dashed}"));
        std::fs::create_dir_all(&dir_path).unwrap();
        let path = dir_path.join(format!("hour={ymd}-{hour_of_day}.parquet"));
        let selects: Vec<String> = rows
            .iter()
            .map(|(coin, class, px)| {
                format!(
                    "SELECT {} AS coin, {} AS coin_class, {} AS time_ms, {hour} AS tid, CAST({px} AS DOUBLE) AS px",
                    sql_string(coin),
                    sql_string(class),
                    start * 1000 + 60_000
                )
            })
            .collect();
        let union = selects.join(" UNION ALL ");
        conn.execute_batch(&format!(
            "COPY ({union}) TO {} (FORMAT PARQUET)",
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    };
    for (index, hour) in (base_hour..base_hour + 72).enumerate() {
        let rows: Vec<(String, String, f64)> = hl_coins()
            .iter()
            .filter(|(_, _, _, _, trade_hours)| index < *trade_hours)
            .map(|(coin, class, px_even, px_odd, _)| {
                (
                    coin.clone(),
                    class.clone(),
                    if index % 2 == 0 { *px_even } else { *px_odd },
                )
            })
            .collect();
        write_hl_hour(&conn, hour, &mut watermark_hours, &rows);
    }
    // Three SOL hours inside the lockbox week: the universe and price filters
    // must keep them out of first_ts/last_ts and out of the price check.
    for offset in 1..=3 {
        write_hl_hour(
            &conn,
            june_hour + offset,
            &mut watermark_hours,
            &vec![("SOL".to_owned(), "perp".to_owned(), 111.0)],
        );
    }
    // A coin that trades only inside the lockbox week: it has no row left
    // after the drop, so it must not appear in the mapping at all.
    for offset in 1..=3 {
        write_hl_hour(
            &conn,
            june_hour + offset,
            &mut watermark_hours,
            &vec![("LOCKCOIN".to_owned(), "perp".to_owned(), 7.0)],
        );
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
    let market_of = |symbol: &str| symbol.bytes().map(u64::from).sum::<u64>() % 97 + 1;
    let mut values = Vec::new();
    for (symbol, ticks, trade_hours) in PHOENIX_ASSETS {
        let market = market_of(symbol);
        for hour in base_hour..base_hour + i64::try_from(*trade_hours).unwrap() {
            values.push(format!(
                "({}, {}, {ticks}, 100, 2, 6, {market})",
                sql_string(symbol),
                hour * 3600 + 120
            ));
        }
    }
    // SOL fills inside the lockbox week (price 111.0 = 11_100 ticks).
    for offset in 1..=3 {
        values.push(format!(
            "({}, {}, 11_100, 100, 2, 6, {})",
            sql_string("SOL"),
            (june_hour + offset) * 3600 + 120,
            market_of("SOL")
        ));
    }
    // PHOENIXONLY fills outside the check days (2026-10-20), inside no lockbox
    // week: the price window must keep them out of the price check.
    let oct20 = super::grid::hour_of_secs(super::grid::day_start_secs(
        super::grid::parse_date("2026-10-20").unwrap(),
    ));
    for offset in 5..=6 {
        values.push(format!(
            "({}, {}, 7_000, 100, 2, 6, {})",
            sql_string("PHOENIXONLY"),
            (oct20 + offset) * 3600 + 120,
            market_of("PHOENIXONLY")
        ));
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

/// First and last fixture fill second on the check days (fills sit 60 s into
/// the hour on HL, 120 s on Phoenix).
fn check_bounds() -> (i64, i64, i64, i64) {
    let base = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    (
        base,
        base + 60,
        base + 71 * 3600 + 60,
        base + 71 * 3600 + 120,
    )
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
        "2026-06-03,2026-10-16,2026-10-17",
    ))
    .unwrap_err();
    assert!(error.to_string().contains("inside the lockbox"), "{error}");
}

#[test]
fn three_distinct_check_days_are_required() {
    let tmp = Tmp::new("days");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    for days in ["2026-10-15,2026-10-16", "2026-10-15,2026-10-15,2026-10-16"] {
        let error =
            mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, days)).unwrap_err();
        assert!(error.to_string().contains("exactly three"), "{error}");
    }
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
fn a_v2_lockbox_with_the_wrong_week_count_refuses() {
    let tmp = Tmp::new("shape");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    // A v2-labeled lockbox with only three weeks is not the lockbox the plan
    // pins: it must refuse before anything is written.
    let body: serde_json::Value = serde_json::from_str(&super::lockbox::tests::v2_body()).unwrap();
    let mut three = body.clone();
    three["weeks"] = three["weeks"]
        .as_array()
        .unwrap()
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .into();
    let path = tmp.dir().join("lockbox-three.json");
    std::fs::write(&path, three.to_string()).unwrap();
    let sha = crate::fsutil::sha256_hex(three.to_string().as_bytes());
    let fixtures = Fixtures {
        lockbox: path.clone(),
        lockbox_sha: sha,
        ..fixtures
    };
    let error =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap_err();
    assert!(error.to_string().contains("864"), "{error}");
    assert!(!out.join("mapping").exists());
}

/// The full mapping rules on the extended fixture: every price-relevant filter
/// (lockbox weeks, check-day files, Phoenix window) is load-bearing, because
/// the fixture holds fills the filters must hide.
#[test]
fn mapping_rules_lockbox_filters_and_receipt() {
    let tmp = Tmp::new("rules");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let report =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    assert_eq!(report.exit_code(), 0);
    // 15 HL + 12 phoenix + 2 pyth + 1 jup; the flash pool is skipped and
    // LOCKCOIN (lockbox-only) has no row at all.
    assert_eq!(report.rows, 30);
    assert_eq!(report.per_venue["hl"], 15);
    assert_eq!(report.per_venue["phoenix"], 12);
    assert_eq!(report.per_venue["pyth"], 2);
    assert_eq!(report.per_venue["jup"], 1);
    assert_eq!(report.pairs_checked, 12);
    assert_eq!(report.pairs_mapped, 6);
    assert_eq!(report.price_divergence, 4);
    assert_eq!(report.thin, 2);
    assert_eq!(report.spot_unnamed, 1);
    assert_eq!(report.outcome, 1);
    assert_eq!(report.lockbox_hours_dropped, 3);
    assert_eq!(report.lockbox_hours_excluded, 864);

    let lines = assets_lines(&out);
    assert_eq!(lines.len(), 31, "header plus 30 rows");
    let (day_start, first, last, phoenix_last) = check_bounds();
    // The sort the ADR states: venue, then venue symbol.
    assert_eq!(
        lines[0],
        "asset_id,venue,venue_symbol,builder,coin_class,venue_market_id,first_ts,last_ts,source_of_truth,mapped,reason,median_abs_log_ratio,n_hours"
    );
    assert!(lines[1].starts_with(",hl,#9,"), "outcomes sort first");
    assert!(lines[2].starts_with(",hl,@1,,spot,"));
    assert!(
        lines[16].starts_with("SOL,jup,"),
        "jup follows the hl block"
    );
    assert!(lines[17].starts_with("CL,phoenix,"));
    assert!(lines[29].starts_with("BTC,pyth,"), "pyth closes the file");

    // The lockbox-week fills (SOL 111.0 on both venues, June) are invisible:
    // SOL keeps its check-day span, its 72 common hours, and its price.
    assert_eq!(
        line_of(&lines, "hl", "SOL"),
        format!("SOL,hl,SOL,,perp,,{first},{last},asset_contexts,true,,0.000000,72")
    );
    assert_eq!(
        line_of(&lines, "phoenix", "SOL"),
        format!(
            "SOL,phoenix,SOL,,spot,45,{},{phoenix_last},asset_contexts,true,,0.000000,72",
            day_start + 120
        )
    );
    assert!(
        !lines.iter().any(|line| line.contains("LOCKCOIN")),
        "a coin trading only inside the lockbox has no mapping row"
    );
    // The k-class is kept as its own base name (the seed-fallback pair).
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
    // Divergent prices demote the pair. DIV alternates 200/201 against 100, so
    // the even-count median is the average of ln 2 and ln 2.01.
    assert_eq!(
        line_of(&lines, "hl", "DIV"),
        format!("DIV,hl,DIV,,perp,,{first},{last},base_name,false,price_divergence,0.695641,72")
    );
    // Thin wins below the hour floor even at a perfect price.
    assert_eq!(
        line_of(&lines, "hl", "THIN"),
        format!(
            "THIN,hl,THIN,,perp,,{first},{},base_name,false,thin,0.000000,5",
            first + 4 * 3600
        )
    );
    // Boundary pairs around the 1% bar and the 24-hour floor.
    assert_eq!(
        line_of(&lines, "hl", "EQ99"),
        format!(
            "EQ99,hl,EQ99,,perp,,{first},{},base_name,true,,0.009900,48",
            first + 47 * 3600
        )
    );
    assert_eq!(
        line_of(&lines, "hl", "EQ101"),
        format!(
            "EQ101,hl,EQ101,,perp,,{first},{},base_name,false,price_divergence,0.010100,24",
            first + 23 * 3600
        )
    );
    assert_eq!(
        line_of(&lines, "hl", "MID02"),
        format!(
            "MID02,hl,MID02,,perp,,{first},{last},base_name,false,price_divergence,0.200000,72"
        )
    );
    assert_eq!(
        line_of(&lines, "hl", "TENH"),
        format!(
            "TENH,hl,TENH,,perp,,{first},{},base_name,false,thin,0.000000,10",
            first + 9 * 3600
        )
    );
    // Two HL coins collapse onto one Phoenix asset and stay separate rows with
    // their own outcomes.
    assert_eq!(
        line_of(&lines, "hl", "TWO"),
        format!("TWO,hl,TWO,,perp,,{first},{last},base_name,true,,0.000000,72")
    );
    let two_hip3 = lines
        .iter()
        .find(|line| line.contains(",hl,xyz:TWO,"))
        .expect("xyz:TWO row");
    assert_eq!(
        two_hip3,
        &format!(
            "TWO,hl,xyz:TWO,xyz,hip3,,{first},{last},base_name,false,price_divergence,0.200000,72"
        )
    );
    // HL spot and outcomes never map; their asset_id is empty.
    assert_eq!(
        lines
            .iter()
            .find(|line| line.contains(",hl,@1,"))
            .expect("@1 row"),
        &format!(
            ",hl,@1,,spot,,{first},{},,false,spot_unnamed,,",
            first + 2 * 3600
        )
    );
    assert_eq!(
        lines
            .iter()
            .find(|line| line.contains(",hl,#9,"))
            .expect("#9 row"),
        &format!(
            ",hl,#9,,outcome,,{first},{},,false,outcome,,",
            first + 2 * 3600
        )
    );
    // No Phoenix candidate: unmapped with no reason.
    assert_eq!(
        line_of(&lines, "hl", "ONLYHL"),
        format!("ONLYHL,hl,ONLYHL,,perp,,{first},{last},,false,,,")
    );
    // Phoenix rows: mapped when ANY HL pair for the asset passed (TWO has one
    // mapped and one divergent pair).
    assert!(line_of(&lines, "phoenix", "TWO").contains(",base_name,true,,0.000000,72"));
    assert!(line_of(&lines, "phoenix", "OLDNAME").contains(",asset_contexts,true,,0.000000,72"));
    assert!(line_of(&lines, "phoenix", "PHOENIXONLY").ends_with(",,false,,,"));
    assert!(line_of(&lines, "phoenix", "EQ99").ends_with(",base_name,true,,0.009900,48"));
    assert!(line_of(&lines, "phoenix", "DIV").ends_with(",false,price_divergence,0.695641,72"));
    // Pyth feed row: venue symbol is the market, market id the feed id.
    let publish0 = day_start;
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
    // drop counts (input hours actually dropped vs the 864-hour set), and the
    // provenance passthrough.
    let receipt_text =
        std::fs::read_to_string(receipt::receipt_path(&out, "mapping", "mapping")).unwrap();
    let receipt: Value = serde_json::from_str(&receipt_text).unwrap();
    assert_eq!(receipt["unit"], "mapping");
    assert_eq!(receipt["unit_version"], 1);
    assert_eq!(receipt["rows"], 30);
    assert_eq!(receipt["lockbox_hours_dropped"], 3);
    assert_eq!(receipt["lockbox_hours_excluded"], 864);
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
    assert_eq!(receipt["inputs"]["hl_fills"]["hours"], 75);
    assert_eq!(receipt["inputs"]["hl_fills"]["lockbox_hours_dropped"], 3);
    assert_eq!(
        receipt["inputs"]["phoenix_fills"]["lockbox_hours_dropped"],
        3
    );
    assert_eq!(receipt["inputs"]["pyth_prices"]["lockbox_hours_dropped"], 0);
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
    assert_eq!(catalog["watermarks"]["mapping"]["lockbox_hours_dropped"], 3);
    assert_eq!(
        catalog["watermarks"]["mapping"]["lockbox_hours_excluded"],
        864
    );
}

#[test]
fn the_hl_price_scan_reads_only_check_day_files() {
    let tmp = Tmp::new("hlfiles");
    let fixtures = build_fixtures(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    let check_days: Vec<chrono::NaiveDate> = CHECK_DAYS
        .split(',')
        .map(|day| super::grid::parse_date(day).unwrap())
        .collect();
    let ranges = super::lockbox::tests::v2_ranges_for_test();
    let hl = super::mapping::HlInput::scan(&conn, &fixtures.hl, &check_days, &ranges).unwrap();
    let window_start =
        super::grid::day_start_secs(chrono::NaiveDate::from_ymd_opt(2026, 10, 15).unwrap());
    let window_end = window_start + 3 * 86_400;
    // The watermark holds 72 check-day hours plus three lockbox-week hours; the
    // price scan must read only the check-day files, so SOL has exactly the
    // check-day hours and the lockbox-only coin has no prices at all.
    assert_eq!(hl.prices.get("SOL").map(BTreeMap::len), Some(72));
    assert!(
        hl.prices
            .get("SOL")
            .unwrap()
            .keys()
            .all(|hour| *hour >= window_start && *hour < window_end)
    );
    assert!(
        !hl.prices.contains_key("LOCKCOIN"),
        "lockbox-only coin has no prices"
    );
}

#[test]
fn the_phoenix_price_window_excludes_out_of_window_fills() {
    let tmp = Tmp::new("phxwindow");
    let fixtures = build_fixtures(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    let files = super::mapping::walk_parquet(&fixtures.phoenix).unwrap();
    let check_days: Vec<chrono::NaiveDate> = CHECK_DAYS
        .split(',')
        .map(|day| super::grid::parse_date(day).unwrap())
        .collect();
    let ranges = super::lockbox::tests::v2_ranges_for_test();
    let phoenix =
        super::mapping::scan_phoenix(&conn, &fixtures.phoenix, &files, &check_days, &ranges);
    let phoenix = phoenix.unwrap();
    // PHOENIXONLY has check-day fills and 2026-10-20 fills; only the check days
    // may reach the price map.
    let phx = phoenix.prices.get("PHOENIXONLY").unwrap();
    let window_start =
        super::grid::day_start_secs(chrono::NaiveDate::from_ymd_opt(2026, 10, 15).unwrap());
    let window_end = window_start + 3 * 86_400;
    assert_eq!(phx.len(), 72, "exactly the check-day hours");
    assert!(
        phx.keys()
            .all(|hour| *hour >= window_start && *hour < window_end)
    );
    // The SOL lockbox-week fills stay out of the price map too.
    assert_eq!(phoenix.prices.get("SOL").unwrap().len(), 72);
    // And out of the universe span: SOL's first fill second is the check day.
    assert_eq!(
        phoenix.universe.get("SOL").unwrap().first_ts,
        window_start + 120
    );
}

#[test]
fn a_failed_output_write_leaves_no_receipt() {
    let tmp = Tmp::new("fsync-assets");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    crate::fsutil::inject_fsync_eio(FsyncStep::TempFile);
    let error = mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS));
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the injected fsync failure must fail the run"
    );
    // The fault must have been consumed by the assets.csv temp-file fsync: the
    // last recorded fsync is that step, the file itself never landed, and with
    // a plain fs::write the fault would instead fire on the receipt write and
    // leave the output behind.
    assert_eq!(
        crate::fsutil::last_fsync_step(),
        Some(FsyncStep::TempFile),
        "the fault hit the output's temp-file fsync"
    );
    let synced = crate::fsutil::last_fsync_path().unwrap_or_default();
    assert!(
        synced.to_string_lossy().ends_with("assets.csv.tmp"),
        "fsynced {synced:?}"
    );
    assert!(!out.join("mapping/assets.csv").exists(), "no sealed output");
    assert!(!receipt::receipt_path(&out, "mapping", "mapping").exists());
    assert!(!out.join("catalog.json").exists());
    // A rerun without the fault succeeds and writes everything, and the failed
    // attempt's temp file is gone (the rename consumed it).
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    assert!(receipt::receipt_path(&out, "mapping", "mapping").exists());
    assert!(out.join("mapping/assets.csv").exists());
    assert!(
        !out.join("mapping/assets.csv.tmp").exists(),
        "no temp file left behind"
    );
}

#[test]
fn a_failed_receipt_write_leaves_the_output_but_no_receipt() {
    let tmp = Tmp::new("fsync-receipt");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    receipt::on_before_write(|| crate::fsutil::inject_fsync_eio(FsyncStep::TempFile));
    let error = mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS));
    receipt::clear_before_write();
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the faulted receipt write must fail the run"
    );
    assert!(
        out.join("mapping/assets.csv").exists(),
        "the output is durable"
    );
    assert!(!receipt::receipt_path(&out, "mapping", "mapping").exists());
    assert!(
        !out.join("catalog.json").exists(),
        "no catalog after a failed receipt"
    );
}

#[test]
fn a_failed_catalog_write_leaves_output_and_receipt() {
    let tmp = Tmp::new("fsync-catalog");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    catalog_seam_arm();
    let error = mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS));
    catalog_seam_clear();
    crate::fsutil::clear_fsync_failure();
    assert!(
        error.is_err(),
        "the faulted catalog write must fail the run"
    );
    assert!(out.join("mapping/assets.csv").exists());
    assert!(receipt::receipt_path(&out, "mapping", "mapping").exists());
    assert!(!out.join("catalog.json").exists());
}

/// Corrupt the sealed file after the durable write but keep the header, so
/// only the read-back row count can see this.
fn corrupt_sealed_output(path: &Path) {
    let header = "asset_id,venue,venue_symbol,builder,coin_class,venue_market_id,first_ts,last_ts,source_of_truth,mapped,reason,median_abs_log_ratio,n_hours";
    std::fs::write(path, format!("{header}\nX\n")).unwrap();
}

fn catalog_seam_arm() {
    crate::augment::combined::catalog::on_before_save(|| {
        crate::fsutil::inject_fsync_eio(FsyncStep::TempFile);
    });
}

fn catalog_seam_clear() {
    crate::augment::combined::catalog::clear_before_save();
}

#[test]
fn a_corrupt_output_fails_the_read_back_check() {
    let tmp = Tmp::new("readback");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    mapping::on_before_verify_for_tests(corrupt_sealed_output);
    let error = mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS));
    mapping::clear_before_verify_for_tests();
    let error = error.unwrap_err();
    assert!(error.to_string().contains("read-back"), "{error}");
    assert!(!receipt::receipt_path(&out, "mapping", "mapping").exists());
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

#[test]
fn an_hl_coin_with_two_classes_refuses() {
    let tmp = Tmp::new("twoclass");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    // Overwrite one receipted hour with a file that carries SOL both as perp
    // and as spot: the universe cannot split a coin's identity.
    let (base_hour, _) = hour_bases();
    let dt = chrono::DateTime::from_timestamp(base_hour * 3600, 0).unwrap();
    let path = fixtures.hl.join(format!(
        "train/date={}/hour={}-{}.parquet",
        dt.format("%Y-%m-%d"),
        dt.format("%Y%m%d"),
        dt.format("%H")
    ));
    let conn = memory_connection("1", "512MB").unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT 'SOL' AS coin, 'perp' AS coin_class, {} AS time_ms, 1 AS tid, 100.0 AS px
               UNION ALL
               SELECT 'SOL', 'spot', {}, 2, 100.0) TO {} (FORMAT PARQUET)",
        base_hour * 3_600_000 + 60_000,
        base_hour * 3_600_000 + 61_000,
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
    let error =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap_err();
    assert!(error.to_string().contains("two classes"), "{error}");
}

#[test]
fn a_wrong_watermark_schema_refuses() {
    let tmp = Tmp::new("wmschema");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let watermark: Value =
        serde_json::from_str(&std::fs::read_to_string(fixtures.hl.join("watermark.json")).unwrap())
            .unwrap();
    let mut wrong = watermark.clone();
    wrong["schema"] = serde_json::json!("hl-fills-v2");
    std::fs::write(
        fixtures.hl.join("watermark.json"),
        serde_json::to_vec(&wrong).unwrap(),
    )
    .unwrap();
    let error =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap_err();
    assert!(error.to_string().contains("schema"), "{error}");
}

#[test]
fn an_empty_phoenix_dir_refuses() {
    let tmp = Tmp::new("emptyphx");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let empty = tmp.dir().join("no-phoenix");
    std::fs::create_dir_all(&empty).unwrap();
    let mut req = request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS);
    req.phoenix_fills = &empty;
    let error = mapping::run(&req).unwrap_err();
    // The exact guard text: a later "no parquet files to read" from the query
    // layer must not satisfy this test.
    assert_eq!(
        error.to_string(),
        "--phoenix-fills has no parquet files",
        "{error}"
    );
}

#[test]
fn venues_config_without_pyth_feeds_refuses() {
    let tmp = Tmp::new("nopyth");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let path = tmp.dir().join("venues-nopyth.json");
    std::fs::write(&path, r#"{"venues": [], "pyth": {"feeds": []}}"#).unwrap();
    let mut req = request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS);
    req.venues_config = &path;
    let error = mapping::run(&req).unwrap_err();
    assert!(error.to_string().contains("no pyth feeds"), "{error}");
}

#[test]
fn non_object_provenance_refuses() {
    let tmp = Tmp::new("prov");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let path = tmp.dir().join("provenance-array.json");
    std::fs::write(&path, "[1,2,3]").unwrap();
    let mut req = request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS);
    req.provenance = &path;
    let error = mapping::run(&req).unwrap_err();
    assert!(error.to_string().contains("JSON object"), "{error}");
}

#[test]
fn an_ambiguous_phoenix_market_id_is_left_empty() {
    let tmp = Tmp::new("marketid");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    // A second Phoenix file gives SOL a different numeric market id.
    let conn = memory_connection("1", "512MB").unwrap();
    let extra = fixtures.phoenix.join("extra.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT 'SOL' AS asset_symbol, 1792022461 AS block_time, 10000 AS price_ticks,
               100 AS tick_size, 2 AS base_lot_decimals, 6 AS quote_lot_decimals,
               999 AS asset_id) TO {} (FORMAT PARQUET)",
        sql_string(&extra.to_string_lossy())
    ))
    .unwrap();
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let sol = line_of(&assets_lines(&out), "phoenix", "SOL");
    assert!(sol.contains(",spot,,"), "market id empty: {sol}");
}
