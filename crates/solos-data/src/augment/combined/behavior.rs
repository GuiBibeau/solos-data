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
        ("GHOST".into(), "perp".into(), 100.0, 100.0, 72),
        ("SEEDED".into(), "perp".into(), 100.0, 100.0, 72),
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
    ("SEEDED", 10_000, 72),
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
    // must keep them out of first_ts/last_ts and out of the price check. The
    // same hours carry LOCKCOIN and LOCKCOIN2, whose whole history lies inside
    // the lockbox: they have no row left after the drop, must not appear in
    // the mapping at all, and are counted as lockbox-only (never named).
    for offset in 1..=3 {
        write_hl_hour(
            &conn,
            june_hour + offset,
            &mut watermark_hours,
            &vec![
                ("SOL".to_owned(), "perp".to_owned(), 111.0),
                ("LOCKCOIN".to_owned(), "perp".to_owned(), 7.0),
                ("LOCKCOIN2".to_owned(), "perp".to_owned(), 8.0),
            ],
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
    // SOL fills inside the lockbox week (price 111.0 = 11_100 ticks), at the
    // three shared locked hours and at a fourth one only Phoenix covers, plus
    // PHOENIXLOCK fills whose whole history lies inside it: counted as
    // lockbox-only, never named. The extra hour is what makes the store-wide
    // dropped-hours union strictly larger than any single venue's set.
    for offset in 1..=3 {
        values.push(format!(
            "({}, {}, 11_100, 100, 2, 6, {})",
            sql_string("SOL"),
            (june_hour + offset) * 3600 + 120,
            market_of("SOL")
        ));
        values.push(format!(
            "({}, {}, 6_100, 100, 2, 6, {})",
            sql_string("PHOENIXLOCK"),
            (june_hour + offset) * 3600 + 120,
            market_of("PHOENIXLOCK")
        ));
    }
    values.push(format!(
        "({}, {}, 11_100, 100, 2, 6, {})",
        sql_string("SOL"),
        (june_hour + 5) * 3600 + 120,
        market_of("SOL")
    ));
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

    // Pyth prices: two directories, one file each, plus LOCKFEED, LOCKFEED2
    // and LOCKFEED3 publishes whose whole history lies inside the lockbox
    // week (counted as lockbox-only, never named), and SOL publishes inside
    // the lockbox week: SOL also publishes on the check days, so its span
    // must stay put when the lockbox filter hides the June hours. The first
    // directory covers the shared locked hours plus one hour only Pyth dir 0
    // sees, the second an hour only dir 1 sees: the venue's covered-hour set
    // is the union over the directories, never a per-directory max.
    let pyth = dir.join("pyth-prices");
    std::fs::create_dir_all(&pyth).unwrap();
    let pyth_path = pyth.join("prices.parquet");
    let first_publish = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    let mut pyth_rows = vec![
        format!("SELECT 'SOL' AS symbol, {first_publish} AS publish_time"),
        format!(
            "SELECT 'BTC' AS symbol, {} AS publish_time",
            first_publish + 3600
        ),
    ];
    for offset in 1..=3 {
        pyth_rows.push(format!(
            "SELECT 'SOL' AS symbol, {} AS publish_time",
            (june_hour + offset) * 3600
        ));
        for feed in ["LOCKFEED", "LOCKFEED2", "LOCKFEED3"] {
            pyth_rows.push(format!(
                "SELECT {} AS symbol, {} AS publish_time",
                sql_string(feed),
                (june_hour + offset) * 3600
            ));
        }
    }
    pyth_rows.push(format!(
        "SELECT 'SOL' AS symbol, {} AS publish_time",
        (june_hour + 7) * 3600
    ));
    conn.execute_batch(&format!(
        "COPY ({}) TO {} (FORMAT PARQUET)",
        pyth_rows.join(" UNION ALL "),
        sql_string(&pyth_path.to_string_lossy())
    ))
    .unwrap();
    let pyth2 = dir.join("pyth-prices2");
    std::fs::create_dir_all(&pyth2).unwrap();
    let pyth2_path = pyth2.join("prices.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT 'SOL' AS symbol, {} AS publish_time) TO {} (FORMAT PARQUET)",
        (june_hour + 9) * 3600,
        sql_string(&pyth2_path.to_string_lossy())
    ))
    .unwrap();

    // asset_contexts: the seed that renames RENAMED to OLDNAME, plus seeds
    // that miss Phoenix entirely: GHOST's seed names an asset Phoenix never
    // had and GHOST itself is not on Phoenix either (no candidate at all);
    // SEEDED's seed misses too, but SEEDED trades on Phoenix under its own
    // base name (the pair must fall back to the base-name seed).
    let contexts = dir.join("asset_contexts");
    std::fs::create_dir_all(&contexts).unwrap();
    let contexts_path = contexts.join("2026-10-17.parquet");
    conn.execute_batch(&format!(
        "COPY (SELECT 'RENAMED' AS symbol, 'OLDNAME' AS phoenix_symbol UNION ALL
               SELECT 'SOL' AS symbol, 'SOL' AS phoenix_symbol UNION ALL
               SELECT 'GHOST' AS symbol, 'NOSUCHPHX' AS phoenix_symbol UNION ALL
               SELECT 'SEEDED' AS symbol, 'NOTINPHX' AS phoenix_symbol) TO {} (FORMAT PARQUET)",
        sql_string(&contexts_path.to_string_lossy())
    ))
    .unwrap();

    let (lockbox_path, lockbox_sha) = lockbox_file(dir);
    Fixtures {
        hl,
        phoenix,
        pyth_dirs: vec![pyth, pyth2],
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
    // 17 HL + 13 phoenix + 2 pyth + 1 jup; the flash pool is skipped and
    // LOCKCOIN/LOCKCOIN2 (lockbox-only) have no row at all.
    assert_eq!(report.rows, 33);
    assert_eq!(report.per_venue["hl"], 17);
    assert_eq!(report.per_venue["phoenix"], 13);
    assert_eq!(report.per_venue["pyth"], 2);
    assert_eq!(report.per_venue["jup"], 1);
    assert_eq!(report.pairs_checked, 13);
    assert_eq!(report.pairs_mapped, 7);
    assert_eq!(report.price_divergence, 4);
    assert_eq!(report.thin, 2);
    assert_eq!(report.spot_unnamed, 1);
    assert_eq!(report.outcome, 1);
    // The store-wide drop is the union of the venues' covered-hour sets: HL
    // covers june+1..=3, Phoenix adds june+5, Pyth adds june+7 (dir 0) and
    // june+9 (dir 1): six distinct hours, one more than any venue's own set
    // (a max across venues would report Pyth's 5).
    assert_eq!(report.lockbox_hours_dropped, 6);
    assert_eq!(report.lockbox_hours_excluded, 864);

    let lines = assets_lines(&out);
    assert_eq!(lines.len(), 34, "header plus 33 rows");
    let (day_start, first, last, phoenix_last) = check_bounds();
    // The sort the ADR states: venue, then venue symbol.
    assert_eq!(
        lines[0],
        "asset_id,venue,venue_symbol,builder,coin_class,venue_market_id,first_ts,last_ts,source_of_truth,mapped,reason,median_abs_log_ratio,n_hours"
    );
    assert!(lines[1].starts_with(",hl,#9,"), "outcomes sort first");
    assert!(lines[2].starts_with(",hl,@1,,spot,"));
    assert!(
        lines[18].starts_with("SOL,jup,"),
        "jup follows the hl block"
    );
    assert!(lines[19].starts_with("CL,phoenix,"));
    assert!(lines[32].starts_with("BTC,pyth,"), "pyth closes the file");

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
    assert_eq!(receipt["rows"], 33);
    assert_eq!(receipt["lockbox_hours_dropped"], 6);
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
        4
    );
    // Pyth's covered hours are the union over its directories: the shared
    // three plus june+7 (dir 0) and june+9 (dir 1).
    assert_eq!(receipt["inputs"]["pyth_prices"]["lockbox_hours_dropped"], 5);
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
    assert_eq!(catalog["watermarks"]["mapping"]["lockbox_hours_dropped"], 6);
    assert_eq!(
        catalog["watermarks"]["mapping"]["lockbox_hours_excluded"],
        864
    );
    assert_eq!(
        catalog["watermarks"]["mapping"]["lockbox_only_symbols"],
        serde_json::json!({"hl": 2, "phoenix": 1, "pyth": 3})
    );
}

/// Symbols whose whole history lies inside the lockbox have no row in any
/// output table, so the per-venue counts are the only trace they leave. The
/// fixture carries two such HL coins, one Phoenix asset and three Pyth feeds,
/// so the counts are nonzero and distinct per venue (2, 1, 3: no two fields
/// can be swapped without this test going red), and the report, the receipt
/// inputs and the catalog watermark all agree on them. Counts only, no names.
#[test]
fn lockbox_only_symbols_are_counted_per_venue() {
    let tmp = Tmp::new("lockonly");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let report =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    assert_eq!(report.lockbox_only_symbols["hl"], 2);
    assert_eq!(report.lockbox_only_symbols["phoenix"], 1);
    assert_eq!(report.lockbox_only_symbols["pyth"], 3);
    let report_json: Value = serde_json::from_str(&report.to_json()).unwrap();
    assert_eq!(
        report_json["lockboxOnlySymbols"],
        serde_json::json!({"hl": 2, "phoenix": 1, "pyth": 3})
    );
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(&out, "mapping", "mapping")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["inputs"]["hl_fills"]["lockbox_only_symbols"], 2);
    assert_eq!(
        receipt["inputs"]["phoenix_fills"]["lockbox_only_symbols"],
        1
    );
    assert_eq!(receipt["inputs"]["pyth_prices"]["lockbox_only_symbols"], 3);
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(
        catalog["watermarks"]["mapping"]["lockbox_only_symbols"],
        serde_json::json!({"hl": 2, "phoenix": 1, "pyth": 3})
    );
    // Counts only: the dropped symbols are named nowhere in the outputs.
    let csv = std::fs::read_to_string(out.join("mapping/assets.csv")).unwrap();
    for name in ["LOCKCOIN", "LOCKCOIN2", "PHOENIXLOCK", "LOCKFEED"] {
        assert!(!csv.contains(name), "{name} must not be named in the CSV");
        assert!(!receipt.to_string().contains(name), "{name} in receipt");
        assert!(!catalog.to_string().contains(name), "{name} in catalog");
    }
}

/// The Pyth span filter is load-bearing: SOL publishes on the check days and
/// again inside the lockbox week, so replacing the filtered min/max with a
/// plain min/max (mutant N3) would drag SOL's first_ts back into June.
#[test]
fn the_pyth_span_excludes_lockbox_hours() {
    let tmp = Tmp::new("pythspan");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let lines = assets_lines(&out);
    let publish0 = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    let sol_feed = lines
        .iter()
        .find(|line| line.contains(",pyth,SOL/USD,"))
        .expect("SOL feed row");
    assert_eq!(
        sol_feed,
        &format!("SOL,pyth,SOL/USD,,price,feed-sol,{publish0},{publish0},venues_config,true,,,")
    );
    // The lockbox-only feeds stay out of the output entirely: the config's
    // feeds are the only Pyth rows.
    assert!(!lines.iter().any(|line| line.contains("LOCKFEED")));
}

/// The seed only counts when the seeded asset actually trades on Phoenix
/// (mutant M20 forces that universe check to true). GHOST's seed names an
/// asset Phoenix never had and GHOST's base name is not on Phoenix either, so
/// the pair must be dropped entirely: no seed, no check, no reason. SEEDED's
/// seed misses Phoenix too, but SEEDED trades there under its own base name,
/// so the pair falls back to the base-name seed and maps.
#[test]
fn a_seeded_phoenix_symbol_missing_from_phoenix_is_not_a_candidate() {
    let tmp = Tmp::new("ghostseed");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let lines = assets_lines(&out);
    let (_, first, last, _) = check_bounds();
    assert_eq!(
        line_of(&lines, "hl", "GHOST"),
        format!("GHOST,hl,GHOST,,perp,,{first},{last},,false,,,")
    );
    assert_eq!(
        line_of(&lines, "hl", "SEEDED"),
        format!("SEEDED,hl,SEEDED,,perp,,{first},{last},base_name,true,,0.000000,72")
    );
    assert!(
        line_of(&lines, "phoenix", "SEEDED").ends_with(",base_name,true,,0.000000,72"),
        "the Phoenix side keeps the base-name pair outcome"
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

/// The shared lockbox fixture with real data inside it: a Phoenix fill on a
/// lockbox day (the fixture's mid hour) stays invisible in the unit's own
/// outputs, and is counted in the receipt's lockbox numbers. This is the U1
/// path of the fixture every U2 unit's tests reuse (`combined::testfix`),
/// asserted on the production output (the written assets.csv and the unit
/// receipt), not on a table the test filtered itself.
#[test]
fn a_fill_inside_the_lockbox_is_excluded_and_counted() {
    let tmp = Tmp::new("testfix");
    let fixtures = build_fixtures(tmp.dir());
    let fix = super::testfix::v2_lockbox(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    let phoenix = tmp.dir().join("phoenix-fills-single");
    std::fs::create_dir_all(&phoenix).unwrap();
    let path = phoenix.join("fills.parquet");
    let check_day = super::grid::day_start_secs(super::grid::parse_date("2026-10-15").unwrap());
    // SOL fills on every check-day hour and on the lockbox day (the fixture's
    // mid hour); LOCKED fills only on the lockbox day (the next second, same
    // hour), so it is lockbox-only.
    let mut values: Vec<String> = (0..72)
        .map(|offset| {
            format!(
                "('SOL', {}, 10_000, 100, 2, 6, 45)",
                check_day + offset * 3600 + 120
            )
        })
        .collect();
    values.push(format!(
        "('SOL', {}, 11_100, 100, 2, 6, 45)",
        fix.secs(fix.mid_hour()) + 120
    ));
    values.push(format!(
        "('LOCKED', {}, 6_100, 100, 2, 6, 77)",
        fix.secs(fix.mid_hour()) + 121
    ));
    conn.execute_batch(&format!(
        "CREATE TABLE pf(asset_symbol VARCHAR, block_time BIGINT, price_ticks UBIGINT,
                        tick_size UBIGINT, base_lot_decimals SMALLINT, quote_lot_decimals SMALLINT,
                        asset_id UBIGINT);
         INSERT INTO pf VALUES {};
         COPY pf TO {} (FORMAT PARQUET);",
        values.join(", "),
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
    let fixtures = Fixtures {
        phoenix,
        ..fixtures
    };
    let out = tmp.dir().join("store");
    let report =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    // The production receipt: Phoenix dropped exactly the mid hour, and the
    // lockbox-only symbol is counted (never named).
    let receipt_text =
        std::fs::read_to_string(receipt::receipt_path(&out, "mapping", "mapping")).unwrap();
    let receipt: Value = serde_json::from_str(&receipt_text).unwrap();
    assert_eq!(
        receipt["inputs"]["phoenix_fills"]["lockbox_hours_dropped"],
        1
    );
    assert_eq!(
        receipt["inputs"]["phoenix_fills"]["lockbox_only_symbols"],
        1
    );
    assert_eq!(report.lockbox_only_symbols["phoenix"], 1);
    // The production assets.csv: LOCKED, whose every fill sits inside the
    // lockbox, has no row at all, and SOL's Phoenix span starts on the check
    // day: the mid-hour fill never touched first_ts.
    let lines = assets_lines(&out);
    assert!(
        !lines.iter().any(|line| line.contains("LOCKED")),
        "a lockbox-only symbol has no row in the written assets.csv"
    );
    assert_eq!(
        line_of(&lines, "phoenix", "SOL"),
        format!(
            "SOL,phoenix,SOL,,spot,45,{},{},asset_contexts,true,,0.000000,72",
            check_day + 120,
            check_day + 71 * 3600 + 120
        )
    );
}

/// The store-wide lockbox drop is the union of the venues' covered-hour sets
/// (R-union: a max across venues undercounts). The fixture gives each venue
/// a covered hour the others miss, so the union (six hours) is strictly
/// larger than every venue's own set and than the max (Pyth's five).
#[test]
fn the_store_wide_drop_is_the_union_of_the_venue_hours() {
    let tmp = Tmp::new("union");
    let fixtures = build_fixtures(tmp.dir());
    let out = tmp.dir().join("store");
    let report =
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(receipt::receipt_path(&out, "mapping", "mapping")).unwrap(),
    )
    .unwrap();
    let hl = receipt["inputs"]["hl_fills"]["lockbox_hours_dropped"]
        .as_u64()
        .unwrap();
    let phoenix = receipt["inputs"]["phoenix_fills"]["lockbox_hours_dropped"]
        .as_u64()
        .unwrap();
    let pyth = receipt["inputs"]["pyth_prices"]["lockbox_hours_dropped"]
        .as_u64()
        .unwrap();
    // Each venue covers a different extra hour, so the max (5) cannot be the
    // union (6): 3 + {+5} + {+7} + {+9}.
    assert_eq!((hl, phoenix, pyth), (3, 4, 5));
    assert_eq!(report.lockbox_hours_dropped, 6);
    assert!(report.lockbox_hours_dropped > hl.max(phoenix).max(pyth));
}

/// The lockbox filter is load-bearing on both ends of the span (a #111
/// review survivor): every venue input holds a symbol with two free fills on
/// a day between two v2 exclusion windows, a fill inside the set strictly
/// BEFORE the free fills (the first window's opening hour) and one strictly
/// AFTER them (the second window's opening hour). The span's first_ts and
/// last_ts must come from the free fills: a scan with the `CASE WHEN free`
/// filter dropped on the min side would take first_ts from the early lockbox
/// row, one dropped on the max side would take last_ts from the late row,
/// and a dropped-hours query without its `NOT (free)` guard would take the
/// hour set from the free rows - all go red. The free-side Parquet files are
/// asserted lockbox-clean through the shared testfix helper, so the fixture
/// itself proves the span has no lockbox row to take.
#[test]
fn a_lockbox_fill_outside_the_free_fills_never_becomes_the_span() {
    let tmp = Tmp::new("spanlock");
    let fix = super::testfix::v2_lockbox(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    // Free fills on 2026-06-15 (between the early_stress and stress guard
    // windows, outside every v2 range); lockbox fills inside the first
    // window's opening hour and the second window's opening hour.
    let free_day = super::grid::day_start_secs(super::grid::parse_date("2026-06-15").unwrap());
    let first_free = free_day + 10 * 3600 + 60;
    let last_free = free_day + 11 * 3600 + 60;
    let early = fix.ranges[0].0 * 3600 + 120;
    let late = fix.ranges[1].0 * 3600 + 120;
    assert!(
        early < first_free,
        "the early lockbox fill must precede the free fills"
    );
    assert!(
        late > last_free,
        "the late lockbox fill must follow the free fills"
    );
    assert!(
        fix.hours.contains(&(early / 3600)) && fix.hours.contains(&(late / 3600)),
        "both lockbox fills must sit inside the set"
    );
    assert!(!fix.hours.contains(&(first_free / 3600)));
    let check_day = super::grid::parse_date("2026-06-15").unwrap();

    // HL: one hour file per fill, all four receipted in the watermark.
    let hl = tmp.dir().join("hl-fills-span");
    let mut watermark_hours = Vec::new();
    let mut hl_free_files = Vec::new();
    for (secs, price) in [
        (early, 99.0),
        (first_free, 100.0),
        (last_free, 101.0),
        (late, 111.0),
    ] {
        let hour = super::grid::hour_of_secs(secs);
        let dt = chrono::DateTime::from_timestamp(hour * 3600, 0).unwrap();
        let ymd = dt.format("%Y%m%d");
        watermark_hours.push(format!("{ymd}/{}", dt.format("%H")));
        let dir_path = hl.join(format!("train/date={}", dt.format("%Y-%m-%d")));
        std::fs::create_dir_all(&dir_path).unwrap();
        let path = dir_path.join(format!("hour={ymd}-{}.parquet", dt.format("%H")));
        conn.execute_batch(&format!(
            "COPY (SELECT 'MAXSOL' AS coin, 'perp' AS coin_class, {} AS time_ms, 1 AS tid,
                    CAST({price} AS DOUBLE) AS px) TO {} (FORMAT PARQUET)",
            secs * 1000,
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
        if secs == first_free || secs == last_free {
            hl_free_files.push(path);
        }
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
    let hl_scan = super::mapping::HlInput::scan(&conn, &hl, &[check_day], &fix.ranges).unwrap();
    let (_, span) = &hl_scan.universe["MAXSOL"];
    assert_eq!((span.first_ts, span.last_ts), (first_free, last_free));

    // Phoenix: free fills in one file, the two lockbox fills in another -
    // both scanned together, like one lane split across files.
    let phoenix = tmp.dir().join("phoenix-fills-span");
    std::fs::create_dir_all(&phoenix).unwrap();
    let phoenix_free = phoenix.join("free.parquet");
    let phoenix_locked = phoenix.join("locked.parquet");
    let phoenix_row = |secs: i64| {
        format!(
            "SELECT 'PHXMAX' AS asset_symbol, {secs} AS block_time, 10_000 AS price_ticks,
                    100 AS tick_size, 2 AS base_lot_decimals, 6 AS quote_lot_decimals,
                    91 AS asset_id"
        )
    };
    conn.execute_batch(&format!(
        "COPY ({} UNION ALL {}) TO {} (FORMAT PARQUET);
         COPY ({} UNION ALL {}) TO {} (FORMAT PARQUET);",
        phoenix_row(first_free),
        phoenix_row(last_free),
        sql_string(&phoenix_free.to_string_lossy()),
        phoenix_row(early),
        phoenix_row(late),
        sql_string(&phoenix_locked.to_string_lossy()),
    ))
    .unwrap();
    let phoenix_scan = super::mapping::scan_phoenix(
        &conn,
        &phoenix,
        &super::mapping::walk_parquet(&phoenix).unwrap(),
        &[check_day],
        &fix.ranges,
    )
    .unwrap();
    assert_eq!(phoenix_scan.universe["PHXMAX"].first_ts, first_free);
    assert_eq!(phoenix_scan.universe["PHXMAX"].last_ts, last_free);
    assert_eq!(
        phoenix_scan.lockbox_dropped_hours,
        std::collections::BTreeSet::from([early / 3600, late / 3600])
    );
    assert_eq!(phoenix_scan.lockbox_only_symbols, 0);

    // Pyth: the same shape over publish_time.
    let pyth = tmp.dir().join("pyth-prices-span");
    std::fs::create_dir_all(&pyth).unwrap();
    let pyth_free = pyth.join("free.parquet");
    let pyth_locked = pyth.join("locked.parquet");
    let pyth_row = |secs: i64| format!("SELECT 'PYXMAX' AS symbol, {secs} AS publish_time");
    conn.execute_batch(&format!(
        "COPY ({} UNION ALL {}) TO {} (FORMAT PARQUET);
         COPY ({} UNION ALL {}) TO {} (FORMAT PARQUET);",
        pyth_row(first_free),
        pyth_row(last_free),
        sql_string(&pyth_free.to_string_lossy()),
        pyth_row(early),
        pyth_row(late),
        sql_string(&pyth_locked.to_string_lossy()),
    ))
    .unwrap();
    let (_, pyth_scan) = super::mapping::scan_pyth(&conn, &[pyth], &fix.ranges).unwrap();
    assert_eq!(pyth_scan.universe["PYXMAX"].first_ts, first_free);
    assert_eq!(pyth_scan.universe["PYXMAX"].last_ts, last_free);
    assert_eq!(
        pyth_scan.lockbox_dropped_hours,
        std::collections::BTreeSet::from([early / 3600, late / 3600])
    );
    assert_eq!(pyth_scan.lockbox_only_symbols, 0);

    // The free-side files are lockbox-clean: the fixture pins where the span
    // can come from, and the scans above prove the filters keep it there.
    for path in &hl_free_files {
        super::testfix::assert_no_parquet_rows_in_lockbox(
            &conn,
            path,
            "time_ms",
            super::testfix::TsUnit::Millis,
            &fix.hours,
        );
    }
    super::testfix::assert_no_parquet_rows_in_lockbox(
        &conn,
        &phoenix_free,
        "block_time",
        super::testfix::TsUnit::Secs,
        &fix.hours,
    );
    super::testfix::assert_no_parquet_rows_in_lockbox(
        &conn,
        &pyth_free,
        "publish_time",
        super::testfix::TsUnit::Secs,
        &fix.hours,
    );
}

/// HL hours exactly at the lockbox edges (R23), through U1's HL path: a
/// watermark hour just inside the set's first or last hour is counted, a
/// watermark hour just outside it is never counted. The test runs twice over
/// the same tree with different watermarks, because a pure count cannot
/// separate the edges when the hours either side are receipted together (a
/// +-1 boundary shift moves one hour in and another out, leaving the count
/// equal): run A receipts the two inside hours, run B the two outside ones,
/// so any shift changes a count.
#[test]
fn hl_lockbox_edge_hours_count_only_when_inside() {
    let tmp = Tmp::new("hledge");
    let fixtures = build_fixtures(tmp.dir());
    let fix = super::testfix::v2_lockbox(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    // Extend the fixture HL tree with the four edge hours; every hour carries
    // HLEDGE, the inside ones also LOCKEDGE, whose whole history lies inside
    // the set.
    let edge_hours = [
        fix.hour_before(),
        fix.first_hour(),
        fix.last_hour(),
        fix.hour_after(),
    ];
    let watermark_path = fixtures.hl.join("watermark.json");
    let mut watermark: Value =
        serde_json::from_str(&std::fs::read_to_string(&watermark_path).unwrap()).unwrap();
    let hours: Vec<String> = watermark["hours"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect();
    let mut edge_keys = BTreeMap::new();
    for hour in edge_hours {
        let start = hour * 3600;
        let dt = chrono::DateTime::from_timestamp(start, 0).unwrap();
        edge_keys.insert(hour, format!("{}/{}", dt.format("%Y%m%d"), dt.format("%H")));
        let dir_path = fixtures
            .hl
            .join(format!("train/date={}", dt.format("%Y-%m-%d")));
        std::fs::create_dir_all(&dir_path).unwrap();
        let path = dir_path.join(format!("hour={}.parquet", dt.format("%Y%m%d-%H")));
        // LOCKEDGE only fills the hours inside the set: it must be
        // lockbox-only, so the free edge hours carry HLEDGE alone.
        let inside = fix.hours.contains(&hour);
        let lockedge = if inside {
            format!(
                " UNION ALL SELECT 'LOCKEDGE', 'perp', {}, {hour}, CAST(7.0 AS DOUBLE)",
                start * 1000 + 120_000
            )
        } else {
            String::new()
        };
        conn.execute_batch(&format!(
            "COPY (
                SELECT 'HLEDGE' AS coin, 'perp' AS coin_class, {} AS time_ms, {hour} AS tid,
                       CAST(100.0 AS DOUBLE) AS px{lockedge}
            ) TO {} (FORMAT PARQUET)",
            start * 1000 + 60_000,
            sql_string(&path.to_string_lossy())
        ))
        .unwrap();
    }
    let mut run_with = |receipted: &[i64], label: &str| {
        let mut keys = hours.clone();
        for hour in receipted {
            keys.push(edge_keys[hour].clone());
        }
        keys.sort();
        watermark["hours"] = serde_json::json!(keys);
        std::fs::write(&watermark_path, serde_json::to_vec(&watermark).unwrap()).unwrap();
        let out = tmp.dir().join(format!("store-{label}"));
        mapping::run(&request(&out, &fixtures, &fixtures.lockbox_sha, CHECK_DAYS)).unwrap();
        let receipt: Value = serde_json::from_str(
            &std::fs::read_to_string(receipt::receipt_path(&out, "mapping", "mapping")).unwrap(),
        )
        .unwrap();
        (out, receipt)
    };
    // Run A: the two inside edge hours are receipted; both count, on top of
    // the fixture's three shared June hours (a boundary +-1 mutant drops one
    // of them or sweeps in an outside hour, and the count moves).
    let (out_a, receipt_a) = run_with(&[fix.first_hour(), fix.last_hour()], "a");
    assert_eq!(receipt_a["inputs"]["hl_fills"]["lockbox_hours_dropped"], 5);
    // LOCKEDGE is lockbox-only on top of the fixture's LOCKCOIN/LOCKCOIN2,
    // and HLEDGE, whose receipted fills all sit inside the set, joins them.
    assert_eq!(receipt_a["inputs"]["hl_fills"]["lockbox_only_symbols"], 4);
    let lines_a = assets_lines(&out_a);
    assert!(!lines_a.iter().any(|line| line.contains("LOCKEDGE")));
    assert!(!lines_a.iter().any(|line| line.contains(",hl,HLEDGE,")));
    // Run B: the two outside edge hours are receipted; neither counts (the
    // store-wide number stays the fixture's own), and HLEDGE keeps the free
    // span its two outside fills give it.
    let (out_b, receipt_b) = run_with(&[fix.hour_before(), fix.hour_after()], "b");
    assert_eq!(receipt_b["inputs"]["hl_fills"]["lockbox_hours_dropped"], 3);
    assert_eq!(receipt_b["inputs"]["hl_fills"]["lockbox_only_symbols"], 2);
    let lines_b = assets_lines(&out_b);
    assert!(!lines_b.iter().any(|line| line.contains("LOCKEDGE")));
    let edge = lines_b
        .iter()
        .find(|line| line.contains(",hl,HLEDGE,"))
        .expect("HLEDGE row");
    assert_eq!(
        edge,
        &format!(
            "HLEDGE,hl,HLEDGE,,perp,,{},{},,false,,,",
            fix.secs(fix.hour_before()) + 60,
            fix.secs(fix.hour_after()) + 60
        )
    );
}

/// Phoenix hour bucketing at the lockbox edges (R26): the bucket is
/// `floor(block_time / 3600)`, so a fill at the last second of the free hour
/// before the set stays out, a fill at the first second of the first inside
/// hour is counted, the last second of the last inside hour is counted, and
/// the first second after the set is out again. A ceil bucketing or a
/// range off by one changes the counted set.
#[test]
fn the_phoenix_hour_bucket_counts_edge_fills_exactly() {
    let tmp = Tmp::new("phxedge");
    let fix = super::testfix::v2_lockbox(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    let phoenix = tmp.dir().join("phoenix-fills-edges");
    std::fs::create_dir_all(&phoenix).unwrap();
    let path = phoenix.join("fills.parquet");
    let last_second = |hour: i64| fix.secs(hour) + 3_599;
    conn.execute_batch(&format!(
        "COPY (
            SELECT 'PHX' AS asset_symbol, {} AS block_time, 10_000 AS price_ticks,
                   100 AS tick_size, 2 AS base_lot_decimals, 6 AS quote_lot_decimals, 91 AS asset_id
            UNION ALL
            SELECT 'PHX', {}, 10_000, 100, 2, 6, 91
            UNION ALL
            SELECT 'PHX', {}, 10_000, 100, 2, 6, 91
            UNION ALL
            SELECT 'PHX', {}, 10_000, 100, 2, 6, 91
        ) TO {} (FORMAT PARQUET)",
        last_second(fix.hour_before()),
        fix.secs(fix.first_hour()),
        last_second(fix.last_hour()),
        fix.secs(fix.hour_after()),
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
    let files = super::mapping::walk_parquet(&phoenix).unwrap();
    let scan = super::mapping::scan_phoenix(
        &conn,
        &phoenix,
        &files,
        &[super::grid::parse_date("2026-10-15").unwrap()],
        &fix.ranges,
    )
    .unwrap();
    // Counted: exactly the first and the last inside hour, never the
    // neighbors (four fills, two inside buckets).
    assert_eq!(
        scan.lockbox_dropped_hours,
        std::collections::BTreeSet::from([fix.first_hour(), fix.last_hour()])
    );
    // The free edge fills stay visible in the universe span: the span is the
    // last second before the set through the first second after it.
    assert_eq!(
        scan.universe["PHX"].first_ts,
        last_second(fix.hour_before())
    );
    assert_eq!(scan.universe["PHX"].last_ts, fix.secs(fix.hour_after()));
}

/// Pyth hour bucketing at the lockbox edges, the Pyth twin of the Phoenix
/// test above: the bucket is `floor(publish_time / 3600)`, so a publish
/// exactly on an hour boundary (k*3600) lands in hour k and the second
/// before it (k*3600-1) lands in hour k-1. At the set's edges that puts the
/// boundary publish in the lockbox-drop set and the last-second publish in
/// the universe, so an off-by-one in the `// 3600` bucketing moves an hour
/// between the two and goes red.
#[test]
fn the_pyth_hour_bucket_counts_edge_fills_exactly() {
    let tmp = Tmp::new("pythedge");
    let fix = super::testfix::v2_lockbox(tmp.dir());
    let conn = memory_connection("1", "512MB").unwrap();
    let pyth = tmp.dir().join("pyth-prices-edges");
    std::fs::create_dir_all(&pyth).unwrap();
    let path = pyth.join("prices.parquet");
    let last_second = |hour: i64| fix.secs(hour) + 3_599;
    conn.execute_batch(&format!(
        "COPY (
            SELECT 'PYX' AS symbol, {} AS publish_time
            UNION ALL
            SELECT 'PYX', {}
            UNION ALL
            SELECT 'PYX', {}
            UNION ALL
            SELECT 'PYX', {}
        ) TO {} (FORMAT PARQUET)",
        last_second(fix.hour_before()),
        fix.secs(fix.first_hour()),
        last_second(fix.last_hour()),
        fix.secs(fix.hour_after()),
        sql_string(&path.to_string_lossy())
    ))
    .unwrap();
    let (_, scan) = super::mapping::scan_pyth(&conn, &[pyth], &fix.ranges).unwrap();
    // Counted: exactly the first and the last inside hour, never the
    // neighbors (four publishes, two inside buckets).
    assert_eq!(
        scan.lockbox_dropped_hours,
        std::collections::BTreeSet::from([fix.first_hour(), fix.last_hour()])
    );
    // The free edge publishes stay visible in the universe span: the span is
    // the last second before the set through the first second after it.
    assert_eq!(
        scan.universe["PYX"].first_ts,
        last_second(fix.hour_before())
    );
    assert_eq!(scan.universe["PYX"].last_ts, fix.secs(fix.hour_after()));
    assert_eq!(scan.lockbox_only_symbols, 0);
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
