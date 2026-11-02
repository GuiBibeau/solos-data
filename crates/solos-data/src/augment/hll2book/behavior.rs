//! Behavior of one offline hour: counts, the forward split, checksums, partial files, raw
//! delete, fsync, and the measured Parquet layout.

use super::publish::Fault;
use super::{FORWARD_START_MS, Report, Request, Stop};
use crate::jsonout::Obj;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

const GIT: &str = "0123456789abcdef0123456789abcdef01234567";

struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hl-l2-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn lz4_frame(body: &[u8]) -> Vec<u8> {
    let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
    encoder.write_all(body).unwrap();
    let (frame, result) = encoder.finish();
    result.unwrap();
    frame
}

/// `(relative path, sha256, size)`.
fn write_coin(
    input: &Path,
    date: &str,
    hour: u32,
    coin: &str,
    body: &str,
) -> (String, String, u64) {
    let dir = input.join(date).join(hour.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let frame = lz4_frame(body.as_bytes());
    let sha = crate::fsutil::sha256_hex(&frame);
    let bytes = u64::try_from(frame.len()).unwrap();
    let relative = format!("{date}/{hour}/{coin}.lz4");
    std::fs::write(dir.join(format!("{coin}.lz4")), &frame).unwrap();
    (relative, sha, bytes)
}

fn manifest(files: &[(String, String, u64)]) -> String {
    let items: Vec<Value> = files
        .iter()
        .map(|(path, sha, size)| json!({"path": path, "sha256": sha, "size": size}))
        .collect();
    json!({"files": items}).to_string()
}

struct Opts {
    floor: u64,
    drop_raw: bool,
    coins: Option<BTreeSet<String>>,
    since: Option<String>,
    until: Option<String>,
    /// Very high here by default so the drop-mechanics tests are not about the ratio guard,
    /// which has its own both-directions test: a one-snapshot fixture is nearly all Parquet
    /// overhead and legitimately exceeds 1.0.
    max_ratio: f64,
    /// The injected count fault, or none.
    fault: Fault,
    /// Hour workers.
    jobs: u32,
    /// Hours this run may re-seal past the superset guard (`--reseal`).
    reseal: BTreeSet<String>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            floor: 0,
            drop_raw: false,
            coins: None,
            since: None,
            until: None,
            max_ratio: 1e12,
            fault: Fault::None,
            jobs: 1,
            reseal: BTreeSet::new(),
        }
    }
}

fn run_with(
    input: &Path,
    out: &Path,
    checksums: &Path,
    opts: Opts,
    free: impl FnMut(&Path) -> Result<u64, String>,
) -> Result<Report, crate::store::StoreError> {
    let request = Request {
        input,
        out,
        checksums,
        min_free_bytes: opts.floor,
        threads: 1,
        memory_limit: "512MB".into(),
        git_sha: GIT.into(),
        coins: opts.coins,
        since: opts.since,
        until: opts.until,
        drop_raw: opts.drop_raw,
        max_ratio: opts.max_ratio,
        jobs: opts.jobs,
        fault: opts.fault,
        reseal: opts.reseal,
    };
    super::run(&request, free)
}

fn run_ok(input: &Path, out: &Path, checksums: &Path) -> Result<Report, crate::store::StoreError> {
    run_with(input, out, checksums, Opts::default(), |_| Ok(u64::MAX))
}

fn read_rows(path: &Path) -> Vec<Obj> {
    let conn = crate::store::memory_connection("1", "512MB").unwrap();
    crate::store::query_rows(
        &conn,
        &format!(
            "SELECT * FROM read_parquet({}) ORDER BY coin, time_ms",
            crate::store::sql_string(&path.to_string_lossy())
        ),
        &[],
    )
    .unwrap()
}

fn parquet_files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, out);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push(relative);
            }
        }
    }
    walk(root, root, &mut out);
    out.sort();
    out
}

fn watermark(out: &Path) -> Vec<String> {
    let path = out.join("watermark.json");
    if !path.is_file() {
        return Vec::new();
    }
    let value: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    value["hours"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|hour| hour.as_str().map(str::to_owned))
        .collect()
}

fn book(coin: &str, time_ms: i64, levels: usize, px0: &str) -> String {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 0..levels {
        let px = if i == 0 {
            px0.to_owned()
        } else {
            format!("{}", 100 - i)
        };
        bids.push(json!({"px": px, "sz": "1.5", "n": i + 1}));
        asks.push(json!({"px": format!("{}", 101 + i), "sz": "2", "n": i + 1}));
    }
    json!({
        "time": "2026-10-22T00:00:00.5",
        "ver_num": 1,
        "raw": {
            "channel": "l2Book",
            "data": {"coin": coin, "time": time_ms, "levels": [bids, asks]}
        }
    })
    .to_string()
}

fn lines(rows: &[String]) -> String {
    let mut text = rows.join("\n");
    text.push('\n');
    text
}

struct Reset;

impl Reset {
    fn arm() -> Self {
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        super::batch::clear_drop_fault();
        crate::fsutil::clear_fsync_failure();
        crate::fsutil::clear_write_failure();
        Self
    }
}

impl Drop for Reset {
    fn drop(&mut self) {
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        super::batch::clear_drop_fault();
        crate::fsutil::clear_fsync_failure();
        crate::fsutil::clear_write_failure();
    }
}

fn stage_one(root: &Tmp, body: &str) -> (PathBuf, PathBuf, PathBuf) {
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let file = write_coin(&input, "20261022", 0, "BTC", body);
    std::fs::write(&sums, manifest(std::slice::from_ref(&file))).unwrap();
    (input, out, sums)
}

#[test]
fn flat_rows_nulls_sort_and_a_rerun_skips() {
    let root = Tmp::new("ok");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let btc = book("BTC", 1_792_627_200_000, 1, "100.5");
    let eth = book("ETH", 1_792_627_200_000 + 5_000, 20, "200");
    let btc_file = write_coin(&input, "20261022", 0, "BTC", &lines(&[btc]));
    let eth_file = write_coin(&input, "20261022", 0, "ETH", &lines(&[eth]));
    std::fs::write(&sums, manifest(&[btc_file, eth_file])).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.stop, Stop::Done);
    assert_eq!(report.hours_verified, 1);
    assert_eq!(report.rows, 2);
    assert_eq!(report.exit_code(), 0);
    let path = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let rows = read_rows(&path);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].str("coin"), Some("BTC"));
    assert_eq!(rows[1].str("coin"), Some("ETH"));
    let btc_px = rows[0].get("bid_px").unwrap().as_array().unwrap();
    assert!(btc_px[1].is_null(), "a short side stays null");
    assert_eq!(btc_px[0].as_f64(), Some(100.5));
    let eth_px = rows[1].get("bid_px").unwrap().as_array().unwrap();
    assert!(eth_px[19].is_number());
    assert!(
        input.join("20261022/0/BTC.lz4").is_file(),
        "raw stays without the flag"
    );
    let again = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(again.hours_skipped, 1);
    assert_eq!(again.hours_verified, 0);
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
}

/// Mutations: delete the `kept_ratio` branch so `--drop-raw-after-verify` always drops; make
/// `skip_verified` ignore the receipt ratio on the rerun path.
#[test]
fn the_ratio_guard_decides_the_drop_in_both_directions() {
    let root = Tmp::new("ratio-guard");
    let body = book("BTC", 1_792_627_200_000, 1, "1");
    let (input, out, sums) = stage_one(&root, &body);
    let raw = input.join("20261022/0/BTC.lz4");
    // A one-snapshot hour is nearly all Parquet overhead, so its ratio is far above 1.
    let under = run_with(
        &input,
        &out,
        &sums,
        Opts {
            drop_raw: true,
            max_ratio: 100.0,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(under.hours_verified, 1);
    assert_eq!(under.hours_kept_ratio, 0, "ratio under the max");
    assert!(!raw.exists(), "an hour at or under the max drops its raw");
    assert!(!under.hours[0].kept_ratio);
    assert!(under.hours[0].ratio.unwrap() > 1.0);

    // Same hour shape, a max the fixture cannot meet: converted and verified, raw kept.
    std::fs::write(&raw, lz4_frame(body.as_bytes())).unwrap();
    let out2 = root.0.join("out2");
    let over = run_with(
        &input,
        &out2,
        &sums,
        Opts {
            drop_raw: true,
            max_ratio: 1.0,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(
        over.hours_verified, 1,
        "the hour is still converted and verified"
    );
    assert_eq!(
        over.hours_kept_ratio, 1,
        "the hour is reported as kept_ratio"
    );
    assert!(raw.is_file(), "an hour over the max keeps its raw");
    assert_eq!(watermark(&out2), vec!["20261022/0".to_owned()]);
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out2.join("receipts/20261022/0.json")).unwrap(),
    )
    .unwrap();
    let lz4 = std::fs::metadata(&raw).unwrap().len();
    assert_eq!(receipt["lz4_bytes"], lz4);
    assert!(receipt["parquet_bytes"].as_u64().unwrap() > lz4);
    let json: Value = serde_json::from_str(&over.to_json()).unwrap();
    assert_eq!(json["hoursKeptRatio"], 1);
    assert_eq!(json["hours"][0]["keptRatio"], true);
    assert_eq!(json["hours"][0]["status"], "verified");

    // A rerun that skips the verified hour keeps honoring the guard.
    let again = run_with(
        &input,
        &out2,
        &sums,
        Opts {
            drop_raw: true,
            max_ratio: 1.0,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(again.hours_skipped, 1);
    assert_eq!(again.hours_kept_ratio, 1);
    assert!(
        raw.is_file(),
        "the skip path does not drop a kept-ratio hour"
    );
}

/// One input, four lines: an identical re-send is dropped, a distinct book at the same
/// `time_ms` is kept in archive line order, and the counts reach the report and the receipt.
#[test]
fn duplicates_follow_the_duplicate_rule() {
    let root = Tmp::new("dups");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let time = 1_792_627_200_000_i64;
    let first = book("BTC", time, 1, "100.0");
    // The same book re-sent under a later envelope time: dropped, dup_identical.
    let resend = book("BTC", time, 1, "100.0");
    // A different book at the same time_ms: kept, dup_distinct.
    let distinct = book("BTC", time, 1, "101.0");
    let later = book("BTC", time + 5_000, 1, "102.0");
    let body = lines(&[first.clone(), resend, distinct.clone(), later.clone()]);
    let file = write_coin(&input, "20261022", 0, "BTC", &body);
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(std::slice::from_ref(&file))).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.hours_verified, 1);
    assert_eq!(report.rows, 3, "3 of 4 lines are kept rows");
    assert_eq!(report.dup_identical, 1);
    assert_eq!(report.dup_distinct, 1);
    let path = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    // No ORDER BY: the physical file order is the deterministic (coin, time_ms, _seq) order.
    let conn = crate::store::memory_connection("1", "512MB").unwrap();
    let in_order = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT bid_px[1] AS px FROM read_parquet({})",
            crate::store::sql_string(&path.to_string_lossy())
        ),
        &[],
    )
    .unwrap();
    let prices: Vec<f64> = in_order
        .iter()
        .map(|row| row.get("px").and_then(|v| v.as_f64()).unwrap_or(f64::NAN))
        .collect();
    assert_eq!(prices, vec![100.0, 101.0, 102.0], "archive line order");
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("receipts/20261022/0.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["input_snapshots"], 4);
    assert_eq!(receipt["output_rows"], 3);
    assert_eq!(receipt["dup_identical"], 1);
    assert_eq!(receipt["dup_distinct"], 1);
    // The identical re-send can also repeat a book kept earlier in the same hour.
    let again = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &lines(&[first.clone(), distinct, first, later]),
    );
    let out2 = root.0.join("out2");
    std::fs::write(&sums, manifest(std::slice::from_ref(&again))).unwrap();
    let report = run_ok(&input, &out2, &sums).unwrap();
    assert_eq!(report.rows, 3);
    assert_eq!(report.dup_identical, 1, "a repeat of the first book");
    assert_eq!(report.dup_distinct, 1);
}

/// Mutation: change `>=` in `Corpus::of_time_ms` to `>`.
#[test]
fn forward_boundary_is_inclusive() {
    let root = Tmp::new("fwd");
    let body = lines(&[
        book("BTC", FORWARD_START_MS - 1, 1, "1"),
        book("BTC", FORWARD_START_MS, 1, "2"),
    ]);
    let (input, out, sums) = stage_one(&root, &body);
    run_ok(&input, &out, &sums).unwrap();
    let train = read_rows(&out.join("train/date=2026-10-22/hour=20261022-0.parquet"));
    let forward = read_rows(&out.join("forward/date=2026-10-23/hour=20261022-0.parquet"));
    assert_eq!(train.len(), 1);
    assert_eq!(train[0].int("time_ms"), Some(FORWARD_START_MS - 1));
    assert_eq!(forward.len(), 1);
    assert_eq!(forward[0].int("time_ms"), Some(FORWARD_START_MS));
}

#[test]
fn decimal_strings_round_trip_as_identical_f64_bits() {
    let root = Tmp::new("bits");
    let text = json!({
        "time": "2026-10-22T00:00:00.5",
        "raw": {"channel": "l2Book", "data": {
            "coin": "BTC",
            "time": 1_792_627_200_000_i64,
            "levels": [
                [
                    {"px": "3411232.9300000002", "sz": "0.1", "n": 1},
                    {"px": "0.3", "sz": "1", "n": 2}
                ],
                [{"px": "1", "sz": "1", "n": 1}]
            ]
        }}
    })
    .to_string();
    let (input, out, sums) = stage_one(&root, &lines(&[text]));
    run_ok(&input, &out, &sums).unwrap();
    let rows = read_rows(&out.join("train/date=2026-10-22/hour=20261022-0.parquet"));
    let px = rows[0].get("bid_px").unwrap().as_array().unwrap();
    let sz = rows[0].get("bid_sz").unwrap().as_array().unwrap();
    assert_eq!(
        px[0].as_f64().unwrap().to_bits(),
        "3411232.9300000002".parse::<f64>().unwrap().to_bits()
    );
    assert_eq!(
        sz[0].as_f64().unwrap().to_bits(),
        "0.1".parse::<f64>().unwrap().to_bits()
    );
    assert_eq!(
        px[1].as_f64().unwrap().to_bits(),
        "0.3".parse::<f64>().unwrap().to_bits()
    );
}

/// The archive line order of tied rows is the output order, not any value order. The books
/// at one `time_ms` arrive as 300, 100, 200 and must come back out in exactly that order.
/// DuckDB keeps input order for ties at this scale by accident of its sort, so the sort key
/// itself is pinned separately in publish::tests.
#[test]
fn tied_distinct_books_keep_the_archive_line_order_not_a_value_order() {
    let root = Tmp::new("tie-order");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let time = 1_792_627_200_000_i64;
    let rows = [
        book("BTC", time, 1, "300.0"),
        book("BTC", time, 1, "100.0"),
        book("BTC", time, 1, "200.0"),
        book("BTC", time + 5_000, 1, "150.0"),
    ]
    .map(String::from)
    .to_vec();
    let file = write_coin(&input, "20261022", 0, "BTC", &lines(&rows));
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(std::slice::from_ref(&file))).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.rows, 4);
    assert_eq!(report.dup_distinct, 2, "two extra books at one time_ms");
    let path = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    // No ORDER BY: physical file order is the deterministic (coin, time_ms, _seq) order.
    let conn = crate::store::memory_connection("1", "512MB").unwrap();
    let found = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT bid_px[1] AS px FROM read_parquet({})",
            crate::store::sql_string(&path.to_string_lossy())
        ),
        &[],
    )
    .unwrap();
    let prices: Vec<f64> = found
        .iter()
        .map(|row| row.get("px").and_then(|v| v.as_f64()).unwrap_or(f64::NAN))
        .collect();
    assert_eq!(
        prices,
        vec![300.0, 100.0, 200.0, 150.0],
        "the tied books must keep their arrival order, not a value order"
    );
}

/// Mutation: replace the `uncovered.contains` skip with the old `no checksum` abort.
#[test]
fn an_hour_with_an_uncovered_file_is_pending_not_an_abort() {
    let root = Tmp::new("no-sum");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let listed = write_coin(
        &input,
        "20261022",
        1,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    // ETH's finished .lz4 is on disk and in scope, but the manifest has no row for it.
    let _missing = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", 1_792_627_200_000, 1, "2"),
    );
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(&[listed])).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.stop, Stop::Done);
    assert_eq!(report.hours_uncovered, 1);
    assert_eq!(report.hours_pending, 1);
    assert_eq!(report.hours_verified, 1, "the covered hour still converts");
    assert_eq!(watermark(&out), vec!["20261022/1".to_owned()]);
    assert!(
        input.join("20261022/0/ETH.lz4").is_file(),
        "an uncovered raw file is never touched"
    );
    assert!(
        !out.join("train/date=2026-10-22/hour=20261022-0.parquet")
            .exists(),
        "the uncovered hour stays pending, half-converted"
    );
    assert_eq!(report.exit_code(), 0, "pending is not a failure");
}

/// Mutation: replace the `uncovered.contains(&hour.key)` skip in the task loop with `if
/// false`. A mixed hour (one covered coin, one uncovered coin on disk) must stay pending at
/// any job count, and no raw file of it may be deleted.
#[test]
fn a_mixed_hour_with_an_uncovered_coin_stays_pending_at_any_jobs() {
    for jobs in [1, 4] {
        let root = Tmp::new(&format!("mixed-{jobs}"));
        let input = root.0.join("in");
        let out = root.0.join("out");
        // BTC is in the manifest; ETH is finished on disk with no manifest row.
        let btc = write_coin(
            &input,
            "20261022",
            0,
            "BTC",
            &book("BTC", 1_792_627_200_000, 1, "1"),
        );
        let _eth = write_coin(
            &input,
            "20261022",
            0,
            "ETH",
            &book("ETH", 1_792_627_205_000, 1, "2"),
        );
        let sums = root.0.join("MANIFEST.json");
        std::fs::write(&sums, manifest(std::slice::from_ref(&btc))).unwrap();
        let report = run_with(
            &input,
            &out,
            &sums,
            Opts {
                drop_raw: true,
                jobs,
                ..Opts::default()
            },
            |_| Ok(u64::MAX),
        )
        .unwrap();
        assert_eq!(jobs, report.jobs, "the run used the requested workers");
        assert_eq!(report.hours_pending, 1, "jobs={jobs}");
        assert_eq!(report.hours_uncovered, 1, "jobs={jobs}");
        assert_eq!(
            report.hours_verified, 0,
            "jobs={jobs}: half-converted is not verified"
        );
        assert_eq!(report.hours_skipped, 0, "jobs={jobs}");
        assert!(
            watermark(&out).is_empty(),
            "jobs={jobs}: the watermark moved for a pending hour"
        );
        assert!(
            input.join("20261022/0/BTC.lz4").is_file(),
            "jobs={jobs}: the covered coin's raw was deleted from a pending hour"
        );
        assert!(
            input.join("20261022/0/ETH.lz4").is_file(),
            "jobs={jobs}: the uncovered coin's raw was touched"
        );
        assert!(
            !out.join("drops/20261022/0.json").is_file(),
            "jobs={jobs}: a drop record exists for a pending hour"
        );
        assert!(
            !out.join("train/date=2026-10-22/hour=20261022-0.parquet")
                .exists(),
            "jobs={jobs}: the pending hour was written anyway"
        );
    }
}

/// Mutation: delete the `--checksums` is-file check so a missing manifest is an IO error.
#[test]
fn a_missing_manifest_file_fails_closed() {
    let root = Tmp::new("no-manifest");
    let input = root.0.join("in");
    let out = root.0.join("out");
    write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let sums = root.0.join("no-such-MANIFEST");
    let error = run_ok(&input, &out, &sums).unwrap_err();
    assert!(
        error.to_string().contains("--checksums is not a manifest"),
        "{error}"
    );
    assert!(watermark(&out).is_empty());
    assert!(parquet_files(&out).is_empty());
}

/// Mutation: delete the sha256 comparison in `read_ready`.
#[test]
fn sha_mismatch_does_not_write_or_advance() {
    let root = Tmp::new("sha");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let file = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let other = lz4_frame(book("BTC", 1_792_627_200_000, 1, "9").as_bytes());
    std::fs::write(input.join("20261022/0/BTC.lz4"), &other).unwrap();
    let lied = (file.0, file.1, u64::try_from(other.len()).unwrap());
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(&[lied])).unwrap();
    let error = run_ok(&input, &out, &sums).unwrap_err();
    assert!(error.to_string().contains("sha256"), "{error}");
    assert!(watermark(&out).is_empty());
    assert!(parquet_files(&out).is_empty());
}

/// Mutation: delete the `len != entry.bytes` partial return in `locate_hour`.
#[test]
fn a_partial_input_file_is_not_processed() {
    let root = Tmp::new("partial");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let whole = write_coin(
        &input,
        "20261022",
        1,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let partial = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "2"),
    );
    let path = input.join(&partial.0);
    let full = std::fs::read(&path).unwrap();
    std::fs::write(&path, &full[..full.len() - 1]).unwrap();
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(&[partial, whole])).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.hours_pending, 1, "a short file must stay pending");
    assert_eq!(report.hours_verified, 1);
    assert_eq!(watermark(&out), vec!["20261022/1".to_owned()]);
    assert!(path.is_file(), "a partial file is not deleted");
    assert!(!watermark(&out).contains(&"20261022/0".to_owned()));
}

/// Mutation: delete `got != events` (Line); delete `output_rows != input_snapshots` (Hour);
/// delete `rows != file.rows` (Staged).
#[test]
fn a_short_write_fails_each_count_guard_without_a_watermark() {
    for fault in [Fault::Line, Fault::Hour, Fault::Staged] {
        let _reset = Reset::arm();
        let root = Tmp::new("short");
        let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
        let error = run_with(
            &input,
            &out,
            &sums,
            Opts {
                fault,
                ..Opts::default()
            },
            |_| Ok(u64::MAX),
        )
        .unwrap_err();
        let message = error.to_string();
        match fault {
            Fault::Line => assert!(message.contains("line "), "{fault:?}: {message}"),
            Fault::Hour => assert!(message.contains("hour kept"), "{fault:?}: {message}"),
            Fault::Staged => assert!(message.contains("staged"), "{fault:?}: {message}"),
            _ => unreachable!(),
        }
        assert!(watermark(&out).is_empty(), "{fault:?}: watermark moved");
    }
}

/// Mutation: delete the per-coin snapshot comparison in `check_coins`.
#[test]
fn a_per_file_count_mismatch_fails_the_hour() {
    let _reset = Reset::arm();
    let root = Tmp::new("per-file");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            fault: Fault::PerFile,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert!(error.to_string().contains("file snapshots"), "{}", error);
    assert!(watermark(&out).is_empty());
}

/// Mutation: delete `check_samples`.
#[test]
fn a_sample_value_mismatch_fails_the_hour() {
    let _reset = Reset::arm();
    let root = Tmp::new("sample");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "4.5"));
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            fault: Fault::Sample,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert!(error.to_string().contains("sample mismatch"), "{error}");
    assert!(watermark(&out).is_empty());
}

/// Mutations: delete `sync_tmp_file` (TempFile); delete `sync_dir_after_rename` in
/// `rename_all` (DirAfterRename); replace the receipt `write_durable` with `std::fs::write`;
/// replace the watermark `write_durable` with `std::fs::write`.
#[test]
fn fsync_failures_leave_the_watermark_and_a_rerun_succeeds() {
    use crate::fsutil::{FsyncStep, inject_fsync_failure};
    type Arm = fn();
    type Check = fn(&Path);
    let cases: &[(&str, Arm, Check)] = &[
        (
            "parquet-tmp",
            || inject_fsync_failure(FsyncStep::TempFile),
            |out| {
                assert!(
                    !out.join("train/date=2026-10-22/hour=20261022-0.parquet")
                        .is_file(),
                    "tmp fsync failed after the rename"
                );
            },
        ),
        (
            "parquet-dir",
            || inject_fsync_failure(FsyncStep::DirAfterRename),
            |out| {
                assert!(
                    !out.join("receipts/20261022/0.json").is_file(),
                    "dir fsync failed after the receipt was written"
                );
            },
        ),
        (
            "receipt",
            || {
                super::publish::seams::on_before_receipt(|| {
                    inject_fsync_failure(FsyncStep::TempFile);
                });
            },
            |out| {
                assert!(
                    !out.join("receipts/20261022/0.json").is_file(),
                    "receipt write was not durable"
                );
            },
        ),
        (
            "watermark",
            || {
                super::batch::on_before_watermark(|| {
                    inject_fsync_failure(FsyncStep::TempFile);
                });
            },
            |out| {
                assert!(
                    out.join("receipts/20261022/0.json").is_file(),
                    "watermark fsync failed before the receipt"
                );
                assert!(!out.join("watermark.json").is_file());
            },
        ),
    ];
    for (label, arm, check) in cases {
        let _reset = Reset::arm();
        let root = Tmp::new(label);
        let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
        arm();
        let error = run_ok(&input, &out, &sums).unwrap_err();
        assert!(
            error.to_string().contains("fsync failed"),
            "{label}: {error}"
        );
        assert!(watermark(&out).is_empty(), "{label}: watermark moved");
        check(&out);
        let again = run_ok(&input, &out, &sums).unwrap();
        assert_eq!(again.hours_verified, 1, "{label}");
        assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
    }
}

/// Mutation: move the raw delete above `write_watermark`.
#[test]
fn drop_raw_waits_until_the_watermark_is_durable() {
    use crate::fsutil::{FsyncStep, inject_fsync_failure};
    let _reset = Reset::arm();
    let root = Tmp::new("drop");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    let raw = input.join("20261022/0/BTC.lz4");
    let before = std::fs::read(&raw).unwrap();
    super::batch::on_before_watermark(|| inject_fsync_failure(FsyncStep::TempFile));
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            drop_raw: true,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert!(error.to_string().contains("fsync failed"), "{error}");
    assert!(watermark(&out).is_empty());
    assert_eq!(
        std::fs::read(&raw).unwrap(),
        before,
        "raw was deleted before the watermark"
    );
    assert!(!out.join("drops/20261022/0.json").is_file());
    let again = run_with(
        &input,
        &out,
        &sums,
        Opts {
            drop_raw: true,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(again.hours_verified, 1);
    assert!(!raw.exists(), "raw remains after a verified watermark");
    let record: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("drops/20261022/0.json")).unwrap())
            .unwrap();
    assert_eq!(record["schema"], "hl-l2book-drop-v1");
    assert_eq!(record["files"][0]["bytes"], before.len() as u64);
    assert_eq!(
        record["files"][0]["sha256"],
        crate::fsutil::sha256_hex(&before)
    );
    assert_eq!(
        record["files"][0]["s3"],
        "s3://hyperliquid-archive/market_data/20261022/0/l2Book/BTC.lz4"
    );
    assert!(
        record["files"][0]["path"]
            .as_str()
            .unwrap()
            .ends_with("BTC.lz4")
    );
}

/// Mutation: delete the canonical-path prefix check in `checked_input_file` and delete that path.
#[test]
fn drop_raw_refuses_a_link_outside_the_input_dir() {
    let root = Tmp::new("link");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let outside_dir = root.0.join("outside");
    std::fs::create_dir_all(input.join("20261022/0")).unwrap();
    std::fs::create_dir_all(&outside_dir).unwrap();
    let body = book("BTC", 1_792_627_200_000, 1, "1");
    let frame = lz4_frame(body.as_bytes());
    let outside = outside_dir.join("secret.lz4");
    std::fs::write(&outside, &frame).unwrap();
    std::os::unix::fs::symlink(&outside, input.join("20261022/0/BTC.lz4")).unwrap();
    let sums = root.0.join("MANIFEST.json");
    let listed = (
        "20261022/0/BTC.lz4".to_owned(),
        crate::fsutil::sha256_hex(&frame),
        u64::try_from(frame.len()).unwrap(),
    );
    std::fs::write(&sums, manifest(&[listed])).unwrap();
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            drop_raw: true,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("outside the input dir"),
        "{error}"
    );
    assert!(
        outside.is_file(),
        "a link target outside the input dir was deleted"
    );
    assert_eq!(std::fs::read(&outside).unwrap(), frame);
}

/// Mutations: drop the output `file_hash` compare; drop the superset guard; drop
/// `receipt.input_sha256 == identity`; drop `receipt.config_hash == config`.
#[test]
fn a_changed_output_or_config_hash_is_rewritten_and_a_shrinking_input_is_refused() {
    let root = Tmp::new("redo");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    run_ok(&input, &out, &sums).unwrap();
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let receipt_path = out.join("receipts/20261022/0.json");

    std::fs::write(&parquet, b"truncated").unwrap();
    let redone = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(redone.hours_verified, 1, "truncated output was skipped");
    assert!(parquet.metadata().unwrap().len() > 8);

    let mut edited: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    edited["config_hash"] = json!("0".repeat(64));
    std::fs::write(&receipt_path, edited.to_string()).unwrap();
    let redone = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(redone.hours_verified, 1, "changed config hash was skipped");
    let receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["config_hash"], super::config_hash());

    // The same coin with new bytes is not a superset of the sealed receipt: the rerun
    // refuses, and the hour's outputs and receipt keep their exact bytes.
    let replacement = book("BTC", 1_792_627_200_000, 1, "9");
    let pair = write_coin(&input, "20261022", 0, "BTC", &replacement);
    std::fs::write(&sums, manifest(std::slice::from_ref(&pair))).unwrap();
    let parquet_sha = crate::fsutil::file_hash(&parquet).unwrap();
    let receipt_before = std::fs::read(&receipt_path).unwrap();
    let error = run_ok(&input, &out, &sums).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("20261022/0"), "{message}");
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("not a superset"), "{message}");
    assert!(message.contains("BTC"), "{message}");
    assert!(message.contains("--reseal 20261022/0"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&parquet).unwrap(),
        parquet_sha,
        "the hour Parquet was rewritten"
    );
    assert_eq!(
        std::fs::read(&receipt_path).unwrap(),
        receipt_before,
        "the receipt was rewritten"
    );
}

#[test]
fn disk_floor_stops_between_hours() {
    let root = Tmp::new("disk");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let first = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let second = write_coin(
        &input,
        "20261022",
        1,
        "BTC",
        &book("BTC", 1_792_627_201_000, 1, "2"),
    );
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(&[first, second])).unwrap();
    let refused = run_with(
        &input,
        &out,
        &sums,
        Opts {
            floor: u64::MAX,
            ..Opts::default()
        },
        |_| Ok(0),
    )
    .unwrap();
    assert_eq!(refused.stop, Stop::DiskFloor);
    assert_eq!(refused.exit_code(), 2);
    assert!(watermark(&out).is_empty());
    let mut calls = 0u32;
    let stopped = run_with(
        &input,
        &out,
        &sums,
        Opts {
            floor: 1_000,
            ..Opts::default()
        },
        |_| {
            calls += 1;
            if calls < 3 { Ok(10_000) } else { Ok(10) }
        },
    )
    .unwrap();
    assert_eq!(stopped.stop, Stop::DiskFloor);
    assert_eq!(stopped.hours_verified, 1);
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
    assert!(
        !out.join("train/date=2026-10-22/hour=20261022-1.parquet")
            .exists()
    );
}

#[test]
fn coins_and_dates_filter_the_hours() {
    let root = Tmp::new("filter");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let btc = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", 1_792_627_200_000, 1, "2"),
    );
    let older = write_coin(
        &input,
        "20261021",
        0,
        "BTC",
        &book("BTC", 1_792_540_800_000, 1, "3"),
    );
    let sums = root.0.join("MANIFEST.json");
    std::fs::write(&sums, manifest(&[btc, eth, older])).unwrap();
    let mut coins = BTreeSet::new();
    coins.insert("BTC".to_owned());
    let report = run_with(
        &input,
        &out,
        &sums,
        Opts {
            coins: Some(coins),
            since: Some("20261022".into()),
            until: Some("20261022".into()),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(report.hours_verified, 1);
    assert_eq!(report.rows, 1);
    let rows = read_rows(&out.join("train/date=2026-10-22/hour=20261022-0.parquet"));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].str("coin"), Some("BTC"));
    assert!(
        !watermark(&out)
            .iter()
            .any(|hour| hour.starts_with("20261021"))
    );
}

/// The layout and encoding pin, on the fixture and against the alternative flat layout.
/// On real hours (ADR-0021) this layout under `PARQUET_VERSION 'V1'` measures 0.58x of the
/// `.lz4`; V2's `BYTE_STREAM_SPLIT` on the array child measures 1.13x, and 120 flat columns
/// measure 0.81x. This test pins the encoding DuckDB must pick (`px` dictionary, ZSTD) and
/// that the arrays stay at or below the flat-column file of the same rows.
#[test]
fn side_arrays_with_v1_dictionaries_beat_the_flat_columns_and_the_lz4() {
    let root = Tmp::new("ratio");
    let mut rows = Vec::new();
    for i in 0..720 {
        let time = 1_700_000_000_000_i64 + i64::from(i) * 5_000;
        let mid = 60_000.0 + (i as f64) * 0.25;
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for level in 0..20 {
            let bid = mid - (level as f64) * 0.5;
            let ask = mid + (level as f64 + 1.0) * 0.5;
            bids.push(json!({
                "px": format!("{bid:.1}"),
                "sz": format!("{:.2}", 1.25 + (level as f64) * 0.1),
                "n": level + 1
            }));
            asks.push(json!({
                "px": format!("{ask:.1}"),
                "sz": format!("{:.2}", 1.5 + (level as f64) * 0.1),
                "n": level + 2
            }));
        }
        rows.push(
            json!({
                "time": "2023-11-14T22:13:20.000",
                "raw": {"channel": "l2Book", "data": {"coin": "BTC", "time": time, "levels": [bids, asks]}}
            })
            .to_string(),
        );
    }
    let body = lines(&rows);
    let (input, out, sums) = stage_one(&root, &body);
    let lz4_bytes = std::fs::metadata(input.join("20261022/0/BTC.lz4"))
        .unwrap()
        .len();
    run_ok(&input, &out, &sums).unwrap();
    // 1_700_000_000_000 ms is 2023-11-14 22:13:20 UTC. 719 steps of 5s stay on that
    // date. Sum every parquet file in case a boundary splits the hour.
    let files = parquet_files(&out);
    assert!(!files.is_empty(), "no parquet");
    let mut parquet_bytes = 0u64;
    for relative in &files {
        parquet_bytes += std::fs::metadata(out.join(relative)).unwrap().len();
    }
    assert!(
        parquet_bytes < 10_000_000,
        "one coin-hour is {parquet_bytes} bytes, so a per-coin file stays under 10 MB"
    );
    let ratio = parquet_bytes as f64 / lz4_bytes as f64;
    assert!(
        parquet_bytes < lz4_bytes,
        "fixture ratio {ratio:.4} flat={parquet_bytes} lz4={lz4_bytes}"
    );

    let chosen = out.join(
        files
            .iter()
            .max_by_key(|path| std::fs::metadata(out.join(path)).unwrap().len())
            .unwrap(),
    );
    let conn = crate::store::memory_connection("1", "512MB").unwrap();
    let flat_path = root.0.join("flat.parquet");
    let mut projected = Vec::new();
    for side in ["bid", "ask"] {
        for kind in ["px", "sz", "n"] {
            for level in 1..=20 {
                projected.push(format!(
                    "{side}_{kind}[{level}] AS {side}_{kind}_{}",
                    level - 1
                ));
            }
        }
    }
    let sql = format!(
        "COPY (SELECT coin, time_ms, recv_time_ms, {} FROM read_parquet({}) ORDER BY coin, time_ms) TO {} ({})",
        projected.join(", "),
        crate::store::sql_string(&chosen.to_string_lossy()),
        crate::store::sql_string(&flat_path.to_string_lossy()),
        super::COPY_OPTIONS
    );
    conn.execute_batch(&sql).unwrap();
    let array_bytes = std::fs::metadata(&chosen).unwrap().len();
    let flat_bytes = std::fs::metadata(&flat_path).unwrap().len();
    assert!(
        array_bytes <= flat_bytes,
        "side-wide arrays {array_bytes} are larger than flat columns {flat_bytes}; switch the schema. ratio {ratio:.4}"
    );
    eprintln!(
        "hl-l2book fixture ratio {ratio:.4} array={array_bytes} flat={flat_bytes} lz4={lz4_bytes} parquet_total={parquet_bytes}"
    );

    let meta = crate::store::query_rows(
        &conn,
        &format!(
            "SELECT path_in_schema, CAST(encodings AS VARCHAR) AS encodings, CAST(compression AS VARCHAR) AS compression \
             FROM parquet_metadata({})",
            crate::store::sql_string(&chosen.to_string_lossy())
        ),
        &[],
    )
    .unwrap();
    let encoded = |fragment: &str, needle: &str| -> bool {
        meta.iter().any(|row| {
            row.str("path_in_schema").unwrap_or("").contains(fragment)
                && row.str("encodings").unwrap_or("").contains(needle)
        })
    };
    let paths: Vec<String> = meta
        .iter()
        .map(|row| {
            format!(
                "{}={}",
                row.str("path_in_schema").unwrap_or(""),
                row.str("encodings").unwrap_or("")
            )
        })
        .collect();
    // V1 has no BYTE_STREAM_SPLIT. That encoding on the array child is what made real
    // hours larger than the lz4 (1.13x under V2, 0.58x under V1); which encoding DuckDB
    // picks for a column is data-dependent (dictionary when prices repeat, PLAIN when they
    // do not, as on this smooth walk), so the pin is its absence, plus ZSTD compression.
    assert!(
        !encoded("", "BYTE_STREAM_SPLIT"),
        "V2 byte-stream-split on the array child is what made real hours larger than the lz4: {paths:?}"
    );
    let compression = meta
        .iter()
        .find(|row| row.str("path_in_schema").unwrap_or("").contains("bid_px"))
        .and_then(|row| row.str("compression"))
        .unwrap_or("");
    assert!(
        compression.to_ascii_lowercase().contains("zstd"),
        "{compression} {paths:?}"
    );
}

/// `--reseal` hour set from a list.
fn reseal_hours(hours: &[&str]) -> BTreeSet<String> {
    hours.iter().map(|hour| (*hour).to_owned()).collect()
}

fn drop_opts() -> Opts {
    Opts {
        drop_raw: true,
        max_ratio: 1000.0,
        ..Opts::default()
    }
}

fn stage_pair(root: &Tmp) -> (PathBuf, PathBuf, PathBuf) {
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let btc = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", 1_792_627_205_000, 1, "2"),
    );
    std::fs::write(&sums, manifest(&[btc, eth])).unwrap();
    (input, out, sums)
}

/// Mutation: replace `already_done(...)` in the partial-hour branch with `true`,
/// or replace `if watermark.contains` there with `if false`.
#[test]
fn a_corrupt_verified_hour_with_partial_raw_is_not_dropped() {
    let root = Tmp::new("partial-verified");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    run_ok(&input, &out, &sums).unwrap();
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    std::fs::write(&parquet, b"truncated").unwrap();
    let raw = input.join("20261022/0/BTC.lz4");
    let bytes = std::fs::read(&raw).unwrap();
    let short = &bytes[..bytes.len() - 1];
    std::fs::write(&raw, short).unwrap();
    let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
    assert!(
        error.to_string().contains("verified hour is incomplete"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(&raw).unwrap(),
        short,
        "partial raw of a corrupt verified hour was deleted"
    );
}

/// Mutation: delete the size comparison in `drop_raw`.
#[test]
fn drop_raw_refuses_a_file_whose_size_grew() {
    let root = Tmp::new("drop-size");
    let (input, out, sums) = stage_pair(&root);
    run_ok(&input, &out, &sums).unwrap();
    let btc = input.join("20261022/0/BTC.lz4");
    let eth = input.join("20261022/0/ETH.lz4");
    std::fs::remove_file(&btc).unwrap();
    let mut grown = std::fs::read(&eth).unwrap();
    grown.push(0);
    std::fs::write(&eth, &grown).unwrap();
    let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("size"), "{error}");
    assert_eq!(std::fs::read(&eth).unwrap(), grown, "grown raw was deleted");
    assert!(!btc.exists());
    assert!(!out.join("drops/20261022/0.json").is_file());
}

/// Mutation: delete the sha256 comparison in `drop_raw`.
#[test]
fn drop_raw_refuses_a_same_size_edit() {
    let root = Tmp::new("drop-sha");
    let (input, out, sums) = stage_pair(&root);
    run_ok(&input, &out, &sums).unwrap();
    let btc = input.join("20261022/0/BTC.lz4");
    let eth = input.join("20261022/0/ETH.lz4");
    std::fs::remove_file(&btc).unwrap();
    let mut edited = std::fs::read(&eth).unwrap();
    edited[0] ^= 0xff;
    std::fs::write(&eth, &edited).unwrap();
    let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("sha256"), "{error}");
    assert_eq!(
        std::fs::read(&eth).unwrap(),
        edited,
        "edited raw was deleted"
    );
    assert!(!out.join("drops/20261022/0.json").is_file());
}

/// Mutation: delete `if !path.exists() { continue }` in `drop_raw`.
/// Also: move `write_drop_record` back to after the unlink loop.
#[test]
fn a_crash_mid_drop_retries_and_keeps_the_drop_record() {
    let _reset = Reset::arm();
    let root = Tmp::new("mid-drop");
    let (input, out, sums) = stage_pair(&root);
    run_ok(&input, &out, &sums).unwrap();
    super::batch::fail_after_next_unlink();
    let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("injected drop stop"), "{error}");
    let record = out.join("drops/20261022/0.json");
    assert!(
        record.is_file(),
        "drop record was written after the unlink that crashed"
    );
    assert!(
        !input.join("20261022/0/BTC.lz4").exists(),
        "first raw file was not unlinked before the crash"
    );
    assert!(input.join("20261022/0/ETH.lz4").is_file());
    let again = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(again.stop, Stop::Done);
    assert!(!input.join("20261022/0/ETH.lz4").exists());
    let value: Value = serde_json::from_str(&std::fs::read_to_string(&record).unwrap()).unwrap();
    assert_eq!(value["files"].as_array().unwrap().len(), 2);
}

/// Mutations: delete the parent-directory fsync loop after the raw unlinks in `drop_raw`.
/// The last fsync of a clean drop run must be the input directory the files were unlinked
/// from, not the drop record's directory.
#[test]
fn the_raw_unlink_dirs_are_fsynced() {
    let _reset = Reset::arm();
    let root = Tmp::new("drop-fsync");
    let (input, out, sums) = stage_pair(&root);
    run_ok(&input, &out, &sums).unwrap();
    let dropped = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(dropped.hours_verified + dropped.hours_skipped, 1);
    assert!(!input.join("20261022/0/BTC.lz4").exists());
    let last = crate::fsutil::last_fsync_path().expect("the drop ran fsyncs");
    assert!(
        last.starts_with(&input),
        "the last fsync was {} and left the unlink un-durable",
        last.display()
    );
    let again = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(again.stop, Stop::Done);
}

/// Mutation: delete the `LockExclusiveNonblock` arm that returns "already owned".
#[test]
fn a_held_lock_refuses_the_run() {
    let root = Tmp::new("lock");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    std::fs::create_dir_all(&out).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(out.join("hl-l2book.lock"))
        .unwrap();
    let _held = nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusive).unwrap();
    let error = run_ok(&input, &out, &sums).unwrap_err();
    assert!(error.to_string().contains("already owned"), "{error}");
    assert!(watermark(&out).is_empty());
}

#[test]
fn cli_command_and_the_live_lane_stays_off() {
    let config = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/augment.json");
    let value: Value = serde_json::from_str(&std::fs::read_to_string(config).unwrap()).unwrap();
    let hl = &value["sources"]["hyperliquidS3"];
    assert_eq!(hl["enabled"], false);
    assert_eq!(hl["estimateOnly"], true);
    assert_eq!(hl["fills"], false);

    let root = Tmp::new("cli");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    let code = crate::cli::main(&[
        "augment".into(),
        "hl-l2book".into(),
        "--input".into(),
        input.to_string_lossy().into_owned(),
        "--out".into(),
        out.to_string_lossy().into_owned(),
        "--checksums".into(),
        sums.to_string_lossy().into_owned(),
        "--min-free-tb".into(),
        "0".into(),
        "--threads".into(),
        "1".into(),
        "--memory".into(),
        "512MB".into(),
    ]);
    assert_eq!(code, 0);
    assert!(out.join("watermark.json").is_file());
    let refused = root.0.join("refused");
    let code = crate::cli::main(&[
        "augment".into(),
        "hl-l2book".into(),
        "--input".into(),
        input.to_string_lossy().into_owned(),
        "--out".into(),
        refused.to_string_lossy().into_owned(),
        "--checksums".into(),
        sums.to_string_lossy().into_owned(),
        "--min-free-tb".into(),
        "1000000".into(),
    ]);
    assert_eq!(code, 2);
    assert!(!refused.join("watermark.json").exists());
}

#[test]
fn market_data_manifest_layout_is_found() {
    let root = Tmp::new("layout");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let dir = input.join("market_data/20261022/0/l2Book");
    std::fs::create_dir_all(&dir).unwrap();
    let frame = lz4_frame(book("BTC", 1_792_627_200_000, 1, "1").as_bytes());
    std::fs::write(dir.join("BTC.lz4"), &frame).unwrap();
    let sums = root.0.join("MANIFEST.json");
    let listed = (
        "market_data/20261022/0/l2Book/BTC.lz4".to_owned(),
        crate::fsutil::sha256_hex(&frame),
        u64::try_from(frame.len()).unwrap(),
    );
    std::fs::write(&sums, manifest(&[listed])).unwrap();
    let report = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(report.hours_verified, 1);
    assert_eq!(report.rows, 1);
}

/// A coin that arrives late makes the current inputs a strict superset of the sealed
/// receipt: the hour re-seals, the receipt records N+1 inputs, and the output keeps every
/// earlier coin's rows plus the late coin's.
#[test]
fn late_coin_superset_reseals() {
    let root = Tmp::new("late-coin");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    let btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    std::fs::write(&sums, manifest(&[btc.clone(), eth.clone()])).unwrap();
    let first = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(first.hours_verified, 1);
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    assert_eq!(read_rows(&parquet).len(), 2);

    // SOL's file and its manifest row arrive after the hour is verified.
    let sol = write_coin(
        &input,
        "20261022",
        0,
        "SOL",
        &book("SOL", time + 10_000, 1, "3"),
    );
    std::fs::write(&sums, manifest(&[btc, eth, sol])).unwrap();
    let again = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(again.hours_verified, 1, "the superset hour re-seals");
    assert_eq!(again.hours_skipped, 0);
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("receipts/20261022/0.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["inputs"].as_array().unwrap().len(), 3);
    let coins: Vec<String> = read_rows(&parquet)
        .iter()
        .filter_map(|row| row.str("coin").map(str::to_owned))
        .collect();
    assert_eq!(
        coins,
        vec!["BTC", "ETH", "SOL"],
        "the late coin's rows joined the earlier coins"
    );
}

/// A rerun whose scope shrank below the sealed receipt is refused before any work: the run
/// fails, the hour Parquet and receipt keep their exact bytes, every raw file stays, no
/// drop record appears, and the watermark is unchanged.
#[test]
fn shrinking_inputs_refused() {
    let root = Tmp::new("shrink");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    let btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    let sol = write_coin(
        &input,
        "20261022",
        0,
        "SOL",
        &book("SOL", time + 10_000, 1, "3"),
    );
    std::fs::write(&sums, manifest(&[btc.clone(), eth.clone(), sol.clone()])).unwrap();
    let first = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(first.hours_verified, 1);
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let parquet_sha = crate::fsutil::file_hash(&parquet).unwrap();
    let receipt = std::fs::read(out.join("receipts/20261022/0.json")).unwrap();
    let watermark_before = watermark(&out);

    // The same manifest, but the rerun only claims BTC and ETH: SOL is missing from the
    // current inputs, and drop-raw is on.
    let mut coins = BTreeSet::new();
    coins.insert("BTC".to_owned());
    coins.insert("ETH".to_owned());
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            coins: Some(coins),
            drop_raw: true,
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("20261022/0"), "{message}");
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("not a superset"), "{message}");
    assert!(message.contains("SOL"), "{message}");
    assert!(message.contains("--reseal 20261022/0"), "{message}");
    assert!(message.contains("Restore the missing file(s)"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&parquet).unwrap(),
        parquet_sha,
        "the hour Parquet was replaced with the subset"
    );
    assert_eq!(
        std::fs::read(out.join("receipts/20261022/0.json")).unwrap(),
        receipt,
        "the receipt was rewritten"
    );
    for file in [&btc, &eth, &sol] {
        assert!(input.join(&file.0).is_file(), "{} was touched", file.0);
    }
    assert!(
        !out.join("drops/20261022/0.json").exists(),
        "a drop record appeared for the refused hour"
    );
    assert_eq!(watermark(&out), watermark_before, "the watermark moved");
}

/// The drop record of a rerun unions the earlier record: entries already on file stay, a
/// path that reappears with a different sha256 keeps both entries, sorted by path then
/// sha256, and a foreign or unparsable record is an error rather than an overwrite.
#[test]
fn drop_record_merge_keeps_earlier_entries() {
    let root = Tmp::new("drop-merge");
    let out = root.0.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let path = out.join("drops/20261022/0.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let earlier = json!({
        "schema": super::DROP_SCHEMA,
        "hour": "20261022/0",
        "files": [{
            "path": "20261022/0/BTC.lz4",
            "bytes": 10u64,
            "sha256": "aa".repeat(32),
            "s3": super::s3_url("20261022", 0, "BTC"),
        }],
    });
    std::fs::write(&path, earlier.to_string()).unwrap();

    // A superset re-seal re-downloaded BTC under different bytes and added SOL.
    let receipt = receipt_with_inputs(&[
        ("BTC", "20261022/0/BTC.lz4", "bb", 12),
        ("SOL", "20261022/0/SOL.lz4", "cc", 14),
    ]);
    super::batch::write_drop_record(&out, "20261022/0", "20261022", 0, &receipt).unwrap();

    let value: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(value["schema"], super::DROP_SCHEMA);
    assert_eq!(value["hour"], "20261022/0");
    let keys: Vec<(String, String)> = value["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| {
            (
                file["path"].as_str().unwrap().to_owned(),
                file["sha256"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            ("20261022/0/BTC.lz4".to_owned(), "aa".repeat(32)),
            ("20261022/0/BTC.lz4".to_owned(), "bb".repeat(32)),
            ("20261022/0/SOL.lz4".to_owned(), "cc".repeat(32)),
        ],
        "the earlier entry, the new sha, and SOL, sorted by path then sha256"
    );

    // A record for another hour is an error, never an overwrite.
    std::fs::write(
        &path,
        json!({"schema": super::DROP_SCHEMA, "hour": "20261022/1", "files": []}).to_string(),
    )
    .unwrap();
    let error =
        super::batch::write_drop_record(&out, "20261022/0", "20261022", 0, &receipt).unwrap_err();
    assert!(error.to_string().contains("20261022/1"), "{error}");
    let value: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        value["hour"], "20261022/1",
        "the foreign record was replaced"
    );

    // So is an unparsable one.
    std::fs::write(&path, "not json").unwrap();
    let error =
        super::batch::write_drop_record(&out, "20261022/0", "20261022", 0, &receipt).unwrap_err();
    assert!(
        error.to_string().contains("drops/20261022/0.json"),
        "{error}"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "not json");
}

/// The commit-point re-check in `seal_hour`: when the hour already has a receipt whose
/// inputs the staged hour no longer covers, the seal is refused and nothing final is
/// touched, including the staged temporaries.
#[test]
fn seal_hour_refuses_to_shrink_a_sealed_hour() {
    let root = Tmp::new("seal-guard");
    let out = root.0.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let time = 1_792_627_200_000_i64;
    let btc_body = lz4_frame(book("BTC", time, 1, "1").as_bytes());
    let sol_body = lz4_frame(book("SOL", time + 5_000, 1, "3").as_bytes());
    let hash = super::config_hash();
    let settings = super::publish::WriterSettings {
        threads: "1".to_owned(),
        memory_limit: "512MB".to_owned(),
    };
    let stage = |files: &[(&str, &Vec<u8>)]| {
        let (conn, _) = super::publish::open_writer(&out, 0, 1, "512MB").unwrap();
        let relatives: Vec<String> = files
            .iter()
            .map(|(coin, _)| format!("20261022/0/{coin}.lz4"))
            .collect();
        let shas: Vec<String> = files
            .iter()
            .map(|(_, body)| crate::fsutil::sha256_hex(body))
            .collect();
        let owned: Vec<super::publish::FileBytes> = files
            .iter()
            .enumerate()
            .map(|(index, (coin, body))| super::publish::FileBytes {
                coin,
                relative: &relatives[index],
                sha256: &shas[index],
                bytes: body.as_slice(),
            })
            .collect();
        let job = super::publish::HourJob {
            hour_key: "20261022/0",
            files: &owned,
            git_sha: GIT,
            config_hash: &hash,
            fault: Fault::None,
            allow_shrink: false,
        };
        super::publish::prepare_hour(&conn, &out, &job, &settings).unwrap()
    };

    let first = stage(&[("BTC", &btc_body), ("SOL", &sol_body)]);
    super::publish::seal_hour(&out, first).unwrap();
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let parquet_sha = crate::fsutil::file_hash(&parquet).unwrap();
    let receipt = std::fs::read(out.join("receipts/20261022/0.json")).unwrap();

    // Stage the same hour without SOL, against the receipt that still records it.
    let second = stage(&[("BTC", &btc_body)]);
    let error = super::publish::seal_hour(&out, second).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("SOL"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&parquet).unwrap(),
        parquet_sha,
        "the hour Parquet was replaced"
    );
    assert_eq!(
        std::fs::read(out.join("receipts/20261022/0.json")).unwrap(),
        receipt,
        "the receipt was rewritten"
    );
    assert_eq!(
        parquet_files(&out),
        vec!["train/date=2026-10-22/hour=20261022-0.parquet"],
        "a staged temporary survived the refusal"
    );
}

/// A receipt for one hour with the named inputs, for direct `write_drop_record` calls.
fn receipt_with_inputs(files: &[(&str, &str, &str, u64)]) -> super::publish::Receipt {
    super::publish::Receipt {
        schema: super::SCHEMA.to_owned(),
        hour: "20261022/0".to_owned(),
        input_sha256: String::new(),
        input_snapshots: 0,
        output_rows: 0,
        dup_identical: 0,
        dup_distinct: 0,
        lz4_bytes: 0,
        parquet_bytes: 0,
        train_rows: 0,
        forward_rows: 0,
        outputs: Vec::new(),
        inputs: files
            .iter()
            .map(|(coin, path, sha, bytes)| super::publish::InputFile {
                coin: (*coin).to_owned(),
                path: (*path).to_owned(),
                sha256: sha.repeat(32),
                bytes: *bytes,
                snapshots: 0,
            })
            .collect(),
        git_sha: "x".to_owned(),
        config_hash: super::config_hash(),
    }
}

/// A corrected re-download of a sealed hour: without `--reseal` the changed sha256 is
/// refused, with the fix in the error; with `--reseal` the hour re-seals from the new
/// bytes, the receipt records the new sha, and stale cleanup removes the forward output
/// the corrected seal no longer produces. A second `--reseal` run is a normal skip.
#[test]
fn reseal_corrected_redownload() {
    let root = Tmp::new("reseal-fix");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    let btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    // The old SOL content has a train row and a forward-dated row, so the old seal
    // produces a forward output the corrected content drops.
    let old_sol = write_coin(
        &input,
        "20261022",
        0,
        "SOL",
        &lines(&[
            book("SOL", time + 10_000, 1, "3"),
            book("SOL", super::FORWARD_START_MS + 5_000, 1, "4"),
        ]),
    );
    std::fs::write(
        &sums,
        manifest(&[btc.clone(), eth.clone(), old_sol.clone()]),
    )
    .unwrap();
    let first = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(first.hours_verified, 1);
    let train = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let forward = out.join("forward/date=2026-10-23/hour=20261022-0.parquet");
    assert!(forward.is_file(), "the old seal wrote a forward output");
    assert_eq!(read_rows(&train).len(), 3);
    assert_eq!(read_rows(&forward).len(), 1);
    let train_sha = crate::fsutil::file_hash(&train).unwrap();

    // Corrected re-download: SOL's bytes change and its manifest row follows.
    let new_sol = write_coin(
        &input,
        "20261022",
        0,
        "SOL",
        &book("SOL", time + 10_000, 1, "33"),
    );
    assert_ne!(
        new_sol.1, old_sol.1,
        "the corrected bytes have a new sha256"
    );
    std::fs::write(
        &sums,
        manifest(&[btc.clone(), eth.clone(), new_sol.clone()]),
    )
    .unwrap();
    let refused = run_ok(&input, &out, &sums).unwrap_err();
    let message = refused.to_string();
    assert!(message.contains("20261022/0"), "{message}");
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("SOL"), "{message}");
    assert!(message.contains("--reseal 20261022/0"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&train).unwrap(),
        train_sha,
        "the refusal touched the hour"
    );
    assert!(forward.is_file());

    let fixed = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(fixed.hours_verified, 1, "the named hour re-seals");
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
    assert!(
        !forward.exists(),
        "the stale forward output survived the re-seal"
    );
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("receipts/20261022/0.json")).unwrap(),
    )
    .unwrap();
    let inputs = receipt["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 3);
    assert!(
        inputs
            .iter()
            .any(|file| file["sha256"] == new_sol.1.as_str()),
        "the receipt records the corrected sha"
    );
    let outputs: Vec<&str> = receipt["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["path"].as_str().unwrap())
        .collect();
    assert_eq!(
        outputs,
        vec!["train/date=2026-10-22/hour=20261022-0.parquet"],
        "the receipt names exactly the outputs on disk"
    );
    let rows = read_rows(&train);
    assert_eq!(rows.len(), 3);
    let sol_px = rows
        .iter()
        .find(|row| row.str("coin") == Some("SOL"))
        .and_then(|row| {
            row.get("bid_px")
                .and_then(|px| px.as_array())
                .map(|a| a[0].clone())
        })
        .and_then(|px| px.as_f64());
    assert_eq!(sol_px, Some(33.0), "the output holds the corrected content");

    // A second --reseal run with nothing changed is a normal, idempotent skip.
    let again = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(again.hours_skipped, 1);
    assert_eq!(again.hours_verified, 0);
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
}

/// `--reseal` without the receipt's raw on disk is refused up front: without the raw the
/// sealed coins cannot be rebuilt, and the refusal touches nothing.
#[test]
fn reseal_refused_when_receipt_raw_missing() {
    let root = Tmp::new("reseal-no-raw");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    let btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    std::fs::write(&sums, manifest(&[btc, eth])).unwrap();
    let sealed = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(sealed.hours_verified, 1);
    assert!(
        !input.join("20261022/0/BTC.lz4").exists(),
        "the seal dropped its raw"
    );
    let train = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let receipt = out.join("receipts/20261022/0.json");
    let drops = out.join("drops/20261022/0.json");
    let train_sha = crate::fsutil::file_hash(&train).unwrap();
    let receipt_bytes = std::fs::read(&receipt).unwrap();
    let drops_bytes = std::fs::read(&drops).unwrap();
    let watermark_before = watermark(&out);

    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..drop_opts()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("20261022/0"), "{message}");
    assert!(message.contains("20261022/0/BTC.lz4"), "{message}");
    assert!(message.contains("not on disk"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&train).unwrap(),
        train_sha,
        "the hour was touched"
    );
    assert_eq!(
        std::fs::read(&receipt).unwrap(),
        receipt_bytes,
        "the receipt was touched"
    );
    assert_eq!(
        std::fs::read(&drops).unwrap(),
        drops_bytes,
        "the drop record was touched"
    );
    assert_eq!(watermark(&out), watermark_before, "the watermark moved");
}

/// `--reseal` of an hour the watermark does not list fails before any work.
#[test]
fn reseal_unknown_hour_refused() {
    let root = Tmp::new("reseal-unknown");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let btc = write_coin(
        &input,
        "20261022",
        0,
        "BTC",
        &book("BTC", 1_792_627_200_000, 1, "1"),
    );
    std::fs::write(&sums, manifest(&[btc])).unwrap();
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/1"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("--reseal 20261022/1"), "{message}");
    assert!(message.contains("not in the watermark"), "{message}");
    assert!(
        !out.join("watermark.json").exists(),
        "the run wrote a watermark"
    );
    assert!(!out.join("receipts").exists(), "the run wrote a receipt");
    assert!(parquet_files(&out).is_empty(), "the run wrote parquet");
}

/// `--reseal` cannot shrink an hour through `--coins`: the receipt-coin coverage check
/// refuses before any work, and the sealed hour, its raw, and the watermark are untouched.
#[test]
fn reseal_refuses_a_narrower_coins() {
    let root = Tmp::new("reseal-narrow");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    let btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    let sol = write_coin(
        &input,
        "20261022",
        0,
        "SOL",
        &book("SOL", time + 10_000, 1, "3"),
    );
    std::fs::write(&sums, manifest(&[btc.clone(), eth.clone(), sol.clone()])).unwrap();
    let first = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(first.hours_verified, 1);
    let train = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let train_sha = crate::fsutil::file_hash(&train).unwrap();
    let rows = read_rows(&train).len();
    let receipt = std::fs::read(out.join("receipts/20261022/0.json")).unwrap();
    let watermark_before = watermark(&out);

    // A strict subset of the receipt's coins, plus the escape hatch: still refused.
    let mut coins = BTreeSet::new();
    coins.insert("BTC".to_owned());
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            coins: Some(coins),
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("missing receipt coins"), "{message}");
    assert!(message.contains("ETH"), "{message}");
    assert!(message.contains("SOL"), "{message}");
    assert_eq!(
        read_rows(&train).len(),
        rows,
        "the hour Parquet row count changed"
    );
    assert_eq!(
        crate::fsutil::file_hash(&train).unwrap(),
        train_sha,
        "the hour Parquet was rewritten"
    );
    assert_eq!(
        std::fs::read(out.join("receipts/20261022/0.json")).unwrap(),
        receipt,
        "the receipt was rewritten"
    );
    for file in [&btc, &eth, &sol] {
        assert!(input.join(&file.0).is_file(), "{} was touched", file.0);
    }
    assert_eq!(watermark(&out), watermark_before, "the watermark moved");
    assert!(
        !out.join("drops/20261022/0.json").exists(),
        "a drop record appeared"
    );
}

/// The `--reseal` bypass is scoped to the named hour. With `--reseal X`, two other
/// watermarked hours whose inputs shrank (a rebuilt manifest with fewer rows, the
/// dropped coin's raw gone) are still refused by the superset guard: sealed output,
/// receipt, and watermark entry untouched, while X alone re-seals past the guard.
/// Y = 20261022/1 shares X's date; Z = 20261023/0 shares X's hour, so a bypass that
/// matched on the date alone dies on Y and one that matched on the hour alone dies
/// on Z. Each shrink is refused in its own run: the run stops at the first refused
/// hour (sorted order), so Y is restored before Z's refusal is observed.
#[test]
fn reseal_bypass_is_scoped_to_the_named_hour() {
    let root = Tmp::new("reseal-scope");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    // Z's rows stay before the forward split at `FORWARD_START_MS` so the hour seals
    // into `train/` like X and Y; the partition date follows the row time, the hour
    // in the file name follows the source hour key.
    let z_time = time + 3_600_000;
    // Hour X = 20261022/0, hour Y = 20261022/1, hour Z = 20261023/0, each with BTC
    // and ETH.
    let x_btc = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "1"));
    let x_eth = write_coin(
        &input,
        "20261022",
        0,
        "ETH",
        &book("ETH", time + 5_000, 1, "2"),
    );
    let y_btc = write_coin(
        &input,
        "20261022",
        1,
        "BTC",
        &book("BTC", time + 3_600_000, 1, "3"),
    );
    let y_eth = write_coin(
        &input,
        "20261022",
        1,
        "ETH",
        &book("ETH", time + 3_605_000, 1, "4"),
    );
    let z_btc = write_coin(&input, "20261023", 0, "BTC", &book("BTC", z_time, 1, "5"));
    let z_eth = write_coin(
        &input,
        "20261023",
        0,
        "ETH",
        &book("ETH", z_time + 5_000, 1, "6"),
    );
    std::fs::write(
        &sums,
        manifest(&[
            x_btc.clone(),
            x_eth.clone(),
            y_btc.clone(),
            y_eth.clone(),
            z_btc.clone(),
            z_eth.clone(),
        ]),
    )
    .unwrap();
    let first = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(first.hours_verified, 3);
    assert_eq!(
        watermark(&out),
        vec![
            "20261022/0".to_owned(),
            "20261022/1".to_owned(),
            "20261023/0".to_owned()
        ]
    );

    // Positive control: with Y's and Z's inputs intact, `--reseal X` re-seals a
    // corrected re-download of X alone and leaves the skipped Y and Z untouched.
    let fixed_x = write_coin(&input, "20261022", 0, "BTC", &book("BTC", time, 1, "11"));
    assert_ne!(fixed_x.1, x_btc.1, "the corrected bytes have a new sha256");
    std::fs::write(
        &sums,
        manifest(&[
            fixed_x.clone(),
            x_eth.clone(),
            y_btc.clone(),
            y_eth.clone(),
            z_btc.clone(),
            z_eth.clone(),
        ]),
    )
    .unwrap();
    let fixed = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(fixed.hours_verified, 1, "the named hour X re-seals");
    assert_eq!(fixed.hours_skipped, 2, "Y or Z was not skipped");
    assert_eq!(
        watermark(&out),
        vec![
            "20261022/0".to_owned(),
            "20261022/1".to_owned(),
            "20261023/0".to_owned()
        ]
    );
    let y_train = out.join("train/date=2026-10-22/hour=20261022-1.parquet");
    let y_train_sha = crate::fsutil::file_hash(&y_train).unwrap();
    let z_train = out.join("train/date=2026-10-22/hour=20261023-0.parquet");
    let z_train_sha = crate::fsutil::file_hash(&z_train).unwrap();

    // Shrink Y first: its ETH row leaves the manifest and the raw goes with it, so Y
    // is Ready over the BTC subset. The run still names X in `--reseal`, but the
    // bypass must not reach Y: the guard refuses the run and Y is untouched. Y shares
    // X's date, so a date-matched bypass dies here.
    let y_eth_raw = input.join("20261022/1/ETH.lz4");
    let y_eth_bytes = std::fs::read(&y_eth_raw).unwrap();
    std::fs::write(
        &sums,
        manifest(&[
            fixed_x.clone(),
            x_eth.clone(),
            y_btc.clone(),
            z_btc.clone(),
            z_eth.clone(),
        ]),
    )
    .unwrap();
    std::fs::remove_file(&y_eth_raw).unwrap();
    let y_receipt = std::fs::read(out.join("receipts/20261022/1.json")).unwrap();
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("20261022/1"), "{message}");
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("not a superset"), "{message}");
    assert!(message.contains("ETH"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&y_train).unwrap(),
        y_train_sha,
        "Y's hour Parquet was rewritten"
    );
    assert_eq!(
        std::fs::read(out.join("receipts/20261022/1.json")).unwrap(),
        y_receipt,
        "Y's receipt was rewritten"
    );
    assert!(
        input.join("20261022/1/BTC.lz4").is_file(),
        "Y's remaining raw was deleted"
    );
    assert_eq!(
        watermark(&out),
        vec![
            "20261022/0".to_owned(),
            "20261022/1".to_owned(),
            "20261023/0".to_owned()
        ],
        "the watermark moved"
    );
    assert!(
        !out.join("drops/20261022/1.json").exists(),
        "a drop record appeared for Y"
    );

    // Now shrink Z, whose only tie to X is the hour number: restore Y first (the run
    // stops at the first refused hour, so a still-shrunk Y would mask Z's refusal),
    // then drop Z's ETH the same way. The guard must refuse Z too, with Z's hour
    // named, and leave Z untouched.
    std::fs::write(&y_eth_raw, y_eth_bytes).unwrap();
    let z_eth_raw = input.join("20261023/0/ETH.lz4");
    std::fs::write(&sums, manifest(&[fixed_x, x_eth, y_btc, y_eth, z_btc])).unwrap();
    std::fs::remove_file(&z_eth_raw).unwrap();
    let z_receipt = std::fs::read(out.join("receipts/20261023/0.json")).unwrap();
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/0"]),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("20261023/0"), "{message}");
    assert!(
        !message.contains("20261022/1"),
        "the refusal named Y, not Z: {message}"
    );
    assert!(message.contains("refusing to re-seal"), "{message}");
    assert!(message.contains("not a superset"), "{message}");
    assert!(message.contains("ETH"), "{message}");
    assert_eq!(
        crate::fsutil::file_hash(&z_train).unwrap(),
        z_train_sha,
        "Z's hour Parquet was rewritten"
    );
    assert_eq!(
        std::fs::read(out.join("receipts/20261023/0.json")).unwrap(),
        z_receipt,
        "Z's receipt was rewritten"
    );
    assert!(
        input.join("20261023/0/BTC.lz4").is_file(),
        "Z's remaining raw was deleted"
    );
    assert_eq!(
        crate::fsutil::file_hash(&y_train).unwrap(),
        y_train_sha,
        "the restored Y was touched while Z was refused"
    );
    assert_eq!(
        watermark(&out),
        vec![
            "20261022/0".to_owned(),
            "20261022/1".to_owned(),
            "20261023/0".to_owned()
        ],
        "the watermark moved"
    );
    assert!(
        !out.join("drops/20261023/0.json").exists(),
        "a drop record appeared for Z"
    );
}

/// The `--reseal` precheck runs before any hour is processed: a refused re-seal of hour A
/// leaves hour B, which dispatches first (same date, lower hour, jobs=1), completely
/// untouched. The on-disk state is asserted before the error's wording so a mutation of
/// the precheck fails on behavior, not on message text.
#[test]
fn reseal_precheck_runs_before_any_work() {
    let root = Tmp::new("reseal-precheck");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("MANIFEST.json");
    let time = 1_792_627_200_000_i64;
    // Hour B (20261022/0) sorts before hour A (20261022/1).
    let b_sol = write_coin(&input, "20261022", 0, "SOL", &book("SOL", time, 1, "1"));
    let a_btc = write_coin(
        &input,
        "20261022",
        1,
        "BTC",
        &book("BTC", time + 3_600_000, 1, "2"),
    );
    let a_eth = write_coin(
        &input,
        "20261022",
        1,
        "ETH",
        &book("ETH", time + 3_605_000, 1, "3"),
    );
    std::fs::write(&sums, manifest(&[b_sol, a_btc, a_eth])).unwrap();
    // Seal only hour A, with drop-raw: --coins names A's coins, so B has no in-scope
    // entries and stays pending.
    let mut a_coins = BTreeSet::new();
    a_coins.insert("BTC".to_owned());
    a_coins.insert("ETH".to_owned());
    let sealed = run_with(
        &input,
        &out,
        &sums,
        Opts {
            coins: Some(a_coins),
            ..drop_opts()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap();
    assert_eq!(sealed.hours_verified, 1);
    assert_eq!(watermark(&out), vec!["20261022/1".to_owned()]);
    assert!(
        !input.join("20261022/1/BTC.lz4").exists(),
        "A's raw survived the seal"
    );

    // Rerun with drop-raw on and --reseal A: the precheck refuses A before B is touched.
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            reseal: reseal_hours(&["20261022/1"]),
            ..drop_opts()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert_eq!(
        watermark(&out),
        vec!["20261022/1".to_owned()],
        "the watermark moved for B"
    );
    assert!(
        !out.join("receipts/20261022/0.json").exists(),
        "B got a receipt"
    );
    assert!(
        !out.join("train/date=2026-10-22/hour=20261022-0.parquet")
            .exists(),
        "B was written"
    );
    assert!(
        input.join("20261022/0/SOL.lz4").is_file(),
        "B's raw was touched"
    );
    assert!(
        !out.join("drops/20261022/0.json").exists(),
        "a drop record appeared for B"
    );
    let message = error.to_string();
    assert!(message.contains("20261022/1"), "{message}");
    assert!(message.contains("20261022/1/BTC.lz4"), "{message}");
    assert!(message.contains("not on disk"), "{message}");
}

/// A foreign-schema or wrong-hour drop record refuses the whole drop: the record is
/// unchanged and no raw file is deleted after it.
#[test]
fn drop_record_wrong_schema_is_refused() {
    for (label, body) in [
        (
            "schema",
            json!({"schema": "hl-l2book-drop-v2", "hour": "20261022/0", "files": []}).to_string(),
        ),
        (
            "hour",
            json!({"schema": super::DROP_SCHEMA, "hour": "20261022/1", "files": []}).to_string(),
        ),
    ] {
        let root = Tmp::new(&format!("drop-bad-{label}"));
        let (input, out, sums) = stage_pair(&root);
        run_ok(&input, &out, &sums).unwrap();
        let record = out.join("drops/20261022/0.json");
        std::fs::create_dir_all(record.parent().unwrap()).unwrap();
        std::fs::write(&record, &body).unwrap();
        let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("drops/20261022/0.json"),
            "{label}: {message}"
        );
        assert_eq!(
            std::fs::read_to_string(&record).unwrap(),
            body,
            "{label}: the record was replaced"
        );
        assert!(
            input.join("20261022/0/BTC.lz4").is_file(),
            "{label}: raw was deleted"
        );
        assert!(
            input.join("20261022/0/ETH.lz4").is_file(),
            "{label}: raw was deleted"
        );
    }
}

/// The drop record's durable write happens before any unlink: a failed write leaves the
/// raw files and the earlier record intact, and a rerun merges and drops as usual.
#[test]
fn drop_record_durable_write_fault() {
    let _reset = Reset::arm();
    let root = Tmp::new("drop-write-fault");
    let (input, out, sums) = stage_pair(&root);
    let btc_path = input.join("20261022/0/BTC.lz4");
    let btc_bytes = std::fs::read(&btc_path).unwrap();
    run_ok(&input, &out, &sums).unwrap();
    let record = out.join("drops/20261022/0.json");
    let dropped = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(dropped.hours_skipped, 1, "the drop ran on the skip path");
    assert!(record.is_file());
    let before = std::fs::read(&record).unwrap();

    // BTC is re-downloaded; the merged record's durable write fails with ENOSPC.
    std::fs::write(&btc_path, &btc_bytes).unwrap();
    crate::fsutil::inject_write_enospc();
    let error = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("No space left"), "{error}");
    assert_eq!(
        std::fs::read(&btc_path).unwrap(),
        btc_bytes,
        "raw was deleted past the failed write"
    );
    assert_eq!(
        std::fs::read(&record).unwrap(),
        before,
        "the record was replaced by a failed write"
    );

    // Without the fault the rerun merges the record and drops the re-downloaded file.
    let again = run_with(&input, &out, &sums, drop_opts(), |_| Ok(u64::MAX)).unwrap();
    assert_eq!(again.hours_skipped, 1);
    assert!(
        !btc_path.exists(),
        "the re-downloaded raw survived a good rerun"
    );
    let value: Value = serde_json::from_str(&std::fs::read_to_string(&record).unwrap()).unwrap();
    assert_eq!(
        value["files"].as_array().unwrap().len(),
        2,
        "the merge kept every entry"
    );
}

/// The work_one guard runs before any staging: a refused run opens no writer, so no
/// `.tmp-duckdb-<worker>` directory is left behind (ClearStaging never removes those), and
/// nothing final moves.
#[test]
fn work_one_guard_alone() {
    let root = Tmp::new("guard-alone");
    let (input, out, sums) = stage_pair(&root);
    run_ok(&input, &out, &sums).unwrap();
    let writer_dir = out.join(".tmp-duckdb-0");
    assert!(writer_dir.is_dir(), "the first run opened a writer");
    std::fs::remove_dir_all(&writer_dir).unwrap();

    // Narrow the hour below its receipt: the refusal must come from work_one, before the
    // writer opens and before anything is staged.
    let mut coins = BTreeSet::new();
    coins.insert("BTC".to_owned());
    let error = run_with(
        &input,
        &out,
        &sums,
        Opts {
            coins: Some(coins),
            ..Opts::default()
        },
        |_| Ok(u64::MAX),
    )
    .unwrap_err();
    assert!(error.to_string().contains("refusing to re-seal"), "{error}");
    assert!(
        !writer_dir.exists(),
        "a writer was opened before the refusal"
    );
    assert_eq!(
        watermark(&out),
        vec!["20261022/0".to_owned()],
        "the watermark moved"
    );
    assert!(
        out.join("train/date=2026-10-22/hour=20261022-0.parquet")
            .is_file(),
        "the hour Parquet was touched"
    );
}
