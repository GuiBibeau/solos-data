//! Behavior of one offline hour: counts, the forward split, checksums, partial files, raw
//! delete, fsync, and the measured Parquet layout.

use super::publish::seams::CountSeam;
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
    };
    super::run(&request, free)
}

fn run_ok(input: &Path, out: &Path, checksums: &Path) -> Result<Report, crate::store::StoreError> {
    run_with(
        input,
        out,
        checksums,
        Opts {
            floor: 0,
            drop_raw: false,
            coins: None,
            since: None,
            until: None,
        },
        |_| Ok(u64::MAX),
    )
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
        super::publish::seams::clear_count_seam();
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        super::batch::clear_drop_fault();
        crate::fsutil::clear_fsync_failure();
        Self
    }
}

impl Drop for Reset {
    fn drop(&mut self) {
        super::publish::seams::clear_count_seam();
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        super::batch::clear_drop_fault();
        crate::fsutil::clear_fsync_failure();
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
        !out.join("train/date=2026-10-22/hour=20261022-0.parquet").exists(),
        "the uncovered hour stays pending, half-converted"
    );
    assert_eq!(report.exit_code(), 0, "pending is not a failure");
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
    for seam in [CountSeam::Line, CountSeam::Hour, CountSeam::Staged] {
        let _reset = Reset::arm();
        super::publish::seams::set_count_seam(seam);
        let root = Tmp::new("short");
        let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
        let error = run_ok(&input, &out, &sums).unwrap_err();
        let message = error.to_string();
        match seam {
            CountSeam::Line => assert!(message.contains("line "), "{seam:?}: {message}"),
            CountSeam::Hour => assert!(message.contains("hour kept"), "{seam:?}: {message}"),
            CountSeam::Staged => assert!(message.contains("staged"), "{seam:?}: {message}"),
            _ => unreachable!(),
        }
        assert!(watermark(&out).is_empty(), "{seam:?}: watermark moved");
    }
}

/// Mutation: delete the per-coin snapshot comparison in `check_coins`.
#[test]
fn a_per_file_count_mismatch_fails_the_hour() {
    let _reset = Reset::arm();
    super::publish::seams::set_count_seam(CountSeam::PerFile);
    let root = Tmp::new("per-file");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    let error = run_ok(&input, &out, &sums).unwrap_err();
    assert!(error.to_string().contains("file snapshots"), "{}", error);
    assert!(watermark(&out).is_empty());
}

/// Mutation: delete `check_samples`.
#[test]
fn a_sample_value_mismatch_fails_the_hour() {
    let _reset = Reset::arm();
    super::publish::seams::set_count_seam(CountSeam::Sample);
    let root = Tmp::new("sample");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "4.5"));
    let error = run_ok(&input, &out, &sums).unwrap_err();
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
            floor: 0,
            drop_raw: true,
            coins: None,
            since: None,
            until: None,
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
            floor: 0,
            drop_raw: true,
            coins: None,
            since: None,
            until: None,
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
            floor: 0,
            drop_raw: true,
            coins: None,
            since: None,
            until: None,
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

/// Mutations: drop the output `file_hash` compare; drop `receipt.input_sha256 == identity`;
/// drop `receipt.config_hash == config`.
#[test]
fn a_changed_output_or_input_or_config_hash_is_rewritten() {
    let root = Tmp::new("redo");
    let (input, out, sums) = stage_one(&root, &book("BTC", 1_792_627_200_000, 1, "1"));
    run_ok(&input, &out, &sums).unwrap();
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let receipt_path = out.join("receipts/20261022/0.json");

    std::fs::write(&parquet, b"truncated").unwrap();
    let redone = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(redone.hours_verified, 1, "truncated output was skipped");
    assert!(parquet.metadata().unwrap().len() > 8);

    let replacement = book("BTC", 1_792_627_200_000, 1, "9");
    let pair = write_coin(&input, "20261022", 0, "BTC", &replacement);
    std::fs::write(&sums, manifest(std::slice::from_ref(&pair))).unwrap();
    let redone = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(redone.hours_verified, 1, "changed input was skipped");
    let receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["inputs"][0]["sha256"], pair.1);

    let mut edited: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    edited["config_hash"] = json!("0".repeat(64));
    std::fs::write(&receipt_path, edited.to_string()).unwrap();
    let redone = run_ok(&input, &out, &sums).unwrap();
    assert_eq!(redone.hours_verified, 1, "changed config hash was skipped");
    let receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["config_hash"], super::config_hash());
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
            drop_raw: false,
            coins: None,
            since: None,
            until: None,
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
            drop_raw: false,
            coins: None,
            since: None,
            until: None,
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
            floor: 0,
            drop_raw: false,
            coins: Some(coins),
            since: Some("20261022".into()),
            until: Some("20261022".into()),
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

fn drop_opts() -> Opts {
    Opts {
        floor: 0,
        drop_raw: true,
        coins: None,
        since: None,
        until: None,
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
