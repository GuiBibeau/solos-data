//! Behavior of one offline hour: every coin class, nulls, the forward split, a bad checksum,
//! a short file, the disk floor, and a rerun.

use super::{FORWARD_START_MS, Report, Request, Stop};
use crate::jsonout::Obj;
use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};

const GIT: &str = "0123456789abcdef0123456789abcdef01234567";

struct Tmp(PathBuf);

impl Tmp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hl-{label}-{}-{}",
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

fn write_lz4(input: &Path, date: &str, hour: u32, body: &str) -> (String, String) {
    let dir = input.join(date);
    std::fs::create_dir_all(&dir).unwrap();
    let frame = lz4_frame(body.as_bytes());
    let sha = crate::fsutil::sha256_hex(&frame);
    std::fs::write(dir.join(format!("{hour}.lz4")), frame).unwrap();
    (format!("{date}/{hour}"), sha)
}

fn sums_lines(pairs: &[(String, String)]) -> String {
    let mut text = String::new();
    for (key, sha) in pairs {
        let (date, hour) = key.split_once('/').unwrap();
        text.push_str(sha);
        text.push_str("  ");
        text.push_str(date);
        text.push('/');
        text.push_str(hour);
        text.push_str(".lz4\n");
    }
    text
}

fn run_with(
    input: &Path,
    out: &Path,
    checksums: &Path,
    floor: u64,
    free: impl FnMut(&Path) -> Result<u64, String>,
) -> Result<Report, crate::store::StoreError> {
    let request = Request {
        input,
        out,
        checksums,
        min_free_bytes: floor,
        threads: 1,
        memory_limit: "512MB".into(),
        git_sha: GIT.into(),
    };
    super::run(&request, free)
}

fn read_rows(path: &Path) -> Vec<Obj> {
    let conn = crate::store::memory_connection("1", "256MB").unwrap();
    crate::store::query_rows(
        &conn,
        &format!(
            "SELECT * FROM read_parquet({})",
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

fn assert_no_partial(root: &Path) {
    fn walk(dir: &Path) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            assert!(!name.ends_with(".tmp"), "partial file {}", path.display());
            assert!(!name.ends_with(".jsonl"), "staging file {}", path.display());
            if path.is_dir() && name != ".tmp-duckdb" {
                walk(&path);
            }
        }
    }
    walk(root);
    assert!(
        !root.join(".staging").exists(),
        "staging directory left behind"
    );
}

fn watermark(out: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(out.join("watermark.json")).unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    value["hours"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hour| hour.as_str().unwrap().to_owned())
        .collect()
}

fn block(number: i64, block_time: &str, events: Vec<Value>) -> Value {
    json!({
        "local_time": "t",
        "block_time": block_time,
        "block_number": number,
        "events": events
    })
}

#[test]
fn all_coins_nulls_forward_split_and_rerun() {
    let root = Tmp::new("ok");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let t0 = 1_792_670_400_000_i64;
    let zero_hash = format!("0x{}", "0".repeat(64));
    let rich = format!(
        "{}\n{}\n",
        block(
            10,
            "2026-10-22T12:00:00.000000000",
            vec![
                json!([
                    "0xoutcome",
                    {
                        "coin": "#35001",
                        "px": "1.25",
                        "sz": "4",
                        "side": "A",
                        "time": t0 + 3_000,
                        "startPosition": "0",
                        "dir": "Open Long",
                        "closedPnl": "0.5",
                        "hash": zero_hash,
                        "oid": 9,
                        "crossed": false,
                        "fee": "-0.012",
                        "tid": 100,
                        "feeToken": "USDC",
                        "twapId": 77
                    }
                ]),
                json!([
                    "0xspot",
                    {
                        "coin": "@107",
                        "px": "2",
                        "sz": "1",
                        "side": "B",
                        "time": t0 + 1_000,
                        "dir": "",
                        "oid": 1,
                        "tid": 2,
                        "twapId": null,
                        "cloid": ""
                    }
                ]),
                json!([
                    "0xhip",
                    {
                        "coin": "xyz:SP500",
                        "px": "10.5",
                        "sz": "0.2",
                        "side": "A",
                        "time": t0 + 2_000,
                        "dir": "Open Short",
                        "startPosition": "1",
                        "closedPnl": "0",
                        "fee": "0.01",
                        "feeToken": "USDC",
                        "crossed": true,
                        "oid": 3,
                        "tid": 4,
                        "hash": "0xabc",
                        "cloid": "0xhip",
                        "builder": "0xbuilder",
                        "builderFee": "0.001",
                        "deployerFee": "0.003",
                        "twapId": null
                    }
                ]),
            ]
        ),
        block(
            11,
            "2026-10-22T12:00:01.000000000",
            vec![json!([
                "0xbtc",
                {
                    "coin": "BTC",
                    "px": "67000.5",
                    "sz": "0.01",
                    "side": "B",
                    "time": t0 + 500,
                    "dir": "Open Long",
                    "startPosition": "0",
                    "closedPnl": "0",
                    "fee": "0.02",
                    "feeToken": "USDC",
                    "crossed": true,
                    "oid": 8,
                    "tid": 9,
                    "hash": "0xdead",
                    "cloid": "0xcloid",
                    "builder": "0xbuilder",
                    "builderFee": "1.5",
                    "priorityGas": "0.25",
                    "twapId": null,
                    "liquidation": {
                        "liquidatedUser": "0xabc",
                        "markPx": "100.5",
                        "method": "market"
                    }
                }
            ])]
        )
    );
    let cross = block(
        12,
        "2026-10-22T23:59:59.000000000",
        vec![
            json!([
                "0xbefore",
                {
                    "coin": "BTC",
                    "px": "1",
                    "sz": "1",
                    "side": "B",
                    "time": FORWARD_START_MS - 1,
                    "tid": 1,
                    "oid": 1
                }
            ]),
            json!([
                "0xafter",
                {
                    "coin": "@1",
                    "px": "2",
                    "sz": "2",
                    "side": "A",
                    "time": FORWARD_START_MS,
                    "tid": 2,
                    "oid": 2
                }
            ]),
        ],
    )
    .to_string();
    let mut pairs = vec![
        write_lz4(&input, "20261022", 12, &rich),
        write_lz4(&input, "20261022", 23, &cross),
    ];
    let sums = root.0.join("SHA256SUMS");
    std::fs::write(&sums, sums_lines(&pairs)).unwrap();
    let report = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(report.stop, Stop::Done);
    assert_eq!(report.hours_verified, 2);
    assert_eq!(report.hours_skipped, 0);
    assert_eq!(report.rows, 6);
    assert_eq!(report.threads, "1");
    // DuckDB prints a decimal megabyte cap in binary MiB (512MB is about 488 MiB).
    assert!(
        report.memory_limit.contains("488") || report.memory_limit.contains("512"),
        "memory cap not applied: {}",
        report.memory_limit
    );
    assert_eq!(
        parquet_files(&out),
        vec![
            "forward/date=2026-10-23/hour=20261022-23.parquet".to_owned(),
            "train/date=2026-10-22/hour=20261022-12.parquet".to_owned(),
            "train/date=2026-10-22/hour=20261022-23.parquet".to_owned(),
        ]
    );
    let rows = read_rows(&out.join("train/date=2026-10-22/hour=20261022-12.parquet"));
    assert_eq!(
        rows.iter()
            .map(|row| row.str("coin").unwrap())
            .collect::<Vec<_>>(),
        vec!["#35001", "@107", "BTC", "xyz:SP500"]
    );
    let outcome = &rows[0];
    assert_eq!(outcome.str("coin_class"), Some("outcome"));
    assert_eq!(outcome.str("hash"), Some(zero_hash.as_str()));
    assert_eq!(outcome.str("twap_id"), Some("77"));
    assert!(outcome.get("cloid").unwrap().is_null());
    assert!(outcome.get("builder").unwrap().is_null());
    assert!(outcome.get("builder_fee").unwrap().is_null());
    assert!(outcome.get("deployer_fee").unwrap().is_null());
    assert!(outcome.get("priority_gas").unwrap().is_null());
    assert!(outcome.get("liquidation").unwrap().is_null());
    assert_eq!(
        outcome.get("fee").unwrap().as_f64().unwrap().to_bits(),
        "-0.012".parse::<f64>().unwrap().to_bits()
    );
    let spot = &rows[1];
    assert_eq!(spot.str("coin_class"), Some("spot"));
    for column in [
        "fee",
        "cloid",
        "hash",
        "twap_id",
        "crossed",
        "dir",
        "builder",
        "builder_fee",
        "deployer_fee",
        "priority_gas",
        "liquidation",
        "start_position",
        "closed_pnl",
    ] {
        assert!(
            spot.get(column).unwrap().is_null(),
            "{column} should be null, got {}",
            spot.get(column).unwrap()
        );
    }
    let btc = &rows[2];
    assert_eq!(btc.str("coin_class"), Some("perp"));
    assert_eq!(btc.str("block_number"), Some("11"));
    assert_eq!(btc.get("closed_pnl").unwrap().as_f64(), Some(0.0));
    assert_eq!(btc.get("priority_gas").unwrap().as_f64(), Some(0.25));
    assert_eq!(btc.get("builder_fee").unwrap().as_f64(), Some(1.5));
    assert!(btc.get("deployer_fee").unwrap().is_null());
    assert_eq!(btc.get("crossed").unwrap().as_bool(), Some(true));
    let liquidation: Value = serde_json::from_str(btc.str("liquidation").unwrap()).unwrap();
    assert_eq!(liquidation["liquidatedUser"], "0xabc");
    assert_eq!(liquidation["method"], "market");
    assert_eq!(liquidation["markPx"], "100.5");
    let hip = &rows[3];
    assert_eq!(hip.str("coin"), Some("xyz:SP500"));
    assert_eq!(hip.str("coin_class"), Some("hip3"));
    assert_eq!(
        hip.get("deployer_fee").unwrap().as_f64().unwrap().to_bits(),
        "0.003".parse::<f64>().unwrap().to_bits()
    );
    assert_eq!(hip.str("builder"), Some("0xbuilder"));
    assert_eq!(hip.str("cloid"), Some("0xhip"));
    assert!(hip.get("priority_gas").unwrap().is_null());
    assert!(hip.get("liquidation").unwrap().is_null());
    assert_eq!(outcome.str("block_number"), Some("10"));
    let train_edge = read_rows(&out.join("train/date=2026-10-22/hour=20261022-23.parquet"));
    assert_eq!(train_edge.len(), 1);
    assert_eq!(train_edge[0].str("coin"), Some("BTC"));
    assert_eq!(
        train_edge[0]
            .str("time_ms")
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        FORWARD_START_MS - 1
    );
    let forward = read_rows(&out.join("forward/date=2026-10-23/hour=20261022-23.parquet"));
    assert_eq!(forward.len(), 1);
    assert_eq!(forward[0].str("coin"), Some("@1"));
    assert_eq!(forward[0].str("coin_class"), Some("spot"));
    assert_eq!(
        forward[0].str("time_ms").unwrap().parse::<i64>().unwrap(),
        FORWARD_START_MS
    );
    let receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("receipts/20261022/12.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["input_fills"], 4);
    assert_eq!(receipt["output_rows"], 4);
    assert_eq!(receipt["train_rows"], 4);
    assert_eq!(receipt["forward_rows"], 0);
    assert_eq!(receipt["git_sha"], GIT);
    assert_eq!(receipt["config_hash"], super::config_hash());
    assert_eq!(receipt["input_sha256"], pairs[0].1);
    let cross_receipt: Value = serde_json::from_str(
        &std::fs::read_to_string(out.join("receipts/20261022/23.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(cross_receipt["input_fills"], 2);
    assert_eq!(cross_receipt["train_rows"], 1);
    assert_eq!(cross_receipt["forward_rows"], 1);
    assert_eq!(watermark(&out), vec!["20261022/12", "20261022/23"]);
    assert_no_partial(&out);
    let sealed =
        crate::fsutil::file_hash(&out.join("train/date=2026-10-22/hour=20261022-12.parquet"))
            .unwrap();
    let again = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(again.hours_verified, 0);
    assert_eq!(again.hours_skipped, 2);
    assert_eq!(
        crate::fsutil::file_hash(&out.join("train/date=2026-10-22/hour=20261022-12.parquet"))
            .unwrap(),
        sealed
    );
    std::fs::remove_file(out.join("receipts/20261022/12.json")).unwrap();
    let rewritten = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(rewritten.hours_verified, 1);
    assert_eq!(rewritten.hours_skipped, 1);
    assert_eq!(
        crate::fsutil::file_hash(&out.join("train/date=2026-10-22/hour=20261022-12.parquet"))
            .unwrap(),
        sealed,
        "rewriting an hour did not converge to the same file"
    );
    pairs.push(write_lz4(
        &input,
        "20261022",
        13,
        &block(
            1,
            "2026-10-22T13:00:00.000000000",
            vec![json!(["0xsol", {"coin": "SOL", "px": "100", "sz": "1", "side": "B", "time": t0 + 3_600_000, "tid": 1, "oid": 1}])],
        )
        .to_string(),
    ));
    std::fs::write(&sums, sums_lines(&pairs)).unwrap();
    let resumed = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(resumed.hours_verified, 1);
    assert_eq!(resumed.rows, 1);
    assert_eq!(
        crate::fsutil::file_hash(&out.join("train/date=2026-10-22/hour=20261022-12.parquet"))
            .unwrap(),
        sealed
    );
    assert!(
        out.join("train/date=2026-10-22/hour=20261022-13.parquet")
            .is_file()
    );
}

#[test]
fn sha_mismatch_does_not_write_or_advance() {
    let root = Tmp::new("sha");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let body = block(
        1,
        "2026-10-22T12:00:00.000000000",
        vec![json!(["0xa", {"coin": "BTC", "px": "1", "sz": "1", "side": "B", "time": 1_792_670_400_000_i64, "tid": 1, "oid": 1}])],
    )
    .to_string();
    let (key, _) = write_lz4(&input, "20261022", 12, &body);
    let sums = root.0.join("SHA256SUMS");
    std::fs::write(&sums, sums_lines(&[(key, "ab".repeat(32))])).unwrap();
    let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("sha256"), "{error}");
    assert!(!out.join("watermark.json").exists());
    assert!(parquet_files(&out).is_empty());
    assert_no_partial(&out);
}

#[test]
fn short_file_fails_without_a_partial_write() {
    let root = Tmp::new("short");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let good = block(
        1,
        "2026-10-22T12:00:00.000000000",
        vec![json!(["0xa", {"coin": "ETH", "px": "1", "sz": "1", "side": "A", "time": 1_792_670_400_000_i64, "tid": 1, "oid": 1}])],
    )
    .to_string();
    let mut pairs = vec![write_lz4(&input, "20261022", 12, &good)];
    let short = lz4_frame(
        br#"{"block_number":1,"block_time":"2026-10-22T14:00:00.000000000","events":[["0xa",{"coin":"BTC","px":"1","sz":"1","side":"B","time":1792670400000"#,
    );
    std::fs::create_dir_all(input.join("20261022")).unwrap();
    std::fs::write(input.join("20261022/14.lz4"), &short).unwrap();
    pairs.push(("20261022/14".into(), crate::fsutil::sha256_hex(&short)));
    let sums = root.0.join("SHA256SUMS");
    std::fs::write(&sums, sums_lines(&pairs)).unwrap();
    let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
    assert!(error.to_string().contains("20261022/14"), "{error}");
    assert_eq!(watermark(&out), vec!["20261022/12".to_owned()]);
    assert!(
        !parquet_files(&out)
            .iter()
            .any(|path| path.contains("20261022-14")),
        "{:?}",
        parquet_files(&out)
    );
    assert!(
        out.join("train/date=2026-10-22/hour=20261022-12.parquet")
            .is_file()
    );
    assert!(!out.join("receipts/20261022/14.json").exists());
    assert_no_partial(&out);
}

#[test]
fn disk_floor_refuses_and_stops_between_hours() {
    let root = Tmp::new("disk");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let body = |hour_ms: i64| {
        block(
            1,
            "2026-10-22T00:00:00.000000000",
            vec![json!(["0xa", {"coin": "SOL", "px": "1", "sz": "1", "side": "B", "time": hour_ms, "tid": 1, "oid": 1}])],
        )
        .to_string()
    };
    let first = write_lz4(&input, "20261022", 0, &body(1_792_627_200_000));
    let second = write_lz4(&input, "20261022", 1, &body(1_792_630_800_000));
    let manifest = root.0.join("MANIFEST.json");
    std::fs::write(
        &manifest,
        json!({
            "files": [
                {"path": format!("hourly/{}.lz4", first.0), "sha256": first.1},
                {"path": format!("node_fills_by_block/hourly/{}.lz4", second.0), "sha256": second.1}
            ]
        })
        .to_string(),
    )
    .unwrap();
    let refused = run_with(&input, &out, &manifest, u64::MAX, |_| Ok(0)).unwrap();
    assert_eq!(refused.stop, Stop::DiskFloor);
    assert_eq!(refused.hours_verified, 0);
    assert!(!out.join("watermark.json").exists());
    assert!(parquet_files(&out).is_empty());
    let mut calls = 0u32;
    let stopped = run_with(&input, &out, &manifest, 1_000, |_| {
        calls += 1;
        if calls < 3 { Ok(10_000) } else { Ok(10) }
    })
    .unwrap();
    assert_eq!(stopped.stop, Stop::DiskFloor);
    assert_eq!(stopped.hours_verified, 1);
    assert_eq!(watermark(&out), vec!["20261022/0".to_owned()]);
    assert!(
        out.join("train/date=2026-10-22/hour=20261022-0.parquet")
            .is_file()
    );
    assert!(
        !out.join("train/date=2026-10-22/hour=20261022-1.parquet")
            .exists()
    );
    assert_no_partial(&out);
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
    let input = root.0.join("in");
    let out = root.0.join("out");
    let body = block(
        1,
        "2026-10-22T00:00:00.000000000",
        vec![json!(["0xa", {"coin": "BTC", "px": "1", "sz": "1", "side": "B", "time": 1_792_627_200_000_i64, "tid": 1, "oid": 1}])],
    )
    .to_string();
    let pair = write_lz4(&input, "20261022", 0, &body);
    let sums = root.0.join("SHA256SUMS");
    std::fs::write(&sums, sums_lines(&[pair])).unwrap();
    let code = crate::cli::main(&[
        "augment".into(),
        "hl-fills".into(),
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
        "hl-fills".into(),
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
    assert!(parquet_files(&refused).is_empty());
}

fn one_fill(px: &str, sz: &str) -> String {
    block(
        1,
        "2026-10-22T00:00:00.000000000",
        vec![json!([
            "0xa",
            {
                "coin": "BTC",
                "px": px,
                "sz": sz,
                "side": "B",
                "time": 1_792_627_200_000_i64,
                "tid": 1,
                "oid": 1
            }
        ])],
    )
    .to_string()
}

fn stage(root: &Tmp, body: &str) -> (PathBuf, PathBuf, PathBuf) {
    let input = root.0.join("in");
    let out = root.0.join("out");
    let sums = root.0.join("SHA256SUMS");
    let pair = write_lz4(&input, "20261022", 0, body);
    std::fs::write(&sums, sums_lines(&[pair])).unwrap();
    (input, out, sums)
}

struct Reset;

impl Reset {
    fn arm() -> Self {
        super::publish::seams::clear_count_seam();
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        crate::fsutil::clear_fsync_failure();
        Self
    }
}

impl Drop for Reset {
    fn drop(&mut self) {
        super::publish::seams::clear_count_seam();
        super::publish::seams::clear_before_receipt();
        super::batch::clear_before_watermark();
        crate::fsutil::clear_fsync_failure();
    }
}

fn watermark_hours(out: &Path) -> Vec<String> {
    if !out.join("watermark.json").is_file() {
        return Vec::new();
    }
    watermark(out)
}

#[test]
fn an_hour_missing_from_the_manifest_fails_the_hour() {
    let root = Tmp::new("no-sum");
    let input = root.0.join("in");
    let out = root.0.join("out");
    let _missing = write_lz4(&input, "20261022", 0, &one_fill("1", "1"));
    let listed = write_lz4(&input, "20261022", 1, &one_fill("2", "2"));
    let sums = root.0.join("SHA256SUMS");
    std::fs::write(&sums, sums_lines(&[listed])).unwrap();
    let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
    assert!(
        error.to_string().contains("20261022/0: no checksum"),
        "{error}"
    );
    assert!(
        !watermark_hours(&out).contains(&"20261022/0".to_owned()),
        "a missing checksum must not be skipped"
    );
}

#[test]
fn a_missing_manifest_file_fails_closed() {
    let root = Tmp::new("no-manifest");
    let input = root.0.join("in");
    let out = root.0.join("out");
    write_lz4(&input, "20261022", 0, &one_fill("1", "1"));
    let sums = root.0.join("no-such-SHA256SUMS");
    let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("--checksums is not a SHA256SUMS file or a JSON manifest"),
        "{error}"
    );
    assert!(!out.join("watermark.json").exists());
    assert!(parquet_files(&out).is_empty());
}

#[test]
fn fsync_failures_leave_the_watermark_and_a_rerun_succeeds() {
    use crate::fsutil::{FsyncStep, inject_fsync_failure};
    // The extra file check is what goes red when that site's fsync is removed and a later
    // write_durable consumes the same one-shot injection instead.
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
        let (input, out, sums) = stage(&root, &one_fill("1", "1"));
        arm();
        let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
        assert!(
            error.to_string().contains("fsync failed"),
            "{label}: {error}"
        );
        assert!(watermark_hours(&out).is_empty(), "{label}: watermark moved");
        check(&out);
        let again = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
        assert_eq!(again.stop, Stop::Done, "{label}");
        assert_eq!(again.hours_verified, 1, "{label}");
        assert_eq!(watermark_hours(&out), vec!["20261022/0".to_owned()]);
    }
}

#[test]
fn a_dropped_row_fails_each_count_guard_without_a_watermark() {
    use super::publish::seams::{CountSeam, set_count_seam};
    for seam in [CountSeam::Line, CountSeam::Hour, CountSeam::Staged] {
        let _reset = Reset::arm();
        set_count_seam(seam);
        let root = Tmp::new("count");
        let (input, out, sums) = stage(&root, &one_fill("1", "1"));
        let error = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap_err();
        let message = error.to_string();
        match seam {
            CountSeam::Line => assert!(message.contains("line "), "{seam:?}: {message}"),
            CountSeam::Hour => assert!(message.contains("!= parquet rows"), "{seam:?}: {message}"),
            CountSeam::Staged => assert!(message.contains("staged"), "{seam:?}: {message}"),
        }
        assert!(
            watermark_hours(&out).is_empty(),
            "{seam:?}: watermark moved"
        );
    }
}

#[test]
fn a_changed_output_or_input_or_config_hash_is_rewritten() {
    let root = Tmp::new("redo");
    let (input, out, sums) = stage(&root, &one_fill("1", "1"));
    run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    let parquet = out.join("train/date=2026-10-22/hour=20261022-0.parquet");
    let receipt_path = out.join("receipts/20261022/0.json");

    std::fs::write(&parquet, b"truncated").unwrap();
    let redone = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(redone.hours_verified, 1, "truncated output was skipped");
    assert_eq!(redone.hours_skipped, 0);
    assert!(parquet.metadata().unwrap().len() > 8);

    let replacement = one_fill("9", "9");
    let pair = write_lz4(&input, "20261022", 0, &replacement);
    std::fs::write(&sums, sums_lines(std::slice::from_ref(&pair))).unwrap();
    let redone = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(redone.hours_verified, 1, "changed input was skipped");
    let receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["input_sha256"], pair.1);

    let mut edited: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    edited["config_hash"] = json!("0".repeat(64));
    std::fs::write(&receipt_path, edited.to_string()).unwrap();
    let redone = run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    assert_eq!(redone.hours_verified, 1, "changed config hash was skipped");
    let receipt: Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["config_hash"], super::config_hash());
}

#[test]
fn decimal_strings_round_trip_as_identical_f64_bits() {
    let root = Tmp::new("bits");
    let (input, out, sums) = stage(&root, &one_fill("0.1", "0.3"));
    run_with(&input, &out, &sums, 1, |_| Ok(u64::MAX)).unwrap();
    let rows = read_rows(&out.join("train/date=2026-10-22/hour=20261022-0.parquet"));
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("px").unwrap().as_f64().unwrap().to_bits(),
        "0.1".parse::<f64>().unwrap().to_bits()
    );
    assert_eq!(
        rows[0].get("sz").unwrap().as_f64().unwrap().to_bits(),
        "0.3".parse::<f64>().unwrap().to_bits()
    );
}
