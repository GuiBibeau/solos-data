//! Absent dump days (ADR-0024) against an in-process server: a hole between published days is
//! marked, counted and asked for again on a bounded schedule; a day before a listing is a leading
//! gap and is not; a file the venue publishes late fills the hole and clears the mark; a failed or
//! killed fill leaves no registered file and the next run retries. Binance `aggTrades` goes
//! through the newest-first walk, `klines` through the listing, and both are covered, so a day
//! older than the newest registered file is reached. One test runs the real binary and reads its
//! log and its run status.

mod augment_common;

use augment_common::{Response, Server, checksum_of, listing_xml, tempdir, zip_of};
use serde_json::Value;
use solos_data::augment::config::{
    AbsentReprobe, AugmentConfig, load_augment_config, repository_config_path, write_augment_config,
};
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::sync::{Filter, run_sync};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

const DAY_MS: i64 = 86_400_000;
const AGG_KEY: &str = "binance/aggTrades/SOLUSDT";
const KLINES_KEY: &str = "binance/klines/SOLUSDT";

/// Noon UTC of 2026-10-`day`; 2026-10-07T00:00:00Z is 1_791_331_200_000.
fn noon(day: i64) -> i64 {
    1_791_331_200_000 + (day - 7) * DAY_MS + DAY_MS / 2
}

fn day_label(day: i64) -> String {
    format!("2026-10-{day:02}")
}

fn config_for(server: &Server, root: &Path, schedule: AbsentReprobe) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.start_date = "2026-10-08".into();
    config.requests_per_second = 50.0;
    config.disk_budget_gb = 1.0;
    config.absent_reprobe = schedule;
    config.symbols.retain(|s| s.phoenix == "SOL");
    let s = &mut config.sources;
    for enabled in [
        &mut s.hyperliquid.enabled,
        &mut s.deribit.enabled,
        &mut s.defillama.enabled,
        &mut s.bybit.enabled,
        &mut s.elfa.enabled,
        &mut s.sec.enabled,
        &mut s.alternative.enabled,
        &mut s.polymarket.enabled,
        &mut s.kalshi.enabled,
        &mut s.phoenix.enabled,
        &mut s.tardis.enabled,
        &mut s.okx.enabled,
        &mut s.hyperliquid_s3.enabled,
    ] {
        *enabled = false;
    }
    s.binance.files_url = format!("{}/files", server.base);
    s.binance.list_url = format!("{}/list", server.base);
    s.binance.datasets.clear();
    s.binance.large_datasets = vec!["aggTrades".into()];
    s.binance.walk.start_date = "2026-10-08".into();
    s.binance.walk.budget_gb = 1.0;
    config
}

fn run(config: &AugmentConfig, now_ms: i64) -> Value {
    run_sync(
        config,
        now_ms,
        &Filter::default(),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap()
    .to_value()
}

fn binance(status: &Value) -> &Value {
    &status["sources"]["binance"]
}

fn rows(config: &AugmentConfig, sql: &str) -> Vec<Value> {
    solos_data::augment::query::query_augment(&config.data_dir, sql)
        .unwrap()
        .to_value()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

/// A series' progress record, read from the ledger of a finished run.
fn progress(root: &Path, key: &str) -> Value {
    let store = ledger::open(root, Lane::Sync).unwrap();
    let value = ledger::get_progress(&store, key).unwrap();
    store.close().unwrap();
    value.unwrap_or(Value::Null)
}

fn absent_labels(root: &Path, key: &str) -> Vec<String> {
    progress(root, key)["absent"]
        .as_array()
        .map(|labels| {
            labels
                .iter()
                .filter_map(|l| l.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn agg_path(day: i64) -> String {
    let label = day_label(day);
    format!("/files/data/futures/um/daily/aggTrades/SOLUSDT/SOLUSDT-aggTrades-{label}.zip")
}

fn serve_zip(server: &Server, path: &str, csv: &str) {
    let name = path.rsplit('/').next().unwrap().replace(".zip", ".csv");
    let zip = zip_of(&name, csv.as_bytes());
    server.serve_bytes(&format!("{path}.CHECKSUM"), checksum_of(&zip, &name));
    server.serve_bytes(path, zip);
}

/// One trade on `day`, published with its checksum.
fn serve_agg(server: &Server, day: i64) {
    let ms = noon(day) - DAY_MS / 2;
    let csv = format!(
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
         {day},120.5,3,11,12,{ms},true\n"
    );
    serve_zip(server, &agg_path(day), &csv);
}

/// Binance lists SOLUSDT from 10-10 (10-08 and 10-09 are before the listing), publishes 10-10 to
/// 10-12, skips 10-13 and publishes 10-14 to 10-16. The first run is on 10-14, the second on
/// 10-17, three days after 10-13 ended, which is when the hole is marked.
fn aggtrades_with_a_hole(server: &Server, root: &Path, schedule: AbsentReprobe) -> AugmentConfig {
    let config = config_for(server, root, schedule);
    for day in 10..=12 {
        serve_agg(server, day);
    }
    let first = run(&config, noon(14));
    let first = binance(&first);
    assert_eq!(first["files"], 3, "{first}");
    assert_eq!(first["errors"], 0, "{first}");
    // 10-08 and 10-09 are marked, and are leading: nothing older is held.
    assert_eq!(first["absentMarked"], 2, "{first}");
    assert_eq!(first["absentInterior"], 0, "{first}");
    for day in 14..=16 {
        serve_agg(server, day);
    }
    config
}

#[test]
fn an_interior_hole_is_counted_and_a_leading_gap_is_not() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let config = aggtrades_with_a_hole(&server, &root, AbsentReprobe::default());
    assert_eq!(
        absent_labels(&root, AGG_KEY),
        ["2026-10-08", "2026-10-09"],
        "the leading gap is recorded"
    );
    // The first run asked for 10-13 once, as a day that may not be published yet.
    assert_eq!(server.hits_of(&agg_path(13)), 1);
    let second = run(&config, noon(17));
    let second = binance(&second);
    assert_eq!(second["files"], 3, "{second}");
    assert_eq!(second["errors"], 0, "{second}");
    assert_eq!(second["absentMarked"], 1, "{second}");
    assert_eq!(second["absentInterior"], 1, "{second}");
    assert_eq!(second["absentGivenUp"], 0, "{second}");
    assert_eq!(second["absentFilled"], 0, "{second}");
    assert_eq!(
        absent_labels(&root, AGG_KEY),
        ["2026-10-08", "2026-10-09", "2026-10-13"]
    );
    // The leading days are not asked for again; the hole was asked for once more, when it was
    // marked.
    assert_eq!(server.hits_of(&agg_path(8)), 1);
    assert_eq!(server.hits_of(&agg_path(9)), 1);
    assert_eq!(server.hits_of(&agg_path(13)), 2);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_hole_the_venue_publishes_late_is_filled_and_its_mark_cleared() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let config = aggtrades_with_a_hole(&server, &root, AbsentReprobe::default());
    run(&config, noon(17));
    let marked_at = server.hits_of(&agg_path(13));
    // The same day: not asked again.
    let same_day = run(&config, noon(17) + 3_600_000);
    assert_eq!(binance(&same_day)["absentInterior"], 1, "{same_day}");
    assert_eq!(binance(&same_day)["absentMarked"], 0, "{same_day}");
    assert_eq!(server.hits_of(&agg_path(13)), marked_at);
    // The venue publishes it; a day after it was marked it is asked for again and stored.
    serve_agg(&server, 13);
    let filled = run(&config, noon(18));
    let filled = binance(&filled);
    assert_eq!(filled["files"], 1, "{filled}");
    assert_eq!(filled["errors"], 0, "{filled}");
    assert_eq!(filled["absentFilled"], 1, "{filled}");
    assert_eq!(filled["absentInterior"], 0, "{filled}");
    assert_eq!(filled["absentMarked"], 0, "{filled}");
    assert_eq!(absent_labels(&root, AGG_KEY), ["2026-10-08", "2026-10-09"]);
    let days = rows(
        &config,
        "SELECT count(*) AS n, count(*) FILTER (WHERE ts >= '2026-10-13' AND ts < '2026-10-14') AS hole \
         FROM binance_aggTrades",
    );
    assert_eq!(days[0]["n"], "7");
    assert_eq!(days[0]["hole"], "1");
    // Nothing asks for it again.
    let asked = server.hits_of(&agg_path(13));
    run(&config, noon(19));
    assert_eq!(server.hits_of(&agg_path(13)), asked);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_reprobe_that_keeps_returning_404_respects_the_bound() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    // Two probes a day apart, then one a week later, then none.
    let schedule = AbsentReprobe {
        daily_probes: 2,
        weekly_probes: 1,
        ..AbsentReprobe::default()
    };
    let config = aggtrades_with_a_hole(&server, &root, schedule);
    let marked = run(&config, noon(17));
    assert_eq!(binance(&marked)["absentInterior"], 1, "{marked}");
    let at_mark = server.hits_of(&agg_path(13));
    for (now, probes) in [
        (noon(18), 1),
        (noon(19), 2),
        // The weekly probe waits a week from the last one.
        (noon(20), 2),
        (noon(25), 2),
    ] {
        let status = run(&config, now);
        assert_eq!(binance(&status)["absentInterior"], 1, "{status}");
        assert_eq!(binance(&status)["absentGivenUp"], 0, "{status}");
        assert_eq!(server.hits_of(&agg_path(13)), at_mark + probes, "at {now}");
    }
    // The weekly probe is the last miss of the schedule. The hole stays absent, counted as
    // given up (a permanent gap of the venue), and is never asked for again.
    let spent = run(&config, noon(26));
    assert_eq!(binance(&spent)["absentInterior"], 0, "{spent}");
    assert_eq!(binance(&spent)["absentGivenUp"], 1, "{spent}");
    assert_eq!(binance(&spent)["errors"], 0, "{spent}");
    assert_eq!(server.hits_of(&agg_path(13)), at_mark + 3);
    for now in [noon(27), noon(40), noon(90)] {
        let status = run(&config, now);
        assert_eq!(binance(&status)["absentGivenUp"], 1, "{status}");
        assert_eq!(server.hits_of(&agg_path(13)), at_mark + 3, "at {now}");
    }
    assert!(absent_labels(&root, AGG_KEY).contains(&"2026-10-13".to_owned()));
    assert_eq!(progress(&root, AGG_KEY)["reprobe"]["2026-10-13"]["n"], 3);
    let held = rows(
        &config,
        "SELECT count(*) AS n FROM binance_aggTrades WHERE ts >= '2026-10-13' AND ts < '2026-10-14'",
    );
    assert_eq!(held[0]["n"], "0");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_mark_written_before_the_schedule_existed_is_asked_for_again_at_once() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let config = aggtrades_with_a_hole(&server, &root, AbsentReprobe::default());
    run(&config, noon(17));
    // ADR-0009 stored the labels only: no probe history.
    let mut store = ledger::open(&root, Lane::Sync).unwrap();
    ledger::set_progress(
        &mut store,
        AGG_KEY,
        &serde_json::json!({
            "absent": ["2026-10-08", "2026-10-09", "2026-10-13"],
            "updatedAt": "2026-10-28T00:00:00.000Z",
        }),
    )
    .unwrap();
    store.close().unwrap();
    serve_agg(&server, 13);
    let status = run(&config, noon(17) + 60_000);
    let status = binance(&status);
    assert_eq!(status["absentFilled"], 1, "{status}");
    assert_eq!(status["absentInterior"], 0, "{status}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_failed_or_killed_fill_registers_nothing_and_the_next_run_retries_cleanly() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let config = aggtrades_with_a_hole(&server, &root, AbsentReprobe::default());
    run(&config, noon(17));
    let hole_file = root.join("tables/binance/aggTrades/SOLUSDT/2026-10-13.parquet");
    // The venue publishes the day with a good checksum and a CSV the lane cannot type.
    serve_zip(
        &server,
        &agg_path(13),
        "agg_trade_id,price\nnot-a-number,x\n",
    );
    let failed = run(&config, noon(18));
    let failed = binance(&failed);
    assert_eq!(failed["errors"], 1, "{failed}");
    assert_eq!(failed["absentFilled"], 0, "{failed}");
    assert_eq!(failed["absentInterior"], 1, "{failed}");
    assert!(!hole_file.exists(), "no file is published for the day");
    assert!(absent_labels(&root, AGG_KEY).contains(&"2026-10-13".to_owned()));
    let none = rows(
        &config,
        "SELECT count(*) AS n FROM binance_aggTrades WHERE ts >= '2026-10-13' AND ts < '2026-10-14'",
    );
    assert_eq!(none[0]["n"], "0");
    // A kill after the rename and before the registration leaves the file, its temporary
    // sibling and a staged CSV on disk and nothing in the ledger.
    std::fs::write(&hole_file, b"killed mid-fill").unwrap();
    std::fs::write(hole_file.with_extension("parquet.tmp"), b"partial").unwrap();
    std::fs::write(root.join("staging/leftover.csv"), b"1,2\n").unwrap();
    serve_agg(&server, 13);
    let retried = run(&config, noon(19));
    assert!(
        retried["recovered"].as_i64().unwrap() >= 3,
        "the leftovers are removed at start: {retried}"
    );
    let retried_binance = binance(&retried);
    assert_eq!(retried_binance["errors"], 0, "{retried}");
    assert_eq!(retried_binance["absentFilled"], 1, "{retried}");
    assert_eq!(retried_binance["absentInterior"], 0, "{retried}");
    assert!(!absent_labels(&root, AGG_KEY).contains(&"2026-10-13".to_owned()));
    let filled = rows(
        &config,
        "SELECT count(*) AS n, min(agg_trade_id) AS id FROM binance_aggTrades WHERE ts >= '2026-10-13' AND ts < '2026-10-14'",
    );
    assert_eq!(filled[0]["n"], "1");
    assert_eq!(filled[0]["id"], "13");
    assert!(!root.join("staging/leftover.csv").exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_run_asks_for_at_most_max_per_run_holes() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let schedule = AbsentReprobe {
        max_per_run: 1,
        ..AbsentReprobe::default()
    };
    let config = config_for(&server, &root, schedule);
    for day in [10, 11, 12, 14, 16, 17] {
        serve_agg(&server, day);
    }
    // Two holes, 10-13 and 10-15, both three days old on 10-19.
    let marked = run(&config, noon(19));
    let marked = binance(&marked);
    assert_eq!(marked["absentInterior"], 2, "{marked}");
    for day in [13, 15] {
        serve_agg(&server, day);
    }
    // One hole a run, newest first.
    let first = run(&config, noon(20));
    assert_eq!(binance(&first)["absentFilled"], 1, "{first}");
    assert_eq!(binance(&first)["absentInterior"], 1, "{first}");
    assert_eq!(
        absent_labels(&root, AGG_KEY).len(),
        3,
        "10-08, 10-09, 10-13"
    );
    let second = run(&config, noon(21));
    assert_eq!(binance(&second)["absentFilled"], 1, "{second}");
    assert_eq!(binance(&second)["absentInterior"], 0, "{second}");
    let _ = std::fs::remove_dir_all(&root);
}

fn klines_key(day: i64) -> String {
    let label = day_label(day);
    format!("data/futures/um/daily/klines/SOLUSDT/1m/SOLUSDT-1m-{label}.zip")
}

fn serve_kline(server: &Server, day: i64) {
    let ms = noon(day) - DAY_MS / 2;
    let csv = format!(
        "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n\
         {ms},1,2,0.5,1.5,10,{},100,5,4,40,0\n",
        ms + 59_999
    );
    serve_zip(server, &format!("/files/{}", klines_key(day)), &csv);
}

#[test]
fn a_hole_below_the_newest_file_of_a_listed_dataset_is_found_and_filled() {
    let server = Server::start();
    let root = tempdir("solos-absent");
    let mut config = config_for(&server, &root, AbsentReprobe::default());
    config.sources.binance.datasets = vec!["klines".into()];
    config.sources.binance.large_datasets.clear();
    // The bucket's listing: what Binance has published, in key order.
    let listed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let keys = Arc::clone(&listed);
    server.route("/list", move |request| {
        let prefix = request.param("prefix").unwrap_or_default();
        let marker = request.param("marker").unwrap_or_default();
        let keys: Vec<String> = keys
            .lock()
            .unwrap()
            .iter()
            .filter(|key| key.starts_with(&prefix) && **key > marker)
            .cloned()
            .collect();
        Response::ok(listing_xml(&prefix, &keys, false, None))
    });
    let publish = |day: i64| {
        serve_kline(&server, day);
        let mut listed = listed.lock().unwrap();
        listed.push(klines_key(day));
        listed.push(format!("{}.CHECKSUM", klines_key(day)));
    };
    for day in 10..=12 {
        publish(day);
    }
    let first = run(&config, noon(14));
    assert_eq!(binance(&first)["files"], 3, "{first}");
    assert_eq!(binance(&first)["absentMarked"], 0, "{first}");
    assert_eq!(binance(&first)["absentInterior"], 0, "{first}");
    // 10-13 is skipped; the days after it are listed and fetched, so the listing marker moves
    // past the hole.
    for day in 14..=16 {
        publish(day);
    }
    let second = run(&config, noon(17));
    let second_binance = binance(&second);
    assert_eq!(second_binance["files"], 3, "{second}");
    assert_eq!(second_binance["errors"], 0, "{second}");
    // Only the hole is marked: the days before the listing are a leading gap and are not.
    assert_eq!(second_binance["absentMarked"], 1, "{second}");
    assert_eq!(second_binance["absentInterior"], 1, "{second}");
    assert_eq!(absent_labels(&root, KLINES_KEY), ["2026-10-13"]);
    let path = format!("/files/{}", klines_key(13));
    assert_eq!(server.hits_of(&path), 1);
    // The same day it is not asked again.
    run(&config, noon(17) + 3_600_000);
    assert_eq!(server.hits_of(&path), 1);
    // The venue publishes it late; the listing never shows it again (it starts after the newest
    // file), the direct ask finds it.
    serve_kline(&server, 13);
    let filled = run(&config, noon(18));
    let filled = binance(&filled);
    assert_eq!(filled["files"], 1, "{filled}");
    assert_eq!(filled["absentFilled"], 1, "{filled}");
    assert_eq!(filled["absentInterior"], 0, "{filled}");
    assert!(absent_labels(&root, KLINES_KEY).is_empty());
    let days = rows(
        &config,
        "SELECT count(DISTINCT ts::DATE) AS n FROM binance_klines",
    );
    assert_eq!(days[0]["n"], "7");
    let _ = std::fs::remove_dir_all(&root);
}

/// `YYYY-MM-DD` of a date, and the instant `hours` into it.
fn at_hour(date: chrono::NaiveDate, hours: u32) -> i64 {
    date.and_hms_opt(hours, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

#[test]
fn the_binary_logs_an_interior_hole_and_reports_it_in_its_run_summary() {
    use chrono::{Days, Utc};
    let server = Server::start();
    let root = tempdir("solos-absent");
    let today = Utc::now().date_naive();
    let hole = today - Days::new(10);
    let mut config = config_for(&server, &root, AbsentReprobe::default());
    config.sources.binance.walk.start_date = (hole - Days::new(3)).to_string();
    // Binance published the three days before the hole and every day from the one after it
    // until it was four days old; the hole itself never.
    let published: Vec<chrono::NaiveDate> = (1..=3)
        .map(|n| hole - Days::new(n))
        .chain((1..=6).map(|n| hole + Days::new(n)))
        .collect();
    for date in &published {
        let label = date.to_string();
        let path =
            format!("/files/data/futures/um/daily/aggTrades/SOLUSDT/SOLUSDT-aggTrades-{label}.zip");
        let csv = format!(
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
             1,120.5,3,11,12,{},true\n",
            at_hour(*date, 1)
        );
        serve_zip(&server, &path, &csv);
    }
    // The day after the hole, the older days are stored and the hole is not yet three days old.
    let first = run(&config, at_hour(hole + Days::new(1), 12));
    assert_eq!(binance(&first)["files"], 3, "{first}");
    assert_eq!(binance(&first)["absentMarked"], 0, "{first}");
    assert_eq!(binance(&first)["absentInterior"], 0, "{first}");
    // Today the binary walks the newer days, then finds the hole between stored days.
    let config_path = root.join("augment-test.json");
    write_augment_config(&config_path, &config).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_solos-data"))
        .args(["augment", "sync", "--config"])
        .arg(&config_path)
        .env("SOLOS_DATA_AUGMENT_DIR", &root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    let lines: Vec<Value> = stdout
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let event = |name: &str| -> Vec<&Value> {
        lines
            .iter()
            .filter(|line| line["event"] == name)
            .collect::<Vec<_>>()
    };
    let hole_label = hole.to_string();
    let marked: Vec<&Value> = event("augment_absent_marked")
        .into_iter()
        .filter(|line| line["period"] == hole_label.as_str())
        .collect();
    assert_eq!(marked.len(), 1, "{stdout}");
    assert_eq!(marked[0]["position"], "interior");
    assert_eq!(marked[0]["reason"], "not_found");
    assert_eq!(marked[0]["dataset"], "aggTrades");
    assert_eq!(marked[0]["symbol"], "SOLUSDT");
    assert!(
        !marked[0].to_string().contains("http"),
        "no URL in the line: {}",
        marked[0]
    );
    // The run summary line and the printed status both carry the hole.
    let done = event("augment_sync_done");
    assert_eq!(done.len(), 1, "{stdout}");
    assert_eq!(done[0]["errors"], 0, "{}", done[0]);
    assert_eq!(done[0]["absentInterior"], 1, "{}", done[0]);
    let status = lines.last().unwrap();
    assert_eq!(binance(status)["absentInterior"], 1, "{status}");
    assert_eq!(binance(status)["errors"], 0, "{status}");
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    let _ = std::fs::remove_dir_all(&root);
}
