//! The capture lane against an in-process HTTP server: Hyperliquid candles merged into day
//! files across cycles, asset contexts buffered and flushed, Elfa streams with the credit
//! guard, and Bybit funding through the sync driver. Everything runs with a fixed clock.

mod augment_common;

use augment_common::{Response, Server, fixture, tempdir};
use serde_json::{Value, json};
use solos_data::augment::candles::{self, CandleLane};
use solos_data::augment::config::{load_augment_config, repository_config_path};
use solos_data::augment::contexts::{self, ContextLane};
use solos_data::augment::elfa::{self, ElfaLane};
use solos_data::augment::http::Http;
use solos_data::augment::ledger::{self, Lane};
use solos_data::augment::series::Ctx;
use solos_data::db::Db;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;

fn ctx(server: &Server, root: &std::path::Path) -> (Ctx, solos_data::db::DbThread) {
    let store = ledger::open(root, Lane::Capture).unwrap();
    let mut store = store;
    ledger::recover(&mut store, root, Lane::Capture).unwrap();
    let (db, thread) = Db::spawn(store);
    let http = Http::new(50.0).unwrap();
    http.set_host_rate(&server.base, 50.0);
    (
        Ctx {
            http,
            db,
            root: root.to_path_buf(),
            lane: Lane::Capture,
            start: solos_data::augment::periods::parse_date("2026-10-01").unwrap(),
            now_ms: NOW_MS,
            stop: Arc::new(AtomicBool::new(false)),
            disk_budget_bytes: 0,
        },
        thread,
    )
}

fn rows(root: &std::path::Path, sql: &str) -> Vec<Value> {
    solos_data::augment::query::query_augment(root, sql)
        .unwrap()
        .to_value()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

fn candle(coin: &str, open: i64) -> Value {
    json!({ "t": open, "T": open + 59_999, "s": coin, "i": "1m", "o": "100", "c": "101", "h": "102", "l": "99", "v": "5", "n": 3 })
}

#[test]
fn candles_merge_into_day_files_across_cycles() {
    let server = Server::start();
    // Candles from 23:58 the day before to now; the last one is still open.
    server.route("/info", |request| {
        let body = request.json();
        assert_eq!(body["type"], "candleSnapshot");
        let coin = body["req"]["coin"].as_str().unwrap().to_owned();
        let start = body["req"]["startTime"].as_i64().unwrap();
        let end = body["req"]["endTime"].as_i64().unwrap();
        let first = NOW_MS - 2 * 60_000 - 86_400_000 - 2 * 60_000;
        let out: Vec<Value> = (0..1_500)
            .map(|i| first + i * 60_000)
            .filter(|t| *t >= start && *t <= end)
            .map(|t| candle(&coin, t))
            .collect();
        Response::json(&Value::Array(out))
    });
    let root = tempdir("solos-capture");
    let (ctx, thread) = ctx(&server, &root);
    let lane = CandleLane {
        url: format!("{}/info", server.base),
        coins: vec![
            ("SOL".into(), "SOL".into()),
            ("xyz:NVDA".into(), "NVDA".into()),
        ],
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let first = runtime.block_on(candles::cycle(&ctx, &lane, NOW_MS));
    assert_eq!(first.errors, 0);
    assert_eq!(first.files, 4, "two coins, yesterday and today");
    let root_clone = root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let per_day = rows(
        &root,
        "SELECT symbol, phoenix_symbol, count(*) AS n, min(ts)::VARCHAR AS first FROM hyperliquid_candles_1m GROUP BY 1, 2 ORDER BY 1",
    );
    assert_eq!(per_day.len(), 2);
    assert_eq!(per_day[0]["phoenix_symbol"], "SOL");
    // 1,445 candles from 11:56 yesterday to 12:00 today were served; the one opening at
    // 12:00 is still open, so 1,444 per coin are stored.
    let total: i64 = per_day
        .iter()
        .map(|r| r["n"].as_str().unwrap().parse::<i64>().unwrap())
        .sum();
    assert_eq!(total, 2 * 1_444);
    // Second cycle, half an hour later: only the new candles are fetched and merged.
    let hits_before = server.hit_count();
    let second = runtime.block_on(candles::cycle(&ctx, &lane, NOW_MS + 1_800_000));
    assert_eq!(second.files, 2);
    assert_eq!(second.rows, 2 * 30);
    let requests: Vec<_> = server.hits().into_iter().skip(hits_before).collect();
    assert!(
        requests
            .iter()
            .all(|r| r.json()["req"]["startTime"].as_i64().unwrap() >= NOW_MS - 120_000)
    );
    let root_clone = root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let today = rows(
        &root,
        "SELECT count(*) AS n, count(DISTINCT open_time_ms) AS distinct_n FROM hyperliquid_candles_1m WHERE symbol = 'SOL' AND ts >= '2026-10-09'",
    );
    assert_eq!(today[0]["n"], today[0]["distinct_n"]);
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog-capture.json")).unwrap())
            .unwrap();
    let files = catalog["files"].as_array().unwrap();
    assert_eq!(files.len(), 4);
    assert!(
        files
            .iter()
            .any(|f| f["period"] == "2026-10-08" && f["complete"] == true)
    );
    assert!(
        files
            .iter()
            .any(|f| f["period"] == "2026-10-09" && f["complete"] == false)
    );
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn contexts_are_buffered_then_flushed_per_day() {
    let server = Server::start();
    let answer: Value = serde_json::from_slice(&fixture("hl-ctxs.json")).unwrap();
    server.route("/info", move |request| {
        let body = request.json();
        assert_eq!(body["type"], "metaAndAssetCtxs");
        let dex = body.get("dex").and_then(Value::as_str).unwrap_or("");
        let mut answer = answer.clone();
        if dex == "xyz" {
            answer[0]["universe"][0]["name"] = json!("xyz:NVDA");
        }
        Response::json(&answer)
    });
    let root = tempdir("solos-capture");
    let (ctx, thread) = ctx(&server, &root);
    let lane = ContextLane::new(
        &format!("{}/info", server.base),
        vec![
            ("BTC".into(), "BTC".into()),
            ("xyz:NVDA".into(), "NVDA".into()),
        ],
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        assert_eq!(contexts::snapshot(&ctx, &lane, NOW_MS).await.unwrap(), 2);
        assert_eq!(
            contexts::snapshot(&ctx, &lane, NOW_MS + 60_000)
                .await
                .unwrap(),
            2
        );
        assert_eq!(lane.buffered(), 4);
        let outcome = contexts::flush(&ctx, &lane, NOW_MS + 60_000).await;
        assert_eq!(outcome.files, 1);
        assert_eq!(outcome.rows, 4);
        assert_eq!(lane.buffered(), 0);
        // A later snapshot merges into the same day file without duplicating.
        contexts::snapshot(&ctx, &lane, NOW_MS + 120_000)
            .await
            .unwrap();
        contexts::flush(&ctx, &lane, NOW_MS + 120_000).await;
    });
    let root_clone = root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let out = rows(
        &root,
        "SELECT symbol, phoenix_symbol, count(*) AS n, max(mark_px) AS mark FROM hyperliquid_asset_contexts GROUP BY 1, 2 ORDER BY 1",
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["symbol"], "BTC");
    assert_eq!(out[0]["n"], "3");
    assert_eq!(out[1]["phoenix_symbol"], "NVDA");
    assert_eq!(out[1]["mark"], 82091.0);
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn elfa_pulls_streams_and_the_credit_guard_disables_the_lane() {
    let server = Server::start();
    // Credits stay put until `billing` is set; then every key-status call shows one more used.
    let used = Arc::new(std::sync::atomic::AtomicI64::new(199));
    let billing = Arc::new(AtomicBool::new(false));
    let (used_server, billing_server) = (Arc::clone(&used), Arc::clone(&billing));
    server.route("/v3/key-status", move |request| {
        assert_eq!(request.header("x-elfa-api-key"), Some("test-key"));
        if billing_server.load(std::sync::atomic::Ordering::SeqCst) {
            used_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Response::json(&json!({ "credits": { "used": used_server.load(std::sync::atomic::Ordering::SeqCst), "remaining": 1 }, "historyFrom": NOW_MS / 1000 - 30 * 86_400 }))
    });
    let now_s = NOW_MS / 1000;
    server.route("/v3/events", move |request| {
        let from: i64 = request.param("from").unwrap().parse().unwrap();
        let to: i64 = request.param("to").unwrap().parse().unwrap();
        assert_eq!(request.param("order").as_deref(), Some("asc"));
        if request.param("cursor").is_none() {
            let events: Vec<Value> = (0..30)
                .map(|i| json!({ "id": format!("e{from}-{i}"), "firstSeenAt": from + i * 3600, "eventClass": "news", "primaryEntities": [{ "id": "a", "symbol": "SOL" }] }))
                .filter(|e| e["firstSeenAt"].as_i64().unwrap() <= to)
                .collect();
            Response::json(&json!({ "events": events, "nextCursor": "c2", "hasMore": true }))
        } else {
            Response::json(&json!({ "events": [{ "id": "e-last", "firstSeenAt": to, "eventClass": "news" }], "nextCursor": null, "hasMore": false }))
        }
    });
    server.route("/v3/calls", move |_| {
        Response::json(&json!({ "calls": [{ "id": "c1", "occurredAt": now_s - 100, "asset": { "symbol": "BTC" }, "callAction": "long" }], "hasMore": false }))
    });
    server.route("/v3/calls/episodes", move |_| {
        Response::json(&json!({ "episodes": [{ "id": "p1", "openedAt": now_s - 5000, "observedAt": now_s - 100, "status": "open", "track": { "alphaScore": 0.5 } }], "hasMore": false }))
    });
    server.route("/v3/market/crypto/call-book", move |_| {
        Response::json(&json!({ "bars": [{ "barAt": now_s - 3600, "longShare": 0.6, "shortShare": 0.4, "live": true }, { "barAt": now_s - 7200, "longShare": 0.5, "shortShare": 0.5, "live": false }], "hasMore": false }))
    });
    let root = tempdir("solos-capture");
    let (ctx, thread) = ctx(&server, &root);
    let mut lane = ElfaLane::new(&server.base, "test-key", now_s - 40 * 86_400);
    // One page per cycle: the events pull cannot finish, so its progress must still move to
    // the newest event received and the next cycle must resume there.
    lane.max_pages = 1;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let outcome = runtime.block_on(elfa::cycle(&ctx, &lane, now_s));
    assert_eq!(outcome.errors, 0);
    assert!(outcome.files >= 4);
    assert!(!lane.disabled());
    let events_hits = server.hits_of("/v3/events");
    let outcome = runtime.block_on(elfa::cycle(&ctx, &lane, now_s + 60));
    assert_eq!(outcome.errors, 0);
    let resumed: i64 = server
        .hits()
        .into_iter()
        .filter(|h| h.path == "/v3/events")
        .nth(events_hits)
        .and_then(|h| h.param("from"))
        .unwrap()
        .parse()
        .unwrap();
    // The first page held events at from, from + 1 h, ... from + 29 h; the next cycle resumes
    // one second before the newest, plus one.
    assert_eq!(resumed, now_s - 30 * 86_400 + 29 * 3600);
    lane.max_pages = elfa::MAX_PAGES;
    let first_from: i64 = server
        .hits()
        .iter()
        .find(|h| h.path == "/v3/events")
        .and_then(|h| h.param("from"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        first_from,
        now_s - 30 * 86_400,
        "historyFrom bounds the first pull"
    );
    let root_clone = root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let events = rows(
        &root,
        "SELECT count(*) AS n, count(DISTINCT id) AS ids FROM elfa_events",
    );
    // Two one-page cycles of thirty events each, all distinct.
    assert_eq!(events[0]["n"], "60");
    assert_eq!(events[0]["n"], events[0]["ids"]);
    let episodes = rows(&root, "SELECT id, status, alpha_score FROM elfa_episodes");
    assert_eq!(episodes[0]["alpha_score"], 0.5);
    let bars = rows(&root, "SELECT count(*) AS n FROM elfa_call_book");
    assert_eq!(bars[0]["n"], "2");
    // Second cycle: progress moved, so `from` follows the last `to`; credits moved, so the
    // lane disables itself.
    billing.store(true, std::sync::atomic::Ordering::SeqCst);
    let hits_before = server.hit_count();
    let outcome = runtime.block_on(elfa::cycle(&ctx, &lane, now_s + 3600));
    assert_eq!(outcome.errors, 0);
    assert!(lane.disabled());
    let second_from: i64 = server
        .hits()
        .into_iter()
        .skip(hits_before)
        .find(|h| h.path == "/v3/events")
        .and_then(|h| h.param("from"))
        .unwrap()
        .parse()
        .unwrap();
    // The second one-page cycle resumed at `resumed` and received events up to
    // resumed + 29 h; this cycle starts one second before that, plus one.
    assert_eq!(second_from, now_s - 30 * 86_400 + 58 * 3600);
    let root_clone = root.clone();
    ctx.db
        .run_blocking(move |store| ledger::write_catalog(store, &root_clone, Lane::Capture))
        .unwrap();
    let events = rows(
        &root,
        "SELECT count(*) AS n, count(DISTINCT id) AS ids FROM elfa_events",
    );
    // Thirty more from the first page and the final `e-last` row of the second page.
    assert_eq!(events[0]["n"], "91");
    assert_eq!(events[0]["n"], events[0]["ids"]);
    let outcome = runtime.block_on(elfa::cycle(&ctx, &lane, now_s + 7200));
    assert_eq!(outcome.skipped, 1);
    let store = thread.join(ctx.db.clone());
    store.close().unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn bybit_funding_is_a_paged_month_series() {
    let server = Server::start();
    server.route("/v5/market/funding/history", |request| {
        assert_eq!(request.param("category").as_deref(), Some("linear"));
        let symbol = request.param("symbol").unwrap();
        let start: i64 = request.param("startTime").unwrap().parse().unwrap();
        let end: i64 = request.param("endTime").unwrap().parse().unwrap();
        let step = 8 * 3_600_000;
        let mut list = Vec::new();
        let mut t = (end / step) * step;
        while t >= start && t < NOW_MS && list.len() < 200 {
            list.push(json!({ "symbol": symbol, "fundingRate": "0.0001", "fundingRateTimestamp": t.to_string() }));
            t -= step;
        }
        Response::json(&json!({ "retCode": 0, "retMsg": "OK", "result": { "category": "linear", "list": list } }))
    });
    let root = tempdir("solos-augment");
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.clone();
    config.start_date = "2026-09-01".into();
    config.requests_per_second = 50.0;
    config
        .symbols
        .retain(|s| s.phoenix == "SOL" || s.phoenix == "kSHIB");
    config.sources.bybit.base_url = server.base.clone();
    let status = solos_data::augment::sync::run_sync(
        &config,
        NOW_MS,
        &solos_data::augment::sync::Filter {
            source: Some("bybit".into()),
            symbol: None,
        },
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap()
    .to_value();
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    assert_eq!(
        status["sources"]["bybit"]["files"], 4,
        "two symbols, September and October"
    );
    let out = rows(
        &root,
        "SELECT symbol, phoenix_symbol, count(*) AS n FROM bybit_funding GROUP BY 1, 2 ORDER BY 1",
    );
    assert_eq!(out[0]["symbol"], "SHIB1000USDT");
    assert_eq!(out[0]["phoenix_symbol"], "kSHIB");
    // Every eight hours from September 1 00:00 through October 9 08:00, inclusive.
    assert_eq!(
        out[0]["n"],
        ((NOW_MS - 1_788_220_800_000) / (8 * 3_600_000) + 1).to_string()
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A gzip body.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn bybit_trade_dumps_walk_days_under_the_disk_budget() {
    let server = Server::start();
    let csv = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI\n\
1791331200.4902,SOLUSDT,Buy,0.5,120.640,ZeroMinusTick,430df55f-04cc-5b09-aff7-5051f01be39a,6.032e+09,0.5,60.32,0\n\
1791331201.268,SOLUSDT,Sell,0.1,120.630,MinusTick,838ff3c1-17cc-5963-8a91-180b422a927a,1.2063e+09,0.1,12.063,0\n";
    let legacy = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n\
1791244800.1,SOLUSDT,Buy,1,100,PlusTick,id-1,1e+08,1,100\n";
    // 10-04 has the legacy ten-column shape; 10-05 was never published and ended more than
    // three days ago, so it counts as a day without trades; 10-06 to 10-08 (yesterday) exist;
    // today is never asked for.
    server.serve_bytes(
        "/trading/SOLUSDT/SOLUSDT2026-10-04.csv.gz",
        gzip(legacy.as_bytes()),
    );
    for day in ["2026-10-06", "2026-10-07", "2026-10-08"] {
        server.serve_bytes(
            &format!("/trading/SOLUSDT/SOLUSDT{day}.csv.gz"),
            gzip(csv.as_bytes()),
        );
    }
    let root = tempdir("solos-augment");
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.clone();
    config.start_date = "2026-10-04".into();
    config.requests_per_second = 50.0;
    config.disk_budget_gb = 1.0;
    config.symbols.retain(|s| s.phoenix == "SOL");
    config.sources.bybit.base_url = server.base.clone();
    config.sources.bybit.files_url = server.base.clone();
    config.sources.bybit.funding_history = false;
    config.sources.bybit.trades = true;
    let filter = solos_data::augment::sync::Filter {
        source: Some("bybit".into()),
        symbol: None,
    };
    let run = |config: &solos_data::augment::config::AugmentConfig| {
        solos_data::augment::sync::run_sync(
            config,
            NOW_MS,
            &filter,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap()
        .to_value()
    };
    let status = run(&config);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    assert_eq!(status["sources"]["bybit"]["files"], 4);
    assert_eq!(
        server.hits_of("/trading/SOLUSDT/SOLUSDT2026-10-05.csv.gz"),
        1
    );
    assert_eq!(
        server.hits_of("/trading/SOLUSDT/SOLUSDT2026-10-09.csv.gz"),
        0
    );
    let out = rows(
        &root,
        "SELECT count(*) AS n, count(rpi) AS with_rpi, min(ts)::VARCHAR AS first, max(price) AS px FROM bybit_trades",
    );
    assert_eq!(out[0]["n"], "7");
    assert_eq!(out[0]["with_rpi"], "6");
    assert_eq!(out[0]["first"], "2026-10-06 00:00:00.1");
    assert_eq!(out[0]["px"], 120.64);
    // Rerun: nothing new, the 404 for 10-05 is not retried, today is still not asked for.
    let hits_before = server.hit_count();
    let status = run(&config);
    assert_eq!(status["sources"]["bybit"]["files"], 0);
    let paths: Vec<String> = server
        .hits()
        .into_iter()
        .skip(hits_before)
        .map(|h| h.path)
        .collect();
    assert!(paths.is_empty(), "{paths:?}");
    // A budget is a ceiling on the registered bytes: a fresh root admits the first file, then
    // the lane stops adding large files; a zero budget keeps the large datasets off entirely.
    config.disk_budget_gb = 0.000_001;
    let mut fresh = config.clone();
    fresh.data_dir = tempdir("solos-augment");
    let status = run(&fresh);
    assert_eq!(status["sources"]["bybit"]["files"], 1);
    let _ = std::fs::remove_dir_all(&fresh.data_dir);
    config.disk_budget_gb = 0.0;
    let mut off = config.clone();
    off.data_dir = tempdir("solos-augment");
    let status = run(&off);
    assert_eq!(status["sources"]["bybit"]["files"], 0);
    let _ = std::fs::remove_dir_all(&off.data_dir);
    let _ = std::fs::remove_dir_all(&root);
}
