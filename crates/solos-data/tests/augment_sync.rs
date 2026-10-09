//! The augmentation lane against an in-process HTTP server: Binance dumps through the listing,
//! checksum and zip; the paged histories (Hyperliquid funding, Deribit DVOL, DefiLlama); the
//! ledger's idempotence across reruns; the catalog, status and query views; and the client's
//! retry and spacing behaviour.

mod augment_common;

use augment_common::{Response, Server, checksum_of, fixture, listing_xml, tempdir, zip_of};
use serde_json::{Value, json};
use solos_data::augment::config::{AugmentConfig, load_augment_config, repository_config_path};
use solos_data::augment::http::Http;
use solos_data::augment::sync::{Filter, run_sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;
/// 2026-10-01T00:00:00Z, the start of the open month the paged histories cover.
const MONTH_START_MS: i64 = 1_790_812_800_000;

fn config_for(server: &Server, root: &std::path::Path) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.start_date = "2026-10-06".into();
    config.requests_per_second = 50.0;
    config
        .symbols
        .retain(|s| ["SOL", "NVDA", "BTC"].contains(&s.phoenix.as_str()));
    let s = &config.sources;
    let mut sources = s.clone();
    sources.binance.files_url = format!("{}/files", server.base);
    sources.binance.list_url = format!("{}/list", server.base);
    sources.binance.datasets = vec!["klines".into(), "metrics".into(), "fundingRate".into()];
    sources.hyperliquid.base_url = server.base.clone();
    sources.hyperliquid.requests_per_second = 50.0;
    sources.deribit.base_url = server.base.clone();
    sources.deribit.resolutions = vec![3600];
    sources.deribit.currencies = vec!["BTC".into()];
    sources.defillama.base_url = server.base.clone();
    sources.defillama.stablecoins.truncate(1);
    // Bybit has its own test and the exogenous sources theirs; every other source points at
    // the local server, so nothing here can reach the network.
    sources.bybit.enabled = false;
    sources.elfa.enabled = false;
    sources.sec.enabled = false;
    sources.alternative.enabled = false;
    sources.polymarket.enabled = false;
    sources.kalshi.enabled = false;
    sources.phoenix.enabled = false;
    config.sources = sources;
    config
}

/// Binance: listing for SOLUSDT klines (two days), metrics (one day) and funding (one month).
fn mount_binance(server: &Server) {
    let klines_prefix = "data/futures/um/daily/klines/SOLUSDT/1m/";
    let metrics_prefix = "data/futures/um/daily/metrics/SOLUSDT/";
    let funding_prefix = "data/futures/um/monthly/fundingRate/SOLUSDT/";
    let kline_csv = fixture("SOLUSDT-1m-2026-10-07.csv");
    let metrics_csv = fixture("SOLUSDT-metrics-2026-10-07.csv");
    let funding_csv = fixture("SOLUSDT-fundingRate-2026-09.csv");
    // The start date is 2026-10-06, so the month file in reach is October's (a September file
    // would be filtered out by the plan and hidden by the listing marker).
    let files: Vec<(String, Vec<u8>)> = vec![
        (
            format!("{klines_prefix}SOLUSDT-1m-2026-10-06.zip"),
            zip_of("SOLUSDT-1m-2026-10-06.csv", &kline_csv),
        ),
        (
            format!("{klines_prefix}SOLUSDT-1m-2026-10-07.zip"),
            zip_of("SOLUSDT-1m-2026-10-07.csv", &kline_csv),
        ),
        (
            format!("{metrics_prefix}SOLUSDT-metrics-2026-10-07.zip"),
            zip_of("SOLUSDT-metrics-2026-10-07.csv", &metrics_csv),
        ),
        (
            format!("{funding_prefix}SOLUSDT-fundingRate-2026-10.zip"),
            zip_of("SOLUSDT-fundingRate-2026-10.csv", &funding_csv),
        ),
    ];
    let mut listed: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (key, bytes) in &files {
        let name = key.rsplit('/').next().unwrap().to_owned();
        server.serve_bytes(&format!("/files/{key}"), bytes.clone());
        server.serve_bytes(&format!("/files/{key}.CHECKSUM"), checksum_of(bytes, &name));
        let prefix = key[..key.len() - name.len()].to_owned();
        let entry = listed.entry(prefix).or_default();
        entry.push(key.clone());
        entry.push(format!("{key}.CHECKSUM"));
    }
    // One stale key before the start date in the klines listing; the marker would hide it in
    // production, the plan must drop it here.
    listed
        .get_mut(klines_prefix)
        .unwrap()
        .insert(0, format!("{klines_prefix}SOLUSDT-1m-2025-12-31.zip"));
    server.route("/list", move |request| {
        let prefix = request.param("prefix").unwrap_or_default();
        let marker = request.param("marker").unwrap_or_default();
        let keys: Vec<String> = listed
            .get(&prefix)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|key| *key > marker)
            .collect();
        Response::ok(listing_xml(&prefix, &keys, false, None))
    });
}

/// Hyperliquid: hourly funding rows for every coin from 2026-10-06 to now, served in pages.
fn mount_hyperliquid(server: &Server) {
    server.route("/info", |request| {
        let body = request.json();
        assert_eq!(body["type"], "fundingHistory");
        let coin = body["coin"].as_str().unwrap().to_owned();
        let start = body["startTime"].as_i64().unwrap();
        let end = body["endTime"].as_i64().unwrap_or(i64::MAX);
        let first = (start / 3_600_000) * 3_600_000 + if start % 3_600_000 == 0 { 0 } else { 3_600_000 };
        let rows: Vec<Value> = (0..500)
            .map(|i| first + i * 3_600_000 + 40)
            .filter(|t| *t <= end && *t < NOW_MS)
            .map(|t| json!({ "coin": coin, "fundingRate": "0.0000125", "premium": "-0.0001", "time": t }))
            .collect();
        Response::json(&Value::Array(rows))
    });
}

/// Deribit: hourly DVOL bars with a `continuation` once per request.
fn mount_deribit(server: &Server) {
    server.route("/api/v2/public/get_volatility_index_data", |request| {
        let start: i64 = request.param("start_timestamp").unwrap().parse().unwrap();
        let end: i64 = request.param("end_timestamp").unwrap().parse().unwrap();
        let resolution: i64 = request.param("resolution").unwrap().parse().unwrap();
        let step = resolution * 1000;
        let mut bars: Vec<Value> = Vec::new();
        let mut t = (end / step) * step;
        while t >= start && t < NOW_MS && bars.len() < 2 {
            bars.push(json!([t, 40.0, 41.0, 39.5, 40.5]));
            t -= step;
        }
        if t >= start && t < NOW_MS && !bars.is_empty() {
            // Older bars exist beyond this page.
            return Response::json(&json!({ "result": { "data": bars, "continuation": t } }));
        }
        Response::json(&json!({ "result": { "data": bars, "continuation": Value::Null } }))
    });
}

/// DefiLlama: the total and one coin, with points before and after the start date.
fn mount_defillama(server: &Server) {
    let points: Vec<Value> = (0..6)
        .map(|i| {
            let date = (NOW_MS / 1000 / 86_400 - 5 + i) * 86_400;
            json!({ "date": date.to_string(), "totalCirculating": { "peggedUSD": 100.0 + i as f64 }, "totalCirculatingUSD": { "peggedUSD": 101.0 } })
        })
        .collect();
    server.serve_json("/stablecoincharts/all", Value::Array(points));
}

fn run(config: &AugmentConfig) -> Value {
    run_sync(
        config,
        NOW_MS,
        &Filter::default(),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap()
    .to_value()
}

fn rows(config: &AugmentConfig, sql: &str) -> Vec<Value> {
    solos_data::augment::query::query_augment(&config.data_dir, sql)
        .unwrap()
        .to_value()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn sync_backfills_every_source_and_reruns_idempotently() {
    let server = Server::start();
    mount_binance(&server);
    mount_hyperliquid(&server);
    mount_deribit(&server);
    mount_defillama(&server);
    let root = tempdir("solos-augment");
    let config = config_for(&server, &root);

    let status = run(&config);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    let binance = &status["sources"]["binance"];
    assert_eq!(binance["files"], 4);
    assert_eq!(binance["rows"], 3 + 3 + 3 + 3);
    // Hyperliquid: SOL, NVDA (xyz:NVDA) and BTC, one month file each (October is open).
    assert_eq!(status["sources"]["hyperliquid"]["files"], 3);
    assert_eq!(status["sources"]["deribit"]["files"], 1);
    assert_eq!(status["sources"]["defillama"]["files"], 2);

    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json")).unwrap()).unwrap();
    let files = catalog["files"].as_array().unwrap();
    assert_eq!(files.len(), 4 + 3 + 1 + 2);
    let kline = files
        .iter()
        .find(|f| f["path"] == "tables/binance/klines/SOLUSDT/2026-10-07.parquet")
        .unwrap();
    assert_eq!(kline["row_count"], "3");
    assert_eq!(kline["complete"], true);
    assert_eq!(kline["phoenix_symbol"], "SOL");
    assert_eq!(kline["sha256"].as_str().unwrap().len(), 64);
    assert!(
        root.join("tables/hyperliquid/funding/xyz_NVDA/2026-10.parquet")
            .is_file()
    );
    let funding = files
        .iter()
        .find(|f| f["path"] == "tables/hyperliquid/funding/xyz_NVDA/2026-10.parquet")
        .unwrap();
    assert_eq!(funding["complete"], false);
    assert_eq!(funding["symbol"], "xyz:NVDA");
    assert!(
        std::fs::read_dir(root.join("staging"))
            .unwrap()
            .next()
            .is_none()
    );

    let klines = rows(
        &config,
        "SELECT count(*) AS n, min(ts)::VARCHAR AS first, max(close) AS close FROM binance_klines",
    );
    assert_eq!(klines[0]["n"], "6");
    assert_eq!(klines[0]["first"], "2026-10-07 00:00:00");
    let metrics = rows(
        &config,
        "SELECT ts::VARCHAR AS ts, sum_open_interest FROM binance_metrics ORDER BY ts LIMIT 1",
    );
    assert_eq!(metrics[0]["ts"], "2026-10-07 00:00:00");
    let hl = rows(
        &config,
        "SELECT phoenix_symbol, count(*) AS n, min(funding_rate) AS rate FROM hyperliquid_funding GROUP BY 1 ORDER BY 1",
    );
    assert_eq!(hl.len(), 3);
    assert_eq!(hl[0]["phoenix_symbol"], "BTC");
    assert_eq!(hl[0]["rate"], 0.0000125);
    // Month files cover the whole month, so the open October file starts at October 1.
    let hours_in_month = (NOW_MS - MONTH_START_MS) / 3_600_000;
    assert_eq!(hl[0]["n"], hours_in_month.to_string());
    let dvol = rows(
        &config,
        "SELECT count(*) AS n, symbol FROM deribit_dvol_3600s GROUP BY 2",
    );
    assert_eq!(dvol[0]["n"], hours_in_month.to_string());
    let llama = rows(
        &config,
        "SELECT symbol, count(*) AS n FROM defillama_stablecoins GROUP BY 1 ORDER BY 1",
    );
    assert_eq!(llama[0]["symbol"], "ALL");
    assert_eq!(llama[0]["n"], "6");
    assert_eq!(llama[1]["symbol"], "USDT");
    assert!(solos_data::augment::query::query_augment(&config.data_dir, "DROP TABLE x").is_err());

    // Rerun: closed periods are not fetched again, open ones are rewritten.
    let hits_before = server.hit_count();
    let status = run(&config);
    assert_eq!(status["totals"]["errors"], 0);
    assert_eq!(status["sources"]["binance"]["files"], 0);
    assert_eq!(status["sources"]["hyperliquid"]["files"], 3);
    let hits: Vec<_> = server.hits().into_iter().skip(hits_before).collect();
    assert!(
        hits.iter().all(|h| !h.path.ends_with(".zip")),
        "no zip refetched"
    );
    // One listing per dataset and Binance symbol (SOLUSDT and BTCUSDT; NVDA has none).
    assert_eq!(hits.iter().filter(|h| h.path == "/list").count(), 6);
    let listing_markers: Vec<String> = hits
        .iter()
        .filter(|h| h.path == "/list")
        .filter_map(|h| h.param("marker"))
        .collect();
    assert!(
        listing_markers
            .iter()
            .any(|m| m.ends_with("SOLUSDT-1m-2026-10-07.zip.CHECKSUM"))
    );
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json")).unwrap()).unwrap();
    assert_eq!(catalog["files"].as_array().unwrap().len(), 10);

    // A stray file and a stale staging file disappear on the next run.
    std::fs::write(
        root.join("tables/binance/klines/SOLUSDT/2026-10-08.parquet"),
        b"junk",
    )
    .unwrap();
    std::fs::write(root.join("staging/left.csv"), b"junk").unwrap();
    let status = run(&config);
    assert_eq!(status["recovered"], 2);
    assert!(
        !root
            .join("tables/binance/klines/SOLUSDT/2026-10-08.parquet")
            .exists()
    );
    let text = std::fs::read_to_string(root.join("status.json")).unwrap();
    assert!(text.contains("\"lane\":\"sync\""));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn checksum_mismatch_is_an_error_and_the_file_is_not_registered() {
    let server = Server::start();
    let prefix = "data/futures/um/daily/metrics/SOLUSDT/";
    let key = format!("{prefix}SOLUSDT-metrics-2026-10-07.zip");
    let zip = zip_of(
        "SOLUSDT-metrics-2026-10-07.csv",
        &fixture("SOLUSDT-metrics-2026-10-07.csv"),
    );
    server.serve_bytes(&format!("/files/{key}"), zip.clone());
    server.serve_bytes(
        &format!("/files/{key}.CHECKSUM"),
        checksum_of(b"other", "x"),
    );
    // The following day is fine, but must wait: fetching it would move the listing marker past
    // the failed one.
    let next = format!("{prefix}SOLUSDT-metrics-2026-10-08.zip");
    server.serve_bytes(&format!("/files/{next}"), zip.clone());
    server.serve_bytes(
        &format!("/files/{next}.CHECKSUM"),
        checksum_of(&zip, "SOLUSDT-metrics-2026-10-08.zip"),
    );
    let keys = vec![
        key.clone(),
        format!("{key}.CHECKSUM"),
        next.clone(),
        format!("{next}.CHECKSUM"),
    ];
    server.route("/list", move |request| {
        let prefix = request.param("prefix").unwrap_or_default();
        Response::ok(listing_xml(&prefix, &keys, false, None))
    });
    let root = tempdir("solos-augment");
    let mut config = config_for(&server, &root);
    config.sources.binance.datasets = vec!["metrics".into()];
    config.sources.hyperliquid.enabled = false;
    config.sources.deribit.enabled = false;
    config.sources.defillama.enabled = false;
    let status = run(&config);
    assert_eq!(status["sources"]["binance"]["errors"], 1);
    assert_eq!(status["sources"]["binance"]["files"], 0);
    assert!(
        !root
            .join("tables/binance/metrics/SOLUSDT/2026-10-07.parquet")
            .exists()
    );
    assert_eq!(server.hits_of(&format!("/files/{next}")), 0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn http_retries_throttles_and_spaces_requests() {
    let server = Server::start();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    server.route("/flaky", move |_| {
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match n {
            0 => Response::status(429).with_header("Retry-After", "1"),
            1 => Response::status(503),
            _ => Response::json(&json!({ "ok": true })),
        }
    });
    server.route("/missing", |_| Response::status(404));
    server.route("/forbidden", |_| Response::status(403));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let http = Http::new(4.0).unwrap();
        let started = std::time::Instant::now();
        let answer = http
            .get_json(&format!("{}/flaky", server.base), &[])
            .await
            .unwrap();
        assert_eq!(answer["ok"], true);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        // Retry-After 1 s, then a 2 s backoff after the 503.
        assert!(started.elapsed() >= std::time::Duration::from_secs(3));
        assert!(matches!(
            http.get_bytes(&format!("{}/missing", server.base), &[])
                .await,
            Err(solos_data::augment::http::HttpError::NotFound)
        ));
        assert!(matches!(
            http.get_bytes(&format!("{}/forbidden", server.base), &[])
                .await,
            Err(solos_data::augment::http::HttpError::Status(403))
        ));
        let started = std::time::Instant::now();
        for _ in 0..4 {
            http.get_json(&format!("{}/flaky", server.base), &[])
                .await
                .unwrap();
        }
        // Four requests at four per second: at least three intervals of 250 ms.
        assert!(started.elapsed() >= std::time::Duration::from_millis(700));
    });
}
