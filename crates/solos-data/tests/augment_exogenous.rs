//! The exogenous sync sources against an in-process HTTP server: SEC filings per issuer and
//! month with the ticker guard and a continuation file, Fear and Greed as year files from 2018,
//! Polymarket events expanded to token price series, Kalshi series expanded to market candles,
//! and Phoenix earnings dates merged per day. Everything runs with a fixed clock.

mod augment_common;

use augment_common::{Response, Server, fixture, tempdir};
use serde_json::{Value, json};
use solos_data::augment::config::{AugmentConfig, load_augment_config, repository_config_path};
use solos_data::augment::sync::{Filter, run_sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;

fn config_for(server: &Server, root: &std::path::Path, source: &str) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.requests_per_second = 50.0;
    config
        .symbols
        .retain(|s| ["SOL", "NVDA", "TSLA"].contains(&s.phoenix.as_str()));
    let s = &mut config.sources;
    for enabled in [
        &mut s.binance.enabled,
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
    ] {
        *enabled = false;
    }
    match source {
        "sec" => {
            s.sec.enabled = true;
            s.sec.base_url = server.base.clone();
            s.sec.tickers_url = format!("{}/files/company_tickers.json", server.base);
            s.sec.archive_url = format!("{}/Archives/edgar/data", server.base);
            s.sec.requests_per_second = 10.0;
        }
        "alternative" => {
            s.alternative.enabled = true;
            s.alternative.base_url = server.base.clone();
        }
        "polymarket" => {
            s.polymarket.enabled = true;
            s.polymarket.gamma_url = server.base.clone();
            s.polymarket.clob_url = server.base.clone();
            s.polymarket.events.truncate(1);
        }
        "kalshi" => {
            s.kalshi.enabled = true;
            s.kalshi.base_url = server.base.clone();
            s.kalshi.series = vec!["KXFEDDECISION".into()];
        }
        _ => {
            s.phoenix.enabled = true;
            s.phoenix.markets_url = format!("{}/v1/view/exchange/markets", server.base);
        }
    }
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

fn rows(config: &AugmentConfig, sql: &str) -> Vec<Value> {
    solos_data::augment::query::query_augment(&config.data_dir, sql)
        .unwrap()
        .to_value()["rows"]
        .as_array()
        .unwrap()
        .clone()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn sec_filings_are_month_files_per_issuer_behind_the_ticker_guard() {
    let server = Server::start();
    let doc = fixture("sec-submissions.json");
    server.route("/submissions/CIK0001045810.json", move |request| {
        assert_eq!(
            request.header("user-agent"),
            Some("solos-data/1.0 (+https://github.com/GuiBibeau/solos-data)")
        );
        assert_eq!(request.header("accept-encoding"), Some("gzip"));
        Response::ok(gzip(&doc)).with_header("Content-Encoding", "gzip")
    });
    server.serve_bytes(
        "/submissions/CIK0001045810-submissions-001.json",
        fixture("sec-submissions-001.json"),
    );
    // TSLA is mapped to NVIDIA's CIK on purpose: EDGAR's tickers do not list it.
    let mut wrong: Value = serde_json::from_slice(&fixture("sec-submissions.json")).unwrap();
    wrong["cik"] = json!("1318605");
    server.serve_json("/submissions/CIK0001318605.json", wrong);
    let root = tempdir("solos-augment");
    let mut config = config_for(&server, &root, "sec");
    config
        .symbols
        .iter_mut()
        .find(|s| s.phoenix == "TSLA")
        .unwrap()
        .sec_cik = Some("1318605".into());
    let status = run(&config, NOW_MS);
    assert_eq!(status["sources"]["sec"]["errors"], 1, "{status}");
    // NVDA: January (continuation), February (continuation), August, September, October.
    assert_eq!(status["sources"]["sec"]["files"], 5);
    assert_eq!(status["sources"]["sec"]["rows"], 5);
    assert_eq!(
        server.hits_of("/submissions/CIK0001045810-submissions-002.json"),
        0,
        "a continuation ending before the start date is not fetched"
    );
    let out = rows(
        &config,
        "SELECT ticker, phoenix_symbol, form, filing_date::VARCHAR AS filing_date, ts::VARCHAR AS ts, url FROM sec_filings ORDER BY ts",
    );
    assert_eq!(out.len(), 5);
    assert_eq!(out[0]["form"], "SC 13G/A");
    assert_eq!(out[0]["filing_date"], "2026-01-05");
    assert_eq!(out[4]["form"], "8-K");
    assert_eq!(out[4]["ts"], "2026-10-02 16:05:12");
    assert_eq!(out[4]["ticker"], "NVDA");
    assert!(
        out[4]["url"]
            .as_str()
            .unwrap()
            .ends_with("/Archives/edgar/data/1045810/000104581026000142/nvda-20261001.htm")
    );
    assert!(!root.join("tables/sec/filings/TSLA").exists());
    // Rerun: closed months are not refetched, the open month is rewritten from one request.
    let hits_before = server.hit_count();
    let status = run(&config, NOW_MS);
    assert_eq!(status["sources"]["sec"]["files"], 1);
    assert_eq!(
        server
            .hits()
            .into_iter()
            .skip(hits_before)
            .filter(|h| h.path == "/submissions/CIK0001045810.json")
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn sec_map_reports_ciks_from_the_ticker_listing() {
    let server = Server::start();
    server.serve_json(
        "/files/company_tickers.json",
        json!({ "0": { "cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP" }, "1": { "cik_str": 1318605, "ticker": "TSLA", "title": "Tesla, Inc." } }),
    );
    let root = tempdir("solos-augment");
    let config = config_for(&server, &root, "sec");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let report = runtime
        .block_on(solos_data::augment::sec::map_ciks(
            "unused.json",
            &config,
            false,
        ))
        .unwrap()
        .to_value();
    assert_eq!(report["written"], false);
    assert_eq!(report["mapped"].as_array().unwrap().len(), 2);
    assert_eq!(report["mapped"][0]["ticker"], "NVDA");
    assert_eq!(report["mapped"][0]["cik"], "1045810");
    assert_eq!(report["mapped"][0]["previous"], "1045810");
    assert_eq!(report["unmapped"], json!([]));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fear_and_greed_is_a_year_series_from_2018() {
    let server = Server::start();
    server.serve_bytes("/fng/", fixture("fng.json"));
    let root = tempdir("solos-augment");
    let config = config_for(&server, &root, "alternative");
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    // 2018 holds the oldest point, 2026 the three newest; the years between are empty and
    // closed, so they advance the progress without a file.
    assert_eq!(status["sources"]["alternative"]["files"], 2);
    assert_eq!(status["sources"]["alternative"]["rows"], 4);
    assert_eq!(server.hits_of("/fng/"), 1, "one call serves every year");
    let out = rows(
        &config,
        "SELECT day::VARCHAR AS day, value, classification FROM alternative_fear_greed ORDER BY day",
    );
    assert_eq!(out[0]["day"], "2018-02-01");
    assert_eq!(out[0]["value"], "30");
    assert_eq!(out[3]["day"], "2026-10-09");
    assert_eq!(out[3]["classification"], "Greed");
    assert!(
        root.join("tables/alternative/fear_greed/ALL/2018.parquet")
            .is_file()
    );
    let catalog: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("catalog.json")).unwrap()).unwrap();
    let files = catalog["files"].as_array().unwrap();
    assert!(
        files
            .iter()
            .any(|f| f["period"] == "2018" && f["complete"] == true)
    );
    assert!(
        files
            .iter()
            .any(|f| f["period"] == "2026" && f["complete"] == false)
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn polymarket_events_expand_to_token_price_series() {
    let server = Server::start();
    server.serve_bytes("/events/606422", fixture("gamma-event.json"));
    let history: Value = serde_json::from_slice(&fixture("clob-history.json")).unwrap();
    server.route("/prices-history", move |request| {
        assert_eq!(request.param("fidelity").as_deref(), Some("60"));
        let start: i64 = request.param("startTs").unwrap().parse().unwrap();
        let token = request.param("market").unwrap();
        // Hourly points from the requested start to now; the second token mirrors the first.
        let flip = token.starts_with('1');
        let points: Vec<Value> = (0..)
            .map(|i| start + i * 3600)
            .take_while(|t| *t < NOW_MS / 1000)
            .map(|t| json!({ "t": t, "p": if flip { 0.8 } else { 0.2 } }))
            .collect();
        let _ = &history;
        Response::json(&json!({ "history": points }))
    });
    let root = tempdir("solos-augment");
    let mut config = config_for(&server, &root, "polymarket");
    config.start_date = "2026-09-01".into();
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    // One catalogue file, then two markets with September and October each.
    assert_eq!(status["sources"]["polymarket"]["files"], 1 + 4);
    // One history request per token for September; October is served from the run's cache.
    assert_eq!(server.hits_of("/prices-history"), 4);
    let markets = rows(
        &config,
        "SELECT market_id, event_slug, question, outcomes, closed FROM polymarket_markets ORDER BY market_id",
    );
    assert_eq!(markets.len(), 2);
    assert_eq!(markets[0]["market_id"], "2589811");
    assert_eq!(
        markets[0]["event_slug"],
        "fed-decision-in-october-20260617190323537"
    );
    let prices = rows(
        &config,
        "SELECT symbol, outcome, count(*) AS n, min(ts)::VARCHAR AS first, max(price) AS px FROM polymarket_prices GROUP BY 1, 2 ORDER BY 1, 2",
    );
    assert_eq!(prices.len(), 4);
    assert_eq!(prices[0]["symbol"], "2589811");
    assert_eq!(prices[0]["outcome"], "No");
    assert_eq!(prices[0]["first"], "2026-09-01 00:00:00");
    // September 1 to October 9 12:00 at one point an hour.
    let hours = (NOW_MS / 1000 - 1_788_220_800) / 3600;
    assert_eq!(prices[0]["n"], hours.to_string());
    // Rerun: only the open month, one request per token.
    let hits_before = server.hit_count();
    let status = run(&config, NOW_MS + 60_000);
    assert_eq!(status["sources"]["polymarket"]["files"], 1 + 2);
    assert_eq!(server.hit_count() - hits_before, 1 + 4);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn kalshi_series_expand_to_market_candles() {
    let server = Server::start();
    server.serve_bytes("/markets", fixture("kalshi-markets.json"));
    server.route(
        "/series/KXFEDDECISION/markets/KXFEDDECISION-28JAN-H26/candlesticks",
        |request| {
            assert_eq!(request.param("period_interval").as_deref(), Some("60"));
            let start: i64 = request.param("start_ts").unwrap().parse().unwrap();
            let end: i64 = request.param("end_ts").unwrap().parse().unwrap();
            let candles: Vec<Value> = (1..)
                .map(|i| start + i * 3600)
                .take_while(|t| *t <= end && *t <= NOW_MS / 1000)
                .map(|t| json!({ "end_period_ts": t, "yes_bid": { "open_dollars": "0.10", "close_dollars": "0.11" }, "yes_ask": { "close_dollars": "0.20" }, "price": {}, "volume_fp": "1.50", "open_interest_fp": "7.00" }))
                .collect();
            Response::json(&json!({ "candlesticks": candles, "ticker": "KXFEDDECISION-28JAN-H26" }))
        },
    );
    server.route(
        "/series/KXFEDDECISION/markets/KXFEDDECISION-28JAN-H25/candlesticks",
        |_| Response::json(&json!({ "candlesticks": [] })),
    );
    let root = tempdir("solos-augment");
    let mut config = config_for(&server, &root, "kalshi");
    config.start_date = "2026-09-01".into();
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    // One catalogue file and the traded market's September and October; the untraded market
    // closes September without a file and stops at the open month.
    assert_eq!(status["sources"]["kalshi"]["files"], 1 + 2);
    let listing = server.hits_of("/markets");
    assert_eq!(listing, 1);
    assert!(
        server
            .hits()
            .iter()
            .find(|h| h.path == "/markets")
            .and_then(|h| h.param("min_close_ts"))
            .is_some()
    );
    let markets = rows(
        &config,
        "SELECT ticker, status, series_ticker, event_ticker FROM kalshi_markets ORDER BY ticker",
    );
    assert_eq!(markets.len(), 2);
    assert_eq!(markets[0]["series_ticker"], "KXFEDDECISION");
    let candles = rows(
        &config,
        "SELECT symbol, count(*) AS n, min(ts)::VARCHAR AS first, max(yes_bid_close) AS bid, max(price_close) AS px FROM kalshi_candles_1h GROUP BY 1",
    );
    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0]["symbol"], "KXFEDDECISION-28JAN-H26");
    assert_eq!(candles[0]["first"], "2026-09-01 00:00:00");
    assert_eq!(candles[0]["bid"], 0.11);
    assert_eq!(candles[0]["px"], Value::Null);
    let hours = (NOW_MS / 1000 - 1_788_220_800) / 3600;
    assert_eq!(candles[0]["n"], hours.to_string());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phoenix_earnings_dates_merge_into_the_day_file() {
    let server = Server::start();
    server.serve_bytes("/v1/view/exchange/markets", fixture("phoenix-markets.json"));
    let root = tempdir("solos-augment");
    let config = config_for(&server, &root, "phoenix");
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    assert_eq!(status["sources"]["phoenix"]["files"], 1);
    assert_eq!(status["sources"]["phoenix"]["rows"], 1);
    // A second sync the same day merges on (symbol, date): still one row, newer fetch time.
    let status = run(&config, NOW_MS + 3_600_000);
    assert_eq!(status["sources"]["phoenix"]["files"], 1);
    let out = rows(
        &config,
        "SELECT phoenix_symbol, asset_id, earnings_date::VARCHAR AS day, ts::VARCHAR AS fetched FROM phoenix_earnings_dates",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["phoenix_symbol"], "QCOM");
    assert_eq!(out[0]["day"], "2026-11-04");
    assert_eq!(out[0]["fetched"], "2026-10-09 13:00:00");
    assert!(
        root.join("tables/phoenix/earnings_dates/ALL/2026-10-09.parquet")
            .is_file()
    );
    let _ = std::fs::remove_dir_all(&root);
}
