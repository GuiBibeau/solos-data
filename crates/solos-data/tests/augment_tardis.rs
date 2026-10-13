//! The Tardis.dev source against an in-process server: the first-of-month samples without a key
//! (typed Parquet per type, thinned book snapshots, the empty file of a day without rows, days
//! before a listing remembered as absent), idempotent reruns, keyed days only with the key and
//! with the Bearer header, and a refused key that stops the keyed walk but not the samples.

mod augment_common;

use augment_common::{Request, Response, Server, tempdir};
use serde_json::Value;
use solos_data::augment::config::{
    ApiKey, AugmentConfig, TardisExchange, load_augment_config, repository_config_path,
};
use solos_data::augment::sync::{Filter, run_sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z: the last exported day is 2026-10-08.
const NOW_MS: i64 = 1_791_547_200_000;
/// 2026-09-01T00:00:00Z in microseconds.
const SEP1_US: i64 = 1_788_220_800_000_000;
const KEY: &str = "test-key";

fn config_for(server: &Server, root: &std::path::Path) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.requests_per_second = 50.0;
    config.symbols.retain(|s| s.phoenix == "SOL");
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
    let t = &mut s.tardis;
    t.enabled = true;
    t.datasets_url = format!("{}/v1", server.base);
    t.sample_start = "2026-08-01".into();
    t.keyed_start = "2026-10-07".into();
    t.book_symbols = vec!["SOL".into()];
    t.budget_gb = 1.0;
    t.api_key = None;
    t.exchanges = vec![TardisExchange {
        id: "binance-futures".into(),
        symbol_field: Some("binance".into()),
        symbols: Default::default(),
        data_types: vec![
            "trades".into(),
            "derivative_ticker".into(),
            "liquidations".into(),
            "book_snapshot_25".into(),
        ],
    }];
    config
}

fn gzip(text: &str) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(text.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

fn trades_csv(day_us: i64) -> String {
    let mut csv = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n".to_owned();
    for (i, side) in ["buy", "sell", "buy"].iter().enumerate() {
        let t = day_us + i64::try_from(i).unwrap() * 1_000_000;
        csv.push_str(&format!(
            "binance-futures,SOLUSDT,{t},{},{},{side},100.{i},2\n",
            t + 900,
            500 + i
        ));
    }
    csv
}

fn ticker_csv() -> String {
    let mut csv = "exchange,symbol,timestamp,local_timestamp,funding_timestamp,funding_rate,predicted_funding_rate,open_interest,last_price,index_price,mark_price\n".to_owned();
    csv.push_str(&format!(
        "binance-futures,SOLUSDT,{SEP1_US},{SEP1_US},,,,20510963,,,\n"
    ));
    csv.push_str(&format!(
        "binance-futures,SOLUSDT,{SEP1_US},{},1788220800000000,0.0000404,,20510963,,100.1,100.2\n",
        SEP1_US + 5
    ));
    csv
}

/// Three snapshots, two in the first second: thinning keeps two rows.
fn book_csv() -> String {
    let mut header = vec!["exchange", "symbol", "timestamp", "local_timestamp"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for level in 0..25 {
        for (side, field) in [
            ("asks", "price"),
            ("asks", "amount"),
            ("bids", "price"),
            ("bids", "amount"),
        ] {
            header.push(format!("{side}[{level}].{field}"));
        }
    }
    let mut csv = format!("{}\n", header.join(","));
    for (offset, bid_amount) in [(100_000, 1.0), (600_000, 2.0), (1_200_000, 3.0)] {
        let t = SEP1_US + offset;
        let mut row = vec![
            "binance-futures".to_owned(),
            "SOLUSDT".to_owned(),
            t.to_string(),
            t.to_string(),
        ];
        for level in 0..25 {
            if level < 2 {
                row.push(format!("{}", 101 + level));
                row.push("1".into());
                row.push(format!("{}", 99 - level));
                row.push(format!("{bid_amount}"));
            } else {
                row.extend(std::iter::repeat_n(String::new(), 4));
            }
        }
        csv.push_str(&format!("{}\n", row.join(",")));
    }
    csv
}

fn path(data_type: &str, day: &str) -> String {
    format!(
        "/v1/binance-futures/{data_type}/{}/SOLUSDT.csv.gz",
        day.replace('-', "/")
    )
}

/// Samples for 2026-09-01 (every type; liquidations empty) and 2026-10-01 (trades); keyed
/// trades for 2026-10-07 and 2026-10-08 behind the Bearer header; 400 for everything else.
fn mount(server: &Server) {
    server.serve_bytes(&path("trades", "2026-09-01"), gzip(&trades_csv(SEP1_US)));
    server.serve_bytes(
        &path("trades", "2026-10-01"),
        gzip(&trades_csv(1_790_812_800_000_000)),
    );
    server.serve_bytes(
        &path("derivative_ticker", "2026-09-01"),
        gzip(&ticker_csv()),
    );
    server.serve_bytes(&path("liquidations", "2026-09-01"), gzip(""));
    server.serve_bytes(&path("book_snapshot_25", "2026-09-01"), gzip(&book_csv()));
    for day in ["2026-10-07", "2026-10-08"] {
        let body = gzip(&trades_csv(SEP1_US));
        server.route(&path("trades", day), move |request: &Request| {
            if request.header("authorization") == Some(&format!("Bearer {KEY}")) {
                Response::ok(body.clone())
            } else {
                Response::status(401)
            }
        });
    }
    for data_type in [
        "trades",
        "derivative_ticker",
        "liquidations",
        "book_snapshot_25",
    ] {
        for day in ["2026-08-01", "2026-10-07", "2026-10-08"] {
            if data_type != "trades" || day == "2026-08-01" {
                server.route(&path(data_type, day), |_| Response::status(400));
            }
        }
        if data_type != "trades" {
            server.route(&path(data_type, "2026-10-01"), |_| Response::status(400));
        }
    }
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

fn authorized(server: &Server) -> usize {
    server
        .hits()
        .iter()
        .filter(|r| r.header("authorization").is_some())
        .count()
}

#[test]
fn samples_without_a_key_then_keyed_days_with_it() {
    let server = Server::start();
    mount(&server);
    let root = tempdir("solos-augment-tardis");
    let mut config = config_for(&server, &root);

    let status = run(&config);
    let tardis = &status["sources"]["tardis"];
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    assert_eq!(tardis["files"], 5, "{status}");
    assert_eq!(
        tardis["rows"], 10,
        "trades 3 + 3, ticker 2, liquidations 0, book 2"
    );
    assert!(tardis["note"].as_str().unwrap().contains("samples only"));
    assert_eq!(authorized(&server), 0, "no key, no header");
    assert!(
        server.hits().iter().all(|r| r.path.contains("/01/SOLUSDT")),
        "only first-of-month days without a key"
    );

    let book = rows(
        &config,
        "SELECT time_us, bid_amount_0, bid_amount_25, ask_notional_25, bid_price_9, exchange, symbol, phoenix_symbol
         FROM tardis_binance_futures_book_snapshot_25 ORDER BY time_us",
    );
    assert_eq!(book.len(), 2, "one snapshot per second");
    assert_eq!(
        book[0]["bid_amount_0"], 2.0,
        "the last snapshot of the second"
    );
    assert_eq!(book[0]["bid_amount_25"], 4.0);
    assert_eq!(book[0]["ask_notional_25"], 101.0 + 102.0);
    assert_eq!(book[0]["bid_price_9"], Value::Null);
    assert_eq!(book[1]["exchange"], "binance-futures");
    assert_eq!(book[1]["phoenix_symbol"], "SOL");
    let ticker = rows(
        &config,
        "SELECT open_interest, mark_price, funding_rate FROM tardis_binance_futures_derivative_ticker ORDER BY local_time_us",
    );
    assert_eq!(ticker[0]["mark_price"], Value::Null);
    assert_eq!(ticker[1]["funding_rate"], 0.0000404);
    let trades = rows(
        &config,
        "SELECT CAST(count(*) AS INTEGER) AS n, sum(CASE WHEN side = 'buy' THEN amount END) AS bought
         FROM tardis_binance_futures_trades",
    );
    assert_eq!(
        (trades[0]["n"].clone(), trades[0]["bought"].clone()),
        (6.into(), 8.0.into())
    );
    let liquidations = rows(
        &config,
        "SELECT CAST(count(*) AS INTEGER) AS n FROM tardis_binance_futures_liquidations",
    );
    assert_eq!(
        liquidations[0]["n"], 0,
        "the empty file is a day without rows"
    );

    let hits = server.hit_count();
    let again = run(&config);
    assert_eq!(again["sources"]["tardis"]["files"], 0, "{again}");
    assert_eq!(
        server.hit_count(),
        hits,
        "a rerun asks for nothing: files are registered, 400s of old days remembered"
    );

    config.sources.tardis.api_key = Some(ApiKey(KEY.into()));
    let keyed = run(&config);
    assert_eq!(keyed["totals"]["errors"], 0, "{keyed}");
    assert_eq!(keyed["sources"]["tardis"]["files"], 2, "{keyed}");
    assert!(keyed["sources"]["tardis"].get("note").is_none());
    assert!(
        server
            .hits()
            .iter()
            .filter(|r| r.header("authorization").is_some())
            .all(|r| r.path.contains("/10/07/") || r.path.contains("/10/08/")),
        "the key goes only with non-sample days"
    );
    // The recent 400s of the other types (2026-10-07, -08) are not remembered: asked again.
    let before = server.hit_count();
    let rerun = run(&config);
    assert_eq!(rerun["sources"]["tardis"]["files"], 0);
    assert_eq!(server.hit_count() - before, 6);
    let days = rows(
        &config,
        "SELECT CAST(count(DISTINCT CAST(ts AS DATE)) AS INTEGER) AS n FROM tardis_binance_futures_trades",
    );
    assert_eq!(
        days[0]["n"], 2,
        "the keyed days hold fixture rows of 2026-09-01"
    );
}

#[test]
fn a_refused_key_stops_keyed_days_but_not_the_samples() {
    let server = Server::start();
    mount(&server);
    let root = tempdir("solos-augment-tardis-refused");
    let mut config = config_for(&server, &root);
    config.sources.tardis.api_key = Some(ApiKey("wrong".into()));

    let status = run(&config);
    assert_eq!(status["sources"]["tardis"]["files"], 5, "{status}");
    assert_eq!(status["sources"]["tardis"]["errors"], 1, "{status}");
    assert_eq!(
        authorized(&server),
        1,
        "the newest keyed day is refused and no other keyed day is asked for"
    );
}

#[test]
fn the_transfer_limit_stops_the_run_without_waiting_or_an_error() {
    let server = Server::start();
    mount(&server);
    // The anonymous transfer limit: 429 with a day-long Retry-After on 2026-09-01's trades.
    server.route(&path("trades", "2026-09-01"), |_| {
        Response::status(429).with_header("Retry-After", "84291")
    });
    let root = tempdir("solos-augment-tardis-quota");
    let config = config_for(&server, &root);

    let started = std::time::Instant::now();
    let status = run(&config);
    assert!(
        started.elapsed().as_secs() < 30,
        "no wait on a day-long Retry-After"
    );
    let tardis = &status["sources"]["tardis"];
    assert_eq!(tardis["errors"], 0, "{status}");
    assert_eq!(tardis["files"], 1, "2026-10-01's trades, before the limit");
    assert!(tardis["note"].as_str().unwrap().contains("transfer limit"));
    let last = server.hits().last().unwrap().path.clone();
    assert_eq!(last, path("trades", "2026-09-01"), "nothing after the 429");
    assert_eq!(server.hits_of(&last), 1, "the 429 is not retried");
}

#[test]
fn a_key_refused_after_a_resume_stops_every_keyed_series() {
    let server = Server::start();
    mount(&server);
    for day in ["2026-10-05", "2026-10-06"] {
        let body = gzip(&trades_csv(SEP1_US));
        server.route(&path("trades", day), move |request: &Request| {
            if request.header("authorization") == Some(&format!("Bearer {KEY}")) {
                Response::ok(body.clone())
            } else {
                Response::status(401)
            }
        });
    }
    let root = tempdir("solos-augment-tardis-resumed");
    let mut config = config_for(&server, &root);
    config.sources.tardis.api_key = Some(ApiKey(KEY.into()));
    let first = run(&config);
    assert_eq!(first["sources"]["tardis"]["files"], 5 + 2, "{first}");

    // The newest keyed days are registered; the key is now wrong and keyedStart moved back,
    // so the first keyed request of the run is an older day of the trades series.
    config.sources.tardis.api_key = Some(ApiKey("expired".into()));
    config.sources.tardis.keyed_start = "2026-10-05".into();
    let before = server.hit_count();
    let status = run(&config);
    assert_eq!(status["sources"]["tardis"]["errors"], 1, "{status}");
    let hits: Vec<String> = server.hits()[before..]
        .iter()
        .map(|r| r.path.clone())
        .collect();
    assert_eq!(
        hits.iter().filter(|p| p.contains("/10/06/")).count(),
        1,
        "the refusal on the trades series stops the other series' keyed days: {hits:?}"
    );
    assert!(!hits.iter().any(|p| p.contains("/10/05/")), "{hits:?}");
}
