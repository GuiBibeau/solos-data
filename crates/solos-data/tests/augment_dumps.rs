//! Exchange dump walks against an in-process server (ADR-0009, exchange dumps 2026-10-19):
//! Binance `aggTrades` and `bookDepth` checked against their `.CHECKSUM`, OKX swap trades (with
//! and without a header line) and monthly funding, newest period first, a checksum mismatch
//! that stops only its series, days before a listing remembered as absent, recent missing days
//! asked again, idempotent reruns, and a run time budget that yields to the next run.

mod augment_common;

use augment_common::{Server, checksum_of, tempdir, zip_of};
use serde_json::Value;
use solos_data::augment::config::{AugmentConfig, load_augment_config, repository_config_path};
use solos_data::augment::sync::{Filter, run_sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;
/// 2026-10-08T00:00:00Z.
const OCT8_MS: i64 = 1_791_417_600_000;

fn config_for(server: &Server, root: &std::path::Path) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.requests_per_second = 50.0;
    config.disk_budget_gb = 1.0;
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
        &mut s.hyperliquid_s3.enabled,
    ] {
        *enabled = false;
    }
    s.binance.files_url = format!("{}/files", server.base);
    s.binance.list_url = format!("{}/list", server.base);
    s.binance.datasets.clear();
    s.binance.large_datasets = vec!["aggTrades".into(), "bookDepth".into()];
    s.binance.walk.start_date = "2026-10-03".into();
    s.okx.files_url = format!("{}/okx", server.base);
    s.okx.walk.start_date = "2026-09-28".into();
    config
}

fn binance_path(dataset: &str, day: &str) -> String {
    format!("/files/data/futures/um/daily/{dataset}/SOLUSDT/SOLUSDT-{dataset}-{day}.zip")
}

fn serve_zip(server: &Server, path: &str, csv: &str, checksum: Option<&[u8]>) {
    let name = path.rsplit('/').next().unwrap().replace(".zip", ".csv");
    let zip = zip_of(&name, csv.as_bytes());
    let sum = checksum.map_or_else(|| checksum_of(&zip, &name), <[u8]>::to_vec);
    server.serve_bytes(path, zip);
    server.serve_bytes(&format!("{path}.CHECKSUM"), sum);
}

fn mount(server: &Server) {
    let agg = format!(
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
         2,120.5,3,11,12,{},true\n1,120.4,1,10,10,{OCT8_MS},false\n",
        OCT8_MS + 5
    );
    serve_zip(server, &binance_path("aggTrades", "2026-10-08"), &agg, None);
    // 10-07's checksum does not match: the series stops for this run, 10-06 is not asked for.
    serve_zip(
        server,
        &binance_path("aggTrades", "2026-10-07"),
        &agg,
        Some(&[b'0'; 64][..]),
    );
    let depth = "timestamp,percentage,depth,notional\n2026-10-08 00:00:01,-1.00,10.5,1200.0\n2026-10-08 00:00:01,1.00,9.5,1100.0\n";
    serve_zip(
        server,
        &binance_path("bookDepth", "2026-10-08"),
        depth,
        None,
    );
    serve_zip(
        server,
        &binance_path("bookDepth", "2026-10-07"),
        depth,
        None,
    );
    let trades = format!(
        "instrument_name,trade_id,side,price,size,created_time,source\nSOL-USDT-SWAP,7,sell,120.1,6.5,{},0\nSOL-USDT-SWAP,6,buy,120.0,1,{},1\n",
        OCT8_MS - 8 * 3_600_000 + 2,
        OCT8_MS - 8 * 3_600_000 + 1
    );
    let zip = zip_of("t.csv", trades.as_bytes());
    server.serve_bytes(
        "/okx/trades/daily/20261008/SOL-USDT-SWAP-trades-2026-10-08.zip",
        zip,
    );
    // An older file without its header line.
    let bare = format!("SOL-USDT-SWAP,5,buy,119,2,{}\n", OCT8_MS - 86_400_000);
    server.serve_bytes(
        "/okx/trades/daily/20261007/SOL-USDT-SWAP-trades-2026-10-07.zip",
        zip_of("t.csv", bare.as_bytes()),
    );
    let funding = "instrument_name,funding_rate,funding_time\nSOL-USDT-SWAP,0.0001,1788192000000\nSOL-USDT-SWAP,-0.00005,1788220800000\n";
    server.serve_bytes(
        "/okx/swaprates/monthly/202609/SOL-USDT-SWAP-fundingrates-2026-09.zip",
        zip_of("f.csv", funding.as_bytes()),
    );
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
fn dumps_walk_newest_first_with_checksums_absences_and_reruns() {
    let server = Server::start();
    mount(&server);
    let root = tempdir("solos-augment-dumps");
    let config = config_for(&server, &root);
    let status = run(&config);
    assert_eq!(status["sources"]["binance"]["files"], 3, "{status}");
    assert_eq!(status["sources"]["binance"]["errors"], 1, "{status}");
    assert_eq!(status["sources"]["okx"]["files"], 3, "{status}");
    assert_eq!(status["sources"]["okx"]["errors"], 0, "{status}");
    // The first Binance request is the newest day; 10-06 of aggTrades is never asked for.
    let hits: Vec<String> = server.hits().into_iter().map(|h| h.path).collect();
    let first = hits.iter().find(|p| p.starts_with("/files/")).unwrap();
    assert!(first.contains("2026-10-08"), "{first}");
    assert_eq!(server.hits_of(&binance_path("aggTrades", "2026-10-06")), 0);
    assert_eq!(server.hits_of(&binance_path("bookDepth", "2026-10-06")), 1);
    let agg = rows(
        &config,
        "SELECT count(*) AS n, min(agg_trade_id) AS first, max(ts)::VARCHAR AS last, any_value(phoenix_symbol) AS p FROM binance_aggTrades",
    );
    assert_eq!(agg[0]["n"], "2");
    assert_eq!(agg[0]["first"], "1");
    assert_eq!(agg[0]["last"], "2026-10-08 00:00:00.005");
    assert_eq!(agg[0]["p"], "SOL");
    let depth = rows(
        &config,
        "SELECT count(*) AS n, sum(notional) AS s FROM binance_bookDepth",
    );
    assert_eq!(depth[0]["n"], "4");
    let okx = rows(
        &config,
        "SELECT count(*) AS n, min(trade_id) AS lo, sum(size) AS sz, min(ts)::VARCHAR AS first, count(trade_source) AS src FROM okx_trades",
    );
    assert_eq!(okx[0]["n"], "3");
    assert_eq!(okx[0]["lo"], "5");
    assert_eq!(okx[0]["sz"], 9.5);
    assert_eq!(okx[0]["first"], "2026-10-07 00:00:00");
    // The seven-column files (since November 2025) keep OKX's `source` column.
    assert_eq!(okx[0]["src"], "2");
    let funding = rows(
        &config,
        "SELECT count(*) AS n, min(funding_rate) AS lo FROM okx_funding",
    );
    assert_eq!(funding[0]["n"], "2");
    assert_eq!(funding[0]["lo"], -0.00005);
    // Rerun: only the failed aggTrades day (and the days behind it) and the recent missing days
    // are asked again; the old absent days are not.
    let before = server.hit_count();
    let status = run(&config);
    assert_eq!(status["sources"]["okx"]["files"], 0, "{status}");
    let again: Vec<String> = server
        .hits()
        .into_iter()
        .skip(before)
        .map(|h| h.path)
        .collect();
    assert!(
        again.iter().all(|p| p.contains("2026-10-07")
            || p.contains("2026-10-06")
            || p.contains("20261006")),
        "{again:?}"
    );
    assert!(!again.iter().any(|p| p.contains("2026-10-08")), "{again:?}");
    assert!(!again.iter().any(|p| p.contains("2026-10-05")), "{again:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_spent_run_budget_yields_to_the_next_run() {
    let server = Server::start();
    mount(&server);
    let root = tempdir("solos-augment-dumps");
    let mut config = config_for(&server, &root);
    config.sources.okx.walk.run_minutes = 0;
    config.sources.binance.enabled = false;
    let status = run(&config);
    assert_eq!(status["sources"]["okx"]["files"], 0, "{status}");
    assert_eq!(
        status["sources"]["okx"]["note"],
        "run time budget spent; the next runs continue the walk"
    );
    assert_eq!(server.hit_count(), 0);
    let _ = std::fs::remove_dir_all(&root);
}
