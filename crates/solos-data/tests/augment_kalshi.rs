//! Kalshi ladders against an in-process server with both tiers (ADR-0009, Kalshi ladders
//! 2026-10-22): the live listing and the archive walk by cursor, the causal centre and the kept
//! strikes, archived markets fetched one by one and live ones in batches (with the archive as
//! the fallback for a market the batch leaves out), trades of the kept markets that traded,
//! today's closed events stored as they close, idempotent reruns, and a request budget that
//! resumes. All values are synthetic.

mod augment_common;

use augment_common::{Response, Server, tempdir};
use serde_json::{Value, json};
use solos_data::augment::config::{
    AugmentConfig, KalshiSeries, load_augment_config, repository_config_path,
};
use solos_data::augment::sync::{Filter, run_sync};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// 2026-10-09T12:00:00Z.
const NOW_MS: i64 = 1_791_547_200_000;
const HOUR: i64 = 3_600;
/// 2026-10-09T00:00:00Z, seconds.
const OCT9: i64 = NOW_MS / 1000 - 12 * HOUR;
const DAY: i64 = 24 * HOUR;

/// One synthetic event: name, close (seconds), settled value, whether archived.
struct Ev(&'static str, i64, Option<f64>, bool);

fn events() -> Vec<Ev> {
    vec![
        Ev("A0", OCT9 - 5 * DAY + 15 * HOUR, Some(100.0), true),
        Ev("A", OCT9 - 5 * DAY + 16 * HOUR, Some(100.4), true),
        Ev("L0", OCT9 - DAY + 15 * HOUR, Some(101.0), false),
        Ev("L", OCT9 - DAY + 16 * HOUR, Some(102.3), false),
        Ev("T0", OCT9 + 10 * HOUR, Some(103.0), false),
        Ev("T", OCT9 + 11 * HOUR, None, false),
        Ev("O", OCT9 + 13 * HOUR, None, false),
    ]
}

fn iso(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn ticker(event: &str, strike: i64) -> String {
    format!("KXTEST-{event}-T{strike}")
}

fn markets(ev: &Ev) -> Vec<Value> {
    let status = match (ev.2, ev.1 <= NOW_MS / 1000) {
        (Some(_), _) => "finalized",
        (None, true) => "determined",
        (None, false) => "active",
    };
    (97..=105)
        .map(|k| {
            json!({
                "ticker": ticker(ev.0, k), "event_ticker": format!("KXTEST-{}", ev.0), "status": status,
                "strike_type": "greater", "floor_strike": k as f64, "open_time": iso(ev.1 - HOUR),
                "close_time": iso(ev.1), "created_time": iso(ev.1 - 2 * HOUR),
                "expiration_value": ev.2.map_or(String::new(), |v| v.to_string()),
                "settlement_ts": ev.2.map(|_| iso(ev.1 + 60)),
                "volume_fp": if k == 100 || k == 101 { "5.00" } else { "0.00" }, "open_interest_fp": "1.00"
            })
        })
        .collect()
}

fn candles(close: i64, n: i64) -> Vec<Value> {
    (0..n)
        .map(|i| json!({"end_period_ts": close - i * 60, "yes_bid": {"close_dollars": "0.40"}, "yes_ask": {"close_dollars": "0.45"}, "price": {}, "volume_fp": "0.00", "open_interest_fp": "1.00"}))
        .collect()
}

fn close_of(t: &str) -> i64 {
    events()
        .into_iter()
        .find(|e| t.starts_with(&format!("KXTEST-{}-", e.0)))
        .unwrap()
        .1
}

fn serve(server: &Server) {
    server.serve_json(
        "/historical/cutoff",
        // Trades move to the archive on their own cutoff: L0's fills straddle it.
        json!({"market_settled_ts": iso(OCT9 - 4 * DAY), "trades_created_ts": iso(OCT9 - DAY + 15 * HOUR)}),
    );
    let live: Vec<Value> = events().iter().filter(|e| !e.3).flat_map(markets).collect();
    server.serve_json("/markets", json!({"markets": live, "cursor": ""}));
    let archived: Vec<Value> = events()
        .iter()
        .filter(|e| e.3)
        .rev()
        .flat_map(markets)
        .collect();
    let old = Ev("OLD", OCT9 - 8 * DAY, Some(90.0), true);
    server.route("/historical/markets", move |request| {
        let page = match request.param("cursor").as_deref() {
            None => json!({"markets": archived, "cursor": "p2"}),
            _ => json!({"markets": markets(&old), "cursor": ""}),
        };
        Response::json(&page)
    });
    server.route("/markets/candlesticks", |request| {
        let tickers = request.param("market_tickers").unwrap();
        let markets: Vec<Value> = tickers
            .split(',')
            .filter(|t| !t.contains("-A") && *t != ticker("T0", 103))
            .map(|t| json!({"market_ticker": t, "candlesticks": candles(close_of(t), 3)}))
            .collect();
        Response::json(&json!({"markets": markets}))
    });
    for ev in events() {
        for k in 97..=105 {
            let t = ticker(ev.0, k);
            let close = ev.1;
            server.route(
                &format!("/historical/markets/{t}/candlesticks"),
                move |_| Response::json(&json!({"candlesticks": candles(close, 2)})),
            );
        }
    }
    for path in ["/markets/trades", "/historical/trades"] {
        server.route(path, |request| {
            let t = request.param("ticker").unwrap();
            let trade = json!({"trade_id": format!("{t}-1"), "ticker": t, "created_time": iso(close_of(&t) - 1800),
                "yes_price_dollars": "0.40", "no_price_dollars": "0.60", "count_fp": "2.00", "taker_side": "yes"});
            Response::json(&json!({"trades": [trade], "cursor": ""}))
        });
    }
}

fn config_for(server: &Server, root: &std::path::Path) -> AugmentConfig {
    let mut config = load_augment_config(repository_config_path().to_str()).unwrap();
    config.data_dir = root.to_path_buf();
    config.requests_per_second = 50.0;
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
        &mut s.phoenix.enabled,
        &mut s.tardis.enabled,
        &mut s.okx.enabled,
        &mut s.hyperliquid_s3.enabled,
    ] {
        *enabled = false;
    }
    s.kalshi.enabled = true;
    s.kalshi.base_url = server.base.clone();
    s.kalshi.requests_per_second = 20.0;
    s.kalshi.start_date = Some("2026-10-03".into());
    s.kalshi.series = vec![KalshiSeries {
        ticker: "KXTEST".into(),
        asset: Some("TEST".into()),
        kind: "crypto".into(),
        interval: 1,
        window_pct: 3.0,
        max_strikes: 3,
        max_life_hours: 26,
        trades: true,
    }];
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

fn per_event(config: &AugmentConfig) -> Value {
    let rows = rows(
        config,
        "SELECT event_ticker AS e, count(*) AS n, string_agg(DISTINCT strike::VARCHAR, ',' ORDER BY strike::VARCHAR) AS k,
                any_value(centre) AS c, any_value(centre_source) AS src
         FROM kalshi_candles_1m GROUP BY 1 ORDER BY 1",
    );
    json!(rows)
}

fn sealed_contract_days(config: &AugmentConfig) -> Vec<String> {
    let text = std::fs::read_to_string(config.data_dir.join("catalog.json")).unwrap();
    let catalog: Value = serde_json::from_str(&text).unwrap();
    let files = catalog["files"].as_array().unwrap();
    let mut days: Vec<String> = files
        .iter()
        .filter(|f| f["source"] == "kalshi" && f["dataset"] == "contracts" && f["complete"] == true)
        .map(|f| f["period"].as_str().unwrap().to_owned())
        .collect();
    days.sort();
    days
}

fn batch_tickers(server: &Server) -> Vec<String> {
    server
        .hits()
        .iter()
        .filter(|h| h.path == "/markets/candlesticks")
        .flat_map(|h| {
            h.param("market_tickers")
                .unwrap()
                .split(',')
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn both_tiers_fill_the_kept_strikes_and_rerun_idempotently() {
    let server = Server::start();
    serve(&server);
    let root = tempdir("solos-kalshi");
    let config = config_for(&server, &root);
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    let series = &status["kalshi"]["series"]["KXTEST"];
    assert_eq!(series["head"], "2026-10-09", "{status}");
    assert_eq!(series["tailDone"], true, "{status}");
    assert_eq!(series["histDone"], true, "{status}");
    let contracts = rows(
        &config,
        "SELECT count(*) AS n, min(close_day) AS first FROM kalshi_contracts",
    );
    assert_eq!(
        contracts[0]["n"], "63",
        "the market closing before the start day is left out"
    );
    assert_eq!(contracts[0]["first"], "2026-10-04");
    // Each event opens as its predecessor closes, and the predecessor settles a minute later:
    // A and L fall back to the median (nothing else settled within two days), T and T0 take
    // L's settlement (T0's own settlement came after T opened).
    let expected = json!([
        {"e": "KXTEST-A", "n": "6", "k": "100.0,101.0,102.0", "c": 101.0, "src": "median"},
        {"e": "KXTEST-A0", "n": "6", "k": "100.0,101.0,102.0", "c": 101.0, "src": "median"},
        {"e": "KXTEST-L", "n": "9", "k": "100.0,101.0,102.0", "c": 101.0, "src": "median"},
        {"e": "KXTEST-L0", "n": "9", "k": "100.0,101.0,102.0", "c": 101.0, "src": "median"},
        {"e": "KXTEST-T", "n": "9", "k": "101.0,102.0,103.0", "c": 102.3, "src": "previous_settle"},
        {"e": "KXTEST-T0", "n": "8", "k": "101.0,102.0,103.0", "c": 102.3, "src": "previous_settle"},
    ]);
    assert_eq!(per_event(&config), expected, "O is still open");
    // Contract days are sealed only when the listing proves them whole: the 8th (live, after
    // the frontier day, all final); not the 4th (the archive's oldest listed day sits one day
    // before the cutoff, where a market may still settle in the live tier) nor today.
    assert_eq!(
        sealed_contract_days(&config),
        vec!["2026-10-08".to_owned()],
        "{status}"
    );
    let batched = batch_tickers(&server);
    assert!(
        batched.iter().all(|t| !t.contains("-A")),
        "archived markets never go to the batch"
    );
    assert_eq!(
        server.hits_of(&format!(
            "/historical/markets/{}/candlesticks",
            ticker("T0", 103)
        )),
        1,
        "fallback"
    );
    let tiers = rows(
        &config,
        "SELECT tier, count(*) AS n FROM kalshi_candles_1m GROUP BY 1 ORDER BY 1",
    );
    assert_eq!(
        tiers,
        vec![
            json!({"tier": "historical", "n": "14"}),
            json!({"tier": "live", "n": "33"})
        ]
    );
    let trades = rows(
        &config,
        "SELECT count(*) AS n, count(DISTINCT ticker) AS m FROM kalshi_trades",
    );
    assert_eq!(
        trades[0]["n"], "10",
        "strikes 100 and 101 traded: two in A, A0, L, L0, one in T, T0"
    );
    let trade_hits = |path: &str, event: &str| {
        server
            .hits()
            .iter()
            .filter(|h| h.path == path)
            .filter(|h| h.param("ticker").is_some_and(|t| t.contains(event)))
            .count()
    };
    assert_eq!(
        trade_hits("/historical/trades", "-L0-"),
        2,
        "before the trade cutoff"
    );
    assert_eq!(trade_hits("/markets/trades", "-L0-"), 2, "after it");
    assert_eq!(
        trade_hits("/historical/trades", "-L-"),
        0,
        "opened after it"
    );
    assert_eq!(trade_hits("/markets/trades", "-A-"), 0, "closed before it");

    // Same clock again: today's stored events are not fetched again, nothing duplicates.
    let before = batched.len();
    let again = run(&config, NOW_MS);
    assert_eq!(again["totals"]["errors"], 0, "{again}");
    assert_eq!(batch_tickers(&server).len(), before);
    assert_eq!(per_event(&config), expected);

    // Three hours later the open event has closed and joins today's file.
    let later = run(&config, NOW_MS + 3 * 3_600_000);
    assert_eq!(later["totals"]["errors"], 0, "{later}");
    let events = per_event(&config);
    assert_eq!(events.as_array().unwrap().len(), 7);
    assert_eq!(events[4]["e"], "KXTEST-O");
    assert_eq!(events[4]["c"], 103.0, "T0 settled before O opened");
    assert_eq!(events[4]["src"], "previous_settle");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_request_budget_stops_the_run_and_the_next_one_resumes() {
    let server = Server::start();
    serve(&server);
    let root = tempdir("solos-kalshi");
    let mut config = config_for(&server, &root);
    // Enough for the largest day (ten requests) and the run's two fixed ones, not for all.
    config.sources.kalshi.max_requests = 12;
    let status = run(&config, NOW_MS);
    assert_eq!(status["totals"]["errors"], 0, "{status}");
    assert_eq!(status["kalshi"]["budgetSpent"], true, "{status}");
    assert_eq!(status["kalshi"]["series"]["KXTEST"]["tailDone"], false);
    for _ in 0..20 {
        let status = run(&config, NOW_MS);
        assert_eq!(status["totals"]["errors"], 0, "{status}");
        if status["kalshi"]["series"]["KXTEST"]["tailDone"] == true {
            break;
        }
    }
    let events = per_event(&config);
    assert_eq!(events.as_array().unwrap().len(), 6, "{events}");
    let dupes = rows(
        &config,
        "SELECT count(*) - count(DISTINCT (ticker, t)) AS d FROM kalshi_candles_1m",
    );
    assert_eq!(dupes[0]["d"], "0");
    let _ = std::fs::remove_dir_all(&root);
}
