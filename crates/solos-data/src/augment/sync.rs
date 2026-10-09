//! `augment sync`: open the ledger, drop what it does not know, run every enabled source for the
//! configured symbols, write `catalog.json` while files land and `status.json` at the end.

use super::config::AugmentConfig;
use super::defillama::StablecoinChart;
use super::deribit::Dvol;
use super::http::Http;
use super::hyperliquid::FundingHistory;
use super::ledger::{self, Lane};
use super::series::{Ctx, Outcome, Series, budget_bytes, sync_series};
use crate::db::Db;
use crate::jsonout::{Obj, log, now};
use crate::store::StoreError;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Which sources and symbols a run covers.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    /// One source, or every enabled one.
    pub source: Option<String>,
    /// One Phoenix (or venue) symbol, or every mapped one.
    pub symbol: Option<String>,
}

impl Filter {
    fn allows(&self, source: &str) -> bool {
        self.source.as_deref().is_none_or(|s| s == source)
    }

    fn symbol_allows(&self, phoenix: &str, venue: &str) -> bool {
        self.symbol
            .as_deref()
            .is_none_or(|s| s == phoenix || s == venue)
    }
}

/// Run one sync at `now_ms` and return the status object. Item failures are counted in the
/// status; the function fails only when the ledger itself cannot be used.
pub fn run_sync(
    config: &AugmentConfig,
    now_ms: i64,
    filter: &Filter,
    stop: Arc<AtomicBool>,
) -> Result<Obj, StoreError> {
    let started = std::time::Instant::now();
    let started_at = now();
    let start = super::periods::parse_date(&config.start_date)
        .ok_or_else(|| StoreError::Check("Invalid startDate".into()))?;
    let mut store = ledger::open(&config.data_dir, Lane::Sync)?;
    let recovered = ledger::recover(&mut store, &config.data_dir, Lane::Sync)?;
    if recovered > 0 {
        log("augment_recovered", Obj::new().with("removed", recovered));
    }
    let (db, thread) = Db::spawn(store);
    let http = Http::new(config.requests_per_second)?;
    http.set_host_rate(
        &config.sources.hyperliquid.base_url,
        config.sources.hyperliquid.requests_per_second,
    );
    let ctx = Ctx {
        http,
        db: db.clone(),
        root: config.data_dir.clone(),
        lane: Lane::Sync,
        start,
        now_ms,
        stop,
        disk_budget_bytes: budget_bytes(config.disk_budget_gb),
    };
    log(
        "augment_sync_start",
        Obj::new()
            .with("startDate", config.start_date.as_str())
            .with("symbols", config.symbols.len())
            .with("source", filter.source.clone())
            .with("symbol", filter.symbol.clone()),
    );
    let runtime = tokio::runtime::Runtime::new()?;
    let sources = runtime.block_on(run_sources(&ctx, config, filter));
    let mut totals = Outcome::default();
    let mut per_source = Obj::new();
    for (name, outcome) in &sources {
        totals.add(outcome);
        per_source.set_obj(name, outcome.to_obj());
    }
    let root = config.data_dir.clone();
    let summary = db.run_blocking(move |store| {
        ledger::write_catalog(store, &root, Lane::Sync)?;
        ledger::summary(store)
    })?;
    let status = Obj::new()
        .with("at", now())
        .with("lane", "sync")
        .with("startedAt", started_at)
        .with("durationSeconds", started.elapsed().as_secs_f64())
        .with("asOf", super::periods::date_of_ms(now_ms).to_string())
        .with("stopped", ctx.stopping())
        .with("recovered", recovered)
        .with_obj("sources", per_source)
        .with_obj("totals", totals.to_obj())
        .with_rows("datasets", summary);
    ledger::write_status(&config.data_dir, Lane::Sync, &status)?;
    let store = thread.join(db);
    store.close()?;
    log(
        "augment_sync_done",
        Obj::new()
            .with("files", totals.files)
            .with("rows", totals.rows)
            .with("errors", totals.errors)
            .with("durationSeconds", started.elapsed().as_secs_f64()),
    );
    Ok(status)
}

async fn run_sources(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Vec<(String, Outcome)> {
    let catalog_writer = {
        let db = ctx.db.clone();
        let root = ctx.root.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let root = root.clone();
                let _ = db
                    .run(move |store| ledger::write_catalog(store, &root, Lane::Sync))
                    .await;
            }
        })
    };
    let (binance, hyperliquid, deribit, defillama, bybit, exogenous) = tokio::join!(
        run_binance(ctx, config, filter),
        run_hyperliquid(ctx, config, filter),
        run_deribit(ctx, config, filter),
        run_defillama(ctx, config, filter),
        run_bybit(ctx, config, filter),
        run_exogenous(ctx, config, filter),
    );
    catalog_writer.abort();
    let (sec, alternative, polymarket, kalshi, phoenix) = exogenous;
    [
        ("binance", binance),
        ("hyperliquid", hyperliquid),
        ("deribit", deribit),
        ("defillama", defillama),
        ("bybit", bybit),
        ("sec", sec),
        ("alternative", alternative),
        ("polymarket", polymarket),
        ("kalshi", kalshi),
        ("phoenix", phoenix),
    ]
    .into_iter()
    .filter_map(|(name, outcome)| outcome.map(|o| (name.to_owned(), o)))
    .collect()
}

type Five = (
    Option<Outcome>,
    Option<Outcome>,
    Option<Outcome>,
    Option<Outcome>,
    Option<Outcome>,
);

/// The exogenous sources (ADR-0009, triggers and alerts): SEC filings, Fear and Greed, the
/// prediction markets and Phoenix's earnings dates, each on its own host.
async fn run_exogenous(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Five {
    let symbol = filter.symbol.as_deref();
    let s = &config.sources;
    tokio::join!(
        async {
            (s.sec.enabled && filter.allows("sec"))
                .then(|| super::sec::sync(ctx, config, symbol))?
                .await
                .into()
        },
        async {
            if !s.alternative.enabled || !filter.allows("alternative") {
                return None;
            }
            let series = super::alternative::FearGreed::new(&s.alternative.base_url);
            Some(sync_series(ctx, &series).await)
        },
        async {
            (s.polymarket.enabled && filter.allows("polymarket"))
                .then(|| super::polymarket::sync(ctx, &s.polymarket, symbol))?
                .await
                .into()
        },
        async {
            (s.kalshi.enabled && filter.allows("kalshi"))
                .then(|| super::kalshi::sync(ctx, &s.kalshi, symbol))?
                .await
                .into()
        },
        async {
            (s.phoenix.enabled && filter.allows("phoenix"))
                .then(|| super::phoenix::sync(ctx, &s.phoenix, ctx.now_ms))?
                .await
                .into()
        },
    )
}

async fn run_binance(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Option<Outcome> {
    let cfg = &config.sources.binance;
    if !cfg.enabled || !filter.allows("binance") {
        return None;
    }
    Some(super::binance::sync(ctx, cfg, &config.symbols, filter.symbol.as_deref()).await)
}

async fn run_hyperliquid(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Option<Outcome> {
    let cfg = &config.sources.hyperliquid;
    if !cfg.enabled || !cfg.funding_history || !filter.allows("hyperliquid") {
        return None;
    }
    let url = format!("{}/info", cfg.base_url.trim_end_matches('/'));
    let mut outcome = Outcome::default();
    for symbol in &config.symbols {
        let Some(coin) = symbol.hyperliquid.as_deref() else {
            continue;
        };
        if !filter.symbol_allows(&symbol.phoenix, coin) {
            continue;
        }
        if ctx.stopping() {
            break;
        }
        let series = FundingHistory {
            url: url.clone(),
            coin: coin.to_owned(),
            phoenix: symbol.phoenix.clone(),
        };
        outcome.add(&sync_series(ctx, &series).await);
    }
    Some(outcome)
}

async fn run_bybit(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Option<Outcome> {
    let cfg = &config.sources.bybit;
    if !cfg.enabled || !filter.allows("bybit") {
        return None;
    }
    let mut outcome = Outcome::default();
    if cfg.trades {
        outcome.add(
            &super::bybit::sync_trades(ctx, cfg, &config.symbols, filter.symbol.as_deref()).await,
        );
    }
    if !cfg.funding_history {
        return Some(outcome);
    }
    for symbol in &config.symbols {
        let Some(venue) = symbol.bybit.as_deref() else {
            continue;
        };
        if !filter.symbol_allows(&symbol.phoenix, venue) || ctx.stopping() {
            continue;
        }
        let series = super::bybit::FundingHistory {
            base_url: cfg.base_url.clone(),
            symbol: venue.to_owned(),
            phoenix: symbol.phoenix.clone(),
        };
        outcome.add(&sync_series(ctx, &series).await);
    }
    Some(outcome)
}

async fn run_deribit(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Option<Outcome> {
    let cfg = &config.sources.deribit;
    if !cfg.enabled || !filter.allows("deribit") {
        return None;
    }
    let mut outcome = Outcome::default();
    for currency in &cfg.currencies {
        if !filter.symbol_allows(currency, currency) {
            continue;
        }
        for resolution in &cfg.resolutions {
            if ctx.stopping() {
                break;
            }
            let series = Dvol::new(&cfg.base_url, currency, *resolution);
            outcome.add(&sync_series(ctx, &series).await);
        }
    }
    Some(outcome)
}

async fn run_defillama(ctx: &Ctx, config: &AugmentConfig, filter: &Filter) -> Option<Outcome> {
    let cfg = &config.sources.defillama;
    if !cfg.enabled || !filter.allows("defillama") {
        return None;
    }
    let mut charts: Vec<StablecoinChart> = vec![StablecoinChart::total(&cfg.base_url)];
    charts.extend(
        cfg.stablecoins
            .iter()
            .map(|coin| StablecoinChart::coin(&cfg.base_url, &coin.id, &coin.symbol)),
    );
    let mut outcome = Outcome::default();
    for chart in &charts {
        if !filter.symbol_allows(chart.symbol(), chart.symbol()) || ctx.stopping() {
            continue;
        }
        outcome.add(&sync_series(ctx, chart).await);
    }
    Some(outcome)
}
