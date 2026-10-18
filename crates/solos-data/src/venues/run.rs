//! `venues run`: the long-running lane (ADR-0012). Per venue, a transaction loop walks finalized
//! slot windows forward from its cursor; one loop polls the venues' accounts, one lists the
//! discoverable account types, one polls the Pyth feeds; rows are flushed into hour files every
//! `flushSeconds`, and the status and catalog are written every minute. SIGTERM ends the loops
//! at their next check; the buffer is flushed and the status written before exit.

use super::accounts::{self, Seen};
use super::config::{Account, Venue, VenuesConfig};
use super::idl::Idl;
use super::pyth::{self, Last};
use super::rpc::{credits, provider};
use super::store::{self, Buffer, cursor_key, cursor_value};
use super::walk::{self, Cycle};
use crate::augment::http::Http;
use crate::augment::ledger::{self, Lane, get_progress};
use crate::augment::series::Ctx;
use crate::collector::rpc::{Provider, Rpc};
use crate::db::Db;
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use serde_json::{Value, json};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Counters of one venue.
#[derive(Default)]
struct Stats {
    cursor: Option<i64>,
    finalized: Option<i64>,
    cycles: u64,
    totals: Cycle,
    errors: u64,
    error_streak: u64,
    last_cycle_at: Option<String>,
    last_error: Option<String>,
    last_error_at: Option<String>,
    account_rows: u64,
    account_errors: u64,
    discovered: usize,
}

/// One venue's state.
struct VenueState {
    venue: Venue,
    idl: Idl,
    discovered: Mutex<Vec<Account>>,
    stats: Mutex<Stats>,
}

#[derive(Default)]
struct PythStats {
    polls: u64,
    rows: u64,
    errors: u64,
    mismatched: u64,
    stale: u64,
    last_poll_at: Option<String>,
    last_error: Option<String>,
    /// Newest publish time read per symbol, stored or not.
    published: std::collections::BTreeMap<String, i64>,
}

struct Lanes {
    ctx: Ctx,
    rpc: Arc<Provider>,
    config: VenuesConfig,
    venues: Vec<VenueState>,
    buffer: Buffer,
    seen: Seen,
    last: Last,
    pyth: Mutex<PythStats>,
    flushes: Mutex<store::Flushed>,
    started_at: String,
    started: std::time::Instant,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn pause(ctx: &Ctx, seconds: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline && !ctx.stopping() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Load every enabled venue's IDL.
///
/// # Errors
///
/// A missing or unparsable IDL file.
pub fn load_idls(config: &VenuesConfig) -> Result<Vec<(Venue, Idl)>, StoreError> {
    let mut out = Vec::new();
    for venue in config.venues.iter().filter(|v| v.enabled) {
        let path = config.idl_path(venue);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| StoreError::Check(format!("IDL {}: {e}", path.display())))?;
        let idl = Idl::parse(&text)
            .map_err(|e| StoreError::Check(format!("IDL {}: {e}", path.display())))?;
        out.push((venue.clone(), idl));
    }
    Ok(out)
}

/// Run the lane until the stop flag is set; returns the final status.
///
/// # Errors
///
/// When the ledger cannot be opened or the IDLs or the RPC URL are missing.
pub fn run(config: VenuesConfig, stop: Arc<AtomicBool>) -> Result<Obj, StoreError> {
    let idls = load_idls(&config)?;
    let root = config.data_dir.clone();
    let mut store = ledger::open(&root, Lane::Venues)?;
    let recovered = ledger::recover(&mut store, &root, Lane::Venues)?;
    let (db, thread) = Db::spawn(store);
    let rpc = provider(config.requests_per_second, 8, Arc::clone(&stop))?;
    let ctx = Ctx {
        http: Http::new(1.0)?,
        db: db.clone(),
        root,
        lane: Lane::Venues,
        start: chrono::Utc::now().date_naive(),
        now_ms: 0,
        stop,
        disk_budget_bytes: 0,
    };
    let venues = idls
        .into_iter()
        .map(|(venue, idl)| VenueState {
            venue,
            idl,
            discovered: Mutex::new(Vec::new()),
            stats: Mutex::new(Stats::default()),
        })
        .collect();
    let lanes = Lanes {
        ctx,
        rpc,
        config,
        venues,
        buffer: Buffer::default(),
        seen: Seen::default(),
        last: Last::default(),
        pyth: Mutex::new(PythStats::default()),
        flushes: Mutex::new(store::Flushed::default()),
        started_at: now(),
        started: std::time::Instant::now(),
    };
    log(
        "venues_start",
        Obj::new()
            .with(
                "venues",
                lanes
                    .venues
                    .iter()
                    .map(|v| v.venue.name.clone())
                    .collect::<Vec<_>>(),
            )
            .with(
                "pythFeeds",
                if lanes.config.pyth.enabled {
                    lanes.config.pyth.feeds.len()
                } else {
                    0
                },
            )
            .with("requestsPerSecond", lanes.config.requests_per_second)
            .with("recovered", recovered),
    );
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let tx_loops =
            futures_util::future::join_all(lanes.venues.iter().map(|v| tx_loop(&lanes, v)));
        tokio::join!(
            tx_loops,
            discover_loop(&lanes),
            accounts_loop(&lanes),
            pyth_loop(&lanes),
            flush_loop(&lanes),
            status_loop(&lanes)
        );
    });
    let flushed = runtime.block_on(store::flush(&lanes.ctx, &lanes.buffer, now_ms()));
    add_flush(&lanes, flushed);
    let status = runtime.block_on(write_status(&lanes, true))?;
    drop(runtime);
    let Lanes { ctx, .. } = lanes;
    drop(ctx);
    let store = thread.join(db);
    store.close()?;
    log("venues_done", Obj::new().with("buffered", 0));
    Ok(status)
}

fn add_flush(lanes: &Lanes, flushed: store::Flushed) {
    let mut total = lanes.flushes.lock().expect("flushes");
    total.files += flushed.files;
    total.rows += flushed.rows;
    total.errors += flushed.errors;
}

fn record_error(state: &VenueState, error: &str) {
    let mut stats = state.stats.lock().expect("venue stats");
    stats.errors += 1;
    stats.error_streak += 1;
    stats.last_error = Some(safe_error(error));
    stats.last_error_at = Some(now());
    drop(stats);
    log(
        "venues_error",
        Obj::new()
            .with("source", state.venue.name.as_str())
            .with("item", "cycle")
            .with("error", safe_error(error)),
    );
}

async fn tx_loop(lanes: &Lanes, state: &VenueState) {
    let ctx = &lanes.ctx;
    let name = state.venue.name.clone();
    let key = cursor_key(&name);
    let stored = {
        let key = key.clone();
        ctx.db.run(move |s| get_progress(s, &key)).await
    };
    let mut cursor = match stored {
        Ok(value) => value.and_then(|v| v.get("next").and_then(Value::as_i64)),
        Err(error) => {
            record_error(state, &error.to_string());
            return;
        }
    };
    let rpc: &dyn Rpc = lanes.rpc.as_ref();
    while !ctx.stopping() {
        let finalized = match walk::finalized_slot(rpc).await {
            Ok(slot) => slot,
            Err(error) => {
                record_error(state, &error);
                pause(ctx, lanes.config.cycle_seconds).await;
                continue;
            }
        };
        let from = *cursor.get_or_insert(finalized);
        state.stats.lock().expect("venue stats").finalized = Some(finalized);
        if from > finalized {
            pause(ctx, lanes.config.cycle_seconds).await;
            continue;
        }
        let to = finalized.min(from + lanes.config.max_slots_per_cycle - 1);
        let program = state.venue.program_id.as_str();
        match walk::cycle(
            rpc,
            program,
            &state.idl,
            (from, to),
            state.venue.store_transactions,
        )
        .await
        {
            Ok((cycle, rows)) => {
                let sets = vec![
                    (store::TRANSACTIONS.dataset, rows.transactions),
                    (store::EVENTS.dataset, rows.events),
                    (store::INSTRUCTIONS.dataset, rows.instructions),
                ];
                lanes
                    .buffer
                    .push_cycle(&name, sets, &key, cursor_value(to + 1));
                cursor = Some(to + 1);
                let mut stats = state.stats.lock().expect("venue stats");
                stats.cursor = cursor;
                stats.cycles += 1;
                stats.error_streak = 0;
                stats.last_cycle_at = Some(now());
                add_cycle(&mut stats.totals, &cycle);
                drop(stats);
                log(
                    "venues_cycle",
                    cycle_obj(&name, &cycle).with("lagSlots", finalized - to),
                );
            }
            Err(error) => {
                record_error(state, &error);
                pause(ctx, lanes.config.cycle_seconds).await;
                continue;
            }
        }
        if to >= finalized {
            pause(ctx, lanes.config.cycle_seconds).await;
        }
    }
}

fn add_cycle(total: &mut Cycle, c: &Cycle) {
    total.manifest += c.manifest;
    total.failed += c.failed;
    total.bulk += c.bulk;
    total.single += c.single;
    total.unlisted += c.unlisted;
    total.missing += c.missing;
    total.events += c.events;
    total.undecoded += c.undecoded;
    total.unknown += c.unknown;
    total.instructions += c.instructions;
    total.unparsed += c.unparsed;
}

fn cycle_obj(name: &str, c: &Cycle) -> Obj {
    Obj::new()
        .with("source", name)
        .with("from", c.from)
        .with("to", c.to)
        .with("manifest", c.manifest)
        .with("failed", c.failed)
        .with("bulk", c.bulk)
        .with("single", c.single)
        .with("unlisted", c.unlisted)
        .with("events", c.events)
        .with("undecoded", c.undecoded)
        .with("unknown", c.unknown)
        .with("instructions", c.instructions)
        .with("unparsed", c.unparsed)
}

async fn discover_loop(lanes: &Lanes) {
    let rpc: &dyn Rpc = lanes.rpc.as_ref();
    while !lanes.ctx.stopping() {
        for state in &lanes.venues {
            let mut found: Vec<Account> = Vec::new();
            let mut failed = false;
            for type_name in &state.venue.discover {
                match accounts::discover(rpc, &state.venue.program_id, &state.idl, type_name).await
                {
                    Ok(addresses) => found.extend(addresses.into_iter().map(|address| Account {
                        address,
                        kind: type_name.to_lowercase(),
                        label: None,
                    })),
                    Err(error) => {
                        failed = true;
                        record_error(state, &format!("discover {type_name}: {error}"));
                    }
                }
            }
            if !failed && !state.venue.discover.is_empty() {
                state.stats.lock().expect("venue stats").discovered = found.len();
                *state.discovered.lock().expect("discovered") = found;
            }
        }
        pause(&lanes.ctx, lanes.config.discover_interval_seconds.max(60)).await;
    }
}

async fn accounts_loop(lanes: &Lanes) {
    let rpc: &dyn Rpc = lanes.rpc.as_ref();
    // Let the first discovery land before the first poll.
    pause(&lanes.ctx, 5).await;
    while !lanes.ctx.stopping() {
        for state in &lanes.venues {
            let mut targets = state.venue.accounts.clone();
            for found in state.discovered.lock().expect("discovered").iter() {
                if !targets.iter().any(|t| t.address == found.address) {
                    targets.push(found.clone());
                }
            }
            if targets.is_empty() {
                continue;
            }
            let addresses: Vec<String> = targets.iter().map(|t| t.address.clone()).collect();
            let at = now_ms();
            match accounts::read(rpc, &addresses).await {
                Ok((slot, reads)) => {
                    let mut rows = Vec::new();
                    for read in &reads {
                        let (due, changed) = lanes.seen.due(&read.address, &read.data, at);
                        if let (true, Some(target)) =
                            (due, targets.iter().find(|t| t.address == read.address))
                        {
                            rows.push(accounts::row(&state.idl, target, read, at, slot, changed));
                        }
                    }
                    state.stats.lock().expect("venue stats").account_rows += rows.len() as u64;
                    lanes
                        .buffer
                        .push(&state.venue.name, store::ACCOUNTS.dataset, rows);
                }
                Err(error) => {
                    state.stats.lock().expect("venue stats").account_errors += 1;
                    log(
                        "venues_error",
                        Obj::new()
                            .with("source", state.venue.name.as_str())
                            .with("item", "accounts")
                            .with("error", safe_error(&error)),
                    );
                }
            }
        }
        pause(&lanes.ctx, lanes.config.accounts_interval_seconds.max(5)).await;
    }
}

async fn pyth_loop(lanes: &Lanes) {
    let cfg = &lanes.config.pyth;
    if !cfg.enabled || cfg.feeds.is_empty() {
        return;
    }
    let rpc: &dyn Rpc = lanes.rpc.as_ref();
    let addresses: Vec<String> = cfg.feeds.iter().map(|f| f.account.clone()).collect();
    while !lanes.ctx.stopping() {
        let at = now_ms();
        match accounts::read(rpc, &addresses).await {
            Ok((slot, reads)) => {
                let mut rows = Vec::new();
                let mut errors = 0;
                let mut mismatched = 0;
                let mut stale = 0;
                let mut published = Vec::new();
                for read in &reads {
                    let Some(feed) = cfg.feeds.iter().find(|f| f.account == read.address) else {
                        continue;
                    };
                    match pyth::decode(&read.data) {
                        Ok(update)
                            if update.feed_id != feed.feed_id || read.owner != pyth::RECEIVER =>
                        {
                            mismatched += 1
                        }
                        Ok(update) => {
                            published.push((feed.symbol.clone(), update.publish_time));
                            if at / 1000 - update.publish_time > cfg.max_age_seconds {
                                stale += 1;
                            } else if lanes.last.newer(&update) {
                                rows.push(pyth::row(feed, &read.address, &update, at, slot));
                            }
                        }
                        Err(_) => errors += 1,
                    }
                }
                let mut stats = lanes.pyth.lock().expect("pyth stats");
                stats.polls += 1;
                stats.rows += rows.len() as u64;
                stats.errors += errors;
                stats.mismatched += mismatched;
                stats.stale = stale;
                stats.published.extend(published);
                stats.last_poll_at = Some(now());
                drop(stats);
                lanes.buffer.push("pyth", store::PRICES.dataset, rows);
            }
            Err(error) => {
                let mut stats = lanes.pyth.lock().expect("pyth stats");
                stats.errors += 1;
                stats.last_error = Some(safe_error(&error));
            }
        }
        pause(&lanes.ctx, cfg.interval_seconds.max(1)).await;
    }
}

async fn flush_loop(lanes: &Lanes) {
    while !lanes.ctx.stopping() {
        pause(&lanes.ctx, lanes.config.flush_seconds).await;
        if lanes.ctx.stopping() {
            break;
        }
        let flushed = store::flush(&lanes.ctx, &lanes.buffer, now_ms()).await;
        add_flush(lanes, flushed);
    }
}

async fn status_loop(lanes: &Lanes) {
    while !lanes.ctx.stopping() {
        pause(&lanes.ctx, 60).await;
        if let Err(error) = write_status(lanes, false).await {
            log(
                "venues_status_error",
                Obj::new().with("error", safe_error(&error.to_string())),
            );
        }
    }
}

fn venue_obj(state: &VenueState) -> Obj {
    let s = state.stats.lock().expect("venue stats");
    let t = &s.totals;
    Obj::new()
        .with("programId", state.venue.program_id.as_str())
        .with("idlSource", state.venue.idl_source.clone())
        .with("cursor", s.cursor)
        .with("finalized", s.finalized)
        .with(
            "lagSlots",
            s.cursor.zip(s.finalized).map(|(c, f)| (f + 1 - c).max(0)),
        )
        .with("cycles", s.cycles)
        .with("manifest", t.manifest)
        .with("failedSkipped", t.failed)
        .with("bulk", t.bulk)
        .with("single", t.single)
        .with("unlisted", t.unlisted)
        .with("events", t.events)
        .with("undecoded", t.undecoded)
        .with("unknown", t.unknown)
        .with("instructions", t.instructions)
        .with("unparsed", t.unparsed)
        .with("accountRows", s.account_rows)
        .with("accountErrors", s.account_errors)
        .with("discovered", s.discovered)
        .with("errors", s.errors)
        .with("errorStreak", s.error_streak)
        .with("lastCycleAt", s.last_cycle_at.clone())
        .with("lastError", s.last_error.clone())
        .with("lastErrorAt", s.last_error_at.clone())
}

async fn write_status(lanes: &Lanes, final_write: bool) -> Result<Obj, StoreError> {
    let root = lanes.ctx.root.clone();
    let summary = lanes
        .ctx
        .db
        .run(move |store| {
            ledger::write_catalog(store, &root, Lane::Venues)?;
            ledger::summary(store)
        })
        .await?;
    let counters = lanes.rpc.counters().to_value();
    let spent = credits(&counters, &lanes.config.credit_costs);
    let hours = lanes.started.elapsed().as_secs_f64() / 3600.0;
    let mut venues = Obj::new();
    for state in &lanes.venues {
        venues.set_obj(&state.venue.name, venue_obj(state));
    }
    let pyth = {
        let p = lanes.pyth.lock().expect("pyth stats");
        Obj::new()
            .with("enabled", lanes.config.pyth.enabled)
            .with("feeds", lanes.config.pyth.feeds.len())
            .with("polls", p.polls)
            .with("rows", p.rows)
            .with("errors", p.errors)
            .with("mismatched", p.mismatched)
            .with("staleFeeds", p.stale)
            .with("maxAgeSeconds", lanes.config.pyth.max_age_seconds)
            .with_obj("feedAgeSeconds", {
                let now_s = now_ms() / 1000;
                let mut ages = Obj::new();
                for feed in &lanes.config.pyth.feeds {
                    ages.set(
                        &feed.symbol,
                        p.published.get(&feed.symbol).map(|t| now_s - t),
                    );
                }
                ages
            })
            .with("lastPollAt", p.last_poll_at.clone())
            .with("lastError", p.last_error.clone())
    };
    let flushed = *lanes.flushes.lock().expect("flushes");
    let status = Obj::new()
        .with("at", now())
        .with("lane", "venues")
        .with("startedAt", lanes.started_at.as_str())
        .with("running", !final_write)
        .with_obj("venues", venues)
        .with_obj("pyth", pyth)
        .with_obj(
            "rpc",
            Obj::new()
                .with("requestsPerSecond", lanes.config.requests_per_second)
                .with("credits", spent)
                .with(
                    "creditsPerDay",
                    if hours > 0.0 {
                        json!((spent / hours * 24.0).round())
                    } else {
                        Value::Null
                    },
                )
                .with("hours", hours)
                .with("counters", counters),
        )
        .with_obj(
            "flushes",
            Obj::new()
                .with("files", flushed.files)
                .with("rows", flushed.rows)
                .with("errors", flushed.errors)
                .with("buffered", lanes.buffer.len()),
        )
        .with_rows("datasets", summary);
    ledger::write_status(&lanes.ctx.root, Lane::Venues, &status)?;
    Ok(status)
}
