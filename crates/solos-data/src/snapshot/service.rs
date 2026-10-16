//! `snapshot run`: the long-running lane. Five loops share one RPC client (its own request
//! ceiling) and one staging area: the exchange view every `exchangeRefreshSeconds` (which markets
//! exist and where their accounts live), order books with their spline collections, the global
//! trader index and the active trader buffer every `bookIntervalSeconds`, the perp asset map every `marketIntervalSeconds`,
//! every trader account every `traderIntervalSeconds`, and the flush and status writer. A book,
//! spline collection or trader that did not change since the previous read gets a summary row
//! only; every account is written in full (decoded and raw) once after each flush, so each file
//! set starts from a complete baseline. SIGTERM stops the loops at their next check; staged rows
//! are published before exit.

use super::config::SnapshotConfig;
use super::decode::{self, At, TraderIndex};
use super::ledger::{self, Batch, Stager, date_of_ms};
use super::rpc::{Account, StateRpc};
use crate::db::Db;
use crate::fsutil::{sha256_hex, write_atomic};
use crate::jsonout::{Obj, log, now, safe_error};
use crate::store::StoreError;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One market from the exchange view.
#[derive(Clone, Debug)]
pub struct Market {
    /// Symbol.
    pub symbol: String,
    /// Asset id.
    pub asset_id: u64,
    /// Status as the view reports it.
    pub status: String,
    /// Order book account.
    pub book: String,
    /// Spline collection account.
    pub spline: String,
}

/// What the exchange view says.
#[derive(Clone, Debug, Default)]
pub struct Exchange {
    /// Markets.
    pub markets: Vec<Market>,
    /// Perp asset map account.
    pub asset_map: String,
    /// Global trader index accounts, header first.
    pub index: Vec<String>,
    /// Active trader buffer accounts, header first (empty when the view lists none).
    pub active: Vec<String>,
    /// The raw view (validation reads its stats snapshots).
    pub view: Value,
}

/// Parse the exchange view.
pub fn parse_exchange(view: Value) -> Result<Exchange, String> {
    let keys = view.get("keys").ok_or("exchange view has no keys")?;
    let asset_map = keys
        .get("perpAssetMap")
        .and_then(Value::as_str)
        .ok_or("exchange view has no perpAssetMap")?
        .to_owned();
    let list = |name: &str| -> Vec<String> {
        keys.get(name)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect()
    };
    let index = list("globalTraderIndex");
    let active = list("activeTraderBuffer");
    let mut markets = Vec::new();
    for m in view
        .get("markets")
        .and_then(Value::as_array)
        .ok_or("exchange view has no markets")?
    {
        let text = |k: &str| m.get(k).and_then(Value::as_str).map(str::to_owned);
        let (Some(symbol), Some(book), Some(spline)) =
            (text("symbol"), text("marketPubkey"), text("splinePubkey"))
        else {
            return Err("exchange market without symbol or accounts".into());
        };
        markets.push(Market {
            symbol,
            asset_id: m.get("assetId").and_then(Value::as_u64).unwrap_or(0),
            status: text("marketStatus").unwrap_or_default(),
            book,
            spline,
        });
    }
    if index.is_empty() || markets.is_empty() {
        return Err("exchange view lists no markets or no trader index".into());
    }
    if index.len() + active.len() > 100 {
        return Err(
            "exchange view lists more trader index and buffer accounts than one call reads".into(),
        );
    }
    Ok(Exchange {
        markets,
        asset_map,
        index,
        active,
        view,
    })
}

#[derive(Default)]
struct Seen {
    sha: String,
    rows_epoch: u64,
    raw_sha: String,
    raw_ms: i64,
    raw_epoch: u64,
}

#[derive(Default, Clone)]
struct LaneStats {
    ticks: u64,
    errors: u64,
    last_at: Option<String>,
    last_slot: u64,
    last_error: Option<String>,
    detail: Obj,
}

impl LaneStats {
    fn to_obj(&self) -> Obj {
        let mut obj = Obj::new()
            .with("ticks", self.ticks)
            .with("errors", self.errors)
            .with("lastAt", self.last_at.clone())
            .with("lastSlot", self.last_slot)
            .with("lastError", self.last_error.clone());
        obj.extend(self.detail.clone());
        obj
    }
}

#[derive(Default)]
struct State {
    exchange: Option<Arc<Exchange>>,
    exchange_at: Option<String>,
    index: Option<Arc<TraderIndex>>,
    index_slot: u64,
    symbols: HashMap<u64, String>,
    seen: HashMap<String, Seen>,
    epoch: u64,
    lanes: HashMap<&'static str, LaneStats>,
    published: (u64, i64, i64),
}

/// The running lane.
pub struct Lane {
    config: SnapshotConfig,
    rpc: Arc<dyn StateRpc>,
    http: reqwest::Client,
    db: Db,
    root: PathBuf,
    stop: Arc<AtomicBool>,
    stager: Mutex<Stager>,
    pending: Mutex<Vec<Batch>>,
    state: Mutex<State>,
    started_at: String,
    started: Instant,
}

/// Rows grouped by dataset, staged together.
type Rows = Vec<(&'static str, Vec<Value>)>;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Lane {
    /// A lane over an opened ledger.
    pub fn new(
        config: SnapshotConfig,
        rpc: Arc<dyn StateRpc>,
        db: Db,
        stop: Arc<AtomicBool>,
    ) -> Result<Lane, StoreError> {
        let root = config.data_dir.clone();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| StoreError::Check(safe_error(&e.to_string())))?;
        Ok(Lane {
            stager: Mutex::new(Stager::new(&root)?),
            root,
            config,
            rpc,
            http,
            db,
            stop,
            pending: Mutex::new(Vec::new()),
            state: Mutex::new(State {
                epoch: 1,
                ..State::default()
            }),
            started_at: now(),
            started: Instant::now(),
        })
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Set the exchange directly (tests and `once` with a fixture).
    pub fn set_exchange(&self, exchange: Exchange) {
        let mut state = self.state.lock().expect("state");
        state.symbols = exchange
            .markets
            .iter()
            .map(|m| (m.asset_id, m.symbol.clone()))
            .collect();
        state.exchange = Some(Arc::new(exchange));
        state.exchange_at = Some(now());
    }

    /// The exchange in use.
    #[must_use]
    pub fn exchange(&self) -> Option<Arc<Exchange>> {
        self.state.lock().expect("state").exchange.clone()
    }

    /// The latest trader index.
    #[must_use]
    pub fn index(&self) -> Option<Arc<TraderIndex>> {
        self.state.lock().expect("state").index.clone()
    }

    fn record(&self, lane: &'static str, outcome: Result<(u64, Obj), String>) {
        let mut state = self.state.lock().expect("state");
        let stats = state.lanes.entry(lane).or_default();
        stats.ticks += 1;
        match outcome {
            Ok((slot, detail)) => {
                stats.last_at = Some(now());
                stats.last_slot = slot;
                stats.detail = detail;
            }
            Err(error) => {
                stats.errors += 1;
                stats.last_error = Some(safe_error(&error));
                log(
                    "snapshot_error",
                    Obj::new()
                        .with("lane", lane)
                        .with("error", safe_error(&error)),
                );
            }
        }
    }

    /// Fetch the exchange view.
    pub async fn refresh_exchange(&self) -> Result<(), String> {
        let response = self
            .http
            .get(&self.config.exchange_url)
            .send()
            .await
            .map_err(|_| "exchange view request failed".to_owned())?;
        if !response.status().is_success() {
            return Err(format!("exchange view HTTP {}", response.status().as_u16()));
        }
        let view: Value = response
            .json()
            .await
            .map_err(|_| "exchange view is not JSON".to_owned())?;
        let exchange = parse_exchange(view)?;
        let count = exchange.markets.len();
        self.set_exchange(exchange);
        self.record("exchange", Ok((0, Obj::new().with("markets", count))));
        Ok(())
    }

    fn stage(&self, captured_ms: i64, rows: Rows) -> Result<(), StoreError> {
        let date = date_of_ms(captured_ms);
        let mut stager = self.stager.lock().expect("stager");
        if stager.date().is_some_and(|d| d != date)
            && let Some(batch) = stager.rotate()?
        {
            self.pending.lock().expect("pending").push(batch);
            self.state.lock().expect("state").epoch += 1;
        }
        for (dataset, rows) in rows {
            stager.append(dataset, &date, &rows)?;
        }
        Ok(())
    }

    /// Whether `account` changed since its last read, and whether its rows are due (changed or
    /// first since a flush). Also returns the data hash.
    fn observe(&self, pubkey: &str, data: &[u8]) -> (String, bool, bool) {
        let sha = sha256_hex(data);
        let mut state = self.state.lock().expect("state");
        let epoch = state.epoch;
        let seen = state.seen.entry(pubkey.to_owned()).or_default();
        let changed = seen.sha != sha;
        let due = changed || seen.rows_epoch != epoch;
        seen.sha.clone_from(&sha);
        if due {
            seen.rows_epoch = epoch;
        }
        (sha, changed, due)
    }

    /// The raw row for `account` when one is due: first since a flush, or changed since the last
    /// copy and at least the kind's spacing after it.
    fn raw_row(
        &self,
        at: At,
        kind: &str,
        account: &Account,
        symbol: Option<&str>,
        sha: &str,
    ) -> Option<Value> {
        let every_ms = i64::try_from(self.config.raw_every(kind)).unwrap_or(0) * 1000;
        let baseline_ms = i64::try_from(self.config.raw_baseline(kind)).unwrap_or(0) * 1000;
        let mut state = self.state.lock().expect("state");
        let epoch = state.epoch;
        let seen = state.seen.entry(account.pubkey.clone()).or_default();
        let baseline_due = seen.raw_epoch != epoch
            && (seen.raw_ms == 0 || at.captured_ms - seen.raw_ms >= baseline_ms);
        let reason = if baseline_due {
            "baseline"
        } else if seen.raw_sha != sha && at.captured_ms - seen.raw_ms >= every_ms {
            "changed"
        } else {
            return None;
        };
        seen.raw_sha = sha.to_owned();
        seen.raw_ms = at.captured_ms;
        seen.raw_epoch = epoch;
        Some(json!({
            "captured_ms": at.captured_ms, "slot": at.slot, "kind": kind,
            "pubkey": account.pubkey, "symbol": symbol, "reason": reason,
            "owner": account.owner, "lamports": account.lamports,
            "data_len": account.data.len(), "sha256": sha,
            "data_zstd": STANDARD.encode(&account.zstd),
        }))
    }

    fn owned<'a>(&self, account: Option<&'a Account>) -> Result<&'a Account, &'static str> {
        match account {
            None => Err("account missing"),
            Some(a) if a.owner != self.config.program_id => Err("account owner is not Phoenix"),
            Some(a) => Ok(a),
        }
    }

    /// One read of the trader index, the order books and the spline collections.
    pub async fn book_tick(&self) -> Result<(u64, Obj), String> {
        let exchange = self.exchange().ok_or("no exchange view yet")?;
        let captured_ms = now_ms();
        let mut raw = Vec::new();
        // The trader index and the active trader buffer first, in one call, so this tick's
        // orders resolve against the index.
        let keys: Vec<String> = exchange
            .index
            .iter()
            .chain(&exchange.active)
            .cloned()
            .collect();
        let answer = self.rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
        let at = At {
            captured_ms,
            slot: answer.slot,
        };
        let mut buffers = Vec::new();
        let mut active_changed = false;
        let mut active_due = false;
        for (i, account) in answer.accounts.iter().enumerate() {
            let account = self.owned(account.as_ref())?;
            let kind = if i < exchange.index.len() {
                "global_trader_index"
            } else {
                "active_trader_buffer"
            };
            let (sha, changed, due) = self.observe(&account.pubkey, &account.data);
            if kind == "active_trader_buffer" {
                active_changed |= changed;
                active_due |= due;
            }
            raw.extend(self.raw_row(at, kind, account, None, &sha));
            buffers.push(account.data.as_slice());
        }
        let (index_buffers, active_buffers) = buffers.split_at(exchange.index.len());
        let (header, arenas) = index_buffers
            .split_first()
            .ok_or("no trader index account")?;
        let index = TraderIndex::decode(header, arenas)?;
        let index_size = index.by_id.len();
        let index = Arc::new(index);
        let symbols = {
            let mut state = self.state.lock().expect("state");
            state.index = Some(Arc::clone(&index));
            state.index_slot = answer.slot;
            state.symbols.clone()
        };
        let mut active_rows = Vec::new();
        let mut active_count = None;
        if let Some((header, arenas)) = active_buffers.split_first() {
            let positions = decode::decode_active(header, arenas)?;
            active_count = Some(positions.len());
            if active_due {
                active_rows = decode::active_rows(&positions, at, &index, &symbols);
                let reason = if active_changed {
                    "changed"
                } else {
                    "baseline"
                };
                for r in &mut active_rows {
                    r["reason"] = json!(reason);
                }
            }
        }
        let per_call = (self.config.accounts_per_call / 2).max(1);
        let mut snapshots = Vec::new();
        let mut orders = Vec::new();
        let mut splines = Vec::new();
        let (mut changed, mut resolved, mut with_id, mut last_slot) = (0u64, 0u64, 0u64, 0u64);
        for chunk in exchange.markets.chunks(per_call) {
            if self.stopping() {
                break;
            }
            let keys: Vec<String> = chunk
                .iter()
                .flat_map(|m| [m.book.clone(), m.spline.clone()])
                .collect();
            let answer = self.rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
            let at = At {
                captured_ms,
                slot: answer.slot,
            };
            last_slot = answer.slot;
            for (market, pair) in chunk.iter().zip(answer.accounts.chunks(2)) {
                // The order book.
                let mut summary = match self.owned(pair[0].as_ref()) {
                    Err(reason) => json!({ "captured_ms": captured_ms, "slot": at.slot,
                        "symbol": market.symbol, "asset_id": market.asset_id,
                        "market": market.book, "decode_error": reason }),
                    Ok(account) => {
                        let (sha, is_changed, due) = self.observe(&account.pubkey, &account.data);
                        raw.extend(self.raw_row(
                            at,
                            "orderbook",
                            account,
                            Some(&market.symbol),
                            &sha,
                        ));
                        changed += u64::from(is_changed);
                        match decode::decode_book(&account.data, at, &market.book, Some(&index)) {
                            Ok(book) => {
                                if due {
                                    for order in &book.orders {
                                        if order.get("trader_id").is_some_and(|v| !v.is_null()) {
                                            with_id += 1;
                                            resolved += u64::from(
                                                order.get("trader").is_some_and(|v| !v.is_null()),
                                            );
                                        }
                                    }
                                    orders.extend(book.orders);
                                }
                                let mut s = book.summary;
                                s["sha256"] = json!(sha);
                                s["reason"] = json!(if is_changed {
                                    "changed"
                                } else if due {
                                    "baseline"
                                } else {
                                    "unchanged"
                                });
                                s
                            }
                            Err(error) => json!({ "captured_ms": captured_ms, "slot": at.slot,
                                "symbol": market.symbol, "asset_id": market.asset_id,
                                "market": market.book, "sha256": sha, "decode_error": error }),
                        }
                    }
                };
                if summary.get("reason").is_none() {
                    summary["reason"] = json!("error");
                }
                snapshots.push(summary);
                // The spline collection.
                if let Ok(account) = self.owned(pair[1].as_ref()) {
                    let (sha, _, due) = self.observe(&account.pubkey, &account.data);
                    raw.extend(self.raw_row(
                        at,
                        "spline_collection",
                        account,
                        Some(&market.symbol),
                        &sha,
                    ));
                    if due {
                        match decode::decode_splines(&account.data, at, &account.pubkey) {
                            Ok(rows) => splines.extend(rows.into_iter().map(|mut r| {
                                r["reason"] = json!("state");
                                r
                            })),
                            Err(error) => log(
                                "snapshot_decode_error",
                                Obj::new()
                                    .with("kind", "spline_collection")
                                    .with("symbol", market.symbol.as_str())
                                    .with("error", error),
                            ),
                        }
                    }
                }
            }
        }
        let detail = Obj::new()
            .with("markets", snapshots.len())
            .with("changedBooks", changed)
            .with("ordersWritten", orders.len())
            .with("splinesWritten", splines.len())
            .with("indexTraders", index_size)
            .with("activePositions", active_count)
            .with("activeRowsWritten", active_rows.len())
            .with("ordersWithTraderId", with_id)
            .with("ordersResolved", resolved)
            .with("rawWritten", raw.len());
        self.stage(
            captured_ms,
            vec![
                ("book_snapshots", snapshots),
                ("book_orders", orders),
                ("splines", splines),
                ("active_positions", active_rows),
                ("raw_accounts", raw),
            ],
        )
        .map_err(|e| e.to_string())?;
        Ok((last_slot, detail))
    }

    /// One read of the perp asset map.
    pub async fn market_tick(&self) -> Result<(u64, Obj), String> {
        let exchange = self.exchange().ok_or("no exchange view yet")?;
        let captured_ms = now_ms();
        let keys = vec![exchange.asset_map.clone()];
        let answer = self.rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
        let at = At {
            captured_ms,
            slot: answer.slot,
        };
        let account = self.owned(answer.accounts.first().and_then(Option::as_ref))?;
        let (sha, _, _) = self.observe(&account.pubkey, &account.data);
        let raw: Vec<Value> = self
            .raw_row(at, "perp_asset_map", account, None, &sha)
            .into_iter()
            .collect();
        let rows = decode::decode_markets(&account.data, at)?;
        {
            let mut state = self.state.lock().expect("state");
            for r in &rows {
                if let (Some(id), Some(symbol)) = (
                    r.get("asset_id").and_then(Value::as_u64),
                    r.get("symbol").and_then(Value::as_str),
                ) {
                    state.symbols.insert(id, symbol.to_owned());
                }
            }
        }
        let detail = Obj::new()
            .with("markets", rows.len())
            .with("rawWritten", raw.len());
        self.stage(
            captured_ms,
            vec![("market_state", rows), ("raw_accounts", raw)],
        )
        .map_err(|e| e.to_string())?;
        Ok((at.slot, detail))
    }

    /// One scan of every trader account, page by page, plus the trader index as of now.
    pub async fn trader_tick(&self) -> Result<(u64, Obj), String> {
        let captured_ms = now_ms();
        let prefix = decode::trader_prefix();
        let index = self.index();
        let ids = index.as_ref().map(|i| i.ids()).unwrap_or_default();
        let symbols = self.state.lock().expect("state").symbols.clone();
        let mut cursor: Option<String> = None;
        let (mut traders, mut positions, mut raw) = (Vec::new(), Vec::new(), Vec::new());
        let (mut pages, mut errors, mut mismatched, mut missing) = (0u64, 0u64, 0u64, 0u64);
        let (mut scanned, mut changed, mut hot) = (0u64, 0u64, 0u64);
        let (mut first_slot, mut last_slot) = (u64::MAX, 0u64);
        loop {
            if self.stopping() {
                return Err("stopped during a trader scan".into());
            }
            let page = self
                .rpc
                .program_page(
                    &self.config.program_id,
                    &prefix,
                    self.config.trader_page_limit,
                    cursor.as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
            pages += 1;
            first_slot = first_slot.min(page.slot);
            last_slot = last_slot.max(page.slot);
            let at = At {
                captured_ms,
                slot: page.slot,
            };
            for account in &page.accounts {
                let (sha, is_changed, due) = self.observe(&account.pubkey, &account.data);
                raw.extend(self.raw_row(at, "trader", account, None, &sha));
                match decode::decode_trader(&account.data, at, &account.pubkey, &symbols, &ids) {
                    Ok(mut rows) => {
                        scanned += 1;
                        changed += u64::from(is_changed);
                        rows.trader["sha256"] = json!(sha);
                        let flags = rows
                            .trader
                            .get("flags")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        hot += flags & 1;
                        match ids.get(account.pubkey.as_str()) {
                            None => missing += flags & 1,
                            Some(id) => {
                                let indexed = index.as_ref().and_then(|i| i.by_id.get(id));
                                if indexed.map(|e| e.collateral)
                                    != rows.trader.get("collateral").and_then(Value::as_i64)
                                {
                                    mismatched += 1;
                                }
                            }
                        }
                        if due {
                            traders.push(rows.trader);
                            positions.extend(rows.positions);
                        }
                    }
                    Err(error) => {
                        errors += 1;
                        log(
                            "snapshot_decode_error",
                            Obj::new()
                                .with("kind", "trader")
                                .with("pubkey", account.pubkey.as_str())
                                .with("error", error),
                        );
                    }
                }
            }
            match page.next {
                Some(next) if !page.accounts.is_empty() => cursor = Some(next),
                _ => break,
            }
        }
        let index_rows = match &index {
            Some(i) => {
                let slot = self.state.lock().expect("state").index_slot;
                i.rows(At { captured_ms, slot })
            }
            None => Vec::new(),
        };
        let detail = Obj::new()
            .with("pages", pages)
            .with("traders", scanned)
            .with("hotTraders", hot)
            .with("changedTraders", changed)
            .with("traderRowsWritten", traders.len())
            .with("positionRowsWritten", positions.len())
            .with("decodeErrors", errors)
            .with("indexTraders", index_rows.len())
            .with("hotTradersMissingFromIndex", missing)
            .with("collateralDiffersFromIndex", mismatched)
            .with(
                "firstPageSlot",
                if first_slot == u64::MAX {
                    0
                } else {
                    first_slot
                },
            )
            .with("rawWritten", raw.len());
        self.stage(
            captured_ms,
            vec![
                ("traders", traders),
                ("trader_positions", positions),
                ("trader_index", index_rows),
                ("raw_accounts", raw),
            ],
        )
        .map_err(|e| e.to_string())?;
        Ok((last_slot, detail))
    }

    /// Publish what is staged (and any batch a date change set aside).
    pub async fn flush(&self) -> Result<u64, StoreError> {
        {
            let mut stager = self.stager.lock().expect("stager");
            if let Some(batch) = stager.rotate()? {
                self.pending.lock().expect("pending").push(batch);
                self.state.lock().expect("state").epoch += 1;
            }
        }
        let batches: Vec<Batch> = std::mem::take(&mut *self.pending.lock().expect("pending"));
        let mut files = 0;
        for batch in batches {
            let root = self.root.clone();
            let published = self
                .db
                .run(move |store| ledger::publish(store, &root, &batch))
                .await?;
            let mut state = self.state.lock().expect("state");
            for p in &published {
                files += 1;
                state.published.0 += 1;
                state.published.1 += p.rows;
                state.published.2 += p.bytes;
            }
            log(
                "snapshot_flush",
                Obj::new()
                    .with("files", published.len())
                    .with("rows", published.iter().map(|p| p.rows).sum::<i64>())
                    .with("bytes", published.iter().map(|p| p.bytes).sum::<i64>()),
            );
        }
        Ok(files)
    }

    /// The status object (also written to `status.json`).
    pub async fn status(&self, running: bool) -> Result<Obj, StoreError> {
        let datasets = self.db.run(|store| ledger::summary(store)).await?;
        let meter = self.rpc.meter();
        let uptime = self.started.elapsed().as_secs_f64().max(1.0);
        let credits = meter.get("credits").and_then(Value::as_f64).unwrap_or(0.0);
        let bytes = meter.get("bytes").and_then(Value::as_f64).unwrap_or(0.0);
        let state = self.state.lock().expect("state");
        let staged = self.stager.lock().expect("stager").bytes();
        let mut lanes = Obj::new();
        for name in ["exchange", "books", "markets", "traders"] {
            lanes.set_obj(
                name,
                state.lanes.get(name).cloned().unwrap_or_default().to_obj(),
            );
        }
        let status = Obj::new()
            .with("at", now())
            .with("startedAt", self.started_at.as_str())
            .with("running", running)
            .with("uptimeSeconds", uptime.round())
            .with(
                "intervals",
                json!({ "books": self.config.book_interval_seconds, "markets": self.config.market_interval_seconds,
                        "traders": self.config.trader_interval_seconds, "flush": self.config.flush_seconds }),
            )
            .with("markets", state.exchange.as_ref().map_or(0, |e| e.markets.len()))
            .with_obj("lanes", lanes)
            .with_obj("rpc", meter)
            .with("creditsPerDayAtThisRate", (credits / uptime * 86_400.0).round())
            .with("rpcBytesPerDayAtThisRate", (bytes / uptime * 86_400.0).round())
            .with("stagedBytes", staged)
            .with(
                "publishedThisRun",
                json!({ "files": state.published.0, "rows": state.published.1, "bytes": state.published.2 }),
            )
            .with(
                "publishedBytesPerDayAtThisRate",
                ((state.published.2 as f64 + staged as f64) / uptime * 86_400.0).round(),
            )
            .with_rows("datasets", datasets);
        drop(state);
        write_atomic(
            &self.root.join("status.json"),
            format!("{}\n", status.to_json()).as_bytes(),
        )?;
        Ok(status)
    }

    async fn every(&self, seconds: u64, name: &'static str) {
        let period = Duration::from_secs(seconds);
        loop {
            if self.stopping() {
                return;
            }
            let started = Instant::now();
            let outcome = match name {
                "books" => self.book_tick().await,
                "markets" => self.market_tick().await,
                "traders" => self.trader_tick().await,
                _ => self.refresh_exchange().await.map(|()| (0, Obj::new())),
            };
            if name != "exchange" {
                self.record(name, outcome);
            } else if let Err(error) = outcome {
                self.record(name, Err(error));
            }
            self.sleep(period.saturating_sub(started.elapsed())).await;
        }
    }

    async fn sleep(&self, total: Duration) {
        let until = Instant::now() + total;
        while !self.stopping() && Instant::now() < until {
            tokio::time::sleep((until - Instant::now()).min(Duration::from_millis(500))).await;
        }
    }

    async fn housekeeping(&self) {
        let mut last_flush = Instant::now();
        loop {
            self.sleep(Duration::from_secs(30)).await;
            if self.stopping() {
                return;
            }
            let staged = self.stager.lock().expect("stager").bytes();
            let due = last_flush.elapsed() >= Duration::from_secs(self.config.flush_seconds)
                || staged >= self.config.flush_bytes
                || !self.pending.lock().expect("pending").is_empty();
            if due {
                last_flush = Instant::now();
                if let Err(error) = self.flush().await {
                    log(
                        "snapshot_error",
                        Obj::new()
                            .with("lane", "flush")
                            .with("error", safe_error(&error.to_string())),
                    );
                }
            }
            if let Err(error) = self.status(true).await {
                log(
                    "snapshot_error",
                    Obj::new()
                        .with("lane", "status")
                        .with("error", safe_error(&error.to_string())),
                );
            }
        }
    }

    /// Run every loop until the stop flag is set (or `duration` passes), then flush.
    pub async fn run(&self, duration: Option<Duration>) -> Result<Obj, StoreError> {
        while let Err(error) = self.refresh_exchange().await {
            self.record("exchange", Err(error));
            self.sleep(Duration::from_secs(15)).await;
            if self.stopping() {
                return self.status(false).await;
            }
        }
        // The markets lane names asset ids before the first trader scan.
        let first = self.market_tick().await;
        self.record("markets", first);
        if let Some(duration) = duration {
            let stop = Arc::clone(&self.stop);
            tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                stop.store(true, Ordering::Relaxed);
            });
        }
        let c = &self.config;
        tokio::join!(
            self.every(c.exchange_refresh_seconds, "exchange"),
            self.every(c.book_interval_seconds, "books"),
            self.every(c.market_interval_seconds, "markets"),
            self.every(c.trader_interval_seconds, "traders"),
            self.housekeeping(),
        );
        self.flush().await?;
        self.status(false).await
    }

    /// One read of everything, published at once.
    pub async fn once(&self) -> Result<Obj, StoreError> {
        if self.exchange().is_none() {
            self.refresh_exchange().await.map_err(StoreError::Check)?;
        }
        let markets = self.market_tick().await;
        self.record("markets", markets);
        let books = self.book_tick().await;
        self.record("books", books);
        let traders = self.trader_tick().await;
        self.record("traders", traders);
        self.flush().await?;
        self.status(false).await
    }
}

/// Open the ledger, publish leftovers, and build a lane.
pub fn start(
    config: SnapshotConfig,
    rpc: Arc<dyn StateRpc>,
    stop: Arc<AtomicBool>,
) -> Result<(Lane, crate::db::DbThread), StoreError> {
    let mut store = ledger::open(&config.data_dir)?;
    let mut recovered = 0;
    for batch in ledger::leftover_batches(&config.data_dir)? {
        recovered += ledger::publish(&mut store, &config.data_dir, &batch)?.len();
    }
    let removed = ledger::remove_unregistered(&store, &config.data_dir)?;
    ledger::write_catalog(&mut store, &config.data_dir)?;
    log(
        "snapshot_start",
        Obj::new()
            .with("recoveredFiles", recovered)
            .with("removedUnregistered", removed)
            .with("bookIntervalSeconds", config.book_interval_seconds)
            .with("traderIntervalSeconds", config.trader_interval_seconds)
            .with("requestsPerSecond", config.requests_per_second),
    );
    let (db, thread) = Db::spawn(store);
    Ok((Lane::new(config, rpc, db, stop)?, thread))
}

/// Stop the database thread and close the ledger.
pub fn finish(lane: Lane, thread: crate::db::DbThread) -> Result<(), StoreError> {
    let Lane { db, .. } = lane;
    thread.join(db).close()
}
