//! `snapshot validate`: one read-only pass that checks the decoders against independent facts.
//! The exchange view (Phoenix's own API) gives each market's symbol, asset id, tick size, lot
//! decimals, funding interval and period, open-interest cap, liquidation size and a recent
//! open-interest and cumulative-funding snapshot; the order book headers and the perp asset map
//! must agree with it. The trader index must hold exactly the trader accounts a program scan
//! finds, and resting orders' `trader_id`s must resolve through it. The active trader buffer,
//! read in the same call as each batch of order books, must carry every (trader, market) with
//! resting orders, with the same best prices and resting lots the books show. Open interest is
//! rebuilt from positions (the buffer for hot traders, trader accounts for the rest) and compared
//! with the perp asset map.

use super::decode::{self, ActivePosition, TraderIndex};
use super::rpc::StateRpc;
use super::service::Exchange;
use crate::jsonout::Obj;
use phoenix_rise_accounts::owned::{Orderbook, PerpAssetMapOwned, SplineCollection, Trader};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Default)]
struct Checks {
    passed: u64,
    failed: Vec<Value>,
}

impl Checks {
    fn check(&mut self, ok: bool, what: &str, detail: Value) {
        if ok {
            self.passed += 1;
        } else {
            self.failed.push(json!({ "check": what, "detail": detail }));
        }
    }
}

fn num(v: Option<&Value>) -> Option<i128> {
    match v? {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// One trader's resting orders in one market, as the order book shows them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Resting {
    /// Lowest ask in ticks (0 without asks).
    pub best_ask_ticks: u64,
    /// Highest bid in ticks (0 without bids).
    pub best_bid_ticks: u64,
    /// Non-reduce-only ask base lots.
    pub ask_lots: u64,
    /// Reduce-only ask base lots.
    pub ask_reduce_only_lots: u64,
    /// Non-reduce-only bid base lots.
    pub bid_lots: u64,
    /// Reduce-only bid base lots.
    pub bid_reduce_only_lots: u64,
}

impl Resting {
    /// The same summary as the active trader buffer holds it.
    #[must_use]
    pub fn of_active(p: &ActivePosition) -> Resting {
        Resting {
            best_ask_ticks: u64::from(p.best_ask_ticks),
            best_bid_ticks: u64::from(p.best_bid_ticks),
            ask_lots: u64::from(p.ask_lots),
            ask_reduce_only_lots: u64::from(p.ask_reduce_only_lots),
            bid_lots: u64::from(p.bid_lots),
            bid_reduce_only_lots: u64::from(p.bid_reduce_only_lots),
        }
    }
}

/// Add a book's resting orders, per (trader id, asset id), to `into`.
pub fn summarize_book(book: &Orderbook, into: &mut HashMap<(u32, u32), Resting>) {
    for (ask, side) in [(true, &book.asks), (false, &book.bids)] {
        for e in side {
            let Some(trader) = e.order.trader_position_id.trader_id else {
                continue;
            };
            let r = into
                .entry((trader, e.order.trader_position_id.asset_id))
                .or_default();
            let price = e.order_id.price_in_ticks.as_inner();
            let lots = e.order.num_base_lots_remaining;
            match (ask, e.order.reduce_only) {
                (true, false) => r.ask_lots += lots,
                (true, true) => r.ask_reduce_only_lots += lots,
                (false, false) => r.bid_lots += lots,
                (false, true) => r.bid_reduce_only_lots += lots,
            }
            if ask && (r.best_ask_ticks == 0 || price < r.best_ask_ticks) {
                r.best_ask_ticks = price;
            }
            if !ask && price > r.best_bid_ticks {
                r.best_bid_ticks = price;
            }
        }
    }
}

/// Compare the buffer with the books of `assets`: `(pairs compared, book pairs missing from the
/// buffer, mismatches)`.
#[must_use]
pub fn compare_active(
    positions: &[ActivePosition],
    assets: &HashSet<u32>,
    books: &HashMap<(u32, u32), Resting>,
) -> (u64, u64, Vec<Value>) {
    let mut compared = 0;
    let mut mismatches = Vec::new();
    let mut seen = HashSet::new();
    for p in positions.iter().filter(|p| assets.contains(&p.asset_id)) {
        compared += 1;
        seen.insert((p.trader_id, p.asset_id));
        let book = books
            .get(&(p.trader_id, p.asset_id))
            .copied()
            .unwrap_or_default();
        let buffer = Resting::of_active(p);
        if book != buffer {
            mismatches.push(json!({ "traderId": p.trader_id, "assetId": p.asset_id,
                "book": format!("{book:?}"), "buffer": format!("{buffer:?}") }));
        }
    }
    let missing = books.keys().filter(|k| !seen.contains(*k)).count() as u64;
    (compared, missing, mismatches)
}

/// Validate a live read against the exchange view.
pub async fn validate(
    rpc: &dyn StateRpc,
    exchange: &Exchange,
    program_id: &str,
    page_limit: u32,
) -> Result<Obj, String> {
    let mut c = Checks::default();
    let view: BTreeMap<&str, &Value> = exchange
        .view
        .get("markets")
        .and_then(Value::as_array)
        .map(|ms| {
            ms.iter()
                .filter_map(|m| Some((m.get("symbol")?.as_str()?, m)))
                .collect()
        })
        .unwrap_or_default();
    // Perp asset map against the view.
    let keys = vec![exchange.asset_map.clone()];
    let answer = rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
    let map_slot = answer.slot;
    let data = &answer.accounts[0].as_ref().ok_or("asset map missing")?.data;
    let map = PerpAssetMapOwned::try_from_account_bytes(data).map_err(|e| e.to_string())?;
    let mut oi_gaps = Vec::new();
    let mut funding_exact = 0u64;
    for (symbol, m) in map.iter() {
        let Some(v) = view.get(symbol.as_str()) else {
            c.check(false, "asset map market listed by the view", json!(symbol));
            continue;
        };
        let p = &m.static_market_params;
        let r = &m.risk_params;
        let f = &m.funding_accumulator;
        let pairs = [
            ("assetId", i128::from(p.asset_id)),
            ("tickSize", i128::from(p.tick_size.as_inner())),
            ("baseLotsDecimals", i128::from(p.base_lot_decimals)),
            (
                "fundingIntervalSeconds",
                i128::from(f.funding_interval_seconds.as_inner()),
            ),
            (
                "fundingPeriodSeconds",
                i128::from(f.funding_period_seconds.as_inner()),
            ),
            (
                "maxFundingRatePerInterval",
                i128::from(f.max_funding_rate.as_inner()),
            ),
            (
                "openInterestCapBaseLots",
                i128::from(m.open_interest_params.open_interest_cap.as_inner()),
            ),
            (
                "maxLiquidationSizeBaseLots",
                i128::from(r.max_liquidation_size.as_inner()),
            ),
        ];
        for (field, decoded) in pairs {
            let api = num(v.get(field));
            c.check(api == Some(decoded), &format!("asset map {field} = view"), json!({ "symbol": symbol, "decoded": decoded.to_string(), "view": api.map(|a| a.to_string()) }));
        }
        c.check(
            v.get("marketPubkey").and_then(Value::as_str)
                == Some(p.market_account.to_string().as_str()),
            "asset map market account = view",
            json!(symbol),
        );
        let stats = v.get("statsSnapshot");
        let api_oi = num(stats.and_then(|s| s.get("openInterestBaseLots")));
        let oi = i128::from(m.open_interest_params.open_interest.as_inner());
        if let Some(api_oi) = api_oi {
            oi_gaps.push(
                json!({ "symbol": symbol, "decoded": oi.to_string(), "view": api_oi.to_string(),
                "viewSlot": stats.and_then(|s| s.get("slot")), "mapSlot": map_slot }),
            );
        }
        let api_start = num(stats.and_then(|s| s.get("fundingStartIntervalTimestamp")));
        if api_start == Some(i128::from(f.start_interval_timestamp.as_inner())) {
            let api_cum = num(stats.and_then(|s| s.get("cumulativeFundingRate")));
            let same = api_cum == Some(i128::from(f.cumulative_funding_rate.as_inner()));
            funding_exact += u64::from(same);
            c.check(same, "cumulative funding = view (same interval)", json!({ "symbol": symbol,
                "decoded": f.cumulative_funding_rate.as_inner(), "view": api_cum.map(|a| a.to_string()) }));
        }
    }
    let oi_equal = oi_gaps.iter().filter(|g| g["decoded"] == g["view"]).count();
    // Trader index and trader accounts.
    let keys: Vec<String> = exchange
        .index
        .iter()
        .chain(&exchange.active)
        .cloned()
        .collect();
    let answer = rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
    let buffers: Vec<&[u8]> = answer
        .accounts
        .iter()
        .map(|a| {
            a.as_ref()
                .map(|a| a.data.as_slice())
                .ok_or("trader index account missing")
        })
        .collect::<Result<_, _>>()?;
    let index_slot = answer.slot;
    let (index_buffers, active_buffers) = buffers.split_at(exchange.index.len());
    let index = TraderIndex::decode(index_buffers[0], &index_buffers[1..])?;
    let ids = index.ids();
    let active = match active_buffers.split_first() {
        Some((header, arenas)) => decode::decode_active(header, arenas)?,
        None => Vec::new(),
    };
    // Open interest rebuilt from positions: hot traders from the buffer, the rest from accounts.
    let mut longs: HashMap<u64, i128> = HashMap::new();
    for p in &active {
        if p.base_lot_position > 0 {
            *longs.entry(u64::from(p.asset_id)).or_default() += i128::from(p.base_lot_position);
        }
    }
    let active_traders: HashSet<u32> = active.iter().map(|p| p.trader_id).collect();
    let mut hot_not_in_buffer = 0u64;
    let prefix = decode::trader_prefix();
    let mut cursor: Option<String> = None;
    let mut scanned: HashSet<String> = HashSet::new();
    let (mut collateral_same, mut collateral_diff, mut missing, mut key_same) =
        (0u64, 0u64, 0u64, 0u64);
    let (mut hot, mut flags_diff) = (0u64, 0u64);
    let mut diff_sample = Vec::new();
    let mut pages = 0;
    loop {
        let page = rpc
            .program_page(program_id, &prefix, page_limit, cursor.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        pages += 1;
        for account in &page.accounts {
            let Ok(t) = Trader::try_from_account_bytes(&account.data) else {
                c.check(false, "trader decodes", json!(account.pubkey));
                continue;
            };
            key_same += u64::from(t.key.to_string() == account.pubkey);
            scanned.insert(account.pubkey.clone());
            let is_hot = t.state.flags & 1 != 0;
            hot += u64::from(is_hot);
            let in_buffer = ids
                .get(account.pubkey.as_str())
                .is_some_and(|id| active_traders.contains(id));
            if is_hot && !in_buffer {
                hot_not_in_buffer += 1;
            }
            if !is_hot {
                for (asset, p) in &t.positions.entries {
                    let lots = p.base_lot_position.as_inner();
                    if lots > 0 {
                        *longs.entry(*asset).or_default() += i128::from(lots);
                    }
                }
            }
            match ids
                .get(account.pubkey.as_str())
                .and_then(|id| index.by_id.get(id))
            {
                None => missing += u64::from(is_hot),
                Some(e) => {
                    flags_diff += u64::from(e.flags != t.state.flags);
                    if e.collateral == t.state.quote_lot_collateral.as_inner() {
                        collateral_same += 1;
                    } else {
                        collateral_diff += 1;
                        if diff_sample.len() < 5 {
                            diff_sample.push(
                                json!({ "trader": account.pubkey, "index": e.collateral,
                                "account": t.state.quote_lot_collateral.as_inner(),
                                "accountSequenceSlot": t.sequence_number.last_update_slot,
                                "positions": t.positions.entries.len(), "pageSlot": page.slot,
                                "indexSlot": index_slot }),
                            );
                        }
                    }
                }
            }
        }
        match page.next {
            Some(next) if !page.accounts.is_empty() => cursor = Some(next),
            _ => break,
        }
    }
    let unscanned = index
        .by_id
        .values()
        .filter(|e| !scanned.contains(&e.trader))
        .count();
    c.check(
        missing == 0,
        "every hot trader account is in the index",
        json!(missing),
    );
    c.check(
        unscanned == 0,
        "every index entry is a trader account",
        json!(unscanned),
    );
    c.check(
        key_same as usize == scanned.len(),
        "trader.key = account address",
        json!({ "same": key_same, "scanned": scanned.len() }),
    );
    // Order books and splines against the view and the index.
    let (mut orders, mut resolved, mut splines, mut spline_known) = (0u64, 0u64, 0u64, 0u64);
    let (mut active_compared, mut active_missing) = (0u64, 0u64);
    let mut active_mismatches = Vec::new();
    let per_call = ((100 - exchange.active.len()) / 2).max(1);
    for chunk in exchange.markets.chunks(per_call) {
        let mut keys: Vec<String> = chunk
            .iter()
            .flat_map(|m| [m.book.clone(), m.spline.clone()])
            .collect();
        keys.extend(exchange.active.iter().cloned());
        let answer = rpc.multiple(&keys).await.map_err(|e| e.to_string())?;
        let (pairs, buffer) = answer.accounts.split_at(chunk.len() * 2);
        let buffer: Vec<&[u8]> = buffer
            .iter()
            .filter_map(|a| a.as_ref().map(|a| a.data.as_slice()))
            .collect();
        let mut resting = HashMap::new();
        let mut assets = HashSet::new();
        for (market, pair) in chunk.iter().zip(pairs.chunks(2)) {
            let Some(book) = pair[0].as_ref() else {
                c.check(false, "order book exists", json!(market.symbol));
                continue;
            };
            let Ok(b) = Orderbook::try_from_account_bytes(&book.data) else {
                c.check(false, "order book decodes", json!(market.symbol));
                continue;
            };
            let v = view.get(market.symbol.as_str());
            c.check(
                b.header.asset_symbol == market.symbol
                    && u64::from(b.header.asset_id) == market.asset_id,
                "book header symbol and asset id = view",
                json!(market.symbol),
            );
            c.check(
                num(v.and_then(|v| v.get("tickSize")))
                    == Some(i128::from(b.header.tick_size_in_quote_lots_per_base_lot))
                    && num(v.and_then(|v| v.get("baseLotsDecimals")))
                        == Some(i128::from(b.header.base_lots_decimals)),
                "book header tick size and lot decimals = view",
                json!(market.symbol),
            );
            let desc = b
                .bids
                .windows(2)
                .all(|w| w[0].order_id.price_in_ticks >= w[1].order_id.price_in_ticks);
            let asc = b
                .asks
                .windows(2)
                .all(|w| w[0].order_id.price_in_ticks <= w[1].order_id.price_in_ticks);
            c.check(
                desc && asc,
                "bids descend and asks ascend",
                json!(market.symbol),
            );
            summarize_book(&b, &mut resting);
            assets.insert(b.header.asset_id);
            for e in b.bids.iter().chain(&b.asks) {
                if let Some(id) = e.order.trader_position_id.trader_id {
                    orders += 1;
                    resolved += u64::from(index.by_id.contains_key(&id));
                }
                c.check(
                    u64::from(e.order.trader_position_id.asset_id) == market.asset_id,
                    "order asset id = market",
                    json!(market.symbol),
                );
            }
            if let Some(Ok(s)) = pair[1]
                .as_ref()
                .map(|a| SplineCollection::try_from_account_bytes(&a.data))
            {
                c.check(
                    s.asset_symbol == market.symbol && s.market.to_string() == market.book,
                    "spline collection names its market",
                    json!(market.symbol),
                );
                for spline in s.splines.iter().filter(|s| s.is_active) {
                    splines += 1;
                    spline_known += u64::from(ids.contains_key(spline.trader.to_string().as_str()));
                }
            }
        }
        if let Some((header, arenas)) = buffer.split_first() {
            let same_slot = decode::decode_active(header, arenas)?;
            let (compared, missing, mismatched) = compare_active(&same_slot, &assets, &resting);
            active_compared += compared;
            active_missing += missing;
            active_mismatches.extend(mismatched);
        }
    }
    if !exchange.active.is_empty() {
        c.check(
            active_missing == 0,
            "every (trader, market) with resting orders is in the active trader buffer",
            json!(active_missing),
        );
        c.check(
            active_mismatches.is_empty(),
            "active trader buffer best prices and resting lots = order books",
            json!({ "compared": active_compared, "mismatched": active_mismatches.len(),
                "sample": active_mismatches.iter().take(5).collect::<Vec<_>>() }),
        );
        c.check(
            hot_not_in_buffer == 0,
            "every hot trader is in the active trader buffer",
            json!(hot_not_in_buffer),
        );
    }
    let mut oi_rebuilt = Vec::new();
    for (symbol, m) in map.iter() {
        let asset = u64::from(m.static_market_params.asset_id);
        let rebuilt = longs.get(&asset).copied().unwrap_or(0);
        let decoded = i128::from(m.open_interest_params.open_interest.as_inner());
        oi_rebuilt.push(
            json!({ "symbol": symbol, "mapOpenInterest": decoded.to_string(),
            "fromPositions": rebuilt.to_string(), "equal": rebuilt == decoded }),
        );
    }
    let oi_rebuilt_equal = oi_rebuilt.iter().filter(|g| g["equal"] == true).count();
    c.check(
        resolved == orders,
        "every resting order's trader_id is an index node",
        json!({ "orders": orders, "resolved": resolved }),
    );
    Ok(Obj::new()
        .with("passed", c.passed)
        .with("failed", c.failed.len())
        .with(
            "failures",
            Value::Array(c.failed.into_iter().take(50).collect()),
        )
        .with("markets", map.metadata.entries.len())
        .with("viewMarkets", view.len())
        .with("openInterestEqualToView", oi_equal)
        .with("openInterestCompared", oi_gaps.len())
        .with(
            "openInterestSample",
            Value::Array(
                oi_gaps
                    .into_iter()
                    .filter(|g| g["decoded"] != g["view"])
                    .take(10)
                    .collect(),
            ),
        )
        .with("cumulativeFundingExact", funding_exact)
        .with("indexTraders", index.by_id.len())
        .with("indexArenas", index.num_arenas)
        .with("traderAccounts", scanned.len())
        .with("hotTraderAccounts", hot)
        .with("flagsDifferFromIndex", flags_diff)
        .with("collateralDiffSample", Value::Array(diff_sample))
        .with("traderPages", pages)
        .with("collateralEqualToIndex", collateral_same)
        .with("collateralDiffersFromIndex", collateral_diff)
        .with("restingOrdersWithTraderId", orders)
        .with("restingOrdersResolved", resolved)
        .with("activeTraderBufferPositions", active.len())
        .with("activeBufferPairsComparedToBooks", active_compared)
        .with("hotTradersNotInActiveBuffer", hot_not_in_buffer)
        .with("openInterestFromPositionsEqual", oi_rebuilt_equal)
        .with("openInterestFromPositionsCompared", oi_rebuilt.len())
        .with(
            "openInterestFromPositionsSample",
            Value::Array(
                oi_rebuilt
                    .into_iter()
                    .filter(|g| g["equal"] != true)
                    .take(10)
                    .collect(),
            ),
        )
        .with("activeSplines", splines)
        .with("activeSplineTradersInIndex", spline_known))
}
