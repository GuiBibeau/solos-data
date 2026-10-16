//! Account bytes to table rows. The layouts are the official Phoenix Rise SDK's
//! (`phoenix-rise-accounts`, pinned to the event codec's release): order books, spline
//! collections, the perp asset map and trader accounts. The global trader index, which maps the
//! `trader_id` of a resting order to a trader account, has no decoder in that SDK; its layout is
//! decoded here (a sokoban red-black tree of `Pubkey -> TraderState` nodes in multi-arena
//! accounts) and checked live against the trader accounts (ADR-0011).
//!
//! Every row is a JSON object staged as one NDJSON line; integers stay exact (u64 and i64 as
//! JSON numbers, i128 as decimal strings).

use phoenix_rise_accounts::PhoenixAccount;
use phoenix_rise_accounts::owned::{
    Orderbook, OrderbookEntry, PerpAssetMapOwned, SplineCollection, TickRegion, Trader,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

/// When and at which slot an account was read.
#[derive(Clone, Copy, Debug)]
pub struct At {
    /// Wall clock of the read, epoch milliseconds.
    pub captured_ms: i64,
    /// Context slot of the RPC answer.
    pub slot: u64,
}

impl At {
    fn base(self) -> serde_json::Map<String, Value> {
        let mut map = serde_json::Map::new();
        map.insert("captured_ms".into(), json!(self.captured_ms));
        map.insert("slot".into(), json!(self.slot));
        map
    }
}

fn row(at: At, fields: Value) -> Value {
    let mut map = at.base();
    if let Value::Object(extra) = fields {
        map.extend(extra);
    }
    Value::Object(map)
}

/// The 8-byte discriminant of an account kind.
#[must_use]
pub fn discriminant(kind: PhoenixAccount) -> [u8; 8] {
    kind.discriminant()
}

/// The kind of a Phoenix account from its first eight bytes, snake case.
#[must_use]
pub fn kind_of(data: &[u8]) -> &'static str {
    let Some(prefix) = data.get(..8) else {
        return "unknown";
    };
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(prefix);
    match PhoenixAccount::from_discriminant(bytes) {
        Some(PhoenixAccount::OrderbookHeader) => "orderbook",
        Some(PhoenixAccount::SplineCollectionHeader) => "spline_collection",
        Some(PhoenixAccount::PerpAssetMap) => "perp_asset_map",
        Some(PhoenixAccount::Trader) => "trader",
        Some(PhoenixAccount::GlobalTraderIndexHeader) => "global_trader_index",
        Some(PhoenixAccount::GlobalTraderIndexArenaHeader) => "global_trader_index",
        Some(_) => "other",
        None => "unknown",
    }
}

/// The trader-account prefix for a `memcmp` filter, base58.
#[must_use]
pub fn trader_prefix() -> String {
    bs58::encode(discriminant(PhoenixAccount::Trader)).into_string()
}

// ---------------------------------------------------------------------------------------------
// Global trader index

/// Account header (48 bytes) + sokoban superblock (32) + red-black tree header (16).
const INDEX_HEADER: usize = 48;
const SUPERBLOCK: usize = 32;
const TREE_HEADER: usize = 16;
/// Arena accounts after the first: their own 32-byte header, then nodes.
const ARENA_HEADER: usize = 32;
/// Four u32 registers (left, right, parent, color) + Pubkey key + 16-byte `TraderState`.
const INDEX_NODE: usize = 16 + 32 + 16;

/// One trader in the global trader index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// Node pointer: the `trader_id` resting orders carry.
    pub trader_id: u32,
    /// Trader account address.
    pub trader: String,
    /// Collateral in quote lots, as the index's `TraderState` holds it.
    pub collateral: i64,
    /// Capability flags.
    pub flags: u32,
}

/// The decoded global trader index.
#[derive(Clone, Debug, Default)]
pub struct TraderIndex {
    /// Live entries, by node pointer.
    pub by_id: BTreeMap<u32, IndexEntry>,
    /// Allocated nodes the superblock reports.
    pub size: u32,
    /// Arena accounts the superblock reports (header account included).
    pub num_arenas: u16,
    /// Nodes per arena.
    pub nodes_per_arena: u32,
}

fn u32_at(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn u16_at(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn i64_at(data: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

impl TraderIndex {
    /// Decode the header account and its arena accounts (in the exchange view's order).
    pub fn decode(header: &[u8], arenas: &[&[u8]]) -> Result<TraderIndex, String> {
        if kind_of(header) != "global_trader_index" {
            return Err("global trader index discriminant mismatch".into());
        }
        let sb = INDEX_HEADER;
        let bad = || "global trader index is truncated".to_owned();
        let size = u32_at(header, sb).ok_or_else(bad)?;
        let num_arenas = u16_at(header, sb + 4).ok_or_else(bad)?;
        let nodes_per_arena = u32_at(header, sb + 8).ok_or_else(bad)?;
        let root = u32_at(header, sb + SUPERBLOCK).ok_or_else(bad)?;
        if nodes_per_arena == 0 {
            return Err("global trader index has no nodes per arena".into());
        }
        if usize::from(num_arenas) > arenas.len() + 1 {
            return Err(format!(
                "global trader index spans {num_arenas} accounts, {} known",
                arenas.len() + 1
            ));
        }
        let mut buffers: Vec<&[u8]> = vec![
            header
                .get(INDEX_HEADER + SUPERBLOCK + TREE_HEADER..)
                .ok_or_else(bad)?,
        ];
        for arena in arenas {
            buffers.push(arena.get(ARENA_HEADER..).ok_or_else(bad)?);
        }
        let node = |pointer: u32| -> Option<&[u8]> {
            let page = pointer.checked_sub(1)?;
            let arena = buffers.get((page / nodes_per_arena) as usize)?;
            let start = (page % nodes_per_arena) as usize * INDEX_NODE;
            arena.get(start..start + INDEX_NODE)
        };
        let mut index = TraderIndex {
            by_id: BTreeMap::new(),
            size,
            num_arenas,
            nodes_per_arena,
        };
        // In-order walk with an explicit stack, bounded by the allocated size.
        let mut stack = Vec::new();
        let mut current = root;
        let limit = size as usize + 1;
        while current != 0 || !stack.is_empty() {
            while current != 0 {
                if stack.len() > limit {
                    return Err("global trader index tree is cyclic".into());
                }
                stack.push(current);
                current = u32_at(node(current).ok_or_else(bad)?, 0).ok_or_else(bad)?;
            }
            let Some(pointer) = stack.pop() else { break };
            let bytes = node(pointer).ok_or_else(bad)?;
            let entry = IndexEntry {
                trader_id: pointer,
                trader: bs58::encode(&bytes[16..48]).into_string(),
                collateral: i64_at(bytes, 48).ok_or_else(bad)?,
                flags: u32_at(bytes, 56).ok_or_else(bad)?,
            };
            if index.by_id.insert(pointer, entry).is_some() || index.by_id.len() > limit {
                return Err("global trader index tree revisits a node".into());
            }
            current = u32_at(bytes, 4).ok_or_else(bad)?;
        }
        if index.by_id.len() != size as usize {
            return Err(format!(
                "global trader index walk found {} of {size} traders",
                index.by_id.len()
            ));
        }
        Ok(index)
    }

    /// Trader address for a node pointer.
    #[must_use]
    pub fn trader(&self, trader_id: u32) -> Option<&str> {
        self.by_id.get(&trader_id).map(|e| e.trader.as_str())
    }

    /// Node pointer by trader address.
    #[must_use]
    pub fn ids(&self) -> HashMap<&str, u32> {
        self.by_id
            .values()
            .map(|e| (e.trader.as_str(), e.trader_id))
            .collect()
    }

    /// One row per trader.
    #[must_use]
    pub fn rows(&self, at: At) -> Vec<Value> {
        self.by_id
            .values()
            .map(|e| {
                row(
                    at,
                    json!({ "trader_id": e.trader_id, "trader": e.trader,
                            "collateral": e.collateral, "flags": e.flags }),
                )
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Order books

/// A decoded book: one summary row and one row per resting order, best first on each side.
pub struct Book {
    /// `book_snapshots` row without the change columns.
    pub summary: Value,
    /// `book_orders` rows.
    pub orders: Vec<Value>,
    /// Market symbol from the header.
    pub symbol: String,
}

fn order_row(
    at: At,
    symbol: &str,
    side: &str,
    rank: usize,
    entry: &OrderbookEntry,
    index: Option<&TraderIndex>,
) -> Value {
    let raw = entry.order_id.order_sequence_number;
    // Bids store the sequence number bit-inverted so the book sorts price-time on both sides.
    let sequence = if side == "bid" { !raw } else { raw };
    let order = &entry.order;
    let trader_id = order.trader_position_id.trader_id;
    row(
        at,
        json!({
            "symbol": symbol,
            "side": side,
            "rank": rank,
            "price_ticks": entry.order_id.price_in_ticks.as_inner(),
            "order_sequence_number": sequence,
            "raw_order_id": raw,
            "trader_id": trader_id,
            "trader": trader_id.and_then(|id| index.and_then(|i| i.trader(id))),
            "asset_id": order.trader_position_id.asset_id,
            "base_lots_remaining": order.num_base_lots_remaining,
            "initial_trade_size": order.initial_trade_size,
            "initial_slot": order.initial_slot,
            "last_valid_slot": order.last_valid_slot,
            "order_flags": order.order_flags,
            "reduce_only": order.reduce_only,
            "is_stop_loss": order.is_stop_loss,
            "is_conditional": order.is_conditional_order,
            "conditional_order_index": order.optional_conditional_order_index,
        }),
    )
}

/// Decode an order book account.
pub fn decode_book(
    data: &[u8],
    at: At,
    market: &str,
    index: Option<&TraderIndex>,
) -> Result<Book, String> {
    let book = Orderbook::try_from_account_bytes(data).map_err(|e| e.to_string())?;
    let header = &book.header;
    let symbol = header.asset_symbol.clone();
    let lots = |side: &[OrderbookEntry]| -> u64 {
        side.iter()
            .map(|e| e.order.num_base_lots_remaining)
            .fold(0u64, u64::saturating_add)
    };
    let mut orders = Vec::with_capacity(book.bids.len() + book.asks.len());
    for (rank, entry) in book.bids.iter().enumerate() {
        orders.push(order_row(at, &symbol, "bid", rank, entry, index));
    }
    for (rank, entry) in book.asks.iter().enumerate() {
        orders.push(order_row(at, &symbol, "ask", rank, entry, index));
    }
    let last_trade = header.trade_list.last();
    let summary = row(
        at,
        json!({
            "symbol": symbol,
            "asset_id": header.asset_id,
            "market": market,
            "market_status": header.market_status,
            "sequence_number": header.sequence_number.sequence_number,
            "sequence_slot": header.sequence_number.last_update_slot,
            "order_sequence_number": header.order_sequence_number.sequence_number,
            "trade_sequence_number": header.trade_sequence_number.sequence_number,
            "tick_size": header.tick_size_in_quote_lots_per_base_lot,
            "base_lots_decimals": header.base_lots_decimals,
            "taker_fee_micro": header.default_taker_fee_micro,
            "maker_fee_micro": header.default_maker_fee_micro,
            "total_maker_fees": header.total_maker_quote_lot_fees,
            "total_taker_fees": header.total_taker_quote_lot_fees,
            "bids": book.bids.len(),
            "asks": book.asks.len(),
            "best_bid_ticks": book.bids.first().map(|e| e.order_id.price_in_ticks.as_inner()),
            "best_ask_ticks": book.asks.first().map(|e| e.order_id.price_in_ticks.as_inner()),
            "bid_lots": lots(&book.bids),
            "ask_lots": lots(&book.asks),
            "last_trade_slot": last_trade.map(|t| t.slot),
            "last_trade_sequence_number": last_trade.map(|t| t.trade_sequence_number),
            "last_trade_price_ticks": last_trade.map(|t| t.maker_fill.price),
        }),
    );
    Ok(Book {
        summary,
        orders,
        symbol,
    })
}

// ---------------------------------------------------------------------------------------------
// Splines

fn regions(side: &str, regions: &[TickRegion], count: u64) -> Vec<Value> {
    regions
        .iter()
        .take(usize::try_from(count).unwrap_or(usize::MAX))
        .enumerate()
        .map(|(i, r)| {
            json!({
                "side": side, "region": i,
                "start_offset": r.start_offset.as_inner(), "end_offset": r.end_offset.as_inner(),
                "density": r.density.as_inner(), "top_level_hidden_take_size": r.top_level_hidden_take_size.as_inner(),
                "total_size": r.total_size.as_inner(), "filled_size": r.filled_size.as_inner(),
                "hidden_filled_size": r.hidden_filled_size.as_inner(), "lifespan": r.lifespan,
            })
        })
        .collect()
}

/// Decode a spline collection: one row per spline, with its regions as a JSON array.
pub fn decode_splines(data: &[u8], at: At, account: &str) -> Result<Vec<Value>, String> {
    let collection = SplineCollection::try_from_account_bytes(data).map_err(|e| e.to_string())?;
    let mut rows = Vec::with_capacity(collection.splines.len());
    for (i, spline) in collection.splines.iter().enumerate() {
        let mut all = regions("bid", &spline.bid_regions, spline.bid_num_regions);
        all.extend(regions("ask", &spline.ask_regions, spline.ask_num_regions));
        rows.push(row(
            at,
            json!({
                "symbol": collection.asset_symbol,
                "market": collection.market.to_string(),
                "spline_collection": account,
                "spline_index": i,
                "trader": spline.trader.to_string(),
                "is_active": spline.is_active,
                "mid_price_ticks": spline.mid_price.as_inner(),
                "bid_offset": spline.bid_offset,
                "ask_offset": spline.ask_offset,
                "bid_filled": spline.bid_filled_amount.as_inner(),
                "ask_filled": spline.ask_filled_amount.as_inner(),
                "bid_num_regions": spline.bid_num_regions,
                "ask_num_regions": spline.ask_num_regions,
                "sequence_number": spline.sequence_number.sequence_number,
                "sequence_slot": spline.sequence_number.last_update_slot,
                "user_update_slot": spline.user_update_slot,
                "user_price_sequence_number": spline.user_price_sequence_number,
                "user_parameter_sequence_number": spline.user_parameter_sequence_number,
                "flags": spline.flags,
                "leverage_decrease_bps": spline.leverage_decrease_in_bps.as_inner(),
                "max_position_size_long": spline.max_position_size_long.as_inner(),
                "max_position_size_short": spline.max_position_size_short.as_inner(),
                "regions": Value::Array(all).to_string(),
            }),
        ));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------------------------
// Market state (perp asset map)

/// Decode the perp asset map: one row per market.
pub fn decode_markets(data: &[u8], at: At) -> Result<Vec<Value>, String> {
    let map = PerpAssetMapOwned::try_from_account_bytes(data).map_err(|e| e.to_string())?;
    let mut rows = Vec::with_capacity(map.metadata.entries.len());
    for (symbol, m) in map.iter() {
        let mark = &m.oracle_price.mark_price;
        let book = &mark.book_price_component;
        let funding = &m.funding_accumulator;
        let params = &m.static_market_params;
        let last = |prices: &[phoenix_rise_accounts::owned::TicksAtSlot]| {
            prices
                .iter()
                .max_by_key(|p| p.slot)
                .map(|p| json!({ "slot": p.slot, "ticks": p.ticks.as_inner() }))
        };
        rows.push(row(
            at,
            json!({
                "symbol": symbol,
                "asset_id": params.asset_id,
                "market": params.market_account.to_string(),
                "tick_size": params.tick_size.as_inner(),
                "base_lots_decimals": params.base_lot_decimals,
                "map_sequence_number": map.sequence_number.sequence_number,
                "map_sequence_slot": map.sequence_number.last_update_slot,
                "price_sequence_number": m.oracle_price.price_sequence_number.sequence_number,
                "price_sequence_slot": m.oracle_price.price_sequence_number.last_update_slot,
                "mark_price_ticks": mark.price.ticks.as_inner(),
                "mark_price_slot": mark.price.slot,
                "finalized_mark_price_ticks": m.finalized_mark_price.as_inner(),
                "last_spot_price": last(&mark.spot_price_component.last_exchange_spot_price).map(|v| v.to_string()),
                "last_perp_price": last(&mark.perp_price_component.last_exchange_perp_price).map(|v| v.to_string()),
                "book_mid_ticks": book.clamped_book_mid_price.ticks.as_inner(),
                "book_mid_slot": book.clamped_book_mid_price.slot,
                "book_best_bid_ticks": book.last_best_bid.ticks.as_inner(),
                "book_best_ask_ticks": book.last_best_ask.ticks.as_inner(),
                "book_last_trade_ticks": book.last_trade_price.ticks.as_inner(),
                "book_last_trade_slot": book.last_trade_price.slot,
                "mid_spot_diff_ema_ticks": mark.spot_price_component.mid_spot_diff_ema_ticks.as_inner(),
                "cumulative_funding_rate": funding.cumulative_funding_rate.as_inner(),
                "funding_acc": funding.acc.as_inner().to_string(),
                "funding_last_diff": funding.last_diff.as_inner().to_string(),
                "funding_start_interval_ts": funding.start_interval_timestamp.as_inner(),
                "funding_last_update_ts": funding.last_funding_update_timestamp.as_inner(),
                "funding_interval_seconds": funding.funding_interval_seconds.as_inner(),
                "funding_period_seconds": funding.funding_period_seconds.as_inner(),
                "max_funding_rate": funding.max_funding_rate.as_inner(),
                "open_interest": m.open_interest_params.open_interest.as_inner(),
                "open_interest_cap": m.open_interest_params.open_interest_cap.as_inner(),
                "last_known_index_price_ticks": m.last_known_index_price.map(|p| p.as_inner()),
                "last_index_expiry_ts": m.last_index_expiry_timestamp,
                "asset_flags": m.asset_flags.bits,
                "risk_params": serde_json::to_string(&m.risk_params).unwrap_or_default(),
            }),
        ));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------------------------
// Traders

/// A decoded trader: one `traders` row and one `trader_positions` row per position.
pub struct TraderRows {
    /// `traders` row.
    pub trader: Value,
    /// `trader_positions` rows.
    pub positions: Vec<Value>,
}

/// Decode a trader account; `symbols` names asset ids, `index` supplies the trader id.
pub fn decode_trader(
    data: &[u8],
    at: At,
    pubkey: &str,
    symbols: &HashMap<u64, String>,
    ids: &HashMap<&str, u32>,
) -> Result<TraderRows, String> {
    let t = Trader::try_from_account_bytes(data).map_err(|e| e.to_string())?;
    let positions: Vec<Value> = t
        .positions
        .entries
        .iter()
        .map(|(asset_id, p)| {
            row(
                at,
                json!({
                    "trader": pubkey,
                    "asset_id": asset_id,
                    "symbol": symbols.get(asset_id),
                    "base_lot_position": p.base_lot_position.as_inner(),
                    "virtual_quote_lot_position": p.virtual_quote_lot_position.as_inner(),
                    "cumulative_funding_snapshot": p.cumulative_funding_snapshot.as_inner(),
                    "accumulated_funding": p.accumulated_funding_for_active_position.as_inner(),
                    "position_sequence_number": p.position_sequence_number,
                }),
            )
        })
        .collect();
    let trader = row(
        at,
        json!({
            "trader": pubkey,
            "trader_id": ids.get(pubkey),
            "key": t.key.to_string(),
            "authority": t.authority.to_string(),
            "position_authority": t.position_authority.to_string(),
            "trader_pda_index": t.trader_pda_index,
            "trader_subaccount_index": t.trader_subaccount_index,
            "collateral": t.state.quote_lot_collateral.as_inner(),
            "flags": t.state.flags,
            "global_position_sequence_number": t.state.global_position_sequence_number,
            "maker_fee_override": t.state.maker_fee_override_multiplier,
            "taker_fee_override": t.state.taker_fee_override_multiplier,
            "sequence_number": t.sequence_number.sequence_number,
            "sequence_slot": t.sequence_number.last_update_slot,
            "max_positions": t.max_positions,
            "positions": positions.len(),
            "native_sol_collateral": t.native_sol_collateral,
            "last_deposit_slot": t.last_deposit_slot,
            "withdraw_queue_node": t.withdraw_queue_node,
            "num_markets_with_splines": t.num_markets_with_splines,
            "data_len": data.len(),
        }),
    );
    Ok(TraderRows { trader, positions })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_bytes(nodes: &[(u32, u32, [u8; 32], i64)], root: u32) -> Vec<u8> {
        let mut data = vec![0u8; INDEX_HEADER + SUPERBLOCK + TREE_HEADER + 4 * INDEX_NODE];
        data[..8].copy_from_slice(&discriminant(PhoenixAccount::GlobalTraderIndexHeader));
        data[48..52].copy_from_slice(&(nodes.len() as u32).to_le_bytes());
        data[52..54].copy_from_slice(&1u16.to_le_bytes());
        data[56..60].copy_from_slice(&4u32.to_le_bytes());
        data[80..84].copy_from_slice(&root.to_le_bytes());
        for (i, (left, right, key, collateral)) in nodes.iter().enumerate() {
            let at = 96 + i * INDEX_NODE;
            data[at..at + 4].copy_from_slice(&left.to_le_bytes());
            data[at + 4..at + 8].copy_from_slice(&right.to_le_bytes());
            data[at + 16..at + 48].copy_from_slice(key);
            data[at + 48..at + 56].copy_from_slice(&collateral.to_le_bytes());
        }
        data
    }

    #[test]
    fn the_trader_index_walks_its_tree_and_rejects_cycles() {
        // Node 2 is the root with children 1 and 3.
        let nodes = [
            (0, 0, [1u8; 32], 10),
            (1, 3, [2u8; 32], 20),
            (0, 0, [3u8; 32], 30),
        ];
        let index = TraderIndex::decode(&index_bytes(&nodes, 2), &[]).unwrap();
        assert_eq!(index.by_id.len(), 3);
        assert_eq!(index.by_id[&3].collateral, 30);
        assert_eq!(
            index.trader(1),
            Some(bs58::encode([1u8; 32]).into_string().as_str())
        );
        let cyclic = [
            (0, 2, [1u8; 32], 10),
            (1, 0, [2u8; 32], 20),
            (0, 0, [3u8; 32], 30),
        ];
        assert!(TraderIndex::decode(&index_bytes(&cyclic, 1), &[]).is_err());
        let mut wrong = index_bytes(&nodes, 2);
        wrong[0] ^= 1;
        assert!(TraderIndex::decode(&wrong, &[]).is_err());
    }

    #[test]
    fn kinds_follow_the_discriminants() {
        assert_eq!(kind_of(&discriminant(PhoenixAccount::Trader)), "trader");
        assert_eq!(
            kind_of(&discriminant(PhoenixAccount::OrderbookHeader)),
            "orderbook"
        );
        assert_eq!(kind_of(&[0u8; 4]), "unknown");
        assert!(!trader_prefix().is_empty());
    }
}
