//! The spot tables' rows (ADR-0014). A row is keyed by its place in the chain: slot, the
//! transaction's index in the block and `seq`, the row's position among the transaction's spot
//! rows in execution order. No signature is stored (64 incompressible bytes a row); the archive
//! or any RPC gives it back from slot and index.

use super::dex::Programs;
use super::dex::{Counts, Decoded, Dex, LiqKind};
use super::frames::frames;
use super::tx::{Key, SpotTx};

/// One pool-level trade.
#[derive(Clone, Debug, PartialEq)]
pub struct Swap {
    /// Slot.
    pub slot: u64,
    /// Transaction index in the block.
    pub tx_index: u32,
    /// Position among the transaction's spot rows.
    pub seq: u16,
    /// The top-level instruction it ran under.
    pub ix: u16,
    /// Stack depth of the program invocation (1: called directly).
    pub depth: u8,
    /// Program.
    pub dex: Dex,
    /// Pool.
    pub pool: Key,
    /// Token a.
    pub mint_a: Key,
    /// Token b.
    pub mint_b: Key,
    /// Token a's decimals.
    pub decimals_a: Option<u8>,
    /// Token b's decimals.
    pub decimals_b: Option<u8>,
    /// Whether a was sold for b.
    pub a_to_b: bool,
    /// Paid in.
    pub amount_in: u64,
    /// Received.
    pub amount_out: u64,
    /// Fees the program reports.
    pub fee: Option<u64>,
    /// Whether the fee is in the input token.
    pub fee_on_input: Option<bool>,
    /// Token a reserve before the trade.
    pub reserve_a: Option<u64>,
    /// Token b reserve before the trade.
    pub reserve_b: Option<u64>,
    /// Square root of the price after the trade.
    pub sqrt_price: Option<f64>,
    /// Tick or bin after the trade.
    pub tick: Option<i32>,
    /// Active liquidity after the trade.
    pub liquidity: Option<f64>,
    /// The transaction's fee payer.
    pub signer: Key,
}

/// One liquidity change or pool creation.
#[derive(Clone, Debug, PartialEq)]
pub struct Liquidity {
    /// Slot.
    pub slot: u64,
    /// Transaction index in the block.
    pub tx_index: u32,
    /// Position among the transaction's spot rows.
    pub seq: u16,
    /// Top-level instruction.
    pub ix: u16,
    /// Stack depth.
    pub depth: u8,
    /// Program.
    pub dex: Dex,
    /// Pool.
    pub pool: Key,
    /// What happened.
    pub kind: LiqKind,
    /// Token a, when known from the event or instruction.
    pub mint_a: Option<Key>,
    /// Token b.
    pub mint_b: Option<Key>,
    /// Token a amount.
    pub amount_a: Option<u64>,
    /// Token b amount.
    pub amount_b: Option<u64>,
    /// The transaction's fee payer.
    pub signer: Key,
}

/// The rows and per-program counts of some transactions.
#[derive(Debug, Default)]
pub struct Rows {
    /// Trades.
    pub swaps: Vec<Swap>,
    /// Liquidity changes.
    pub liquidity: Vec<Liquidity>,
    /// Per program, in [`super::dex::ALL`] order.
    pub counts: Vec<Counts>,
    /// Transactions whose log was truncated.
    pub truncated: u64,
    /// Invocations that left a gap, by `program:instruction:gap` (the instruction's first data
    /// bytes in hex; gaps `missing_event`, `undecoded`, `unresolved`): where a layout changed.
    pub gaps: std::collections::BTreeMap<String, u64>,
}

impl Rows {
    /// Empty rows with a count slot per program.
    #[must_use]
    pub fn new() -> Rows {
        Rows {
            counts: vec![Counts::default(); super::dex::ALL.len()],
            ..Rows::default()
        }
    }

    /// Add `other`'s rows and counts.
    pub fn append(&mut self, mut other: Rows) {
        self.swaps.append(&mut other.swaps);
        self.liquidity.append(&mut other.liquidity);
        if self.counts.len() < other.counts.len() {
            self.counts.resize(other.counts.len(), Counts::default());
        }
        for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
            mine.add(theirs);
        }
        self.truncated += other.truncated;
        for (k, n) in other.gaps {
            *self.gaps.entry(k).or_default() += n;
        }
    }

    fn slots(&mut self) {
        if self.counts.len() < super::dex::ALL.len() {
            self.counts.resize(super::dex::ALL.len(), Counts::default());
        }
    }

    /// Count a failed transaction naming `dex`.
    pub fn failed(&mut self, dex: Dex) {
        self.slots();
        self.counts[dex as usize].failed += 1;
    }

    /// Decode a successful transaction into rows. `named` lists the programs whose addresses
    /// the transaction names (each counts one transaction, invoked or not).
    pub fn add_tx(&mut self, tx: &SpotTx, programs: &Programs, named: &[Dex]) {
        self.slots();
        for dex in named {
            self.counts[*dex as usize].txs += 1;
        }
        let framed = frames(tx, programs);
        if framed.truncated {
            self.truncated += 1;
        }
        let signer = tx.keys.first().copied().unwrap_or([0; 32]);
        let decoded = super::dex::decode_tx(tx, &framed.frames);
        let mut seq: u16 = 0;
        for Decoded {
            dex,
            at,
            ix,
            depth,
            out,
        } in decoded
        {
            self.counts[dex as usize].add(&out.counts);
            let c = &out.counts;
            for (gap, n) in [
                ("missing_event", c.missing_event),
                ("undecoded", c.undecoded),
                ("unresolved", c.unresolved),
            ] {
                if n > 0 {
                    let data = &tx.ixs[at].data;
                    let head = hex::encode(&data[..data.len().min(dex.head_len())]);
                    *self
                        .gaps
                        .entry(format!("{}:{head}:{gap}", dex.name()))
                        .or_default() += n;
                }
            }
            for t in out.trades {
                self.swaps.push(Swap {
                    slot: tx.slot,
                    tx_index: tx.index,
                    seq,
                    ix,
                    depth,
                    dex,
                    decimals_a: decimals(tx, &t.mint_a),
                    decimals_b: decimals(tx, &t.mint_b),
                    pool: t.pool,
                    mint_a: t.mint_a,
                    mint_b: t.mint_b,
                    a_to_b: t.a_to_b,
                    amount_in: t.amount_in,
                    amount_out: t.amount_out,
                    fee: t.fee,
                    fee_on_input: t.fee_on_input,
                    reserve_a: t.reserve_a,
                    reserve_b: t.reserve_b,
                    sqrt_price: t.sqrt_price,
                    tick: t.tick,
                    liquidity: t.liquidity,
                    signer,
                });
                seq = seq.saturating_add(1);
            }
            for l in out.liquidity {
                self.liquidity.push(Liquidity {
                    slot: tx.slot,
                    tx_index: tx.index,
                    seq,
                    ix,
                    depth,
                    dex,
                    pool: l.pool,
                    kind: l.kind,
                    mint_a: l.mint_a,
                    mint_b: l.mint_b,
                    amount_a: l.amount_a,
                    amount_b: l.amount_b,
                    signer,
                });
                seq = seq.saturating_add(1);
            }
        }
    }
}

/// A mint's decimals from the transaction's token balances (wrapped SOL is 9 even when no
/// token account of it is named, as on SOL-quoted pump.fun curves).
fn decimals(tx: &SpotTx, mint: &Key) -> Option<u8> {
    tx.decimals_of(mint)
        .or_else(|| (*mint == super::dex::wsol()).then_some(9))
}

/// The programs a transaction names among its account keys.
#[must_use]
pub fn named(keys: &[Key], programs: &Programs) -> Vec<Dex> {
    let mut out: Vec<Dex> = programs
        .list()
        .iter()
        .copied()
        .filter(|d| keys.contains(&d.key()))
        .collect();
    out.sort_unstable();
    out
}

/// A swap as JSON (keys in base58, amounts as decimal strings), for fixtures and tests.
#[must_use]
pub fn swap_json(s: &Swap) -> serde_json::Value {
    use super::tx::b58;
    serde_json::json!({
        "slot": s.slot, "tx_index": s.tx_index, "seq": s.seq, "ix": s.ix, "depth": s.depth,
        "program": s.dex.name(), "pool": b58(&s.pool), "mint_a": b58(&s.mint_a),
        "mint_b": b58(&s.mint_b), "decimals_a": s.decimals_a, "decimals_b": s.decimals_b,
        "a_to_b": s.a_to_b, "amount_in": s.amount_in.to_string(),
        "amount_out": s.amount_out.to_string(), "fee": s.fee.map(|f| f.to_string()),
        "fee_on_input": s.fee_on_input, "reserve_a": s.reserve_a.map(|r| r.to_string()),
        "reserve_b": s.reserve_b.map(|r| r.to_string()), "sqrt_price": s.sqrt_price,
        "tick": s.tick, "liquidity": s.liquidity, "signer": b58(&s.signer),
    })
}

/// A liquidity row as JSON.
#[must_use]
pub fn liquidity_json(l: &Liquidity) -> serde_json::Value {
    use super::tx::b58;
    serde_json::json!({
        "slot": l.slot, "tx_index": l.tx_index, "seq": l.seq, "ix": l.ix, "depth": l.depth,
        "program": l.dex.name(), "pool": b58(&l.pool), "kind": l.kind.name(),
        "mint_a": l.mint_a.as_ref().map(b58), "mint_b": l.mint_b.as_ref().map(b58),
        "amount_a": l.amount_a.map(|a| a.to_string()), "amount_b": l.amount_b.map(|a| a.to_string()),
        "signer": b58(&l.signer),
    })
}
