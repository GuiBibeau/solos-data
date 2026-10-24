//! The spot programs (ADR-0014) and what one transaction's invocations of them become: one
//! `swaps` row per pool-level trade and one `liquidity` row per deposit, withdrawal or pool
//! creation, read from each program's own events (Anchor `emit!` log lines, `emit_cpi!` self-CPIs,
//! Raydium AMM v4's `ray_log`), or, for programs that emit none, from the token transfers the
//! invocation makes ([`prop`]). Mints come from the instruction's accounts or from the token
//! balances of the pool's vaults, never from the trader's token accounts (a temporary wrapped-SOL
//! account created and closed in the same transaction has no balance entry).
//!
//! Rows are recorded at the pool: an aggregator route through three pools is three rows, and the
//! aggregator's own events are not read, so nothing is counted twice.

pub mod discs;
pub mod meteora;
pub mod orca;
pub mod prop;
pub mod pump;
pub mod raydium;

use super::frames::{Frame, Payload};
use super::tx::{Ix, Key, SpotTx, b58, key_of};
use std::sync::LazyLock;

/// A spot program.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Dex {
    /// Raydium AMM v4 (constant product, OpenBook-era).
    RaydiumAmm,
    /// Raydium concentrated liquidity.
    RaydiumClmm,
    /// Raydium constant product (CPMM).
    RaydiumCpmm,
    /// Raydium LaunchLab bonding curves.
    RaydiumLaunchlab,
    /// Orca Whirlpool.
    Whirlpool,
    /// Meteora DLMM.
    MeteoraDlmm,
    /// Meteora Dynamic AMM (DAMM v1).
    MeteoraDamm,
    /// Meteora DAMM v2 (CP-AMM).
    MeteoraDammV2,
    /// Meteora Dynamic Bonding Curve.
    MeteoraDbc,
    /// pump.fun's AMM (PumpSwap).
    PumpSwap,
    /// pump.fun bonding curves.
    Pump,
    /// HumidiFi (proprietary AMM, read from its transfers).
    HumidiFi,
    /// Tessera V (proprietary AMM).
    Tessera,
    /// ZeroFi (proprietary AMM).
    ZeroFi,
    /// AlphaQ (proprietary AMM).
    AlphaQ,
    /// SolFi (proprietary AMM).
    SolFi,
    /// SolFi V2 (proprietary AMM).
    SolFiV2,
    /// GoonFi (proprietary AMM).
    GoonFi,
    /// PancakeSwap's concentrated liquidity on Solana (read from its transfers).
    PancakeClmm,
}

/// Every program, in table order.
pub const ALL: [Dex; 19] = [
    Dex::RaydiumAmm,
    Dex::RaydiumClmm,
    Dex::RaydiumCpmm,
    Dex::RaydiumLaunchlab,
    Dex::Whirlpool,
    Dex::MeteoraDlmm,
    Dex::MeteoraDamm,
    Dex::MeteoraDammV2,
    Dex::MeteoraDbc,
    Dex::PumpSwap,
    Dex::Pump,
    Dex::HumidiFi,
    Dex::Tessera,
    Dex::ZeroFi,
    Dex::AlphaQ,
    Dex::SolFi,
    Dex::SolFiV2,
    Dex::GoonFi,
    Dex::PancakeClmm,
];

/// The wrapped SOL mint (the quote of SOL-quoted pump.fun curves, which hold lamports).
pub const WSOL: &str = "So11111111111111111111111111111111111111112";

static KEYS: LazyLock<Vec<Key>> = LazyLock::new(|| {
    ALL.iter()
        .map(|d| key_of(d.program_id()).expect("program ids are base58 keys"))
        .collect()
});

static WSOL_KEY: LazyLock<Key> = LazyLock::new(|| key_of(WSOL).expect("wSOL is a key"));

impl Dex {
    /// The name in the tables and the config.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Dex::RaydiumAmm => "raydium_amm",
            Dex::RaydiumClmm => "raydium_clmm",
            Dex::RaydiumCpmm => "raydium_cpmm",
            Dex::RaydiumLaunchlab => "raydium_launchlab",
            Dex::Whirlpool => "orca_whirlpool",
            Dex::MeteoraDlmm => "meteora_dlmm",
            Dex::MeteoraDamm => "meteora_damm",
            Dex::MeteoraDammV2 => "meteora_damm_v2",
            Dex::MeteoraDbc => "meteora_dbc",
            Dex::PumpSwap => "pumpswap",
            Dex::Pump => "pump",
            Dex::HumidiFi => "humidifi",
            Dex::Tessera => "tessera_v",
            Dex::ZeroFi => "zerofi",
            Dex::AlphaQ => "alphaq",
            Dex::SolFi => "solfi",
            Dex::SolFiV2 => "solfi_v2",
            Dex::GoonFi => "goonfi",
            Dex::PancakeClmm => "pancake_clmm",
        }
    }

    /// The program address.
    #[must_use]
    pub fn program_id(self) -> &'static str {
        match self {
            Dex::RaydiumAmm => "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            Dex::RaydiumClmm => "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
            Dex::RaydiumCpmm => "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
            Dex::RaydiumLaunchlab => "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
            Dex::Whirlpool => "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            Dex::MeteoraDlmm => "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
            Dex::MeteoraDamm => "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
            Dex::MeteoraDammV2 => "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
            Dex::MeteoraDbc => "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
            Dex::PumpSwap => "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
            Dex::Pump => "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
            Dex::HumidiFi => "9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp",
            Dex::Tessera => "TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH",
            Dex::ZeroFi => "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY",
            Dex::AlphaQ => "ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA",
            Dex::SolFi => "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe",
            Dex::SolFiV2 => "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF",
            Dex::GoonFi => "goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j",
            Dex::PancakeClmm => "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
        }
    }

    /// The program address as a key.
    #[must_use]
    pub fn key(self) -> Key {
        KEYS[self as usize]
    }

    /// The program named `name` in the tables.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Dex> {
        ALL.iter().copied().find(|d| d.name() == name)
    }

    /// Whether its trades are read from token transfers ([`prop`]) rather than events.
    #[must_use]
    pub fn by_transfers(self) -> bool {
        matches!(
            self,
            Dex::HumidiFi
                | Dex::Tessera
                | Dex::ZeroFi
                | Dex::AlphaQ
                | Dex::SolFi
                | Dex::SolFiV2
                | Dex::GoonFi
                | Dex::PancakeClmm
        )
    }

    /// For a program read from its transfers with one vault authority, the position of that
    /// authority among a swap's accounts: the last way to settle a trade that reads both ways.
    /// PancakeSwap's CLMM is Raydium's (`pool_state` at 2 in `swap` and `swap_v2`); the others
    /// were measured on every resolved trade of the seven trial ranges (ADR-0014). AlphaQ and
    /// ZeroFi keep self-owned vaults, which never read both ways.
    #[must_use]
    pub fn pool_account(self) -> Option<usize> {
        match self {
            Dex::Tessera => Some(0),
            Dex::HumidiFi | Dex::SolFi | Dex::SolFiV2 | Dex::GoonFi => Some(1),
            Dex::PancakeClmm => Some(2),
            _ => None,
        }
    }

    /// How many leading instruction bytes name the instruction in gap and coverage keys: the
    /// Anchor discriminator, AMM v4's tag, none for the proprietary programs (whose data is not
    /// a public layout).
    #[must_use]
    pub fn head_len(self) -> usize {
        match self {
            Dex::RaydiumAmm => 1,
            d if d.by_transfers() => 0,
            _ => 8,
        }
    }

    /// Whether its events come in log lines (`emit!`, `ray_log`), which a transaction's log
    /// limit can truncate, rather than as self-CPIs.
    #[must_use]
    pub fn logs_events(self) -> bool {
        matches!(
            self,
            Dex::RaydiumAmm
                | Dex::RaydiumClmm
                | Dex::RaydiumCpmm
                | Dex::Whirlpool
                | Dex::MeteoraDamm
        )
    }
}

/// The programs a run reads.
#[derive(Clone, Debug)]
pub struct Programs {
    enabled: Vec<Dex>,
}

impl Programs {
    /// The given programs.
    #[must_use]
    pub fn new(enabled: Vec<Dex>) -> Programs {
        Programs { enabled }
    }

    /// Every program.
    #[must_use]
    pub fn all() -> Programs {
        Programs::new(ALL.to_vec())
    }

    /// The program at `key`, when it is one the run reads.
    #[must_use]
    pub fn of(&self, key: &Key) -> Option<Dex> {
        self.enabled.iter().copied().find(|d| &d.key() == key)
    }

    /// The program whose base58 address is `id`, when the run reads it.
    #[must_use]
    pub fn of_id(&self, id: &str) -> Option<Dex> {
        self.enabled.iter().copied().find(|d| d.program_id() == id)
    }

    /// The programs, in table order.
    #[must_use]
    pub fn list(&self) -> &[Dex] {
        &self.enabled
    }
}

/// A little-endian cursor; every read is `None` past the end.
pub struct Rd<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Rd<'a> {
    /// A reader at the start of `data`.
    #[must_use]
    pub fn new(data: &'a [u8]) -> Rd<'a> {
        Rd { data, at: 0 }
    }

    /// The next `n` bytes.
    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n).filter(|e| *e <= self.data.len())?;
        let out = &self.data[self.at..end];
        self.at = end;
        Some(out)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N).and_then(|b| b.try_into().ok())
    }

    /// Skip `n` bytes.
    pub fn skip(&mut self, n: usize) -> Option<()> {
        self.take(n).map(|_| ())
    }

    /// Bytes left.
    #[must_use]
    pub fn rest(&self) -> usize {
        self.data.len() - self.at
    }

    /// A `u8`.
    pub fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|b| b[0])
    }

    /// A Borsh `bool`.
    pub fn bool(&mut self) -> Option<bool> {
        self.u8().map(|b| b != 0)
    }

    /// A `u16`.
    pub fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_le_bytes)
    }

    /// A `u32`.
    pub fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    /// An `i32`.
    pub fn i32(&mut self) -> Option<i32> {
        self.array().map(i32::from_le_bytes)
    }

    /// A `u64`.
    pub fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    /// A `u128`.
    pub fn u128(&mut self) -> Option<u128> {
        self.array().map(u128::from_le_bytes)
    }

    /// A public key.
    pub fn key(&mut self) -> Option<Key> {
        self.array()
    }

    /// A Borsh string (skipped; its length is returned).
    pub fn skip_string(&mut self) -> Option<usize> {
        let n = self.u32()? as usize;
        self.skip(n)?;
        Some(n)
    }
}

/// A Q64.64 square-root price as a float (the square root of the raw price of a in b).
#[must_use]
pub fn sqrt_q64(value: u128) -> f64 {
    value as f64 / 18_446_744_073_709_551_616.0
}

/// One trade as a program reports it, before the transaction's context is added.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Trade {
    /// The pool (the bonding curve for launchpads).
    pub pool: Key,
    /// The pool's first token (base, coin, token 0, x).
    pub mint_a: Key,
    /// The pool's second token (quote, pc, token 1, y).
    pub mint_b: Key,
    /// Whether the trader sold token a for token b.
    pub a_to_b: bool,
    /// What the trader paid in, in the input token's smallest unit.
    pub amount_in: u64,
    /// What the trader received.
    pub amount_out: u64,
    /// The fees the program reports for the trade.
    pub fee: Option<u64>,
    /// Whether `fee` is in the input token (else the output token).
    pub fee_on_input: Option<bool>,
    /// The pool's token a reserve before the trade, where the program reports it.
    pub reserve_a: Option<u64>,
    /// The pool's token b reserve before the trade.
    pub reserve_b: Option<u64>,
    /// The square root of the pool's price after the trade (raw units of b per a).
    pub sqrt_price: Option<f64>,
    /// The tick (CLMM) or bin (DLMM) after the trade.
    pub tick: Option<i32>,
    /// The active liquidity after the trade (Raydium CLMM).
    pub liquidity: Option<f64>,
}

/// What a liquidity row records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiqKind {
    /// Tokens deposited into the pool.
    Add,
    /// Tokens withdrawn from the pool.
    Remove,
    /// The pool was created (with its initial deposit when the event carries one).
    Create,
}

impl LiqKind {
    /// The name in the table.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            LiqKind::Add => "add",
            LiqKind::Remove => "remove",
            LiqKind::Create => "create",
        }
    }
}

/// One liquidity change as a program reports it.
#[derive(Clone, Debug, PartialEq)]
pub struct Liq {
    /// The pool.
    pub pool: Key,
    /// What happened.
    pub kind: LiqKind,
    /// Token a, when the event or the instruction names it.
    pub mint_a: Option<Key>,
    /// Token b.
    pub mint_b: Option<Key>,
    /// Token a amount.
    pub amount_a: Option<u64>,
    /// Token b amount.
    pub amount_b: Option<u64>,
}

/// Per-program counts of a stretch of transactions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Successful transactions naming the program.
    pub txs: u64,
    /// Failed transactions naming it (not decoded).
    pub failed: u64,
    /// Invocations of the program (top level and CPI, event self-CPIs excluded).
    pub frames: u64,
    /// Invocations of a swap instruction.
    pub swap_ixs: u64,
    /// Swap rows.
    pub swaps: u64,
    /// Liquidity rows.
    pub liquidity: u64,
    /// Swap instructions with no swap event (log truncated, or a layout not known).
    pub missing_event: u64,
    /// Known events that did not decode.
    pub undecoded: u64,
    /// Swap events whose pool tokens could not be resolved.
    pub unresolved: u64,
}

impl Counts {
    /// Add `o`.
    pub fn add(&mut self, o: &Counts) {
        self.txs += o.txs;
        self.failed += o.failed;
        self.frames += o.frames;
        self.swap_ixs += o.swap_ixs;
        self.swaps += o.swaps;
        self.liquidity += o.liquidity;
        self.missing_event += o.missing_event;
        self.undecoded += o.undecoded;
        self.unresolved += o.unresolved;
    }
}

/// What one invocation produced.
#[derive(Debug, Default)]
pub struct FrameOut {
    /// Trades, in event order.
    pub trades: Vec<Trade>,
    /// Liquidity changes, in event order.
    pub liquidity: Vec<Liq>,
    /// Its counts.
    pub counts: Counts,
}

impl FrameOut {
    /// Record a trade, or count it unresolved when its tokens are unknown.
    pub fn trade(&mut self, trade: Option<Trade>) {
        match trade {
            Some(t) => self.trades.push(t),
            None => self.counts.unresolved += 1,
        }
    }
}

/// One invocation in its transaction.
pub struct Ctx<'a> {
    /// The transaction.
    pub tx: &'a SpotTx,
    /// The instruction.
    pub ix: &'a Ix,
    /// Its position in [`SpotTx::ixs`] (its CPIs follow it).
    pub at: usize,
    /// The events it emitted.
    pub events: &'a [Payload],
}

impl Ctx<'_> {
    /// The account at position `i` of the instruction.
    #[must_use]
    pub fn account(&self, i: usize) -> Option<Key> {
        let index = *self.ix.accounts.get(i)?;
        self.tx.keys.get(usize::from(index)).copied()
    }

    /// The instruction's first eight data bytes (an Anchor discriminator).
    #[must_use]
    pub fn disc(&self) -> Option<[u8; 8]> {
        self.ix.data.get(..8).and_then(|d| d.try_into().ok())
    }

    /// Whether the instruction is one of `discs`.
    #[must_use]
    pub fn is(&self, discs: &[[u8; 8]]) -> bool {
        self.disc().is_some_and(|d| discs.contains(&d))
    }

    /// The mint of the token account `key`, from the token balances.
    #[must_use]
    pub fn mint_of(&self, key: &Key) -> Option<Key> {
        let index = self.tx.index_of(key)?;
        self.tx.balance(index).and_then(|b| key_of(&b.mint))
    }

    /// The mint of the token account at position `i` of the instruction.
    #[must_use]
    pub fn mint_at(&self, i: usize) -> Option<Key> {
        self.mint_of(&self.account(i)?)
    }

    /// The two mints of the token accounts the pool owns (its vaults), ordered by key bytes,
    /// for pools whose vaults' authority is the pool itself and whose tokens are ordered so
    /// (Raydium CLMM, Orca Whirlpool). `None` unless exactly two mints.
    #[must_use]
    pub fn owned_mints(&self, pool: &Key) -> Option<(Key, Key)> {
        let owner = b58(pool);
        let mut mints: Vec<Key> = self
            .tx
            .balances
            .iter()
            .filter(|b| b.owner.as_deref() == Some(owner.as_str()))
            .filter_map(|b| key_of(&b.mint))
            .collect();
        mints.sort_unstable();
        mints.dedup();
        match mints.as_slice() {
            [a, b] => Some((*a, *b)),
            _ => None,
        }
    }

    /// The pool's (a, b) tokens when at least one side is known and the pool owns token
    /// accounts of exactly two mints: the known side fixes which is which.
    #[must_use]
    pub fn pair_of(&self, pool: &Key, a: Option<Key>, b: Option<Key>) -> Option<(Key, Key)> {
        match (a, b) {
            (Some(a), Some(b)) => Some((a, b)),
            (Some(a), None) => {
                let (x, y) = self.owned_mints(pool)?;
                (a == x)
                    .then_some((x, y))
                    .or_else(|| (a == y).then_some((y, x)))
            }
            (None, Some(b)) => {
                let (x, y) = self.owned_mints(pool)?;
                (b == y)
                    .then_some((x, y))
                    .or_else(|| (b == x).then_some((y, x)))
            }
            (None, None) => None,
        }
    }

    /// The mint of the token account `pool` owns that held `amount` before the transaction:
    /// a vault named by the reserve an event reports.
    #[must_use]
    pub fn owned_mint_holding(&self, pool: &Key, amount: u64) -> Option<Key> {
        let owner = b58(pool);
        let mut found = self
            .tx
            .balances
            .iter()
            .filter(|b| b.owner.as_deref() == Some(owner.as_str()) && b.pre == Some(amount));
        let first = found.next()?;
        found
            .next()
            .is_none()
            .then(|| key_of(&first.mint))
            .flatten()
    }

    /// The Anchor events: (discriminator, body).
    pub fn anchor_events(&self) -> impl Iterator<Item = ([u8; 8], &[u8])> {
        self.events.iter().filter_map(|p| match p {
            Payload::Event(bytes) => {
                let disc: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
                Some((disc, &bytes[8..]))
            }
            Payload::RayLog(_) => None,
        })
    }

    /// The first `ray_log` whose log type is one of `types`.
    #[must_use]
    pub fn ray_log(&self, types: &[u8]) -> Option<&[u8]> {
        self.events.iter().find_map(|p| match p {
            Payload::RayLog(bytes) if bytes.first().is_some_and(|t| types.contains(t)) => {
                Some(bytes.as_slice())
            }
            _ => None,
        })
    }
}

/// Order two mints by key bytes, with the flag saying whether `first` became token a.
#[must_use]
pub fn ordered(first: Key, second: Key) -> (Key, Key, bool) {
    if first <= second {
        (first, second, true)
    } else {
        (second, first, false)
    }
}

/// The wrapped SOL mint as a key.
#[must_use]
pub fn wsol() -> Key {
    *WSOL_KEY
}

/// Decode one invocation.
#[must_use]
pub fn decode_frame(dex: Dex, c: &Ctx<'_>) -> FrameOut {
    let mut out = FrameOut::default();
    out.counts.frames = 1;
    match dex {
        Dex::RaydiumAmm => raydium::amm(c, &mut out),
        Dex::RaydiumClmm => raydium::clmm(c, &mut out),
        Dex::RaydiumCpmm => raydium::cpmm(c, &mut out),
        Dex::RaydiumLaunchlab => raydium::launchlab(c, &mut out),
        Dex::Whirlpool => orca::whirlpool(c, &mut out),
        Dex::MeteoraDlmm => meteora::dlmm(c, &mut out),
        Dex::MeteoraDamm => meteora::damm(c, &mut out),
        Dex::MeteoraDammV2 => meteora::damm_v2(c, &mut out),
        Dex::MeteoraDbc => meteora::dbc(c, &mut out),
        Dex::PumpSwap => pump::pumpswap(c, &mut out),
        Dex::Pump => pump::pump(c, &mut out),
        _ => prop::by_transfers(c, dex.pool_account(), &mut out),
    }
    out.counts.swaps = out.trades.len() as u64;
    out.counts.liquidity = out.liquidity.len() as u64;
    out
}

/// A decoded frame with where it sits in the transaction.
pub struct Decoded {
    /// The program.
    pub dex: Dex,
    /// Its position in the transaction's instructions.
    pub at: usize,
    /// The top-level instruction it belongs to.
    pub ix: u16,
    /// Its stack depth (1 at the top level).
    pub depth: u8,
    /// What it produced.
    pub out: FrameOut,
}

/// Decode every invocation of the programs in a transaction, in execution order.
#[must_use]
pub fn decode_tx(tx: &SpotTx, frames: &[Frame]) -> Vec<Decoded> {
    frames
        .iter()
        .map(|frame| {
            let ix = &tx.ixs[frame.at];
            let ctx = Ctx {
                tx,
                ix,
                at: frame.at,
                events: &frame.events,
            };
            Decoded {
                dex: frame.dex,
                at: frame.at,
                ix: ix.top,
                depth: ix.depth,
                out: decode_frame(frame.dex, &ctx),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_program_id_is_a_key_and_names_round_trip() {
        for dex in ALL {
            assert_eq!(b58(&dex.key()), dex.program_id());
            assert_eq!(Dex::from_name(dex.name()), Some(dex));
        }
        assert_eq!(b58(&wsol()), WSOL);
    }

    #[test]
    fn the_reader_stops_at_the_end() {
        let mut r = Rd::new(&[1, 0, 0, 0, 2]);
        assert_eq!(r.u32(), Some(1));
        assert_eq!(r.u64(), None);
        assert_eq!(r.u8(), Some(2));
        assert_eq!(r.rest(), 0);
        assert!((sqrt_q64(1u128 << 64) - 1.0).abs() < 1e-12);
    }
}
