//! Programs that emit no swap event (ADR-0014): the proprietary AMMs (HumidiFi, Tessera V,
//! ZeroFi, AlphaQ, SolFi, SolFi V2, GoonFi) and PancakeSwap's CLMM. A trade is read from the
//! token transfers the invocation makes itself (its direct SPL Token or Token-2022 `Transfer` and
//! `TransferChecked` CPIs): the trader's payment into a pool account and the pool's payment out
//! of a pool account of the other token. Pool accounts come in two shapes:
//!
//! - **owned**: both vaults belong to one authority, which signs the payment out (HumidiFi,
//!   Tessera V, SolFi, PancakeSwap). The row's pool is that authority.
//! - **self-owned**: each vault is its own authority (AlphaQ, ZeroFi). The row's pool is the lower
//!   of the two vault addresses; with the mints it names the market.
//!
//! Two transfers can read both ways round (each side pays into an account the other owns), and
//! the order of the two transfers is the program's choice (HumidiFi pays out first). So a reading
//! in self-owned vaults wins (a trader's account is never its own authority); a reading whose pool
//! signed the transaction is dropped (vaults belong to program addresses, which cannot sign); if
//! two readings remain, the one whose pool accounts no other transfer of the transaction touches
//! is preferred (a router's accounts carry the tokens in and out of the hop, though a split route
//! may pay one vault twice); and if a trader's program still holds its tokens in accounts of its
//! own address, the pool's position among the instruction's accounts settles it
//! ([`super::Dex::pool_account`]). Amounts are exact; fees,
//! reserves and price state are not reported. An invocation whose transfers read as no trade (a
//! market maker's quote update moves no tokens, a deposit only pays in) is not counted; one that
//! reads as a trade more than one way is counted unresolved. Liquidity is not read for these
//! programs.

use super::{Ctx, FrameOut, Key, Trade, ordered};
use crate::spot::tx::{Ix, SpotTx, key_of};
use std::sync::LazyLock;

/// SPL Token.
pub const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
/// Token-2022.
pub const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

static TOKEN_KEYS: LazyLock<[Key; 2]> = LazyLock::new(|| {
    [
        key_of(TOKEN).expect("token program id"),
        key_of(TOKEN_2022).expect("token-2022 program id"),
    ]
});

/// One token transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transfer {
    /// Its position in [`SpotTx::ixs`].
    pub at: usize,
    /// Source token account.
    pub source: Key,
    /// Destination token account.
    pub destination: Key,
    /// The signing authority.
    pub authority: Key,
    /// Raw amount.
    pub amount: u64,
    /// The mint, from `TransferChecked` or the token balances.
    pub mint: Option<Key>,
}

/// The mint of the token account `key`, from the token balances.
fn mint_of(tx: &SpotTx, key: &Key) -> Option<Key> {
    let index = tx.index_of(key)?;
    tx.balance(index).and_then(|b| key_of(&b.mint))
}

/// The owner (authority) of token account `key`, from the token balances.
fn owner_of(tx: &SpotTx, key: &Key) -> Option<Key> {
    let index = tx.index_of(key)?;
    tx.balance(index)
        .and_then(|b| b.owner.as_deref())
        .and_then(key_of)
}

/// The instruction at `at` as a token transfer, when it is one.
fn transfer_at(tx: &SpotTx, at: usize, ix: &Ix) -> Option<Transfer> {
    if !TOKEN_KEYS.contains(tx.keys.get(usize::from(ix.program))?) {
        return None;
    }
    let key = |i: usize| {
        ix.accounts
            .get(i)
            .and_then(|&a| tx.keys.get(usize::from(a)).copied())
    };
    let amount = ix
        .data
        .get(1..9)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)?;
    match (ix.data.first(), ix.data.len()) {
        (Some(3), 9) => {
            let (source, destination) = (key(0)?, key(1)?);
            Some(Transfer {
                at,
                source,
                destination,
                authority: key(2)?,
                amount,
                mint: mint_of(tx, &source).or_else(|| mint_of(tx, &destination)),
            })
        }
        (Some(12), 10) => Some(Transfer {
            at,
            source: key(0)?,
            mint: Some(key(1)?),
            destination: key(2)?,
            authority: key(3)?,
            amount,
        }),
        _ => None,
    }
}

/// The token transfers the invocation at `c` made directly (its own CPIs, not deeper ones).
#[must_use]
pub fn transfers(c: &Ctx<'_>) -> Vec<Transfer> {
    let depth = c.ix.depth;
    let mut out = Vec::new();
    for (at, ix) in c.tx.ixs.iter().enumerate().skip(c.at + 1) {
        if ix.top != c.ix.top || ix.depth <= depth {
            break;
        }
        if ix.depth == depth + 1 {
            out.extend(transfer_at(c.tx, at, ix));
        }
    }
    out
}

/// One way to read two transfers as a trade.
struct Reading {
    trade: Trade,
    /// The pool's accounts: the one paid into and the one paid out of.
    vaults: [Key; 2],
    self_owned: bool,
}

/// `paid` as the trader's payment into a pool account and `got` as the pool's payment out.
fn read(tx: &SpotTx, paid: &Transfer, got: &Transfer) -> Option<Reading> {
    let (mint_in, mint_out) = (paid.mint?, got.mint?);
    if mint_in == mint_out {
        return None;
    }
    let into = owner_of(tx, &paid.destination)?;
    let out_of = owner_of(tx, &got.source).unwrap_or(got.authority);
    let self_owned =
        into == paid.destination && got.authority == got.source && out_of == got.source;
    let owned = into == got.authority && out_of == got.authority;
    if !self_owned && !owned {
        return None;
    }
    let pool = if self_owned {
        paid.destination.min(got.source)
    } else {
        got.authority
    };
    let (mint_a, mint_b, a_to_b) = ordered(mint_in, mint_out);
    Some(Reading {
        trade: Trade {
            pool,
            mint_a,
            mint_b,
            a_to_b,
            amount_in: paid.amount,
            amount_out: got.amount,
            ..Trade::default()
        },
        vaults: [paid.destination, got.source],
        self_owned,
    })
}

/// Whether a token transfer of the transaction other than `own` moves tokens through `vaults`.
fn touched_elsewhere(tx: &SpotTx, own: &[Transfer], vaults: &[Key; 2]) -> bool {
    tx.ixs.iter().enumerate().any(|(at, ix)| {
        !own.iter().any(|t| t.at == at)
            && transfer_at(tx, at, ix)
                .is_some_and(|t| vaults.contains(&t.source) || vaults.contains(&t.destination))
    })
}

/// Keep the readings `keep` accepts, unless it accepts none.
fn prefer(readings: &mut Vec<Reading>, keep: impl Fn(&Reading) -> bool) {
    if readings.iter().any(&keep) {
        readings.retain(keep);
    }
}

/// A proprietary AMM or another program read from its transfers; `pool_at` is the pool's
/// position among the instruction's accounts, the last way to settle a trade read both ways.
pub fn by_transfers(c: &Ctx<'_>, pool_at: Option<usize>, o: &mut FrameOut) {
    let list = transfers(c);
    let mut readings = Vec::new();
    for (i, paid) in list.iter().enumerate() {
        for (j, got) in list.iter().enumerate() {
            if i != j {
                readings.extend(read(c.tx, paid, got));
            }
        }
    }
    if readings.is_empty() {
        return;
    }
    o.counts.swap_ixs += 1;
    prefer(&mut readings, |r| r.self_owned);
    readings.retain(|r| !c.tx.is_signer(&r.trade.pool));
    if readings.len() > 1 {
        prefer(&mut readings, |r| {
            !touched_elsewhere(c.tx, &list, &r.vaults)
        });
    }
    if readings.len() > 1
        && let Some(pool) = pool_at.and_then(|i| c.account(i))
    {
        prefer(&mut readings, |r| r.trade.pool == pool);
    }
    // One trade per invocation; several readings left are ambiguous.
    match readings.as_slice() {
        [r] => o.trades.push(r.trade.clone()),
        _ => o.counts.unresolved += 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spot::tx::{Balance, b58};

    fn transfer(from: u8, to: u8, authority: u8, amount: u64) -> Ix {
        let mut data = vec![3];
        data.extend(amount.to_le_bytes());
        Ix {
            program: 7,
            accounts: vec![from, to, authority],
            data,
            depth: 2,
            top: 0,
        }
    }

    fn balance(account: u8, mint: u8, owner: u8) -> Balance {
        Balance {
            account,
            mint: b58(&[mint; 32]),
            decimals: 6,
            owner: Some(b58(&[owner; 32])),
            pre: Some(1),
        }
    }

    /// Keys: 0 signer, 1 pool authority, 2 and 3 the trader's accounts of mints 20 and 21, 4 and
    /// 5 the vaults, 6 the AMM, 7 SPL Token, 8 a router's authority.
    fn tx(cpis: Vec<Ix>) -> SpotTx {
        let mut keys: Vec<Key> = (0..9u8).map(|k| [k; 32]).collect();
        keys[6] = super::super::Dex::HumidiFi.key();
        keys[7] = TOKEN_KEYS[0];
        let mut ixs = vec![Ix {
            program: 6,
            accounts: vec![1, 4, 5],
            data: vec![9],
            depth: 1,
            top: 0,
        }];
        ixs.extend(cpis);
        SpotTx {
            keys,
            ixs,
            signers: 1,
            balances: vec![
                balance(2, 20, 0),
                balance(3, 21, 0),
                balance(4, 20, 1),
                balance(5, 21, 1),
            ],
            ..SpotTx::default()
        }
    }

    fn run(t: &SpotTx) -> FrameOut {
        let c = Ctx {
            tx: t,
            ix: &t.ixs[0],
            at: 0,
            events: &[],
        };
        let mut o = FrameOut::default();
        by_transfers(&c, None, &mut o);
        o
    }

    fn one(o: &FrameOut) -> &Trade {
        let [t] = o.trades.as_slice() else {
            panic!("one trade: {o:?}");
        };
        t
    }

    #[test]
    fn a_trade_is_the_payment_in_and_the_pools_payment_out_either_order() {
        // Paid out first, as HumidiFi does; the reverse reading's pool is the signer.
        let o = run(&tx(vec![transfer(5, 3, 1, 70), transfer(2, 4, 0, 100)]));
        assert_eq!(o.counts.swap_ixs, 1);
        let t = one(&o);
        assert_eq!(t.pool, [1; 32]);
        assert_eq!((t.mint_a, t.mint_b, t.a_to_b), ([20; 32], [21; 32], true));
        assert_eq!((t.amount_in, t.amount_out), (100, 70));
        // A quote update moves no tokens: not a trade.
        let quiet = run(&tx(Vec::new()));
        assert_eq!((quiet.counts.swap_ixs, quiet.trades.len()), (0, 0));
        // A transfer of a deeper CPI is not the invocation's own.
        let mut deep = transfer(5, 3, 1, 70);
        deep.depth = 3;
        assert_eq!(
            run(&tx(vec![transfer(2, 4, 0, 100), deep])).counts.swap_ixs,
            0
        );
        // Two trades in one invocation are ambiguous.
        let o = run(&tx(vec![
            transfer(2, 4, 0, 100),
            transfer(5, 3, 1, 70),
            transfer(3, 5, 0, 5),
            transfer(4, 2, 1, 9),
        ]));
        assert_eq!((o.trades.len(), o.counts.unresolved), (0, 1));
    }

    #[test]
    fn through_a_router_the_pool_is_the_side_no_other_transfer_touches() {
        // The router's authority (8, not a signer) owns the hop's accounts 2 and 3, which its own
        // transfers fill from and empty to the signer's account 2 before and after the hop.
        let mut t = tx(vec![transfer(5, 3, 1, 70), transfer(2, 4, 8, 100)]);
        for b in &mut t.balances[..2] {
            b.owner = Some(b58(&[8; 32]));
        }
        // Top level: 0 the signer's payment to the router, 1 the hop, 2 the router's payout.
        for ix in &mut t.ixs {
            ix.top = 1;
        }
        t.ixs.insert(0, transfer(0, 2, 0, 100));
        t.ixs.push(transfer(3, 0, 8, 70));
        t.ixs[0].depth = 1;
        t.ixs[0].top = 0;
        let last = t.ixs.len() - 1;
        t.ixs[last].depth = 1;
        t.ixs[last].top = 2;
        let c = Ctx {
            tx: &t,
            ix: &t.ixs[1],
            at: 1,
            events: &[],
        };
        let mut o = FrameOut::default();
        by_transfers(&c, None, &mut o);
        let trade = one(&o);
        assert_eq!((trade.pool, trade.amount_in), ([1; 32], 100));
    }

    #[test]
    fn self_owned_vaults_name_the_pool_by_the_lower_vault() {
        // Vaults 4 and 5 are their own authorities; the trader (0) owns 2 and 3.
        let mut t = tx(vec![transfer(2, 4, 0, 100), transfer(5, 3, 5, 70)]);
        t.balances[2].owner = Some(b58(&[4; 32]));
        t.balances[3].owner = Some(b58(&[5; 32]));
        t.signers = 0;
        let trade = one(&run(&t)).clone();
        assert_eq!(
            (trade.pool, trade.amount_in, trade.a_to_b),
            ([4; 32], 100, true)
        );
    }
}
