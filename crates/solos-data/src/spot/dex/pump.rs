//! pump.fun: its AMM (PumpSwap: `BuyEvent`, `SellEvent`, `DepositEvent`, `WithdrawEvent`,
//! `CreatePoolEvent`) and its bonding curves (`TradeEvent`, `CreateEvent`), all `emit_cpi!`.

use super::discs::*;
use super::{Ctx, FrameOut, Key, Liq, LiqKind, Rd, Trade, wsol};
use crate::spot::tx::{b58, key_of};

/// PumpSwap trade instructions; every one has the pool at account 0, base mint at 3 and quote
/// mint at 4 (`boost_buy_and_burn`, the protocol's buyback, too). `multi_hop_swap` names its
/// pools in remaining accounts and is resolved through the event's pool and the base and quote
/// token accounts it names.
const SWAPS: [[u8; 8]; 7] = [
    IX_BUY,
    IX_SELL,
    IX_BUY_EXACT_QUOTE_IN,
    IX_BUY_V2,
    IX_SELL_V2,
    IX_BUY_EXACT_QUOTE_IN_V2,
    IX_BOOST_BUY_AND_BURN,
];

/// PumpSwap. Buy: the trader pays the quote amount with the LP, protocol and creator fees for
/// `base_amount_out`; sell: `base_amount_in` for `user_quote_amount_out` (after the fees). Token a
/// is the base. The reserves are the pool's before the trade.
pub fn pumpswap(c: &Ctx<'_>, o: &mut FrameOut) {
    let layout = c.is(&SWAPS);
    let swap_ix = layout || c.is(&[IX_MULTI_HOP_SWAP]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_BUY_EVENT | EV_SELL_EVENT => {
                swaps += 1;
                let buy = disc == EV_BUY_EVENT;
                // ts, base amount, limit, user base and quote reserves, pool base and quote
                // reserves, quote amount, lp fee bps, lp fee, protocol fee bps, protocol fee,
                // quote with/without lp fee, user quote amount, pool, user, user base account,
                // user quote account, protocol recipient and its account, coin creator,
                // creator fee bps, creator fee.
                let parsed = (|| {
                    r.skip(8)?;
                    let base = r.u64()?;
                    r.skip(24)?;
                    let (pool_base, pool_quote, quote_amount) = (r.u64()?, r.u64()?, r.u64()?);
                    r.skip(8)?;
                    let lp_fee = r.u64()?;
                    r.skip(8)?;
                    let protocol_fee = r.u64()?;
                    r.skip(8)?;
                    let user_quote = r.u64()?;
                    let pool = r.key()?;
                    r.skip(32)?;
                    let (user_base_account, user_quote_account) = (r.key()?, r.key()?);
                    r.skip(96 + 8)?;
                    let creator_fee = r.u64()?;
                    // A buy pays the larger of the two quote amounts: `buy` reports the fees on
                    // top of `quote_amount_in` in `user_quote_amount_in`, `buy_exact_quote_in`
                    // reports the amount paid in `quote_amount_in` and the net in the other.
                    let quote = if buy {
                        quote_amount.max(user_quote)
                    } else {
                        user_quote
                    };
                    Some((
                        pool,
                        base,
                        quote,
                        pool_base,
                        pool_quote,
                        lp_fee + protocol_fee + creator_fee,
                        user_base_account,
                        user_quote_account,
                    ))
                })();
                let Some((pool, base, quote, pool_base, pool_quote, fee, ub, uq)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let mints = if layout {
                    (|| {
                        (c.account(0)? == pool).then_some(())?;
                        Some((c.account(3)?, c.account(4)?))
                    })()
                } else {
                    // A multi-hop route's trader accounts are intermediates (often none):
                    // the base vault is the pool's account that held the base reserve.
                    let base = c
                        .mint_of(&ub)
                        .or_else(|| c.owned_mint_holding(&pool, pool_base));
                    c.pair_of(&pool, base, c.mint_of(&uq))
                };
                let (amount_in, amount_out) = if buy { (quote, base) } else { (base, quote) };
                o.trade(mints.map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b: !buy,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(buy),
                    reserve_a: Some(pool_base),
                    reserve_b: Some(pool_quote),
                    ..Trade::default()
                }));
            }
            EV_DEPOSIT_EVENT | EV_WITHDRAW_EVENT => {
                // ts, lp amount, two limits, user base and quote reserves, pool base and quote
                // reserves (eight u64), base amount, quote amount, lp supply, pool.
                let parsed = (|| {
                    r.skip(8 * 8)?;
                    let (a, b) = (r.u64()?, r.u64()?);
                    r.skip(8)?;
                    Some((r.key()?, a, b))
                })();
                match parsed {
                    Some((pool, a, b)) => o.liquidity.push(Liq {
                        pool,
                        kind: if disc == EV_DEPOSIT_EVENT {
                            LiqKind::Add
                        } else {
                            LiqKind::Remove
                        },
                        mint_a: c.account(3),
                        mint_b: c.account(4),
                        amount_a: Some(a),
                        amount_b: Some(b),
                    }),
                    None => o.counts.undecoded += 1,
                }
            }
            EV_CREATE_POOL_EVENT => {
                // ts, index u16, creator, base mint, quote mint, base and quote decimals, base
                // in, quote in, pool base, pool quote, minimum liquidity, initial liquidity,
                // lp out, bump u8, pool.
                let parsed = (|| {
                    r.skip(8 + 2 + 32)?;
                    let (a, b) = (r.key()?, r.key()?);
                    r.skip(2)?;
                    let (base_in, quote_in) = (r.u64()?, r.u64()?);
                    r.skip(5 * 8 + 1)?;
                    Some((r.key()?, a, b, base_in, quote_in))
                })();
                match parsed {
                    Some((pool, a, b, base_in, quote_in)) => o.liquidity.push(Liq {
                        pool,
                        kind: LiqKind::Create,
                        mint_a: Some(a),
                        mint_b: Some(b),
                        amount_a: Some(base_in),
                        amount_b: Some(quote_in),
                    }),
                    None => o.counts.undecoded += 1,
                }
            }
            _ => {}
        }
    }
    if swap_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}

/// Where a bonding-curve trade instruction keeps the mint and the curve.
fn curve_layout(c: &Ctx<'_>) -> Option<(usize, usize)> {
    if c.is(&[IX_BUY, IX_SELL, IX_BUY_EXACT_SOL_IN]) {
        Some((2, 3))
    } else if c.is(&[IX_BUY_V2, IX_SELL_V2, IX_BUY_EXACT_QUOTE_IN_V2]) {
        Some((1, 10))
    } else if c.is(&[IX_BUY_V3, IX_SELL_V3, IX_BUY_EXACT_QUOTE_IN_V3]) {
        Some((1, 5))
    } else {
        None
    }
}

/// The bonding curve of `mint` among the instruction's accounts, for instructions that name
/// their curves in remaining accounts (`multi_hop_curve_swap`): the one account, other than the
/// trader, that owns a token account of `mint` (the curve's associated token account).
fn curve_of(c: &Ctx<'_>, mint: &Key, user: &Key) -> Option<Key> {
    let mint = b58(mint);
    let owners: Vec<Key> =
        c.tx.balances
            .iter()
            .filter(|b| b.mint == mint)
            .filter_map(|b| b.owner.as_deref().and_then(key_of))
            .collect();
    let signer = c.tx.keys.first();
    let mut found: Vec<Key> =
        c.ix.accounts
            .iter()
            .filter_map(|&i| c.tx.keys.get(usize::from(i)).copied())
            .filter(|k| k != user && Some(k) != signer && owners.contains(k))
            .collect();
    found.sort_unstable();
    found.dedup();
    match found.as_slice() {
        [curve] => Some(*curve),
        _ => None,
    }
}

/// The quote side of a `TradeEvent`'s tail (2026 releases): mint and amount when the curve is
/// quoted in something other than SOL.
fn quote_tail(r: &mut Rd<'_>) -> Option<(Key, u64, u64)> {
    // track_volume, total unclaimed, total claimed, current volume, last update, ix_name,
    // mayhem_mode, cashback bps, cashback, buyback bps, buyback fee, shareholders.
    r.skip(1 + 8 * 4)?;
    r.skip_string()?;
    r.skip(1 + 8 * 4)?;
    let holders = r.u32()? as usize;
    r.skip(holders.checked_mul(34)?)?;
    let (mint, amount, virtual_reserves) = (r.key()?, r.u64()?, r.u64()?);
    Some((mint, amount, virtual_reserves))
}

/// pump.fun bonding curves. `TradeEvent`: SOL (or, since 2026, quote) amount, token amount,
/// side, virtual reserves after the trade, fees. The pool is the bonding curve account of the
/// trade instruction. Amounts are the curve's, before fees: a buy pays them on top, a sell
/// receives the quote less them (protocol and creator fees in `fee`; the newer holder-reward and
/// buyback fields are not added). The reserves are the curve's virtual reserves before the
/// trade (the event's, after, less the trade), so they and the amounts follow the curve exactly.
pub fn pump(c: &Ctx<'_>, o: &mut FrameOut) {
    let layout = curve_layout(c);
    let swap_ix = layout.is_some() || c.is(&[IX_MULTI_HOP_CURVE_SWAP]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_TRADE_EVENT => {
                swaps += 1;
                // mint, sol, token, is_buy, user, ts, virtual sol, virtual token, real sol,
                // real token, fee recipient, fee bps, fee, creator, creator fee bps, creator fee.
                let parsed = (|| {
                    let mint = r.key()?;
                    let (sol, token, buy, user) = (r.u64()?, r.u64()?, r.bool()?, r.key()?);
                    r.skip(8)?;
                    let (v_sol, v_token) = (r.u64()?, r.u64()?);
                    r.skip(16 + 32 + 8)?;
                    let fee = r.u64()?;
                    r.skip(32 + 8)?;
                    let fee = fee + r.u64()?;
                    let quote =
                        quote_tail(&mut r).filter(|(m, _, _)| *m != [0; 32] && *m != wsol());
                    Some((mint, sol, token, buy, user, v_sol, v_token, fee, quote))
                })();
                let Some((mint, sol, token, buy, user, v_sol, v_token, fee, quote)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let (quote_mint, quote_amount, v_quote) = quote.unwrap_or((wsol(), sol, v_sol));
                let (amount_in, amount_out) = if buy {
                    (quote_amount, token)
                } else {
                    (token, quote_amount)
                };
                // Virtual reserves before: a buy added the quote and removed the tokens.
                let (reserve_a, reserve_b) = if buy {
                    (
                        v_token.checked_add(token),
                        v_quote.checked_sub(quote_amount),
                    )
                } else {
                    (
                        v_token.checked_sub(token),
                        v_quote.checked_add(quote_amount),
                    )
                };
                let pool = match layout {
                    Some((at_mint, at_curve)) => c
                        .account(at_mint)
                        .filter(|m| *m == mint)
                        .and_then(|_| c.account(at_curve)),
                    None => curve_of(c, &mint, &user),
                };
                let trade = pool.map(|pool| Trade {
                    pool,
                    mint_a: mint,
                    mint_b: quote_mint,
                    a_to_b: !buy,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(buy),
                    reserve_a,
                    reserve_b,
                    ..Trade::default()
                });
                o.trade(trade);
            }
            EV_CREATE_EVENT => {
                // name, symbol, uri, mint, bonding_curve, ...
                let parsed = (|| {
                    r.skip_string()?;
                    r.skip_string()?;
                    r.skip_string()?;
                    Some((r.key()?, r.key()?))
                })();
                match parsed {
                    Some((mint, curve)) => o.liquidity.push(Liq {
                        pool: curve,
                        kind: LiqKind::Create,
                        mint_a: Some(mint),
                        mint_b: None,
                        amount_a: None,
                        amount_b: None,
                    }),
                    None => o.counts.undecoded += 1,
                }
            }
            _ => {}
        }
    }
    if swap_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}
