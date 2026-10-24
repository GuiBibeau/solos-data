//! Meteora: DLMM (`emit_cpi!`), the Dynamic AMM v1 (`emit!`), DAMM v2 and the Dynamic Bonding
//! Curve (`emit_cpi!`). DLMM, DAMM v2 and DBC swap one pool per instruction; when a release emits
//! an older and a newer swap event together, the newest is kept, so a trade is one row.

use super::discs::*;
use super::{Ctx, FrameOut, Key, Liq, LiqKind, Rd, Trade, sqrt_q64};

fn create(o: &mut FrameOut, parsed: Option<(Key, Key, Key)>) {
    match parsed {
        Some((pool, a, b)) => o.liquidity.push(Liq {
            pool,
            kind: LiqKind::Create,
            mint_a: Some(a),
            mint_b: Some(b),
            amount_a: None,
            amount_b: None,
        }),
        None => o.counts.undecoded += 1,
    }
}

fn change(
    o: &mut FrameOut,
    parsed: Option<(Key, u64, u64)>,
    kind: LiqKind,
    mints: (Option<Key>, Option<Key>),
) {
    match parsed {
        Some((pool, a, b)) => o.liquidity.push(Liq {
            pool,
            kind,
            mint_a: mints.0,
            mint_b: mints.1,
            amount_a: Some(a),
            amount_b: Some(b),
        }),
        None => o.counts.undecoded += 1,
    }
}

/// Keep the newest of the swap events a frame emitted: `(rank, trade or None when undecoded)`.
fn keep(best: &mut Option<(u8, Option<Trade>)>, rank: u8, trade: Option<Trade>) {
    if best.as_ref().is_none_or(|(r, _)| rank > *r) {
        *best = Some((rank, trade));
    }
}

/// A trade whose tokens sit at accounts `a` and `b` of the instruction when account `at` is the
/// pool the event names.
fn at_accounts(c: &Ctx<'_>, pool: Key, at: usize, a: usize, b: usize) -> Option<(Key, Key)> {
    (c.account(at)? == pool).then_some(())?;
    Some((c.account(a)?, c.account(b)?))
}

fn finish(o: &mut FrameOut, swap_ix: bool, best: Option<(u8, Option<Trade>)>) {
    match best {
        Some((_, Some(trade))) => o.trades.push(trade),
        Some((_, None)) => o.counts.unresolved += 1,
        None if swap_ix => o.counts.missing_event += 1,
        None => {}
    }
}

/// Meteora DLMM. `Swap` (and `Swap2Evt` since the limit-order release); token x and y mints at
/// accounts 6 and 7 of every swap instruction (pair at 0). `tick` is the active bin after the
/// trade. Liquidity: `AddLiquidity`, `RemoveLiquidity` (amounts x, y), `LbPairCreate`.
pub fn dlmm(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[
        IX_SWAP,
        IX_SWAP2,
        IX_SWAP_EXACT_OUT,
        IX_SWAP_EXACT_OUT2,
        IX_SWAP_WITH_PRICE_IMPACT,
        IX_SWAP_WITH_PRICE_IMPACT2,
    ]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut best: Option<(u8, Option<Trade>)> = None;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_SWAP => {
                // lb_pair, from, start_bin, end_bin, amount_in, amount_out, swap_for_y, fee,
                // protocol_fee, fee_bps u128, host_fee.
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(32 + 4)?;
                    let end_bin = r.i32()?;
                    let (amount_in, amount_out, swap_for_y, fee) =
                        (r.u64()?, r.u64()?, r.bool()?, r.u64()?);
                    Some((pool, end_bin, amount_in, amount_out, swap_for_y, fee))
                })();
                let Some((pool, end_bin, amount_in, amount_out, a_to_b, fee)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let trade = at_accounts(c, pool, 0, 6, 7).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(true),
                    tick: Some(end_bin),
                    ..Trade::default()
                });
                keep(&mut best, 1, trade);
            }
            EV_SWAP2_EVT => {
                // lb_pair, from, start_bin, end_bin, swap_for_y, fee_bps u128, amount_in,
                // amount_left, amount_out, mm_fee, protocol_fee, limit_order_fee, host_fee,
                // fees_on_input, fees_on_token_x.
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(32 + 4)?;
                    let end_bin = r.i32()?;
                    let swap_for_y = r.bool()?;
                    r.skip(16)?;
                    let (amount_in, _left, amount_out) = (r.u64()?, r.u64()?, r.u64()?);
                    let fee = r.u64()? + r.u64()? + r.u64()?;
                    r.skip(8)?;
                    Some((
                        pool,
                        end_bin,
                        swap_for_y,
                        amount_in,
                        amount_out,
                        fee,
                        r.bool()?,
                    ))
                })();
                let Some((pool, end_bin, a_to_b, amount_in, amount_out, fee, on_input)) = parsed
                else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let trade = at_accounts(c, pool, 0, 6, 7).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(on_input),
                    tick: Some(end_bin),
                    ..Trade::default()
                });
                keep(&mut best, 2, trade);
            }
            EV_ADD_LIQUIDITY | EV_REMOVE_LIQUIDITY => {
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(64)?;
                    Some((pool, r.u64()?, r.u64()?))
                })();
                let kind = if disc == EV_ADD_LIQUIDITY {
                    LiqKind::Add
                } else {
                    LiqKind::Remove
                };
                change(o, parsed, kind, (None, None));
            }
            EV_LB_PAIR_CREATE => {
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(2)?;
                    Some((pool, r.key()?, r.key()?))
                })();
                create(o, parsed);
            }
            _ => {}
        }
    }
    finish(o, swap_ix, best);
}

/// Meteora Dynamic AMM (DAMM v1). `Swap` carries amounts and fees only: the pool is account 0
/// of the swap, the tokens are the mints of its vault token accounts (5 and 6), and the
/// direction is which of them the trader's source (account 1) or destination (2) holds, or the
/// protocol fee account (11), which is in the input token.
/// Liquidity: `AddLiquidity`, `RemoveLiquidity` (pool at 0, vault token accounts at 9 and 10),
/// `BootstrapLiquidity`, `PoolCreated`.
pub fn damm(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[IX_SWAP]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_SWAP => {
                swaps += 1;
                let parsed = (|| Some((r.u64()?, r.u64()?, r.u64()? + r.u64()?)))();
                let Some((amount_in, amount_out, fee)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                o.trade(
                    (|| {
                        let (pool, mint_a, mint_b) = (c.account(0)?, c.mint_at(5)?, c.mint_at(6)?);
                        // The trader's accounts, else the protocol fee account (11), which
                        // takes its cut in the input token: a router's temporary accounts are
                        // created and closed in the transaction and have no balance entry.
                        let a_to_b = match (c.mint_at(1), c.mint_at(2), c.mint_at(11)) {
                            (Some(src), _, _) if src == mint_a => true,
                            (Some(src), _, _) if src == mint_b => false,
                            (_, Some(dst), _) if dst == mint_b => true,
                            (_, Some(dst), _) if dst == mint_a => false,
                            (_, _, Some(fee)) if fee == mint_a => true,
                            (_, _, Some(fee)) if fee == mint_b => false,
                            _ => return None,
                        };
                        Some(Trade {
                            pool,
                            mint_a,
                            mint_b,
                            a_to_b,
                            amount_in,
                            amount_out,
                            fee: Some(fee),
                            fee_on_input: Some(true),
                            ..Trade::default()
                        })
                    })()
                    .filter(|_| swap_ix),
                );
            }
            EV_ADD_LIQUIDITY | EV_REMOVE_LIQUIDITY => {
                let parsed = (|| {
                    r.skip(8)?;
                    Some((c.account(0)?, r.u64()?, r.u64()?))
                })();
                let kind = if disc == EV_ADD_LIQUIDITY {
                    LiqKind::Add
                } else {
                    LiqKind::Remove
                };
                change(o, parsed, kind, (c.mint_at(9), c.mint_at(10)));
            }
            EV_POOL_CREATED => {
                // lp_mint, token_a_mint, token_b_mint, pool_type u8, pool.
                let parsed = (|| {
                    r.skip(32)?;
                    let (a, b) = (r.key()?, r.key()?);
                    r.skip(1)?;
                    Some((r.key()?, a, b))
                })();
                create(o, parsed);
            }
            _ => {}
        }
    }
    if swap_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}

/// Meteora DAMM v2. `EvtSwap` (to 0.1.5) and `EvtSwap2` (0.1.6, December 2025); tokens a and b
/// at accounts 6 and 7 of the swap (pool at 1); direction 0 is a to b. Liquidity:
/// `EvtLiquidityChange` (change type 0 add, 1 remove), the older `EvtAddLiquidity` and
/// `EvtRemoveLiquidity` (a release emitted both; the newer one is kept), `EvtInitializePool`.
pub fn damm_v2(c: &Ctx<'_>, o: &mut FrameOut) {
    let changes = c
        .anchor_events()
        .any(|(disc, _)| disc == EV_EVT_LIQUIDITY_CHANGE);
    let swap_ix = c.is(&[IX_SWAP, IX_SWAP2]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut best: Option<(u8, Option<Trade>)> = None;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_EVT_SWAP => {
                // pool, direction, has_referral, params (2 u64), result (output, next_sqrt u128,
                // lp, protocol, partner, referral fees), actual_amount_in, timestamp.
                let parsed = (|| {
                    let pool = r.key()?;
                    let direction = r.u8()?;
                    r.skip(1 + 16)?;
                    let (amount_out, sqrt) = (r.u64()?, r.u128()?);
                    let fee = r.u64()? + r.u64()? + r.u64()? + r.u64()?;
                    Some((pool, direction == 0, r.u64()?, amount_out, sqrt, fee))
                })();
                let Some((pool, a_to_b, amount_in, amount_out, sqrt, fee)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let trade = at_accounts(c, pool, 1, 6, 7).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    sqrt_price: Some(sqrt_q64(sqrt)),
                    ..Trade::default()
                });
                keep(&mut best, 1, trade);
            }
            EV_EVT_SWAP2 => {
                // pool, direction, collect_fee_mode, has_referral, params (2 u64, u8), result
                // (included-fee input, excluded-fee input, left, output, next_sqrt u128, four
                // fees), transfer-fee amounts (3 u64), timestamp, reserves a and b after.
                let parsed = (|| {
                    let pool = r.key()?;
                    let direction = r.u8()?;
                    r.skip(1 + 1 + 17)?;
                    let amount_in = r.u64()?;
                    r.skip(16)?;
                    let (amount_out, sqrt) = (r.u64()?, r.u128()?);
                    let fee = r.u64()? + r.u64()? + r.u64()? + r.u64()?;
                    Some((pool, direction == 0, amount_in, amount_out, sqrt, fee))
                })();
                let Some((pool, a_to_b, amount_in, amount_out, sqrt, fee)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let trade = at_accounts(c, pool, 1, 6, 7).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    sqrt_price: Some(sqrt_q64(sqrt)),
                    ..Trade::default()
                });
                keep(&mut best, 2, trade);
            }
            EV_EVT_LIQUIDITY_CHANGE => {
                // pool, position, owner, a, b, ..., change_type (last byte).
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(64)?;
                    let (a, b) = (r.u64()?, r.u64()?);
                    r.skip(4 * 8 + 16 + 16)?;
                    Some((pool, a, b, r.u8()?))
                })();
                match parsed {
                    Some((pool, a, b, kind @ 0..=1)) => change(
                        o,
                        Some((pool, a, b)),
                        if kind == 0 {
                            LiqKind::Add
                        } else {
                            LiqKind::Remove
                        },
                        (None, None),
                    ),
                    _ => o.counts.undecoded += 1,
                }
            }
            EV_EVT_ADD_LIQUIDITY | EV_EVT_REMOVE_LIQUIDITY if !changes => {
                // pool, position, owner, params (u128, 2 u64), a, b.
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(64 + 32)?;
                    Some((pool, r.u64()?, r.u64()?))
                })();
                let kind = if disc == EV_EVT_ADD_LIQUIDITY {
                    LiqKind::Add
                } else {
                    LiqKind::Remove
                };
                change(o, parsed, kind, (None, None));
            }
            EV_EVT_INITIALIZE_POOL => {
                create(o, (|| Some((r.key()?, r.key()?, r.key()?)))());
            }
            _ => {}
        }
    }
    finish(o, swap_ix, best);
}

/// Meteora Dynamic Bonding Curve. `EvtSwap`, `EvtSwap2` and `EvtSwap3` (each also with a
/// transfer-hook twin); base and quote mints at accounts 7 and 8 of the swap (pool at 2);
/// direction 0 is base to quote.
pub fn dbc(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[IX_SWAP, IX_SWAP2, IX_SWAP2_WITH_TRANSFER_HOOK]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut best: Option<(u8, Option<Trade>)> = None;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        // (rank, pool, base to quote, in, out, sqrt, fee, fee on base)
        let parsed = match disc {
            EV_EVT_SWAP => (|| {
                // pool, config, direction, has_referral, params (2 u64), result (actual input,
                // output, next_sqrt u128, trading, protocol, referral fees), amount_in, ts.
                let pool = r.key()?;
                r.skip(32)?;
                let direction = r.u8()?;
                r.skip(1 + 16)?;
                let (amount_in, amount_out, sqrt) = (r.u64()?, r.u64()?, r.u128()?);
                let fee = r.u64()? + r.u64()? + r.u64()?;
                Some((
                    1,
                    pool,
                    direction == 0,
                    amount_in,
                    amount_out,
                    sqrt,
                    fee,
                    None,
                ))
            })(),
            EV_EVT_SWAP2 | EV_EVT_SWAP2_WITH_TRANSFER_HOOK => (|| {
                // pool, config, direction, has_referral, params (2 u64, u8), result
                // (included-fee input, excluded-fee input, left, output, next_sqrt u128,
                // trading, protocol, referral fees), quote reserve, threshold, ts.
                let pool = r.key()?;
                r.skip(32)?;
                let direction = r.u8()?;
                r.skip(1 + 17)?;
                let amount_in = r.u64()?;
                r.skip(16)?;
                let (amount_out, sqrt) = (r.u64()?, r.u128()?);
                let fee = r.u64()? + r.u64()? + r.u64()?;
                Some((
                    2,
                    pool,
                    direction == 0,
                    amount_in,
                    amount_out,
                    sqrt,
                    fee,
                    None,
                ))
            })(),
            EV_EVT_SWAP3 | EV_EVT_SWAP3_WITH_TRANSFER_HOOK => (|| {
                // pool, config, direction, swap_mode, has_referral, fee_on_base, in (incl. and
                // excl. transfer fee), out (incl., excl.), trading, protocol, referral fees,
                // next_sqrt u128, quote reserve, ts.
                let pool = r.key()?;
                r.skip(32)?;
                let direction = r.u8()?;
                r.skip(2)?;
                let fee_on_base = r.bool()?;
                let amount_in = r.u64()?;
                r.skip(16)?;
                let amount_out = r.u64()?;
                let fee = r.u64()? + r.u64()? + r.u64()?;
                let sqrt = r.u128()?;
                let a_to_b = direction == 0;
                Some((
                    3,
                    pool,
                    a_to_b,
                    amount_in,
                    amount_out,
                    sqrt,
                    fee,
                    Some(fee_on_base == a_to_b),
                ))
            })(),
            _ => continue,
        };
        let Some((rank, pool, a_to_b, amount_in, amount_out, sqrt, fee, on_input)) = parsed else {
            o.counts.undecoded += 1;
            continue;
        };
        let trade = at_accounts(c, pool, 2, 7, 8).map(|(mint_a, mint_b)| Trade {
            pool,
            mint_a,
            mint_b,
            a_to_b,
            amount_in,
            amount_out,
            fee: Some(fee),
            fee_on_input: on_input,
            sqrt_price: Some(sqrt_q64(sqrt)),
            ..Trade::default()
        });
        keep(&mut best, rank, trade);
    }
    finish(o, swap_ix, best);
}
