//! Raydium: AMM v4 (`ray_log` lines), CLMM and CPMM (`emit!` events) and LaunchLab (`emit_cpi!`).

use super::discs::*;
use super::{Ctx, FrameOut, Liq, LiqKind, Rd, Trade, ordered, sqrt_q64};

/// AMM v4 `ray_log` types.
const LOG_INIT: u8 = 0;
const LOG_DEPOSIT: u8 = 1;
const LOG_WITHDRAW: u8 = 2;
const LOG_SWAP_IN: u8 = 3;
const LOG_SWAP_OUT: u8 = 4;

/// Raydium AMM v4. Instruction tags: 1 `Initialize2`, 3 `Deposit`, 4 `Withdraw`, 9 `SwapBaseIn`,
/// 11 `SwapBaseOut`, 16 `SwapBaseInV2`, 17 `SwapBaseOutV2`. The pool is account 1 (account 4
/// for `Initialize2`); its coin and pc vaults sit at 5 and 6 when the swap passes the optional
/// target-orders account (18 accounts), at 4 and 5 without it, and at 3 and 4 in the V2 swaps.
/// Direction 2 is coin to pc.
pub fn amm(c: &Ctx<'_>, o: &mut FrameOut) {
    let Some(&tag) = c.ix.data.first() else {
        return;
    };
    match tag {
        9 | 11 | 16 | 17 => {
            o.counts.swap_ixs += 1;
            let (coin, pc) = match tag {
                16 | 17 => (3, 4),
                _ if c.ix.accounts.len() >= 18 => (5, 6),
                _ => (4, 5),
            };
            let Some(log) = c.ray_log(&[LOG_SWAP_IN, LOG_SWAP_OUT]) else {
                o.counts.missing_event += 1;
                return;
            };
            let mut r = Rd::new(log);
            let parsed = (|| {
                let kind = r.u8()?;
                let (a, b, direction) = (r.u64()?, r.u64()?, r.u64()?);
                r.skip(8)?;
                let (pool_coin, pool_pc, last) = (r.u64()?, r.u64()?, r.u64()?);
                // SwapBaseIn: amount_in, minimum_out, ..., out_amount.
                // SwapBaseOut: max_in, amount_out, ..., deduct_in.
                let (amount_in, amount_out) = if kind == LOG_SWAP_IN {
                    (a, last)
                } else {
                    (last, b)
                };
                let a_to_b = match direction {
                    2 => true,
                    1 => false,
                    _ => return None,
                };
                Some((a_to_b, amount_in, amount_out, pool_coin, pool_pc))
            })();
            let Some((a_to_b, amount_in, amount_out, pool_coin, pool_pc)) = parsed else {
                o.counts.undecoded += 1;
                return;
            };
            o.trade((|| {
                Some(Trade {
                    pool: c.account(1)?,
                    mint_a: c.mint_at(coin)?,
                    mint_b: c.mint_at(pc)?,
                    a_to_b,
                    amount_in,
                    amount_out,
                    reserve_a: Some(pool_coin),
                    reserve_b: Some(pool_pc),
                    ..Trade::default()
                })
            })());
        }
        3 | 4 => {
            let (types, kind) = if tag == 3 {
                ([LOG_DEPOSIT], LiqKind::Add)
            } else {
                ([LOG_WITHDRAW], LiqKind::Remove)
            };
            let Some(log) = c.ray_log(&types) else {
                return;
            };
            let mut r = Rd::new(log);
            // Deposit: type, max_coin, max_pc, base, pool_coin, pool_pc, pool_lp, pnl_x u128,
            // pnl_y u128, deduct_coin, deduct_pc. Withdraw: type, withdraw_lp, user_lp,
            // pool_coin, pool_pc, pool_lp, pnl_x u128, pnl_y u128, out_coin, out_pc.
            let skip = if tag == 3 {
                1 + 6 * 8 + 32
            } else {
                1 + 5 * 8 + 32
            };
            let amounts = r.skip(skip).and_then(|()| Some((r.u64()?, r.u64()?)));
            match (c.account(1), amounts) {
                (Some(pool), Some((coin, pc))) => o.liquidity.push(Liq {
                    pool,
                    kind,
                    mint_a: c.mint_at(6),
                    mint_b: c.mint_at(7),
                    amount_a: Some(coin),
                    amount_b: Some(pc),
                }),
                _ => o.counts.undecoded += 1,
            }
        }
        1 => {
            let Some(log) = c.ray_log(&[LOG_INIT]) else {
                return;
            };
            // type, time, pc_decimals u8, coin_decimals u8, pc_lot, coin_lot, pc_amount,
            // coin_amount, market.
            let mut r = Rd::new(log);
            let amounts = r
                .skip(1 + 8 + 2 + 16)
                .and_then(|()| Some((r.u64()?, r.u64()?)));
            match (c.account(4), amounts) {
                (Some(pool), Some((pc, coin))) => o.liquidity.push(Liq {
                    pool,
                    kind: LiqKind::Create,
                    mint_a: c.account(8),
                    mint_b: c.account(9),
                    amount_a: Some(coin),
                    amount_b: Some(pc),
                }),
                _ => o.counts.undecoded += 1,
            }
        }
        _ => {}
    }
}

/// Raydium CLMM. `SwapEvent` carries the pool, both amounts, the direction and the state after
/// the trade; the tokens are the mints of the pool's vaults (owned by the pool, token 0 below
/// token 1). Liquidity: `CreatePersonalPositionEvent` (open), `IncreaseLiquidityEvent` (pool
/// at account 2), `DecreaseLiquidityEvent` (pool at account 3), `PoolCreatedEvent`.
pub fn clmm(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[IX_SWAP, IX_SWAP_V2, IX_SWAP_ROUTER_BASE_IN]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_SWAP_EVENT => {
                swaps += 1;
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(96)?;
                    let (amount_0, _fee_0, amount_1, _fee_1) =
                        (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                    let zero_for_one = r.bool()?;
                    Some((
                        pool,
                        amount_0,
                        amount_1,
                        zero_for_one,
                        r.u128()?,
                        r.u128()?,
                        r.i32()?,
                    ))
                })();
                let Some((pool, amount_0, amount_1, zero_for_one, sqrt, liquidity, tick)) = parsed
                else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let (amount_in, amount_out) = if zero_for_one {
                    (amount_0, amount_1)
                } else {
                    (amount_1, amount_0)
                };
                o.trade(c.owned_mints(&pool).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b: zero_for_one,
                    amount_in,
                    amount_out,
                    sqrt_price: Some(sqrt_q64(sqrt)),
                    tick: Some(tick),
                    liquidity: Some(liquidity as f64),
                    ..Trade::default()
                }));
            }
            EV_CREATE_PERSONAL_POSITION_EVENT => {
                // pool, minter, nft_owner, tick_lower, tick_upper, liquidity u128, amounts.
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(64 + 8 + 16)?;
                    Some((pool, r.u64()?, r.u64()?))
                })();
                push_liq(o, parsed, LiqKind::Add);
            }
            EV_INCREASE_LIQUIDITY_EVENT | EV_DECREASE_LIQUIDITY_EVENT => {
                // position_nft_mint, liquidity u128, amount_0, amount_1, ...
                let (kind, at) = if disc == EV_INCREASE_LIQUIDITY_EVENT {
                    (LiqKind::Add, 2)
                } else {
                    (LiqKind::Remove, 3)
                };
                let known = c.is(&[
                    IX_INCREASE_LIQUIDITY,
                    IX_INCREASE_LIQUIDITY_V2,
                    IX_DECREASE_LIQUIDITY,
                    IX_DECREASE_LIQUIDITY_V2,
                ]);
                let parsed = (|| {
                    r.skip(32 + 16)?;
                    let amounts = (r.u64()?, r.u64()?);
                    Some((c.account(at).filter(|_| known)?, amounts.0, amounts.1))
                })();
                push_liq(o, parsed, kind);
            }
            EV_POOL_CREATED_EVENT => {
                let parsed = (|| {
                    let (mint_0, mint_1) = (r.key()?, r.key()?);
                    r.skip(2)?;
                    Some((r.key()?, mint_0, mint_1))
                })();
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
            _ => {}
        }
    }
    if swap_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}

fn push_liq(o: &mut FrameOut, parsed: Option<(super::Key, u64, u64)>, kind: LiqKind) {
    match parsed {
        Some((pool, a, b)) => o.liquidity.push(Liq {
            pool,
            kind,
            mint_a: None,
            mint_b: None,
            amount_a: Some(a),
            amount_b: Some(b),
        }),
        None => o.counts.undecoded += 1,
    }
}

/// Raydium CPMM. `SwapEvent` (pool, both vaults before the trade, both amounts; since the
/// creator-fee release also both mints and the fees). The mints otherwise come from the swap
/// instruction (accounts 10 and 11); token 0 is the lower key. Liquidity: `LpChangeEvent`
/// (change type 0 deposit, 1 withdraw; mints at accounts 10 and 11) and `initialize` (pool at
/// account 3, mints at 4 and 5, the initial amounts in its arguments).
pub fn cpmm(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[IX_SWAP_BASE_INPUT, IX_SWAP_BASE_OUTPUT]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_SWAP_EVENT => {
                swaps += 1;
                let parsed = (|| {
                    let pool = r.key()?;
                    let (in_before, out_before, amount_in, amount_out) =
                        (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                    r.skip(16)?;
                    r.bool()?;
                    // The creator-fee release appends the mints and fees.
                    let tail = (|| {
                        let (input, output) = (r.key()?, r.key()?);
                        let (trade_fee, creator_fee, on_input) = (r.u64()?, r.u64()?, r.bool()?);
                        Some((
                            input,
                            output,
                            trade_fee + if on_input { creator_fee } else { 0 },
                        ))
                    })();
                    Some((pool, in_before, out_before, amount_in, amount_out, tail))
                })();
                let Some((pool, in_before, out_before, amount_in, amount_out, tail)) = parsed
                else {
                    o.counts.undecoded += 1;
                    continue;
                };
                let mints = match tail {
                    Some((i, out, _)) => Some((i, out)),
                    None if swap_ix => c.account(10).zip(c.account(11)),
                    None => None,
                };
                o.trade(mints.map(|(input, output)| {
                    let (mint_a, mint_b, a_to_b) = ordered(input, output);
                    let (reserve_a, reserve_b) = if a_to_b {
                        (in_before, out_before)
                    } else {
                        (out_before, in_before)
                    };
                    Trade {
                        pool,
                        mint_a,
                        mint_b,
                        a_to_b,
                        amount_in,
                        amount_out,
                        fee: tail.map(|t| t.2),
                        fee_on_input: tail.map(|_| true),
                        reserve_a: Some(reserve_a),
                        reserve_b: Some(reserve_b),
                        ..Trade::default()
                    }
                }));
            }
            EV_LP_CHANGE_EVENT => {
                // pool, lp_before, vault_0_before, vault_1_before, amount_0, amount_1, fees,
                // change_type.
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(24)?;
                    let (a, b) = (r.u64()?, r.u64()?);
                    r.skip(16)?;
                    Some((pool, a, b, r.u8()?))
                })();
                match parsed {
                    Some((pool, a, b, change)) if change <= 1 => o.liquidity.push(Liq {
                        pool,
                        kind: if change == 0 {
                            LiqKind::Add
                        } else {
                            LiqKind::Remove
                        },
                        mint_a: c.account(10),
                        mint_b: c.account(11),
                        amount_a: Some(a),
                        amount_b: Some(b),
                    }),
                    _ => o.counts.undecoded += 1,
                }
            }
            _ => {}
        }
    }
    if c.is(&[IX_INITIALIZE]) {
        let mut r = Rd::new(&c.ix.data[8..]);
        let amounts = (|| Some((r.u64()?, r.u64()?)))();
        if let (Some(pool), Some((a, b))) = (c.account(3), amounts) {
            o.liquidity.push(Liq {
                pool,
                kind: LiqKind::Create,
                mint_a: c.account(4),
                mint_b: c.account(5),
                amount_a: Some(a),
                amount_b: Some(b),
            });
        }
    }
    if swap_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}

/// Raydium LaunchLab. `TradeEvent` (pool, curve state before and after, amounts, fees,
/// direction 0 buy / 1 sell); base and quote mints at accounts 9 and 10 of the trade
/// instructions (pool at 4). A later release added `creator_fee` before `share_fee`: the body is
/// 131 bytes without it.
pub fn launchlab(c: &Ctx<'_>, o: &mut FrameOut) {
    let trade_ix = c.is(&[
        IX_BUY_EXACT_IN,
        IX_BUY_EXACT_OUT,
        IX_SELL_EXACT_IN,
        IX_SELL_EXACT_OUT,
    ]);
    if trade_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        if disc != EV_TRADE_EVENT {
            continue;
        }
        swaps += 1;
        let mut r = Rd::new(body);
        let creator_fee = body.len() != 131;
        let parsed = (|| {
            let pool = r.key()?;
            r.skip(7 * 8)?;
            let (amount_in, amount_out) = (r.u64()?, r.u64()?);
            let mut fee = r.u64()? + r.u64()?;
            if creator_fee {
                fee += r.u64()?;
            }
            fee += r.u64()?;
            Some((pool, amount_in, amount_out, fee, r.u8()?))
        })();
        let Some((pool, amount_in, amount_out, fee, direction)) = parsed else {
            o.counts.undecoded += 1;
            continue;
        };
        let buy = direction == 0;
        o.trade(
            (|| {
                (c.account(4)? == pool).then_some(())?;
                Some(Trade {
                    pool,
                    mint_a: c.account(9)?,
                    mint_b: c.account(10)?,
                    a_to_b: !buy,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(buy),
                    ..Trade::default()
                })
            })()
            .filter(|_| trade_ix),
        );
    }
    if trade_ix && swaps == 0 {
        o.counts.missing_event += 1;
    }
}
