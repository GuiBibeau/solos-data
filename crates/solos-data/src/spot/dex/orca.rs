//! Orca Whirlpool: `Traded`, `LiquidityIncreased`, `LiquidityDecreased` and `PoolInitialized`
//! (`emit!`, released 2025-02-27). A two-hop swap emits one `Traded` per pool. The tokens are the
//! mints of the vaults the whirlpool owns (token a below token b).

use super::discs::*;
use super::{Ctx, FrameOut, Liq, LiqKind, Rd, Trade, sqrt_q64};

/// Orca Whirlpool.
pub fn whirlpool(c: &Ctx<'_>, o: &mut FrameOut) {
    let swap_ix = c.is(&[IX_SWAP, IX_SWAP_V2, IX_TWO_HOP_SWAP, IX_TWO_HOP_SWAP_V2]);
    if swap_ix {
        o.counts.swap_ixs += 1;
    }
    let mut swaps = 0;
    for (disc, body) in c.anchor_events() {
        let mut r = Rd::new(body);
        match disc {
            EV_TRADED => {
                swaps += 1;
                // whirlpool, a_to_b, pre_sqrt u128, post_sqrt u128, input, output, input
                // transfer fee, output transfer fee, lp_fee, protocol_fee.
                let parsed = (|| {
                    let pool = r.key()?;
                    let a_to_b = r.bool()?;
                    r.skip(16)?;
                    let post = r.u128()?;
                    let (amount_in, amount_out) = (r.u64()?, r.u64()?);
                    r.skip(16)?;
                    let fee = r.u64()? + r.u64()?;
                    Some((pool, a_to_b, post, amount_in, amount_out, fee))
                })();
                let Some((pool, a_to_b, post, amount_in, amount_out, fee)) = parsed else {
                    o.counts.undecoded += 1;
                    continue;
                };
                o.trade(c.owned_mints(&pool).map(|(mint_a, mint_b)| Trade {
                    pool,
                    mint_a,
                    mint_b,
                    a_to_b,
                    amount_in,
                    amount_out,
                    fee: Some(fee),
                    fee_on_input: Some(true),
                    sqrt_price: Some(sqrt_q64(post)),
                    ..Trade::default()
                }));
            }
            EV_LIQUIDITY_INCREASED | EV_LIQUIDITY_DECREASED => {
                // whirlpool, position, tick_lower, tick_upper, liquidity u128, a, b, ...
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(32 + 8 + 16)?;
                    Some((pool, r.u64()?, r.u64()?))
                })();
                match parsed {
                    Some((pool, a, b)) => o.liquidity.push(Liq {
                        pool,
                        kind: if disc == EV_LIQUIDITY_INCREASED {
                            LiqKind::Add
                        } else {
                            LiqKind::Remove
                        },
                        mint_a: None,
                        mint_b: None,
                        amount_a: Some(a),
                        amount_b: Some(b),
                    }),
                    None => o.counts.undecoded += 1,
                }
            }
            EV_POOL_INITIALIZED => {
                let parsed = (|| {
                    let pool = r.key()?;
                    r.skip(32)?;
                    Some((pool, r.key()?, r.key()?))
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
