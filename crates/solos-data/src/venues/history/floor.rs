//! The slot of an instant: the first produced block whose block time is at or after it, found
//! by stepping from the finalized slot at the observed slot rate (`getBlocks` skips the empty
//! slots, `getBlockTime` reads the block's time). A few calls; recorded once per floor.

use crate::collector::rpc::Rpc;
use crate::venues::rpc::call;
use crate::venues::walk::finalized_slot;
use serde_json::{Value, json};

/// The first produced block at or after `slot` and its block time.
async fn block_at(rpc: &dyn Rpc, slot: i64) -> Result<(i64, i64), String> {
    let reply = call(
        rpc,
        "getBlocks",
        json!([slot, slot + 200, { "commitment": "finalized" }]),
    )
    .await
    .map_err(|e| e.to_string())?;
    let first = reply
        .result
        .as_array()
        .and_then(|blocks| blocks.first())
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("no produced block in slots {slot}..{}", slot + 200))?;
    let time = call(rpc, "getBlockTime", json!([first]))
        .await
        .map_err(|e| e.to_string())?
        .result
        .as_i64()
        .ok_or_else(|| format!("block {first} has no block time"))?;
    Ok((first, time))
}

/// The slot of the first block at or after `seconds` (Unix time).
///
/// # Errors
///
/// RPC failure, or no convergence in sixteen steps.
pub async fn slot_at(rpc: &dyn Rpc, seconds: i64) -> Result<i64, String> {
    let tip = finalized_slot(rpc).await?;
    let (tip_block, tip_time) = block_at(rpc, tip.saturating_sub(400)).await?;
    // Mainnet produces about 2.5 slots a second; each step corrects by the observed rate.
    let mut rate = 2.5_f64;
    let mut slot = tip_block - ((tip_time - seconds) as f64 * rate) as i64;
    let mut previous = (tip_block, tip_time);
    for _ in 0..16 {
        let (block, time) = block_at(rpc, slot).await?;
        if time != previous.1 && block != previous.0 {
            let observed = (previous.0 - block) as f64 / (previous.1 - time) as f64;
            if observed.is_finite() && (1.0..4.0).contains(&observed) {
                rate = observed;
            }
        }
        let error = time - seconds;
        if (0..=2).contains(&error) {
            // Step back to the first block at or after the instant.
            let mut out = block;
            let mut probe = block;
            for _ in 0..8 {
                let (before, before_time) = block_at(rpc, probe - 8).await?;
                if before_time < seconds || before >= probe {
                    break;
                }
                out = before;
                probe = before;
            }
            return Ok(out);
        }
        previous = (block, time);
        let step = ((error as f64) * rate).round() as i64;
        slot = block - if step == 0 { error.signum() } else { step };
    }
    Err("the floor slot did not converge".into())
}
