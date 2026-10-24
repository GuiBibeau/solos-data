//! `spot trial` (ADR-0014): stream a slot range from the archive into a scratch directory and
//! report what the backfill would produce there: transactions, rows and decode coverage per
//! program, block completeness, Parquet bytes per row, distinct pools and tokens, time and
//! peak memory. With `--capture N` it also keeps up to N real transactions per coverage key
//! (program, instruction, events) in the RPC shape with the rows the stream decoded, re-decodes
//! them through the RPC path and reports any difference: the fixtures of the tests.

use super::archive::{self, Options, Stream};
use super::dex::{ALL, Programs};
use super::rows::{Rows, liquidity_json, swap_json};
use super::tx::from_rpc;
use super::write::{Names, connection, copy_to, load_liquidity, load_swaps};
use crate::jsonout::Obj;
use crate::store::sql_string;
use serde_json::{Value, json};
use std::path::Path;
use std::time::Instant;

/// The windows of `[lo, hi]`, newest first, each at most `size` slots.
#[must_use]
pub fn windows(lo: u64, hi: u64, size: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut to = hi;
    loop {
        let from = to.saturating_sub(size - 1).max(lo);
        out.push((from, to));
        if from <= lo {
            break;
        }
        to = from - 1;
    }
    out
}

/// Peak resident memory of this process, bytes (Linux; `None` elsewhere).
#[must_use]
pub fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Stream `[from, to]` in one archive call with `parallel` times `JETSTREAMER_THREADS`
/// threads. Not one call per chunk: two `firehose` calls in flight in one process share
/// Jetstreamer's thread bookkeeping and can wait on each other forever ([`crate::stall`]).
///
/// # Errors
///
/// A stream failure.
pub async fn stream_range(
    from: u64,
    to: u64,
    parallel: usize,
    programs: &Programs,
    options: &Options,
) -> Result<Stream, String> {
    archive::stream(from, to, parallel, programs, options).await
}

fn fixtures(stream: &Stream, programs: &Programs) -> (Vec<Value>, u64) {
    let mut out = Vec::new();
    let mut mismatched = 0;
    for c in &stream.captured {
        let swaps: Vec<Value> = stream
            .rows
            .swaps
            .iter()
            .filter(|s| s.slot == c.slot && s.tx_index == c.index)
            .map(swap_json)
            .collect();
        let liquidity: Vec<Value> = stream
            .rows
            .liquidity
            .iter()
            .filter(|l| l.slot == c.slot && l.tx_index == c.index)
            .map(liquidity_json)
            .collect();
        let meta: Value = serde_json::from_str(&c.meta_json).unwrap_or(Value::Null);
        let again = from_rpc(&c.tx_b64, &meta, c.slot, c.index).map(|tx| {
            let mut rows = Rows::new();
            let named = super::rows::named(&tx.keys, programs);
            rows.add_tx(&tx, programs, &named);
            (
                rows.swaps.iter().map(swap_json).collect::<Vec<_>>(),
                rows.liquidity
                    .iter()
                    .map(liquidity_json)
                    .collect::<Vec<_>>(),
            )
        });
        let same = again
            .as_ref()
            .is_ok_and(|(s, l)| *s == swaps && *l == liquidity);
        if !same {
            mismatched += 1;
        }
        out.push(json!({
            "signature": c.signature, "slot": c.slot, "index": c.index, "cover": c.cover,
            "tx_b64": c.tx_b64, "meta": meta, "swaps": swaps, "liquidity": liquidity,
            "rpcPathAgrees": same,
        }));
    }
    (out, mismatched)
}

fn per_program(rows: &Rows) -> Obj {
    let mut out = Obj::new();
    for dex in ALL {
        let c = rows.counts[dex as usize];
        if c == Default::default() {
            continue;
        }
        out.set(
            dex.name(),
            json!({ "txs": c.txs, "failed": c.failed, "frames": c.frames, "swapIxs": c.swap_ixs,
                    "swaps": c.swaps, "liquidity": c.liquidity, "missingEvent": c.missing_event,
                    "undecoded": c.undecoded, "unresolved": c.unresolved }),
        );
    }
    out
}

/// Run the trial and describe it.
///
/// # Errors
///
/// A stream, DuckDB or file failure.
pub async fn trial(
    range: (u64, u64),
    parallel: usize,
    out_dir: &Path,
    capture: usize,
    programs: &Programs,
) -> Result<Obj, String> {
    let (from, to) = range;
    std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
    let started = Instant::now();
    let options = Options {
        windows: Vec::new(),
        capture,
    };
    let stream = stream_range(from, to, parallel, programs, &options).await?;
    let stream_s = started.elapsed().as_secs_f64();
    let mismatched_blocks = stream
        .blocks
        .iter()
        .filter(|(slot, b)| stream.delivered.get(slot).copied().unwrap_or(0) != b.executed)
        .count();
    let transactions: u64 = stream.delivered.values().sum();
    let written = Instant::now();
    let conn = connection("16", "32GB").map_err(|e| e.to_string())?;
    let times = &stream.blocks;
    let time = |slot: u64| times.get(&slot).and_then(|b| b.time);
    let mut names = Names::default();
    load_swaps(&conn, &stream.rows.swaps, &time, &mut names).map_err(|e| e.to_string())?;
    load_liquidity(&conn, &stream.rows.liquidity, &time, &mut names).map_err(|e| e.to_string())?;
    let load_s = written.elapsed().as_secs_f64();
    let mut files = Obj::new();
    for (name, select) in [
        (
            "swaps",
            format!("SELECT * FROM swaps ORDER BY {}", super::write::ORDER),
        ),
        (
            "swaps-by-slot",
            "SELECT * FROM swaps ORDER BY slot, tx_index, seq".to_owned(),
        ),
        (
            "liquidity",
            format!("SELECT * FROM liquidity ORDER BY {}", super::write::ORDER),
        ),
    ] {
        let record = copy_to(
            &conn,
            &select,
            out_dir,
            &format!("{name}.parquet"),
            ("spot", name, "trial"),
        )
        .map_err(|e| e.to_string())?;
        files.set(
            name,
            json!({ "rows": record.row_count, "bytes": record.bytes,
                    "bytesPerRow": if record.row_count > 0 { record.bytes as f64 / record.row_count as f64 } else { 0.0 } }),
        );
    }
    let write_s = written.elapsed().as_secs_f64() - load_s;
    let path = sql_string(&out_dir.join("swaps.parquet").to_string_lossy());
    let distinct: (i64, i64, i64) = conn
        .query_row(
            &format!(
                "SELECT count(DISTINCT pool), (SELECT count(DISTINCT m) FROM (SELECT mint_a AS m FROM read_parquet({path}) UNION SELECT mint_b FROM read_parquet({path}))), count(DISTINCT signer) FROM read_parquet({path})"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| e.to_string())?;
    let (fixtures, mismatched) = fixtures(&stream, programs);
    if capture > 0 {
        let text = serde_json::to_string_pretty(&json!({
            "source": format!("mainnet slots {from}..={to} from the Old Faithful archive through `solos-data spot trial` (ADR-0014); public chain data"),
            "transactions": fixtures,
        }))
        .map_err(|e| e.to_string())?;
        std::fs::write(out_dir.join("fixtures.json"), text).map_err(|e| e.to_string())?;
    }
    Ok(Obj::new()
        .with("from", from)
        .with("to", to)
        .with("slots", to - from + 1)
        .with("blocks", stream.blocks.len())
        .with("blocksWithMissingTransactions", mismatched_blocks)
        .with("transactions", transactions)
        .with("truncatedLogs", stream.rows.truncated)
        .with(
            "gaps",
            serde_json::to_value(&stream.rows.gaps).unwrap_or(Value::Null),
        )
        .with("swaps", stream.rows.swaps.len())
        .with("liquidity", stream.rows.liquidity.len())
        .with("distinctPools", distinct.0)
        .with("distinctMints", distinct.1)
        .with("distinctSigners", distinct.2)
        .with_obj("programs", per_program(&stream.rows))
        .with_obj("files", files)
        .with("streamSeconds", stream_s)
        .with("loadSeconds", load_s)
        .with("writeSeconds", write_s)
        .with("peakRssBytes", peak_rss())
        .with("fixtures", fixtures.len())
        .with("fixtureMismatches", mismatched))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_tile_the_range_newest_first() {
        assert_eq!(windows(1, 25, 10), vec![(16, 25), (6, 15), (1, 5)]);
        assert_eq!(windows(5, 5, 10), vec![(5, 5)]);
        assert_eq!(windows(0, 9, 10), vec![(0, 9)]);
    }
}
