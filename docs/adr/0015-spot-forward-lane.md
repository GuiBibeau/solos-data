# ADR-0015: Spot swaps forward from the history's top, over RPC

Status: accepted, October 24, 2026. Extends ADR-0014 (spot swaps from the archive) with the
slots after its range; reuses its rows, checks and files.

The spot history (ADR-0014) ends at slot 460,511,999, the last slot of epoch 1065, the newest
epoch Old Faithful served when it was set up. Without a forward lane the dataset stops there.
This lane collects the same 19 programs, rows and tables from slot 460,512,000 (the first slot of
epoch 1066) onward and keeps up with the finalized tip.

## Source: `getBlock`, not `getTransactionsForAddress`

The venues lane (ADR-0012) reads each program's transactions with `getTransactionsForAddress`
bulk pages, 10 Helius credits per 100 transactions. For the spot programs that is too dear: a
manifest window at the tip lists about 145 to 206 successful program transactions a slot (summed
over programs; a route through two programs counts in both), so bulk pages would cost about 3 to
4.5M credits a day, twice the 2M-a-day budget the venues lane was held to, before failed
transactions. `getBlock` costs 1 credit (Helius's published prices, 2026-10-24) and returns the
whole block, so a slot costs one credit whatever its volume, and the block itself is the unit the
history's completeness check is built on.

Each produced block of a batch (`getBlocks`) is fetched with `getBlock` (finalized, `base64`,
`transactionDetails: full`, `rewards: false`, `maxSupportedTransactionVersion: 1`) and decoded as
it arrives through the RPC-shape decoder (`spot::tx::from_rpc`), the path the ADR-0014 fixtures
prove gives the archive's rows. A transaction's `tx_index` is its position in the block, as the
archive's is. Only rows are kept.

## Decision

- **`solos-data spot live run --config config/spot-live.json`**, its own unit
  (`ops/solos-data-spot.service`), ledger (`<augment root>/spot/checkpoint.duckdb`), file tree
  (`spot/tables/spot/{swaps,liquidity,batches}/ALL/<from>-<to>.parquet`), `catalog-spot.json` and
  `status-spot.json` (`Lane::SpotLive`). `augment query` reads every lane's catalog, so
  `spot_swaps`, `spot_liquidity` and `spot_batches` span both lanes.
- **Oldest first, one batch at a time**: `batchSlots` (9,000, about an hour; 48 to an epoch),
  never across an epoch boundary, taken once its last slot is finalized. The first batch starts at
  `start`.
- **The seam.** `start` is 460,512,000, the history's `top` plus one (a config test holds them
  together). The history only descends from `top` and this lane only climbs from `start`, so
  every slot is in exactly one lane's batches: no gap, no slot twice. `spot live check` checks
  the lane like `spot check` checks the history (the batches tile one range, each fetched every
  listed block and missed no manifest signature, each file holds its batch's rows) and checks
  the seam: its first batch starts at `start`, and the history's newest batch ends on the slot
  before.
- **Checks, as in ADR-0014, before a batch is written.** (1) Every block `getBlocks` lists was
  fetched, with its transactions. (2) For each program, every successful signature its
  `getSignaturesForAddress` manifest lists over one 1,000-slot window of the batch was among
  the fetched transactions. A failed batch is retried whole after 30 s. The `batches` rows have
  the history's columns.
- **Files and cursor** as in the history: the batch's three files are written, registered, and
  the cursor (`next`, the batch's last slot plus one) recorded with the last one; a restart
  redoes at most the batch in flight. A first run records `start` before it collects anything.
- **RPC.** The collector's provider (`SOLANA_RPC_URL`) with a credit limiter of its own
  (`creditsPerSecond` 60, `concurrency` 24 `getBlock` calls in flight) over a transport that
  asks for gzip: a block is about 2.7 MB of JSON and 0.66 MB on the wire.
- **Stalls** (ADR-0014): progress is any RPC answer or written batch; `stallMinutes` (20)
  without it exits the process with code 75 and the unit (`Restart=always`) restarts it.
- **Health.** `solos-data health` reads `status-spot.json` when it exists: its age (15 min),
  the lag behind the finalized slot (warn above 27,000 slots, three batches), no batch for an
  hour (warn) or three (critical) while at least two batches' worth of finalized slots wait, and
  the failed-batch streak. While it catches up the lag warns; it is critical only when batches
  stop or keep failing.
- **Backup.** The nightly backup copies `spot/tables` by size, like the history's, and snapshots
  `catalog-spot.json` and `status-spot.json`.

## Measured (2026-10-24, `spot live trial`, 300 slots each)

| | At the tip (460,918,593) | In the archive's range (460,411,000) |
|---|---|---|
| Blocks / transactions | 300 / 131,888 | 300 / 122,498 (the archive: 122,498) |
| Swaps / liquidity rows | 30,843 / 187 | 22,436 / 382 (the archive: 22,437 / 382) |
| Manifest signatures checked, missing | 43,863, 0 | 43,593, 0 |
| Logs cut with `Log truncated` | 97 | 72 (the archive: 0) |
| Time at 16 in flight | 16.1 s, 18.7 slots/s | 15.4 s, 19.5 slots/s |
| JSON / on the wire | 806 MB / 198 MB | 748 MB / 169 MB |
| Credits | 591 | 586 |

A first run on a scratch root (the deployed config, 24 in flight) took the first two batches,
460,512,000 to 460,529,999: 16,946 blocks, 7.3M transactions, 1,616,612 swaps and 11,877
liquidity rows in 257 and 294 s (31 to 35 slots a second), 274,735 manifest signatures checked
and none missing. It spent 18,204 credits (1.011 a slot, so 218,000 a day at the tip) and
10.7 GB on the wire for 45.5 GB of JSON (128 GB a day at the tip). `spot live check` passed,
with the seam met: the history's newest batch ends at 460,511,999.

- **Cost at the tip: about 220,000 to 240,000 credits a day**: one per block (about 216,000
  slots a day), one `getBlocks` and the manifest walks per batch (19 `getTransactionsForAddress`
  seeds at 10 and about 200 `getSignaturesForAddress` pages for a 1,000-slot window, about 400
  credits, 24 batches a day), and a `getSlot` per 30-second wait. That is an eighth of the 2M-a-day budget the
  venues lane was held to. The status reports `creditsPerDayAtTip` from the lane's own counters.
- **Transfer: about 120 to 140 GB a day on the wire** (gzip; 4.3 times less than the JSON), about
  0.55 TB a day of JSON decoded.
- **Catch-up.** About 31 to 35 slots a second at 24 in flight, so the backlog from 460,512,000
  (about 2.5 slots a second accrue) clears at about 30 slots a second net: about 9 to 10 hours
  for a million slots.
- **Rows.** On the same 300 slots the RPC route gave the archive's transactions, blocks and
  liquidity rows and one swap fewer (22,436 against 22,437). The provider's logs end with
  `Log truncated` on 72 transactions whose archive logs are whole (the archive's source keeps
  longer logs than the provider serves), and a Raydium CLMM swap whose `emit!` event was cut is
  counted `missing_event`, as ADR-0014 counts every such gap. In the scratch run 8,928 of 7.3M
  logs (0.12 %) were cut, and about 105 swaps of 1.6M (Orca, Raydium CLMM, CPMM and AMM v4,
  Meteora DAMM v1, the programs read from `emit!` logs) lost their event: about 1 in 15,000.
- Blocks at the tip carried 440 to 510 transactions in the two trials, and every block of a
  batch is read whole: the credits follow the slots, the transfer follows the transactions.

## Not done

The archive is not used for this lane's slots. Epochs it publishes later could replace the RPC
batches (exact logs, no credits), at the cost of a second source for the same slots and a
replacement step; at about 1 swap in 15,000 the gain is small, and the gaps are counted per
batch and program. The lane can be pointed at the archive later without changing its tables.

## Consequences

The spot tables are continuous from 2025-10-01 to the finalized tip, two lanes meeting at slot
460,512,000. The forward lane needs no `archive`-feature binary; it runs in the services' binary.
Account state (pool reserves, prices of the closed AMMs) still has no history.
