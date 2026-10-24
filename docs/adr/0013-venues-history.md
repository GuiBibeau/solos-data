# ADR-0013: Venues history from the Old Faithful archive

Status: accepted, October 23, 2026. Extends ADR-0012 (the venues lane), reuses ADR-0008 (the
archive lane).

The venues lane (ADR-0012) collects Jupiter Perps, Flash Trade and Pyth from 2026-10-18 15:55 UTC
forward. A data-scaling study of the markets model showed it is data-limited (skill still rises
with more data on 5 of 6 targets), so the user asked for these sources back to 2025-10-01, the
start of the Phoenix dataset, at the granularity the event-level model uses (fills and trades
with side, size, price, trader, liquidation flags; funding, borrow and pool state).

## Sources and what they give

| Source | Address(es) walked | Decode | Kept |
|---|---|---|---|
| Jupiter Perps | program `PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu` | Anchor `emit_cpi!` self-CPI events and every program instruction, through the on-chain IDL with the GitHub IDL of 2025-08-19 as fallback | `transactions`, `events` (`IncreasePositionEvent`, `DecreasePositionEvent`, `InstantIncrease/DecreasePositionEvent`, `LiquidateFullPositionEvent`, `PoolSwapEvent`, `Add/RemoveLiquidityEvent`, TP/SL and request events, ...), `instructions` |
| Flash Trade | program `FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn` | the same; eleven IDL versions of flash-trade-sdk (2025-09-01 to 2026-05-05) behind the on-chain 16.3.0 | the same (`SwapAndOpenLogUSDv1`, `CloseAndSwapLogUSDv1`, `...PositionLog`, liquidation and staking logs, ...) |
| Pyth | the 39 shard-0 price feed accounts of `config/venues.json` | push oracle `update_price_feed` instructions: the signed Pythnet `PriceFeedMessage` (big-endian, 85 bytes) in the instruction data, counted as applied when the oracle CPIs the receiver | `prices`, one row per update applied to a configured account, the same columns as the live poll plus `signature` and `shard` |

Every program upgrade inside the window is handled by the IDL fallbacks: an event the newest IDL
does not decode exactly is tried against each older one, newest first
(`config/venues-history.json` `idlFallbacks`, files under `config/idl/history/`). Flash Trade's
v1 IDLs before 2026-02 exist only as TypeScript in its SDK; they were converted to JSON as
published (`export const IDL`). On a 100,000-slot sample of 2026-03-31/04-01 every Flash and
Jupiter event decoded (0 undecoded, 0 unknown); an event no IDL decodes keeps its bytes and its
`decode_error`, as in the live lane.

Pool, custody and market **account state** has no history: neither the RPC nor the archive keeps
past account data (the archive has no account updates), so Jupiter's `aumUsd`, custody assets and
funding accumulators exist only from the live lane's start. The events carry what each trade
changed (prices, sizes, fees, PnL), and instruction accounts name the custodies involved.

Flash Trade moved trading to a MagicBlock rollup on 2026-06-27/29 (ADR-0012); its L1 history after
that is settlement and liquidity only. Before it, Flash v1 had about as many successful L1
transactions as Jupiter.

## Pyth: on chain, not Benchmarks or Hermes

Pyth's site Terms of Use (updated 2025-08-07, pyth.network/legal/terms-conditions) grant use of
the site and its content "solely for your own private, non-commercial informational purposes"
and expressly grant no licence to "price feed data (including asset prices, confidence intervals,
or related metadata)"; they forbid automated extraction beyond what a human could do. Hermes
historical updates and Benchmarks need an API key since 2026-08-26 (ADR-0012). Training a
commercial model on Benchmarks or Hermes history is therefore not covered by any terms we hold.
The history is read from Solana instead: public chain data, collected like the rest of the
dataset. Whether Pyth claims rights in prices read off a public chain is a question for the user's
counsel; the dataset records the source of every row (`signature`), so the rows can be dropped if
needed.

Granularity: the sponsored shard-0 accounts are updated about once a minute (the 60 s heartbeat
or a 0.5 % move); trial 1 found one applied update per feed every 53 s on average. Most
transactions that name a feed account only read it (trial 2: 125,555 transactions named the
accounts and carried 17,739 updates), which is why the RPC route below costs so much for Pyth.

## Decision

- **`solos-data venues history run --config config/venues-history.json`**, its own unit
  (`ops/solos-data-venues-history.service`), ledger (`<augment root>/venues-history/checkpoint.duckdb`),
  file tree (`venues-history/tables/<source>/<dataset>/ALL/<hour>.parquet`, so the live lane's
  recovery never sees these files), `catalog-venues-history.json` and `status-venues-history.json`
  (`Lane::History`). `augment query` reads every lane's catalog, so `jupiter_events`,
  `flash_transactions`, `pyth_prices` and the rest span both lanes; a `<source>_batches` view
  holds the history's own counts.
- **Newest first, one range at a time.** The run starts at each source's `top`, the slot before
  the live lane's first window (Jupiter 458,492,106; Flash and Pyth 458,492,107), and walks down to
  the slot of `floorTime` (2025-10-01T00:00Z, slot 370,364,680), resolved once from block times and
  recorded. Recent months are complete first.
- **Transactions from the archive** (ADR-0008's Jetstreamer machinery, the `archive` feature).
  Every archive source shares one stream per range: chunks of `chunkSlots` (50,000), `parallel`
  (4) in flight, watching all addresses of all sources at once, so each epoch is downloaded once.
  Since 2026-10-24 a batch is one Jetstreamer call with `parallel` times the threads, with bounded
  retries and a stall watchdog (ADR-0014, "Stalls").
  A streamed transaction goes to every source whose addresses it names (static keys or loaded
  through a lookup table); failed ones are dropped. `via: rpc` per source switches it to
  `getTransactionsForAddress` bulk pages per `windowSlots` window instead (the forward cycle run
  backward; tested, not used).
- **Manifest from the RPC.** Each address keeps one continuous `getSignaturesForAddress` walk,
  newest first, cut into the batch's slot stretch (the walker of ADR-0008 with its fallback
  cursor for empty pages; a venue program's walk that ends above the floor is an error, a Pyth
  account's may end early). A first walk starts just above `top` from one Helius
  `getTransactionsForAddress` signatures call, not at the tip. Every listed successful signature
  must be in the stream or be served by `getTransaction`, or the whole batch is retried;
  streamed transactions the manifest does not list are kept and counted (`unlisted`).
- **Files and cursors** as in the live lane: rows buffered, merged into hour files, registered,
  then the cursor (next slot down and each walk's cursor) is recorded. One `batches` row per
  source and batch records the manifest, what was fetched and what was stored.
- **Check.** `solos-data venues history check` reads only the catalog's Parquet (never the
  running ledger): per source, the batches must tile one slot range with no hole or overlap; for a
  venue, the distinct transactions stored in a batch's slots must equal the batch's count, which
  must equal its manifest plus `unlisted`; for Pyth, the price rows of a batch's slots must equal
  the batch's count. Exit 2 on any mismatch.
- **Health.** `solos-data health` reads `status-venues-history.json` when present: the file's age
  and a stop before the floor warn, each unfinished source's failed-batch streak follows the error
  streak thresholds, and no batch for `historyStallWarnMinutes` (60) warns and for
  `historyStallCriticalMinutes` (180) is critical (until 2026-10-24: `backfillStallMinutes`, 120,
  warn only). A finished backfill is reported, not judged. The nightly backup copies the tables
  and snapshots the catalog and status.

## Estimates (measured 2026-10-23)

`venues history estimate` sampled each address at four points of the window
(`getTransactionsForAddress` in `signatures` mode); two trial runs on scratch roots measured the
rest.

| | Jupiter | Flash | Pyth (39 accounts) |
|---|---|---|---|
| Signatures naming the address(es) | about 60M (46 % failed) | about 27M | about 365M summed over accounts |
| Successful transactions | about 33M | about 27M (most before 2026-06-27) | the stream keeps the distinct ones; trial 2: 125,555 per 100,000 slots |
| Rows | trial 2: 76,615 per 100,000 slots | 67,050 | 16,057 applied updates per 100,000 slots, about 13M in all |

- **RPC route, rejected for Pyth:** bulk pages cost 10 credits per 100 transactions; Jupiter and
  Flash would cost about 6M credits, Pyth about 34M (its accounts are read by far more transactions
  than update them).
- **Archive route, chosen:** credits only for the manifests (trial 2: about 1,000 per 100,000
  slots, about 1M for the window). The stream downloads every block of the range whatever the
  filter: about 1.7 MB a slot, about 150 TB for 88M slots through the mirror, which the Phoenix
  archive backfill finished using on 2026-10-20. Trial 2 streamed 100,000 slots in 160 s with two
  8-thread streams, so the window is about 39 hours at `parallel` 2 and about 20 to 25 hours at 4
  if the mirror keeps up.
- **Disk:** trial 2 wrote 60 MB of Parquet per 100,000 slots (Jupiter 36, Flash 22, Pyth 1.7), so
  about 50 GB for the window; the ledger is small. **Memory:** a batch holds its ranges' matching
  transactions (a few GB); the unit is capped at 100 GB (`MemoryMax`) with DuckDB at 32 GB and
  3,200 % CPU.

## Consequences

The live lane and the history meet at the live lane's first slot: no slot is collected twice
for transactions; for Pyth, the live lane's first poll of each feed may repeat the last update
the history stored (same `feed_id` and `publish_time`). The history needs the `archive`-feature
binary; the live services' binary is not replaced. Account-state history before 2026-10-18 stays
missing for all three sources.
