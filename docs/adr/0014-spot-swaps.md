# ADR-0014: Spot DEX swaps from the Old Faithful archive

Status: accepted, October 24, 2026. Reuses ADR-0008 (the archive lane) and ADR-0013 (the history
lane's ledger, walker and checks).

The markets model is data-limited, and adding thin, narrow data did not help: what helps is
breadth across markets (regimes ADR-0025, "Solana spot DEX swaps across thousands of tokens" is the
third step of its data roadmap). This lane records every pool-level trade on the Solana spot DEXs
with material volume from 2025-10-01, the start of the Phoenix dataset, to the newest epoch the
archive serves, with liquidity changes where the programs report them.

## Programs

The programs the user named, the launchpads whose curves carry most new tokens (pump.fun, Raydium
LaunchLab, Meteora DBC), and every other program with more than about 0.2M successful transactions
a day over the window, measured with `venues history estimate` (`getTransactionsForAddress`
density at twelve points) and then with trial streams. The share is of the swap rows of seven
5,000-slot trials spread over the window (4,563,502 rows).

| Program | Address | Read from | Rows | Share |
|---|---|---|---|---|
| PumpSwap | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | `emit_cpi!` `BuyEvent`, `SellEvent`, `DepositEvent`, `WithdrawEvent`, `CreatePoolEvent` | swaps, liquidity | 48.0 % |
| Meteora DAMM v2 | `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG` | `emit_cpi!` `EvtSwap`, `EvtSwap2`, `EvtLiquidityChange` (and the older add/remove events), `EvtInitializePool` | swaps, liquidity | 20.7 % |
| pump.fun bonding curves | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | `emit_cpi!` `TradeEvent` (with the 2026 quote-mint tail), `CreateEvent` | swaps, creations | 7.3 % |
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | `emit!` `SwapEvent`, position and pool events | swaps, liquidity | 7.2 % |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | `emit_cpi!` `Swap`, `Swap2Evt`, `AddLiquidity`, `RemoveLiquidity`, `LbPairCreate` | swaps, liquidity | 2.8 % |
| Orca Whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | `emit!` `Traded`, `LiquidityIncreased`, `LiquidityDecreased`, `PoolInitialized` | swaps, liquidity | 2.5 % |
| Meteora DBC | `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` | `emit_cpi!` `EvtSwap`, `EvtSwap2`, `EvtSwap3` | swaps | 2.2 % |
| Raydium AMM v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | `ray_log` lines (swap in/out, deposit, withdraw, init) | swaps, liquidity | 2.2 % |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | `emit!` `SwapEvent`, `LpChangeEvent`, `initialize` arguments | swaps, liquidity | 1.6 % |
| HumidiFi | `9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp` | token transfers | swaps | 1.1 % |
| PancakeSwap CLMM | `HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq` | token transfers | swaps | 0.9 % |
| Tessera V | `TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH` | token transfers | swaps | 0.8 % |
| AlphaQ | `ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA` | token transfers | swaps | 0.7 % |
| Raydium LaunchLab | `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj` | `emit_cpi!` `TradeEvent` | swaps | 0.6 % |
| SolFi V2 | `SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF` | token transfers | swaps | 0.6 % |
| ZeroFi | `ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY` | token transfers | swaps | 0.3 % |
| Meteora DAMM v1 | `Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB` | `emit!` `Swap`, `AddLiquidity`, `RemoveLiquidity`, `PoolCreated` | swaps, liquidity | 0.3 % |
| GoonFi | `goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j` | token transfers | swaps | 0.2 % |
| SolFi | `SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe` | token transfers | swaps | 0.1 % |

Measured and left out (successful transactions a day): the order books Phoenix spot (0.78M),
Manifest (0.48M) and OpenBook v2 (0.04M), whose transactions are mostly market makers' order
updates on a few majors and whose fills need decoders of their own; Stabble (0.13M), Byreal CLMM
(0.12M), Lifinity v2 (0.06M), FluxBeam (0.05M), Obric v2 (0.03M), Orca's legacy pools (0.02M), and
Saros DLMM, Heaven, Sanctum Infinity, Boop, Moonit and Raydium's stable pools (0.01M or less each).
Aggregators (Jupiter, Titan, OKX, DFlow and the rest) are not read at all: a route
through three pools is three rows, recorded at the pools, so nothing is counted twice.

**The proprietary AMMs and PancakeSwap emit no swap event.** Their trades are read from the token
transfers the invocation makes itself (its direct `Transfer` and `TransferChecked` CPIs to SPL
Token or Token-2022): the trader's payment into a pool account and the pool's payment out of a
pool account of the other token. Pool accounts come in two shapes: one authority owns both vaults
and signs the payment out (HumidiFi, Tessera V, SolFi, SolFi V2, GoonFi, PancakeSwap), and the
row's `pool` is that authority; or each vault is its own authority (AlphaQ, ZeroFi), and `pool` is
the lower of the two vault addresses. The two transfers also read the other way round (each side
pays into an account the other owns) and the program decides their order (HumidiFi pays out
first), so: self-owned vaults win; a reading whose pool signed the transaction is dropped (vaults
belong to program addresses, which cannot sign); a reading whose pool accounts no other transfer
of the transaction touches is preferred (a router's accounts carry the tokens in and out of the
hop); and the pool authority's position among the instruction's accounts settles the rest
(PancakeSwap's is Raydium's documented layout; for the closed programs it was the same on every
resolved trade of the trials: Tessera V 0, HumidiFi, SolFi, SolFi V2 and GoonFi 1). An invocation
that moves no tokens (a market maker's quote update, most of these programs' transactions) is not
a trade. Check: at slot 430,000,000 the median SOL/USDC price of every one of the 14 programs that
trade the pair lies between 74.10 and 74.49 USDC, with 5th and 95th percentiles within 1 % of the
others; a reading the wrong way round would invert it. These rows have no fee, reserves or price
state, and no liquidity rows.

## What a row is

`swaps`: one row per trade at a pool: `slot`, `block_time`, `tx_index` (the transaction's index in
its block), `seq` (its place among the transaction's spot rows), `ix` and `depth` (top-level
instruction and stack height: 1 is a direct call, more is a route), `program`, `pool`, `mint_a`,
`mint_b` (the pool's tokens in the program's own order: base and quote, token 0 and 1, x and y;
by key bytes for the transfer-read programs), `decimals_a`, `decimals_b`, `a_to_b`, `amount_in`,
`amount_out` (raw units), `fee` and `fee_on_input` where the program reports them, `reserve_a` and
`reserve_b` (before the trade, where reported), `sqrt_price`, `tick` (CLMM tick or DLMM bin after
the trade) and `liquidity` where reported, and `signer` (the fee payer). The trade price is
`amount_out / amount_in` scaled by the decimals; `sqrt_price` is the pool's after the trade where
the program reports it. pump.fun curve amounts are the curve's, before fees; its reserves are the
virtual reserves before the trade.

`liquidity`: one row per deposit (`add`), withdrawal (`remove`) and pool or curve creation
(`create`), with the amounts and mints where the event or instruction carries them.

No signature is stored: 64 incompressible bytes a row, and `(slot, tx_index)` gives it back from
the archive or any RPC. Mints come from the instruction's accounts, the events or the token
balances of the pool's vaults, never from the trader's accounts (a router's temporary wrapped-SOL
account has no balance entry).

## Decision

- **`solos-data spot run --config config/spot-history.json`**, its own unit
  (`ops/solos-data-spot-history.service`), its own binary built with the `archive` feature, ledger
  (`<augment root>/spot-history/checkpoint.duckdb`), file tree
  (`spot-history/tables/spot/{swaps,liquidity,batches}/ALL/<from>-<to>.parquet`),
  `catalog-spot-history.json` and `status-spot-history.json` (`Lane::Spot`). `augment query`
  reads them as `spot_swaps`, `spot_liquidity` and `spot_batches`.
- **Newest first, one batch at a time**: `chunkSlots` (50,000) × `parallel` (4) slots, from `top`
  (460,511,999, the last slot of epoch 1065, the newest epoch the archive served on 2026-10-23) to
  the slot of `floorTime` (2025-10-01T00:00Z, 370,364,680), resolved once and recorded.
- **Decoded in the stream.** Each Jetstreamer thread turns every successful transaction that names
  a program (static key or loaded through a lookup table) into rows as it arrives; only rows are
  held. Each invocation's events are found in execution order: self-CPI events belong to their
  parent, and log events (`Program data:`, `ray_log:`) to the invocation on top of the log's
  invoke stack, the k-th `invoke` line of a program being its k-th instruction. A log cut by
  `Log truncated` stops attribution; the swaps whose event was cut are counted missing.
- **Checks, before a batch is written.** (1) The archive's blocks equal the RPC's `getBlocks`
  list for the range, and every block delivered as many transactions as its entries name: every
  transaction of the range reached the filter. (2) For each program, every successful signature
  its `getSignaturesForAddress` manifest lists over one 1,000-slot window of the batch (at an
  offset that moves from batch to batch) was streamed: the filter saw every transaction naming the
  program. A full manifest, as ADR-0013 keeps for the perp venues, would be about 36 billion
  signatures here (24 billion successful); the block check already proves completeness, so the manifest is a sampled
  check of the filter. A failed check retries the batch whole after 30 s.
- **Gaps are counted, not hidden.** Per program and batch, `batches` records transactions,
  failed transactions, invocations, swap instructions, swaps, liquidity rows, swap instructions
  with no event (`missing_event`), events that did not decode (`undecoded`) and trades whose tokens
  could not be resolved (`unresolved`), and the status lists the gaps by program and instruction.
- **Files and cursor.** The batch's three files are written with ZSTD (swaps and liquidity sorted
  by pool, slot, transaction and position), fsynced, registered, and the cursor recorded with the
  last one; a restart redoes at most the batch in flight. A batch redone under another range (after
  `chunkSlots` or `parallel` changed) retires the overlapping files.
- **`solos-data spot check`** reads only the catalog's files (never the running ledger): the
  batches must tile one slot range with no hole or overlap, each batch must have streamed exactly
  the listed blocks and missed no manifest signature, and each batch's files must hold the rows it
  counted, in the catalog and on disk. Exit 2 otherwise.
- **`solos-data spot trial --from --to`** streams a range into a scratch directory and reports
  rows, decode coverage, bytes, distinct pools and tokens, time and memory; `--capture N` keeps real
  transactions in the RPC shape for the tests, re-decoded through the RPC path to prove both paths
  give the same rows.
- **Health and backup.** `solos-data health` judges `status-spot-history.json` like the venues
  history (age, stop before the floor, failed-batch streak, no batch for `backfillStallMinutes`);
  a finished run is reported, not judged. The nightly backup copies `spot-history/tables` comparing
  sizes (files are written once under their slot range) and snapshots the catalog and status.
- **No filter.** The whole set fits the budget the user set (1.5 TB of Parquet, 10 days): every
  pool and every token is kept.

## Estimates (measured 2026-10-24)

Seven 5,000-slot trials at slots 371M, 385M, 400M, 415M, 430M, 445M and 459M, and one
production-size batch (200,000 slots from 430,000,000, `chunkSlots` 50,000, `parallel` 4,
`JETSTREAMER_THREADS` 8) streamed while the venues history (ADR-0013) was using the same mirror.

| | Per slot | Window (90,147,320 slots) |
|---|---|---|
| Swap rows | 130.4 (84 to 160 by point) | about 11.8 billion |
| Liquidity rows | 0.89 | about 80 million |
| Transactions streamed (votes included) | 1,215 | about 110 billion |
| Archive bytes streamed | about 1.7 MB (ADR-0013) | about 150 TB, nothing kept |
| Parquet | 35.3 bytes a swap row in a batch file | about 420 GB |

- **Breadth.** The 200,000-slot batch (about 22 hours of chain) held 30.1 million swaps on
  105,087 pools and 82,570 tokens by 751,626 signers. pump.fun alone created 32,679 curves in it,
  so the window holds on the order of ten million tokens, most of them short-lived launches; the
  run's own counts will replace this estimate.
- **Time.** The batch streamed in 253 s and its files took 61 s: about 640 slots a second, about
  40 hours for the window. The 5,000-slot trials streamed at 100 to 570 slots a second depending on
  the epoch and the mirror's load, so 1.5 to 3 days.
- **Memory.** The batch peaked at 42 GB (rows plus a 32 GB DuckDB load); the unit is capped at
  100 GB (`MemoryMax`), with the writer's DuckDB at 32 GB (`writerMemory`) and the ledger's at
  32 GB on the command line, since `collector.env` sets the collector's.
- **RPC credits.** Per batch one `getBlocks` call, one `getTransactionsForAddress` seed per program
  (10 credits) and about 400 manifest pages for the window over all programs: about 600 credits,
  about 0.3M for the run. The archive needs none.

## Consequences

The lane ends at `top`: there is no live spot lane, so the newest days are missing until a forward
lane or a second range is added. Account state (pool reserves, prices of the closed AMMs) has no
history. The proprietary AMMs' rows have no fees, reserves or price state, their `pool` is a vault
authority or a vault (a market is `pool` with the two mints), and their liquidity is not read.
Known gaps in the trials, all counted per batch: a few hundredths of a percent of pump.fun's
2026 multi-hop curve swaps (`multi_hop_curve_swap`, unresolved curves) and PumpSwap's
`multi_hop_swap` frames whose trade is the bonding curve's (counted `missing_event`, the curve's
row is kept). The run needs the `archive`-feature binary; the live services' binary is not
replaced, so `solos-data health` judges the spot status and the backup copies its tables once the
services' checkout and binary are next updated.
