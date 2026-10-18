# ADR-0012: Other Solana perp venues and Pyth prices

Status: accepted, October 18, 2026.

The Phoenix Rise dataset describes one Solana venue; the augmentation lane (ADR-0009) adds the
large centralized and Hyperliquid perpetuals. What the other Solana perpetuals venues were doing
at the same time, and the oracle prices they and Phoenix settle against, were missing. The user
approved collecting Jupiter Perps, Zeta and Flash Trade, plus Pyth prices, on October 18, 2026.
Drift is excluded entirely, at the user's request (it was exploited).

## What each venue is today (research, 2026-10-18)

| Venue | Program | State | Events | Collected |
|---|---|---|---|---|
| Jupiter Perps | `PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu` | live: SOL, ETH, BTC against the JLP pool | Anchor `emit_cpi!`: self-CPI with `EVENT_IX_TAG` `e445a52e51cb9a1d` | transactions, events, instructions, pool and custody state |
| Flash Trade | `FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn` | trading moved to a MagicBlock ephemeral rollup on 2026-06-27/29 (v2); wind-down announced 2026-08-07 | `emit_cpi!` (on the rollup since v2) | its remaining L1 transactions (settlement, liquidity, delegation), pool, market and custody accounts |
| Zeta Markets | `ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD` | ceased operating in May 2025; Bullet, its successor, is a gated network extension | `emit!` (logs) | nothing: no activity in the period the dataset covers |
| Pyth | Receiver `rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`, push oracle `pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT` | live | none (account writes) | `PriceUpdateV2` of the shard-0 price feed accounts of the Phoenix markets |

Sources: Jupiter developer docs (developers.jup.ag/docs/perps, which point to
github.com/julianfssen/jupiter-perps-anchor-idl-parsing for the IDL and the pool and custody
addresses); Flash Trade's SDKs (github.com/flash-trade/flash-trade-sdk, v1 IDL 15.2.0 and
`PoolConfig.json`; github.com/flash-trade/flash-sdk-v2, IDL 16.2.0, whose README describes the
rollup execution) and docs.flash.trade; docs.zeta.markets ("Zeta Markets has ceased operating as of
May 2025") and docs.bullet.xyz; pyth-crosschain (`pyth_solana_receiver_sdk/src/price_update.rs`,
`pythnet_sdk/src/messages.rs`, `pyth-push-oracle/src/lib.rs`) and Pyth's sponsored Solana feed list
(pyth-network/documentation `push-feeds/data/svm/solana_mainnet.json`, docs.pyth.network).

**IDLs come from the chain.** The GitHub copies lag the deployed programs: on the first run the
Jupiter IDL from GitHub left three event types with trailing bytes (`ClosePositionRequestEvent` 8,
`InstantDecreasePositionEvent` 1, `RepayToCustodyEvent` 16) and five log payloads unnamed. The
committed IDLs (`config/idl/`) are the programs' own Anchor IDL accounts, read with
`solos-data venues idl --address <account> --out <file>` (one `getAccountInfo`, zlib-inflated);
the account is `createWithSeed(findProgramAddress([], program), "anchor:idl", program)`: Jupiter
`38GK1i4cQPAxqrbfKX4RRMNfXpKRn5PgLFHVJeXm1C8Y` (25 events, 8 accounts, 59 instructions at slot
458,478,391), Flash `dK94tgJUbS8ZotN2ELzCmRvWEfpD2REMHRQneK1ngSd` (IDL 16.3.0). The source and slot
are recorded in `config/venues.json` and in the status.

## Decision

- **One process, its own everything.** `solos-data venues run --config config/venues.json` is a
  long-running service (`ops/solos-data-venues.service`) with its own ledger
  (`<augment root>/venues/checkpoint.duckdb`), catalog (`catalog-venues.json`), status
  (`status-venues.json`), staging directory and RPC limiter. It shares nothing with the Phoenix
  collector and decoder: no checkpoint, no cursor, no compute-unit bucket. It uses the same
  provider account (`SOLANA_RPC_URL` from `collector.env`) at `requestsPerSecond` (20) through a
  limiter of its own, the collector's provider code with a uniform weight per request.
- **Under the augment root.** The files are `tables/<source>/<dataset>/ALL/<YYYY-MM-DD>T<HH>.parquet`
  (sources `jupiter`, `flash`, `pyth`), registered in the venues ledger, which is a third lane of
  the augment ledger (`Lane::Venues`). `augment query` and `augment status` read all three lanes,
  so the views are `jupiter_events`, `jupiter_instructions`, `jupiter_transactions`,
  `jupiter_accounts`, `flash_*`, `pyth_prices`.
- **Transactions forward, with a manifest.** Per venue, each cycle (`cycleSeconds`, 30) takes the
  finalized slot window from the cursor to the finalized slot (at most `maxSlotsPerCycle`, 2,000).
  The manifest is the program's successful signatures from `getSignaturesForAddress`, walked
  newest first past the window start; an empty page before the start is an error (the dead cursor
  of ADR-0008), not the end. The transactions come from `getTransactionsForAddress` (slot filter,
  `status: succeeded`, full, base64, 100 a page); a manifest signature the bulk pages missed is
  fetched alone with `getTransaction`, and a window with a signature neither path serves is
  retried whole. A bulk transaction the manifest does not list is kept and counted (`unlisted`).
  Failed transactions are counted (`failedSkipped`), not stored. All reads are finalized.
- **What a transaction becomes.** `transactions` (signature, slot, block time, the base64 wire,
  the provider's exact `meta` text), `instructions` (every instruction of the program, top level
  and CPI, with its stack path, discriminator, IDL name, data and resolved accounts) and `events`
  (every Anchor event: self-CPI payloads and `Program data:` lines attributed to the program by
  the invoke stack; ordinal, discriminator, IDL name, the payload's bytes, the decoded `fields`
  as JSON, or the reason it did not decode). Decoding is a Borsh reader driven by the IDL (both
  Anchor dialects); integers wider than 32 bits are decimal strings, keys base58, byte arrays hex.
  An event must consume its payload exactly, so a layout change shows up as `decode_error`
  ("N trailing bytes") with the bytes kept for a later decoder; an account may carry spare bytes.
- **State on a cadence.** `accounts` rows: the configured accounts (Jupiter's pool and six
  custodies, Flash's eight pools) and the account types listed in `discover` (Jupiter `Custody`,
  Flash `Market` and `Custody`, through `getProgramAccounts` with a discriminator filter and no
  data, hourly), read with `getMultipleAccounts` every `accountsIntervalSeconds` (60). A row is
  written when the bytes changed since the previous poll and at least once an hour per account
  (`changed` says which), with the raw bytes and the IDL-decoded fields. Jupiter's pool `aumUsd`,
  custody `assets` (owned, locked, guaranteed USD, global short sizes and prices) and funding
  state (`cumulativeInterestRate`, `hourlyFundingDbps`) live only here: the events carry deltas.
- **Pyth.** The `PriceUpdateV2` accounts of the Pyth push oracle's shard 0 for the Phoenix
  markets, read together every `intervalSeconds` (5). A row is written when a feed's
  `publish_time` moves; the account's owner must be the receiver and its feed id the configured
  one, or it is counted as `mismatched` and skipped. An update older than `maxAgeSeconds` (a week)
  is counted as stale and not stored, so an abandoned feed does not write rows into hours long
  past. The layout is decoded by hand (it is short and fixed: the `verification_level` enum is one
  byte longer for `Partial`). Twelve feeds are Pyth-sponsored (kept fresh at a 60 s heartbeat or
  0.5 % deviation, so a few rows a minute per feed); the others exist only while someone pushes
  them, and `pyth.feedAgeSeconds` in the status shows each feed's age (null: no account). The
  Pyth price of `kBONK` is BONK/USD per BONK; Phoenix's `k` markets are per thousand.
- **Files.** Rows are buffered with the cursor they advance and flushed every `flushSeconds`
  (300) into the hour's file, merged on the dataset's key (`signature`; `signature, ordinal`;
  `signature, path`; `at_ms, account`; `feed_id, publish_time`) with what the hour already
  holds; a file is replaced by an atomic rename and never deleted, so a reader of an older catalog
  never meets a missing file. The cursors are recorded after every file they cover is registered;
  a crash in between refetches the window and the merge drops the duplicates. A first run
  records its starting slot before its first cycle. Polled rows (accounts, Pyth) have no window to
  refetch: SIGTERM flushes them, a crash loses at most `flushSeconds` of them (the trade-off the
  Hyperliquid asset contexts made in ADR-0009), and the account rows resume with the next poll. An hour is
  `complete` in the catalog once it ended an hour before the write.
- **Health and backup.** `solos-data health` judges `status-venues.json` when it exists: its age
  (15 min), each venue's slot lag behind the finalized slot (warn 1,500 slots, about ten minutes;
  critical 9,000), its run of failed cycles (the collector's streak thresholds) and the Pyth poll.
  The nightly backup copies the venues tables with the rest of the augment root and snapshots
  `catalog-venues.json` and `status-venues.json` with the other catalogs: this is our own
  collection of public chain data, with no licence restriction.

## History

Forward collection starts at the first run's finalized slot. History is a separate step. Two
ways, measured against the forward costs below:

1. **RPC, `getTransactionsForAddress` by slot range** (the forward cycle run backward over closed
   windows, a `venues backfill --from <slot> --to <slot>` command). Helius bills 10 credits per
   page of up to 100 full transactions and 1 credit per 1,000 manifest signatures, so a
   transaction costs about 0.1 credit. Nothing new to build except the command; it reuses the
   cycle, so the manifest check and the files are the same.
2. **The Old Faithful archive through Jetstreamer** (ADR-0008), filtered on the venue programs.
   Every epoch is downloaded whole (about 735 GB for epoch 1048) whatever the filter, so the cost
   is the download and the replay, not the venue's volume: about 145 epochs from 2026-01-01, on
   the order of 100 TB through the mirror, while the Phoenix archive lane is downloading the same
   epochs for its own backfill. Sharing that stream (one replay, several program filters) would
   cost nothing extra but changes the Phoenix collector, which this ADR leaves alone.

**Estimate and recommendation.** The pre-merge runs (2026-10-18, ten minutes each) measured about
37,000 successful Jupiter Perps transactions a day and about 0.5 MB of hour files per ten minutes
(about 70 MB a day). From 2026-01-01 that is roughly 290 days and 11M transactions: about 110,000
bulk pages and 11,000 manifest pages, so about 1.1M Helius credits (half a day of the 2M-a-day
budget; early 2026 volume was higher, so plan for up to 3M), a few hours at 20 requests a second,
and 20 to 40 GB of Parquet. Flash Trade's v1 history to 2026-06-27 is smaller. The archive route
costs about 100 TB of transfer for the same rows. History therefore goes through RPC, as a
separate `venues backfill` command run in budgeted slices (windows walked newest first, each one
the forward cycle), unless an independent source is required; the archive replay stays the
fallback and the cross-check.

Pyth price history is not in transactions in a usable form: the receiver and push oracle emit no
events, and the posted updates are Wormhole Merkle proofs inside instruction data. Hermes
historical updates and Benchmarks need an API key since 2026-08-26 (`/v2/updates/price/<t>` and
`benchmarks.pyth.network` answered 401 on 2026-10-18; `/v2/price_feeds` still answers without a
key).

## Consequences

Flash Trade's trades since its v2 launch are on the MagicBlock rollup, not on Solana: this lane
sees only its L1 settlement, liquidity and delegation transactions, and its pool, market and
custody accounts while they are not delegated (a delegated account's L1 copy is owned by the
delegation program and refreshed on commits). Its v1 trade history (to about 2026-06-27) is on
L1 and in the archive. Zeta has nothing in the dataset's period. Jupiter's oracle is its own
(Doves, `DoVEsk76QybCEHQGzkvYPWLQu9gzNoZZZt3TPiL597e`), not Pyth; the custody accounts name the
oracle accounts, which are not collected yet.
