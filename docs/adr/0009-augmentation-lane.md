# ADR-0009: An augmentation lane for free exogenous market data

Status: accepted, October 9, 2026.

The Phoenix Rise dataset describes one venue. A model of that venue needs what the rest of the
market was doing at the same time: where the perpetuals on the large venues traded, what funding
and open interest looked like elsewhere, implied volatility, the stablecoin supply, and what was
being said. The user chose to collect the free sources on October 9, 2026, store them as Parquet
next to the raw and decoded roots, and keep them current from the same binary.

## Decision

- **Two commands, one module.** `solos-data augment sync` backfills and catches up dated files and
  paged histories, then exits; a systemd timer runs it hourly and never overlaps two runs.
  `solos-data augment capture` is a long-running service for the streams that cannot be fetched
  after the fact. Both read `config/augment.json`; the data root is `dataDir` or
  `SOLOS_DATA_AUGMENT_DIR`, on the box `~/.local/share/solos-data/augment`. The collector and the
  decoder are untouched: no shared checkpoint, no shared process.
- **Layout.** `tables/<source>/<dataset>/<symbol>/<YYYY-MM-DD or YYYY-MM>.parquet`, one file per
  source file or per period of a paged history, ZSTD, written by DuckDB `COPY`. The venue's
  symbol stays as the venue spells it (`1000BONKUSDT`, `xyz:NVDA`; a `:` becomes `_` in the
  directory name only) and every file carries `symbol` and `phoenix_symbol` columns. Timestamps
  keep the source's epoch milliseconds and gain a `ts TIMESTAMP` column. Downloaded zips, CSV and
  JSON pages live in `staging/` only until converted.
- **Ledger.** A small `checkpoint.duckdb` registers every file (path, source, dataset, symbol,
  period, complete, row count, bytes, SHA-256) and holds per-series progress. A file is durable
  (fsync, rename, fsync of the directory, rows counted back) before the transaction that
  registers it. A run starts by deleting what the ledger does not know. `catalog.json` is the
  `files` table, rewritten every minute while a run lands files and at the end; `status.json` is
  the last run's outcome per source and the per-dataset totals. `augment query --sql` opens the
  catalogued files in an independent in-memory DuckDB with one view per `<source>_<dataset>`.
- **Periods.** A paged history is cut into UTC days (1-minute series) or months (hourly and
  slower). A period is closed once `now` is past its end plus the source's publication lag; a
  closed period is fetched once and its label recorded as `completeThrough`; the open period is
  refetched whole and its file replaced on every run. A closed period that returns no rows (a
  market listed after the start date) advances the progress without a file.
- **Symbol map.** Generated once from the Phoenix markets list, the Hyperliquid `meta` of the main
  and HIP-3 dexes (`xyz` preferred, then `flx`, `cash`, `km`, `mkts`, `para`, `vntl`), the Binance
  bucket listing, the Bybit instruments list and the Elfa entity sample, then committed. Of the 94
  markets: 48 have a Binance USD-M perpetual (`kBONK`, `kPEPE`, `kSHIB` are `1000…USDT`; ANSEM,
  STONK and BP have none), 87 a Hyperliquid coin (none for ANSEM, SPY, QQQ, STONK, RAY, OPEN, BP;
  `WTIOIL` is `xyz:CL`, the dexes' oil oracles differ and this is the `xyz` one), 50 a Bybit
  perpetual (`PUMP` is `PUMPFUNUSDT`, `RAY` `RAYDIUMUSDT`, `kSHIB` `SHIB1000USDT`, `GOLD`
  `XAUTUSDT`), all 94 an Elfa entity id. Tests never fetch the map.
- **Politeness.** One HTTP client spaces requests per host at `requestsPerSecond` (2; Hyperliquid
  has its own, 1, because its info API weighs most requests 20 against 1,200 a minute per
  address and answered 429 at two a second), retries 429, 5xx and transport errors six times
  with exponential backoff from one second, honours `Retry-After`, and never logs a URL. Sources
  run concurrently within a run; each is bound by its host's spacing.
- **Start.** Everything is stored from 2026-01-01, before the first Phoenix slot the collector
  reaches.

## Sources

Phase 1, `augment sync`:

| Source | Dataset | Cadence | Files | Notes |
|---|---|---|---|---|
| Binance USD-M dumps | `fundingRate` | 8 h | month | `monthly/fundingRate`, back to 2020 |
| Binance USD-M dumps | `klines`, `premiumIndexKlines`, `markPriceKlines`, `indexPriceKlines` | 1 min | day | `daily/<dataset>/<SYMBOL>/1m` |
| Binance USD-M dumps | `metrics` | 5 min | day | open interest, long/short ratios, taker volume ratio |
| Hyperliquid | `funding` | 1 h | month | `fundingHistory`, full history, 500 rows per page |
| Deribit | `dvol_60s`, `dvol_3600s` | 1 min, 1 h | day, month | `get_volatility_index_data` for BTC and ETH, paged through `continuation` |
| DefiLlama | `stablecoins` | 1 day | month | `stablecoincharts/all` total and per coin (USDT, USDC, USDS, USDe, DAI, USD1, PYUSD, RLUSD) |

Binance files are listed through the bucket's S3 index starting after the newest registered
file, so an hourly run costs one listing per dataset and symbol plus the new day's files. Each
zip is compared with its published `.CHECKSUM` (SHA-256) before conversion; the zip and the
checksum are requested together, since each is an origin round trip of about 0.7 s from the box
and the lane is otherwise bound by latency rather than by its request spacing. Older dumps
without a header line are detected by their first byte. A (dataset, symbol) walk stops at its
first failed file so the next run's listing marker never moves past it.

Phase 2, `augment capture`, a second process with its own ledger (`capture/checkpoint.duckdb`,
`catalog-capture.json`, `status-capture.json`, `staging-capture/`) writing distinct datasets into
the same `tables/` tree; each lane's recovery only touches the dataset directories its own ledger
knows:

| Source | Dataset | Cadence | Files | Notes |
|---|---|---|---|---|
| Hyperliquid | `candles_1m` | every 30 min | day per coin | `candleSnapshot` serves about 5,000 candles; closed candles since the last stored open time are merged into the day's file on `open_time_ms` |
| Hyperliquid | `asset_contexts` | every 1 min | day, all coins | `metaAndAssetCtxs` per dex (main, `xyz`, `flx`, …): funding, open interest, mark, oracle and mid prices, premium, day volumes; buffered in memory and merged every ten minutes on `(at_ms, symbol)`, so a crash loses at most ten minutes |
| Elfa v3 | `events`, `calls`, `episodes`, `call_book` | every 1 h | day | from the last `to` with ascending cursors, 30 (bars: 200) per page, at most 200 pages a cycle; rows flattened with a `raw_json` column; episodes keep their newest observation |

Bybit funding history (`/v5/market/funding/history`, 200 rows per page, newest first) is a paged
history like Hyperliquid's and runs in `augment sync` as `bybit/funding`, month files.

A day's file grows by a merge: the new rows and the existing file are unioned and deduplicated on
the key with the new row winning, then written through the same durable path. Merged files are
registered with `complete=false` until the day has passed.

Phase 3, opt-in and bounded by `diskBudgetGb` (0, the default, keeps every large dataset off):
Binance `aggTrades` and `bookDepth` daily zips through `largeDatasets`, and Bybit tick trades
(`public.bybit.com/trading/<SYMBOL>/<SYMBOL><YYYY-MM-DD>.csv.gz`, one day per file, read as
gzip CSV by DuckDB) through `trades: true`. Before each large file the lane sums the bytes its
ledger has registered; past the budget it logs `augment_budget_reached` and skips the rest of the
large datasets for that run, so a budget is a ceiling on the root, not a pace. Bybit publishes no
machine-readable listing; the lane walks the days from its progress record to the day before
yesterday and treats a 404 older than three days as "no trades that day" (the market was listed
later) and a newer 404 as "not published yet".

## What is deliberately not fetched

Pyth Benchmarks and Hermes answer 401 without a key. The Hyperliquid S3 archive is
requester-pays. Tardis and Yellowstone are paid. Binance `bookTicker` dumps stopped in April
2024. Elfa is free today and undocumented; the capture lane reads `credits.used` before and after
every cycle and disables itself for the rest of the process if it moves, logging
`elfa_billing_started`. The lane is skipped when `ELFA_API_KEY` is unset.

## Consequences

The first Binance backfill is about 70,000 zips plus their checksums at two requests a second,
about twenty hours of one run; the timer only starts the next run an hour after the previous one
ended, so a long backfill and the hourly catch-up are the same code path. Deribit's 60-second
history reaches back as far as its `continuation` chain goes and no further; the hourly series
covers the whole year. DefiLlama returns its entire history on every call and is cut into months
locally. A rerun never re-downloads a closed period; deleting the ledger re-downloads everything.
