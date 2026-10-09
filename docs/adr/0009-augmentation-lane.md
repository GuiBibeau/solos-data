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

## Exogenous triggers and alerts (2026-10-09)

The venue model needs the events that move its markets from outside the order book: an
issuer's filing, a macro print the prediction markets had already priced, a shift in crowd
sentiment, a liquidation cascade elsewhere. The user chose to add them on October 9, 2026 as
more sources of the same two lanes, and to record the moments they become known, since most of
them cannot be fetched after the fact.

### Sync lane

| Source | Dataset | Cadence | Files | Notes |
|---|---|---|---|---|
| SEC EDGAR | `sec/filings` | per filing | month per issuer | `data.sec.gov/submissions/CIK##########.json` plus the `filings.files` continuations that reach the start date; rows carry the acceptance instant (`filed_at`), form, items, accession, primary document and its URL |
| Phoenix | `phoenix/earnings_dates` | per sync | day, all markets | `metadata.earningsDates` of `perp-api.phoenix.trade/v1/view/exchange/markets`, merged on (symbol, date) so a moved date shows up as a new row |
| alternative.me | `alternative/fear_greed` | 1 day | year | the whole index since 2018-02-01 from one call; the series overrides the lane's start date |
| Polymarket | `polymarket/markets`, `polymarket/prices` | per sync, 1 h | day; month per market | the curated events in the config are expanded through Gamma (`/events/<id>`) into markets and outcome tokens; each token's CLOB `prices-history` at `fidelity=60` from the period's start (`startTs`; the API refuses a long `startTs`–`endTs` window) |
| Kalshi | `kalshi/markets`, `kalshi/candles_1h` | per sync, 1 h | day; month per market | the curated series in the config are expanded through `/markets?series_ticker=…&min_close_ts=<start>` (both lanes of status, paged by cursor); hourly `candlesticks` per market with bid, ask and trade OHLC, volume and open interest. The public read endpoints need no key |

The symbol map gained `secCik`: 34 of the 40 equity perps have an EDGAR issuer (TSM, ASML,
BABA, NBIS and ARM file as foreign private issuers, 6-K and 20-F). SPY and QQQ are funds
without issuer filings; SK hynix (SKHY) is not an SEC registrant; SpaceX (SPCX), Cerebras
(CBRS) and Sandisk (SNDK) are left unmapped until `augment sec-map` confirms their keys. A wrong
key cannot store another issuer's filings: the lane compares the issuer's `tickers` with the
symbol and refuses the series when they differ.

EDGAR's fair-access policy asks every automated client to declare itself in the User-Agent and
answers `403 Undeclared Automated Tool` otherwise; the lane sends `sources.sec.userAgent`
(`solos-data/1.0 (+https://github.com/GuiBibeau/solos-data)` by default), `Accept-Encoding:
gzip`, inflates the body itself and keeps to four requests a second against the policy's ten.
What the declaration must contain is the operator's decision and lives in the config, not in
the code. `augment sec-map` reads `company_tickers.json` with the same header and reports or
writes (`--write`) the CIK of every equity symbol.

Prediction markets are curated, not discovered: `polymarket.events` lists Gamma event ids
(Fed decisions for the next three meetings, the September CPI prints, US recession by 2026 and
2027, the yearly and monthly BTC, ETH and SOL price ladders, a national Bitcoin reserve) and
`kalshi.series` lists series tickers (`KXFED`, `KXFEDDECISION`, `KXCPI`, `KXCPIYOY`,
`KXRECSSNBER`, `KXU3`). Monthly events expire; adding the next month's is a config change. A
closed market whose last month is complete in the ledger is skipped (`skipped` in the status),
so the hourly run only refetches the open month of live markets.

### Capture lane

| Source | Dataset | Cadence | Files | Notes |
|---|---|---|---|---|
| Elfa v3 | `events` | every 60 s | day | incremental from the last `to` with `order=asc`, at most two pages (sixty events) a poll, so the poll costs at most two of the key's sixty requests a minute; every row of every Elfa stream now carries `received_at_ms`, the instant the lane received it, so publication latency against `first_seen_at` can be measured |
| Elfa v3 | `calls`, `episodes`, `call_book` | every 1 h | day | unchanged, under the credit guard |
| Elfa Auto | `auto_events` | as they fire | day | the notifications of the account's alerts from the server-sent event stream `GET /v2/auto/queries/stream`, one row per frame with `received_at_ms`, the outbox event id, query id and title, status, title, body, execution id, trigger time and the raw payload |

The alerts are seven definitions in `elfa.auto.alerts`, `notify` action only (free to receive;
no webhook, Telegram or LLM step): liquidation `total_usd_5m crosses_above` on
`BTC:HYPERLIQUID` (2,000,000 USD), `ETH:HYPERLIQUID` (1,000,000) and `SOL:HYPERLIQUID`
(500,000) with a fifteen-minute cooldown, funding `annualized_rate` on `SOL:HYPERLIQUID` and
`BTC:HYPERLIQUID` crossing above 50 % or below −30 % (one alert per symbol, `OR`), and two
`news.semantic` alerts at confidence 80 for a Solana outage, halt or major exploit and for a
Phoenix perps incident, exploit or delisting, each split into atomic claims under `OR`. The
thresholds are defaults, not calibrated from Phoenix's own liquidations: Phoenix's flow is an
order of magnitude below Hyperliquid's and the alert watches Hyperliquid. All expire in
`720h`, the API's ceiling.

On start and then hourly the lane lists the account's queries (one credit), creates every
missing title after a free `validate`, recreates any alert expiring within 48 hours and
cancels the old one, and records what it holds in the ledger (`elfa/auto/queries`) with the
month's measured spend (`elfa/auto/spend`). Creation costs five credits ("baseline") and is
the only paid call besides the listing; `credits.used` is read before and after each
reconciliation under a billing lock shared with the hourly v3 cycle, so the v3 credit guard
never sees the Auto lane's spend as billing of the free reads. `creditBudgetPerMonth` (60) and
`maxAlerts` (8) stop creation when reached (`elfa_auto_budget_reached`). The stream is
reopened with exponential backoff; `410` means the account has no active query and the lane
waits five minutes; a frame-less minute (keep-alives count) reopens it. Logs:
`elfa_auto_created` (with the answer's `x-elfa-credits`), `elfa_auto_renewed`,
`elfa_auto_fired`, `elfa_auto_credits` (the measured delta per reconciliation).

### What cannot be fetched after the fact

Polymarket's CLOB serves hourly history for the life of a token, Kalshi's candlesticks for the
life of a market, EDGAR and alternative.me their whole archives; those backfill. Elfa's alert
firings, cross-venue liquidation cascades (Elfa publishes decaying trailing-window snapshots,
no archive) and raw mentions beyond the key's thirty-day `historyFrom` exist only while they
happen; the capture lane records them as they arrive, with the instant of receipt.

## Production validation (2026-10-09)

Deployed on the box from `main` at 7139fec (PR #15) at 04:18 UTC into
`~/.local/share/solos-data/augment`, next to the raw and decoded roots; the collector and decoder
services were not touched and kept publishing throughout (active since October 7 and 8). The
quick sources were run first in the foreground, one `--source` at a time, then the timer was
enabled and started the full run at 04:46:13 UTC. The capture lane (PR #16, 1067673) started at
05:03:52 UTC.

### Sync lane

| Source | Files | Rows | Duration | Notes |
|---|---|---|---|---|
| DefiLlama `stablecoins` | 90 (9 series, 10 months) | 2,538 | 4 s | daily points 2026-01-01 to 2026-10-09; total 311.6 B USD on the last point |
| Deribit `dvol_60s` | 374 | 535,021 | 478 s (both resolutions) | the 60-second history ends at 2026-04-06 04:17 UTC, 187 days back; 1,440 bars per full day, no gaps inside a day |
| Deribit `dvol_3600s` | 20 | 13,498 | (same run) | 6,749 hourly bars per currency from 2026-01-01 00:00 to the run |
| Hyperliquid `funding` | 778 (87 coins) | 511,302 | 1,057 s | zero duplicates on `(symbol, time_ms)`; 24 coins start after January 1 at their listing (`PONS` 2026-08-31, `xyz:MRNA` 2026-08-19, `xyz:SKHY` 2026-07-09, …) |
| Binance `fundingRate` | 406 (47 symbols, 9 months) | 53,734 | 2 min | 93 rows a month at eight-hour funding; `HYPEUSDT` four-hour until the market ended in April; `RAYUSDT` has no monthly funding dump after 2025-06 although its daily dumps continue, so its funding comes from Hyperliquid and Bybit only |
| Binance `klines` and the four other daily datasets | running | | | 1,440 rows a day; see the pace below |

Zero `augment_series_error` in those runs. Hyperliquid answered 429 to 110 of the funding
requests at two a second (every one succeeded on retry with the one-second backoff); the info API
weighs most requests 20 against 1,200 a minute, so the source now has its own
`requestsPerSecond` of 1 (PR #17). The timer's run re-synced the quick sources incrementally in
seconds (only the open month of each series rewritten) before starting Binance.

Pace: the Binance walk landed one daily file every 1.67 s (median; p10 1.23 s, p90 1.90 s) with
the zip and its checksum fetched one after the other, each an origin round trip of about 0.7 s
from the box, so the run was bound by latency rather than by the two-a-second spacing. Phase 1 is
about 68,000 daily files, thirty hours at that pace; PR #17 issues the two requests together,
which brings the walk to the spacing's one file a second. The timer starts the next run an hour
after this one ends, and that run's listings start after the newest registered file of each
dataset and symbol.

### Capture lane

First 18 minutes, zero errors:

| Dataset | Files | Rows | Notes |
|---|---|---|---|
| Hyperliquid `candles_1m` | 435 (87 coins, 5 days) | 434,886 | 4,999 candles per coin from 2026-10-05 17:44 UTC, the API's window; zero duplicate open times; 22 throttles, all recovered |
| Hyperliquid `asset_contexts` | 1 (day file, all coins) | 957 | first flush at 05:14:12 after ten minutes: 11 snapshots × 87 coins, zero duplicates on `(at_ms, symbol)` |
| Elfa `call_book` | 31 | 726 | complete, hourly bars 2026-09-09 to the current hour |
| Elfa `events` | 24 | 6,000 | 200 pages, 2026-09-03 to 2026-09-28 |
| Elfa `calls` | 2 | 6,000 | 200 pages cover only 2026-09-09 to 2026-09-10: about 3,900 calls a day |
| Elfa `episodes` | 2 | 6,000 | 200 pages, 2026-09-21 to 2026-09-22 |

`credits.used` read 199 before and after the Elfa cycle, so the guard did not trip; no 429 at one
request a second. The 200-pages-per-stream cap is 13 minutes of requests per cycle; with the
resume rule of PR #17 the calls and episodes streams need about a day of hourly cycles to reach
the present, after which an hour's increment is a few pages. Before that rule a capped stream
restarted from `historyFrom` every hour: the merge on `id` kept the tables correct, the requests
were wasted.

The two lanes wrote 3,102 files, 2.9 million rows and 108 MB in the first hour, with the sync
ledger, catalogs and status files included. `augment query` served every check above while both
lanes were writing.

### Redeploy with PR #17 (05:50 UTC)

`main` at 500e158 was built and installed while both lanes ran, then only the two augment units
were restarted (`systemctl --user restart`; note that a restart of the oneshot sync unit blocks
the caller until the new run ends, so use `--no-block`). The sync run of 04:46 stopped at its
next item with `stopped: true` after 2,479 files and zero errors; the capture lane flushed its
buffers and exited (`augment_capture_done` after 2,819 s). Both restarted on the new binary
within a second with `recovered: 0`: nothing on disk was unregistered. The new sync run re-walked
the quick sources incrementally, added the Bybit `funding` month series (475 files for 50
symbols, zero errors) and resumed the Binance klines after the newest registered day of each
symbol at 1.23 s per file (median and p90), the pace of two requests per file against the host's
two-a-second spacing; three throttles in six minutes, all recovered. The capture lane resumed the
candles from each coin's last stored open time (86 files in its first cycle). Its first Elfa
cycle had no progress to resume from, because the previous binary recorded none for a capped
pull: it fetched the same 200 pages of events again, the merge on `id` left the 6,000 rows as
they were, and this time the newest row was recorded as `lastTo`, so the following cycles move
forward. The collector and decoder stayed active throughout, since October 7 and 8 respectively.
