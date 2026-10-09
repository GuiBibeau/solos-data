# Run on a server

Docker Compose is the simplest setup. Follow the [README](../README.md), then use:

```sh
docker compose logs --tail=20 collector decoder
docker compose stop
docker compose up -d
```

`.env` holds the RPC URL and rate setting. Keep it out of Git and mode 0600.
Data is stored under `data/`. Back up both raw and decoded directories.

Cleanup runs every minute. Historical raw and decoded Parquet remain on disk.
The checkpoint retains unpublished work and a recent 10,000-slot working set.
The raw writer automatically rewrites a growing checkpoint after it exceeds
both 16 GiB and twice its previous compact size. Database writes queue briefly
during the verified rewrite; immutable Parquet remains queryable.
Superseded inputs are removed after ten minutes when no local reader is using
an old catalog and the replacement passes checksum/count verification.
`status` counters describe the hot checkpoint; use `query` for historical totals.
`retentionSlotsPerPass` (default 16,000) bounds one routine trimming pass; raise it when the
archive lane publishes faster than the checkpoint is trimmed, or the checkpoint grows until the
next rewrite.

Historical collection below the last published Old Faithful epoch comes from the
archive lane (ADR-0008) when `archiveBackfillEnabled` is true in `config/phoenix.json`.
It needs no credential; it reads `files.old-faithful.net` and spends compute units
only on the signature manifest and a sampled `getBlock` cross-check. Set
`JETSTREAMER_THREADS` to bound its parallelism on a shared machine. After a burst of provider
throttles the limiter recovers slowly by design; restarting the collector resets its rate.

For an existing large checkpoint, stop its writer and run:

```sh
solos-data collector maintain --all
solos-data collector repack
solos-data collector verify-storage
```

Then restart the collector. `maintain --all` verifies archived contents before
trimming copies. `repack` verifies every table before atomically replacing the
closed checkpoint. Never run these offline commands while a writer is running.
`maintain --all --legacy` optionally scans superseded files from older releases.
This can take much longer and retains any file whose full contents do not match
the latest archive. It is separate from routine cleanup and checkpoint rewrites.
Use `solos-data decoder repack` and `verify-storage` with its writer stopped if needed.
`validate-next-backfill` reports a blocked historical range;
`repair-next-backfill` repairs ordering using already cached finalized blocks only.
R2 is optional; cleanup preserves the whole archive locally.
Do not point two writers at the same checkpoint directory.

## Native Linux services

The example units expect the checkout at `~/solos-data` and the binary at
`~/.local/share/solos-data/bin/solos-data`. Build the binary in Docker on the
server (the archive lane needs clang and cmake, which the build image installs):

```sh
DOCKER_BUILDKIT=1 docker build --target artifact --output type=local,dest=/tmp/solos-data-out .
mkdir -p ~/.local/share/solos-data/bin
install -m 0755 /tmp/solos-data-out/solos-data ~/.local/share/solos-data/bin/solos-data
~/.local/share/solos-data/bin/solos-data collector help
```

The build stage runs the test suite first. Set `JETSTREAMER_THREADS` in `collector.env` to bound
the archive lane's parallelism (64 is a good start on a large machine); without it Jetstreamer
assumes a 1 GB/s link and picks very few threads.

Create `~/.config/solos-data/collector.env` in a text editor with these variable
names. Add your RPC URL, choose your rate, and use an absolute raw data path:

```dotenv
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_API_KEY
SOLOS_DATA_DIR=/path/to/phoenix_raw
SOLOS_DATA_CU_PER_SECOND=300
```

Set `SOLOS_DATA_DIR` to the raw path used by the decoder unit. Its default is
`~/.local/share/solos-data/phoenix_raw`. The unit memory limits are examples;
adjust them for your server. DuckDB defaults to 4GB unless `SOLOS_DATA_DB_MEMORY`
is set. `SOLOS_DATA_QUERY_MEMORY` independently controls analytical query memory
(default 4GB); increase it for full-history scans on a larger server. Effective CU/s is the configured rate multiplied by utilization.

Scope decoded queries to the slots you need: `solos-data decoder query --sql "..." --slots
<from>-<to>` reads only the epoch partitions (432,000 slots each) covering the range, and the
result names them under `epochs`. An unscoped query rebuilds the latest-revision views over every
decoded transaction and needs memory in proportion (more than 96 GB at 441M transactions on
2026-10-08); the 1.1 TB box runs one with `SOLOS_DATA_QUERY_MEMORY=400GB`. The decoder's
`decoded_maintenance` line reports `merges`, `mergedFiles` and `deferred` (groups a pass had no
time for); after the 2026-10-08 compaction change the file count under `tables/` should fall from
tens of thousands to hundreds over a day and then stay there (ADR-0006).

Size `MemoryMax` from the archive lane, not from the RPC lanes: the collector buffers a whole
archive range before inserting it, about 25 KB of memory per transaction, so a 50,000-slot range
on a busy day (2.8M transactions) peaks near 70 GB. A 64 GB limit was OOM-killed on such a range
on 2026-10-07; the units now say 256 GB for the collector and 128 GB for the decoder, whose
working set grows to tens of GB over a day. Halve `archiveChunkSlots` instead if the server is
small. `systemctl --user set-property <unit> MemoryMax=…` changes a running unit without a
restart.

The decoder is bound by its storage engine once the `processed` table holds hundreds of millions
of rows: with DuckDB's defaults (4 GB buffer, 256 MB checkpoint threshold) it spent two thirds of
its time in 45-second checkpoints and in the per-batch update of that table. The example unit sets
`SOLOS_DATA_DB_MEMORY=96GB`, `SOLOS_DATA_DB_THREADS=16` and `SOLOS_DATA_DB_CHECKPOINT=48GB`; the
checkpoint cost is fixed per call, so a large threshold amortizes it, at the price of a longer
write-ahead log replay after a crash. Transactions in a batch decode in parallel on
`SOLOS_DATA_DECODE_THREADS` threads (default: the machine's parallelism, at most 32); the
published rows and the progress record are assembled in source order, so output is identical to
the sequential decoder.
The explicit checkpoint that used to follow every minute's compaction now runs every
`checkpointIntervalSeconds` (`config/decoded.json`, default 900): on a large store each one costs
45 to 60 seconds whatever the amount of change, and the write-ahead log keeps committed batches
durable in between.


Set `SOLOS_DATA_CU_PER_SECOND` to the provider account's real ceiling, not above it. Alchemy
enforces compute units per second per account over a 10-second rolling window (300 CU/s on the
free tier), and a configured rate above that ceiling produces bursts of 429s that collapse the
limiter far below the ceiling. Alchemy's costs match `cuWeights` (getBlock 40, getSignaturesForAddress
40, getTransactionsForAddress 100). `tailShare` in `config/phoenix.json` is the fraction reserved for
the live tail; the tail cannot borrow the backfill's share, so size it from the tail's cost (about
48,000 CU per 1,000 slots at twenty transactions per slot, 180 CU/s at the chain's pace; more
when the venue is busier). The archive
lane spends compute units only on the manifest walk (40 CU per 1,000 signatures) and the sampled
ordering check (`archiveOrderingSample`). Check the ceiling with a short burst of
`getSignaturesForAddress` pages: the first 429 arrives when the window is spent.

The archive lane logs `archive_tip` when the mirror's newest epoch changes and
`archive_tip_unknown` when the mirror does not answer; it then keeps the last known tip (stored
as `archive-tip` in the checkpoint) and, if none is known, waits rather than backfilling over RPC.
A run of `backfill_chunk` events with `source: rpc` below the archive tip means the archive lane
is not being used; check the mirror and the `archive_tip` events.

`tailEnabled: false` in `config/phoenix.json` runs the collector without the live tail: the archive
lane backfills history (about 1.1 CU per slot for the manifest check, roughly 10M CU per wall-day
at 280 CU/s) and the retention pass keeps running. The watermark stays put. Switching the tail back
on resumes it from the watermark over RPC, so the gap costs about 65k CU per 1,000 slots.
`SOLOS_DATA_DB_CHECKPOINT` defaults to `256MB`. It controls automatic checkpoint
frequency; committed changes stay durable in the write-ahead log between checkpoints.

```sh
chmod 600 ~/.config/solos-data/collector.env
mkdir -p ~/.config/systemd/user
cp ops/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-phoenix.service solos-data-phoenix-decoder.service
```

If jobs must continue after logout, configure user lingering on the server.
Read status without opening the writer's checkpoint:

```sh
SOLOS_DATA_DIR=/path/to/phoenix_raw solos-data collector status
SOLOS_DATA_DECODED_DIR=/path/to/decoded/v1 solos-data decoder status
```

## Augmentation lane

`solos-data augment sync --config config/augment.json` (ADR-0009) downloads the free exogenous
data for the Phoenix markets (Binance USD-M dumps, Hyperliquid and Bybit funding, Deribit DVOL,
DefiLlama stablecoins, SEC filings, Phoenix earnings dates, Fear and Greed, Polymarket and
Kalshi odds) into `SOLOS_DATA_AUGMENT_DIR` (default `dataDir`, `data/augment`) and exits. It is
idempotent: closed periods are fetched once, the open day or month is rewritten each run, and
anything on disk the ledger does not know is removed at start. The timer runs it hourly, one run
at a time; the first run backfills from `startDate` (2026-01-01) and takes about a day because
of the two-requests-a-second spacing per host. No credential is needed; `augment.env` exists
for `ELFA_API_KEY`, which only the capture lane reads.

```sh
cp ops/solos-data-augment.service ops/solos-data-augment.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-augment.timer
systemctl --user list-timers solos-data-augment.timer
journalctl --user -u solos-data-augment.service -o cat -f
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment status
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT phoenix_symbol, count(*) AS rows, min(ts), max(ts) FROM binance_klines GROUP BY 1 ORDER BY 1"
```

`status.json` has one entry per source (`files`, `rows`, `errors`, `skipped`) and one per
dataset (files, symbols, rows, bytes, oldest and newest period). `augment_series_error` lines
name the source, dataset, symbol and item that failed; the item is retried on the next run.
`--source <name>` (`binance`, `hyperliquid`, `deribit`, `defillama`, `bybit`, `sec`,
`alternative`, `polymarket`, `kalshi`, `phoenix`) and `--symbol <SYM>` restrict a run for a
check. Query views are named `<source>_<dataset>` (`binance_klines`, `hyperliquid_funding`,
`deribit_dvol_60s`, `defillama_stablecoins`, `sec_filings`, `alternative_fear_greed`,
`polymarket_prices`, `kalshi_candles_1h`, `phoenix_earnings_dates`).

The exogenous sources (ADR-0009, "Exogenous triggers and alerts") run in the same sync. SEC
EDGAR filings need the issuers' CIKs in the symbol map (`secCik`) and a declared User-Agent:
EDGAR answers `403` to a client it considers undeclared, and `sources.sec.userAgent` is where
the operator declares one. `augment sec-map` reports the CIK of every equity symbol from
EDGAR's `company_tickers.json`, `--write` stores them in the config, and the lane refuses a CIK
whose EDGAR tickers do not include the symbol (`augment_series_error` with the issuer's name).
Polymarket events and Kalshi series are curated lists in the config; add the next month's
events when the current ones close.

```sh
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment sync --source sec --symbol NVDA
solos-data augment sec-map            # report; add --write to store the CIKs in config/augment.json
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT ticker, form, ts, url FROM sec_filings WHERE form IN ('8-K', '10-Q') ORDER BY ts DESC LIMIT 20"
```

To re-download one series, delete its rows from `files` and its `progress` record in
`checkpoint.duckdb` with no run active, or delete the whole root to start over.

The large datasets (Binance `aggTrades` and `bookDepth` under `largeDatasets`, Bybit tick trades
under `bybit.trades`) are off by default and stay off while `diskBudgetGb` is 0. Set the budget to
the size the augment root may reach, in GB, and list the datasets; a run stops adding large files
once the lane's registered bytes pass the budget (`augment_budget_reached`) and resumes where it
left off when the budget is raised. SOLUSDT alone is about 17 MB of gzip ticks a day on Bybit;
size the budget from `augment status`'s per-dataset bytes after a day.

`solos-data augment capture --config config/augment.json` is the long-running lane for what
cannot be fetched later: Hyperliquid 1-minute candles every thirty minutes, Hyperliquid asset
contexts (funding, open interest, mark/oracle/mid prices) every minute, Elfa events, calls,
episodes and call-book bars every hour. It keeps its own ledger under `capture/` and its own
`catalog-capture.json` and `status-capture.json`, so it runs alongside the sync timer on the same
root; `augment status` prints both and `augment query` sees both catalogs. Elfa needs
`ELFA_API_KEY` in `~/.config/solos-data/augment.env` (mode 0600); without it the lane logs
`augment_elfa_skipped` and runs the Hyperliquid captures only. Elfa is free today and
undocumented: the lane reads `credits.used` before and after every cycle and, if it moved, logs
`elfa_billing_started` and stops calling Elfa until the service restarts (the status shows
`disabledByCreditGuard`). Asset contexts are buffered for up to ten minutes before they are
merged into the day's file; a SIGTERM flushes them.

```sh
chmod 600 ~/.config/solos-data/augment.env
cp ops/solos-data-augment-capture.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-augment-capture.service
journalctl --user -u solos-data-augment-capture.service -o cat -f
```

## Switch a running TypeScript deployment to the binary

The Rust binary reuses the checkpoints and cursors as they are (ADR-0007). Install the
binary and the new unit files, then:

```sh
systemctl --user stop solos-data-phoenix-decoder.service
systemctl --user stop solos-data-phoenix.service
backup=~/.local/share/solos-data/backup-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup/raw" "$backup/decoded"
cp ~/.local/share/solos-data/phoenix_raw/{checkpoint.duckdb,catalog.json,status.json} "$backup/raw/"
cp ~/.local/share/solos-data/phoenix_raw/checkpoint.duckdb.wal "$backup/raw/" 2>/dev/null || true
cp ~/.local/share/solos-data/phoenix_decoded/v1/{checkpoint.duckdb,catalog.json,status.json} "$backup/decoded/"
cp ~/.local/share/solos-data/phoenix_decoded/v1/checkpoint.duckdb.wal "$backup/decoded/" 2>/dev/null || true
systemctl --user daemon-reload
systemctl --user start solos-data-phoenix.service
systemctl --user start solos-data-phoenix-decoder.service
journalctl --user -u solos-data-phoenix.service -f
```

Watch for the first `backfill_chunk`, `tail_chunk` and `decoded_batch` events and compare
`query` row counts with the numbers from before the stop. Rollback is the reverse `ExecStart`
swap in the unit files; keep the Node runtime for a week.

## Move the data

Stop both writers, copy both data roots and the private credential file, then
point the new services at those paths. Before restarting a moved raw collector:

```sh
SOLOS_DATA_DIR=/path/to/new/raw solos-data collector relocate
```

Decoded catalogs use relative paths and need no rebase. Restart with the same
codec/schema version. [Table details](../docs/decoded-tables.md) cover read-only copies.

## Verify changes

With Rust 1.96 installed, run `cargo test --workspace`. Tests are offline and need no
credentials. The archive lane compiles with `cargo build --features archive -p solos-data`,
which needs clang and cmake. The `clang-sys` build script wants an unversioned `libclang.so`
and `llvm-config`; a Debian box with only `libclang1-19` and `llvm-19-dev` builds with
`ln -s /usr/lib/llvm-19/lib/libclang-19.so.19 ~/.local/lib/libclang.so` once, then
`LIBCLANG_PATH=$HOME/.local/lib LLVM_CONFIG_PATH=/usr/bin/llvm-config-19 cargo build …`
(installing `libclang-19-dev` provides the symlink instead). Until the TypeScript sources are
removed, `npm run verify` still runs their suite.
