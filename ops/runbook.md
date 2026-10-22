# Run on a server

Docker Compose is the simplest setup. Follow the [README](../README.md), then use:

```sh
docker compose logs --tail=20 collector decoder
docker compose stop
docker compose up -d
```

`.env` holds the RPC URL and rate setting. Keep it out of Git and mode 0600.
Data is stored under `data/`. Back up both raw and decoded directories (the native
deployment's nightly off-box copy is under [Off-box backup](#off-box-backup)).

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

Maintenance (compaction, checkpoint trimming, the automatic rewrite) runs on its own schedule
beside the lanes: every `maintenanceIntervalSeconds`, and again 5 s after a pass that trimmed a
range, so a small backlog drains between the lanes' writes. Each pass logs `storage_maintenance`
(`trimmedRows`, `ranges`, `checkpoint`); a failing pass logs `collector_error` with lane
`maintenance`. Trimming keeps the newest `checkpointHotSlots` below the watermark and never touches
an active tail cycle's unpublished slots (from its floor while it walks, from its next chunk once it
publishes). The automatic rewrite runs only on a pass that found nothing left to trim, once the file
is at least 16 GB and twice its size after the previous rewrite. Every rewrite (automatic or
`collector repack`) keeps the closed original as `checkpoint.prev.duckdb` in the raw root, a hard
link that costs no space until the new file replaces it; it is the way back if the rewritten file
is ever in doubt, and the next rewrite replaces it. Remove it by hand to reclaim its space once the
collector has run on the new file. The off-box backup never uploads it (it copies catalog files
only).

A large backlog does not drain in service. Every pass ends with a DuckDB `CHECKPOINT`, and on a
grown file that checkpoint dominates: on 2026-10-18 the 1.2 TB checkpoint trimmed about 2.5M rows
per 100,000-slot pass in under a minute, then checkpointed for 5 to 6 minutes (72 checkpoints,
23,090 s of 27,600 s), so it drained about 5M transactions an hour and held the single writer
while doing it: the tail published 2 chunks an hour and the backfill 1. With more than a few
million published rows waiting (`checkpointCounts` far above the hot window, `storage_maintenance`
reporting `ranges: 1` on every pass), stop the collector and rewrite offline with
`collector repack --keep-hot` (below) instead. To watch a backlog drain:

```sh
journalctl --user -u solos-data-phoenix -o cat --since -1h | grep -E '"event":"storage_maintenance"' | tail -5
python3 -c 'import json, os; s = json.load(open(os.path.expanduser("~/.local/share/solos-data/phoenix_raw/status.json"))); print(s["checkpointCounts"], s.get("retention"), s.get("checkpointRepack"))'
ls -l ~/.local/share/solos-data/phoenix_raw/checkpoint*.duckdb
```

Historical collection below the last published Old Faithful epoch comes from the
archive lane (ADR-0008) when `archiveBackfillEnabled` is true in `config/phoenix.json`.
It needs no credential; it reads `files.old-faithful.net` and spends compute units
only on the signature manifest and a sampled `getBlock` cross-check. Set
`JETSTREAMER_THREADS` to bound its parallelism on a shared machine. After a burst of provider
throttles the limiter recovers slowly by design; restarting the collector resets its rate.

For an existing large checkpoint, stop its writer and run, with the service's environment
(`SOLOS_DATA_DB_MEMORY`, and `SOLOS_DATA_DB_THREADS` raised for the run):

```sh
solos-data collector repack --keep-hot --dry-run   # optional: the proof alone, changes nothing
solos-data collector repack --keep-hot
solos-data collector verify-storage
```

Then restart the collector. `repack --keep-hot` builds a new checkpoint holding only the rows the
collector still needs and swaps it in. It drops the signature, transaction, ordering and RPC-page
rows inside the published coverage up to the trimming cutoff (the same cutoff as in-service
trimming: the hot window below the watermark, never an active tail cycle's unpublished slots),
after proving them chunk by chunk (`--slots-per-check`, default 1,000,000 slots) equal to their
newest published Parquet copy with the same whole-row digest check trimming uses. A chunk that
fails the proof keeps its rows and is listed under `unverified`. Everything else is copied row for
row: rows above the cutoff or outside the coverage, rows without a slot, `program_versions`, and
every other table (`kv` with the watermark, walks, cycle and backfill progress, file
registrations, published ranges, addresses, exchange snapshots). The new file is proven before the
swap: per table, the rows left out equal the rows proven against the archive, and the kept rows
equal the copy as whole-row digest multisets in both directions. The original stays as
`checkpoint.prev.duckdb`, and the result is recorded under `checkpoint-repack` (so the automatic
rewrite measures growth from the new size). Published Parquet and `catalog.json` are not changed
(the catalog is rewritten with the same files and coverage). `maintain --all` still trims in place,
but on a large checkpoint it is the slow in-service drain run offline; plain `repack` copies every
row, published copies included. Never run these offline commands while a writer is running.
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
`checkpointIntervalSeconds` (`config/decoded.json`, default 900, 7,200 in the repository config):
each one rewrites the `processed` index whatever the amount of change (570 to 700 seconds at 1.49
billion rows on 2026-10-22), and the write-ahead log keeps committed batches durable in between.
A longer interval means a longer log replay after a crash.

The other decoder settings in `config/decoded.json` (ADR-0007, 2026-10-22): `batchSize`
(transactions per batch, at most 100,000) and `maxBatchSources` (raw files one batch may span; 1
restores one file per batch); `compactionIntervalSeconds` (default 60) and
`compactionBudgetSeconds` (default 15) set how often compaction and garbage collection run and how
long a pass keeps starting merges; `compactionSettledBatches` (default 100, 0 turns it off) is how
many batches a `(table, epoch)` group must go without one before its head merges with two files
(ADR-0006). To measure pace, read `decoded_batch` lines: `batchRaw` transactions from
`batchSources` files covering `batchOldestSlot` to `batchNewestSlot`, with `readSeconds`,
`decodeSeconds`, `publishSeconds` and `loopSeconds`; `decoded_maintenance` adds
`compactionSeconds`, `garbageSeconds` and `checkpointSeconds`.


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

A `backfill_chunk` with `manifest: 0` while the matching `archive_range` reports transactions means
the manifest walk ended early; since 2026-10-11 validation reports such rows as `unlisted` and
refuses the range. `walk_reseeded` means the collector resumed an unconfirmed manifest walk from
the oldest published transaction above the chunk; its `unverifiedFrom`..`unverifiedTo` slots were
published without manifest rows and need re-validation (ADR-0008). `walk_cursor_unresolved` means
the provider answered an empty page for a cursor and the walk continued from the row above it.

Re-validate published ranges with the collector running (ADR-0008):

```sh
nice -n 10 env SOLOS_DATA_DB_MEMORY=1GB SOLOS_DATA_DB_THREADS=1 \
  solos-data collector reverify --config config/phoenix.json \
  --from 427163137 --to 428813136 --out ~/reverify/2026-10-13 --rps 50 --memory 32GB --threads 8
```

It needs `SOLANA_RPC_URL` (source `collector.env` in the same shell, never print it) and reads the
raw root's catalog under a reader lease; it never opens the live checkpoint and writes only under
`--out`, which must lie outside the raw root. Each published range gets a store under
`--out/ranges/<from>-<to>/` (the slots it compares; the ProgramData walk is under
`--out/programdata-<from>-<to>/`) (one DuckDB per range, hence the small `SOLOS_DATA_DB_*` values; `--memory`
and `--threads` size the comparison connection), a `report.json` and, on a mismatch,
`missing.csv`/`unlisted.csv`. An interrupted run resumes: walks continue from their stored cursor
and ranges with a report are skipped while the published files and ProgramData rows behind it are unchanged (a changed
catalog compares again; delete a range's directory to redo its walk). `--rps` caps its own
requests (default 50/s, separate from the collector's limiter); `--parallel` bounds the ranges
walked at once (default 32). It prints the summary, also written to `--out/report.json`, and exits
1 when any range fails; it never rewrites published data.

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
data for the Phoenix markets (Binance USD-M dumps, Bybit and OKX tick dumps, Hyperliquid and
Bybit funding, Deribit DVOL,
DefiLlama stablecoins, SEC filings, Phoenix earnings dates, Fear and Greed, Polymarket and
Kalshi odds) into `SOLOS_DATA_AUGMENT_DIR` (default `dataDir`, `data/augment`) and exits. It is
idempotent: closed periods are fetched once, the open day or month is rewritten each run, and
anything on disk the ledger does not know is removed at start. The timer runs it hourly, one run
at a time; the first run backfills from `startDate` (2026-01-01) and takes about a day because
of the two-requests-a-second spacing per host. No credential is needed. Both augment units read
`~/.config/solos-data/augment.env` (mode 0600): the capture lane for `ELFA_API_KEY`, the sync
for `SEC_USER_AGENT`.

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
`alternative`, `polymarket`, `kalshi`, `phoenix`, `tardis`, `okx`, `hyperliquid_s3`) and `--symbol <SYM>` restrict a run for a
check. Query views are named `<source>_<dataset>` (`binance_klines`, `hyperliquid_funding`,
`deribit_dvol_60s`, `defillama_stablecoins`, `sec_filings`, `alternative_fear_greed`,
`polymarket_prices`, `kalshi_candles_1h`, `phoenix_earnings_dates`).

The exogenous sources (ADR-0009, "Exogenous triggers and alerts") run in the same sync. SEC
EDGAR filings need the issuers' CIKs in the symbol map (`secCik`) and a declared User-Agent
with a contact email: EDGAR answers `403` to any User-Agent without one. The operator declares
it in the environment, never in the config:

```sh
# in ~/.config/solos-data/augment.env (mode 0600); the next timer run picks it up
SEC_USER_AGENT=<name> <contact email>
```

Without it (or without an `@` in it) each sync logs one `augment_source_disabled` line with
`"source":"sec"` and the reason, sends nothing to EDGAR, and `augment status` shows
`sources.sec.disabled: true` with the reason; `augment sec-map` refuses to run. `augment sec-map` reports the CIK of every equity symbol from
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

The exchange tick dumps (ADR-0009, "Exchange dumps") are walked newest day first from
`walk.startDate` (2025-10-01) for the crypto markets: Binance `aggTrades` and `bookDepth`
(`binance.largeDatasets`, SHA-256 checked), Bybit tick trades (`bybit.trades`) and OKX swap
trades and monthly funding (`sources.okx`). They need the lane's `diskBudgetGb` above 0 (1,000
on the box). Each walk spends at most `walk.runMinutes` (45) of a run and stops at its own
`walk.budgetGb`; the status note of the source says why it stopped, and the next hourly run
continues where it left off. A period the venue does not serve and that ended more than three
days ago is recorded as absent (`progress` key `<source>/<dataset>/<symbol>`); a failed file is
an `augment_series_error` and is retried next run. OKX partitions are OKX's UTC+8 days. The
dumps are excluded from the R2 backup. Binance's data is CC BY-NC-SA 4.0, non-commercial
research only, no live trading use (ADR-0009, "Terms").

```sh
python3 -c 'import json, os; s = json.load(open(os.path.expanduser("~/.local/share/solos-data/augment/status.json"))); [print(k, s["sources"].get(k)) for k in ("binance", "bybit", "okx", "hyperliquid_s3")]'
journalctl --user -u solos-data-augment.service -o cat | grep -E '"event":"augment_(dump_yield|budget_reached|series_error)"' | tail
python3 - <<'PY'
import collections, json, os
files = json.load(open(os.path.expanduser("~/.local/share/solos-data/augment/catalog.json")))["files"]
agg = collections.defaultdict(lambda: [set(), 0, 0, "9", "0"])
for f in files:
    k = f["source"] + "/" + f["dataset"]
    if k in ("binance/aggTrades", "binance/bookDepth", "bybit/trades", "okx/trades", "okx/funding"):
        a = agg[k]; a[0].add(f["symbol"]); a[1] += 1; a[2] += f["bytes"]; a[3] = min(a[3], f["period"]); a[4] = max(a[4], f["period"])
for k, (syms, n, b, lo, hi) in sorted(agg.items()):
    print(k, len(syms), "symbols", n, "files", round(b / 1e9, 1), "GB", lo, hi)
PY
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT phoenix_symbol, count(*) AS trades, min(ts), max(ts) FROM okx_trades GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
```

A one-symbol check: `augment sync --source okx --symbol SOL`.

Hyperliquid's S3 archive (`sources.hyperliquidS3`) is requester-pays: the AWS account whose keys
sign the requests pays the transfer ($0.09/GB from `hyperliquid-archive`, $0.114/GB from
`hl-mainnet-node-data`). Without keys the source logs `augment_source_disabled` and sends
nothing. Create an IAM user whose policy allows only `s3:GetObject` and `s3:ListBucket` on
`hyperliquid-archive` and `hl-mainnet-node-data` (and their `/*`), and put its keys in
`augment.env` without them touching the terminal or the history:

```sh
~/.local/bin/solos-hl-aws-keys          # hidden input; backs up augment.env, prints only the lengths
systemctl --user start --no-block solos-data-augment.service
journalctl --user -u solos-data-augment.service -o cat | grep -E '"event":"augment_hl_s3_estimate"' | tail -1
```

With `estimateOnly: true` (committed) the runs only list the window, a day once, and the status
note gives the bytes and price per dataset. Read it, then set `estimateOnly` to false (and
`fills` to true if wanted) in `config/augment.json`; each run then downloads at most
`transferGbPerRun` (20 GB, about $1.80).

Tardis.dev (`sources.tardis`, ADR-0009 "Tardis.dev") downloads the free first-of-month CSV
samples for the Phoenix crypto markets on Binance USD-M, Bybit, OKX swaps, Hyperliquid and
Deribit (trades, derivative ticker, liquidations; thinned book snapshots for BTC, ETH and SOL)
into `tables/tardis/`. Anonymous downloads hit a transfer limit after a few hundred MB to a few
GB a day: the run logs `augment_tardis_quota`, `augment status` shows the note, and the next
runs resume when it resets, so the 13 sample days take several days. Every other day needs a
paid key. Put it in `augment.env` without it touching the terminal or the history, then check:

```sh
~/.local/bin/solos-tardis-key           # hidden input; backs up augment.env, prints only the key length
systemctl --user start --no-block solos-data-augment.service
journalctl --user -u solos-data-augment.service -o cat | grep -E '"event":"augment_tardis_(mode|quota)"' | tail -3
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT symbol, count(*) AS days FROM (SELECT DISTINCT symbol, CAST(ts AS DATE) FROM tardis_binance_futures_trades) GROUP BY 1 ORDER BY 1"
```

Set `sources.tardis.keyedStart` to the first day the plan's history covers (a quarterly plan
reaches 12 months back) and `budgetGb` to what the root may hold. The licence keeps these files on
the box: the R2 backup skips `tables/tardis/`, and no Tardis-derived data goes to Modal, GitHub or
another provider without written terms (ADR-0009).

`solos-data augment capture --config config/augment.json` is the long-running lane for what
cannot be fetched later: Hyperliquid 1-minute candles every thirty minutes, Hyperliquid asset
contexts (funding, open interest, mark/oracle/mid prices) every minute, Elfa events every
minute (`eventsIntervalSeconds`, two pages a poll), Elfa calls, episodes and call-book bars
every hour, and the Elfa Auto alerts (`elfa.auto`): reconciled hourly, streamed continuously
into `elfa/auto_events`. It keeps its own ledger under `capture/` and its own
`catalog-capture.json` and `status-capture.json`, so it runs alongside the sync timer on the same
root; `augment status` prints both and `augment query` sees both catalogs. Elfa needs
`ELFA_API_KEY` in `~/.config/solos-data/augment.env` (mode 0600); without it the lane logs
`augment_elfa_skipped` and runs the Hyperliquid captures only. Elfa v3 is
undocumented: the credit guard disables any v3 endpoint whose answer declares a credit until
the service restarts (the status shows `disabledByCreditGuard` and `billedEndpoints`); see
the credit paragraph below. Episodes page newest first and resume their
catch-up from a stored cursor (ADR-0009, "Episodes page newest first"); each cycle logs
`elfa_episodes_cycle` with its requests, rows, `newestOpenedAt` and pending segments. Asset contexts are buffered for up to ten minutes before they are
merged into the day's file; a SIGTERM flushes them.

Every Elfa answer is metered per endpoint from its `x-elfa-credits` header and the whole client
stops at `elfa.creditCapPerMonth` (default 100 credits a calendar month, v3 and Auto together;
ADR-0009, "Elfa credits"). The month's total survives restarts (ledger key `elfa/credits`).
The v3 guard is per endpoint: an answer that declares a credit disables that endpoint only
(`elfa_billing_started` with `endpoint`; `elfa.billedEndpoints` in the status); the whole lane
stops only when `credits.used` moved and a v3 answer carried no header (`elfa.laneDisabled`).
`elfa_cycle_credits` logs each hourly cycle's `usedDelta`, `declared` and `unattributed`
(credits spent on the key that no answer of this client declared).

```sh
python3 -c 'import json, os; s = json.load(open(os.path.expanduser("~/.local/share/solos-data/augment/status-capture.json"))); print(json.dumps(s["elfaCredits"], indent=1)); print(s["elfa"])'
journalctl --user -u solos-data-augment-capture.service -o cat | grep -E '"event":"elfa_(credits|cycle_credits|billing_started|credit_cap_reached|credit_meter|auto_held)"' | tail -20
```

To raise the cap for the rest of a month, edit `creditCapPerMonth` and restart the capture unit.

Measured per endpoint on 2026-10-10 (ADR-0009): every `/v3/*` endpoint (`events`, `calls`,
`calls/episodes`, `market/crypto/call-book`, `key-status`) and the Auto stream declare 0
credits; the Auto listing `/v2/auto/queries` declares 1; a creation declares 5. Credits that
move `credits.used` without being declared show as `elfa_key_drift` (between hourly cycles)
and `elfa.unattributedCreditsSinceStart` in the status: an alert's server-side work or another
client of the same key. The cap does not stop them; cancelling the alert does.

```sh
journalctl --user -u solos-data-augment-capture.service -o cat | grep -E '"event":"elfa_(key_drift|auto_listing)"' | tail -12
```

The Auto alerts cost credits: five per creation, one per listing of the account's queries
(at most every `reconcileIntervalHours`, 12, or when an alert is missing or due for renewal;
a restart in between holds the alerts stored in the ledger). `elfa.auto.creditBudgetPerMonth`
and `maxAlerts` cap what the lane may create; `status-capture.json` shows `elfaAuto`
(active titles, created, renewed, fired, `creditsSpentMonth`, `streamConnected`,
`connections`, `reconnects`, `errors`) and
`elfaEvents` (polls, requests, `requestsPerPoll`). To add an alert, append its definition to
`elfa.auto.alerts` (conditions and repeat only; the lane adds the `notify` action and the
expiry) and restart the capture unit; to retire one, remove it from the config and cancel it
once with the Elfa API, since the lane never cancels an alert it did not renew. Check the
spend against the key with `credits.used` before and after:

```sh
journalctl --user -u solos-data-augment-capture.service -o cat | grep -E "elfa_auto_(created|renewed|fired|credits|budget_reached)"
# Reconnections (end event, server close, drop, idle) are expected and are not errors:
journalctl --user -u solos-data-augment-capture.service -o cat | grep -E "elfa_auto_stream_(reconnect|closed)|\"auto_events\".*augment_series_error"
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT ts, query_title, status, title FROM elfa_auto_events ORDER BY ts DESC LIMIT 20"
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT quantile_cont((received_at_ms - first_seen_at * 1000) / 1000.0, [0.5, 0.9]) AS latency_s FROM elfa_events WHERE received_at_ms IS NOT NULL"
```

```sh
chmod 600 ~/.config/solos-data/augment.env
cp ops/solos-data-augment-capture.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-augment-capture.service
journalctl --user -u solos-data-augment-capture.service -o cat -f
```

## Other Solana perp venues and Pyth

`solos-data venues run --config config/venues.json` (ADR-0012) collects Jupiter Perps (and Flash
Trade's remaining L1 activity) from mainnet: every successful program transaction, its program
instructions and its Anchor events decoded through the programs' on-chain IDLs, the pool, custody
and market accounts every minute, and the Pyth push-oracle prices of the Phoenix markets every five
seconds. It writes hour files under the augment root (`tables/jupiter/…`, `tables/flash/…`,
`tables/pyth/prices/…`) with its own ledger (`venues/checkpoint.duckdb`), `catalog-venues.json` and
`status-venues.json`, and its own RPC limiter (`requestsPerSecond`, 20) on the collector's provider
account. It never touches the collector's checkpoint or cursors.

```sh
cp ops/solos-data-venues.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-venues.service
journalctl --user -u solos-data-venues.service -o cat | grep -E '"event":"venues_(cycle|error|file)"' | tail -20
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data venues status | python3 -m json.tool | head -60
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT name, count(*) AS n, count(fields) AS decoded FROM jupiter_events GROUP BY 1 ORDER BY 2 DESC"
SOLOS_DATA_AUGMENT_DIR=~/.local/share/solos-data/augment solos-data augment query --sql \
  "SELECT symbol, count(*) AS rows, max(ts) FROM pyth_prices GROUP BY 1 ORDER BY 1"
```

Each cycle logs `venues_cycle` (`manifest`, `bulk`, `single`, `unlisted`, `failed`, `events`,
`undecoded`, `unknown`, `lagSlots`); a failed cycle logs `venues_error` and retries the same window.
The status has, per venue, the cursor and its lag behind the finalized slot, the totals, the
error streak and the last error; `rpc.credits` and `rpc.creditsPerDay` estimate the Helius credits
from the request counters and `creditCosts`; `pyth.feedAgeSeconds` is each feed's age (null: the
account does not exist; stale feeds are not stored, `pyth.maxAgeSeconds`). `undecoded` above zero
means a program changed a layout: fetch the program's IDL again and commit it.

```sh
# The program's Anchor IDL account (createWithSeed(findProgramAddress([], program), "anchor:idl", program)).
set -a; . ~/.config/solos-data/collector.env; set +a
solos-data venues idl --address 38GK1i4cQPAxqrbfKX4RRMNfXpKRn5PgLFHVJeXm1C8Y --out config/idl/jupiter-perpetuals.json
```

A restart resumes from the stored cursor; a window flushed but not yet recorded is fetched again
and merged. To start over, stop the unit and delete `venues/` and `tables/{jupiter,flash,pyth}` under
the augment root together.

## Dataset health

`solos-data health` reads the JSON snapshots the services already write (raw `status.json` and
`catalog.json`, decoded `status.json` and `catalog.json`, the augment `status.json` and
`status-capture.json`) and the free space of the data volume, writes `health.json` (path from
`SOLOS_DATA_HEALTH_FILE`) and logs one `health` line with `level` (`ok`, `warn`, `critical`)
and the `warn` and `critical` lists. It opens no DuckDB file and writes nothing under the raw or
decoded roots. `--json` also prints the whole report. The thresholds are the `thresholds` block
of `config/health.json`; a field left out keeps its default.

| Check | Source | Warn | Critical |
|---|---|---|---|
| decoded files (compaction) | decoded `catalog.json` `files` | > 10,000 | > 50,000 |
| decoder cadence | batch ids of non-compaction files created in the last 30 min | none while the raw catalog's `at` is newer than the newest batch | none for 120 min |
| collector alive | raw `status.json` `at` | > 10 min old | > 60 min old |
| backfill progress | raw `status.json` `backfill.next` against the previous `health.json` | unchanged for 120 min (not when `phase` is `complete`) | |
| tail progress | raw `status.json` `watermark.completedAt` (written with every published tail chunk), only while the tail is enabled (`lanes.tail.enabled`) | not advanced for 15 min | not advanced for 60 min |
| checkpoint maintenance | raw `status.json` `maintenance.at` (written by every storage maintenance pass); the `maintenance` lane's error streak is judged with the lanes below | > 15 min old, or no pass recorded | > 60 min old |
| venues lane | augment `status-venues.json` (ADR-0012), only when it exists | file > 15 min old, a venue's last cycle > 15 min ago, lag > 3,000 slots, 3 failed cycles in a row, no Pyth poll | lag > 9,000 slots, 10 failed cycles in a row |
| lane error streaks | raw `status.json` `lanes.{tail,backfill,maintenance}`: consecutive `collector_error`s with no chunk published in between (the lane's progress marker unchanged), last error < 30 min old | 3 in a row | 10 in a row; the finding names the lane and the error class (`oom`, `constraint`, `rollback_failed`, `rpc`, `other`) |
| augment sync | `status.json` age and `sources.*.errors` | > 150 min old, or errors in the last run | |
| augment capture | `status-capture.json` age and the lanes' `errors` | > 10 min old, errors grown since the previous check, the Elfa credit guard tripped, the last Elfa hourly cycle > 150 min ago | |
| disk free | `statvfs` of the raw root's parent | < 500 GB | < 100 GB |

The timer runs it every fifteen minutes; its `ExecStartPost` logs one `health_services` line
with `systemctl --user is-active` of every solos-data unit (service activity is not the
binary's business). There is no notification channel yet: read the file or the journal.

```sh
cp ops/solos-data-health.service ops/solos-data-health.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-health.timer
python3 -m json.tool ~/.local/share/solos-data/health.json | head -40
python3 -c 'import json, os; h = json.load(open(os.path.expanduser("~/.local/share/solos-data/health.json"))); print(h["level"], h["warn"], h["critical"])'
journalctl --user -u solos-data-health.service -o cat | grep -E '"event":"health(_services)?"' | tail -4
```

`level` is the worst finding; each `warn`/`critical` entry is a sentence naming the check and
the measured value. `decoded` has `files`, `catalogBytes`, `batchesInWindow`, `newestBatchAt`
and `transactionsProcessed`; `raw` has `statusAgeSeconds`, `catalogFiles` and
`backfill.{next, phase, nextChangedAt, unchangedMinutes}`; `augment.sync` and
`augment.capture` have their age and error totals (capture: since its `startedAt`); `disk` has
`freeGb`. `nextChangedAt` and the capture error baseline come from the previous `health.json`,
so deleting the file resets them (the first run cannot judge backfill movement).

`raw.tail` has `watermarkSlot`, `watermarkAt`, `minutesSinceAdvance`, `headSlot` and `lagSlots`: a
tail catching up (lag in the millions of slots after it was off for days) is fine as long as the
watermark advances, so only the advance is judged. `raw.errorStreaks.<lane>` has the collector's
`errorStreak`, the `current` count health judged (0 once the last error is older than the window
or the lane published since: its progress marker moved, or its `watermark`/`backfill` record's
`completedAt` is newer than the last error), its `class`, `since` and `lastErrorAt`. The collector keeps the
streaks in memory, not in DuckDB, so they are still counted when the database is what fails. A
status from a collector older than this check has no `lanes`: the tail then falls back to
`tailHealth.errorStreak` and `lastError`, and is judged only while a tail cycle is in flight or
its streak is non-zero; the backfill streak needs the new collector.

### 2026-10-14 incident: DuckDB out of memory with both lanes writing

At 07:22 UTC on 2026-10-14 the collector's DuckDB reached its 32GB `SOLOS_DATA_DB_MEMORY` while
the live tail (re-enabled on 2026-10-13, catching up from 2026-10-05) and the archive backfill
wrote together. A commit then failed to roll back (`Failed to commit: Failed to rollback
transaction. Cannot continue operation`), and from then on both lanes failed every 30 seconds: the
backfill with `Out of Memory Error`, the tail with `Constraint Error: Duplicate key` on its
`bulk/tail/<from>/<to>` progress record and on a transaction signature. The service stayed
`active` and `status.json` stayed fresh, so health said `ok` until the backfill stall warned at
120 min; the tail stall was never flagged. The two checks above now go critical within one timer
run. The operator restarted the collector at 02:51 UTC on 2026-10-15 with the limit raised to
64GB by a drop-in, which the unit in `ops/` now matches:

```sh
mkdir -p ~/.config/systemd/user/solos-data-phoenix.service.d
printf '[Service]\nEnvironment=SOLOS_DATA_DB_MEMORY=64GB\n' \
  > ~/.config/systemd/user/solos-data-phoenix.service.d/memory.conf
systemctl --user daemon-reload
systemctl --user cat solos-data-phoenix.service | grep DB_MEMORY   # the drop-in's line comes last and wins
```

A drop-in overrides the installed unit's `Environment=` line and survives copying a new unit from
`ops/` over it; remove it once the installed unit says 64GB. The new value takes effect only on a
restart, which needs the operator. A collector whose lanes both log `collector_error` every 30 s,
`rollback_failed` then `constraint`, has a poisoned DuckDB connection: only a restart recovers it
(`docs/issues/002-poisoned-connection.md`). Since 2026-10-17 the collector does that itself, see
below.

### Fatal errors restart the collector (since 2026-10-17)

An error after which DuckDB cannot be trusted (a failed rollback or "cannot continue", a failed
commit, or out of memory) ends the collector: the lane logs `collector_error`, then
`collector_fatal` with `lane`, `class` (`rollback_failed`, `commit_failed`, `oom`) and `exitCode`
70, and the process exits without touching the database again: the writer refuses every job after
the failing one, already queued ones included (`checkpoint connection poisoned: ...`). systemd restarts it after
`RestartSec` (15 s); DuckDB replays its write-ahead log and startup recovery removes files written
for a range that never registered. Other errors are retried after 30 s as before.

```sh
journalctl --user -u solos-data-phoenix -o cat --since -1d | grep -E '"event":"(collector_fatal|collector_started)"'
systemctl --user show solos-data-phoenix -p NRestarts -p ExecMainStatus
```

A few restarts a day are the mechanism working. Restarts every minute or two with the same class
mean the fault reproduces on a fresh connection (most likely `oom` in a maintenance pass): look at
the checkpoint size and the last `storage_maintenance` line before raising the memory.

### Memory

The service's `SOLOS_DATA_DB_MEMORY` is set twice on the box: by the drop-in `memory.conf`
(`Environment=`) and by `collector.env` (`EnvironmentFile=`). systemd applies the environment file
after `Environment=` lines, so the value in `collector.env` wins; change both. Since 2026-10-18 the
box runs 400GB with `MemoryMax=700G` (the in-service trim of the 1.2 TB checkpoint ran out of 64GB;
ADR-0008, "Checkpoint drain, 2026-10-18").

`SOLOS_DATA_DB_MEMORY=64GB` covers the tail and the archive lane writing together while the
checkpoint is trimmed: it then holds the hot window plus the unpublished rows of the tail cycle in
flight (a catch-up cycle lists all its signatures before its first chunk, about 26M rows for 1.5M
slots). What exhausted 32GB on 2026-10-14 and 64GB on 2026-10-16 was the checkpoint itself, no
longer trimmed from 2026-10-13 and grown to 1.12 TB: its primary-key indexes over hundreds of
millions of rows filled the buffer (ADR-0008, "Checkpoint growth, 2026-10-17"). If the checkpoint
grows again, check that `storage_maintenance` lines appear every minute before changing memory;
if it has already grown past a few million published rows, stop the collector and run
`collector repack --keep-hot` rather than letting the in-service trim drain it.

## Off-box backup

A nightly `rclone copy` sends the raw Parquet the raw catalog lists, the augment root, `config/`
and the `solos-*` user units to a private Cloudflare R2 bucket (ADR-0010). Not sent: decoded
Parquet (regenerable from raw), DuckDB checkpoints, unlisted raw files, `.readers/`, staging,
`*.env`, `~/research/`. It never deletes anything in the bucket: files the box retires stay
there. Bucket layout: `phoenix_raw/`, `augment/`, `config/`, `systemd-user/`, and
`catalogs/<UTC date>/{phoenix_raw,augment}/` (each night's catalogs).

Install once (rclone goes to `~/.local/bin`, checked against the release's `SHA256SUMS`):

```sh
sh ops/backup/install-rclone.sh
# The operator writes the bucket-scoped R2 token; never print the file.
grep -oE '^[A-Z0-9_]+=' ~/.config/solos-data/backup.env   # R2_ACCOUNT_ID R2_BUCKET R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
cp ops/backup/solos-data-backup.service ops/backup/solos-data-backup.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-backup.timer
```

The timer fires daily at 02:00 UTC (plus up to 30 minutes) and catches up after downtime. A run
is idle-priority I/O and `nice 10`, eight transfers, 40 MiB/s from 06:00 to 22:00 UTC and
unlimited at night (`SOLOS_BACKUP_BWLIMIT` overrides the rclone timetable). The first upload
of about 790 GB takes hours; start it detached and watch the logs rather than an SSH session:

```sh
systemctl --user start --no-block solos-data-backup.service
tail -f ~/.local/state/solos-data/backup/raw-files.log        # rclone stats every 10 minutes
journalctl --user -u solos-data-backup.service -o cat | grep '"event":"backup"' | tail -3
jq 'del(.steps)' ~/.local/share/solos-data/backup-status.json
```

`backup-status.json` has `state` (`running`, `ok`, `failed`), `files`, `bytes`,
`durationSeconds`, `mbPerSecond`, `errors`, `failedSteps`, `rawCatalogUploaded`,
`lastSuccessAt` and one entry per step. A raw file that compaction retires mid-run makes the run
take a fresh catalog snapshot and copy again (three passes at most); if files are still missing
the raw catalog is not uploaded and the run fails, leaving the previous catalog in the bucket.
The health timer's `health_services` line carries the backup timer's state and
`backupLastSuccessAt`. Without the credentials file the run fails with
`{"event":"backup_credentials","state":"missing"}`.

Tests and rehearsals take `SOLOS_BACKUP_DEST` (any rclone path, e.g. a local directory, which
skips the credentials), `SOLOS_BACKUP_DRY_RUN=1`, `SOLOS_DATA_HOME`, `SOLOS_BACKUP_STATUS` and
`SOLOS_BACKUP_LOG_DIR`, so a run against a scratch tree never touches the real status file.

**Cost.** Infrequent Access: about $0.01 per GB-month (about $8 a month at 790 GB), a 30-day
minimum per object, and retrieval billed per GB. Upload is a Class A request per object or part.
A nightly run lists the bucket and uploads the day's new files only; it never downloads.

**Restore rehearsal.** Downloads one raw table-epoch directory (at most
`SOLOS_RESTORE_MAX_MB`, default 1024) and `SOLOS_RESTORE_AUGMENT_FILES` (default 20) random
complete augment files into a temp directory, checks every SHA-256 against the bucket's
catalogs and prints one `restore_check` line:

```sh
sh ops/backup/restore-check.sh
```

**Full restore.** The raw catalog holds the box's absolute paths: an object's key is its path
with `$HOME/.local/share/solos-data/phoenix_raw/` replaced by `phoenix_raw/`. Augment catalog
paths are relative to the augment root. To restore, load the credentials as `backup.sh` does
(`. ops/backup/r2-env.sh` sets `$dest`), then copy down what is needed, for example
`rclone copy "$dest/augment" /restore/augment` or the files one catalog lists with
`--files-from-raw`. Use a dated catalog under `catalogs/` for an earlier state; the bucket also
holds retired compaction inputs, so copy what a catalog lists rather than a whole prefix.
Restored Parquet is readable directly; the collector cannot resume on it without a new
checkpoint.

**Rotate the token.** Create a new bucket-scoped token in the Cloudflare dashboard, run
`~/.local/bin/solos-backup-credentials` on the box to overwrite `backup.env`, run
`systemctl --user start --no-block solos-data-backup.service` and check that it ends `ok`, then
revoke the old token.

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

## CI

`.github/workflows/ci.yml` runs three jobs in parallel on GitHub-hosted runners: `fmt`
(`cargo fmt --all --check`), `clippy` (`cargo clippy --workspace --all-targets --locked -- -D
warnings`) and `test` (`cargo test --workspace --locked`, then `npm ci` and `npm run verify`).
On a pull request the npm steps run only when the change touches `src/`, `tests/`, `config/`,
the npm manifests, `tsconfig.json`, `crates/phoenix-codec/`, the Cargo manifests or the
workflow; every push to `main` runs them. A newer push to a pull request cancels its run in
flight.

`clippy` and `test` each cache `~/.cargo` and `target/` with `Swatinem/rust-cache`, keyed on
the job, the toolchain, the `CARGO_*` environment and `Cargo.lock` (a changed lockfile restores
the newest older cache and rebuilds only what changed). The two cannot share one `target/`:
check and test resolve different libduckdb-sys build-script units, so DuckDB would compile
twice anyway. CI builds without debuginfo (`CARGO_PROFILE_DEV_DEBUG=0`), which compiles
DuckDB's C++ without `-g`, and deletes DuckDB's `.o` files (already archived in
`libduckdb.a`) before the cache is saved. To bust the caches, delete them with
`gh cache delete --all -R GuiBibeau/solos-data`, or change the `CARGO_*` environment in the
workflow. A cold run compiles DuckDB once in each Rust job and takes about 16 minutes (the
caches are then about 0.5 GB and 0.3 GB); a warm run (full cache hit) takes about 4 minutes.
