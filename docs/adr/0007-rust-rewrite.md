# ADR-0007: One Rust binary, migrated in place

Status: accepted, October 5, 2026.

The TypeScript collector and decoder are rewritten as one Rust binary, `solos-data`, with the
same feature set, the same files on disk, and a cutover that reuses the running deployment's
checkpoints and cursors. The reasons are exactness, resource use and one artefact to deploy;
collection speed over RPC stays bound by the provider's compute-unit ceiling and is not a goal
of this change (see ADR-0008 for the archive lane that changes that bound).

## Decision

- **Surface.** `solos-data collector <command>` and `solos-data decoder <command>` carry every
  command, flag and environment variable of the two TypeScript entry points. Logs, `status.json`,
  `catalog.json`, Parquet layouts, the `.readers` lease directory and the `kv` progress records
  keep their names, fields and order. `solos-data dev` adds migration tooling only.
- **Storage engine.** DuckDB through the `duckdb` crate pinned to the 1.4 line (`=1.4.5`,
  `bundled, json, parquet`). Extension auto-install and auto-load are off; the storage version
  is never set, so files written by the Node writer (DuckDB 1.4.4, storage version 64) open
  unchanged and the Node binary can reopen them after a rollback. Parquet is still written by
  DuckDB `COPY`. The hot-store churn observed before the cutover (checkpoint rewrites every
  fifteen minutes) is inherited on purpose; replacing the hot store is a separate decision.
- **Exactness.** Provider JSON is stored as the text the provider sent, not a re-serialization
  through IEEE doubles. Row rendering follows `getRowObjectsJson()` (64-bit integers as decimal
  strings) and the decoder's content hash reproduces `JSON.stringify` of the TypeScript decoder,
  so revisions continue across the cutover. A transaction republished across the 150-slot tail
  overlap at cutover may receive one extra revision; readers already select the latest.
- **Envelopes.** Legacy, v0 and v1 (SIMD-0385) transactions are parsed structurally through the
  Solana SDK's `wincode` reader without `sanitize()`, matching `@solana/kit`. The crate line is
  pinned to the one Jetstreamer locks so the workspace has one type graph.
- **Codec.** `phoenix-rise-events 0.6.12` is linked into the decoder; the stdio codec binary stays
  only while the TypeScript decoder exists.
- **Known defect kept.** `fills.taker_side` compares the SDK's lowercase side against `Bid` and is
  therefore always `Bid`; the correction is a separate, data-affecting change.
- **Concurrency.** One writer thread owns each checkpoint and executes closures sent by the
  asynchronous lanes, the TypeScript `exclusive` queue made explicit. RPC calls, the limiter
  (same token bucket, 20 % tail reserve, idle borrowing, AIMD) and the lanes run on tokio.
- **Delivery.** Five pull requests: workspace and codec library; decoder with a differential
  proof; collector with the ported tests and a shadow run; cutover (units, image, runbook, this
  record); TypeScript removal after seven stable days.

## Migration

Preflight opens both checkpoints read-only and checks the storage-version tag, the schema and the
cursors. The decoder is stopped, then the collector (an interrupted chunk replays). Both
`checkpoint.duckdb` files with their WAL, both catalogs and both status files are copied to a
dated backup directory. The unit files' `ExecStart` switch to the Rust binary; the collector
starts, then the decoder. The first `backfill_chunk`, `tail_chunk` and `decoded_batch` events and
the `query` row counts against the pre-cutover numbers close the cutover. Rollback is the reverse
`ExecStart` swap; the Node runtime stays installed for seven days.

## Consequences

Parity is proven by ported tests (the TypeScript suites one to one), by `dev compare-decoded`
over two decoded roots, and by a shadow collector on a copy of the raw checkpoint compared row
for row with the live collector's files. Byte-level Parquet equality is not a goal: files are
hashed at creation; rows are what the proofs compare. Nested objects embedded from DuckDB rows
keep column order; JSON values nested inside them sort their keys.

## Production validation (2026-10-05)

Shadow proof: a Rust collector on a seeded scratch root re-collected slots 452,396,000 to
452,400,999 over RPC at 1,000 CU/s and stopped cleanly on SIGTERM; its 45,810 transactions over
2,966 slots matched the live archive in ordering, wire bytes, `meta` text and every column, and
the signature manifests matched. Decoder proof: both decoders ran on the same raw file; all six
tables were identical, as were `status.json`, `catalog.json` and query output.

Cutover at 12:53 UTC: both services stopped, 190 GB of checkpoints, WAL, catalogs and status files
backed up, unit files swapped, services started at 12:55. Pre-cutover state: watermark
453,584,819, backfill cursor 450,228,136 after 2,945 chunks, 70,672,864 decoded transactions. The
first live tail chunk published within four seconds of start; the decoder resumed with
10,000-transaction batches. The TypeScript decoder had exited with status 1 on stop because it
killed its codec child mid-batch; the batch had not reached its publish transaction and was
replayed. One operational note: eleven provider throttles in the first minutes, inherited from
parallel proof runs, left the limiter's additive recovery at 188 CU/s; a service restart resets
the rate (done at 13:04).

