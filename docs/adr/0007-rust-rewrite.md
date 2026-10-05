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
