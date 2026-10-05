# How it works

The project has two separate jobs. Each has its own DuckDB checkpoint database.
The final tables are Parquet files, so moving them does not require a database server.

## Collect raw transactions

The collector records a starting slot once. It then runs two lanes:

- Live collection follows new finalized slots, with a small overlap to catch retries.
- Backfill starts at the recorded slot and moves backward in small batches.

It first lists signatures from standard RPC. This list is the reference for missing
transactions. It fetches full transactions with Alchemy's bulk API and retries any
remaining signatures with standard `getTransaction`.

Requests share a CU limiter. Live collection has reserved capacity. Independent
bulk windows may run in parallel, but each window saves its pagination token with
its transaction rows. HTTP rate limits reduce the rate and concurrency.

For slots with multiple program transactions, a finalized block response gives
their order. A range is published only after every listed transaction is fetched
and ordering checks pass. Files are synced, hashed and registered before progress
moves forward or backward. Interrupted work resumes from saved progress.

Raw tables retain base64 transaction bytes, RPC metadata, failed transactions,
block indexes and original bulk responses. Legacy, v0 and v1 wires are supported.

## Decode events

The decoder reads published raw files. It uses the pinned Phoenix Rise Rust SDK
to decode event instructions, including CPI instructions. It needs no RPC key.

Failed attempts remain in `events`, marked `committed=false`. Only committed events
enter fill, order and funding tables. Unknown or incomplete payloads are kept in
`decode_errors` so a later codec can retry them.

Amounts and sequence numbers retain exact 64-bit values. JSON integers are decimal
strings. Funding rows describe settlements, not a market funding-rate time series.

Each transaction has a content hash and decoder version. Corrections produce a new
revision. SQL readers select the latest revision and exclude outdated event rows.

## Read and move data

`catalog.json` lists published files and their hashes. Query commands open those
files in a separate in-memory DuckDB. They can run while collection continues.

Stop writers before moving full checkpoint directories. Readers need only the
catalog and its registered files. See [server setup](../ops/runbook.md) and
[decoded tables](decoded-tables.md) for commands.

The current code checks fetch completeness, ordering and file integrity. Independent
provider/fill comparisons and full historical codec validation are unfinished.
No partition is certified complete. The [work list](issues/001-phoenix-acceptance.md)
records the missing checks, snapshots and reconstructed state.

## Code map

| Path | Job |
| --- | --- |
| `src/` | RPC collection, checkpoints, validation and raw publication |
| `src/decode/` | Event extraction, decoded tables and readers |
| `crates/phoenix-codec/` | Rust library and stdio binary around the official Phoenix event SDK |
| `crates/solana-wire/` | Rust parser for legacy, v0 and v1 transaction envelopes |
| `crates/solos-data/` | Rust `solos-data` binary: `collector` (RPC tail and backfill, Jetstreamer archive lane behind the `archive` feature), `decoder`, and `dev` tooling (`compare-decoded`, `crash-writer`) |
| `tests/` | Offline fixtures and interruption/restart tests |
| `config/` | Rates, batch sizes and default data paths |
| `ops/` | Linux services and installation notes |
