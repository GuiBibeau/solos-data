# ADR-0016: Golden day hash is a content digest

Status: accepted, October 29, 2026. Closes the golden-day-hash item in
[docs/issues/001](../issues/001-phoenix-acceptance.md).

The same decoded input has to produce one hash across fresh runs and across a DuckDB toolchain
bump. ADR-0007 said byte-level Parquet equality was not a goal. The golden hash needs a stable
value anyway, and the file bytes are not that value.

## What varies

Decoded Parquet is written by DuckDB `COPY` in `decoder/publish.rs` and `decoder/compact.rs`.
Three things keep two writes of the same rows from sharing a file SHA-256:

- `decoded_transactions.decoded_at` is the wall clock (`now()`), one column on every transaction
  row. It is there so a reader can see when a row was decoded. Stripping it would change the
  column that already-sealed partitions store.
- The Parquet footer `created_by` is `DuckDB version <version> (build <source id>)`. The crate
  is pinned to `duckdb = "=1.4.5"`, and the build id still changes if that pin moves. DuckDB
  does not offer a switch to omit it.
- `ORDER BY slot, signature` is not a total order. Every event of one transaction ties, and
  DuckDB's sort does not keep input order for ties.

Catalog `at` and `files.created_at` are wall clocks too. They are not part of the Parquet
payload. The `sha256` column on `files` stays the raw file hash: V7 checks that the bytes on
disk are the bytes that were registered. That check is integrity, not replay.

## Decision

Pin the decoded writer to DuckDB 1.4.5's defaults, written out so a later default cannot move
them: ZSTD at level 3, row groups of 122880, Parquet V1, bloom filters on. `ROW_GROUP_SIZE_BYTES`
stays unset, which in 1.4.5 means unlimited while insertion order is preserved. Sort each table
by a total key: `slot, signature` for transactions, plus `instruction_path` and `event_ordinal`
(or `error_id`) for the other tables.

Hash a canonical content digest, not the file bytes. The digest reads the registered files the
way the reader does (latest revision of each key), projects columns in definition order,
drops `decoded_at`, sorts by that same key, and SHA-256s the rows. Footer `created_by` and the
catalog clocks never enter it. The committed value is `tests/data/golden-day.sha256`, from
`tests/data/phoenix-events.json` decoded in a temp directory.

`SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day` rewrites the digest after a
decoder or writer change that should move it. The test does not seal a partition.

The raw collector, the spot writer and the augment writer are unchanged. The TypeScript decoder
is the pre-cutover writer and is unchanged. The supervised writer after ADR-0007 is the Rust
binary, and the golden hash covers that path.

## Consequences

Explicit options that match the 1.4.5 defaults do not change file bytes by themselves. A unit
test writes the same rows with the pinned clause and with `FORMAT PARQUET, COMPRESSION ZSTD`
and requires equal SHA-256s. The same test shows two writes of fixed rows are byte-identical.

The decoder inserts events in instruction-path and ordinal order. On DuckDB 1.4.5, `ORDER BY
slot, signature` keeps that order for a tie, so adding the total key does not move those rows
and does not change their file bytes. A tie inserted out of that order does move: the same
writer produced different SHA-256s for `(ordinal 2, ordinal 1)` sorted only by `slot, signature`
versus the total key. Already-sealed files are not rewritten and not re-sealed. Their
registered SHA-256 remains the hash of the bytes that were written.

A replay of a sealed file is still not byte-identical to it. `decoded_at` is a new wall clock
on every write (that was already true), and `created_by` changes when the DuckDB build changes.
Compare replays with the content digest.
