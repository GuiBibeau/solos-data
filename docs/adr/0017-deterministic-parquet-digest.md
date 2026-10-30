# ADR-0017: Golden day hash is a content digest

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
disk are the bytes that were registered. That check is integrity, not replay. The decoder
copies that `sha256` into `decoded_transactions.source_file`; it is provenance (which raw file
produced the row), not decoded content, so the content digest omits it like `decoded_at`.

## Decision

Pin the decoded writer to DuckDB 1.4.5's defaults, written out so a later default cannot move
them: ZSTD at level 3, row groups of 122880, Parquet V1, bloom filters on. `ROW_GROUP_SIZE_BYTES`
stays unset, which in 1.4.5 means unlimited while insertion order is preserved.

Sort each table by a key that ends in its primary key: `signature`, `event_id`, or `error_id`.
`event_id` is `signature/instruction_path/event_ordinal` and a base58 signature contains no `/`,
so the key is one event. The publisher and the digest refuse a second row with that key in the
same batch. A repeated key is not hashed: the order of a tie is not a function of the rows, and
the refusal is the no-duplicate-primary-key check. A later batch may carry a revised row; the
reader still keeps the newest.

`files.sha256` stays the SHA-256 of the file bytes. That is the bit-rot check (V7): the bytes
on disk are the bytes that were registered. It is not the equivalence check.

The equivalence check is a content digest. It hashes, in order, the schema (column name, type,
and order, including `decoded_at` and `source_file`) and the reader rows. Every field is a tag.
Variable-length payloads are a big-endian `u32` length plus bytes. `NULL` is its own tag.
Integers are 16-byte big-endian. Floats are IEEE-754 bits. Decimals are one normalized digit
string. `decoded_at` and `source_file` keep their schema slots and are omitted tags, not the
clock or the raw-file byte hash. Joined strings are not hashed. Footer `created_by` is not
read.

The golden fixture sets `decoded_at` to `2026-10-04T00:00:00.000Z` and compares both the digest
and every registered file's SHA-256. On this DuckDB build the file hashes catch a writer-option
change that would leave the rows, and therefore the digest, unchanged. Production does not set
the fixture clock. The committed record is `tests/data/golden-day.sha256`.

`SOLOS_DATA_REFRESH_GOLDEN_DAY=1 cargo test --test golden_day` rewrites that file after a
decoder or writer change that should move either hash. The test does not seal a partition.

The raw collector, the spot writer and the augment writer are unchanged. The TypeScript decoder
is the pre-cutover writer and is unchanged. The supervised writer after ADR-0007 is the Rust
binary, and the golden hash covers that path.

## Consequences

Explicit options that match the 1.4.5 defaults do not change file bytes by themselves. A unit
test writes the same rows with the pinned clause and with `FORMAT PARQUET, COMPRESSION ZSTD`
and requires equal SHA-256s. The same test shows two writes of fixed rows are byte-identical.

The decoder inserts events in instruction-path and ordinal order. On DuckDB 1.4.5, `ORDER BY
slot, signature` keeps that order for a tie, so adding the primary key does not move those rows
when they were already unique and in key order. A tie inserted out of that order does move.
Already-sealed files are not rewritten and not re-sealed. Their registered SHA-256 remains the
hash of the bytes that were written.

A production replay is still not byte-identical to a sealed file: `decoded_at` is a new wall
clock on every write, and `created_by` changes when the DuckDB build changes. Compare replays
with the content digest. The golden fixture is the exception, because its clock is fixed and
its file hashes are committed for this DuckDB build.

Production `decoded_at` is taken once per batch in the decoder service, not once per row. Every
row published from that batch shares the instant. The golden fixture replaces the batch clock
with `2026-10-04T00:00:00.000Z`.

The digest reads every registered row into JSON before encoding it. That is enough for the
fixture day. It is not acceptable for a production day: streaming the digest, so the day is not
held as JSON, has to land before this digest runs on production days.

The digest encoding changed when the schema and the length-prefixed fields replaced the earlier
JSON concatenation (`63c06f73d2691e6d8bea9cb45a44505403002cc47fe2c001e376b095b889315d`). The
committed golden file was refreshed with the new digest and the file hashes.
