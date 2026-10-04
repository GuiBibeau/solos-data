# ADR-0003: Live decoded Parquet and DuckDB

Accepted October 4, 2026. Current data should be immediately useful while history
fills backward. Select Parquet with ZSTD as canonical output and DuckDB as the SQL
engine. Keep the raw collector and decoder independently supervised, each owning
its own checkpoint database. The decoder requires no RPC credential or network
access: it reads immutable, registered raw transaction publications.

Use the official MIT Phoenix Rise Rust event codec, pinned to 0.6.12 with a Cargo
lockfile. Do not confuse Phoenix Perps/Rise with the older Phoenix spot SDK. Decode
legacy and length-guided event batches with parse_with_errors; additionally check
complete envelopes. Quarantine an entire instruction if any event cannot decode,
so unknown event types cannot shift ordinal identities. Keep the encoded payload
and decoder version available for later reprocessing. Current SDK compatibility
does not establish compatibility with every historical program upgrade.

Identity is signature/instruction stack path/event ordinal. CPI stack heights
establish ownership; missing attribution remains explicit and is excluded from
analytic tables. Failed transaction attempts remain in generic events and never
become committed fills/order/funding rows. All serialized event integers become
decimal strings before crossing into JavaScript; typed Parquet columns use signed
or unsigned 64-bit integers. Native ticks and lots retain their header conversion
metadata. No rounded USD values or inferred state is published.

Select unconsumed raw files by publication time, newest first, processing bounded
batches. File hash and row offset are atomic checkpoints. Deduplicate transactions
by content hash including codec/schema version and ordering metadata; revised
transactions publish a new status record. Readers join events to the latest status
hash, eliminating stale revisions, including fills removed by a correction.

Fsync and verify Parquet files before committing file registrations and progress.
Recovery removes unregistered files and rebuilds the catalog. Catalog paths are
relative to the decoded root. A Docker image/Compose definition carries the same
code and codecs to another Linux machine. Stop writers before copying checkpoints;
SQL readers need only the catalog and registered Parquet files. The native raw
collector and Docker collector must not share an active checkpoint directory.

Initial tables: decoded_transactions, events, fills (orderbook and spline),
order_events, funding_events, decode_errors. Reconstructed positions, order books,
funding rates, features and independent completeness checks remain acceptance
work. Shared ClickHouse ingestion can be added later without changing canonical
storage. Do not add a database server before the access/query load requires one.
