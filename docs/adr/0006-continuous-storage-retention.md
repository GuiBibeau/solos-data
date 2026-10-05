# Continuous cleanup with a permanent raw archive

Status: accepted.

Keeping every published transaction in DuckDB as well as Parquet makes the writer
grow with the entire history. Retained compaction inputs add another copy. The
collector must keep the full history locally and continue live collection.

## Decision

Keep canonical raw Parquet permanently, including wire bytes, metadata, signatures,
block ordering, program attention events and original bulk responses. Decoded
events, fills, orders, settlements and quarantined payloads remain permanent too.
No transaction-age rule removes historical data.

Every minute, compact bounded newest prefixes and trim old checkpoint rows whose
entire contents match the latest registered archive. Keep 10,000 slots behind the
live watermark, never trim the active tail overlap, and never trim unpublished
work. Only a published range can be trimmed. Verify source SHA256 and row counts,
then compare a SHA256 digest of every checkpoint column with canonical archive rows
before deleting. Compute per-row hashes before revision sorting, keeping payload
strings out of the sorting working set. Merge adjacent published ranges for the
initial cleanup; ongoing passes trim at most a 16,000-slot interval. A durable round-robin cursor
prevents arriving older backfill rows from starving other cold ranges.
Archive bounds limit reads to overlapping files. Cursors and coverage stay durable.
Program-version attention rows remain in the checkpoint.

Raw status counters now describe the checkpoint working set; use the canonical
query command for the historical total. Retention metrics describe removed copies,
not deleted historical transactions.

Decoded compaction selects a newest prefix per table and epoch and retains its
maximum original batch ID, preserving correction precedence. Readers still join
events to the latest transaction source hash. Raw compaction records its input
hashes and counts, letting the decoder skip a merge only when all parents were
fully consumed. Incomplete parents still require reading the merged payloads.

Catalog writes, hashes/counts and replacement registrations are durable before
cleanup. Queries and decoder source reads acquire a local process lease before
reading a catalog. Cleanup publishes the current catalog first, waits until there
are no readers, observes a ten-minute grace, and verifies the surviving replacement
chain before unlinking inputs. Replacement links survive deletion and restart.
Readers of a read-only exported copy need writable space for the small `.readers`
directory; truly immutable mirrors must disable deletion upstream instead.

Optional offline `maintain --all --legacy` additionally removes legacy superseded files only when
all their columns still exist in the canonical archive. Changed payloads are kept.
Offline `repack` copies the closed checkpoint to a new DuckDB file, compares every
table through SHA256 whole-row digest multisets in both directions, flushes it and atomically replaces the old file. A live
writer prevents acquiring the DuckDB lock. Offline commands require services to stay stopped during the
rewrite. The supervised collector also rewrites automatically after allocation
exceeds both 16 GiB and twice its previous compact size. It queues all database
users, closes its sole connection, verifies the replacement, and reopens before
servicing queued work; Parquet queries remain available. A crash before the rename leaves the original; afterward it leaves the
verified replacement. Routine CHECKPOINT permits block reuse, while an offline
rewrite can shrink the file. [DuckDB space reclamation](https://duckdb.org/docs/stable/operations_manual/footprint_of_duckdb/reclaiming_space).

Decoded temporary batch tables are recreated between batches, releasing deleted
payload storage instead of retaining it indefinitely. Decoder maintenance also
checkpoints durable progress. Archive queries accept `SOLOS_DATA_QUERY_MEMORY`
(default 4GB) for larger analytical working sets.

Ordering commits now cover 256 slots instead of 64. Every multi-transaction slot
still requires finalized block membership and transaction-index checks. The CU
ceiling, reserved live capacity, retry/backoff and publication gates are unchanged.

## Acceptance

- Checkpoint trimming preserves historical query results, cursors, unpublished work,
  and live overlap; changed/corrupt archives reject deletion.
- File cleanup preserves active readers and rejects corrupt replacements.
- Compaction preserves latest revisions and failed-transaction fill exclusion,
  including after cleanup and restart.
- Offline and queued writer repack preserve all rows, views, cursors and primary-key constraints.
- Repeated decoded batches release previous temporary payload storage.
- Relocation rebases archive bounds and replacement lineage, including removed files.
- A 256-slot ordering fixture uses one durable commit and retains V6 checks.
- Production rollout compares canonical counts/fingerprints before and after cleanup,
  verifies registered file integrity, resumes both lanes and records storage/throughput.

Independent provider/fill comparisons, historical codec audit and reconstructed
books/positions/funding rates remain pending. Cleanup does not certify completeness.

## Production validation (2026-10-05)

A stopped-writer migration preserved 45,283,957 canonical raw transactions and the
same number of decoded transaction rows. Before/after fingerprints of transaction
identity/order and decoded revision/status/counts matched exactly. Full SHA256 and
row-count checks passed for all 2,775 active raw files and 11,369 decoded files.
Each checkpoint rewrite additionally compared SHA256 whole-row digest multisets
for every persisted table, including progress and constraints.

The raw checkpoint fell from 260,276,760,576 to 1,275,080,704 bytes; its verified
rewrite took 22.6 seconds. The decoded checkpoint fell from 21,133,012,992 to
13,340,258,304 bytes. Closing the old decoder released its accumulated temporary
spill files. Data-root sizes after migration were 65,481,489,416 raw bytes and
23,740,442,181 decoded bytes, including retained legacy inputs. Historical Parquet
was preserved. Both supervised processes restarted after validation.

Offline verification passed strict TypeScript, 47 JavaScript tests and one Rust
codec test, with all 47 JavaScript tests also passing on Linux. The container image
built and its CLI smoke test and Compose configuration validation passed.

A short throughput observation after restart covered 15 descending 1,000-slot
chunks (351,006 transactions): mean chunk time 13.2 seconds and about 1,771
transactions/second, compared with 29.3 seconds and about 635/second across
39 chunks during the preceding 20-minute observation. This is an early comparison
across different historical activity, not a controlled benchmark or long-term SLO.
Two automatic maintenance passes completed, live publication continued, and no
collector errors or provider throttles occurred in the post-restart sample.
The previously blocked partial range passed V1/V6 after the checkpoint rewrite.
