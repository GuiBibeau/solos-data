# ADR-0005: Bound storage work as the dataset grows

Accepted October 4, 2026. Provider throughput alone does not determine collection
speed. Small conflict-handling inserts and ordering updates scanned the entire
accumulated transaction table. Deep decoder resume sorted wide transaction rows
and exhausted its memory limit on a multi-million-row compacted source.

Under the existing serialized writer lock, check duplicates only within the
validated slot window and use normal primary-key-checked inserts. A mismatched
existing identity remains a hard constraint failure. Manifest address updates and
ordering replacements also stay within their slot window. Raw page/cursor commits
and finalized block-order verification remain atomic and unchanged.
Each bulk window commits its first page immediately, then up to ten pages per
transaction. Store all their raw responses, transaction rows and the last cursor
together. A failed or interrupted batch re-fetches from the last durable cursor.
This reduces small commit/checkpoint overhead without weakening restart safety.
Use a configurable 256 MB automatic checkpoint threshold instead of the 16 MiB
default. Commits still persist the write-ahead log; SIGKILL recovery must retain
committed manifests/progress and discard unfinished transactions. Retain the WAL
with its database when copying a stopped database that has not checkpointed.

Decoder resume selects narrow ordering keys and physical Parquet row numbers,
then loads only that batch's full payloads and processed identities. Retain the
original descending logical row offsets, including legacy partially consumed files
and unsorted inputs. Bound processed revision updates to the batch's signatures.
Empty replay batches commit progress without rewriting the public catalog.
Choose unconsumed sources by their newest transaction range, with publication
time breaking ties. Freshly written historical files cannot take priority over
live transactions. Cache the actual newest slot for legacy compacted sources.
Use 10,000-transaction decode batches to amortize publication and catalog writes.

Compaction selects a newest publication prefix containing at least ten files and
at most 250,000 input rows and 256 MiB of compressed input. Never merge an older subset above an excluded newer
revision. Large existing compacted bases stay registered and readable. Superseded
files remain retained. A live catch-up publishes each 1,000-slot chunk rather than
waiting for an entire large cycle. Persistent tail checkpoints remain compatible.

Use native offline storage benchmarks and failure/restart regression fixtures.
Status snapshots include statement counts, total time and worst time by SQL
operation/table, without statement text, parameters or provider details.
Validate both lanes and decoder progress on the accumulated production dataset.
Short benchmark improvements alone do not establish a sustained freshness SLO.

DuckDB supports physical row numbers and projection/filter pushdown:
[Parquet documentation](https://duckdb.org/docs/current/data/parquet/overview).
The checkpoint threshold is described in the
[configuration reference](https://duckdb.org/docs/current/configuration/overview).
