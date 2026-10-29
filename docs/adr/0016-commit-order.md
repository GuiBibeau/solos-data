# ADR-0016: Commit order and the single-owner lock

Status: accepted, October 29, 2026. Records the collector's existing publication order. It does
not change that order.

AGENTS.md requires two durability rules: one supervised process owns DuckDB, and watermarks
advance only after durable files and range checks. This note is the numbered sequence later
crash-safety tests cite, and the lock that enforces the first rule.

## Single owner

`Store::open` and `Store::open_read_only` take a non-blocking exclusive `flock` on
`<root>/checkpoint.duckdb.lock` before DuckDB opens `<root>/checkpoint.duckdb`. The lock file is
mode `0600` and stays on disk after unlock. A second process fails with `StoreError::AlreadyOwned`
and does not open the database. The `Store` drops the connection first and the lock second. If the
process is killed, the kernel releases the flock; the next open can acquire it. That is process
death, not power loss.

Each checkpoint directory has its own lock file, so the collector, the decoder, and an augment
lane can run as separate processes on separate databases. Two opens of the same database cannot.

`repack_checkpoint` attaches the database file directly and does not take this lock. Its comment
says the writer must already be closed. That path is outside `Store::open`.

## Commit order

Collector publication is `validate_range` and `assert_publishable`, then `publish_range`
(`collector/pipeline.rs` for the tail watermark `W`, `collector/history.rs` for the `backfill`
cursor). `publish_range` is the same sequence in the Rust writer and in `src/writer.ts`.

1. Range validation runs on the checkpoint rows. On failure the function returns before any file
   is written, and the watermark is not advanced.
2. For each non-empty table and epoch in the range, `COPY` the rows to a `.tmp` sibling of the
   final Parquet path.
3. fsync that temporary file (`sync_path`).
4. Rename it onto the final path.
5. fsync the parent directory of that path (the `epoch=…` directory that received the rename).
6. Read the Parquet back and require its row count to equal the exported count, then SHA-256 the
   file.
7. In one DuckDB transaction, insert `files`, `file_bounds`, and `published_ranges`, and advance
   the checkpoint (`kv` key `W` on the tail, `backfill` on the historical lane). The watermark
   moves only in this transaction, after steps 2–6 for every file in the batch.

A crash after step 4 and before step 7 leaves a Parquet file that `files` does not list. The next
process's `recover_files` deletes unregistered `.parquet` and `.tmp` files. The watermark does not
include that range. Calling `recover_files` again deletes nothing further.

The decoder, augment, and spot writers use the same file steps (temporary file, fsync, rename,
fsync the parent) and then a transaction that records their own progress. The sequence above is
the one tests cite for the collector watermark.

## Gaps

These are differences from "write to a temporary file, fsync the file, rename, fsync the parent
directory, range-check, then advance the watermark." The code was not changed to match that shape.

- The range check is step 1, before any durable write. It is not repeated after step 5. The check
  between the directory fsync and the watermark is the exported row count (step 6). The hash is
  stored in that step, not compared to a previous hash.
- Step 5 fsyncs the file's parent directory. It does not fsync ancestor directories that
  `create_dir_all` just created (`staging/`, `staging/<table>/`). A new epoch directory's own
  directory entry is not fsynced.
- Step 7 does not fsync the DuckDB file after `COMMIT`. Watermark durability is DuckDB's commit.
- `catalog.json` is written later, by status and maintenance, and is not part of step 7. A restart
  trusts the committed `files` rows. The catalog can lag.
- `recover_files` removes unregistered Parquet and `.tmp` files. It does not remove a registered
  file whose bounds sit above the watermark, and publication names are unique, so a later chunk
  does not overwrite that file. The restart predicate does not trust those files. Deleting them
  is not implemented.

## Restart predicate

`restart::restart_predicate` checks the committed manifest. Every active `files` row whose
`file_bounds.slot_to` is absent or at or below `W.slot` must exist, parse, and match `row_count`
and `sha256`. A row with `slot_to` above `W` is an orphan and is not read. When `W` has not been
recorded, every active file is checked. The predicate does not delete orphans and does not advance
a watermark.
