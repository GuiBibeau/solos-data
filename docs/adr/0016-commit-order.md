# ADR-0016: Commit order and the single-owner lock

Status: accepted, October 29, 2026. Records the collector's existing publication order. It does
not change that order.

AGENTS.md requires two durability rules: one supervised process owns DuckDB, and watermarks
advance only after durable files and range checks. This note is the numbered sequence later
crash-safety tests cite, and the lock that enforces the first rule.

## Single owner

`Store::open` and `Store::open_read_only` take a non-blocking exclusive `flock` on
`<root>/checkpoint.duckdb.lock` before DuckDB opens `<root>/checkpoint.duckdb`. A newly created
lock file is mode `0600`; an existing one is not chmodded. After the lock is held, the
descriptor's inode is compared with the path, and a mismatch is retried. The file stays on disk
after unlock. A second process fails with `StoreError::AlreadyOwned` and does not open the
database. The `Store` drops the connection first and the lock second. If the process is killed,
the kernel releases the flock; the next open can acquire it. That is process death, not power
loss.

Each checkpoint directory has its own lock file, so the collector, the decoder, and an augment
lane can run as separate processes on separate databases. Two opens of the same database cannot.
A second `flock` of the same file conflicts inside one process too, so code that already holds
the lock must pass that `DbLock` through instead of locking again.

`repack_checkpoint` takes `&DbLock`, which records the lock file's `(dev, ino)`; `assert_root`
compares the requested root's lock-file inode with the held one rather than path text, so the same
checkpoint reached through a symlink still matches while `repack_checkpoint(root_a, &lock_b)` is
refused. The CLI `decoder repack` and `collector repack` paths call `acquire_lock` and pass it.
`Store::while_closed` already holds the lock, closes the connection, and passes that same lock into
the job; the in-service checkpoint rewrite uses that path and does not lock again. If reopening the
checkpoint afterwards fails, the store is marked unusable and later calls fail instead of writing
to the in-memory placeholder.

The TypeScript offline commands that open `<dataDir>/checkpoint.duckdb` (collector `maintain`,
`repack`, `relocate`, `repair-next-backfill`, `validate-next-backfill`, `verify-storage`,
`storage-stats`, `benchmark-ordering`, `benchmark-ingest`, and decoder `once`, `watch`, `repack`,
`verify-storage`) re-exec themselves under `flock -n -x <dataDir>/checkpoint.duckdb.lock`
(`src/lock.ts`) before DuckDB is opened, the same lock file and exclusive non-blocking semantics as
`DbLock`; `flock` refuses (the wrapper prints `checkpoint already owned`) when the lock is held,
so these commands never open a locked checkpoint. Read-only commands that do not open the
checkpoint (`status`, `catalog`, `query`, `benchmark-decoder`) take no lock, and `probe` locks its
own `<dataDir>/probe` subdirectory. The retired lockless live collector is described under Commit
order.

## Commit order

Collector publication is `validate_range` and `assert_publishable`, then `publish_range`
(`collector/pipeline.rs` for the tail watermark `W`, `collector/history.rs` for the `backfill`
cursor). The TypeScript live collector (`npm start` → `src/cli.ts run` → `tailCycle`/`backfill` →
`src/writer.ts publishRange`) is retired: it used a plain `mkdir` and never took
`checkpoint.duckdb.lock`, so it was outside the single-owner guarantee. `publishRange` remains
only as an offline test fixture, not a shipped path. This sequence is the Rust writer.

1. Range validation runs on the checkpoint rows. On failure the function returns before any file
   is written, and the watermark is not advanced.
2. For each non-empty table and epoch in the range, create the epoch directory with
   `create_dir_all_durable`. Each newly created directory's parent is fsynced, including ancestors
   that did not exist (`staging/`, `staging/<table>/`). The parent of the last existing ancestor is
   also fsynced, so a crash between creating a directory and fsyncing its parent is repaired by the
   next run. Then `COPY` the rows to a `.tmp` sibling of the final Parquet path.
3. fsync that temporary file (`sync_tmp_file`).
4. Rename it onto the final path.
5. fsync the parent directory of that path (the `epoch=…` directory that received the rename,
   `sync_dir_after_rename`).
6. Read the Parquet back and require its row count to equal the exported count, then SHA-256 the
   file.
7. In one DuckDB transaction, insert `files`, `file_bounds`, and `published_ranges`, and advance
   the checkpoint (`kv` key `W` on the tail, `backfill` on the historical lane). The watermark
   moves only in this transaction, after steps 2–6 for every file in the batch.

A failed fsync (step 2's parent, step 3, or step 5) is fatal. The error message starts with
`fsync failed`. `fatal_class` returns `fsync`, the lane does not record a retry, and the process
exits so the supervisor restarts it from `W`. The same rule applies to every temporary-file fsync
and every directory fsync after a rename, and to the fsync inside `write_durable`.

The decoder (`decoder/publish.rs`), augment (`augment/parquet.rs`), spot (`spot/write.rs`) and raw
compactor (`collector/compactor.rs`) create their output directories with `create_dir_all_durable`
and use the same file steps (temporary file, fsync, rename, fsync the parent).

A crash after step 4 and before step 7 leaves a Parquet file that `files` does not list. The next
process's `recover_files` deletes unregistered `.parquet` and `.tmp` files. The watermark does not
include that range. Calling `recover_files` again deletes nothing further. The sequence above is
the one tests cite for the collector watermark.

## Gaps

These are differences from "write to a temporary file, fsync the file, rename, fsync the parent
directory, range-check, then advance the watermark." The code was not changed to match that shape.

- The range check is step 1, before any durable write. It is not repeated after step 5. The check
  between the directory fsync and the watermark is the exported row count (step 6). The hash is
  stored in that step, not compared to a previous hash.
- Step 7 does not fsync the DuckDB file after `COMMIT`. Watermark durability is DuckDB's commit.
- `catalog.json` is written later, by status and maintenance, and is not part of step 7. A restart
  trusts the committed `files` rows. The catalog can lag.

## Restart predicate

`restart::restart_predicate` checks the committed manifest. An active `files` row whose
`file_bounds.slot_to` is greater than `W.slot` is a violation (`AboveWatermark`): `W` and the file
rows commit in one transaction, so that row cannot be legitimate. Every other active row must
exist, match `sha256`, parse, and match `row_count`. The hash is compared before the file is
parsed, so a same-size bit flip is a hash mismatch. When `W` has not been recorded, every active
file is checked. Parquet that `files` does not list is still an orphan. `recover_files` deletes
those, matching paths with `manifest_path` (relative paths join the root, then symlinks are
resolved) so a relative registration or a symlinked data directory is not deleted. The predicate
does not delete files and does not advance a watermark.

## Crash-safety matrix (issue #56)

The kill-point matrix (`tests/failpoints.rs`) proves the order above against process death. Seven
named kill points sit in `publish_range`: `publish/after-tmp-write`,
`publish/after-file-fsync`, `publish/before-dir-fsync`, `publish/after-dir-fsync`,
`publish/after-range-check`, `publish/before-watermark-commit`, and
`publish/before-watermark-write`. Each point aborts the process (`std::process::abort`, SIGABRT);
none returns an error, because an error path runs destructors and is not a crash. They are
compiled in only under the `failpoints` cargo feature, which release builds leave off. Before
aborting, an armed point writes the point's name to the file named by
`SOLOS_DATA_FAILPOINT_MARKER`, so the harness asserts on that marker, not only on death by
signal.

The harness spawns the real binary against a fixture source, arms one point, restarts after the
kill, and asserts the restart predicate. After each kill it also asserts that the orphan Parquet
and `.tmp` files left by the interrupted publication are removed by `recover_files` before the
rerun. A re-run to completion is byte-identical to an uncrashed golden run: the same partition
hashes and manifest, and no duplicated primary key. The matrix runs twice, with DuckDB's default
checkpointing and with `SOLOS_DATA_WAL_AUTOCHECKPOINT` forced tiny, so kills land on both sides
of a checkpoint.

Power loss is out of scope. SIGABRT/SIGKILL is process death; the kernel keeps the page cache and
releases the flock, so the files and lock behave as after a supervised restart. A power loss that
drops the page cache can still lose a directory entry whose fsync had not completed; the dir
fsync step exists for that case but is not what the matrix proves.

## Fatal classes in the augment lane

The augment lane's `series::is_fatal` ends the lane on every error `fatal_class` classifies:
`fsync`, `rollback_failed`, `commit_failed` and `oom`, not only on a failed fsync. That is
intended. A failed rollback or commit leaves the DuckDB connection untrusted
(`docs/issues/002-poisoned-connection.md`), and DuckDB out of its buffer is the same class; none
of them should be counted and retried in place. The spot and decoder lanes apply the same
classification through `fatal_class`, so one rule covers every lane.
