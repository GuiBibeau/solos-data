# A failed rollback poisons the collector's DuckDB connection

Found in the 2026-10-14 incident (ADR-0008, runbook "2026-10-14 incident"). Closed on 2026-10-17:
the collector exits (see Resolution).

When DuckDB runs out of memory inside a commit and the rollback fails too, it reports
`TransactionContext Error: Failed to commit: Failed to rollback transaction. Cannot continue
operation.` The collector logs a `collector_error` and retries after 30 s on the same connection,
which never recovers: every later write fails, the backfill with `Out of Memory Error` and the tail
with `Constraint Error: Duplicate key` on `bulk/tail/<from>/<to>` and on transaction signatures.
On 2026-10-14 this lasted 19.5 hours, until the operator restarted the service.

Making the tail's progress insert idempotent does not help: it is already
`INSERT OR REPLACE INTO kv` (`collector/bulk.rs`), and the signature insert already anti-joins
existing rows (`fetcher::insert_raw`). The duplicates come from the connection's index state after
the failed rollback, so no statement-level change can clear them.

Acceptance criteria:

- [x] An error whose message says the rollback failed or that DuckDB cannot continue ends the
  collector with a non-zero exit (a `collector_fatal` log line naming the lane and the class), so
  systemd's `Restart=always` reopens the database and replays its write-ahead log; or the writer
  thread reopens its connection in place. Either is decided with the operator first, since it
  restarts the collector without them.
- [x] A fixture that injects the failed-rollback error shows the chosen path, and that nothing is
  published between the failure and the recovery.
- [x] The memory sizing note in the runbook covers the tail and the archive lane writing together
  (64GB since 2026-10-15), or the lanes are throttled so they cannot exhaust the buffer together.

Until then `solos-data health` goes critical on the error streak (`rollback_failed`, then
`constraint` or `oom`) within one timer run, and the operator restarts the collector.

## Resolution, 2026-10-17

The operator asked for the collector to be fixed until it runs unattended, which settles the first
criterion: the collector exits. `collector::service::fatal_class` names three classes: a failed
rollback or "cannot continue" (`rollback_failed`), a failed commit (`commit_failed`) and DuckDB out
of memory (`oom`). The second incident, at 16:57 UTC on 2026-10-16, began with `Failed to commit:
Out of Memory Error` and then failed every statement with `Out of Memory Error` for 12 hours, the
buffer never released, so an out-of-memory error is fatal on its own. On any of them the lane logs
`collector_error` and `collector_fatal` (`lane`, `class`, `exitCode`) without touching the database
and the process exits with code 70. Jobs already queued on the writer thread when the failure
happens do not run either: the collector's writer is guarded (`Db::guard_fatal`), so the first job
that fails with one of these classes poisons it and every later job, queued or new, fails with
`checkpoint connection poisoned: <error>` (itself fatal) without touching the connection; systemd restarts it
after `RestartSec` (15 s), DuckDB replays its write-ahead log, and startup recovery removes files
written for a range whose registration never committed. Any other error is retried after 30 s as
before. `tests/collector_service.rs` injects the failed-rollback and out-of-memory messages: the
hook is called with the lane and class, no error row or published range is written, and a write
queued behind the failing job never runs. A
deterministic out-of-memory error would restart the collector in a loop; health shows the streaks
and the journal the `collector_fatal` lines.

Memory: the tail and the archive lane writing together exhausted 32GB and then 64GB because the
checkpoint was no longer trimmed (ADR-0008, "Checkpoint growth, 2026-10-17"), not because of what
the two lanes write per chunk. With trimming on its own schedule the checkpoint holds the hot window
and the unpublished rows of the tail cycle in flight; 64GB stays the setting (runbook, "Memory").
