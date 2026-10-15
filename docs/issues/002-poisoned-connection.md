# A failed rollback poisons the collector's DuckDB connection

Found in the 2026-10-14 incident (ADR-0008, runbook "2026-10-14 incident"). Open.

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

- [ ] An error whose message says the rollback failed or that DuckDB cannot continue ends the
  collector with a non-zero exit (a `collector_fatal` log line naming the lane and the class), so
  systemd's `Restart=always` reopens the database and replays its write-ahead log; or the writer
  thread reopens its connection in place. Either is decided with the operator first, since it
  restarts the collector without them.
- [ ] A fixture that injects the failed-rollback error shows the chosen path, and that nothing is
  published between the failure and the recovery.
- [ ] The memory sizing note in the runbook covers the tail and the archive lane writing together
  (64GB since 2026-10-15), or the lanes are throttled so they cannot exhaust the buffer together.

Until then `solos-data health` goes critical on the error streak (`rollback_failed`, then
`constraint` or `oom`) within one timer run, and the operator restarts the collector.
