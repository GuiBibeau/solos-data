# ADR-0018: Nightly random-kill loop

Status: accepted, October 30, 2026. Closes issue #61.

The crash-safety matrix (ADR-0016, issue #56) kills the process at seven named points inside
`publish_range`. That proves the commit order, but only the orderings someone named. A random
kill catches orderings nobody did. This note records the nightly loop that does that, on top of
the same harness, fixture source, restart predicate and byte-identical re-run check.

## Decision

The harness is `crates/solos-data/tests/nightly_random_kill.rs`, an ignored integration test
built with the `failpoints` feature through the same self dev-dependency as the matrix. It
spawns the real binary (`dev publish-fixture`, a test fixture compiled only under `failpoints`),
SIGKILLs it, restarts it, and asserts:

- After the kill, `recover_files` removes the interrupted publication's orphans and the restart
  predicate (ADR-0016) passes.
- The commit is atomic: either the watermark is `None` and no active file is registered, or the
  watermark is the tail slot and the manifest is byte-identical to an uncrashed golden run.
  A committed watermark without the full golden manifest is a crash-ordering bug.
- When the watermark was not committed, the re-run to completion is byte-identical to the golden
  run (same partition hashes and manifest) and duplicates no primary key.

`dev publish-fixture` generates a deterministic fixture source — one row per raw table per slot,
one slot per epoch, four epochs by default — and prints one progress line per published file.
The seed maps to a byte offset inside that output stream (`splitmix64`), and the reader thread
SIGKILLs the process the moment that byte arrives, so a seed deterministically selects the kill
point. A `delay` mode (kill after a seeded wall-clock delay) is also available but is only
approximately replayable.

Each iteration logs its seed. A failure reports the seed, and one command replays it:

```sh
SOLOS_DATA_NIGHTLY_SEED=<seed> cargo test --test nightly_random_kill -- --ignored --nocapture
```

The loop runs 200 iterations by default (`SOLOS_DATA_NIGHTLY_ITERATIONS`, 200-500 in CI). It is
offline, on temporary directories, and is not part of per-PR CI: the ignored test does not run
under `cargo test --workspace`, and the scheduled `nightly-random-kill` GitHub Actions workflow
(with `workflow_dispatch`) is the only place it runs automatically.

## Consequences

SIGKILL is process death, not power loss. The loop inherits ADR-0016's scope: it cannot catch a
missing fsync, because the page cache survives SIGKILL and the kernel still has the bytes. A
planted "write `W` before the Parquet rename" ordering bug is caught — the proof for this loop —
but a planted "publish before fsync" is not, by design.

The loop re-runs the fixture only when the watermark was not committed, so a kill that lands
after the atomic commit is asserted directly against the golden manifest instead of publishing
the range twice. The harness's own re-run is therefore idempotent; it never duplicates files.

The file name still carries a random UUID. Byte-identity is asserted through the partition
SHA-256 and the manifest (table, epoch, row count, hash, bounds), the same comparison the matrix
uses.
