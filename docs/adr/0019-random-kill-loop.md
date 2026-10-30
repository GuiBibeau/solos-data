# ADR-0019: Nightly random-kill loop

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

- After the kill, the harness restarts the binary, whose production startup recovery
  (`collector/service.rs`) removes the interrupted publication's orphans; the harness does not
  call `recover_files` itself. No orphan `.parquet` or `.tmp` file may remain, and the restart
  predicate (ADR-0016) passes.
- The commit is atomic: either the watermark is `None` and no active file is registered, or the
  watermark is the tail slot and the manifest is byte-identical to an uncrashed golden run.
  A committed watermark without the full golden manifest is a crash-ordering bug.
- When the watermark was not committed, the re-run to completion is byte-identical to the golden
  run (same partition hashes and manifest) and registers exactly one file per partition.

`dev publish-fixture` generates a deterministic fixture source (one row per raw table per slot,
one slot per epoch, four epochs by default) and prints one progress line per publication step
(`tmp-write`, `tmp-fsync`, `rename`, `dir-fsync`, `readback`, `commit`, and `committed` after the
atomic commit). A seed picks a trigger point (a publication stage, then a progress line of that
stage), and the harness schedules the SIGKILL the moment that line's bytes arrive plus a seeded
jitter. A seed therefore picks a trigger point deterministically, but the kill still races the
child: the jitter and the child's own progress decide exactly where inside the step it lands.
The jitter budget is pinned by `SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS` when set (defaulting to the
golden run's measured duration over 8, clamped to [2, 25] ms), so a replay pins it too.
The stage is drawn before the line so the single `commit` and `committed` lines are not starved
by the per-file stages' many lines. A `delay` mode (kill after a seeded wall-clock delay scaled
to the golden run's measured duration) is also available but is only approximately replayable.

Each iteration is classified (killed or completed, the last progress line seen, and whether the
watermark was committed), and the harness prints a histogram and asserts a kill lands in every
publication stage at least once and that the vacuous `completed-before-kill` class stays under a
cap.

Each iteration logs its seed. A failure reports the seed, and one command replays it with the
seed, mode, epochs, jitter budget and delay cap pinned:

```sh
SOLOS_DATA_NIGHTLY_SEED=<seed> SOLOS_DATA_NIGHTLY_KILL=<mode> SOLOS_DATA_NIGHTLY_EPOCHS=<epochs> \
  SOLOS_DATA_NIGHTLY_JITTER_BUDGET_MS=<ms> SOLOS_DATA_NIGHTLY_MAX_DELAY_MS=<ms> \
  cargo test --test nightly_random_kill -- --ignored --nocapture
```

The loop runs 200 iterations by default (`SOLOS_DATA_NIGHTLY_ITERATIONS`, 200-500 in CI). It is
offline, on temporary directories, and is not part of per-PR CI: the ignored test does not run
under `cargo test --workspace`, and the scheduled `nightly-random-kill` GitHub Actions workflow
(with `workflow_dispatch`) is the only place it runs automatically. On failure the workflow
uploads the seed, the harness log and the iteration root with `actions/upload-artifact`.

## Consequences

SIGKILL is process death, not power loss. The loop inherits ADR-0016's scope: it cannot catch a
missing fsync, because the page cache survives SIGKILL and the kernel still has the bytes. A
planted "write `W` outside the file transaction (after the Parquet renames)" ordering bug is
caught (the proof for this loop, committed as
`patches/0019-planted-w-outside-transaction.patch`); a planted "publish before fsync" is not, by
design.

The loop re-runs the fixture only when the watermark was not committed, so a kill that lands
after the atomic commit is asserted directly against the golden manifest instead of publishing
the range twice. The harness's own re-run is therefore idempotent; it never duplicates files.

The file name still carries a random UUID. Byte-identity is asserted through the partition
SHA-256 and the manifest (table, epoch, row count, hash, bounds), the same comparison the matrix
uses.
