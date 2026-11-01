# Hyperliquid l2Book real-archive revision: open items

Follow-ups to this branch's revision of `solos-data augment hl-l2book`
(ADR-0021). The items resolved from the PR #97 review list of `aa3aeb6` are
recorded here rather than in that PR's file. The throughput items are the
accepted deviation from this branch's brief; everything else is non-blocking.

## Throughput levers (the 20 MB/s deviation)

Measured 13.9 MB/s of lz4 at twelve workers on the shared box (ADR-0021 has
the table); the serial per-hour work is the bottleneck, roughly 13-15 s of
single-threaded Rust per all-coin hour. Levers, in the order they should be
tried:

- [ ] Drop the double `serde_json` parse per line. `count_snapshot` and
  `snapshot_of_line` each parse the full line; the count guard could count
  structurally (one parse feeding both) or the two passes could be merged
  behind the same independence argument, measured before and after.
- [ ] Re-measure the ZSTD level. Level 16 costs roughly 13x the CPU of level 3
  for about 12 percent smaller output on these hours; if the ratio target
  holds at 9-12 with the double-parse fix, the encode stops dominating the
  DuckDB side. Pick by measurement, not by the worst single 2023-era hour.
- [ ] Parallelize within an hour, per coin: one file per coin means the
  decode, digest, and spill of a 49-coin hour can run across coins, not just
  across hours. This is the lever that helps the tail hours (the last worker
  finishing a 43 MB hour while others idle).

## Resolved on this branch

- [x] A book missing from the MANIFEST aborted the whole run. An in-scope
  finished `.lz4` with no manifest row now makes its hour pending, and
  `augment hl-l2book-manifest` builds the sha256 manifest from the downloader
  TSV (MD5 etag as a gate only). `.part` files are never read. The mixed-hour
  guard is pinned at one and four jobs.
- [x] No directory fsync after the raw unlinks; now fsynced per parent, with a
  mutation test.
- [x] Duplicate `time_ms` aborted the hour; `DuplicateRule` now drops identical
  books and keeps distinct ones in archive line order, and the COPY's `_seq`
  tiebreak is pinned structurally and behaviorally.
- [x] The git sha was never embedded (build.rs looked for `<crate>/.git`);
  now resolved by walking ancestors and worktree pointers, tested against
  `git rev-parse HEAD`.

## Non-blocking

- [ ] `DuplicateRule` is book identity (same coin, `time_ms`, and levels), not
  byte identity: it ignores `recv_time_ms`, so a re-send under a later
  envelope time is `Identical`. `A, B, A` within one `time_ms` drops the final
  `A` (it repeats the first book). Both are intended; recorded because the
  receipts say "identical" without the qualifier.
- [ ] The `ratio == max` boundary is untested (only strictly-above and
  strictly-below).
- [ ] The count checks after `seal_hour` (batch) and the second hour-total
  compare in publish are defense in depth; `prepare_hour` already returns the
  error. Kept, documented as such.
- [ ] Drop-record durability (`write_drop_record`) has no fault test of its
  own; `write_durable`-to-`write_atomic` and
  `create_dir_all_durable`-to-`create_dir_all` mutants survive.
- [ ] The `data.time` upper bound (4102444800000) is tested only at the lower
  end.
- [ ] No test runs the whole lane with `--jobs > 1` beyond the mixed-hour
  guard test; the pool's ordering and floor paths deserve their own
  multi-hour, multi-worker fixture.
- [ ] The manifest builder's mid-run flush (every 8 GiB of new hashing) has
  no test seam; only the final write and the interrupted-batch resume are
  covered.
- [ ] Empty `.tmp-duckdb-<worker>` directories are left behind after a run;
  harmless (the next run removes its own on open) but untidy.
- [ ] The builder aborts if a file vanishes between the walk and the open.
  The converter treats a missing file as pending, so the retry heals, but the
  builder could count it instead.
