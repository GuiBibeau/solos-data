# Hyperliquid l2Book: hours sealed before the download finishes

`solos-data augment hl-l2book` (ADR-0021) seals one Parquet output and one
receipt per hour. It treats an hour as ready when every manifest row for that
hour is on disk at the right size. The manifest builder writes a row for each
finished `.lz4` on disk. Neither step compares the hour with the source
listing, so "ready" means "every file we have is checked", not "the hour has
every coin".

## How it showed up

The all-coin downloader (`hl-s3-l2book-all`) works coin by coin, in volume
order, across every date and hour. It stopped at its disk floor on
2026-11-01 with 2,097,680 of 4,432,011 files. At that point:

- 0 of the 30,484 listed hours had all their coins on disk;
- 104 of 234 coins were complete, AR was partial, and 129 had nothing;
- the manifest covered 100% of the files on disk.

A run over the whole tree would have sealed every hour with about 104 coins
and dropped their raw files.

## What a sealed hour does with a late coin

Two cases, depending on whether the hour's raw files are still on disk.

Raw dropped (`--drop-raw-after-verify` on and the hour passed the ratio
guard): add a coin's `.lz4` and its manifest row and rerun. The hour's
manifest rows now include files no longer on disk, so `locate_hour`
(`crates/solos-data/src/augment/hll2book/batch.rs:865`; line numbers are as
of main 447c95e) sees the hour as Partial. The Partial branch of `work_one`
(`batch.rs:547`) then finds the hour watermarked, the receipt identity no
longer matches, and the run exits 1 with `verified hour is incomplete and
its output does not match the receipt` (`batch.rs:555`; reproduced on
scratch at 8ceca2a). No data is lost, but every later run stops at that
hour.

Raw still on disk (drop-raw off, or the ratio guard held the hour and kept
its raw): every manifest file is present at the right size, so the hour is
Ready with a new input identity, the `verified_receipt` call in the Ready
path (`batch.rs:567`) returns `None`, and the hour re-seals cleanly from
every coin, the late one included. Note that if that later run uses
drop-raw, it then deletes all of that hour's raw, including the late
coin's file, because the new receipt lists every input.

## Shrinking re-seal: silent data loss

Found in review. If a watermarked hour's inputs shrink while its raw is still
on disk, the hour is Ready with a new identity and re-seals from the subset.
Triggers: a rebuilt or fresh manifest with fewer rows for that hour, a
different `--checksums` file, a different `--out` pointing at an existing
root, or a rerun of the same root with a narrower `--coins`.

Then: `seal_hour`
(`crates/solos-data/src/augment/hll2book/publish.rs:282`) renames the new
Parquet over the hour's file, and `stale_paths` (`publish.rs:938`) removes
any earlier output path not in the new set, so the earlier coins' rows are
gone. `drop_raw` deletes the remaining raw for the subset. And
`write_drop_record` (`batch.rs:1081`) overwrites `drops/<hour>.json` instead
of merging, so the re-download record of the earlier coins is lost too. The
verification gates all pass, because each one checks the subset against
itself.

Required code guard, regardless of which proposal below wins: refuse to
re-seal a watermarked hour unless its current inputs are a superset of the
sealed receipt's inputs (same coin and sha256 for every receipt input). The
error names the hour and the missing inputs, and nothing is deleted: no
Parquet, no raw, no drop record change. `write_drop_record` must also merge
with an existing `drops/<hour>.json` rather than overwrite. Tests: a late
coin (a superset) is allowed and re-seals with every coin; shrinking inputs
are refused with no Parquet deleted and no raw dropped; a drop record merge
keeps earlier entries. PR #104
(https://github.com/GuiBibeau/solos-data/pull/104) implements the guard and
the drop record merge.

#104's behavior: a changed sha256 for a coin already in the receipt is
refused too (it counts as a missing input), so a changed file is not a
silent re-seal; only a config-hash change or a strict superset re-seals.
The escape hatch for a corrected re-download is the new
`--reseal <YYYYMMDD/H>` flag in #104: it bypasses the guard for that hour,
refuses unless every receipt input's raw file is still on disk, and uses
the old receipt so `stale_paths` removes old outputs. It is validated
before any work, and refuses an hour not in the watermark, without a
receipt, outside `--since`/`--until`, or whose in-scope manifest group
lacks any receipt coin, so a narrowed `--coins` cannot shrink it. Without
it, the only manual way is deleting `receipts/<hour>.json`. That works
only while the raw is on disk (otherwise the hour goes Partial and fails at
`batch.rs:555`), and it leaves `stale_paths` no receipt to clean old
outputs with, so do not do that.

## Quarantine: a raw line that does not decode

Found in production. The live batch-01 run exited 1 at 2026-11-02 10:18 UTC with
`20251114/11: MEGA line 1303: json: expected , or } at line 1 column 483`. Diagnosis,
read-only: the raw file `market_data/20251114/11/l2Book/MEGA.lz4` (283,377 B) matches the
MANIFEST sha256 `81ce31ed…9dde1` and the S3 listing md5/ETag `113b3008…d9ad677e3`, so the
bytes are upstream data as published, not a download fault. Decompressed: 8,277,841 B,
5,247 newline-terminated lines (5,248 is the split-on-newline element count including
the empty tail). Line 1303 (2,061 B) is a record from 11:15:17.445 truncated at byte 480, with
a complete record from 11:21:59.289 spliced on right after it (no newline); every other
line parses; line 1302 is 11:15:16.88 and line 1304 is 11:22:03.71. Real corruption in
the source.

A corrupt hour must not stop a run with thousands of good hours behind it (the old
follow-up "one refused hour aborts the whole run", for this failure mode). The rule now:

- A raw line that fails to decode as an `l2Book` snapshot (`count_snapshot` /
  `snapshot_of_line`: JSON syntax, shape, or field errors of one line) quarantines the
  WHOLE hour. The hour is not sealed: no Parquet, no receipt, no watermark entry, and
  none of that hour's raw files is dropped, even under `--drop-raw-after-verify` (a
  drop would make the fault unrepairable in this root).
- Every fault is merged into `quarantine.jsonl` in the output root: hour, coin, path,
  raw sha256, 1-based line number, JSON byte column (`null` for shape/field errors),
  error text, git sha of the detecting binary, detected-at UTC. Durable write (`.tmp`,
  fsync, rename, directory fsync). A rerun re-detects the same hour and merges into the
  existing entry (same hour, coin, path, sha256, line, column, error): no duplicate
  entries, the original detected-at survives. A foreign-schema or unparsable record is
  an error, never an overwrite.
- The watermark is the set of sealed hours; a quarantined hour is simply absent from it.
  Nothing in the codebase computes a contiguous "through" hour from the watermark, so
  nothing skips past the gap; readers must keep treating the set that way (an absent
  hour is pending or quarantined, never sealed). Hours after it in the same run seal and
  appear normally.
- Other hours continue and seal normally, also with `--jobs > 1`: detection happens in
  the worker, the quarantine record is written by the commit loop in hour order.
- The run finishes all other hours and then exits 3 (`QUARANTINE_EXIT`; `0` done and
  `2` disk floor keep their meanings, a quarantine wins over the floor). The JSON report
  carries `hoursQuarantined`, `quarantineEntriesAdded`, and `quarantineExamples` (at
  most 8).
- Only line decode errors quarantine. IO errors, lz4 frame errors, checksum mismatches,
  and manifest problems still fail the run with exit 1, as before. A line that decodes
  but trips a count or sample check is still fatal: those guards verify the writer,
  not the source.

Repair: put the coin's raw back at its manifest sha256, update the manifest row, and
rerun; the hour seals normally and the old quarantine entry stays on file as history.
Resuming the interrupted batch-01 run needs the same root, the same `--coins`, and the
same MANIFEST per the operating rules above; not done here.

The tests build the incident's shape: a valid line truncated at byte 480 with a complete
valid record spliced on. The mutant patches Q1..Q9 pin the rule red (skip-line, raw drop
on a quarantined hour, seal on quarantine, missing record, exit 0, watermark gain,
any-error quarantine, blind append, run stops).

## Coin-batch roots

Convert in coin batches into separate output roots. This is the official
backfill mode. Each batch takes only coins that are complete against the S3
listing (`--coins`, checked right before launch), so every hour in that root
is complete for its coin set. Batch 01 is the 104 complete coins, in
`l2book-parquet/batch-01`.

The split is checked on scratch with one real hour (20250610/13, 86 coins):

- half the coins went to root X and the other half to root Y, both with
  drop-raw;
- both passes verified;
- each pass dropped only its own coins' raw files;
- a rerun of X skipped the hour;
- X plus Y matched a single full conversion: 567,326 rows, with equal row
  counts and sorted-content MD5 for every coin.

Readers must union the batch roots, permanently. Once a batch drops its raw
files, no later full root can complete those coins for those hours: the raw
is gone, and the manifest rows for the dropped files would make the hour
Partial. The batch roots are the record for those coins.

## Operating rules

- Permanent: coin sets are disjoint across roots. Each root has its coin
  list recorded as a file, and disjointness is checked before launch. The
  guard stops shrinking re-seals, not overlapping roots: two roots holding
  the same coin would duplicate that coin's rows in the reader union.
- Until #104 merges: a root is never rerun with a different `--coins`, and
  no full run (without `--coins`) ever points at a batch root.
- Until #104 merges: every run of a root passes the same manifest file, and
  nothing rebuilds or rewrites that manifest while a run is live (the
  manifest builder and the downloader are stopped).

Batch-01 was checked against these rules on 2026-11-01 while running: one
root, its coin list file matches the process `--coins`, the manifest was last
written before launch, the downloader service is stopped, and every receipt,
drop record and watermark entry is one-to-one with matching inputs.

## Recommendation

Forward and future hours: the listing gate plus hour-window downloads.
Download a bounded window of hours with every coin, convert, drop raw, move
on. The superset guard lands regardless of the download mode. Coin-batch
roots are the official backfill mode, not an interim workaround.

The listing gate deadlocks with a coin-by-coin downloader: the gate converts
nothing until the last coin of an hour lands, but the coin-major downloader
stopped at its disk floor at 2,097,680 of 4,432,011 objects (948.9 of
1,694.8 GB), the whole archive does not fit on disk, and so no hour ever
completes. The gate only works with hour-by-hour or bounded date-window
downloads.

Not per-(coin, hour) sealing: about 7M small Parquet files. The alternative,
rewriting an hour by merging new coins into the sealed Parquet, breaks the
binding between the receipt and the raw sha256 of every input, since the
earlier coins' raw is already dropped.

Caveat: listing completeness is only a snapshot. For the newest hours the
listing can still grow after it was taken, so the gate must treat hours near
the listing time as possibly incomplete.

- [ ] Superset guard: refuse to re-seal a watermarked hour whose inputs are
  not a superset of the receipt's inputs, naming the hour and the missing
  inputs, with the three tests above (late coin allowed, shrink refused with
  nothing deleted, drop record merge keeps earlier entries). PR #104; tick
  when it merges.
- [ ] `write_drop_record` merges with an existing `drops/<hour>.json` instead
  of overwriting. PR #104; tick when it merges.
- [ ] Hour-window downloader: bounded windows of hours, every coin, convert
  and drop raw before the next window.
- [ ] Listing gate: an hour stays pending until every listed key for it is
  covered, and hours near the listing time are treated as possibly
  incomplete.

## Follow-ups (non-blocking)

- [ ] One refused hour aborts the whole run: in a jobs=4 probe, 3 valid
  hours were never sealed, and every rerun stops at the refused hour.
  Consider a pending-refused status that lets other hours seal and exits 1
  at the end. Done for line-decode faults by the quarantine rule above
  (other hours seal, exit 3); the superset guard refusal still aborts.
- [x] Tests added in #104: `drop_record_wrong_schema_is_refused`,
  `drop_record_durable_write_fault`, and `work_one_guard_alone` (the
  `work_one` guard is covered on its own).
- [ ] The guard compares coin and sha256 only, not path or bytes. Low risk;
  noted.
- [ ] The `--reseal` parser accepts forms like `20261022/05` and
  `20261022/+5` but stores them as typed, so they never match the watermark
  key (`20261022/5`) and the refusal says "not in the watermark" for a real
  hour, which is misleading: normalize the hour or reject non-canonical forms.
- [ ] The `--reseal` receipt-readable check and the missing-manifest-row check
  have no tests yet.
- [x] Quarantine rule: a raw line that does not decode quarantines its hour
  (no seal, no raw drop, a durable idempotent `quarantine.jsonl` entry,
  other hours seal, exit 3), with the 2026-11-02 20251114/11 MEGA incident
  numbers, the incident-shaped fixture tests, and the Q1..Q9 mutants.
