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
(batch.rs) sees the hour as Partial. The hour is watermarked, the receipt
identity no longer matches, and the run exits 1 with `verified hour is
incomplete and its output does not match the receipt` (reproduced on scratch
at 8ceca2a). No data is lost, but every later run stops at that hour.

Raw still on disk (drop-raw off, or the ratio guard held the hour and kept
its raw): every manifest file is present at the right size, so the hour is
Ready with a new input identity, `verified_receipt` returns `None`, and the
hour re-seals cleanly from every coin, the late one included. Note that if
that later run uses drop-raw, it then deletes all of that hour's raw,
including the late coin's file, because the new receipt lists every input.

## Shrinking re-seal: silent data loss

Found in review. If a watermarked hour's inputs shrink while its raw is still
on disk, the hour is Ready with a new identity and re-seals from the subset.
Triggers: a rebuilt or fresh manifest with fewer rows for that hour, a
different `--checksums` file, a different `--out` pointing at an existing
root, or a rerun of the same root with a narrower `--coins`.

Then: the new Parquet replaces the hour's file, and `stale_paths`
(publish.rs) removes any earlier output path not in the new set, so the
earlier coins' rows are gone. `drop_raw` deletes the remaining raw for the
subset. And `write_drop_record` (batch.rs) overwrites `drops/<hour>.json`
instead of merging, so the re-download record of the earlier coins is lost
too. The verification gates all pass, because each one checks the subset
against itself.

Required code guard, regardless of which proposal below wins: refuse to
re-seal a watermarked hour unless its current inputs are a superset of the
sealed receipt's inputs (same coin and sha256 for every receipt input). The
error names the hour and the missing inputs, and nothing is deleted: no
Parquet, no raw, no drop record change. `write_drop_record` must also merge
with an existing `drops/<hour>.json` rather than overwrite. Tests: a late
coin (a superset) is allowed and re-seals with every coin; shrinking inputs
are refused with no Parquet deleted and no raw dropped; a drop record merge
keeps earlier entries. A separate code PR implements this.

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

## Operating rules until the guard lands

The three rules below are mandatory for every l2Book root until the superset
guard lands.

- Coin sets are disjoint across roots. Each root has its coin list recorded
  as a file, and disjointness is checked before launch.
- A root is never rerun with a different `--coins`, and no full run (without
  `--coins`) ever points at a batch root.
- Every run of a root passes the same manifest file, and nothing rebuilds or
  rewrites that manifest while a run is live (the manifest builder and the
  downloader are stopped).

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
  nothing deleted, drop record merge keeps earlier entries).
- [ ] `write_drop_record` merges with an existing `drops/<hour>.json` instead
  of overwriting.
- [ ] Hour-window downloader: bounded windows of hours, every coin, convert
  and drop raw before the next window.
- [ ] Listing gate: an hour stays pending until every listed key for it is
  covered, and hours near the listing time are treated as possibly
  incomplete.
