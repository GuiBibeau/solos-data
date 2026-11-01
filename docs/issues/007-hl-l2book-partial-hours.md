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

Reproduced on scratch at 8ceca2a. Convert an hour with drop-raw, then add a
coin's `.lz4` and its manifest row and rerun. The hour's manifest rows now
include the dropped files, so the hour is incomplete, and the watermark lists
it. The run fails with `verified hour is incomplete and its output does not
match the receipt` and exits 1. No data is lost, but every later run stops at
that hour, and the late coin can't be added without a code change.

## Workaround in use

Convert in coin batches into separate output roots. Each batch takes only
coins that are complete against the S3 listing (`--coins`, checked right
before launch), so every hour in that root is complete for its coin set.
Batch 01 is the 104 complete coins, in `l2book-parquet/batch-01`.

The split is checked on scratch with one real hour (20250610/13, 86 coins):

- half the coins went to root X and the other half to root Y, both with
  drop-raw;
- both passes verified;
- each pass dropped only its own coins' raw files;
- a rerun of X skipped the hour;
- X plus Y matched a single full conversion: 567,326 rows, with equal row
  counts and sorted-content MD5 for every coin.

Readers must union the batch roots.

## Proposed fix (pick one)

- [ ] Seal per (coin, hour): one output, receipt, and watermark entry per coin
  and hour, so a late coin is a new unit. This needs a layout change to
  per-coin files or an hour rewrite that keeps prior coins.
- [ ] Gate on source-listing completeness: pass the downloader listing (the
  TSV) to the converter and treat an hour as pending until every listed key for
  it is covered. This keeps one file per hour, but nothing converts until the
  last coin of that hour lands.

Either way, test the late-coin case: seal an hour, add a coin, rerun.
