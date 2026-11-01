# ADR-0021: Offline Hyperliquid l2Book hours

Status: accepted, November 1, 2026.

The box is holding the Hyperliquid `l2Book` archive as LZ4 JSONL. A full book is
about twenty levels a side at a roughly five-second cadence, and the raw tree is
on the order of 660 GB. This note is the offline conversion of those local files
into Parquet, with a verified delete of the raw bytes afterwards. It follows the
fills batch (ADR-0020): one process, one in-memory DuckDB, one hour at a time,
and the watermark moves only after the Parquet and the receipt are durable.

The live `hyperliquid_s3` source is unchanged and still off (`enabled: false`,
`estimateOnly: true`). `book_row` in that lane still keeps ten levels for the
mapped coins. This command does not call it, does not download, does not open
the collector checkpoint, and does not register files in the augment ledger.

## Decision

`solos-data augment hl-l2book` reads local hours and writes Parquet.

```sh
solos-data augment hl-l2book \
  --input <l2book-dir> \
  --out <output-root> \
  --checksums <MANIFEST> \
  --min-free-tb 1.0 \
  --threads 4 \
  --memory 8GB \
  --drop-raw-after-verify
```

`<l2book-dir>` is either `YYYYMMDD/H/<COIN>.lz4` or
`market_data/YYYYMMDD/H/l2Book/<COIN>.lz4`. The manifest paths say which. `H` is
unpadded `0`..`23`. Coin names are the archive's native names, taken from the
file stem and checked against `data.coin`.

The manifest is JSON (`{"files":[{"path","sha256","size"}]}` or a path-to-object
map) or a three-field listing `<sha256> <size> <path>`. A book entry without a
size is an error. A path counts when it ends in an l2Book object of that shape.
`s3://hyperliquid-archive/` on a key is stripped before the match.

`--coins`, `--since`, and `--until` limit which manifest rows are in scope.
`--since` and `--until` are inclusive `YYYYMMDD` dates of the source hour. A
file outside that scope is ignored, even if it is on disk. A file inside the
scope with no manifest row fails the run.

### Layout

```
<out>/train/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
<out>/forward/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
```

One file holds every in-scope coin of that source hour whose `time_ms` falls on
that UTC date, sorted by `coin`, `time_ms`. The directory date is the row's date.
The file name is the source hour, so a rerun replaces that hour only.

Not one file per coin. A fixture of one coin for one hour (720 snapshots, 20
levels) is well under 10 MB after ZSTD level 9, so `hour=/coin=<COIN>.parquet`
would be a small-file pile. An hour of the archive is one file, large enough to
scan, and the resume unit stays one hour. The behavior test
`side_arrays_are_smaller_than_flat_columns_and_the_lz4` records the fixture
byte counts.

### Columns

`coin`, `time_ms`, `recv_time_ms`, and six fixed arrays: `bid_px`, `bid_sz`,
`bid_n`, `ask_px`, `ask_sz`, `ask_n`, each of length 20. The same 720-snapshot
fixture written as 120 flat columns is 27,933 bytes; the array file is 4,110
bytes. One hundred and twenty column chunks dominate a single coin-hour.
DuckDB reads an element as `bid_px[1]` (1-indexed). Element 1
is the first archive level, the best bid or ask. Fewer than 20 levels are NULL.
Levels past 20 are dropped.

`px` and `sz` are DOUBLE, the same conversion as ADR-0020: a decimal string
parsed as `f64` comes back with the same bits, including a 17-significant-digit
value. A missing field, JSON null, or `""` is NULL, never 0. `n` is BIGINT.
`time_ms` is `data.time` and is required. `recv_time_ms` is the envelope `time`
when present (ISO-8601 truncated to milliseconds, or a number that is already
milliseconds, or seconds when it is at least 10^9 and below 10^12).

### Encoding

ZSTD level 9, 122880-row groups, Parquet V2, bloom filters on. `PARQUET_VERSION
V2` is what makes DuckDB 1.4.5 write `BYTE_STREAM_SPLIT` on floating columns and
`DELTA_BINARY_PACKED` on integers that benefit from it. On the fixture, `bid_px`
and `ask_px` are `BYTE_STREAM_SPLIT`, `time_ms` is `DELTA_BINARY_PACKED`, and a
low-cardinality count or size column stays dictionary-encoded. The test reads
`parquet_metadata` and fails if the price or `time_ms` encoding is absent. This
is not the decoder's V1 pin (ADR-0017).

On that 720-snapshot fixture the `.lz4` input is 231,258 bytes and the Parquet
file is 4,110 bytes, a ratio of 0.0178. The series is a smooth walk, so this is
not the archive's ratio. The operator's big-box sample of these encodings was
0.612 times the `.lz4` size. The test requires the Parquet file to be smaller
than its `.lz4` input and smaller than the flat-column file of the same rows.

### Forward split

`Corpus::of_time_ms` is the rule. `time_ms >= 1792713600000` is `forward/`.
Anything earlier is `train/`. Before the watermark moves, a train file with any
row on or after the boundary, or a forward file with any row before it, fails
the hour. `forward/` is eval-only.

### Verification

For each hour, in order, and only then does `watermark.json` gain the hour:

1. Every in-scope manifest file exists and its size equals the manifest. A
   missing or short file makes the hour pending. The run continues with other
   hours. Pending is how a downloader can still be appending: a partial `.lz4`
   is never decoded and never deleted.
2. SHA-256 of the `.lz4` bytes equals the manifest. A mismatch fails the hour.
3. An independent per-line snapshot count equals the rows written for that coin,
   and the hour total equals the rows read back from Parquet.
4. A deterministic sample of up to 16 snapshots is parsed again from the staged
   row and compared to the Parquet row, including `f64` bits.
5. Corpus and UTC date match the stored rows.
6. Temp file fsync, rename, directory fsync, then the receipt the same way, then
   the watermark.

A short frame, bad JSON, or a failed check deletes the temps and does not move
the watermark. The run stops on the first failed hour. Hours already verified
stay verified.

`receipts/YYYYMMDD/H.json` records `input_sha256` (SHA-256 of the sorted
`coin sha256` lines), each input's path, size, sha256, and snapshot count, and
each output's path, rows, bytes, and sha256, plus `config_hash` and `git_sha`.

A rerun skips an hour when the watermark lists it, the receipt's input sha and
config hash still match, and every output file is still there with the
receipt's sha. Deleting a receipt, changing an input, or editing the config hash
rewrites that hour.

### Deleting raw

`--drop-raw-after-verify` is the only way a raw file is removed, and the delete
runs only after `watermark.json` for that hour is durable. The hour's
`drops/YYYYMMDD/H.json` is written first (fsync the file, then fsync the
directory) and a rerun overwrites it. Only then are the `.lz4` files unlinked.
The record lists each input's relative path, byte size, sha256, and
`s3://hyperliquid-archive/market_data/<YYYYMMDD>/<H>/l2Book/<COIN>.lz4`, so the
object can be fetched again. A file that is already gone is skipped. A file
whose size or sha256 no longer matches the receipt is left in place and the
hour fails. The path is canonicalized and must stay inside the input directory.
A symlink that leaves the input directory is refused and the target is not
deleted.

Without the flag the `.lz4` files stay. A second run with the flag deletes them
if the receipt still matches.

### Disk and resources

Free space on the output filesystem is checked before the run and before each
complete hour (`statvfs`). Below `--min-free-tb` (default 1.0, meaning 10^12
bytes) the process exits 2 and does not start the next hour. Exit 0 means every
complete hour is verified; pending hours are counted in `hoursPending` and are
not a failure. Exit 1 is a bad hour or a bad argument.

`--threads` and `--memory` are the DuckDB caps. The sort spills under
`<out>/.tmp-duckdb`. A second run against the same output root is refused while
the first holds `hl-l2book.lock`.

## Consequences

Readers train on `<out>/train/date=*/*.parquet` and keep `<out>/forward/` for
eval. They must filter by `watermark.json` only. A crash after a Parquet rename
and before the watermark write leaves a file that is not yet a verified hour.
A schema change bumps `hl-l2book-v1`. Raw bytes that were deleted are listed
under `drops/` and can be restored from `s3://hyperliquid-archive`.

## Big-box command

The input tree this was written for is the local archive mirror. Adjust
`--checksums` if the manifest file has another name. `--drop-raw-after-verify`
is what frees the raw gigabytes, and it is safe to omit on a first look.

```sh
solos-data augment hl-l2book \
  --input ~/.local/share/solos-data/free/hyperliquid/s3/l2book \
  --out ~/.local/share/solos-data/free/hyperliquid/l2book-parquet \
  --checksums ~/.local/share/solos-data/free/hyperliquid/s3/l2book/MANIFEST \
  --min-free-tb 1.0 \
  --threads 4 \
  --memory 8GB \
  --drop-raw-after-verify
```
