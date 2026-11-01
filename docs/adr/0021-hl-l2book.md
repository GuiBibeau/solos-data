# ADR-0021: Offline Hyperliquid l2Book hours

Status: accepted, November 1, 2026. Revised the same day after the first
contact with the real archive: the manifest-builder subcommand, the duplicate
rule, the Parquet V1 encoding, the ratio guard, and `--jobs`.

The box is holding the Hyperliquid `l2Book` archive as LZ4 JSONL. A full book is
about twenty levels a side at a roughly five-second cadence, and the raw tree is
on the order of 660 GB. This note is the offline conversion of those local files
into Parquet, with a verified delete of the raw bytes afterwards. It follows the
fills batch (ADR-0020): one supervised process, in-memory DuckDB only, and the
watermark moves only after the Parquet and the receipt are durable. With
`--jobs` several hours stage at once; the commit order stays one hour at a time.

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
  --jobs 4 \
  --memory 8GB \
  --max-ratio 1.0 \
  --drop-raw-after-verify
```

`<l2book-dir>` is either `YYYYMMDD/H/<COIN>.lz4` or
`market_data/YYYYMMDD/H/l2Book/<COIN>.lz4`. The manifest paths say which. `H` is
unpadded `0`..`23`. Coin names are the archive's native names, taken from the
file stem and checked against `data.coin`.

The manifest is JSON (`{"files":[{"path","sha256","size"}]}` or a path-to-object
map) or a three-field listing `<sha256> <size> <path>`. A book entry without a
size is an error. A path counts when it ends in an l2Book object of that shape.
`s3://hyperliquid-archive/` on a key is stripped before the match. The
manifest-builder subcommand below writes exactly that listing, so the converter
reads its output directly. Duplicate detection is one hash-set pass, not a scan
per row: the full-archive manifest is millions of lines.

`--coins`, `--since`, and `--until` limit which manifest rows are in scope.
`--since` and `--until` are inclusive `YYYYMMDD` dates of the source hour. A
file outside that scope is ignored, even if it is on disk.

### Manifest coverage

An in-scope finished `.lz4` on disk with no manifest row makes that whole hour
pending, never an abort: the run skips it, counts it in `hoursPending` and
`hoursUncovered`, and does not touch its raw files. The sha256 manifest lags the
downloader, and the builder closes the gap. `.part` files never match a book
path, so a downloading file is invisible to the run. An hour whose manifest
files are present but short (the downloader still appending) is pending the same
way. Pending is not a failure: exit 0.

### The manifest builder

```sh
solos-data augment hl-l2book-manifest \
  --input <l2book-dir> \
  --tsv <downloader.tsv.gz> \
  --out <MANIFEST> \
  --jobs 8
```

The downloader listing has `key`, `size`, and `etag` columns (gzip or plain;
column order is read from the header). For every finished `.lz4` on disk whose
byte size equals the TSV size and whose local MD5 equals the TSV etag, the
builder writes `<sha256> <size> <path>`, where the sha256 is computed from the
local bytes. The MD5 etag is a gate that ties the local file to the TSV row; it
is never the checksum, and a multipart-style etag (`md5-N`) can never pass. MD5
comes from the `md-5` crate (RustCrypto), cross-checked in a test against
DuckDB's `md5()`. It replaced an earlier hand-rolled RFC 1321 file: the hash is
a gate, not a checksum, and a widely used crate is cheaper to trust than a
local implementation.

Resume: the manifest and its `<out>.state.json` sidecar are each written whole
through `write_durable` (temp, fsync, rename, directory fsync), so a crash
leaves the previous complete file or the new complete file, never a torn line.
An entry is not re-hashed when the manifest already has the path at the same
byte size and the sidecar still records those bytes with an unchanged mtime;
anything else is hashed again. A size or MD5 mismatch is counted, drops any
entry the manifest had for that path, and is not written. `.part` files are
counted (`skippedPart`) and never read. Hashing runs across `--jobs` threads
with per-thread buffers. The report carries `present` (finished books on disk),
`covered`, `newlyHashed`, `reused`, `skippedPart`, `mismatched`, and `notInTsv`.
A mismatch is not an error: it is a downloader still writing, and a rerun covers
the file once it finishes.

The builder holds the TSV keys in memory, about one gigabyte for the full
4.4-million-row listing, which is fine on the archive box and is the price of
random access by local path.

### Layout

```
<out>/train/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
<out>/forward/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
```

One file holds every in-scope coin of that source hour whose `time_ms` falls on
that UTC date, sorted by `coin`, `time_ms`, and the hour's archive line order
(`_seq`, a staging-only key; see the duplicate rule). The directory date is the
row's date. The file name is the source hour, so a rerun replaces that hour
only.

Not one file per coin. A fixture of one coin for one hour (720 snapshots, 20
levels) is well under 10 MB after ZSTD level 16, so `hour=/coin=<COIN>.parquet`
would be a small-file pile. An hour of the archive is one file, large enough to
scan, and the resume unit stays one hour.

### Columns

`coin`, `time_ms`, `recv_time_ms`, and six fixed arrays: `bid_px`, `bid_sz`,
`bid_n`, `ask_px`, `ask_sz`, `ask_n`, each of length 20. DuckDB reads an element
as `bid_px[1]` (1-indexed). Element 1 is the first archive level, the best bid
or ask. Fewer than 20 levels are NULL. Levels past 20 are dropped.

`px` and `sz` are DOUBLE, the same conversion as ADR-0020: a decimal string
parsed as `f64` comes back with the same bits, including a 17-significant-digit
value. A missing field, JSON null, or `""` is NULL, never 0. `n` is BIGINT.
`time_ms` is `data.time` and is required. `recv_time_ms` is the envelope `time`
when present (ISO-8601 truncated to milliseconds, or a number that is already
milliseconds, or seconds when it is at least 10^9 and below 10^12).

### The duplicate rule (`DuplicateRule`)

A downloader sample found duplicate `time_ms` in 22 percent of real coin-hours:
208 duplicates across 150 files, 207 byte-identical re-sends and one a different
book. The first converter aborted every such hour, which is most of the archive.

Two snapshots of one coin with the same `time_ms` are compared by their book:
both sides' `px`, `sz`, and `n`, all 20 levels, as exact `f64` bits
(`decode::book_digest`). `recv_time_ms` is envelope metadata and is not part of
book identity; the first line's value stays.

- Identical book: the later line is a re-send. It is dropped and counted
  `dup_identical`.
- Distinct book: both rows are kept, in archive line order, and the extra row is
  counted `dup_distinct`. The staged `_seq` key is the COPY's sort tiebreak, so
  the file bytes are deterministic and the order is the order the lines arrived.

A duplicate is never an error. The independent per-line count must equal the
kept rows plus `dup_identical`, checked in the hour, in the receipt, and in the
run summary; `dup_distinct` explains the rows beyond one per `time_ms`.

### Encoding: Parquet V1

ZSTD level 16, 122880-row groups, Parquet V1, bloom filters on. The first
revision used `PARQUET_VERSION 'V2'` for `BYTE_STREAM_SPLIT` on the floats. On
the real archive that is exactly wrong: DuckDB 1.4.5 applies BYTE_STREAM_SPLIT
to the DOUBLE child of the LIST columns, and its page-granular split defeats
ZSTD on real book sizes. Measured on the real 20251001/0 BTC+ETH hour against
its 2,715,699-byte `.lz4`:

| Layout | Version | Bytes | Ratio of lz4 |
| --- | --- | --- | --- |
| six arrays (this lane) | V2 | 3,061,189 | 1.13 |
| six arrays (this lane) | V1 | 1,561,036 | 0.575 |
| 120 flat columns | V1 | 2,196,970 | 0.809 |
| 120 flat columns | V2 | 3,600,448 | 1.326 |
| one row per level | V1 | 1,978,787 | 0.729 |
| one row per level | V2 | 3,626,964 | 1.336 |

Under V1 the size doubles are PLAIN and ZSTD compresses them (1.24 MB of the
1.56 MB file), while the prices keep the dictionary (49 KB). Under V2 the sizes
are BYTE_STREAM_SPLIT and cost 2.74 MB. The array layout stays (readers and the
schema are unchanged); only the version flips, and `CONFIG_HASH` moves with it.
The behavior test pins the absence of BYTE_STREAM_SPLIT rather than a specific
encoding, because which encoding DuckDB picks per column is data-dependent: on
the smooth fixture the prices are nearly all distinct and go PLAIN, on real
hours they repeat and go dictionary.

Measured per era on all of each hour's coins (see `agent-results.md` for the
run log):

| Hour | Coins | lz4 bytes | Parquet bytes | Ratio | Guard (1.0) |
| --- | --- | --- | --- | --- | --- |
| 20231111/2 | all 22 | 8,959,346 | 6,326,551 | 0.706 | kept no |
| 20251001/0 | BTC, ETH, SOL | 3,984,146 | 2,091,734 | 0.525 | kept no |
| 20251001/1 | BTC, ETH, SOL | 4,213,338 | 2,217,942 | 0.526 | kept no |
| 20261001/0 | all 50 | 7,619,130 | 4,072,499 | 0.535 | kept no |

This is not the decoder's V1 pin (ADR-0017); this lane does not share that
golden hash.

### The ratio guard (`--max-ratio`, default 1.0)

If an hour's Parquet bytes exceed its `.lz4` bytes times the ratio, the hour is
still converted and verified, the watermark still moves, but
`--drop-raw-after-verify` keeps its raw files, the run counts it in
`hoursKeptRatio`, and the hour's line in the report carries `keptRatio`. The
receipt records the hour's `lz4_bytes` and `parquet_bytes`, so a rerun that
skips the verified hour re-applies the guard from the receipt instead of
trusting a stored flag. The intent: a conversion that would consume net disk
must not free the bytes it would need to re-download.

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
3. An independent per-line snapshot count equals the kept rows plus the
   identical re-sends, and the hour total equals the rows read back from
   Parquet.
4. A deterministic sample of up to 16 snapshots is parsed again from the staged
   row and compared to the Parquet row, including `f64` bits. A distinct book
   kept at a repeated `time_ms` shares its key with another row, so exactly one
   of the tied rows must match the sample.
5. Corpus and UTC date match the stored rows.
6. Temp file fsync, rename, directory fsync, then the receipt the same way, then
   the watermark.

A short frame, bad JSON, or a failed check deletes the temps and does not move
the watermark. The run stops on the first failed hour; hours it already
committed stay committed, and hours staged but not sealed leave only
temporaries that the next run cleans. Hours already verified stay verified.

`receipts/YYYYMMDD/H.json` records `input_sha256` (SHA-256 of the sorted
`coin sha256` lines), each input's path, size, sha256, and kept-row count, the
hour's `dup_identical`, `dup_distinct`, `lz4_bytes`, and `parquet_bytes`, each
output's path, rows, bytes, and sha256, plus `config_hash` and `git_sha`.

A rerun skips an hour when the watermark lists it, the receipt's input sha and
config hash still match, and every output file is still there with the
receipt's sha. A config-hash change re-seals that hour, and so does a strict
superset of the receipt's inputs (a late coin with its manifest row). A dropped
input, or a coin whose sha256 changed, is refused before any work, because
sealing the subset would delete the missing coins' rows and then their raw:

```
20261022/0: refusing to re-seal: current inputs are not a superset of the sealed
receipt; missing SOL (sha256 3f2a91c4...). Restore the missing file(s) at the
receipt sha256 with their manifest rows, or, for a corrected re-download, rerun
with --reseal 20261022/0 while every receipt input's raw file is on disk (see
ADR-0021).
```

`--reseal YYYYMMDD/H` (repeatable, comma lists) is the supported way to re-seal
a corrected re-download of a verified hour. The run validates up front, before
any work, that each named hour is in the watermark, has a readable receipt,
sits inside `--since`/`--until`, is covered by in-scope manifest entries for
every receipt coin, and keeps every receipt input's raw file on disk; the
hour's files must also all be present at their manifest sizes. For a named hour
only, the guard is bypassed, the old receipt stays in place until the new seal
so stale cleanup still removes outputs the new seal does not produce, and the
drop record merge keeps both the old and the new sha256 of a changed file.

Without the flag the only manual route is deleting `receipts/YYYYMMDD/H.json`.
That works only while the raw is on disk: with the receipt gone and a file
missing or short, the hour falls to the partial branch and fails with "verified
hour is incomplete and its output does not match the receipt". It also forfeits
stale cleanup, which reads the old receipt to remove outputs the new seal does
not produce. Do not do that; use `--reseal`.

### Parallel hours (`--jobs`)

`--jobs N` (default 1) stages up to N hours at once. A worker thread reads the
hour's files, checks their sha256, decodes, spills, and runs the COPY into
`.tmp` files plus every check that can read a temporary (`prepare_hour`). The
main thread owns dispatch (after the disk floor), and the ordered commit:
fsync, rename, receipt, watermark, then the raw delete (`seal_hour`). Two
workers never share a DuckDB connection; each opens its own in-memory database
under the same `--threads` and `--memory` caps, so the totals multiply, and
AGENTS.md's one-owner rule holds per connection. The staging directory and the
DuckDB spill directory are per hour and per worker (`.tmp-duckdb-<worker>`).

Throughput measured on the fourteen scratch hours (all coins, 402,328,613
bytes of lz4, one input set for every row; an earlier revision mixed a
ten-hour byte total into twelve-hour rows, which the review caught):

| jobs | threads/worker | wall | MB/s of lz4 |
| --- | --- | --- | --- |
| 1 | 4 | 194.1 s | 2.1 |
| 4 | 4 | 72.9 s | 5.5 |
| 8 | 4 | 73.3 s | 5.5 |
| 16 | 4 | 86.8 s | 4.6 |
| 10 | 1 | 39.8 s | 10.1 |
| 12 | 1 | 28.6 s | 14.1 |

Measured on the shared box while the downloader itself was running (load
about 20 of 128 cores). Downloads arrive at about 73 GB/h (20 MB/s), which
is the bar, and this box was short of it: 14.1 MB/s at the best setting.
The bottleneck is the serial per-hour work, not the job count. One all-coin
hour (20251001/0, 32.4 MB of lz4) takes 20.8 s with one DuckDB thread and
16.0 s with four, so roughly 13-15 s of each hour is single-threaded Rust:
two serde_json passes per line (the independent count and the decode), the
duplicate digest, the spill serialize, and the input sha256. Four DuckDB
threads per worker oversubscribe past eight jobs (4.6 MB/s at sixteen jobs
times four threads versus 14.1 at twelve jobs times one). On an idle archive
box, twelve to sixteen workers at one thread each should hold the download
pace; the levers to get there, in order (drop the double parse, re-measure
the ZSTD level against its 13x CPU cost, parallelize per coin within an
hour), are the open deviation in docs/issues/006-hl-l2book-realdata.md.

### Deleting raw

`--drop-raw-after-verify` is the only way a raw file is removed, and the delete
runs only after `watermark.json` for that hour is durable and the hour is not
`kept_ratio`. The hour's `drops/YYYYMMDD/H.json` is written first (fsync the
file, then fsync the directory) and a rerun overwrites it. Only then are the
`.lz4` files unlinked. The record lists each input's relative path, byte size,
sha256, and `s3://hyperliquid-archive/market_data/<YYYYMMDD>/<H>/l2Book/<COIN>.lz4`,
so the object can be fetched again. A file that is already gone is skipped. A
file whose size or sha256 no longer matches the receipt is left in place and the
hour fails. The path is canonicalized and must stay inside the input directory.
A symlink that leaves the input directory is refused and the target is not
deleted.

Without the flag the `.lz4` files stay. A second run with the flag deletes them
if the receipt still matches.

### Git SHA

Receipts carry the git commit embedded at build time by `build.rs`
(`SOLOS_DATA_GIT_SHA` in the build environment, verbatim; else
`git rev-parse HEAD`; else `unknown` when git is unavailable), with rerun
inputs on the worktree's `gitdir:` pointer, `HEAD`, `packed-refs`, and `refs/`.
A runtime override is deliberately absent: a receipt names the binary that
wrote it, not the shell that happened to start it.

### Disk and resources

Free space on the output filesystem is checked before the run and before each
hour is dispatched (`statvfs`). Below `--min-free-tb` (default 1.0, meaning
10^12 bytes) the process exits 2 and does not dispatch more hours; hours
already dispatched finish and commit. Exit 0 means every complete hour is
verified; pending hours are counted in `hoursPending` and are not a failure.
Exit 1 is a bad hour or a bad argument.

`--threads` and `--memory` are the per-worker DuckDB caps; with `--jobs J` the
process can use about `J` times each. The sort spills under
`<out>/.tmp-duckdb-<worker>`. A second run against the same output root is
refused while the first holds `hl-l2book.lock`.

## Consequences

Readers train on `<out>/train/date=*/*.parquet` and keep `<out>/forward/` for
eval. They must filter by `watermark.json` only. A crash after a Parquet rename
and before the watermark write leaves a file that is not yet a verified hour.
A schema change bumps `hl-l2book-v1`. Raw bytes that were deleted are listed
under `drops/` and can be restored from `s3://hyperliquid-archive`.

An hour kept by the ratio guard keeps its raw files until a run with a higher
`--max-ratio` (or a smaller output) re-verifies it; the drop path is what
frees the raw gigabytes, and it is safe to omit on a first look.

## Big-box command

The input tree this was written for is the local archive mirror; `MANIFEST` is
built from the downloader listing. `--drop-raw-after-verify` is what frees the
raw gigabytes.

```sh
solos-data augment hl-l2book-manifest \
  --input ~/.local/share/solos-data/free/hyperliquid/s3/l2book \
  --tsv ~/.local/share/solos-data/free/hyperliquid/s3/MANIFEST-l2book-all.tsv.gz \
  --out ~/.local/share/solos-data/free/hyperliquid/s3/l2book/MANIFEST \
  --jobs 8

solos-data augment hl-l2book \
  --input ~/.local/share/solos-data/free/hyperliquid/s3/l2book \
  --out ~/.local/share/solos-data/free/hyperliquid/l2book-parquet \
  --checksums ~/.local/share/solos-data/free/hyperliquid/s3/l2book/MANIFEST \
  --min-free-tb 1.0 \
  --threads 4 \
  --jobs 8 \
  --memory 8GB \
  --drop-raw-after-verify
```
