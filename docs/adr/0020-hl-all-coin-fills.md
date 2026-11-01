# ADR-0020: All-coin Hyperliquid fills, offline

Status: accepted, November 1, 2026.

The general model trains on every Hyperliquid coin, not the Phoenix-mapped subset. Measured
on the archive for 2025-03-22 through 2026-10-31: 36,589 coins, about 4.56 billion fills.
One file per coin per day would be about 36,000 files a day. This note is step 1 of the
loader: `node_fills_by_block` only. Older `node_fills` and `node_trades`, `l2Book`,
`asset_ctxs`, and the regimes loader are later steps.

## Decision

`solos-data augment hl-fills` is an offline batch over local hourly files. It does not
download, does not open the collector checkpoint, and does not register rows in the augment
ledger. One process owns one in-memory DuckDB connection and writes hours one at a time.
The live `hyperliquid_s3` source stays `enabled: false`, `estimateOnly: true`,
`fills: false`. Its mapped-coin `fill_rows` path is unchanged.

```sh
solos-data augment hl-fills \
  --input <hourly-dir> \
  --out <output-root> \
  --checksums <SHA256SUMS-or-MANIFEST> \
  --min-free-tb 1.0 \
  --threads 4 \
  --memory 8GB
```

`<hourly-dir>` is `node_fills_by_block/hourly`: `YYYYMMDD/H.lz4`, `H` unpadded `0`..`23`.
The checksum file is a `sha256sum` listing (`<hex>  <path>` or `<hex> *<path>`) or a JSON
manifest (`{"files":[{"path","sha256"}]}` or a path-to-hex object). A path counts when it
ends in `YYYYMMDD/H.lz4`.

### Decimals

`px`, `sz`, `start_position`, `closed_pnl`, `fee`, `builder_fee`, `deployer_fee`, and
`priority_gas` are DOUBLE. That is the live fill lane's conversion (`number` to `f64`), and
it is the encoding behind the measured 49 bytes per row. A missing field, JSON null, or
empty string is NULL, never 0 and never `""`. A present value that is not a finite decimal
fails the hour. `coin`, `user`, and `time` are required, because the row cannot be classed
or split without them.

### Schema

| column | type | null when |
|---|---|---|
| `time_ms` | BIGINT | never (the fill's `time`; the hour fails if it is missing) |
| `coin` | VARCHAR | never |
| `coin_class` | VARCHAR | never: `perp`, `spot`, `hip3`, `outcome` |
| `user` | VARCHAR | never |
| `px`, `sz` | DOUBLE | missing, null, or `""` |
| `side`, `dir` | VARCHAR | missing, null, or `""` |
| `start_position`, `closed_pnl`, `fee` | DOUBLE | missing, null, or `""` |
| `fee_token` | VARCHAR | missing, null, or `""` |
| `crossed` | BOOLEAN | missing or null |
| `oid`, `tid` | BIGINT | missing, null, or `""` |
| `hash` | VARCHAR | missing, null, or `""`. A TWAP zero hash is kept as the 0x string |
| `liquidation` | VARCHAR | missing or null. An object is stored as JSON text |
| `block_number` | BIGINT | the block omitted it |
| `block_time` | VARCHAR | the block omitted it (source text, no `Z` added) |
| `twap_id` | BIGINT | missing, null, or `""` |
| `cloid`, `builder` | VARCHAR | missing, null, or `""` |
| `builder_fee`, `deployer_fee`, `priority_gas` | DOUBLE | missing, null, or `""` |

`coin_class` is the name: `@` is spot, `#` is outcome, a `:` otherwise is HIP-3 (`xyz:SP500`),
and anything else is a native perp (`BTC`). `@` and `#` win over `:`.

Files are ZSTD level 3, 122880-row groups, Parquet V1, bloom filters on: the same pinned
DuckDB 1.4.5 options as the decoder (ADR-0017). Rows are ordered by `coin`, `time_ms`, `tid`.

### Partition layout

```
<out>/train/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
<out>/forward/date=<YYYY-MM-DD>/hour=<YYYYMMDD-H>.parquet
```

One file holds every coin of that source hour whose fill time falls on that UTC date, sorted
by `coin`, `time_ms`, `tid`. The directory date is the row's UTC date, so a training scan of
`train/date=2026-10-22/` is that calendar day. The file name is the source hour, so a rerun
replaces that hour's files and cannot collide with another hour. An hour that crosses
midnight or the forward boundary writes one file on each side.

A per-coin layout is the wrong shape here: 36,589 coins is tens of thousands of files a day.
A single file per day would make the resume unit a whole day (about 30 million rows) instead
of one hour (about 1.4 million).

### Forward split

`Corpus::of_time_ms` is the rule. `time_ms >= 1792713600000` (2026-10-23 00:00:00 UTC,
`FORWARD_START_SECS = 1792713600`) is `Corpus::Forward`; anything earlier is `Corpus::Train`.
`output_relative` takes a `Corpus` and builds `train/` or `forward/`. There is no path prefix
a caller passes in. Before the watermark moves, the Parquet file is read back: a train file
with any row at or after the boundary, or a forward file with any row before it, fails the
hour and is not renamed into place.

`forward/` is eval-only. A training read of `train/` cannot see those rows.

### Verification

For each hour, before the watermark gains that hour:

1. SHA-256 of the `.lz4` bytes equals the manifest entry. A mismatch fails the hour.
2. An independent count of `events` (the array length only) equals the number of rows
   written and the number of rows read back from Parquet.
3. Every train row is before the forward boundary, every forward row is on or after it, and
   every row's UTC date is the directory date.
4. `coin_class` matches the coin name.
5. The temporary Parquet is fsynced, renamed, and the directory is fsynced. Then the receipt
   is written the same way. Then `watermark.json` gains the hour.

A short or corrupt frame, a truncated JSON line, or a failed check deletes the temporary
files and does not move the watermark. Hours already verified stay verified. The run stops
on the first failed hour.

`receipts/YYYYMMDD/H.json`:

```json
{
  "schema": "hl-fills-v1",
  "hour": "20261022/12",
  "input_sha256": "<hex of the lz4>",
  "input_fills": 1359244,
  "output_rows": 1359244,
  "train_rows": 1359244,
  "forward_rows": 0,
  "outputs": [
    {
      "path": "train/date=2026-10-22/hour=20261022-12.parquet",
      "rows": 1359244,
      "bytes": 0,
      "sha256": "<hex of the parquet>"
    }
  ],
  "git_sha": "<git rev-parse HEAD, or SOLOS_DATA_GIT_SHA, or unknown>",
  "config_hash": "<sha256 of schema, sort, forward start, decimal mode, and Parquet options>"
}
```

A rerun skips an hour when the watermark lists it, the receipt's input sha and config hash
still match, and every output file is still there with the receipt's sha. Deleting a receipt
rewrites that hour. On this DuckDB build the rewrite is the same file bytes.

### Disk and resources

The run checks free space on the output filesystem before it starts and before each hour
(`statvfs`). Below `--min-free-tb` (default 1.0, meaning 10^12 bytes) it stops with exit
code 2 and does not start the next hour. Exit code 0 means every discovered hour is
verified. Exit code 1 is a failed hour or a bad argument.

`--threads` and `--memory` are the DuckDB caps (`SET threads`, `SET memory_limit`). The
sort spills under `<out>/.tmp-duckdb` on the same filesystem. The process also holds the
compressed hour while it streams the frame. A second run against the same output root is
refused while the first still holds `hl-fills.lock`.

### Size

Measured on one real hour and scaled to 4,559,791,903 fills (tier "everything", zstd-3,
49.00 bytes/row): about 220 GB for all columns (the tier table's product is 223 GB) and
about 145 GB without `hash` and `cloid` (31.8 bytes/row). Treat that as ±20%. This lane
keeps `hash` and `cloid`, so the planning figure is about 220 GB. `coin_class` replaces the
`local_time` column of that measurement; the rest of the wide columns match.

## Consequences

Readers train on `<out>/train/date=*/*.parquet` and keep `<out>/forward/` for eval. The
watermark, not the presence of a file, is the list of verified hours. A schema change bumps
`hl-fills-v1`; an old watermark is refused rather than mixed with new files.
