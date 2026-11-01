# ADR-0022: R2 is the durable copy of a sealed raw epoch

Status: accepted, November 1, 2026. Supersedes the keep-raw-locally rule in
[ADR-0006](0006-continuous-storage-retention.md) and the backup-only rule in
[ADR-0010](0010-offline-backup.md) for sealed epochs. R2 is the sole durable copy of an
epoch only after the code in this note is running and the checks below have passed.
Until that command has recorded the epoch, ADR-0006 still applies: the local file is
the copy that matters, and R2 is a backup.

## Problem

`phoenix_raw/staging` is the permanent raw archive. Every active file stays local, so
`verify-storage` re-hashes all of it and the decoder, reverify, and queries open every
path in `catalog.json`. The nightly backup copies only paths the catalog lists. The
catalog is rewritten by maintenance, after the Parquet file is already durable, so files
registered between catalog writes are absent from R2. On 2026-11-01 that gap was 810
files, 3.6 GB.

## Decision

An epoch is `slot / 432000`, the same width the publisher uses.

**Seal.** `maintain seal` marks an epoch in `sealed_epochs` and in `catalog.json`
`sealedEpochs` only when all of these hold:

- The epoch number is below `current - lag` (`epoch < current - lag`). `current` is the
  newest active epoch and `lag` is `sealLagEpochs` (default 2, overridable with `--lag`).
  That is stricter than "at least `lag` epochs behind" (`epoch <= current - lag`): with
  lag 2 and current epoch 10, only epochs 0 through 7 seal. The current epoch never
  seals, including when the lag is 0.
- Merged `published_ranges` cover every slot of the epoch. Those ranges were inserted
  only after V1/V6, so seal does not re-run V1/V6. Seal does not check that cold
  checkpoint rows were trimmed. Retention still opens the local files to prove those
  rows (`archive_relation`), and a routine pass trims at most `retentionSlotsPerPass`
  slots. Finish that trim before `--drop-local` (the runbook says how). Dropping first
  makes the next trim fail on the missing files.
- Every active file in the epoch hashes to its registered SHA-256 and its Parquet row
  count matches.
- Every active transactions file is fully consumed in the decoder `sources` table
  (`row_offset >= row_count`).

This is a storage seal. It is not the V3-V5 acceptance seal, which stays open in
[issue 001](../issues/001-phoenix-acceptance.md). `publish_range` and raw compaction
refuse a sealed epoch, so nothing new is written into it. There is no unseal command.
`restore` puts bytes back and does not unseal.

`publish_range` returns an error, and writes no file, when the range includes a sealed
epoch. The live tail republishes only `overlapSlots` below the watermark (150 in
`config/phoenix.json`). Backfill publishes the next older chunk (1,000 slots, or the
archive lane's 10,000). With the default lag of 2 epochs (864,000 slots) both windows
stay in epochs that are not sealed, so neither path hits that error or stalls the
collector. `--lag 0` would let the 150-slot overlap cross into the previous epoch after
that epoch seals. The runbook uses `--lag 2`.

**Catalog location.** Each active file carries `location` (`local`, `both`, or `r2`),
`r2_key`, `sha256`, and `byte_size`. The key is `phoenix_raw/` plus the path relative to
the raw root, the same key the nightly backup uses. Missing `location` means `local`.
The collector rewrites `catalog.json` from these columns, so a later maintenance pass
does not drop them.

**Offload.** `maintain offload --epochs <n|n-m|sealed> --verify` confirms each file in
R2: the listing size must match, and the SHA-256 must match the catalog hash. A stored
checksum on the listing is used when one exists; otherwise the body is streamed and
hashed. The catalog is then fsync'd with `location: both`. `--drop-local` unlinks the
local file only after that write, and only when the epoch is sealed. The deletion is an
`offload_deleted` log line (key, sha256, size, epoch). A crash after the `both` write
and before the unlink leaves the file in place; the same command deletes it on the next
run and then records `r2`. A file already at `r2` with no local copy is left alone, so
a finished run can be repeated without downloading the epoch again.

**Readers.** `verify-storage` does not hash r2-only files. `--remote` streams one
r2-only file per epoch and fails the run on a mismatch. The decoder skips a fully
consumed r2-only file and refuses an unfinished one instead of opening a missing path.
`reverify` skips a range whose transaction files are all r2-only and sealed, and refuses
a range that mixes those files with local files or an unsealed r2-only file. A raw
query that would open an r2-only file fails and names `restore`. `maintain restore
--epochs` downloads the objects, checks each SHA-256, and records `both`. A mismatch
deletes the temporary file and leaves `location` unchanged.

**Backup gap.** The nightly `rclone copy` also copies every `*.parquet` under the raw
root, size-only and immutable, whether or not `catalog.json` lists it. `*.tmp` files
are not included. `maintain backup-gap --older-than N` (default 30 hours) exits 2 when
a local catalogued file, or an uncatalogued Parquet file, is older than that and has no
R2 object. It reads the catalog and the directory tree. It does not open DuckDB.

**Who must be stopped.** One process owns each checkpoint (ADR-0016).

- `seal` opens the raw checkpoint and the decoder checkpoint. Stop
  `solos-data-phoenix.service` and `solos-data-phoenix-decoder.service`.
- `offload` and `restore` open the raw checkpoint. Stop the collector.
- `offload --drop-local` also holds the decoder checkpoint for the whole run, so the
  decoder cannot start while files are unlinked. Stop the decoder and pass
  `--decoded-root` (or `SOLOS_DATA_DECODED_DIR`).
- `offload --verify` without `--drop-local`, and `backup-gap`, do not open the decoder
  checkpoint. `backup-gap` does not open the raw checkpoint either.

Rclone gets `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, and `R2_SECRET_ACCESS_KEY`
from the backup env file, in the child environment only. Those values are not written
to a log or an error. Tests use an in-memory store and never read that file.

## Consequences

- A sealed epoch's durable copy is the R2 object. Losing the bucket loses that epoch.
  The bucket stays private, copy-only, and is not pruned by this command.
- The verify pass retrieves each object that has no stored SHA-256 once. Infrequent
  Access bills that retrieval. Repeating `offload --verify` after `location` is `r2`
  does not download those objects again.
- `collector query` and `reverify` of an off-box epoch need `restore` first. A skipped
  reverify range is `skipped: sealed-r2` and is not reported as a match against a
  re-walk.
- Compaction does not run inside a sealed epoch, so a sealed epoch keeps the files it
  had when it sealed.

## Acceptance

Recorded in [issue 003](../issues/003-sealed-epoch-offload.md). The mutation each test
names is the line whose deletion must turn that test red.
