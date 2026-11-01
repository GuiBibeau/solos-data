# Sealed raw epochs can leave the disk

`phoenix_raw/staging` is about 1.65 TB. The catalog keeps every active file local, so
verification, the decoder, and reverify keep reading it. The nightly backup copies only
catalogued paths, and the catalog lags the durable files: on 2026-11-01, 810 recent
files (3.6 GB) were missing from R2. ADR-0020 is the decision. R2 is the sole durable
copy of an epoch only after the checks below have passed on the box.

## Acceptance criteria

- [x] An epoch is marked sealed in the raw catalog only when it is older than the current
  epoch by the lag, its slot span is covered by published ranges, every active file
  matches its SHA-256 and row count, and every transactions file is fully consumed by
  the decoder. The current epoch is not sealed (`the_current_epoch_is_not_sealed`,
  `an_uncovered_epoch_is_not_sealed`, `an_undecoded_epoch_is_not_sealed`).
- [x] `publish_range` writes nothing into a sealed epoch (`publish_refuses_a_sealed_epoch`).
  Raw compaction skips sealed epochs.
- [x] `maintain offload --epochs <n|n-m|sealed> --verify` records `location`, `r2_key`,
  `sha256`, and size only after the R2 object matches. `--drop-local` deletes the local
  file only after that catalog write, and only for a sealed epoch. A missing object, a
  size mismatch, or a SHA-256 mismatch leaves the local file
  (`no_drop_without_a_verified_r2_copy`, `a_size_mismatch_blocks_the_drop`,
  `a_sha_mismatch_blocks_the_drop`). A crash after the `both` record and before the
  unlink leaves the file and a catalog that still says `both`
  (`a_crash_between_verify_and_delete_leaves_a_consistent_catalog`). The command is
  idempotent. It does not run without `--verify`.
- [x] The collector is stopped for seal, offload, and restore (they open its checkpoint).
  Seal and `--drop-local` also require the decoder to be stopped. `backup-gap` does not
  open DuckDB. The runbook states the order.
- [x] `verify-storage` does not re-hash r2-only files. `--remote` can reject a bad
  sample (`verify_skips_r2_only_files_and_a_remote_sample_can_reject_them`). The decoder
  skips a consumed r2-only file and refuses an unconsumed one
  (`the_decoder_skips_a_consumed_r2_file_and_refuses_an_unconsumed_one`). Reverify does
  not open a sealed r2-only range and does not skip a mixed or unsealed one
  (`sealed_r2_ranges_are_not_opened_and_unsealed_ones_are_not_skipped`). A raw query
  refuses an r2-only file (`a_query_refuses_an_r2_only_file`).
- [x] `maintain restore --epochs` downloads the epoch and checks SHA-256
  (`restore_round_trip_checks_sha256`).
- [x] The nightly backup copies `*.parquet` under the raw root whether or not the catalog
  lists them. `maintain backup-gap` exits 2 when a local file older than N hours is
  missing from R2 (`backup_gap_alerts_when_an_old_local_file_is_not_in_r2`).
- [x] Tests use an in-memory object store. They do not read R2 credentials.

Each test's doc comment names the mutation that must turn it red.
