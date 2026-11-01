# Hyperliquid l2Book real-archive revision: open items

Follow-ups to this branch's revision of `solos-data augment hl-l2book`
(ADR-0021). The items this branch resolves from the PR #97 review list of
`aa3aeb6` are recorded here rather than in that PR's file; the line numbers
below refer to this branch.

## Resolved on this branch

- [x] A book missing from the MANIFEST aborted the whole run. An in-scope
  finished `.lz4` with no manifest row now makes its hour pending
  (`hoursPending` plus `hoursUncovered`), and `augment hl-l2book-manifest`
  builds the sha256 manifest from the downloader TSV (MD5 etag as a gate
  only), so coverage is a one-command fix. `.part` files are never read.
- [x] No directory fsync after the raw unlinks. The drop now fsyncs each
  parent directory after the deletes; `the_raw_unlink_dirs_are_fsynced` goes
  red without it.
- [x] Duplicate `time_ms` aborted the hour. `DuplicateRule` drops identical
  re-sends (`dup_identical`) and keeps distinct books in archive line order
  (`dup_distinct`); never an error, counted in the receipt and the summary.

## Open

- [ ] The count check after `seal_hour` is defense in depth (`prepare_hour`
  already returns that error). Kept, documented as such.
- [ ] Drop-record durability (`write_drop_record`) has no fault test of its
  own. Two mutants survive: `write_durable` changed to `write_atomic`, and
  `create_dir_all_durable` changed to `create_dir_all`.
- [ ] The `data.time` upper bound (`decode.rs`) is tested only at the lower
  end. Add a fixture past 4102444800000.
- [ ] `drop_raw_refuses_a_file_whose_size_grown` goes red only on the error
  message. Tighten it so removing the size check is what fails.
- [ ] The manifest builder holds the TSV key map in memory (about 1 GB for
  4.4 million rows) and flushes its state sidecar every 8 GiB of new hashing.
  Both are fine on the archive box; measure a full-archive run before trusting
  them for the 660 GB tree.
- [ ] `--jobs` workers each hold one hour's bytes, spill JSONL, and an
  in-memory DuckDB. Measured at tens of MB per worker on the sample hours; a
  peak 2026 hour (all coins) should be measured before the default moves
  above 1.
