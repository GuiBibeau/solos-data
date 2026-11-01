# Hyperliquid l2Book follow-ups

Left open after the ADR-0021 review of `solos-data augment hl-l2book` (merged as
aa3aeb6). These are not part of that command's acceptance. Line numbers are
from that commit, in `crates/solos-data/src/augment/hll2book/`.

## Acceptance criteria

- [ ] There is no directory fsync after the raw unlinks (`batch.rs` 649-656). A
  crash can bring a file back. The retry deletes it again, so only
  `raw_bytes_dropped` on the interrupted run is affected.
- [ ] The count check at `batch.rs` 270-275 is unreachable, because
  `publish.rs` 151 already returns that error. Keep it as defense in depth
  (and say so) or remove it.
- [ ] Drop-record durability (`batch.rs` 709-712) is untested. Two mutants
  survive: `write_durable` changed to `write_atomic`, and
  `create_dir_all_durable` changed to `create_dir_all`. Add a test that arms
  `inject_fsync_failure(TempFile)` before `write_drop_record` and asserts the
  raw file is kept.
- [ ] The `data.time` upper bound (`decode.rs` 70) is untested. The existing
  test only rejects a seconds-scale value, which fails the lower end of the
  same range (`decode.rs` 71).
- [ ] `drop_raw_refuses_a_file_whose_size_grew` goes red only on the error
  message. The sha256 check still refuses the grown file. Tighten the test so
  removing the size check is what makes it fail.
- [ ] If any book is missing from the MANIFEST, the whole run aborts
  (`batch.rs` 182). That costs availability, not safety. Consider skipping the
  hour and reporting it instead.
