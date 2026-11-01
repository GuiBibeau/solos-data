# Sealed-epoch offload follow-ups

Left open after the ADR-0022 review. The checks below are untested or only partly
specified. They are not part of the seal and offload acceptance in
[issue 003](003-sealed-epoch-offload.md).

## Acceptance criteria

- [ ] The fsync after an offload unlink (`sync_dir_after_rename` on the parent), the fsync
  after a restore rename, and the `File::sync_all` of the restore temporary file each have
  a test that fails when that call is removed. `sync_all` on the temporary file does not go
  through `FsyncStep::TempFile`, so the existing fsync failpoint cannot reach it. Today
  `restore` records `location: both` only after the directory fsync, so a failed fsync
  returns before that write. A crash after the rename and before the directory fsync can
  still leave a file that the next restore refuses to replace when the hash does not match.
- [ ] `absent_or_error` in the rclone store treats exit code 3 and a "not found" stderr as a
  missing object, and any other failure as an error. A transport failure must not look like
  a missing object.
- [ ] `range_rows` refuses an r2-only file the same way a raw query does. The query path is
  tested. This reader is not.
- [ ] `seal` refuses an epoch that has coverage and a settled lag but no active transactions
  file (`no transactions`).
