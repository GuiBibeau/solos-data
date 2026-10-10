# ADR-0010: Nightly off-box backup to Cloudflare R2

Status: accepted, October 10, 2026.

Every byte of the dataset lives on one disk in one server. The raw Phoenix archive is
regenerable only at the cost of weeks of archive and RPC backfill, and parts of the augment root
cannot be fetched again at all: the Hyperliquid candles and asset contexts (the venue serves a
rolling window) and the Elfa captures (live streams). The user approved an off-box copy on
October 10, 2026, as the backup half of the roadmap's first step.

## Decision

- **Destination.** A private Cloudflare R2 bucket, location hint Western Europe,
  default storage class Infrequent Access. A bucket-scoped R2 API token lives in
  `~/.config/solos-data/backup.env` (mode 0600; `R2_ACCOUNT_ID`, `R2_BUCKET`,
  `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`), written by the operator. It reaches rclone
  through environment variables only; no rclone config file holds a secret.
- **Tool.** rclone, a pinned release installed in user space by `ops/backup/install-rclone.sh`
  after checking the archive against the release's `SHA256SUMS`.
- **What.** The raw table files the raw `catalog.json` lists (published Parquet, including the
  archive lane's files under `staging/`) with that catalog, `status.json` and
  `capabilities.json`; the whole augment root except its DuckDB ledgers and `staging*/`; the
  checkout's `config/`; the `solos-*` user units and drop-ins. Not backed up: `phoenix_decoded`
  (regenerable from raw), every `checkpoint.duckdb*`, raw Parquet the catalog does not list
  (in-flight or retired), `.readers/`, `~/.config/solos-data/*.env`, `~/research/`.
- **Copy, never sync.** `rclone copy` adds and never deletes. Compaction retires superseded raw
  files on the box; their copies stay in the bucket, which therefore accumulates history. Raw
  file names are unique and immutable, so the raw pass compares sizes only and runs with
  `--immutable` (a changed object is an error, not an overwrite). Augment files for an open
  period grow in place, so that pass compares checksums from the listing and re-uploads only
  what changed. Nothing unchanged is rewritten: Infrequent Access bills a 30-day minimum.
- **Order.** The raw catalog is snapshotted first and its list drives the copy. If a listed file
  vanished during the run (compaction retired it), the run takes a new snapshot and copies
  again, at most three passes; if files are still missing the catalog is not uploaded and the
  run fails, so the bucket keeps the previous catalog, whose files it holds. Augment catalogs
  are snapshotted before the augment pass. Catalogs go last, to `phoenix_raw/` and `augment/`
  and to `catalogs/<UTC date>/` for point-in-time restores.
- **Schedule.** `solos-data-backup.timer` runs the oneshot service daily at 02:00 UTC with up to
  30 minutes of randomized delay, catching up after downtime. rclone runs under `nice -n 10`
  and `ionice -c3`, eight transfers, and 40 MiB/s between 06:00 and 22:00 UTC (unlimited at
  night). A lock prevents overlapping runs.
- **Reporting.** One JSON line (`event: backup`: files, bytes, duration, MB/s, errors, failed
  steps, whether the raw catalog went up) to the journal and the same object with per-step detail
  to `~/.local/share/solos-data/backup-status.json`, which carries `lastSuccessAt` forward. The
  health timer's `health_services` line reports the backup timer's state and `lastSuccessAt`.
- **Rehearsal.** `ops/backup/restore-check.sh` downloads one raw table-epoch directory (at most
  1 GB) and 20 random complete augment files and checks each against the SHA-256 the bucket's
  catalogs register.

## Consequences

- Storage is about $0.01 per GB-month: 791 GB on October 10, 2026, about $8 a month, growing
  with the archive and with every retired compaction input. Pruning old history is a later,
  deliberate decision, never a side effect of a run.
- A restore yields readable Parquet and catalogs, not a running collector: the DuckDB
  checkpoints (cursors, the hot working set) are not backed up. Resuming collection on restored
  data needs a fresh checkpoint and is outside this ADR.
- The bucket's raw catalog stores the box's absolute paths; the object key is the path with the
  raw root (`$HOME/.local/share/solos-data/phoenix_raw/` on the box) replaced by `phoenix_raw/`.
  Augment catalogs are relative to the augment root.
- Reads cost money (retrieval per GB, Class B per request), so the rehearsal stays small and the
  nightly run never downloads an object; it only lists them.
