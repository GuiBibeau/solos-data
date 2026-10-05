# Run on a server

Docker Compose is the simplest setup. Follow the [README](../README.md), then use:

```sh
docker compose logs --tail=20 collector decoder
docker compose stop
docker compose up -d
```

`.env` holds the RPC URL and rate setting. Keep it out of Git and mode 0600.
Data is stored under `data/`. Back up both raw and decoded directories.

Cleanup runs every minute. Historical raw and decoded Parquet remain on disk.
The checkpoint retains unpublished work and a recent 10,000-slot working set.
The raw writer automatically rewrites a growing checkpoint after it exceeds
both 16 GiB and twice its previous compact size. Database writes queue briefly
during the verified rewrite; immutable Parquet remains queryable.
Superseded inputs are removed after ten minutes when no local reader is using
an old catalog and the replacement passes checksum/count verification.
`status` counters describe the hot checkpoint; use `query` for historical totals.
`retentionSlotsPerPass` (default 16,000) bounds one routine trimming pass; raise it when the
archive lane publishes faster than the checkpoint is trimmed, or the checkpoint grows until the
next rewrite.

Historical collection below the last published Old Faithful epoch comes from the
archive lane (ADR-0008) when `archiveBackfillEnabled` is true in `config/phoenix.json`.
It needs no credential; it reads `files.old-faithful.net` and spends compute units
only on the signature manifest and a sampled `getBlock` cross-check. Set
`JETSTREAMER_THREADS` to bound its parallelism on a shared machine. After a burst of provider
throttles the limiter recovers slowly by design; restarting the collector resets its rate.

For an existing large checkpoint, stop its writer and run:

```sh
solos-data collector maintain --all
solos-data collector repack
solos-data collector verify-storage
```

Then restart the collector. `maintain --all` verifies archived contents before
trimming copies. `repack` verifies every table before atomically replacing the
closed checkpoint. Never run these offline commands while a writer is running.
`maintain --all --legacy` optionally scans superseded files from older releases.
This can take much longer and retains any file whose full contents do not match
the latest archive. It is separate from routine cleanup and checkpoint rewrites.
Use `solos-data decoder repack` and `verify-storage` with its writer stopped if needed.
`validate-next-backfill` reports a blocked historical range;
`repair-next-backfill` repairs ordering using already cached finalized blocks only.
R2 is optional; cleanup preserves the whole archive locally.
Do not point two writers at the same checkpoint directory.

## Native Linux services

The example units expect the checkout at `~/solos-data` and the binary at
`~/.local/share/solos-data/bin/solos-data`. Build the binary in Docker on the
server (the archive lane needs clang and cmake, which the build image installs):

```sh
DOCKER_BUILDKIT=1 docker build --target artifact --output type=local,dest=/tmp/solos-data-out .
mkdir -p ~/.local/share/solos-data/bin
install -m 0755 /tmp/solos-data-out/solos-data ~/.local/share/solos-data/bin/solos-data
~/.local/share/solos-data/bin/solos-data collector help
```

The build stage runs the test suite first. Set `JETSTREAMER_THREADS` in `collector.env` to bound
the archive lane's parallelism (64 is a good start on a large machine); without it Jetstreamer
assumes a 1 GB/s link and picks very few threads.

Create `~/.config/solos-data/collector.env` in a text editor with these variable
names. Add your RPC URL, choose your rate, and use an absolute raw data path:

```dotenv
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_API_KEY
SOLOS_DATA_DIR=/path/to/phoenix_raw
SOLOS_DATA_CU_PER_SECOND=1000
```

Set `SOLOS_DATA_DIR` to the raw path used by the decoder unit. Its default is
`~/.local/share/solos-data/phoenix_raw`. The unit memory limits are examples;
adjust them for your server. DuckDB defaults to 4GB unless `SOLOS_DATA_DB_MEMORY`
is set. `SOLOS_DATA_QUERY_MEMORY` independently controls analytical query memory
(default 4GB); increase it for full-history scans on a larger server. Effective CU/s is the configured rate multiplied by utilization.
`SOLOS_DATA_DB_CHECKPOINT` defaults to `256MB`. It controls automatic checkpoint
frequency; committed changes stay durable in the write-ahead log between checkpoints.

```sh
chmod 600 ~/.config/solos-data/collector.env
mkdir -p ~/.config/systemd/user
cp ops/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now solos-data-phoenix.service solos-data-phoenix-decoder.service
```

If jobs must continue after logout, configure user lingering on the server.
Read status without opening the writer's checkpoint:

```sh
SOLOS_DATA_DIR=/path/to/phoenix_raw solos-data collector status
SOLOS_DATA_DECODED_DIR=/path/to/decoded/v1 solos-data decoder status
```

## Switch a running TypeScript deployment to the binary

The Rust binary reuses the checkpoints and cursors as they are (ADR-0007). Install the
binary and the new unit files, then:

```sh
systemctl --user stop solos-data-phoenix-decoder.service
systemctl --user stop solos-data-phoenix.service
backup=~/.local/share/solos-data/backup-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup/raw" "$backup/decoded"
cp ~/.local/share/solos-data/phoenix_raw/{checkpoint.duckdb,catalog.json,status.json} "$backup/raw/"
cp ~/.local/share/solos-data/phoenix_raw/checkpoint.duckdb.wal "$backup/raw/" 2>/dev/null || true
cp ~/.local/share/solos-data/phoenix_decoded/v1/{checkpoint.duckdb,catalog.json,status.json} "$backup/decoded/"
cp ~/.local/share/solos-data/phoenix_decoded/v1/checkpoint.duckdb.wal "$backup/decoded/" 2>/dev/null || true
systemctl --user daemon-reload
systemctl --user start solos-data-phoenix.service
systemctl --user start solos-data-phoenix-decoder.service
journalctl --user -u solos-data-phoenix.service -f
```

Watch for the first `backfill_chunk`, `tail_chunk` and `decoded_batch` events and compare
`query` row counts with the numbers from before the stop. Rollback is the reverse `ExecStart`
swap in the unit files; keep the Node runtime for a week.

## Move the data

Stop both writers, copy both data roots and the private credential file, then
point the new services at those paths. Before restarting a moved raw collector:

```sh
SOLOS_DATA_DIR=/path/to/new/raw solos-data collector relocate
```

Decoded catalogs use relative paths and need no rebase. Restart with the same
codec/schema version. [Table details](../docs/decoded-tables.md) cover read-only copies.

## Verify changes

With Rust 1.96 installed, run `cargo test --workspace`. Tests are offline and need no
credentials. The archive lane compiles with `cargo build --features archive -p solos-data`,
which needs clang and cmake. Until the TypeScript sources are removed, `npm run verify`
still runs their suite.
