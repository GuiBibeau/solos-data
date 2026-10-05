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

For an existing large checkpoint, stop its writer and run:

```sh
npm run collector -- maintain --all
npm run collector -- repack
npm run collector -- verify-storage
```

Then restart the collector. `maintain --all` verifies archived contents before
trimming copies. `repack` verifies every table before atomically replacing the
closed checkpoint. Never run these offline commands while a writer is running.
`maintain --all --legacy` optionally scans superseded files from older releases.
This can take much longer and retains any file whose full contents do not match
the latest archive. It is separate from routine cleanup and checkpoint rewrites.
Use decoder `repack` and `verify-storage` with its writer stopped if needed.
`validate-next-backfill` reports a blocked historical range;
`repair-next-backfill` repairs ordering using already cached finalized blocks only.
R2 is optional; cleanup preserves the whole archive locally.
Do not point two writers at the same checkpoint directory.

## Native Linux services

The example units expect the checkout at `~/solos-data`. Edit their working directory
if you use another location. They use a private Node.js runtime installed by
`ops/install-runtime.sh` (Linux x64). The Rust codec can be built with Docker.

```sh
sh ops/install-runtime.sh
export PATH="$HOME/.local/share/solos-data/runtime/bin:$PATH"
npm ci
docker build -t solos-data:local .
mkdir -p bin
codec_container=$(docker create solos-data:local)
docker cp "$codec_container":/app/bin/solos-data-phoenix-codec bin/
docker rm "$codec_container"
```

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
SOLOS_DATA_DIR=/path/to/phoenix_raw npm run collector -- status
SOLOS_DATA_DECODED_DIR=/path/to/decoded/v1 npm run decoder -- status
```

## Move the data

Stop both writers, copy both data roots and the private credential file, then
point the new services at those paths. Before restarting a moved raw collector:

```sh
SOLOS_DATA_DIR=/path/to/new/raw npm run collector -- relocate
```

Decoded catalogs use relative paths and need no rebase. Restart with the same
codec/schema version. [Table details](../docs/decoded-tables.md) cover read-only copies.

## Verify changes

With Node.js and Rust installed, run `npm run verify`. Tests are offline and need
no credentials. On a Linux host with only the extracted codec, run `npm run check`
and `SOLOS_DATA_CODEC="$PWD/bin/solos-data-phoenix-codec" npm test`.
