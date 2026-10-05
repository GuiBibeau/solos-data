# Live decoded dataset

Canonical output is MIT-compatible open-source tooling: Apache Parquet/ZSTD files,
DuckDB SQL, Node.js and the official Rust Phoenix Rise events SDK. Repository code
is MIT licensed. Data and credentials remain private runtime files; licensing the
collector code does not publish either.

Default raw input: `data/phoenix_raw`.
Default decoded output: `data/phoenix_decoded/v1`.
`catalog.json` registers relative file paths, SHA256 hashes, row counts and batch IDs.
`checkpoint.duckdb` holds resume/dedupe metadata, not a second full analytical copy.
Files are partitioned by table and Solana epoch, sorted by slot and signature.
Small files are compacted every minute. Superseded inputs have a ten-minute
reader-aware grace period; historical rows remain in their verified replacements.

| Table | Contents |
| --- | --- |
| decoded_transactions | Latest decode outcome, source hash, status, event/error counts |
| events | Every fully decoded instruction's events, including failed attempts marked committed=false |
| fills | Committed OrderFilled and SplineFilled, maker side, native ticks/lots, exact sequences |
| order_events | Committed placements, modifications/cancellations, rejections and residual discards |
| funding_events | Committed TraderFundingSettled payments and collateral/funding accumulator values |
| decode_errors | Quarantined binary payloads/extraction failures, ready for later versioned reprocessing |

Common event columns include signature, slot, block_time, tx_index/single_in_slot,
instruction_path, event_ordinal, event_type, asset_symbol/asset_id, header signer,
trader context, tick_size and lot decimals, decoder_version and event_json. `trader`
is an explicit event trader or the header's trader account; it is not a universally
verified taker identity. Maker side is the maker's perspective; taker_side is the
opposite. Order modifications retain their reason; they are not all cancellations.
Funding settlement rows are payments, not a time series of market funding rates.

All JSON integer values are decimal strings, including byte-array elements. Native
typed quantities use DuckDB/Parquet BIGINT or UBIGINT without floating-point rounding.
`event_json` preserves the complete SDK variant. Header metadata stays attached to
each event. USD conversions and reconstructed state need separate validated logic.

Run a query without opening the live checkpoint writer:

```sh
export SOLOS_DATA_DECODED_DIR=./data/phoenix_decoded/v1
solos-data decoder status
solos-data decoder query --sql 'SELECT asset_symbol, count(*) AS fills FROM fills GROUP BY asset_symbol ORDER BY fills DESC LIMIT 20'
solos-data decoder query --sql 'SELECT status, count(*) AS transactions FROM decoded_transactions GROUP BY status'
solos-data decoder query --sql 'SELECT event_type, count(*) AS n FROM events GROUP BY event_type ORDER BY n DESC'
```

Use the registered-file reader, not an arbitrary Parquet glob: overlap/corrections
are deduplicated against each transaction's latest hash. A quarantined transaction
can have other fully decoded instructions; check its status when strict whole-
transaction completeness matters. Raw independent validation and partition sealing
remain pending. This dataset is incrementally useful, not certified complete.

Native setup requires Rust 1.96:

```sh
cargo build --release -p solos-data
export SOLOS_DATA_RAW_DIR=/path/to/published/raw
export SOLOS_DATA_DECODED_DIR=/path/to/decoded/v1
target/release/solos-data decoder watch
```

Portable deployment can use `docker compose build` and `docker compose up -d`.
Provide SOLANA_RPC_URL through the shell or a private env file. It is required only
for the collector; the decoder has no credential. Mount raw input read-only for the
decoder; its `.readers` subdirectory must be writable for cleanup leases. Compose
shares the collector's PID namespace so abandoned reader leases can be recognized.
Do not run Compose and systemd collectors on the same checkpoint directory.

To move decoded data, gracefully stop its writer, copy the entire decoded root
(including checkpoint, catalog and tables), and point SOLOS_DATA_DECODED_DIR at the
new location. Restart with the same codec/schema version. For read-only analytics,
copy just catalog.json and registered Parquet files, and run `query` on that root.
The catalog uses relative paths; no Alchemy credential is needed. Relocating the raw
collector also requires rebasing its legacy absolute registrations before resuming;
see the collector's offline `relocate` command.

Decoder staging tables are recreated for each batch so temporary storage remains
bounded. `SOLOS_DATA_QUERY_MEMORY` sets the native query working-memory limit
(default 4GB). Stop the decoder before `repack` or `verify-storage`.
