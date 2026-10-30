# verify-solos-data

Project-local verification skill: drives the real `solos-data` binary through offline
journeys and leaves reviewable evidence. Unit tests passing is not enough to show a change
works; this proves the binary itself publishes the golden fixtures, answers SQL over the
catalog, refuses to advance the watermark over a gap, and decodes the spot fixtures.

Run it after `scripts/verify.sh` passes:

```sh
node .cursor/skills/verify-solos-data/run.mjs
```

It builds `target/release/solos-data`, runs four offline journeys, prints `PASS` or `FAIL`,
and writes `target/verify-evidence/evidence.json` plus `commands.log`.

## Doctor

Before any journey the skill checks:

- no `.env` file is present at the repository root;
- no provider URL or RPC environment variables are set (`*_RPC_URL`, `*_RPC_ENDPOINT`,
  `*_RPC_HOST`, `PROVIDER_URL`);
- no data-directory overrides are set (`SOLOS_DATA_DIR`, `SOLOS_DATA_RAW_DIR`,
  `SOLOS_DATA_DECODED_DIR`); subprocesses are scrubbed of them anyway, but the doctor
  refuses to start when the invoking shell exported any of them;
- a fresh temp run directory is created under `target/verify-evidence/`;
- `solos-data dev version` reports the binary and bundled DuckDB versions;
- the release binary is built from the current source.

Everything is offline; no journey touches a real data directory.

## Journeys

1. **Fixture raw decodes to Parquet.** `dev make-decoder-fixture` materialises
   `tests/data/phoenix-events.json` as a raw Parquet source plus `catalog.json`; the
   production decoder (`decoder once`) consumes it through the same source-reading,
   durable-write and watermark logic the live decoder uses. `dev decoder-verify` checks the
   published tables against `golden/decoder.json` (per-table row counts and one content hash
   over every latest-revision row except the wall-clock `decoded_at`).
2. **A query over the fixture catalog.** `decoder query` reads `catalog.json` to locate the
   Parquet files and answers `SELECT count(*) AS n FROM fills`; the result must equal the
   golden `fills` count.
3. **Watermark/range check refuses on a gap fixture.** `dev make-range-fixture` builds a
   collector checkpoint with a `backfill` cursor and a complete manifest/transactions range,
   and a second one whose middle slot's transaction is missing. `collector
   validate-next-backfill` passes the complete range and refuses the gap (non-zero exit,
   `watermark will not advance`), through the same `validate_range`/`assert_publishable` pair
   the publication path runs before advancing the watermark (ADR-0016).
4. **Spot fixture decode.** `dev spot-fixture-decode` decodes every transaction of
   `tests/data/spot-fixtures.json` through the RPC-shape spot decoder and compares the rows
   with the fixture's recorded rows.

## Evidence

`target/verify-evidence/evidence.json` records the git SHA, binary and DuckDB versions, the
doctor results, every journey's command and exit code, and a decoder throughput baseline
(transactions decoded per second from the real pipeline's `status.json`). `commands.log`
lists each command with its exit code.

## Golden refresh

The golden file is a baseline for the pinned toolchain (DuckDB 1.4.5). After a deliberate
decoder change that alters published rows, regenerate it with:

```sh
node .cursor/skills/verify-solos-data/run.mjs   # run once to see the actual digest
```

then copy the `actual` rows and `actualContentSha256` from the failed `decoder-verify`
journey into `golden/decoder.json`, review the diff, and re-run. Never regenerate the golden
to make a failing check pass without reviewing why the rows changed.
