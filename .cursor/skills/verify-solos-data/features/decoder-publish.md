# Fixture raw decodes to Parquet

A researcher materialises the golden Phoenix transactions as a raw Parquet source and catalog,
runs the production decoder over it, and checks the published tables against a committed golden
row count and content hash.

## Sub-features

- `decoder-fixture-build` materialises `tests/data/phoenix-events.json` as a raw source and catalog.
- `decoder-once` runs the production decoder over that raw source.
- `decoder-golden` checks per-table row counts and the canonical content hash against `golden/decoder.json`.

## How to get to it (user POV)

- Run `solos-data dev make-decoder-fixture --out <dir>` to write the raw source.
- Run `solos-data decoder once --config <config.json>` to publish the decoded tables.
- Run `solos-data dev decoder-verify --root <decoded dir> --golden <file>` to compare.

## Driving it with the binary

Preconditions:

- The binary is built from the current source (`target/debug/solos-data`).
- The scratch directory is fresh and under `target/verify-evidence/`.
- `golden/decoder.json` is the committed baseline.

- **Materialise the raw source.** Run
  `solos-data dev make-decoder-fixture --out <run>/fixture`. Exit code `0` and a report whose
  `transactions` equals the fixture count. `<run>/fixture/raw/catalog.json` and
  `<run>/fixture/raw/staging/tx.parquet` exist.
- **Run the production decoder.** Write a decoder config whose `rawDir` is
  `<run>/fixture/raw` and whose `dataDir` is `<run>/decoded`, then run
  `solos-data decoder once --config <config.json>`. Exit code `0`.
- **Check the golden.** Run
  `solos-data dev decoder-verify --root <run>/decoded --golden golden/decoder.json`.
  Exit code `0`, `rowsMatch` true and `contentSha256Match` true.
- **Proof.** Capture the decoder-verify stdout (the expected/actual rows and both hashes) in
  `target/verify-evidence/evidence.json`.

## Gotchas

- The raw source must come from `dev make-decoder-fixture`, never a real data directory.
- The golden hash is the canonical `content_digest` (ADR-0017): it omits `decoded_at` and
  `source_file`, so a wall-clock stamp or a raw-file byte-hash change does not move it.
- A deliberate decoder change that alters rows fails `decoder-verify`; regenerate the golden
  only after reviewing the row diff.
