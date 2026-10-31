# A query over the fixture catalog

A researcher answers SQL over the published tables, located through the decoded catalog, without
touching a real data directory.

## Sub-features

- `query-catalog` reads `catalog.json` to find the Parquet files.
- `query-fills` answers `SELECT count(*) AS n FROM fills` and matches the golden count.

## How to get to it (user POV)

- Run `solos-data decoder query --config <config.json> --sql <sql>`.

## Driving it with the binary

Preconditions:

- The decoded root was published by the decoder journey into `<run>/decoded`.

- **Query the catalog.** Run
  `solos-data decoder query --config <config.json> --sql "SELECT count(*) AS n FROM fills"`.
  Exit code `0` and stdout whose first row's `n` equals the golden `fills` count.
- **Proof.** Capture the query stdout in `target/verify-evidence/evidence.json`.

## Gotchas

- The query must read through `catalog.json`; pointing `dataDir` at a real directory would
  answer over live data instead of the fixture.
- The count is a decimal string in the reader's convention; compare it to the golden string.
