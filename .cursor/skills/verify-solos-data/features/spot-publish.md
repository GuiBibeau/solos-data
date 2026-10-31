# Spot fixture publish

A researcher decodes the spot fixtures through the RPC-shape decoder, publishes the rows through
the production durable write and watermark (not an in-process decode), and checks the published
lane's files against its own batch counts.

## Sub-features

- `spot-decode` decodes every fixture and compares the rows with the fixture's recorded rows.
- `spot-publish` writes the swaps, liquidity and batches Parquet files and advances the watermark.
- `spot-check` verifies the published files against the batches table, with no hole or overlap.

## How to get to it (user POV)

- Run `solos-data dev spot-fixture-publish --root <dir>`.
- Run `solos-data spot check --config <dir>/config.json`.

## Driving it with the binary

Preconditions:

- The spot root is fresh and under `target/verify-evidence/`.

- **Publish.** Run `solos-data dev spot-fixture-publish --root <run>/spot`. Exit code `0`, and a
  report whose `ok` is true, `transactions` is greater than zero, and `swaps`/`liquidity` are
  greater than zero. The report's `watermarkNext` is below the fixture's lowest slot.
- **Check the lane.** Run `solos-data spot check --config <run>/spot/config.json`. Exit code `0`
  (the check exits `2` on a hole or a file mismatch).
- **Proof.** Capture the publish report and the check report in
  `target/verify-evidence/evidence.json`.

## Gotchas

- The proof is the durable output: the publish must run `write_batch_to` and
  `register_batch_to`, not only decode into memory.
- The spot lane's files live under `<root>/spot-history/tables/`; `spot check` reads the catalog
  written beside them, so the config's `dataDir` must be the publish root.
- A zero-transaction fixture is a sign the fixture file was not read; the publish asserts
  `transactions > 0`.
