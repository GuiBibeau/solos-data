# Watermark/range check refuses a gap

A researcher validates the next backfill range before publication: a complete range passes and a
range whose middle transaction is missing refuses with `watermark will not advance`, leaving the
backfill watermark unchanged.

## Sub-features

- `range-complete` passes `validate-next-backfill` on a tiled range.
- `range-gap` refuses the same range when one slot's transaction is missing.
- `range-watermark-unchanged` proves the refusal did not advance the backfill watermark.

## How to get to it (user POV)

- Run `solos-data dev make-range-fixture --out <dir> [--gap]`.
- Run `solos-data collector validate-next-backfill --config <dir>/config.json`.
- Run `solos-data dev collector-watermark --config <dir>/config.json`.

## Driving it with the binary

Preconditions:

- The fixture directories are fresh and under `target/verify-evidence/`.

- **Complete range.** Run
  `solos-data dev make-range-fixture --out <run>/range-good --chunk 10 --next 5000`, then
  `solos-data collector validate-next-backfill --config <run>/range-good/config.json`.
  Exit code `0`.
- **Gap fixture.** Run
  `solos-data dev make-range-fixture --out <run>/range-gap --chunk 10 --next 5000 --gap`.
  Exit code `0`.
- **Gap refusal.** Run
  `solos-data collector validate-next-backfill --config <run>/range-gap/config.json`.
  Non-zero exit code and stdout contains `watermark will not advance`.
- **Watermark unchanged.** Run
  `solos-data dev collector-watermark --config <run>/range-gap/config.json` before and after the
  refusal. Both reports' `next` equal `5000`.
- **Proof.** Capture both `validate-next-backfill` reports and both watermark reports in
  `target/verify-evidence/evidence.json`.

## Gotchas

- The gap refusal must come from the production `assert_publishable` path (ADR-0016), not from a
  fixture-only flag.
- Reading the watermark opens the checkpoint with the same exclusive lock the validate command
  uses; run the reads before and after the refusal, never while it is in flight.
- The watermark is the `backfill.next` record; a refusal leaves it untouched by design.
