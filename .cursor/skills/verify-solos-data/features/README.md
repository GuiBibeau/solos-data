# solos-data offline verification map

This directory is the maintained source for verifying the offline behavior the
`verify-solos-data` skill drives. Read the index before driving the binary, then use the
matching feature file as the recipe. Each file answers, from the user's point of view: what
the feature is, how to reach it, how to drive it with the binary, and what observable end
state proves it works.

## Baseline preconditions

- Build the binary once: `cargo build --locked --bin solos-data` into `target/debug/`.
- Run every drive through `node .cursor/skills/verify-solos-data/run.mjs`, which creates a
  fresh `target/verify-evidence/run.<id>` scratch directory and scrubs subprocess environments
  to the build allowlist.
- There is no `.env` at the repository root and no `SOLOS_DATA_*`, `*_RPC_*`, `*_API_KEY` or
  `JETSTREAMER_*` variable in the invoking environment.
- Never drive a scratch directory that was not created by this verification run.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say otherwise.
- Treat every command as literal. Keep quoted names and flags unchanged.
- The harness is the binary itself (`solos-data dev ...`, `solos-data decoder ...`,
  `solos-data collector ...`, `solos-data spot ...`); the skill spawns one subprocess per
  drive.
- Restore seeded data after a mutation. Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- Capture the command and the resulting state, not only the exit code.
- CLI proof includes the command, stdout, stderr, and exit code.
- Mutation proof includes a read-only second view of the stored value (for example the
  watermark before and after a refused range check).
- Record the feature ID and entry point used with every artifact.
- Report an unreachable path with the attempted command and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible
behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with the binary` starts with `Preconditions:` and uses labeled bullets that
   pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required
state, commands, and observable proof.

## Features

- [Fixture raw decodes to Parquet](./decoder-publish.md) covers materialising the golden raw
  fixture, the production decode, and the golden row-count/hash check.
- [A query over the fixture catalog](./decoder-query.md) covers reading the catalog and
  answering SQL over the published tables.
- [Watermark/range check refuses a gap](./range-watermark.md) covers the complete range, the
  gap refusal, and the unchanged watermark.
- [Spot fixture publish](./spot-publish.md) covers the durable write, the watermark advance,
  and the spot lane's own file check.
