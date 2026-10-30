# verify-solos-data

Project-local verification skill: drives the real `solos-data` binary through offline
journeys and leaves reviewable evidence. Unit tests passing is not enough to show a change
works; this proves the binary itself publishes the golden fixtures, answers SQL over the
catalog, refuses to advance the watermark over a gap, and publishes the spot fixtures through
the durable write and watermark.

Run it after `scripts/verify.sh` passes:

```sh
node .cursor/skills/verify-solos-data/run.mjs
```

It builds `target/debug/solos-data`, runs the four offline journeys mapped in
[`features/`](features/README.md), prints `PASS` or `FAIL`, and writes
`target/verify-evidence/evidence.json` plus `commands.log`.

## Launch

The skill drives a short-lived CLI, so there is no server to keep alive. Launch means building
the binary once from the current source, then starting each journey as its own isolated
subprocess:

```sh
cargo build --locked --bin solos-data   # into target/debug/solos-data
```

The debug profile is deliberate: it reuses the same build profile as `cargo test`, so the
offline journeys share the test suite's cached objects, and the fixture materialisation
commands (`dev make-decoder-fixture`, `dev make-range-fixture`) stay out of release builds
(the `fixtures` module is gated to debug/test builds). Ready means `solos-data dev version`
reports the binary and bundled DuckDB versions. Every subprocess runs with the environment
allowlist in [`env.mjs`](env.mjs): only `PATH`, `HOME`, `TMPDIR`, `LANG` and the
`RUSTUP_*`/`CARGO_*` build variables survive, so no data-directory, provider or archive
override can redirect a journey.

## Doctor

Before any journey the skill checks, and refuses (fatal) if any fail:

- no `.env` file is present at the repository root;
- no provider, credential or data-directory environment variables are set (`SOLOS_DATA_*`,
  `*_RPC_URL`/`*_RPC_ENDPOINT`/`*_RPC_HOST`, `PROVIDER_URL`, `*_API_KEY`, `JETSTREAMER_*`);
- `solos-data dev version` reports the binary and bundled DuckDB versions;
- the binary builds from the current source;
- a fresh temp run directory is created under `target/verify-evidence/`.

The two environment checks are fatal *before* the build and before any journey: a `.env` or an
exported provider/credential variable refuses to start, it does not merely record a failure.
Everything is offline; no journey touches a real data directory.

## Journeys

The feature map in [`features/README.md`](features/README.md) is the maintained source: each
file gives the user path, the exact drive, and the observable end state.

1. **Fixture raw decodes to Parquet.** [`decoder-publish.md`](features/decoder-publish.md).
2. **A query over the fixture catalog.** [`decoder-query.md`](features/decoder-query.md).
3. **Watermark/range check refuses on a gap, leaving the watermark unchanged.**
   [`range-watermark.md`](features/range-watermark.md).
4. **Spot fixture publish through the durable write and watermark.**
   [`spot-publish.md`](features/spot-publish.md).

## Evidence

`target/verify-evidence/evidence.json` records the git SHA, binary and DuckDB versions, the
doctor results, every journey's command and exit code, and a decoder throughput baseline
(transactions decoded per second from the real pipeline's `status.json`). `commands.log`
lists each command with its exit code.

## Cleanup

Cleanup removes the scratch run directories `target/verify-evidence/run.*` created by the
run, including any left behind by a failed earlier attempt. It never removes the evidence:
`evidence.json` and `commands.log` survive at `target/verify-evidence/`. The cleanup runs after
the evidence is written and also after any journey failure, so no attempt strands fixture
directories or a ledger lock.

## Golden refresh

The golden file is a baseline for the pinned toolchain (DuckDB 1.4.5). After a deliberate
decoder change that alters published rows, regenerate it with:

```sh
node .cursor/skills/verify-solos-data/run.mjs   # run once to see the actual digest
```

then copy the `actual` rows and `actualContentSha256` from the failed `decoder-verify`
journey into `golden/decoder.json`, review the diff, and re-run. Never regenerate the golden
to make a failing check pass without reviewing why the rows changed.
