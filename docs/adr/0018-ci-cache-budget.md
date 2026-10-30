# ADR-0018: PR checks finish inside a five-minute budget

Status: accepted, October 30, 2026. Records the CI cache and gating scheme that keeps every
`pull_request` check inside the three-minute warm budget and keeps the archive compile check off
unrelated PRs, so no pull request waits on a 23-minute cold compile (issue #68).

## Decision

- **`fmt`, `clippy`, `test` and `archive-check` all keep running.** Nothing is dropped:
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo test --workspace --locked` and the archive compile check
  (`cargo check -p solos-data --features archive --locked`) still run. The archive check moved
  off unrelated PRs, not out of the pipeline: it always runs on pushes to main, on the nightly
  schedule and on `workflow_dispatch`, and it also runs on a PR whose diff touches its inputs.
- **The archive check is path-gated on pull requests, at the job level.** A `changes` job runs
  dorny/paths-filter once and exports the result; the `archive-check` job is gated on that
  output, so a skipped check is a skipped job, not a green job that compiled nothing. The filter
  covers everything that drives `cargo check -p solos-data --features archive`'s build graph: the
  workspace manifest (`Cargo.toml`), `Cargo.lock`, every crate manifest (`**/Cargo.toml`), the
  pinned toolchain (`rust-toolchain.toml`), this workflow (`.github/workflows/ci.yml`), the
  `solos-data` crate and its in-repo path dependencies (`crates/solos-data/**`,
  `crates/phoenix-codec/**`, `crates/solana-wire/**`) and the vendored Jetstreamer crate
  (`vendor/**`). Matching the crate's sources keeps archive-only callers honest: `spot/archive.rs`
  builds `SpotTx`, `Ix` and `Balance` from `spot/tx.rs`, and `venues/history/archive.rs` builds
  `Tx` from `venues/walk.rs`, so changing a shared type still compiles its archive caller. Every
  other PR skips the whole job, which removes the 23-minute cold compile from the common case.
- **Every Rust job uses a shared cache key with a main-only save.** The three cargo jobs pass
  `shared-key` to Swatinem/rust-cache and `save-if: github.ref == 'refs/heads/main'`. Pull
  requests only restore; only pushes to main save, so a main merge is what keeps the cache warm.
  Swatinem/rust-cache's restore key omits the Cargo.lock hash, so a PR that changes the lockfile
  still restores main's warm cache and recompiles only the crates whose versions actually changed
  instead of rebuilding DuckDB, RocksDB, protobuf and the agave crates.
- **`ci-ok` is the single required check.** It aggregates `fmt`, `clippy`, `test` and
  `archive-check`, and treats a skipped `archive-check` as a pass only when the `changes` job
  said no archive inputs changed. Branch protection must require `ci-ok`, not the individual
  jobs, because GitHub does not accept a skipped required job.
- **`cargo nextest` and split test jobs were not adopted.** With main's cache restored, `cargo test`
  is already under the budget; the extra machinery bought nothing. The CI npm step also stops
  re-running `cargo test --workspace` (npm's `codec:test`) because the Rust suite already ran in
  the job's own `cargo test` step — the same non-duplicative order `scripts/verify.sh` uses.
- **`scripts/verify.sh` is unchanged.** The offline regression script keeps running the same
  checks in the same order; only the GitHub Actions cache keying, the archive-check gating and
  the CI npm step changed.

## Cache sizes

Measured from the Actions cache list (`gh cache list`) on 2026-10-30, the live entry per key is:

| key | size |
| --- | --- |
| `npm` (node cache) | 29.16 MiB |
| `clippy` | 325.32 MiB |
| `test` | 488.99 MiB |
| `archive-check` | 543.49 MiB |

That working set is about 1.35 GiB against the repository's 10 GB cache limit. The raw list still
shows older entries (older lockfile-hash suffixes from before the main-only save) that sum to
about 10.4 GiB; GitHub evicts those least-recently-used entries as new main saves land, and only
the live entry per shared-key is restored.

## Consequences

- A PR that does not touch the archive inputs sees `fmt` (seconds), `clippy` and `test` restored
  from main's warm cache (both under the three-minute warm budget), a skipped `archive-check`,
  and a green `ci-ok` that accepts that skip.
- The one-time, never-cached ("cold") compile is a bootstrap cost that main pays, not PRs: with
  no cache at all, `clippy` is about 11 minutes and `test` about 15 minutes on the standard
  runner (measured on this PR's first run, which used a fresh cache key). A pull request only
  ever goes cold when its own lockfile hash misses, and then Swatinem's restore key (which omits
  the Cargo.lock hash) still restores main's cache and recompiles just the changed crates.
- A PR that does touch the archive inputs also runs `archive-check`, warm from main's cache
  (about a minute). The only case that can exceed the budget is a PR that bumps the native
  dependency versions themselves (RocksDB, DuckDB, protobuf or the agave crates), because those
  C++ and Rust graphs genuinely must recompile; that recompilation is still a required check,
  and main pays the same cost on merge.
- Cache writes happen only on main, so PR runs no longer churn the shared cache with
  near-duplicate entries. Each job's `shared-key` is the stable job name (`clippy`, `test`,
  `archive-check`); bump the suffix to start the cache fresh if the scheme ever changes.
