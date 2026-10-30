# ADR-0018: PR checks finish inside a five-minute budget when warm

Status: accepted, October 30, 2026. Records the CI cache and gating scheme that keeps warm
`pull_request` checks — a cache hit on main's cache — inside a five-minute budget and keeps the
archive compile check off unrelated PRs. Cold, never-cached runs are slower; that is accepted
(issue #68), and pull requests can pay it too.

## Decision

- **The target is a five-minute budget for warm runs.** A warm run is a cache hit on main's
  cache; the measured warm run finishes in about three minutes. Cold, never-cached runs are
  slower (about 22 minutes wall) and that is accepted. Cold runs happen when main's cache is
  missing (before the first save, after a `shared-key` bump, or after eviction: GitHub drops
  caches unused for 7 days, and the repository is already near its 10 GB cache limit) or when a
  PR bumps native dependencies; pull requests then pay the cold cost too.
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
  other PR skips the whole job, which removes the 22-minute cold compile from the common case.
- **Every Rust job uses a shared cache key with a main-only save.** The three cargo jobs pass
  `shared-key` to Swatinem/rust-cache and `save-if: github.ref == 'refs/heads/main'`. Pull
  requests only restore; only pushes to main save, so a main merge is what keeps the cache warm.
  Swatinem/rust-cache's restore key omits the Cargo.lock hash, so a PR that changes the lockfile
  still restores main's warm cache and recompiles only the crates whose versions actually changed
  instead of rebuilding DuckDB, RocksDB, protobuf and the agave crates.
- **`ci-ok` is the single required check.** It aggregates `fmt`, `clippy`, `test` and
  `archive-check`, and treats a skipped `archive-check` as a pass only when the `changes` job
  said no archive inputs changed. Branch protection must require `ci-ok`, not the individual
  jobs, because GitHub does not accept a skipped required job. `ci-ok` only protects merges once
  branch protection requires it; Gui will set that up.
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

That working set is about 1.35 GiB, but the raw list still shows older entries (older
lockfile-hash suffixes from before the main-only save) that sum to about 10.4 GiB, so the
repository is already at or near its 10 GB cache limit. GitHub evicts least-recently-used
entries as new saves land and drops any cache unused for 7 days; only the live entry per
shared-key is restored. If main's live entries are evicted (for example after a week with no
merge to main), every PR runs cold until main saves again.

## Consequences

- A PR that does not touch the archive inputs sees `fmt` (seconds), `clippy` and `test` restored
  from main's warm cache (both under the five-minute warm budget), a skipped `archive-check`,
  and a green `ci-ok` that accepts that skip.
- A cold, never-cached run does not meet the five-minute budget, and that is accepted. Measured
  on a forced fresh-cache run (https://github.com/GuiBibeau/solos-data/actions/runs/41022348960),
  which came from a throwaway measurement commit (e603b5f, `shared-key: test-cold-measure`):
  `clippy` 14 m 6 s, `test` 16 m 48 s, `archive-check` 22 m 30 s, total wall 22 m 45 s. The warm
  run (https://github.com/GuiBibeau/solos-data/actions/runs/41025154500) finished in about
  3 minutes wall with `test` at 2 m 54 s. A pull request goes cold whenever main's cache is
  missing (before the first save, after a `shared-key` bump, or after eviction) or when it bumps
  native dependencies. A PR whose lockfile hash misses still restores main's cache as a fallback
  (Swatinem's restore key omits the Cargo.lock hash) and recompiles just the changed crates.
- A PR that does touch the archive inputs also runs `archive-check`, warm from main's cache
  (about a minute). A PR that bumps the native dependency versions themselves (RocksDB, DuckDB,
  protobuf or the agave crates) recompiles those C++ and Rust graphs genuinely, so that PR pays
  close to the cold cost; the recompilation is still a required check, and main pays the same
  cost again on merge.
- Cache writes happen only on main, so PR runs no longer churn the shared cache with
  near-duplicate entries. Each job's `shared-key` is the stable job name (`clippy`, `test`,
  `archive-check`); bump the suffix to start the cache fresh if the scheme ever changes.
