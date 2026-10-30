# ADR-0017: PR checks finish inside a five-minute budget

Status: accepted, October 30, 2026. Records the CI cache and gating scheme that keeps every
`pull_request` check inside five minutes cold and three minutes warm (issue #68).

## Decision

- **`fmt`, `clippy`, `test` and `archive-check` all keep running.** Nothing is dropped:
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo test --workspace --locked` and the archive compile check
  (`cargo check -p solos-data --features archive --locked`) still run. The archive check moved
  off unrelated PRs, not out of the pipeline: it always runs on pushes to main, on the nightly
  schedule and on `workflow_dispatch`, and it also runs on a PR whose diff touches its inputs.
- **The archive check is path-gated on pull requests.** It runs on a PR only when the diff
  touches the archive code paths (`crates/solos-data/src/**/archive.rs`,
  `crates/solos-data/src/stall.rs`), the vendored Jetstreamer crate (`vendor/**`) or the manifests
  that drive its build graph (`Cargo.toml`, `Cargo.lock`, `**/Cargo.toml`). Every other PR skips
  it, which removes the 23-minute cold compile from the common case.
- **Every Rust job uses a shared cache key with a main-only save.** The three cargo jobs pass
  `shared-key` to Swatinem/rust-cache and `save-if:
  github.ref == 'refs/heads/main' || github.event_name == 'workflow_dispatch'`. Pull requests
  only restore; only main (and manual dispatches, which seed and maintain the cache) save.
  Swatinem/rust-cache's restore key omits the Cargo.lock hash, so a PR that changes the lockfile
  still restores main's warm cache and recompiles only the crates whose versions actually changed
  instead of rebuilding DuckDB, RocksDB, protobuf and the agave crates.
- **`cargo nextest` and split test jobs were not adopted.** The test job was already under the
  budget (`cargo test` about 2m10s cold), so the extra machinery bought nothing. If the suite
  grows past the budget, splitting test jobs or `cargo nextest` is the first lever.
- **`scripts/verify.sh` is unchanged.** The offline regression script keeps running the same
  checks in the same order; only the GitHub Actions cache keying and the archive-check gating
  changed.

## Consequences

- A PR that does not touch the archive inputs sees four checks: `fmt` (seconds), `clippy`
  (about two minutes cold), `test` (about two and a half minutes cold, including the npm suite),
  and a skipped `archive-check`. All are inside the five-minute cold and three-minute warm
  budgets.
- A PR that does touch the archive inputs also runs `archive-check`, warm from main's cache
  (about a minute). The only case that can exceed the budget is a PR that bumps the native
  dependency versions themselves (RocksDB, DuckDB, protobuf or the agave crates), because those
  C++ and Rust graphs genuinely must recompile; that recompilation is still a required check,
  and main pays the same cost on merge.
- Cache writes happen only on main and manual dispatches, so PR runs no longer churn the shared
  10 GB cache with near-duplicate entries. Each job's `shared-key` is the stable job name
  (`clippy`, `test`, `archive-check`); bump the suffix to start the cache fresh if the scheme
  ever changes.
