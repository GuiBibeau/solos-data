#!/usr/bin/env bash
# Release graph check: test-only crates must not reach a release build, and `fail` must be
# reachable only through the `failpoints` feature. Read-only; cargo tree resolves the graph
# without compiling, so this never builds DuckDB.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The release graph the checks inspect: the exact feature set Dockerfile:16 builds
# (`cargo build --release -p solos-data --features archive`). The archive graph is a superset of
# the default graph, so resolving archive covers both.
RELEASE_PACKAGE=solos-data
RELEASE_FEATURES=(archive)

# Test-only crates that must never appear in the release graph, each with the manifest that
# declares it so the error names the exact file to fix. Scoped to what the workspace itself
# declares, not a flat list of common test crates: the archive graph legitimately pulls
# `tempfile` (via agave-fs) and `toml` 0.5 (via proc-macro-crate -> jsonrpc-derive), and `borsh`
# is a normal dependency of phoenix-rise-events, so denying any of those would false-positive.
DENY_LIST=(
  "fail:crates/solos-data/Cargo.toml"
  "proptest:crates/solos-data/Cargo.toml"
)

fail() {
  printf 'check-release-deps: %s\n' "$1" >&2
  exit 1
}

for features in "${RELEASE_FEATURES[@]}"; do
  tree="$(cargo tree --locked -p "$RELEASE_PACKAGE" --features "$features" -e normal,build --prefix none 2>&1)" || {
    printf '%s\n' "$tree" >&2
    fail "cargo tree for $RELEASE_PACKAGE (--features $features) failed"
  }
  for entry in "${DENY_LIST[@]}"; do
    crate="${entry%%:*}"
    manifest="${entry#*:}"
    if grep -qE "^${crate} " <<< "$tree"; then
      fail "release build of $RELEASE_PACKAGE (--features $features) pulls test-only crate '$crate' (declared in $manifest)"
    fi
  done
done

# `-e normal,build,features` excludes dev-dependencies, so the test-only self dev-dependency
# (solos-data -> solos-data with feature failpoints) does not show up. The archive feature is
# resolved because that is the release feature set. Any `fail` line here means a release path
# reaches it without the failpoints feature.
for features in "${RELEASE_FEATURES[@]}"; do
  out="$(cargo tree --locked -p "$RELEASE_PACKAGE" --features "$features" -e normal,build,features -i fail --prefix none 2>&1)" || {
    printf '%s\n' "$out" >&2
    fail "cargo tree -i fail for $RELEASE_PACKAGE (--features $features) failed"
  }
  if grep -q '^fail ' <<< "$out"; then
    printf '%s\n' "$out" >&2
    fail "'fail' is reachable without the failpoints feature in package $RELEASE_PACKAGE (--features $features)"
  fi
done

printf 'check-release-deps: release graphs are clean\n'
