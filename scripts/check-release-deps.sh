#!/usr/bin/env bash
# Release graph check: test-only crates must not reach a release build, and `fail` must be
# reachable only through the `failpoints` feature. Read-only; cargo tree resolves the graph
# without compiling, so this never builds DuckDB.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Test-only crates that must never appear in a release build graph.
DENY_LIST=(fail proptest quickcheck tempfile assert_cmd insta)

# The release graph that the deny list inspects.
DENY_PACKAGES=(solos-data)

# Packages where `fail` may enter the graph only through the `failpoints` feature.
FAIL_REACH_PACKAGES=(solos-data phoenix-codec)

fail() {
  printf 'check-release-deps: %s\n' "$1" >&2
  exit 1
}

# The Cargo.toml that declares a dependency, named in the error so the fix is obvious.
declare_cargo_toml() {
  local crate=$1
  grep -rlE --include=Cargo.toml "^[[:space:]]*${crate}[[:space:]]*=" Cargo.toml crates vendor 2>/dev/null | head -n1
}

for pkg in "${DENY_PACKAGES[@]}"; do
  tree="$(cargo tree --locked -p "$pkg" -e normal,build --prefix none 2>&1)" || {
    printf '%s\n' "$tree" >&2
    fail "cargo tree for $pkg failed"
  }
  for crate in "${DENY_LIST[@]}"; do
    if printf '%s\n' "$tree" | grep -qE "^${crate} "; then
      declaring="$(declare_cargo_toml "$crate")"
      fail "release build of $pkg pulls test-only crate '$crate' (declared in ${declaring:-<unknown Cargo.toml>})"
    fi
  done
done

# `-e normal,build,features` excludes dev-dependencies, so the test-only self dev-dependency
# (solos-data -> solos-data with feature failpoints) does not show up. Any `fail` line here means
# a release path reaches it without the failpoints feature.
for pkg in "${FAIL_REACH_PACKAGES[@]}"; do
  out="$(cargo tree --locked -e normal,build,features -i fail -p "$pkg" 2>&1)" || true
  if printf '%s\n' "$out" | grep -q '^fail '; then
    printf '%s\n' "$out" >&2
    fail "'fail' is reachable without the failpoints feature in package $pkg"
  fi
done

printf 'check-release-deps: release graphs are clean\n'
