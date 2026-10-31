#!/usr/bin/env bash
# Offline verification, same checks as CI, with cargo tests run once:
#   the text lint, ci-ok coverage, release dependency graph, fmt, clippy -D warnings,
#   cargo test --workspace --locked, tsc, the release codec binary the Node tests spawn,
#   then the Node tests.
# Prints VERIFY PASS, or VERIFY FAIL <step> and exits non-zero.

set -u

cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() {
  printf 'VERIFY FAIL %s\n' "$1"
  exit 1
}

run() {
  local step=$1
  shift
  printf 'verify: %s\n' "${step}" >&2
  if ! "$@"; then
    fail "${step}"
  fi
}

if ! command -v gcc >/dev/null 2>&1 || ! command -v g++ >/dev/null 2>&1; then
  fail setup
fi
if ! command -v cargo >/dev/null 2>&1 || ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  fail setup
fi
if ! node -e 'const [M,m]=process.versions.node.split(".").map(Number); process.exit((M>22||(M===22&&m>=18))?0:1)'; then
  fail setup
fi

# DuckDB's bundled C++ does not build with clang (thrift / libc++). Force g++
# even when /usr/bin/c++ still points at clang++.
export CC=gcc
export CXX=g++

# Match CI: no incremental cache, and no debuginfo, so DuckDB's C++ is built
# without -g. That is smaller and uses less RAM than a default dev build.
export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0

cpus=$(nproc)
jobs=${cpus}
mem_kb=$(awk '/MemAvailable:/ { print $2; exit }' /proc/meminfo || true)
# One heavy C++ compile wants about 2 GiB. When available RAM is below that
# budget, cap cargo's jobs: a burst of parallel g++ processes can OOM before a
# hypervisor balloon deflates.
if [[ -n "${mem_kb}" ]]; then
  by_mem=$((mem_kb / 2000000))
  if ((by_mem < 1)); then
    by_mem=1
  fi
  if ((by_mem < jobs)); then
    jobs=${by_mem}
  fi
fi
export CARGO_BUILD_JOBS=${jobs}
printf 'verify: CARGO_BUILD_JOBS=%s\n' "${jobs}" >&2

run lint npm run lint:text
run ci-check node scripts/check-ci.mjs
run release-deps scripts/check-release-deps.sh
run fmt cargo fmt --all --check
run clippy cargo clippy --workspace --all-targets --locked -- -D warnings
run test cargo test --workspace --locked
run tsc npm run check
run codec cargo build --release --locked -p phoenix-codec
run node npm test

printf 'VERIFY PASS\n'
