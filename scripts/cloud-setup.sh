#!/usr/bin/env bash
# Install what a cloud VM needs to build and verify this repo. Safe to re-run.
# Compiler note: DuckDB's bundled C++ (thrift) does not build with clang's
# libc++. build-essential's g++ is the compiler; clang stays for the archive
# feature's libclang/bindgen.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=()
else
  SUDO=(sudo)
fi

export DEBIAN_FRONTEND=noninteractive
export CC=gcc
export CXX=g++

"${SUDO[@]}" apt-get update
"${SUDO[@]}" apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  clang \
  cmake \
  curl \
  libclang-dev \
  pkg-config \
  xz-utils

# The base image pins cc/c++ to clang (manual alternatives) even though gcc is
# installed at a higher priority. Point them back at g++ so a plain cargo
# invocation compiles DuckDB the same way verify.sh does.
if [[ -x /usr/bin/gcc ]] && update-alternatives --query cc >/dev/null 2>&1; then
  "${SUDO[@]}" update-alternatives --set cc /usr/bin/gcc
fi
if [[ -x /usr/bin/g++ ]] && update-alternatives --query c++ >/dev/null 2>&1; then
  "${SUDO[@]}" update-alternatives --set c++ /usr/bin/g++
fi

if ! command -v gcc >/dev/null 2>&1 || ! command -v g++ >/dev/null 2>&1; then
  echo "cloud-setup: g++ is required to compile DuckDB" >&2
  exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
fi

if [[ -d "${HOME}/.cargo/bin" ]]; then
  export PATH="${HOME}/.cargo/bin:${PATH}"
  # Non-interactive login shells do not source the rustup PATH line in bashrc.
  for cmd in rustup cargo rustc rustfmt cargo-fmt cargo-clippy; do
    if [[ -x "${HOME}/.cargo/bin/${cmd}" && ! -e "/usr/local/bin/${cmd}" ]]; then
      "${SUDO[@]}" ln -sfn "${HOME}/.cargo/bin/${cmd}" "/usr/local/bin/${cmd}"
    fi
  done
fi

if [[ ! -f rust-toolchain.toml ]]; then
  echo "cloud-setup: rust-toolchain.toml is missing" >&2
  exit 1
fi

channel=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)
profile=$(sed -n 's/^profile = "\(.*\)"/\1/p' rust-toolchain.toml)
components=$(sed -n 's/^components = \[\(.*\)\]/\1/p' rust-toolchain.toml | tr -d '"[:space:]')
if [[ -z "${channel}" ]]; then
  echo "cloud-setup: rust-toolchain.toml has no channel" >&2
  exit 1
fi
profile=${profile:-minimal}

install_args=(toolchain install "${channel}" --profile "${profile}")
if [[ -n "${components}" ]]; then
  install_args+=(--component "${components}")
fi
rustup "${install_args[@]}"
rustup default "${channel}"

# Proxies for clippy and rustfmt appear only after the components exist.
if [[ -d "${HOME}/.cargo/bin" ]]; then
  for cmd in rustfmt cargo-fmt cargo-clippy; do
    if [[ -x "${HOME}/.cargo/bin/${cmd}" && ! -e "/usr/local/bin/${cmd}" ]]; then
      "${SUDO[@]}" ln -sfn "${HOME}/.cargo/bin/${cmd}" "/usr/local/bin/${cmd}"
    fi
  done
fi

rustc_version=$(rustc --version)
if [[ "${rustc_version}" != *"1.96.0"* ]]; then
  echo "cloud-setup: expected rustc 1.96.0, found: ${rustc_version}" >&2
  exit 1
fi
command -v cargo-clippy >/dev/null
command -v rustfmt >/dev/null

# package.json engines: >=22.18. CI uses Node 24. Install that line only when
# the image has nothing new enough, into /usr/local so login shells see it.
node_ok=0
if command -v node >/dev/null 2>&1; then
  if node -e 'const [M,m]=process.versions.node.split(".").map(Number); process.exit((M>22||(M===22&&m>=18))?0:1)'; then
    node_ok=1
  fi
fi
if [[ "${node_ok}" -ne 1 ]]; then
  node_version=24.22.2
  tarball="node-v${node_version}-linux-x64.tar.xz"
  tmp=$(mktemp)
  curl -fsSL "https://nodejs.org/dist/v${node_version}/${tarball}" -o "${tmp}"
  "${SUDO[@]}" tar -xJf "${tmp}" -C /usr/local --strip-components=1 --no-same-owner
  rm -f "${tmp}"
  hash -r
fi

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "cloud-setup: node and npm are required" >&2
  exit 1
fi
if ! node -e 'const [M,m]=process.versions.node.split(".").map(Number); process.exit((M>22||(M===22&&m>=18))?0:1)'; then
  echo "cloud-setup: node $(node --version) is older than 22.18" >&2
  exit 1
fi

npm ci
cargo fetch --locked

# The archive feature is not part of verify. libclang-dev's unversioned
# libclang.so lives under /usr/lib/llvm-*/lib, which clang-sys 1.9 searches
# without llvm-config on PATH. Fail if that library is absent.
libclang=$(find /usr/lib /usr/lib/llvm-* -name 'libclang.so' -print -quit 2>/dev/null || true)
if [[ -z "${libclang}" ]]; then
  echo "cloud-setup: libclang.so is missing after libclang-dev" >&2
  exit 1
fi
command -v clang >/dev/null
command -v cmake >/dev/null
command -v pkg-config >/dev/null
