#!/bin/sh
set -eu
version=v24.21.0
root="$HOME/.local/share/solos-data"
mkdir -p "$root/runtime"
if [ -x "$root/runtime/bin/node" ]; then "$root/runtime/bin/node" --version; exit 0; fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
archive="node-$version-linux-x64.tar.xz"
curl -fsSLO "https://nodejs.org/dist/$version/$archive"
curl -fsSLO "https://nodejs.org/dist/$version/SHASUMS256.txt"
awk -v name="$archive" '$2==name { print }' SHASUMS256.txt > selected.sha256
test -s selected.sha256
sha256sum -c selected.sha256
tar -xJf "$archive" --strip-components=1 -C "$root/runtime"
"$root/runtime/bin/node" --version
