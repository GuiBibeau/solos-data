#!/bin/sh
# Installs a pinned rclone into ~/.local/bin without sudo, verified against the release's SHA256SUMS.
set -eu
version=v1.75.1
dest="$HOME/.local/bin/rclone"
if [ -x "$dest" ] && "$dest" version 2>/dev/null | head -1 | grep -q "rclone $version\$"; then
  "$dest" version | head -1
  exit 0
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
archive="rclone-$version-linux-amd64.zip"
base="https://github.com/rclone/rclone/releases/download/$version"
curl -fsSLO "$base/$archive"
curl -fsSLO "$base/SHA256SUMS"
awk -v name="$archive" '$2==name { print }' SHA256SUMS > selected.sha256
test -s selected.sha256
sha256sum -c selected.sha256
unzip -q "$archive"
mkdir -p "$(dirname "$dest")"
install -m 0755 "rclone-$version-linux-amd64/rclone" "$dest.new"
mv "$dest.new" "$dest"
"$dest" version | head -1
