#!/bin/sh
# Restore rehearsal for the R2 backup (ADR-0010): downloads a small sample into a temp dir and
# checks every file's SHA-256 against the catalog in the bucket. Infrequent Access bills
# retrieval per GB, so the sample is bounded: one raw table-epoch directory of at most
# SOLOS_RESTORE_MAX_MB (default 1024) plus SOLOS_RESTORE_AUGMENT_FILES (default 20) augment files.
set -eu
. "$(dirname "$0")/r2-env.sh"
max_bytes=$(( ${SOLOS_RESTORE_MAX_MB:-1024} * 1000000 ))
augment_files=${SOLOS_RESTORE_AUGMENT_FILES:-20}
raw_root=${SOLOS_RESTORE_RAW_ROOT:-$HOME/.local/share/solos-data/phoenix_raw}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
rc() { "$rclone" "$@" --s3-no-check-bucket --log-level ERROR; }

# Raw: the bucket's catalog plus one listing (metadata only, nothing retrieved) gives sizes; pick
# the published (non-staging) table-epoch directory with the most files under the size cap.
rc copyto "$dest/phoenix_raw/catalog.json" "$work/raw-catalog.json"
rc lsjson -R --files-only --fast-list "$dest/phoenix_raw" > "$work/raw-objects.json"
jq -r --arg root "$raw_root/" --argjson max "$max_bytes" --slurpfile objects "$work/raw-objects.json" '
  ($objects[0] | map({key: .Path, value: .Size}) | from_entries) as $size
  | [.files[] | (.path | ltrimstr($root)) as $rel
     | select(($rel | startswith("staging/")) | not)
     | {rel: $rel, dir: ($rel | sub("/[^/]+$"; "")), bytes: ($size[$rel] // null)}]
  | map(select(.bytes != null)) | group_by(.dir)
  | map({dir: .[0].dir, files: length, bytes: (map(.bytes) | add)})
  | map(select(.bytes <= $max)) | sort_by(-.files, .bytes) | .[0].dir // empty' \
  "$work/raw-catalog.json" > "$work/raw-dir.txt"
raw_dir=$(cat "$work/raw-dir.txt")
[ -n "$raw_dir" ] || { echo '{"event":"restore_check","ok":false,"reason":"no raw directory under the cap"}'; exit 1; }
jq -r --arg root "$raw_root/" --arg dir "$raw_dir/" '.files[] | (.path | ltrimstr($root)) as $rel
  | select($rel | startswith($dir)) | "\(.sha256)  raw/\($rel)"' "$work/raw-catalog.json" > "$work/sums.txt"
sed 's|^[0-9a-f]*  raw/||' "$work/sums.txt" > "$work/raw-files.txt"
rc copy "$dest/phoenix_raw" "$work/raw" --files-from-raw "$work/raw-files.txt"

# Augment: a random sample of complete files from both lanes' catalogs.
for c in catalog.json catalog-capture.json; do
  rc copyto "$dest/augment/$c" "$work/augment-$c"
done
jq -s -r '.[].files[] | select(.complete) | "\(.sha256)  augment/\(.path)"' \
  "$work/augment-catalog.json" "$work/augment-catalog-capture.json" | shuf -n "$augment_files" > "$work/augment-sums.txt"
sed 's|^[0-9a-f]*  augment/||' "$work/augment-sums.txt" > "$work/augment-files.txt"
rc copy "$dest/augment" "$work/augment" --files-from-raw "$work/augment-files.txt"

cat "$work/augment-sums.txt" >> "$work/sums.txt"
checked=$(wc -l < "$work/sums.txt")
failed=$(cd "$work" && sha256sum -c --quiet "$work/sums.txt" 2>/dev/null | grep -c . || true)
bytes=$(du -sb "$work/raw" "$work/augment" | awk '{ s += $1 } END { print s }')
jq -cn --arg dir "$raw_dir" --argjson checked "$checked" --argjson failed "$failed" \
  --argjson bytes "$bytes" --argjson raw "$(wc -l < "$work/raw-files.txt")" \
  --argjson aug "$(wc -l < "$work/augment-files.txt")" \
  '{event:"restore_check", ok:($failed == 0 and $checked > 0), rawDir:$dir, rawFiles:$raw,
    augmentFiles:$aug, filesChecked:$checked, mismatched:$failed, bytesDownloaded:$bytes}'
[ "$failed" = 0 ]
