#!/bin/sh
# Nightly off-box backup of the Phoenix dataset to Cloudflare R2 (ADR-0010).
# `rclone copy` only, never `sync`: files the box retires stay in the bucket.
# Order: raw table files listed by a catalog snapshot, the augment tables, config and units,
# then the catalogs, so the bucket never holds a catalog newer than the files it lists.
set -eu

data=${SOLOS_DATA_HOME:-$HOME/.local/share/solos-data}
raw=$data/phoenix_raw
augment=$data/augment
checkout=${SOLOS_DATA_CHECKOUT:-$HOME/solos-data}
units=$HOME/.config/systemd/user
status_file=${SOLOS_BACKUP_STATUS:-$data/backup-status.json}
log_dir=${SOLOS_BACKUP_LOG_DIR:-$HOME/.local/state/solos-data/backup}
bwlimit=${SOLOS_BACKUP_BWLIMIT:-06:00,40M 22:00,off}
dry=${SOLOS_BACKUP_DRY_RUN:-0}

mkdir -p "$log_dir"
exec 9>"$log_dir/lock"
if ! flock -n 9; then
  echo '{"event":"backup","state":"skipped","reason":"another run holds the lock"}'
  exit 0
fi

. "$(dirname "$0")/r2-env.sh"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
day=$(date -u +%Y-%m-%d)
t0=$(date +%s)
previous_success=$(jq -r '.lastSuccessAt // empty' "$status_file" 2>/dev/null || true)

write_status() {
  tmp="$status_file.tmp.$$"
  printf '%s\n' "$1" > "$tmp"
  mv "$tmp" "$status_file"
}
write_status "$(jq -cn --arg s "$started" --arg p "$previous_success" \
  '{event:"backup",state:"running",startedAt:$s,lastSuccessAt:(if $p=="" then null else $p end)}')"

dry_flag=
if [ "$dry" = 1 ]; then dry_flag=--dry-run; fi

# run_step NAME PASS rclone-args... : one rclone call, its final stats appended to steps.jsonl.
run_step() {
  step_name=$1 step_pass=$2
  shift 2
  log="$log_dir/$step_name.log"
  : > "$log"
  step_rc=0
  ionice -c3 nice -n 10 "$rclone" "$@" $dry_flag \
    --s3-no-check-bucket --transfers 8 --checkers 16 \
    --s3-upload-concurrency 4 --s3-chunk-size 64M --bwlimit "$bwlimit" \
    --retries 3 --low-level-retries 10 \
    --use-json-log --log-level NOTICE --stats 10m --stats-log-level NOTICE \
    --log-file "$log" || step_rc=$?
  stats=$(grep '"stats"' "$log" | tail -1 | jq -c '.stats' 2>/dev/null || true)
  [ -n "$stats" ] || stats='{}'
  jq -cn --arg n "$step_name" --argjson p "$step_pass" --argjson rc "$step_rc" --argjson s "$stats" \
    '{step:$n,pass:$p,rc:$rc,files:($s.transfers//0),bytes:($s.bytes//0),errors:($s.errors//0),seconds:($s.elapsedTime//0)}' \
    >> "$work/steps.jsonl"
  return "$step_rc"
}

# Raw: the catalog defines the published files. A file retired mid-run (compaction) is absent
# from the next catalog, so take a new snapshot and copy again, at most three times.
mkdir -p "$work/raw-meta"
raw_ok=0
pass=0
while [ "$pass" -lt 3 ] && [ "$raw_ok" = 0 ]; do
  pass=$((pass + 1))
  cp "$raw/catalog.json" "$work/raw-meta/catalog.json"
  jq -r --arg root "$raw/" '.files[].path
    | if startswith($root) then ltrimstr($root) else error("outside the raw root") end' \
    "$work/raw-meta/catalog.json" > "$work/raw-files.txt"
  rc=0
  run_step raw-files "$pass" copy "$raw" "$dest/phoenix_raw" \
    --files-from-raw "$work/raw-files.txt" --size-only --immutable --fast-list || rc=$?
  missing=0
  while IFS= read -r rel; do
    [ -e "$raw/$rel" ] || missing=$((missing + 1))
  done < "$work/raw-files.txt"
  if [ "$rc" = 0 ] && [ "$missing" = 0 ]; then raw_ok=1; fi
done
for f in status.json capabilities.json; do
  if [ -f "$raw/$f" ]; then cp "$raw/$f" "$work/raw-meta/$f"; fi
done

# Augment: snapshot its catalogs first, then the whole tree (files that grow are rewritten in
# place on the box, so compare checksums; unchanged files are never re-uploaded).
mkdir -p "$work/augment-meta"
for f in catalog.json catalog-capture.json status.json status-capture.json; do
  if [ -f "$augment/$f" ]; then cp "$augment/$f" "$work/augment-meta/$f"; fi
done
run_step augment-files 1 copy "$augment" "$dest/augment" --checksum --fast-list \
  --filter '- checkpoint.duckdb*' --filter '- /staging*/**' --filter '- *.tmp' \
  --filter '- /catalog*.json' --filter '- /status*.json' || true

run_step config 1 copy "$checkout/config" "$dest/config" --checksum --filter '- *.env' || true
run_step units 1 copy "$units" "$dest/systemd-user" --checksum \
  --filter '- *.env' --filter '+ /solos-*' --filter '+ /solos-*/**' --filter '- *' || true

# Catalogs last, plus a dated copy of each for point-in-time restores.
if [ "$raw_ok" = 1 ]; then
  run_step raw-catalog 1 copy "$work/raw-meta" "$dest/phoenix_raw" --checksum || true
  run_step raw-catalog-dated 1 copy "$work/raw-meta" "$dest/catalogs/$day/phoenix_raw" --checksum || true
fi
run_step augment-catalog 1 copy "$work/augment-meta" "$dest/augment" --checksum || true
run_step augment-catalog-dated 1 copy "$work/augment-meta" "$dest/catalogs/$day/augment" --checksum || true

finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
seconds=$(($(date +%s) - t0))
summary=$(jq -sc --arg s "$started" --arg f "$finished" --argjson d "$seconds" \
  --argjson raw "$raw_ok" --arg p "$previous_success" --argjson dry "$([ "$dry" = 1 ] && echo true || echo false)" '
  (map(.errors) | add // 0) as $errors
  | (map(select(.rc != 0)) | length) as $failed
  | ($raw == 1 and $errors == 0 and $failed == 0) as $ok
  | {event:"backup", state:(if $ok then "ok" else "failed" end), dryRun:$dry,
     startedAt:$s, finishedAt:$f, durationSeconds:$d,
     files:(map(.files) | add // 0), bytes:(map(.bytes) | add // 0),
     errors:$errors, failedSteps:$failed, rawCatalogUploaded:($raw == 1),
     mbPerSecond:(if $d > 0 and ($dry | not) then ((map(.bytes) | add // 0) / $d / 1e6 * 10 | floor / 10) else 0 end),
     lastSuccessAt:(if $ok and ($dry | not) then $f elif $p == "" then null else $p end),
     steps:.}' "$work/steps.jsonl")
echo "$summary" | jq -c 'del(.steps)'
write_status "$summary"
[ "$(echo "$summary" | jq -r .state)" = ok ]
