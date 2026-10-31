#!/usr/bin/env bash
# The `ci-ok` aggregator: fail when any required job did not succeed, except a `skipped` job
# named in ALLOWED_SKIPS whose entry names the legitimizing condition. Reads the GitHub Actions
# contexts from the environment (NEEDS, ALLOWED_SKIPS, IS_PR, ARCHIVE_CHANGED), so it is
# testable offline against a fake NEEDS JSON.
set -euo pipefail

echo "$NEEDS" | jq .
allowed_skips="$(printf '%s\n' "$ALLOWED_SKIPS" | sed -n 's/^\([^:]*\):.*/\1/p')"
for job in $(echo "$NEEDS" | jq -r 'keys[]'); do
  result="$(echo "$NEEDS" | jq -r --arg j "$job" '.[$j].result')"
  if [ "$result" = "success" ]; then
    continue
  fi
  if [ "$result" = "skipped" ] && printf '%s\n' "$allowed_skips" | grep -qxF "$job"; then
    case "$job" in
      archive-check)
        if [ "$IS_PR" = "true" ] && [ "$ARCHIVE_CHANGED" != "true" ]; then
          echo "archive-check intentionally skipped: no archive inputs changed on this pull request"
          continue
        fi
        ;;
    esac
  fi
  echo "required job '$job' did not succeed: $result" >&2
  exit 1
done
echo "ci-ok: all required jobs passed"
