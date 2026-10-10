#!/bin/sh
# One JSON line with the state of each solos-data unit, for the health timer's journal.
line='{"event":"health_services"'
for unit in solos-data-phoenix.service solos-data-phoenix-decoder.service \
  solos-data-augment-capture.service solos-data-augment.timer solos-data-health.timer \
  solos-data-backup.timer; do
  state=$(systemctl --user is-active "$unit" 2>/dev/null)
  line="$line,\"${unit}\":\"${state:-unknown}\""
done
# Off-box backup age (ADR-0010): the last successful run, null before the first one.
backup=$(jq -c '.lastSuccessAt' "$HOME/.local/share/solos-data/backup-status.json" 2>/dev/null)
line="$line,\"backupLastSuccessAt\":${backup:-null}"
printf '%s}\n' "$line"
