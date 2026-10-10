#!/bin/sh
# One JSON line with the state of each solos-data unit, for the health timer's journal.
line='{"event":"health_services"'
for unit in solos-data-phoenix.service solos-data-phoenix-decoder.service \
  solos-data-augment-capture.service solos-data-augment.timer solos-data-health.timer; do
  state=$(systemctl --user is-active "$unit" 2>/dev/null)
  line="$line,\"${unit}\":\"${state:-unknown}\""
done
printf '%s}\n' "$line"
