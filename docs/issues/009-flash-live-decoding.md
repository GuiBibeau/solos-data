# Flash live lane is undecoded

The Flash live capture (`augment/tables/flash/events/ALL`, 2026-10-19
onward) holds 151 events since 2026-10-19 and every one of them has a NULL
`name`: the live capture matches none of the Flash IDLs, while the
venues-history dump decodes ~15k events per day for the same program. The
U2c units therefore see no Flash position events at all after the history
lane ends.

## Consequence for the hourly units

Since the undecoded-events rule (ADR-0023) every hour with a NULL-name event
is a gap hour and is never sealed:

- 2026-10-17T23:00Z through 2026-10-19T00:00Z are gap hours with the
  history-end reason (the finished history lane ends 2026-10-17T23:50:48Z,
  the live lane starts 2026-10-19T00:36:29Z).
- Every later hour that contains one of the 151 undecoded live events is a
  gap hour with reason `undecoded_events` (the event count is in the
  receipt's `gapHours` entry).

So `solana_perp_coin_hour` is effectively Flash-history-only through
2026-10-17T22Z until this is fixed; Jupiter and Pyth are unaffected (their
live captures decode).

## Ask

Fix the live decoding so the capture matches the Flash IDLs the way the
history dump does (same event names, same `fields`), then re-run the perp
hours over the affected range. Do NOT touch the live decoder as part of the
U2c branch; this issue is the hand-off.
