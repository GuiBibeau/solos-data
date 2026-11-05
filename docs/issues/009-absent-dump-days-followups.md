# Absent dump days: open acceptance criteria (ADR-0024)

Code and fixtures are done; these need the box or a decision.

- [ ] After the first sync that runs the new binary, confirm on the box that
  `status.json` shows `absentInterior` for `binance` and that `health.json` carries the
  "interior absent dump days" warning for the 2026-10-28 and 2026-10-29 holes (all `aggTrades`
  symbols for 10-28; `klines`, `indexPriceKlines`, `markPriceKlines`, `premiumIndexKlines` for
  10-28 and 10-29). Those days have no probe history, so the first run asks for each of them
  once; either a file lands (`augment_absent_filled`) or the mark stays and the schedule starts.
  Not testable offline: no collector was run against production data for this change.
- [ ] Tardis (`tardis.rs`) and the Hyperliquid S3 archive (`hls3.rs`) keep their own `absent`
  sets: a missing day is still marked without a log line or a counter and never asked for
  again. Move them onto `augment::absent` (Tardis has the same three-day rule; Hyperliquid
  waits 45 days) or record why they stay.
- [ ] A permanent gap the operator has accepted stays in `absentGivenUp` and the progress
  record but has no acknowledgement list, so a consumer that needs continuity reads the
  record. Decide whether a config list of accepted holes is worth having.
- [ ] The crash case is covered by the state a kill leaves (an unregistered Parquet file, its
  temporary sibling and a staged CSV on disk, nothing in the ledger) and by a failed
  conversion, not by killing a running process: `augment sync` has no failpoint between the
  Parquet write and the registration. Add one to the random-kill loop (ADR-0019) if the fill
  path should be part of it.
- [ ] During a newest-first backfill a hole is logged as `leading` when it is marked, because
  the older days are not fetched yet (it counts as `absentInterior` in the status from the run
  after they are). If that proves confusing, log the final position at the end of the run.
