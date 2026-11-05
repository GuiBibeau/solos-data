# ADR-0024: Absent dump days are logged, counted and asked for again

Status: accepted, November 5, 2026 (owner approved). Amends the walk and the Binance dump
sections of [ADR-0009](0009-augmentation-lane.md).

## Problem

ADR-0009 remembers a dump day that still answers 404 more than three days after it ended as
`absent` in its series' progress record, and never asks for it again. That rule fits a market
listed later. It also swallowed a hole in a continuous series without a trace.

Binance Vision never published 2026-10-28 `aggTrades` for any of the 46 symbols, and never
published 2026-10-28 and 2026-10-29 `klines`, `indexPriceKlines`, `markPriceKlines` and
`premiumIndexKlines`. The lane went on to the days after them and registered those. Nothing
said so:

1. Marking a day `absent` wrote no log line and no counter, and the run reported `errors: 0`.
   An interior hole (a missing day between two present days) looked exactly like a market that
   listed later. It went unnoticed for four days.
2. An `absent` day was never asked for again, so a venue that publishes late would never be
   picked up. The listed datasets were worse: `sync_pair` lists only after the newest registered
   file, so it never revisited any older missing day, and kept no record of one at all.

## Decision

**Where each position is told apart.** An absent day is `leading` (no registered day before it:
the market was listed later), `interior` (registered days on both sides: a hole) or `trailing`
(none after it). The position is judged from the days the series has registered, so it is
recomputed every run and a day moves from `trailing` to `interior` the moment a newer day lands.
Only `interior` days are asked for again and counted as holes. A leading gap is a listing date,
not a defect.

**Logged.** Each mark writes one `augment_absent_marked` line with `source`, `dataset`,
`symbol`, `period`, `reason` (`not_found`) and `position`. A re-probe that finds the file writes
`augment_absent_filled`; the last miss of the schedule writes `augment_absent_given_up`. None
carries a URL, a header or provider text. The position in the marked line is judged when the
mark is written: during a newest-first backfill the older days are not fetched yet, so a hole
reads `leading` there and shows as `absentInterior` in the status once they are.

**Counted.** Each source that tracks absence (Binance, OKX, Bybit tick walks and the Binance
listed datasets) adds four fields to its entry in `status.json` and to the `augment_sync_done`
line: `absentMarked` (marks written in this run), `absentInterior` (interior holes still on the
schedule when the run ends), `absentGivenUp` (interior holes whose schedule is spent) and
`absentFilled` (holes a re-probe stored in this run). `errors` is unchanged: a venue that does
not publish is not a failure of the collector, and counting it there would turn `augment sync`
red for as long as the gap lasts.

**Surfaced.** `solos-data health` sums `absentInterior` over the sources and adds one warning,
"augment sync: N interior absent dump days still being re-probed", and `augment.sync` in
`health.json` carries `absentInterior` and `absentGivenUp`. This is the same `warn` list and
section the sync status already feeds; no new channel. A hole whose schedule is spent no
longer warns: Binance genuinely never published 2026-10-28, a warning that can never clear
teaches the operator to ignore the level, and the gap stays in `absentGivenUp`, in the progress
record and in the `augment_absent_given_up` line.

**Asked for again.** An interior absent day is asked for on a schedule from the `absentReprobe`
block of `config/augment.json`: at most once every 24 hours for `dailyProbes` (14) misses, then
at most once every 7 days for `weeklyProbes` (12) more, then never, about 98 days from the
mark. A run asks for at most `maxPerRun` (500) holes per source, newest first, so a first run
that finds many cannot starve the walk; the rest wait for the next run. Both probe counts 0
turn the re-probe off and leave the logs and counters. The numbers are judgements, not
measurements: a venue repair is a manual act, so two weeks of daily asks catch a prompt one,
twelve weekly asks catch a slow one, and after a quarter the gap is the venue's. The cost is
bounded: a hole costs at most 26 asks, two requests each (the file and its `.CHECKSUM`, both
404). If all 46 symbols miss all five datasets on those days, the 2026-10-28 and 29 case is 414
holes: about 830 requests a day for two weeks, a few minutes a day at two requests a second,
and about 830 a week after that. Only a 404 spends one
of the probes: a transfer failure or a bad checksum is an `augment_series_error` and waits for
its interval again without being counted, so a venue outage cannot use the schedule up.
Progress records written before this ADR carry the labels only; their interior days have never
been asked for again and are asked at the first run, which is what the 2026-10-28 holes need.

**Reached below the newest file.** The newest-first walk asks for each series directly, so it
reaches any day. For the listed datasets, each run now also asks directly for the days missing
between the oldest registered day (at or after `startDate`) and the newest, which a listing that
starts after the newest file cannot reach. A hole younger than three days is asked for every run
and not marked (it may not be published yet); an older one that answers 404 is marked, logged
and counted, and then follows the schedule. The same `absent` record, in the same progress key
shape (`binance/<dataset>/<symbol>`), serves both paths.

**Durable before it clears.** A re-probe that finds the file downloads it, checks the
`.CHECKSUM`, types and writes the Parquet durably (temporary file, fsync, rename, directory
fsync), and then registers the file and rewrites the progress record without the mark in one
ledger transaction. A crash before that transaction leaves the mark and no registered file;
the unregistered file and the staging copy are removed at the next start (the ledger's
recovery pass, ADR-0009) and the fill runs again. The watermark rule is unchanged: nothing advances past a
file before it is durable.

## Alternatives considered

- Count holes in `errors`. Rejected: it fails `augment sync` and the `augment sync: N errors`
  warning for as long as the venue leaves the gap, and mixes a venue fact with a lane fault.
- Re-list the whole prefix to find a late file. Rejected: it moves a page of keys per series
  per run to learn what a direct ask answers with one 404.
- Warn on every interior hole for ever. Rejected, as above.
- Ask again for leading and trailing days too. Rejected: a leading gap is a listing date and
  would cost asks for every symbol, for ever; a trailing day becomes interior as soon as a
  newer day lands, and is asked for then.

## Consequences

- A late file fills the hole: the Parquet file registers with the venue's period label, exactly
  as a first-time fetch would have.
- The `absent` progress record gains a `reprobe` object (`n` misses and `lastMs` per day). The
  `absent` array keeps its ADR-0009 shape, so an older binary still reads the record.
- Unfinished acceptance criteria are in
  [issue 009](../issues/009-absent-dump-days-followups.md).
