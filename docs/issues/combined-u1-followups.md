# Combined U1 follow-ups (from review round 1, PR #99)

Non-blocking items the reviewer asked to record rather than fix now.

- grid.rs `date_of_hour`/`hour_key` `expect` panics on data-derived hours; return
  a `StoreError` instead so a corrupt input cannot panic the process.
- The watermark hour key goes into a path with format checks but no canonicalization;
  validate the full shape (digits, hour range 0..=23) in one place.
- A null `coin_class` on an HL row is treated as a perp/HIP-3 coin by the fallthrough
  arm; make the class match exhaustive and refuse unknown classes.
- Duplicate Pyth feeds or duplicate Jupiter custody labels/addresses in
  `config/venues.json` are unchecked; refuse or dedupe with a count.
- A Pyth feed with no history rows is still `mapped=true` (config-only); decide
  whether it needs a `no_history` marker or stays config-truth.
- HL coins with no Phoenix candidate carry an empty `reason`; give them a
  `no_candidate` reason and a counter in the report.
- The ADR should spell out that the grid works in unix hour indices while the CSV
  timestamps are unix seconds (done in ADR-0023 round 1, keep in sync).
- Test the Pyth base-symbol fallback (27 of 39 feeds have no market name) end to
  end; today only the unit-level fallback is covered by the config types.

## Review round 2 (PR #99)

Non-blocking items the reviewer asked to record rather than fix now.

- Pyth `lockbox_hours_dropped` is the max across the `--pyth-prices`
  directories, not their union, so it undercounts when the directories cover
  different lockbox hours (the misleading comment was fixed in round 2; the
  behaviour is unchanged).
- `watermark_unix_hour` silently skips malformed watermark keys (returns
  `None`) and does not bound the hour of day, so a corrupt key drops an hour
  from the HL drop count without an error.
- `receipt::write` takes `unit_dir` and the receipt's `unit` as separate
  strings, so a caller can pass a mismatched pair and put a receipt in the
  wrong directory; consider a type that ties the two together.
- Minor surviving mutants, pinned only by incidentals today: M22 (the
  `ppx > 0` guard in `price_check`; killing it needs a fixture with a
  non-positive Phoenix price), N13 (the finite-price filter in `hourly_prices`;
  needs a non-finite fixture price), N8 (Phoenix is left out of the dropped
  hours max, so it needs fixture counts where HL and Phoenix differ).

## Post-merge real-data rerun (main 83da358)

After #99 merged, the U1 mapping was rerun on real data at main
83da3584aed1b24f93ca1792ec2f0c236559cbab (code identical to PR head 5d9d0a6)
from a release binary built at that SHA
(sha256 7f259c899530734e461b187bedccf9eed524a41a3546e5e5f3c1455b159ffd8d),
same inputs and check days (2026-10-15..17) as review round 1, written to a
new dated store `free/combined/qa-u1-20261101-83da358` (under the solos-data
data root). Results: rows 29,370 (hl 29,220, phoenix 105, pyth 39, jup 6),
unchanged from round 1; 144 pairs checked, 96 mapped, 0 price divergence,
48 thin; lockbox_hours_dropped 864, lockbox_hours_excluded 864;
lockbox_only_symbols hl 7,354, phoenix 0, pyth 0 (the 7,354 matches the
drop PR #99 reported against the pre-review QA run (36,724 to 29,370 rows):
the lockbox-only coins). Rows and symbols only match because
lockbox_only_symbols counts (coin, class) groups and there is no two-class case
in this data. assets.csv sha256
54a076d5686a098850703c791e51c6b45033681d0728af12217b1bbac6b2c715; versus the
round-1 output it differs only in last_ts on 104 phoenix and 13 pyth rows
(live lanes moved on), no other column changed.

Non-blocking follow-ups:

- HL groups by (coin, coin_class). If a coin trades in two classes and one
  class exists only inside the lockbox, that class is counted as lockbox-only
  and skipped before the two-classes guard runs, so the guard never fires for
  that coin and lockbox_only_symbols is off by one (it counts a class, not a
  coin). Fix later by checking classes per coin before the lockbox skip, or
  counting distinct coins.

## U2a hl-hours (#101) review follow-ups

Non-blocking items from the U2a review round 1 that are recorded rather than
fixed now.

- The per-hour skip key's ctx facts are conservative: they hash both lanes'
  day and previous-day files even when a lane was not consulted for the
  bucket's coins, so an unrelated ctx file change rebuilds hours that its
  bytes cannot affect. Track exact per-lane, per-bucket usage if the extra
  rebuilds ever matter.
- The run receipt's `inputs` carries the fills watermark, the lockbox sha and
  the assets.csv hash, but not the ctx input facts; only the per-hour receipts
  do. Add a run-level ctx summary if run receipts are ever audited alone.
- A run whose buckets all sit inside the lockbox writes no output files, so
  the catalog's `watermarks["hl-hours"]` entry is not refreshed that run (the
  upsert is per sealed file); it keeps the last run that built something.
  Refresh the watermark entry even on a zero-output run if that stale window
  ever matters.

## U2d spotdex-hours (#100) review round 1 follow-ups

From the #100 review (Carmack, REQUEST CHANGES). Recorded here so nothing is
lost; the shared-helper items have since landed and the dedupe happened in
the post-#101 rebase.

- Spotdex has no crash failpoint seam of its own (no `on_before_write`-style
  hook around the hour receipt -> venue seal gap). The lost-venue-month probe
  is covered by `a_rerun_rebuilds_a_lost_venue_month` (delete the venue dir,
  rerun, byte-identical rebuild); a real kill-point seam is future work.
- The month skip key holds no output-derived value (the pre-seal month file
  sha came out again in round 2): `seal::check` verifies the sealed file
  against the receipt's output sha, so the first rerun must skip. Pinned by
  `an_unchanged_month_skips` and mutant M16.
- The month key's price-file facts are whole-list: any Pyth file change
  rebuilds every month (and hour) even when the changed print cannot affect
  the bytes. Track per-file span pruning if the extra rebuilds ever matter.
