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
