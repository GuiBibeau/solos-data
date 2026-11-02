# Combined hourly store follow-ups

Left open after building the U2b/U2c hourly units (`phoenix-hours`,
`solana-perp-hours`, `pyth-hours`) on the combined scaffold. These are not part
of those units' acceptance; each needs an owner decision before the full-span
build.

## Acceptance criteria

- [ ] The Pyth mapping rows are config-seeded and `mapped=true` without a
  price check. QA over 2026-10-15..17 shows the `kBONK` Pyth feed does not
  track the HL `kBONK` perp (median |ln ratio| 6.9, about 1000x: the feed is
  not the same instrument). The other 9 checkable feeds pass. Give U1 a
  price-tracking check for Pyth rows (same rule as the Phoenix pairs, demote
  to `mapped=false` otherwise), or drop `kBONK` from the config's Pyth feeds.
- [ ] Flash Trade events carry `market` addresses, but no Flash market rows
  exist in `mapping/assets.csv` (the venues config has pools and custodies
  only), so every flash row in `solana_perp_coin_hour` has a NULL `asset_id`.
  Price-matching works for the liquid markets (prototype, 2025-12-01..07,
  median |ln(event price / Pyth)| : SOL markets 0.0018-0.0020, BTC 0.0020,
  ETH 0.0017-0.0021; long-tail markets fail) but is not wired anywhere.
  Decide: extend U1 with price-checked flash market rows, or keep flash
  venue-grain only.
- [ ] The stored Phoenix `taker_side` column is the constant `Bid` on every
  row (the decoder port compares against a capitalised `Bid` while the SDK
  emits lowercase, kept for parity - `decoder/normalize.rs`). The units
  derive the taker direction from `maker_side` instead. Fixing the decoder
  would change decoded bytes (a re-decode), so for now the quirk is documented
  here, in the unit docs, and in each receipt's config hash input.
- [ ] The Phoenix parent-revision check reads `decoded_transactions` for the
  selected fills' epochs on every run (bounded runs read a few epochs; a
  full-span run reads all 552 registered files). If that proves too slow for
  the full build, consider checking only epochs whose fills files carry more
  than one batch id (today that is the live epoch only).

## #106 review follow-ups

Left open from the first review of PR #106 (U2b/U2c). The shared-root items
move to the shared seal PR (`pi/combined-shared-seal`) and are listed here so
#106 does not lose them.

- [ ] Shared, blocking: the idempotent skip key omits `assets.csv` content,
  `config_sha256`, the lockbox sha and the requested range, and a skipped hour
  does not re-hash the sealed file against its receipt (`M11`/`M12`). Fix lands
  in the shared seal PR's skip-key helper; #106 routes through it afterwards.
- [ ] Shared, blocking: `lockbox_hours_dropped` counts requested-range hours,
  while the receipt schema (and ADR-0023) define it as covered input hours the
  lockbox actually removed. Same shared PR; the U2 call sites switch to the
  shared computation (`M01`'s filter itself is pinned here by the in-lockbox
  fixtures).
- [ ] Shared: a fixture builder with data inside the lockbox. This round ships
  per-unit local builders (`phoenix`/`perp`/`pyth` lockbox tests with 10-03
  data); they get swapped for the shared builder when it lands.
- [ ] The report's `assets`/`pairs`/`feeds` counts cover the whole aggregate
  (lockbox hours included), not just sealed hours. Report-only cosmetics; the
  receipts carry the exact per-run facts.
- [ ] Pyth rows can duplicate on `(hour, feed_id)` across lanes when the two
  lanes disagree on `symbol` (the dedupe key is `(feed_id, publish_time)`), and
  the `at_ms` tie-break is arrival-order, not chain order. Needs a decision on
  which lane's symbol is authoritative.
- [ ] The Flash `1e6` size scale and the default price exponent `-8` are read
  off the data and verified against Pyth for SOL/BTC/ETH, but not derived from
  the Flash program. Pin them from the IDL when one is fetched.
- [ ] Each run overwrites the per-run receipt; only the per-hour receipts are a
  durable history. Fine for now (the catalog watermarks repeat the latest
  run), but a run-history file may be wanted for audits.
- [ ] Hour files sealed before a lockbox change stay on disk (the plan drops
  the hours, nothing deletes the old files). The skip-key fix must also
  invalidate on the lockbox sha; physical cleanup of stale partitions is still
  open.
- [ ] `pyth_coin_hour` has no `mapped` column (`asset_id` NULL marks unmapped).
  Adding one is an output-shape change (unit version bump) so it waits for the
  next schema window.
- [ ] Hours that become gap hours after having been sealed keep their stale
  file, hour receipt and catalog entry: a key change rebuilds everything
  except gap hours, which are cut from the plan and neither rebuilt nor
  deleted. Same mechanism as the lockbox case above; physical cleanup of stale
  partitions is still open for both.
- [ ] The `lockbox_hours_dropped` covered-hour sets are window-sensitive in
  phoenix (only fills files overlapping the requested window) but
  window-independent in solana-perp/pyth (all walked files' spans). Both are
  defensible readings of "covered input hours"; pick one and say it in
  ADR-0023 before the full-span build.
- [ ] io.rs's `lock_output`/`walk_parquet`/`open_reader` are verbatim copies
  of the mapping.rs helpers; `load_provenance` already moved, finishing the
  move would satisfy the one-helper rule.
