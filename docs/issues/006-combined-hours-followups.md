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
