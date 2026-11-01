# ADR-0023: Combined hourly store

Status: accepted, November 1, 2026.

The all-venue model needs Hyperliquid and Solana features on one hourly grid.
This note adds `solos-data augment combined <unit>`: offline batch units that
build one store under an operator-given root, beside `hl-fills-v1`. Nothing
under `augment/`, `phoenix_decoded/` or the collector is touched; the units
read local Parquet and CSV only, never open a `.duckdb` file, never download,
and never send a transaction.

## Units

One command, one receipt, one unit. `mapping` (this note) assigns canonical
`asset_id`s. Hourly aggregators and assembly follow in their own units. The
hour grid is UTC, unix hour `t = floor(ts/3600)*3600`, the same construction
as hl-train's full grid.

## Lockbox

Every unit takes `--lockbox <json> --lockbox-sha256 <hex>` and refuses on a
sha mismatch before reading anything else. The v2 file's weeks are expanded
with `guard_hours` on both sides into a set of excluded UTC hours; the run
asserts the v2 shape (864 hours over 36 dates) and refuses a check day inside
that set. The hours are dropped from every output table the unit writes, not
only from assembly. Each receipt records `lockbox_sha256` and
`lockbox_hours_dropped`.

## Outputs, receipts, catalog

Output files are written through the shared durable pattern: tmp, fsync,
rename, directory fsync. A unit verifies its output (read-back count, sha)
before it writes `<unit>/receipt-<unit>.json`, and rewrites `catalog.json`
only after that. The receipt carries: unit, unit version, rows, outputs with
sha256, the input watermarks (hl-fills-v1 hours and their hash, file lists of
the other inputs as relative paths and sizes), the lockbox fields, a config
sha256 over the unit's semantic rules, the operator's `--provenance` object
verbatim, and started/finished UTC. A rerun with the same inputs writes the
same bytes; `--threads` (1..=8) and `--memory` (at most 32GB) are DuckDB caps
that do not change the table.

## Mapping

`mapping/assets.csv` has one row per (venue, venue symbol) with columns
`asset_id, venue, venue_symbol, builder, coin_class, venue_market_id,
first_ts, last_ts, source_of_truth, mapped, reason, median_abs_log_ratio,
n_hours`, sorted by venue then venue symbol. HL perp and HIP-3 coins are one
row each; a `builder:` prefix is stripped into `builder`. `@N` spot stays
unmapped (`spot_unnamed`) until spotMeta is available; `#N` outcomes never map
(`outcome`). A Phoenix candidate comes from the augment asset_contexts
`phoenix_symbol` column when that asset trades on Phoenix, else from the equal
base name. Phoenix prices are `price_ticks * tick_size *
10^(base_lot_decimals - quote_lot_decimals)`, the fill's own tick and lot
decimals (slice 0 validated it against HL for BTC and SOL). Every candidate
pair is checked over three operator-given,
non-lockbox days: hourly last price on both venues, median |ln(hl/ph)| over
hours both traded; mapped only when the median is under 0.01 and at least 24
hours traded, otherwise `price_divergence` or `thin`. Pyth feeds (market
symbol, feed id) and Jupiter custodies (label, address) come from
`config/venues.json`; a feed without a market name falls back to its base
symbol. Venue names are the store's: `hl`, `phoenix`, `jup`, `pyth`. Pyth rows
are venue rows with `source_of_truth venues_config`; unmapped or demoted
assets keep their per-venue features and get no cross-venue block downstream.
