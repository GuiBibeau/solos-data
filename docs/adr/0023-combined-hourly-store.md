# ADR-0023: Combined hourly store

Status: accepted, November 1, 2026.

The all-venue model needs Hyperliquid and Solana features on one hourly grid.
This note adds `solos-data augment combined <unit>`: offline batch units that
build one store under an operator-given root, beside `hl-fills-v1`. Nothing
under `augment/`, `phoenix_decoded/` or the collector is touched; the units
read local Parquet and CSV only, never open a `.duckdb` file, never download,
and never send a transaction.

## Units

One command, one receipt, one unit. Each unit's receipt lives in that unit's
output directory: `<root>/<unit_dir>/receipt-<unit>.json` (the mapping unit
writes `mapping/receipt-mapping.json`; a spotdex unit would write
`spotdex/receipt-spotdex.json`). `mapping` (this note) assigns canonical
`asset_id`s. Hourly aggregators and assembly follow in their own units. The
hour grid works in unix hour indices internally and in unix seconds in the CSV;
the hour is `t = floor(ts/3600)*3600`, the same construction as hl-train's full
grid.

## Lockbox

Every unit takes `--lockbox <json> --lockbox-sha256 <hex>` and refuses on a
sha mismatch before reading anything else. The v2 file's weeks are expanded
with `guard_hours` on both sides into a set of excluded UTC hours; the run
asserts the v2 shape (864 hours over 36 dates) and refuses a check day inside
that set. The exclusion is applied to every input span, not only to the price
check: first_ts and last_ts of every venue row are the min/max over non-lockbox
hours, and an HL coin whose fills all sit inside the lockbox has no mapping row
at all (it has no row in any output table). The receipt records two numbers:
`lockbox_hours_excluded`, the size of the exclusion set itself (864 for v2),
and `lockbox_hours_dropped`, the input hours the lockbox actually removed
(hours each input covers that fall inside the set; exact for HL from its
watermark, distinct covered hours for Phoenix and Pyth; the widest input is
the store-wide number). Symbols whose whole history lies inside the lockbox
get no row in any output table; each receipt input records how many of them
it dropped as `lockbox_only_symbols` (`inputs.hl_fills`,
`inputs.phoenix_fills`, `inputs.pyth_prices`), the run report carries the
same per-venue counts as `lockboxOnlySymbols`, and the catalog watermark
repeats them, always as counts, never as symbol names. The two counts are
computed once, in the combined `seal` module, and every unit routes its
computation through it: dropped counts covered input hours inside the set,
excluded is the set's own size.

## Outputs, receipts, catalog

Output files are written through the shared durable pattern: tmp, fsync,
rename, directory fsync. A unit verifies its output (read-back count, sha)
before it writes `<unit_dir>/receipt-<unit>.json`, and rewrites `catalog.json`
only after that. The receipt carries: unit, unit version, rows, outputs with
sha256, the input watermarks (hl-fills-v1 hours and their hash, file lists of
the other inputs as relative paths and sizes), the lockbox fields, a config
sha256 over the unit's semantic rules, the operator's `--provenance` object
verbatim, and started/finished UTC. Only the data output is byte-identical
across reruns of the same inputs; the receipt and catalog carry run timestamps.
An hourly unit that seals one file per hour must route every rerun skip through
the `seal` module's shared check instead of a hand-rolled comparison: a sealed
hour is reused only when the hour receipt's embedded skip key (unit, unit
version, config sha256, lockbox sha256, and an inputs sha256 over the
canonical JSON of every fact that can change the hour's bytes: source file
shas, the mapping `assets.csv` sha256, ctx day-file shas, the requested
window) equals the current run's key field for field, the receipt's stored
inputs still hash to its stored inputs sha256, and the sealed file's streamed
sha256 still equals the receipt's output sha256. Any other outcome (missing or
garbled receipt, any key field changed, missing or rewritten file) rebuilds
the hour, so an input change such as a renamed symbol in `assets.csv` can
never be skipped away. `--threads` (1..=8) and `--memory` (at most 32GB) are
DuckDB caps that do not change the table.

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
hours traded, otherwise `price_divergence` or `thin`; thin wins first, so a
pair with fewer than 24 common hours reads as `thin` whatever its prices say.
Pyth feeds (market
symbol, feed id) and Jupiter custodies (label, address) come from
`config/venues.json`; a feed without a market name falls back to its base
symbol. Pyth and Jupiter rows are `mapped=true` from the config alone: no
price check, no exposure or decimals check, and feed ids are recorded as the
config carries them, not normalized. Venue names are the store's: `hl`, `phoenix`,
`jup`, `pyth`. `asset_id` is not unique within a venue on purpose: the perp
`BTC` and the HIP-3 `dex:BTC` both map to `BTC` and stay separate rows.
Unmapped or demoted
assets keep their per-venue features and get no cross-venue block downstream.
