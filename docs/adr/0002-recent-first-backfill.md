# ADR-0002: Publish recent history while walking backward

Date: October 4, 2026. Status: accepted.

Recent history should be usable while collection continues. Waiting for a complete
signature manifest and then fetching from launch delays the useful dataset.

Starting at immutable H0, collect and publish descending, contiguous slot ranges.
Each range is at most 1000 slots by default, independently configurable from tail
catch-up chunks. Before fetching a range, advance the standard program and
ProgramData signature cursors strictly below its lower slot, or to EOF. Strictly
crossing the boundary includes transactions split across same-slot cursor pages.
Reuse the existing index and cursor; preserve legacy hydration checkpoints for
audit. Previously fetched transactions and published files are retained.

Fetch raw transactions, join finalized block ordering, perform V1/V6, and durably
publish Parquet before atomically moving the backward checkpoint. Failed or
interrupted ranges replay without skipping slots. Tail W remains independent and
nonregressing. Stop at the earliest indexed address transaction after both cursors
reach EOF. Independent validation and sealing are still pending.

Maintain a persisted published-range registry alongside file registrations. Migrate
existing publication ranges from registered transaction microbatch filenames.
The checkpoint reader view uses published coverage, so partially hydrated rows do
not appear usable just because their slot is below the forward watermark.

Emit an atomic catalog of active immutable files and merged published intervals.
The native query command reads the catalog and Parquet in an independent in-memory
DuckDB, deduping each signature by publication time. Readers continue while the
supervised writer collects or compacts; superseded files remain available to
in-flight catalog snapshots. A later retention policy must preserve that contract.

Bulk validation loads each completed window's manifest once, without weakening
identity or gap checks.
