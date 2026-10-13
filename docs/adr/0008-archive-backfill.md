# ADR-0008: Historical backfill from the Old Faithful archive

Status: accepted, October 5, 2026. Amends ADR-0001's independence rule for historical ranges.

The RPC backfill is bound by the provider's compute-unit ceiling: about 68k CU per 1,000-slot
chunk, most of it `getBlock` for ordering, which gave about nine chain-hours of history per
wall-hour. The Old Faithful archive (Triton One, Project Yellowstone) holds every finalized block
of every completed epoch as CAR files on a public mirror, and Anza's Jetstreamer replays them from
Rust at memory speed. The user chose it for the backfill on October 5, 2026.

## Decision

- **Three lanes.** Live tail over RPC (unchanged). Gap backfill over RPC for slots above the
  archive tip. Archive backfill through `jetstreamer-firehose` for ranges whose newest slot is at
  or below the archive tip, when `archiveBackfillEnabled` is set. The archive tip is the last
  slot of the newest epoch whose CAR the mirror serves, probed downward from the current epoch
  and refreshed every ten minutes.
- **Independence kept where it matters.** The signature manifest still comes from RPC
  `getSignaturesForAddress` for the program and its ProgramData address. V1 completeness is
  exact: every manifest signature must arrive from the archive stream for the range, or the range
  stays unpublished and replays whole. V6 ordering takes `transaction_slot_index` from the
  archive and keeps `getBlock` as a sampled cross-check (`archiveOrderingSample`, default 2 % of
  multi-transaction slots) instead of one call per slot.
- **Same rows.** Archive ranges fill the same tables. `tx_b64` is the wire re-serialized by the
  SDK, `meta_json` is the RPC JSON shape produced from `TransactionStatusMeta` through the SDK's
  `UiTransactionStatusMeta`, `provider` is `old-faithful`, `raw_rpc_json` records the epoch and
  in-block index, `rpc_pages` stays empty, and `slot_order.block_signature_count` is the block's
  executed transaction count. A transaction seen from both sources at a lane boundary may differ
  in `meta_json` text and receive one decoder revision.
- **Chunking.** Ranges of `archiveChunkSlots` (default 10,000) stream in reverse with
  Jetstreamer's own parallelism; the range commits only when the stream has returned, so
  publication stays contiguous and descending. The lane spends no compute units except the
  manifest walk and the sampled cross-check.
- **Build.** The lane is behind the `archive` feature because its dependency tree (agave 4.2
  crates, RocksDB) needs clang and cmake and multiplies build time. The release binary for the
  box is built with the feature; CI builds without it. Jetstreamer is pinned to a commit on its
  `main` branch: the crates.io release fails on recent epochs and cannot decode v1 transactions.

## Consequences

The archive lags the tip by up to about two days plus publishing time, so the live tail and the
gap lane remain RPC. Every epoch is downloaded whole (about 735 GB for epoch 1048) because the
archive has no program filter; the box's network interface makes that minutes to an hour per
epoch. The archive carries no account updates. Old Faithful does not state finality in its
documentation; the blocks are the canonical chain as archived after the epoch closed, and the
RPC manifest check guards completeness. The proof before cutover re-collects a range the RPC
lane already published and compares every row.

## Production validation (2026-10-05)

`dev archive-check` on slots 452,000,000 to 452,009,999, a range the RPC lane had published:
338,521 transactions and 9,996 blocks from the archive, every row identical to the archive
(signatures, block indexes, wire bytes, decisive meta fields). Streaming the range in reverse took
371 s; streaming forward took 12 s for the neighbouring range 452,010,000 to 452,019,999 (285,714
transactions, identical rows), so forward is the default. The mirror delivered 116 MB/s to one
plain stream and 522 MB/s to eight, so the lane is not network-bound at this box.

First live range after cutover (reverse, 450,217,137 to 450,227,136): 212,435 transactions, stream
230 s, insert 69 s, manifest 36 s, validation 28 s, V1 and V6 passed, published with
`source: archive`. With forward streaming the stream phase is seconds, so the manifest walk, the
checkpoint insert and validation dominate a range; `archiveChunkSlots` moves from 10,000 to 50,000
to amortize them. The reverse direction stays available through `SOLOS_DATA_ARCHIVE_REVERSE=1`.
Routine retention trimmed 16,000 slots per minute, below what the lane now publishes, so
`retentionSlotsPerPass` becomes a setting and is 100,000 on the box.

Budget, 15:00 UTC: the provider account answered 429 ("exceeded its compute units per second
capacity") at about 300 CU/s, measured with sequential `getSignaturesForAddress` pages, where the
TypeScript run of the previous day had averaged about 776 CU/s with two throttles in thirty hours.
At that ceiling the lane's manifest walk (over 1,000 pages per 50,000-slot range) starved the tail,
whose share was 20 % and which cannot borrow the backfill's tokens; the watermark fell 85 minutes
behind the finalized slot. The limiter is now configured at the measured ceiling, `tailShare` is
0.85 and `archiveOrderingSample` 0.005: at the venue's current activity (about 55 transactions per
slot, three times late September) the tail alone needs about 200 CU/s to stay current. The lane is bound by the manifest walk, not by the archive:
about 7 chain-hours per wall-hour at 300 CU/s, against about 30 while the tail starved. More
provider throughput, or a second provider for the tail, is the way to raise it.

Archive-only, 16:00 UTC: at that ceiling the live tail alone costs about 21M CU a day, so the user
chose to backfill first. `tailEnabled` (default true) switches the tail lane off; the follower loop
keeps the exchange refresh and the maintenance pass. The archive lane then spends about 1.1 CU per
slot, the whole nine-month history about 90M CU.

Live tail back on, 2026-10-13: the operator moved `SOLANA_RPC_URL` to a Helius plan. A read-only burst
of `getSignaturesForAddress` pages (limit 1,000) ran at about 610 requests a second with no 429, against
about 7 a second (300 CU/s) on the previous account, so the limiter is set to 8,000 CU/s
(`SOLOS_DATA_CU_PER_SECOND`, about 200 such pages a second) and `tailEnabled` is true again. The tail
resumes from its watermark (2026-10-05) over RPC and closes the gap at about 65k CU per 1,000 slots
while the archive lane keeps its share. The previous `collector.env` is kept beside it for rollback.

Tip refresh, 2026-10-07: the lane had probed the archive tip once per process and treated a
throttled mirror as "no archive", so after an out-of-memory restart at 02:27 UTC, while the mirror
answered 429, the lane fell back to RPC chunks of 1,000 slots for twelve hours at forty times the
compute units per slot and one fiftieth of the pace, without an error. The tip is now probed every
ten minutes in the background; a probe that gets no answer keeps the last known tip (the mirror
only adds epochs), the last tip survives restarts in the `archive-tip` record, and when no tip is
known at all the lane waits and logs `archive_tip_unknown` instead of using RPC below the tip.

Manifest walk dead cursor, 2026-10-11: from 01:30 UTC the lane retried slots 427,113,137 to
427,163,136 every ten minutes with `badSlots=2` (slots 427,120,310 and 427,131,354). The program's
manifest walk had stored `done` at slot 428,785,356 on 2026-10-10 20:30 UTC because the provider
answered `getSignaturesForAddress` with an empty page for `before=<the walk's cursor>`, every time
(re-checked on 2026-10-11), while `before=<the row above it>` lists that cursor at position 34 of a
full page and history continues below it. The walker read every empty page as the end of history.
Below that slot the manifest held only ProgramData-walk rows, so V1 (every manifest signature
arrived) and V6 (per manifest slot) passed vacuously: 33 ranges, slots 427,163,137 to 428,785,355,
65,865,105 archive transactions, were published with no RPC manifest behind them. The stuck range
failed only because it held two ProgramData-walk signatures in busy slots: the manifest saw one
transaction in each slot, the archive saw the whole block (single_in_slot false, tx_index set).
Retries were idempotent: no Parquet files or catalog entries since 01:23 UTC, the checkpoint kept
2,155,092 distinct rows for the range; each retry re-streamed the epoch and spent its sampled
`getBlock` cross-check.

Changes: (1) V1 now runs in both directions: a transaction in the range whose signature is not in
the manifest counts as `unlisted` and fails the range, so a short manifest can no longer pass.
Before this incident every archive range had `unlisted` 0 (archive and manifest counts were equal
for every earlier range). (2) A walk keeps a `fallback` cursor, the row above `before`; an empty
page is confirmed from it, and a confirming page that lists rows below the cursor resumes the walk
(`walk_cursor_unresolved`). (3) A stored walk that ended on an unconfirmed empty page above the
chunk being collected is resumed from the oldest published transaction above the chunk
(`walk_reseeded`); the slots it skips stay as published and are listed in the event. Open: the 33
ranges above need a manifest re-walk and comparison before sealing; they stay unverified until then.

Manifest re-verification, 2026-10-13: `collector reverify --from <slot> --to <slot> --out <dir>`
re-walks the manifest the backfill uses (the program and its ProgramData account, through the same
`walk_page`, fallback cursor included) and compares it with the published transactions, so the 33
ranges above can be checked without stopping the collector. It never opens the live checkpoint: each
published range intersecting the request (from the `<from>-<to>-<id>.parquet` names of the
catalog's transaction files; slots only compacted files hold become a range from the catalog's
coverage, and an unpublished slot or an overlap is an error) gets its own DuckDB store under
`--out`, keyed by the compared window,
and its walk starts from the oldest published transaction above the range, with the next oldest as
the fallback cursor, so ranges walk in parallel and a cursor the provider cannot resolve is
confirmed like any other. The ProgramData walk runs once from the tip. The archive side is read
from the catalog's Parquet under a reader lease, newest publication per signature, as `query` and
the reseed anchor read it. Per range it reports manifest and archive counts, `missing` (listed, not
published), `unlisted` (published, not listed), `slotMismatch`, and V6 (`unordered`,
`badOrderingSlots`) exactly as range validation computes them, with samples inline and the full
mismatch lists as CSV next to the range's store; a walk that ended on an unconfirmed empty page
above its floor fails the range.
Requests go through a limiter of their own (`--rps`, default 50 `getSignaturesForAddress` calls a
second, which backs off on 429s like the collector's), so the running collector keeps its share.
The command changes nothing in the raw root; a range that fails is repaired separately.
