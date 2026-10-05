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
