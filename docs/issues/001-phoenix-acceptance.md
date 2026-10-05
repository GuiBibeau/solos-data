# Phoenix acceptance backlog

The first release starts collection. This ledger prevents unfinished work
from being mistaken for complete acceptance. Checkmarks require recorded evidence.

- [x] Portable Docker/Compose and systemd deployment with restart recovery.
- [x] Finalized-only standard RPC; identity and mainnet checks.
- [x] Atomic manifest/cursor, signature dedupe, immutable H0, resumable history walk.
- [x] Recent-first hydration/publication in bounded descending ranges without waiting
  for manifest EOF; preserve existing cursors, checkpoints and tail reservation.
- [x] Atomic published-coverage/file catalog and a deduplicating native query command
  usable while the supervised writer runs; unpublished fetched rows stay hidden.
- [x] Base64 raw wire, original RPC JSON, failed transaction inclusion.
- [x] Verified Alchemy bulk API acceleration with slot bounds; legacy/v0/v1 wire
  signatures and transaction version 1 opt-in; raw page and cursor atomic checkpoints.
- [x] Multi-transaction block order join; missing signature is a hard failure.
- [x] Paid-plan CU ceiling with burst smoothing/backoff, disjoint parallel bulk
  windows, bounded ordering commits, range-limited joins and per-method/range timings;
  interruption/retry fixtures and short live throughput/freshness observation.
- [x] W only advances after V1/V6 and durable registrations; overlap and catch-up chunks.
- [x] Atomic Parquet output, hashes/counts, orphan cleanup and revision compaction.
- [x] Range-bounded duplicate checks and ordering writes; bounded compaction prefix;
  decoder payload reads selected by narrow keys with compatible resume offsets.
- [x] Independently supervised live decoded Parquet/DuckDB pipeline, official pinned
  MIT event codec, failed-attempt exclusion, unknown payload quarantine, precise
  integers, durable resume/dedupe and portable relative catalog; offline golden fixtures.
- [ ] Audit codec compatibility against every historical upgrade and independent
  event/fill sources; preserve corrected versions and reprocess quarantined payloads.
- [ ] Validated state snapshots and forward replay for books/positions, market funding
  rates, and coverage-aware 10-minute features. Decoded events alone do not provide these.
- [x] Bounded decoded compaction, replacement verification, reader-aware input cleanup,
  permanent raw archive with verified checkpoint trimming and checkpoint rewrites;
  staging-memory and cleanup-fairness regression checks. Production counts/hashes
  and storage results: [ADR 0006](../adr/0006-continuous-storage-retention.md).
- [ ] Shared-server SQL serving if needed.
- [ ] Complete M0: full recent day, JSON-RPC batch probe, same-slot until boundary,
  lookup-table/CPI independent samples, event-sequence decision D1, websocket deltas.
- [ ] Size tail at >=2R from a representative day and the actual account CU/s limit.
  Demonstrate sustained tail freshness while backfill is active; no SLO claim yet.
- [ ] V3 full-history market/spline cross-checks at first discovery, daily afterward;
  extras must be fetched, ordered and published even below the current W.
- [ ] V4 authenticated fills pagination for all retained markets (Phoenix credential needed).
- [ ] V5 independent provider range samples (Helius credential needed); provider identity on
  every result; disagreement holds sealing and emits an explicit discrepancy report.
- [ ] Null fallback/terminal rows and persistent-outage tail failover; backfill pauses.
- [ ] Paginated present-state snapshots with getProgramAccountsV2, context slot per page,
  daily and on backfill completion, including restart-safe snapshot pagination.
- [ ] WebSocket sequence-aware config deltas and polling fallback; retain deleted markets;
  preserve each status change's observed slot rather than only first/last observation.
- [ ] Classify ProgramData deploy/upgrade/authority/close events from loader instructions;
  journal attention events alone do not establish program_versions semantics.
- [ ] Complete metrics: freshness, per-cycle/range throughput, null/fallback rates,
  persisted CU estimates across restarts, error streak/SLO breach alerts and retention.
- [ ] Partition state/version/seal transitions, target-size compaction, explicit versioned
  corrections and safe garbage collection. V1–V7 required before sealing.
- [ ] Full 24-hour outage, five random SIGKILL tests across fetch/compaction/sealing,
  golden day hash, new-market and upgrade fixtures, seven-day p99 freshness evidence.
- [ ] Optional V8 live-recorder reconciliation after locating the independent recorder.

Keep validation evidence with changes. Do not include credentials, personal paths,
private deployment details or unfiltered provider exceptions.
