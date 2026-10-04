# ADR-0004: Bound request rates and remove repeated work

Accepted October 4, 2026. Configure the CU ceiling for the deployment's RPC plan.
The native sample config uses 10,000 CU/s with 80% utilization; Docker defaults
to 1,000 CU/s with the same utilization. Override these for the actual account.

Use the utilization factor to leave capacity for other account traffic.
Reserve 20% for live capture, permit existing idle borrowing,
and increase bounded request concurrency to 24 live / 32 historical. Smooth bursts
to 100ms of CU budget (at least the largest method weight). Existing rate-limit
backoff, Retry-After handling and concurrency reduction remain active. This is a
ceiling, not a promised request rate or a guarantee that the shared account cannot
be throttled. Per-method queue/network time and range timings expose the next limit.

Live bulk fetching bounds itself to the first/last missing manifest slots instead
of re-fetching hydrated overlap. Standard fallback and validation still inspect the
whole original range. Bulk responses remain checked against the standard manifest,
and page/cursor writes stay atomic. Interrupted work can replay under tighter bounds.
Split bulk fetching into disjoint 128-slot windows, with up to eight windows in
flight under the same global CU limiter. Each window keeps its own durable sequential
pagination token. Load its completed standard manifest once rather than querying the
entire signature table for every hundred-row response. Await all in-flight work on
failure before retry; original range validation and publication remain the boundary.
Bound both sides of range joins to avoid hash-building the whole accumulated transaction
table. Database and process memory limits are separate, configurable ceilings.
Tune both to the machine and data size.

Ordering reads the range cache once, validates finalized full-block signatures with
up to 32 requests in flight, and commits bounded 64-slot groups. A failed group is
unpublished and retried; earlier groups remain reusable. All V1/V6 checks and durable
publication/watermark gates are unchanged. No bulk-provided index substitutes for
the independent block-order lookup. One serialized DuckDB writer remains the owner.

Measure published historical slots per wall time, RPC errors/throttles, and forward
watermark freshness before estimating collection time. Retain the
unfulfilled independent-validation and sealing criteria in the acceptance ledger.

Sources: https://www.alchemy.com/docs/reference/pricing-plans and
https://www.alchemy.com/docs/reference/throughput (accessed October 4, 2026).
