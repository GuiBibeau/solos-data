# Hyperliquid l2Book quarantine review follow-ups

Left open after the PR #110 review of the quarantine rule (merged as f6ad0f4d).
The review approved with no blockers; these are its non-blocking notes. Mutant
names are the reviewer's (M8a, M8b), from `augment::hll2book` tests.

## Acceptance criteria

- [x] Commit order at `--jobs > 1` is not pinned by any test: the reviewer's
  mutant M8b (commit in reverse arrival order instead of `ready.remove(&next)`)
  survived. Harmless today, because `quarantine.jsonl` is a `BTreeMap` merge
  whose content is order-independent by construction, the watermark is a set,
  and raw drops are receipt-keyed per hour. But if a future change makes any of
  those order-sensitive, there is no test net. Proposed test: run mixed
  outcomes (seal, quarantine, disk floor) at `--jobs > 1` and assert the
  report's `hours` line order and the `quarantine.jsonl` record write order.
  Done: `commit_order_at_jobs_4_is_hour_order_not_arrival_order` pins both; M8b and M8a go red.
- [x] The suite has no per-test timeout: the reviewer's mutant M8a (quarantine
  record written immediately on worker arrival, out of commit order) deadlocked
  the test binary (about 0% CPU, killed after 18 minutes), and CI would hang
  until the Actions job timeout instead of failing fast. Propose a per-test
  timeout in CI: `cargo-nextest` with `slow-timeout` plus `terminate-after`, or
  a `timeout` wrapper around `cargo test`. Trade-offs: nextest runs each test
  in its own process, so the kill is clean and the report names the hung test,
  but it replaces the runner and changes how shared fixtures and environment
  are handled; a wrapper needs no migration, but it kills the whole
  `cargo test` process and telling which test hung depends on captured output.
  Done: verify.sh and the ci.yml test step wrap `cargo test` in a 1500 s limit that prints the timeout message.

## Ops

A systematic decoder regression (a field-parse bug hitting every line) would
quarantine many hours and exit 3 instead of failing the run fast, and
`LINE_FAULT_CAP` caps each hour's entries at the first 8 faults. Operators
should watch `hoursQuarantined` and `quarantineEntriesAdded` magnitudes per
run, so a wide regression reads as a burst rather than as scattered bad files.
