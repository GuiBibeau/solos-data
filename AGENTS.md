# solos-data

Read-only research collector. Runtime data, credentials, personal paths and
private deployment notes never enter Git. No trading or transaction-send code.

Use `npm run verify` for offline regression checks; `npm run collector -- probe`
for provider checks. No throwaway RPC scripts. Provider URLs stay in environment
files (0600); never log URLs, request headers or unfiltered provider error bodies.
All Solana reads use finalized commitment. No signer or transaction-send method.

One supervised process owns DuckDB. Network tasks may overlap, but database
transactions are serialized. Watermarks advance only after durable files and
range checks. Never seal partitions without all required independent checks.
Record engineering decisions in docs/adr and unfinished acceptance criteria in
docs/issues. Keep modules small and test failure/restart behavior with fixtures.
