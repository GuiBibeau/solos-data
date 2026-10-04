# ADR-0001: Raw transaction collection

Date: October 4, 2026. Status: accepted for the first engineering release.

Collect Phoenix perpetuals transactions continuously, with usable recent history
and portable storage. Keep the collector independent of any trading application.

Use TypeScript with Node 24, Solana Kit for identity checks, and a raw transport
for transaction capture so that the original RPC JSON survives numeric conversion.
DuckDB owns checkpoints and Parquet output. One process owns one serialized DuckDB
connection; tail and history network work run concurrently through one CU bucket.
Deploy with Docker Compose or systemd user services. Store runtime data separately
from source code.

Pass SOLANA_RPC_URL through the environment. Keep credential files mode 0600 and
outside Git. The decoder needs no RPC key. No wallet or signer is used.

The exchange endpoint supplies market metadata.
Confirm the owner of the snapshot's globalConfig address against the expected
program, require that program to be executable, then resolve its ProgramData
account from the parsed loader state. This pins identity to the official exchange
snapshot and finalized chain data rather than the third-party program listing.

Preserve block ordering (D1 remains unresolved). Single-signature slots are marked
explicitly; multi-signature slots must join every signature to the finalized block.
Keep the full original response alongside base64 wire bytes and extracted columns.
Advance W only after fetch completeness, ordering coverage, fsync and registrations.
W represents coverage through a slot, including quiet slots, with the newest observed
program signature at or below it. H0 is recorded once and never replaced on restart.

Independent V3/V4/V5 checks, authenticated Phoenix fills, a second provider,
snapshots, websocket refresh, classified upgrade events and external alerts are
remaining milestones. Until those checks exist, **never seal or claim full
completeness**. ProgramData touches raise journal attention events with an explicit
unclassified kind. External alert integrations remain future work.

Alchemy supports getTransactionsForAddress. Paginated accounts use getProgramAccountsV2 with
paginationKey and withContext, not pageKey/order on getProgramAccounts. Keep the
standard signature manifest as the reference. A native capability probe verified
the bulk API's oldest records, base64 encoding, signature recovery and recent slot
filtering, so fetch uses bulk pages (100 transactions for 100 CU) before retrying
any remaining manifest signatures with getTransaction. Bulk cursors and raw pages
commit with their transaction rows; canonical original responses are exported once
per page. The standard block join still validates every multi-transaction slot.

Phoenix transactions include v1 envelopes. A maxSupportedTransactionVersion=0
setting would omit them. Set 1 on
transaction/block reads and use Kit 8.4's envelope decoder for signatures: v1 moves
the signature vector to the end. Regression fixtures cover legacy, v0 and v1.

References (checked October 4, 2026):

- https://www.alchemy.com/docs/reference/compute-unit-costs
- https://www.alchemy.com/docs/reference/throughput
- https://www.alchemy.com/docs/chains/solana/solana-api-endpoints/get-transactions-for-address
- https://www.alchemy.com/docs/chains/solana/solana-api-endpoints/get-program-accounts-v-2
- https://perp-api.phoenix.trade/v1/view/exchange
- https://github.com/solana-foundation/solana-dev-skill/blob/main/skills/solana-dev/references/transactions-v1.md
