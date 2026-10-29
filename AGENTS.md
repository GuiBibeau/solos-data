# solos-data

Read-only research collector. Runtime data, credentials, personal paths and
private deployment notes never enter Git. No trading or transaction-send code.

Use `scripts/verify.sh` for offline regression checks; `npm run collector -- probe`
for provider checks. No throwaway RPC scripts. Provider URLs stay in environment
files (0600); never log URLs, request headers or unfiltered provider error bodies.
All Solana reads use finalized commitment. No signer or transaction-send method.

One supervised process owns DuckDB. Network tasks may overlap, but database
transactions are serialized. Watermarks advance only after durable files and
range checks. Never seal partitions without all required independent checks.
Record engineering decisions in docs/adr and unfinished acceptance criteria in
docs/issues. Keep modules small and test failure/restart behavior with fixtures.

## Verify

`scripts/cloud-setup.sh` installs the pinned toolchain, g++, the archive
feature's clang/cmake/libclang/pkg-config packages, Node 22.18 or newer, and
the npm lockfile. Then:

```sh
scripts/verify.sh
```

That runs `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
and `cargo test --workspace --locked`, then the TypeScript check and the Node
tests. Rust tests run once. The script prints `VERIFY PASS` or `VERIFY FAIL <step>`
and exits non-zero on failure. It compiles DuckDB's bundled C++ with g++
(`CC=gcc`, `CXX=g++`).

One Rust integration test:

```sh
CC=gcc CXX=g++ cargo test --test <name>
```

After a spot-decoder change, `SOLOS_DATA_REFRESH_SPOT_FIXTURES=1 cargo test --test spot`
rewrites the expected rows in the fixture. Review that diff before committing it.
Never point tests at provider URLs.
