# solos-data

We collect Phoenix perpetuals transactions from Solana and turn them into tables
you can query with SQL.

Two jobs run together: one saves new transactions, the other works backward from
now into older history. Each finished batch becomes available right away.

```text
Solana → raw transactions → decoded events → Parquet files → SQL
```

## 1. Set up

You need Docker and an Alchemy Solana mainnet RPC URL.

```sh
cp .env.example .env
```

Edit `.env` and add your RPC URL. Keep this file on your machine.
Set the collection rate to fit your Alchemy plan.

## 2. Start collecting

```sh
docker compose up -d --build
```

The collector saves raw transactions. The decoder reads them and writes tables.
Both jobs resume from saved progress after a restart.

## 3. See the progress

```sh
docker compose exec collector solos-data collector status
docker compose exec decoder solos-data decoder status
```

## 4. Ask a question

For example: which markets have the most recorded fills?

```sh
docker compose exec decoder solos-data decoder query --sql \
  'SELECT asset_symbol, count(*) AS fills FROM fills GROUP BY asset_symbol ORDER BY fills DESC LIMIT 10'
```

| Table | What it contains |
| --- | --- |
| `decoded_transactions` | Decode result for each transaction |
| `events` | Decoded events, including failed attempts |
| `fills` | Completed order-book and spline fills |
| `order_events` | Order placements, changes and cancellations |
| `funding_events` | Funding payments |
| `decode_errors` | Payloads that need more decoder work |

Files live in `data/`. We use Parquet with ZSTD compression and DuckDB for SQL.
Use the query command to get the latest rows without counting corrections twice.
You can move the files and run the same code on another machine.

This is work in progress. Older data is still being collected. Decoded events do
not yet reconstruct full order books or positions. Full independent checks are
still pending. See the [remaining work](docs/issues/001-phoenix-acceptance.md).

[How it works](docs/architecture.md) · [Table details](docs/decoded-tables.md) ·
[Server setup](ops/runbook.md)

The collector and decoder are one Rust binary, `solos-data`. History below the last
published Old Faithful epoch is replayed from the archive through Jetstreamer; the
live tail follows RPC. For development, use Rust 1.96: `cargo test --workspace`.
The code is [MIT licensed](LICENSE). This repo contains code and test fixtures;
collected data is stored separately.
