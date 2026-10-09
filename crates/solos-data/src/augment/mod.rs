//! The augmentation lane (ADR-0009): free exogenous market data for the Phoenix Rise perps
//! dataset, downloaded from public venues and APIs, stored as Parquet next to the raw and
//! decoded roots, and kept current. `augment sync` is the idempotent backfill and catch-up of
//! dated files and paged histories; `augment capture` is the long-running lane for streams that
//! cannot be fetched later.

pub mod alternative;
pub mod auto;
pub mod binance;
pub mod bybit;
pub mod candles;
pub mod capture;
pub mod config;
pub mod contexts;
pub mod defillama;
pub mod deribit;
pub mod elfa;
pub mod http;
pub mod hyperliquid;
pub mod kalshi;
pub mod ledger;
pub mod parquet;
pub mod periods;
pub mod phoenix;
pub mod polymarket;
pub mod query;
pub mod sec;
pub mod series;
pub mod sse;
pub mod sync;
