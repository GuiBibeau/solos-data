//! The augmentation lane (ADR-0009): free exogenous market data for the Phoenix Rise perps
//! dataset, downloaded from public venues and APIs, stored as Parquet next to the raw and
//! decoded roots, and kept current. `augment sync` is the idempotent backfill and catch-up of
//! dated files and paged histories; `augment capture` is the long-running lane for streams that
//! cannot be fetched later. `augment hl-fills` is an offline batch over local
//! `node_fills_by_block` hours (ADR-0020). `augment hl-l2book` is an offline batch over local
//! `l2Book` hours (ADR-0021). Neither runs inside `augment sync`.

pub mod alternative;
pub mod auto;
pub mod binance;
pub mod bybit;
pub mod candles;
pub mod capture;
pub mod combined;
pub mod config;
pub mod contexts;
pub mod defillama;
pub mod deribit;
pub mod dumps;
pub mod elfa;
pub mod episodes;
pub mod hlfills;
pub mod hll2book;
pub mod hls3;
pub mod http;
pub mod hyperliquid;
pub mod kalshi;
pub mod ledger;
pub mod meter;
pub mod okx;
pub mod parquet;
pub mod periods;
pub mod phoenix;
pub mod polymarket;
pub mod query;
pub mod sec;
pub mod series;
pub mod sigv4;
pub mod sse;
pub mod sync;
pub mod tardis;
