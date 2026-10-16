//! The state snapshot lane (ADR-0011): periodic finalized reads of Phoenix on-chain state (order
//! books, spline collections, the perp asset map, the global trader index and every trader
//! account), kept as raw zstd account frames and decoded Parquet tables under their own root,
//! so the dataset has ground-truth book and position state to check a replay of the decoded
//! order events against. A separate process with its own ledger, status and request budget:
//! enabling it never restarts the collector.

pub mod config;
pub mod decode;
pub mod ledger;
pub mod query;
pub mod rpc;
pub mod service;
pub mod validate;
