//! The collector: finalized Phoenix transactions into the raw checkpoint and the permanent raw
//! Parquet archive, from RPC (live tail, gap backfill) and from the Old Faithful archive
//! through Jetstreamer (historical backfill, ADR-0008).

pub mod archive;
pub mod bulk;
pub mod catalog;
pub mod compactor;
pub mod config;
pub mod diagnostics;
pub mod exchange;
pub mod fetcher;
pub mod history;
pub mod keep_hot;
pub mod lanes;
pub mod limiter;
pub mod maintenance;
pub mod object_store;
pub mod offload;
pub mod ordering;
pub mod pipeline;
pub mod probe;
pub mod reader;
pub mod retention;
pub mod reverify;
pub mod rpc;
pub mod schema;
pub mod seal;
pub mod service;
pub mod status;
pub mod validation;
pub mod walker;
pub mod writer;
