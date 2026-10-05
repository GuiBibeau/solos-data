//! `solos-data`: the Phoenix perpetuals collector and decoder as one binary. This library holds
//! everything the binary and the integration tests share.

pub mod catalog;
pub mod cli;
pub mod decoder;
pub mod dev;
pub mod fsutil;
pub mod gc;
pub mod jsonout;
pub mod lease;
pub mod repack;
pub mod store;
