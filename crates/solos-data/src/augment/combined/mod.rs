//! The combined Hyperliquid + Solana hourly store (DESIGN.md 5, ADR-0023).
//!
//! `solos-data augment combined <unit>` builds the store under one operator-given
//! root, one unit at a time. Every unit drops the lockbox v2 hours from its
//! outputs, writes durable files before its receipt, and registers its outputs in
//! the root's `catalog.json` only after the checks pass. It never opens a DuckDB
//! database file, never sends a transaction, and never touches the network.
//!
//! Units: `mapping` (U1) assigns canonical `asset_id`s across venues.

pub mod catalog;
pub mod grid;
pub mod lockbox;
pub mod mapping;
pub mod receipt;
pub mod seal;

#[cfg(test)]
mod behavior;
#[cfg(test)]
mod testfix;
