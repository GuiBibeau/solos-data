//! Spot DEX swaps from the Old Faithful archive (ADR-0014): Raydium (AMM v4, CLMM, CPMM,
//! LaunchLab), Orca Whirlpool, Meteora (DLMM, DAMM v1 and v2, DBC), pump.fun (PumpSwap and its
//! bonding curves), PancakeSwap's CLMM and the proprietary AMMs (HumidiFi, Tessera V, ZeroFi,
//! AlphaQ, SolFi, SolFi V2, GoonFi): one row per pool-level trade and per liquidity change, read
//! from each program's own events (or, for programs that emit none, its token transfers) while the
//! archive streams, into Parquet under the augment root.

pub mod archive;
pub mod check;
pub mod config;
pub mod dex;
pub mod frames;
pub mod rows;
pub mod run;
pub mod trial;
pub mod tx;
pub mod write;
