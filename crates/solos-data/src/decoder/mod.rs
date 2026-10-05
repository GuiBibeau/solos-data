//! The decoder: raw Parquet publications in, six decoded Parquet tables out.

pub mod compact;
pub mod extract;
pub mod normalize;
pub mod publish;
pub mod reader;
pub mod schema;
pub mod service;
pub mod source;
pub mod verify;
