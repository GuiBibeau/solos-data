//! OKX historical market data (ADR-0009, exchange dumps 2026-10-19): the official static files
//! that OKX's `/api/v5/public/market-data-history` endpoint and its historical-data page link to.
//! Swap trades are one zip of one CSV per instrument and day
//! (`trades/daily/<YYYYMMDD>/<INST>-trades-<YYYY-MM-DD>.zip`), funding rates one per instrument
//! and month (`swaprates/monthly/<YYYYMM>/<INST>-fundingrates-<YYYY-MM>.zip`). OKX cuts both at
//! midnight UTC+8: the file labelled `2025-10-12` holds 2025-10-11 16:00 to 2025-10-12 16:00
//! UTC. The label is kept as OKX names it; `ts` is UTC. Sizes are in contracts (`ctVal` per
//! instrument), as OKX reports them.

use super::config::{Okx, Symbol};
use super::dumps::{Container, Kind, staged_starts_with};
use super::parquet::read_csv;
use super::periods::Granularity;
use crate::store::{Store, StoreError};
use std::path::Path;
use std::sync::Arc;

/// Source name.
pub const SOURCE: &str = "okx";

const TRADE_COLUMNS: &[(&str, &str)] = &[
    ("instrument_name", "VARCHAR"),
    ("trade_id", "BIGINT"),
    ("side", "VARCHAR"),
    ("price", "DOUBLE"),
    ("size", "DOUBLE"),
    ("created_time", "BIGINT"),
];
const TRADE_SELECT: &str = "created_time AS time_ms, make_timestamp(created_time * 1000) AS ts, trade_id, side, price, size";

const FUNDING_COLUMNS: &[(&str, &str)] = &[
    ("instrument_name", "VARCHAR"),
    ("funding_rate", "DOUBLE"),
    ("funding_time", "BIGINT"),
];
const FUNDING_SELECT: &str =
    "funding_time AS time_ms, make_timestamp(funding_time * 1000) AS ts, funding_rate";

/// One OKX dataset of one instrument.
pub struct Series {
    files_url: String,
    funding: bool,
    instrument: String,
    phoenix: String,
}

/// URL of a period's file; `label` is `YYYY-MM-DD` (trades) or `YYYY-MM` (funding).
#[must_use]
pub fn file_url(files_url: &str, funding: bool, instrument: &str, label: &str) -> String {
    let compact = label.replace('-', "");
    let base = files_url.trim_end_matches('/');
    if funding {
        format!("{base}/swaprates/monthly/{compact}/{instrument}-fundingrates-{label}.zip")
    } else {
        format!("{base}/trades/daily/{compact}/{instrument}-trades-{label}.zip")
    }
}

/// The trades and funding series of every mapped instrument of the walk's kinds.
#[must_use]
pub fn kinds(cfg: &Okx, symbols: &[Symbol], only_symbol: Option<&str>) -> Vec<Arc<dyn Kind>> {
    let mut out: Vec<Arc<dyn Kind>> = Vec::new();
    for funding in [true, false] {
        if (funding && !cfg.funding) || (!funding && !cfg.trades) {
            continue;
        }
        for symbol in symbols {
            let Some(instrument) = symbol.okx.as_deref() else {
                continue;
            };
            let safe = !instrument.is_empty()
                && instrument
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-');
            if !safe
                || !cfg.walk.kinds.contains(&symbol.kind)
                || only_symbol.is_some_and(|s| s != symbol.phoenix && s != instrument)
            {
                continue;
            }
            out.push(Arc::new(Series {
                files_url: cfg.files_url.clone(),
                funding,
                instrument: instrument.to_owned(),
                phoenix: symbol.phoenix.clone(),
            }));
        }
    }
    out
}

impl Kind for Series {
    fn source(&self) -> &str {
        SOURCE
    }
    fn dataset(&self) -> &str {
        if self.funding { "funding" } else { "trades" }
    }
    fn symbol(&self) -> &str {
        &self.instrument
    }
    fn phoenix(&self) -> &str {
        &self.phoenix
    }
    fn granularity(&self) -> Granularity {
        if self.funding {
            Granularity::Month
        } else {
            Granularity::Day
        }
    }
    fn url(&self, label: &str) -> String {
        file_url(&self.files_url, self.funding, &self.instrument, label)
    }
    fn container(&self) -> Container {
        Container::Zip
    }
    fn select(
        &self,
        _store: &mut Store,
        staged: &Path,
        constants: &str,
    ) -> Result<String, StoreError> {
        // Rows start with the instrument name, so only the header name tells a header line apart.
        let header = staged_starts_with(staged, "instrument_name")?;
        let (columns, select, order) = if self.funding {
            (FUNDING_COLUMNS, FUNDING_SELECT, "funding_time")
        } else {
            (TRADE_COLUMNS, TRADE_SELECT, "created_time, trade_id")
        };
        Ok(format!(
            "SELECT {select}, {constants} FROM {} ORDER BY {order}",
            read_csv(staged, columns, header)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let base = "https://static.okx.com/cdn/okex/traderecords/";
        assert_eq!(
            file_url(base, false, "BTC-USDT-SWAP", "2025-10-12"),
            "https://static.okx.com/cdn/okex/traderecords/trades/daily/20251012/BTC-USDT-SWAP-trades-2025-10-12.zip"
        );
        assert_eq!(
            file_url(base, true, "BTC-USDT-SWAP", "2025-10"),
            "https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/202510/BTC-USDT-SWAP-fundingrates-2025-10.zip"
        );
    }
}
