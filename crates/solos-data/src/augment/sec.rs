//! SEC EDGAR filings of the equity perps' issuers: the submissions JSON of each mapped CIK
//! (`data.sec.gov/submissions/CIK##########.json`, with its continuation files when the recent
//! window does not reach the start date) becomes one Parquet file per issuer and month. EDGAR
//! wants a declared User-Agent, `Accept-Encoding: gzip` and at most ten requests a second.

use super::config::{AugmentConfig, Sec, Symbol};
use super::http::Http;
use super::parquet::read_json_array;
use super::periods::Granularity;
use super::series::{Ctx, Outcome, RowsFuture, Series, sync_series};
use crate::jsonout::Obj;
use crate::store::{StoreError, sql_string};
use serde_json::{Value, json};
use std::io::Read;
use std::path::Path;
use tokio::sync::OnceCell;

/// The filings of one issuer.
pub struct Filings {
    /// `data.sec.gov` base.
    pub base_url: String,
    /// Base of the document URLs.
    pub archive_url: String,
    /// The declared User-Agent.
    pub user_agent: String,
    /// Exchange ticker, the directory name.
    pub ticker: String,
    /// Phoenix symbol.
    pub phoenix: String,
    /// CIK as configured, without padding.
    pub cik: String,
    /// `YYYY-MM-DD`: continuation files ending before this day are not fetched.
    pub since: String,
    cache: OnceCell<Vec<Value>>,
}

impl Filings {
    /// The series of one mapped symbol.
    #[must_use]
    pub fn new(cfg: &Sec, symbol: &Symbol, cik: &str, since: &str) -> Filings {
        Filings {
            base_url: cfg.base_url.trim_end_matches('/').to_owned(),
            archive_url: cfg.archive_url.trim_end_matches('/').to_owned(),
            user_agent: cfg.user_agent.clone(),
            ticker: symbol.phoenix.clone(),
            phoenix: symbol.phoenix.clone(),
            cik: cik.to_owned(),
            since: since.to_owned(),
            cache: OnceCell::new(),
        }
    }

    fn headers(&self) -> [(&str, &str); 3] {
        [
            ("user-agent", self.user_agent.as_str()),
            ("accept-encoding", "gzip"),
            ("accept", "application/json"),
        ]
    }

    async fn fetch_json(&self, http: &Http, url: &str) -> Result<Value, StoreError> {
        let fetched = http.get_bytes(url, &self.headers()).await?;
        let body = inflate(fetched.body)?;
        serde_json::from_slice(&body).map_err(|e| StoreError::Check(e.to_string()))
    }

    async fn history(&self, http: &Http) -> Result<&Vec<Value>, StoreError> {
        self.cache
            .get_or_try_init(|| async {
                let url = format!(
                    "{}/submissions/CIK{}.json",
                    self.base_url,
                    padded(&self.cik)
                );
                let doc = self.fetch_json(http, &url).await?;
                if !issuer_lists_ticker(&doc, &self.ticker) {
                    return Err(StoreError::Check(format!(
                        "CIK {} is {} with tickers {}, not {}",
                        self.cik,
                        doc.get("name").and_then(Value::as_str).unwrap_or("?"),
                        doc.get("tickers").cloned().unwrap_or(Value::Null),
                        self.ticker
                    )));
                }
                let filings = doc.get("filings").cloned().unwrap_or(Value::Null);
                let mut rows = flatten(&filings["recent"], &self.cik, &self.archive_url);
                for name in continuation_files(&filings, &rows, &self.since) {
                    let page = self
                        .fetch_json(http, &format!("{}/submissions/{name}", self.base_url))
                        .await?;
                    rows.extend(flatten(&page, &self.cik, &self.archive_url));
                }
                Ok(rows)
            })
            .await
    }
}

/// Ten digits, zero-padded.
#[must_use]
pub fn padded(cik: &str) -> String {
    format!("{cik:0>10}")
}

/// Gunzip a body when it carries the gzip magic, else pass it through.
pub fn inflate(body: Vec<u8>) -> Result<Vec<u8>, StoreError> {
    if body.len() < 2 || body[0] != 0x1f || body[1] != 0x8b {
        return Ok(body);
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(body.as_slice())
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Check(format!("gzip: {e}")))?;
    Ok(out)
}

/// Whether the issuer's `tickers` include ours (case-insensitive): the guard against a wrong CIK.
#[must_use]
pub fn issuer_lists_ticker(doc: &Value, ticker: &str) -> bool {
    doc.get("tickers")
        .and_then(Value::as_array)
        .is_some_and(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .any(|t| t.eq_ignore_ascii_case(ticker))
        })
}

/// Continuation files to fetch: when the recent window's oldest filing date is after `since`,
/// every file whose `filingTo` reaches `since`.
#[must_use]
pub fn continuation_files(filings: &Value, recent: &[Value], since: &str) -> Vec<String> {
    let oldest = recent
        .iter()
        .filter_map(|r| r.get("filing_date").and_then(Value::as_str))
        .min()
        .unwrap_or("");
    if !recent.is_empty() && oldest <= since {
        return Vec::new();
    }
    filings
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|f| f.get("filingTo").and_then(Value::as_str).unwrap_or("") >= since)
        .filter_map(|f| f.get("name").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

fn column<'a>(columns: &'a Value, name: &str) -> &'a [Value] {
    columns
        .get(name)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// The columnar arrays of a submissions page as rows with the acceptance instant in
/// milliseconds and the document URL.
#[must_use]
pub fn flatten(columns: &Value, cik: &str, archive_url: &str) -> Vec<Value> {
    let accessions = column(columns, "accessionNumber");
    let get = |name: &str, i: usize| column(columns, name).get(i).cloned().unwrap_or(Value::Null);
    accessions
        .iter()
        .enumerate()
        .filter_map(|(i, accession)| {
            let accession = accession.as_str()?;
            let acceptance = get("acceptanceDateTime", i);
            let filed_at = chrono::DateTime::parse_from_rfc3339(acceptance.as_str()?)
                .ok()?
                .timestamp_millis();
            let document = get("primaryDocument", i);
            let url = document.as_str().filter(|d| !d.is_empty()).map(|d| {
                format!(
                    "{archive_url}/{}/{}/{d}",
                    cik.trim_start_matches('0'),
                    accession.replace('-', "")
                )
            });
            Some(json!({
                "accession": accession, "filed_at": filed_at, "acceptance": acceptance,
                "filing_date": get("filingDate", i), "report_date": get("reportDate", i),
                "form": get("form", i), "items": get("items", i), "cik": cik,
                "primary_document": document, "primary_doc_description": get("primaryDocDescription", i),
                "size": get("size", i), "is_xbrl": get("isXBRL", i), "url": url,
            }))
        })
        .collect()
}

impl Series for Filings {
    fn source(&self) -> &str {
        "sec"
    }
    fn dataset(&self) -> &str {
        "filings"
    }
    fn symbol(&self) -> &str {
        &self.ticker
    }
    fn phoenix_symbol(&self) -> Option<&str> {
        Some(&self.phoenix)
    }
    fn granularity(&self) -> Granularity {
        Granularity::Month
    }
    fn lag_ms(&self) -> i64 {
        2 * 86_400_000
    }
    fn fetch<'a>(&'a self, http: &'a Http, start_ms: i64, end_ms: i64) -> RowsFuture<'a> {
        Box::pin(async move {
            let history = self.history(http).await?;
            Ok(history
                .iter()
                .filter(|row| {
                    let ms = row.get("filed_at").and_then(Value::as_i64).unwrap_or(0);
                    ms >= start_ms && ms < end_ms
                })
                .cloned()
                .collect())
        })
    }
    fn select(&self, staged: &Path, constants: &str) -> String {
        format!(
            "SELECT filed_at AS filed_at_ms, make_timestamp(filed_at * 1000) AS ts, {} AS ticker, cik, form, items,
                    CAST(filing_date AS DATE) AS filing_date, acceptance, report_date, accession, primary_document,
                    primary_doc_description, size, is_xbrl, url, {constants}
             FROM {} ORDER BY filed_at, accession",
            sql_string(&self.ticker),
            read_json_array(
                staged,
                &[
                    ("accession", "VARCHAR"), ("filed_at", "BIGINT"), ("acceptance", "VARCHAR"), ("filing_date", "VARCHAR"),
                    ("report_date", "VARCHAR"), ("form", "VARCHAR"), ("items", "VARCHAR"), ("cik", "VARCHAR"),
                    ("primary_document", "VARCHAR"), ("primary_doc_description", "VARCHAR"), ("size", "BIGINT"),
                    ("is_xbrl", "BIGINT"), ("url", "VARCHAR")
                ]
            )
        )
    }
}

/// Sync every symbol with a CIK.
pub async fn sync(ctx: &Ctx, config: &AugmentConfig, only_symbol: Option<&str>) -> Outcome {
    let cfg = &config.sources.sec;
    ctx.http
        .set_host_rate(&cfg.base_url, cfg.requests_per_second);
    let mut outcome = Outcome::default();
    for symbol in &config.symbols {
        let Some(cik) = symbol.sec_cik.as_deref() else {
            continue;
        };
        if only_symbol.is_some_and(|s| s != symbol.phoenix) || ctx.stopping() {
            continue;
        }
        let series = Filings::new(cfg, symbol, cik, &config.start_date);
        outcome.add(&sync_series(ctx, &series).await);
    }
    outcome
}

/// `augment sec-map`: look the equity symbols up in EDGAR's `company_tickers.json`, report the
/// mapping, and with `write` store it in the config file as `secCik`.
pub async fn map_ciks(
    config_path: &str,
    config: &AugmentConfig,
    write: bool,
) -> Result<Obj, StoreError> {
    let cfg = &config.sources.sec;
    let http = Http::new(cfg.requests_per_second)?;
    let fetched = http
        .get_bytes(
            &cfg.tickers_url,
            &[
                ("user-agent", cfg.user_agent.as_str()),
                ("accept-encoding", "gzip"),
            ],
        )
        .await?;
    let listing: Value = serde_json::from_slice(&inflate(fetched.body)?)
        .map_err(|e| StoreError::Check(e.to_string()))?;
    let (mapped, unmapped) = apply_listing(&listing, &config.symbols);
    let mut stored = config.symbols.clone();
    for row in &mapped {
        if let (Some(ticker), Some(cik)) = (row.str("ticker"), row.str("cik"))
            && let Some(symbol) = stored.iter_mut().find(|s| s.phoenix == ticker)
        {
            symbol.sec_cik = Some(cik.to_owned());
        }
    }
    if write {
        let text = std::fs::read_to_string(config_path)?;
        let mut raw: AugmentConfig =
            serde_json::from_str(&text).map_err(|e| StoreError::Check(e.to_string()))?;
        raw.symbols = stored;
        super::config::write_augment_config(Path::new(config_path), &raw)?;
    }
    Ok(Obj::new()
        .with("written", write)
        .with_rows("mapped", mapped)
        .with("unmapped", unmapped))
}

/// Match the equity symbols against the listing (`{"0": {"cik_str": 1045810, "ticker":
/// "NVDA", "title": "NVIDIA CORP"}, ...}`).
#[must_use]
pub fn apply_listing(listing: &Value, symbols: &[Symbol]) -> (Vec<Obj>, Vec<String>) {
    let mut mapped = Vec::new();
    let mut unmapped = Vec::new();
    let entries: Vec<&Value> = listing
        .as_object()
        .map(|o| o.values().collect())
        .unwrap_or_default();
    for symbol in symbols.iter().filter(|s| s.kind == "equity") {
        let found = entries.iter().find(|e| {
            e.get("ticker")
                .and_then(Value::as_str)
                .is_some_and(|t| t.eq_ignore_ascii_case(&symbol.phoenix))
        });
        match found {
            Some(entry) => mapped.push(
                Obj::new()
                    .with("ticker", symbol.phoenix.as_str())
                    .with(
                        "cik",
                        entry
                            .get("cik_str")
                            .map(|c| c.to_string().trim_matches('"').to_owned()),
                    )
                    .with("name", entry.get("title").cloned().unwrap_or(Value::Null))
                    .with("previous", symbol.sec_cik.clone()),
            ),
            None => unmapped.push(symbol.phoenix.clone()),
        }
    }
    (mapped, unmapped)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../../../../tests/data/augment/sec-submissions.json");

    #[test]
    fn flattens_pages_and_picks_continuations() {
        let doc: Value = serde_json::from_str(SAMPLE).unwrap();
        assert!(issuer_lists_ticker(&doc, "nvda"));
        assert!(!issuer_lists_ticker(&doc, "TSLA"));
        let rows = flatten(
            &doc["filings"]["recent"],
            "1045810",
            "https://www.sec.gov/Archives/edgar/data",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["form"], "8-K");
        assert_eq!(rows[0]["filed_at"], 1_790_957_112_000_i64);
        assert_eq!(
            rows[0]["url"],
            "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000142/nvda-20261001.htm"
        );
        assert_eq!(
            continuation_files(&doc["filings"], &rows, "2026-01-01"),
            ["CIK0001045810-submissions-001.json"]
        );
        assert!(continuation_files(&doc["filings"], &rows, "2026-09-01").is_empty());
        assert_eq!(padded("1045810"), "0001045810");
        let listing =
            json!({ "0": { "cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP" } });
        let symbols = vec![
            Symbol {
                phoenix: "NVDA".into(),
                asset_id: 1,
                kind: "equity".into(),
                coin_gecko_id: None,
                binance: None,
                hyperliquid: None,
                bybit: None,
                elfa_entity_id: None,
                sec_cik: None,
            },
            Symbol {
                phoenix: "SPY".into(),
                asset_id: 2,
                kind: "equity".into(),
                coin_gecko_id: None,
                binance: None,
                hyperliquid: None,
                bybit: None,
                elfa_entity_id: None,
                sec_cik: None,
            },
            Symbol {
                phoenix: "SOL".into(),
                asset_id: 3,
                kind: "crypto".into(),
                coin_gecko_id: None,
                binance: None,
                hyperliquid: None,
                bybit: None,
                elfa_entity_id: None,
                sec_cik: None,
            },
        ];
        let (mapped, unmapped) = apply_listing(&listing, &symbols);
        assert_eq!(mapped[0].str("cik"), Some("1045810"));
        assert_eq!(unmapped, ["SPY"]);
    }

    #[test]
    fn inflates_gzip_only() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"{\"a\":1}").unwrap();
        let gz = encoder.finish().unwrap();
        assert_eq!(inflate(gz).unwrap(), b"{\"a\":1}");
        assert_eq!(inflate(b"plain".to_vec()).unwrap(), b"plain");
    }
}
