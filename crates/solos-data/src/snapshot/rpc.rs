//! The lane's own JSON-RPC client: finalized `getMultipleAccounts` and Helius's paginated
//! `getProgramAccountsV2`, both with `base64+zstd` account data, spaced to the lane's own
//! requests-per-second ceiling so the collector keeps its budget. Every call is counted with the
//! credits the provider bills for it and the bytes it returned. The URL and provider error bodies
//! are never logged.

use crate::jsonout::Obj;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// One account as the RPC returned it: the provider's zstd frame and the decompressed data.
#[derive(Clone, Debug)]
pub struct Account {
    /// Address.
    pub pubkey: String,
    /// Owner program.
    pub owner: String,
    /// Lamports.
    pub lamports: u64,
    /// The zstd frame exactly as the provider sent it (base64-decoded).
    pub zstd: Vec<u8>,
    /// The account data.
    pub data: Vec<u8>,
}

/// A `getMultipleAccounts` answer: one context slot for every account in it.
#[derive(Clone, Debug)]
pub struct Accounts {
    /// Context slot.
    pub slot: u64,
    /// One entry per requested key, `None` for a missing account.
    pub accounts: Vec<Option<Account>>,
}

/// One `getProgramAccountsV2` page.
#[derive(Clone, Debug)]
pub struct ProgramPage {
    /// The page's context slot (pages of one scan may differ).
    pub slot: u64,
    /// Accounts on the page.
    pub accounts: Vec<Account>,
    /// Cursor for the next page; `None` at the end.
    pub next: Option<String>,
}

/// A failed call; the provider's message is deliberately not kept.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("RPC {method} failed ({reason})")]
pub struct RpcError {
    /// Method.
    pub method: String,
    /// HTTP status, JSON-RPC code or a short local reason.
    pub reason: String,
}

/// Boxed future.
pub type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T, RpcError>> + Send + 'a>>;

/// What the lane needs from a provider; tests implement it over fixtures.
pub trait StateRpc: Send + Sync {
    /// Finalized `getMultipleAccounts` of at most 100 keys.
    fn multiple<'a>(&'a self, keys: &'a [String]) -> Fut<'a, Accounts>;
    /// One finalized page of program accounts whose data starts with `prefix` (base58).
    fn program_page<'a>(
        &'a self,
        program: &'a str,
        prefix: &'a str,
        limit: u32,
        cursor: Option<&'a str>,
    ) -> Fut<'a, ProgramPage>;
    /// Calls, credits and bytes so far.
    fn meter(&self) -> Obj;
}

fn err(method: &str, reason: impl Into<String>) -> RpcError {
    RpcError {
        method: method.to_owned(),
        reason: reason.into(),
    }
}

/// Parse one account object (`{lamports, owner, data: [b64, "base64+zstd"], ...}`).
pub fn parse_account(pubkey: &str, value: &Value, method: &str) -> Result<Account, RpcError> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| err(method, "account data missing"))?;
    let (Some(text), Some(encoding)) = (
        data.first().and_then(Value::as_str),
        data.get(1).and_then(Value::as_str),
    ) else {
        return Err(err(method, "account data malformed"));
    };
    let raw = STANDARD
        .decode(text)
        .map_err(|_| err(method, "account data is not base64"))?;
    let (zstd, data) = match encoding {
        "base64+zstd" => {
            let data = zstd::stream::decode_all(raw.as_slice())
                .map_err(|_| err(method, "account data is not a zstd frame"))?;
            (raw, data)
        }
        "base64" => {
            let frame = zstd::bulk::compress(&raw, 3)
                .map_err(|_| err(method, "account data could not be compressed"))?;
            (frame, raw)
        }
        _ => return Err(err(method, "unexpected account encoding")),
    };
    Ok(Account {
        pubkey: pubkey.to_owned(),
        owner: value
            .get("owner")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        lamports: value.get("lamports").and_then(Value::as_u64).unwrap_or(0),
        zstd,
        data,
    })
}

/// Parse a `getMultipleAccounts` result for `keys`.
pub fn parse_multiple(keys: &[String], result: &Value) -> Result<Accounts, RpcError> {
    let method = "getMultipleAccounts";
    let slot = result
        .pointer("/context/slot")
        .and_then(Value::as_u64)
        .ok_or_else(|| err(method, "context slot missing"))?;
    let values = result
        .get("value")
        .and_then(Value::as_array)
        .ok_or_else(|| err(method, "value missing"))?;
    if values.len() != keys.len() {
        return Err(err(method, "answer length differs from the request"));
    }
    let mut accounts = Vec::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(values) {
        accounts.push(if value.is_null() {
            None
        } else {
            Some(parse_account(key, value, method)?)
        });
    }
    Ok(Accounts { slot, accounts })
}

/// Parse a `getProgramAccountsV2` result requested `withContext`.
pub fn parse_program_page(result: &Value) -> Result<ProgramPage, RpcError> {
    let method = "getProgramAccountsV2";
    let slot = result
        .pointer("/context/slot")
        .and_then(Value::as_u64)
        .ok_or_else(|| err(method, "context slot missing"))?;
    let value = result.get("value").unwrap_or(&Value::Null);
    let list = value
        .get("accounts")
        .and_then(Value::as_array)
        .ok_or_else(|| err(method, "accounts missing"))?;
    let mut accounts = Vec::with_capacity(list.len());
    for entry in list {
        let pubkey = entry
            .get("pubkey")
            .and_then(Value::as_str)
            .ok_or_else(|| err(method, "pubkey missing"))?;
        let account = entry
            .get("account")
            .ok_or_else(|| err(method, "account missing"))?;
        accounts.push(parse_account(pubkey, account, method)?);
    }
    let next = value
        .get("paginationKey")
        .and_then(Value::as_str)
        .filter(|k| !k.is_empty())
        .map(str::to_owned);
    Ok(ProgramPage {
        slot,
        accounts,
        next,
    })
}

#[derive(Default)]
struct Meter {
    calls: BTreeMap<String, u64>,
    credits: u64,
    bytes: u64,
    retries: u64,
    errors: u64,
}

/// The live provider.
pub struct HttpRpc {
    url: String,
    client: reqwest::Client,
    spacing: Duration,
    next_slot: tokio::sync::Mutex<Instant>,
    credits: BTreeMap<String, u64>,
    max_retries: u32,
    meter: Mutex<Meter>,
}

impl HttpRpc {
    /// A client for `url` at `requests_per_second`, billing `credits` per method.
    pub fn new(
        url: String,
        requests_per_second: f64,
        credits: BTreeMap<String, u64>,
        max_retries: u32,
    ) -> Result<Self, RpcError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|_| err("client", "HTTP client could not be built"))?;
        Ok(HttpRpc {
            url,
            client,
            spacing: Duration::from_secs_f64(1.0 / requests_per_second),
            next_slot: tokio::sync::Mutex::new(Instant::now()),
            credits,
            max_retries,
            meter: Mutex::new(Meter::default()),
        })
    }

    async fn pace(&self) {
        let wait = {
            let mut next = self.next_slot.lock().await;
            let now = Instant::now();
            let at = (*next).max(now);
            *next = at + self.spacing;
            at.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let body = body.to_string();
        let mut last = err(method, "no attempt");
        for attempt in 0..self.max_retries {
            self.pace().await;
            {
                let mut meter = self.meter.lock().expect("meter");
                *meter.calls.entry(method.to_owned()).or_default() += 1;
                meter.credits += self.credits.get(method).copied().unwrap_or(1);
                if attempt > 0 {
                    meter.retries += 1;
                }
            }
            let (retryable, retry_after, outcome) = self.attempt(method, &body).await;
            match outcome {
                Ok(value) => return Ok(value),
                Err(error) => {
                    self.meter.lock().expect("meter").errors += 1;
                    last = error;
                    if !retryable {
                        break;
                    }
                }
            }
            let backoff = Duration::from_millis((500u64 << attempt.min(5)).min(16_000));
            tokio::time::sleep(backoff.max(retry_after)).await;
        }
        Err(last)
    }

    /// One HTTP round trip: (retryable, retry-after, result).
    async fn attempt(&self, method: &str, body: &str) -> (bool, Duration, Result<Value, RpcError>) {
        let response = match self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => return (true, Duration::ZERO, Err(err(method, "transport"))),
        };
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map_or(Duration::ZERO, Duration::from_secs);
        let text = match response.text().await {
            Ok(text) => text,
            Err(_) => return (true, retry_after, Err(err(method, "body read"))),
        };
        self.meter.lock().expect("meter").bytes += text.len() as u64;
        if !(200..300).contains(&status) {
            let retryable = status == 429 || status >= 500;
            return (
                retryable,
                retry_after,
                Err(err(method, format!("HTTP {status}"))),
            );
        }
        let Ok(envelope) = serde_json::from_str::<Value>(&text) else {
            return (true, retry_after, Err(err(method, "invalid JSON")));
        };
        if let Some(error) = envelope.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-1);
            let retryable = ![-32600, -32601, -32602, -32015].contains(&code);
            return (
                retryable,
                retry_after,
                Err(err(method, format!("code {code}"))),
            );
        }
        match envelope.get("result") {
            Some(result) => (false, Duration::ZERO, Ok(result.clone())),
            None => (true, retry_after, Err(err(method, "result missing"))),
        }
    }
}

impl StateRpc for HttpRpc {
    fn multiple<'a>(&'a self, keys: &'a [String]) -> Fut<'a, Accounts> {
        Box::pin(async move {
            let result = self
                .call(
                    "getMultipleAccounts",
                    json!([keys, { "encoding": "base64+zstd", "commitment": "finalized" }]),
                )
                .await?;
            parse_multiple(keys, &result)
        })
    }

    fn program_page<'a>(
        &'a self,
        program: &'a str,
        prefix: &'a str,
        limit: u32,
        cursor: Option<&'a str>,
    ) -> Fut<'a, ProgramPage> {
        Box::pin(async move {
            let mut options = json!({
                "encoding": "base64+zstd",
                "commitment": "finalized",
                "withContext": true,
                "limit": limit,
                "filters": [{ "memcmp": { "offset": 0, "bytes": prefix } }],
            });
            if let Some(cursor) = cursor {
                options["paginationKey"] = json!(cursor);
            }
            let result = self
                .call("getProgramAccountsV2", json!([program, options]))
                .await?;
            parse_program_page(&result)
        })
    }

    fn meter(&self) -> Obj {
        let meter = self.meter.lock().expect("meter");
        let mut calls = Obj::new();
        for (method, count) in &meter.calls {
            calls.set(method, *count);
        }
        Obj::new()
            .with_obj("calls", calls)
            .with("credits", meter.credits)
            .with("bytes", meter.bytes)
            .with("retries", meter.retries)
            .with("errors", meter.errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(data: &[u8]) -> String {
        STANDARD.encode(zstd::bulk::compress(data, 3).unwrap())
    }

    #[test]
    fn multiple_accounts_decode_their_zstd_frames_and_keep_them() {
        let keys = vec!["A".to_owned(), "B".to_owned()];
        let result = json!({ "context": { "slot": 42 }, "value": [
            { "lamports": 7, "owner": "P", "data": [encoded(b"hello"), "base64+zstd"], "executable": false },
            null
        ]});
        let parsed = parse_multiple(&keys, &result).unwrap();
        assert_eq!(parsed.slot, 42);
        let account = parsed.accounts[0].as_ref().unwrap();
        assert_eq!(account.data, b"hello");
        assert_eq!(
            zstd::stream::decode_all(account.zstd.as_slice()).unwrap(),
            b"hello"
        );
        assert_eq!(account.owner, "P");
        assert!(parsed.accounts[1].is_none());
        assert!(parse_multiple(&keys[..1], &result).is_err());
    }

    #[test]
    fn program_pages_carry_their_slot_and_cursor() {
        let result = json!({ "context": { "slot": 9 }, "value": { "accounts": [
            { "pubkey": "T1", "account": { "lamports": 1, "owner": "P", "data": [encoded(&[1, 2, 3]), "base64+zstd"] } }
        ], "paginationKey": "next" }});
        let page = parse_program_page(&result).unwrap();
        assert_eq!((page.slot, page.next.as_deref()), (9, Some("next")));
        assert_eq!(page.accounts[0].data, [1, 2, 3]);
        let last =
            json!({ "context": { "slot": 10 }, "value": { "accounts": [], "paginationKey": null }});
        assert!(parse_program_page(&last).unwrap().next.is_none());
    }

    #[test]
    fn plain_base64_is_accepted_and_compressed_for_storage() {
        let value =
            json!({ "lamports": 1, "owner": "P", "data": [STANDARD.encode([5u8; 64]), "base64"] });
        let account = parse_account("K", &value, "m").unwrap();
        assert_eq!(account.data, [5u8; 64]);
        assert_eq!(
            zstd::stream::decode_all(account.zstd.as_slice()).unwrap(),
            [5u8; 64]
        );
    }
}
