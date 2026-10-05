//! The JSON-RPC provider: one CU bucket, per-method counters, retry and backoff rules identical
//! to `rpc.ts`, and the raw response text kept for `getTransaction` and
//! `getTransactionsForAddress` so the archive stores exactly what the provider sent.

use super::config::{Config, Lane};
use super::limiter::Limiter;
use crate::jsonout::Obj;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A failed RPC call, with the provider's message deliberately absent.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("RPC {method} failed (code {code})")]
pub struct RpcFailure {
    /// The method.
    pub method: String,
    /// HTTP status or JSON-RPC error code; -1 when unknown.
    pub code: i64,
    /// Whether the provider signalled a throughput limit.
    pub throttled: bool,
}

/// A response to one RPC call: the parsed `result` and the raw body text.
#[derive(Debug, Clone)]
pub struct RpcResponse {
    /// `result`, possibly `null`.
    pub result: Value,
    /// The exact body the provider sent.
    pub raw: String,
}

/// Boxed future of an RPC call.
pub type RpcFuture<'a> = Pin<Box<dyn Future<Output = Result<RpcResponse, RpcFailure>> + Send + 'a>>;

/// What the collector needs from a provider; fixtures implement it in tests.
pub trait Rpc: Send + Sync {
    /// Call `method` with `params` on `lane`.
    fn call<'a>(&'a self, method: &'a str, params: Value, lane: Lane) -> RpcFuture<'a>;
    /// Provider name recorded on rows.
    fn provider_name(&self) -> &str;
}

/// What an HTTP POST returned.
#[derive(Debug, Clone)]
pub struct HttpReply {
    /// Status code.
    pub status: u16,
    /// `retry-after` header, if any.
    pub retry_after: Option<String>,
    /// Body text.
    pub body: String,
}

/// HTTP transport, replaceable in tests.
pub trait Transport: Send + Sync {
    /// POST `body` as JSON to `url`.
    fn post<'a>(
        &'a self,
        url: &'a str,
        body: String,
    ) -> Pin<Box<dyn Future<Output = Result<HttpReply, String>> + Send + 'a>>;
}

/// `reqwest` transport with a 30 s timeout.
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestTransport {
    /// New client.
    #[must_use]
    pub fn new() -> Self {
        ReqwestTransport {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("http client"),
        }
    }
}

impl Transport for ReqwestTransport {
    fn post<'a>(
        &'a self,
        url: &'a str,
        body: String,
    ) -> Pin<Box<dyn Future<Output = Result<HttpReply, String>> + Send + 'a>> {
        Box::pin(async move {
            let response = self
                .client
                .post(url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = response.text().await.map_err(|e| e.to_string())?;
            Ok(HttpReply {
                status,
                retry_after,
                body,
            })
        })
    }
}

/// Counters as the TypeScript provider kept them, in first-seen order.
#[derive(Default)]
pub struct Counters {
    entries: Vec<(String, f64)>,
}

impl Counters {
    fn add(&mut self, key: &str, by: f64) {
        match self.entries.iter_mut().find(|(k, _)| k == key) {
            Some(entry) => entry.1 += by,
            None => self.entries.push((key.to_owned(), by)),
        }
    }

    /// One counter.
    #[must_use]
    pub fn get(&self, key: &str) -> f64 {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map_or(0.0, |(_, v)| *v)
    }

    /// As an ordered JSON object; integral values render as integers.
    #[must_use]
    pub fn to_obj(&self) -> Obj {
        let mut obj = Obj::new();
        for (key, value) in &self.entries {
            if value.fract() == 0.0 && value.abs() < 9.0e15 {
                obj.set(key, *value as i64);
            } else {
                obj.set(key, *value);
            }
        }
        obj
    }
}

/// The live provider.
pub struct Provider {
    url: String,
    config: Config,
    /// The shared limiter.
    pub limiter: Arc<Limiter>,
    name: String,
    /// Counters behind a mutex.
    pub counters: std::sync::Mutex<Counters>,
    sequence: AtomicU64,
    /// Set to stop every lane.
    pub shutdown: Arc<AtomicBool>,
    transport: Box<dyn Transport>,
}

impl Provider {
    /// New provider over `reqwest`.
    #[must_use]
    pub fn new(url: String, config: Config, limiter: Arc<Limiter>) -> Self {
        Self::with_transport(url, config, limiter, Box::new(ReqwestTransport::new()))
    }

    /// New provider over a custom transport (tests).
    #[must_use]
    pub fn with_transport(
        url: String,
        config: Config,
        limiter: Arc<Limiter>,
        transport: Box<dyn Transport>,
    ) -> Self {
        Provider {
            url,
            config,
            limiter,
            name: "alchemy".into(),
            counters: std::sync::Mutex::new(Counters::default()),
            sequence: AtomicU64::new(0),
            shutdown: Arc::new(AtomicBool::new(false)),
            transport,
        }
    }

    fn count(&self, key: &str, by: f64) {
        self.counters.lock().expect("counters").add(key, by);
    }

    /// Counters snapshot.
    #[must_use]
    pub fn counters(&self) -> Obj {
        self.counters.lock().expect("counters").to_obj()
    }

    async fn call_inner(
        &self,
        method: &str,
        params: Value,
        lane: Lane,
    ) -> Result<RpcResponse, RpcFailure> {
        let failure = |code: i64, throttled: bool| RpcFailure {
            method: method.to_owned(),
            code,
            throttled,
        };
        let Some(&weight) = self.config.cu_weights.get(method) else {
            return Err(failure(-1, false));
        };
        for attempt in 0..self.config.max_retries {
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(failure(-1, false));
            }
            let queued = Instant::now();
            self.limiter
                .acquire(lane, weight, &self.shutdown)
                .await
                .map_err(|_| failure(-1, false))?;
            let started = Instant::now();
            let prefix = format!("{}_{}", lane.as_str(), method);
            self.count(
                &format!("{prefix}_wait_ms"),
                started.duration_since(queued).as_secs_f64() * 1000.0,
            );
            self.count("requests", 1.0);
            self.count("cu", f64::from(weight));
            self.count(&format!("{}_cu", lane.as_str()), f64::from(weight));
            self.count(&format!("{prefix}_requests"), 1.0);
            let id = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let body = serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
            let outcome = self.transport.post(&self.url, body).await;
            let mut throttled = false;
            let mut retry_ms: u64 = 0;
            let result: Result<RpcResponse, RpcFailure> = match outcome {
                Err(_) => Err(failure(-1, false)),
                Ok(reply) => {
                    retry_ms = reply
                        .retry_after
                        .as_deref()
                        .map(retry_after_ms)
                        .unwrap_or(0);
                    if !(200..300).contains(&reply.status) {
                        Err(failure(i64::from(reply.status), reply.status == 429))
                    } else {
                        match serde_json::from_str::<Value>(&reply.body) {
                            Err(_) => Err(failure(-1, false)),
                            Ok(envelope) => {
                                if let Some(error) = envelope.get("error") {
                                    let code =
                                        error.get("code").and_then(Value::as_i64).unwrap_or(-1);
                                    let message = error
                                        .get("message")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_lowercase();
                                    let limited = code == 429
                                        || (code == -32005
                                            && (message.contains("throughput")
                                                || message.contains("rate limit")
                                                || message.contains("rate-limit")
                                                || message.contains("too many")));
                                    Err(failure(code, limited))
                                } else if let Some(result) = envelope.get("result") {
                                    if result.is_null() {
                                        self.count("nulls", 1.0);
                                    }
                                    Ok(RpcResponse {
                                        result: result.clone(),
                                        raw: reply.body,
                                    })
                                } else {
                                    Err(failure(-1, false))
                                }
                            }
                        }
                    }
                }
            };
            self.count(
                &format!("{prefix}_duration_ms"),
                started.elapsed().as_secs_f64() * 1000.0,
            );
            match result {
                Ok(response) => {
                    self.limiter.release(lane, false, 0);
                    return Ok(response);
                }
                Err(error) => {
                    throttled = throttled || error.throttled;
                    self.limiter.release(lane, throttled, retry_ms);
                    if self.shutdown.load(Ordering::Relaxed) {
                        return Err(failure(-1, false));
                    }
                    self.count(&format!("error_code_{}", error.code), 1.0);
                    self.count("errors", 1.0);
                    if throttled {
                        self.count("throttled", 1.0);
                    }
                    if [400, 401, 403, -32601, -32602, -32015].contains(&error.code)
                        || attempt + 1 == self.config.max_retries
                    {
                        return Err(failure(error.code, false));
                    }
                    let jitter = jitter_ms((500.0 * 2f64.powi(attempt as i32)).min(32_000.0));
                    tokio::time::sleep(Duration::from_millis(retry_ms.max(jitter))).await;
                }
            }
        }
        Err(failure(-1, false))
    }
}

impl Rpc for Provider {
    fn call<'a>(&'a self, method: &'a str, params: Value, lane: Lane) -> RpcFuture<'a> {
        Box::pin(self.call_inner(method, params, lane))
    }

    fn provider_name(&self) -> &str {
        &self.name
    }
}

/// `retry-after` seconds or HTTP date → milliseconds.
fn retry_after_ms(header: &str) -> u64 {
    if let Ok(seconds) = header.trim().parse::<u64>() {
        return seconds.saturating_mul(1000);
    }
    chrono::DateTime::parse_from_rfc2822(header.trim())
        .ok()
        .map(|at| (at.timestamp_millis() - chrono::Utc::now().timestamp_millis()).max(0) as u64)
        .unwrap_or(0)
}

/// `Math.random() * max` without a dependency.
fn jitter_ms(max: f64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let unit = f64::from(nanos % 1_000_000) / 1_000_000.0;
    (unit * max) as u64
}

/// Convenience: call and take the result value.
pub async fn call_value(
    rpc: &dyn Rpc,
    method: &str,
    params: Value,
    lane: Lane,
) -> Result<Value, RpcFailure> {
    Ok(rpc.call(method, params, lane).await?.result)
}
