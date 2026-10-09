//! The one HTTP client of the lane: a per-host request spacing (`requestsPerSecond`), bounded
//! retries with exponential backoff on 429, 5xx and transport errors, `Retry-After` honoured,
//! and error text that never carries a URL. Ordinary requests carry a whole-request deadline;
//! a server-sent event stream is opened on a second client without one, because that deadline
//! also covers reading the body and would cut every long-lived stream at the same instant.

use crate::jsonout::{Obj, log, safe_error};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Why a request failed after its retries.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// The server answered 404.
    #[error("not found")]
    NotFound,
    /// A final non-success status.
    #[error("HTTP {0}")]
    Status(u16),
    /// Network or protocol failure.
    #[error("{0}")]
    Transport(String),
    /// The body was not what the caller expected.
    #[error("{0}")]
    Decode(String),
}

impl From<HttpError> for crate::store::StoreError {
    fn from(error: HttpError) -> Self {
        crate::store::StoreError::Check(error.to_string())
    }
}

/// How the lane identifies itself to every host.
pub const USER_AGENT: &str = "solos-data/1.0 (+https://github.com/GuiBibeau/solos-data)";

/// A successful response.
pub struct Fetched {
    /// Status code (2xx).
    pub status: u16,
    /// Body bytes.
    pub body: Vec<u8>,
    /// Response headers, names lower-cased.
    pub headers: Vec<(String, String)>,
}

impl Fetched {
    /// One response header.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Whole-request deadline of ordinary requests (head and body).
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The shared client.
pub struct Http {
    client: reqwest::Client,
    stream_client: reqwest::Client,
    request_timeout: Duration,
    interval: Duration,
    host_intervals: Mutex<HashMap<String, Duration>>,
    next_slot: Mutex<HashMap<String, Instant>>,
    attempts: u32,
}

impl Http {
    /// A client spacing requests to each host at `requests_per_second`.
    pub fn new(requests_per_second: f64) -> Result<Http, HttpError> {
        Http::with_timeout(requests_per_second, REQUEST_TIMEOUT)
    }

    /// The same with another whole-request deadline (tests shorten it). It bounds every
    /// ordinary request and the response head of a stream, never a stream's body.
    pub fn with_timeout(requests_per_second: f64, timeout: Duration) -> Result<Http, HttpError> {
        let build = |builder: reqwest::ClientBuilder| {
            builder
                .connect_timeout(Duration::from_secs(20))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| HttpError::Transport(safe_error(&e.to_string())))
        };
        Ok(Http {
            client: build(reqwest::Client::builder().timeout(timeout))?,
            stream_client: build(reqwest::Client::builder())?,
            request_timeout: timeout,
            interval: Duration::from_secs_f64(1.0 / requests_per_second.max(0.01)),
            host_intervals: Mutex::new(HashMap::new()),
            next_slot: Mutex::new(HashMap::new()),
            attempts: 6,
        })
    }

    /// A slower (or faster) pace for one host, from a URL on that host.
    pub fn set_host_rate(&self, url: &str, requests_per_second: f64) {
        self.host_intervals.lock().expect("host intervals").insert(
            host_of(url).to_owned(),
            Duration::from_secs_f64(1.0 / requests_per_second.max(0.01)),
        );
    }

    /// Wait for this host's next slot and claim the one after it.
    async fn acquire(&self, url: &str) {
        let host = host_of(url).to_owned();
        let interval = self
            .host_intervals
            .lock()
            .expect("host intervals")
            .get(&host)
            .copied()
            .unwrap_or(self.interval);
        let wait = {
            let mut slots = self.next_slot.lock().expect("http slots");
            let now = Instant::now();
            let slot = slots.entry(host).or_insert(now);
            let at = (*slot).max(now);
            *slot = at + interval;
            at.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    /// GET bytes.
    pub async fn get_bytes(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Fetched, HttpError> {
        self.send(url, headers, None).await
    }

    /// GET JSON.
    pub async fn get_json(&self, url: &str, headers: &[(&str, &str)]) -> Result<Value, HttpError> {
        let fetched = self.send(url, headers, None).await?;
        serde_json::from_slice(&fetched.body).map_err(|e| HttpError::Decode(e.to_string()))
    }

    /// POST a JSON body, keep the answer's bytes and headers.
    pub async fn post(
        &self,
        url: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Result<Fetched, HttpError> {
        self.send(url, headers, Some(body)).await
    }

    /// POST a JSON body, parse a JSON answer.
    pub async fn post_json(
        &self,
        url: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Result<Value, HttpError> {
        let fetched = self.send(url, headers, Some(body)).await?;
        serde_json::from_slice(&fetched.body).map_err(|e| HttpError::Decode(e.to_string()))
    }

    /// Open a GET whose body is read by the caller (a server-sent event stream): the same
    /// spacing and retries up to the response head, then the response itself. The head is
    /// bounded by the request deadline; the body has no deadline, the caller bounds idleness.
    pub async fn open(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<reqwest::Response, HttpError> {
        let mut backoff = Duration::from_secs(1);
        for attempt in 1..=self.attempts {
            self.acquire(url).await;
            let mut request = self.stream_client.get(url);
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            let (reason, retry_after) =
                match tokio::time::timeout(self.request_timeout, request.send()).await {
                    Ok(Ok(response)) => match classify(&response) {
                        Ok(()) => return Ok(response),
                        Err(Retry::Fatal(error)) => return Err(error),
                        Err(Retry::Later(reason, retry_after)) => (reason, retry_after),
                    },
                    Ok(Err(e)) => (safe_error(&e.to_string()), None),
                    Err(_) => ("response head timed out".to_owned(), None),
                };
            if attempt == self.attempts {
                return Err(final_error(reason));
            }
            let wait = retry_after.unwrap_or(backoff);
            log(
                "augment_http_retry",
                Obj::new()
                    .with("host", host_of(url))
                    .with("attempt", attempt)
                    .with("reason", reason)
                    .with("waitSeconds", wait.as_secs()),
            );
            tokio::time::sleep(wait).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
        Err(HttpError::Transport("retries exhausted".into()))
    }

    async fn send(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Fetched, HttpError> {
        let mut backoff = Duration::from_secs(1);
        for attempt in 1..=self.attempts {
            self.acquire(url).await;
            let (reason, retry_after) = match self.attempt(url, headers, body).await {
                Ok(fetched) => return Ok(fetched),
                Err(Retry::Fatal(error)) => return Err(error),
                Err(Retry::Later(reason, retry_after)) => (reason, retry_after),
            };
            if attempt == self.attempts {
                return Err(final_error(reason));
            }
            let wait = retry_after.unwrap_or(backoff);
            log(
                "augment_http_retry",
                Obj::new()
                    .with("host", host_of(url))
                    .with("attempt", attempt)
                    .with("reason", reason)
                    .with("waitSeconds", wait.as_secs()),
            );
            tokio::time::sleep(wait).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
        Err(HttpError::Transport("retries exhausted".into()))
    }

    /// One request: success, a final failure, or a reason to retry.
    async fn attempt(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Fetched, Retry> {
        let mut request = match body {
            Some(body) => self.client.post(url).json(body),
            None => self.client.get(url),
        };
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Retry::Later(safe_error(&e.to_string()), None))?;
        classify(&response)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned())))
            .collect();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| Retry::Later(safe_error(&e.to_string()), None))?;
        Ok(Fetched {
            status,
            body: bytes.to_vec(),
            headers,
        })
    }
}

/// A response head: success, a final failure, or a reason to retry.
fn classify(response: &reqwest::Response) -> Result<(), Retry> {
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(());
    }
    if status == 404 {
        return Err(Retry::Fatal(HttpError::NotFound));
    }
    if status == 429 || status >= 500 {
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|s| Duration::from_secs(s.min(300)));
        return Err(Retry::Later(format!("HTTP {status}"), retry_after));
    }
    Err(Retry::Fatal(HttpError::Status(status)))
}

fn final_error(reason: String) -> HttpError {
    match reason.strip_prefix("HTTP ") {
        Some(status) => HttpError::Status(status.parse().unwrap_or(0)),
        None => HttpError::Transport(reason),
    }
}

enum Retry {
    Fatal(HttpError),
    Later(String, Option<Duration>),
}

/// The host part of a URL (for the limiter and for logs).
#[must_use]
pub fn host_of(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split(['/', '?', '#']).next().unwrap_or("")
}

/// Append query pairs to a base path.
#[must_use]
pub fn with_query(base: &str, pairs: &[(&str, String)]) -> String {
    let mut out = String::from(base);
    for (i, (name, value)) in pairs.iter().enumerate() {
        out.push(if i == 0 { '?' } else { '&' });
        out.push_str(name);
        out.push('=');
        out.push_str(&encode(value));
    }
    out
}

fn encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_and_queries() {
        assert_eq!(
            host_of("https://api.example.com/v1/x?y=1"),
            "api.example.com"
        );
        assert_eq!(host_of("http://127.0.0.1:8080"), "127.0.0.1:8080");
        assert_eq!(
            with_query("https://h/p", &[("a", "1 2".into()), ("b", "x:y".into())]),
            "https://h/p?a=1%202&b=x%3Ay"
        );
    }
}
