//! Venue state on a cadence: the configured and discovered accounts read with
//! `getMultipleAccounts` (100 a request, finalized), decoded through the venue's IDL. A row is
//! written when an account's bytes changed since the previous poll, and at least once an hour
//! per account, so "the state as of t" is the newest row at or before t (ADR-0012). Discovery
//! lists the configured account types with `getProgramAccounts` and a discriminator filter,
//! without data.

use super::config::Account;
use super::idl::Idl;
use super::rpc::call;
use crate::collector::rpc::Rpc;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;

/// A heartbeat row per account at least this often, milliseconds.
pub const HEARTBEAT_MS: i64 = 3_600_000;

/// One account as read.
#[derive(Debug, Clone)]
pub struct Read {
    /// Address.
    pub address: String,
    /// Owner program.
    pub owner: String,
    /// Data bytes.
    pub data: Vec<u8>,
}

/// Read accounts in requests of 100; returns the context slot and the accounts that exist.
///
/// # Errors
///
/// RPC failure or a malformed answer.
pub async fn read(rpc: &dyn Rpc, addresses: &[String]) -> Result<(i64, Vec<Read>), String> {
    let mut slot = 0;
    let mut out = Vec::new();
    for chunk in addresses.chunks(100) {
        let params = json!([chunk, { "commitment": "finalized", "encoding": "base64" }]);
        let reply = call(rpc, "getMultipleAccounts", params)
            .await
            .map_err(|e| e.to_string())?;
        slot = slot.max(
            reply
                .result
                .pointer("/context/slot")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        );
        let Some(values) = reply.result.get("value").and_then(Value::as_array) else {
            return Err("getMultipleAccounts answered no list".into());
        };
        for (address, value) in chunk.iter().zip(values) {
            if value.is_null() {
                continue;
            }
            let data = value
                .pointer("/data/0")
                .and_then(Value::as_str)
                .unwrap_or("");
            out.push(Read {
                address: address.clone(),
                owner: value
                    .get("owner")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                data: STANDARD
                    .decode(data)
                    .map_err(|_| "account data is not base64")?,
            });
        }
    }
    Ok((slot, out))
}

/// The addresses of a program's accounts of one IDL type.
///
/// # Errors
///
/// An unknown type or an RPC failure.
pub async fn discover(
    rpc: &dyn Rpc,
    program: &str,
    idl: &Idl,
    type_name: &str,
) -> Result<Vec<String>, String> {
    let disc = idl
        .account_discriminator(type_name)
        .ok_or_else(|| format!("the IDL defines no account {type_name}"))?;
    let params = json!([program, {
        "commitment": "finalized", "encoding": "base64", "dataSlice": { "offset": 0, "length": 0 },
        "filters": [{ "memcmp": { "offset": 0, "bytes": bs58::encode(disc).into_string() } }]
    }]);
    let reply = call(rpc, "getProgramAccounts", params)
        .await
        .map_err(|e| e.to_string())?;
    let list = reply
        .result
        .as_array()
        .ok_or("getProgramAccounts answered no list")?;
    let mut out: Vec<String> = list
        .iter()
        .filter_map(|item| {
            item.get("pubkey")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    out.sort();
    Ok(out)
}

/// What the previous poll saw per account: hash of the bytes and when a row was last written.
#[derive(Default)]
pub struct Seen {
    inner: Mutex<HashMap<String, (String, i64)>>,
}

impl Seen {
    /// Whether a row is due, recording it when it is.
    pub fn due(&self, address: &str, data: &[u8], at_ms: i64) -> (bool, bool) {
        let hash = crate::fsutil::sha256_hex(data);
        let mut guard = self.inner.lock().expect("seen accounts");
        let (changed, stale) = match guard.get(address) {
            Some((last, written)) => (*last != hash, at_ms - written >= HEARTBEAT_MS),
            None => (true, true),
        };
        if changed || stale {
            guard.insert(address.to_owned(), (hash, at_ms));
        }
        (changed || stale, changed)
    }
}

/// One `accounts` row.
#[must_use]
pub fn row(
    idl: &Idl,
    account: &Account,
    read: &Read,
    at_ms: i64,
    slot: i64,
    changed: bool,
) -> Value {
    let (name, fields, error) = match idl.decode_account(&read.data) {
        Some((name, Ok((fields, _)))) => (Some(name), Some(fields), None),
        Some((name, Err(error))) => (Some(name), None, Some(error.to_string())),
        None => (None, None, Some("unknown account discriminator".to_owned())),
    };
    json!({
        "at_ms": at_ms, "context_slot": slot, "account": read.address, "kind": account.kind,
        "label": account.label, "owner": read.owner, "type": name, "data_b64": STANDARD.encode(&read.data),
        "fields": fields, "decode_error": error, "changed": changed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_on_change_and_hourly() {
        let seen = Seen::default();
        assert_eq!(seen.due("a", b"x", 0), (true, true));
        assert_eq!(seen.due("a", b"x", 60_000), (false, false));
        assert_eq!(seen.due("a", b"y", 120_000), (true, true));
        assert_eq!(seen.due("a", b"y", 120_000 + HEARTBEAT_MS), (true, false));
    }
}
