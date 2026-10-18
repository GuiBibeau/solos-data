//! What a venue's transaction says: its own instructions (top level and CPI) with their accounts,
//! and the Anchor events it emitted, either as a self-CPI carrying `EVENT_IX_TAG` (`emit_cpi!`)
//! or as a `Program data:` log line attributed to the program by the invoke stack (`emit!`).
//! Every payload is kept as bytes; the IDL names and decodes what it can, and a payload it
//! cannot decode keeps its reason (ADR-0012).

use super::idl::{EVENT_IX_TAG, Idl};
use crate::decoder::extract::advance_path;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

/// One instruction of the transaction with its resolved program and accounts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ix {
    /// Program address.
    pub program: String,
    /// Stack path, `top.cpi.cpi`.
    pub path: String,
    /// Instruction data.
    pub data: Vec<u8>,
    /// Account addresses in instruction order.
    pub accounts: Vec<String>,
}

/// Where a transaction was read from and when it landed.
#[derive(Clone, Debug)]
pub struct TxMeta<'a> {
    /// Signature.
    pub signature: &'a str,
    /// Slot.
    pub slot: i64,
    /// Block time, seconds, when the provider gave one.
    pub block_time: Option<i64>,
}

fn strings(meta: &Value, key: &str) -> Vec<String> {
    meta.get("loadedAddresses")
        .and_then(|l| l.get(key))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Every instruction of a transaction, top level then each one's CPIs, from the base64 wire
/// and the RPC `meta` (inner instructions and loaded addresses).
///
/// # Errors
///
/// When the wire does not parse or an index does not resolve.
pub fn flatten(tx_base64: &str, meta: &Value) -> Result<Vec<Ix>, String> {
    let wire = solana_wire::parse_base64(tx_base64).map_err(|e| e.to_string())?;
    let addresses = solana_wire::resolve_accounts(
        &wire.static_accounts(),
        &strings(meta, "writable"),
        &strings(meta, "readonly"),
    );
    let resolve = |index: usize| {
        addresses
            .get(index)
            .cloned()
            .ok_or("unresolved account index")
    };
    let mut out = Vec::new();
    let inner_groups = meta.get("innerInstructions").and_then(Value::as_array);
    for (i, ix) in wire.instructions().iter().enumerate() {
        out.push(Ix {
            program: resolve(usize::from(ix.program_id_index))?,
            path: i.to_string(),
            data: ix.data.clone(),
            accounts: ix
                .accounts
                .iter()
                .map(|a| resolve(usize::from(*a)))
                .collect::<Result<_, _>>()?,
        });
        let inner = inner_groups
            .and_then(|groups| {
                groups
                    .iter()
                    .find(|g| g.get("index").and_then(Value::as_u64) == Some(i as u64))
            })
            .and_then(|g| g.get("instructions"))
            .and_then(Value::as_array);
        let mut path: Vec<usize> = Vec::new();
        for item in inner.into_iter().flatten() {
            let index = item.get("programIdIndex").and_then(Value::as_u64);
            let data = item.get("data").and_then(Value::as_str);
            let (Some(index), Some(data)) = (index, data) else {
                return Err("unresolved CPI instruction".into());
            };
            let height = item
                .get("stackHeight")
                .and_then(Value::as_i64)
                .filter(|h| *h >= 2);
            path = advance_path(&path, height.unwrap_or(2))?;
            let accounts = item
                .get("accounts")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .map(|a| {
                            a.as_u64()
                                .ok_or("unresolved account index")
                                .and_then(|a| resolve(usize::try_from(a).unwrap_or(usize::MAX)))
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            let mut full = vec![i];
            full.extend(&path);
            out.push(Ix {
                program: resolve(usize::try_from(index).unwrap_or(usize::MAX))?,
                path: full
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("."),
                data: bs58::decode(data)
                    .into_vec()
                    .map_err(|_| "CPI data is not base58")?,
                accounts,
            });
        }
    }
    Ok(out)
}

/// The `Program data:` payloads `program` logged, in order, attributed by the invoke stack.
#[must_use]
pub fn log_payloads(meta: &Value, program: &str) -> Vec<Vec<u8>> {
    let mut stack: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    let lines = meta.get("logMessages").and_then(Value::as_array);
    for line in lines.into_iter().flatten().filter_map(Value::as_str) {
        if let Some(rest) = line.strip_prefix("Program data: ") {
            if stack.last() == Some(&program) {
                // Anchor writes one base64 chunk; `sol_log_data` may write several.
                let mut bytes = Vec::new();
                for chunk in rest.split(' ') {
                    if let Ok(decoded) = STANDARD.decode(chunk) {
                        bytes.extend(decoded);
                    }
                }
                out.push(bytes);
            }
            continue;
        }
        let Some(rest) = line.strip_prefix("Program ") else {
            continue;
        };
        let mut words = rest.split(' ');
        let (Some(id), Some(verb)) = (words.next(), words.next()) else {
            continue;
        };
        match verb {
            "invoke" => stack.push(id),
            // A failure reads `Program <id> failed: <reason>`.
            verb if (verb == "success" || verb.starts_with("failed"))
                && stack.last() == Some(&id) =>
            {
                stack.pop();
            }
            _ => {}
        }
    }
    out
}

/// The rows one transaction contributes: the program's instructions and its events.
#[derive(Debug, Default)]
pub struct Rows {
    /// `instructions` rows.
    pub instructions: Vec<Value>,
    /// `events` rows.
    pub events: Vec<Value>,
    /// Events the IDL names but could not decode.
    pub undecoded: u64,
    /// Event payloads with a discriminator the IDL does not list.
    pub unknown: u64,
}

fn event_row(
    idl: &Idl,
    tx: &TxMeta<'_>,
    ordinal: usize,
    via: &str,
    path: Option<&str>,
    payload: &[u8],
) -> (Value, bool, bool) {
    let disc = hex::encode(&payload[..payload.len().min(8)]);
    let (name, fields, error) = match idl.decode_event(payload) {
        Some((name, Ok(fields))) => (Some(name), Some(fields), None),
        Some((name, Err(error))) => (Some(name), None, Some(error.to_string())),
        None => (None, None, None),
    };
    let known = name.is_some();
    let decoded = fields.is_some();
    let row = json!({
        "signature": tx.signature, "slot": tx.slot, "block_time": tx.block_time,
        "ordinal": ordinal, "via": via, "path": path, "discriminator": disc, "name": name,
        "payload_b64": STANDARD.encode(payload), "fields": fields, "decode_error": error,
    });
    (row, known, decoded)
}

/// Extract the program's instructions and events from one transaction.
///
/// # Errors
///
/// When the wire or its instruction indices do not resolve.
pub fn extract(
    tx_base64: &str,
    meta: &Value,
    program: &str,
    idl: &Idl,
    tx: &TxMeta<'_>,
) -> Result<Rows, String> {
    let mut rows = Rows::default();
    let mut ordinal = 0;
    let count = |rows: &mut Rows, known: bool, decoded: bool| {
        if !known {
            rows.unknown += 1;
        } else if !decoded {
            rows.undecoded += 1;
        }
    };
    for ix in flatten(tx_base64, meta)?
        .into_iter()
        .filter(|ix| ix.program == program)
    {
        if ix.data.len() >= 16 && ix.data[..8] == EVENT_IX_TAG {
            let (row, known, decoded) =
                event_row(idl, tx, ordinal, "cpi", Some(&ix.path), &ix.data[8..]);
            count(&mut rows, known, decoded);
            rows.events.push(row);
            ordinal += 1;
            continue;
        }
        let disc = &ix.data[..ix.data.len().min(8)];
        rows.instructions.push(json!({
            "signature": tx.signature, "slot": tx.slot, "block_time": tx.block_time,
            "path": ix.path, "discriminator": hex::encode(disc), "name": idl.instruction_name(disc),
            "data_b64": STANDARD.encode(&ix.data), "accounts": ix.accounts,
        }));
    }
    for payload in log_payloads(meta, program)
        .into_iter()
        .filter(|p| p.len() >= 8)
    {
        let (row, known, decoded) = event_row(idl, tx, ordinal, "log", None, &payload);
        count(&mut rows, known, decoded);
        rows.events.push(row);
        ordinal += 1;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_payloads_belong_to_the_innermost_invocation() {
        let meta = json!({ "logMessages": [
            "Program P invoke [1]",
            "Program data: AQID",
            "Program Q invoke [2]",
            "Program data: BAUG",
            "Program Q success",
            "Program R invoke [2]",
            "Program R failed: custom program error: 0x1",
            "Program data: Bwg= CQ==",
            "Program P success",
            "Program data: Cg==",
        ]});
        assert_eq!(log_payloads(&meta, "P"), vec![vec![1, 2, 3], vec![7, 8, 9]]);
        assert_eq!(log_payloads(&meta, "Q"), vec![vec![4, 5, 6]]);
    }
}
