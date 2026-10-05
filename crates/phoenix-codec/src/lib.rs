//! Phoenix Rise event decoding for solos-data.
//!
//! One instruction group goes in: the Phoenix program instruction's path, the base64 log payloads
//! of its CPI children, and how ownership was established. Events come out with every integer as
//! a decimal string, or the whole group is quarantined. The decoder calls [`decode`] in-process;
//! the `solos-data-phoenix-codec` binary exposes the same function over stdio for the TypeScript
//! decoder while it still exists.

use base64::{Engine, engine::general_purpose::STANDARD};
use phoenix_rise_events::{PhoenixLogInstruction, parse_with_errors};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub mod integrity;

/// One Phoenix instruction group as the extractor assembles it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Group {
    /// Instruction stack path, for example `1` or `0.2.0`.
    pub path: String,
    /// Base64 log instruction payloads in instruction order.
    pub logs: Vec<String>,
    /// `stack_height` when CPI ownership is established, `unknown` otherwise.
    pub attribution: String,
}

/// A payload the codec could not turn into events; kept for a later codec version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecodeError {
    /// Why the payload was quarantined.
    pub error: String,
    /// The base64 payload.
    pub bytes: String,
}

/// A decoded group: events in ordinal order, or quarantined errors, never both.
#[derive(Debug, Clone, Serialize)]
pub struct DecodedGroup {
    /// Instruction stack path, copied from the input.
    pub path: String,
    /// Attribution, copied from the input.
    pub attribution: String,
    /// Events as JSON, every integer rendered as a decimal string.
    pub events: Vec<Value>,
    /// Quarantined payloads; non-empty means `events` is empty.
    pub errors: Vec<DecodeError>,
}

/// JSON consumers must not round native u64/i128 quantities through IEEE doubles.
#[must_use]
pub fn exact(value: Value) -> Value {
    match value {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(items) => Value::Array(items.into_iter().map(exact).collect()),
        Value::Object(items) => {
            Value::Object(items.into_iter().map(|(k, v)| (k, exact(v))).collect())
        }
        other => other,
    }
}

/// Decode one group. `Err` means the input itself was malformed (bad base64), which the caller
/// treats as fatal; a payload the SDK cannot parse is a quarantined error inside `Ok`.
///
/// # Errors
///
/// Returns the base64 decoding error message when a log payload is not valid base64.
pub fn decode(group: Group) -> Result<DecodedGroup, String> {
    let bytes = group
        .logs
        .iter()
        .map(|payload| STANDARD.decode(payload))
        .collect::<Result<Vec<Vec<u8>>, _>>()
        .map_err(|error| error.to_string())?;
    let mut errors = Vec::new();
    let instructions: Vec<_> = bytes
        .iter()
        .filter_map(|payload| {
            let instruction = PhoenixLogInstruction::from_instruction_data(payload);
            if instruction.is_none() {
                errors.push(DecodeError {
                    error: "unrecognized log instruction".into(),
                    bytes: STANDARD.encode(payload),
                });
            }
            instruction
        })
        .collect();
    if let Some(error) = integrity::check(&instructions) {
        for payload in &group.logs {
            errors.push(DecodeError {
                error: error.clone(),
                bytes: payload.clone(),
            });
        }
    }
    let parsed = parse_with_errors(instructions);
    for failure in &parsed.failures {
        errors.push(DecodeError {
            error: failure.error.to_string(),
            bytes: STANDARD.encode(failure.bytes),
        });
    }
    let values: Vec<Value> = parsed
        .events
        .iter()
        .map(|event| serde_json::to_value(event).expect("SDK events serialize"))
        .collect();
    if values.iter().any(|value| !value.is_object()) {
        for payload in &group.logs {
            errors.push(DecodeError {
                error: "unsupported placeholder variant".into(),
                bytes: payload.clone(),
            });
        }
    }
    // A partial decode cannot assign stable ordinals to skipped events. Quarantine the complete
    // instruction rather than publish misleading semantic rows.
    let events = if errors.is_empty() {
        values.into_iter().map(exact).collect()
    } else {
        Vec::new()
    };
    Ok(DecodedGroup {
        path: group.path,
        attribution: group.attribution,
        events,
        errors,
    })
}

/// Decode every group of one transaction, stopping at the first malformed input.
///
/// # Errors
///
/// See [`decode`].
pub fn decode_all(groups: Vec<Group>) -> Result<Vec<DecodedGroup>, String> {
    groups.into_iter().map(decode).collect()
}

/// The stdio protocol's response line for one request: `{"groups":[...]}` or `{"fatal":"..."}`.
#[must_use]
pub fn stdio_response(groups: Vec<Group>) -> Value {
    match decode_all(groups) {
        Ok(decoded) => json!({ "groups": decoded }),
        Err(error) => json!({ "fatal": error }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use phoenix_rise_events::market_events::SlotContextEvent;
    use phoenix_rise_events::{LOG_INSTRUCTION_TAG, MarketEvent, OffChainMarketEvent};

    fn group(bytes: &[u8]) -> Group {
        Group {
            path: "0".into(),
            logs: vec![STANDARD.encode(bytes)],
            attribution: "stack_height".into(),
        }
    }

    #[test]
    fn legacy_exact_and_unknown_quarantined() {
        let payload = OffChainMarketEvent {
            batch_index: 0,
            events: vec![MarketEvent::SlotContext(SlotContextEvent {
                slot: u64::MAX,
                timestamp: 1,
            })],
        };
        let mut bytes = LOG_INSTRUCTION_TAG.to_le_bytes().to_vec();
        bytes.extend(borsh::to_vec(&payload).unwrap());
        let result = decode(group(&bytes)).unwrap();
        assert_eq!(
            result.events[0]["SlotContext"]["slot"],
            u64::MAX.to_string()
        );
        assert!(result.errors.is_empty());
        bytes[16] = 255;
        let result = decode(group(&bytes)).unwrap();
        assert!(result.events.is_empty());
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn stdio_response_shapes() {
        let ok = stdio_response(vec![]);
        assert_eq!(ok["groups"], json!([]));
        let bad = Group {
            path: "0".into(),
            logs: vec!["%%".into()],
            attribution: "unknown".into(),
        };
        assert!(stdio_response(vec![bad])["fatal"].is_string());
    }
}
