use std::io::{self, BufRead, Write};
use base64::{Engine, engine::general_purpose::STANDARD};
use phoenix_rise_events::{PhoenixLogInstruction, parse_with_errors};
use serde::Deserialize;
use serde_json::{Value, json};
mod integrity;

#[derive(Deserialize)]
struct Group { path: String, logs: Vec<String>, attribution: String }

// JSON consumers must not round native u64/i128 quantities through IEEE doubles.
fn exact(value: Value) -> Value {
    match value {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(items) => Value::Array(items.into_iter().map(exact).collect()),
        Value::Object(items) => Value::Object(items.into_iter().map(|(k,v)| (k,exact(v))).collect()),
        other => other,
    }
}

fn decode(group: Group) -> Result<Value, String> {
    let bytes: Vec<Vec<u8>> = group.logs.iter().map(|s| STANDARD.decode(s))
        .collect::<Result<_,_>>().map_err(|e| e.to_string())?;
    let mut errors = Vec::new();
    let instructions: Vec<_> = bytes.iter().filter_map(|b| {
        let ix = PhoenixLogInstruction::from_instruction_data(b);
        if ix.is_none() { errors.push(json!({"error":"unrecognized log instruction", "bytes":STANDARD.encode(b)})); }
        ix
    }).collect();
    if let Some(error) = integrity::check(&instructions) {
        for payload in &group.logs { errors.push(json!({"error":error,"bytes":payload})); }
    }
    let parsed = parse_with_errors(instructions);
    for failure in &parsed.failures {
        errors.push(json!({"error":failure.error.to_string(),"bytes":STANDARD.encode(failure.bytes)}));
    }
    if parsed.events.iter().any(|e| !serde_json::to_value(e).unwrap().is_object()) {
        for payload in &group.logs { errors.push(json!({"error":"unsupported placeholder variant","bytes":payload})); }
    }
    // A partial decode cannot assign stable ordinals to skipped events. Quarantine
    // the complete instruction rather than publish misleading semantic rows.
    let events: Vec<Value> = if errors.is_empty() {
        parsed.events.iter().map(|event| exact(serde_json::to_value(event).unwrap())).collect()
    } else { Vec::new() };
    Ok(json!({"path":group.path,"attribution":group.attribution,"events":events,"errors":errors}))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut out = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let groups: Vec<Group> = serde_json::from_str(&line?)?;
        let result: Result<Vec<_>,_> = groups.into_iter().map(decode).collect();
        let response = match result { Ok(groups) => json!({"groups":groups}), Err(e) => json!({"fatal":e}) };
        writeln!(out,"{}",response)?;
        out.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use phoenix_rise_events::{MarketEvent, OffChainMarketEvent, LOG_INSTRUCTION_TAG};
    use phoenix_rise_events::market_events::SlotContextEvent;
    #[test]
    fn legacy_exact_and_unknown_quarantined() {
        let payload = OffChainMarketEvent { batch_index:0, events:vec![MarketEvent::SlotContext(
            SlotContextEvent { slot:u64::MAX, timestamp:1 })] };
        let mut bytes = LOG_INSTRUCTION_TAG.to_le_bytes().to_vec();
        bytes.extend(borsh::to_vec(&payload).unwrap());
        let group = || Group { path:"0".into(), logs:vec![STANDARD.encode(&bytes)], attribution:"stack_height".into() };
        let result = decode(group()).unwrap();
        assert_eq!(result["events"][0]["SlotContext"]["slot"],u64::MAX.to_string());
        bytes[16] = 255;
        let result = decode(Group { path:"0".into(), logs:vec![STANDARD.encode(&bytes)], attribution:"stack_height".into() }).unwrap();
        assert_eq!(result["events"].as_array().unwrap().len(),0);
        assert!(!result["errors"].as_array().unwrap().is_empty());
    }
}
