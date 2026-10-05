//! Instruction extraction: which Phoenix program instructions a transaction carries, their CPI
//! stack paths, and the base64 log payloads each one owns. A port of `decode/instructions.ts`.

use base64::{Engine, engine::general_purpose::STANDARD};
use phoenix_codec::Group;
use serde_json::Value;

/// The Phoenix Rise perpetuals program.
pub const PROGRAM: &str = "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih";

const TAGS: [&str; 2] = ["8de6d6f209d1cfaa", "f70786cbb5479947"];

/// One compiled instruction with its stack path and how the path was established.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instruction {
    /// Base58 program address.
    pub program_id: String,
    /// Stack path: top-level index, then CPI siblings per depth.
    pub path: Vec<usize>,
    /// Instruction data.
    pub data: Vec<u8>,
    /// `stack_height` or `unknown`.
    pub attribution: String,
}

/// A CPI sibling increments at its stack depth; descendants start at zero.
pub fn advance_path(path: &[usize], height: i64) -> Result<Vec<usize>, String> {
    if height < 2 {
        return Err("invalid CPI stack height".into());
    }
    let depth = usize::try_from(height - 1).map_err(|_| "invalid CPI stack height".to_owned())?;
    if depth > path.len() + 1 {
        return Err("CPI stack depth skips an ancestor".into());
    }
    if depth > path.len() {
        let mut next = path.to_vec();
        next.push(0);
        return Ok(next);
    }
    let mut next = path[..depth].to_vec();
    next[depth - 1] += 1;
    Ok(next)
}

fn join(path: &[usize]) -> String {
    path.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

/// Group log instructions under their parent Phoenix instruction.
#[must_use]
pub fn group_instructions(instructions: &[Instruction]) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    for ix in instructions {
        if ix.program_id != PROGRAM {
            continue;
        }
        let path = join(&ix.path);
        let tag = hex::encode(&ix.data[..ix.data.len().min(8)]);
        if !TAGS.contains(&tag.as_str()) {
            groups.push(Group {
                path,
                logs: Vec::new(),
                attribution: ix.attribution.clone(),
            });
            continue;
        }
        let parent = join(&ix.path[..ix.path.len().saturating_sub(1)]);
        let index = match groups.iter().rposition(|group| group.path == parent) {
            Some(index) => index,
            None => {
                // Missing stackHeight cannot safely associate nested Phoenix instructions.
                groups.push(Group {
                    path: format!("orphan.{path}"),
                    logs: Vec::new(),
                    attribution: "unknown".into(),
                });
                groups.len() - 1
            }
        };
        groups[index].logs.push(STANDARD.encode(&ix.data));
        if ix.attribution != "stack_height" {
            groups[index].attribution = "unknown".into();
        }
    }
    groups
        .into_iter()
        .filter(|group| !group.logs.is_empty())
        .collect()
}

/// Extract the Phoenix instruction groups of one transaction from its base64 wire and its RPC
/// `meta` (for inner instructions and loaded addresses).
pub fn extract_groups(tx_base64: &str, meta: &Value) -> Result<Vec<Group>, String> {
    let wire = solana_wire::parse_base64(tx_base64).map_err(|e| e.to_string())?;
    let strings = |key: &str| -> Vec<String> {
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
    };
    let addresses = solana_wire::resolve_accounts(
        &wire.static_accounts(),
        &strings("writable"),
        &strings("readonly"),
    );
    let mut instructions = Vec::new();
    for (i, ix) in wire.instructions().iter().enumerate() {
        let program = addresses
            .get(usize::from(ix.program_id_index))
            .ok_or("unresolved program address")?;
        instructions.push(Instruction {
            program_id: program.clone(),
            path: vec![i],
            data: ix.data.clone(),
            attribution: "stack_height".into(),
        });
        let mut inner_path: Vec<usize> = Vec::new();
        let inner = meta
            .get("innerInstructions")
            .and_then(Value::as_array)
            .and_then(|groups| {
                groups
                    .iter()
                    .find(|group| group.get("index").and_then(Value::as_u64) == Some(i as u64))
            })
            .and_then(|group| group.get("instructions"))
            .and_then(Value::as_array);
        for item in inner.into_iter().flatten() {
            let program_index = item
                .get("programIdIndex")
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok());
            let data = item.get("data").and_then(Value::as_str);
            let (Some(program_index), Some(data)) = (program_index, data) else {
                return Err("unresolved CPI instruction".into());
            };
            let Some(program) = addresses.get(program_index) else {
                return Err("unresolved CPI instruction".into());
            };
            let height = item.get("stackHeight").and_then(Value::as_i64);
            let known = matches!(height, Some(h) if h >= 2);
            inner_path = advance_path(&inner_path, if known { height.unwrap_or(2) } else { 2 })?;
            let mut path = vec![i];
            path.extend(&inner_path);
            let bytes = bs58::decode(data)
                .into_vec()
                .map_err(|_| "unresolved CPI instruction".to_owned())?;
            instructions.push(Instruction {
                program_id: program.clone(),
                path,
                data: bytes,
                attribution: if known {
                    "stack_height".into()
                } else {
                    "unknown".into()
                },
            });
        }
    }
    Ok(group_instructions(&instructions))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `nested CPI parents and missing stack heights are explicit` from `tests/decode.test.ts`.
    #[test]
    fn nested_cpi_parents_and_missing_stack_heights_are_explicit() {
        assert_eq!(advance_path(&[], 2).unwrap(), vec![0]);
        assert_eq!(advance_path(&[0], 3).unwrap(), vec![0, 0]);
        assert_eq!(advance_path(&[0, 0], 2).unwrap(), vec![1]);
        let log = hex::decode("8de6d6f209d1cfaa0000000000000000").unwrap();
        let groups = group_instructions(&[
            Instruction {
                program_id: PROGRAM.into(),
                path: vec![3, 0],
                data: vec![0; 8],
                attribution: "stack_height".into(),
            },
            Instruction {
                program_id: PROGRAM.into(),
                path: vec![3, 0, 0],
                data: log.clone(),
                attribution: "stack_height".into(),
            },
        ]);
        assert_eq!(groups[0].path, "3.0");
        assert_eq!(groups[0].logs.len(), 1);
        let orphan = group_instructions(&[Instruction {
            program_id: PROGRAM.into(),
            path: vec![3, 0],
            data: log,
            attribution: "unknown".into(),
        }]);
        assert_eq!(orphan[0].attribution, "unknown");
        assert!(orphan[0].path.starts_with("orphan."));
    }
}
