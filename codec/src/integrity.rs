use std::collections::{BTreeMap, VecDeque};
use phoenix_rise_events::{PhoenixLogInstruction, PhoenixLogInstructionKind};

// The upstream parser tolerates truncated length vectors and trailing bytes.
// Require the complete envelope before publishing a stable ordinal sequence.
pub fn check(instructions: &[PhoenixLogInstruction<'_>]) -> Option<String> {
    if !instructions.iter().any(|ix| ix.kind == PhoenixLogInstructionKind::LogEventLengths) { return None; }
    let mut lengths = BTreeMap::<u32, VecDeque<Vec<usize>>>::new();
    for ix in instructions {
        let data = ix.data;
        if data.len() < 8 { return Some("short log envelope".into()); }
        let batch = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        match ix.kind {
            PhoenixLogInstructionKind::LogEventLengths => {
                if data.len() != 8 + count * 2 { return Some("length vector size mismatch".into()); }
                let sizes = data[8..].chunks_exact(2).map(|b| u16::from_le_bytes(b.try_into().unwrap()) as usize).collect();
                lengths.entry(batch).or_default().push_back(sizes);
            }
            PhoenixLogInstructionKind::Log => {
                let Some(sizes) = lengths.get_mut(&batch).and_then(VecDeque::pop_front) else {
                    return Some("missing length envelope".into());
                };
                if sizes.len() != count || data.len() != 8 + sizes.iter().sum::<usize>() {
                    return Some("event count or total byte size mismatch".into());
                }
            }
        }
    }
    if lengths.values().any(|q| !q.is_empty()) { return Some("unconsumed event lengths".into()); }
    None
}
