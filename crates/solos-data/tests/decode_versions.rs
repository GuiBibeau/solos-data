//! Ported from `tests/decode-versions.test.ts`: the same instructions wrapped in legacy, v0 and
//! v1 envelopes extract identically.

mod common;

use base64::{Engine, engine::general_purpose::STANDARD};
use common::*;
use solos_data::decoder::extract::extract_groups;

#[test]
fn event_instruction_extraction_resolves_legacy_v0_and_v1_wire_layouts_identically() {
    let fixture = &golden_fixtures()[0];
    let top = top_level(fixture);
    let meta = meta(fixture);
    let legacy = legacy_wire(fixture);
    let expected =
        serde_json::to_string(&extract_groups(&STANDARD.encode(&legacy), &meta).unwrap()).unwrap();
    assert!(expected.contains("\"logs\""));

    // v0: signatures first, version byte 0x80, legacy body, zero address table lookups.
    let mut v0 = vec![1];
    v0.extend([0u8; 64]);
    v0.push(0x80);
    v0.extend(legacy_message(&top));
    v0.push(0);

    // v1 (SIMD-0385): message first, signatures last, instruction headers then payloads.
    let mut v1 = vec![0x81, 1, 0, 1];
    v1.extend(0u32.to_le_bytes());
    v1.extend([0u8; 32]);
    v1.extend([u8::try_from(top.len()).unwrap(), 2]);
    v1.extend([0u8; 32]);
    v1.extend(
        bs58::decode(solos_data::decoder::extract::PROGRAM)
            .into_vec()
            .unwrap(),
    );
    for ix in &top {
        let len = ix.as_ref().map_or(0, Vec::len);
        v1.extend([u8::from(ix.is_some()), 0]);
        v1.extend(u16::try_from(len).unwrap().to_le_bytes());
    }
    for ix in &top {
        v1.extend(ix.clone().unwrap_or_default());
    }
    v1.extend([0u8; 64]);

    for wire in [v0, v1] {
        let groups = extract_groups(&STANDARD.encode(&wire), &meta).unwrap();
        assert_eq!(serde_json::to_string(&groups).unwrap(), expected);
    }
}
