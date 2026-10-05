//! Ported from `tests/bulk.test.ts` ("wire signature extraction handles legacy, v0 and
//! tail-signature v1 envelopes") and `tests/decode-fixtures.ts` (`rawFixture`): synthetic wires
//! with zero keys and zero or trivial instructions, which Kit decodes without sanitizing.

use base64::{Engine, engine::general_purpose::STANDARD};
use solana_wire::{Version, WireError, parse, parse_base64, resolve_accounts, signature_of_base64};

fn legacy_message() -> Vec<u8> {
    // header (1 signer, 0 readonly signed, 0 readonly unsigned), 1 static key, blockhash, 0 instructions
    let mut message = vec![1, 0, 0, 1];
    message.extend([0u8; 64]);
    message.push(0);
    message
}

/// The TypeScript `wire(version, byte)` helper: a 64-byte signature filled with `byte`.
fn wire(version: Version, byte: u8) -> String {
    let signature = vec![byte; 64];
    let bytes = match version {
        Version::Legacy => [vec![1], signature, legacy_message()].concat(),
        Version::V0 => [vec![1], signature, vec![128], legacy_message(), vec![0]].concat(),
        Version::V1 => [
            vec![129, 1, 0, 0],
            vec![0; 4 + 32],
            vec![0, 1],
            vec![0; 32],
            signature,
        ]
        .concat(),
    };
    STANDARD.encode(bytes)
}

fn base58(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let mut digits: Vec<u8> = vec![0];
    for &byte in bytes {
        let mut carry = u32::from(byte);
        for digit in &mut digits {
            carry += u32::from(*digit) << 8;
            *digit = u8::try_from(carry % 58).unwrap();
            carry /= 58;
        }
        while carry > 0 {
            digits.push(u8::try_from(carry % 58).unwrap());
            carry /= 58;
        }
    }
    let zeros = bytes.iter().take_while(|&&b| b == 0).count();
    let mut out: Vec<u8> = vec![b'1'; zeros];
    out.extend(digits.iter().rev().map(|&d| ALPHABET[d as usize]));
    String::from_utf8(out).unwrap()
}

#[test]
fn wire_signature_extraction_handles_legacy_v0_and_tail_signature_v1_envelopes() {
    let expected = base58(&[7u8; 64]);
    for version in [Version::Legacy, Version::V0, Version::V1] {
        let parsed = parse_base64(&wire(version, 7)).unwrap();
        assert_eq!(parsed.signature(), expected, "{version:?}");
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.static_accounts().len(), 1);
        assert!(parsed.instructions().is_empty());
    }
    assert_eq!(
        signature_of_base64(&wire(Version::V1, 8)).unwrap(),
        base58(&[8u8; 64])
    );
    assert!(matches!(
        parse_base64("AA=="),
        Err(WireError::Decode(_) | WireError::NoSignature)
    ));
    assert!(matches!(parse_base64("%%"), Err(WireError::Base64)));
}

/// `rawFixture`: legacy wire, two static keys (fee payer then the program), top-level instructions
/// that call program index 1 with account 0 and the fixture's bytes.
#[test]
fn legacy_instructions_resolve_program_and_accounts() {
    let payloads: [&[u8]; 2] = [&[0xAB, 0xCD], &[]];
    let mut bytes = vec![1];
    bytes.extend([0u8; 64]);
    bytes.extend([1, 0, 1, 2]);
    bytes.extend([0u8; 32]);
    bytes.extend([9u8; 32]);
    bytes.extend([0u8; 32]);
    bytes.push(2);
    for payload in payloads {
        bytes.extend([1, 1, 0, u8::try_from(payload.len()).unwrap()]);
        bytes.extend(payload);
    }
    let parsed = parse(&bytes).unwrap();
    assert_eq!(parsed.version(), Version::Legacy);
    let keys = parsed.static_accounts();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[1], base58(&[9u8; 32]));
    let instructions = parsed.instructions();
    assert_eq!(instructions.len(), 2);
    assert_eq!(instructions[0].program_id_index, 1);
    assert_eq!(instructions[0].accounts, vec![0]);
    assert_eq!(instructions[0].data, vec![0xAB, 0xCD]);
    assert!(instructions[1].data.is_empty());
}

/// v1 with config values set (priority fee and compute unit limit), one instruction header and
/// payload, signatures last.
#[test]
fn v1_config_values_headers_and_payloads() {
    let mut bytes = vec![129, 1, 0, 1];
    bytes.extend(0b111u32.to_le_bytes());
    bytes.extend([5u8; 32]);
    bytes.extend([1, 2]);
    bytes.extend([0u8; 32]);
    bytes.extend([3u8; 32]);
    bytes.extend(1_000u64.to_le_bytes());
    bytes.extend(200_000u32.to_le_bytes());
    bytes.extend([1, 1, 2, 0]);
    bytes.extend([0, 0xEE, 0xFF]);
    bytes.extend([4u8; 64]);
    let parsed = parse(&bytes).unwrap();
    assert_eq!(parsed.version(), Version::V1);
    assert_eq!(parsed.signature(), base58(&[4u8; 64]));
    assert_eq!(parsed.static_accounts()[1], base58(&[3u8; 32]));
    let instruction = &parsed.instructions()[0];
    assert_eq!(instruction.program_id_index, 1);
    assert_eq!(instruction.accounts, vec![0]);
    assert_eq!(instruction.data, vec![0xEE, 0xFF]);
    assert!(matches!(
        parse(&bytes[..bytes.len() - 1]),
        Err(WireError::Decode(_))
    ));
}

#[test]
fn loaded_addresses_follow_static_keys_writable_first() {
    let keys = resolve_accounts(
        &["a".into(), "b".into()],
        &["w".into()],
        &["r1".into(), "r2".into()],
    );
    assert_eq!(keys, ["a", "b", "w", "r1", "r2"]);
}
