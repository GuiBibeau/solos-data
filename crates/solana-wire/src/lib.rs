//! Solana transaction envelopes parsed the way `@solana/kit` parses them: structurally, without
//! sanitization. The archive holds whatever the chain finalized, including wires a runtime check
//! would reject, so this crate only answers three questions about a wire: its signature, its
//! static account keys, and its compiled instructions.
//!
//! Legacy and v0 wires put the signature vector first; v1 (SIMD-0385) puts the message first and
//! the fixed-length signatures last. All three decode through the Solana SDK's `wincode` reader.

use base64::{Engine, engine::general_purpose::STANDARD};
use solana_message::VersionedMessage;
use solana_message::compiled_instruction::CompiledInstruction;
use solana_transaction::versioned::VersionedTransaction;

/// Why a wire could not be parsed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    /// The text was not valid base64.
    #[error("transaction is not valid base64")]
    Base64,
    /// The bytes are not a legacy, v0 or v1 transaction envelope.
    #[error("invalid transaction wire: {0}")]
    Decode(String),
    /// The envelope carries no signature, so it has no transaction signature.
    #[error("transaction has no signature")]
    NoSignature,
}

/// The transaction envelope version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// Signatures first, no version byte.
    Legacy,
    /// Signatures first, version byte `0x80`, address lookup tables.
    V0,
    /// Message first, version byte `0x81`, signatures last.
    V1,
}

/// A parsed transaction envelope.
#[derive(Debug, Clone)]
pub struct Wire {
    transaction: VersionedTransaction,
}

/// Parse raw wire bytes.
///
/// # Errors
///
/// [`WireError::Decode`] when the bytes are not an envelope, [`WireError::NoSignature`] when the
/// envelope has an empty signature vector.
pub fn parse(bytes: &[u8]) -> Result<Wire, WireError> {
    let transaction: VersionedTransaction =
        wincode::deserialize(bytes).map_err(|error| WireError::Decode(error.to_string()))?;
    if transaction.signatures.is_empty() {
        return Err(WireError::NoSignature);
    }
    Ok(Wire { transaction })
}

/// Parse a base64 wire as RPC responses carry it.
///
/// # Errors
///
/// [`WireError::Base64`] for bad text, then as [`parse`].
pub fn parse_base64(text: &str) -> Result<Wire, WireError> {
    let bytes = STANDARD.decode(text).map_err(|_| WireError::Base64)?;
    parse(&bytes)
}

/// The base58 transaction signature of a base64 wire: the first signature, as Kit's
/// `getSignatureFromTransaction` returns it.
///
/// # Errors
///
/// As [`parse_base64`].
pub fn signature_of_base64(text: &str) -> Result<String, WireError> {
    Ok(parse_base64(text)?.signature())
}

impl Wire {
    /// The transaction signature (the fee payer's), base58.
    #[must_use]
    pub fn signature(&self) -> String {
        self.transaction.signatures[0].to_string()
    }

    /// Which envelope the bytes used.
    #[must_use]
    pub fn version(&self) -> Version {
        match self.transaction.message {
            VersionedMessage::Legacy(_) => Version::Legacy,
            VersionedMessage::V0(_) => Version::V0,
            VersionedMessage::V1(_) => Version::V1,
        }
    }

    /// Static account keys in message order, base58.
    #[must_use]
    pub fn static_accounts(&self) -> Vec<String> {
        self.transaction
            .message
            .static_account_keys()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Top-level compiled instructions in message order. Account and program indices refer to the
    /// list [`resolve_accounts`] builds.
    #[must_use]
    pub fn instructions(&self) -> &[CompiledInstruction] {
        self.transaction.message.instructions()
    }
}

/// The account list that instruction indices refer to: static keys, then the addresses the
/// runtime loaded from lookup tables, writable first, as `meta.loadedAddresses` reports them.
#[must_use]
pub fn resolve_accounts(
    static_keys: &[String],
    writable: &[String],
    readonly: &[String],
) -> Vec<String> {
    static_keys
        .iter()
        .chain(writable)
        .chain(readonly)
        .cloned()
        .collect()
}
