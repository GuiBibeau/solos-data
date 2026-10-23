//! A Borsh reader driven by an Anchor IDL, so event and account layouts come from the venue's
//! own published IDL rather than from hand-copied structs (ADR-0012). Both IDL dialects are read:
//! the legacy one (Anchor ≤ 0.29: `publicKey`, `defined: "Name"`, events with inline fields,
//! discriminators computed from names) and the current one (Anchor ≥ 0.30: `pubkey`,
//! `defined: {name}`, explicit `discriminator` arrays, events typed in `types`).
//!
//! Integers wider than 32 bits render as decimal strings, as every other table of the dataset
//! renders them; public keys as base58; byte arrays as hex.

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// The Anchor `emit_cpi!` instruction tag: `sha256("anchor:event")[..8]` byte-reversed (Anchor
/// reads the prefix as a big-endian `u64` and writes it little-endian).
pub const EVENT_IX_TAG: [u8; 8] = [0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];

/// The first eight bytes of `sha256(<namespace>:<name>)`.
#[must_use]
pub fn discriminator(namespace: &str, name: &str) -> [u8; 8] {
    let digest = Sha256::digest(format!("{namespace}:{name}").as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

/// The JSON text of an on-chain Anchor IDL account: discriminator, authority, `u32` length,
/// then the zlib-compressed IDL. The account lives at
/// `createWithSeed(findProgramAddress([], program), "anchor:idl", program)`.
///
/// # Errors
///
/// A short account, a length past its end, or data that does not inflate to UTF-8.
pub fn inflate_idl_account(data: &[u8]) -> Result<String, String> {
    use std::io::Read as _;
    let header = 8 + 32 + 4;
    let len = data
        .get(40..44)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        .ok_or("IDL account shorter than its header")?;
    let body = data
        .get(header..header + len)
        .ok_or("IDL length past the account end")?;
    let mut text = String::new();
    flate2::read::ZlibDecoder::new(body)
        .read_to_string(&mut text)
        .map_err(|e| format!("IDL does not inflate: {e}"))?;
    Ok(text)
}

/// Why a payload did not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The payload ended before the layout did.
    #[error("payload ends at byte {0}")]
    Short(usize),
    /// The IDL names a type it does not define, or a type kind this reader does not know.
    #[error("unsupported type {0}")]
    Unsupported(String),
    /// An enum tag past the variants the IDL lists.
    #[error("enum {0} has no variant {1}")]
    Variant(String, u8),
    /// Bytes left after the layout ended (events must consume their payload exactly).
    #[error("{0} trailing bytes")]
    Trailing(usize),
}

/// An account's fields and the bytes its layout used, or why it did not decode.
pub type AccountFields = Result<(Value, usize), DecodeError>;

/// One parsed IDL: its types, events and accounts by discriminator.
#[derive(Debug, Default)]
pub struct Idl {
    types: HashMap<String, Value>,
    events: HashMap<[u8; 8], (String, Value)>,
    accounts: HashMap<[u8; 8], (String, Value)>,
    instructions: HashMap<[u8; 8], String>,
    /// Older IDLs of the same program, tried in order when this one does not decode an event or
    /// name an instruction: history crosses program upgrades (ADR-0013).
    fallbacks: Vec<Idl>,
}

fn disc_of(item: &Value, namespace: &str, name: &str) -> [u8; 8] {
    if let Some(bytes) = item.get("discriminator").and_then(Value::as_array)
        && bytes.len() == 8
    {
        let mut out = [0u8; 8];
        for (slot, byte) in out.iter_mut().zip(bytes) {
            *slot = byte
                .as_u64()
                .and_then(|b| u8::try_from(b).ok())
                .unwrap_or(0);
        }
        return out;
    }
    discriminator(namespace, name)
}

/// Anchor ≤ 0.29 hashes `global:<snake_case name>` for instructions; names are camelCase there.
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

impl Idl {
    /// Parse an IDL document.
    ///
    /// # Errors
    ///
    /// When the text is not JSON.
    pub fn parse(text: &str) -> Result<Idl, String> {
        let doc: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let mut idl = Idl::default();
        let list = |key: &str| {
            doc.get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        for item in list("types") {
            if let (Some(name), Some(ty)) =
                (item.get("name").and_then(Value::as_str), item.get("type"))
            {
                idl.types.insert(name.to_owned(), ty.clone());
            }
        }
        for item in list("events") {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                continue;
            };
            // Legacy events carry their fields inline; current ones point at a type.
            let ty = match item.get("fields") {
                Some(fields) => json!({ "kind": "struct", "fields": fields }),
                None => json!({ "defined": { "name": name } }),
            };
            idl.events
                .insert(disc_of(&item, "event", name), (name.to_owned(), ty));
        }
        for item in list("accounts") {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                continue;
            };
            let ty = match item.get("type") {
                Some(ty) => ty.clone(),
                None => json!({ "defined": { "name": name } }),
            };
            idl.accounts
                .insert(disc_of(&item, "account", name), (name.to_owned(), ty));
        }
        for item in list("instructions") {
            let Some(name) = item.get("name").and_then(Value::as_str) else {
                continue;
            };
            let disc = if item.get("discriminator").is_some() {
                disc_of(&item, "global", name)
            } else {
                discriminator("global", &snake_case(name))
            };
            idl.instructions.insert(disc, name.to_owned());
        }
        Ok(idl)
    }

    /// This IDL with `older` tried after it (and after the fallbacks it already has).
    #[must_use]
    pub fn with_fallback(mut self, older: Idl) -> Idl {
        self.fallbacks.push(older);
        self
    }

    /// Number of fallback IDLs.
    #[must_use]
    pub fn fallback_count(&self) -> usize {
        self.fallbacks.len()
    }

    /// The event name for a discriminator.
    #[must_use]
    pub fn event_name(&self, disc: &[u8]) -> Option<&str> {
        let key: [u8; 8] = disc.get(..8)?.try_into().ok()?;
        self.events.get(&key).map(|(name, _)| name.as_str())
    }

    /// The instruction name for a discriminator.
    #[must_use]
    pub fn instruction_name(&self, disc: &[u8]) -> Option<&str> {
        let key: [u8; 8] = disc.get(..8)?.try_into().ok()?;
        self.instructions
            .get(&key)
            .map(String::as_str)
            .or_else(|| self.fallbacks.iter().find_map(|f| f.instruction_name(disc)))
    }

    /// The account type name for a discriminator.
    #[must_use]
    pub fn account_name(&self, disc: &[u8]) -> Option<&str> {
        let key: [u8; 8] = disc.get(..8)?.try_into().ok()?;
        self.accounts.get(&key).map(|(name, _)| name.as_str())
    }

    /// The discriminator of a named account type.
    #[must_use]
    pub fn account_discriminator(&self, name: &str) -> Option<[u8; 8]> {
        self.accounts
            .iter()
            .find(|(_, (n, _))| n == name)
            .map(|(d, _)| *d)
    }

    /// Number of events, accounts and instructions the IDL defines.
    #[must_use]
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.events.len(),
            self.accounts.len(),
            self.instructions.len(),
        )
    }

    /// Decode an event payload (discriminator first). Returns the name when known, and the
    /// fields or the reason they did not decode. The payload must be consumed exactly.
    #[must_use]
    pub fn decode_event(&self, payload: &[u8]) -> Option<(String, Result<Value, DecodeError>)> {
        let own = self.decode_event_own(payload);
        if matches!(own, Some((_, Ok(_)))) || self.fallbacks.is_empty() {
            return own;
        }
        // An older layout that consumes the payload exactly wins; otherwise the newest answer
        // (this IDL's error, or the first fallback that knows the discriminator) stands.
        let mut first = own;
        for older in &self.fallbacks {
            match older.decode_event(payload) {
                Some((name, Ok(fields))) => return Some((name, Ok(fields))),
                Some(other) if first.is_none() => first = Some(other),
                _ => {}
            }
        }
        first
    }

    fn decode_event_own(&self, payload: &[u8]) -> Option<(String, Result<Value, DecodeError>)> {
        let key: [u8; 8] = payload.get(..8)?.try_into().ok()?;
        let (name, ty) = self.events.get(&key)?;
        let mut reader = Reader {
            data: &payload[8..],
            at: 0,
        };
        let result = self
            .value(&mut reader, ty, 0)
            .and_then(|value| match reader.rest() {
                0 => Ok(value),
                n => Err(DecodeError::Trailing(n)),
            });
        Some((name.clone(), result))
    }

    /// Decode an account (discriminator first). Accounts are allocated with spare room, so
    /// trailing bytes are allowed; the number consumed is returned with the fields.
    #[must_use]
    pub fn decode_account(&self, data: &[u8]) -> Option<(String, AccountFields)> {
        let key: [u8; 8] = data.get(..8)?.try_into().ok()?;
        let (name, ty) = self.accounts.get(&key)?;
        let mut reader = Reader {
            data: &data[8..],
            at: 0,
        };
        let result = self.value(&mut reader, ty, 0).map(|v| (v, reader.at + 8));
        Some((name.clone(), result))
    }

    /// Every type the IDL's events and accounts reach, checked without data: a name it does not
    /// define or a kind this reader does not know is returned (`Ok` means every layout is
    /// readable).
    ///
    /// # Errors
    ///
    /// The first unsupported type, with the event or account that reaches it.
    pub fn check(&self) -> Result<(), String> {
        let roots = self.events.values().chain(self.accounts.values());
        for (name, ty) in roots {
            self.check_type(ty, 0).map_err(|e| format!("{name}: {e}"))?;
        }
        Ok(())
    }

    fn check_type(&self, ty: &Value, depth: usize) -> Result<(), String> {
        if depth > 32 {
            return Err("nesting deeper than 32".into());
        }
        if let Some(name) = ty.as_str() {
            return match name {
                "bool" | "u8" | "i8" | "u16" | "i16" | "u32" | "i32" | "u64" | "i64" | "u128"
                | "i128" | "f32" | "f64" | "publicKey" | "pubkey" | "string" | "bytes" => Ok(()),
                other => Err(format!("unsupported type {other}")),
            };
        }
        let Some(obj) = ty.as_object() else {
            return Err(format!("unsupported type {ty}"));
        };
        if let Some(inner) = obj
            .get("option")
            .or_else(|| obj.get("coption"))
            .or_else(|| obj.get("vec"))
        {
            return self.check_type(inner, depth + 1);
        }
        if let Some(array) = obj.get("array").and_then(Value::as_array) {
            return match (array.first(), array.get(1).and_then(Value::as_u64)) {
                (Some(inner), Some(_)) => self.check_type(inner, depth + 1),
                _ => Err(format!("unsupported array {ty}")),
            };
        }
        if let Some(defined) = obj.get("defined") {
            let name = defined
                .as_str()
                .or_else(|| defined.get("name").and_then(Value::as_str));
            return match name.and_then(|n| self.types.get(n)) {
                Some(target) => self.check_type(target, depth + 1),
                None => Err(format!("undefined type {defined}")),
            };
        }
        let fields_of = |fields: Option<&Value>| -> Result<(), String> {
            for field in fields.and_then(Value::as_array).into_iter().flatten() {
                let ty = field.get("type").unwrap_or(field);
                self.check_type(ty, depth + 1)?;
            }
            Ok(())
        };
        match obj.get("kind").and_then(Value::as_str) {
            Some("struct") => fields_of(obj.get("fields")),
            Some("enum") => {
                for variant in obj
                    .get("variants")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    fields_of(variant.get("fields"))?;
                }
                Ok(())
            }
            Some("alias") => obj
                .get("value")
                .map_or(Err("alias without value".into()), |v| {
                    self.check_type(v, depth + 1)
                }),
            _ => Err(format!(
                "unsupported kind {}",
                ty.to_string().chars().take(80).collect::<String>()
            )),
        }
    }

    fn value(&self, r: &mut Reader<'_>, ty: &Value, depth: usize) -> Result<Value, DecodeError> {
        if depth > 32 {
            return Err(DecodeError::Unsupported("nesting deeper than 32".into()));
        }
        if let Some(name) = ty.as_str() {
            return primitive(r, name);
        }
        let Some(obj) = ty.as_object() else {
            return Err(DecodeError::Unsupported(ty.to_string()));
        };
        if let Some(inner) = obj.get("option").or_else(|| obj.get("coption")) {
            let tag = if obj.contains_key("coption") {
                r.u32()? as u8
            } else {
                r.u8()?
            };
            return if tag == 0 {
                Ok(Value::Null)
            } else {
                self.value(r, inner, depth + 1)
            };
        }
        if let Some(inner) = obj.get("vec") {
            let n = r.u32()? as usize;
            if inner.as_str() == Some("u8") {
                return Ok(Value::String(hex::encode(r.take(n)?)));
            }
            let mut items = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                items.push(self.value(r, inner, depth + 1)?);
            }
            return Ok(Value::Array(items));
        }
        if let Some(array) = obj.get("array").and_then(Value::as_array) {
            let (Some(inner), Some(n)) = (array.first(), array.get(1).and_then(Value::as_u64))
            else {
                return Err(DecodeError::Unsupported(ty.to_string()));
            };
            let n = usize::try_from(n).unwrap_or(usize::MAX);
            if inner.as_str() == Some("u8") {
                return Ok(Value::String(hex::encode(r.take(n)?)));
            }
            let mut items = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                items.push(self.value(r, inner, depth + 1)?);
            }
            return Ok(Value::Array(items));
        }
        if let Some(defined) = obj.get("defined") {
            let name = defined
                .as_str()
                .or_else(|| defined.get("name").and_then(Value::as_str));
            let Some(target) = name.and_then(|n| self.types.get(n)) else {
                return Err(DecodeError::Unsupported(format!("defined {defined}")));
            };
            return self.value(r, target, depth + 1);
        }
        match obj.get("kind").and_then(Value::as_str) {
            Some("struct") => self.fields(r, obj.get("fields"), depth),
            Some("enum") => {
                let tag = r.u8()?;
                let variants = obj.get("variants").and_then(Value::as_array);
                let Some(variant) = variants.and_then(|v| v.get(usize::from(tag))) else {
                    return Err(DecodeError::Variant(
                        ty.to_string().chars().take(60).collect(),
                        tag,
                    ));
                };
                let name = variant.get("name").and_then(Value::as_str).unwrap_or("?");
                match variant.get("fields") {
                    None => Ok(Value::String(name.to_owned())),
                    Some(fields) => Ok(json!({ name: self.fields(r, Some(fields), depth)? })),
                }
            }
            Some("alias") => match obj.get("value") {
                Some(inner) => self.value(r, inner, depth + 1),
                None => Err(DecodeError::Unsupported(ty.to_string())),
            },
            _ => Err(DecodeError::Unsupported(
                ty.to_string().chars().take(80).collect(),
            )),
        }
    }

    /// Named fields as an object, tuple fields as an array.
    fn fields(
        &self,
        r: &mut Reader<'_>,
        fields: Option<&Value>,
        depth: usize,
    ) -> Result<Value, DecodeError> {
        let list = fields
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let named = list.first().is_some_and(|f| f.get("name").is_some());
        if named {
            let mut out = Map::new();
            for field in &list {
                let name = field.get("name").and_then(Value::as_str).unwrap_or("?");
                let ty = field.get("type").unwrap_or(&Value::Null);
                out.insert(name.to_owned(), self.value(r, ty, depth + 1)?);
            }
            Ok(Value::Object(out))
        } else {
            let mut out = Vec::new();
            for ty in &list {
                out.push(self.value(r, ty, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
    }
}

/// A cursor over a payload.
pub struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// A reader at the start of `data`.
    #[must_use]
    pub fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, at: 0 }
    }

    /// Bytes not read yet.
    #[must_use]
    pub fn rest(&self) -> usize {
        self.data.len() - self.at
    }

    /// The next `n` bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Short`] past the end.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.at.checked_add(n).filter(|end| *end <= self.data.len());
        let Some(end) = end else {
            return Err(DecodeError::Short(self.data.len()));
        };
        let out = &self.data[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// A `u8`.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.fixed::<1>()?[0])
    }

    /// A little-endian `u32`.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    /// A little-endian `u64`.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    /// A little-endian `i64`.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }

    /// A little-endian `i32`.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.fixed()?))
    }

    /// A public key, base58.
    ///
    /// # Errors
    ///
    /// Past the end.
    pub fn pubkey(&mut self) -> Result<String, DecodeError> {
        Ok(bs58::encode(self.take(32)?).into_string())
    }
}

fn primitive(r: &mut Reader<'_>, name: &str) -> Result<Value, DecodeError> {
    Ok(match name {
        "bool" => Value::Bool(r.u8()? != 0),
        "u8" => json!(r.u8()?),
        "i8" => json!(i8::from_le_bytes(r.fixed()?)),
        "u16" => json!(u16::from_le_bytes(r.fixed()?)),
        "i16" => json!(i16::from_le_bytes(r.fixed()?)),
        "u32" => json!(r.u32()?),
        "i32" => json!(r.i32()?),
        "u64" => Value::String(r.u64()?.to_string()),
        "i64" => Value::String(r.i64()?.to_string()),
        "u128" => Value::String(u128::from_le_bytes(r.fixed()?).to_string()),
        "i128" => Value::String(i128::from_le_bytes(r.fixed()?).to_string()),
        "f32" => json!(f32::from_le_bytes(r.fixed()?)),
        "f64" => json!(f64::from_le_bytes(r.fixed()?)),
        "publicKey" | "pubkey" => Value::String(r.pubkey()?),
        "string" => {
            let n = r.u32()? as usize;
            Value::String(String::from_utf8_lossy(r.take(n)?).into_owned())
        }
        "bytes" => {
            let n = r.u32()? as usize;
            Value::String(hex::encode(r.take(n)?))
        }
        other => return Err(DecodeError::Unsupported(other.to_owned())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY: &str = r#"{
      "instructions": [{ "name": "increasePosition4" }],
      "accounts": [{ "name": "Pool", "type": { "kind": "struct", "fields": [
        { "name": "name", "type": "string" }, { "name": "aum", "type": "u128" } ] } }],
      "types": [{ "name": "Side", "type": { "kind": "enum", "variants": [{ "name": "None" }, { "name": "Long" }, { "name": "Short" }] } }],
      "events": [{ "name": "Fill", "fields": [
        { "name": "key", "type": "publicKey", "index": false },
        { "name": "side", "type": { "defined": "Side" }, "index": false },
        { "name": "size", "type": "u64", "index": false },
        { "name": "fee", "type": { "option": "i64" }, "index": false } ] }]
    }"#;

    #[test]
    fn an_onchain_idl_account_inflates() {
        use std::io::Write as _;
        let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        z.write_all(br#"{"events":[]}"#).unwrap();
        let body = z.finish().unwrap();
        let mut data = vec![0u8; 40];
        data.extend(u32::try_from(body.len()).unwrap().to_le_bytes());
        data.extend(&body);
        data.extend([0u8; 64]);
        assert_eq!(inflate_idl_account(&data).unwrap(), r#"{"events":[]}"#);
        assert!(inflate_idl_account(&data[..50]).is_err());
    }

    #[test]
    fn anchor_tags_match_their_definitions() {
        // Anchor reads the hash prefix as a big-endian u64 and writes it little-endian.
        let mut tag = discriminator("anchor", "event");
        tag.reverse();
        assert_eq!(tag, EVENT_IX_TAG);
        assert_eq!(snake_case("increasePosition4"), "increase_position4");
    }

    #[test]
    fn legacy_events_and_accounts_decode_exactly() {
        let idl = Idl::parse(LEGACY).unwrap();
        let mut payload = discriminator("event", "Fill").to_vec();
        payload.extend([1u8; 32]);
        payload.push(2);
        payload.extend(u64::MAX.to_le_bytes());
        payload.extend([1u8]);
        payload.extend((-5i64).to_le_bytes());
        let (name, fields) = idl.decode_event(&payload).unwrap();
        assert_eq!(name, "Fill");
        let fields = fields.unwrap();
        assert_eq!(fields["side"], "Short");
        assert_eq!(fields["size"], "18446744073709551615");
        assert_eq!(fields["fee"], "-5");
        assert_eq!(fields["key"], bs58::encode([1u8; 32]).into_string());
        payload.push(0);
        assert_eq!(
            idl.decode_event(&payload).unwrap().1,
            Err(DecodeError::Trailing(1))
        );
        payload.truncate(20);
        assert!(matches!(
            idl.decode_event(&payload).unwrap().1,
            Err(DecodeError::Short(_))
        ));

        let mut account = discriminator("account", "Pool").to_vec();
        account.extend(3u32.to_le_bytes());
        account.extend(b"JLP");
        account.extend(7u128.to_le_bytes());
        account.extend([0u8; 100]);
        let (name, decoded) = idl.decode_account(&account).unwrap();
        assert_eq!(name, "Pool");
        let (fields, used) = decoded.unwrap();
        assert_eq!(fields, json!({ "name": "JLP", "aum": "7" }));
        assert_eq!(used, 8 + 4 + 3 + 16);
        assert_eq!(
            idl.instruction_name(&discriminator("global", "increase_position4")),
            Some("increasePosition4")
        );
    }

    #[test]
    fn current_dialect_uses_listed_discriminators_and_typed_events() {
        let idl = Idl::parse(r#"{
          "events": [{ "name": "Swap", "discriminator": [1,2,3,4,5,6,7,8] }],
          "types": [{ "name": "Swap", "type": { "kind": "struct", "fields": [
            { "name": "who", "type": "pubkey" }, { "name": "amounts", "type": { "array": ["u16", 2] } },
            { "name": "tags", "type": { "vec": "u8" } } ] } }]
        }"#).unwrap();
        let mut payload = vec![1, 2, 3, 4, 5, 6, 7, 8];
        payload.extend([0u8; 32]);
        payload.extend([1, 0, 2, 0]);
        payload.extend(2u32.to_le_bytes());
        payload.extend([0xab, 0xcd]);
        let (name, fields) = idl.decode_event(&payload).unwrap();
        assert_eq!(name, "Swap");
        assert_eq!(fields.unwrap()["amounts"], json!([1, 2]));
        assert!(idl.decode_event(&[9; 16]).is_none());
    }
}
