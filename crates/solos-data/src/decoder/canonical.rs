//! Length-prefixed encoding for the content digest.
//!
//! Every field is a tag, and every variable-length payload is a big-endian `u32` length followed
//! by its bytes. Values are never concatenated. `NULL` is a tag of its own, distinct from an
//! empty string. Floats are IEEE-754 bits. Decimals are a normalized digit string.

use serde_json::Value;

const NULL: u8 = 0x00;
const BOOL: u8 = 0x01;
const INT: u8 = 0x02;
const UINT: u8 = 0x03;
const FLOAT: u8 = 0x04;
const DECIMAL: u8 = 0x05;
const TEXT: u8 = 0x06;
/// `decoded_at` keeps its schema slot and contributes no clock value.
const OMITTED: u8 = 0x0f;

/// One encoded value. Used by the collision test and the row encoder.
#[derive(Clone, Debug)]
pub(crate) enum Field {
    /// SQL NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer, fixed 16-byte big-endian two's complement.
    Int(i128),
    /// Unsigned integer, fixed 16-byte big-endian.
    Uint(u128),
    /// IEEE-754 binary64, big-endian bits.
    Float(f64),
    /// Decimal text, normalized before it is length-prefixed.
    Decimal(String),
    /// UTF-8 text, length-prefixed.
    Text(String),
}

/// Append a big-endian length and `bytes`.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), String> {
    let len = u32::try_from(bytes.len()).map_err(|_| "digest field longer than u32".to_owned())?;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Append one tagged field.
pub(crate) fn put_field(buf: &mut Vec<u8>, field: &Field) -> Result<(), String> {
    match field {
        Field::Null => buf.push(NULL),
        Field::Bool(value) => {
            buf.push(BOOL);
            buf.push(u8::from(*value));
        }
        Field::Int(value) => {
            buf.push(INT);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        Field::Uint(value) => {
            buf.push(UINT);
            buf.extend_from_slice(&value.to_be_bytes());
        }
        Field::Float(value) => {
            buf.push(FLOAT);
            buf.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Field::Decimal(value) => {
            buf.push(DECIMAL);
            put_bytes(buf, canonical_decimal(value).as_bytes())?;
        }
        Field::Text(value) => {
            buf.push(TEXT);
            put_bytes(buf, value.as_bytes())?;
        }
    }
    Ok(())
}

/// Schema record: table name, then each column's name and type in order.
pub(crate) fn put_schema(
    buf: &mut Vec<u8>,
    table: &str,
    columns: &[(String, String)],
) -> Result<(), String> {
    put_bytes(buf, table.as_bytes())?;
    let count = u32::try_from(columns.len()).map_err(|_| "too many columns".to_owned())?;
    buf.extend_from_slice(&count.to_be_bytes());
    for (name, ty) in columns {
        put_bytes(buf, name.as_bytes())?;
        put_bytes(buf, ty.as_bytes())?;
    }
    Ok(())
}

/// Encode one row in schema order. `decoded_at` is the omitted tag, not the stored clock.
pub(crate) fn put_row(
    buf: &mut Vec<u8>,
    columns: &[(String, String)],
    row: &crate::jsonout::Obj,
) -> Result<(), String> {
    let count = u32::try_from(columns.len()).map_err(|_| "too many columns".to_owned())?;
    buf.extend_from_slice(&count.to_be_bytes());
    for (name, ty) in columns {
        if name == "decoded_at" {
            buf.push(OMITTED);
            continue;
        }
        let value = row.get(name).unwrap_or(&Value::Null);
        put_field(buf, &typed_field(ty, value)?)?;
    }
    Ok(())
}

fn typed_field(ty: &str, value: &Value) -> Result<Field, String> {
    if value.is_null() {
        return Ok(Field::Null);
    }
    match ty {
        "BOOLEAN" => Ok(Field::Bool(
            value.as_bool().ok_or_else(|| "boolean column".to_owned())?,
        )),
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" => {
            Ok(Field::Int(parse_i128(value)?))
        }
        "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" | "UHUGEINT" => {
            Ok(Field::Uint(parse_u128(value)?))
        }
        "FLOAT" | "REAL" | "DOUBLE" => {
            let number = value.as_f64().ok_or_else(|| "float column".to_owned())?;
            Ok(Field::Float(number))
        }
        "DECIMAL" => Ok(Field::Decimal(canonical_decimal(&value_text(value)))),
        _ => Ok(Field::Text(value_text(value))),
    }
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn parse_i128(value: &Value) -> Result<i128, String> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from))
            .ok_or_else(|| format!("integer {number}")),
        Value::String(text) => text.parse().map_err(|_| format!("integer {text}")),
        other => Err(format!("integer {other}")),
    }
}

fn parse_u128(value: &Value) -> Result<u128, String> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .map(u128::from)
            .or_else(|| number.as_i64().and_then(|n| u128::try_from(n).ok()))
            .ok_or_else(|| format!("unsigned {number}")),
        Value::String(text) => text.parse().map_err(|_| format!("unsigned {text}")),
        other => Err(format!("unsigned {other}")),
    }
}

/// `1.00` and `01.0` and `-0` become `1` and `0`. The sign is kept only when the value is not zero.
pub(crate) fn canonical_decimal(raw: &str) -> String {
    let neg = raw.starts_with('-');
    let raw = raw.trim_start_matches(['+', '-']).trim();
    let (int, frac) = raw.split_once('.').unwrap_or((raw, ""));
    let int = int.trim_start_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let frac = frac.trim_end_matches('0');
    let body = if frac.is_empty() {
        int.to_owned()
    } else {
        format!("{int}.{frac}")
    };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn digest(fields: &[Field]) -> String {
        let mut buf = Vec::new();
        for field in fields {
            put_field(&mut buf, field).unwrap();
        }
        hex::encode(Sha256::digest(buf))
    }

    #[test]
    fn length_prefixes_distinguish_rows_that_join_to_the_same_string() {
        let joined = format!("{}{}", "ab", "c");
        assert_eq!(joined, format!("{}{}", "a", "bc"), "naive join collides");
        let left = digest(&[Field::Text("ab".into()), Field::Text("c".into())]);
        let right = digest(&[Field::Text("a".into()), Field::Text("bc".into())]);
        assert_ne!(left, right);
    }

    #[test]
    fn null_is_not_an_empty_string_and_decimals_have_one_rendering() {
        assert_ne!(
            digest(&[Field::Null]),
            digest(&[Field::Text(String::new())])
        );
        assert_eq!(canonical_decimal("1.00"), "1");
        assert_eq!(canonical_decimal("01.10"), "1.1");
        assert_eq!(canonical_decimal("-0.0"), "0");
        assert_eq!(
            digest(&[Field::Decimal("1.00".into())]),
            digest(&[Field::Decimal("01".into())])
        );
        assert_ne!(digest(&[Field::Float(0.0)]), digest(&[Field::Float(-0.0)]));
        assert_ne!(digest(&[Field::Int(1)]), digest(&[Field::Text("1".into())]));
    }

    #[test]
    fn schema_name_type_and_order_change_the_record() {
        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut retyped = Vec::new();
        put_schema(
            &mut left,
            "events",
            &[
                ("a".into(), "VARCHAR".into()),
                ("b".into(), "BIGINT".into()),
            ],
        )
        .unwrap();
        put_schema(
            &mut right,
            "events",
            &[
                ("b".into(), "BIGINT".into()),
                ("a".into(), "VARCHAR".into()),
            ],
        )
        .unwrap();
        put_schema(&mut retyped, "events", &[("a".into(), "INTEGER".into())]).unwrap();
        let mut original = Vec::new();
        put_schema(&mut original, "events", &[("a".into(), "VARCHAR".into())]).unwrap();
        assert_ne!(left, right);
        assert_ne!(original, retyped);
    }
}
