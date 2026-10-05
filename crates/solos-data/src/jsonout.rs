//! Ordered JSON objects, timestamps and the JSON log line, matching the TypeScript output byte
//! for byte: objects keep insertion order, `at` is an ISO-8601 instant with milliseconds, and
//! error text never carries a provider URL.

use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;
use std::io::Write;

/// One entry of an [`Obj`]: a plain JSON value, a nested ordered object, or a list of ordered
/// objects (DuckDB rows), so column order survives into `status.json`, `catalog.json` and query
/// results exactly as `getRowObjectsJson()` produced it.
#[derive(Clone, Debug, PartialEq)]
pub enum Slot {
    /// A JSON value (nested objects inside it sort their keys).
    Value(Value),
    /// A nested ordered object.
    Obj(Obj),
    /// An array of ordered objects.
    Rows(Vec<Obj>),
}

impl Serialize for Slot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Slot::Value(value) => value.serialize(serializer),
            Slot::Obj(obj) => obj.serialize(serializer),
            Slot::Rows(rows) => {
                let mut seq = serializer.serialize_seq(Some(rows.len()))?;
                for row in rows {
                    seq.serialize_element(row)?;
                }
                seq.end()
            }
        }
    }
}

/// A JSON object whose keys stay in insertion order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Obj(Vec<(String, Slot)>);

impl Obj {
    /// An empty object.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder form of [`Obj::set`].
    #[must_use]
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.set(key, value);
        self
    }

    /// Builder: a nested ordered object.
    #[must_use]
    pub fn with_obj(mut self, key: &str, value: Obj) -> Self {
        self.put(key, Slot::Obj(value));
        self
    }

    /// Builder: an array of ordered objects.
    #[must_use]
    pub fn with_rows(mut self, key: &str, rows: Vec<Obj>) -> Self {
        self.put(key, Slot::Rows(rows));
        self
    }

    /// Set a key, replacing an existing value in place (JavaScript spread semantics).
    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        self.put(key, Slot::Value(value.into()));
    }

    /// Set a nested ordered object.
    pub fn set_obj(&mut self, key: &str, value: Obj) {
        self.put(key, Slot::Obj(value));
    }

    /// Set an array of ordered objects.
    pub fn set_rows(&mut self, key: &str, rows: Vec<Obj>) {
        self.put(key, Slot::Rows(rows));
    }

    fn put(&mut self, key: &str, slot: Slot) {
        match self.0.iter_mut().find(|(k, _)| k == key) {
            Some(entry) => entry.1 = slot,
            None => self.0.push((key.to_owned(), slot)),
        }
    }

    /// Append every entry of another object (`{...a, ...b}`).
    pub fn extend(&mut self, other: Obj) {
        for (key, slot) in other.0 {
            self.put(&key, slot);
        }
    }

    /// Look a plain value up (nested objects and row lists are not values).
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self.0.iter().find(|(k, _)| k == key) {
            Some((_, Slot::Value(value))) => Some(value),
            _ => None,
        }
    }

    /// A string value, when the key holds one.
    #[must_use]
    pub fn str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Value::as_str)
    }

    /// A number rendered by DuckDB as a string or a number, parsed as `i64`.
    #[must_use]
    pub fn int(&self, key: &str) -> Option<i64> {
        match self.get(key)? {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Plain-value entries in order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().filter_map(|(k, slot)| match slot {
            Slot::Value(value) => Some((k.as_str(), value)),
            _ => None,
        })
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the object has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The same entries as a `serde_json::Value` (nested keys sort), for callers that need a
    /// plain value.
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("JSON object serializes")
    }

    /// Serialize to a compact JSON string.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("JSON object serializes")
    }
}

impl Serialize for Obj {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, slot) in &self.0 {
            map.serialize_entry(key, slot)?;
        }
        map.end()
    }
}

impl From<Obj> for Value {
    fn from(obj: Obj) -> Self {
        obj.to_value()
    }
}

/// The current instant as `new Date().toISOString()` renders it.
#[must_use]
pub fn now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// One JSON log line on stdout: `{"at":...,"event":...,...fields}`.
pub fn log(event: &str, fields: Obj) {
    let mut line = Obj::new().with("at", now()).with("event", event);
    line.extend(fields);
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", line.to_json());
}

/// Error text safe for logs: URLs redacted, 300 characters at most.
#[must_use]
pub fn safe_error(message: &str) -> String {
    let mut out = String::new();
    let mut rest = message;
    while let Some(start) = rest
        .find("http://")
        .into_iter()
        .chain(rest.find("https://"))
        .min()
    {
        out.push_str(&rest[..start]);
        out.push_str("[redacted-url]");
        let tail = &rest[start..];
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'')
            .unwrap_or(tail.len());
        rest = &tail[end..];
    }
    out.push_str(rest);
    out.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_insertion_order_and_spreads() {
        let mut obj = Obj::new().with("b", 1).with("a", 2);
        obj.extend(Obj::new().with("b", 3).with("c", Value::Null));
        obj.set_rows("rows", vec![Obj::new().with("z", 1).with("y", 2)]);
        obj.set_obj("nested", Obj::new().with("q", true).with("p", false));
        assert_eq!(
            obj.to_json(),
            r#"{"b":3,"a":2,"c":null,"rows":[{"z":1,"y":2}],"nested":{"q":true,"p":false}}"#
        );
    }

    #[test]
    fn redacts_urls_and_caps_length() {
        assert_eq!(
            safe_error("RPC https://x.y/v2/secret failed"),
            "RPC [redacted-url] failed"
        );
        assert_eq!(safe_error(&"a".repeat(400)).len(), 300);
    }

    #[test]
    fn iso_instant_has_milliseconds() {
        let at = now();
        assert_eq!(at.len(), 24);
        assert!(at.ends_with('Z'));
    }
}
