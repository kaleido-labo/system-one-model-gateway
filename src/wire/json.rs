//! Helpers for JSON the gateway forwards without re-encoding.
//!
//! The vendor reads `state`, `instructions` and `criteria` as text, so the
//! gateway never round-trips them through `serde_json::Value`: that would
//! reorder keys and respell numbers. It works on the raw text instead.

/// Removes insignificant whitespace from valid JSON text.
///
/// Every other byte is kept, so key order, number spelling and string escapes
/// survive. Two payloads that differ only in indentation minify to the same
/// text, which is what lets the coalescer recognise the same state sent by two
/// services with different JSON encoders.
pub fn minify(raw: &str) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let mut in_string = false;
    let mut escaped = false;
    for &byte in raw.as_bytes() {
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b' ' | b'\n' | b'\r' | b'\t' => {}
            b'"' => {
                in_string = true;
                out.push(byte);
            }
            _ => out.push(byte),
        }
    }
    // Only ASCII whitespace outside strings was dropped, and no UTF-8
    // continuation byte is ASCII, so the bytes are still valid UTF-8.
    String::from_utf8(out).expect("minify only removes ASCII whitespace")
}

/// The JSON type of a value, read from its first significant byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    String,
    Object,
    Array,
    Number,
    Bool,
    Null,
}

impl JsonKind {
    /// Reads the kind of `raw`, which must be valid JSON.
    pub fn of(raw: &str) -> Self {
        match raw.trim_start().as_bytes().first() {
            Some(b'"') => Self::String,
            Some(b'{') => Self::Object,
            Some(b'[') => Self::Array,
            Some(b't' | b'f') => Self::Bool,
            Some(b'n') => Self::Null,
            _ => Self::Number,
        }
    }

    /// Whether this is one of the three shapes the vendor accepts for
    /// `state`, `instructions` and descriptions: string, object or array.
    pub fn is_text_like(self) -> bool {
        matches!(self, Self::String | Self::Object | Self::Array)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::String => "a string",
            Self::Object => "an object",
            Self::Array => "an array",
            Self::Number => "a number",
            Self::Bool => "a boolean",
            Self::Null => "null",
        }
    }
}

/// Writes a JSON object whose values are already-encoded JSON text.
pub struct ObjectWriter {
    buf: String,
    empty: bool,
}

impl ObjectWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut buf = String::with_capacity(capacity + 2);
        buf.push('{');
        Self { buf, empty: true }
    }

    /// Appends `"key": value`, where `value` must be valid JSON text.
    pub fn field(&mut self, key: &str, value: &str) {
        if !self.empty {
            self.buf.push(',');
        }
        self.empty = false;
        self.buf
            .push_str(&serde_json::to_string(key).expect("a str always serializes"));
        self.buf.push(':');
        self.buf.push_str(value);
    }

    pub fn finish(mut self) -> String {
        self.buf.push('}');
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minify_drops_whitespace_outside_strings_only() {
        let raw = "{\n  \"a b\" : [ 1 , 2.50 ],\n\t\"c\" : \"x  y\"\r\n}";
        assert_eq!(minify(raw), r#"{"a b":[1,2.50],"c":"x  y"}"#);
    }

    #[test]
    fn minify_keeps_escaped_quotes_and_backslashes() {
        let raw = r#"{ "q" : "say \"hi\" \\ ok " , "n" : "\\" }"#;
        assert_eq!(minify(raw), r#"{"q":"say \"hi\" \\ ok ","n":"\\"}"#);
    }

    #[test]
    fn minify_keeps_key_order_and_number_spelling() {
        assert_eq!(minify(r#"{ "b": 1e3, "a": 1.0 }"#), r#"{"b":1e3,"a":1.0}"#);
    }

    #[test]
    fn minify_keeps_non_ascii_text() {
        assert_eq!(
            minify("{ \"note\" : \"frais de péage — 12 €\" }"),
            "{\"note\":\"frais de péage — 12 €\"}"
        );
    }

    #[test]
    fn kind_is_read_from_the_first_byte() {
        assert_eq!(JsonKind::of(r#" "x""#), JsonKind::String);
        assert_eq!(JsonKind::of("{}"), JsonKind::Object);
        assert_eq!(JsonKind::of("[1]"), JsonKind::Array);
        assert_eq!(JsonKind::of("-3"), JsonKind::Number);
        assert_eq!(JsonKind::of("false"), JsonKind::Bool);
        assert_eq!(JsonKind::of("null"), JsonKind::Null);
        assert!(!JsonKind::of("12").is_text_like());
    }

    #[test]
    fn object_writer_escapes_keys_and_keeps_values_verbatim() {
        let mut writer = ObjectWriter::with_capacity(32);
        writer.field("a\"b", "1");
        writer.field("c", r#"{"d":[true]}"#);
        assert_eq!(writer.finish(), r#"{"a\"b":1,"c":{"d":[true]}}"#);
    }

    #[test]
    fn object_writer_handles_the_empty_object() {
        assert_eq!(ObjectWriter::with_capacity(0).finish(), "{}");
    }
}
