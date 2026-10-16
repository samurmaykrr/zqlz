//! Lossless-as-possible JSON form of a [`Value`] for agent and IPC responses.
//!
//! `Value`'s `Display` is for human grids: it prints SQL NULL as the text
//! "NULL", arrays as "[2 items]" and bytes as "<4 bytes>", so a consumer cannot
//! tell NULL from the string 'NULL' or read an array at all.

use serde_json::{Number, Value as Json};

use crate::Value;

/// Bytes shown per cell before the hex is cut off.
const MAX_BYTES_IN_CELL: usize = 4096;

/// Largest integer magnitude every JSON consumer reads back exactly. JavaScript
/// (the MCP clients, `jq`) parses numbers as doubles, so wider BIGINTs would
/// silently lose digits.
const MAX_SAFE_JSON_INT: i64 = (1 << 53) - 1;

impl Value {
    /// Typed JSON for responses to agents and the CLI.
    ///
    /// NULL is JSON `null`; numbers and booleans stay typed; NaN and infinite
    /// floats (not representable in JSON), integers beyond 2^53 and every
    /// exact-text type (decimal, uuid, dates) become strings, so nothing is
    /// rounded on the way; JSON values are embedded as-is; bytes are
    /// `"\x<hex>"` (Postgres bytea text form), cut at 4096 bytes with a note.
    pub fn to_wire_json(&self) -> Json {
        match self {
            Value::Null => Json::Null,
            Value::Bool(v) => Json::Bool(*v),
            Value::Int8(v) => Json::from(*v),
            Value::Int16(v) => Json::from(*v),
            Value::Int32(v) => Json::from(*v),
            Value::Int64(v) if v.unsigned_abs() > MAX_SAFE_JSON_INT as u64 => {
                Json::String(v.to_string())
            }
            Value::Int64(v) => Json::from(*v),
            // f32 -> f64 would print 0.1f32 as 0.10000000149011612.
            Value::Float32(v) => Number::from_f64(v.to_string().parse().unwrap_or(f64::NAN))
                .map_or_else(|| Json::String(v.to_string()), Json::Number),
            Value::Float64(v) => {
                Number::from_f64(*v).map_or_else(|| Json::String(v.to_string()), Json::Number)
            }
            Value::Decimal(v) | Value::String(v) => Json::String(v.clone()),
            Value::Bytes(bytes) => Json::String(bytes_text(bytes)),
            Value::Uuid(v) => Json::String(v.to_string()),
            Value::Date(v) => Json::String(v.to_string()),
            Value::Time(v) => Json::String(v.to_string()),
            Value::DateTime(v) => Json::String(v.to_string()),
            Value::DateTimeUtc(v) => Json::String(v.to_rfc3339()),
            Value::Json(v) => v.clone(),
            Value::Array(items) => Json::Array(items.iter().map(Value::to_wire_json).collect()),
        }
    }
}

fn bytes_text(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let shown = &bytes[..bytes.len().min(MAX_BYTES_IN_CELL)];
    let mut text = String::with_capacity(2 + shown.len() * 2);
    text.push_str("\\x");
    for byte in shown {
        let _ = write!(text, "{byte:02x}");
    }
    if shown.len() < bytes.len() {
        let _ = write!(text, "... (truncated, {} bytes total)", bytes.len());
    }
    text
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use super::MAX_SAFE_JSON_INT;
    use crate::Value;

    #[test]
    fn null_is_json_null_not_the_text_null() {
        assert_eq!(Value::Null.to_wire_json(), json!(null));
        assert_eq!(Value::String("NULL".into()).to_wire_json(), json!("NULL"));
    }

    #[test]
    fn scalars_keep_their_json_types() {
        assert_eq!(Value::Bool(true).to_wire_json(), json!(true));
        assert_eq!(Value::Int16(-3).to_wire_json(), json!(-3));
        assert_eq!(Value::Int64(MAX_SAFE_JSON_INT).to_wire_json(), json!(MAX_SAFE_JSON_INT));
        assert_eq!(Value::Float64(1.5).to_wire_json(), json!(1.5));
        assert_eq!(Value::Float32(0.1).to_wire_json(), json!(0.1));
        assert_eq!(Value::Decimal("12.3400".into()).to_wire_json(), json!("12.3400"));
        assert_eq!(
            Value::Date(NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()).to_wire_json(),
            json!("2024-02-29")
        );
    }

    #[test]
    fn bigints_a_double_cannot_hold_become_strings() {
        // 2^53 + 1 is the first integer a JSON double reads back wrongly.
        assert_eq!(
            Value::Int64(MAX_SAFE_JSON_INT + 1).to_wire_json(),
            json!("9007199254740992")
        );
        assert_eq!(Value::Int64(i64::MAX).to_wire_json(), json!("9223372036854775807"));
        assert_eq!(Value::Int64(i64::MIN).to_wire_json(), json!("-9223372036854775808"));
        assert_eq!(Value::Int64(-MAX_SAFE_JSON_INT).to_wire_json(), json!(-MAX_SAFE_JSON_INT));
        // MySQL BIGINT UNSIGNED above i64::MAX arrives as exact text.
        assert_eq!(
            Value::Decimal("18446744073709551615".into()).to_wire_json(),
            json!("18446744073709551615")
        );
    }

    #[test]
    fn temporal_values_are_exact_text() {
        use chrono::{NaiveDateTime, NaiveTime};
        let time = NaiveTime::from_hms_micro_opt(12, 34, 56, 500_000).unwrap();
        assert_eq!(Value::Time(time).to_wire_json(), json!("12:34:56.500"));
        let datetime = NaiveDateTime::parse_from_str("2024-01-02 03:04:05.25", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
        assert_eq!(Value::DateTime(datetime).to_wire_json(), json!("2024-01-02 03:04:05.250"));
    }

    #[test]
    fn non_finite_floats_become_strings() {
        assert_eq!(Value::Float64(f64::NAN).to_wire_json(), json!("NaN"));
        assert_eq!(Value::Float64(f64::INFINITY).to_wire_json(), json!("inf"));
        assert_eq!(Value::Float32(f32::NEG_INFINITY).to_wire_json(), json!("-inf"));
    }

    #[test]
    fn json_is_embedded_and_arrays_recurse() {
        assert_eq!(
            Value::Json(json!({"a": [1, null]})).to_wire_json(),
            json!({"a": [1, null]})
        );
        let array = Value::Array(vec![
            Value::Int32(1),
            Value::Null,
            Value::Array(vec![Value::String("x".into())]),
        ]);
        assert_eq!(array.to_wire_json(), json!([1, null, ["x"]]));
    }

    #[test]
    fn bytes_are_hex_and_large_payloads_are_cut() {
        assert_eq!(Value::Bytes(vec![0, 255, 16]).to_wire_json(), json!("\\x00ff10"));

        let big = Value::Bytes(vec![0xab; 5000]).to_wire_json();
        let text = big.as_str().unwrap();
        assert!(text.starts_with("\\xabab"));
        assert!(text.ends_with("... (truncated, 5000 bytes total)"));
        assert_eq!(text.len(), 2 + 4096 * 2 + "... (truncated, 5000 bytes total)".len());
    }
}
