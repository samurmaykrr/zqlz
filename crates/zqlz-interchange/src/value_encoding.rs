//! Value Encoding/Decoding for UDIF
//!
//! This module handles the conversion between `zqlz_core::Value` and the
//! JSON-serializable `EncodedValue` format used in UDIF documents.
//!
//! The encoding preserves type information so that values can be correctly
//! interpreted during import, even when the target database has different types.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zqlz_core::Value;

/// Errors during value encoding/decoding
#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("Failed to encode value: {0}")]
    EncodeError(String),

    #[error("Failed to decode value: {0}")]
    DecodeError(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid date format: {0}")]
    InvalidDate(String),

    #[error("Invalid time format: {0}")]
    InvalidTime(String),

    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),

    #[error("Invalid base64 data: {0}")]
    InvalidBase64(String),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// An encoded value for JSON serialization
///
/// This type preserves the original type information while being JSON-compatible.
/// Binary data is base64-encoded, and special types use tagged representations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EncodedValue {
    /// Null value
    Null,

    /// Boolean value
    Bool(bool),

    /// Integer value (fits in i64)
    Integer(i64),

    /// Floating-point value
    Float(f64),

    /// String value
    String(String),

    /// Tagged value for types that need explicit type information
    Tagged(TaggedValue),
}

/// A value with explicit type tag for disambiguation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaggedValue {
    /// Type identifier
    #[serde(rename = "$type")]
    pub type_tag: TypeTag,
    /// The encoded value
    #[serde(rename = "$value")]
    pub value: serde_json::Value,
}

/// Type tags for special values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeTag {
    /// 8-bit integer
    Int8,
    /// 16-bit integer
    Int16,
    /// 32-bit integer
    Int32,
    /// 64-bit integer
    Int64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// Decimal (arbitrary precision)
    Decimal,
    /// Binary data (base64 encoded)
    Binary,
    /// UUID
    Uuid,
    /// Date (ISO 8601)
    Date,
    /// Time (ISO 8601)
    Time,
    /// DateTime without timezone
    DateTime,
    /// DateTime with timezone (UTC)
    DateTimeUtc,
    /// JSON value
    Json,
    /// Array of values
    Array,
    /// IEEE 754 special float: NaN, Infinity, or -Infinity.
    ///
    /// JSON cannot represent these natively; silently mapping them to 0 would
    /// corrupt the data on round-trip. We preserve them as tagged strings so the
    /// decoder can reconstruct the exact IEEE 754 value.
    FloatSpecial,
}

impl EncodedValue {
    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, EncodedValue::Null)
    }

    /// Try to get as a string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            EncodedValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as an integer
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EncodedValue::Integer(i) => Some(*i),
            EncodedValue::Tagged(TaggedValue {
                type_tag: TypeTag::Int8 | TypeTag::Int16 | TypeTag::Int32 | TypeTag::Int64,
                value,
            }) => value.as_i64(),
            _ => None,
        }
    }

    /// Try to get as a float
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            EncodedValue::Float(f) => Some(*f),
            EncodedValue::Integer(i) => Some(*i as f64),
            EncodedValue::Tagged(TaggedValue {
                type_tag: TypeTag::Float32 | TypeTag::Float64,
                value,
            }) => value.as_f64(),
            _ => None,
        }
    }

    /// Try to get as a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            EncodedValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

/// Map a non-finite float to the canonical label stored in the tagged encoding.
///
/// Using a fixed set of labels ("NaN", "Infinity", "-Infinity") keeps the
/// format stable and unambiguous across platforms and locale settings.
fn float_special_label(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value.is_infinite() && value.is_sign_positive() {
        "Infinity"
    } else {
        "-Infinity"
    }
}

/// Encode a `Value` into an `EncodedValue`
pub fn encode_value(value: &Value) -> EncodedValue {
    match value {
        Value::Null => EncodedValue::Null,
        Value::Bool(b) => EncodedValue::Bool(*b),

        Value::Int8(v) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Int8,
            value: serde_json::Value::Number((*v as i64).into()),
        }),
        Value::Int16(v) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Int16,
            value: serde_json::Value::Number((*v as i64).into()),
        }),
        Value::Int32(v) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Int32,
            value: serde_json::Value::Number((*v as i64).into()),
        }),
        Value::Int64(v) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Int64,
            value: serde_json::Value::Number((*v).into()),
        }),

        Value::Float32(v) => {
            match serde_json::Number::from_f64(*v as f64) {
                Some(n) => EncodedValue::Tagged(TaggedValue {
                    type_tag: TypeTag::Float32,
                    value: serde_json::Value::Number(n),
                }),
                // JSON cannot represent NaN or infinities, so we encode them
                // as tagged strings to avoid silent data corruption on round-trip.
                None => EncodedValue::Tagged(TaggedValue {
                    type_tag: TypeTag::FloatSpecial,
                    value: serde_json::Value::String(float_special_label(*v as f64).to_string()),
                }),
            }
        }
        Value::Float64(v) => match serde_json::Number::from_f64(*v) {
            Some(n) => EncodedValue::Tagged(TaggedValue {
                type_tag: TypeTag::Float64,
                value: serde_json::Value::Number(n),
            }),
            None => EncodedValue::Tagged(TaggedValue {
                type_tag: TypeTag::FloatSpecial,
                value: serde_json::Value::String(float_special_label(*v).to_string()),
            }),
        },

        Value::Decimal(s) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Decimal,
            value: serde_json::Value::String(s.clone()),
        }),

        Value::String(s) => EncodedValue::String(s.clone()),

        Value::Bytes(b) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Binary,
            value: serde_json::Value::String(BASE64.encode(b)),
        }),

        Value::Uuid(u) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Uuid,
            value: serde_json::Value::String(u.to_string()),
        }),

        Value::Date(d) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Date,
            value: serde_json::Value::String(d.to_string()),
        }),

        Value::Time(t) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Time,
            value: serde_json::Value::String(t.to_string()),
        }),

        Value::DateTime(dt) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::DateTime,
            value: serde_json::Value::String(dt.to_string()),
        }),

        Value::DateTimeUtc(dt) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::DateTimeUtc,
            value: serde_json::Value::String(dt.to_rfc3339()),
        }),

        Value::Json(j) => EncodedValue::Tagged(TaggedValue {
            type_tag: TypeTag::Json,
            value: j.clone(),
        }),

        Value::Array(arr) => {
            let encoded: Vec<serde_json::Value> = arr
                .iter()
                .map(|v| serde_json::to_value(encode_value(v)).unwrap_or(serde_json::Value::Null))
                .collect();
            EncodedValue::Tagged(TaggedValue {
                type_tag: TypeTag::Array,
                value: serde_json::Value::Array(encoded),
            })
        }
    }
}

/// Decode an `EncodedValue` back to a `Value`
pub fn decode_value(encoded: &EncodedValue) -> Result<Value, EncodingError> {
    match encoded {
        EncodedValue::Null => Ok(Value::Null),
        EncodedValue::Bool(b) => Ok(Value::Bool(*b)),
        EncodedValue::Integer(i) => Ok(Value::Int64(*i)),
        EncodedValue::Float(f) => Ok(Value::Float64(*f)),
        EncodedValue::String(s) => Ok(Value::String(s.clone())),

        EncodedValue::Tagged(tagged) => decode_tagged_value(tagged),
    }
}

fn decode_tagged_value(tagged: &TaggedValue) -> Result<Value, EncodingError> {
    match tagged.type_tag {
        TypeTag::Int8 => {
            let i = tagged
                .value
                .as_i64()
                .ok_or_else(|| EncodingError::DecodeError("Expected integer".into()))?;
            Ok(Value::Int8(i as i8))
        }
        TypeTag::Int16 => {
            let i = tagged
                .value
                .as_i64()
                .ok_or_else(|| EncodingError::DecodeError("Expected integer".into()))?;
            Ok(Value::Int16(i as i16))
        }
        TypeTag::Int32 => {
            let i = tagged
                .value
                .as_i64()
                .ok_or_else(|| EncodingError::DecodeError("Expected integer".into()))?;
            Ok(Value::Int32(i as i32))
        }
        TypeTag::Int64 => {
            let i = tagged
                .value
                .as_i64()
                .ok_or_else(|| EncodingError::DecodeError("Expected integer".into()))?;
            Ok(Value::Int64(i))
        }

        TypeTag::Float32 => {
            let f = tagged
                .value
                .as_f64()
                .ok_or_else(|| EncodingError::DecodeError("Expected float".into()))?;
            Ok(Value::Float32(f as f32))
        }
        TypeTag::Float64 => {
            let f = tagged
                .value
                .as_f64()
                .ok_or_else(|| EncodingError::DecodeError("Expected float".into()))?;
            Ok(Value::Float64(f))
        }

        TypeTag::Decimal => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected string for decimal".into()))?;
            Ok(Value::Decimal(s.to_string()))
        }

        TypeTag::Binary => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected base64 string".into()))?;
            let bytes = BASE64
                .decode(s)
                .map_err(|e| EncodingError::InvalidBase64(e.to_string()))?;
            Ok(Value::Bytes(bytes))
        }

        TypeTag::Uuid => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected UUID string".into()))?;
            let uuid =
                uuid::Uuid::parse_str(s).map_err(|e| EncodingError::InvalidUuid(e.to_string()))?;
            Ok(Value::Uuid(uuid))
        }

        TypeTag::Date => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected date string".into()))?;
            let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| EncodingError::InvalidDate(e.to_string()))?;
            Ok(Value::Date(date))
        }

        TypeTag::Time => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected time string".into()))?;
            let time = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                .map_err(|e| EncodingError::InvalidTime(e.to_string()))?;
            Ok(Value::Time(time))
        }

        TypeTag::DateTime => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected datetime string".into()))?;
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .map_err(|e| EncodingError::InvalidDate(e.to_string()))?;
            Ok(Value::DateTime(dt))
        }

        TypeTag::DateTimeUtc => {
            let s = tagged
                .value
                .as_str()
                .ok_or_else(|| EncodingError::DecodeError("Expected datetime string".into()))?;
            let dt = chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|e| EncodingError::InvalidDate(e.to_string()))?;
            Ok(Value::DateTimeUtc(dt.with_timezone(&chrono::Utc)))
        }

        TypeTag::Json => Ok(Value::Json(tagged.value.clone())),

        TypeTag::Array => {
            let arr = tagged
                .value
                .as_array()
                .ok_or_else(|| EncodingError::DecodeError("Expected array".into()))?;
            let values: Result<Vec<Value>, _> = arr
                .iter()
                .map(|v| {
                    let encoded: EncodedValue =
                        serde_json::from_value(v.clone()).map_err(EncodingError::JsonError)?;
                    decode_value(&encoded)
                })
                .collect();
            Ok(Value::Array(values?))
        }

        TypeTag::FloatSpecial => {
            let label = tagged.value.as_str().ok_or_else(|| {
                EncodingError::DecodeError("Expected string for float_special".into())
            })?;
            // The label is intentionally limited to these three values to keep
            // the format stable and parseable without locale-dependent formatting.
            match label {
                "NaN" => Ok(Value::Float64(f64::NAN)),
                "Infinity" => Ok(Value::Float64(f64::INFINITY)),
                "-Infinity" => Ok(Value::Float64(f64::NEG_INFINITY)),
                other => Err(EncodingError::DecodeError(format!(
                    "Unknown float_special value: {other}"
                ))),
            }
        }
    }
}

/// Encode a row of values
pub fn encode_row(values: &[Value]) -> Vec<EncodedValue> {
    values.iter().map(encode_value).collect()
}

/// Decode a row of encoded values
pub fn decode_row(encoded: &[EncodedValue]) -> Result<Vec<Value>, EncodingError> {
    encoded.iter().map(decode_value).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, Utc};

    #[test]
    fn test_encode_decode_primitives() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int32(42),
            Value::Int64(1234567890123),
            Value::Float64(3.14159),
            Value::String("hello world".into()),
        ];

        for value in values {
            let encoded = encode_value(&value);
            let decoded = decode_value(&encoded).expect("decode");
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_encode_decode_binary() {
        let bytes = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let value = Value::Bytes(bytes.clone());

        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).expect("decode");

        assert_eq!(Value::Bytes(bytes), decoded);
    }

    #[test]
    fn test_encode_decode_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let value = Value::Uuid(uuid);

        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).expect("decode");

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_date_time() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        let datetime = date.and_time(time);
        let datetime_utc = Utc::now();

        let values = vec![
            Value::Date(date),
            Value::Time(time),
            Value::DateTime(datetime),
            Value::DateTimeUtc(datetime_utc),
        ];

        for value in values {
            let encoded = encode_value(&value);
            let decoded = decode_value(&encoded).expect("decode");

            match (&value, &decoded) {
                (Value::DateTimeUtc(a), Value::DateTimeUtc(b)) => {
                    assert_eq!(a.timestamp(), b.timestamp());
                }
                _ => assert_eq!(value, decoded),
            }
        }
    }

    #[test]
    fn test_encode_decode_array() {
        let array = Value::Array(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::String("three".into()),
        ]);

        let encoded = encode_value(&array);
        let decoded = decode_value(&encoded).expect("decode");

        match decoded {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_json_serialization() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(42),
            Value::String("test".into()),
        ];

        for value in values {
            let encoded = encode_value(&value);
            let json = serde_json::to_string(&encoded).expect("serialize");
            let parsed: EncodedValue = serde_json::from_str(&json).expect("deserialize");
            let decoded = decode_value(&parsed).expect("decode");

            match (&value, &decoded) {
                (Value::Int64(a), Value::Int64(b)) => assert_eq!(a, b),
                (Value::Int32(a), Value::Int64(b)) => assert_eq!(*a as i64, *b),
                _ => assert_eq!(value, decoded),
            }
        }
    }

    #[test]
    fn test_nan_is_not_silently_mapped_to_zero() {
        let encoded = encode_value(&Value::Float64(f64::NAN));
        // Must never produce a numeric 0 — that would silently corrupt the data.
        match &encoded {
            EncodedValue::Tagged(t) => {
                assert_eq!(t.type_tag, TypeTag::FloatSpecial);
                assert_eq!(t.value.as_str(), Some("NaN"));
            }
            other => panic!("Expected Tagged(FloatSpecial), got {other:?}"),
        }
    }

    #[test]
    fn test_infinity_round_trips() {
        for original in [f64::INFINITY, f64::NEG_INFINITY] {
            let encoded = encode_value(&Value::Float64(original));
            let json = serde_json::to_string(&encoded).expect("serialize");
            let parsed: EncodedValue = serde_json::from_str(&json).expect("deserialize");
            let decoded = decode_value(&parsed).expect("decode");
            match decoded {
                Value::Float64(v) => assert_eq!(v, original),
                other => panic!("Expected Float64, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_nan_round_trips() {
        let encoded = encode_value(&Value::Float64(f64::NAN));
        let json = serde_json::to_string(&encoded).expect("serialize");
        let parsed: EncodedValue = serde_json::from_str(&json).expect("deserialize");
        let decoded = decode_value(&parsed).expect("decode");
        match decoded {
            Value::Float64(v) => assert!(v.is_nan()),
            other => panic!("Expected Float64(NaN), got {other:?}"),
        }
    }

    #[test]
    fn test_float32_nan_round_trips() {
        let encoded = encode_value(&Value::Float32(f32::NAN));
        let json = serde_json::to_string(&encoded).expect("serialize");
        let parsed: EncodedValue = serde_json::from_str(&json).expect("deserialize");
        let decoded = decode_value(&parsed).expect("decode");
        // FloatSpecial always decodes to Float64 since the original width is lost
        // after the special-value path; the NaN-ness is what matters.
        match decoded {
            Value::Float64(v) => assert!(v.is_nan()),
            other => panic!("Expected Float64(NaN), got {other:?}"),
        }
    }

    #[test]
    fn test_normal_finite_float_unchanged() {
        let value = Value::Float64(3.14);
        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).expect("decode");
        assert_eq!(value, decoded);
    }
}
