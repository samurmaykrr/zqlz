//! Core types for ZQLZ

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Broad semantic category for database column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlTypeFamily {
    Text,
    Number,
    Boolean,
    Temporal,
    Json,
    Binary,
    Uuid,
    Enum,
    Network,
    Geometry,
    Array,
    Range,
    Unknown,
}

/// Numeric subtype used by validation and editors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlNumericKind {
    Integer,
    Float,
    Decimal,
}

/// Temporal subtype used by date/time editors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlTemporalKind {
    Date,
    Time,
    DateTime,
}

/// Canonical semantic information derived from database type metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlTypeInfo {
    pub family: SqlTypeFamily,
    pub numeric_kind: Option<SqlNumericKind>,
    pub temporal_kind: Option<SqlTemporalKind>,
    pub is_range: bool,
    pub is_array: bool,
    pub base_type: String,
    pub normalized_type: String,
}

impl SqlTypeInfo {
    pub fn from_column_meta(meta: &ColumnMeta) -> Self {
        let mut info = Self::from_data_type(&meta.data_type);
        if meta
            .enum_values
            .as_ref()
            .is_some_and(|values| !values.is_empty())
        {
            info.family = SqlTypeFamily::Enum;
        }
        info
    }

    pub fn from_data_type(data_type: &str) -> Self {
        let normalized_type = normalize_sql_type_name(data_type);
        let array_element_type = Value::array_element_type(&normalized_type);
        let is_array = array_element_type.is_some();
        let classified_type = array_element_type.as_deref().unwrap_or(&normalized_type);
        let base_type = strip_sql_type_modifiers(classified_type).to_string();
        let is_range = is_range_type(&base_type);
        let is_boolean = matches!(base_type.as_str(), "bool" | "boolean" | "bit")
            || normalized_type == "tinyint(1)";
        let numeric_kind = if is_boolean {
            None
        } else {
            classify_numeric_kind(&base_type)
        };
        let temporal_kind = classify_temporal_kind(&base_type, classified_type);

        let family = if is_array {
            SqlTypeFamily::Array
        } else if is_boolean {
            SqlTypeFamily::Boolean
        } else if numeric_kind.is_some() {
            SqlTypeFamily::Number
        } else if matches!(base_type.as_str(), "uuid" | "uniqueidentifier" | "guid") {
            SqlTypeFamily::Uuid
        } else if matches!(base_type.as_str(), "json" | "jsonb" | "bson") {
            SqlTypeFamily::Json
        } else if is_binary_type(&base_type) {
            SqlTypeFamily::Binary
        } else if temporal_kind.is_some() {
            SqlTypeFamily::Temporal
        } else if matches!(base_type.as_str(), "inet" | "cidr" | "macaddr" | "macaddr8") {
            SqlTypeFamily::Network
        } else if is_geometry_type(&base_type) {
            SqlTypeFamily::Geometry
        } else if is_range {
            SqlTypeFamily::Range
        } else if is_text_type(&base_type) {
            SqlTypeFamily::Text
        } else if base_type.starts_with("enum") || base_type.starts_with("set") {
            SqlTypeFamily::Enum
        } else {
            SqlTypeFamily::Unknown
        };

        Self {
            family,
            numeric_kind,
            temporal_kind,
            is_range,
            is_array,
            base_type,
            normalized_type,
        }
    }

    pub fn display_label_for_column(meta: &ColumnMeta) -> String {
        let data_type = meta.data_type.trim();
        if data_type.is_empty() {
            return "unknown".to_string();
        }

        if let (Some(precision), Some(scale)) = (meta.precision, meta.scale)
            && !data_type.contains('(')
            && matches!(
                strip_sql_type_modifiers(&data_type.to_ascii_lowercase()),
                "numeric" | "decimal"
            )
        {
            return format!("{}({},{})", data_type, precision, scale);
        }

        if let Some(max_length) = meta.max_length
            && max_length > 0
            && !data_type.contains('(')
            && matches!(
                strip_sql_type_modifiers(&data_type.to_ascii_lowercase()),
                "char" | "varchar" | "nchar" | "nvarchar" | "binary" | "varbinary"
            )
        {
            return format!("{}({})", data_type, max_length);
        }

        data_type.to_string()
    }

    pub fn is_scalar_temporal(&self) -> bool {
        self.temporal_kind.is_some() && !self.is_range && !self.is_array
    }
}

fn normalize_sql_type_name(data_type: &str) -> String {
    let lower = data_type.trim().to_ascii_lowercase();
    match lower.as_str() {
        "boolean" => "bool".to_string(),
        "integer" | "int" => "int4".to_string(),
        "bigint" => "int8".to_string(),
        "smallint" => "int2".to_string(),
        "decimal" => "numeric".to_string(),
        "double" | "double precision" => "float8".to_string(),
        "real" => "float4".to_string(),
        "datetime" => "timestamp".to_string(),
        "timestamp with time zone" => "timestamptz".to_string(),
        "timestamp without time zone" => "timestamp".to_string(),
        "time without time zone" => "time".to_string(),
        "time with time zone" => "timetz".to_string(),
        "blob" | "tinyblob" | "mediumblob" | "longblob" => "bytea".to_string(),
        "json" | "jsonb" | "jsonarray" => lower,
        _ => lower,
    }
}

/// Normalize a plain decimal string to (negative, integer digits, fraction
/// digits) with padding removed, so `1.50`, `1.5` and `+1.5` all agree.
///
/// Returns `None` for anything that is not a plain number, including exponent
/// notation, leaving callers to fall back to text comparison.
fn canonical_number(text: &str) -> Option<(bool, String, String)> {
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };

    let (integer, fraction) = match digits.find('.') {
        Some(index) => (&digits[..index], &digits[index + 1..]),
        None => (digits, ""),
    };

    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let integer = integer.trim_start_matches('0').to_string();
    let fraction = fraction.trim_end_matches('0').to_string();
    // "-0" and "0" are the same number.
    let negative = negative && !(integer.is_empty() && fraction.is_empty());

    Some((negative, integer, fraction))
}

fn strip_sql_type_modifiers(data_type: &str) -> &str {
    match data_type.find('(') {
        Some(index) => data_type[..index].trim(),
        None => data_type.trim(),
    }
}

fn classify_numeric_kind(base_type: &str) -> Option<SqlNumericKind> {
    if matches!(
        base_type,
        "int2"
            | "int4"
            | "int8"
            | "smallint"
            | "integer"
            | "bigint"
            | "int"
            | "mediumint"
            | "tinyint"
            | "serial"
            | "bigserial"
            | "smallserial"
    ) {
        return Some(SqlNumericKind::Integer);
    }

    if matches!(
        base_type,
        "float4" | "float8" | "real" | "double precision" | "double" | "float"
    ) {
        return Some(SqlNumericKind::Float);
    }

    if matches!(base_type, "numeric" | "decimal" | "money") {
        return Some(SqlNumericKind::Decimal);
    }

    None
}

fn classify_temporal_kind(base_type: &str, normalized_type: &str) -> Option<SqlTemporalKind> {
    if matches!(base_type, "date" | "daterange" | "datemultirange") {
        return Some(SqlTemporalKind::Date);
    }

    if matches!(base_type, "time" | "timetz")
        || normalized_type.starts_with("time without")
        || normalized_type.starts_with("time with")
    {
        return Some(SqlTemporalKind::Time);
    }

    if matches!(
        base_type,
        "datetime"
            | "datetime2"
            | "smalldatetime"
            | "datetimeoffset"
            | "timestamp"
            | "timestamptz"
            | "tsrange"
            | "tstzrange"
            | "tsmultirange"
            | "tstzmultirange"
    ) || normalized_type.starts_with("timestamp without")
        || normalized_type.starts_with("timestamp with")
    {
        return Some(SqlTemporalKind::DateTime);
    }

    None
}

fn is_range_type(base_type: &str) -> bool {
    base_type.ends_with("range") || base_type.ends_with("multirange")
}

fn is_binary_type(base_type: &str) -> bool {
    matches!(
        base_type,
        "blob"
            | "mediumblob"
            | "longblob"
            | "tinyblob"
            | "bytea"
            | "binary"
            | "varbinary"
            | "image"
            | "raw"
    )
}

fn is_text_type(base_type: &str) -> bool {
    matches!(
        base_type,
        "text"
            | "varchar"
            | "char"
            | "bpchar"
            | "name"
            | "citext"
            | "character varying"
            | "character"
            | "nvarchar"
            | "nchar"
            | "longtext"
            | "mediumtext"
            | "tinytext"
            | "string"
    )
}

fn is_geometry_type(base_type: &str) -> bool {
    matches!(
        base_type,
        "point"
            | "line"
            | "lseg"
            | "box"
            | "path"
            | "polygon"
            | "circle"
            | "geometry"
            | "geography"
    )
}

/// A database value that can represent any SQL type
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    #[default]
    Null,
    /// Boolean
    Bool(bool),
    /// 8-bit signed integer
    Int8(i8),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 32-bit floating point
    Float32(f32),
    /// 64-bit floating point
    Float64(f64),
    /// Decimal/Numeric (stored as string for precision)
    Decimal(String),
    /// UTF-8 string
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// UUID
    Uuid(Uuid),
    /// Date (year, month, day)
    Date(NaiveDate),
    /// Time (hour, minute, second, nanosecond)
    Time(NaiveTime),
    /// DateTime without timezone
    DateTime(NaiveDateTime),
    /// DateTime with timezone (UTC)
    DateTimeUtc(DateTime<Utc>),
    /// JSON value
    Json(serde_json::Value),
    /// Array of values
    Array(Vec<Value>),
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl Value {
    pub fn normalize_data_type(data_type: &str) -> String {
        normalize_sql_type_name(data_type)
    }

    pub fn is_json_data_type(data_type: &str) -> bool {
        matches!(
            Self::normalize_data_type(data_type).as_str(),
            "json" | "jsonb"
        )
    }

    fn strip_type_modifiers(data_type: &str) -> &str {
        strip_sql_type_modifiers(data_type)
    }

    /// The digits behind a numeric value, or `None` when it is not a number.
    fn numeric_text(&self) -> Option<String> {
        match self {
            Value::Int8(v) => Some(v.to_string()),
            Value::Int16(v) => Some(v.to_string()),
            Value::Int32(v) => Some(v.to_string()),
            Value::Int64(v) => Some(v.to_string()),
            Value::Float32(v) => Some(v.to_string()),
            Value::Float64(v) => Some(v.to_string()),
            Value::Decimal(v) if Value::is_sql_numeric_literal(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Whether two values denote the same data, treating numeric variants that
    /// hold the same number as equal.
    ///
    /// Editors compare what the user typed against what was read from the
    /// database, and those can arrive in different variants — a DECIMAL column
    /// reads back as [`Value::Decimal`] but parses from text as a float — so a
    /// derived equality check would report a change where there is none and
    /// issue a pointless UPDATE.
    pub fn is_equivalent_to(&self, other: &Value) -> bool {
        if self == other {
            return true;
        }

        match (self.numeric_text(), other.numeric_text()) {
            (Some(left), Some(right)) => {
                match (canonical_number(&left), canonical_number(&right)) {
                    (Some(left), Some(right)) => left == right,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// Whether `text` is safe to emit into SQL as a bare numeric literal.
    ///
    /// `Value::Decimal` holds its digits as a string to preserve precision, and
    /// drivers emit it unquoted. Anything that is not strictly a number would
    /// therefore be injected as SQL, so callers must check before emitting.
    /// Accepts optional sign, digits with at most one decimal point, and an
    /// optional exponent. Rejects `NaN`, `inf`, whitespace and separators.
    pub fn is_sql_numeric_literal(text: &str) -> bool {
        let mut characters = text.chars().peekable();

        if matches!(characters.peek(), Some('+' | '-')) {
            characters.next();
        }

        let mut mantissa_digits = 0usize;
        let mut seen_point = false;
        while let Some(&character) = characters.peek() {
            match character {
                '0'..='9' => {
                    mantissa_digits += 1;
                    characters.next();
                }
                '.' if !seen_point => {
                    seen_point = true;
                    characters.next();
                }
                _ => break,
            }
        }

        if mantissa_digits == 0 {
            return false;
        }

        if matches!(characters.peek(), Some('e' | 'E')) {
            characters.next();
            if matches!(characters.peek(), Some('+' | '-')) {
                characters.next();
            }
            let mut exponent_digits = 0usize;
            while matches!(characters.peek(), Some('0'..='9')) {
                exponent_digits += 1;
                characters.next();
            }
            if exponent_digits == 0 {
                return false;
            }
        }

        characters.next().is_none()
    }

    pub fn array_element_type(data_type: &str) -> Option<String> {
        if let Some(element_type) = data_type.strip_suffix("[]").map(str::trim) {
            return Some(Self::normalize_data_type(element_type));
        }

        let element_type = data_type.strip_suffix("array")?.trim();
        if element_type.is_empty() {
            return None;
        }

        Some(match element_type {
            "bigint" => "int8".to_string(),
            "bool" | "boolean" => "bool".to_string(),
            "byte" | "bytea" | "binary" | "bytes" => "bytea".to_string(),
            "datetime" => "timestamp".to_string(),
            "decimal" => "numeric".to_string(),
            "double" => "float8".to_string(),
            "integer" | "int" => "int4".to_string(),
            "smallint" => "int2".to_string(),
            "string" => "text".to_string(),
            other => Self::normalize_data_type(other),
        })
    }

    pub fn is_array_data_type(data_type: &str) -> bool {
        let normalized = Self::normalize_data_type(data_type);
        Self::array_element_type(&normalized).is_some()
    }

    pub fn requires_string_round_trip(data_type: &str) -> bool {
        let normalized = Self::normalize_data_type(data_type);
        let base_type = Self::strip_type_modifiers(&normalized);

        if Self::is_json_data_type(base_type)
            || Self::is_array_data_type(&normalized)
            || Self::is_string_data_type(base_type)
        {
            return false;
        }

        if base_type.ends_with("range") || base_type.ends_with("multirange") {
            return true;
        }

        matches!(
            base_type,
            "inet"
                | "cidr"
                | "macaddr"
                | "macaddr8"
                | "interval"
                | "bit"
                | "varbit"
                | "bit varying"
                | "hstore"
                | "point"
                | "line"
                | "lseg"
                | "box"
                | "path"
                | "polygon"
                | "circle"
                | "geometry"
                | "geography"
                | "ltree"
                | "lquery"
                | "ltxtquery"
                | "xml"
        )
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(value) => serde_json::Value::Bool(*value),
            Value::Int8(value) => serde_json::json!(value),
            Value::Int16(value) => serde_json::json!(value),
            Value::Int32(value) => serde_json::json!(value),
            Value::Int64(value) => serde_json::json!(value),
            Value::Float32(value) => serde_json::json!(value),
            Value::Float64(value) => serde_json::json!(value),
            Value::Decimal(value) => value
                .parse::<f64>()
                .map(|value| serde_json::json!(value))
                .unwrap_or_else(|_| serde_json::Value::String(value.clone())),
            Value::String(value) => serde_json::Value::String(value.clone()),
            Value::Bytes(value) => {
                let hex: String = value.iter().map(|byte| format!("{:02x}", byte)).collect();
                serde_json::Value::String(format!("0x{}", hex))
            }
            Value::Uuid(value) => serde_json::Value::String(value.to_string()),
            Value::Date(value) => serde_json::Value::String(value.to_string()),
            Value::Time(value) => {
                serde_json::Value::String(value.format("%H:%M:%S%.f").to_string())
            }
            Value::DateTime(value) => {
                serde_json::Value::String(value.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }
            Value::DateTimeUtc(value) => serde_json::Value::String(value.to_rfc3339()),
            Value::Json(value) => value.clone(),
            Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(Value::to_json_value).collect())
            }
        }
    }

    fn parse_hex_bytes(input: &str) -> Option<Vec<u8>> {
        let hex = input.trim().strip_prefix("0x")?;
        if hex.len() % 2 != 0 {
            return None;
        }

        let mut bytes = Vec::with_capacity(hex.len() / 2);
        let mut index = 0;
        while index < hex.len() {
            let byte = u8::from_str_radix(&hex[index..index + 2], 16).ok()?;
            bytes.push(byte);
            index += 2;
        }

        Some(bytes)
    }

    fn parse_date(input: &str) -> Option<Value> {
        ["%Y-%m-%d"]
            .into_iter()
            .find_map(|format| NaiveDate::parse_from_str(input, format).ok())
            .map(Value::Date)
    }

    fn parse_time(input: &str) -> Option<Value> {
        ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"]
            .into_iter()
            .find_map(|format| NaiveTime::parse_from_str(input, format).ok())
            .map(Value::Time)
    }

    fn parse_datetime(input: &str) -> Option<Value> {
        [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
        ]
        .into_iter()
        .find_map(|format| NaiveDateTime::parse_from_str(input, format).ok())
        .map(Value::DateTime)
    }

    fn parse_datetime_utc(input: &str) -> Option<Value> {
        DateTime::parse_from_rfc3339(input)
            .map(|value| Value::DateTimeUtc(value.with_timezone(&Utc)))
            .ok()
            .or_else(|| {
                DateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f %Z")
                    .map(|value| Value::DateTimeUtc(value.with_timezone(&Utc)))
                    .ok()
            })
            .or_else(|| {
                Self::parse_datetime(input).and_then(|value| match value {
                    Value::DateTime(value) => Some(Value::DateTimeUtc(value.and_utc())),
                    _ => None,
                })
            })
    }

    fn parse_json_with_type_hint(value: &serde_json::Value, data_type: &str) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(value) => Value::Bool(*value),
            serde_json::Value::Number(value) => {
                Value::parse_from_string(&value.to_string(), data_type)
            }
            serde_json::Value::String(value) => Value::parse_from_string(value, data_type),
            serde_json::Value::Array(values) => Value::Array(
                values
                    .iter()
                    .map(|value| Self::parse_json_with_type_hint(value, data_type))
                    .collect(),
            ),
            serde_json::Value::Object(_) => Value::Json(value.clone()),
        }
    }

    fn split_array_items(input: &str) -> Option<Vec<String>> {
        let trimmed = input.trim();
        let inner = trimmed.strip_prefix('[')?.strip_suffix(']')?;

        if inner.trim().is_empty() {
            return Some(Vec::new());
        }

        let mut items = Vec::new();
        let mut current = String::new();
        let mut in_string = false;
        let mut escape = false;
        let mut depth = 0usize;

        for character in inner.chars() {
            if in_string {
                current.push(character);
                if escape {
                    escape = false;
                    continue;
                }

                match character {
                    '\\' => escape = true,
                    '"' => in_string = false,
                    _ => {}
                }

                continue;
            }

            match character {
                '"' => {
                    in_string = true;
                    current.push(character);
                }
                '[' | '{' => {
                    depth += 1;
                    current.push(character);
                }
                ']' | '}' => {
                    depth = depth.saturating_sub(1);
                    current.push(character);
                }
                ',' if depth == 0 => {
                    items.push(current.trim().to_string());
                    current.clear();
                }
                _ => current.push(character),
            }
        }

        items.push(current.trim().to_string());
        Some(items)
    }

    fn parse_array(input: &str, element_type: &str) -> Option<Value> {
        if let Ok(serde_json::Value::Array(values)) =
            serde_json::from_str::<serde_json::Value>(input.trim())
        {
            return Some(Value::Array(
                values
                    .iter()
                    .map(|value| Self::parse_json_with_type_hint(value, element_type))
                    .collect(),
            ));
        }

        let items = Self::split_array_items(input)?;
        let values = items
            .into_iter()
            .map(|item| {
                let trimmed = item.trim();
                if trimmed.eq_ignore_ascii_case("null") {
                    Value::Null
                } else if trimmed.starts_with('"') && trimmed.ends_with('"') {
                    serde_json::from_str::<String>(trimmed)
                        .map(|value| Value::parse_from_string(&value, element_type))
                        .unwrap_or_else(|_| Value::parse_from_string(trimmed, element_type))
                } else {
                    Value::parse_from_string(trimmed, element_type)
                }
            })
            .collect();

        Some(Value::Array(values))
    }

    fn format_table_preview_string(value: &str, max_chars: usize) -> String {
        let mut chars = value.chars();
        let preview: String = chars.by_ref().take(max_chars).collect();
        if chars.next().is_some() {
            format!("{}…", preview)
        } else {
            preview
        }
    }

    fn format_nested_preview(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::String(value) => format!("\"{}\"", value.replace('"', "\\\"")),
            Value::Bytes(bytes) => Self::format_bytes_preview(bytes),
            Value::Json(value) => value.to_string(),
            Value::Array(values) => Self::format_array_preview(values, 8),
            Value::Date(value) => value.to_string(),
            Value::Time(value) => value.format("%H:%M:%S%.f").to_string(),
            Value::DateTime(value) => value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            Value::DateTimeUtc(value) => value.format("%Y-%m-%d %H:%M:%S%.f UTC").to_string(),
            other => other.to_string(),
        }
    }

    fn format_bytes_preview(bytes: &[u8]) -> String {
        const PREVIEW_BYTE_COUNT: usize = 16;

        let preview_hex: String = bytes
            .iter()
            .take(PREVIEW_BYTE_COUNT)
            .map(|byte| format!("{:02x}", byte))
            .collect();

        if bytes.len() > PREVIEW_BYTE_COUNT {
            format!("0x{}… ({} bytes)", preview_hex, bytes.len())
        } else {
            format!("0x{} ({} bytes)", preview_hex, bytes.len())
        }
    }

    fn format_array_preview(values: &[Value], max_items: usize) -> String {
        let mut rendered_items: Vec<String> = values
            .iter()
            .take(max_items)
            .map(Value::format_nested_preview)
            .collect();

        if values.len() > max_items {
            rendered_items.push("…".to_string());
        }

        format!("[{}]", rendered_items.join(", "))
    }

    fn format_array_full(values: &[Value]) -> String {
        let rendered_items: Vec<String> = values.iter().map(Value::format_nested_preview).collect();
        format!("[{}]", rendered_items.join(", "))
    }

    fn is_string_data_type(base_type: &str) -> bool {
        matches!(
            base_type,
            "text"
                | "varchar"
                | "char"
                | "bpchar"
                | "name"
                | "citext"
                | "character varying"
                | "character"
                | "nvarchar"
                | "nchar"
                | "longtext"
                | "mediumtext"
                | "tinytext"
                | "enum"
                | "set"
        )
    }

    /// Check if the value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int8(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::Decimal(v) => v.parse::<f64>().ok(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as a string array
    pub fn as_string_array(&self) -> Option<Vec<String>> {
        match self {
            Value::Array(arr) => Some(
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Return a display-ready string for rendering in the UI.
    ///
    /// Unlike `Display`, which returns "NULL" for null and "<N bytes>" for
    /// binary data, this method returns specialized placeholder strings that
    /// the table renderer can detect (e.g. `"BLOB"` for bytes).
    pub fn display_for_table(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bytes(bytes) => Self::format_bytes_preview(bytes),
            Value::Array(values) => Self::format_array_preview(values, 8),
            Value::Json(value) => value.to_string(),
            Value::Time(value) => value.format("%H:%M:%S%.f").to_string(),
            Value::DateTime(value) => value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            Value::DateTimeUtc(value) => value.format("%Y-%m-%d %H:%M:%S%.f UTC").to_string(),
            Value::String(value) => Self::format_table_preview_string(value, 240),
            other => other.to_string(),
        }
    }

    /// Return a readable string that preserves the full logical value.
    ///
    /// This is used by editor flows and tooltips where losing detail would make
    /// round-tripping or inspection harder than necessary.
    pub fn display_for_editor(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bytes(bytes) => {
                let hex: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("0x{}", hex)
            }
            Value::Array(values) => Self::format_array_full(values),
            Value::Json(value) => value.to_string(),
            Value::Time(value) => value.format("%H:%M:%S%.f").to_string(),
            Value::DateTime(value) => value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            Value::DateTimeUtc(value) => value.format("%Y-%m-%d %H:%M:%S%.f UTC").to_string(),
            other => other.to_string(),
        }
    }

    /// Parse a user-entered string back into a typed `Value` based on column
    /// metadata. The `data_type` parameter is the database column type (e.g.
    /// "integer", "boolean", "varchar", "timestamp", etc.).
    ///
    /// Empty strings and the literal "NULL" (case-insensitive) produce `Value::Null`
    /// for non-string columns. String-like columns preserve the literal input.
    pub fn parse_from_string(input: &str, data_type: &str) -> Value {
        let lower = Self::normalize_data_type(data_type);
        if let Some(element_type) = Self::array_element_type(&lower)
            && let Some(value) = Self::parse_array(input, &element_type)
        {
            return value;
        }

        if Self::strip_type_modifiers(&lower) == "set"
            && let Ok(serde_json::Value::Array(values)) =
                serde_json::from_str::<serde_json::Value>(input.trim())
        {
            return Value::Array(
                values
                    .iter()
                    .map(|value| Self::parse_json_with_type_hint(value, "text"))
                    .collect(),
            );
        }

        let base_type = Self::strip_type_modifiers(&lower);

        if input.is_empty() || input.eq_ignore_ascii_case("null") {
            if Self::is_string_data_type(base_type) || Self::requires_string_round_trip(base_type) {
                return Value::String(input.to_string());
            }

            return Value::Null;
        }

        match base_type {
            "boolean" | "bool" | "tinyint"
                if lower == "tinyint(1)" || base_type == "boolean" || base_type == "bool" =>
            {
                match input.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" | "on" => Value::Bool(true),
                    "false" | "f" | "0" | "no" | "n" | "off" => Value::Bool(false),
                    _ => Value::String(input.to_string()),
                }
            }
            "int2" | "smallint" | "smallserial" => input
                .parse::<i16>()
                .map(Value::Int16)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "int4" | "integer" | "int" | "mediumint" | "serial" => input
                .parse::<i32>()
                .map(Value::Int32)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "int8" | "bigint" | "bigserial" => input
                .parse::<i64>()
                .map(Value::Int64)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "long" => input
                .parse::<i64>()
                .map(Value::Int64)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "tinyint" => input
                .parse::<i8>()
                .map(Value::Int8)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "float4" | "real" | "float" => input
                .parse::<f32>()
                .map(Value::Float32)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "float8" | "double precision" | "double" => input
                .parse::<f64>()
                .map(Value::Float64)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            // Kept as digits rather than routed through f64, which cannot hold
            // the full width of a wide DECIMAL. Only genuine numbers may become
            // Decimal: drivers emit that variant unquoted, so anything else has
            // to travel as an escaped string.
            "numeric" | "decimal" | "money" => {
                if Value::is_sql_numeric_literal(input) {
                    Value::Decimal(input.to_string())
                } else {
                    Value::String(input.to_string())
                }
            }
            "json" | "jsonb" => serde_json::from_str::<serde_json::Value>(input)
                .map(Value::Json)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "uuid" => uuid::Uuid::parse_str(input)
                .map(Value::Uuid)
                .unwrap_or_else(|_| Value::String(input.to_string())),
            "date" => Self::parse_date(input).unwrap_or_else(|| Value::String(input.to_string())),
            "time" | "timetz" | "time without time zone" | "time with time zone" => {
                Self::parse_time(input).unwrap_or_else(|| Value::String(input.to_string()))
            }
            "timestamp" | "datetime" | "timestamp without time zone" | "smalldatetime" => {
                Self::parse_datetime(input).unwrap_or_else(|| Value::String(input.to_string()))
            }
            "timestamptz" | "timestamp with time zone" => {
                Self::parse_datetime_utc(input).unwrap_or_else(|| Value::String(input.to_string()))
            }
            "objectid" | "object_id" | "string" | "regex" | "javascript" | "symbol"
            | "dbpointer" | "minkey" | "maxkey" => Value::String(input.to_string()),
            "bytea" | "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
                Self::parse_hex_bytes(input)
                    .map(Value::Bytes)
                    .unwrap_or_else(|| Value::String(input.to_string()))
            }
            _ => Value::String(input.to_string()),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int8(v) => write!(f, "{}", v),
            Value::Int16(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Decimal(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::DateTimeUtc(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", v),
            Value::Array(v) => write!(f, "[{} items]", v.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SqlNumericKind, SqlTemporalKind, SqlTypeFamily, SqlTypeInfo, Value};
    use chrono::NaiveDate;

    /// A DECIMAL column reads back as `Decimal` but the editor may hand back a
    /// float, and that variant mismatch used to look like a real edit — which is
    /// why re-typing an unchanged decimal still issued an UPDATE.
    #[test]
    fn is_equivalent_to_ignores_numeric_variant() {
        assert!(Value::Decimal("-6.0000".to_string()).is_equivalent_to(&Value::Float64(-6.0)));
        assert!(Value::Float64(-6.0).is_equivalent_to(&Value::Decimal("-6.0000".to_string())));
        assert!(Value::Decimal("7".to_string()).is_equivalent_to(&Value::Int32(7)));
        assert!(Value::Int64(7).is_equivalent_to(&Value::Decimal("007".to_string())));
        assert!(Value::Decimal("-0".to_string()).is_equivalent_to(&Value::Int32(0)));
    }

    #[test]
    fn is_equivalent_to_still_reports_real_changes() {
        assert!(!Value::Decimal("-6.0000".to_string()).is_equivalent_to(&Value::Float64(-6.00001)));
        assert!(!Value::Int32(7).is_equivalent_to(&Value::Int32(8)));
        assert!(
            !Value::String("7".to_string()).is_equivalent_to(&Value::Int32(7)),
            "text and numbers are different data"
        );
        assert!(!Value::Null.is_equivalent_to(&Value::Int32(0)));
    }

    #[test]
    fn is_sql_numeric_literal_rejects_sql_payloads() {
        assert!(!Value::is_sql_numeric_literal("0 WHERE 1=1 -- "));
        assert!(!Value::is_sql_numeric_literal("1; DROP TABLE users"));
        assert!(!Value::is_sql_numeric_literal("abc"));
        assert!(!Value::is_sql_numeric_literal(""));
        assert!(!Value::is_sql_numeric_literal("NaN"));
        assert!(!Value::is_sql_numeric_literal("inf"));
        assert!(!Value::is_sql_numeric_literal("1 2"));
        assert!(!Value::is_sql_numeric_literal("1,000"));
        assert!(!Value::is_sql_numeric_literal("1.2.3"));
        assert!(!Value::is_sql_numeric_literal("1e"));
        assert!(!Value::is_sql_numeric_literal(" 1"));
    }

    #[test]
    fn is_sql_numeric_literal_accepts_numbers() {
        assert!(Value::is_sql_numeric_literal("-6.0000"));
        assert!(Value::is_sql_numeric_literal("+.5"));
        assert!(Value::is_sql_numeric_literal("1e10"));
        assert!(Value::is_sql_numeric_literal("1E-10"));
        assert!(Value::is_sql_numeric_literal("0"));
        assert!(Value::is_sql_numeric_literal("20000000000000000001.5"));
    }

    /// A decimal column must never turn unparseable input into `Value::Decimal`,
    /// which drivers emit into SQL unquoted.
    #[test]
    fn parse_from_string_does_not_make_decimals_from_sql_payloads() {
        assert_eq!(
            Value::parse_from_string("0 WHERE 1=1 -- ", "decimal(12,4)"),
            Value::String("0 WHERE 1=1 -- ".to_string())
        );
    }

    #[test]
    fn parse_from_string_preserves_literal_empty_and_null_for_text_columns() {
        assert_eq!(
            Value::parse_from_string("", "text"),
            Value::String(String::new())
        );
        assert_eq!(
            Value::parse_from_string("NULL", "varchar(255)"),
            Value::String("NULL".to_string())
        );
    }

    #[test]
    fn parse_from_string_still_maps_empty_and_null_to_null_for_non_string_columns() {
        assert_eq!(Value::parse_from_string("", "integer"), Value::Null);
        assert_eq!(Value::parse_from_string("NULL", "boolean"), Value::Null);
    }

    #[test]
    fn display_for_table_formats_arrays_with_content_preview() {
        let value = Value::Array(vec![
            Value::Int32(1),
            Value::String("two".to_string()),
            Value::Null,
        ]);

        assert_eq!(value.display_for_table(), "[1, \"two\", NULL]");
    }

    #[test]
    fn display_for_table_formats_bytes_with_hex_preview() {
        let value = Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]);

        assert_eq!(value.display_for_table(), "0xdeadbeef (4 bytes)");
        assert_eq!(value.display_for_editor(), "0xdeadbeef");
    }

    #[test]
    fn parse_from_string_parses_json_timestamp_and_bytes() {
        assert_eq!(
            Value::parse_from_string("{\"enabled\":true}", "jsonb"),
            Value::Json(serde_json::json!({ "enabled": true }))
        );
        assert_eq!(
            Value::parse_from_string("2024-03-15 10:11:12", "timestamp"),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .expect("valid test date")
                    .and_hms_opt(10, 11, 12)
                    .expect("valid test time")
            )
        );
        assert_eq!(
            Value::parse_from_string("0xdeadbeef", "bytea"),
            Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn parse_from_string_parses_array_editor_output() {
        assert_eq!(
            Value::parse_from_string("[1, 2, NULL]", "int4[]"),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Null])
        );
        assert_eq!(
            Value::parse_from_string("[\"one\", \"2\"]", "TextArray"),
            Value::Array(vec![
                Value::String("one".to_string()),
                Value::String("2".to_string())
            ])
        );
        assert_eq!(
            Value::parse_from_string("[]", "TextArray"),
            Value::Array(vec![])
        );
        assert_eq!(
            Value::parse_from_string("[true, false, null]", "BoolArray"),
            Value::Array(vec![Value::Bool(true), Value::Bool(false), Value::Null])
        );
        assert_eq!(
            Value::parse_from_string("[1, 2]", "IntArray"),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );
        assert_eq!(
            Value::parse_from_string("[\"550e8400-e29b-41d4-a716-446655440000\"]", "UuidArray"),
            Value::Array(vec![Value::Uuid(
                uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
            )])
        );
        assert_eq!(
            Value::parse_from_string("[{\"enabled\":true}]", "JsonArray"),
            Value::Array(vec![Value::Json(serde_json::json!({ "enabled": true }))])
        );
        assert_eq!(
            Value::parse_from_string("[{\"enabled\":true}]", "jsonb[]"),
            Value::Array(vec![Value::Json(serde_json::json!({ "enabled": true }))])
        );
        // Numeric elements keep their digits so wide values survive the trip.
        assert_eq!(
            Value::parse_from_string("[12.5, null]", "NumericArray"),
            Value::Array(vec![Value::Decimal("12.5".to_string()), Value::Null])
        );
        assert_eq!(
            Value::parse_from_string("[\"0xdeadbeef\"]", "ByteaArray"),
            Value::Array(vec![Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])])
        );
        assert_eq!(
            Value::parse_from_string("[\"14:30:00\"]", "TimeArray"),
            Value::Array(vec![Value::Time(
                chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap()
            )])
        );
        assert_eq!(
            Value::parse_from_string("[\"read\", \"write\"]", "set"),
            Value::Array(vec![
                Value::String("read".to_string()),
                Value::String("write".to_string())
            ])
        );
    }

    #[test]
    fn parse_from_string_round_trips_special_scalars_as_strings() {
        assert_eq!(
            Value::parse_from_string("192.168.0.1", "inet"),
            Value::String("192.168.0.1".to_string())
        );
        assert_eq!(
            Value::parse_from_string("[1,4)", "int4range"),
            Value::String("[1,4)".to_string())
        );
        assert_eq!(
            Value::parse_from_string("1 day", "interval"),
            Value::String("1 day".to_string())
        );
        assert_eq!(
            Value::parse_from_string("1010", "bit(4)"),
            Value::String("1010".to_string())
        );
    }

    #[test]
    fn parse_from_string_handles_mongodb_type_names() {
        assert_eq!(Value::parse_from_string("42", "long"), Value::Int64(42));
        assert_eq!(
            Value::parse_from_string("12.5", "double"),
            Value::Float64(12.5)
        );
        assert_eq!(
            Value::parse_from_string("507f1f77bcf86cd799439011", "objectId"),
            Value::String("507f1f77bcf86cd799439011".to_string())
        );
        assert_eq!(
            Value::parse_from_string("/tenant-.*/i", "regex"),
            Value::String("/tenant-.*/i".to_string())
        );
    }

    #[test]
    fn sql_type_info_classifies_core_database_types() {
        let cases = [
            (
                "date",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::Date),
                false,
            ),
            (
                "time",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::Time),
                false,
            ),
            (
                "timestamp",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::DateTime),
                false,
            ),
            (
                "timestamptz",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::DateTime),
                false,
            ),
            (
                "timestamp(6) with time zone",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::DateTime),
                false,
            ),
            (
                "daterange",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::Date),
                true,
            ),
            (
                "tsrange",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::DateTime),
                true,
            ),
            (
                "tstzrange",
                SqlTypeFamily::Temporal,
                None,
                Some(SqlTemporalKind::DateTime),
                true,
            ),
            (
                "int4",
                SqlTypeFamily::Number,
                Some(SqlNumericKind::Integer),
                None,
                false,
            ),
            (
                "integer",
                SqlTypeFamily::Number,
                Some(SqlNumericKind::Integer),
                None,
                false,
            ),
            (
                "numeric(18,4)",
                SqlTypeFamily::Number,
                Some(SqlNumericKind::Decimal),
                None,
                false,
            ),
            ("tinyint(1)", SqlTypeFamily::Boolean, None, None, false),
            ("uuid", SqlTypeFamily::Uuid, None, None, false),
            ("jsonb", SqlTypeFamily::Json, None, None, false),
            ("bytea", SqlTypeFamily::Binary, None, None, false),
            ("varchar(255)", SqlTypeFamily::Text, None, None, false),
        ];

        for (data_type, family, numeric_kind, temporal_kind, is_range) in cases {
            let info = SqlTypeInfo::from_data_type(data_type);
            assert_eq!(info.family, family, "{data_type}");
            assert_eq!(info.numeric_kind, numeric_kind, "{data_type}");
            assert_eq!(info.temporal_kind, temporal_kind, "{data_type}");
            assert_eq!(info.is_range, is_range, "{data_type}");
        }
    }
}

/// A row from a query result
#[derive(Debug, Clone)]
pub struct Row {
    /// Column values
    pub values: Vec<Value>,
    /// Column names (shared reference)
    columns: Vec<String>,
}

impl Row {
    /// Create a new row
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { values, columns }
    }

    /// Get a value by column index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a value by column name
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == name)
            .and_then(|idx| self.values.get(idx))
    }

    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Convert to a HashMap
    pub fn to_map(&self) -> HashMap<String, Value> {
        self.columns
            .iter()
            .zip(self.values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnMeta {
    /// Column name
    #[serde(default)]
    pub name: String,
    /// Data type (database-specific string)
    #[serde(default)]
    pub data_type: String,
    /// Whether the column can be NULL
    #[serde(default)]
    pub nullable: bool,
    /// Column ordinal position (0-based)
    #[serde(default)]
    pub ordinal: usize,
    /// Maximum character length (for string types)
    #[serde(default)]
    pub max_length: Option<i64>,
    /// Numeric precision
    #[serde(default)]
    pub precision: Option<i32>,
    /// Numeric scale
    #[serde(default)]
    pub scale: Option<i32>,
    /// Whether the column is auto-increment
    #[serde(default)]
    pub auto_increment: bool,
    /// Default value expression
    #[serde(default)]
    pub default_value: Option<String>,
    /// Column comment/description
    #[serde(default)]
    pub comment: Option<String>,
    /// Enum values (for enum/set types)
    /// PostgreSQL: fetched from pg_enum
    /// MySQL: parsed from ENUM('a','b','c') type definition
    #[serde(default)]
    pub enum_values: Option<Vec<String>>,
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Unique query ID
    pub id: Uuid,
    /// Column metadata
    pub columns: Vec<ColumnMeta>,
    /// Result rows
    pub rows: Vec<Row>,
    /// Total row count (if known)
    pub total_rows: Option<u64>,
    /// Whether `total_rows` is an estimate from database metadata
    /// (e.g. `information_schema.TABLES` for MySQL, `pg_class.reltuples`
    /// for PostgreSQL) rather than an exact COUNT(*).
    pub is_estimated_total: bool,
    /// Rows affected (for DML statements)
    pub affected_rows: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Warnings from the database
    pub warnings: Vec<String>,
}

impl QueryResult {
    /// Create a new empty query result
    pub fn empty() -> Self {
        Self {
            id: Uuid::new_v4(),
            columns: Vec::new(),
            rows: Vec::new(),
            total_rows: None,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    }

    /// Check if the result has rows
    pub fn has_rows(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// Result of a single statement in a batch
#[derive(Debug, Clone)]
pub struct StatementResult {
    /// Whether this was a query (SELECT) or a command (INSERT/UPDATE/DELETE)
    pub is_query: bool,
    /// Query result (if is_query is true)
    pub result: Option<QueryResult>,
    /// Rows affected (if is_query is false)
    pub affected_rows: u64,
    /// Error message (if execution failed)
    pub error: Option<String>,
}
