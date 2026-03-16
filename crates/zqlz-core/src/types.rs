//! Core types for ZQLZ

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

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
    fn strip_type_modifiers(data_type: &str) -> &str {
        match data_type.find('(') {
            Some(index) => data_type[..index].trim(),
            None => data_type.trim(),
        }
    }

    fn strip_array_suffix(data_type: &str) -> Option<&str> {
        data_type.strip_suffix("[]").map(str::trim)
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
        let lower = data_type.trim().to_lowercase();
        if let Some(element_type) = Self::strip_array_suffix(&lower)
            && let Some(value) = Self::parse_array(input, element_type)
        {
            return value;
        }

        let base_type = Self::strip_type_modifiers(&lower);

        if input.is_empty() || input.eq_ignore_ascii_case("null") {
            if Self::is_string_data_type(base_type) {
                return Value::String(input.to_string());
            }

            return Value::Null;
        }

        match base_type {
            "boolean" | "bool" | "bit" | "tinyint"
                if lower == "tinyint(1)"
                    || base_type == "boolean"
                    || base_type == "bool"
                    || base_type == "bit" =>
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
            "numeric" | "decimal" | "money" => Value::Decimal(input.to_string()),
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
    use super::Value;
    use chrono::NaiveDate;

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
