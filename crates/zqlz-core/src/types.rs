//! Core types for ZQLZ

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A database value that can represent any SQL type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
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

impl Value {
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
