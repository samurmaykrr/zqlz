//! Canonical Type System for Universal Data Interchange
//!
//! This module defines a type system that can represent any database type.
//! Each database driver maps its native types to/from these canonical types.

use serde::{Deserialize, Serialize};

/// Canonical types that can represent any database type.
///
/// These types serve as a universal intermediate representation. When exporting,
/// drivers map their native types to canonical types. When importing, drivers
/// map canonical types to their native types (with potential type coercion).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "params")]
pub enum CanonicalType {
    // === Null ===
    /// Null/missing value (all databases support this)
    Null,

    // === Boolean ===
    /// Boolean true/false
    Boolean,

    // === Integer Types ===
    /// 8-bit signed integer (-128 to 127)
    /// Maps to: TINYINT (MySQL), smallint (PostgreSQL), INTEGER (SQLite)
    TinyInt,

    /// 16-bit signed integer (-32,768 to 32,767)
    /// Maps to: SMALLINT (most databases)
    SmallInt,

    /// 32-bit signed integer
    /// Maps to: INT/INTEGER (most databases)
    Integer,

    /// 64-bit signed integer
    /// Maps to: BIGINT (most databases), INTEGER (SQLite)
    BigInt,

    /// Unsigned 64-bit integer (for databases that support it)
    /// Maps to: BIGINT UNSIGNED (MySQL), BIGINT (others, may lose sign info)
    UnsignedBigInt,

    // === Floating Point ===
    /// 32-bit floating point
    /// Maps to: FLOAT/REAL (most databases)
    Float,

    /// 64-bit floating point
    /// Maps to: DOUBLE/DOUBLE PRECISION (most databases), REAL (SQLite)
    Double,

    // === Exact Numeric ===
    /// Exact decimal/numeric type with precision and scale
    /// Maps to: DECIMAL/NUMERIC (most databases), TEXT (SQLite)
    Decimal {
        /// Total number of digits (None = database default)
        precision: Option<u8>,
        /// Digits after decimal point (None = database default)
        scale: Option<u8>,
    },

    /// Monetary type (for databases that have it)
    /// Maps to: MONEY (PostgreSQL), DECIMAL (others)
    Money {
        precision: Option<u8>,
        scale: Option<u8>,
    },

    // === String Types ===
    /// Fixed or variable-length character string with max length
    /// Maps to: VARCHAR/CHAR (most databases), TEXT (SQLite)
    String {
        /// Maximum length in characters (None = unlimited/TEXT)
        max_length: Option<u32>,
        /// Whether it's fixed-length (CHAR vs VARCHAR)
        fixed_length: bool,
    },

    /// Unbounded text (CLOB)
    /// Maps to: TEXT (most databases)
    Text,

    /// Case-insensitive text (PostgreSQL CITEXT)
    /// Maps to: CITEXT (PostgreSQL), TEXT (others)
    CaseInsensitiveText,

    // === Binary Types ===
    /// Binary data with optional max length
    /// Maps to: VARBINARY/BINARY (MySQL/MSSQL), BYTEA (PostgreSQL), BLOB (SQLite)
    Binary { max_length: Option<u32> },

    /// Unbounded binary data (BLOB)
    /// Maps to: BLOB (most databases), BYTEA (PostgreSQL)
    Blob,

    // === Date/Time Types ===
    /// Date only (year, month, day)
    /// Maps to: DATE (most databases), TEXT (SQLite as ISO8601)
    Date,

    /// Time only (hour, minute, second, optional fractional seconds)
    /// Maps to: TIME (most databases), TEXT (SQLite)
    Time {
        /// Fractional seconds precision (0-6)
        precision: Option<u8>,
        /// Whether to include timezone
        with_timezone: bool,
    },

    /// Date and time without timezone
    /// Maps to: DATETIME (MySQL/SQLite), TIMESTAMP WITHOUT TIME ZONE (PostgreSQL)
    DateTime {
        /// Fractional seconds precision (0-6)
        precision: Option<u8>,
    },

    /// Date and time with timezone (stored as UTC)
    /// Maps to: TIMESTAMP WITH TIME ZONE (PostgreSQL), DATETIME (MySQL with conversion)
    Timestamp {
        /// Fractional seconds precision (0-6)
        precision: Option<u8>,
    },

    /// Time interval/duration
    /// Maps to: INTERVAL (PostgreSQL), TEXT (others)
    Interval,

    /// Year only
    /// Maps to: YEAR (MySQL), INTEGER (others)
    Year,

    // === Special Types ===
    /// UUID/GUID
    /// Maps to: UUID (PostgreSQL), CHAR(36) (MySQL), TEXT (SQLite)
    Uuid,

    /// JSON data
    /// Maps to: JSON (MySQL/PostgreSQL), JSONB (PostgreSQL), TEXT (SQLite)
    Json {
        /// Whether to use binary JSON (JSONB in PostgreSQL)
        binary: bool,
    },

    /// XML data
    /// Maps to: XML (PostgreSQL/MSSQL), TEXT (others)
    Xml,

    // === Array Types (PostgreSQL, etc.) ===
    /// Array of values
    /// Maps to: ARRAY (PostgreSQL), JSON (others)
    Array { element_type: Box<CanonicalType> },

    // === Enum Types ===
    /// Enumerated type with allowed values
    /// Maps to: ENUM (MySQL/PostgreSQL), CHECK constraint (others)
    Enum {
        name: Option<String>,
        values: Vec<String>,
    },

    // === Set Types ===
    /// Set of values (MySQL SET type)
    /// Maps to: SET (MySQL), TEXT/JSON (others)
    Set { values: Vec<String> },

    // === Network Types (PostgreSQL) ===
    /// IP address (IPv4 or IPv6)
    /// Maps to: INET (PostgreSQL), VARCHAR (others)
    IpAddress,

    /// MAC address
    /// Maps to: MACADDR (PostgreSQL), VARCHAR (others)
    MacAddress,

    /// CIDR network address
    /// Maps to: CIDR (PostgreSQL), VARCHAR (others)
    Cidr,

    // === Geospatial Types ===
    /// Geographic point (lat, lon)
    /// Maps to: POINT (PostgreSQL/MySQL), TEXT as JSON (others)
    Point,

    /// Geographic line
    /// Maps to: LINE (PostgreSQL), TEXT as JSON (others)
    Line,

    /// Geographic polygon
    /// Maps to: POLYGON (PostgreSQL/MySQL), TEXT as JSON (others)
    Polygon,

    /// Generic geometry (WKT/WKB format)
    /// Maps to: GEOMETRY (PostGIS/MySQL), TEXT (others)
    Geometry {
        /// Spatial Reference System Identifier
        srid: Option<i32>,
    },

    /// Geography type (spherical coordinates)
    /// Maps to: GEOGRAPHY (PostGIS), GEOMETRY (others)
    Geography { srid: Option<i32> },

    // === Full-Text Search Types ===
    /// Text search vector (PostgreSQL tsvector)
    /// Maps to: TSVECTOR (PostgreSQL), TEXT (others)
    TextSearchVector,

    /// Text search query (PostgreSQL tsquery)
    /// Maps to: TSQUERY (PostgreSQL), TEXT (others)
    TextSearchQuery,

    // === Range Types (PostgreSQL) ===
    /// Range of integers
    /// Maps to: INT4RANGE/INT8RANGE (PostgreSQL), JSON (others)
    IntegerRange,

    /// Range of timestamps
    /// Maps to: TSRANGE/TSTZRANGE (PostgreSQL), JSON (others)
    TimestampRange,

    /// Range of dates
    /// Maps to: DATERANGE (PostgreSQL), JSON (others)
    DateRange,

    // === Document Store Types (MongoDB, etc.) ===
    /// Embedded document/object
    /// Maps to: JSONB (PostgreSQL), JSON (MySQL), Object (MongoDB)
    Document,

    /// Object ID (MongoDB's _id)
    /// Maps to: ObjectId (MongoDB), UUID/VARCHAR (others)
    ObjectId,

    // === Key-Value Types (Redis, etc.) ===
    /// Key-value pair (for Redis-style data)
    /// Maps to: HSTORE (PostgreSQL), JSON (others)
    KeyValue,

    /// Redis-style list
    /// Maps to: ARRAY (PostgreSQL), JSON (others)
    List,

    /// Redis-style sorted set with scores
    /// Maps to: JSON (most databases)
    SortedSet,

    /// Redis-style hash map
    /// Maps to: HSTORE/JSONB (PostgreSQL), JSON (others)
    Hash,

    // === Bit Types ===
    /// Fixed-length bit string
    /// Maps to: BIT (most databases)
    Bit { length: Option<u32> },

    /// Variable-length bit string
    /// Maps to: BIT VARYING (PostgreSQL), VARBINARY (others)
    BitVarying { max_length: Option<u32> },

    // === Auto-increment / Serial ===
    /// Auto-incrementing integer (small)
    /// Maps to: SERIAL/SMALLSERIAL (PostgreSQL), AUTO_INCREMENT (MySQL)
    SmallSerial,

    /// Auto-incrementing integer (standard)
    /// Maps to: SERIAL (PostgreSQL), INT AUTO_INCREMENT (MySQL)
    Serial,

    /// Auto-incrementing integer (large)
    /// Maps to: BIGSERIAL (PostgreSQL), BIGINT AUTO_INCREMENT (MySQL)
    BigSerial,

    // === Custom / Unknown ===
    /// Unknown or database-specific type (preserved as string)
    /// Used when no canonical mapping exists
    Custom {
        /// The original type name from the source database
        source_type: String,
        /// Hint about the underlying data format
        format_hint: Option<DataFormatHint>,
    },
}

/// Hints about how to interpret custom type data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataFormatHint {
    /// Treat as text
    Text,
    /// Treat as binary
    Binary,
    /// Treat as JSON
    Json,
    /// Treat as integer
    Integer,
    /// Treat as floating point
    Float,
}

impl CanonicalType {
    /// Returns true if this type can be null
    pub fn is_nullable_type(&self) -> bool {
        !matches!(self, CanonicalType::Null)
    }

    /// Returns a simple string representation for display
    pub fn display_name(&self) -> String {
        match self {
            CanonicalType::Null => "NULL".into(),
            CanonicalType::Boolean => "BOOLEAN".into(),
            CanonicalType::TinyInt => "TINYINT".into(),
            CanonicalType::SmallInt => "SMALLINT".into(),
            CanonicalType::Integer => "INTEGER".into(),
            CanonicalType::BigInt => "BIGINT".into(),
            CanonicalType::UnsignedBigInt => "UNSIGNED BIGINT".into(),
            CanonicalType::Float => "FLOAT".into(),
            CanonicalType::Double => "DOUBLE".into(),
            CanonicalType::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                (Some(p), None) => format!("DECIMAL({})", p),
                _ => "DECIMAL".into(),
            },
            CanonicalType::Money { .. } => "MONEY".into(),
            CanonicalType::String {
                max_length,
                fixed_length,
            } => {
                let base = if *fixed_length { "CHAR" } else { "VARCHAR" };
                match max_length {
                    Some(len) => format!("{}({})", base, len),
                    None => base.into(),
                }
            }
            CanonicalType::Text => "TEXT".into(),
            CanonicalType::CaseInsensitiveText => "CITEXT".into(),
            CanonicalType::Binary { max_length } => match max_length {
                Some(len) => format!("BINARY({})", len),
                None => "BINARY".into(),
            },
            CanonicalType::Blob => "BLOB".into(),
            CanonicalType::Date => "DATE".into(),
            CanonicalType::Time { with_timezone, .. } => if *with_timezone {
                "TIME WITH TIME ZONE"
            } else {
                "TIME"
            }
            .into(),
            CanonicalType::DateTime { .. } => "DATETIME".into(),
            CanonicalType::Timestamp { .. } => "TIMESTAMP".into(),
            CanonicalType::Interval => "INTERVAL".into(),
            CanonicalType::Year => "YEAR".into(),
            CanonicalType::Uuid => "UUID".into(),
            CanonicalType::Json { binary } => if *binary { "JSONB" } else { "JSON" }.into(),
            CanonicalType::Xml => "XML".into(),
            CanonicalType::Array { element_type } => {
                format!("{}[]", element_type.display_name())
            }
            CanonicalType::Enum { name, .. } => name.clone().unwrap_or_else(|| "ENUM".into()),
            CanonicalType::Set { .. } => "SET".into(),
            CanonicalType::IpAddress => "INET".into(),
            CanonicalType::MacAddress => "MACADDR".into(),
            CanonicalType::Cidr => "CIDR".into(),
            CanonicalType::Point => "POINT".into(),
            CanonicalType::Line => "LINE".into(),
            CanonicalType::Polygon => "POLYGON".into(),
            CanonicalType::Geometry { .. } => "GEOMETRY".into(),
            CanonicalType::Geography { .. } => "GEOGRAPHY".into(),
            CanonicalType::TextSearchVector => "TSVECTOR".into(),
            CanonicalType::TextSearchQuery => "TSQUERY".into(),
            CanonicalType::IntegerRange => "INT RANGE".into(),
            CanonicalType::TimestampRange => "TIMESTAMP RANGE".into(),
            CanonicalType::DateRange => "DATE RANGE".into(),
            CanonicalType::Document => "DOCUMENT".into(),
            CanonicalType::ObjectId => "OBJECT ID".into(),
            CanonicalType::KeyValue => "KEY-VALUE".into(),
            CanonicalType::List => "LIST".into(),
            CanonicalType::SortedSet => "SORTED SET".into(),
            CanonicalType::Hash => "HASH".into(),
            CanonicalType::Bit { length } => match length {
                Some(len) => format!("BIT({})", len),
                None => "BIT".into(),
            },
            CanonicalType::BitVarying { max_length } => match max_length {
                Some(len) => format!("BIT VARYING({})", len),
                None => "BIT VARYING".into(),
            },
            CanonicalType::SmallSerial => "SMALLSERIAL".into(),
            CanonicalType::Serial => "SERIAL".into(),
            CanonicalType::BigSerial => "BIGSERIAL".into(),
            CanonicalType::Custom { source_type, .. } => source_type.clone(),
        }
    }

    /// Returns the most compatible fallback type for databases that don't support this type
    pub fn fallback_type(&self) -> CanonicalType {
        match self {
            // Types that need TEXT fallback
            CanonicalType::Uuid
            | CanonicalType::Xml
            | CanonicalType::Interval
            | CanonicalType::IpAddress
            | CanonicalType::MacAddress
            | CanonicalType::Cidr
            | CanonicalType::TextSearchVector
            | CanonicalType::TextSearchQuery
            | CanonicalType::ObjectId
            | CanonicalType::CaseInsensitiveText => CanonicalType::Text,

            // Types that need JSON fallback
            CanonicalType::Array { .. }
            | CanonicalType::Document
            | CanonicalType::KeyValue
            | CanonicalType::List
            | CanonicalType::SortedSet
            | CanonicalType::Hash
            | CanonicalType::IntegerRange
            | CanonicalType::TimestampRange
            | CanonicalType::DateRange
            | CanonicalType::Point
            | CanonicalType::Line
            | CanonicalType::Polygon
            | CanonicalType::Geometry { .. }
            | CanonicalType::Geography { .. } => CanonicalType::Json { binary: false },

            // Enum/Set need TEXT fallback
            CanonicalType::Enum { .. } | CanonicalType::Set { .. } => CanonicalType::Text,

            // Money needs DECIMAL fallback
            CanonicalType::Money { precision, scale } => CanonicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },

            // Serial types need their base integer type
            CanonicalType::SmallSerial => CanonicalType::SmallInt,
            CanonicalType::Serial => CanonicalType::Integer,
            CanonicalType::BigSerial => CanonicalType::BigInt,

            // Bit types need binary fallback
            CanonicalType::Bit { length } | CanonicalType::BitVarying { max_length: length } => {
                CanonicalType::Binary {
                    max_length: *length,
                }
            }

            // Custom types need TEXT fallback
            CanonicalType::Custom { .. } => CanonicalType::Text,

            // These types are universally supported, no fallback needed
            _ => self.clone(),
        }
    }

    /// Check if this type requires special handling for binary data
    pub fn is_binary(&self) -> bool {
        matches!(
            self,
            CanonicalType::Binary { .. }
                | CanonicalType::Blob
                | CanonicalType::Bit { .. }
                | CanonicalType::BitVarying { .. }
        )
    }

    /// Check if this type stores text data
    pub fn is_text(&self) -> bool {
        matches!(
            self,
            CanonicalType::String { .. }
                | CanonicalType::Text
                | CanonicalType::CaseInsensitiveText
                | CanonicalType::Xml
        )
    }

    /// Check if this type stores numeric data
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            CanonicalType::TinyInt
                | CanonicalType::SmallInt
                | CanonicalType::Integer
                | CanonicalType::BigInt
                | CanonicalType::UnsignedBigInt
                | CanonicalType::Float
                | CanonicalType::Double
                | CanonicalType::Decimal { .. }
                | CanonicalType::Money { .. }
                | CanonicalType::SmallSerial
                | CanonicalType::Serial
                | CanonicalType::BigSerial
        )
    }

    /// Check if this type stores temporal data
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            CanonicalType::Date
                | CanonicalType::Time { .. }
                | CanonicalType::DateTime { .. }
                | CanonicalType::Timestamp { .. }
                | CanonicalType::Interval
                | CanonicalType::Year
        )
    }
}

impl Default for CanonicalType {
    fn default() -> Self {
        CanonicalType::Text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_names() {
        assert_eq!(CanonicalType::Integer.display_name(), "INTEGER");
        assert_eq!(
            CanonicalType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
            .display_name(),
            "DECIMAL(10,2)"
        );
        assert_eq!(
            CanonicalType::String {
                max_length: Some(255),
                fixed_length: false
            }
            .display_name(),
            "VARCHAR(255)"
        );
        assert_eq!(
            CanonicalType::Array {
                element_type: Box::new(CanonicalType::Integer)
            }
            .display_name(),
            "INTEGER[]"
        );
    }

    #[test]
    fn test_fallback_types() {
        assert_eq!(CanonicalType::Uuid.fallback_type(), CanonicalType::Text);
        assert_eq!(
            CanonicalType::Array {
                element_type: Box::new(CanonicalType::Integer)
            }
            .fallback_type(),
            CanonicalType::Json { binary: false }
        );
        assert_eq!(
            CanonicalType::Serial.fallback_type(),
            CanonicalType::Integer
        );
    }
}
