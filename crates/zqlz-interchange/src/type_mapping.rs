//! Type Mapping for Database Drivers
//!
//! This module defines traits and implementations for mapping between
//! native database types and canonical UDIF types.

use crate::canonical_types::{CanonicalType, DataFormatHint};

/// Trait for mapping database types to/from canonical types
///
/// Each database driver should implement this trait to enable
/// import/export functionality.
pub trait TypeMapper: Send + Sync {
    /// Get the driver name (e.g., "postgresql", "mysql", "sqlite")
    fn driver_name(&self) -> &str;

    /// Convert a native database type string to a canonical type
    fn to_canonical(&self, native_type: &str) -> CanonicalType;

    /// Convert a canonical type to the native database type string
    fn from_canonical(&self, canonical: &CanonicalType) -> String;

    /// Check if this database fully supports the canonical type
    /// Returns false if the type needs to be converted/degraded
    fn supports_type(&self, canonical: &CanonicalType) -> bool;

    /// Get the best available type for a canonical type that isn't directly supported
    fn best_available_type(&self, canonical: &CanonicalType) -> CanonicalType {
        if self.supports_type(canonical) {
            canonical.clone()
        } else {
            canonical.fallback_type()
        }
    }
}

/// SQLite type mapper
#[derive(Debug, Clone, Default)]
pub struct SqliteTypeMapper;

impl TypeMapper for SqliteTypeMapper {
    fn driver_name(&self) -> &str {
        "sqlite"
    }

    fn to_canonical(&self, native_type: &str) -> CanonicalType {
        let upper = native_type.to_uppercase();
        let upper = upper.trim();

        match upper {
            "INTEGER" | "INT" | "BIGINT" | "MEDIUMINT" | "SMALLINT" | "TINYINT" => {
                CanonicalType::Integer
            }
            "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => CanonicalType::Double,
            "TEXT" | "CLOB" => CanonicalType::Text,
            "BLOB" => CanonicalType::Blob,
            "NUMERIC" | "DECIMAL" => CanonicalType::Decimal {
                precision: None,
                scale: None,
            },
            "BOOLEAN" | "BOOL" => CanonicalType::Boolean,
            "DATE" => CanonicalType::Date,
            "TIME" => CanonicalType::Time {
                precision: None,
                with_timezone: false,
            },
            "DATETIME" | "TIMESTAMP" => CanonicalType::DateTime { precision: None },
            _ => {
                if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                    let max_length = parse_length(&upper);
                    CanonicalType::String {
                        max_length,
                        fixed_length: upper.starts_with("CHAR("),
                    }
                } else if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
                    let (precision, scale) = parse_precision_scale(&upper);
                    CanonicalType::Decimal { precision, scale }
                } else {
                    CanonicalType::Custom {
                        source_type: native_type.to_string(),
                        format_hint: Some(DataFormatHint::Text),
                    }
                }
            }
        }
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> String {
        match canonical {
            CanonicalType::Null => "NULL".into(),
            CanonicalType::Boolean => "INTEGER".into(),
            CanonicalType::TinyInt
            | CanonicalType::SmallInt
            | CanonicalType::Integer
            | CanonicalType::BigInt
            | CanonicalType::UnsignedBigInt
            | CanonicalType::SmallSerial
            | CanonicalType::Serial
            | CanonicalType::BigSerial => "INTEGER".into(),
            CanonicalType::Float | CanonicalType::Double => "REAL".into(),
            CanonicalType::Decimal { .. } | CanonicalType::Money { .. } => "NUMERIC".into(),
            CanonicalType::String { max_length, .. } => match max_length {
                Some(len) => format!("VARCHAR({})", len),
                None => "TEXT".into(),
            },
            CanonicalType::Text | CanonicalType::CaseInsensitiveText => "TEXT".into(),
            CanonicalType::Binary { .. } | CanonicalType::Blob => "BLOB".into(),
            CanonicalType::Date => "DATE".into(),
            CanonicalType::Time { .. } => "TIME".into(),
            CanonicalType::DateTime { .. } | CanonicalType::Timestamp { .. } => "DATETIME".into(),
            CanonicalType::Interval | CanonicalType::Year => "TEXT".into(),
            CanonicalType::Uuid => "TEXT".into(),
            CanonicalType::Json { .. } => "TEXT".into(),
            CanonicalType::Xml => "TEXT".into(),
            CanonicalType::Array { .. } => "TEXT".into(),
            CanonicalType::Enum { .. } | CanonicalType::Set { .. } => "TEXT".into(),
            CanonicalType::IpAddress
            | CanonicalType::MacAddress
            | CanonicalType::Cidr
            | CanonicalType::Point
            | CanonicalType::Line
            | CanonicalType::Polygon
            | CanonicalType::Geometry { .. }
            | CanonicalType::Geography { .. }
            | CanonicalType::TextSearchVector
            | CanonicalType::TextSearchQuery
            | CanonicalType::IntegerRange
            | CanonicalType::TimestampRange
            | CanonicalType::DateRange
            | CanonicalType::Document
            | CanonicalType::ObjectId
            | CanonicalType::KeyValue
            | CanonicalType::List
            | CanonicalType::SortedSet
            | CanonicalType::Hash => "TEXT".into(),
            CanonicalType::Bit { .. } | CanonicalType::BitVarying { .. } => "BLOB".into(),
            CanonicalType::Custom { .. } => "TEXT".into(),
        }
    }

    fn supports_type(&self, canonical: &CanonicalType) -> bool {
        matches!(
            canonical,
            CanonicalType::Null
                | CanonicalType::Integer
                | CanonicalType::Double
                | CanonicalType::Text
                | CanonicalType::Blob
                | CanonicalType::String { .. }
                | CanonicalType::Decimal { .. }
        )
    }
}

/// PostgreSQL type mapper
#[derive(Debug, Clone, Default)]
pub struct PostgresTypeMapper;

impl TypeMapper for PostgresTypeMapper {
    fn driver_name(&self) -> &str {
        "postgresql"
    }

    fn to_canonical(&self, native_type: &str) -> CanonicalType {
        let lower = native_type.to_lowercase();
        let lower = lower.trim();

        if lower.ends_with("[]") {
            let element_type = &lower[..lower.len() - 2];
            return CanonicalType::Array {
                element_type: Box::new(self.to_canonical(element_type)),
            };
        }

        match &*lower {
            "boolean" | "bool" => CanonicalType::Boolean,
            "smallint" | "int2" => CanonicalType::SmallInt,
            "integer" | "int" | "int4" => CanonicalType::Integer,
            "bigint" | "int8" => CanonicalType::BigInt,
            "smallserial" | "serial2" => CanonicalType::SmallSerial,
            "serial" | "serial4" => CanonicalType::Serial,
            "bigserial" | "serial8" => CanonicalType::BigSerial,
            "real" | "float4" => CanonicalType::Float,
            "double precision" | "float8" => CanonicalType::Double,
            "money" => CanonicalType::Money {
                precision: None,
                scale: None,
            },
            "text" => CanonicalType::Text,
            "citext" => CanonicalType::CaseInsensitiveText,
            "bytea" => CanonicalType::Blob,
            "date" => CanonicalType::Date,
            "time" | "time without time zone" => CanonicalType::Time {
                precision: None,
                with_timezone: false,
            },
            "time with time zone" | "timetz" => CanonicalType::Time {
                precision: None,
                with_timezone: true,
            },
            "timestamp" | "timestamp without time zone" => {
                CanonicalType::DateTime { precision: None }
            }
            "timestamp with time zone" | "timestamptz" => {
                CanonicalType::Timestamp { precision: None }
            }
            "interval" => CanonicalType::Interval,
            "uuid" => CanonicalType::Uuid,
            "json" => CanonicalType::Json { binary: false },
            "jsonb" => CanonicalType::Json { binary: true },
            "xml" => CanonicalType::Xml,
            "inet" => CanonicalType::IpAddress,
            "cidr" => CanonicalType::Cidr,
            "macaddr" | "macaddr8" => CanonicalType::MacAddress,
            "point" => CanonicalType::Point,
            "line" => CanonicalType::Line,
            "polygon" => CanonicalType::Polygon,
            "geometry" => CanonicalType::Geometry { srid: None },
            "geography" => CanonicalType::Geography { srid: None },
            "tsvector" => CanonicalType::TextSearchVector,
            "tsquery" => CanonicalType::TextSearchQuery,
            "int4range" | "int8range" => CanonicalType::IntegerRange,
            "tsrange" | "tstzrange" => CanonicalType::TimestampRange,
            "daterange" => CanonicalType::DateRange,
            "hstore" => CanonicalType::KeyValue,
            _ => {
                if lower.starts_with("character varying") || lower.starts_with("varchar") {
                    let max_length = parse_length(&lower);
                    CanonicalType::String {
                        max_length,
                        fixed_length: false,
                    }
                } else if lower.starts_with("character") || lower.starts_with("char") {
                    let max_length = parse_length(&lower);
                    CanonicalType::String {
                        max_length,
                        fixed_length: true,
                    }
                } else if lower.starts_with("numeric") || lower.starts_with("decimal") {
                    let (precision, scale) = parse_precision_scale(&lower);
                    CanonicalType::Decimal { precision, scale }
                } else if lower.starts_with("bit varying") || lower.starts_with("varbit") {
                    let max_length = parse_length(&lower);
                    CanonicalType::BitVarying { max_length }
                } else if lower.starts_with("bit") {
                    let length = parse_length(&lower);
                    CanonicalType::Bit { length }
                } else {
                    CanonicalType::Custom {
                        source_type: native_type.to_string(),
                        format_hint: Some(DataFormatHint::Text),
                    }
                }
            }
        }
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> String {
        match canonical {
            CanonicalType::Null => "NULL".into(),
            CanonicalType::Boolean => "boolean".into(),
            CanonicalType::TinyInt | CanonicalType::SmallInt => "smallint".into(),
            CanonicalType::Integer => "integer".into(),
            CanonicalType::BigInt | CanonicalType::UnsignedBigInt => "bigint".into(),
            CanonicalType::SmallSerial => "smallserial".into(),
            CanonicalType::Serial => "serial".into(),
            CanonicalType::BigSerial => "bigserial".into(),
            CanonicalType::Float => "real".into(),
            CanonicalType::Double => "double precision".into(),
            CanonicalType::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("numeric({},{})", p, s),
                (Some(p), None) => format!("numeric({})", p),
                _ => "numeric".into(),
            },
            CanonicalType::Money { .. } => "money".into(),
            CanonicalType::String {
                max_length,
                fixed_length,
            } => {
                let base = if *fixed_length {
                    "character"
                } else {
                    "character varying"
                };
                match max_length {
                    Some(len) => format!("{}({})", base, len),
                    None => "text".into(),
                }
            }
            CanonicalType::Text => "text".into(),
            CanonicalType::CaseInsensitiveText => "citext".into(),
            CanonicalType::Binary { .. } | CanonicalType::Blob => "bytea".into(),
            CanonicalType::Date => "date".into(),
            CanonicalType::Time {
                precision,
                with_timezone,
            } => {
                let base = if *with_timezone {
                    "time with time zone"
                } else {
                    "time"
                };
                match precision {
                    Some(p) => format!("{}({})", base, p),
                    None => base.into(),
                }
            }
            CanonicalType::DateTime { precision } => match precision {
                Some(p) => format!("timestamp({})", p),
                None => "timestamp".into(),
            },
            CanonicalType::Timestamp { precision } => match precision {
                Some(p) => format!("timestamp({}) with time zone", p),
                None => "timestamp with time zone".into(),
            },
            CanonicalType::Interval => "interval".into(),
            CanonicalType::Year => "integer".into(),
            CanonicalType::Uuid => "uuid".into(),
            CanonicalType::Json { binary } => if *binary { "jsonb" } else { "json" }.into(),
            CanonicalType::Xml => "xml".into(),
            CanonicalType::Array { element_type } => {
                format!("{}[]", self.from_canonical(element_type))
            }
            CanonicalType::Enum { name, values } => name
                .clone()
                .unwrap_or_else(|| format!("text CHECK (value IN ({}))", values.join(", "))),
            CanonicalType::Set { .. } => "text[]".into(),
            CanonicalType::IpAddress => "inet".into(),
            CanonicalType::MacAddress => "macaddr".into(),
            CanonicalType::Cidr => "cidr".into(),
            CanonicalType::Point => "point".into(),
            CanonicalType::Line => "line".into(),
            CanonicalType::Polygon => "polygon".into(),
            CanonicalType::Geometry { srid } => match srid {
                Some(s) => format!("geometry(Geometry, {})", s),
                None => "geometry".into(),
            },
            CanonicalType::Geography { srid } => match srid {
                Some(s) => format!("geography(Geometry, {})", s),
                None => "geography".into(),
            },
            CanonicalType::TextSearchVector => "tsvector".into(),
            CanonicalType::TextSearchQuery => "tsquery".into(),
            CanonicalType::IntegerRange => "int8range".into(),
            CanonicalType::TimestampRange => "tstzrange".into(),
            CanonicalType::DateRange => "daterange".into(),
            CanonicalType::Document => "jsonb".into(),
            CanonicalType::ObjectId => "uuid".into(),
            CanonicalType::KeyValue | CanonicalType::Hash => "hstore".into(),
            CanonicalType::List => "jsonb".into(),
            CanonicalType::SortedSet => "jsonb".into(),
            CanonicalType::Bit { length } => match length {
                Some(l) => format!("bit({})", l),
                None => "bit".into(),
            },
            CanonicalType::BitVarying { max_length } => match max_length {
                Some(l) => format!("bit varying({})", l),
                None => "bit varying".into(),
            },
            CanonicalType::Custom { source_type, .. } => source_type.clone(),
        }
    }

    fn supports_type(&self, canonical: &CanonicalType) -> bool {
        !matches!(
            canonical,
            CanonicalType::TinyInt
                | CanonicalType::UnsignedBigInt
                | CanonicalType::Year
                | CanonicalType::Set { .. }
                | CanonicalType::SortedSet
                | CanonicalType::Custom { .. }
        )
    }
}

/// MySQL type mapper
#[derive(Debug, Clone, Default)]
pub struct MySqlTypeMapper;

impl TypeMapper for MySqlTypeMapper {
    fn driver_name(&self) -> &str {
        "mysql"
    }

    fn to_canonical(&self, native_type: &str) -> CanonicalType {
        let upper = native_type.to_uppercase();
        let upper = upper.trim();

        match upper {
            "TINYINT" => CanonicalType::TinyInt,
            "SMALLINT" => CanonicalType::SmallInt,
            "MEDIUMINT" | "INT" | "INTEGER" => CanonicalType::Integer,
            "BIGINT" => CanonicalType::BigInt,
            "FLOAT" => CanonicalType::Float,
            "DOUBLE" | "DOUBLE PRECISION" | "REAL" => CanonicalType::Double,
            "BOOLEAN" | "BOOL" => CanonicalType::Boolean,
            "DATE" => CanonicalType::Date,
            "TIME" => CanonicalType::Time {
                precision: None,
                with_timezone: false,
            },
            "DATETIME" => CanonicalType::DateTime { precision: None },
            "TIMESTAMP" => CanonicalType::Timestamp { precision: None },
            "YEAR" => CanonicalType::Year,
            "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => CanonicalType::Text,
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => CanonicalType::Blob,
            "BINARY" => CanonicalType::Binary { max_length: None },
            "JSON" => CanonicalType::Json { binary: false },
            "POINT" => CanonicalType::Point,
            "LINESTRING" => CanonicalType::Line,
            "POLYGON" => CanonicalType::Polygon,
            "GEOMETRY" => CanonicalType::Geometry { srid: None },
            _ => {
                if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                    let max_length = parse_length(&upper);
                    CanonicalType::String {
                        max_length,
                        fixed_length: upper.starts_with("CHAR("),
                    }
                } else if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
                    let (precision, scale) = parse_precision_scale(&upper);
                    CanonicalType::Decimal { precision, scale }
                } else if upper.starts_with("ENUM") {
                    let values = parse_enum_values(&upper);
                    CanonicalType::Enum { name: None, values }
                } else if upper.starts_with("SET") {
                    let values = parse_enum_values(&upper);
                    CanonicalType::Set { values }
                } else if upper.starts_with("VARBINARY") {
                    let max_length = parse_length(&upper);
                    CanonicalType::Binary { max_length }
                } else if upper.starts_with("BIT") {
                    let length = parse_length(&upper);
                    CanonicalType::Bit { length }
                } else if upper.contains("UNSIGNED") {
                    if upper.contains("BIGINT") {
                        CanonicalType::UnsignedBigInt
                    } else {
                        CanonicalType::Integer
                    }
                } else {
                    CanonicalType::Custom {
                        source_type: native_type.to_string(),
                        format_hint: Some(DataFormatHint::Text),
                    }
                }
            }
        }
    }

    fn from_canonical(&self, canonical: &CanonicalType) -> String {
        match canonical {
            CanonicalType::Null => "NULL".into(),
            CanonicalType::Boolean => "TINYINT(1)".into(),
            CanonicalType::TinyInt => "TINYINT".into(),
            CanonicalType::SmallInt => "SMALLINT".into(),
            CanonicalType::Integer | CanonicalType::Serial => "INT".into(),
            CanonicalType::BigInt | CanonicalType::BigSerial => "BIGINT".into(),
            CanonicalType::SmallSerial => "SMALLINT AUTO_INCREMENT".into(),
            CanonicalType::UnsignedBigInt => "BIGINT UNSIGNED".into(),
            CanonicalType::Float => "FLOAT".into(),
            CanonicalType::Double => "DOUBLE".into(),
            CanonicalType::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                (Some(p), None) => format!("DECIMAL({})", p),
                _ => "DECIMAL".into(),
            },
            CanonicalType::Money { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
                _ => "DECIMAL(19,4)".into(),
            },
            CanonicalType::String {
                max_length,
                fixed_length,
            } => {
                let base = if *fixed_length { "CHAR" } else { "VARCHAR" };
                match max_length {
                    Some(len) if *len <= 65535 => format!("{}({})", base, len),
                    _ => "TEXT".into(),
                }
            }
            CanonicalType::Text | CanonicalType::CaseInsensitiveText => "TEXT".into(),
            CanonicalType::Binary { max_length } => match max_length {
                Some(len) => format!("VARBINARY({})", len),
                None => "BLOB".into(),
            },
            CanonicalType::Blob => "LONGBLOB".into(),
            CanonicalType::Date => "DATE".into(),
            CanonicalType::Time { precision, .. } => match precision {
                Some(p) => format!("TIME({})", p),
                None => "TIME".into(),
            },
            CanonicalType::DateTime { precision } => match precision {
                Some(p) => format!("DATETIME({})", p),
                None => "DATETIME".into(),
            },
            CanonicalType::Timestamp { precision } => match precision {
                Some(p) => format!("TIMESTAMP({})", p),
                None => "TIMESTAMP".into(),
            },
            CanonicalType::Interval => "VARCHAR(255)".into(),
            CanonicalType::Year => "YEAR".into(),
            CanonicalType::Uuid => "CHAR(36)".into(),
            CanonicalType::Json { .. } => "JSON".into(),
            CanonicalType::Xml => "TEXT".into(),
            CanonicalType::Array { .. } => "JSON".into(),
            CanonicalType::Enum { values, .. } => {
                format!(
                    "ENUM({})",
                    values
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            CanonicalType::Set { values } => {
                format!(
                    "SET({})",
                    values
                        .iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            CanonicalType::IpAddress | CanonicalType::MacAddress | CanonicalType::Cidr => {
                "VARCHAR(45)".into()
            }
            CanonicalType::Point => "POINT".into(),
            CanonicalType::Line => "LINESTRING".into(),
            CanonicalType::Polygon => "POLYGON".into(),
            CanonicalType::Geometry { .. } | CanonicalType::Geography { .. } => "GEOMETRY".into(),
            CanonicalType::TextSearchVector | CanonicalType::TextSearchQuery => "TEXT".into(),
            CanonicalType::IntegerRange
            | CanonicalType::TimestampRange
            | CanonicalType::DateRange => "JSON".into(),
            CanonicalType::Document
            | CanonicalType::KeyValue
            | CanonicalType::List
            | CanonicalType::SortedSet
            | CanonicalType::Hash => "JSON".into(),
            CanonicalType::ObjectId => "CHAR(24)".into(),
            CanonicalType::Bit { length } => match length {
                Some(l) => format!("BIT({})", l),
                None => "BIT(1)".into(),
            },
            CanonicalType::BitVarying { max_length } => match max_length {
                Some(l) => format!("BIT({})", l),
                None => "BLOB".into(),
            },
            CanonicalType::Custom { source_type, .. } => source_type.clone(),
        }
    }

    fn supports_type(&self, canonical: &CanonicalType) -> bool {
        !matches!(
            canonical,
            CanonicalType::Interval
                | CanonicalType::CaseInsensitiveText
                | CanonicalType::IpAddress
                | CanonicalType::MacAddress
                | CanonicalType::Cidr
                | CanonicalType::TextSearchVector
                | CanonicalType::TextSearchQuery
                | CanonicalType::IntegerRange
                | CanonicalType::TimestampRange
                | CanonicalType::DateRange
                | CanonicalType::KeyValue
                | CanonicalType::Hash
                | CanonicalType::Custom { .. }
        )
    }
}

fn parse_length(type_str: &str) -> Option<u32> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let inner = &type_str[start + 1..end];
            if let Some(comma) = inner.find(',') {
                return inner[..comma].trim().parse().ok();
            }
            return inner.trim().parse().ok();
        }
    }
    None
}

fn parse_precision_scale(type_str: &str) -> (Option<u8>, Option<u8>) {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let inner = &type_str[start + 1..end];
            let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
            let precision = parts.first().and_then(|p| p.parse().ok());
            let scale = parts.get(1).and_then(|s| s.parse().ok());
            return (precision, scale);
        }
    }
    (None, None)
}

fn parse_enum_values(type_str: &str) -> Vec<String> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.rfind(')') {
            let inner = &type_str[start + 1..end];
            return inner
                .split(',')
                .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
                .collect();
        }
    }
    Vec::new()
}

/// Get a type mapper for a given driver name
pub fn get_type_mapper(driver: &str) -> Box<dyn TypeMapper> {
    match driver.to_lowercase().as_str() {
        "sqlite" | "sqlite3" => Box::new(SqliteTypeMapper),
        "postgres" | "postgresql" | "pg" => Box::new(PostgresTypeMapper),
        "mysql" | "mariadb" => Box::new(MySqlTypeMapper),
        _ => Box::new(SqliteTypeMapper),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_type_mapping() {
        let mapper = SqliteTypeMapper;

        assert_eq!(mapper.to_canonical("INTEGER"), CanonicalType::Integer);
        assert_eq!(mapper.to_canonical("TEXT"), CanonicalType::Text);
        assert_eq!(mapper.to_canonical("REAL"), CanonicalType::Double);
        assert_eq!(mapper.to_canonical("BLOB"), CanonicalType::Blob);

        assert_eq!(mapper.from_canonical(&CanonicalType::Integer), "INTEGER");
        assert_eq!(mapper.from_canonical(&CanonicalType::Text), "TEXT");
        assert_eq!(mapper.from_canonical(&CanonicalType::Uuid), "TEXT");
    }

    #[test]
    fn test_postgres_type_mapping() {
        let mapper = PostgresTypeMapper;

        assert_eq!(mapper.to_canonical("integer"), CanonicalType::Integer);
        assert_eq!(mapper.to_canonical("text"), CanonicalType::Text);
        assert_eq!(mapper.to_canonical("uuid"), CanonicalType::Uuid);
        assert_eq!(
            mapper.to_canonical("jsonb"),
            CanonicalType::Json { binary: true }
        );
        assert_eq!(
            mapper.to_canonical("integer[]"),
            CanonicalType::Array {
                element_type: Box::new(CanonicalType::Integer)
            }
        );

        assert_eq!(mapper.from_canonical(&CanonicalType::Integer), "integer");
        assert_eq!(mapper.from_canonical(&CanonicalType::Uuid), "uuid");
        assert_eq!(
            mapper.from_canonical(&CanonicalType::Array {
                element_type: Box::new(CanonicalType::Integer)
            }),
            "integer[]"
        );
    }

    #[test]
    fn test_mysql_type_mapping() {
        let mapper = MySqlTypeMapper;

        assert_eq!(mapper.to_canonical("INT"), CanonicalType::Integer);
        assert_eq!(
            mapper.to_canonical("VARCHAR(255)"),
            CanonicalType::String {
                max_length: Some(255),
                fixed_length: false,
            }
        );
        assert_eq!(
            mapper.to_canonical("JSON"),
            CanonicalType::Json { binary: false }
        );

        assert_eq!(mapper.from_canonical(&CanonicalType::Integer), "INT");
        assert_eq!(mapper.from_canonical(&CanonicalType::Uuid), "CHAR(36)");
    }

    #[test]
    fn test_parse_helpers() {
        assert_eq!(parse_length("VARCHAR(255)"), Some(255));
        assert_eq!(parse_length("DECIMAL(10,2)"), Some(10));
        assert_eq!(parse_precision_scale("DECIMAL(10,2)"), (Some(10), Some(2)));
        assert_eq!(parse_enum_values("ENUM('a','b','c')"), vec!["a", "b", "c"]);
    }
}
