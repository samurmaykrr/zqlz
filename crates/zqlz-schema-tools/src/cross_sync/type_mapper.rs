//! Cross-database type mapping
//!
//! This module provides functionality to map data types between different
//! database systems (PostgreSQL, MySQL, SQLite, MS SQL Server).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Supported database dialects for type mapping
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    PostgreSQL,
    MySQL,
    SQLite,
    MsSql,
}

impl Dialect {
    /// Returns the dialect name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            Dialect::PostgreSQL => "postgresql",
            Dialect::MySQL => "mysql",
            Dialect::SQLite => "sqlite",
            Dialect::MsSql => "mssql",
        }
    }

    /// Parses a dialect from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "postgresql" | "postgres" | "pg" => Some(Dialect::PostgreSQL),
            "mysql" | "mariadb" => Some(Dialect::MySQL),
            "sqlite" | "sqlite3" => Some(Dialect::SQLite),
            "mssql" | "sqlserver" | "sql server" => Some(Dialect::MsSql),
            _ => None,
        }
    }
}

/// Errors that can occur during type mapping
#[derive(Debug, Error)]
pub enum TypeMapperError {
    #[error("Unknown source type: {0}")]
    UnknownSourceType(String),
    #[error("Type '{source_type}' from {source_dialect:?} cannot be mapped to {target_dialect:?}")]
    UnsupportedMapping {
        source_type: String,
        source_dialect: Dialect,
        target_dialect: Dialect,
    },
    #[error("Invalid type format: {0}")]
    InvalidTypeFormat(String),
}

/// Result type for type mapping operations
pub type TypeMapperResult<T> = Result<T, TypeMapperError>;

/// Represents a parsed SQL data type with optional parameters
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedType {
    /// Base type name (e.g., "VARCHAR", "INTEGER")
    pub base_type: String,
    /// Type parameters (e.g., length, precision, scale)
    pub params: Vec<String>,
    /// Whether the type is an array
    pub is_array: bool,
}

impl ParsedType {
    /// Creates a new parsed type
    pub fn new(base_type: impl Into<String>) -> Self {
        Self {
            base_type: base_type.into(),
            params: Vec::new(),
            is_array: false,
        }
    }

    /// Adds a parameter to the type
    pub fn with_param(mut self, param: impl Into<String>) -> Self {
        self.params.push(param.into());
        self
    }

    /// Marks the type as an array
    pub fn as_array(mut self) -> Self {
        self.is_array = true;
        self
    }

    /// Formats the type as a SQL string
    pub fn to_sql(&self) -> String {
        let mut result = self.base_type.clone();
        if !self.params.is_empty() {
            result.push('(');
            result.push_str(&self.params.join(", "));
            result.push(')');
        }
        if self.is_array {
            result.push_str("[]");
        }
        result
    }
}

/// Cross-database type mapper
///
/// Maps data types from one database dialect to another, handling differences
/// in naming, semantics, and supported features.
#[derive(Debug, Clone)]
pub struct TypeMapper {
    custom_mappings: HashMap<(Dialect, String), HashMap<Dialect, String>>,
}

impl Default for TypeMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeMapper {
    /// Creates a new type mapper with default mappings
    pub fn new() -> Self {
        Self {
            custom_mappings: HashMap::new(),
        }
    }

    /// Adds a custom type mapping
    pub fn add_custom_mapping(
        &mut self,
        source_dialect: Dialect,
        source_type: &str,
        target_dialect: Dialect,
        target_type: &str,
    ) {
        let source_key = (source_dialect, source_type.to_uppercase());
        self.custom_mappings
            .entry(source_key)
            .or_default()
            .insert(target_dialect, target_type.to_string());
    }

    /// Maps a type from source dialect to target dialect
    pub fn map_type(
        &self,
        source_type: &str,
        source_dialect: Dialect,
        target_dialect: Dialect,
    ) -> TypeMapperResult<String> {
        if source_dialect == target_dialect {
            return Ok(source_type.to_string());
        }

        let parsed = self.parse_type(source_type)?;
        let source_key = (source_dialect, parsed.base_type.to_uppercase());

        if let Some(custom_targets) = self.custom_mappings.get(&source_key) {
            if let Some(target_type) = custom_targets.get(&target_dialect) {
                return Ok(self.apply_params(target_type, &parsed.params, parsed.is_array));
            }
        }

        let mapped = self.map_base_type(&parsed.base_type, source_dialect, target_dialect)?;
        Ok(self.apply_params(&mapped, &parsed.params, parsed.is_array))
    }

    /// Parses a SQL type string into its components
    fn parse_type(&self, type_str: &str) -> TypeMapperResult<ParsedType> {
        let type_str = type_str.trim();
        if type_str.is_empty() {
            return Err(TypeMapperError::InvalidTypeFormat(
                "Empty type string".to_string(),
            ));
        }

        let is_array = type_str.ends_with("[]");
        let type_str = type_str.trim_end_matches("[]");

        if let Some(paren_start) = type_str.find('(') {
            let paren_end = type_str
                .rfind(')')
                .ok_or_else(|| TypeMapperError::InvalidTypeFormat(type_str.to_string()))?;

            let base_type = type_str[..paren_start].trim().to_uppercase();
            let params_str = &type_str[paren_start + 1..paren_end];
            let params: Vec<String> = params_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            Ok(ParsedType {
                base_type,
                params,
                is_array,
            })
        } else {
            Ok(ParsedType {
                base_type: type_str.to_uppercase(),
                params: Vec::new(),
                is_array,
            })
        }
    }

    /// Applies parameters to a mapped type
    fn apply_params(&self, base_type: &str, params: &[String], is_array: bool) -> String {
        let mut result = base_type.to_string();
        if !params.is_empty() && self.type_accepts_params(base_type) {
            result.push('(');
            result.push_str(&params.join(", "));
            result.push(')');
        }
        if is_array {
            result.push_str("[]");
        }
        result
    }

    /// Checks if a type accepts parameters
    fn type_accepts_params(&self, type_name: &str) -> bool {
        let upper = type_name.to_uppercase();
        matches!(
            upper.as_str(),
            "VARCHAR"
                | "CHAR"
                | "NVARCHAR"
                | "NCHAR"
                | "VARBINARY"
                | "BINARY"
                | "DECIMAL"
                | "NUMERIC"
                | "NUMBER"
                | "FLOAT"
                | "DOUBLE"
                | "TIME"
                | "TIMESTAMP"
                | "DATETIME2"
                | "BIT"
        )
    }

    /// Maps a base type from source to target dialect
    fn map_base_type(
        &self,
        base_type: &str,
        source_dialect: Dialect,
        target_dialect: Dialect,
    ) -> TypeMapperResult<String> {
        let upper = base_type.to_uppercase();

        let result = match (source_dialect, target_dialect) {
            (Dialect::PostgreSQL, Dialect::MySQL) => self.postgres_to_mysql(&upper)?,
            (Dialect::PostgreSQL, Dialect::SQLite) => self.postgres_to_sqlite(&upper)?,
            (Dialect::PostgreSQL, Dialect::MsSql) => self.postgres_to_mssql(&upper)?,
            (Dialect::MySQL, Dialect::PostgreSQL) => self.mysql_to_postgres(&upper)?,
            (Dialect::MySQL, Dialect::SQLite) => self.mysql_to_sqlite(&upper)?,
            (Dialect::MySQL, Dialect::MsSql) => self.mysql_to_mssql(&upper)?,
            (Dialect::SQLite, Dialect::PostgreSQL) => self.sqlite_to_postgres(&upper)?,
            (Dialect::SQLite, Dialect::MySQL) => self.sqlite_to_mysql(&upper)?,
            (Dialect::SQLite, Dialect::MsSql) => self.sqlite_to_mssql(&upper)?,
            (Dialect::MsSql, Dialect::PostgreSQL) => self.mssql_to_postgres(&upper)?,
            (Dialect::MsSql, Dialect::MySQL) => self.mssql_to_mysql(&upper)?,
            (Dialect::MsSql, Dialect::SQLite) => self.mssql_to_sqlite(&upper)?,
            _ => base_type.to_string(),
        };

        Ok(result)
    }

    fn postgres_to_mysql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "SERIAL" => "INT AUTO_INCREMENT".to_string(),
            "BIGSERIAL" => "BIGINT AUTO_INCREMENT".to_string(),
            "SMALLSERIAL" => "SMALLINT AUTO_INCREMENT".to_string(),
            "INTEGER" | "INT4" => "INT".to_string(),
            "BIGINT" | "INT8" => "BIGINT".to_string(),
            "SMALLINT" | "INT2" => "SMALLINT".to_string(),
            "BOOLEAN" | "BOOL" => "TINYINT(1)".to_string(),
            "TEXT" => "LONGTEXT".to_string(),
            "VARCHAR" | "CHARACTER VARYING" => "VARCHAR".to_string(),
            "CHAR" | "CHARACTER" => "CHAR".to_string(),
            "REAL" | "FLOAT4" => "FLOAT".to_string(),
            "DOUBLE PRECISION" | "FLOAT8" => "DOUBLE".to_string(),
            "NUMERIC" | "DECIMAL" => "DECIMAL".to_string(),
            "BYTEA" => "LONGBLOB".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "DATETIME".to_string(),
            "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "DATETIME".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" | "TIME WITHOUT TIME ZONE" => "TIME".to_string(),
            "TIME WITH TIME ZONE" | "TIMETZ" => "TIME".to_string(),
            "INTERVAL" => "VARCHAR(255)".to_string(),
            "UUID" => "CHAR(36)".to_string(),
            "JSON" | "JSONB" => "JSON".to_string(),
            "INET" | "CIDR" => "VARCHAR(45)".to_string(),
            "MACADDR" | "MACADDR8" => "VARCHAR(17)".to_string(),
            "MONEY" => "DECIMAL(19, 4)".to_string(),
            "OID" => "INT UNSIGNED".to_string(),
            "BIT" => "BIT".to_string(),
            "BIT VARYING" | "VARBIT" => "VARBINARY".to_string(),
            "POINT" | "LINE" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "CIRCLE" => {
                "GEOMETRY".to_string()
            }
            "XML" => "LONGTEXT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn postgres_to_sqlite(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" => "INTEGER PRIMARY KEY".to_string(),
            "INTEGER" | "INT4" | "INT" | "BIGINT" | "INT8" | "SMALLINT" | "INT2" => {
                "INTEGER".to_string()
            }
            "BOOLEAN" | "BOOL" => "INTEGER".to_string(),
            "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" => "TEXT".to_string(),
            "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOAT8" | "NUMERIC" | "DECIMAL" => {
                "REAL".to_string()
            }
            "BYTEA" => "BLOB".to_string(),
            "TIMESTAMP"
            | "TIMESTAMP WITHOUT TIME ZONE"
            | "TIMESTAMP WITH TIME ZONE"
            | "TIMESTAMPTZ"
            | "DATE"
            | "TIME"
            | "TIME WITHOUT TIME ZONE"
            | "TIME WITH TIME ZONE"
            | "TIMETZ" => "TEXT".to_string(),
            "INTERVAL" => "TEXT".to_string(),
            "UUID" => "TEXT".to_string(),
            "JSON" | "JSONB" => "TEXT".to_string(),
            "INET" | "CIDR" | "MACADDR" | "MACADDR8" => "TEXT".to_string(),
            "MONEY" => "REAL".to_string(),
            "OID" => "INTEGER".to_string(),
            "BIT" | "BIT VARYING" | "VARBIT" => "BLOB".to_string(),
            "POINT" | "LINE" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "CIRCLE" => "TEXT".to_string(),
            "XML" => "TEXT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn postgres_to_mssql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "SERIAL" => "INT IDENTITY(1,1)".to_string(),
            "BIGSERIAL" => "BIGINT IDENTITY(1,1)".to_string(),
            "SMALLSERIAL" => "SMALLINT IDENTITY(1,1)".to_string(),
            "INTEGER" | "INT4" => "INT".to_string(),
            "BIGINT" | "INT8" => "BIGINT".to_string(),
            "SMALLINT" | "INT2" => "SMALLINT".to_string(),
            "BOOLEAN" | "BOOL" => "BIT".to_string(),
            "TEXT" => "NVARCHAR(MAX)".to_string(),
            "VARCHAR" | "CHARACTER VARYING" => "NVARCHAR".to_string(),
            "CHAR" | "CHARACTER" => "NCHAR".to_string(),
            "REAL" | "FLOAT4" => "REAL".to_string(),
            "DOUBLE PRECISION" | "FLOAT8" => "FLOAT".to_string(),
            "NUMERIC" | "DECIMAL" => "DECIMAL".to_string(),
            "BYTEA" => "VARBINARY(MAX)".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "DATETIME2".to_string(),
            "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "DATETIMEOFFSET".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" | "TIME WITHOUT TIME ZONE" => "TIME".to_string(),
            "TIME WITH TIME ZONE" | "TIMETZ" => "TIME".to_string(),
            "INTERVAL" => "NVARCHAR(255)".to_string(),
            "UUID" => "UNIQUEIDENTIFIER".to_string(),
            "JSON" | "JSONB" => "NVARCHAR(MAX)".to_string(),
            "INET" | "CIDR" => "NVARCHAR(45)".to_string(),
            "MACADDR" | "MACADDR8" => "NVARCHAR(17)".to_string(),
            "MONEY" => "MONEY".to_string(),
            "OID" => "INT".to_string(),
            "BIT" => "BIT".to_string(),
            "BIT VARYING" | "VARBIT" => "VARBINARY".to_string(),
            "POINT" | "LINE" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "CIRCLE" => {
                "GEOMETRY".to_string()
            }
            "XML" => "XML".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mysql_to_postgres(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" => "SMALLINT".to_string(),
            "MEDIUMINT" => "INTEGER".to_string(),
            "INT" | "INTEGER" => "INTEGER".to_string(),
            "BIGINT" => "BIGINT".to_string(),
            "SMALLINT" => "SMALLINT".to_string(),
            "FLOAT" => "REAL".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" => "DOUBLE PRECISION".to_string(),
            "DECIMAL" | "NUMERIC" => "NUMERIC".to_string(),
            "BIT" => "BIT".to_string(),
            "BOOL" | "BOOLEAN" => "BOOLEAN".to_string(),
            "CHAR" => "CHAR".to_string(),
            "VARCHAR" => "VARCHAR".to_string(),
            "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => "TEXT".to_string(),
            "BINARY" => "BYTEA".to_string(),
            "VARBINARY" => "BYTEA".to_string(),
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => "BYTEA".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "DATETIME" => "TIMESTAMP".to_string(),
            "TIMESTAMP" => "TIMESTAMP WITH TIME ZONE".to_string(),
            "YEAR" => "SMALLINT".to_string(),
            "ENUM" => "VARCHAR(255)".to_string(),
            "SET" => "VARCHAR(255)".to_string(),
            "JSON" => "JSONB".to_string(),
            "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
            | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => "GEOMETRY".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mysql_to_sqlite(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => {
                "INTEGER".to_string()
            }
            "FLOAT" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" => "REAL".to_string(),
            "BIT" | "BOOL" | "BOOLEAN" => "INTEGER".to_string(),
            "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM"
            | "SET" => "TEXT".to_string(),
            "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                "BLOB".to_string()
            }
            "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "YEAR" => "TEXT".to_string(),
            "JSON" => "TEXT".to_string(),
            "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" => "TEXT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mysql_to_mssql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" => "TINYINT".to_string(),
            "SMALLINT" => "SMALLINT".to_string(),
            "MEDIUMINT" => "INT".to_string(),
            "INT" | "INTEGER" => "INT".to_string(),
            "BIGINT" => "BIGINT".to_string(),
            "FLOAT" => "REAL".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" => "FLOAT".to_string(),
            "DECIMAL" | "NUMERIC" => "DECIMAL".to_string(),
            "BIT" => "BIT".to_string(),
            "BOOL" | "BOOLEAN" => "BIT".to_string(),
            "CHAR" => "NCHAR".to_string(),
            "VARCHAR" => "NVARCHAR".to_string(),
            "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => "NVARCHAR(MAX)".to_string(),
            "BINARY" => "BINARY".to_string(),
            "VARBINARY" => "VARBINARY".to_string(),
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => "VARBINARY(MAX)".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "DATETIME" => "DATETIME2".to_string(),
            "TIMESTAMP" => "DATETIME2".to_string(),
            "YEAR" => "SMALLINT".to_string(),
            "ENUM" | "SET" => "NVARCHAR(255)".to_string(),
            "JSON" => "NVARCHAR(MAX)".to_string(),
            "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" => "GEOMETRY".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn sqlite_to_postgres(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "INTEGER" | "INT" => "INTEGER".to_string(),
            "REAL" => "DOUBLE PRECISION".to_string(),
            "TEXT" => "TEXT".to_string(),
            "BLOB" => "BYTEA".to_string(),
            "NUMERIC" => "NUMERIC".to_string(),
            "BOOLEAN" => "BOOLEAN".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn sqlite_to_mysql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "INTEGER" | "INT" => "BIGINT".to_string(),
            "REAL" => "DOUBLE".to_string(),
            "TEXT" => "LONGTEXT".to_string(),
            "BLOB" => "LONGBLOB".to_string(),
            "NUMERIC" => "DECIMAL(65, 30)".to_string(),
            "BOOLEAN" => "TINYINT(1)".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn sqlite_to_mssql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "INTEGER" | "INT" => "BIGINT".to_string(),
            "REAL" => "FLOAT".to_string(),
            "TEXT" => "NVARCHAR(MAX)".to_string(),
            "BLOB" => "VARBINARY(MAX)".to_string(),
            "NUMERIC" => "DECIMAL(38, 19)".to_string(),
            "BOOLEAN" => "BIT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mssql_to_postgres(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" => "SMALLINT".to_string(),
            "SMALLINT" => "SMALLINT".to_string(),
            "INT" => "INTEGER".to_string(),
            "BIGINT" => "BIGINT".to_string(),
            "REAL" => "REAL".to_string(),
            "FLOAT" => "DOUBLE PRECISION".to_string(),
            "DECIMAL" | "NUMERIC" => "NUMERIC".to_string(),
            "MONEY" => "MONEY".to_string(),
            "SMALLMONEY" => "NUMERIC(10, 4)".to_string(),
            "BIT" => "BOOLEAN".to_string(),
            "CHAR" => "CHAR".to_string(),
            "VARCHAR" => "VARCHAR".to_string(),
            "NCHAR" => "CHAR".to_string(),
            "NVARCHAR" => "VARCHAR".to_string(),
            "TEXT" => "TEXT".to_string(),
            "NTEXT" => "TEXT".to_string(),
            "BINARY" => "BYTEA".to_string(),
            "VARBINARY" => "BYTEA".to_string(),
            "IMAGE" => "BYTEA".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "DATETIME" | "SMALLDATETIME" | "DATETIME2" => "TIMESTAMP".to_string(),
            "DATETIMEOFFSET" => "TIMESTAMP WITH TIME ZONE".to_string(),
            "UNIQUEIDENTIFIER" => "UUID".to_string(),
            "XML" => "XML".to_string(),
            "GEOMETRY" | "GEOGRAPHY" => "GEOMETRY".to_string(),
            "HIERARCHYID" => "VARCHAR(255)".to_string(),
            "SQL_VARIANT" => "TEXT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mssql_to_mysql(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" => "TINYINT UNSIGNED".to_string(),
            "SMALLINT" => "SMALLINT".to_string(),
            "INT" => "INT".to_string(),
            "BIGINT" => "BIGINT".to_string(),
            "REAL" => "FLOAT".to_string(),
            "FLOAT" => "DOUBLE".to_string(),
            "DECIMAL" | "NUMERIC" => "DECIMAL".to_string(),
            "MONEY" => "DECIMAL(19, 4)".to_string(),
            "SMALLMONEY" => "DECIMAL(10, 4)".to_string(),
            "BIT" => "TINYINT(1)".to_string(),
            "CHAR" | "NCHAR" => "CHAR".to_string(),
            "VARCHAR" | "NVARCHAR" => "VARCHAR".to_string(),
            "TEXT" | "NTEXT" => "LONGTEXT".to_string(),
            "BINARY" => "BINARY".to_string(),
            "VARBINARY" => "VARBINARY".to_string(),
            "IMAGE" => "LONGBLOB".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "DATETIME" | "SMALLDATETIME" | "DATETIME2" | "DATETIMEOFFSET" => "DATETIME".to_string(),
            "UNIQUEIDENTIFIER" => "CHAR(36)".to_string(),
            "XML" => "LONGTEXT".to_string(),
            "GEOMETRY" | "GEOGRAPHY" => "GEOMETRY".to_string(),
            "HIERARCHYID" => "VARCHAR(255)".to_string(),
            "SQL_VARIANT" => "LONGTEXT".to_string(),
            _ => type_name.to_string(),
        })
    }

    fn mssql_to_sqlite(&self, type_name: &str) -> TypeMapperResult<String> {
        Ok(match type_name {
            "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "BIT" => "INTEGER".to_string(),
            "REAL" | "FLOAT" | "DECIMAL" | "NUMERIC" | "MONEY" | "SMALLMONEY" => "REAL".to_string(),
            "CHAR" | "VARCHAR" | "NCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "XML"
            | "UNIQUEIDENTIFIER" | "HIERARCHYID" | "SQL_VARIANT" => "TEXT".to_string(),
            "BINARY" | "VARBINARY" | "IMAGE" => "BLOB".to_string(),
            "DATE" | "TIME" | "DATETIME" | "SMALLDATETIME" | "DATETIME2" | "DATETIMEOFFSET" => {
                "TEXT".to_string()
            }
            "GEOMETRY" | "GEOGRAPHY" => "TEXT".to_string(),
            _ => type_name.to_string(),
        })
    }
}

/// Convenience function to map a type between dialects
pub fn map_type(
    source_type: &str,
    source_dialect: &str,
    target_dialect: &str,
) -> TypeMapperResult<String> {
    let source = Dialect::from_str(source_dialect).ok_or_else(|| {
        TypeMapperError::InvalidTypeFormat(format!("Unknown dialect: {}", source_dialect))
    })?;
    let target = Dialect::from_str(target_dialect).ok_or_else(|| {
        TypeMapperError::InvalidTypeFormat(format!("Unknown dialect: {}", target_dialect))
    })?;

    let mapper = TypeMapper::new();
    mapper.map_type(source_type, source, target)
}
