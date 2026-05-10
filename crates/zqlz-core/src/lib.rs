//! ZQLZ Core - Core abstractions and traits for the database IDE
//!
//! This crate provides the fundamental traits and types that all other
//! ZQLZ crates depend on. It defines:
//!
//! - `DatabaseDriver` - Trait for database driver implementations
//! - `Connection` - Trait for database connections
//! - `SchemaIntrospection` - Trait for schema inspection
//! - `DialectInfo` - SQL dialect metadata (keywords, functions, types)
//! - Common types like `Value`, `Row`, `Column`, etc.

pub mod command;
mod connection;
mod connection_security;
mod dialect;
pub mod dialect_config;
pub mod dialects;
mod driver;
mod driver_capabilities;
mod error;
mod feature_set;
mod formatter;
mod naming_validation;
mod object_identity;
pub mod paths;
mod schema;
pub mod security;
pub mod transaction;
mod types;

pub use connection::*;
pub use connection_security::*;
pub use dialect::*;
// Re-export specific types from dialect_config to avoid conflicts with dialect module
pub use dialect_config::{
    CommentsConfig,
    CompletionsConfig,
    DataTypeDef,
    DiagnosticSeverity,
    DiagnosticsConfig,
    DialectBundle,
    DialectConfig,
    FunctionDef,
    GrammarConfig,
    GrammarType,
    KeywordDef,
    LanguageType,
    ParserConfig,
    SnippetDef,
    SyntaxConfig,
    ValidationRule,
    ValidationType,
    // Note: KeywordCategory, FunctionCategory, DataTypeCategory are NOT re-exported
    // because they would conflict with the enums in dialect.rs.
    // Use dialect_config::KeywordCategory etc. to access the TOML config versions.
};
// Re-export dialects module
pub use dialects::{
    BracketCapability, DIALECT_REGISTRY, DialectProfile, DialectRegistry, FoldingCapability,
    FormatterCapability, ParserCapability, SqlDialect, TreeSitterGrammar, ValidationError,
    get_dialect_profile, get_sql_dialect, is_sql_driver,
};
pub use driver::*;
pub use driver_capabilities::*;
pub use error::*;
pub use feature_set::*;
pub use formatter::*;
pub use naming_validation::*;
pub use object_identity::*;
pub use schema::*;
pub use security::*;
pub use transaction::*;
pub use types::*;
