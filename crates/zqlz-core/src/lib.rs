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

mod connection;
mod connection_security;
mod dialect;
pub mod dialect_config;
pub mod dialects;
mod driver;
mod error;
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
    BracketCapability,
    DialectProfile,
    DialectRegistry,
    FoldingCapability,
    FormatterCapability,
    ParserCapability,
    SqlDialect,
    TreeSitterGrammar,
    ValidationError,
    get_dialect_profile,
    get_sql_dialect,
    is_sql_driver,
    DIALECT_REGISTRY,
};
pub use driver::*;
pub use error::*;
pub use schema::*;
pub use security::*;
pub use transaction::*;
pub use types::*;
