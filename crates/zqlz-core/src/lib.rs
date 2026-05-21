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
pub mod dialect_syntax;
pub mod dialects;
pub mod document_syntax;
mod driver;
mod driver_capabilities;
mod error;
pub mod execution_unit;
mod feature_set;
mod formatter;
mod naming_validation;
mod object_identity;
pub mod paths;
pub mod redis_command_catalog;
pub mod redis_command_spec;
mod schema;
pub mod security;
pub mod sql_lexing;
pub mod syntax_brackets;
pub mod transaction;
mod types;

pub use connection::*;
pub use connection_security::*;
pub use dialect::*;
pub use dialect_syntax::{
    IncompleteSqlCompletionContext, SignificantSqlToken, SqlCompletionTriggerContext,
    SyntaxCompletionContextKind, SyntaxCompletionItemKind, SyntaxContextToken,
    SyntaxOperatorCompletion, SyntaxTermProfile, active_sql_context_tokens,
    active_sql_unquoted_word_tokens, aggregate_function_sort_text, base_function_terms,
    base_keyword_terms, base_type_terms, compare_syntax_completion_sort_texts,
    condition_operator_completion_terms, create_table_keyword_insert_text,
    create_table_keyword_sort_text, create_table_prefers_constraint_terms,
    data_type_completion_detail, data_type_insert_text, data_type_labels, data_type_matches_suffix,
    data_type_sort_text, dialect_function_detail, dialect_function_sort_text,
    dialect_function_terms, dialect_keyword_terms, dialect_type_terms,
    function_category_allowed_in_condition, function_category_sort_bucket, function_term_category,
    function_term_rank, incomplete_sql_completion_context, is_dialect_reserved_sql_symbol,
    is_inside_active_create_table_column_list, is_valid_dialect_sql_identifier,
    keyword_allowed_in_completion_context, keyword_allowed_in_condition, keyword_allowed_in_cte,
    keyword_allowed_in_from_clause, keyword_allowed_in_join_condition,
    keyword_allowed_in_select_expression, keyword_category_rank,
    keyword_completion_sort_prefix_for_context, keyword_context_categories,
    keyword_context_categories_for_clause, keyword_context_implies_condition_clause,
    keyword_context_is_start, keyword_info_relevant_for_context, keyword_relevant_for_context,
    keyword_suffix_for_context, keyword_term_allowed_in_condition, keyword_term_allowed_in_cte,
    keyword_term_allowed_in_from_clause, keyword_term_allowed_in_join_condition,
    keyword_term_allowed_in_select_expression, keyword_term_category,
    keyword_term_implies_condition_clause, keyword_term_is_context_clause, keyword_term_rank,
    keyword_token_byte_ranges, line_column_for_offset, line_column_to_offset,
    normalize_syntax_profile, scalar_function_sort_text, select_wildcard_token_ranges,
    semicolon_insertion_offset, significant_sql_word_lookback, sql_completion_trigger_context,
    sql_function_call_context, sql_has_query_context_tokens, sql_query_context_token,
    sqlparser_location_to_offset, supported_syntax_profiles, syntax_completion_context_bucket,
    syntax_completion_dedup_key, syntax_completion_prefix, syntax_completion_word_char,
    syntax_operator_completions_for_context, syntax_term_profile, unclosed_quote_insertion,
};
// Re-export specific types from dialect_config to avoid conflicts with dialect module
pub use dialect_config::{
    CommentsConfig,
    CompletionsConfig,
    CustomValidatorKind,
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
    completions_config_from_dialect_info,
    dialect_bundle_from_legacy_info,
};
// Re-export dialects module
pub use dialects::{
    BracketCapability, DIALECT_REGISTRY, DialectProfile, DialectRegistry, FoldingCapability,
    FormatterCapability, HighlightQueryLanguage, ParameterPlaceholderCapability, ParserCapability,
    SQL_DIAGNOSTIC_RULES, SQL_FOLDING_RULES, SqlDialect, SyntaxDiagnosticRules,
    SyntaxDocumentSymbolMode, SyntaxDriverCapabilities, SyntaxExecutionUnitMode,
    SyntaxFoldingRules, SyntaxOverlayMode, TreeSitterGrammar, ValidationError,
    get_dialect_language_name, get_dialect_profile, get_highlight_language,
    get_highlight_query_language, get_parameter_placeholder_capability, get_sql_dialect,
    get_syntax_driver_capabilities, get_syntax_term_profile, get_tree_sitter_grammar,
    is_sql_driver, line_comment_prefix_for_profile, markdown_fence_language_for_capabilities,
    paints_quoted_identifier, supports_dollar_quoted_strings,
    syntax_driver_capabilities_from_bundle, uses_command_syntax, uses_document_syntax,
    uses_sql_syntax_overlays,
};
pub use document_syntax::{
    DocumentSyntaxToken, DriverSyntaxOverlayKind, DriverSyntaxOverlayToken,
    driver_document_symbols, driver_document_symbols_for_capabilities,
    driver_syntax_overlay_tokens, driver_syntax_overlay_tokens_for_capabilities,
    driver_syntax_protected_ranges, driver_syntax_protected_ranges_for_capabilities,
    mongodb_syntax_tokens,
};
pub use driver::*;
pub use driver_capabilities::*;
pub use error::*;
pub use execution_unit::{
    ExecutionUnit, execution_unit_for_capabilities, execution_unit_for_profile,
};
pub use feature_set::*;
pub use formatter::*;
pub use naming_validation::*;
pub use object_identity::*;
pub use schema::*;
pub use security::*;
pub use sql_lexing::{
    SqlDocumentSymbol, SqlParameterPlaceholder, SqlParameterPlaceholderKind, SqlProtectedRange,
    SqlProtectedRangeKind, SqlStatementSpan, SqlSymbolWord, is_position_in_sql_ranges,
    qualified_sql_reference_at_offset, qualified_sql_reference_prefix, split_sql_statement_spans,
    split_sql_statements, sql_document_symbols, sql_matching_qualified_symbol_segments,
    sql_matching_symbol_segments, sql_parameter_placeholders,
    sql_parameter_placeholders_with_ranges, sql_protected_range_at, sql_protected_ranges,
    sql_protected_ranges_with_unclosed_dollar_strings, sql_statement_span_at, sql_symbol_at_offset,
    sql_symbol_word_spans, sql_symbol_words, unclosed_dollar_quoted_string_start,
};
pub use syntax_brackets::{
    SyntaxBracketPair, SyntaxBracketScanMode, syntax_bracket_pairs_for_capabilities,
    syntax_bracket_pairs_for_profile, syntax_bracket_scan_mode_for_capabilities,
    syntax_bracket_scan_mode_for_profile,
};
pub use transaction::*;
pub use types::*;
