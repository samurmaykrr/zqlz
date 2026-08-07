//! SQL Language Server Protocol integration
//!
//! Provides IntelliSense, completions, linting, and error diagnostics for SQL queries.
//! Uses sqlparser-rs for accurate SQL parsing and validation.

use anyhow::Result;
use lsp_types::{
    CodeAction, Diagnostic, GotoDefinitionResponse, Hover, Location, SignatureHelp, WorkspaceEdit,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_services::{DatabaseSchema, SchemaService};
use zqlz_ui::widgets::Rope;

mod best_practices;
mod code_actions;
mod completion_cache;
mod completion_context;
mod completion_dispatch;
mod completion_edit;
mod completion_engine;
mod completion_items;
mod completion_ranking;
mod completions;
mod context_analyzer;
mod context_rescue;
mod context_tables;
mod diagnostics;
mod diagnostics_schema;
mod diagnostics_syntax;
mod dialect;
mod dialect_completions;
mod document_completions;
mod fuzzy_matcher;
mod hover;
mod hover_render;
mod keyword_completions;
mod lsp_lifecycle;
mod navigation;
mod operator_completions;
mod parser_pool;
mod query_sources;
mod redis_completion;
mod redis_validation_error;
mod redis_validator;
mod schema_cache;
mod schema_completions;
mod schema_fetch;
mod schema_lookup;
mod schema_metadata;
mod schema_validation_expr;
mod schema_validation_issue;
mod schema_validation_lookup;
mod schema_validation_query;
mod schema_validation_table;
mod schema_validator;
mod signature_help;
mod snippets;
mod sql_dialect;
mod validation;

#[cfg(test)]
mod tests;

pub use completion_cache::{CacheStats, CompletionCache};
pub use context_analyzer::{ContextAnalyzer, SqlContext as AstSqlContext};
pub use context_tables::TableRef;
pub use dialect::SqlDialect;
pub use sql_dialect::{SqlDialectConfig, get_sql_dialect_config, is_sql_driver};

pub use completions::SqlCompletionProvider;
pub use diagnostics::{
    DIAGNOSTIC_CODE_SQL_TOKENIZER, DIAGNOSTIC_CODE_SQLPARSER_SYNTAX,
    DIAGNOSTIC_CODE_TREE_SITTER_SYNTAX, SqlDiagnostics,
};
pub use fuzzy_matcher::{FuzzyMatch, FuzzyMatcher, MatchQuality};
pub use hover::SqlHoverProvider;
pub use schema_cache::{
    ColumnInfo, DatabaseObject, FunctionInfo, IndexInfo, ParameterDirection, ParameterInfo,
    ProcedureInfo, SchemaCache, TableInfo, TriggerInfo, ViewInfo,
};
pub use schema_validation_issue::{ValidationIssue, ValidationSeverity};
pub use schema_validator::SchemaValidator;

pub(crate) fn clamp_to_char_boundary(text: &str, offset: usize) -> usize {
    let mut offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

/// SQL Language Server configuration
#[allow(dead_code)]
pub struct SqlLsp {
    /// Current database connection
    connection_id: Option<Uuid>,

    /// Connection to get schema information
    connection: Option<Arc<dyn Connection>>,

    /// Database driver type (sqlite, mysql, postgres, etc.)
    pub(crate) driver_type: String,

    /// SQL dialect for this connection
    pub(crate) dialect: SqlDialect,

    /// Cached schema information
    pub(crate) schema_cache: SchemaCache,

    /// Active database selected by the user for multi-database drivers
    /// (for example MySQL where schema introspection can target databases
    /// other than the connection default).
    active_database: Option<String>,

    /// Active schema selected by the user for schema-scoped drivers.
    active_schema: Option<String>,

    /// Schema service for cached schema introspection
    schema_service: Arc<SchemaService>,

    /// Syntax-tree based context analyzer
    context_analyzer: ContextAnalyzer,

    /// Schema validator for semantic validation
    schema_validator: SchemaValidator,

    /// SQL diagnostics with precise error positioning
    sql_diagnostics: SqlDiagnostics,

    /// Fuzzy matcher for flexible completions
    fuzzy_matcher: FuzzyMatcher,

    /// Completion cache for performance
    completion_cache: CompletionCache,

    /// True while a background schema fetch is in flight.
    /// Used to suppress the keyword fallback in table-name completion contexts so
    /// the user sees an empty list (indicating "loading") rather than irrelevant keywords.
    pub schema_loading: bool,

    /// Monotonically-increasing counter incremented before each background fetch.
    /// The fetching task captures its epoch; if the counter has advanced by the time
    /// the result arrives, the result is discarded so a newer fetch can apply instead.
    fetch_epoch: u64,
}

#[allow(dead_code)]
impl SqlLsp {
    /// Get the appropriate SQL dialect for parsing
    #[allow(dead_code)]
    fn get_dialect(&self) -> Box<dyn Dialect> {
        zqlz_core::get_sql_dialect(&self.driver_type)
            .map(|dialect| dialect.sqlparser_dialect())
            .unwrap_or_else(|| Box::new(GenericDialect {}))
    }

    /// Get the dialect name for display
    fn get_dialect_name(&self) -> &str {
        zqlz_core::get_dialect_profile(self.driver_type.trim())
            .map(|profile| profile.language_name)
            .unwrap_or("SQL")
    }

    /// Fetch stored procedures (driver-specific)
    async fn fetch_procedures(&self, conn: &dyn Connection) -> Result<Vec<ProcedureInfo>> {
        schema_fetch::fetch_procedures(conn).await
    }

    /// Fetch functions (driver-specific)
    async fn fetch_functions(&self, conn: &dyn Connection) -> Result<Vec<FunctionInfo>> {
        schema_fetch::fetch_functions(conn).await
    }

    /// Fetch triggers (driver-specific)
    async fn fetch_triggers(&self, conn: &dyn Connection) -> Result<Vec<TriggerInfo>> {
        schema_fetch::fetch_triggers(conn).await
    }

    /// Fetch indexes
    async fn fetch_indexes(&self, conn: &dyn Connection) -> Result<Vec<IndexInfo>> {
        schema_fetch::fetch_indexes(conn).await
    }

    /// Get hover information for a position
    pub fn get_hover(&self, text: &Rope, offset: usize) -> Option<Hover> {
        hover::get_hover(self, text, offset)
    }

    /// Get definition location for the symbol at the given position
    ///
    /// For SQL, this provides go-to-definition functionality for:
    /// - Table references: Returns location info about the table
    /// - Column references: Returns the table that contains the column
    /// - View/function/procedure references: Returns info about the object
    ///
    /// Since we don't have actual DDL file locations, this returns a GotoDefinitionResponse
    /// with location information about the database object.
    pub fn get_definition(&self, text: &Rope, offset: usize) -> Option<GotoDefinitionResponse> {
        navigation::get_definition(self, text, offset)
    }

    /// Find all references to a symbol at the given offset.
    ///
    /// This searches through the text for all occurrences of the identifier
    /// and returns their locations. It excludes SQL keywords from results.
    ///
    /// Returns a list of locations where the symbol is referenced.
    pub fn get_references(&self, text: &Rope, offset: usize) -> Vec<Location> {
        navigation::get_references(self, text, offset)
    }

    /// Rename a symbol at the given offset to a new name.
    ///
    /// This method:
    /// 1. Gets the word at the cursor position
    /// 2. Validates the new name is a valid SQL identifier
    /// 3. Finds all occurrences of the word in the text
    /// 4. Returns a WorkspaceEdit with TextEdits for all locations
    ///
    /// Returns None if:
    /// - No word is at the cursor position
    /// - The word is a SQL keyword
    /// - The new name is not a valid SQL identifier
    pub fn rename(&self, text: &Rope, offset: usize, new_name: &str) -> Option<WorkspaceEdit> {
        navigation::rename(self, text, offset, new_name)
    }

    /// Get code actions for a given position in the text.
    ///
    /// Code actions provide quick fixes for diagnostics or suggestions
    /// based on the context at the cursor position.
    ///
    /// Returns a list of code actions that can be applied.
    pub fn get_code_actions(
        &self,
        text: &Rope,
        offset: usize,
        diagnostics: &[Diagnostic],
    ) -> Vec<CodeAction> {
        let dialect = self.get_dialect();
        code_actions::get_code_actions(text, offset, diagnostics, dialect.as_ref())
    }

    fn qualified_reference_at_offset(
        &self,
        text: &Rope,
        offset: usize,
    ) -> Option<(String, String)> {
        schema_lookup::qualified_reference_at_offset(self, text, offset)
    }

    fn resolve_table_identifier(
        &self,
        identifier: &str,
        text: &Rope,
        offset: usize,
    ) -> Option<String> {
        schema_lookup::resolve_table_identifier(self, identifier, text, offset)
    }

    fn table_info(&self, table_name: &str) -> Option<&TableInfo> {
        schema_lookup::table_info(self, table_name)
    }

    fn columns_for_table(&self, table_name: &str) -> Option<&Vec<ColumnInfo>> {
        schema_lookup::columns_for_table(self, table_name)
    }

    fn sequence_info(&self, sequence_name: &str) -> Option<&zqlz_core::SequenceInfo> {
        schema_lookup::sequence_info(self, sequence_name)
    }

    fn foreign_keys_for_table(&self, table_name: &str) -> Option<&Vec<zqlz_core::ForeignKeyInfo>> {
        schema_lookup::foreign_keys_for_table(self, table_name)
    }

    fn reverse_foreign_keys_for_table(
        &self,
        table_name: &str,
    ) -> Option<&Vec<(String, zqlz_core::ForeignKeyInfo)>> {
        schema_lookup::reverse_foreign_keys_for_table(self, table_name)
    }

    fn derived_columns_for_identifier(&self, identifier: &str, text: &Rope) -> Option<Vec<String>> {
        schema_lookup::derived_columns_for_identifier(self, identifier, text)
    }

    fn derived_columns_for_identifier_at(
        &self,
        identifier: &str,
        text: &Rope,
        offset: usize,
    ) -> Option<Vec<String>> {
        schema_lookup::derived_columns_for_identifier_at(self, identifier, text, offset)
    }

    /// Get signature help for SQL function calls
    ///
    /// Returns signature help when the cursor is inside a function call.
    /// Shows function parameters and highlights the current parameter based on cursor position.
    pub fn get_signature_help(&self, text: &Rope, offset: usize) -> Option<SignatureHelp> {
        let dialect = self.get_dialect();
        signature_help::get_signature_help(
            text,
            offset,
            dialect.as_ref(),
            &self.dialect.dialect_info(),
        )
    }

    /// Validate SQL and return diagnostics with precise error positions
    ///
    /// Uses tree-sitter for accurate error node detection and sqlparser for
    /// syntax validation. Returns diagnostics with exact line/column positions.
    ///
    /// For non-SQL dialects (like Redis), validation is skipped and no errors
    /// are returned for valid dialect commands.
    pub fn validate_sql(&mut self, text: &Rope) -> Vec<Diagnostic> {
        self.validate_sql_at_cursor(text, None)
    }

    /// Same as [`Self::validate_sql`], but tolerates a qualified reference that is
    /// still being typed at `cursor_offset` (`select c.|  from t`) instead of
    /// reporting it as a syntax error.
    pub fn validate_sql_at_cursor(
        &mut self,
        text: &Rope,
        cursor_offset: Option<usize>,
    ) -> Vec<Diagnostic> {
        let dialect_name = self.get_dialect_name().to_string();
        validation::validate_sql(
            &mut self.sql_diagnostics,
            text,
            &self.schema_cache,
            &self.driver_type,
            self.dialect,
            &dialect_name,
            cursor_offset,
        )
    }

    /// Resolve a table alias to the actual table name using the provided table references
    /// This uses the AST-extracted TableRef information for accurate alias resolution
    fn resolve_alias_from_context(
        &self,
        identifier: &str,
        available_tables: &[TableRef],
    ) -> String {
        schema_lookup::resolve_alias_from_context(identifier, available_tables)
    }

    /// Resolve a table alias to the actual table name
    fn resolve_alias_to_table(&self, alias: &str, text: &Rope, offset: usize) -> Option<String> {
        schema_lookup::resolve_alias_to_table(self, alias, text, offset)
    }

    /// Get the word at a given byte offset
    fn get_word_at_offset(&self, text: &Rope, offset: usize) -> Option<String> {
        schema_lookup::get_word_at_offset(self, text, offset)
    }

    /// Test helper to set schema cache directly
    #[cfg(test)]
    pub fn set_schema_cache(&mut self, cache: SchemaCache) {
        self.schema_cache = cache;
    }

    /// Get the current schema as DatabaseSchema format for use with SchemaMetadataProvider
    ///
    /// This exports the cached schema information in a format that can be used
    /// by the schema metadata overlay to show table/column information.
    pub fn get_schema_for_metadata(&self) -> DatabaseSchema {
        schema_metadata::schema_for_metadata(&self.schema_cache)
    }
}

// Note: No Default impl since SqlLsp requires SchemaService parameter
