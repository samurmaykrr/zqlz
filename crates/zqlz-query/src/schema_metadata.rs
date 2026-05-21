//! Schema metadata overlay module
//!
//! This module provides infrastructure for showing schema metadata overlays
//! when hovering over table/column names in the editor, replacing the broken
//! LSP hover popover with a schema-aware metadata display.
//!
//! Architecture:
//! - SchemaMetadataRenderer: Trait for rendering metadata overlays (similar to Zed's BlameRenderer)
//! - SchemaMetadataProvider: Trait for fetching schema data (implemented for DatabaseSchema)
//! - SchemaMetadataOverlay: UI component that displays the metadata

use gpui::{AnyElement, ScrollHandle, TextStyle};
use std::collections::HashMap;

use zqlz_core::ForeignKeyInfo;
use zqlz_core::IndexInfo;
use zqlz_services::{
    ColumnInfo as ServicesColumnInfo, DatabaseSchema, TableDetails as ServicesTableDetails,
};

/// Information about a schema symbol (table, column, etc.)
#[derive(Debug, Clone)]
pub struct SchemaSymbolInfo {
    /// The symbol name (table name, column name, etc.)
    pub name: String,
    /// The symbol type (table, view, column, index, etc.)
    pub symbol_type: SchemaSymbolType,
    /// Optional detailed information
    pub details: Option<SchemaSymbolDetails>,
}

/// Type of schema symbol
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaSymbolType {
    Table,
    View,
    Column,
    Index,
    ForeignKey,
    PrimaryKey,
    Function,
    Procedure,
    Trigger,
    Unknown,
}

/// Detailed information about a schema symbol
#[derive(Debug, Clone)]
pub struct SchemaSymbolDetails {
    /// For tables/views: column information
    pub columns: Option<Vec<ServicesColumnInfo>>,
    /// For tables: indexes
    pub indexes: Option<Vec<IndexInfo>>,
    /// For tables: foreign keys
    pub foreign_keys: Option<Vec<ForeignKeyInfo>>,
    /// For columns: the table they belong to
    pub table_name: Option<String>,
    /// For columns: data type
    pub data_type: Option<String>,
    /// For columns: whether nullable
    pub nullable: Option<bool>,
    /// For columns: default value
    pub default_value: Option<String>,
    /// For columns: primary key member
    pub is_primary_key: Option<bool>,
    /// Description/comment if available
    pub description: Option<String>,
    /// For tables: row count
    pub row_count: Option<usize>,
}

impl SchemaSymbolDetails {
    /// Create details for a column
    pub fn for_column(
        table_name: String,
        column: &ServicesColumnInfo,
        is_primary_key: bool,
    ) -> Self {
        Self {
            columns: None,
            indexes: None,
            foreign_keys: None,
            table_name: Some(table_name),
            data_type: Some(column.data_type.clone()),
            nullable: Some(column.nullable),
            default_value: column.default_value.clone(),
            is_primary_key: Some(is_primary_key),
            description: None,
            row_count: None,
        }
    }

    /// Create details for a table
    pub fn for_table(details: &ServicesTableDetails) -> Self {
        Self {
            columns: Some(details.columns.clone()),
            indexes: Some(details.indexes.clone()),
            foreign_keys: Some(details.foreign_keys.clone()),
            table_name: None,
            data_type: None,
            nullable: None,
            default_value: None,
            is_primary_key: None,
            description: None,
            row_count: details.row_count,
        }
    }
}

impl SchemaSymbolInfo {
    /// Create a table symbol
    pub fn table(name: String, details: ServicesTableDetails) -> Self {
        Self {
            name,
            symbol_type: SchemaSymbolType::Table,
            details: Some(SchemaSymbolDetails::for_table(&details)),
        }
    }

    /// Create a column symbol
    pub fn column(
        name: String,
        table_name: String,
        column: &ServicesColumnInfo,
        is_primary_key: bool,
    ) -> Self {
        Self {
            name,
            symbol_type: SchemaSymbolType::Column,
            details: Some(SchemaSymbolDetails::for_column(
                table_name,
                column,
                is_primary_key,
            )),
        }
    }

    /// Create a view symbol
    pub fn view(name: String) -> Self {
        Self {
            name,
            symbol_type: SchemaSymbolType::View,
            details: None,
        }
    }

    /// Get display name for symbol type
    pub fn symbol_type_name(&self) -> &'static str {
        match self.symbol_type {
            SchemaSymbolType::Table => "Table",
            SchemaSymbolType::View => "View",
            SchemaSymbolType::Column => "Column",
            SchemaSymbolType::Index => "Index",
            SchemaSymbolType::ForeignKey => "Foreign Key",
            SchemaSymbolType::PrimaryKey => "Primary Key",
            SchemaSymbolType::Function => "Function",
            SchemaSymbolType::Procedure => "Procedure",
            SchemaSymbolType::Trigger => "Trigger",
            SchemaSymbolType::Unknown => "Unknown",
        }
    }
}

/// Trait for rendering schema metadata overlays
///
/// Similar to Zed's BlameRenderer trait, this allows different
/// rendering implementations while keeping the data fetching separate.
pub trait SchemaMetadataRenderer: Send {
    /// Render a schema metadata entry (table info, column info, etc.)
    fn render_metadata_entry(
        &self,
        style: &TextStyle,
        symbol_info: SchemaSymbolInfo,
        window: &mut gpui::Window,
        cx: &mut gpui::App,
    ) -> Option<AnyElement>;

    /// Render an inline metadata indicator (shown in gutter or inline)
    fn render_inline_metadata(
        &self,
        style: &TextStyle,
        symbol_info: SchemaSymbolInfo,
        cx: &mut gpui::App,
    ) -> Option<AnyElement>;

    /// Render a popover with full metadata details
    fn render_metadata_popover(
        &self,
        symbol_info: SchemaSymbolInfo,
        scroll_handle: ScrollHandle,
        window: &mut gpui::Window,
        cx: &mut gpui::App,
    ) -> Option<AnyElement>;

    /// Handle click on metadata element (e.g., navigate to table definition)
    fn open_metadata_definition(
        &self,
        symbol_info: SchemaSymbolInfo,
        window: &mut gpui::Window,
        cx: &mut gpui::App,
    );
}

/// Default renderer for builds that do not install schema metadata UI.
impl SchemaMetadataRenderer for () {
    fn render_metadata_entry(
        &self,
        _style: &TextStyle,
        _symbol_info: SchemaSymbolInfo,
        _window: &mut gpui::Window,
        _cx: &mut gpui::App,
    ) -> Option<AnyElement> {
        None
    }

    fn render_inline_metadata(
        &self,
        _style: &TextStyle,
        _symbol_info: SchemaSymbolInfo,
        _cx: &mut gpui::App,
    ) -> Option<AnyElement> {
        None
    }

    fn render_metadata_popover(
        &self,
        _symbol_info: SchemaSymbolInfo,
        _scroll_handle: ScrollHandle,
        _window: &mut gpui::Window,
        _cx: &mut gpui::App,
    ) -> Option<AnyElement> {
        None
    }

    fn open_metadata_definition(
        &self,
        _symbol_info: SchemaSymbolInfo,
        _window: &mut gpui::Window,
        _cx: &mut gpui::App,
    ) {
    }
}

/// Global renderer holder (similar to GlobalBlameRenderer)
#[allow(dead_code)]
pub(crate) struct GlobalSchemaMetadataRenderer(pub std::sync::Arc<dyn SchemaMetadataRenderer>);

impl gpui::Global for GlobalSchemaMetadataRenderer {}

/// Schema metadata provider trait
///
/// Implement this to provide schema metadata for different data sources.
pub trait SchemaMetadataProvider: Send {
    /// Get table information by name
    fn get_table_info(&self, table_name: &str) -> Option<ServicesTableDetails>;

    /// Get column information for a specific table
    fn get_column_info(&self, table_name: &str, column_name: &str) -> Option<ServicesColumnInfo>;

    /// Find symbol at the given text offset
    fn find_symbol_at_offset(&self, text: &str, offset: usize) -> Option<SchemaSymbolInfo>;

    /// Get all tables in the schema
    fn get_tables(&self) -> Vec<String>;

    /// Get all views in the schema
    fn get_views(&self) -> Vec<String>;
}

/// Simple in-memory schema metadata provider using DatabaseSchema
pub struct SchemaMetadata {
    schema: Option<DatabaseSchema>,
    /// Optional separate column data for better lookups
    columns: HashMap<String, Vec<ServicesColumnInfo>>,
}

impl SchemaMetadata {
    /// Create a new SchemaMetadata from a DatabaseSchema
    pub fn new(schema: DatabaseSchema) -> Self {
        Self {
            schema: Some(schema),
            columns: HashMap::new(),
        }
    }

    /// Create an empty SchemaMetadata
    pub fn empty() -> Self {
        Self {
            schema: None,
            columns: HashMap::new(),
        }
    }

    /// Update the schema
    pub fn set_schema(&mut self, schema: DatabaseSchema) {
        self.schema = Some(schema);
    }

    /// Clear the schema
    pub fn clear(&mut self) {
        self.schema = None;
        self.columns.clear();
    }

    /// Add column information for a table
    pub fn add_table_columns(&mut self, table_name: &str, columns: Vec<ServicesColumnInfo>) {
        self.columns.insert(table_name.to_lowercase(), columns);
    }

    /// Check if we have column information for a table
    pub fn has_columns(&self, table_name: &str) -> bool {
        self.columns.contains_key(&table_name.to_lowercase())
    }
}

impl SchemaMetadataProvider for SchemaMetadata {
    fn get_table_info(&self, table_name: &str) -> Option<ServicesTableDetails> {
        let schema = self.schema.as_ref()?;

        // First, try to find in table_infos
        let table_info = schema
            .table_infos
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(table_name));

        // Get columns from our columns store or use empty
        let columns = self
            .columns
            .get(&table_name.to_lowercase())
            .cloned()
            .unwrap_or_else(Vec::new);

        // Get indexes from schema
        let indexes = schema
            .table_indexes
            .get(table_name)
            .cloned()
            .unwrap_or_default();

        // Foreign keys would need separate fetching
        let foreign_keys: Vec<ForeignKeyInfo> = vec![];

        // Get primary key columns from column info
        let primary_key_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        // If we found the table in table_infos, use it; otherwise create basic info from tables list
        if let Some(info) = table_info {
            Some(ServicesTableDetails {
                name: info.name.clone(),
                table_type: info.table_type,
                columns,
                indexes,
                foreign_keys,
                constraints: Vec::new(),
                triggers: Vec::new(),
                primary_key_columns,
                row_count: info.row_count.map(|c| c as usize),
            })
        } else if schema
            .tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(table_name))
        {
            // Table is in the tables list but not in table_infos - create basic details
            Some(ServicesTableDetails {
                name: table_name.to_string(),
                table_type: zqlz_core::TableType::Table,
                columns,
                indexes,
                foreign_keys,
                constraints: Vec::new(),
                triggers: Vec::new(),
                primary_key_columns,
                row_count: None,
            })
        } else {
            None
        }
    }

    fn get_column_info(&self, table_name: &str, column_name: &str) -> Option<ServicesColumnInfo> {
        // First try our columns store
        if let Some(columns) = self.columns.get(&table_name.to_lowercase()) {
            return columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(column_name))
                .cloned();
        }

        // Fall back to table info if we have it
        self.get_table_info(table_name)?
            .columns
            .into_iter()
            .find(|c| c.name.eq_ignore_ascii_case(column_name))
    }

    fn find_symbol_at_offset(&self, text: &str, offset: usize) -> Option<SchemaSymbolInfo> {
        let schema = self.schema.as_ref()?;

        let token = extract_identifier_at_offset(text, offset)?;

        if let Some((table_name, _table_range)) =
            previous_qualified_identifier(text, token.range.start)
            && let Some(column_info) = self.get_column_info(&table_name, &token.name)
        {
            let is_pk = column_info.is_primary_key;
            return Some(SchemaSymbolInfo::column(
                token.name,
                table_name,
                &column_info,
                is_pk,
            ));
        }

        let word = token.name.trim();

        if word.is_empty() {
            return None;
        }

        // Check if it's a table name (case-insensitive)
        if schema.tables.iter().any(|t| t.eq_ignore_ascii_case(word))
            && let Some(details) = self.get_table_info(word)
        {
            return Some(SchemaSymbolInfo::table(word.to_string(), details));
        }

        // Check if it's a view name
        if schema.views.iter().any(|v| v.eq_ignore_ascii_case(word)) {
            return Some(SchemaSymbolInfo::view(word.to_string()));
        }

        // Check if it's a materialized view
        if schema
            .materialized_views
            .iter()
            .any(|v| v.eq_ignore_ascii_case(word))
        {
            return Some(SchemaSymbolInfo::view(word.to_string()));
        }

        // Check if it's a function
        if schema
            .functions
            .iter()
            .any(|f| f.eq_ignore_ascii_case(word))
        {
            return Some(SchemaSymbolInfo {
                name: word.to_string(),
                symbol_type: SchemaSymbolType::Function,
                details: None,
            });
        }

        // Check if it's a procedure
        if schema
            .procedures
            .iter()
            .any(|p| p.eq_ignore_ascii_case(word))
        {
            return Some(SchemaSymbolInfo {
                name: word.to_string(),
                symbol_type: SchemaSymbolType::Procedure,
                details: None,
            });
        }

        // Check if it's a trigger
        if schema.triggers.iter().any(|t| t.eq_ignore_ascii_case(word)) {
            return Some(SchemaSymbolInfo {
                name: word.to_string(),
                symbol_type: SchemaSymbolType::Trigger,
                details: None,
            });
        }

        None
    }

    fn get_tables(&self) -> Vec<String> {
        self.schema
            .as_ref()
            .map(|s| s.tables.clone())
            .unwrap_or_default()
    }

    fn get_views(&self) -> Vec<String> {
        self.schema
            .as_ref()
            .map(|s| s.views.clone())
            .unwrap_or_default()
    }
}

/// Check if a string is a valid SQL identifier
#[cfg(test)]
fn is_valid_identifier(s: &str) -> bool {
    let Some(first) = s.chars().next() else {
        return false;
    };
    if !first.is_alphabetic() && first != '_' {
        return false;
    }
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IdentifierToken {
    name: String,
    range: std::ops::Range<usize>,
}

/// Extract the word at the given offset
#[cfg(test)]
fn extract_word_at_offset(text: &str, offset: usize) -> Option<String> {
    extract_identifier_at_offset(text, offset).map(|token| token.name)
}

fn extract_identifier_at_offset(text: &str, offset: usize) -> Option<IdentifierToken> {
    if offset >= text.len() || !text.is_char_boundary(offset) {
        return None;
    }

    if let Some(quoted) = extract_quoted_identifier_at_offset(text, offset) {
        return Some(quoted);
    }

    let mut start = offset;
    while let Some((previous_start, character)) = previous_char(text, start) {
        if is_word_char(character) {
            start = previous_start;
        } else {
            break;
        }
    }

    let mut end = offset;
    while let Some((character_start, character_end, character)) = char_at(text, end) {
        if is_word_char(character) {
            end = character_end;
        } else if character_start == offset && start == end {
            return None;
        } else {
            break;
        }
    }

    if start == end {
        return None;
    }

    Some(IdentifierToken {
        name: text[start..end].to_string(),
        range: start..end,
    })
}

fn extract_quoted_identifier_at_offset(text: &str, offset: usize) -> Option<IdentifierToken> {
    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
        let Some(open_start) = scan_backward_for_delimiter(text, offset, open) else {
            continue;
        };
        let open_end = open_start + open.len_utf8();
        let Some(close_start) = scan_forward_for_delimiter(text, open_end, close) else {
            continue;
        };
        let close_end = close_start + close.len_utf8();
        if open_start <= offset && offset < close_end {
            return Some(IdentifierToken {
                name: text[open_end..close_start].to_string(),
                range: open_start..close_end,
            });
        }
    }
    None
}

fn previous_qualified_identifier(
    text: &str,
    identifier_start: usize,
) -> Option<(String, std::ops::Range<usize>)> {
    let (dot_start, dot) = previous_non_whitespace_char(text, identifier_start)?;
    if dot != '.' {
        return None;
    }
    let (previous_start, _previous) = previous_non_whitespace_char(text, dot_start)?;
    extract_identifier_at_offset(text, previous_start).map(|token| (token.name, token.range))
}

fn scan_backward_for_delimiter(text: &str, offset: usize, delimiter: char) -> Option<usize> {
    let mut cursor = offset.min(text.len());
    while let Some((start, character)) = previous_char(text, cursor) {
        if matches!(character, '\n' | '\r') {
            return None;
        }
        if character == delimiter {
            return Some(start);
        }
        cursor = start;
    }
    None
}

fn scan_forward_for_delimiter(text: &str, offset: usize, delimiter: char) -> Option<usize> {
    let mut cursor = offset;
    while let Some((start, end, character)) = char_at(text, cursor) {
        if matches!(character, '\n' | '\r') {
            return None;
        }
        if character == delimiter {
            return Some(start);
        }
        cursor = end;
    }
    None
}

fn previous_non_whitespace_char(text: &str, offset: usize) -> Option<(usize, char)> {
    let mut cursor = offset.min(text.len());
    while let Some((start, character)) = previous_char(text, cursor) {
        if !character.is_whitespace() {
            return Some((start, character));
        }
        cursor = start;
    }
    None
}

fn previous_char(text: &str, offset: usize) -> Option<(usize, char)> {
    text.get(..offset)?.char_indices().next_back()
}

fn char_at(text: &str, offset: usize) -> Option<(usize, usize, char)> {
    let (relative_start, character) = text.get(offset..)?.char_indices().next()?;
    let start = offset + relative_start;
    Some((start, start + character.len_utf8(), character))
}

/// Check if a character can be part of a word
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::IndexInfo;

    fn test_column(name: &str) -> ServicesColumnInfo {
        ServicesColumnInfo {
            name: name.to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_auto_increment: false,
            comment: None,
            enum_values: None,
        }
    }

    fn test_table(name: &str) -> zqlz_core::TableInfo {
        zqlz_core::TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: zqlz_core::TableType::Table,
            row_count: None,
            owner: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        }
    }

    fn test_schema(table_infos: Vec<zqlz_core::TableInfo>) -> DatabaseSchema {
        DatabaseSchema {
            tables: table_infos.iter().map(|table| table.name.clone()).collect(),
            table_infos,
            objects_panel_data: None,
            objects_panel_manifest: None,
            views: vec![],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec![],
            procedures: vec![],
            events: vec![],
            sequences: vec![],
            domains: vec![],
            types: vec![],
            extensions: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
            schema_names: vec![],
        }
    }

    #[test]
    fn test_schema_symbol_info_creation() {
        let info = SchemaSymbolInfo::view("my_view".to_string());
        assert_eq!(info.name, "my_view");
        assert_eq!(info.symbol_type, SchemaSymbolType::View);
    }

    #[test]
    fn test_schema_symbol_type_names() {
        let table = SchemaSymbolInfo::view("test".to_string());
        assert_eq!(table.symbol_type_name(), "View");
    }

    #[test]
    fn test_schema_metadata_empty() {
        let metadata = SchemaMetadata::empty();
        assert!(metadata.get_tables().is_empty());
        assert!(metadata.get_views().is_empty());
        assert!(metadata.get_table_info("any").is_none());
    }

    #[test]
    fn test_schema_metadata_with_schema() {
        let schema = DatabaseSchema {
            table_infos: vec![],
            objects_panel_data: None,
            objects_panel_manifest: None,
            tables: vec!["users".to_string(), "orders".to_string()],
            views: vec!["user_stats".to_string()],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec!["get_user".to_string()],
            procedures: vec![],
            events: vec![],
            sequences: vec![],
            domains: vec![],
            types: vec![],
            extensions: vec![],
            table_indexes: HashMap::new(),
            database_name: Some("testdb".to_string()),
            schema_name: Some("public".to_string()),
            schema_names: vec!["public".to_string()],
        };

        let metadata = SchemaMetadata::new(schema);
        assert_eq!(metadata.get_tables(), vec!["users", "orders"]);
        assert_eq!(metadata.get_views(), vec!["user_stats"]);
    }

    #[test]
    fn test_find_symbol_table() {
        let schema = DatabaseSchema {
            table_infos: vec![],
            objects_panel_data: None,
            objects_panel_manifest: None,
            tables: vec!["users".to_string(), "orders".to_string()],
            views: vec![],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec![],
            procedures: vec![],
            events: vec![],
            sequences: vec![],
            domains: vec![],
            types: vec![],
            extensions: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
            schema_names: vec![],
        };

        let metadata = SchemaMetadata::new(schema);

        // Find "users" in the text "SELECT * FROM users"
        // "SELECT * FROM " is 14 chars, so offset 14 points to 'u' in "users"
        let text = "SELECT * FROM users";
        let offset = 14; // Point to 'u' in "users"

        let result = metadata.find_symbol_at_offset(text, offset);
        assert!(result.is_some());
        let symbol = result.unwrap();
        assert_eq!(symbol.name, "users");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::Table);
    }

    #[test]
    fn test_find_symbol_view() {
        let schema = DatabaseSchema {
            table_infos: vec![],
            objects_panel_data: None,
            objects_panel_manifest: None,
            tables: vec![],
            views: vec!["active_users".to_string()],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec![],
            procedures: vec![],
            events: vec![],
            sequences: vec![],
            domains: vec![],
            types: vec![],
            extensions: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
            schema_names: vec![],
        };

        let metadata = SchemaMetadata::new(schema);

        // "SELECT * FROM " is 14 chars, so offset 14 points to 'a' in "active_users"
        let text = "SELECT * FROM active_users";
        let offset = 14; // Point to 'a' in "active_users"

        let result = metadata.find_symbol_at_offset(text, offset);
        assert!(result.is_some());
        let symbol = result.unwrap();
        assert_eq!(symbol.name, "active_users");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::View);
    }

    #[test]
    fn test_find_symbol_function() {
        let schema = DatabaseSchema {
            table_infos: vec![],
            objects_panel_data: None,
            objects_panel_manifest: None,
            tables: vec![],
            views: vec![],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec!["calculate_total".to_string()],
            procedures: vec![],
            events: vec![],
            sequences: vec![],
            domains: vec![],
            types: vec![],
            extensions: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
            schema_names: vec![],
        };

        let metadata = SchemaMetadata::new(schema);

        // "SELECT " is 7 chars, so offset 7 points to 'c' in "calculate_total"
        let text = "SELECT calculate_total(order_id) FROM orders";
        let offset = 7; // Point to 'c' in "calculate_total"

        let result = metadata.find_symbol_at_offset(text, offset);
        assert!(result.is_some());
        let symbol = result.unwrap();
        assert_eq!(symbol.name, "calculate_total");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::Function);
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(is_valid_identifier("users"));
        assert!(is_valid_identifier("user_id"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("table123"));
        assert!(!is_valid_identifier("123table"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("table-name"));
    }

    #[test]
    fn test_extract_word_at_offset() {
        assert_eq!(
            extract_word_at_offset("hello world", 0),
            Some("hello".to_string())
        );
        assert_eq!(
            extract_word_at_offset("hello world", 5),
            Some("hello".to_string())
        );
        assert_eq!(
            extract_word_at_offset("hello world", 6),
            Some("world".to_string())
        );
        assert_eq!(
            extract_word_at_offset("hello world", 10),
            Some("world".to_string())
        );
        assert_eq!(extract_word_at_offset("hello world", 11), None); // After end
        assert_eq!(extract_word_at_offset("hello", 100), None); // Way past end
    }

    #[test]
    fn test_extract_word_at_offset_handles_quoted_identifiers() {
        assert_eq!(
            extract_word_at_offset("SELECT \"User Name\" FROM accounts", 10),
            Some("User Name".to_string())
        );
        assert_eq!(
            extract_word_at_offset("SELECT [User Name] FROM accounts", 10),
            Some("User Name".to_string())
        );
        assert_eq!(
            extract_word_at_offset("SELECT `User Name` FROM accounts", 10),
            Some("User Name".to_string())
        );
    }

    #[test]
    fn test_find_symbol_quoted_table() {
        let metadata = SchemaMetadata::new(test_schema(vec![test_table("User Name")]));
        let text = "SELECT * FROM \"User Name\"";
        let offset = text.find("User").expect("quoted table");

        let symbol = metadata
            .find_symbol_at_offset(text, offset)
            .expect("quoted table symbol");

        assert_eq!(symbol.name, "User Name");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::Table);
    }

    #[test]
    fn test_find_symbol_qualified_column() {
        let mut metadata = SchemaMetadata::new(test_schema(vec![test_table("users")]));
        metadata.add_table_columns("users", vec![test_column("email")]);
        let text = "SELECT users.email FROM users";
        let offset = text.find("email").expect("column");

        let symbol = metadata
            .find_symbol_at_offset(text, offset)
            .expect("qualified column symbol");

        assert_eq!(symbol.name, "email");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::Column);
        assert_eq!(
            symbol.details.and_then(|details| details.table_name),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_find_symbol_qualified_quoted_column() {
        let mut metadata = SchemaMetadata::new(test_schema(vec![test_table("User Table")]));
        metadata.add_table_columns("User Table", vec![test_column("Email Address")]);
        let text = "SELECT \"User Table\".\"Email Address\" FROM \"User Table\"";
        let offset = text.find("Email").expect("quoted column");

        let symbol = metadata
            .find_symbol_at_offset(text, offset)
            .expect("quoted qualified column symbol");

        assert_eq!(symbol.name, "Email Address");
        assert_eq!(symbol.symbol_type, SchemaSymbolType::Column);
        assert_eq!(
            symbol.details.and_then(|details| details.table_name),
            Some("User Table".to_string())
        );
    }

    #[test]
    fn test_schema_details_for_column() {
        let column = ServicesColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: Some("nextval('users_id_seq')".to_string()),
            max_length: None,
            precision: None,
            scale: None,
            is_auto_increment: true,
            comment: None,
            enum_values: None,
        };

        let details = SchemaSymbolDetails::for_column("users".to_string(), &column, true);

        assert_eq!(details.table_name, Some("users".to_string()));
        assert_eq!(details.data_type, Some("INTEGER".to_string()));
        assert_eq!(details.nullable, Some(false));
        assert_eq!(details.is_primary_key, Some(true));
    }

    #[test]
    fn test_schema_details_for_table() {
        let table_details = ServicesTableDetails {
            name: "users".to_string(),
            table_type: zqlz_core::TableType::Table,
            columns: vec![
                ServicesColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_auto_increment: true,
                    comment: None,
                    enum_values: None,
                },
                ServicesColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_auto_increment: false,
                    comment: None,
                    enum_values: None,
                },
            ],
            indexes: vec![IndexInfo {
                name: "users_pkey".to_string(),
                columns: vec!["id".to_string()],
                is_unique: true,
                is_primary: true,
                index_type: "btree".to_string(),
                comment: None,
                ..Default::default()
            }],
            foreign_keys: vec![],
            constraints: Vec::new(),
            triggers: Vec::new(),
            primary_key_columns: vec!["id".to_string()],
            row_count: Some(100),
        };

        let details = SchemaSymbolDetails::for_table(&table_details);

        assert_eq!(details.columns.as_ref().map(|c| c.len()), Some(2));
        assert_eq!(details.indexes.as_ref().map(|i| i.len()), Some(1));
        assert_eq!(details.row_count, Some(100));
    }
}
