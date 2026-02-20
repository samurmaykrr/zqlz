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

/// Default no-op renderer (placeholder)
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
        let metadata = Self {
            schema: Some(schema),
            columns: HashMap::new(),
        };
        // Try to extract column info from objects_panel_data if available
        if let Some(ref schema) = metadata.schema {
            if let Some(ref _objects_data) = schema.objects_panel_data {
                // ObjectsPanelData has columns and rows but not in the format we need
                // This is a placeholder - actual column extraction would require
                // additional schema queries
            }
        }
        metadata
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
                columns,
                indexes,
                foreign_keys,
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
                columns,
                indexes,
                foreign_keys,
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

        // Get the text up to the offset for context analysis
        let text_before = &text[..offset.min(text.len())];
        let text_after = &text[offset.min(text.len())..];

        // Check for "table.column" pattern first (more specific)
        if let Some(dot_pos) = text_before.rfind(|c: char| !c.is_alphanumeric() && c != '_') {
            let potential_table = &text_before[dot_pos + 1..];
            if !potential_table.is_empty() && is_valid_identifier(potential_table) {
                // Check if there's a dot
                let rest = &text_before[..dot_pos + 1];
                if rest.ends_with('.') {
                    let table_name = potential_table;
                    // Now get the column part from after the cursor
                    let column_part: String = text_after
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();

                    if !column_part.is_empty() {
                        // This is a table.column reference
                        if let Some(column_info) = self.get_column_info(table_name, &column_part) {
                            let is_pk = column_info.is_primary_key;
                            return Some(SchemaSymbolInfo::column(
                                column_part,
                                table_name.to_string(),
                                &column_info,
                                is_pk,
                            ));
                        }
                    }
                }
            }
        }

        // Fall back to simple word finding
        let word = extract_word_at_offset(text, offset)?;
        let word = word.trim();

        if word.is_empty() || !is_valid_identifier(word) {
            return None;
        }

        // Check if it's a table name (case-insensitive)
        if schema.tables.iter().any(|t| t.eq_ignore_ascii_case(word)) {
            if let Some(details) = self.get_table_info(word) {
                return Some(SchemaSymbolInfo::table(word.to_string(), details));
            }
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
fn is_valid_identifier(s: &str) -> bool {
    let Some(first) = s.chars().next() else {
        return false;
    };
    if !first.is_alphabetic() && first != '_' {
        return false;
    }
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Extract the word at the given offset
fn extract_word_at_offset(text: &str, offset: usize) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    if offset >= chars.len() {
        return None;
    }

    // Find start of word
    let mut start = offset;
    while start > 0 && is_word_char(chars[start - 1]) {
        start -= 1;
    }

    // Find end of word
    let mut end = offset;
    while end < chars.len() && is_word_char(chars[end]) {
        end += 1;
    }

    if start == end {
        return None;
    }

    Some(chars[start..end].iter().collect())
}

/// Check if a character can be part of a word
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{ForeignKeyInfo, IndexInfo};
    use zqlz_services::TableDetails;

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
            tables: vec!["users".to_string(), "orders".to_string()],
            views: vec!["user_stats".to_string()],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec!["get_user".to_string()],
            procedures: vec![],
            table_indexes: HashMap::new(),
            database_name: Some("testdb".to_string()),
            schema_name: Some("public".to_string()),
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
            tables: vec!["users".to_string(), "orders".to_string()],
            views: vec![],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec![],
            procedures: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
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
            tables: vec![],
            views: vec!["active_users".to_string()],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec![],
            procedures: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
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
            tables: vec![],
            views: vec![],
            materialized_views: vec![],
            triggers: vec![],
            functions: vec!["calculate_total".to_string()],
            procedures: vec![],
            table_indexes: HashMap::new(),
            database_name: None,
            schema_name: None,
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
    fn test_schema_details_for_column() {
        let column = ServicesColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: Some("nextval('users_id_seq')".to_string()),
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
            columns: vec![
                ServicesColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                },
                ServicesColumnInfo {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
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
            primary_key_columns: vec!["id".to_string()],
            row_count: Some(100),
        };

        let details = SchemaSymbolDetails::for_table(&table_details);

        assert_eq!(details.columns.as_ref().map(|c| c.len()), Some(2));
        assert_eq!(details.indexes.as_ref().map(|i| i.len()), Some(1));
        assert_eq!(details.row_count, Some(100));
    }
}
