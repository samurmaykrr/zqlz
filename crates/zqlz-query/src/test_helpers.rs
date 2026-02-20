//! GPUI Test Scaffolding for zqlz-query crate
//!
//! This module provides test utilities for GPUI-based testing of editor components,
//! LSP flows, and schema metadata overlays.
//!
//! Usage:
//! ```ignore
//! #[gpui::test]
//! fn test_schema_overlay(cx: &mut gpui::TestAppContext) {
//!     let schema = test_schema();
//!     let metadata = SchemaMetadata::new(schema);
//!     // ... test logic
//! }
//! ```

use zqlz_services::DatabaseSchema;

use std::collections::HashMap;

/// Creates a test database schema with common tables for testing.
///
/// This is a simple schema using only the `tables` field for symbol lookup.
/// The `table_infos` field is empty - use `test_schema_with_details()`
/// when you need column details.
///
/// # Example
/// ```
/// let schema = test_schema();
/// let metadata = SchemaMetadata::new(schema);
/// let result = metadata.find_symbol_at_offset("SELECT * FROM users", 14);
/// assert!(result.is_some());
/// ```
pub fn test_schema() -> DatabaseSchema {
    DatabaseSchema {
        table_infos: vec![],
        objects_panel_data: None,
        tables: vec![
            "users".to_string(),
            "orders".to_string(),
            "products".to_string(),
        ],
        views: vec!["active_users".to_string()],
        materialized_views: vec![],
        triggers: vec!["update_orders".to_string()],
        functions: vec!["calculate_total".to_string(), "get_user_name".to_string()],
        procedures: vec!["process_order".to_string()],
        table_indexes: HashMap::new(),
        database_name: Some("testdb".to_string()),
        schema_name: Some("public".to_string()),
    }
}

/// Creates a test database schema with complex queries for testing SQL parsing.
pub fn test_schema_complex() -> DatabaseSchema {
    DatabaseSchema {
        table_infos: vec![],
        objects_panel_data: None,
        tables: vec![
            "users".to_string(),
            "orders".to_string(),
            "products".to_string(),
            "order_items".to_string(),
            "categories".to_string(),
        ],
        views: vec!["active_users".to_string(), "order_summaries".to_string()],
        materialized_views: vec!["monthly_sales".to_string()],
        triggers: vec!["update_orders".to_string(), "log_user_login".to_string()],
        functions: vec![
            "calculate_total".to_string(),
            "get_user_name".to_string(),
            "format_currency".to_string(),
            "parse_json".to_string(),
        ],
        procedures: vec![
            "process_order".to_string(),
            "archive_old_records".to_string(),
        ],
        table_indexes: HashMap::new(),
        database_name: Some("testdb".to_string()),
        schema_name: Some("public".to_string()),
    }
}

/// Helper to create a minimal schema for quick testing.
pub fn test_schema_minimal() -> DatabaseSchema {
    DatabaseSchema {
        table_infos: vec![],
        objects_panel_data: None,
        tables: vec!["test".to_string()],
        views: vec![],
        materialized_views: vec![],
        triggers: vec![],
        functions: vec![],
        procedures: vec![],
        table_indexes: HashMap::new(),
        database_name: None,
        schema_name: None,
    }
}

/// Helper to create an empty schema.
pub fn test_schema_empty() -> DatabaseSchema {
    DatabaseSchema {
        table_infos: vec![],
        objects_panel_data: None,
        tables: vec![],
        views: vec![],
        materialized_views: vec![],
        triggers: vec![],
        functions: vec![],
        procedures: vec![],
        table_indexes: HashMap::new(),
        database_name: None,
        schema_name: None,
    }
}
