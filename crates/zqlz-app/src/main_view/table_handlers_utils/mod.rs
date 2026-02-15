//! Utility modules for table handlers.
//!
//! This module contains helper functions organized by their purpose:
//! - `conversion`: Type conversions and data transformations
//! - `formatting`: Data formatting for display
//! - `redis`: Redis-specific utilities
//! - `validation`: Input validation and parsing

pub(super) mod conversion;
pub(super) mod formatting;
pub(super) mod redis;
pub(super) mod validation;

// Re-export commonly used functions for convenience
pub(super) use conversion::{convert_to_schema_details, driver_name_to_category, resolve_schema_qualifier};
pub(super) use formatting::{escape_redis_value, format_bytes, format_sql_value, format_ttl_seconds};
pub(super) use redis::{fetch_redis_key_value, parse_human_readable_ttl};
pub(super) use validation::{parse_inline_value, validate_table_name};

use std::sync::Arc;

/// Generates DDL for a table if schema introspection is available.
pub(super) async fn generate_ddl_for_table(
    conn: &Arc<dyn zqlz_core::Connection>,
    table_name: &str,
) -> Option<String> {
    if let Some(schema_introspection) = conn.as_schema_introspection() {
        use zqlz_core::{DatabaseObject, ObjectType};
        let db_object = DatabaseObject {
            object_type: ObjectType::Table,
            schema: None,
            name: table_name.to_string(),
        };
        schema_introspection.generate_ddl(&db_object).await.ok()
    } else {
        None
    }
}
