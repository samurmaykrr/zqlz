//! Table design service
//!
//! Provides table structure modification operations including:
//! - Creating new tables
//! - Altering existing tables (add/modify/drop columns)
//! - Managing indexes and foreign keys
//! - Generating DDL for preview
//!
//! This service is designed to be database-agnostic with driver-specific
//! options passed through the design models.
//!
//! Note: Table design types and DDL generation have moved to `zqlz-table-designer`.
//! This service wraps that functionality and provides async operations.

use std::sync::Arc;
use zqlz_core::Connection;
use zqlz_table_designer::{DatabaseDialect, DdlGenerator, TableDesign};

use crate::error::{ServiceError, ServiceResult};

/// Service for table design operations
///
/// Handles:
/// - DDL generation for table creation/modification
/// - Executing schema changes
/// - Loading existing table structure for editing
///
/// This service wraps `zqlz_table_designer::DdlGenerator` and provides
/// additional async operations for executing changes.
pub struct TableDesignService;

impl TableDesignService {
    /// Create a new table design service
    pub fn new() -> Self {
        Self
    }

    /// Load an existing table's structure for editing
    ///
    /// Returns a TableDesign that can be modified and saved back.
    /// The dialect parameter should match the database driver being used.
    #[tracing::instrument(skip(self, connection))]
    pub async fn load_table(
        &self,
        connection: Arc<dyn Connection>,
        dialect: DatabaseDialect,
        schema: Option<&str>,
        table_name: &str,
    ) -> ServiceResult<TableDesign> {
        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let table_details = schema_introspection
            .get_table(schema, table_name)
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        Ok(TableDesign::from_table_details(table_details, dialect))
    }

    /// Create a new empty table design
    pub fn new_table_design(&self, dialect: DatabaseDialect) -> TableDesign {
        TableDesign::empty(dialect)
    }

    /// Generate DDL for creating a new table
    pub fn generate_create_table_ddl(&self, design: &TableDesign) -> ServiceResult<String> {
        DdlGenerator::generate_create_table(design)
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))
    }

    /// Execute the table creation DDL
    #[tracing::instrument(skip(self, connection, design))]
    pub async fn create_table(
        &self,
        connection: Arc<dyn Connection>,
        design: &TableDesign,
    ) -> ServiceResult<()> {
        let ddl = self.generate_create_table_ddl(design)?;

        tracing::debug!("Executing CREATE TABLE DDL:\n{}", ddl);

        // Execute each statement (CREATE TABLE and CREATE INDEX statements)
        for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
            let sql = format!("{};", statement.trim());
            connection.execute(&sql, &[]).await.map_err(|e| {
                ServiceError::TableOperationFailed(format!("Failed to execute DDL: {}", e))
            })?;
        }

        tracing::info!(table_name = %design.table_name, "Table created successfully");
        Ok(())
    }

    /// Generate DDL for dropping a table
    pub fn generate_drop_table_ddl(&self, table_name: &str) -> String {
        DdlGenerator::generate_drop_table(table_name)
    }

    /// Drop a table
    #[tracing::instrument(skip(self, connection))]
    pub async fn drop_table(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
    ) -> ServiceResult<()> {
        let ddl = self.generate_drop_table_ddl(table_name);

        connection.execute(&ddl, &[]).await.map_err(|e| {
            ServiceError::TableOperationFailed(format!("Failed to drop table: {}", e))
        })?;

        tracing::info!(table_name = %table_name, "Table dropped successfully");
        Ok(())
    }

    /// Generate ALTER TABLE DDL for modifying an existing table
    /// Note: SQLite has limited ALTER TABLE support
    pub fn generate_alter_table_ddl(
        &self,
        original: &TableDesign,
        modified: &TableDesign,
    ) -> ServiceResult<Vec<String>> {
        DdlGenerator::generate_alter_table(original, modified)
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))
    }

    /// Apply alterations to an existing table
    #[tracing::instrument(skip(self, connection, original, modified))]
    pub async fn alter_table(
        &self,
        connection: Arc<dyn Connection>,
        original: &TableDesign,
        modified: &TableDesign,
    ) -> ServiceResult<()> {
        let statements = self.generate_alter_table_ddl(original, modified)?;

        for statement in &statements {
            tracing::debug!("Executing ALTER DDL: {}", statement);
            connection.execute(statement, &[]).await.map_err(|e| {
                ServiceError::TableOperationFailed(format!("Failed to alter table: {}", e))
            })?;
        }

        tracing::info!(
            table_name = %modified.table_name,
            statement_count = statements.len(),
            "Table altered successfully"
        );
        Ok(())
    }

    /// Get available data types for a dialect
    ///
    /// Uses driver-provided dialect information for consistent type metadata.
    pub fn get_data_types(
        &self,
        dialect: &DatabaseDialect,
    ) -> Vec<zqlz_table_designer::DataTypeInfo> {
        zqlz_table_designer::get_data_types(dialect)
    }
}

impl Default for TableDesignService {
    fn default() -> Self {
        Self::new()
    }
}
