//! Cross-database schema sync orchestrator
//!
//! This module provides functionality to synchronize schemas between different
//! database systems by combining schema comparison, type mapping, and migration
//! generation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zqlz_core::{ColumnInfo, TableDetails, TableInfo, ViewInfo};

use crate::compare::{SchemaComparator, SchemaDiff};
use crate::migration::{Migration, MigrationConfig, MigrationDialect, MigrationGenerator};

use super::{Dialect, TypeMapper, TypeMapperError};

/// Errors that can occur during cross-database sync
#[derive(Debug, Error)]
pub enum SyncError {
    /// Type mapping failed
    #[error("Type mapping error: {0}")]
    TypeMapping(#[from] TypeMapperError),
    /// Migration generation failed
    #[error("Migration error: {0}")]
    Migration(#[from] crate::migration::MigrationError),
    /// Schema comparison failed
    #[error("Schema comparison error: {0}")]
    Comparison(String),
    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Result type for sync operations
pub type SyncResult<T> = Result<T, SyncError>;

/// Configuration for cross-database synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Source database dialect
    pub source_dialect: Dialect,
    /// Target database dialect
    pub target_dialect: Dialect,
    /// Whether to perform a dry run (generate SQL but don't execute)
    pub dry_run: bool,
    /// Whether to include IF EXISTS/IF NOT EXISTS clauses
    pub use_if_exists: bool,
    /// Whether to sync table structure
    pub sync_tables: bool,
    /// Whether to sync views
    pub sync_views: bool,
    /// Whether to sync indexes
    pub sync_indexes: bool,
    /// Whether to sync foreign keys
    pub sync_foreign_keys: bool,
    /// Whether to sync sequences
    pub sync_sequences: bool,
    /// Whether to sync custom types
    pub sync_types: bool,
    /// Tables to exclude from sync (patterns like "temp_*")
    pub exclude_tables: Vec<String>,
    /// Schemas to include (empty means all)
    pub include_schemas: Vec<String>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            source_dialect: Dialect::PostgreSQL,
            target_dialect: Dialect::PostgreSQL,
            dry_run: true,
            use_if_exists: true,
            sync_tables: true,
            sync_views: true,
            sync_indexes: true,
            sync_foreign_keys: true,
            sync_sequences: true,
            sync_types: true,
            exclude_tables: Vec::new(),
            include_schemas: Vec::new(),
        }
    }
}

impl SyncConfig {
    /// Creates a new config with default settings
    pub fn new(source_dialect: Dialect, target_dialect: Dialect) -> Self {
        Self {
            source_dialect,
            target_dialect,
            ..Default::default()
        }
    }

    /// Sets dry run mode
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Adds a table pattern to exclude
    pub fn exclude_table(mut self, pattern: impl Into<String>) -> Self {
        self.exclude_tables.push(pattern.into());
        self
    }

    /// Adds a schema to include
    pub fn include_schema(mut self, schema: impl Into<String>) -> Self {
        self.include_schemas.push(schema.into());
        self
    }

    /// Enables or disables table sync
    pub fn with_tables(mut self, sync: bool) -> Self {
        self.sync_tables = sync;
        self
    }

    /// Enables or disables view sync
    pub fn with_views(mut self, sync: bool) -> Self {
        self.sync_views = sync;
        self
    }

    /// Enables or disables index sync
    pub fn with_indexes(mut self, sync: bool) -> Self {
        self.sync_indexes = sync;
        self
    }

    /// Enables or disables foreign key sync
    pub fn with_foreign_keys(mut self, sync: bool) -> Self {
        self.sync_foreign_keys = sync;
        self
    }
}

/// Result of a sync operation
#[derive(Debug, Clone)]
pub struct SyncPlan {
    /// The schema diff that was calculated
    pub diff: SchemaDiff,
    /// The generated migration
    pub migration: Migration,
    /// Summary statistics
    pub stats: SyncStats,
}

impl SyncPlan {
    /// Returns true if there are no changes to sync
    pub fn is_empty(&self) -> bool {
        self.diff.is_empty()
    }

    /// Returns the up SQL script
    pub fn up_script(&self) -> String {
        self.migration.up_script()
    }

    /// Returns the down SQL script
    pub fn down_script(&self) -> String {
        self.migration.down_script()
    }
}

/// Statistics about the sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Number of tables to add
    pub tables_added: usize,
    /// Number of tables to remove
    pub tables_removed: usize,
    /// Number of tables to modify
    pub tables_modified: usize,
    /// Number of columns to add
    pub columns_added: usize,
    /// Number of columns to remove
    pub columns_removed: usize,
    /// Number of columns to modify
    pub columns_modified: usize,
    /// Number of indexes to add
    pub indexes_added: usize,
    /// Number of indexes to remove
    pub indexes_removed: usize,
    /// Number of types mapped
    pub types_mapped: usize,
}

impl SyncStats {
    /// Returns total number of changes
    pub fn total_changes(&self) -> usize {
        self.tables_added
            + self.tables_removed
            + self.tables_modified
            + self.columns_added
            + self.columns_removed
            + self.columns_modified
            + self.indexes_added
            + self.indexes_removed
    }

    /// Returns true if there are any breaking changes
    pub fn has_breaking_changes(&self) -> bool {
        self.tables_removed > 0 || self.columns_removed > 0
    }
}

/// Cross-database schema synchronizer
///
/// Orchestrates schema synchronization between different database systems
/// by combining schema comparison, type mapping, and migration generation.
#[derive(Debug)]
pub struct CrossDatabaseSync {
    config: SyncConfig,
    type_mapper: TypeMapper,
}

impl Default for CrossDatabaseSync {
    fn default() -> Self {
        Self::new(SyncConfig::default())
    }
}

impl CrossDatabaseSync {
    /// Creates a new sync instance with the given configuration
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            type_mapper: TypeMapper::new(),
        }
    }

    /// Creates a sync instance for syncing from source to target dialect
    pub fn from_to(source: Dialect, target: Dialect) -> Self {
        Self::new(SyncConfig::new(source, target))
    }

    /// Returns the current configuration
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Returns a mutable reference to the type mapper for customization
    pub fn type_mapper_mut(&mut self) -> &mut TypeMapper {
        &mut self.type_mapper
    }

    /// Converts table details from source dialect to target dialect
    ///
    /// This transforms all column data types to their equivalent
    /// types in the target database system.
    pub fn convert_table_details(&self, source: &TableDetails) -> SyncResult<TableDetails> {
        let mut converted = source.clone();
        for column in &mut converted.columns {
            let mapped_type = self.type_mapper.map_type(
                &column.data_type,
                self.config.source_dialect,
                self.config.target_dialect,
            )?;
            column.data_type = mapped_type;
        }
        Ok(converted)
    }

    /// Converts a map of table details from source dialect to target dialect
    pub fn convert_all_tables(
        &self,
        source_details: &HashMap<String, TableDetails>,
    ) -> SyncResult<HashMap<String, TableDetails>> {
        let mut result = HashMap::with_capacity(source_details.len());
        for (name, details) in source_details {
            result.insert(name.clone(), self.convert_table_details(details)?);
        }
        Ok(result)
    }

    /// Generates a sync plan between source and target schemas
    ///
    /// This compares the schemas and generates migration SQL to make
    /// the target schema match the source schema (after type conversion).
    pub fn plan_sync(
        &self,
        source_tables: &[TableInfo],
        target_tables: &[TableInfo],
        source_details: &HashMap<String, TableDetails>,
        target_details: &HashMap<String, TableDetails>,
    ) -> SyncResult<SyncPlan> {
        // Convert source types to target dialect
        let converted_details = self.convert_all_tables(source_details)?;

        // Compare schemas
        let comparator = SchemaComparator::new();
        let mut diff = comparator.compare_tables(
            source_tables,
            target_tables,
            &converted_details,
            target_details,
        );

        // Filter diff based on config
        self.filter_diff(&mut diff);

        // Calculate stats
        let stats = self.calculate_stats(&diff);

        // Generate migration
        let migration_config = self.create_migration_config();
        let generator = MigrationGenerator::with_config(migration_config);
        let migration = generator.generate(&diff)?;

        Ok(SyncPlan {
            diff,
            migration,
            stats,
        })
    }

    /// Generates a sync plan for views
    pub fn plan_view_sync(
        &self,
        source_views: &[ViewInfo],
        target_views: &[ViewInfo],
    ) -> SyncResult<SyncPlan> {
        let comparator = SchemaComparator::new();
        let mut diff = comparator.compare_views(source_views, target_views);

        // Filter based on config
        if !self.config.sync_views {
            diff.added_views.clear();
            diff.removed_views.clear();
            diff.modified_views.clear();
        }

        // Filter by included schemas
        if !self.config.include_schemas.is_empty() {
            diff.added_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
            diff.removed_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
            diff.modified_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
        }

        let stats = SyncStats::default();
        let migration_config = self.create_migration_config();
        let generator = MigrationGenerator::with_config(migration_config);
        let migration = generator.generate(&diff)?;

        Ok(SyncPlan {
            diff,
            migration,
            stats,
        })
    }

    /// Filters the diff based on sync configuration
    fn filter_diff(&self, diff: &mut SchemaDiff) {
        if !self.config.sync_tables {
            diff.added_tables.clear();
            diff.removed_tables.clear();
            diff.modified_tables.clear();
        }

        if !self.config.sync_views {
            diff.added_views.clear();
            diff.removed_views.clear();
            diff.modified_views.clear();
        }

        if !self.config.sync_sequences {
            diff.added_sequences.clear();
            diff.removed_sequences.clear();
            diff.modified_sequences.clear();
        }

        if !self.config.sync_types {
            diff.added_types.clear();
            diff.removed_types.clear();
            diff.modified_types.clear();
        }

        // Filter out excluded tables
        if !self.config.exclude_tables.is_empty() {
            diff.added_tables
                .retain(|t| !self.is_table_excluded(&t.name));
            diff.removed_tables
                .retain(|t| !self.is_table_excluded(&t.name));
            diff.modified_tables
                .retain(|t| !self.is_table_excluded(&t.table_name));
        }

        // Filter by included schemas
        if !self.config.include_schemas.is_empty() {
            diff.added_tables
                .retain(|t| self.is_schema_included(t.schema.as_deref()));
            diff.removed_tables
                .retain(|t| self.is_schema_included(t.schema.as_deref()));
            diff.modified_tables
                .retain(|t| self.is_schema_included(t.schema.as_deref()));
            diff.added_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
            diff.removed_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
            diff.modified_views
                .retain(|v| self.is_schema_included(v.schema.as_deref()));
        }

        // Filter indexes and foreign keys from modified tables
        if !self.config.sync_indexes {
            for table_diff in &mut diff.modified_tables {
                table_diff.added_indexes.clear();
                table_diff.removed_indexes.clear();
                table_diff.modified_indexes.clear();
            }
        }

        if !self.config.sync_foreign_keys {
            for table_diff in &mut diff.modified_tables {
                table_diff.added_foreign_keys.clear();
                table_diff.removed_foreign_keys.clear();
                table_diff.modified_foreign_keys.clear();
            }
        }
    }

    /// Checks if a table should be excluded based on patterns
    fn is_table_excluded(&self, table_name: &str) -> bool {
        for pattern in &self.config.exclude_tables {
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if table_name.starts_with(prefix) {
                    return true;
                }
            } else if pattern == table_name {
                return true;
            }
        }
        false
    }

    /// Checks if a schema should be included
    fn is_schema_included(&self, schema: Option<&str>) -> bool {
        if self.config.include_schemas.is_empty() {
            return true;
        }
        match schema {
            Some(s) => self.config.include_schemas.iter().any(|inc| inc == s),
            None => true,
        }
    }

    /// Calculates statistics from the diff
    fn calculate_stats(&self, diff: &SchemaDiff) -> SyncStats {
        let mut stats = SyncStats::default();

        stats.tables_added = diff.added_tables.len();
        stats.tables_removed = diff.removed_tables.len();
        stats.tables_modified = diff.modified_tables.len();

        for table_diff in &diff.modified_tables {
            stats.columns_added += table_diff.added_columns.len();
            stats.columns_removed += table_diff.removed_columns.len();
            stats.columns_modified += table_diff.modified_columns.len();
            stats.indexes_added += table_diff.added_indexes.len();
            stats.indexes_removed += table_diff.removed_indexes.len();
        }

        stats
    }

    /// Creates migration config from sync config
    fn create_migration_config(&self) -> MigrationConfig {
        let dialect = match self.config.target_dialect {
            Dialect::PostgreSQL => MigrationDialect::PostgreSQL,
            Dialect::MySQL => MigrationDialect::MySQL,
            Dialect::SQLite => MigrationDialect::SQLite,
            Dialect::MsSql => MigrationDialect::MsSql,
        };

        MigrationConfig::for_dialect(dialect).with_if_exists(self.config.use_if_exists)
    }

    /// Maps a single type from source to target dialect
    pub fn map_type(&self, source_type: &str) -> SyncResult<String> {
        Ok(self.type_mapper.map_type(
            source_type,
            self.config.source_dialect,
            self.config.target_dialect,
        )?)
    }

    /// Maps multiple column types and returns the converted columns
    pub fn map_columns(&self, columns: &[ColumnInfo]) -> SyncResult<Vec<ColumnInfo>> {
        let mut result = Vec::with_capacity(columns.len());
        for col in columns {
            let mut converted = col.clone();
            converted.data_type = self.map_type(&col.data_type)?;
            result.push(converted);
        }
        Ok(result)
    }
}

/// Convenience function to map columns from one dialect to another
pub fn map_columns(
    columns: &[ColumnInfo],
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> SyncResult<Vec<ColumnInfo>> {
    let syncer = CrossDatabaseSync::from_to(source_dialect, target_dialect);
    syncer.map_columns(columns)
}

/// Convenience function to map a single type
pub fn map_type_between(
    source_type: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> SyncResult<String> {
    let syncer = CrossDatabaseSync::from_to(source_dialect, target_dialect);
    syncer.map_type(source_type)
}
