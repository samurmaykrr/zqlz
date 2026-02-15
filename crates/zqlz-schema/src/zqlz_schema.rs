//! ZQLZ Schema - Schema introspection, DDL generation, and UI widgets
//!
//! This crate provides:
//! - Schema caching (basic and lazy loading)
//! - DDL generation
//! - Schema comparison utilities
//! - Dependency analysis between database objects
//! - Statistics collection for tables and indexes
//! - GPUI panels for schema browsing

mod cache;
mod ddl;
pub mod dependencies;
pub mod lazy;
pub mod statistics;
pub mod widgets;

pub use cache::SchemaCache;
pub use ddl::DdlGenerator;

// Re-export dependencies types for convenience
pub use dependencies::{
    AnalyzerConfig, Dependencies, DependencyAnalyzer, DependencyGraph, ObjectRef,
    extract_table_references,
};

// Re-export lazy loading types for convenience
pub use lazy::{
    CacheEntry, LazyCacheConfig, LazyCacheStats, LazySchemaCache, SharedLazySchemaCache,
    new_shared_cache, new_shared_cache_with_config,
};

// Re-export statistics types for convenience
pub use statistics::{
    CollectorConfig, IndexStatistics, SchemaStatistics, StatisticsCollector, StatisticsConnection,
    StatisticsQuery, TableStatistics,
};

// Re-export widget types for convenience
pub use widgets::{
    ColumnInfo, DatabaseSchemaData, ForeignKeyInfo, IndexInfo, ObjectsPanel,
    ObjectsPanelEvent, SchemaDetails, SchemaDetailsPanel, SchemaDetailsPanelEvent, SchemaNode,
    SchemaNodeType, SchemaTreeEvent, SchemaTreePanel,
};

// Re-export core schema types that users will cache
pub use zqlz_core::{
    ColumnInfo as CoreColumnInfo, IndexInfo as CoreIndexInfo, TableDetails, TableInfo,
};
