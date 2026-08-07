//! ZQLZ Services Layer
//!
//! This crate provides the service layer that sits between the UI and domain logic.
//! Services orchestrate business operations and provide a clean API for the UI layer.
//!
//! # Architecture
//!
//! ```text
//! UI Layer (zqlz-app)
//!     ↓
//! Service Layer (zqlz-services) ← This crate
//!     ↓
//! Domain Layer (zqlz-query, zqlz-schema, zqlz-connection, zqlz-table-designer)
//!     ↓
//! Infrastructure Layer (zqlz-core abstractions, driver crates behind connection)
//! ```
//!
//! # Services
//!
//! - [`SchemaService`] - Schema operations with caching
//! - [`TableService`] - Table browsing and cell editing
//! - [`TableDesignService`] - Table structure design and DDL generation
//! - [`ConnectionService`] - Connection lifecycle management
//!
//! Note: `QueryService` has been moved to `zqlz-query` crate.
//! Note: `TableDesignerPanel` and table design models are in `zqlz-table-designer` crate.
//!
//! # Design Principles
//!
//! 1. **No UI dependencies** - Services never import GPUI or UI types
//! 2. **Return ViewModels** - Services return DTOs, not domain objects
//! 3. **Centralize logic** - Business logic lives here, not in UI handlers
//! 4. **Use domain abstractions** - Services use SchemaCache, etc.

mod connection_service;
mod document_service;
mod error;
mod key_value_service;
mod object_action_service;
mod refresh_service;
mod schema_service;
mod table_design_service;
mod table_service;
mod view_models;

pub use connection_service::{
    ConnectionDatabaseEntry, ConnectionInfo, ConnectionService, DatabaseScopedConnection,
    DiscoverDatabasesSidebarOutcome, LazySidebarSectionLoadOutcome, PaletteSchemaCommandsData,
    RedisDatabaseEntry, RelationalSidebarBootstrap, ResolvedConnection, SidebarSectionLoadOutcome,
    SidebarSectionLoadResult, TestResult,
};
pub use document_service::DocumentService;
pub use error::{ServiceError, ServiceResult};
pub use key_value_service::{
    KeyValueService, LoadKeyValueDatabaseRowsOutcome, LoadKeyValueDatabaseRowsRequest,
    LoadKeyValueKeysOutcome, LoadKeyValueKeysRequest,
};
pub use object_action_service::{
    classify_objects_panel_action_resolution, manifest_action_coverage_gaps,
    object_type_for_kind_id, objects_panel_action_feature_availability,
    objects_panel_action_issue_message, plan_object_form_action, schema_qualified_action_name,
    selected_object_ref, ActionRegistryError, ObjectFormActionPlan, ObjectsPanelActionRegistry,
    ObjectsPanelActionResolutionTelemetry, ResolvedObjectsPanelAction, SelectedObjectRef,
};
pub use refresh_service::{
    ConnectionRefresh, ConnectionRefreshPayload, DocumentConnectionRefresh,
    KeyValueConnectionRefresh, RefreshIntent, RefreshPlan, RefreshPlanStep, RefreshRequest,
    RefreshService, RelationalConnectionRefresh, SurfaceRefreshKind,
};
pub use schema_service::{SchemaService, SidebarSectionLoadData};
pub use table_service::{
    build_open_viewer_schema_viewer_metadata, build_schema_only_query_result, decide_delete_tables,
    decide_design_tables, decide_duplicate_tables, decide_empty_tables,
    decide_open_tables_workflow, decide_open_viewer_count_workflow,
    should_use_schema_only_table_browse_fallback, BrowseLastPageRequest, BrowseNearEndPageRequest,
    BrowseTableWithFiltersRequest, CommitCellChange, CommitTableChangesOutcome,
    CommitTableChangesRequest, DeleteTablesDecision, DeleteTablesDecisionRequest,
    DeleteTablesOutcome, DeleteTablesRequest, DesignTablesDecision, DesignTablesDecisionRequest,
    DumpTablesSqlOutcome, DumpTablesSqlRequest, DuplicateTableOperation, DuplicateTableResult,
    DuplicateTablesDecision, DuplicateTablesDecisionRequest, DuplicateTablesOutcome,
    DuplicateTablesRequest, EmptyTablesDecision, EmptyTablesDecisionRequest, EmptyTablesOutcome,
    EmptyTablesRequest, FailedModifiedCellCommit, FailedNewRowCommit, ForeignKeyValueOption,
    GenerateTableChangesSqlRequest, LoadDistinctValuesOutcome, LoadDistinctValuesRequest,
    LoadForeignKeyValuesOutcome, LoadForeignKeyValuesRequest, ModifiedCellSqlChange,
    OpenTableViewerCountDecision, OpenTableViewerCountDecisionRequest, OpenTablesDecision,
    OpenTablesDecisionRequest, OpenViewerInitialLoadOutcome, OpenViewerInitialLoadRequest,
    OpenViewerSchemaLoad, OpenViewerSchemaViewerMetadata, RenameTableRequest, TableWorkflowError,
};
pub use table_service::{
    CellUpdateData, CellUpdateOutcome, RowDeleteData, RowInsertData, TableService,
};
pub use view_models::{ColumnInfo, DatabaseSchema, TableColumnSummary, TableDetails};
pub use zqlz_core::ConnectionFeatureSet;
pub use zqlz_core::ConnectionScope;

// Re-export table design types from zqlz-table-designer for backward compatibility
pub use zqlz_table_designer::{
    ColumnDesign, DataTypeCategory, DataTypeInfo, DatabaseDialect, DdlGenerator, ForeignKeyDesign,
    IndexDesign, TableDesign, TableDesignerEvent, TableDesignerPanel, TableOptions,
    ValidationError,
};

// Keep TableDesignService as a wrapper that uses DdlGenerator
pub use table_design_service::TableDesignService;
