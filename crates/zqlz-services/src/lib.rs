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
//! Infrastructure Layer (zqlz-core, zqlz-drivers)
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
mod error;
mod schema_service;
mod table_design_service;
mod table_service;
mod view_models;

pub use connection_service::{ConnectionInfo, ConnectionService, TestResult};
pub use error::{ServiceError, ServiceResult};
pub use schema_service::SchemaService;
pub use table_service::{CellUpdateData, RowDeleteData, RowInsertData, TableService};
pub use view_models::{ColumnInfo, DatabaseSchema, TableDetails};

// Re-export table design types from zqlz-table-designer for backward compatibility
pub use zqlz_table_designer::{
    ColumnDesign, DataTypeCategory, DataTypeInfo, DatabaseDialect, DdlGenerator, ForeignKeyDesign,
    IndexDesign, TableDesign, TableDesignerEvent, TableDesignerPanel, TableOptions,
    ValidationError,
};

// Keep TableDesignService as a wrapper that uses DdlGenerator
pub use table_design_service::TableDesignService;
