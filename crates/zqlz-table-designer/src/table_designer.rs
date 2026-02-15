//! Table Designer for ZQLZ
//!
//! A standalone GPUI panel for designing and modifying database tables.
//!
//! ## Features
//!
//! - Visual column editor with type selection
//! - Index management
//! - Foreign key constraint editor
//! - Dialect-specific options (SQLite, PostgreSQL, MySQL)
//! - DDL preview and generation
//!
//! ## Usage
//!
//! ```rust,ignore
//! use zqlz_table_designer::{TableDesignerPanel, TableDesign, DatabaseDialect};
//!
//! // Create a new table designer for a new table
//! let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, DatabaseDialect::Sqlite, window, cx));
//!
//! // Or load an existing table for editing
//! let design = TableDesign::from_table_details(table_details, dialect);
//! let panel = cx.new(|cx| TableDesignerPanel::edit(connection_id, design, window, cx));
//! ```

pub mod events;
pub mod models;
pub mod service;

mod panel;

// Re-exports for convenience
pub use events::TableDesignerEvent;
pub use models::{
    ColumnDesign, DataTypeCategory, DataTypeInfo, DatabaseDialect, ForeignKeyDesign, IndexDesign,
    TableDesign, TableOptions, ValidationError, get_data_types,
};
pub use panel::{DesignerTab, TableDesignerPanel};
pub use service::{DdlGenerator, TableLoader, fk_action_to_sql};
