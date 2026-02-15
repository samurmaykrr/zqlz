//! Trigger Designer for ZQLZ
//!
//! A standalone GPUI panel for designing and modifying database triggers.
//!
//! ## Features
//!
//! - Visual trigger configuration (name, table, timing, event)
//! - Trigger body editor with syntax highlighting
//! - Support for SQLite, PostgreSQL, and MySQL dialects
//! - DDL preview and generation
//!
//! ## Usage
//!
//! ```rust,ignore
//! use zqlz_trigger_designer::{TriggerDesignerPanel, TriggerDesign, DatabaseDialect};
//!
//! // Create a new trigger designer for a new trigger
//! let panel = cx.new(|cx| TriggerDesignerPanel::new(connection_id, dialect, tables, window, cx));
//!
//! // Or load an existing trigger for editing
//! let design = TriggerDesign::from_sql(sql, dialect);
//! let panel = cx.new(|cx| TriggerDesignerPanel::edit(connection_id, design, tables, window, cx));
//! ```

pub mod events;
pub mod models;

mod panel;

// Re-exports for convenience
pub use events::TriggerDesignerEvent;
pub use models::{DatabaseDialect, TriggerDesign, TriggerEvent, TriggerTiming, ValidationError};
pub use panel::TriggerDesignerPanel;
