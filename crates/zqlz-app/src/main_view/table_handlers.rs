//! Table viewer and database operation handlers for MainView.
//!
//! This module organizes table-related functionality into logical submodules:
//!
//! - **events**: Event handlers for UI components (cell editor, inspector, key-value editor, viewer lifecycle)
//! - **table_ops**: Table operations (create, delete, design, duplicate, empty, import/export, open, rename)
//! - **redis_ops**: Redis-specific operations (keys loading, database/key viewing, deletion)
//! - **document_ops**: Document-store operations (collection viewing)
//! - **standalone_events**: Standalone event handlers called from viewer event subscriptions
//!
//! All functionality is implemented as methods on `MainView` and re-exported through this module.

// Re-export event handler modules
mod document_ops;
mod events;
mod redis_ops;
mod standalone_events;
pub(in crate::main_view) mod table_ops;

pub(in crate::main_view) use events::TableViewerSessionOpenRequest;

// Import standalone event handlers for use within this module's event subscriptions
use standalone_events::*;
