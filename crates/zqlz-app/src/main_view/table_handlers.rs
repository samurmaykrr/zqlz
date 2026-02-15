//! Table viewer and database operation handlers for MainView.
//!
//! This module organizes table-related functionality into logical submodules:
//!
//! - **events**: Event handlers for UI components (cell editor, inspector, key-value editor, viewer lifecycle)
//! - **table_ops**: Table operations (create, delete, design, duplicate, empty, import/export, open, rename)
//! - **redis_ops**: Redis-specific operations (keys loading, database/key viewing, deletion)
//! - **standalone_events**: Standalone event handlers called from viewer event subscriptions
//!
//! All functionality is implemented as methods on `MainView` and re-exported through this module.

// Re-export event handler modules
mod events;
mod redis_ops;
mod standalone_events;
mod table_ops;

// Import standalone event handlers for use within this module's event subscriptions
pub(self) use standalone_events::*;
