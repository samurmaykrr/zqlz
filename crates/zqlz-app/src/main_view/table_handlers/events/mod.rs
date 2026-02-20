//! Event handler modules for table viewer operations.
//!
//! This module contains event handlers for various UI components:
//! - Cell editing events
//! - Key-value editor events for Redis
//! - Table viewer lifecycle events (opening viewers, refreshing objects/schema)

mod cell_editor;
mod key_value_editor;
mod open_viewer;
mod refresh_objects;
mod refresh_schema;

// open_viewer, refresh_objects, and refresh_schema define impl MainView methods
// that are accessible throughout the main_view module via pub(in crate::main_view)
