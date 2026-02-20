//! Table viewer module
//!
//! Displays table data in an editable grid view with inline editing and context menus.
//!
//! ## Module Structure
//!
//! - `events` - Event types emitted by the table viewer
//! - `delegate` - TableDelegate implementation for the Table widget
//! - `panel` - Main TableViewerPanel component
//! - `filter_types` - Filter, sort, and profile data structures
//! - `filter_panel` - Filter and sort UI component
//! - `column_visibility` - Column show/hide UI component
//! - `sorting` - Multi-column sorting with null handling
//! - `filter_presets` - SQLite-based filter preset persistence
//! - `operations` - Row operations (duplicate, etc.)

mod column_visibility;
pub(crate) mod delegate;
mod events;
mod filter_panel;
mod filter_presets;
mod filter_types;
mod operations;
mod panel;
mod sorting;

pub use delegate::PendingCellChange;
pub use events::TableViewerEvent;
pub use filter_types::{
    FilterCondition,
    SortCriterion, SortDirection,
};
pub use panel::{CopySelection, PasteClipboard, TableViewerPanel, ToggleSearch};
