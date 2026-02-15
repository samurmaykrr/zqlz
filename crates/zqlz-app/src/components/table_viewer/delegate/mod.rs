//! Split delegate modules for TableViewerDelegate
//
// This module contains the `TableViewerDelegate` struct (fields only)
// and declares submodules that implement functionality split out of
// the original large `delegate.rs` file.

use std::collections::{HashMap, HashSet};

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{ColumnMeta, DriverCategory, ForeignKeyInfo, QueryResult};
use zqlz_ui::widgets::{
    checkbox::Checkbox,
    date_picker::{DatePickerInline, DatePickerMode, DatePickerPopover, DatePickerState},
    input::{ArrowDirection, Input, InputEvent, InputState},
    menu::PopupMenu,
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    table::{Column, ColumnFixed, ColumnSort, TableDelegate, TableState},
    ActiveTheme, Icon, IconName, Sizable, Size,
};

use super::events::TableViewerEvent;
use super::panel::TableViewerPanel;

pub mod types;
mod init;
mod inline_edit;
mod bulk_edit;
mod clipboard;
mod render;
mod column_types;
mod fk;
mod columns;
mod pending;
mod filtering;
mod sort;
mod context_menu;
mod trait_impl;

pub use types::*;
pub use types::PendingCellChange; // ensure public re-export

/// Table viewer delegate - implements the TableDelegate trait
///
/// Fields are `pub(super)` so submodules under `delegate/` can access them.
pub struct TableViewerDelegate {
    /// Column definitions (includes row number column at index 0)
    pub(super) columns: Vec<Column>,

    /// Column metadata from the database (excludes row number column)
    pub(crate) column_meta: Vec<ColumnMeta>,

    /// Table data as strings (easier to display and edit)
    pub(crate) rows: Vec<Vec<String>>,

    /// UI size (small/medium/large - affects padding/fonts)
    pub(super) size: Size,

    /// Name of the table being displayed
    pub(crate) table_name: String,

    /// Connection ID (needed for edit operations)
    pub(super) connection_id: Uuid,

    /// Driver category (Relational, KeyValue, etc.) — determines sort/filter behavior
    pub(super) driver_category: DriverCategory,

    /// Weak reference back to the panel (to emit events)
    pub(super) viewer_panel: WeakEntity<TableViewerPanel>,

    /// Currently editing cell (row, col) - for inline editing
    pub(super) editing_cell: Option<(usize, usize)>,

    /// Input state for the cell being edited
    pub(super) cell_input: Option<Entity<InputState>>,

    /// DatePicker state for editing date/time columns
    pub(super) date_picker_state: Option<Entity<DatePickerState>>,

    /// Select state for editing enum columns
    pub(super) enum_select_state: Option<Entity<SelectState<Vec<String>>>>,

    /// Cells to update when bulk editing (multiple cells selected)
    /// When Some, stop_editing will apply the value to all these cells
    pub(super) bulk_edit_cells: Option<Vec<zqlz_ui::widgets::table::CellPosition>>,

    /// Whether the cell being edited originally contained newlines
    /// (used to show visual indicator that content was flattened)
    pub(super) editing_cell_has_newlines: bool,

    /// Flag to ignore the first blur event after starting edit
    /// This prevents immediate commit when focus transfers to the input
    pub(super) ignore_next_blur: bool,

    /// Selected row indices (for delete operations)
    pub(super) selected_rows: HashSet<usize>,

    /// Context menu selected rows (from cell selection, set when context menu opens)
    /// Used by delete menu item to know which rows to delete
    pub(super) context_menu_selected_rows: Vec<usize>,

    /// Search filter text (lowercase for case-insensitive matching)
    pub(super) search_filter: Option<String>,

    /// Indices of rows that match the search filter
    pub(crate) filtered_row_indices: Vec<usize>,

    /// Whether filtering is active
    pub(crate) is_filtering: bool,

    /// Pending changes (not yet committed to database)
    /// This enables batch editing workflow
    pub(crate) pending_changes: PendingChanges,

    /// Whether inline editing is disabled (e.g., for Redis key viewers)
    /// When disabled, clicks emit EditCell events instead of starting inline edit
    pub(crate) disable_inline_edit: bool,

    /// Auto-commit mode: when true, edits emit SaveCell immediately
    /// When false, edits are stored in pending_changes
    pub(crate) auto_commit_mode: bool,

    /// Row offset for pagination (to show global row numbers like 101, 102, etc.)
    /// When pagination is enabled, this is (current_page - 1) * records_per_page
    pub(super) row_offset: usize,

    /// Infinite scroll mode enabled
    pub(super) infinite_scroll_enabled: bool,

    /// Whether more data is available for infinite scroll
    pub(super) has_more_data: bool,

    /// Prevent duplicate load_more requests (pub for error handling)
    pub(crate) is_loading_more: bool,

    /// Foreign key mapping: column index -> FK info
    /// Used to detect FK columns and show dropdown with referenced values
    pub(super) fk_by_column: HashMap<usize, ForeignKeyInfo>,

    /// Cached values for FK dropdown (referenced_table -> list of FkSelectItem)
    /// Populated lazily when user starts editing a FK column
    pub(super) fk_values_cache: HashMap<String, Vec<FkSelectItem>>,

    /// Select state for editing foreign key columns (searchable)
    pub(super) fk_select_state: Option<Entity<SelectState<SearchableVec<FkSelectItem>>>>,

    /// Whether FK values are currently being loaded
    pub(super) fk_loading: bool,

    /// Raw binary data for blob/binary cells, keyed by (row_index, data_col_index).
    /// Only populated for Value::Bytes columns to avoid holding all data as strings.
    pub(crate) raw_bytes: HashMap<(usize, usize), Vec<u8>>,
}

// Re-export submodules' public items where appropriate
pub use init::*;
pub use inline_edit::*;
pub use bulk_edit::*;
pub use clipboard::*;
pub use render::*;
pub use column_types::*;
pub use fk::*;
pub use columns::*;
pub use pending::*;
pub use filtering::*;
pub use sort::*;
pub use context_menu::*;
pub use trait_impl::*;
