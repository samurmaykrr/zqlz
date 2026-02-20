//! Table viewer events
//!
//! Events emitted by the table viewer panel to communicate with MainView,
//! which coordinates between the table viewer and other panels (like CellEditorPanel).

use std::collections::{HashMap, HashSet};

use uuid::Uuid;
use zqlz_core::{ColumnMeta, DriverCategory};

use super::delegate::PendingCellChange;
use super::filter_types::{FilterCondition, SortCriterion};

/// Events emitted by the table viewer panel
///
/// These events are used to communicate with the MainView, which coordinates
/// between the table viewer and other panels (like CellEditorPanel).
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum TableViewerEvent {
    /// Cell value should be set to NULL (future feature)
    SetToNull { row: usize, col: usize },

    /// Cell value should be set to empty string (future feature)
    SetToEmpty { row: usize, col: usize },

    /// Cell value was saved via inline editing
    ///
    /// Emitted when user finishes inline editing (presses Enter/Tab or clicks away).
    /// MainView handles this by calling TableService.update_cell() to persist to database.
    SaveCell {
        table_name: String,
        connection_id: Uuid,
        row: usize,
        col: usize,
        column_name: String,
        new_value: String,
        /// Original value before editing (for rollback on failure)
        original_value: String,
        /// All values in the row (needed for building WHERE clause)
        all_row_values: Vec<String>,
        /// All column names (needed for primary key identification)
        all_column_names: Vec<String>,
        /// Database column types for type-aware value parsing
        all_column_types: Vec<String>,
    },

    /// Cell should be edited in the advanced Cell Editor panel
    ///
    /// Emitted when user double-clicks a cell. MainView handles this by:
    /// 1. Opening CellEditorPanel in right sidebar
    /// 2. Populating it with current cell data
    /// 3. Waiting for user to save/cancel
    EditCell {
        table_name: String,
        connection_id: Uuid,
        row: usize,
        col: usize,
        column_name: String,
        column_type: String,
        current_value: Option<String>,
        /// All values in the row (needed for building WHERE clause)
        all_row_values: Vec<String>,
        /// All column names (needed for primary key identification)
        all_column_names: Vec<String>,
        /// Database column types for type-aware value parsing
        all_column_types: Vec<String>,
        /// Raw binary data for blob/binary columns
        raw_bytes: Option<Vec<u8>>,
    },

    /// User wants to refresh the table data
    ///
    /// Emitted when user clicks the refresh button in the toolbar.
    /// MainView handles this by reloading the table via TableService.
    RefreshTable {
        connection_id: Uuid,
        table_name: String,
        /// Driver category determines how to refresh (SQL browse vs key listing)
        driver_category: DriverCategory,
        /// Database name for MySQL multi-database context
        database_name: Option<String>,
    },

    /// Add a new empty row to the table
    ///
    /// MainView handles this by creating an empty row in pending changes,
    /// scrolling to it, selecting the first cell, and auto-starting editing.
    /// The row is NOT saved to DB until user commits via CommitChanges (Cmd+S).
    AddRow {
        connection_id: Uuid,
        table_name: String,
        /// All column names in the table
        all_column_names: Vec<String>,
    },

    /// Save a newly created row to the database
    ///
    /// Currently unused — new rows are committed via the CommitChanges event.
    /// Retained for potential future use (e.g., single-row commit action).
    SaveNewRow {
        table_name: String,
        connection_id: Uuid,
        /// Index of the row in the pending_changes.new_rows array
        new_row_index: usize,
        /// Values for all columns in the new row
        row_data: Vec<String>,
        /// All column names (ordered to match row_data)
        column_names: Vec<String>,
    },

    /// Add a new Redis key
    ///
    /// Emitted when user clicks "Add" button in a Redis key viewer.
    /// MainView handles this by opening the KeyValueEditor in "new key" mode.
    AddRedisKey { connection_id: Uuid },

    /// Delete selected rows from the table
    ///
    /// MainView handles this by deleting the specified rows via TableService
    DeleteRows {
        connection_id: Uuid,
        table_name: String,
        /// All column names in the table
        all_column_names: Vec<String>,
        /// Rows to delete (each row contains all column values for identification)
        rows_to_delete: Vec<Vec<String>>,
    },

    /// Table viewer became active/visible (tab switched to)
    ///
    /// Used to update SchemaDetailsPanel with this table's metadata
    BecameActive {
        connection_id: Uuid,
        table_name: String,
        /// Database name for resolving the correct schema qualifier
        database_name: Option<String>,
    },

    /// Table viewer became inactive/closed (tab closed or switched away)
    ///
    /// Used to clear SchemaDetailsPanel if it's showing this table
    BecameInactive {
        connection_id: Uuid,
        table_name: String,
    },

    /// Inline editing started on a cell
    ///
    /// MainView handles this by closing the sidebar CellEditorPanel
    /// to prevent conflicts between the two editors
    InlineEditStarted,

    /// Multi-line content was flattened for inline editing
    ///
    /// Emitted when user starts inline editing a cell containing newlines.
    /// The newlines are replaced with spaces for the single-line editor.
    /// MainView handles this by showing a notification suggesting the Cell Editor panel.
    MultiLineContentFlattened,

    /// Apply filters and sorts to the table
    ///
    /// Emitted when user clicks "Apply Filter & Sort" button or when search text changes.
    /// MainView handles this by calling TableService.browse_table_with_filters()
    ApplyFilters {
        connection_id: Uuid,
        table_name: String,
        /// Filter conditions to apply
        filters: Vec<FilterCondition>,
        /// Sort criteria to apply
        sorts: Vec<SortCriterion>,
        /// Which columns to show (if empty, show all)
        visible_columns: Vec<String>,
        /// Global search text to match across all columns (empty = no search)
        search_text: String,
    },

    /// User clicked a column header to sort (server-side)
    ///
    /// Emitted when user clicks a column header to sort the table.
    /// MainView handles this by reloading the table with ORDER BY.
    /// This replaces client-side sorting for better performance with large tables.
    SortColumn {
        connection_id: Uuid,
        table_name: String,
        /// Column name to sort by
        column_name: String,
        /// Sort direction (Ascending or Descending)
        direction: super::filter_types::SortDirection,
    },

    /// Column visibility changed
    ///
    /// Emitted when user toggles column visibility checkboxes.
    /// Used to update the table view without reloading data.
    ColumnVisibilityChanged {
        /// List of visible column names
        visible_columns: Vec<String>,
    },

    /// Hide a specific column from the table view
    ///
    /// Emitted when user right-clicks column header and selects "Hide Column"
    HideColumn {
        /// Column name to hide
        column_name: String,
    },

    /// Freeze/pin a column to the left side of the table
    ///
    /// Emitted when user right-clicks column header and selects "Freeze Column"
    FreezeColumn {
        /// Column index to freeze (including row number column)
        col_ix: usize,
    },

    /// Unfreeze a frozen column
    ///
    /// Emitted when user right-clicks a frozen column header and selects "Unfreeze Column"
    UnfreezeColumn {
        /// Column index to unfreeze (including row number column)
        col_ix: usize,
    },

    /// Auto-size a column to fit its content
    ///
    /// Emitted when user right-clicks column header and selects "Size Column to Fit"
    SizeColumnToFit {
        /// Column index to auto-size (including row number column)
        col_ix: usize,
    },

    /// Auto-size all columns to fit their content
    ///
    /// Emitted when user right-clicks column header and selects "Size All Columns to Fit"
    SizeAllColumnsToFit,

    /// Add a quick filter for a cell value
    ///
    /// Emitted when user right-clicks a cell and selects "Filter"
    AddQuickFilter {
        /// Column name to filter on
        column_name: String,
        /// Value to filter for (equals comparison)
        value: String,
    },

    /// Commit all pending changes to the database
    ///
    /// Emitted when user clicks the "Commit Changes" button.
    /// MainView handles this by executing UPDATE/INSERT/DELETE statements
    /// for all pending changes in a transaction.
    CommitChanges {
        connection_id: Uuid,
        table_name: String,
        /// Modified cells: (row, col) -> change details
        modified_cells: HashMap<(usize, usize), PendingCellChange>,
        /// Rows marked for deletion
        deleted_rows: HashSet<usize>,
        /// New rows to insert
        new_rows: Vec<Vec<String>>,
        /// Column metadata for building SQL statements
        column_meta: Vec<ColumnMeta>,
        /// All rows data (needed for building WHERE clauses)
        all_rows: Vec<Vec<String>>,
    },

    /// Discard all pending changes
    ///
    /// Emitted when user clicks the "Discard Changes" button.
    /// The delegate reverts all local changes to original values.
    DiscardChanges,

    /// Generate SQL for all pending changes (copy to clipboard)
    ///
    /// Emitted when user clicks the "Generate SQL" button.
    /// MainView handles this by generating SQL statements and copying to clipboard.
    GenerateChangesSql {
        connection_id: Uuid,
        table_name: String,
        /// Modified cells: (row, col) -> change details
        modified_cells: HashMap<(usize, usize), PendingCellChange>,
        /// Rows marked for deletion
        deleted_rows: HashSet<usize>,
        /// New rows to insert
        new_rows: Vec<Vec<String>>,
        /// Column metadata for building SQL statements
        column_meta: Vec<ColumnMeta>,
        /// All rows data (needed for building WHERE clauses)
        all_rows: Vec<Vec<String>>,
    },

    /// User navigated to a different page
    ///
    /// Emitted when user clicks page navigation buttons.
    /// MainView handles this by reloading the table with new offset.
    PageChanged {
        connection_id: Uuid,
        table_name: String,
        /// New page number (1-indexed)
        page: usize,
        /// Records per page limit
        limit: usize,
    },

    /// User changed the records per page limit
    ///
    /// Emitted when user selects a new page size from dropdown.
    /// MainView handles this by reloading the table with new limit.
    LimitChanged {
        connection_id: Uuid,
        table_name: String,
        /// New records per page limit
        limit: usize,
    },

    /// User toggled the limit checkbox (enable/disable pagination)
    ///
    /// When disabled, the table loads all rows (no pagination).
    LimitEnabledChanged {
        connection_id: Uuid,
        table_name: String,
        /// Whether pagination is enabled
        enabled: bool,
    },

    /// Load more data for infinite scroll mode
    ///
    /// Emitted automatically when user scrolls near bottom in infinite scroll mode.
    /// MainView handles this by fetching the next batch and appending to existing rows.
    LoadMore {
        /// Current number of rows already loaded (used as offset for next batch)
        current_offset: usize,
    },

    /// Load foreign key values for a referenced table
    ///
    /// Emitted when user starts editing a FK column and values aren't cached.
    /// MainView handles this by querying the referenced table and caching the values.
    LoadFkValues {
        connection_id: Uuid,
        /// The referenced table to query for values
        referenced_table: String,
        /// The columns in the referenced table to fetch (typically the PK columns)
        referenced_columns: Vec<String>,
    },

    /// Navigate to a foreign key referenced table
    ///
    /// Emitted when user clicks the FK link icon in a cell.
    /// MainView handles this by opening the referenced table in a new tab.
    NavigateToFkTable {
        connection_id: Uuid,
        /// The table that contains the referenced data
        referenced_table: String,
        /// Database name for MySQL multi-database context
        database_name: Option<String>,
    },

    /// Mark rows for deletion from context menu
    ///
    /// Emitted from the cell context menu "Delete Row" / "Delete Selected Rows" action.
    /// This is an internal event handled by the panel itself (not MainView) to defer
    /// the table_state update and avoid double-borrow panics.
    ///
    /// The panel handles this by marking all specified rows for deletion.
    /// In auto-commit mode, this triggers immediate deletion via DeleteRows event.
    /// Otherwise, rows are marked pending and require "Commit Changes".
    MarkRowsForDeletion {
        /// Row indices to delete (includes right-clicked row and any selected via cell selection)
        rows_to_delete: Vec<usize>,
    },

    /// User clicked "Last Page" but total row count is unknown.
    ///
    /// MainView handles this by running a COUNT(*) query, updating the
    /// pagination state with the result, then navigating to the last page.
    LastPageRequested {
        connection_id: Uuid,
        table_name: String,
    },

    /// Edit an existing row in the Row Editor form panel
    ///
    /// Emitted when user wants to edit a full row via the form-based editor.
    /// MainView handles this by opening the KeyValueEditorPanel in SQL row mode
    /// and populating it with the row data.
    EditRow {
        connection_id: Uuid,
        table_name: String,
        row_index: usize,
        row_values: Vec<String>,
        column_meta: Vec<ColumnMeta>,
        all_column_names: Vec<String>,
    },

    /// Add a new row via the Row Editor form panel
    ///
    /// Emitted when user wants a form-based new row creation experience.
    /// MainView handles this by opening the KeyValueEditorPanel in SQL row mode
    /// with empty fields for all columns.
    AddRowForm {
        connection_id: Uuid,
        table_name: String,
        column_meta: Vec<ColumnMeta>,
    },

    /// A row was selected in the table viewer (clicked the row number)
    ///
    /// Emitted when a user clicks the row number column to select a row.
    /// MainView handles this by auto-populating the KeyValueEditorPanel
    /// with the selected row's data (if the Row Editor is currently visible).
    RowSelected {
        connection_id: Uuid,
        table_name: String,
        row_index: usize,
        row_values: Vec<String>,
        column_meta: Vec<ColumnMeta>,
        all_column_names: Vec<String>,
    },

    /// A cell was clicked/selected in the table grid
    ///
    /// Emitted when a user clicks a data cell (not the row-number column).
    /// MainView uses this to sync the Row Editor panel: it opens the editor
    /// with the row's data and focuses the field for the clicked column.
    CellSelected {
        connection_id: Uuid,
        table_name: String,
        row_index: usize,
        /// Data column index (0-based, excludes the row-number column)
        col_index: usize,
        row_values: Vec<String>,
        column_meta: Vec<ColumnMeta>,
        all_column_names: Vec<String>,
    },
}
