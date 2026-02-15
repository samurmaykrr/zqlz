//! Panel module split into submodules
use std::time::Instant;

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{ColumnMeta, DriverCategory, ForeignKeyInfo, QueryResult};
use zqlz_services::ColumnInfo as SchemaColumnInfo;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Selectable, Sizable,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{DropdownMenu, PopupMenuItem},
    table::{PaginationEvent, PaginationState, Table, TableState, render_pagination_controls},
    v_flex,
};

use crate::actions::{CancelCellEditing, CommitChanges, DeleteSelectedRows};
use crate::icons::ZqlzIcon;

actions!(
    table_viewer,
    [
        ToggleSearch,
        CloseSearch,
        CopySelection,
        PasteClipboard,
        OpenRowEditor
    ]
);

use super::column_visibility::{
    ColumnVisibilityEvent, ColumnVisibilityPanel, ColumnVisibilityState,
};
use crate::components::table_viewer::delegate::TableViewerDelegate;
use crate::components::table_viewer::events::TableViewerEvent;
use crate::components::table_viewer::filter_panel::{FilterPanel, FilterPanelEvent, FilterPanelState};
use crate::components::table_viewer::filter_types::ColumnSelectItem;

// Submodules
mod loader;
mod state;
mod toolbar;
mod search;
mod actions;
mod selection;
mod clipboard;
mod export;
mod filters;
mod column_ops;
mod render;
mod traits;

/// Table viewer panel - main component
pub struct TableViewerPanel {
    /// Focus handle for keyboard focus management
    pub(super) focus_handle: FocusHandle,

    /// Table state (contains delegate with data)
    pub(crate) table_state: Option<Entity<TableState<TableViewerDelegate>>>,

    /// Connection ID (for refresh/reload operations)
    pub(super) connection_id: Option<Uuid>,

    /// Connection name (shown in panel title)
    pub(super) connection_name: Option<String>,

    /// Table name (shown in panel title)
    pub(super) table_name: Option<String>,

    /// Database name for MySQL multi-database context (e.g. "sakila")
    pub(crate) database_name: Option<String>,

    /// Whether this is a database view (vs a table)
    pub(super) is_view: bool,

    /// Driver category (Relational, KeyValue, etc.) - determines refresh behavior
    pub(crate) driver_category: DriverCategory,

    /// Row count (shown in header)
    pub(super) row_count: usize,

    /// Loading state (shows spinner)
    pub(super) is_loading: bool,

    /// Selected row indices (for delete operations)
    pub(crate) selected_rows: std::collections::HashSet<usize>,

    /// Filter panel state
    pub(crate) filter_panel_state: Option<Entity<FilterPanelState>>,

    /// Column visibility state
    pub(crate) column_visibility_state: Option<Entity<ColumnVisibilityState>>,

    /// Whether the filter panel is expanded
    pub(super) filter_expanded: bool,

    /// Whether the column visibility panel is shown
    pub(super) column_visibility_shown: bool,

    /// Column metadata for currently displayed columns
    pub(crate) column_meta: Vec<ColumnMeta>,

    /// Original/master column metadata (all columns from initial table load)
    pub(super) original_column_meta: Vec<ColumnMeta>,

    /// Search bar state
    pub(super) search_input: Option<Entity<InputState>>,

    /// Whether the search bar is visible
    pub(super) search_visible: bool,

    /// Current search text (for server-side filtering)
    pub(crate) search_text: String,

    /// Debounce task for search input - prevents firing a query on every keystroke
    pub(super) _search_debounce_task: Option<Task<()>>,

    /// Pagination state
    pub(crate) pagination_state: Option<Entity<PaginationState>>,

    /// Auto-commit mode
    pub(super) auto_commit_mode: bool,

    /// Whether transaction controls panel is expanded
    pub(super) transaction_panel_expanded: bool,

    /// Cached foreign key info for re-applying after table refresh
    pub(super) foreign_keys: Vec<zqlz_core::ForeignKeyInfo>,

    /// Primary key column names (populated from schema details, used for
    /// efficient "last page" navigation via reversed ORDER BY instead of
    /// expensive high-OFFSET scans).
    pub(crate) primary_key_columns: Vec<String>,

    /// When the current loading operation started (for elapsed time display)
    pub(super) loading_started_at: Option<Instant>,

    /// Periodic re-render task that ticks during loading to update the elapsed timer
    pub(super) _loading_timer_task: Option<Task<()>>,
}

impl TableViewerPanel {
    /// Create a new empty table viewer panel
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            table_state: None,
            connection_id: None,
            connection_name: None,
            table_name: None,
            database_name: None,
            is_view: false,
            driver_category: DriverCategory::Relational,
            row_count: 0,
            is_loading: false,
            selected_rows: std::collections::HashSet::new(),
            filter_panel_state: None,
            column_visibility_state: None,
            filter_expanded: false,
            column_visibility_shown: false,
            column_meta: Vec::new(),
            original_column_meta: Vec::new(),
            search_input: None,
            search_visible: false,
            search_text: String::new(),
            _search_debounce_task: None,
            pagination_state: None,
            auto_commit_mode: true,
            transaction_panel_expanded: false,
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            loading_started_at: None,
            _loading_timer_task: None,
        }
    }

    /// Returns whether this panel is displaying a database view (vs a table)
    pub fn is_view(&self) -> bool {
        self.is_view
    }

    /// Returns the connection ID (if set)
    pub(crate) fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    /// Returns the table name (if set)
    pub(crate) fn table_name(&self) -> Option<String> {
        self.table_name.clone()
    }

    /// Returns the database name (if set) for MySQL multi-database context
    pub(crate) fn database_name(&self) -> Option<String> {
        self.database_name.clone()
    }
}

// TableViewerPanel type is defined in this module and exposed by parent with `pub use panel::TableViewerPanel`.
