//! Panel module split into submodules
use std::time::Instant;

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{ColumnMeta, DriverCategory, ForeignKeyInfo, QueryResult, RowIdentity, TableType};
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

use crate::actions::{CancelCellEditing, CommitChanges, DeleteSelectedRows, RedoEdit, UndoEdit};
use crate::icons::ZqlzIcon;
use crate::workspace_state::{
    WorkspaceSessionFilterCondition, WorkspaceSessionSortCriterion,
    WorkspaceSessionTableViewerState,
};

actions!(
    table_viewer,
    [
        ToggleSearch,
        CloseSearch,
        CutSelection,
        CopySelection,
        PasteClipboard,
        SelectAllSelection,
        OpenRowEditor,
        ToggleReplace,
        SelectColumnVisibilityPrevious,
        SelectColumnVisibilityNext,
        ToggleSelectedColumnVisibility,
        CloseColumnVisibility,
    ]
);

use super::column_visibility::{
    ColumnVisibilityEvent, ColumnVisibilityPanel, ColumnVisibilityState,
};
use crate::components::table_viewer::delegate::TableViewerDelegate;
use crate::components::table_viewer::events::TableViewerEvent;
use crate::components::table_viewer::filter_panel::{
    FilterPanel, FilterPanelEvent, FilterPanelState,
};
use crate::components::table_viewer::filter_types::ColumnSelectItem;

// Submodules
mod actions;
mod clipboard;
mod column_ops;
mod export;
mod fetch;
mod filters;
mod loader;
mod render;
mod search;
mod selection;
mod selection_stats;
mod state;
mod toolbar;
mod traits;

pub(super) struct SelectionStatsSummary {
    pub selection_label: String,
    pub numeric_stats: Option<(String, String, String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct TablePerformanceProfile {
    pub searchable_columns: Vec<String>,
}

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

    /// The table as the open resolved it against the catalog. Every fetch reads
    /// this, so paging never pays for resolution again.
    pub(super) table: Option<zqlz_core::TableRef>,

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

    /// The open-time load for this viewer. Owned here so closing the tab
    /// cancels it, while opening another table leaves it running.
    pub(crate) open_task: Option<Task<anyhow::Result<()>>>,

    /// The row fetch in flight; replacing it cancels the previous one.
    pub(super) fetch_task: Option<Task<()>>,

    /// The filters the pagination total was counted under, so it is only reused
    /// while they still apply.
    pub(super) counted_where_clauses: Vec<String>,

    /// Restored session filters must be fetched once the controls exist.
    pub(super) reload_after_session_restore: bool,

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

    /// Replace input state (for find & replace)
    pub(super) replace_input: Option<Entity<InputState>>,

    /// Whether the replace bar is visible
    pub(super) replace_visible: bool,

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

    /// How rows of the current table are addressed when edited or deleted.
    pub(crate) row_identity: RowIdentity,

    /// When the current loading operation started (for elapsed time display)
    pub(super) loading_started_at: Option<Instant>,

    /// Periodic re-render task that ticks during loading to update the elapsed timer
    pub(super) _loading_timer_task: Option<Task<()>>,

    /// Monotonic generation for server-driven table loads.
    ///
    /// Async refresh, pagination, filter, and initial-load tasks capture the
    /// current value before they start. Their results are only applied if the
    /// generation still matches when they complete.
    pub(super) active_request_generation: u64,

    /// Cached selection summary rendered in the footer.
    ///
    /// This is recomputed when the table emits selection-change events so
    /// render stays lightweight even for large multi-cell selections.
    pub(super) selection_stats: Option<SelectionStatsSummary>,

    /// Lightweight performance profile derived from loaded table schema.
    pub(crate) performance_profile: Option<TablePerformanceProfile>,

    /// Session state waiting for filter/column-visibility controls to exist.
    pub(crate) pending_session_state: Option<WorkspaceSessionTableViewerState>,
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
            open_task: None,
            table: None,
            fetch_task: None,
            counted_where_clauses: Vec::new(),
            reload_after_session_restore: false,
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
            replace_input: None,
            replace_visible: false,
            pagination_state: None,
            auto_commit_mode: true,
            transaction_panel_expanded: false,
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            row_identity: RowIdentity::default(),
            loading_started_at: None,
            _loading_timer_task: None,
            active_request_generation: 0,
            selection_stats: None,
            performance_profile: None,
            pending_session_state: None,
        }
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

    /// The table this viewer shows, once its open has resolved it.
    pub(crate) fn table(&self) -> Option<zqlz_core::TableRef> {
        self.table.clone()
    }

    pub(crate) fn session_state_snapshot(
        &self,
        cx: &App,
    ) -> Option<(Uuid, zqlz_core::TableRef, WorkspaceSessionTableViewerState)> {
        let connection_id = self.connection_id?;
        let table = self.table.clone()?;
        let filters = self
            .filter_panel_state
            .as_ref()
            .map(|state| state.read(cx).get_filter_conditions())
            .unwrap_or_default()
            .into_iter()
            .map(|filter| WorkspaceSessionFilterCondition {
                id: filter.id,
                enabled: filter.enabled,
                column: filter.column,
                operator: format!("{:?}", filter.operator),
                value: filter.value,
                value2: filter.value2,
                custom_sql: filter.custom_sql,
                logical_operator: format!("{:?}", filter.logical_operator),
            })
            .collect();
        let sorts = self
            .filter_panel_state
            .as_ref()
            .map(|state| state.read(cx).get_sort_criteria())
            .unwrap_or_default()
            .into_iter()
            .map(|sort| WorkspaceSessionSortCriterion {
                id: sort.id,
                column: sort.column,
                direction: format!("{:?}", sort.direction),
            })
            .collect();
        let visible_columns = self
            .column_visibility_state
            .as_ref()
            .map(|state| state.read(cx).visible_columns())
            .unwrap_or_default();

        Some((
            connection_id,
            table,
            WorkspaceSessionTableViewerState {
                filters,
                sorts,
                visible_columns,
                search_text: self.search_text.clone(),
            },
        ))
    }

    pub(crate) fn set_pending_session_state(
        &mut self,
        session_state: Option<WorkspaceSessionTableViewerState>,
        cx: &mut Context<Self>,
    ) {
        self.pending_session_state = session_state;
        cx.notify();
    }

    pub(crate) fn apply_pending_session_state(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(session_state) = self.pending_session_state.take() else {
            return;
        };

        self.reload_after_session_restore = !session_state.filters.is_empty()
            || !session_state.sorts.is_empty()
            || !session_state.search_text.trim().is_empty();
        self.search_text = session_state.search_text.clone();
        if let Some(filter_state) = &self.filter_panel_state {
            filter_state.update(cx, |state, cx| {
                state.restore_criteria(&session_state.filters, &session_state.sorts, window, cx);
            });
        }
        if let Some(column_visibility_state) = &self.column_visibility_state
            && !session_state.visible_columns.is_empty()
        {
            let visible_columns = std::collections::HashSet::<String>::from_iter(
                session_state.visible_columns.iter().cloned(),
            );
            column_visibility_state.update(cx, |state, cx| {
                let column_names = state
                    .visible_columns()
                    .into_iter()
                    .chain(state.hidden_columns())
                    .collect::<Vec<_>>();
                for column_name in column_names {
                    state.set_column_visibility(
                        &column_name,
                        visible_columns.contains(&column_name),
                        cx,
                    );
                }
            });
        }
        cx.notify();
    }
}

// TableViewerPanel type is defined in this module and exposed by parent with `pub use panel::TableViewerPanel`.
