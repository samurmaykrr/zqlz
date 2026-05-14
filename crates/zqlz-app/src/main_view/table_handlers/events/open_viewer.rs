//! Table viewer opening and initialization
//!
//! This module handles opening table viewers as new tabs with full event subscription setup.
//! It manages the complex async operations of loading table data, schema details, and
//! maintaining consistency between the UI panels.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::table_viewer::delegate::SaveCellRequest;
use crate::components::{
    InspectorView, RowData, SchemaDetailsPanel, TableViewerEvent, TableViewerPanel,
};

use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::conversion::convert_to_schema_details;
use crate::workspace_state::{
    WorkspaceSessionSortCriterion, WorkspaceSessionTableViewerState, WorkspaceSessionViewerKind,
    WorkspaceSessionViewerTab,
};

use super::super::standalone_events::{
    ApplyFiltersRequest, BecameActiveRequest, SaveNewRowRequest, handle_add_row_event,
    handle_apply_filters_event, handle_became_active_event, handle_became_inactive_event,
    handle_commit_changes_event, handle_delete_rows_event, handle_edit_cell_event,
    handle_generate_sql_event, handle_last_page_requested_event, handle_limit_changed_event,
    handle_limit_enabled_changed_event, handle_load_distinct_values_event,
    handle_load_fk_values_event, handle_load_more_event, handle_page_changed_event,
    handle_refresh_table_event, handle_save_cell_event, handle_save_new_row_event,
    handle_sort_column_event,
};

pub(in crate::main_view) struct TableViewerSessionOpenRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub database_name: Option<String>,
    pub is_view: bool,
    pub session_state: Option<WorkspaceSessionTableViewerState>,
}

use zqlz_services::{
    OpenTableViewerCountDecisionRequest, OpenViewerInitialLoadRequest,
    build_open_viewer_schema_viewer_metadata, decide_open_viewer_count_workflow,
};

#[derive(Clone, Copy, Debug)]
struct OpenViewerAsyncRequestContext<'a> {
    connection_id: Uuid,
    table_name: &'a str,
    database_name: Option<&'a str>,
    request_generation: u64,
    task_generation: u64,
}

impl<'a> OpenViewerAsyncRequestContext<'a> {
    fn new(
        connection_id: Uuid,
        table_name: &'a str,
        database_name: Option<&'a str>,
        request_generation: u64,
        task_generation: u64,
    ) -> Self {
        Self {
            connection_id,
            table_name,
            database_name,
            request_generation,
            task_generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenViewerAsyncRequestContext, OpenViewerRequestOwnershipSnapshot};
    use uuid::Uuid;

    #[test]
    fn open_viewer_request_ownership_matches_stamped_viewer_identity() {
        let connection_id = Uuid::new_v4();
        let snapshot = OpenViewerRequestOwnershipSnapshot {
            current_request_generation: 1,
            current_connection_id: Some(connection_id),
            current_table_name: Some("catalog.bundle_items".to_string()),
            current_database_name: Some("erp_lab".to_string()),
        };
        let request_context = OpenViewerAsyncRequestContext::new(
            connection_id,
            "catalog.bundle_items",
            Some("erp_lab"),
            1,
            1,
        );

        assert!(snapshot.matches_request(request_context));
    }

    #[test]
    fn open_viewer_request_ownership_rejects_missing_connection_identity() {
        let connection_id = Uuid::new_v4();
        let snapshot = OpenViewerRequestOwnershipSnapshot {
            current_request_generation: 1,
            current_connection_id: None,
            current_table_name: Some("catalog.bundle_items".to_string()),
            current_database_name: Some("erp_lab".to_string()),
        };
        let request_context = OpenViewerAsyncRequestContext::new(
            connection_id,
            "catalog.bundle_items",
            Some("erp_lab"),
            1,
            1,
        );

        assert!(!snapshot.matches_request(request_context));
    }

    #[test]
    fn open_viewer_request_ownership_accepts_unresolved_database_identity() {
        let connection_id = Uuid::new_v4();
        let snapshot = OpenViewerRequestOwnershipSnapshot {
            current_request_generation: 1,
            current_connection_id: Some(connection_id),
            current_table_name: Some("catalog.bundle_items".to_string()),
            current_database_name: None,
        };
        let request_context = OpenViewerAsyncRequestContext::new(
            connection_id,
            "catalog.bundle_items",
            Some("erp_lab"),
            1,
            1,
        );

        assert!(snapshot.matches_request(request_context));
    }

    #[test]
    fn open_viewer_request_ownership_rejects_wrong_database_identity() {
        let connection_id = Uuid::new_v4();
        let snapshot = OpenViewerRequestOwnershipSnapshot {
            current_request_generation: 1,
            current_connection_id: Some(connection_id),
            current_table_name: Some("catalog.bundle_items".to_string()),
            current_database_name: Some("warehouse".to_string()),
        };
        let request_context = OpenViewerAsyncRequestContext::new(
            connection_id,
            "catalog.bundle_items",
            Some("erp_lab"),
            1,
            1,
        );

        assert!(!snapshot.matches_request(request_context));
    }
}

#[derive(Debug)]
struct OpenViewerRequestOwnershipSnapshot {
    current_request_generation: u64,
    current_connection_id: Option<Uuid>,
    current_table_name: Option<String>,
    current_database_name: Option<String>,
}

struct OpenViewerSqlRowDataRequest<'a> {
    connection_id: Uuid,
    table_name: &'a str,
    column_meta: &'a [zqlz_core::ColumnMeta],
    row_values: &'a [zqlz_core::Value],
    row_index: Option<usize>,
    is_new: bool,
    all_column_names: Option<&'a [String]>,
    source_viewer: WeakEntity<TableViewerPanel>,
}

struct OpenViewerExistingSqlRowEditorUpdateRequest<'a> {
    connection_id: Uuid,
    table_name: &'a str,
    column_meta: &'a [zqlz_core::ColumnMeta],
    row_values: &'a [zqlz_core::Value],
    row_index: usize,
    all_column_names: &'a [String],
    focused_column_index: Option<usize>,
    reveal_inspector_panel: Option<bool>,
    source_viewer: WeakEntity<TableViewerPanel>,
}

#[derive(Clone, Copy, Debug)]
enum OpenViewerExistingSqlRowApplyRoute {
    Always,
    OnlyWhenSqlRowModeActive,
}

#[derive(Clone, Copy, Debug)]
struct OpenViewerExistingSqlRowEventBehavior {
    focused_column_index: Option<usize>,
    reveal_inspector_panel: Option<bool>,
    apply_route: OpenViewerExistingSqlRowApplyRoute,
}

impl OpenViewerRequestOwnershipSnapshot {
    fn from_viewer(viewer: &TableViewerPanel) -> Self {
        Self {
            current_request_generation: viewer.current_request_generation(),
            current_connection_id: viewer.connection_id(),
            current_table_name: viewer.table_name(),
            current_database_name: viewer.database_name(),
        }
    }

    fn matches_request(&self, request_context: OpenViewerAsyncRequestContext) -> bool {
        let is_same_connection = self.current_connection_id == Some(request_context.connection_id);
        let is_same_table = self.current_table_name.as_deref() == Some(request_context.table_name);
        let is_same_database =
            self.current_database_name
                .as_deref()
                .is_none_or(|current_database_name| {
                    Some(current_database_name) == request_context.database_name
                });

        is_same_connection && is_same_table && is_same_database
    }
}

fn log_open_viewer_request_guard_skip(
    request_context: OpenViewerAsyncRequestContext,
    ownership_snapshot: &OpenViewerRequestOwnershipSnapshot,
    message: &'static str,
) {
    tracing::debug!(
        connection_id = %request_context.connection_id,
        table_name = request_context.table_name,
        database_name = request_context.database_name,
        request_generation = request_context.request_generation,
        task_generation = request_context.task_generation,
        current_request_generation = ownership_snapshot.current_request_generation,
        current_connection_id = ?ownership_snapshot.current_connection_id,
        current_table_name = ?ownership_snapshot.current_table_name,
        current_database_name = ?ownership_snapshot.current_database_name,
        "{}",
        message
    );
}

fn log_open_viewer_dropped_request_update(
    error: &anyhow::Error,
    request_context: OpenViewerAsyncRequestContext,
    message: &'static str,
) {
    tracing::debug!(
        error = %error,
        connection_id = %request_context.connection_id,
        table_name = request_context.table_name,
        database_name = request_context.database_name,
        request_generation = request_context.request_generation,
        task_generation = request_context.task_generation,
        "{}",
        message
    );
}

fn log_open_viewer_dropped_schema_details_panel_update(
    error: &anyhow::Error,
    request_context: OpenViewerAsyncRequestContext,
    ownership_snapshot: &OpenViewerRequestOwnershipSnapshot,
    message: &'static str,
) {
    let current_request_generation = ownership_snapshot.current_request_generation;
    let current_connection_id = ownership_snapshot.current_connection_id;
    let current_table_name = ownership_snapshot.current_table_name.as_deref();
    let current_database_name = ownership_snapshot.current_database_name.as_deref();

    tracing::debug!(
        error = %error,
        connection_id = %request_context.connection_id,
        table_name = request_context.table_name,
        database_name = request_context.database_name,
        request_generation = request_context.request_generation,
        task_generation = request_context.task_generation,
        current_request_generation,
        current_connection_id = ?current_connection_id,
        current_table_name = ?current_table_name,
        current_database_name = ?current_database_name,
        "{}",
        message
    );
}

fn log_open_viewer_task_cleanup_guard_skip(
    request_context: OpenViewerAsyncRequestContext,
    current_task_generation: u64,
    message: &'static str,
) {
    tracing::debug!(
        connection_id = %request_context.connection_id,
        table_name = request_context.table_name,
        database_name = request_context.database_name,
        request_generation = request_context.request_generation,
        task_generation = request_context.task_generation,
        current_task_generation,
        "{}",
        message
    );
}

fn log_open_viewer_background_count_failure(
    request_context: OpenViewerAsyncRequestContext,
    error: &dyn std::fmt::Display,
) {
    tracing::warn!(
        error = %error,
        connection_id = %request_context.connection_id,
        table_name = request_context.table_name,
        database_name = request_context.database_name,
        request_generation = request_context.request_generation,
        task_generation = request_context.task_generation,
        "Background row count failed during open-viewer load"
    );
}

fn capture_open_viewer_request_ownership_if_current(
    viewer: &TableViewerPanel,
    request_context: OpenViewerAsyncRequestContext,
    update_kind: OpenViewerAsyncUpdateKind,
) -> Option<OpenViewerRequestOwnershipSnapshot> {
    let ownership_snapshot = OpenViewerRequestOwnershipSnapshot::from_viewer(viewer);

    if ownership_snapshot.current_request_generation != request_context.request_generation {
        log_open_viewer_request_guard_skip(
            request_context,
            &ownership_snapshot,
            update_kind.stale_debug_message(),
        );
        return None;
    }

    if ownership_snapshot.matches_request(request_context) {
        Some(ownership_snapshot)
    } else {
        log_open_viewer_request_guard_skip(
            request_context,
            &ownership_snapshot,
            update_kind.ownership_changed_debug_message(),
        );
        None
    }
}

fn read_open_viewer_request_ownership_if_current(
    viewer_weak: &WeakEntity<TableViewerPanel>,
    request_context: OpenViewerAsyncRequestContext,
    update_kind: OpenViewerAsyncUpdateKind,
    cx: &mut AsyncWindowContext,
) -> Option<OpenViewerRequestOwnershipSnapshot> {
    match viewer_weak.read_with(cx, |viewer, _cx| {
        capture_open_viewer_request_ownership_if_current(viewer, request_context, update_kind)
    }) {
        Ok(ownership_snapshot) => ownership_snapshot,
        Err(error) => {
            log_open_viewer_dropped_request_update(
                &error,
                request_context,
                update_kind.dropped_debug_message(),
            );
            None
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum OpenViewerAsyncUpdateKind {
    Viewer,
    ViewerWindow,
    BackgroundCount,
    SchemaDetails,
    SchemaDetailsPreApply,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaDetailsApplyOutcome {
    Applied,
    SkippedRequestOwnership,
    SkippedPanelUnavailable,
}

#[derive(Clone, Copy, Debug)]
enum OpenViewerSchemaDetailsFailurePhase {
    ConnectionResolution,
    SchemaLoad,
}

impl OpenViewerSchemaDetailsFailurePhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionResolution => "connection-resolution",
            Self::SchemaLoad => "schema-details-load",
        }
    }
}

fn log_open_viewer_schema_details_loading_reset_outcome(
    request_context: OpenViewerAsyncRequestContext,
    failure_phase: OpenViewerSchemaDetailsFailurePhase,
    outcome: SchemaDetailsApplyOutcome,
) {
    match outcome {
        SchemaDetailsApplyOutcome::Applied => {
            tracing::debug!(
                connection_id = %request_context.connection_id,
                table_name = request_context.table_name,
                database_name = request_context.database_name,
                request_generation = request_context.request_generation,
                task_generation = request_context.task_generation,
                failure_phase = failure_phase.as_str(),
                "Cleared schema-details loading state after open-viewer failure"
            );
        }
        SchemaDetailsApplyOutcome::SkippedRequestOwnership => {
            tracing::debug!(
                connection_id = %request_context.connection_id,
                table_name = request_context.table_name,
                database_name = request_context.database_name,
                request_generation = request_context.request_generation,
                task_generation = request_context.task_generation,
                failure_phase = failure_phase.as_str(),
                "Skipped schema-details loading reset because open-viewer request ownership changed"
            );
        }
        SchemaDetailsApplyOutcome::SkippedPanelUnavailable => {
            tracing::debug!(
                connection_id = %request_context.connection_id,
                table_name = request_context.table_name,
                database_name = request_context.database_name,
                request_generation = request_context.request_generation,
                task_generation = request_context.task_generation,
                failure_phase = failure_phase.as_str(),
                "Skipped schema-details loading reset because schema details panel was unavailable"
            );
        }
    }
}

impl OpenViewerAsyncUpdateKind {
    fn stale_debug_message(self) -> &'static str {
        match self {
            Self::Viewer => "Skipped stale open-viewer async update",
            Self::ViewerWindow => "Skipped stale open-viewer async window update",
            Self::BackgroundCount => "Skipped stale open-viewer background-count async update",
            Self::SchemaDetails => "Skipped stale schema-details async update",
            Self::SchemaDetailsPreApply => "Skipped stale schema-details pre-apply revalidation",
        }
    }

    fn ownership_changed_debug_message(self) -> &'static str {
        match self {
            Self::Viewer => "Skipped open-viewer async update because viewer ownership changed",
            Self::ViewerWindow => {
                "Skipped open-viewer async window update because viewer ownership changed"
            }
            Self::BackgroundCount => {
                "Skipped open-viewer background-count update because viewer ownership changed"
            }
            Self::SchemaDetails => "Skipped schema-details update because viewer ownership changed",
            Self::SchemaDetailsPreApply => {
                "Skipped schema-details pre-apply update because viewer ownership changed"
            }
        }
    }

    fn dropped_debug_message(self) -> &'static str {
        match self {
            Self::Viewer => "Skipped open-viewer async update after viewer dropped",
            Self::ViewerWindow => "Skipped open-viewer async window update after viewer dropped",
            Self::BackgroundCount => {
                "Skipped open-viewer background-count update after viewer dropped"
            }
            Self::SchemaDetails => "Skipped schema-details update after viewer dropped",
            Self::SchemaDetailsPreApply => {
                "Skipped schema-details pre-apply revalidation after viewer dropped"
            }
        }
    }

    fn dropped_panel_debug_message(self) -> &'static str {
        match self {
            Self::Viewer => "Skipped open-viewer async update after panel dropped",
            Self::ViewerWindow => "Skipped open-viewer async window update after panel dropped",
            Self::BackgroundCount => {
                "Skipped open-viewer background-count update after panel dropped"
            }
            Self::SchemaDetails => {
                "Skipped schema-details update after schema details panel dropped"
            }
            Self::SchemaDetailsPreApply => {
                "Skipped schema-details pre-apply update after schema details panel dropped"
            }
        }
    }
}

impl MainView {
    fn build_open_viewer_sql_row_data(request: OpenViewerSqlRowDataRequest<'_>) -> RowData {
        let all_column_names = match request.all_column_names {
            Some(all_column_names) => all_column_names.to_vec(),
            None => request
                .column_meta
                .iter()
                .map(|column| column.name.clone())
                .collect(),
        };

        RowData {
            table_name: request.table_name.to_owned(),
            connection_id: request.connection_id,
            column_meta: request.column_meta.to_vec(),
            row_values: request.row_values.to_vec(),
            row_index: request.row_index,
            is_new: request.is_new,
            source_viewer: Some(request.source_viewer),
            all_column_names,
        }
    }

    fn set_open_viewer_key_editor_inspector_active(
        &mut self,
        reveal_inspector_panel: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.inspector_panel.update(cx, |panel, cx| {
            panel.set_active_view(InspectorView::KeyEditor, cx);
        });

        if reveal_inspector_panel {
            self.workspace_controller.update(cx, |workspace, cx| {
                workspace.reveal_panel(
                    "InspectorPanel",
                    zqlz_ui::widgets::dock::DockPlacement::Right,
                    window,
                    cx,
                );
            });
        }
    }

    fn apply_open_viewer_sql_row_editor_update(
        &mut self,
        row_data: RowData,
        focused_column_index: Option<usize>,
        reveal_inspector_panel: Option<bool>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.key_value_editor_panel.update(cx, |editor, cx| {
            editor.edit_row(row_data, window, cx);
            if let Some(focused_column_index) = focused_column_index {
                editor.focus_field(focused_column_index, window, cx);
            }
        });

        if let Some(reveal_inspector_panel) = reveal_inspector_panel {
            self.set_open_viewer_key_editor_inspector_active(reveal_inspector_panel, window, cx);
        }
    }

    fn apply_open_viewer_existing_sql_row_editor_update(
        &mut self,
        request: OpenViewerExistingSqlRowEditorUpdateRequest<'_>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row_data = Self::build_open_viewer_sql_row_data(OpenViewerSqlRowDataRequest {
            connection_id: request.connection_id,
            table_name: request.table_name,
            column_meta: request.column_meta,
            row_values: request.row_values,
            row_index: Some(request.row_index),
            is_new: false,
            all_column_names: Some(request.all_column_names),
            source_viewer: request.source_viewer,
        });

        self.apply_open_viewer_sql_row_editor_update(
            row_data,
            request.focused_column_index,
            request.reveal_inspector_panel,
            window,
            cx,
        );
    }

    fn handle_open_viewer_existing_sql_row_event(
        &mut self,
        request: OpenViewerExistingSqlRowEditorUpdateRequest<'_>,
        apply_route: OpenViewerExistingSqlRowApplyRoute,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // RowSelected should only mutate the key editor while the user is
        // already in SQL row-edit mode; EditRow/CellSelected keep unconditional
        // behavior so explicit edit intent still opens/syncs the editor.
        if matches!(
            apply_route,
            OpenViewerExistingSqlRowApplyRoute::OnlyWhenSqlRowModeActive
        ) {
            let is_key_editor_active =
                self.inspector_panel.read(cx).active_view() == InspectorView::KeyEditor;
            let is_sql_row_mode = self.key_value_editor_panel.read(cx).mode()
                == &crate::components::RowEditorMode::SqlRow;
            if !(is_key_editor_active && is_sql_row_mode) {
                return;
            }
        }

        self.apply_open_viewer_existing_sql_row_editor_update(request, window, cx);
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_open_viewer_existing_sql_row_event_from_payload(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        column_meta: &[zqlz_core::ColumnMeta],
        row_values: &[zqlz_core::Value],
        row_index: usize,
        all_column_names: &[String],
        source_viewer: WeakEntity<TableViewerPanel>,
        behavior: OpenViewerExistingSqlRowEventBehavior,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.handle_open_viewer_existing_sql_row_event(
            OpenViewerExistingSqlRowEditorUpdateRequest {
                connection_id,
                table_name,
                column_meta,
                row_values,
                row_index,
                all_column_names,
                focused_column_index: behavior.focused_column_index,
                reveal_inspector_panel: behavior.reveal_inspector_panel,
                source_viewer,
            },
            behavior.apply_route,
            window,
            cx,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_open_viewer_add_row_form_event(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        column_meta: &[zqlz_core::ColumnMeta],
        row_values: Option<&[zqlz_core::Value]>,
        row_index: Option<usize>,
        source_viewer: WeakEntity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(row_values) = row_values {
            let row_data = Self::build_open_viewer_sql_row_data(OpenViewerSqlRowDataRequest {
                connection_id,
                table_name,
                column_meta,
                row_values,
                row_index,
                is_new: true,
                all_column_names: None,
                source_viewer,
            });
            self.apply_open_viewer_sql_row_editor_update(row_data, None, None, window, cx);
            self.key_value_editor_panel.update(cx, |editor, cx| {
                editor.focus_first_editable_row_field(window, cx);
            });
        } else {
            self.key_value_editor_panel.update(cx, |editor, cx| {
                editor.new_row(
                    table_name.to_owned(),
                    connection_id,
                    column_meta.to_vec(),
                    Some(source_viewer),
                    window,
                    cx,
                );
            });
        }

        // Both AddRowForm paths transition to key editor; apply reveal once
        // after branch-specific row setup to avoid duplicate panel activation.
        self.set_open_viewer_key_editor_inspector_active(true, window, cx);
    }

    fn handle_open_viewer_add_redis_key_event(
        &mut self,
        connection_id: Uuid,
        viewer_entity_for_events: &Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let active_database_name = viewer_entity_for_events.read(cx).database_name();
        tracing::info!(
            connection_id = %connection_id,
            database_name = ?active_database_name,
            "Opening KeyValueEditor for new Redis key"
        );
        self.key_value_editor_panel.update(cx, |editor, cx| {
            editor.new_key(connection_id, active_database_name, window, cx);
        });
        self.set_open_viewer_key_editor_inspector_active(true, window, cx);
    }

    fn handle_open_viewer_navigate_to_fk_table_event(
        &mut self,
        connection_id: Uuid,
        referenced_table: &str,
        database_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            connection_id = %connection_id,
            referenced_table = %referenced_table,
            database_name = ?database_name,
            "Navigating to foreign-key referenced table"
        );
        self.open_table_viewer(
            connection_id,
            referenced_table.to_owned(),
            database_name,
            false,
            window,
            cx,
        );
    }

    fn handle_open_viewer_refresh_table_event(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        driver_category: zqlz_core::DriverCategory,
        viewer_entity_for_events: &Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Refresh invalidates row indices, so any inline cell edit context must
        // be dropped before the table snapshot is replaced.
        self.cell_editor_panel.update(cx, |editor, cx| {
            editor.clear(cx);
        });

        handle_refresh_table_event(
            connection_id,
            table_name,
            driver_category,
            viewer_entity_for_events.clone(),
            window,
            cx,
        );
    }

    fn handle_open_viewer_apply_filters_event(
        &mut self,
        request: ApplyFiltersRequest,
        viewer_entity_for_events: &Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Filter application can reorder/remap visible rows, so clear active
        // cell edits first to avoid retaining stale row-index references.
        self.cell_editor_panel.update(cx, |editor, cx| {
            editor.clear(cx);
        });

        handle_apply_filters_event(request, viewer_entity_for_events.clone(), window, cx);
    }

    fn persist_open_viewer_state_snapshot(
        &mut self,
        viewer_entity: &Entity<TableViewerPanel>,
        connection_id: Uuid,
        table_name: &str,
        cx: &mut Context<Self>,
    ) {
        let Some((_, _, database_name, viewer_state)) =
            viewer_entity.read_with(cx, |panel, cx| panel.session_state_snapshot(cx))
        else {
            return;
        };

        self.workspace_state.update(cx, |state, cx| {
            state.update_table_viewer_state(
                connection_id,
                table_name,
                database_name.as_deref(),
                viewer_state,
                cx,
            );
        });
    }

    pub(in crate::main_view) fn detach_table_viewer_subscription(
        &mut self,
        table_viewer: &Entity<TableViewerPanel>,
    ) {
        self.table_viewer_subscriptions
            .remove(&table_viewer.entity_id());
    }

    fn handle_open_viewer_panel_event(
        &mut self,
        event: &TableViewerEvent,
        viewer_entity_for_events: &Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            TableViewerEvent::HideColumn { column_name } => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.hide_column(column_name, cx);
                });
                if let Some((connection_id, table_name, _, _)) = viewer_entity_for_events
                    .read_with(cx, |panel, cx| panel.session_state_snapshot(cx))
                {
                    self.persist_open_viewer_state_snapshot(
                        viewer_entity_for_events,
                        connection_id,
                        &table_name,
                        cx,
                    );
                }
            }
            TableViewerEvent::FreezeColumn { col_ix } => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.freeze_column(*col_ix, cx);
                });
            }
            TableViewerEvent::UnfreezeColumn { col_ix } => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.unfreeze_column(*col_ix, cx);
                });
            }
            TableViewerEvent::SizeColumnToFit { col_ix } => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.size_column_to_fit(*col_ix, cx);
                });
            }
            TableViewerEvent::SizeAllColumnsToFit => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.size_all_columns_to_fit(cx);
                });
            }
            TableViewerEvent::AddQuickFilter {
                column_name,
                operator,
                value,
            } => {
                viewer_entity_for_events.update(cx, |panel, cx| {
                    panel.add_quick_filter(
                        column_name.clone(),
                        *operator,
                        value.clone(),
                        window,
                        cx,
                    );
                });
            }
            // Column-visibility data updates are panel-owned and intentionally
            // ignored in the app event layer.
            TableViewerEvent::ColumnVisibilityChanged { .. } => {
                if let Some((connection_id, table_name, _, _)) = viewer_entity_for_events
                    .read_with(cx, |panel, cx| panel.session_state_snapshot(cx))
                {
                    self.persist_open_viewer_state_snapshot(
                        viewer_entity_for_events,
                        connection_id,
                        &table_name,
                        cx,
                    );
                }
            }
            // Discard handling is panel-owned and immediately reverts delegate state.
            TableViewerEvent::DiscardChanges => {}
            // Mark-for-deletion state is managed entirely by the panel delegate.
            TableViewerEvent::MarkRowsForDeletion { .. } => {}
            TableViewerEvent::InlineEditStarted => {
                tracing::debug!("Inline editing started - closing cell editor panel");
                self.cell_editor_panel.update(cx, |editor, cx| {
                    editor.clear(cx);
                });
            }
            TableViewerEvent::MultiLineContentFlattened => {
                tracing::debug!("Multi-line content flattened for inline editing");
            }
            TableViewerEvent::ValidationFailed { message } => {
                window.push_notification(Notification::warning(message.clone()), cx);
            }
            _ => {
                debug_assert!(
                    false,
                    "handle_open_viewer_panel_event called with non-panel event"
                );
            }
        }
    }

    /// Opens a table viewer as a new tab in the center dock.
    pub(in crate::main_view) fn open_table_viewer(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        database_name: Option<String>,
        is_view: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_table_viewer_with_session_state(
            TableViewerSessionOpenRequest {
                connection_id,
                table_name,
                database_name,
                is_view,
                session_state: None,
            },
            window,
            cx,
        );
    }

    pub(in crate::main_view) fn open_table_viewer_with_session_state(
        &mut self,
        request: TableViewerSessionOpenRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let TableViewerSessionOpenRequest {
            connection_id,
            table_name,
            database_name,
            is_view,
            session_state,
        } = request;

        // Verify prerequisites before creating viewer entities/subscriptions so
        // invalid connection entry points fail fast without wiring orphaned UI work.
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!(
                connection_id = %connection_id,
                table_name = %table_name,
                database_name = ?database_name,
                "Cannot open table viewer without AppState"
            );
            return;
        };

        let Some(conn) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!(
                connection_id = %connection_id,
                table_name = %table_name,
                database_name = ?database_name,
                "Cannot open table viewer because connection was not found"
            );
            return;
        };

        let table_service = app_state.table_service.clone();
        let schema_service = app_state.schema_service.clone();
        let connection_service = app_state.connection_service.clone();
        // Keep existing fallback semantics for unknown connection names while
        // making lookup failures observable with structured context.
        let connection_name = match app_state
            .connection_service
            .get_saved_connection(connection_id)
        {
            Ok(saved_connection) => saved_connection.name.clone(),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    connection_id = %connection_id,
                    table_name = %table_name,
                    database_name = ?database_name,
                    "Failed to resolve saved connection name during open-viewer setup"
                );
                "Unknown".to_string()
            }
        };

        let viewer_entity = cx.new(TableViewerPanel::new);
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        // Show loading state immediately so the user sees a spinner instead of "No table selected"
        let initial_request_generation = viewer_entity.update(cx, |panel, cx| {
            panel.set_pending_session_state(session_state.clone(), cx);
            panel.begin_loading_table(connection_id, table_name.clone(), database_name.clone(), cx)
        });

        self.active_table_load_task_generation =
            self.active_table_load_task_generation.saturating_add(1);
        let active_task_generation = self.active_table_load_task_generation;

        // Clone entities needed by the event subscription closure
        let viewer_entity_for_events = viewer_entity.clone();
        let viewer_weak_for_save = viewer_entity.downgrade();
        let viewer_weak_for_edit = viewer_entity.downgrade();
        let cell_editor_panel = self.cell_editor_panel.clone();
        let workspace_controller = self.workspace_controller.clone();
        let schema_details_panel = self.schema_details_panel.clone();
        let results_panel = self.results_panel.clone();
        let inspector_panel = self.inspector_panel.clone();

        let table_viewer_subscription = cx.subscribe_in(&viewer_entity, window, {
            move |_this, _viewer, event: &TableViewerEvent, window, cx| {
                match event {
                    TableViewerEvent::HideColumn { .. }
                    | TableViewerEvent::FreezeColumn { .. }
                    | TableViewerEvent::UnfreezeColumn { .. }
                    | TableViewerEvent::SizeColumnToFit { .. }
                    | TableViewerEvent::SizeAllColumnsToFit
                    | TableViewerEvent::AddQuickFilter { .. }
                    | TableViewerEvent::ColumnVisibilityChanged { .. }
                    | TableViewerEvent::DiscardChanges
                    | TableViewerEvent::MarkRowsForDeletion { .. }
                    | TableViewerEvent::InlineEditStarted
                    | TableViewerEvent::MultiLineContentFlattened
                    | TableViewerEvent::ValidationFailed { .. } => {
                        _this.handle_open_viewer_panel_event(
                            event,
                            &viewer_entity_for_events,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::EditRow {
                        connection_id,
                        table_name,
                        row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                    } => {
                        _this.handle_open_viewer_existing_sql_row_event_from_payload(
                            *connection_id,
                            table_name,
                            column_meta,
                            row_values,
                            *row_index,
                            all_column_names,
                            viewer_weak_for_edit.clone(),
                            OpenViewerExistingSqlRowEventBehavior {
                                focused_column_index: None,
                                reveal_inspector_panel: Some(true),
                                apply_route: OpenViewerExistingSqlRowApplyRoute::Always,
                            },
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::AddRowForm {
                        connection_id,
                        table_name,
                        column_meta,
                        row_values,
                        row_index,
                    } => {
                        _this.handle_open_viewer_add_row_form_event(
                            *connection_id,
                            table_name,
                            column_meta,
                            row_values.as_deref(),
                            *row_index,
                            viewer_weak_for_edit.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::RowSelected {
                        connection_id,
                        table_name,
                        row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                    } => {
                        _this.handle_open_viewer_existing_sql_row_event_from_payload(
                            *connection_id,
                            table_name,
                            column_meta,
                            row_values,
                            *row_index,
                            all_column_names,
                            viewer_weak_for_edit.clone(),
                            OpenViewerExistingSqlRowEventBehavior {
                                focused_column_index: None,
                                reveal_inspector_panel: None,
                                apply_route:
                                    OpenViewerExistingSqlRowApplyRoute::OnlyWhenSqlRowModeActive,
                            },
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::CellSelected {
                        connection_id,
                        table_name,
                        row_index,
                        col_index,
                        row_values,
                        column_meta,
                        all_column_names,
                    } => {
                        _this.handle_open_viewer_existing_sql_row_event_from_payload(
                            *connection_id,
                            table_name,
                            column_meta,
                            row_values,
                            *row_index,
                            all_column_names,
                            viewer_weak_for_edit.clone(),
                            OpenViewerExistingSqlRowEventBehavior {
                                focused_column_index: Some(*col_index),
                                reveal_inspector_panel: Some(false),
                                apply_route: OpenViewerExistingSqlRowApplyRoute::Always,
                            },
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::RefreshTable {
                        connection_id,
                        table_name,
                        driver_category,
                        ..
                    } => {
                        _this.handle_open_viewer_refresh_table_event(
                            *connection_id,
                            table_name,
                            *driver_category,
                            &viewer_entity_for_events,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::AddRow {
                        connection_id,
                        table_name,
                        all_column_names,
                    } => handle_add_row_event(
                        *connection_id,
                        table_name,
                        all_column_names,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LoadDistinctValues {
                        connection_id,
                        table_name,
                        column_name,
                    } => handle_load_distinct_values_event(
                        *connection_id,
                        table_name,
                        column_name,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::PageChanged {
                        connection_id,
                        table_name,
                        page,
                        limit,
                    } => handle_page_changed_event(
                        *connection_id,
                        table_name,
                        *page,
                        *limit,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LimitChanged {
                        connection_id,
                        table_name,
                        limit,
                    } => handle_limit_changed_event(
                        *connection_id,
                        table_name,
                        *limit,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LimitEnabledChanged {
                        connection_id,
                        table_name,
                        enabled,
                    } => handle_limit_enabled_changed_event(
                        *connection_id,
                        table_name,
                        *enabled,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LoadMore { current_offset } => handle_load_more_event(
                        *current_offset,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LoadFkValues {
                        connection_id,
                        referenced_table,
                        referenced_schema,
                        cache_key,
                        referenced_columns,
                        query,
                        limit,
                        request_id,
                    } => handle_load_fk_values_event(
                        *connection_id,
                        referenced_table,
                        referenced_schema.as_deref(),
                        cache_key,
                        referenced_columns,
                        query.as_deref(),
                        *limit,
                        *request_id,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::LastPageRequested {
                        connection_id,
                        table_name,
                    } => handle_last_page_requested_event(
                        *connection_id,
                        table_name,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    ),
                    TableViewerEvent::SaveCell {
                        table_name,
                        connection_id,
                        row,
                        col,
                        column_name,
                        new_value,
                        original_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                    } => {
                        let request = SaveCellRequest {
                            table_name: table_name.clone(),
                            connection_id: *connection_id,
                            row: *row,
                            data_col: *col,
                            column_name: column_name.clone(),
                            new_value: new_value.clone(),
                            original_value: original_value.clone(),
                            all_row_values: all_row_values.clone(),
                            all_column_names: all_column_names.clone(),
                            all_column_types: all_column_types.clone(),
                        };
                        let rendered_value = request.new_value.as_option_string();

                        handle_save_cell_event(
                            request.clone(),
                            viewer_weak_for_save.clone(),
                            viewer_entity_for_events.read(cx).database_name(),
                            window,
                            cx,
                        );

                        let (value_text, is_null) = match rendered_value.as_deref() {
                            None => ("", true),
                            Some(value) => (value, false),
                        };

                        _this.key_value_editor_panel.update(cx, |editor, cx| {
                            if editor.is_editing_row(&request.table_name, request.row) {
                                editor.update_field_value(
                                    request.data_col,
                                    value_text,
                                    is_null,
                                    window,
                                    cx,
                                );
                            }
                        });
                    }
                    TableViewerEvent::EditCell {
                        table_name,
                        connection_id,
                        row,
                        col,
                        column_meta,
                        column_name,
                        column_type,
                        current_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                        raw_bytes,
                    } => {
                        let cell_data = crate::components::CellData {
                            table_name: table_name.clone(),
                            column_name: column_name.clone(),
                            column_type: column_type.clone(),
                            column_meta: column_meta.clone(),
                            current_value: current_value.clone(),
                            row_index: *row,
                            col_index: *col,
                            connection_id: *connection_id,
                            all_row_values: all_row_values.clone(),
                            all_column_names: all_column_names.clone(),
                            all_column_types: all_column_types.clone(),
                            raw_bytes: raw_bytes.clone(),
                        };

                        handle_edit_cell_event(
                            cell_data,
                            viewer_weak_for_edit.clone(),
                            &cell_editor_panel,
                            &workspace_controller,
                            &inspector_panel,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::BecameActive {
                        connection_id,
                        table_name,
                        database_name,
                    } => handle_became_active_event(
                        BecameActiveRequest {
                            connection_id: *connection_id,
                            table_name: table_name.clone(),
                            database_name: database_name.clone(),
                        },
                        schema_details_panel.clone(),
                        results_panel.clone(),
                        &workspace_controller,
                        &inspector_panel,
                        window,
                        cx,
                    ),
                    TableViewerEvent::BecameInactive {
                        connection_id,
                        table_name,
                    } => handle_became_inactive_event(
                        *connection_id,
                        table_name,
                        &schema_details_panel,
                        cx,
                    ),
                    TableViewerEvent::AddRedisKey { connection_id } => {
                        _this.handle_open_viewer_add_redis_key_event(
                            *connection_id,
                            &viewer_entity_for_events,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::NavigateToFkTable {
                        connection_id,
                        referenced_table,
                        database_name,
                    } => {
                        _this.handle_open_viewer_navigate_to_fk_table_event(
                            *connection_id,
                            referenced_table,
                            database_name.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::SaveNewRow {
                        table_name,
                        connection_id,
                        new_row_index,
                        row_data,
                        column_names,
                    } => {
                        handle_save_new_row_event(
                            SaveNewRowRequest {
                                connection_id: *connection_id,
                                table_name: table_name.clone(),
                                new_row_index: *new_row_index,
                                row_data: row_data.clone(),
                                column_names: column_names.clone(),
                            },
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::ApplyFilters {
                        connection_id,
                        table_name,
                        filters,
                        sorts,
                        visible_columns,
                        search_text,
                        search_columns,
                    } => {
                        _this.persist_open_viewer_state_snapshot(
                            &viewer_entity_for_events,
                            *connection_id,
                            table_name,
                            cx,
                        );
                        _this.handle_open_viewer_apply_filters_event(
                            ApplyFiltersRequest {
                                connection_id: *connection_id,
                                table_name: table_name.clone(),
                                filters: filters.clone(),
                                sorts: sorts.clone(),
                                visible_columns: visible_columns.clone(),
                                search_text: search_text.clone(),
                                search_columns: search_columns.clone(),
                            },
                            &viewer_entity_for_events,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::DeleteRows {
                        connection_id,
                        table_name,
                        all_column_names,
                        rows_to_delete,
                    } => {
                        // Since DeleteRows lacks row indices, clear any active editor for
                        // the table so stale cell values cannot survive the mutation.
                        cell_editor_panel.update(cx, |editor, cx| {
                            if editor.is_editing_table(table_name) {
                                tracing::debug!(
                                    "Clearing cell editor - rows being deleted from table {}",
                                    table_name
                                );
                                editor.clear(cx);
                            }
                        });

                        handle_delete_rows_event(
                            *connection_id,
                            table_name,
                            all_column_names,
                            rows_to_delete,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::CommitChanges {
                        connection_id,
                        table_name,
                        modified_cells,
                        deleted_rows,
                        new_rows,
                        column_meta,
                        all_rows,
                    } => {
                        if !deleted_rows.is_empty() {
                            let deleted_indices: Vec<usize> =
                                deleted_rows.iter().copied().collect();
                            cell_editor_panel.update(cx, |editor, cx| {
                                editor.clear_if_editing_rows(table_name, &deleted_indices, cx);
                            });
                        }

                        handle_commit_changes_event(
                            *connection_id,
                            table_name.clone(),
                            modified_cells.clone(),
                            deleted_rows.clone(),
                            new_rows.clone(),
                            column_meta.clone(),
                            all_rows.clone(),
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::GenerateChangesSql {
                        table_name,
                        modified_cells,
                        deleted_rows,
                        new_rows,
                        column_meta,
                        all_rows,
                        ..
                    } => handle_generate_sql_event(
                        table_name.clone(),
                        modified_cells.clone(),
                        deleted_rows.clone(),
                        new_rows.clone(),
                        column_meta.clone(),
                        all_rows.clone(),
                        cx,
                    ),
                    TableViewerEvent::SortColumn {
                        connection_id,
                        table_name,
                        column_name,
                        direction,
                    } => {
                        let (database_name, mut viewer_state) =
                            viewer_entity_for_events.read_with(cx, |panel, cx| {
                                panel
                                    .session_state_snapshot(cx)
                                    .map(|(_, _, database_name, viewer_state)| {
                                        (database_name, viewer_state)
                                    })
                                    .unwrap_or_default()
                            });
                        if viewer_state.sorts.is_empty() {
                            viewer_state.sorts.push(WorkspaceSessionSortCriterion {
                                id: 1,
                                column: column_name.clone(),
                                direction: format!("{:?}", direction),
                            });
                        }
                        _this.workspace_state.update(cx, |state, cx| {
                            state.update_table_viewer_state(
                                *connection_id,
                                table_name,
                                database_name.as_deref(),
                                viewer_state,
                                cx,
                            );
                        });
                        handle_sort_column_event(
                            *connection_id,
                            table_name,
                            column_name,
                            *direction,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::CountCompleted {
                        connection_id,
                        table_name,
                        database_name,
                        total_rows,
                        is_estimated,
                        request_generation,
                        ..
                    } => {
                        viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.update_total_rows(
                                *connection_id,
                                *total_rows,
                                *is_estimated,
                                table_name,
                                database_name.as_deref(),
                                *request_generation,
                                cx,
                            );
                        });
                    }
                }
            }
        });
        self.table_viewer_subscriptions
            .insert(viewer_entity.entity_id(), table_viewer_subscription);

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(table_viewer, window, cx);
        });
        self.workspace_state.update(cx, |state, cx| {
            state.record_open_viewer_tab(
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::Table {
                        table_name: table_name.clone(),
                        database_name: database_name.clone(),
                        is_view,
                        viewer_state: None,
                    },
                },
                cx,
            );
        });

        // Load table data and schema details asynchronously
        let viewer_weak = viewer_entity.downgrade();
        let schema_details_panel_weak = self.schema_details_panel.downgrade();
        let table_name_for_spawn = table_name.clone();
        let database_name_for_spawn = database_name;
        let schema_qualifier = zqlz_core::resolve_schema_qualifier_for_connection(
            conn.as_ref(),
            database_name_for_spawn.as_deref(),
        );

        // Only show a loading spinner in the schema panel when the details are not
        // already warm in the cache — avoids a flicker for repeat opens.
        let details_already_cached = schema_service
            .peek_table_details_cache(connection_id, &table_name, schema_qualifier.as_deref())
            .is_some();
        if !details_already_cached {
            self.schema_details_panel.update(cx, |panel, cx| {
                panel.set_loading_for_table(connection_id, &table_name, cx);
            });
        }

        let task = cx.spawn_in(window, async move |this, cx| {
            tracing::info!(
                connection_id = %connection_id,
                table_name = %table_name_for_spawn,
                request_generation = initial_request_generation,
                task_generation = active_task_generation,
                database_name = ?database_name_for_spawn,
                "Starting open-viewer async load"
            );

            let resolved_connection = match connection_service
                .resolve_database_scoped_connection(connection_id, database_name_for_spawn.clone())
                .await
            {
                Ok(resolved_connection) => resolved_connection,
                Err(error) => {
                    let request_context = OpenViewerAsyncRequestContext::new(
                        connection_id,
                        &table_name_for_spawn,
                        database_name_for_spawn.as_deref(),
                        initial_request_generation,
                        active_task_generation,
                    );
                    tracing::error!(
                        error = %error,
                        connection_id = %request_context.connection_id,
                        table_name = request_context.table_name,
                        database_name = request_context.database_name,
                        request_generation = request_context.request_generation,
                        task_generation = request_context.task_generation,
                        "Failed to resolve database-scoped connection during open-viewer load"
                    );

                    Self::update_open_viewer_if_current_request_with_kind(
                        &viewer_weak,
                        request_context,
                        OpenViewerAsyncUpdateKind::Viewer,
                        cx,
                        |viewer, cx| {
                            viewer.set_loading(false, cx);
                        },
                    );

                    Self::reset_schema_details_loading_after_open_viewer_failure(
                        &viewer_weak,
                        &schema_details_panel_weak,
                        request_context,
                        OpenViewerSchemaDetailsFailurePhase::ConnectionResolution,
                        cx,
                    );

                    Self::clear_active_table_load_task_if_current_generation(
                        &this,
                        request_context,
                        cx,
                        "Table load task cleanup skipped after connection resolution failure because main view dropped",
                    );

                    return anyhow::Ok(());
                }
            };
            let conn = resolved_connection.connection;
            let effective_database_name_for_spawn = resolved_connection.effective_database_name;
            let driver_category = resolved_connection.driver_category;
            let is_key_value = driver_category == zqlz_core::DriverCategory::KeyValue;
            let request_context = OpenViewerAsyncRequestContext::new(
                connection_id,
                &table_name_for_spawn,
                effective_database_name_for_spawn.as_deref(),
                initial_request_generation,
                active_task_generation,
            );

            let load_outcome = table_service
                .load_open_viewer_initial_data(
                    schema_service.clone(),
                    OpenViewerInitialLoadRequest {
                        connection: conn.clone(),
                        connection_id,
                        table_name: table_name_for_spawn.clone(),
                        database_name: database_name_for_spawn.clone(),
                        is_view,
                        limit: Some(1000),
                    },
                )
                .await;

            let browse_result = load_outcome.browse_result;
            let schema_result = load_outcome.schema_result;
            let used_schema_only_fallback = load_outcome.used_schema_only_fallback;
            let schema_qualifier = load_outcome.schema_qualifier;
            // Deliver browse data first so row content appears quickly, then schema metadata.
            let needs_background_count = match browse_result {
                Ok(query_result) => {
                    let needs_background_count = decide_open_viewer_count_workflow(
                        OpenTableViewerCountDecisionRequest {
                            total_rows: query_result.total_rows,
                            is_key_value,
                            supports_fast_exact_count: conn.supports_fast_exact_count(),
                        },
                    )
                    .needs_background_count;

                    let table_name_for_initial_load = table_name_for_spawn.clone();
                    let database_name_for_initial_load =
                        effective_database_name_for_spawn.clone();

                    Self::update_open_viewer_in_if_current_request_with_kind(
                        &viewer_weak,
                        request_context,
                        OpenViewerAsyncUpdateKind::ViewerWindow,
                        cx,
                        move |viewer, window, cx| {
                            viewer.load_table(
                                connection_id,
                                connection_name.to_string(),
                                table_name_for_initial_load.clone(),
                                database_name_for_initial_load.clone(),
                                is_view,
                                query_result,
                                driver_category,
                                window,
                                cx,
                            );
                        },
                    );

                    if used_schema_only_fallback {
                        tracing::warn!(
                            connection_id = %connection_id,
                            table_name = %table_name_for_spawn,
                            request_generation = initial_request_generation,
                            task_generation = active_task_generation,
                            database_name = ?effective_database_name_for_spawn,
                            "Opened table viewer in schema-only mode after browse failure"
                        );
                    }

                    needs_background_count
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        connection_id = %request_context.connection_id,
                        table_name = request_context.table_name,
                        database_name = request_context.database_name,
                        request_generation = request_context.request_generation,
                        task_generation = request_context.task_generation,
                        "Failed initial table browse load during open-viewer flow"
                    );

                    Self::update_open_viewer_if_current_request_with_kind(
                        &viewer_weak,
                        request_context,
                        OpenViewerAsyncUpdateKind::Viewer,
                        cx,
                        |viewer, cx| {
                            viewer.set_loading(false, cx);
                        },
                    );
                    false
                }
            };

            match schema_result {
                Ok(schema_payload) => {
                    let viewer_metadata = build_open_viewer_schema_viewer_metadata(&schema_payload);
                    let table_details = schema_payload.table_details;
                    let create_statement = schema_payload.create_statement;
                    let fk_info_for_viewer = viewer_metadata.foreign_keys_for_viewer;

                    if !fk_info_for_viewer.is_empty() {
                        tracing::info!(
                            connection_id = %connection_id,
                            table_name = %table_name_for_spawn,
                            request_generation = initial_request_generation,
                            task_generation = active_task_generation,
                            database_name = ?effective_database_name_for_spawn,
                            foreign_key_count = fk_info_for_viewer.len(),
                            "Applying open-viewer foreign-key metadata"
                        );
                        Self::update_open_viewer_if_current_request_with_kind(
                            &viewer_weak,
                            request_context,
                            OpenViewerAsyncUpdateKind::Viewer,
                            cx,
                            |viewer, cx| {
                                viewer.set_foreign_keys(fk_info_for_viewer, cx);
                            },
                        );
                    }

                    let schema_columns = viewer_metadata.schema_columns;
                    let pk_columns = viewer_metadata.primary_key_columns;
                    Self::update_open_viewer_if_current_request_with_kind(
                        &viewer_weak,
                        request_context,
                        OpenViewerAsyncUpdateKind::Viewer,
                        cx,
                        |viewer, cx| {
                            viewer.update_column_types_from_schema(&schema_columns, cx);
                            viewer.set_primary_key_columns(pk_columns, cx);
                        },
                    );

                    let details = convert_to_schema_details(
                        connection_id,
                        &table_name_for_spawn,
                        table_details,
                        create_statement,
                    );

                    let schema_details_apply_outcome =
                        Self::update_schema_details_panel_if_current_request(
                            &viewer_weak,
                            &schema_details_panel_weak,
                            request_context,
                            cx,
                            |panel, cx| {
                                panel.set_details(details, cx);
                            },
                        );

                    match schema_details_apply_outcome {
                        SchemaDetailsApplyOutcome::Applied => {
                            tracing::info!(
                                connection_id = %request_context.connection_id,
                                table_name = request_context.table_name,
                                request_generation = request_context.request_generation,
                                task_generation = request_context.task_generation,
                                database_name = ?request_context.database_name,
                                "Schema details loaded during open-viewer flow"
                            );
                        }
                        SchemaDetailsApplyOutcome::SkippedRequestOwnership => {
                            tracing::debug!(
                                connection_id = %request_context.connection_id,
                                table_name = request_context.table_name,
                                request_generation = request_context.request_generation,
                                task_generation = request_context.task_generation,
                                database_name = ?request_context.database_name,
                                "Skipped schema details apply because open-viewer request ownership changed"
                            );
                        }
                        SchemaDetailsApplyOutcome::SkippedPanelUnavailable => {
                            tracing::debug!(
                                connection_id = %request_context.connection_id,
                                table_name = request_context.table_name,
                                request_generation = request_context.request_generation,
                                task_generation = request_context.task_generation,
                                database_name = ?request_context.database_name,
                                "Skipped schema details apply because schema details panel was unavailable"
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        connection_id = %request_context.connection_id,
                        table_name = request_context.table_name,
                        database_name = request_context.database_name,
                        request_generation = request_context.request_generation,
                        task_generation = request_context.task_generation,
                        "Failed schema-details load during open-viewer flow"
                    );

                    Self::reset_schema_details_loading_after_open_viewer_failure(
                        &viewer_weak,
                        &schema_details_panel_weak,
                        request_context,
                        OpenViewerSchemaDetailsFailurePhase::SchemaLoad,
                        cx,
                    );
                }
            }

            // For slow-count drivers, fetch the estimated row count now that
            // both the data and schema have been delivered to the UI.
            if needs_background_count
                && read_open_viewer_request_ownership_if_current(
                    &viewer_weak,
                    request_context,
                    OpenViewerAsyncUpdateKind::BackgroundCount,
                    cx,
                )
                .is_some()
            {
                let table_service = table_service.clone();
                let connection = conn.clone();
                let table_name_for_count = table_name_for_spawn.clone();
                let schema_qualifier = schema_qualifier.clone();
                let count_result = cx
                    .background_spawn(async move {
                        table_service
                            .estimate_row_count(
                                connection,
                                &table_name_for_count,
                                schema_qualifier.as_deref(),
                            )
                            .await
                    })
                    .await;

                match count_result {
                    Ok(Some((total_rows, is_estimated))) => {
                        Self::update_open_viewer_if_current_request_with_kind(
                            &viewer_weak,
                            request_context,
                            OpenViewerAsyncUpdateKind::BackgroundCount,
                            cx,
                            move |_viewer, cx| {
                                cx.emit(TableViewerEvent::CountCompleted {
                                    connection_id: request_context.connection_id,
                                    table_name: request_context.table_name.to_owned(),
                                    database_name: request_context
                                        .database_name
                                        .map(ToOwned::to_owned),
                                    request_generation: request_context.request_generation,
                                    total_rows,
                                    is_estimated,
                                });
                            },
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        log_open_viewer_background_count_failure(request_context, &error);
                    }
                }
            }

            // Clear the stored task reference now that work is done so the
            // Task is dropped and its memory is freed.
            Self::clear_active_table_load_task_if_current_generation(
                &this,
                request_context,
                cx,
                "Table load task finished after main view dropped",
            );

            anyhow::Ok(())
        });

        // Store the task — this automatically cancels any previous stale load.
        self.active_table_load_task = Some(task);
    }

    fn update_open_viewer_if_current_request_with_kind(
        viewer_weak: &WeakEntity<TableViewerPanel>,
        request_context: OpenViewerAsyncRequestContext,
        update_kind: OpenViewerAsyncUpdateKind,
        cx: &mut AsyncWindowContext,
        update: impl FnOnce(&mut TableViewerPanel, &mut Context<TableViewerPanel>),
    ) {
        let mut update = Some(update);
        if let Err(error) = viewer_weak.update(cx, |viewer, cx| {
            if capture_open_viewer_request_ownership_if_current(
                viewer,
                request_context,
                update_kind,
            )
            .is_none()
            {
                return;
            }

            if let Some(update) = update.take() {
                update(viewer, cx);
            }
        }) {
            log_open_viewer_dropped_request_update(
                &error,
                request_context,
                update_kind.dropped_debug_message(),
            );
        }
    }

    /// Applies open-viewer updates that require a Window only when the async
    /// response still owns the active request and viewer identity.
    fn update_open_viewer_in_if_current_request_with_kind(
        viewer_weak: &WeakEntity<TableViewerPanel>,
        request_context: OpenViewerAsyncRequestContext,
        update_kind: OpenViewerAsyncUpdateKind,
        cx: &mut AsyncWindowContext,
        update: impl FnOnce(&mut TableViewerPanel, &mut Window, &mut Context<TableViewerPanel>),
    ) {
        let mut update = Some(update);
        if let Err(error) = viewer_weak.update_in(cx, |viewer, window, cx| {
            if capture_open_viewer_request_ownership_if_current(
                viewer,
                request_context,
                update_kind,
            )
            .is_none()
            {
                return;
            }

            if let Some(update) = update.take() {
                update(viewer, window, cx);
            }
        }) {
            log_open_viewer_dropped_request_update(
                &error,
                request_context,
                update_kind.dropped_debug_message(),
            );
        }
    }

    /// Clears the tracked open-viewer task only when the generation still owns
    /// the slot so superseded task completions cannot cancel newer loads.
    fn clear_active_table_load_task_if_current_generation(
        main_view_weak: &WeakEntity<MainView>,
        request_context: OpenViewerAsyncRequestContext,
        cx: &mut AsyncWindowContext,
        dropped_main_view_debug_message: &'static str,
    ) {
        if let Err(error) = main_view_weak.update(cx, |main_view, _cx| {
            let task_generation = request_context.task_generation;
            if main_view.active_table_load_task_generation == task_generation {
                main_view.active_table_load_task = None;
            } else {
                log_open_viewer_task_cleanup_guard_skip(
                    request_context,
                    main_view.active_table_load_task_generation,
                    "Skipped open-viewer task cleanup because task generation no longer owns active load slot",
                );
            }
        }) {
            log_open_viewer_dropped_request_update(
                &error,
                request_context,
                dropped_main_view_debug_message,
            );
        }
    }

    /// Keeps schema-details updates aligned with the active open-viewer request
    /// so stale async responses cannot overwrite a newer table's metadata.
    fn update_schema_details_panel_if_current_request(
        viewer_weak: &WeakEntity<TableViewerPanel>,
        schema_details_panel_weak: &WeakEntity<SchemaDetailsPanel>,
        request_context: OpenViewerAsyncRequestContext,
        cx: &mut AsyncWindowContext,
        update: impl FnOnce(&mut SchemaDetailsPanel, &mut Context<SchemaDetailsPanel>),
    ) -> SchemaDetailsApplyOutcome {
        let initial_update_kind = OpenViewerAsyncUpdateKind::SchemaDetails;
        let pre_apply_update_kind = OpenViewerAsyncUpdateKind::SchemaDetailsPreApply;
        let dropped_panel_debug_message = pre_apply_update_kind.dropped_panel_debug_message();

        if read_open_viewer_request_ownership_if_current(
            viewer_weak,
            request_context,
            initial_update_kind,
            cx,
        )
        .is_none()
        {
            return SchemaDetailsApplyOutcome::SkippedRequestOwnership;
        }

        // Revalidate right before applying schema-details writes so a fast retarget
        // between the initial guard read and panel mutation cannot overwrite newer state.
        let Some(latest_ownership_snapshot) = read_open_viewer_request_ownership_if_current(
            viewer_weak,
            request_context,
            pre_apply_update_kind,
            cx,
        ) else {
            return SchemaDetailsApplyOutcome::SkippedRequestOwnership;
        };

        if let Err(error) = schema_details_panel_weak.update(cx, |panel, cx| {
            update(panel, cx);
        }) {
            log_open_viewer_dropped_schema_details_panel_update(
                &error,
                request_context,
                &latest_ownership_snapshot,
                dropped_panel_debug_message,
            );
            return SchemaDetailsApplyOutcome::SkippedPanelUnavailable;
        }

        SchemaDetailsApplyOutcome::Applied
    }

    fn reset_schema_details_loading_after_open_viewer_failure(
        viewer_weak: &WeakEntity<TableViewerPanel>,
        schema_details_panel_weak: &WeakEntity<SchemaDetailsPanel>,
        request_context: OpenViewerAsyncRequestContext,
        failure_phase: OpenViewerSchemaDetailsFailurePhase,
        cx: &mut AsyncWindowContext,
    ) {
        let schema_details_loading_reset_outcome =
            Self::update_schema_details_panel_if_current_request(
                viewer_weak,
                schema_details_panel_weak,
                request_context,
                cx,
                |panel, cx| {
                    panel.set_loading(false, cx);
                },
            );

        log_open_viewer_schema_details_loading_reset_outcome(
            request_context,
            failure_phase,
            schema_details_loading_reset_outcome,
        );
    }
}
