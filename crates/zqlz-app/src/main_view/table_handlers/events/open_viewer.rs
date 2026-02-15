//! Table viewer opening and initialization
//!
//! This module handles opening table viewers as new tabs with full event subscription setup.
//! It manages the complex async operations of loading table data, schema details, and
//! maintaining consistency between the UI panels.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppState;
use crate::components::{
    InspectorView, RowData, TableViewerEvent, TableViewerPanel,
};

use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::{
    conversion::{convert_to_schema_details, driver_name_to_category, resolve_schema_qualifier},
    generate_ddl_for_table,
};

use super::super::standalone_events::{
    handle_add_row_event, handle_apply_filters_event, handle_became_active_event,
    handle_became_inactive_event, handle_commit_changes_event, handle_delete_rows_event,
    handle_edit_cell_event, handle_generate_sql_event, handle_last_page_requested_event,
    handle_limit_changed_event, handle_limit_enabled_changed_event, handle_load_fk_values_event,
    handle_load_more_event, handle_page_changed_event, handle_refresh_table_event,
    handle_save_cell_event, handle_save_new_row_event, handle_sort_column_event,
};

impl MainView {
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
        let viewer_entity = cx.new(|cx| TableViewerPanel::new(cx));
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        // Show loading state immediately so the user sees a spinner instead of "No table selected"
        viewer_entity.update(cx, |panel, cx| {
            panel.begin_loading_table(table_name.clone(), cx);
        });

        // Clone entities needed by the event subscription closure
        let cell_editor_panel = self.cell_editor_panel.clone();
        let key_value_editor_panel = self.key_value_editor_panel.clone();
        let dock_area = self.dock_area.clone();
        let schema_details_panel = self.schema_details_panel.clone();
        let results_panel = self.results_panel.clone();
        let inspector_panel = self.inspector_panel.clone();
        let viewer_entity_for_events = viewer_entity.clone();
        let viewer_weak_for_edit = viewer_entity.downgrade();

        // Subscribe to table viewer events
        cx.subscribe_in(&viewer_entity, window, {
            let viewer_weak_for_save = viewer_entity.downgrade();
            move |_this, _viewer, event: &TableViewerEvent, window, cx| {
                match event {
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
                        handle_save_cell_event(
                            table_name,
                            *connection_id,
                            *row,
                            *col,
                            column_name,
                            new_value,
                            original_value,
                            all_row_values,
                            all_column_names,
                            all_column_types,
                            viewer_weak_for_save.clone(),
                            viewer_entity_for_events.read(cx).database_name(),
                            window,
                            cx,
                        );

                        // Sync inline cell edit to the row editor if it's showing this row.
                        // col includes the row-number column at index 0, so data_col = col - 1.
                        if *col > 0 {
                            let data_col = *col - 1;
                            let is_null = new_value == "NULL";
                            key_value_editor_panel.update(cx, |editor, cx| {
                                if editor.is_editing_row(table_name, *row) {
                                    editor.update_field_value(
                                        data_col, new_value, is_null, window, cx,
                                    );
                                }
                            });
                        }
                    }
                    TableViewerEvent::EditCell {
                        table_name,
                        connection_id,
                        row,
                        col,
                        column_name,
                        column_type,
                        current_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                        raw_bytes,
                    } => {
                        handle_edit_cell_event(
                            table_name,
                            *connection_id,
                            *row,
                            *col,
                            column_name,
                            column_type,
                            current_value,
                            all_row_values,
                            all_column_names,
                            all_column_types,
                            raw_bytes.clone(),
                            viewer_weak_for_edit.clone(),
                            &cell_editor_panel,
                            &dock_area,
                            &inspector_panel,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::BecameActive {
                        connection_id,
                        table_name,
                        database_name,
                    } => {
                        handle_became_active_event(
                            *connection_id,
                            table_name,
                            database_name.as_deref(),
                            schema_details_panel.clone(),
                            results_panel.clone(),
                            &dock_area,
                            &inspector_panel,
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::BecameInactive {
                        connection_id,
                        table_name,
                    } => {
                        handle_became_inactive_event(
                            *connection_id,
                            table_name,
                            &schema_details_panel,
                            cx,
                        );
                    }
                    TableViewerEvent::RefreshTable {
                        connection_id,
                        table_name,
                        driver_category,
                        database_name: _,
                    } => {
                        // Clear cell editor — row indices become stale after refresh
                        _ = cell_editor_panel.update(cx, |editor, cx| {
                            editor.clear(cx);
                        });
                        handle_refresh_table_event(
                            *connection_id,
                            table_name,
                            *driver_category,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::AddRow {
                        connection_id,
                        table_name,
                        all_column_names,
                    } => {
                        handle_add_row_event(
                            *connection_id,
                            table_name,
                            all_column_names,
                            viewer_entity_for_events.clone(),
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
                            *connection_id,
                            table_name,
                            *new_row_index,
                            row_data,
                            column_names,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::AddRedisKey { connection_id } => {
                        tracing::info!("AddRedisKey event: opening KeyValueEditor for new key");
                        // Open the KeyValueEditor in "new key" mode
                        key_value_editor_panel.update(cx, |editor, cx| {
                            editor.new_key(*connection_id, window, cx);
                        });

                        // Activate Key Editor in Inspector
                        inspector_panel.update(cx, |panel, cx| {
                            panel.set_active_view(InspectorView::KeyEditor, cx);
                        });

                        // Ensure InspectorPanel is visible
                        dock_area.update(cx, |area, cx| {
                            area.activate_panel(
                                "InspectorPanel",
                                zqlz_ui::widgets::dock::DockPlacement::Right,
                                window,
                                cx,
                            );
                        });
                    }
                    TableViewerEvent::DeleteRows {
                        connection_id,
                        table_name,
                        all_column_names,
                        rows_to_delete,
                    } => {
                        // Clear cell editor if it's editing a cell in the table being modified
                        // Since DeleteRows doesn't have row indices, we clear if editing any cell
                        // in this table to prevent showing stale data from deleted rows
                        _ = cell_editor_panel.update(cx, |editor, cx| {
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
                    TableViewerEvent::InlineEditStarted => {
                        tracing::debug!("Inline editing started - closing cell editor panel");
                        _ = cell_editor_panel.update(cx, |editor, cx| {
                            editor.clear(cx);
                        });
                    }
                    TableViewerEvent::MultiLineContentFlattened => {
                        tracing::debug!("Multi-line content flattened for inline editing");
                    }
                    TableViewerEvent::ApplyFilters {
                        connection_id,
                        table_name,
                        filters,
                        sorts,
                        visible_columns,
                        search_text,
                    } => {
                        // Clear cell editor — row indices become stale after filter change
                        _ = cell_editor_panel.update(cx, |editor, cx| {
                            editor.clear(cx);
                        });
                        handle_apply_filters_event(
                            *connection_id,
                            table_name,
                            filters,
                            sorts,
                            visible_columns,
                            search_text,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::SortColumn {
                        connection_id,
                        table_name,
                        column_name,
                        direction,
                    } => {
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
                    TableViewerEvent::ColumnVisibilityChanged { .. } => {
                        // Future: update table view without reloading data
                    }
                    TableViewerEvent::HideColumn { column_name } => {
                        // Hide column is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.hide_column(column_name, cx);
                        });
                    }
                    TableViewerEvent::FreezeColumn { col_ix } => {
                        // Freeze column is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.freeze_column(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::UnfreezeColumn { col_ix } => {
                        // Unfreeze column is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.unfreeze_column(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::SizeColumnToFit { col_ix } => {
                        // Size column to fit is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.size_column_to_fit(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::SizeAllColumnsToFit => {
                        // Size all columns to fit is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.size_all_columns_to_fit(cx);
                        });
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
                        // Clear cell editor if it's editing a cell in a row being deleted
                        if !deleted_rows.is_empty() {
                            let deleted_indices: Vec<usize> =
                                deleted_rows.iter().copied().collect();
                            _ = cell_editor_panel.update(cx, |editor, cx| {
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
                    TableViewerEvent::DiscardChanges => {
                        // Discard is handled locally in the panel
                        // (already reverts the data in the delegate)
                    }
                    TableViewerEvent::GenerateChangesSql {
                        connection_id: _,
                        table_name,
                        modified_cells,
                        deleted_rows,
                        new_rows,
                        column_meta,
                        all_rows,
                    } => {
                        handle_generate_sql_event(
                            table_name.clone(),
                            modified_cells.clone(),
                            deleted_rows.clone(),
                            new_rows.clone(),
                            column_meta.clone(),
                            all_rows.clone(),
                            cx,
                        );
                    }
                    TableViewerEvent::SetToNull { .. } | TableViewerEvent::SetToEmpty { .. } => {
                        // These are handled via SaveCell events
                    }
                    TableViewerEvent::AddQuickFilter { column_name, value } => {
                        // Add quick filter is handled directly by the panel
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.add_quick_filter(column_name.clone(), value.clone(), window, cx);
                        });
                    }
                    TableViewerEvent::PageChanged {
                        connection_id,
                        table_name,
                        page,
                        limit,
                    } => {
                        handle_page_changed_event(
                            *connection_id,
                            table_name,
                            *page,
                            *limit,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::LimitChanged {
                        connection_id,
                        table_name,
                        limit,
                    } => {
                        handle_limit_changed_event(
                            *connection_id,
                            table_name,
                            *limit,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::LimitEnabledChanged {
                        connection_id,
                        table_name,
                        enabled,
                    } => {
                        handle_limit_enabled_changed_event(
                            *connection_id,
                            table_name,
                            *enabled,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::LoadMore { current_offset } => {
                        handle_load_more_event(
                            *current_offset,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::LoadFkValues {
                        connection_id,
                        referenced_table,
                        referenced_columns,
                    } => {
                        handle_load_fk_values_event(
                            *connection_id,
                            referenced_table,
                            referenced_columns,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::NavigateToFkTable {
                        connection_id,
                        referenced_table,
                        database_name,
                    } => {
                        // Navigate to the referenced table by opening it in a new tab
                        tracing::info!(
                            "Navigating to FK referenced table: {} (connection: {})",
                            referenced_table,
                            connection_id
                        );
                        // Open the referenced table using MainView's open_table_viewer method
                        _this.open_table_viewer(
                            *connection_id,
                            referenced_table.clone(),
                            database_name.clone(),
                            false, // FK targets are always tables, not views
                            window,
                            cx,
                        );
                    }
                    TableViewerEvent::LastPageRequested {
                        connection_id,
                        table_name,
                    } => {
                        handle_last_page_requested_event(
                            *connection_id,
                            table_name,
                            viewer_entity_for_events.clone(),
                            window,
                            cx,
                        );
                    }
                    // This event is handled internally by the TableViewerPanel itself
                    // (marks rows for deletion in edit-and-commit mode). MainView doesn't
                    // need to do anything here.
                    TableViewerEvent::MarkRowsForDeletion { .. } => {}
                    TableViewerEvent::EditRow {
                        connection_id,
                        table_name,
                        row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                    } => {
                        let data = RowData {
                            table_name: table_name.clone(),
                            connection_id: *connection_id,
                            column_meta: column_meta.clone(),
                            row_values: row_values.clone(),
                            row_index: Some(*row_index),
                            is_new: false,
                            source_viewer: Some(viewer_weak_for_edit.clone()),
                            all_column_names: all_column_names.clone(),
                        };

                        key_value_editor_panel.update(cx, |editor, cx| {
                            editor.edit_row(data, window, cx);
                        });

                        inspector_panel.update(cx, |panel, cx| {
                            panel.set_active_view(InspectorView::KeyEditor, cx);
                        });

                        dock_area.update(cx, |area, cx| {
                            area.activate_panel(
                                "InspectorPanel",
                                zqlz_ui::widgets::dock::DockPlacement::Right,
                                window,
                                cx,
                            );
                        });
                    }
                    TableViewerEvent::AddRowForm {
                        connection_id,
                        table_name,
                        column_meta,
                    } => {
                        key_value_editor_panel.update(cx, |editor, cx| {
                            editor.new_row(
                                table_name.clone(),
                                *connection_id,
                                column_meta.clone(),
                                Some(viewer_weak_for_edit.clone()),
                                window,
                                cx,
                            );
                        });

                        inspector_panel.update(cx, |panel, cx| {
                            panel.set_active_view(InspectorView::KeyEditor, cx);
                        });

                        dock_area.update(cx, |area, cx| {
                            area.activate_panel(
                                "InspectorPanel",
                                zqlz_ui::widgets::dock::DockPlacement::Right,
                                window,
                                cx,
                            );
                        });
                    }
                    TableViewerEvent::RowSelected {
                        connection_id,
                        table_name,
                        row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                    } => {
                        let is_key_editor_active = inspector_panel
                            .read(cx)
                            .active_view()
                            == InspectorView::KeyEditor;
                        let is_sql_row_mode = key_value_editor_panel
                            .read(cx)
                            .mode()
                            == &crate::components::RowEditorMode::SqlRow;

                        if is_key_editor_active && is_sql_row_mode {
                            let data = RowData {
                                table_name: table_name.clone(),
                                connection_id: *connection_id,
                                column_meta: column_meta.clone(),
                                row_values: row_values.clone(),
                                row_index: Some(*row_index),
                                is_new: false,
                                source_viewer: Some(viewer_weak_for_edit.clone()),
                                all_column_names: all_column_names.clone(),
                            };

                            key_value_editor_panel.update(cx, |editor, cx| {
                                editor.edit_row(data, window, cx);
                            });
                        }
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
                        // For SQL tables, always open the Key Editor (Row Editor) when a cell is clicked.
                        // This allows users to edit the entire row in a dedicated panel.
                        let data = RowData {
                            table_name: table_name.clone(),
                            connection_id: *connection_id,
                            column_meta: column_meta.clone(),
                            row_values: row_values.clone(),
                            row_index: Some(*row_index),
                            is_new: false,
                            source_viewer: Some(viewer_weak_for_edit.clone()),
                            all_column_names: all_column_names.clone(),
                        };

                        // Load the row into the key editor
                        key_value_editor_panel.update(cx, |editor, cx| {
                            editor.edit_row(data, window, cx);
                            editor.focus_field(*col_index, window, cx);
                        });

                        // Activate the Key Editor panel in the inspector
                        inspector_panel.update(cx, |panel, cx| {
                            panel.set_active_view(InspectorView::KeyEditor, cx);
                        });
                    }
                }
            }
        })
        .detach();

        // Get app state and connection for initial data load
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(conn) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let table_service = app_state.table_service.clone();
        let schema_service = app_state.schema_service.clone();
        // Get connection name for tab title
        let connection_name = app_state
            .connection_manager()
            .get_saved(connection_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "Unknown".to_string());

        // Add the panel to the dock
        self.dock_area.update(cx, |dock_area, cx| {
            dock_area.add_panel(
                table_viewer,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });

        // Load table data and schema details asynchronously
        let viewer_weak = viewer_entity.downgrade();
        let schema_details_panel = self.schema_details_panel.clone();
        let table_name_for_spawn = table_name.clone();
        let database_name_for_spawn = database_name;
        let schema_qualifier = resolve_schema_qualifier(conn.driver_name(), &database_name_for_spawn);

        cx.spawn_in(window, async move |_this, cx| {
            tracing::info!("Loading table data: {}", table_name_for_spawn);

            // Establish identity early so concurrent BecameActive events see this
            // table is already being loaded and skip their redundant fetch.
            {
                let table_name = table_name_for_spawn.clone();
                _ = schema_details_panel.update(cx, |panel, cx| {
                    panel.set_loading_for_table(connection_id, &table_name, cx);
                });
            }

            // Load table data - use different method for Redis
            let is_redis = conn.driver_name() == "redis";
            let driver_category = driver_name_to_category(conn.driver_name());
            let browse_result = if is_redis {
                table_service
                    .browse_redis_key(conn.clone(), &table_name_for_spawn, Some(1000))
                    .await
            } else {
                table_service
                    .browse_table(conn.clone(), &table_name_for_spawn, schema_qualifier.as_deref(), Some(1000), None)
                    .await
            };

            match browse_result {
                Ok(query_result) => {
                    let conn_name = connection_name.clone();
                    _ = viewer_weak.update_in(cx, |viewer, window, cx| {
                        viewer.load_table(
                            connection_id,
                            conn_name,
                            table_name_for_spawn.clone(),
                            database_name_for_spawn.clone(),
                            is_view,
                            query_result,
                            driver_category,
                            window,
                            cx,
                        );
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load table data: {}", e);
                    _ = viewer_weak.update(cx, |viewer, cx| {
                        viewer.set_loading(false, cx);
                    });
                }
            }

            // Load schema details
            tracing::info!(
                "Fetching schema details for table: {}",
                table_name_for_spawn
            );

            match schema_service
                .get_table_details(conn.clone(), connection_id, &table_name_for_spawn, schema_qualifier.as_deref())
                .await
            {
                Ok(table_details) => {
                    // Pass foreign key info to the table viewer for FK dropdown editing
                    let fk_info_for_viewer: Vec<zqlz_core::ForeignKeyInfo> = table_details
                        .foreign_keys
                        .iter()
                        .map(|fk| zqlz_core::ForeignKeyInfo {
                            name: fk.name.clone(),
                            columns: fk.columns.clone(),
                            referenced_table: fk.referenced_table.clone(),
                            referenced_schema: fk.referenced_schema.clone(),
                            referenced_columns: fk.referenced_columns.clone(),
                            on_update: fk.on_update,
                            on_delete: fk.on_delete,
                        })
                        .collect();

                    if !fk_info_for_viewer.is_empty() {
                        tracing::info!(
                            "Setting {} foreign keys on table viewer for {}",
                            fk_info_for_viewer.len(),
                            table_name_for_spawn
                        );
                        _ = viewer_weak.update(cx, |viewer, cx| {
                            viewer.set_foreign_keys(fk_info_for_viewer, cx);
                        });
                    }

                    // Update column types from schema info
                    // This fixes SQLite columns that report TEXT instead of their declared types (e.g., DATE)
                    let schema_columns = table_details.columns.clone();
                    let pk_columns = table_details.primary_key_columns.clone();
                    _ = viewer_weak.update(cx, |viewer, cx| {
                        viewer.update_column_types_from_schema(&schema_columns, cx);
                        viewer.set_primary_key_columns(pk_columns);
                    });

                    let create_statement =
                        generate_ddl_for_table(&conn, &table_name_for_spawn).await;
                    let details = convert_to_schema_details(
                        connection_id,
                        &table_name_for_spawn,
                        table_details,
                        create_statement,
                    );

                    _ = schema_details_panel.update(cx, |panel, cx| {
                        panel.set_details(details, cx);
                    });

                    tracing::info!("Schema details loaded for table: {}", table_name_for_spawn);
                }
                Err(e) => {
                    tracing::error!("Failed to load schema details: {}", e);
                    _ = schema_details_panel.update(cx, |panel, cx| {
                        panel.set_loading(false, cx);
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }
}
