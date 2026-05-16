//! This module handles opening Redis databases and keys in the viewer.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::LoadKeyValueDatabaseRowsRequest;

use crate::MainView;
use crate::app::AppState;
use crate::components::{InspectorView, TableViewerEvent, TableViewerPanel};
use crate::main_view::table_handlers::standalone_events::{
    BecameActiveRequest, RedisKeyEditRequest,
};
use crate::workspace_state::{WorkspaceSessionViewerKind, WorkspaceSessionViewerTab};

use super::super::{
    handle_became_active_event, handle_became_inactive_event, handle_delete_redis_keys_event,
    handle_redis_key_edit_event, handle_refresh_table_event,
};

impl MainView {
    pub(in crate::main_view) fn open_redis_key(
        &mut self,
        connection_id: Uuid,
        database_index: u16,
        key_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening Redis key: {} in database {} for connection {}",
            key_name,
            database_index,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(_connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let key_name_clone = key_name.clone();
        let database_name = format!("db{}", database_index);

        cx.spawn_in(window, async move |this, cx| {
            _ = this.update_in(cx, |this, window, cx| {
                this.open_redis_key_viewer(
                    connection_id,
                    database_index,
                    key_name_clone,
                    database_name,
                    window,
                    cx,
                );
            });

            anyhow::Ok(())
        })
        .detach();
    }

    fn open_redis_key_viewer(
        &mut self,
        connection_id: Uuid,
        database_index: u16,
        key_name: String,
        db_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Redis".to_string());
        let connection_service = app_state.connection_service.clone();
        let key_value_service = app_state.key_value_service.clone();

        let viewer_entity = cx.new(TableViewerPanel::new);
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        let schema_details_panel = self.schema_details_panel.clone();
        let results_panel = self.results_panel.clone();
        let inspector_panel = self.inspector_panel.clone();
        let workspace_controller = self.workspace_controller.clone();
        let viewer_entity_for_refresh = viewer_entity.clone();
        let db_name_for_events = db_name.clone();

        cx.subscribe_in(&viewer_entity, window, {
            move |_this, _viewer, event: &TableViewerEvent, window, cx| match event {
                TableViewerEvent::RefreshTable {
                    connection_id,
                    table_name,
                    driver_category,
                    database_name: _,
                } => {
                    handle_refresh_table_event(
                        *connection_id,
                        table_name,
                        *driver_category,
                        viewer_entity_for_refresh.clone(),
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
                TableViewerEvent::HideColumn { column_name } => {
                    _viewer.update(cx, |panel, cx| {
                        panel.hide_column(column_name, cx);
                    });
                }
                TableViewerEvent::FreezeColumn { col_ix } => {
                    _viewer.update(cx, |panel, cx| {
                        panel.freeze_column(*col_ix, cx);
                    });
                }
                TableViewerEvent::UnfreezeColumn { col_ix } => {
                    _viewer.update(cx, |panel, cx| {
                        panel.unfreeze_column(*col_ix, cx);
                    });
                }
                TableViewerEvent::SizeColumnToFit { col_ix } => {
                    _viewer.update(cx, |panel, cx| {
                        panel.size_column_to_fit(*col_ix, cx);
                    });
                }
                TableViewerEvent::SizeAllColumnsToFit => {
                    _viewer.update(cx, |panel, cx| {
                        panel.size_all_columns_to_fit(cx);
                    });
                }
                TableViewerEvent::EditCell { .. }
                | TableViewerEvent::SaveCell { .. }
                | TableViewerEvent::InlineEditStarted
                | TableViewerEvent::ValidationFailed { .. }
                | TableViewerEvent::DiscardChanges
                | TableViewerEvent::ApplyFilters { .. }
                | TableViewerEvent::SortColumn { .. }
                | TableViewerEvent::ColumnVisibilityChanged { .. }
                | TableViewerEvent::MultiLineContentFlattened
                | TableViewerEvent::CellSelected { .. }
                | TableViewerEvent::RowSelected { .. }
                | TableViewerEvent::EditRow { .. }
                | TableViewerEvent::DeleteRows { .. }
                | TableViewerEvent::MarkRowsForDeletion { .. }
                | TableViewerEvent::AddRedisKey { .. }
                | TableViewerEvent::AddRowForm { .. }
                | TableViewerEvent::AddRow { .. }
                | TableViewerEvent::SaveNewRow { .. }
                | TableViewerEvent::CommitChanges { .. }
                | TableViewerEvent::GenerateChangesSql { .. }
                | TableViewerEvent::PageChanged { .. }
                | TableViewerEvent::LimitChanged { .. }
                | TableViewerEvent::LimitEnabledChanged { .. }
                | TableViewerEvent::LoadMore { .. }
                | TableViewerEvent::LoadFkValues { .. }
                | TableViewerEvent::NavigateToFkTable { .. }
                | TableViewerEvent::AddQuickFilter { .. }
                | TableViewerEvent::LastPageRequested { .. }
                | TableViewerEvent::LoadDistinctValues { .. }
                | TableViewerEvent::CountCompleted { .. } => {
                    tracing::debug!(
                        database_name = %db_name_for_events,
                        "Ignoring table event for key-value key viewer"
                    );
                }
            }
        })
        .detach();

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(table_viewer, window, cx);
        });
        self.workspace_state.update(cx, |state, cx| {
            state.record_open_viewer_tab(
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::RedisKey {
                        database_index,
                        key_name: key_name.clone(),
                    },
                },
                cx,
            );
        });

        let viewer_weak = viewer_entity.downgrade();
        let db_name_for_spawn = db_name.clone();
        let key_name_for_spawn = key_name.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let connection = match connection_service
                .get_connection_for_database(connection_id, &db_name_for_spawn)
                .await
            {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database_index,
                        error = %error,
                        "Failed to get key-value database-specific connection"
                    );
                    return anyhow::Ok(());
                }
            };

            let query_result = match key_value_service
                .browse_key(connection, &key_name_for_spawn, Some(1000))
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database_index,
                        key = %key_name_for_spawn,
                        error = %error,
                        "Failed to load key-value key"
                    );
                    return anyhow::Ok(());
                }
            };

            _ = viewer_weak.update_in(cx, |viewer, window, cx| {
                viewer.load_table(
                    connection_id,
                    connection_name,
                    key_name_for_spawn,
                    Some(db_name_for_spawn),
                    true,
                    query_result,
                    DriverCategory::KeyValue,
                    window,
                    cx,
                );
            });

            anyhow::Ok(())
        })
        .detach();
    }

    pub(in crate::main_view) fn open_redis_database(
        &mut self,
        connection_id: Uuid,
        database_index: u16,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening Redis database {} for connection {}",
            database_index,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(_connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let db_name = format!("db{}", database_index);

        cx.spawn_in(window, async move |this, cx| {
            _ = this.update_in(cx, |this, window, cx| {
                this.open_redis_keys_viewer(connection_id, database_index, db_name, window, cx);
            });

            anyhow::Ok(())
        })
        .detach();
    }

    fn open_redis_keys_viewer(
        &mut self,
        connection_id: Uuid,
        database_index: u16,
        db_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Redis".to_string());
        let connection_service = app_state.connection_service.clone();
        let key_value_service = app_state.key_value_service.clone();
        let event_db_name = db_name.clone();

        let viewer_entity = cx.new(TableViewerPanel::new);
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        let key_value_editor_panel = self.key_value_editor_panel.clone();
        let cell_editor_panel = self.cell_editor_panel.clone();
        let workspace_controller = self.workspace_controller.clone();
        let inspector_panel = self.inspector_panel.clone();
        let schema_details_panel = self.schema_details_panel.clone();
        let results_panel = self.results_panel.clone();
        let viewer_entity_for_refresh = viewer_entity.clone();
        let viewer_entity_for_events = viewer_entity.clone();

        cx.subscribe_in(&viewer_entity, window, {
            let db_name = event_db_name.clone();
            move |_this, _viewer, event: &TableViewerEvent, window, cx| match event {
                TableViewerEvent::EditCell {
                    connection_id,
                    all_row_values,
                    all_column_names,
                    ..
                } => {
                    handle_redis_key_edit_event(
                        RedisKeyEditRequest {
                            connection_id: *connection_id,
                            database_name: db_name.clone(),
                            all_row_values: all_row_values.clone(),
                            all_column_names: all_column_names.clone(),
                        },
                        &key_value_editor_panel,
                        &cell_editor_panel,
                        &workspace_controller,
                        &inspector_panel,
                        window,
                        cx,
                    );
                }
                TableViewerEvent::RefreshTable {
                    connection_id,
                    table_name,
                    driver_category,
                    database_name: _,
                } => {
                    handle_refresh_table_event(
                        *connection_id,
                        table_name,
                        *driver_category,
                        viewer_entity_for_refresh.clone(),
                        window,
                        cx,
                    );
                }
                TableViewerEvent::AddRedisKey { connection_id } => {
                    tracing::info!("AddRedisKey event: opening KeyValueEditor for new key");
                    key_value_editor_panel.update(cx, |editor, cx| {
                        editor.new_key(*connection_id, Some(db_name.clone()), window, cx);
                    });

                    inspector_panel.update(cx, |panel, cx| {
                        panel.set_active_view(InspectorView::KeyEditor, cx);
                    });

                    workspace_controller.update(cx, |workspace, cx| {
                        workspace.reveal_panel(
                            "InspectorPanel",
                            zqlz_ui::widgets::dock::DockPlacement::Right,
                            window,
                            cx,
                        );
                    });
                }
                TableViewerEvent::DeleteRows {
                    connection_id,
                    table_name: _,
                    all_column_names,
                    rows_to_delete,
                } => {
                    handle_delete_redis_keys_event(
                        *connection_id,
                        Some(db_name.clone()),
                        all_column_names,
                        rows_to_delete,
                        viewer_entity_for_events.clone(),
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
                TableViewerEvent::HideColumn { column_name } => {
                    viewer_entity_for_events.update(cx, |panel, cx| {
                        panel.hide_column(column_name, cx);
                    });
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
                TableViewerEvent::ApplyFilters { .. }
                | TableViewerEvent::SortColumn { .. }
                | TableViewerEvent::InlineEditStarted
                | TableViewerEvent::MultiLineContentFlattened
                | TableViewerEvent::ValidationFailed { .. }
                | TableViewerEvent::ColumnVisibilityChanged { .. }
                | TableViewerEvent::DiscardChanges
                | TableViewerEvent::MarkRowsForDeletion { .. } => {}
                TableViewerEvent::EditRow { .. }
                | TableViewerEvent::AddRowForm { .. }
                | TableViewerEvent::RowSelected { .. }
                | TableViewerEvent::CellSelected { .. }
                | TableViewerEvent::SaveCell { .. }
                | TableViewerEvent::AddRow { .. }
                | TableViewerEvent::SaveNewRow { .. }
                | TableViewerEvent::CommitChanges { .. }
                | TableViewerEvent::GenerateChangesSql { .. }
                | TableViewerEvent::PageChanged { .. }
                | TableViewerEvent::LimitChanged { .. }
                | TableViewerEvent::LimitEnabledChanged { .. }
                | TableViewerEvent::LoadMore { .. }
                | TableViewerEvent::LoadFkValues { .. }
                | TableViewerEvent::NavigateToFkTable { .. }
                | TableViewerEvent::AddQuickFilter { .. }
                | TableViewerEvent::LastPageRequested { .. }
                | TableViewerEvent::LoadDistinctValues { .. }
                | TableViewerEvent::CountCompleted { .. } => {
                    tracing::debug!("Ignoring SQL-specific event for KeyValue viewer");
                }
            }
        })
        .detach();

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(table_viewer, window, cx);
        });
        self.workspace_state.update(cx, |state, cx| {
            state.record_open_viewer_tab(
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::RedisDatabase { database_index },
                },
                cx,
            );
        });

        let viewer_weak = viewer_entity.downgrade();
        let db_name_for_spawn = db_name.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let connection_clone = match connection_service
                .get_connection_for_database(connection_id, &db_name_for_spawn)
                .await
            {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database = %db_name_for_spawn,
                        error = %error,
                        "Failed to get Redis database-specific connection"
                    );
                    return anyhow::Ok(());
                }
            };

            let query_result = match key_value_service
                .load_database_rows(
                    connection_clone,
                    LoadKeyValueDatabaseRowsRequest { database_index },
                )
                .await
            {
                Ok(outcome) => outcome.query_result,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database_index,
                        error = %error,
                        "Failed to load Redis database rows via table service"
                    );
                    return anyhow::Ok(());
                }
            };

            tracing::info!(
                "Loaded {} keys for Redis database {}",
                query_result.rows.len(),
                database_index
            );

            _ = viewer_weak.update_in(cx, |viewer, window, cx| {
                viewer.load_table(
                    connection_id,
                    connection_name,
                    db_name,
                    Some(db_name_for_spawn),
                    true,
                    query_result,
                    DriverCategory::KeyValue,
                    window,
                    cx,
                );

                if let Some(table_state) = &viewer.table_state {
                    table_state.update(cx, |table, _cx| {
                        table.delegate_mut().set_disable_inline_edit(true);
                    });
                }
            });

            anyhow::Ok(())
        })
        .detach();
    }
}
