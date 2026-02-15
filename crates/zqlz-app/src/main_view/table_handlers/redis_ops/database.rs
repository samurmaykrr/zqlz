//! This module handles opening Redis databases and keys in the viewer.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::DriverCategory;

use crate::app::AppState;
use crate::components::{InspectorView, TableViewerEvent, TableViewerPanel};
use crate::main_view::table_handlers_utils::formatting::{format_bytes, format_ttl_seconds};
use crate::MainView;

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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let key_name_clone = key_name.clone();

        cx.spawn_in(window, async move |this, cx| {
            let select_cmd = format!("SELECT {}", database_index);
            if let Err(e) = connection.execute(&select_cmd, &[]).await {
                tracing::error!("Failed to select Redis database {}: {}", database_index, e);
                return anyhow::Ok(());
            }

            _ = this.update_in(cx, |this, window, cx| {
                this.open_table_viewer(connection_id, key_name_clone, None, false, window, cx);
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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let db_name = format!("db{}", database_index);

        cx.spawn_in(window, async move |this, cx| {
            let select_cmd = format!("SELECT {}", database_index);
            if let Err(e) = connection.execute(&select_cmd, &[]).await {
                tracing::error!("Failed to select Redis database {}: {}", database_index, e);
                return anyhow::Ok(());
            }

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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection_name = app_state
            .connection_manager()
            .get_saved(connection_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "Redis".to_string());

        let viewer_entity = cx.new(|cx| TableViewerPanel::new(cx));
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        let key_value_editor_panel = self.key_value_editor_panel.clone();
        let dock_area = self.dock_area.clone();
        let inspector_panel = self.inspector_panel.clone();
        let schema_details_panel = self.schema_details_panel.clone();
        let results_panel = self.results_panel.clone();
        let viewer_entity_for_refresh = viewer_entity.clone();
        let viewer_entity_for_events = viewer_entity.clone();

        cx.subscribe_in(&viewer_entity, window, {
            move |_this, _viewer, event: &TableViewerEvent, window, cx| {
                match event {
                    TableViewerEvent::EditCell {
                        connection_id,
                        all_row_values,
                        all_column_names,
                        ..
                    } => {
                        handle_redis_key_edit_event(
                            *connection_id,
                            all_row_values,
                            all_column_names,
                            &key_value_editor_panel,
                            &dock_area,
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
                            editor.new_key(*connection_id, window, cx);
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
                    TableViewerEvent::DeleteRows {
                        connection_id,
                        table_name: _,
                        all_column_names,
                        rows_to_delete,
                    } => {
                        handle_delete_redis_keys_event(
                            *connection_id,
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
                    TableViewerEvent::HideColumn { column_name } => {
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.hide_column(column_name, cx);
                        });
                    }
                    TableViewerEvent::FreezeColumn { col_ix } => {
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.freeze_column(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::UnfreezeColumn { col_ix } => {
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.unfreeze_column(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::SizeColumnToFit { col_ix } => {
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.size_column_to_fit(*col_ix, cx);
                        });
                    }
                    TableViewerEvent::SizeAllColumnsToFit => {
                        _ = viewer_entity_for_events.update(cx, |panel, cx| {
                            panel.size_all_columns_to_fit(cx);
                        });
                    }
                    TableViewerEvent::ApplyFilters { .. }
                    | TableViewerEvent::SortColumn { .. }
                    | TableViewerEvent::InlineEditStarted
                    | TableViewerEvent::MultiLineContentFlattened
                    | TableViewerEvent::ColumnVisibilityChanged { .. }
                    | TableViewerEvent::DiscardChanges
                    | TableViewerEvent::SetToNull { .. }
                    | TableViewerEvent::SetToEmpty { .. }
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
                    | TableViewerEvent::LastPageRequested { .. } => {
                        tracing::debug!("Ignoring SQL-specific event for KeyValue viewer");
                    }
                }
            }
        })
        .detach();

        self.dock_area.update(cx, |dock_area, cx| {
            dock_area.add_panel(
                table_viewer,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });

        let viewer_weak = viewer_entity.downgrade();
        let connection_clone = connection.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let select_cmd = format!("SELECT {}", database_index);
            if let Err(e) = connection_clone.execute(&select_cmd, &[]).await {
                tracing::error!("Failed to select Redis database {}: {}", database_index, e);
                return anyhow::Ok(());
            }

            let table_infos =
                if let Some(schema_introspection) = connection_clone.as_schema_introspection() {
                    match schema_introspection.list_tables(None).await {
                        Ok(tables) => tables,
                        Err(e) => {
                            tracing::error!("Failed to list Redis keys: {}", e);
                            return anyhow::Ok(());
                        }
                    }
                } else {
                    tracing::error!("Redis connection does not support schema introspection");
                    return anyhow::Ok(());
                };

            tracing::info!(
                "Loaded {} keys for Redis database {}",
                table_infos.len(),
                database_index
            );

            let columns = vec![
                zqlz_core::ColumnMeta {
                    name: "Key".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    ordinal: 0,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Redis key name".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Type".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    ordinal: 1,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Redis data type".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Value".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 2,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Value preview".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Size".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 3,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Memory size".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "TTL".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 4,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Time to live".to_string()),
                    enum_values: None,
                },
            ];

            let column_names = vec![
                "Key".to_string(),
                "Type".to_string(),
                "Value".to_string(),
                "Size".to_string(),
                "TTL".to_string(),
            ];

            let rows: Vec<zqlz_core::Row> = table_infos
                .iter()
                .map(|info| {
                    let key_value_info = info.key_value_info.as_ref();

                    let key = zqlz_core::Value::String(info.name.clone());

                    let key_type = key_value_info
                        .map(|kv| zqlz_core::Value::String(kv.key_type.clone()))
                        .unwrap_or(zqlz_core::Value::Null);

                    let value = key_value_info
                        .and_then(|kv| kv.value_preview.as_ref())
                        .map(|v| zqlz_core::Value::String(v.clone()))
                        .unwrap_or(zqlz_core::Value::Null);

                    let size = key_value_info
                        .and_then(|kv| kv.size_bytes)
                        .map(|bytes| zqlz_core::Value::String(format_bytes(bytes)))
                        .unwrap_or(zqlz_core::Value::Null);

                    let ttl = key_value_info
                        .and_then(|kv| kv.ttl_seconds)
                        .map(|ttl| {
                            zqlz_core::Value::String(if ttl == -1 {
                                "No TTL".to_string()
                            } else if ttl == -2 {
                                "Not Found".to_string()
                            } else {
                                format_ttl_seconds(ttl)
                            })
                        })
                        .unwrap_or(zqlz_core::Value::String("No TTL".to_string()));

                    zqlz_core::Row::new(column_names.clone(), vec![key, key_type, value, size, ttl])
                })
                .collect();

            let total_rows = rows.len();

            let query_result = zqlz_core::QueryResult {
                id: uuid::Uuid::new_v4(),
                columns,
                rows,
                total_rows: Some(total_rows as u64),
                is_estimated_total: false,
                affected_rows: 0,
                execution_time_ms: 0,
                warnings: vec![],
            };

            _ = viewer_weak.update_in(cx, |viewer, window, cx| {
                viewer.load_table(
                    connection_id,
                    connection_name,
                    db_name,
                    None,
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
