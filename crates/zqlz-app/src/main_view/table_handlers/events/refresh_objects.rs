//! Objects panel refresh functionality
//!
//! This module handles refreshing the objects panel when connections or databases change,
//! maintaining consistency between the UI panels and the current connection state.

use gpui::*;
use zqlz_core::ObjectsPanelData;

use crate::app::AppState;
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::conversion::driver_name_to_category;

impl MainView {
    /// Refresh the objects panel by reloading tables from the active connection
    ///
    /// Uses WorkspaceState as the source of truth for active connection.
    pub(in crate::main_view) fn refresh_objects_panel(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        // Get active connection from WorkspaceState (source of truth)
        let conn_id = self.workspace_state.read(cx).active_connection_id();

        let Some(conn_id) = conn_id else {
            tracing::debug!("refresh_objects_panel: no active connection");
            return;
        };

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("refresh_objects_panel: no AppState available");
            return;
        };

        let connections = app_state.connections.clone();
        let schema_service = app_state.schema_service.clone();
        let objects_panel = self.objects_panel.clone();

        // Get connection name from saved connections
        let connection_name = app_state
            .connection_manager()
            .get_saved(conn_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "Unknown".to_string());

        // Check if the connection is active and determine driver type
        let Some(conn) = connections.get(conn_id) else {
            tracing::debug!("refresh_objects_panel: connection {} not active", conn_id);
            return;
        };

        let driver_name = conn.driver_name().to_string();
        let driver_category = driver_name_to_category(&driver_name);
        let is_redis = driver_name.to_lowercase() == "redis";

        // Preserve the current database context so refresh doesn't lose it
        let current_database_name = objects_panel.read(cx).database_name();

        cx.spawn(async move |_main_view, cx| {
            if let Some(conn) = connections.get(conn_id) {
                if is_redis {
                    // Redis: load databases using schema introspection
                    if let Some(schema_introspection) = conn.as_schema_introspection() {
                        match schema_introspection.list_databases().await {
                            Ok(databases) => {
                                let redis_dbs: Vec<(u16, Option<i64>)> = databases
                                    .iter()
                                    .filter_map(|db| {
                                        db.name
                                            .strip_prefix("db")
                                            .and_then(|s| s.parse::<u16>().ok())
                                            .map(|index| (index, db.size_bytes))
                                    })
                                    .collect();

                                tracing::info!(
                                    "Redis databases refreshed: {} databases",
                                    redis_dbs.len()
                                );

                                _ = objects_panel.update(cx, |panel, cx| {
                                    panel.load_redis_databases(
                                        conn_id,
                                        connection_name,
                                        redis_dbs,
                                        cx,
                                    );
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to refresh Redis databases: {}", e);
                            }
                        }
                    } else {
                        tracing::error!("Redis connection does not support schema introspection");
                    }
                } else {
                    // Non-Redis: use standard schema loading
                    match schema_service.load_database_schema(conn, conn_id).await {
                        Ok(schema) => {
                            let objects_data = schema.objects_panel_data
                                .unwrap_or_else(|| ObjectsPanelData::from_table_infos(schema.table_infos));

                            _ = objects_panel.update(cx, |panel, cx| {
                                panel.load_objects(
                                    conn_id,
                                    connection_name,
                                    current_database_name.clone(),
                                    objects_data,
                                    driver_category,
                                    cx,
                                );
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to refresh objects panel: {}", e);
                        }
                    }
                }
            }
        })
        .detach();
    }
}
