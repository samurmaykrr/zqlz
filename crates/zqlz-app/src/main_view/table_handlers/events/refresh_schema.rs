//! Schema refresh functionality
//!
//! This module handles refreshing schema metadata for specific connections,
//! including both Redis databases and traditional SQL database schemas.

use gpui::*;
use uuid::Uuid;

use crate::app::AppState;
use crate::main_view::MainView;

impl MainView {
    /// Refreshes schema for a specific connection
    pub(in crate::main_view) fn refresh_schema(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Refresh schema for connection: {}", connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let connection_sidebar = self.connection_sidebar.clone();
        let is_redis = connection.driver_name() == "redis";

        // Invalidate cache before refreshing so we get fresh data
        schema_service.invalidate_connection_cache(connection_id);

        cx.spawn_in(window, async move |_this, cx| {
            if is_redis {
                // For Redis, use list_databases to get the database list
                if let Some(schema_introspection) = connection.as_schema_introspection() {
                    match schema_introspection.list_databases().await {
                        Ok(databases) => {
                            // Convert DatabaseInfo to (index, key_count) tuples
                            let redis_dbs: Vec<(u16, Option<i64>)> = databases
                                .iter()
                                .filter_map(|db| {
                                    // Parse "db0", "db1", etc. to get the index
                                    db.name
                                        .strip_prefix("db")
                                        .and_then(|s| s.parse::<u16>().ok())
                                        .map(|index| (index, db.size_bytes))
                                })
                                .collect();

                            tracing::info!("Redis schema refreshed: {} databases", redis_dbs.len());

                            _ = connection_sidebar.update_in(cx, |sidebar, _window, cx| {
                                sidebar.set_redis_databases(connection_id, redis_dbs, cx);
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to list Redis databases: {}", e);
                        }
                    }
                } else {
                    tracing::error!("Redis connection does not support schema introspection");
                }
            } else {
                // For non-Redis databases, use the standard schema loading
                match schema_service
                    .load_database_schema(connection.clone(), connection_id)
                    .await
                {
                    Ok(schema) => {
                        tracing::info!(
                            "Schema refreshed: {} tables, {} views",
                            schema.tables.len(),
                            schema.views.len()
                        );

                        let active_db = schema.database_name.clone();

                        _ = connection_sidebar.update_in(cx, |sidebar, _window, cx| {
                            sidebar.set_schema(
                                connection_id,
                                schema.tables,
                                schema.views,
                                schema.materialized_views,
                                schema.triggers,
                                schema.functions,
                                schema.procedures,
                                schema.schema_name,
                                cx,
                            );
                        });

                        // Refresh the database list for multi-database display
                        if let Some(schema_introspection) = connection.as_schema_introspection() {
                            match schema_introspection.list_databases().await {
                                Ok(databases) => {
                                    let dbs: Vec<(String, Option<i64>)> = databases
                                        .iter()
                                        .map(|db| (db.name.clone(), db.size_bytes))
                                        .collect();
                                    _ = connection_sidebar.update_in(cx, |sidebar, _window, cx| {
                                        sidebar.set_databases(
                                            connection_id,
                                            dbs,
                                            active_db.as_deref(),
                                            cx,
                                        );
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to list databases on refresh: {}", e);
                                }
                            }
                        }

                        tracing::info!("Schema refreshed");
                    }
                    Err(e) => {
                        tracing::error!("Failed to refresh schema: {}", e);
                    }
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }
}
