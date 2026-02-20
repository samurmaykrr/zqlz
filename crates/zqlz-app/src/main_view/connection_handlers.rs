// Connection management methods for MainView

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme, DatabaseLogo, WindowExt, button::ButtonVariant, dialog::DialogButtonProps,
    notification::Notification, v_flex,
};

use crate::app::AppState;
use crate::components::ConnectionEntry;
use zqlz_connection::SavedQueryInfo;
use zqlz_core::{DriverCategory, ObjectsPanelData};

use super::MainView;

/// Map driver name to DriverCategory
fn driver_name_to_category(driver_name: &str) -> DriverCategory {
    match driver_name.to_lowercase().as_str() {
        "redis" | "memcached" | "valkey" | "keydb" | "dragonfly" => DriverCategory::KeyValue,
        "mongodb" | "couchdb" | "couchbase" => DriverCategory::Document,
        "influxdb" | "timescaledb" | "questdb" => DriverCategory::TimeSeries,
        "neo4j" | "arangodb" | "janusgraph" => DriverCategory::Graph,
        "elasticsearch" | "opensearch" | "meilisearch" => DriverCategory::Search,
        _ => DriverCategory::Relational,
    }
}

/// Database type definition with all metadata for the selection grid
#[derive(Clone, Debug)]
pub struct DatabaseType {
    /// Internal driver identifier
    pub id: &'static str,
    /// Display name
    pub name: &'static str,
    /// Logo to display (PNG-based for colored logos)
    pub logo: DatabaseLogo,
    /// Whether this database type is currently supported/implemented
    pub supported: bool,
}

impl DatabaseType {
    /// Returns all available database types for the connection dialog.
    /// Only includes database types whose drivers are actually compiled in.
    pub fn all() -> Vec<Self> {
        use zqlz_drivers::DriverRegistry;
        
        let registry = DriverRegistry::with_defaults();
        
        // Define all possible database types
        let all_types = vec![
            // SQL databases
            DatabaseType {
                id: "sqlite",
                name: "SQLite",
                logo: DatabaseLogo::SQLite,
                supported: true,
            },
            DatabaseType {
                id: "postgres",
                name: "PostgreSQL",
                logo: DatabaseLogo::PostgreSQL,
                supported: true,
            },
            DatabaseType {
                id: "mysql",
                name: "MySQL",
                logo: DatabaseLogo::MySQL,
                supported: true,
            },
            DatabaseType {
                id: "mariadb",
                name: "MariaDB",
                logo: DatabaseLogo::MariaDB,
                supported: true,
            },
            DatabaseType {
                id: "mssql",
                name: "SQL Server",
                logo: DatabaseLogo::MsSql,
                supported: true,
            },
            DatabaseType {
                id: "duckdb",
                name: "DuckDB",
                logo: DatabaseLogo::DuckDB,
                supported: true,
            },
            // NoSQL databases
            DatabaseType {
                id: "redis",
                name: "Redis",
                logo: DatabaseLogo::Redis,
                supported: true,
            },
            DatabaseType {
                id: "mongodb",
                name: "MongoDB",
                logo: DatabaseLogo::MongoDB,
                supported: true,
            },
            DatabaseType {
                id: "clickhouse",
                name: "ClickHouse",
                logo: DatabaseLogo::ClickHouse,
                supported: true,
            },
        ];
        
        // Filter to only include types where the driver is actually available
        all_types
            .into_iter()
            .filter(|db| {
                // MariaDB uses the mysql driver
                let driver_id = if db.id == "mariadb" { "mysql" } else { db.id };
                registry.has(driver_id)
            })
            .collect()
    }

    /// Filter database types by search query
    pub fn filter(query: &str) -> Vec<Self> {
        let query = query.to_lowercase();
        Self::all()
            .into_iter()
            .filter(|db| db.name.to_lowercase().contains(&query) || db.id.contains(&query))
            .collect()
    }
}

impl MainView {
    /// Connect to a database
    pub(super) fn connect_to_database(
        &mut self,
        id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let saved = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == id);

        let Some(saved) = saved else {
            tracing::error!("Connection not found: {}", id);
            return;
        };

        // Use ConnectionService for connecting and loading schema
        let connection_service = app_state.connection_service.clone();
        let schema_service = app_state.schema_service.clone();
        let connections = app_state.connections.clone();
        let storage = app_state.storage.clone();
        let sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let schema_details_panel = self.schema_details_panel.downgrade();
        let workspace_state = self.workspace_state.downgrade();
        let driver_type = saved.driver.clone(); // Capture driver type for LSP
        let connection_name = saved.name.clone(); // Capture connection name for UI
        // Known up-front from the saved config; used to pre-populate the sidebar
        // with the active database node before any async queries complete.
        let active_db_from_config = saved.params.get("database").cloned();

        // Set connecting state immediately
        workspace_state
            .update(cx, |state, _cx| {
                state.set_connecting(id, true);
            })
            .ok();

        // Update sidebar to show connecting state
        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connecting(id, true, cx);
        });

        cx.spawn_in(window, async move |this, cx| {
            tracing::info!("Fast connecting to: {} (driver: {})", connection_name, driver_type);

            // Step 1: Connect immediately without waiting for schema
            match connection_service.connect_fast(&saved).await {
                Ok(conn_info) => {
                    let conn_id = conn_info.id;
                    tracing::info!("Connected successfully (fast): {}", conn_id);

                    // Get connection handle for further use
                    let Some(conn) = connections.get(conn_id) else {
                        tracing::error!("Connection not found after establishment: {}", conn_id);
                        return;
                    };

                    // Step 2: Immediately show connected state in UI
                    _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                        sidebar.set_connected(conn_id, true, cx);
                        // Mark all sections as loading immediately so headers appear with
                        // spinners before any schema queries complete.
                        sidebar.set_all_sections_loading(conn_id, cx);
                    });

                    // Clear connecting state
                    _ = workspace_state.update(cx, |state, cx| {
                        state.set_connecting(conn_id, false);
                        state.set_connection_status(conn_id, true, cx);
                        state.set_active_connection(Some(conn_id), cx);
                    });

                    // Update AppState with active connection (legacy - will be removed)
                    _ = cx.update(|_window, cx| {
                        if let Some(app_state) = cx.try_global::<AppState>() {
                            app_state.set_active_connection(Some(conn_id));
                        }
                        anyhow::Ok(())
                    });

                    // Update all existing QueryEditor panels with the new connection
                    _ = this.update(cx, |main_view, cx| {
                        let mut updated_count = 0;
                        let mut dead_editors = Vec::new();
                        
                        for (i, weak_editor) in main_view.query_editors.iter().enumerate() {
                            if let Some(editor) = weak_editor.upgrade() {
                                editor.update(cx, |ed, cx| {
                                    ed.set_connection(Some(conn_id), Some(connection_name.clone()), Some(conn.clone()), Some(driver_type.clone()), cx);
                                    updated_count += 1;
                                });
                            } else {
                                dead_editors.push(i);
                            }
                        }
                        
                        for i in dead_editors.iter().rev() {
                            main_view.query_editors.swap_remove(*i);
                        }
                        
                        tracing::info!("Updated {} QueryEditor panels with connection", updated_count);
                    });

                    // Load saved queries immediately (fast, local operation)
                    match storage.load_queries_for_connection(conn_id) {
                        Ok(queries) => {
                            let saved_queries: Vec<SavedQueryInfo> = queries
                                .into_iter()
                                .map(|q| SavedQueryInfo {
                                    id: q.id,
                                    name: q.name,
                                })
                                .collect();
                            
                            if !saved_queries.is_empty() {
                                tracing::info!("Loaded {} saved queries for connection {}", saved_queries.len(), conn_id);
                                _ = sidebar.update(cx, |sidebar, cx| {
                                    sidebar.set_saved_queries(conn_id, saved_queries, cx);
                                });
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to load saved queries for connection {}: {}", conn_id, e);
                        }
                    }

                    // Step 3: Load schema in background (slow operation)
                    tracing::info!("Starting background schema load for connection {}", conn_id);
                    
                    if driver_type == "redis" {
                        // For Redis, load databases list
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

                                    tracing::info!("Redis databases loaded: {} databases", redis_dbs.len());

                                    let conn_name = connection_name.clone();
                                    let redis_dbs_for_panel = redis_dbs.clone();
                                    _ = objects_panel.update(cx, |panel, cx| {
                                        panel.load_redis_databases(conn_id, conn_name, redis_dbs_for_panel, cx);
                                    });

                                    _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                                        sidebar.set_redis_databases(conn_id, redis_dbs, cx);
                                    });
                                }
                                Err(e) => {
                                    tracing::error!("Failed to list Redis databases: {}", e);
                                    _ = sidebar.update_in(cx, |_sidebar, window, cx| {
                                        window.push_notification(
                                            Notification::warning(format!(
                                                "Connected but failed to list databases: {}",
                                                e
                                            )),
                                            cx,
                                        );
                                    });
                                }
                            }
                        }
                    } else {
                        // For SQL databases, load schema progressively
                        tracing::info!("Starting progressive schema load");

                        // Clear schema details panel when switching connections
                        _ = schema_details_panel.update(cx, |panel, cx| {
                            panel.clear_if_not_connection(conn_id, cx);
                        });

                        // Show the database hierarchy immediately using the database name
                        // from the saved connection config, so the multi-DB structure is
                        // visible before any async queries complete. This prevents the
                        // jarring flat-table-list → database-nodes restructuring.
                        if let Some(ref db_name) = active_db_from_config {
                            _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                                sidebar.init_database_view(conn_id, db_name, cx);
                            });
                        }

                        // Fetch the full database list concurrently with the table load.
                        // Spawned as a detached foreground task so that a slow
                        // pg_database_size() on serverless Postgres (Neon etc.) does not
                        // delay the tables section from appearing in the sidebar.
                        {
                            let sidebar = sidebar.clone();
                            let active_db_from_config = active_db_from_config.clone();
                            let conn_for_dbs = conn.clone();
                            cx.spawn(async move |cx| {
                                if let Some(schema_introspection) = conn_for_dbs.as_schema_introspection() {
                                    match schema_introspection.list_databases().await {
                                        Ok(databases) => {
                                            let dbs: Vec<(String, Option<i64>)> = databases
                                                .iter()
                                                .map(|db| (db.name.clone(), db.size_bytes))
                                                .collect();

                                            _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                                                sidebar.merge_databases(
                                                    conn_id,
                                                    dbs,
                                                    active_db_from_config.as_deref(),
                                                    cx,
                                                );
                                            });
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to list databases: {}", e);
                                        }
                                    }
                                }
                            })
                            .detach();
                        }

                        // Load tables first — they are the most important objects in the sidebar.
                        match schema_service.load_tables_only(conn.clone(), conn_id).await {
                            Ok(tables) => {
                                tracing::info!("Loaded {} tables", tables.len());

                                let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();

                                // Clear the tables spinner and the database-level loading
                                // indicator now that we have actual data.
                                _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                                    sidebar.set_tables_only(conn_id, table_names.clone(), cx);
                                    if let Some(ref db_name) = active_db_from_config {
                                        sidebar.set_database_loading(conn_id, db_name, false, cx);
                                    }
                                });

                                // Update objects panel with basic table list
                                let conn_name = connection_name.clone();
                                let driver_category = driver_name_to_category(&driver_type);
                                let objects_data = ObjectsPanelData::from_table_infos(tables.clone());
                                _ = objects_panel.update(cx, |panel, cx| {
                                    panel.load_objects(conn_id, conn_name, None, objects_data, driver_category, cx);
                                });

                                // Push table names into every open QueryEditor's LSP cache
                                // immediately, so FROM-clause completions work as soon as the
                                // sidebar shows tables — without waiting for the slower
                                // per-table column-detail fetches to finish.
                                _ = this.update(cx, |main_view, cx| {
                                    for weak_editor in &main_view.query_editors {
                                        if let Some(editor) = weak_editor.upgrade() {
                                            editor.update(cx, |ed, cx| {
                                                ed.notify_tables_available(table_names.clone(), cx);
                                            });
                                        }
                                    }
                                });

                                // Load remaining schema sections sequentially to avoid
                                // waker contention on the shared Postgres mutex. Each
                                // result clears its own spinner as soon as it arrives.
                                match schema_service.load_views(conn.clone(), conn_id).await {
                                    Ok(v) => {
                                        let names: Vec<String> = v.into_iter().map(|x| x.name).collect();
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.set_views_only(conn_id, names, cx);
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to load views: {}", e);
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.clear_section_loading(conn_id, "views", cx);
                                        });
                                    }
                                }

                                match schema_service.load_materialized_views(conn.clone(), conn_id).await {
                                    Ok(v) => {
                                        let names: Vec<String> = v.into_iter().map(|x| x.name).collect();
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.set_materialized_views_only(conn_id, names, cx);
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to load materialized views: {}", e);
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.clear_section_loading(conn_id, "materialized_views", cx);
                                        });
                                    }
                                }

                                match schema_service.load_functions(conn.clone(), conn_id).await {
                                    Ok(v) => {
                                        let names: Vec<String> = v.into_iter().map(|x| x.name).collect();
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.set_functions_only(conn_id, names, cx);
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to load functions: {}", e);
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.clear_section_loading(conn_id, "functions", cx);
                                        });
                                    }
                                }

                                match schema_service.load_procedures(conn.clone(), conn_id).await {
                                    Ok(v) => {
                                        let names: Vec<String> = v.into_iter().map(|x| x.name).collect();
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.set_procedures_only(conn_id, names, cx);
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to load procedures: {}", e);
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.clear_section_loading(conn_id, "procedures", cx);
                                        });
                                    }
                                }

                                match schema_service.load_triggers(conn.clone(), conn_id).await {
                                    Ok(v) => {
                                        let names: Vec<String> = v.into_iter().map(|x| x.name).collect();
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.set_triggers_only(conn_id, names, cx);
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to load triggers: {}", e);
                                        _ = sidebar.update_in(cx, |s, _window, cx| {
                                            s.clear_section_loading(conn_id, "triggers", cx);
                                        });
                                    }
                                }

                                tracing::info!("Sequential schema load complete");

                                // Pre-warm the table details cache and then signal every open
                                // editor to re-fetch its LSP schema cache, so column completions
                                // become available as soon as the per-table detail round-trips
                                // finish (rather than never, which was the fire-and-forget case).
                                let introspection_schema = schema_service
                                    .get_introspection_schema_cached(&conn, conn_id)
                                    .await;
                                cx.background_spawn({
                                    let schema_service = schema_service.clone();
                                    let conn = conn.clone();
                                    async move {
                                        schema_service
                                            .prefetch_all_table_details(
                                                conn,
                                                conn_id,
                                                table_names,
                                                introspection_schema,
                                            )
                                            .await;
                                    }
                                })
                                .await;

                                tracing::info!("Table details pre-warmed; triggering LSP schema refresh for open editors");
                                _ = this.update(cx, |main_view, cx| {
                                    for weak_editor in &main_view.query_editors {
                                        if let Some(editor) = weak_editor.upgrade() {
                                            editor.update(cx, |ed, cx| {
                                                ed.trigger_lsp_schema_refresh(cx);
                                            });
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!("Failed to load tables: {}", e);
                                _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                                    sidebar.clear_section_loading(conn_id, "tables", cx);
                                    if let Some(ref db_name) = active_db_from_config {
                                        sidebar.set_database_loading(conn_id, db_name, false, cx);
                                    }
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect: {}", e);
                    
                    // Clear connecting state
                    _ = workspace_state.update(cx, |state, _cx| {
                        state.set_connecting(id, false);
                    });
                    
                    // Clear connecting state in sidebar
                    _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                        sidebar.set_connecting(id, false, cx);
                    });
                    
                    // Show notification to user
                    _ = this.update_in(cx, |_this, window, cx| {
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to connect to '{}': {}",
                                connection_name, e
                            )),
                            cx,
                        );
                    });
                }
            }
        })
        .detach();
    }

    /// Load schema for a specific database without disconnecting or switching the main connection.
    ///
    /// For MySQL/ClickHouse: reuses the existing connection's SchemaIntrospection
    /// with the database name as the schema parameter, since information_schema
    /// supports cross-database queries.
    ///
    /// For PostgreSQL/MSSQL: creates a temporary connection to the target database,
    /// loads schema, then closes it — the main connection stays untouched.
    pub(super) fn load_database_schema(
        &mut self,
        connection_id: Uuid,
        database_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available for load_database_schema");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("No active connection {} for load_database_schema", connection_id);
            return;
        };

        let saved = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id);

        let Some(saved) = saved else {
            tracing::error!("Saved connection not found: {}", connection_id);
            return;
        };

        let driver_type = saved.driver.clone();
        let sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();

        tracing::info!(
            "Loading schema for database '{}' on connection {} (driver: {})",
            database_name,
            connection_id,
            driver_type
        );

        // Mark the database as loading in the sidebar
        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_database_loading(connection_id, &database_name, true, cx);
        });

        cx.spawn_in(window, async move |_this, cx| {
            let result = match driver_type.as_str() {
                // MySQL and ClickHouse can query any database's schema via
                // information_schema on the existing connection
                "mysql" | "mariadb" | "clickhouse" => {
                    Self::load_schema_via_existing_connection(
                        &connection,
                        &database_name,
                    )
                    .await
                }
                // PostgreSQL, MSSQL, and others need a temporary connection
                // to the target database
                _ => {
                    Self::load_schema_via_temp_connection(
                        &saved,
                        &database_name,
                    )
                    .await
                }
            };

            match result {
                Ok((tables, views, materialized_views, triggers, functions, procedures, schema_name, objects_panel_data)) => {
                    tracing::info!(
                        "Loaded schema for '{}': {} tables, {} views, {} mat_views, {} triggers, {} functions, {} procedures",
                        database_name,
                        tables.len(),
                        views.len(),
                        materialized_views.len(),
                        triggers.len(),
                        functions.len(),
                        procedures.len(),
                    );

                    // Update the Objects Panel with extended data for this database
                    if let Some(data) = objects_panel_data {
                        let conn_name = database_name.clone();
                        let db_name = Some(database_name.clone());
                        let driver_category = driver_name_to_category(&driver_type);
                        _ = objects_panel.update(cx, |panel, cx| {
                            panel.load_objects(connection_id, conn_name, db_name, data, driver_category, cx);
                        });
                    }

                    _ = sidebar.update_in(cx, |sidebar, _window, cx| {
                        sidebar.set_database_schema(
                            connection_id,
                            &database_name,
                            tables,
                            views,
                            materialized_views,
                            triggers,
                            functions,
                            procedures,
                            schema_name,
                            cx,
                        );
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load schema for '{}': {}", database_name, e);

                    _ = sidebar.update_in(cx, |sidebar, window, cx| {
                        sidebar.set_database_loading(connection_id, &database_name, false, cx);
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to load schema for '{}': {}",
                                database_name, e
                            )),
                            cx,
                        );
                    });
                }
            }
        })
        .detach();
    }

    /// Load schema objects using the existing connection's SchemaIntrospection,
    /// passing the database name as the schema parameter.
    /// Works for MySQL/ClickHouse where information_schema spans all databases.
    async fn load_schema_via_existing_connection(
        connection: &std::sync::Arc<dyn zqlz_core::Connection>,
        database_name: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Option<String>, Option<ObjectsPanelData>)> {
        let introspection = connection
            .as_schema_introspection()
            .ok_or_else(|| anyhow::anyhow!("Connection does not support schema introspection"))?;

        let schema_param = Some(database_name);

        // All schema queries are independent reads — fire them all concurrently.
        let (tables_result, views_result, mat_views_result, triggers_result, functions_result, procedures_result, extended_result) = futures::join!(
            introspection.list_tables(schema_param),
            introspection.list_views(schema_param),
            introspection.list_materialized_views(schema_param),
            introspection.list_triggers(schema_param, None),
            introspection.list_functions(schema_param),
            introspection.list_procedures(schema_param),
            introspection.list_tables_extended(schema_param),
        );

        let tables = tables_result?.into_iter().map(|t| t.name).collect();
        let views = views_result?.into_iter().map(|v| v.name).collect();
        let materialized_views = mat_views_result?.into_iter().map(|v| v.name).collect();
        let triggers = triggers_result?.into_iter().map(|t| t.name).collect();
        let functions = functions_result?.into_iter().map(|f| f.name).collect();
        let procedures = procedures_result?.into_iter().map(|p| p.name).collect();
        let objects_panel_data = extended_result.ok();

        Ok((tables, views, materialized_views, triggers, functions, procedures, Some(database_name.to_string()), objects_panel_data))
    }

    /// Load schema by creating a temporary connection to the target database.
    /// Required for PostgreSQL where each connection is scoped to a single database.
    async fn load_schema_via_temp_connection(
        saved: &zqlz_connection::SavedConnection,
        database_name: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Option<String>, Option<ObjectsPanelData>)> {
        let registry = zqlz_drivers::DriverRegistry::with_defaults();
        let driver = registry
            .get(&saved.driver)
            .ok_or_else(|| anyhow::anyhow!("Driver '{}' not found", saved.driver))?;

        let mut config = zqlz_core::ConnectionConfig::new(&saved.driver, &saved.name);
        for (key, value) in &saved.params {
            config = config.with_param(key, value.clone());
        }
        config = config.with_param("database", database_name);

        let temp_conn = driver.connect(&config).await?;

        let introspection = temp_conn
            .as_schema_introspection()
            .ok_or_else(|| anyhow::anyhow!("Temp connection does not support schema introspection"))?;

        let schema_name = introspection
            .list_schemas()
            .await
            .ok()
            .and_then(|schemas| {
                schemas
                    .into_iter()
                    .find(|s| s.name == "public")
                    .map(|s| s.name)
            });

        let schema_param = schema_name.as_deref();
        let is_postgres = saved.driver == "postgres";

        // All schema queries are independent reads — fire them all concurrently.
        let (tables_result, views_result, mat_views_result, triggers_result, functions_result, procedures_result, extended_result) = futures::join!(
            introspection.list_tables(schema_param),
            introspection.list_views(schema_param),
            introspection.list_materialized_views(schema_param),
            async {
                // PostgreSQL triggers are table-level objects, not top-level sidebar items
                if is_postgres {
                    Ok(Vec::new())
                } else {
                    introspection.list_triggers(schema_param, None).await
                }
            },
            introspection.list_functions(schema_param),
            introspection.list_procedures(schema_param),
            introspection.list_tables_extended(schema_param),
        );

        let tables = tables_result?.into_iter().map(|t| t.name).collect();
        let views = views_result?.into_iter().map(|v| v.name).collect();
        let materialized_views = mat_views_result?.into_iter().map(|v| v.name).collect();
        let triggers = triggers_result?.into_iter().map(|t| t.name).collect();
        let functions = functions_result?.into_iter().map(|f| f.name).collect();
        let procedures = procedures_result?.into_iter().map(|p| p.name).collect();
        let objects_panel_data = extended_result.ok();

        if let Err(e) = temp_conn.close().await {
            tracing::warn!("Failed to close temp connection to '{}': {}", database_name, e);
        }

        Ok((tables, views, materialized_views, triggers, functions, procedures, schema_name, objects_panel_data))
    }

    /// Disconnect from a database
    pub(super) fn disconnect_from_database(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let connection_service = app_state.connection_service.clone();
        let sidebar = self.connection_sidebar.downgrade();
        let schema_details_panel = self.schema_details_panel.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let workspace_state = self.workspace_state.downgrade();

        cx.spawn(async move |_this, cx| {
            tracing::info!("Disconnecting: {}", id);

            // Use ConnectionService which handles both disconnection and cache invalidation
            if let Err(e) = connection_service.disconnect(id).await {
                tracing::error!("Failed to disconnect: {}", e);
                // Error is already logged by the service layer
            } else {
                tracing::info!("Disconnected successfully: {}", id);
            }

            // Update WorkspaceState with disconnection (new centralized state)
            _ = workspace_state.update(cx, |state, cx| {
                state.set_connection_status(id, false, cx);
            });

            _ = sidebar.update(cx, |sidebar, cx| {
                sidebar.set_connected(id, false, cx);
            });

            // Clear schema details if it was showing details from the disconnected connection
            _ = schema_details_panel.update(cx, |panel, cx| {
                if panel.active_connection() == Some(id) {
                    panel.clear(cx);
                }
            });

            // Clear objects panel if it was showing objects from the disconnected connection
            _ = objects_panel.update(cx, |panel, cx| {
                if panel.selected_connection_id() == Some(id) {
                    panel.clear(cx);
                }
            });
        })
        .detach();
    }

    /// Delete a connection
    pub(super) fn delete_connection(
        &mut self,
        id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let saved = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == id);

        let Some(saved) = saved else {
            tracing::error!("Connection not found: {}", id);
            return;
        };

        let name = saved.name.clone();
        let sidebar = self.connection_sidebar.downgrade();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let sidebar = sidebar.clone();
            dialog
                .title("Delete Connection")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete connection '{}'?",
                            name
                        )))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _, cx| {
                    if let Some(app_state) = cx.try_global::<AppState>() {
                        app_state.delete_connection(id);
                    }

                    _ = sidebar.update(cx, |sidebar, cx| {
                        sidebar.remove_connection(id, cx);
                    });

                    tracing::info!("Connection deleted: {}", id);
                    true
                })
                .confirm()
        });
    }

    /// Duplicate a connection
    pub(super) fn duplicate_connection(
        &mut self,
        id: Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let saved = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == id);

        let Some(saved) = saved else {
            tracing::error!("Connection not found: {}", id);
            return;
        };

        let mut new_connection = saved.clone();
        new_connection.id = Uuid::new_v4();
        new_connection.name = format!("{} (Copy)", new_connection.name);

        app_state.save_connection(new_connection.clone());

        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.add_connection(
                ConnectionEntry::new(
                    new_connection.id,
                    new_connection.name.clone(),
                    new_connection.driver.clone(),
                ),
                cx,
            );
        });

        tracing::info!("Connection duplicated: {} -> {}", id, new_connection.id);
    }

    /// Open connection settings in a window (uses the same window as new connections)
    pub(super) fn open_connection_settings(
        &mut self,
        id: Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let saved = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == id);

        let Some(saved) = saved else {
            tracing::error!("Connection not found: {}", id);
            return;
        };

        // Open the connection window in edit mode
        super::connection_window::ConnectionWindow::open_for_edit(saved, cx);
    }

    /// Open the new connection window
    pub(super) fn open_new_connection_dialog(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Opening new connection window");
        super::connection_window::ConnectionWindow::open(cx);
    }

    /// Fetch a single sidebar section on demand (lazy loading).
    ///
    /// Called when the user first expands a section that has never been loaded.
    /// The `section` string must match one of the arms below and the keys used in
    /// `clear_section_loading`.
    pub(super) fn load_sidebar_section(
        &mut self,
        connection_id: Uuid,
        section: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            // Connection was removed between the expand click and this handler running.
            self.connection_sidebar.update(cx, |sidebar, cx| {
                sidebar.clear_section_loading(connection_id, section, cx);
            });
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let sidebar = self.connection_sidebar.downgrade();

        cx.spawn_in(window, async move |_this, cx| {
            match section {
                "views" => {
                    match schema_service.load_views(connection, connection_id).await {
                        Ok(views) => {
                            let names: Vec<String> = views.into_iter().map(|v| v.name).collect();
                            tracing::info!("Lazy-loaded {} views", names.len());
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.set_views_only(connection_id, names, cx);
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to lazy-load views: {}", e);
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.clear_section_loading(connection_id, "views", cx);
                            });
                        }
                    }
                }
                "materialized_views" => {
                    match schema_service
                        .load_materialized_views(connection, connection_id)
                        .await
                    {
                        Ok(views) => {
                            let names: Vec<String> = views.into_iter().map(|v| v.name).collect();
                            tracing::info!("Lazy-loaded {} materialized views", names.len());
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.set_materialized_views_only(connection_id, names, cx);
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to lazy-load materialized views: {}", e);
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.clear_section_loading(connection_id, "materialized_views", cx);
                            });
                        }
                    }
                }
                "functions" => {
                    match schema_service.load_functions(connection, connection_id).await {
                        Ok(functions) => {
                            let names: Vec<String> =
                                functions.into_iter().map(|f| f.name).collect();
                            tracing::info!("Lazy-loaded {} functions", names.len());
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.set_functions_only(connection_id, names, cx);
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to lazy-load functions: {}", e);
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.clear_section_loading(connection_id, "functions", cx);
                            });
                        }
                    }
                }
                "procedures" => {
                    match schema_service.load_procedures(connection, connection_id).await {
                        Ok(procedures) => {
                            let names: Vec<String> =
                                procedures.into_iter().map(|p| p.name).collect();
                            tracing::info!("Lazy-loaded {} procedures", names.len());
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.set_procedures_only(connection_id, names, cx);
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to lazy-load procedures: {}", e);
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.clear_section_loading(connection_id, "procedures", cx);
                            });
                        }
                    }
                }
                "triggers" => {
                    match schema_service.load_triggers(connection, connection_id).await {
                        Ok(triggers) => {
                            let names: Vec<String> =
                                triggers.into_iter().map(|t| t.name).collect();
                            tracing::info!("Lazy-loaded {} triggers", names.len());
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.set_triggers_only(connection_id, names, cx);
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to lazy-load triggers: {}", e);
                            _ = sidebar.update_in(cx, |s, _window, cx| {
                                s.clear_section_loading(connection_id, "triggers", cx);
                            });
                        }
                    }
                }
                _ => {
                    tracing::warn!("load_sidebar_section: unknown section '{}'", section);
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }
}
