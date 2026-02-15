//! Connection state management methods for ConnectionSidebar

use gpui::Context;
use uuid::Uuid;

use crate::widgets::sidebar::types::*;
use crate::widgets::sidebar::ConnectionSidebar;

impl ConnectionSidebar {
    /// Set connections from external source (e.g., AppState)
    /// This decouples the sidebar from the app's global state.
    pub fn set_connections(&mut self, connections: Vec<ConnectionEntry>, cx: &mut Context<Self>) {
        self.connections = connections;
        tracing::info!("Set {} connections", self.connections.len());
        cx.notify();
    }

    /// Add a new connection to the sidebar
    pub fn add_connection(&mut self, entry: ConnectionEntry, cx: &mut Context<Self>) {
        self.connections.push(entry);
        cx.notify();
    }

    /// Remove a connection from the sidebar
    pub fn remove_connection(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.connections.retain(|c| c.id != id);
        if self.selected_connection == Some(id) {
            self.selected_connection = None;
        }
        cx.notify();
    }

    /// Update a connection's connected state
    pub fn set_connected(&mut self, id: Uuid, connected: bool, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.is_connected = connected;
            conn.is_connecting = false; // Clear connecting state when connection status changes
            if !connected {
                conn.is_expanded = false;
                conn.tables.clear();
                conn.views.clear();
                conn.materialized_views.clear();
                conn.triggers.clear();
                conn.functions.clear();
                conn.procedures.clear();
                conn.redis_databases.clear();
                conn.databases.clear();
                conn.schema_name = None;
            }
        }
        cx.notify();
    }

    /// Set whether a connection is currently connecting
    pub fn set_connecting(&mut self, id: Uuid, connecting: bool, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.is_connecting = connecting;
        }
        cx.notify();
    }

    /// Update tables only (progressive loading - step 1)
    pub fn set_tables_only(&mut self, id: Uuid, tables: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.tables = tables;
            conn.is_expanded = true;
            conn.schema_expanded = true;
            conn.tables_expanded = true;
        }
        cx.notify();
    }

    /// Update views only (progressive loading - step 2)
    pub fn set_views_only(&mut self, id: Uuid, views: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.views = views;
            conn.views_expanded = !conn.views.is_empty();
        }
        cx.notify();
    }

    /// Update materialized views only (progressive loading - step 3)
    pub fn set_materialized_views_only(
        &mut self,
        id: Uuid,
        materialized_views: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.materialized_views = materialized_views;
            conn.materialized_views_expanded = !conn.materialized_views.is_empty();
        }
        cx.notify();
    }

    /// Update functions only (progressive loading - step 4)
    pub fn set_functions_only(&mut self, id: Uuid, functions: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.functions = functions;
            conn.functions_expanded = !conn.functions.is_empty();
        }
        cx.notify();
    }

    /// Update procedures only (progressive loading - step 5)
    pub fn set_procedures_only(
        &mut self,
        id: Uuid,
        procedures: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.procedures = procedures;
            conn.procedures_expanded = !conn.procedures.is_empty();
        }
        cx.notify();
    }

    /// Update triggers only (progressive loading - step 6)
    pub fn set_triggers_only(&mut self, id: Uuid, triggers: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.triggers = triggers;
            conn.triggers_expanded = !conn.triggers.is_empty();
        }
        cx.notify();
    }

    /// Update a connection's schema info
    pub fn set_schema(
        &mut self,
        id: Uuid,
        tables: Vec<String>,
        views: Vec<String>,
        materialized_views: Vec<String>,
        triggers: Vec<String>,
        functions: Vec<String>,
        procedures: Vec<String>,
        schema_name: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.tables = tables;
            conn.views = views;
            conn.materialized_views = materialized_views;
            conn.triggers = triggers;
            conn.functions = functions;
            conn.procedures = procedures;
            conn.schema_name = schema_name;
            conn.is_expanded = true;
            conn.schema_expanded = true;
            conn.tables_expanded = true;
        }
        cx.notify();
    }

    /// Set the list of all databases on the server for a connection.
    /// Migrates existing connection-level schema data into the active database node.
    pub fn set_databases(
        &mut self,
        id: Uuid,
        databases: Vec<(String, Option<i64>)>,
        active_database: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            // Build per-database schema data for the active database from
            // connection-level fields that were populated by set_schema
            let active_schema = DatabaseSchemaData {
                schema_name: conn.schema_name.clone(),
                schema_expanded: conn.schema_expanded,
                tables: conn.tables.clone(),
                views: conn.views.clone(),
                materialized_views: conn.materialized_views.clone(),
                triggers: conn.triggers.clone(),
                functions: conn.functions.clone(),
                procedures: conn.procedures.clone(),
                tables_expanded: conn.tables_expanded,
                views_expanded: conn.views_expanded,
                materialized_views_expanded: conn.materialized_views_expanded,
                triggers_expanded: conn.triggers_expanded,
                functions_expanded: conn.functions_expanded,
                procedures_expanded: conn.procedures_expanded,
            };

            conn.databases = databases
                .into_iter()
                .map(|(name, size_bytes)| {
                    let is_active = active_database.map_or(false, |active| active == name);
                    SidebarDatabaseInfo {
                        name,
                        size_bytes,
                        is_active,
                        is_expanded: is_active,
                        is_loading: false,
                        schema: if is_active {
                            Some(active_schema.clone())
                        } else {
                            None
                        },
                    }
                })
                .collect();
        }
        cx.notify();
    }

    /// Set schema data for a specific database within a connection.
    /// Used when loading schema on demand (e.g. user expands an inactive database).
    pub fn set_database_schema(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        tables: Vec<String>,
        views: Vec<String>,
        materialized_views: Vec<String>,
        triggers: Vec<String>,
        functions: Vec<String>,
        procedures: Vec<String>,
        schema_name: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(db) = conn.databases.iter_mut().find(|d| d.name == database_name) {
                db.is_loading = false;
                db.schema = Some(DatabaseSchemaData {
                    schema_name,
                    schema_expanded: true,
                    tables,
                    views,
                    materialized_views,
                    triggers,
                    functions,
                    procedures,
                    tables_expanded: true,
                    views_expanded: false,
                    materialized_views_expanded: false,
                    triggers_expanded: false,
                    functions_expanded: false,
                    procedures_expanded: false,
                });
            }
        }
        cx.notify();
    }

    /// Mark a database as loading schema
    pub fn set_database_loading(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        loading: bool,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(db) = conn.databases.iter_mut().find(|d| d.name == database_name) {
                db.is_loading = loading;
            }
        }
        cx.notify();
    }

    /// Remove a table from a connection's schema
    pub fn remove_table(&mut self, conn_id: Uuid, table_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.tables.retain(|t| t != table_name);
        }
        cx.notify();
    }

    /// Add a table to a connection's schema
    pub fn add_table(&mut self, conn_id: Uuid, table_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if !conn.tables.contains(&table_name) {
                conn.tables.push(table_name);
                conn.tables.sort();
            }
        }
        cx.notify();
    }

    /// Remove a view from a connection's schema
    pub fn remove_view(&mut self, conn_id: Uuid, view_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.views.retain(|v| v != view_name);
        }
        cx.notify();
    }

    /// Add a view to a connection's schema
    pub fn add_view(&mut self, conn_id: Uuid, view_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if !conn.views.contains(&view_name) {
                conn.views.push(view_name);
                conn.views.sort();
            }
        }
        cx.notify();
    }

    /// Remove a trigger from a connection's schema
    pub fn remove_trigger(&mut self, conn_id: Uuid, trigger_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.triggers.retain(|t| t != trigger_name);
        }
        cx.notify();
    }

    /// Add a trigger to a connection's schema
    pub fn add_trigger(&mut self, conn_id: Uuid, trigger_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if !conn.triggers.contains(&trigger_name) {
                conn.triggers.push(trigger_name);
                conn.triggers.sort();
            }
        }
        cx.notify();
    }

    /// Set saved queries for a connection
    pub fn set_saved_queries(
        &mut self,
        conn_id: Uuid,
        queries: Vec<SavedQueryInfo>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.queries = queries;
        }
        cx.notify();
    }

    /// Add a saved query to a connection
    pub fn add_saved_query(
        &mut self,
        conn_id: Uuid,
        query: SavedQueryInfo,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if !conn.queries.iter().any(|q| q.id == query.id) {
                conn.queries.push(query);
                conn.queries.sort_by(|a, b| a.name.cmp(&b.name));
            }
        }
        cx.notify();
    }

    /// Remove a saved query from a connection
    pub fn remove_saved_query(&mut self, conn_id: Uuid, query_id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.queries.retain(|q| q.id != query_id);
        }
        cx.notify();
    }

    /// Rename a saved query in a connection
    pub fn rename_saved_query(
        &mut self,
        conn_id: Uuid,
        query_id: Uuid,
        new_name: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(query) = conn.queries.iter_mut().find(|q| q.id == query_id) {
                query.name = new_name;
            }
            conn.queries.sort_by(|a, b| a.name.cmp(&b.name));
        }
        cx.notify();
    }

    // =========================================================================
    // Redis-specific methods
    // =========================================================================

    /// Set Redis databases for a connection
    pub fn set_redis_databases(
        &mut self,
        conn_id: Uuid,
        databases: Vec<(u16, Option<i64>)>, // (index, key_count)
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.redis_databases = databases
                .into_iter()
                .map(|(index, key_count)| RedisDatabaseInfo::new(index, key_count))
                .collect();
            conn.is_expanded = true;
            conn.redis_databases_expanded = true;
        }
        cx.notify();
    }

    /// Set keys for a specific Redis database
    pub fn set_redis_keys(
        &mut self,
        conn_id: Uuid,
        database_index: u16,
        keys: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(db) = conn
                .redis_databases
                .iter_mut()
                .find(|d| d.index == database_index)
            {
                db.keys = keys;
                db.is_loading = false;
                db.key_count = Some(db.keys.len() as i64);
            }
        }
        cx.notify();
    }
}
