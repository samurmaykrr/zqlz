//! Connection state management methods for ConnectionSidebar

use gpui::Context;
use uuid::Uuid;
use zqlz_core::{
    DocumentCollectionInfo, DocumentDatabaseObjects, ObjectFeatureSet, ObjectsPanelManifest,
};

use crate::widgets::sidebar::ConnectionSidebar;
use crate::widgets::sidebar::types::*;

impl ConnectionSidebar {
    fn invalidate_virtual_rows(&mut self) {
        self.virtual_rows.clear();
        self.virtual_rows_dirty = true;
    }

    fn table_name_exists_in_schema(
        tables: &[String],
        key_schema_name: Option<&str>,
        key_table_name: &str,
    ) -> bool {
        tables.iter().any(|table_name| {
            if table_name == key_table_name {
                return true;
            }

            table_name
                .split_once('.')
                .is_some_and(|(schema_name, object_name)| {
                    object_name == key_table_name
                        && key_schema_name
                            .is_none_or(|key_schema_name| key_schema_name == schema_name)
                })
        })
    }

    fn retain_current_table_state(
        table_details: &mut std::collections::HashMap<SidebarTableKey, SidebarTableDetailsData>,
        expanded_table_keys: &mut std::collections::HashSet<SidebarTableKey>,
        loading_table_keys: &mut std::collections::HashSet<SidebarTableKey>,
        tables: &[String],
    ) {
        table_details.retain(|key, _| {
            Self::table_name_exists_in_schema(tables, key.schema_name.as_deref(), &key.table_name)
        });
        expanded_table_keys.retain(|key| {
            Self::table_name_exists_in_schema(tables, key.schema_name.as_deref(), &key.table_name)
        });
        loading_table_keys.retain(|key| {
            Self::table_name_exists_in_schema(tables, key.schema_name.as_deref(), &key.table_name)
        });
    }

    fn apply_database_schema_to_connections(
        connections: &mut [ConnectionEntry],
        conn_id: Uuid,
        database_name: &str,
        schema: SchemaObjects,
    ) {
        let Some(conn) = connections
            .iter_mut()
            .find(|connection| connection.id == conn_id)
        else {
            return;
        };

        for database in &mut conn.databases {
            database.is_active = database.name == database_name;
        }

        let target = conn
            .databases
            .iter()
            .position(|database| database.name == database_name);

        // A database taking ownership of the schema for the first time inherits
        // the connection-scope expansion state it was rendering until now.
        if let Some(target) = target {
            let previous = conn.databases[target]
                .schema
                .as_ref()
                .unwrap_or(&conn.schema);
            let schema = DatabaseSchemaData::from(schema).with_expansion_from(Some(previous));
            let database = &mut conn.databases[target];
            database.is_active = true;
            database.is_loading = false;
            database.schema = Some(schema);
        } else {
            let schema = DatabaseSchemaData::from(schema).with_expansion_from(Some(&conn.schema));
            conn.databases.push(SidebarDatabaseInfo {
                name: database_name.to_string(),
                size_bytes: None,
                is_active: true,
                is_expanded: true,
                is_loading: false,
                schema: Some(schema),
                collections: Vec::new(),
                indexes: Vec::new(),
                functions: Vec::new(),
                gridfs_buckets: Vec::new(),
                users: Vec::new(),
                roles: Vec::new(),
                search_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                server: Vec::new(),
                sharding: Vec::new(),
                collections_expanded: false,
                collections_loading: false,
                document_expanded_sections: std::collections::HashSet::new(),
            });
        }
    }

    /// Set connections from external source (e.g., AppState)
    /// This decouples the sidebar from the app's global state.
    pub fn set_connections(&mut self, connections: Vec<ConnectionEntry>, cx: &mut Context<Self>) {
        self.connections = connections;
        self.invalidate_virtual_rows();
        tracing::info!("Set {} connections", self.connections.len());
        cx.notify();
    }

    /// Add a new connection to the sidebar
    pub fn add_connection(&mut self, entry: ConnectionEntry, cx: &mut Context<Self>) {
        self.connections.push(entry);
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Remove a connection from the sidebar
    pub fn remove_connection(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.connections.retain(|c| c.id != id);
        if self.selected_connection == Some(id) {
            self.selected_connection = None;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update a connection's connected state
    pub fn set_connected(&mut self, id: Uuid, connected: bool, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.is_connected = connected;
            conn.is_connecting = false; // Clear connecting state when connection status changes
            if connected {
                // Show the immediate children (sections / database nodes) as soon as
                // the connection succeeds so users can start navigating immediately.
                conn.is_expanded = true;
            }
            if !connected {
                conn.is_expanded = false;
                conn.redis_databases.clear();
                conn.databases.clear();
                conn.schema = DatabaseSchemaData::default();
                conn.queries_expanded = false;
                conn.redis_databases_expanded = false;
            }
        }
        if !connected {
            self.table_details.retain(|key, _| key.conn_id != id);
            self.expanded_table_keys.retain(|key| key.conn_id != id);
            self.loading_table_keys.retain(|key| key.conn_id != id);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Set whether a connection is currently connecting
    pub fn set_connecting(&mut self, id: Uuid, connecting: bool, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.is_connecting = connecting;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update tables only (progressive loading - step 1)
    pub fn set_tables_only(
        &mut self,
        id: Uuid,
        tables: Vec<String>,
        schema_name: Option<String>,
        schema_names: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            // The placeholder node created at connect time is no longer waiting
            // on anything once its tables land.
            if let Some(database) = conn
                .databases
                .iter_mut()
                .find(|database| database.is_active)
            {
                database.is_loading = false;
            }

            let schema = conn.schema_mut(None);
            schema.tables = tables;
            schema.schema_name = schema_name;
            schema.schema_names = schema_names;
            schema.loading_sections.remove(&SidebarSection::Tables);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Replace one section's object list, clearing its loading state.
    pub fn set_section_objects(
        &mut self,
        id: Uuid,
        section: SidebarSection,
        names: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            let schema = conn.schema_mut(None);
            schema.loading_sections.remove(&section);
            if let Some(objects) = schema.objects_mut(section) {
                *objects = names;
            }
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update views only (lazy or eager load).
    pub fn set_views_only(&mut self, id: Uuid, views: Vec<String>, cx: &mut Context<Self>) {
        self.set_section_objects(id, SidebarSection::Views, views, cx);
    }

    /// Update materialized views only (lazy or eager load).
    pub fn set_materialized_views_only(
        &mut self,
        id: Uuid,
        materialized_views: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        self.set_section_objects(id, SidebarSection::MaterializedViews, materialized_views, cx);
    }

    /// Update functions only (lazy or eager load).
    pub fn set_functions_only(&mut self, id: Uuid, functions: Vec<String>, cx: &mut Context<Self>) {
        self.set_section_objects(id, SidebarSection::Functions, functions, cx);
    }

    /// Update procedures only (lazy or eager load).
    pub fn set_procedures_only(
        &mut self,
        id: Uuid,
        procedures: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        self.set_section_objects(id, SidebarSection::Procedures, procedures, cx);
    }

    /// Update triggers only (lazy or eager load).
    pub fn set_triggers_only(&mut self, id: Uuid, triggers: Vec<String>, cx: &mut Context<Self>) {
        self.set_section_objects(id, SidebarSection::Triggers, triggers, cx);
    }

    /// Apply a loaded sidebar section payload for one connection.
    ///
    /// The `Queries` and `RedisDatabases` sections are intentionally treated as
    /// loading-clear only here because their data is managed by dedicated query
    /// and Redis workflows.
    pub fn set_loaded_section_names(
        &mut self,
        connection_id: Uuid,
        section: SidebarSection,
        names: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        match section {
            SidebarSection::Views => {
                self.set_views_only(connection_id, names, cx);
            }
            SidebarSection::MaterializedViews => {
                self.set_materialized_views_only(connection_id, names, cx);
            }
            SidebarSection::Functions => {
                self.set_functions_only(connection_id, names, cx);
            }
            SidebarSection::Procedures => {
                self.set_procedures_only(connection_id, names, cx);
            }
            SidebarSection::Triggers => {
                self.set_triggers_only(connection_id, names, cx);
            }
            SidebarSection::Events
            | SidebarSection::Sequences
            | SidebarSection::Domains
            | SidebarSection::Types
            | SidebarSection::Extensions => {
                self.set_section_objects(connection_id, section, names, cx);
            }
            SidebarSection::Tables | SidebarSection::Queries | SidebarSection::RedisDatabases => {
                self.clear_section_loading(connection_id, section, cx);
            }
        }
    }

    /// Clear the loading spinner for a section without updating its data.
    ///
    /// Used when a lazy-load fetch fails so the spinner doesn't spin forever.
    pub fn clear_section_loading(
        &mut self,
        conn_id: Uuid,
        section: SidebarSection,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.schema_mut(None).loading_sections.remove(&section);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn set_table_details(
        &mut self,
        key: SidebarTableKey,
        details: SidebarTableDetailsData,
        cx: &mut Context<Self>,
    ) {
        self.loading_table_keys.remove(&key);
        self.table_details.insert(key, details);
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn clear_table_details_loading(&mut self, key: &SidebarTableKey, cx: &mut Context<Self>) {
        self.loading_table_keys.remove(key);
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Replace the schema objects rendering currently shows for a connection,
    /// keeping which sections the user has expanded.
    pub fn set_schema(&mut self, id: Uuid, schema: SchemaObjects, cx: &mut Context<Self>) {
        Self::retain_current_table_state(
            &mut self.table_details,
            &mut self.expanded_table_keys,
            &mut self.loading_table_keys,
            &schema.tables,
        );

        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            let target = conn.schema_mut(None);
            let updated = DatabaseSchemaData::from(schema).with_expansion_from(Some(target));
            *target = updated;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn set_objects_panel_manifest(
        &mut self,
        id: Uuid,
        manifest: ObjectsPanelManifest,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.objects_panel_manifest = Some(manifest);
        }
        cx.notify();
    }

    /// Record which object actions the connection advertises, so object menus can
    /// explain why an action is unavailable.
    /// Replace the driver-derived capability baseline with the
    /// connection-refined one (e.g. runtime-detected MariaDB sequences).
    pub fn set_object_capabilities(
        &mut self,
        id: Uuid,
        capabilities: SidebarObjectCapabilities,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if conn.object_capabilities == capabilities {
                return;
            }
            conn.object_capabilities = capabilities;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn set_object_features(
        &mut self,
        id: Uuid,
        object_features: ObjectFeatureSet,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.object_features = Some(object_features);
        }
        cx.notify();
    }

    /// Which object actions `connection_id` advertises, if known.
    pub(in crate::widgets) fn connection_object_features(
        &self,
        connection_id: Uuid,
    ) -> Option<ObjectFeatureSet> {
        self.connections
            .iter()
            .find(|connection| connection.id == connection_id)
            .and_then(|connection| connection.object_features.clone())
    }

    /// Pre-populate the sidebar with the known active database immediately after
    /// connecting, before any async schema queries complete.
    ///
    /// Creates a single loading database node so the multi-DB hierarchy is visible
    /// right away, preventing the jarring flat-tables → database-nodes transition.
    pub fn init_database_view(&mut self, id: Uuid, active_db_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.databases = vec![SidebarDatabaseInfo {
                name: active_db_name.to_string(),
                size_bytes: None,
                is_active: true,
                is_expanded: false,
                is_loading: true,
                schema: None,
                collections: Vec::new(),
                indexes: Vec::new(),
                functions: Vec::new(),
                gridfs_buckets: Vec::new(),
                users: Vec::new(),
                roles: Vec::new(),
                search_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                server: Vec::new(),
                sharding: Vec::new(),
                collections_expanded: false,
                collections_loading: false,
                document_expanded_sections: std::collections::HashSet::new(),
            }];
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Merge a freshly fetched database list into the sidebar without discarding
    /// state for databases that are already visible.
    ///
    /// For databases already in the list: keeps existing schema, expansion state,
    /// and loading flags; only updates `size_bytes` and `is_active`.
    /// For databases not yet present: appends them with default (collapsed) state.
    pub fn merge_databases(
        &mut self,
        id: Uuid,
        databases: Vec<(String, Option<i64>)>,
        active_database: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            use std::collections::HashMap;
            let single_database_name = if databases.len() == 1 {
                databases.first().map(|(name, _)| name.clone())
            } else {
                None
            };
            let existing_active_database_name = conn
                .databases
                .iter()
                .find(|database| database.is_active)
                .map(|database| database.name.clone());
            let effective_active_database = active_database
                .map(ToOwned::to_owned)
                .or(existing_active_database_name)
                .or(single_database_name);
            // Discovery names the database the connection scope was standing in
            // for, so its schema state moves across intact.
            let active_schema = conn.schema(effective_active_database.as_deref()).clone();
            let mut existing: HashMap<String, SidebarDatabaseInfo> = conn
                .databases
                .drain(..)
                .map(|db| (db.name.clone(), db))
                .collect();

            conn.databases = databases
                .into_iter()
                .map(|(name, size_bytes)| {
                    let is_active = effective_active_database.as_deref() == Some(name.as_str());
                    if let Some(mut db) = existing.remove(&name) {
                        db.size_bytes = size_bytes;
                        db.is_active = is_active;
                        if is_active && db.schema.is_none() {
                            db.is_loading = false;
                            db.schema = Some(active_schema.clone());
                        }
                        db
                    } else {
                        SidebarDatabaseInfo {
                            name,
                            size_bytes,
                            is_active,
                            is_expanded: false,
                            is_loading: false,
                            schema: if is_active {
                                Some(active_schema.clone())
                            } else {
                                None
                            },
                            collections: Vec::new(),
                            indexes: Vec::new(),
                            functions: Vec::new(),
                            gridfs_buckets: Vec::new(),
                            users: Vec::new(),
                            roles: Vec::new(),
                            search_indexes: Vec::new(),
                            vector_indexes: Vec::new(),
                            server: Vec::new(),
                            sharding: Vec::new(),
                            collections_expanded: false,
                            collections_loading: false,
                            document_expanded_sections: std::collections::HashSet::new(),
                        }
                    }
                })
                .collect();
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Replace the list of all databases on the server for a connection,
    /// carrying the schema state the active database was already rendering.
    pub fn set_databases(
        &mut self,
        id: Uuid,
        databases: Vec<(String, Option<i64>)>,
        active_database: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            let active_schema = conn.schema(active_database).clone();

            conn.databases = databases
                .into_iter()
                .map(|(name, size_bytes)| {
                    let is_active = active_database.is_some_and(|active| active == name);
                    SidebarDatabaseInfo {
                        name,
                        size_bytes,
                        is_active,
                        is_expanded: false,
                        is_loading: false,
                        schema: if is_active {
                            Some(active_schema.clone())
                        } else {
                            None
                        },
                        collections: Vec::new(),
                        indexes: Vec::new(),
                        functions: Vec::new(),
                        gridfs_buckets: Vec::new(),
                        users: Vec::new(),
                        roles: Vec::new(),
                        search_indexes: Vec::new(),
                        vector_indexes: Vec::new(),
                        server: Vec::new(),
                        sharding: Vec::new(),
                        collections_expanded: false,
                        collections_loading: false,
                        document_expanded_sections: std::collections::HashSet::new(),
                    }
                })
                .collect();
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Set schema data for a specific database within a connection.
    /// Used when loading schema on demand (e.g. user expands an inactive database).
    pub fn set_database_schema(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        schema: SchemaObjects,
        cx: &mut Context<Self>,
    ) {
        Self::apply_database_schema_to_connections(
            &mut self.connections,
            conn_id,
            database_name,
            schema,
        );
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Apply schema data to a database node, creating the node when database
    /// discovery was intentionally skipped for targeted refreshes.
    pub fn apply_database_schema(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        schema: SchemaObjects,
        cx: &mut Context<Self>,
    ) {
        Self::apply_database_schema_to_connections(
            &mut self.connections,
            conn_id,
            database_name,
            schema,
        );
        self.invalidate_virtual_rows();
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
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(db) = conn.databases.iter_mut().find(|d| d.name == database_name)
        {
            db.is_loading = loading;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Mutate one section's object list where rendering reads it.
    fn update_section_objects(
        &mut self,
        conn_id: Uuid,
        section: SidebarSection,
        mutate: impl Fn(&mut Vec<String>),
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(objects) = conn.schema_mut(None).objects_mut(section)
        {
            mutate(objects);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn remove_table(&mut self, conn_id: Uuid, table_name: &str, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Tables,
            |tables| tables.retain(|t| t != table_name),
            cx,
        );
    }

    /// Add a table to a connection's schema
    pub fn add_table(&mut self, conn_id: Uuid, table_name: String, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Tables,
            |tables| {
                if !tables.contains(&table_name) {
                    tables.push(table_name.clone());
                    tables.sort();
                }
            },
            cx,
        );
    }

    /// Remove a view from a connection's schema
    pub fn remove_view(&mut self, conn_id: Uuid, view_name: &str, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Views,
            |views| views.retain(|v| v != view_name),
            cx,
        );
    }

    /// Add a view to a connection's schema
    pub fn add_view(&mut self, conn_id: Uuid, view_name: String, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Views,
            |views| {
                if !views.contains(&view_name) {
                    views.push(view_name.clone());
                    views.sort();
                }
            },
            cx,
        );
    }

    /// Remove a trigger from a connection's schema
    pub fn remove_trigger(&mut self, conn_id: Uuid, trigger_name: &str, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Triggers,
            |triggers| triggers.retain(|t| t != trigger_name),
            cx,
        );
    }

    /// Add a trigger to a connection's schema
    pub fn add_trigger(&mut self, conn_id: Uuid, trigger_name: String, cx: &mut Context<Self>) {
        self.update_section_objects(
            conn_id,
            SidebarSection::Triggers,
            |triggers| {
                if !triggers.contains(&trigger_name) {
                    triggers.push(trigger_name.clone());
                    triggers.sort();
                }
            },
            cx,
        );
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
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Add a saved query to a connection
    pub fn add_saved_query(
        &mut self,
        conn_id: Uuid,
        query: SavedQueryInfo,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && !conn.queries.iter().any(|q| q.id == query.id)
        {
            conn.queries.push(query);
            Self::sort_saved_queries(&mut conn.queries);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Remove a saved query from a connection
    pub fn remove_saved_query(&mut self, conn_id: Uuid, query_id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.queries.retain(|q| q.id != query_id);
        }
        self.invalidate_virtual_rows();
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
            Self::sort_saved_queries(&mut conn.queries);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn update_saved_query_text(
        &mut self,
        conn_id: Uuid,
        query_id: Uuid,
        query_text: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(query) = conn.queries.iter_mut().find(|q| q.id == query_id)
        {
            query.query_text = query_text;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn move_saved_query_to_folder(
        &mut self,
        conn_id: Uuid,
        query_id: Uuid,
        folder: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(query) = conn.queries.iter_mut().find(|q| q.id == query_id) {
                query.folder = folder;
            }
            Self::sort_saved_queries(&mut conn.queries);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    fn sort_saved_queries(queries: &mut [SavedQueryInfo]) {
        queries.sort_by(|a, b| a.folder.cmp(&b.folder).then_with(|| a.name.cmp(&b.name)));
    }

    /// Mark all schema sections as loading for a connection.
    ///
    /// Called immediately after connect so every section header shows a spinner
    /// before the parallel eager-load queries complete.
    pub fn set_all_sections_loading(&mut self, conn_id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            let eagerly_loaded = [
                SidebarSection::Tables,
                SidebarSection::Views,
                SidebarSection::MaterializedViews,
                SidebarSection::Triggers,
                SidebarSection::Functions,
                SidebarSection::Procedures,
            ]
            .into_iter()
            .filter(|section| conn.object_capabilities.supports_section(*section))
            .collect::<Vec<_>>();

            conn.schema_mut(None).loading_sections.extend(eagerly_loaded);
        }
        self.invalidate_virtual_rows();
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
            conn.redis_databases_expanded = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn set_document_collections(
        &mut self,
        id: Uuid,
        database_name: &str,
        collections: Vec<DocumentCollectionInfo>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|conn| conn.id == id)
            && let Some(database) = conn
                .databases
                .iter_mut()
                .find(|database| database.name == database_name)
        {
            database.collections = collections;
            database.indexes.clear();
            database.functions.clear();
            database.gridfs_buckets.clear();
            database.users.clear();
            database.roles.clear();
            database.search_indexes.clear();
            database.vector_indexes.clear();
            database.server.clear();
            database.sharding.clear();
            database.collections_loading = false;
            database.collections_expanded = true;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    pub fn set_document_database_objects(
        &mut self,
        id: Uuid,
        database_name: &str,
        objects: DocumentDatabaseObjects,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|conn| conn.id == id)
            && let Some(database) = conn
                .databases
                .iter_mut()
                .find(|database| database.name == database_name)
        {
            database.collections = objects.collections;
            database.indexes = objects.indexes;
            database.functions = objects.functions;
            database.gridfs_buckets = objects.gridfs_buckets;
            database.users = objects.users;
            database.roles = objects.roles;
            database.search_indexes = objects.search_indexes;
            database.vector_indexes = objects.vector_indexes;
            database.server = objects.server;
            database.sharding = objects.sharding;
            database.collections_loading = false;
            database.collections_expanded = true;
        }
        self.invalidate_virtual_rows();
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
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(db) = conn
                .redis_databases
                .iter_mut()
                .find(|d| d.index == database_index)
        {
            db.keys = keys;
            db.is_loading = false;
            db.key_count = Some(db.keys.len() as i64);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionSidebar;
    use crate::widgets::sidebar::types::{
        ConnectionEntry, DatabaseSchemaData, SchemaObjects, SidebarDatabaseInfo, SidebarSection,
    };
    use std::collections::HashSet;
    use uuid::Uuid;

    fn schema_objects() -> SchemaObjects {
        SchemaObjects {
            tables: vec!["users".to_string()],
            schema_name: Some("public".to_string()),
            schema_names: vec!["public".to_string()],
            ..SchemaObjects::default()
        }
    }

    #[test]
    fn apply_database_schema_creates_target_database_when_list_absent() {
        let connection_id = Uuid::new_v4();
        let mut connection = ConnectionEntry::new(
            connection_id,
            "postgres@localhost".to_string(),
            "postgres".to_string(),
        );

        ConnectionSidebar::apply_database_schema_to_connections(
            std::slice::from_mut(&mut connection),
            connection_id,
            "postgres",
            schema_objects(),
        );

        let database = connection
            .databases
            .iter()
            .find(|database| database.name == "postgres")
            .expect("target database should be created");

        assert!(database.is_active);
        assert!(database.is_expanded);
        assert!(!database.is_loading);
        assert_eq!(
            database.schema.as_ref().map(|schema| schema.tables.clone()),
            Some(vec!["users".to_string()])
        );
    }

    #[test]
    fn apply_database_schema_preserves_loaded_schema_after_database_list_merge_shape() {
        let connection_id = Uuid::new_v4();
        let mut connection = ConnectionEntry::new(
            connection_id,
            "postgres@localhost".to_string(),
            "postgres".to_string(),
        );
        connection.databases = vec![
            SidebarDatabaseInfo {
                name: "erp_lab".to_string(),
                size_bytes: Some(1),
                is_active: false,
                is_expanded: true,
                is_loading: false,
                schema: None,
                collections: Vec::new(),
                indexes: Vec::new(),
                functions: Vec::new(),
                gridfs_buckets: Vec::new(),
                users: Vec::new(),
                roles: Vec::new(),
                search_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                server: Vec::new(),
                sharding: Vec::new(),
                collections_expanded: false,
                collections_loading: false,
                document_expanded_sections: std::collections::HashSet::new(),
            },
            SidebarDatabaseInfo {
                name: "postgres".to_string(),
                size_bytes: Some(2),
                is_active: true,
                is_expanded: true,
                is_loading: true,
                schema: None,
                collections: Vec::new(),
                indexes: Vec::new(),
                functions: Vec::new(),
                gridfs_buckets: Vec::new(),
                users: Vec::new(),
                roles: Vec::new(),
                search_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                server: Vec::new(),
                sharding: Vec::new(),
                collections_expanded: false,
                collections_loading: false,
                document_expanded_sections: std::collections::HashSet::new(),
            },
        ];

        ConnectionSidebar::apply_database_schema_to_connections(
            std::slice::from_mut(&mut connection),
            connection_id,
            "postgres",
            schema_objects(),
        );

        let database = connection
            .databases
            .iter()
            .find(|database| database.name == "postgres")
            .expect("target database should remain");

        assert!(database.is_active);
        assert!(database.is_expanded);
        assert!(!database.is_loading);
        assert!(database.schema.is_some());
    }

    #[test]
    fn apply_database_schema_preserves_expanded_database_schema_state() {
        let connection_id = Uuid::new_v4();
        let mut connection = ConnectionEntry::new(
            connection_id,
            "postgres@localhost".to_string(),
            "postgres".to_string(),
        );

        connection.databases = vec![SidebarDatabaseInfo {
            name: "postgres".to_string(),
            size_bytes: Some(2),
            is_active: true,
            is_expanded: true,
            is_loading: false,
            schema: Some(DatabaseSchemaData {
                schema_name: Some("public".to_string()),
                schema_names: vec!["public".to_string()],
                schema_expanded: true,
                expanded_schema_groups: HashSet::from(["public".to_string()]),
                expanded_schema_section_keys: HashSet::from(["public::tables".to_string()]),
                tables: vec!["public.users".to_string(), "public.old_table".to_string()],
                expanded_sections: HashSet::from([SidebarSection::Tables]),
                ..DatabaseSchemaData::default()
            }),
            collections: Vec::new(),
            indexes: Vec::new(),
            functions: Vec::new(),
            gridfs_buckets: Vec::new(),
            users: Vec::new(),
            roles: Vec::new(),
            search_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            server: Vec::new(),
            sharding: Vec::new(),
            collections_expanded: false,
            collections_loading: false,
            document_expanded_sections: std::collections::HashSet::new(),
        }];

        ConnectionSidebar::apply_database_schema_to_connections(
            std::slice::from_mut(&mut connection),
            connection_id,
            "postgres",
            SchemaObjects {
                tables: vec!["public.users".to_string(), "public.accounts".to_string()],
                schema_name: Some("public".to_string()),
                schema_names: vec!["public".to_string()],
                ..SchemaObjects::default()
            },
        );

        let schema = connection
            .databases
            .iter()
            .find(|database| database.name == "postgres")
            .and_then(|database| database.schema.as_ref())
            .expect("schema should remain loaded");

        assert!(schema.schema_expanded);
        assert!(schema.expanded_schema_groups.contains("public"));
        assert!(
            schema
                .expanded_schema_section_keys
                .contains("public::tables")
        );
        assert!(schema.expanded_sections.contains(&SidebarSection::Tables));
        assert_eq!(
            schema.tables,
            vec!["public.users".to_string(), "public.accounts".to_string()]
        );
    }
}
