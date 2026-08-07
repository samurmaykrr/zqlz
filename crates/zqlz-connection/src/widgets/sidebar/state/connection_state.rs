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

    fn schema_objects_from_connection(conn: &ConnectionEntry) -> SchemaObjects {
        SchemaObjects {
            tables: conn.tables.clone(),
            views: conn.views.clone(),
            materialized_views: conn.materialized_views.clone(),
            triggers: conn.triggers.clone(),
            functions: conn.functions.clone(),
            procedures: conn.procedures.clone(),
            events: conn.events.clone(),
            sequences: conn.sequences.clone(),
            domains: conn.domains.clone(),
            types: conn.types.clone(),
            extensions: conn.extensions.clone(),
            schema_name: conn.schema_name.clone(),
            schema_names: conn.schema_names.clone(),
        }
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

    fn database_schema_data_from_schema_objects(
        schema: SchemaObjects,
        previous: Option<&DatabaseSchemaData>,
    ) -> DatabaseSchemaData {
        let SchemaObjects {
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            events,
            sequences,
            domains,
            types,
            extensions,
            schema_name,
            schema_names,
        } = schema;

        let mut schema_data = DatabaseSchemaData {
            schema_name,
            schema_names,
            schema_expanded: previous.is_some_and(|previous| previous.schema_expanded),
            collapsed_schema_groups: previous
                .map(|previous| previous.collapsed_schema_groups.clone())
                .unwrap_or_default(),
            collapsed_schema_section_keys: previous
                .map(|previous| previous.collapsed_schema_section_keys.clone())
                .unwrap_or_default(),
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            events,
            sequences,
            domains,
            types,
            extensions,
            table_details: previous
                .map(|previous| previous.table_details.clone())
                .unwrap_or_default(),
            expanded_table_keys: previous
                .map(|previous| previous.expanded_table_keys.clone())
                .unwrap_or_default(),
            loading_table_keys: previous
                .map(|previous| previous.loading_table_keys.clone())
                .unwrap_or_default(),
            tables_expanded: previous.is_some_and(|previous| previous.tables_expanded),
            views_expanded: previous.is_some_and(|previous| previous.views_expanded),
            materialized_views_expanded: previous
                .is_some_and(|previous| previous.materialized_views_expanded),
            triggers_expanded: previous.is_some_and(|previous| previous.triggers_expanded),
            functions_expanded: previous.is_some_and(|previous| previous.functions_expanded),
            procedures_expanded: previous.is_some_and(|previous| previous.procedures_expanded),
            events_expanded: previous.is_some_and(|previous| previous.events_expanded),
            tables_loading: false,
            views_loading: false,
            materialized_views_loading: false,
            triggers_loading: false,
            functions_loading: false,
            procedures_loading: false,
        };

        Self::retain_current_table_state(
            &mut schema_data.table_details,
            &mut schema_data.expanded_table_keys,
            &mut schema_data.loading_table_keys,
            &schema_data.tables,
        );

        schema_data
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

        if let Some(database) = conn
            .databases
            .iter_mut()
            .find(|database| database.name == database_name)
        {
            let schema =
                Self::database_schema_data_from_schema_objects(schema, database.schema.as_ref());
            database.is_active = true;
            database.is_loading = false;
            database.schema = Some(schema);
        } else {
            let schema = Self::database_schema_data_from_schema_objects(schema, None);
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
                conn.tables.clear();
                conn.views.clear();
                conn.materialized_views.clear();
                conn.triggers.clear();
                conn.functions.clear();
                conn.procedures.clear();
                conn.events.clear();
                conn.sequences.clear();
                conn.domains.clear();
                conn.types.clear();
                conn.extensions.clear();
                conn.redis_databases.clear();
                conn.databases.clear();
                conn.schema_name = None;
                conn.schema_names.clear();
                conn.collapsed_schema_groups.clear();
                conn.collapsed_schema_section_keys.clear();
                conn.tables_loading = false;
                conn.views_loading = false;
                conn.materialized_views_loading = false;
                conn.triggers_loading = false;
                conn.functions_loading = false;
                conn.procedures_loading = false;
                conn.events_expanded = false;
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
            conn.tables = tables;
            conn.schema_name = schema_name;
            conn.schema_names = schema_names;
            conn.tables_loading = false;

            let active_database_name = conn
                .databases
                .iter()
                .find(|database| database.is_active)
                .map(|database| database.name.clone());
            if let Some(active_database_name) = active_database_name {
                let schema = Self::schema_objects_from_connection(conn);
                if let Some(database) = conn
                    .databases
                    .iter_mut()
                    .find(|database| database.name == active_database_name)
                {
                    database.is_loading = false;
                    database.schema = Some(Self::database_schema_data_from_schema_objects(
                        schema,
                        database.schema.as_ref(),
                    ));
                }
            }
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update views only (lazy or eager load).
    pub fn set_views_only(&mut self, id: Uuid, views: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.views = views;
            conn.views_loading = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update materialized views only (lazy or eager load).
    pub fn set_materialized_views_only(
        &mut self,
        id: Uuid,
        materialized_views: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.materialized_views = materialized_views;
            conn.materialized_views_loading = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update functions only (lazy or eager load).
    pub fn set_functions_only(&mut self, id: Uuid, functions: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.functions = functions;
            conn.functions_loading = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update procedures only (lazy or eager load).
    pub fn set_procedures_only(
        &mut self,
        id: Uuid,
        procedures: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.procedures = procedures;
            conn.procedures_loading = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Update triggers only (lazy or eager load).
    pub fn set_triggers_only(&mut self, id: Uuid, triggers: Vec<String>, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.triggers = triggers;
            conn.triggers_loading = false;
        }
        self.invalidate_virtual_rows();
        cx.notify();
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
            SidebarSection::Events => {
                if let Some(conn) = self.connections.iter_mut().find(|c| c.id == connection_id) {
                    conn.events = names;
                }
                self.invalidate_virtual_rows();
                cx.notify();
            }
            SidebarSection::Sequences => {
                if let Some(conn) = self.connections.iter_mut().find(|c| c.id == connection_id) {
                    conn.sequences = names;
                }
                self.invalidate_virtual_rows();
                cx.notify();
            }
            SidebarSection::Domains => {
                if let Some(conn) = self.connections.iter_mut().find(|c| c.id == connection_id) {
                    conn.domains = names;
                }
                self.invalidate_virtual_rows();
                cx.notify();
            }
            SidebarSection::Types => {
                if let Some(conn) = self.connections.iter_mut().find(|c| c.id == connection_id) {
                    conn.types = names;
                }
                self.invalidate_virtual_rows();
                cx.notify();
            }
            SidebarSection::Extensions => {
                if let Some(conn) = self.connections.iter_mut().find(|c| c.id == connection_id) {
                    conn.extensions = names;
                }
                self.invalidate_virtual_rows();
                cx.notify();
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
            match section {
                SidebarSection::Tables => conn.tables_loading = false,
                SidebarSection::Views => conn.views_loading = false,
                SidebarSection::MaterializedViews => conn.materialized_views_loading = false,
                SidebarSection::Triggers => conn.triggers_loading = false,
                SidebarSection::Functions => conn.functions_loading = false,
                SidebarSection::Procedures => conn.procedures_loading = false,
                SidebarSection::Sequences
                | SidebarSection::Events
                | SidebarSection::Domains
                | SidebarSection::Types
                | SidebarSection::Extensions
                | SidebarSection::Queries
                | SidebarSection::RedisDatabases => {}
            }
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

    /// Update a connection's schema info
    pub fn set_schema(&mut self, id: Uuid, schema: SchemaObjects, cx: &mut Context<Self>) {
        let SchemaObjects {
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            events,
            sequences,
            domains,
            types,
            extensions,
            schema_name,
            schema_names,
        } = schema;

        Self::retain_current_table_state(
            &mut self.table_details,
            &mut self.expanded_table_keys,
            &mut self.loading_table_keys,
            &tables,
        );

        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.tables = tables;
            conn.views = views;
            conn.materialized_views = materialized_views;
            conn.triggers = triggers;
            conn.functions = functions;
            conn.procedures = procedures;
            conn.events = events;
            conn.sequences = sequences;
            conn.domains = domains;
            conn.types = types;
            conn.extensions = extensions;
            conn.schema_name = schema_name;
            conn.schema_names = schema_names;
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
            let active_schema = Self::database_schema_data_from_schema_objects(
                Self::schema_objects_from_connection(conn),
                None,
            );
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
            let previous_active_schema = active_database.and_then(|active_database| {
                conn.databases
                    .iter()
                    .find(|database| database.name == active_database)
                    .and_then(|database| database.schema.clone())
            });
            let connection_schema_state = DatabaseSchemaData {
                schema_expanded: conn.schema_expanded,
                collapsed_schema_groups: conn.collapsed_schema_groups.clone(),
                collapsed_schema_section_keys: conn.collapsed_schema_section_keys.clone(),
                tables_expanded: conn.tables_expanded,
                views_expanded: conn.views_expanded,
                materialized_views_expanded: conn.materialized_views_expanded,
                triggers_expanded: conn.triggers_expanded,
                functions_expanded: conn.functions_expanded,
                procedures_expanded: conn.procedures_expanded,
                events_expanded: conn.events_expanded,
                ..DatabaseSchemaData::default()
            };
            let previous_schema = previous_active_schema
                .as_ref()
                .unwrap_or(&connection_schema_state);
            let active_schema = Self::database_schema_data_from_schema_objects(
                SchemaObjects {
                    tables: conn.tables.clone(),
                    views: conn.views.clone(),
                    materialized_views: conn.materialized_views.clone(),
                    triggers: conn.triggers.clone(),
                    functions: conn.functions.clone(),
                    procedures: conn.procedures.clone(),
                    events: conn.events.clone(),
                    sequences: conn.sequences.clone(),
                    domains: conn.domains.clone(),
                    types: conn.types.clone(),
                    extensions: conn.extensions.clone(),
                    schema_name: conn.schema_name.clone(),
                    schema_names: conn.schema_names.clone(),
                },
                Some(previous_schema),
            );

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

    /// Remove a table from a connection's schema
    pub fn remove_table(&mut self, conn_id: Uuid, table_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.tables.retain(|t| t != table_name);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Add a table to a connection's schema
    pub fn add_table(&mut self, conn_id: Uuid, table_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && !conn.tables.contains(&table_name)
        {
            conn.tables.push(table_name);
            conn.tables.sort();
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Remove a view from a connection's schema
    pub fn remove_view(&mut self, conn_id: Uuid, view_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.views.retain(|v| v != view_name);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Add a view to a connection's schema
    pub fn add_view(&mut self, conn_id: Uuid, view_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && !conn.views.contains(&view_name)
        {
            conn.views.push(view_name);
            conn.views.sort();
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Remove a trigger from a connection's schema
    pub fn remove_trigger(&mut self, conn_id: Uuid, trigger_name: &str, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            conn.triggers.retain(|t| t != trigger_name);
        }
        self.invalidate_virtual_rows();
        cx.notify();
    }

    /// Add a trigger to a connection's schema
    pub fn add_trigger(&mut self, conn_id: Uuid, trigger_name: String, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && !conn.triggers.contains(&trigger_name)
        {
            conn.triggers.push(trigger_name);
            conn.triggers.sort();
        }
        self.invalidate_virtual_rows();
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
            conn.tables_loading = true;
            conn.views_loading = conn.object_capabilities.supports_views;
            conn.materialized_views_loading = conn.object_capabilities.supports_materialized_views;
            conn.triggers_loading = conn.object_capabilities.supports_triggers;
            conn.functions_loading = conn.object_capabilities.supports_functions;
            conn.procedures_loading = conn.object_capabilities.supports_procedures;
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
        ConnectionEntry, DatabaseSchemaData, SchemaObjects, SidebarDatabaseInfo,
        SidebarTableDetailsData, SidebarTableKey,
    };
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    fn schema_objects() -> SchemaObjects {
        SchemaObjects {
            tables: vec!["users".to_string()],
            schema_name: Some("public".to_string()),
            schema_names: vec!["public".to_string()],
            ..SchemaObjects::default()
        }
    }

    fn table_key(
        connection_id: Uuid,
        database_name: Option<&str>,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> SidebarTableKey {
        SidebarTableKey {
            conn_id: connection_id,
            database_name: database_name.map(ToOwned::to_owned),
            schema_name: schema_name.map(ToOwned::to_owned),
            table_name: table_name.to_string(),
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
        let users_key = table_key(connection_id, Some("postgres"), Some("public"), "users");
        let old_key = table_key(connection_id, Some("postgres"), Some("public"), "old_table");
        let mut table_details = HashMap::new();
        table_details.insert(
            users_key.clone(),
            SidebarTableDetailsData {
                fields: vec!["id".to_string()],
                ..SidebarTableDetailsData::default()
            },
        );
        table_details.insert(old_key.clone(), SidebarTableDetailsData::default());

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
                collapsed_schema_groups: HashSet::from(["public".to_string()]),
                collapsed_schema_section_keys: HashSet::from(["public::tables".to_string()]),
                tables: vec!["public.users".to_string(), "public.old_table".to_string()],
                table_details,
                expanded_table_keys: HashSet::from([users_key.clone(), old_key.clone()]),
                loading_table_keys: HashSet::from([old_key.clone()]),
                tables_expanded: true,
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
        assert!(schema.collapsed_schema_groups.contains("public"));
        assert!(
            schema
                .collapsed_schema_section_keys
                .contains("public::tables")
        );
        assert!(schema.tables_expanded);
        assert_eq!(
            schema.tables,
            vec!["public.users".to_string(), "public.accounts".to_string()]
        );
        assert!(schema.table_details.contains_key(&users_key));
        assert!(schema.expanded_table_keys.contains(&users_key));
        assert!(!schema.table_details.contains_key(&old_key));
        assert!(!schema.expanded_table_keys.contains(&old_key));
        assert!(schema.loading_table_keys.is_empty());
    }
}
