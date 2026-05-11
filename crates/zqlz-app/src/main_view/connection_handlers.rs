// Connection management methods for MainView

use gpui::*;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zqlz_core::{ConstraintType, DriverCategory, ObjectsPanelData};
use zqlz_ui::widgets::{
    ActiveTheme, WindowExt, button::ButtonVariant, dialog::DialogButtonProps, dock::PanelView,
    notification::Notification, v_flex,
};

use crate::app::AppState;
use crate::components::{
    ConnectionEntry, ConnectionSidebar, QueryEditor, SchemaDetailsPanel, TableViewerPanel,
};
use zqlz_connection::SavedConnection;
use zqlz_connection::{
    SidebarObjectCapabilities, SidebarSection, SidebarTableDetailsData, SidebarTableKey,
};

use super::{MainView, objects_panel_action_helpers::manifest_action_coverage_gaps};
use zqlz_services::{
    DiscoverDatabasesSidebarOutcome, LazySidebarSectionLoadOutcome, SidebarSectionLoadOutcome,
};

#[derive(Clone, Copy)]
struct SidebarSectionRouteContext {
    connection_id: Uuid,
    section: SidebarSection,
}

impl SidebarSectionRouteContext {
    fn new(connection_id: Uuid, section: SidebarSection) -> Self {
        Self {
            connection_id,
            section,
        }
    }
}

fn apply_loaded_sidebar_section_names(
    sidebar: &WeakEntity<ConnectionSidebar>,
    route_context: SidebarSectionRouteContext,
    names: Vec<String>,
    apply_failure_message: &'static str,
    cx: &mut AsyncWindowContext,
) {
    if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
        sidebar.set_loaded_section_names(
            route_context.connection_id,
            route_context.section,
            names,
            cx,
        );
    }) {
        tracing::warn!(
            %error,
            connection_id = %route_context.connection_id,
            section = ?route_context.section,
            "{}",
            apply_failure_message
        );
    }
}

fn clear_sidebar_section_loading_state(
    sidebar: &WeakEntity<ConnectionSidebar>,
    route_context: SidebarSectionRouteContext,
    clear_failure_message: &'static str,
    cx: &mut AsyncWindowContext,
) {
    if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
        sidebar.clear_section_loading(route_context.connection_id, route_context.section, cx);
    }) {
        tracing::warn!(
            %error,
            connection_id = %route_context.connection_id,
            section = ?route_context.section,
            "{}",
            clear_failure_message
        );
    }
}

fn parse_connection_bool_param(value: Option<&String>, default: bool) -> bool {
    value
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn clear_sidebar_table_details_loading_state(
    sidebar: &WeakEntity<ConnectionSidebar>,
    key: SidebarTableKey,
    cx: &mut AsyncWindowContext,
) {
    if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
        sidebar.clear_table_details_loading(&key, cx);
    }) {
        tracing::warn!(
            %error,
            connection_id = %key.conn_id,
            table_name = %key.table_name,
            "Failed to clear sidebar table details loading state"
        );
    }
}

fn warn_and_clear_sidebar_section_loading_state(
    sidebar: &WeakEntity<ConnectionSidebar>,
    route_context: SidebarSectionRouteContext,
    warning_message: &'static str,
    clear_failure_message: &'static str,
    error_message: &str,
    cx: &mut AsyncWindowContext,
) {
    tracing::warn!(
        connection_id = %route_context.connection_id,
        section = ?route_context.section,
        error = %error_message,
        "{}",
        warning_message
    );

    clear_sidebar_section_loading_state(sidebar, route_context, clear_failure_message, cx);
}

async fn run_relational_bootstrap_follow_up(
    connection_service: Arc<zqlz_services::ConnectionService>,
    sidebar: &WeakEntity<ConnectionSidebar>,
    connection_id: Uuid,
    cx: &mut AsyncWindowContext,
) {
    match connection_service
        .load_supported_sidebar_sections(connection_id)
        .await
    {
        Ok(section_results) => {
            for section_result in section_results {
                let route_context =
                    SidebarSectionRouteContext::new(connection_id, section_result.section);

                match section_result.outcome {
                    SidebarSectionLoadOutcome::Loaded(names) => {
                        apply_loaded_sidebar_section_names(
                            sidebar,
                            route_context,
                            names,
                            "Failed to apply loaded sidebar section names",
                            cx,
                        );
                    }
                    SidebarSectionLoadOutcome::Failed(error_message) => {
                        warn_and_clear_sidebar_section_loading_state(
                            sidebar,
                            route_context,
                            "Failed to load sidebar section during connect bootstrap",
                            "Failed to clear sidebar section loading state after section-load failure",
                            &error_message,
                            cx,
                        );
                    }
                }
            }
        }
        Err(error) => {
            tracing::warn!(
                connection_id = %connection_id,
                error = %error,
                "Failed to load supported sidebar sections"
            );
        }
    }

    tracing::info!(
        connection_id = %connection_id,
        "Sequential schema load complete"
    );
}

#[allow(clippy::too_many_arguments)]
async fn run_connection_sidebar_bootstrap(
    connection_service: Arc<zqlz_services::ConnectionService>,
    sidebar: &WeakEntity<ConnectionSidebar>,
    objects_panel: &WeakEntity<crate::components::ObjectsPanel>,
    schema_details_panel: &WeakEntity<SchemaDetailsPanel>,
    main_view: &WeakEntity<MainView>,
    connection_id: Uuid,
    connection_name: &str,
    active_database_name_from_config: Option<String>,
    is_key_value: bool,
    object_capabilities: SidebarObjectCapabilities,
    cx: &mut AsyncWindowContext,
) {
    if is_key_value {
        match connection_service
            .load_redis_sidebar_databases(connection_id)
            .await
        {
            Ok(redis_databases) => {
                let databases = redis_databases
                    .into_iter()
                    .map(|entry| (entry.index, entry.size_bytes))
                    .collect::<Vec<_>>();
                tracing::info!(
                    connection_id = %connection_id,
                    database_count = databases.len(),
                    "Redis databases loaded"
                );

                let (objects_panel_data, objects_panel_manifest) =
                    ObjectsPanelData::from_redis_databases_with_manifest(databases.clone());
                let coverage_gaps = manifest_action_coverage_gaps(&objects_panel_manifest);

                if !coverage_gaps.is_empty() {
                    tracing::error!(
                        connection_id = %connection_id,
                        database_count = databases.len(),
                        coverage_gaps = ?coverage_gaps,
                        "Redis objects panel manifest has action coverage gaps"
                    );
                }

                if let Err(error) = objects_panel.update(cx, |panel, cx| {
                    panel.load_objects(
                        connection_id,
                        connection_name.to_string(),
                        None,
                        objects_panel_data,
                        objects_panel_manifest.clone(),
                        object_capabilities,
                        cx,
                    );
                }) {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        "Failed to load Redis databases into objects panel"
                    );
                }
                if let Err(error) = sidebar.update(cx, |sidebar, cx| {
                    sidebar.set_objects_panel_manifest(connection_id, objects_panel_manifest, cx);
                }) {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        "Failed to apply Redis manifest to sidebar"
                    );
                }

                if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
                    sidebar.set_redis_databases(connection_id, databases, cx);
                }) {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        "Failed to load Redis databases into sidebar"
                    );
                }
            }
            Err(error) => {
                let error_message = error.to_string();
                tracing::warn!(
                    connection_id = %connection_id,
                    error = %error_message,
                    "Failed to list Redis databases"
                );

                if let Err(error) = sidebar.update_in(cx, |_sidebar, window, cx| {
                    window.push_notification(
                        Notification::warning(format!(
                            "Connected but failed to list databases: {}",
                            error_message
                        )),
                        cx,
                    );
                }) {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        "Failed to surface Redis database-load warning"
                    );
                }
            }
        }
        return;
    }

    tracing::info!("Starting progressive schema load");

    // Clear schema details panel when switching connections
    if let Err(error) = schema_details_panel.update(cx, |panel, cx| {
        panel.clear_if_not_connection(connection_id, cx);
    }) {
        tracing::warn!(
            %error,
            connection_id = %connection_id,
            "Failed to clear schema details panel on connect"
        );
    }

    // Show the database hierarchy immediately using the database name
    // from the saved connection config, so the multi-DB structure is
    // visible before any async queries complete. This prevents the
    // jarring flat-table-list → database-nodes restructuring.
    if let Some(database_name) = active_database_name_from_config.as_deref()
        && let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
            sidebar.init_database_view(connection_id, database_name, cx);
        })
    {
        tracing::warn!(
            %error,
            connection_id = %connection_id,
            "Failed to initialize database view in sidebar"
        );
    }

    // Fetch the full database list concurrently with the table load.
    // Spawned as a detached foreground task so that a slow
    // pg_database_size() on serverless Postgres (Neon etc.) does not
    // delay the tables section from appearing in the sidebar.
    {
        let sidebar = sidebar.clone();
        let active_database_name_from_config = active_database_name_from_config.clone();
        let connection_service = connection_service.clone();
        cx.spawn(async move |cx| {
            match connection_service
                .discover_databases_sidebar_outcome(connection_id)
                .await
            {
                DiscoverDatabasesSidebarOutcome::Loaded(databases) => {
                    let databases = databases
                        .into_iter()
                        .map(|database| (database.name, database.size_bytes))
                        .collect::<Vec<_>>();

                    if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
                        sidebar.merge_databases(
                            connection_id,
                            databases,
                            active_database_name_from_config.as_deref(),
                            cx,
                        );
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %connection_id,
                            "Failed to merge discovered databases into sidebar"
                        );
                    }
                }
                DiscoverDatabasesSidebarOutcome::NoData => {}
                DiscoverDatabasesSidebarOutcome::Failed(error) => {
                    tracing::warn!(
                        connection_id = %connection_id,
                        error = %error,
                        "Failed to list databases"
                    );
                }
            }
        })
        .detach();
    }

    // Load tables first — they are the most important objects in the sidebar.
    let table_names = match connection_service
        .load_relational_sidebar_bootstrap(connection_id)
        .await
    {
        Ok(bootstrap) => {
            tracing::info!(
                connection_id = %connection_id,
                table_count = bootstrap.tables.len(),
                "Loaded relational sidebar bootstrap tables"
            );

            apply_relational_sidebar_bootstrap_payload(
                sidebar,
                objects_panel,
                main_view,
                connection_id,
                connection_name,
                active_database_name_from_config.as_deref(),
                &bootstrap,
                cx,
            );

            {
                let connection_service = connection_service.clone();
                let objects_panel = objects_panel.clone();
                let active_database_name_from_config = active_database_name_from_config.clone();
                if let Err(error) = objects_panel.update(cx, |panel, cx| {
                    panel.set_loading(true, cx);
                }) {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        "Failed to set initial rich table objects panel loading state"
                    );
                }
                cx.spawn(async move |cx| {
                    match connection_service
                        .load_objects_panel_kind_data(
                            connection_id,
                            active_database_name_from_config,
                            "table",
                            None,
                        )
                        .await
                    {
                        Ok(data) => {
                            if let Err(error) = objects_panel.update(cx, |panel, cx| {
                                panel.set_loading(false, cx);
                                panel.replace_kind_objects("table", data, cx);
                            }) {
                                tracing::warn!(
                                    %error,
                                    connection_id = %connection_id,
                                    "Failed to apply initial rich table objects panel data"
                                );
                            }
                        }
                        Err(error) => {
                            if let Err(clear_error) = objects_panel.update(cx, |panel, cx| {
                                panel.set_loading(false, cx);
                            }) {
                                tracing::warn!(
                                    %clear_error,
                                    connection_id = %connection_id,
                                    "Failed to clear initial rich table objects panel loading state"
                                );
                            }
                            tracing::warn!(
                                %error,
                                connection_id = %connection_id,
                                "Failed to load initial rich table objects panel data"
                            );
                        }
                    }
                })
                .detach();
            }

            Some(bootstrap.table_names)
        }
        Err(error) => {
            let error_message = error.to_string();
            tracing::warn!(
                connection_id = %connection_id,
                error = %error_message,
                "Failed to load tables"
            );

            if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
                sidebar.clear_section_loading(connection_id, SidebarSection::Tables, cx);
                if let Some(database_name) = active_database_name_from_config.as_deref() {
                    sidebar.set_database_loading(connection_id, database_name, false, cx);
                }
            }) {
                tracing::warn!(
                    %error,
                    connection_id = %connection_id,
                    "Failed to clear sidebar loading state after table-load failure"
                );
            }

            None
        }
    };

    if table_names.is_some() {
        run_relational_bootstrap_follow_up(connection_service, sidebar, connection_id, cx).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_relational_sidebar_bootstrap_payload(
    sidebar: &WeakEntity<ConnectionSidebar>,
    objects_panel: &WeakEntity<crate::components::ObjectsPanel>,
    main_view: &WeakEntity<MainView>,
    connection_id: Uuid,
    connection_name: &str,
    active_database_name_from_config: Option<&str>,
    bootstrap: &zqlz_services::RelationalSidebarBootstrap,
    cx: &mut AsyncWindowContext,
) {
    let table_names = bootstrap.table_names.clone();

    if let Err(error) = sidebar.update_in(cx, |sidebar, _window, cx| {
        sidebar.set_tables_only(
            connection_id,
            table_names.clone(),
            bootstrap.resolved_schema_name.clone(),
            bootstrap.schema_names.clone(),
            cx,
        );
        sidebar.set_objects_panel_manifest(
            connection_id,
            bootstrap.objects_panel_manifest.clone(),
            cx,
        );
        if let Some(database_name) = active_database_name_from_config {
            sidebar.set_database_loading(connection_id, database_name, false, cx);
        }
    }) {
        tracing::warn!(
            %error,
            connection_id = %connection_id,
            "Failed to apply initial tables payload to sidebar"
        );
    }

    if let Err(error) = objects_panel.update(cx, |panel, cx| {
        let coverage_gaps = manifest_action_coverage_gaps(&bootstrap.objects_panel_manifest);

        if !coverage_gaps.is_empty() {
            tracing::error!(
                connection_id = %connection_id,
                coverage_gaps = ?coverage_gaps,
                "Relational objects panel manifest has action coverage gaps"
            );
        }

        panel.load_objects(
            connection_id,
            connection_name.to_string(),
            None,
            bootstrap.objects_panel_data.clone(),
            bootstrap.objects_panel_manifest.clone(),
            bootstrap.object_capabilities,
            cx,
        );
    }) {
        tracing::warn!(
            %error,
            connection_id = %connection_id,
            "Failed to load objects panel data after connect"
        );
    }

    if let Err(error) = main_view.update(cx, |main_view, cx| {
        main_view.for_each_live_query_editor(cx, |editor, cx| {
            let table_names = table_names.clone();
            editor.update(cx, |query_editor, cx| {
                query_editor.notify_tables_available(table_names, cx);
            });
        });
    }) {
        tracing::warn!(
            %error,
            connection_id = %connection_id,
            "Failed to notify query editors about available tables"
        );
    }
}

impl MainView {
    pub(super) fn close_all_connections(&mut self, cx: &mut Context<Self>) {
        let connected_ids = {
            let connections = self.connection_sidebar.read(cx).connections();
            connections
                .iter()
                .filter(|connection| connection.is_connected)
                .map(|connection| connection.id)
                .collect::<Vec<_>>()
        };

        for connection_id in connected_ids {
            self.disconnect_from_database(connection_id, cx);
        }
    }

    fn set_connection_connecting_state(
        &mut self,
        connection_id: Uuid,
        is_connecting: bool,
        cx: &mut Context<Self>,
    ) {
        self.workspace_state.update(cx, |state, _cx| {
            state.set_connecting(connection_id, is_connecting);
        });

        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connecting(connection_id, is_connecting, cx);
        });
    }

    fn set_connection_connected_state(
        &mut self,
        connection_id: Uuid,
        is_connected: bool,
        cx: &mut Context<Self>,
    ) {
        self.workspace_state.update(cx, |state, cx| {
            state.set_connection_status(connection_id, is_connected, cx);
        });

        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connected(connection_id, is_connected, cx);
        });
    }

    fn apply_post_connect_state(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connected(connection_id, true, cx);
            // Mark all sections as loading immediately so headers appear with
            // spinners before any schema queries complete.
            sidebar.set_all_sections_loading(connection_id, cx);
        });

        self.workspace_state.update(cx, |state, cx| {
            state.set_connecting(connection_id, false);
            state.set_connection_status(connection_id, true, cx);
            state.set_active_connection(Some(connection_id), cx);
        });
    }

    fn for_each_live_query_editor(
        &mut self,
        cx: &mut Context<Self>,
        mut visit: impl FnMut(Entity<QueryEditor>, &mut Context<Self>),
    ) -> usize {
        let mut live_editors = Vec::with_capacity(self.query_editors.len());
        let mut visited_count = 0;

        for weak_editor in self.query_editors.drain(..) {
            if let Some(editor) = weak_editor.upgrade() {
                visited_count += 1;
                visit(editor.clone(), cx);
                live_editors.push(editor.downgrade());
            }
        }

        self.query_editors = live_editors;
        visited_count
    }

    fn apply_connected_connection_to_live_query_editors(
        &mut self,
        connection_id: Uuid,
        connection_name: &str,
        connection: &Arc<dyn zqlz_core::Connection>,
        driver_type: &str,
        cx: &mut Context<Self>,
    ) -> usize {
        self.for_each_live_query_editor(cx, |editor, cx| {
            let connection = connection.clone();
            editor.update(cx, |query_editor, cx| {
                query_editor.set_connection(
                    Some(connection_id),
                    Some(connection_name.to_string()),
                    Some(connection),
                    Some(driver_type.to_string()),
                    cx,
                );
            });
        })
    }

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

        // Use ConnectionService for connecting and loading schema
        let connection_service = app_state.connection_service.clone();
        let saved = match connection_service.get_saved_connection(id) {
            Ok(saved_connection) => saved_connection,
            Err(error) => {
                tracing::error!(%error, connection_id = %id, "Connection not found");
                return;
            }
        };
        let sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let schema_details_panel = self.schema_details_panel.downgrade();
        let driver_type = saved.driver.clone(); // Capture driver type for LSP
        let connection_name = saved.name.clone(); // Capture connection name for UI
        // Known up-front from the saved config; used to pre-populate the sidebar
        // with the active database node before any async queries complete.
        let active_db_from_config = saved.params.get("database").cloned();
        let load_schema_on_connect = parse_connection_bool_param(
            saved.params.get("load_schema_on_connect"),
            zqlz_settings::ZqlzSettings::global(cx)
                .connections
                .fetch_schema_on_connect,
        );

        // Set connecting state immediately
        self.set_connection_connecting_state(id, true, cx);

        cx.spawn_in(window, async move |this, cx| {
            tracing::info!(
                "Fast connecting to: {} (driver: {})",
                connection_name,
                driver_type
            );

            // Step 1: Connect immediately without waiting for schema
            match connection_service.connect_fast_and_resolve(&saved).await {
                Ok((conn_info, conn)) => {
                    let conn_id = conn_info.id;
                    tracing::info!("Connected successfully (fast): {}", conn_id);

                    // Step 2: Immediately show connected state in UI
                    if let Err(error) = this.update(cx, |main_view, cx| {
                        main_view.apply_post_connect_state(conn_id, cx);
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %conn_id,
                            "Failed to apply post-connect UI state"
                        );
                    }

                    if let Err(error) = this.update_in(cx, |main_view, window, cx| {
                        main_view.restore_workspace_session_viewer_tabs_for_connection(
                            conn_id, window, cx,
                        );
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %conn_id,
                            "Failed to restore session viewer tabs after connection"
                        );
                    }

                    // Update all existing QueryEditor panels with the new connection
                    if let Err(error) = this.update(cx, |main_view, cx| {
                        let updated_count = main_view
                            .apply_connected_connection_to_live_query_editors(
                                conn_id,
                                &connection_name,
                                &conn,
                                &driver_type,
                                cx,
                            );

                        tracing::info!(
                            "Updated {} QueryEditor panels with connection",
                            updated_count
                        );
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %conn_id,
                            "Failed to update query editors after connection"
                        );
                    }

                    // Keep connected-sidebar saved-query hydration on query facade boundaries so
                    // connection handlers stay focused on connection lifecycle orchestration.
                    if let Err(error) = this.update(cx, |main_view, cx| {
                        main_view.query_facade_hydrate_connected_sidebar_saved_queries(conn_id, cx);
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %conn_id,
                            "Failed to hydrate connected-sidebar saved queries"
                        );
                    }

                    if load_schema_on_connect {
                        // Step 3: Load schema in background (slow operation)
                        tracing::info!(
                            "Starting background schema load for connection {}",
                            conn_id
                        );

                        run_connection_sidebar_bootstrap(
                            connection_service.clone(),
                            &sidebar,
                            &objects_panel,
                            &schema_details_panel,
                            &this,
                            conn_id,
                            &connection_name,
                            active_db_from_config.clone(),
                            conn.driver_category() == DriverCategory::KeyValue,
                            SidebarObjectCapabilities::for_connection(conn.as_ref()),
                            cx,
                        )
                        .await;
                    } else {
                        tracing::info!(
                            connection_id = %conn_id,
                            "Skipping schema load on connect"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect: {}", e);

                    // Clear connecting state in both workspace and sidebar.
                    if let Err(error) = this.update(cx, |main_view, cx| {
                        main_view.set_connection_connecting_state(id, false, cx);
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %id,
                            "Failed to clear connecting state after connection failure"
                        );
                    }

                    // Show notification to user
                    if let Err(error) = this.update_in(cx, |_this, window, cx| {
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to connect to '{}': {}",
                                connection_name, e
                            )),
                            cx,
                        );
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %id,
                            "Failed to surface connection failure notification"
                        );
                    }
                }
            }
        })
        .detach();
    }

    /// Disconnect from a database
    pub(super) fn disconnect_from_database(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let connection_service = app_state.connection_service.clone();
        let schema_details_panel = self.schema_details_panel.downgrade();
        let objects_panel = self.objects_panel.downgrade();

        cx.spawn(async move |this, cx| {
            tracing::info!("Disconnecting: {}", id);

            // Use ConnectionService which handles both disconnection and cache invalidation
            if let Err(e) = connection_service.disconnect(id).await {
                tracing::error!("Failed to disconnect: {}", e);
                // Error is already logged by the service layer
            } else {
                tracing::info!("Disconnected successfully: {}", id);
            }

            if let Err(error) = this.update(cx, |main_view, cx| {
                main_view.set_connection_connected_state(id, false, cx);
            }) {
                tracing::warn!(
                    %error,
                    connection_id = %id,
                    "Failed to update connected state after disconnect"
                );
            }

            // Clear schema details if it was showing details from the disconnected connection
            if let Err(error) = schema_details_panel.update(cx, |panel, cx| {
                if panel.active_connection() == Some(id) {
                    panel.clear(cx);
                }
            }) {
                tracing::warn!(
                    %error,
                    connection_id = %id,
                    "Failed to clear schema details panel after disconnect"
                );
            }

            // Clear objects panel if it was showing objects from the disconnected connection
            if let Err(error) = objects_panel.update(cx, |panel, cx| {
                if panel.selected_connection_id() == Some(id) {
                    panel.clear(cx);
                }
            }) {
                tracing::warn!(
                    %error,
                    connection_id = %id,
                    "Failed to clear objects panel after disconnect"
                );
            }
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

        let saved = match app_state.connection_service.get_saved_connection(id) {
            Ok(saved_connection) => saved_connection,
            Err(error) => {
                tracing::error!(%error, connection_id = %id, "Connection not found");
                return;
            }
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
                        // This dialog API stores button intent as data before render, which is why
                        // the destructive action remains a ButtonVariant here.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _, cx| {
                    if let Some(app_state) = cx.try_global::<AppState>() {
                        app_state.delete_connection(id);
                    }

                    if let Err(error) = sidebar.update(cx, |sidebar, cx| {
                        sidebar.remove_connection(id, cx);
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %id,
                            "Failed to remove deleted connection from sidebar"
                        );
                    }

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
        let saved = match app_state.connection_service.get_saved_connection(id) {
            Ok(saved_connection) => saved_connection,
            Err(error) => {
                tracing::error!(%error, connection_id = %id, "Connection not found");
                return;
            }
        };

        let new_connection = app_state
            .connection_service
            .build_duplicate_saved_connection(&saved);

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
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let saved = match app_state.connection_service.get_saved_connection(id) {
            Ok(saved_connection) => saved_connection,
            Err(error) => {
                tracing::error!(%error, connection_id = %id, "Connection not found");
                return;
            }
        };

        if !app_state.is_connected(id) {
            super::connection_window::ConnectionWindow::open_for_edit(saved, cx);
            return;
        }

        self.close_connected_connection_for_settings(id, saved, window, cx);
    }

    fn close_connected_connection_for_settings(
        &mut self,
        id: Uuid,
        saved: SavedConnection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let panels = self.connection_scoped_center_panels(id, cx);
        let dirty_count = panels
            .iter()
            .filter(|panel| panel.has_unsaved_changes(cx))
            .count();

        if dirty_count > 0 {
            let main_view = cx.entity().downgrade();
            window.open_dialog(cx, move |dialog, _window, _cx| {
                let main_view = main_view.clone();
                let saved = saved.clone();
                dialog
                    .title("Close Connection Tabs?")
                    .child(div().text_sm().child(format!(
                        "Opening settings will disconnect this database and close {} tab{} with unsaved changes.",
                        dirty_count,
                        if dirty_count == 1 { "" } else { "s" }
                    )))
                    .button_props(
                        DialogButtonProps::default()
                            .ok_text("Discard & Open Settings")
                            .ok_variant(ButtonVariant::Danger)
                            .cancel_text("Cancel"),
                    )
                    .on_ok(move |_, window, cx| {
                        if let Err(error) = main_view.update(cx, |main_view, cx| {
                            main_view.finish_open_connected_connection_settings(
                                id,
                                saved.clone(),
                                window,
                                cx,
                            );
                        }) {
                            tracing::warn!(
                                %error,
                                connection_id = %id,
                                "Failed to continue opening connection settings"
                            );
                        }
                        true
                    })
                    .confirm()
            });
            return;
        }

        self.finish_open_connected_connection_settings(id, saved, window, cx);
    }

    fn finish_open_connected_connection_settings(
        &mut self,
        id: Uuid,
        saved: SavedConnection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.force_close_connection_center_tabs(id, window, cx);
        self.workspace_state.update(cx, |state, cx| {
            if state.active_connection_id() == Some(id) {
                state.set_active_connection(None, cx);
            }
        });
        self.disconnect_from_database(id, cx);
        window.push_notification(
            Notification::info("Connection closed before editing settings"),
            cx,
        );
        super::connection_window::ConnectionWindow::open_for_edit(saved, cx);
    }

    fn connection_scoped_center_panels(&self, id: Uuid, cx: &App) -> Vec<Arc<dyn PanelView>> {
        self.workspace_controller
            .read(cx)
            .center_panels(cx)
            .into_iter()
            .filter(|panel| Self::panel_belongs_to_connection(panel, id, cx))
            .collect()
    }

    fn panel_belongs_to_connection(panel: &Arc<dyn PanelView>, id: Uuid, cx: &App) -> bool {
        if let Ok(editor) = panel.view().downcast::<QueryEditor>()
            && editor.read(cx).connection_id() == Some(id)
        {
            return true;
        }

        if let Ok(viewer) = panel.view().downcast::<TableViewerPanel>()
            && viewer.read(cx).connection_id() == Some(id)
        {
            return true;
        }

        false
    }

    fn force_close_connection_center_tabs(
        &mut self,
        id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let panels = self.connection_scoped_center_panels(id, cx);
        if panels.is_empty() {
            return;
        }

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.remove_panels_from_all_docks(panels, window, cx);
        });
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

    pub(super) fn import_database_file_and_open_query(
        &mut self,
        path: &Path,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let path_string = path.to_string_lossy().into_owned();
        let imported = match app_state
            .connection_service
            .import_saved_connection_from_external_target(&path_string)
        {
            Ok(Some(connection)) => connection,
            Ok(None) => {
                window.push_notification(
                    Notification::warning("That file is not a supported database file"),
                    cx,
                );
                return;
            }
            Err(error) => {
                window.push_notification(
                    Notification::error(format!("Failed to import database file: {error}")),
                    cx,
                );
                return;
            }
        };

        let connection_id = imported.id;
        let connection_name = imported.name.clone();

        app_state.save_connection(imported);
        self.refresh_connections_list_preserving_state(cx);

        self.workspace_state.update(cx, |state, cx| {
            state.set_active_connection(Some(connection_id), cx);
        });

        self.connect_to_database(connection_id, window, cx);

        cx.spawn_in(window, async move |this, cx| {
            cx.background_spawn(async move {
                smol::Timer::after(Duration::from_millis(150)).await;
            })
            .await;

                if let Err(error) = this.update_in(cx, |this, window, cx| {
                    this.create_new_query_editor(window, cx);
                }) {
                    tracing::warn!(%error, connection_id = %connection_id, "failed to open query after importing database file");
                }
        })
        .detach();

        window.push_notification(
            Notification::success(format!("Opened database file as {connection_name}")),
            cx,
        );
    }

    /// Fetch a single sidebar section on demand (lazy loading).
    ///
    /// Called when the user first expands a section that has never been loaded.
    pub(super) fn load_sidebar_section(
        &mut self,
        connection_id: Uuid,
        section: SidebarSection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let sidebar = self.connection_sidebar.downgrade();
        let connection_service = app_state.connection_service.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let section_load_result = connection_service
                .load_sidebar_section_lazy_outcome(connection_id, section)
                .await;
            let route_context = SidebarSectionRouteContext::new(connection_id, section);

            match section_load_result {
                LazySidebarSectionLoadOutcome::Loaded(names) => {
                    let loaded_count = names.len();
                    tracing::info!(
                        connection_id = %route_context.connection_id,
                        section = ?route_context.section,
                        loaded_count,
                        "Lazy-loaded sidebar section entries"
                    );

                    apply_loaded_sidebar_section_names(
                        &sidebar,
                        route_context,
                        names,
                        "Failed to apply lazy-loaded sidebar section entries",
                        cx,
                    );
                }
                LazySidebarSectionLoadOutcome::NoData => {
                    clear_sidebar_section_loading_state(
                        &sidebar,
                        route_context,
                        "Failed to clear sidebar section loading state",
                        cx,
                    );
                }
                LazySidebarSectionLoadOutcome::Failed(error) => {
                    warn_and_clear_sidebar_section_loading_state(
                        &sidebar,
                        route_context,
                        "Failed to lazy-load sidebar section",
                        "Failed to clear sidebar loading state after lazy-load error",
                        &error,
                        cx,
                    );
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    pub(super) fn load_sidebar_table_details(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let sidebar = self.connection_sidebar.downgrade();
        let connection_service = app_state.connection_service.clone();
        let schema_service = app_state.schema_service.clone();
        let table_key = SidebarTableKey {
            conn_id: connection_id,
            database_name: database_name.clone(),
            schema_name: object_schema.clone(),
            table_name: table_name.clone(),
        };

        cx.spawn_in(window, async move |_this, cx| {
            let connection = database_name
                .as_deref()
                .and_then(|database_name| {
                    connection_service
                        .get_connection_for_database_cached(connection_id, Some(database_name))
                })
                .or_else(|| connection_service.get_connection(connection_id));

            let Some(connection) = connection else {
                clear_sidebar_table_details_loading_state(&sidebar, table_key, cx);
                return anyhow::Ok(());
            };

            let details_result = schema_service
                .get_table_details(
                    connection,
                    connection_id,
                    table_name.as_str(),
                    object_schema.as_deref(),
                )
                .await;

            match details_result {
                Ok(details) => {
                    let mut detail_data = SidebarTableDetailsData {
                        fields: details
                            .columns
                            .into_iter()
                            .map(|column| format!("{} {}", column.name, column.data_type))
                            .collect(),
                        indexes: details
                            .indexes
                            .into_iter()
                            .map(|index| index.name)
                            .collect(),
                        foreign_keys: details
                            .foreign_keys
                            .into_iter()
                            .map(|foreign_key| foreign_key.name)
                            .collect(),
                        uniques: Vec::new(),
                        checks: Vec::new(),
                        excludes: Vec::new(),
                        triggers: details
                            .triggers
                            .into_iter()
                            .map(|trigger| trigger.name)
                            .collect(),
                    };

                    for constraint in details.constraints {
                        match constraint.constraint_type {
                            ConstraintType::Unique => detail_data.uniques.push(constraint.name),
                            ConstraintType::Check => detail_data.checks.push(constraint.name),
                            ConstraintType::Exclusion => detail_data.excludes.push(constraint.name),
                            ConstraintType::PrimaryKey | ConstraintType::ForeignKey => {}
                        }
                    }

                    if let Err(error) = sidebar.update(cx, |sidebar, cx| {
                        sidebar.set_table_details(table_key, detail_data, cx);
                    }) {
                        tracing::warn!(%error, "Failed to apply sidebar table details");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "Failed to load sidebar table details");
                    clear_sidebar_table_details_loading_state(&sidebar, table_key, cx);
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }
}
