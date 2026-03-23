use std::collections::HashMap;

use gpui::*;
use uuid::Uuid;
use zqlz_connection::{ConnectionEntry, SchemaObjects, SidebarObjectCapabilities};
use zqlz_core::ObjectsPanelData;
use zqlz_services::{ConnectionRefreshPayload, RefreshRequest};

use crate::app::AppState;
use crate::main_view::MainView;

#[derive(Clone, Copy, Debug)]
pub(super) enum RefreshTarget {
    ActiveConnection,
    Connection(Uuid),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SurfaceRefreshOptions {
    pub invalidate_schema_cache: bool,
    pub refresh_sidebar: bool,
    pub refresh_objects_panel: bool,
}

impl SurfaceRefreshOptions {
    pub const MANUAL: Self = Self {
        invalidate_schema_cache: true,
        refresh_sidebar: true,
        refresh_objects_panel: true,
    };

    pub const OBJECTS_ONLY: Self = Self {
        invalidate_schema_cache: true,
        refresh_sidebar: false,
        refresh_objects_panel: true,
    };

    pub const SIDEBAR_AND_OBJECTS: Self = Self {
        invalidate_schema_cache: true,
        refresh_sidebar: true,
        refresh_objects_panel: true,
    };
}

impl MainView {
    fn resolve_objects_panel_database_name(
        panel_database_name: Option<String>,
        schema_database_name: Option<String>,
    ) -> Option<String> {
        schema_database_name.or(panel_database_name)
    }

    pub(super) fn refresh_connection_surfaces(
        &mut self,
        target: RefreshTarget,
        options: SurfaceRefreshOptions,
        cx: &mut Context<Self>,
    ) {
        let connection_id = match target {
            RefreshTarget::ActiveConnection => self.workspace_state.read(cx).active_connection_id(),
            RefreshTarget::Connection(connection_id) => Some(connection_id),
        };

        let Some(connection_id) = connection_id else {
            tracing::debug!("refresh_connection_surfaces: no connection selected");
            if options.refresh_objects_panel {
                self.objects_panel.update(cx, |panel, cx| panel.clear(cx));
            }
            return;
        };

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("refresh_connection_surfaces: no AppState available");
            return;
        };

        let refresh_service = app_state.refresh_service.clone();
        let target_database = app_state.active_database();
        let connection_name = app_state
            .connection_manager()
            .get_saved(connection_id)
            .map(|saved| saved.name)
            .unwrap_or_else(|| "Unknown".to_string());
        let object_capabilities = app_state
            .connections
            .get(connection_id)
            .map(|connection| SidebarObjectCapabilities::for_connection(connection.as_ref()))
            .unwrap_or_default();
        let workspace_state = self.workspace_state.downgrade();
        let sidebar = self.connection_sidebar.clone();
        let objects_panel = self.objects_panel.clone();

        cx.spawn(async move |_this, cx| {
            let refresh = refresh_service
                .refresh_connection(RefreshRequest {
                    connection_id,
                    invalidate_schema_cache: options.invalidate_schema_cache,
                    target_database: target_database.clone(),
                })
                .await;

            match refresh {
                Ok(refresh) => match refresh.payload {
                    ConnectionRefreshPayload::Relational(payload) => {
                        let zqlz_services::RelationalConnectionRefresh {
                            schema,
                            databases,
                            driver_category,
                        } = *payload;

                        if options.refresh_sidebar {
                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.set_schema(
                                    connection_id,
                                    SchemaObjects {
                                        tables: schema.tables.clone(),
                                        views: schema.views.clone(),
                                        materialized_views: schema.materialized_views.clone(),
                                        triggers: schema.triggers.clone(),
                                        functions: schema.functions.clone(),
                                        procedures: schema.procedures.clone(),
                                        schema_name: schema.schema_name.clone(),
                                        schema_names: schema.schema_names.clone(),
                                    },
                                    cx,
                                );

                                if let Some(databases) = &databases {
                                    sidebar.merge_databases(
                                        connection_id,
                                        databases.clone(),
                                        schema.database_name.as_deref(),
                                        cx,
                                    );

                                    if let Some(active_database) = schema.database_name.as_deref() {
                                        sidebar.set_database_schema(
                                            connection_id,
                                            active_database,
                                            SchemaObjects {
                                                tables: schema.tables.clone(),
                                                views: schema.views.clone(),
                                                materialized_views: schema
                                                    .materialized_views
                                                    .clone(),
                                                triggers: schema.triggers.clone(),
                                                functions: schema.functions.clone(),
                                                procedures: schema.procedures.clone(),
                                                schema_name: schema.schema_name.clone(),
                                                schema_names: schema.schema_names.clone(),
                                            },
                                            cx,
                                        );
                                    }
                                }
                            });
                        }

                        let should_update_objects = options.refresh_objects_panel
                            && workspace_state
                                .read_with(cx, |state, _cx| state.active_connection_id())
                                .ok()
                                .flatten()
                                == Some(connection_id);

                        if should_update_objects {
                            let objects_data = schema.objects_panel_data.unwrap_or_else(|| {
                                ObjectsPanelData::from_table_infos(schema.table_infos)
                            });
                            objects_panel.update(cx, |panel, cx| {
                                let database_name = Self::resolve_objects_panel_database_name(
                                    panel.database_name(),
                                    schema.database_name.clone(),
                                );
                                panel.load_objects(
                                    connection_id,
                                    connection_name.clone(),
                                    database_name,
                                    objects_data,
                                    driver_category,
                                    object_capabilities,
                                    cx,
                                );
                            });
                        }
                    }
                    ConnectionRefreshPayload::Redis(payload) => {
                        if options.refresh_sidebar {
                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.set_redis_databases(
                                    connection_id,
                                    payload.databases.clone(),
                                    cx,
                                );
                            });
                        }

                        let should_update_objects = options.refresh_objects_panel
                            && workspace_state
                                .read_with(cx, |state, _cx| state.active_connection_id())
                                .ok()
                                .flatten()
                                == Some(connection_id);

                        if should_update_objects {
                            objects_panel.update(cx, |panel, cx| {
                                panel.load_redis_databases(
                                    connection_id,
                                    connection_name.clone(),
                                    payload.databases.clone(),
                                    cx,
                                );
                            });
                        }
                    }
                },
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        %error,
                        "Failed to refresh connection surfaces"
                    );
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    pub(super) fn refresh_connections_list_preserving_state(&mut self, cx: &mut Context<Self>) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let saved = app_state.saved_connections();
        let current_entries: HashMap<Uuid, ConnectionEntry> = self
            .connection_sidebar
            .read(cx)
            .connections()
            .iter()
            .map(|connection| (connection.id, connection.clone()))
            .collect();

        let entries: Vec<_> = saved
            .into_iter()
            .map(|saved_connection| {
                if let Some(existing) = current_entries.get(&saved_connection.id) {
                    let mut entry = existing.clone();
                    entry.name = saved_connection.name;
                    entry.set_db_type(saved_connection.driver);
                    entry
                } else {
                    ConnectionEntry::new(
                        saved_connection.id,
                        saved_connection.name,
                        saved_connection.driver,
                    )
                }
            })
            .collect();

        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connections(entries, cx);
        });
    }
}
