use std::collections::HashMap;

use gpui::*;
use uuid::Uuid;
use zqlz_connection::{ConnectionEntry, SchemaObjects};
use zqlz_core::{ObjectsPanelData, ObjectsPanelManifest};
use zqlz_services::{ConnectionRefreshPayload, RefreshRequest, ServiceError};

use crate::app::AppState;
use crate::main_view::{MainView, objects_panel_action_helpers::manifest_action_coverage_gaps};

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
    pub refresh_database_list: bool,
}

impl SurfaceRefreshOptions {
    pub const SIDEBAR_AND_OBJECTS: Self = Self {
        invalidate_schema_cache: true,
        refresh_sidebar: true,
        refresh_objects_panel: true,
        refresh_database_list: false,
    };

    pub const SELECTION_SYNC_OBJECTS_ONLY: Self = Self {
        invalidate_schema_cache: false,
        refresh_sidebar: false,
        refresh_objects_panel: true,
        refresh_database_list: false,
    };

    pub const CONNECTIONS_LIST: Self = Self {
        invalidate_schema_cache: true,
        refresh_sidebar: true,
        refresh_objects_panel: true,
        refresh_database_list: true,
    };
}

impl MainView {
    fn resolve_objects_panel_database_name(
        panel_database_name: Option<String>,
        schema_database_name: Option<String>,
    ) -> Option<String> {
        schema_database_name.or(panel_database_name)
    }

    fn resolve_sidebar_database_name(
        schema_database_name: Option<String>,
        requested_database_name: Option<String>,
        existing_active_database_name: Option<String>,
        available_databases: &[(String, Option<i64>)],
    ) -> Option<String> {
        schema_database_name
            .or(requested_database_name)
            .or(existing_active_database_name)
            .or_else(|| {
                if available_databases.len() == 1 {
                    Some(available_databases[0].0.clone())
                } else {
                    None
                }
            })
    }

    fn resolve_refresh_database_name(
        schema_database_name: Option<String>,
        requested_database_name: Option<String>,
        existing_active_database_name: Option<String>,
        available_databases: Option<&[(String, Option<i64>)]>,
    ) -> Option<String> {
        if let Some(available_databases) = available_databases {
            return Self::resolve_sidebar_database_name(
                schema_database_name,
                requested_database_name,
                existing_active_database_name,
                available_databases,
            );
        }

        schema_database_name
            .or(requested_database_name)
            .or(existing_active_database_name)
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
        let connection_service = app_state.connection_service.clone();
        let workspace_state = self.workspace_state.downgrade();
        let target_database = workspace_state
            .read_with(cx, |state, _cx| state.active_database().map(str::to_owned))
            .ok()
            .flatten();
        let connection_name = app_state
            .connection_service
            .get_saved_connection(connection_id)
            .ok()
            .map(|saved| saved.name)
            .unwrap_or_else(|| "Unknown".to_string());
        let sidebar = self.connection_sidebar.clone();
        let objects_panel = self.objects_panel.clone();

        cx.spawn(async move |_this, cx| {
            let refresh = refresh_service
                .refresh_connection(RefreshRequest {
                    connection_id,
                    invalidate_schema_cache: options.invalidate_schema_cache,
                    target_database: target_database.clone(),
                    refresh_database_list: options.refresh_database_list,
                })
                .await;

            if refresh.is_ok() {
                match connection_service
                    .connection_feature_set(connection_id, target_database.clone())
                    .await
                {
                    Ok(feature_set) => {
                        if let Err(error) = workspace_state.update(cx, |state, cx| {
                            state.set_connection_feature_set(
                                connection_id,
                                target_database.clone(),
                                feature_set,
                                cx,
                            );
                        }) {
                            tracing::warn!(
                                %error,
                                connection_id = %connection_id,
                                "Failed to cache connection feature set"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            connection_id = %connection_id,
                            %error,
                            "Failed to derive connection feature set during refresh"
                        );
                    }
                }
            }

            match refresh {
                Ok(refresh) => match refresh.payload {
                    ConnectionRefreshPayload::Relational(payload) => {
                        let zqlz_services::RelationalConnectionRefresh {
                            schema,
                            databases,
                            driver_category: _,
                            object_capabilities,
                        } = *payload;

                        let schema_objects = SchemaObjects {
                            tables: schema.tables.clone(),
                            views: schema.views.clone(),
                            materialized_views: schema.materialized_views.clone(),
                            triggers: schema.triggers.clone(),
                            functions: schema.functions.clone(),
                            procedures: schema.procedures.clone(),
                            events: schema.events.clone(),
                            sequences: schema.sequences.clone(),
                            domains: schema.domains.clone(),
                            types: schema.types.clone(),
                            extensions: schema.extensions.clone(),
                            schema_name: schema.schema_name.clone(),
                            schema_names: schema.schema_names.clone(),
                        };

                        if options.refresh_sidebar {
                            sidebar.update(cx, |sidebar, cx| {
                                let existing_active_database_name = sidebar
                                    .connections()
                                    .iter()
                                    .find(|connection| connection.id == connection_id)
                                    .and_then(|connection| {
                                        connection
                                            .databases
                                            .iter()
                                            .find(|database| database.is_active)
                                            .map(|database| database.name.clone())
                                    });
                                let resolved_database_name = Self::resolve_refresh_database_name(
                                    schema.database_name.clone(),
                                    target_database.clone(),
                                    existing_active_database_name,
                                    databases.as_deref(),
                                );

                                sidebar.set_schema(connection_id, schema_objects.clone(), cx);
                                if let Some(objects_panel_manifest) =
                                    schema.objects_panel_manifest.clone()
                                {
                                    sidebar.set_objects_panel_manifest(
                                        connection_id,
                                        objects_panel_manifest,
                                        cx,
                                    );
                                }
                                if let Some(database_name) = resolved_database_name.as_deref() {
                                    sidebar.apply_database_schema(
                                        connection_id,
                                        database_name,
                                        schema_objects.clone(),
                                        cx,
                                    );
                                }

                                if let Some(databases) = &databases {
                                    sidebar.merge_databases(
                                        connection_id,
                                        databases.clone(),
                                        resolved_database_name.as_deref(),
                                        cx,
                                    );

                                    if let Some(database_name) = resolved_database_name.as_deref() {
                                        sidebar.apply_database_schema(
                                            connection_id,
                                            database_name,
                                            schema_objects.clone(),
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
                            let objects_manifest = schema
                                .objects_panel_manifest
                                .unwrap_or_else(|| ObjectsPanelManifest::from_data(&objects_data));

                            let coverage_gaps = manifest_action_coverage_gaps(&objects_manifest);

                            if !coverage_gaps.is_empty() {
                                tracing::error!(
                                    connection_id = %connection_id,
                                    coverage_gaps = ?coverage_gaps,
                                    "Relational refresh manifest has action coverage gaps"
                                );
                            }

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
                                    objects_manifest,
                                    object_capabilities,
                                    cx,
                                );
                            });
                        }
                    }
                    ConnectionRefreshPayload::KeyValue(payload) => {
                        if options.refresh_sidebar {
                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.set_redis_databases(
                                    connection_id,
                                    payload.databases.clone(),
                                    cx,
                                );
                                sidebar.set_objects_panel_manifest(
                                    connection_id,
                                    payload.objects_panel_manifest.clone(),
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
                                let objects_panel_data = payload.objects_panel_data.clone();
                                let objects_panel_manifest = payload.objects_panel_manifest.clone();

                                let coverage_gaps =
                                    manifest_action_coverage_gaps(&objects_panel_manifest);

                                if !coverage_gaps.is_empty() {
                                    tracing::error!(
                                        connection_id = %connection_id,
                                        coverage_gaps = ?coverage_gaps,
                                        "Redis refresh manifest has action coverage gaps"
                                    );
                                }

                                panel.load_objects(
                                    connection_id,
                                    connection_name.clone(),
                                    None,
                                    objects_panel_data,
                                    objects_panel_manifest,
                                    payload.object_capabilities,
                                    cx,
                                );
                            });
                        }
                    }
                    ConnectionRefreshPayload::Document(payload) => {
                        if options.refresh_sidebar {
                            sidebar.update(cx, |sidebar, cx| {
                                sidebar.set_databases(
                                    connection_id,
                                    payload.databases.clone(),
                                    None,
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
                                let objects_panel_data = payload.objects_panel_data.clone();
                                let objects_panel_manifest = payload.objects_panel_manifest.clone();

                                let coverage_gaps =
                                    manifest_action_coverage_gaps(&objects_panel_manifest);

                                if !coverage_gaps.is_empty() {
                                    tracing::error!(
                                        connection_id = %connection_id,
                                        coverage_gaps = ?coverage_gaps,
                                        "Document refresh manifest has action coverage gaps"
                                    );
                                }

                                panel.load_objects(
                                    connection_id,
                                    connection_name.clone(),
                                    None,
                                    objects_panel_data,
                                    objects_panel_manifest,
                                    payload.object_capabilities,
                                    cx,
                                );
                            });
                        }
                    }
                },
                Err(ServiceError::ConnectionNotFound) => {
                    tracing::debug!(
                        connection_id = %connection_id,
                        "Skipped refresh for stale connection selection"
                    );

                    if options.refresh_objects_panel {
                        objects_panel.update(cx, |panel, cx| panel.clear(cx));
                    }

                    if let Err(error) = workspace_state.update(cx, |state, cx| {
                        if state.active_connection_id() == Some(connection_id) {
                            state.set_active_connection(None, cx);
                        }
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %connection_id,
                            "Failed to clear stale active connection"
                        );
                    }
                }
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

        let saved = app_state.connection_service.list_saved_connections();
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

#[cfg(test)]
mod tests {
    use crate::main_view::MainView;

    #[test]
    fn resolve_sidebar_database_name_prefers_schema_database_name() {
        let resolved = MainView::resolve_sidebar_database_name(
            Some("from-schema".to_string()),
            Some("from-request".to_string()),
            Some("from-existing".to_string()),
            &[("only-db".to_string(), None)],
        );

        assert_eq!(resolved.as_deref(), Some("from-schema"));
    }

    #[test]
    fn resolve_sidebar_database_name_falls_back_to_requested_database_name() {
        let resolved = MainView::resolve_sidebar_database_name(
            None,
            Some("from-request".to_string()),
            Some("from-existing".to_string()),
            &[("only-db".to_string(), None)],
        );

        assert_eq!(resolved.as_deref(), Some("from-request"));
    }

    #[test]
    fn resolve_sidebar_database_name_falls_back_to_existing_active_database_name() {
        let resolved = MainView::resolve_sidebar_database_name(
            None,
            None,
            Some("from-existing".to_string()),
            &[("only-db".to_string(), None)],
        );

        assert_eq!(resolved.as_deref(), Some("from-existing"));
    }

    #[test]
    fn resolve_sidebar_database_name_falls_back_to_single_database_entry() {
        let resolved = MainView::resolve_sidebar_database_name(
            None,
            None,
            None,
            &[("only-db".to_string(), None)],
        );

        assert_eq!(resolved.as_deref(), Some("only-db"));
    }

    #[test]
    fn resolve_sidebar_database_name_returns_none_when_no_hint_and_multiple_databases() {
        let resolved = MainView::resolve_sidebar_database_name(
            None,
            None,
            None,
            &[("db-a".to_string(), None), ("db-b".to_string(), None)],
        );

        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_refresh_database_name_uses_schema_database_without_database_list() {
        let resolved = MainView::resolve_refresh_database_name(
            Some("postgres".to_string()),
            Some("requested".to_string()),
            Some("existing".to_string()),
            None,
        );

        assert_eq!(resolved.as_deref(), Some("postgres"));
    }

    #[test]
    fn resolve_refresh_database_name_uses_target_database_without_database_list() {
        let resolved = MainView::resolve_refresh_database_name(
            None,
            Some("postgres".to_string()),
            Some("existing".to_string()),
            None,
        );

        assert_eq!(resolved.as_deref(), Some("postgres"));
    }

    const _: () = {
        assert!(!super::SurfaceRefreshOptions::SELECTION_SYNC_OBJECTS_ONLY.invalidate_schema_cache);
        assert!(!super::SurfaceRefreshOptions::SELECTION_SYNC_OBJECTS_ONLY.refresh_sidebar);
        assert!(super::SurfaceRefreshOptions::SELECTION_SYNC_OBJECTS_ONLY.refresh_objects_panel);
        assert!(!super::SurfaceRefreshOptions::SELECTION_SYNC_OBJECTS_ONLY.refresh_database_list);
        assert!(!super::SurfaceRefreshOptions::SIDEBAR_AND_OBJECTS.refresh_database_list);
        assert!(super::SurfaceRefreshOptions::CONNECTIONS_LIST.refresh_database_list);
    };
}
