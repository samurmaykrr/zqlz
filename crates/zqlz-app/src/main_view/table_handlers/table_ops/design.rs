//! Table design operations - opening the designer and saving table changes.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::TableDetails;
use zqlz_ui::widgets::WindowExt;
use zqlz_versioning::{DatabaseObjectType, make_object_id};

use crate::app::AppState;
use crate::main_view::MainView;

pub(in crate::main_view) struct TableDesignSaveRequest {
    pub connection_id: Uuid,
    pub design: zqlz_table_designer::TableDesign,
    pub is_new: bool,
    pub original_design: Option<zqlz_table_designer::TableDesign>,
    pub panel: Entity<zqlz_table_designer::TableDesignerPanel>,
}

impl MainView {
    pub(in crate::main_view) fn design_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Design table: {} on connection {}",
            table_name,
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

        // Get the driver name directly from the connection
        let driver_name = connection.driver_name().to_string();
        let dialect = zqlz_table_designer::TableLoader::detect_dialect_from_driver(&driver_name);
        let connection = connection.clone();
        let table_name_clone = table_name.clone();
        let dock_area = self.dock_area.downgrade();

        cx.spawn_in(window, async move |this, cx| {
            // Load table structure
            match zqlz_table_designer::TableLoader::load_table(
                connection,
                None,
                &table_name_clone,
                dialect,
            )
            .await
            {
                Ok(design) => {
                    cx.update(|window, cx| {
                        // Create the table designer panel
                        let panel = cx.new(|cx| {
                            zqlz_table_designer::TableDesignerPanel::edit(
                                connection_id,
                                design,
                                window,
                                cx,
                            )
                        });

                        // Subscribe to table designer events
                        _ = this.update(cx, |main_view, cx| {
                            let panel_clone = panel.clone();
                            let subscription = cx.subscribe_in(&panel, window, {
                                move |this,
                                      _panel,
                                      event: &zqlz_table_designer::TableDesignerEvent,
                                      window,
                                      cx| {
                                    this.handle_table_designer_event(
                                        panel_clone.clone(),
                                        event.clone(),
                                        window,
                                        cx,
                                    );
                                }
                            });
                            main_view._subscriptions.push(subscription);
                        });

                        // Add to center dock
                        if let Some(dock_area) = dock_area.upgrade() {
                            dock_area.update(cx, |area, cx| {
                                area.add_panel(
                                    Arc::new(panel.clone()),
                                    zqlz_ui::widgets::dock::DockPlacement::Center,
                                    None,
                                    window,
                                    cx,
                                );
                            });
                        }

                        tracing::info!("Opened table designer for '{}'", table_name_clone);
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to load table structure: {}", e);
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    pub(in crate::main_view) fn design_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for table_name in table_names {
            self.design_table(connection_id, table_name, window, cx);
        }
    }

    pub(in crate::main_view) fn save_table_design(
        &mut self,
        request: TableDesignSaveRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let TableDesignSaveRequest {
            connection_id,
            design,
            is_new,
            original_design,
            panel,
        } = request;
        tracing::info!(
            "Saving table design: {} (is_new={})",
            design.table_name,
            is_new
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
        let _connection_sidebar = self.connection_sidebar.clone();
        let table_name = design.table_name.clone();
        let object_schema = design.schema.clone();
        let version_repository = self.version_repository.clone();
        let dock_area = self.dock_area.clone();

        // Generate the DDL — CREATE for new tables, ALTER for existing
        let ddl_statements: Vec<String> = if is_new {
            match zqlz_table_designer::DdlGenerator::generate_create_table(&design) {
                Ok(ddl) => vec![ddl],
                Err(e) => {
                    tracing::error!("Failed to generate CREATE TABLE DDL: {}", e);
                    window.push_notification(
                        zqlz_ui::widgets::notification::Notification::error(format!(
                            "Failed to generate DDL: {}",
                            e
                        )),
                        cx,
                    );
                    return;
                }
            }
        } else {
            let Some(original) = original_design else {
                tracing::error!("Cannot alter table without original design");
                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::error(
                        "Cannot alter table: original design not available",
                    ),
                    cx,
                );
                return;
            };

            match zqlz_table_designer::DdlGenerator::generate_alter_table(&original, &design) {
                Ok(statements) => {
                    if statements.is_empty() {
                        window.push_notification(
                            zqlz_ui::widgets::notification::Notification::warning(
                                "No changes detected",
                            ),
                            cx,
                        );
                        return;
                    }
                    statements
                }
                Err(e) => {
                    tracing::error!("Failed to generate ALTER TABLE DDL: {}", e);
                    window.push_notification(
                        zqlz_ui::widgets::notification::Notification::error(format!(
                            "Failed to generate DDL: {}",
                            e
                        )),
                        cx,
                    );
                    return;
                }
            }
        };

        cx.spawn_in(window, async move |this, cx| {
            let pre_save_snapshot = if is_new {
                None
            } else {
                match connection.as_schema_introspection() {
                    Some(schema_introspection) => match schema_introspection
                        .get_table(object_schema.as_deref(), &table_name)
                        .await
                    {
                        Ok(table_details) => Some(table_details),
                        Err(error) => {
                            tracing::warn!(%error, table = %table_name, "Failed to capture pre-save table snapshot");
                            None
                        }
                    },
                    None => None,
                }
            };

            // Execute each DDL statement in sequence
            for ddl in &ddl_statements {
                if let Err(e) = connection.execute(ddl, &[]).await {
                    tracing::error!("Failed to execute DDL: {}", e);

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            zqlz_ui::widgets::notification::Notification::error(format!(
                                "Failed to save table '{}': {}",
                                table_name, e
                            )),
                            cx,
                        );
                    });

                    return anyhow::Ok(());
                }
            }

            if let Some(schema_introspection) = connection.as_schema_introspection() {
                if let Some(table_details) = &pre_save_snapshot {
                        let object_id = make_object_id(object_schema.as_deref(), &table_name);
                        match version_repository.get_latest(connection_id, &object_id) {
                            Ok(None) => {
                                if let Err(error) = record_table_version_snapshot(
                                    &version_repository,
                                    connection_id,
                                    object_schema.as_deref(),
                                    &table_name,
                                    table_details,
                                    "Capture table before manual edit".to_string(),
                                ) {
                                tracing::warn!(%error, table = %table_name, "Failed to persist pre-save table snapshot");
                            }
                        }
                        Ok(Some(_)) => {}
                        Err(error) => {
                            tracing::warn!(%error, table = %table_name, "Failed to inspect existing table history before snapshot commit");
                        }
                    }
                }

                match schema_introspection
                    .get_table(object_schema.as_deref(), &table_name)
                    .await
                {
                    Ok(table_details) => match serde_json::to_string_pretty(&table_details) {
                        Ok(snapshot) => {
                            let version_message = build_table_version_message(&table_details, is_new);
                            if let Err(error) = version_repository.commit(
                                connection_id,
                                DatabaseObjectType::Table,
                                object_schema.clone(),
                                table_name.clone(),
                                snapshot,
                                version_message,
                            ) {
                                tracing::error!(%error, table = %table_name, "Failed to store table version snapshot");

                                _ = cx.update(|window, cx| {
                                    window.push_notification(
                                        zqlz_ui::widgets::notification::Notification::warning(format!(
                                            "Table saved, but version history was not updated: {}",
                                            error
                                        )),
                                        cx,
                                    );
                                });
                            }
                        }
                        Err(error) => {
                            tracing::error!(%error, table = %table_name, "Failed to serialize table snapshot");

                            _ = cx.update(|window, cx| {
                                window.push_notification(
                                    zqlz_ui::widgets::notification::Notification::warning(format!(
                                        "Table saved, but version snapshot could not be serialized: {}",
                                        error
                                    )),
                                    cx,
                                );
                            });
                        }
                    },
                    Err(error) => {
                        tracing::error!(%error, table = %table_name, "Failed to reload table after save");

                        _ = cx.update(|window, cx| {
                            window.push_notification(
                                zqlz_ui::widgets::notification::Notification::warning(format!(
                                    "Table saved, but version snapshot could not be reloaded: {}",
                                    error
                                )),
                                cx,
                            );
                        });
                    }
                }
            } else {
                _ = cx.update(|window, cx| {
                    window.push_notification(
                        zqlz_ui::widgets::notification::Notification::warning(
                            "Table saved, but version snapshot could not be recorded because schema introspection is unavailable",
                        ),
                        cx,
                    );
                });
            }

            tracing::info!("Table '{}' saved successfully", table_name);

            _ = cx.update(|window, cx| {
                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::success(format!(
                        "Table '{}' {} successfully",
                        table_name,
                        if is_new { "created" } else { "updated" }
                    )),
                    cx,
                );

                dock_area.update(cx, |area, cx| {
                    area.remove_panel(
                        std::sync::Arc::new(panel),
                        zqlz_ui::widgets::dock::DockPlacement::Center,
                        window,
                        cx,
                    );
                });

                // Refresh both schema-backed surfaces through the shared coordinator.
                _ = this.update(cx, |main_view, cx| {
                    main_view
                        .request_refresh(crate::workspace_state::RefreshScope::ConnectionSurfaces(
                            connection_id,
                        ), cx);
                });
            });

            anyhow::Ok(())
        })
        .detach();
    }
}

fn build_table_version_message(table_details: &TableDetails, is_new: bool) -> String {
    let action = if is_new { "Create" } else { "Update" };
    format!(
        "{} table with {} columns, {} indexes, and {} foreign keys",
        action,
        table_details.columns.len(),
        table_details.indexes.len(),
        table_details.foreign_keys.len()
    )
}

fn record_table_version_snapshot(
    version_repository: &zqlz_versioning::VersionRepository,
    connection_id: Uuid,
    table_schema: Option<&str>,
    table_name: &str,
    table_details: &TableDetails,
    message: String,
) -> anyhow::Result<()> {
    let snapshot = serde_json::to_string_pretty(table_details)?;
    version_repository.commit(
        connection_id,
        DatabaseObjectType::Table,
        table_schema.map(ToOwned::to_owned),
        table_name.to_string(),
        snapshot,
        message,
    )?;
    Ok(())
}
