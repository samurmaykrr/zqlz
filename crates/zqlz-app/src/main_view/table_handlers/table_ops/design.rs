//! Table design operations - opening the designer and saving table changes.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::main_view::MainView;

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

        cx.spawn_in(window, async move |this, mut cx| {
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
        connection_id: Uuid,
        design: zqlz_table_designer::TableDesign,
        is_new: bool,
        original_design: Option<zqlz_table_designer::TableDesign>,
        panel: Entity<zqlz_table_designer::TableDesignerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
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
        let connection_sidebar = self.connection_sidebar.clone();
        let table_name = design.table_name.clone();
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

                // Refresh both schema (sidebar) and objects panel
                _ = this.update(cx, |main_view, cx| {
                    main_view.refresh_schema(connection_id, window, cx);
                    main_view.refresh_objects_panel(window, cx);
                });
            });

            anyhow::Ok(())
        })
        .detach();
    }
}
