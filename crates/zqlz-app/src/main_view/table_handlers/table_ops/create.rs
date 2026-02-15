// This module handles new table creation operations.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_table_designer::{DatabaseDialect, TableDesignerPanel, TableLoader};

use crate::app::AppState;
use crate::main_view::MainView;

impl MainView {
    pub(in crate::main_view) fn new_table(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("New table on connection {}", connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        // Get the connection and driver name
        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        // Get the driver name directly from the connection
        let driver_name = connection.driver_name().to_string();
        let dialect = TableLoader::detect_dialect_from_driver(&driver_name);

        // Create an empty table designer panel
        let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, dialect, window, cx));

        // Subscribe to table designer events
        let panel_clone = panel.clone();
        let subscription = cx.subscribe_in(&panel, window, {
            move |this, _panel, event: &zqlz_table_designer::TableDesignerEvent, window, cx| {
                this.handle_table_designer_event(panel_clone.clone(), event.clone(), window, cx);
            }
        });
        self._subscriptions.push(subscription);

        // Add to center dock
        let dock_area = self.dock_area.clone();
        dock_area.update(cx, |area, cx| {
            area.add_panel(
                Arc::new(panel.clone()),
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });

        tracing::info!("New table designer opened");
    }
}
