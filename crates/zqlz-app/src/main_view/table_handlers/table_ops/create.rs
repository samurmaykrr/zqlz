// This module handles new table creation operations.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_table_designer::TableDesignerPanel;

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
        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let dialect = app_state
            .table_design_service
            .dialect_for_connection(connection.as_ref());

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);

        // Create an empty table designer panel
        let panel = cx
            .new(|cx| TableDesignerPanel::new(connection_id, dialect, target_database, window, cx));

        // Subscribe to table designer events
        let panel_clone = panel.clone();
        let subscription = cx.subscribe_in(&panel, window, {
            move |this, _panel, event: &zqlz_table_designer::TableDesignerEvent, window, cx| {
                this.handle_table_designer_event(panel_clone.clone(), event.clone(), window, cx);
            }
        });
        self._subscriptions.push(subscription);

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(Arc::new(panel.clone()), window, cx);
        });

        tracing::info!("New table designer opened");
    }
}
