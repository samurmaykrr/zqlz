// This module handles table renaming operations.

use gpui::*;
use uuid::Uuid;
use zqlz_core::TableRef;

use crate::app::AppState;
use crate::main_view::MainView;
use crate::main_view::rename_window::RenameWindow;

impl MainView {
    pub(in crate::main_view) fn rename_table(
        &mut self,
        connection_id: Uuid,
        table: TableRef,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        RenameWindow::open_table(
            connection_id,
            table,
            app_state.connection_service.clone(),
            app_state.table_service.clone(),
            cx.entity().downgrade(),
            cx,
        );
    }
}
