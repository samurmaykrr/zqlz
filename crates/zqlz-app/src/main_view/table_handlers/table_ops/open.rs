// This module handles opening multiple tables simultaneously.

use gpui::*;
use uuid::Uuid;

use crate::main_view::MainView;

impl MainView {
    /// Opens multiple tables in the table viewer
    pub(in crate::main_view) fn open_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
        is_view: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(decision) = self.decide_open_tables_workflow(
            connection_id,
            table_names,
            database_name,
            is_view,
            window,
            cx,
        ) else {
            return;
        };

        for request in decision.requests {
            self.open_table_viewer(
                request.connection_id,
                request.table_name,
                request.database_name,
                request.is_view,
                window,
                cx,
            );
        }
    }
}
