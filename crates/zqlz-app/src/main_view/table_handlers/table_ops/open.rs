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
        for table_name in table_names {
            self.open_table_viewer(
                connection_id,
                table_name,
                database_name.clone(),
                is_view,
                window,
                cx,
            );
        }
    }
}
