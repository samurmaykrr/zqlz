// This module handles opening multiple tables simultaneously.

use gpui::*;
use uuid::Uuid;
use zqlz_core::TableRef;

use crate::main_view::MainView;

impl MainView {
    /// Opens each table in its own viewer tab.
    pub(in crate::main_view) fn open_tables(
        &mut self,
        connection_id: Uuid,
        tables: Vec<TableRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for table in tables {
            self.open_table_viewer(connection_id, table, window, cx);
        }
    }
}
