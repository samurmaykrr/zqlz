// This module handles copying table names to clipboard.

use gpui::*;

use crate::main_view::MainView;

impl MainView {
    /// Copies table name to clipboard
    pub(in crate::main_view) fn copy_table_name(&mut self, table_name: &str, cx: &mut Context<Self>) {
        tracing::info!("Copy table name: {}", table_name);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(table_name.to_string()));
    }

    /// Copies multiple table names to clipboard
    pub(in crate::main_view) fn copy_table_names(&mut self, table_names: &[String], cx: &mut Context<Self>) {
        let text = table_names.join("\n");
        tracing::info!("Copy {} table name(s) to clipboard", table_names.len());
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
    }
}
