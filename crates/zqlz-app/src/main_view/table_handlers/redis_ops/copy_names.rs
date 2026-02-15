//! This module handles copying Redis key names to clipboard.

use gpui::{ClipboardItem, Context};

use crate::main_view::MainView;

impl MainView {
    pub(in crate::main_view) fn copy_key_names(&mut self, key_names: &[String], cx: &mut Context<Self>) {
        let text = key_names.join("\n");
        tracing::info!("Copy {} key name(s) to clipboard", key_names.len());
        cx.write_to_clipboard(ClipboardItem::new_string(text));
    }
}
