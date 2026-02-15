//! SQL Hover Provider

use anyhow::Result;
use gpui::{App, Task, Window};
use lsp_types::Hover;
use parking_lot::RwLock;
use std::sync::Arc;
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::lsp::HoverProvider;

use super::SqlLsp;

pub struct SqlHoverProvider {
    lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlHoverProvider {
    pub fn new(lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { lsp }
    }
}

impl HoverProvider for SqlHoverProvider {
    fn hover(
        &self,
        text: &Rope,
        offset: usize,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Task<Result<Option<Hover>>> {
        // CRITICAL: Hover must be fast and never trigger database operations
        // Only use already-cached schema data
        tracing::debug!(offset, "🔍 SQL hover request received");

        let lsp = self.lsp.read();

        // Always attempt hover - get_hover() handles both SQL keywords and schema objects
        // SQL keywords don't require schema cache, so we can always try
        tracing::debug!("Calling get_hover()");
        let hover = lsp.get_hover(text, offset);

        if hover.is_some() {
            tracing::info!("✅ Hover content returned");
        } else {
            tracing::warn!("❌ No hover content found");
        }

        Task::ready(Ok(hover))
    }
}
