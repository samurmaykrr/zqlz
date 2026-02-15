//! SQL Completion Provider

use anyhow::Result;
use gpui::{Context, Task, Window};
use parking_lot::RwLock;
use std::sync::Arc;
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::InputState;
use zqlz_ui::widgets::input::lsp::CompletionProvider;

use super::SqlLsp;

pub struct SqlCompletionProvider {
    lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlCompletionProvider {
    pub fn new(lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { lsp }
    }
}

impl CompletionProvider for SqlCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: lsp_types::CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<InputState>,
    ) -> Task<Result<lsp_types::CompletionResponse>> {
        tracing::debug!(offset = offset, "Getting SQL completions");

        let mut lsp = self.lsp.write();

        let table_count = lsp.schema_cache.tables.len();
        let column_count: usize = lsp
            .schema_cache
            .columns_by_table
            .values()
            .map(|cols| cols.len())
            .sum();
        tracing::debug!(
            tables = table_count,
            columns = column_count,
            "Schema cache status"
        );

        let completions = lsp.get_completions(text, offset);

        tracing::debug!(count = completions.len(), "Returning completions");

        Task::ready(Ok(lsp_types::CompletionResponse::Array(completions)))
    }

    fn is_completion_trigger(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<InputState>,
    ) -> bool {
        tracing::debug!("is_completion_trigger called with: '{}'", new_text);

        // Single character handling
        if new_text.len() == 1 {
            let ch = new_text.chars().next().unwrap();

            // Only trigger on dot (for table.column pattern) or alphanumeric
            // Do NOT trigger on space - too aggressive
            let should_trigger = ch == '.' || (ch.is_alphanumeric() || ch == '_');

            tracing::debug!("Single char '{}': trigger={}", ch, should_trigger);
            return should_trigger;
        }

        // Multi-character input (paste or rapid typing)
        // Only trigger if it looks like meaningful text (not just spaces/punctuation)
        let has_alphanum = new_text.chars().any(|c| c.is_alphanumeric());
        tracing::debug!("Multi-char input '{}': trigger={}", new_text, has_alphanum);
        has_alphanum
    }
}
