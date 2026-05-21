//! SQL Hover Provider

use anyhow::Result;
use gpui::{App, Task, Window};
use lsp_types::Hover;
use parking_lot::RwLock;
use std::sync::Arc;
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::lsp::HoverProvider;

use super::SqlLsp;
use crate::hover_render::{
    create_column_hover, create_derived_table_hover, create_dialect_metadata_hover,
    create_function_hover, create_index_hover, create_procedure_hover, create_sequence_hover,
    create_table_hover, create_trigger_hover, create_view_hover,
};

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

pub(crate) fn get_hover(lsp: &SqlLsp, text: &Rope, offset: usize) -> Option<Hover> {
    tracing::debug!("get_hover: offset={}", offset);
    let word = match lsp.get_word_at_offset(text, offset) {
        Some(word) => {
            tracing::debug!("word extracted: '{}'", word);
            word
        }
        None => {
            tracing::debug!("no word at offset");
            return None;
        }
    };

    let word_lower = word.to_lowercase();
    tracing::debug!("checking hover metadata for word: '{}'", word);

    if let Some(hover) =
        create_dialect_metadata_hover(&word, &lsp.dialect.dialect_info(), lsp.get_dialect_name())
    {
        return Some(hover);
    }

    if let Some((qualifier, _)) = lsp.qualified_reference_at_offset(text, offset)
        && let Some(table_name) = lsp.resolve_table_identifier(&qualifier, text, offset)
        && let Some(columns) = lsp.columns_for_table(&table_name)
    {
        for column in columns {
            if column.name.to_lowercase() == word_lower {
                tracing::trace!(qualifier = qualifier, table = table_name, column = %column.name, "Found qualified column");
                return Some(create_column_hover(column, Some(&table_name)));
            }
        }
    }

    if let Some(table_name) = lsp.resolve_table_identifier(&word, text, offset) {
        if let Some(table) = lsp.table_info(&table_name) {
            return Some(create_table_hover(
                table,
                lsp.columns_for_table(&table.name).map(Vec::as_slice),
                lsp.foreign_keys_for_table(&table.name).map(Vec::as_slice),
                lsp.reverse_foreign_keys_for_table(&table.name)
                    .map(Vec::as_slice),
            ));
        }

        if let Some(columns) = lsp.derived_columns_for_identifier(&table_name, text) {
            let kind = if word.eq_ignore_ascii_case(&table_name) {
                "Derived table"
            } else {
                "CTE"
            };
            return Some(create_derived_table_hover(&table_name, &columns, kind));
        }
    }

    for (table_name, columns) in &lsp.schema_cache.columns_by_table {
        for column in columns {
            if column.name.to_lowercase() == word_lower {
                return Some(create_column_hover(column, Some(table_name)));
            }
        }
    }

    if let Some(sequence) = lsp.sequence_info(&word) {
        return Some(create_sequence_hover(sequence));
    }

    if let Some(view) = lsp.schema_cache.views.get(&word) {
        return Some(create_view_hover(view));
    }

    if let Some(procedure) = lsp.schema_cache.procedures.get(&word) {
        return Some(create_procedure_hover(procedure));
    }

    if let Some(function) = lsp.schema_cache.functions.get(&word) {
        return Some(create_function_hover(function));
    }

    if let Some(index) = lsp.schema_cache.indexes.get(&word) {
        return Some(create_index_hover(index));
    }

    if let Some(trigger) = lsp.schema_cache.triggers.get(&word) {
        return Some(create_trigger_hover(trigger));
    }

    None
}
