use lsp_types::CompletionItem;
use zqlz_core::{
    SyntaxCompletionContextKind, create_table_prefers_constraint_terms,
    keyword_context_implies_condition_clause,
};
use zqlz_ui::widgets::Rope;

use crate::{
    AstSqlContext, SqlDialect, SqlLsp, TableRef, completion_dispatch, completion_edit,
    completion_ranking, dialect_completions, keyword_completions, redis_completion,
    schema_completions,
};

#[allow(dead_code)]
impl SqlLsp {
    pub(crate) fn completion_sql_dialects(&self) -> Vec<SqlDialect> {
        dialect_completions::completion_sql_dialects(self.dialect)
    }

    pub(crate) fn uses_command_syntax(&self) -> bool {
        zqlz_core::get_syntax_driver_capabilities(&self.driver_type).command_syntax
    }

    pub(crate) fn uses_document_syntax(&self) -> bool {
        zqlz_core::get_syntax_driver_capabilities(&self.driver_type).document_syntax
    }

    pub(crate) fn prefer_create_table_constraints(&self, text_before_cursor: &str) -> bool {
        let dialects = self.completion_sql_dialects();
        let data_types = dialect_completions::completion_data_type_infos(&dialects);
        create_table_prefers_constraint_terms(text_before_cursor, data_types.as_slice())
    }

    /// Get completion items for the current context (auto-trigger mode)
    pub fn get_completions(&mut self, text: &Rope, offset: usize) -> Vec<CompletionItem> {
        self.get_completions_with_trigger(text, offset, false)
    }

    /// Get completion items with explicit trigger support.
    pub fn get_completions_with_trigger(
        &mut self,
        text: &Rope,
        offset: usize,
        is_manual_trigger: bool,
    ) -> Vec<CompletionItem> {
        tracing::debug!(
            "SqlLsp::get_completions at offset {} (manual={})",
            offset,
            is_manual_trigger
        );

        let mut completions = Vec::new();
        let sql = text.to_string();
        let current_word = zqlz_core::syntax_completion_prefix(&sql, offset, &[]);
        let current_word_lower = current_word.to_lowercase();

        tracing::debug!(
            "Current word: '{}', lowercase: '{}'",
            current_word,
            current_word_lower
        );

        if self.uses_command_syntax() {
            return redis_completion::get_redis_completions(
                &self.driver_type,
                text,
                offset,
                is_manual_trigger,
            );
        }

        let syntax_capabilities = zqlz_core::get_syntax_driver_capabilities(&self.driver_type);
        if syntax_capabilities.document_syntax {
            return crate::document_completions::get_document_completions(
                &syntax_capabilities,
                text,
                offset,
            );
        }

        let trigger_context = zqlz_core::sql_completion_trigger_context(
            &sql,
            offset,
            &current_word,
            is_manual_trigger,
        );
        let lines_before_cursor = trigger_context.before_cursor;

        tracing::debug!(
            "Text before cursor (last 50 chars): '{}'",
            lines_before_cursor
                .chars()
                .rev()
                .take(50)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>()
        );

        if !trigger_context.should_show {
            tracing::debug!("Auto-trigger: skipping (no trigger condition met)");
            return completions;
        }

        let mut context = self.context_analyzer.analyze(text, offset);
        tracing::debug!(context = ?context, "syntax-tree analysis complete");

        if matches!(context, AstSqlContext::General)
            && keyword_context_implies_condition_clause(&lines_before_cursor)
        {
            context = AstSqlContext::ConditionClause {
                available_tables: self.context_analyzer.extract_available_tables(text, offset),
            };
        }

        if let Some(table_or_alias) =
            zqlz_core::qualified_sql_reference_prefix(&lines_before_cursor)
        {
            let available_tables = match &context {
                AstSqlContext::SelectList { available_tables }
                | AstSqlContext::ConditionClause { available_tables }
                | AstSqlContext::AfterDot {
                    available_tables, ..
                } => available_tables.clone(),
                AstSqlContext::Subquery { parent_tables } => parent_tables.clone(),
                _ => Vec::new(),
            };

            context = AstSqlContext::AfterDot {
                table_or_alias,
                available_tables,
            };
        }

        tracing::debug!("Detected context: {:?}", context);

        let is_after_dot = matches!(context, AstSqlContext::AfterDot { .. });

        if completion_dispatch::add_completions_for_context(
            self,
            text,
            offset,
            &context,
            &current_word_lower,
            &lines_before_cursor,
            &mut completions,
        ) {
            return completions;
        }

        let is_table_name_context = matches!(
            context,
            AstSqlContext::FromClause | AstSqlContext::JoinClause { .. }
        );
        if completions.is_empty() && sql.len() < 20 && !is_after_dot && !is_table_name_context {
            tracing::debug!("No completions found for short query, falling back to keywords");
            self.add_filtered_keywords(&current_word_lower, &lines_before_cursor, &mut completions);
        }

        let prefer_create_table_constraints = matches!(context, AstSqlContext::CreateTable)
            && self.prefer_create_table_constraints(&lines_before_cursor);

        completions = completion_ranking::rank_and_dedup_completions(
            &self.fuzzy_matcher,
            &context,
            &current_word,
            completions,
            prefer_create_table_constraints,
        );

        completions.truncate(20);
        completion_edit::apply_completion_text_edits(&mut completions, text, offset);

        tracing::debug!("Returning {} completions", completions.len());
        completions
    }

    pub(crate) fn add_columns_from_tables(
        &self,
        tables: &[TableRef],
        completions: &mut Vec<CompletionItem>,
    ) {
        schema_completions::add_columns_from_tables(&self.schema_cache, tables, completions);
    }

    pub(crate) fn add_columns_from_names(
        &self,
        column_names: &[String],
        completions: &mut Vec<CompletionItem>,
    ) {
        schema_completions::add_columns_from_names(column_names, completions);
    }

    pub(crate) fn add_tables_with_fk_suggestions(
        &self,
        existing_tables: &[TableRef],
        completions: &mut Vec<CompletionItem>,
    ) {
        schema_completions::add_tables_with_fk_suggestions(
            &self.schema_cache,
            existing_tables,
            completions,
        );
    }

    pub(crate) fn add_context_keywords(
        &self,
        context: SyntaxCompletionContextKind,
        filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        let dialects = self.completion_sql_dialects();
        keyword_completions::add_context_keywords(&dialects, context, filter, completions);
    }

    pub(crate) fn add_filtered_keywords(
        &self,
        filter: &str,
        context: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        let dialects = self.completion_sql_dialects();
        keyword_completions::add_filtered_keywords(
            &dialects,
            &self.fuzzy_matcher,
            self.get_dialect_name(),
            filter,
            context,
            completions,
        );
    }

    pub(crate) fn add_create_table_constraints(
        &self,
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        self.add_driver_create_table_keywords(completions);
    }

    pub(crate) fn add_driver_create_table_keywords(&self, completions: &mut Vec<CompletionItem>) {
        let dialects = self.completion_sql_dialects();
        dialect_completions::add_create_table_keywords(&dialects, completions);
    }

    pub(crate) fn add_filtered_data_types(
        &self,
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        let dialects = self.completion_sql_dialects();
        let data_types = dialect_completions::completion_data_type_infos(&dialects);
        dialect_completions::add_data_types(&data_types, completions);
    }

    pub(crate) fn add_filtered_tables(&self, completions: &mut Vec<CompletionItem>) {
        schema_completions::add_tables(&self.schema_cache, completions);
    }

    pub(crate) fn add_filtered_views(&self, completions: &mut Vec<CompletionItem>) {
        schema_completions::add_views(&self.schema_cache, completions);
    }

    pub(crate) fn add_filtered_columns(&self, completions: &mut Vec<CompletionItem>) {
        schema_completions::add_all_columns(&self.schema_cache, completions);
    }

    pub(crate) fn add_filtered_functions(
        &self,
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        let dialects = self.completion_sql_dialects();
        let functions = dialect_completions::completion_function_infos(&dialects);
        dialect_completions::add_functions(&self.schema_cache, &functions, completions);
    }

    pub(crate) fn add_scalar_functions(
        &self,
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        let dialects = self.completion_sql_dialects();
        let functions = dialect_completions::completion_scalar_function_infos(&dialects);
        dialect_completions::add_scalar_functions(&self.schema_cache, functions, completions);
    }
}
