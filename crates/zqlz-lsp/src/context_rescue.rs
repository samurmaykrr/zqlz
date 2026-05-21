use crate::{TableRef, context_analyzer::SqlContext};

pub(crate) fn rescue_incomplete_context(
    text_before: &str,
    available_tables: &[TableRef],
) -> Option<SqlContext> {
    if is_inside_create_table_column_list(text_before) {
        return Some(SqlContext::CreateTable);
    }

    if let Some(context) = zqlz_core::incomplete_sql_completion_context(text_before) {
        return Some(match context {
            zqlz_core::IncompleteSqlCompletionContext::SelectList => SqlContext::SelectList {
                available_tables: available_tables.to_vec(),
            },
            zqlz_core::IncompleteSqlCompletionContext::FromClause => SqlContext::FromClause,
            zqlz_core::IncompleteSqlCompletionContext::JoinClause => SqlContext::JoinClause {
                existing_tables: available_tables.to_vec(),
            },
        });
    }

    if let Some(table_or_alias) = zqlz_core::qualified_sql_reference_prefix(text_before) {
        tracing::trace!(
            table_or_alias = table_or_alias,
            "Detected qualified reference before cursor"
        );
        return Some(SqlContext::AfterDot {
            table_or_alias,
            available_tables: available_tables.to_vec(),
        });
    }

    None
}

pub(crate) fn is_inside_create_table_column_list(text_before: &str) -> bool {
    zqlz_core::is_inside_active_create_table_column_list(text_before)
}
