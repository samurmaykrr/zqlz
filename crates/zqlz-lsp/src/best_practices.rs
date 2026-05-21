use lsp_types::{Diagnostic, DiagnosticSeverity, NumberOrString, Position, Range};
use sqlparser::ast::{SelectItem, SetExpr, Statement};
use sqlparser::dialect::Dialect as SqlParserDialect;
use sqlparser::parser::Parser;
use zqlz_core::{SyntaxDiagnosticRules, keyword_token_byte_ranges, select_wildcard_token_ranges};
use zqlz_ui::widgets::{Rope, RopeExt};

use crate::diagnostics::{
    DIAGNOSTIC_CODE_DML_WITHOUT_WHERE, DIAGNOSTIC_CODE_SELECT_WILDCARD,
    DIAGNOSTIC_CODE_SQL_INJECTION_PATTERN,
};

pub(crate) fn analyze_best_practices(
    sql: &str,
    text: &Rope,
    dialect: &dyn SqlParserDialect,
    rules: &SyntaxDiagnosticRules,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if rules.select_wildcard {
        diagnostics.extend(check_select_wildcards(sql, text, dialect));
    }

    if rules.dml_without_where {
        diagnostics.extend(check_dml_without_where(sql, text, dialect));
    }
    if rules.suspicious_drop_chain {
        diagnostics.extend(check_suspicious_drop_chain(sql, text, dialect));
    }

    diagnostics
}

fn check_select_wildcards(
    sql: &str,
    text: &Rope,
    dialect: &dyn SqlParserDialect,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return Vec::new();
    };
    let mut diagnostics = Vec::new();
    let wildcard_ranges = select_wildcard_token_ranges(sql, dialect);
    let mut wildcard_range_index = 0usize;

    for statement in statements {
        let wildcard_count = select_wildcard_count_in_statement(&statement);
        for _ in 0..wildcard_count {
            let Some(wildcard_range) = wildcard_ranges.get(wildcard_range_index) else {
                continue;
            };
            wildcard_range_index += 1;
            let start = text.offset_to_position(wildcard_range.start);
            let end = text.offset_to_position(wildcard_range.end);

            diagnostics.push(Diagnostic {
                range: Range::new(
                    Position::new(start.line, start.character),
                    Position::new(end.line, end.character),
                ),
                severity: Some(DiagnosticSeverity::INFORMATION),
                code: diagnostic_code(DIAGNOSTIC_CODE_SELECT_WILDCARD),
                message: "Consider specifying explicit column names instead of SELECT *"
                    .to_string(),
                source: Some("best-practices".to_string()),
                ..Default::default()
            });
        }
    }

    diagnostics
}

fn check_suspicious_drop_chain(
    sql: &str,
    text: &Rope,
    dialect: &dyn SqlParserDialect,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return Vec::new();
    };
    if statements.len() <= 1 {
        return Vec::new();
    }

    let mut diagnostics = Vec::new();
    let drop_ranges = keyword_token_ranges(text, sql, dialect, "drop");
    let mut drop_range_index = 0usize;

    for (statement_index, statement) in statements.iter().enumerate() {
        if !statement_is_drop(statement) {
            continue;
        }
        let Some(drop_range) = drop_ranges.get(drop_range_index) else {
            continue;
        };
        drop_range_index += 1;
        if statement_index == 0 {
            continue;
        }

        diagnostics.push(Diagnostic {
            range: *drop_range,
            severity: Some(DiagnosticSeverity::ERROR),
            code: diagnostic_code(DIAGNOSTIC_CODE_SQL_INJECTION_PATTERN),
            message: "Potential SQL injection pattern detected".to_string(),
            source: Some("security".to_string()),
            ..Default::default()
        });
    }

    diagnostics
}

fn check_dml_without_where(
    sql: &str,
    text: &Rope,
    dialect: &dyn SqlParserDialect,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return Vec::new();
    };
    let mut diagnostics = Vec::new();
    let update_ranges = keyword_token_ranges(text, sql, dialect, "update");
    let delete_ranges = keyword_token_ranges(text, sql, dialect, "delete");
    let mut update_range_index = 0usize;
    let mut delete_range_index = 0usize;

    for statement in statements {
        let (keyword, keyword_range) = match statement {
            Statement::Update {
                selection: None, ..
            } => {
                let Some(range) = update_ranges.get(update_range_index) else {
                    continue;
                };
                update_range_index += 1;
                ("update", *range)
            }
            Statement::Delete(delete) if delete.selection.is_none() => {
                let Some(range) = delete_ranges.get(delete_range_index) else {
                    continue;
                };
                delete_range_index += 1;
                ("delete", *range)
            }
            Statement::Update { .. } => {
                update_range_index += 1;
                continue;
            }
            Statement::Delete(_) => {
                delete_range_index += 1;
                continue;
            }
            _ => {
                continue;
            }
        };
        if keyword_range.start == keyword_range.end {
            continue;
        }

        diagnostics.push(Diagnostic {
            range: keyword_range,
            severity: Some(DiagnosticSeverity::WARNING),
            code: diagnostic_code(DIAGNOSTIC_CODE_DML_WITHOUT_WHERE),
            message: format!(
                "{} without WHERE clause will affect all rows",
                keyword.to_uppercase()
            ),
            source: Some("best-practices".to_string()),
            ..Default::default()
        });
    }

    diagnostics
}

fn select_wildcard_count_in_statement(statement: &Statement) -> usize {
    match statement {
        Statement::Query(query) => select_wildcard_count_in_set_expr(&query.body),
        _ => 0,
    }
}

fn statement_is_drop(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Drop { .. }
            | Statement::DropFunction { .. }
            | Statement::DropProcedure { .. }
            | Statement::DropSecret { .. }
            | Statement::DropPolicy { .. }
            | Statement::DropTrigger { .. }
    )
}

fn select_wildcard_count_in_set_expr(set_expr: &SetExpr) -> usize {
    match set_expr {
        SetExpr::Select(select) => select
            .projection
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            })
            .count(),
        SetExpr::Query(query) => select_wildcard_count_in_set_expr(&query.body),
        SetExpr::SetOperation { left, right, .. } => {
            select_wildcard_count_in_set_expr(left) + select_wildcard_count_in_set_expr(right)
        }
        _ => 0,
    }
}

fn keyword_token_ranges(
    text: &Rope,
    sql: &str,
    dialect: &dyn SqlParserDialect,
    keyword: &str,
) -> Vec<Range> {
    keyword_token_byte_ranges(sql, dialect, keyword)
        .into_iter()
        .map(|range| {
            let start = text.offset_to_position(range.start);
            let end = text.offset_to_position(range.end);
            Range::new(
                Position::new(start.line, start.character),
                Position::new(end.line, end.character),
            )
        })
        .collect()
}

fn diagnostic_code(code: &'static str) -> Option<NumberOrString> {
    Some(NumberOrString::String(code.to_string()))
}
