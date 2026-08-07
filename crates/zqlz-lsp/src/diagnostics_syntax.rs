use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use sqlparser::dialect::Dialect as SqlParserDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithLocation, Tokenizer};

use crate::diagnostics::{
    DIAGNOSTIC_CODE_SQL_TOKENIZER, DIAGNOSTIC_CODE_SQLPARSER_SYNTAX, diagnostic_code,
};

pub(crate) fn check_sqlparser_syntax(
    sql: &str,
    dialect: &dyn SqlParserDialect,
    fallback_range: Option<Range>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    if is_compound_routine_statement(sql) {
        return diagnostics;
    }

    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = match tokenizer.tokenize_with_location() {
        Ok(tokens) => tokens,
        Err(error) => {
            let line = error.location.line.saturating_sub(1) as u32;
            let character = error.location.column.saturating_sub(1) as u32;
            diagnostics.push(Diagnostic {
                range: Range::new(
                    Position::new(line, character),
                    Position::new(line, character.saturating_add(1)),
                ),
                severity: Some(DiagnosticSeverity::ERROR),
                code: diagnostic_code(DIAGNOSTIC_CODE_SQL_TOKENIZER),
                message: format!("SQL Tokenizer Error: {}", error.message),
                source: Some("sqlparser".to_string()),
                ..Default::default()
            });
            return diagnostics;
        }
    };

    let mut parser = Parser::new(dialect).with_tokens_with_locations(tokens);
    match parser.parse_statements() {
        Ok(_statements) => {}
        Err(error) => {
            let range = parser_error_range(&parser.peek_token()).unwrap_or_else(|| {
                fallback_range
                    .unwrap_or_else(|| Range::new(Position::new(0, 0), Position::new(0, 1)))
            });

            diagnostics.push(Diagnostic {
                range,
                severity: Some(DiagnosticSeverity::ERROR),
                code: diagnostic_code(DIAGNOSTIC_CODE_SQLPARSER_SYNTAX),
                message: format!("SQL Syntax Error: {}", error),
                source: Some("sqlparser".to_string()),
                ..Default::default()
            });
        }
    }

    diagnostics
}

/// True for `CREATE ... FUNCTION|PROCEDURE|TRIGGER|EVENT`, whose `BEGIN ... END` bodies
/// neither sqlparser nor tree-sitter-sequel can parse. Reporting their syntax would paint
/// every line of a server-emitted routine definition red.
///
/// Shares [`zqlz_core::is_compound_routine_header`] with the statement splitter so that what
/// is exempt from syntax checking and what is kept whole when splitting cannot drift apart.
pub(crate) fn is_compound_routine_statement(sql: &str) -> bool {
    zqlz_core::is_compound_routine_header(sql)
}

fn parser_error_range(token: &TokenWithLocation) -> Option<Range> {
    if token.location.line == 0 || token.location.column == 0 {
        return None;
    }

    let line = token.location.line.saturating_sub(1) as u32;
    let character = token.location.column.saturating_sub(1) as u32;
    let width = parser_token_display_width(&token.token).max(1) as u32;
    Some(Range::new(
        Position::new(line, character),
        Position::new(line, character.saturating_add(width)),
    ))
}

fn parser_token_display_width(token: &Token) -> usize {
    match token {
        Token::Word(word) => word.to_string().chars().count(),
        Token::EOF => 1,
        _ => token.to_string().chars().count(),
    }
}
