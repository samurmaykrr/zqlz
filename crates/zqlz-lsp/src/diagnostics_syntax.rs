use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect as SqlParserDialect;
use sqlparser::parser::{Parser, ParserError};
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

    if !crate::mysql_syntax::is_mysql(dialect) && is_compound_routine_statement(sql) {
        return diagnostics;
    }

    let tokens = match tokenize(dialect, sql) {
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

    let mut parser =
        Parser::new(dialect).with_tokens_with_locations(prepare_statements(dialect, tokens));
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

/// Valid PostgreSQL statements that sqlparser has no grammar for. Without this,
/// one `REFRESH MATERIALIZED VIEW` marks the whole buffer as a syntax error and
/// switches off schema validation for every other statement in it.
const UNSUPPORTED_STATEMENT_KEYWORDS: &[&str] = &[
    "REFRESH",
    "VACUUM",
    "REINDEX",
    "CLUSTER",
    "CHECKPOINT",
    "DO",
];

/// Drops statements that begin with an [`UNSUPPORTED_STATEMENT_KEYWORDS`] word,
/// keeping their terminating `;`. Tokens keep their locations, so diagnostics
/// for the remaining statements still point at the right place.
pub(crate) fn skip_unsupported_statements(
    tokens: Vec<TokenWithLocation>,
) -> Vec<TokenWithLocation> {
    let mut kept = Vec::with_capacity(tokens.len());
    let mut at_statement_start = true;
    let mut skipping = false;
    for token in tokens {
        match &token.token {
            Token::SemiColon => {
                skipping = false;
                at_statement_start = true;
            }
            Token::Whitespace(_) if at_statement_start => {}
            Token::Word(word) if at_statement_start => {
                at_statement_start = false;
                skipping = word.quote_style.is_none()
                    && UNSUPPORTED_STATEMENT_KEYWORDS
                        .iter()
                        .any(|keyword| word.value.eq_ignore_ascii_case(keyword));
            }
            _ => at_statement_start = false,
        }
        if !skipping {
            kept.push(token);
        }
    }
    kept
}

/// [`Parser::parse_sql`] that tolerates [`UNSUPPORTED_STATEMENT_KEYWORDS`]
/// statements by leaving them out of the result.
pub(crate) fn parse_supported_statements(
    dialect: &dyn SqlParserDialect,
    sql: &str,
) -> Result<Vec<Statement>, ParserError> {
    let tokens =
        tokenize(dialect, sql).map_err(|error| ParserError::TokenizerError(error.to_string()))?;
    Parser::new(dialect)
        .with_tokens_with_locations(prepare_statements(dialect, tokens))
        .parse_statements()
}

/// Tokenizes `sql` the way every diagnostics pass must see it; for MySQL that is the
/// [`crate::mysql_syntax::mask_sql`] view, whose locations still match the buffer.
pub(crate) fn tokenize(
    dialect: &dyn SqlParserDialect,
    sql: &str,
) -> Result<Vec<TokenWithLocation>, sqlparser::tokenizer::TokenizerError> {
    if crate::mysql_syntax::is_mysql(dialect) {
        Tokenizer::new(dialect, &crate::mysql_syntax::mask_sql(sql)).tokenize_with_location()
    } else {
        Tokenizer::new(dialect, sql).tokenize_with_location()
    }
}

fn prepare_statements(
    dialect: &dyn SqlParserDialect,
    tokens: Vec<TokenWithLocation>,
) -> Vec<TokenWithLocation> {
    if crate::mysql_syntax::is_mysql(dialect) {
        crate::mysql_syntax::prepare_statements(dialect, tokens)
    } else {
        skip_unsupported_statements(tokens)
    }
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
