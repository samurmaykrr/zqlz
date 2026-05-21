use lsp_types::{Position, Range};
use sqlparser::dialect::Dialect as SqlParserDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::{ValidationIssue, schema_validation_issue::ValidationSymbolRole};

pub(crate) fn schema_issue_range(
    sql: &str,
    dialect: &dyn SqlParserDialect,
    issue: &ValidationIssue,
    occurrence: usize,
) -> Option<Range> {
    let symbol = issue.symbol.as_deref()?;
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = tokenizer.tokenize_with_location().ok()?;
    let mut matched_occurrences = 0usize;

    tokens.iter().enumerate().find_map(|(index, token)| {
        let token_length = schema_issue_token_length(&token.token, symbol)?;
        if !schema_issue_token_matches_role(&tokens, index, issue.symbol_role) {
            return None;
        }
        if !schema_issue_token_matches_qualifier(
            &tokens,
            index,
            issue.symbol_role,
            issue.qualifier.as_deref(),
        ) {
            return None;
        }
        if matched_occurrences < occurrence {
            matched_occurrences += 1;
            return None;
        }
        let line = token.location.line.saturating_sub(1) as u32;
        let character = token.location.column.saturating_sub(1) as u32;
        Some(Range::new(
            Position::new(line, character),
            Position::new(line, character.saturating_add(token_length as u32)),
        ))
    })
}

fn schema_issue_token_matches_qualifier(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
    role: Option<ValidationSymbolRole>,
    qualifier: Option<&str>,
) -> bool {
    let Some(qualifier) = qualifier else {
        if role == Some(ValidationSymbolRole::Table)
            && qualified_table_reference_starts_after_table_keyword(tokens, index)
        {
            return true;
        }
        return previous_significant_token(tokens, index)
            .is_none_or(|token| !matches!(token.token, Token::Period));
    };

    let Some(period_index) = previous_significant_token_index(tokens, index) else {
        return false;
    };
    if !matches!(tokens[period_index].token, Token::Period) {
        return false;
    }

    previous_significant_token_index(tokens, period_index).is_some_and(|qualifier_index| {
        schema_issue_token_length(&tokens[qualifier_index].token, qualifier).is_some()
    })
}

fn qualified_table_reference_starts_after_table_keyword(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
) -> bool {
    let Some(period_index) = previous_significant_token_index(tokens, index) else {
        return false;
    };
    if !matches!(tokens[period_index].token, Token::Period) {
        return false;
    }

    let mut segment_index = index;
    while let Some(period_index) = previous_significant_token_index(tokens, segment_index) {
        if !matches!(tokens[period_index].token, Token::Period) {
            break;
        }
        let Some(previous_segment_index) = previous_significant_token_index(tokens, period_index)
        else {
            return false;
        };
        if !matches!(tokens[previous_segment_index].token, Token::Word(_)) {
            return false;
        }
        segment_index = previous_segment_index;
    }

    previous_significant_token(tokens, segment_index)
        .is_some_and(|token| token_starts_table_reference(&token.token))
}

fn schema_issue_token_matches_role(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
    role: Option<ValidationSymbolRole>,
) -> bool {
    match role {
        Some(ValidationSymbolRole::Table) => {
            previous_significant_token(tokens, index)
                .is_some_and(|token| token_starts_table_reference(&token.token))
                || qualified_table_reference_starts_after_table_keyword(tokens, index)
        }
        Some(ValidationSymbolRole::Column) => previous_significant_token(tokens, index)
            .is_none_or(|token| !token_starts_table_reference(&token.token)),
        Some(ValidationSymbolRole::Wildcard) | None => true,
    }
}

fn previous_significant_token_index(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
) -> Option<usize> {
    tokens
        .get(..index)?
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, token)| (!matches!(token.token, Token::Whitespace(_))).then_some(index))
}

fn token_starts_table_reference(token: &Token) -> bool {
    token_word_eq(token, "from")
        || token_word_eq(token, "join")
        || token_word_eq(token, "into")
        || token_word_eq(token, "update")
        || token_word_eq(token, "table")
}

fn schema_issue_token_length(token: &Token, symbol: &str) -> Option<usize> {
    match token {
        Token::Word(word) if word.value.eq_ignore_ascii_case(symbol) => {
            Some(word.to_string().chars().count())
        }
        Token::Mul if symbol == "*" => Some(1),
        _ => None,
    }
}

fn previous_significant_token(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
) -> Option<&sqlparser::tokenizer::TokenWithLocation> {
    tokens
        .get(..index)?
        .iter()
        .rev()
        .find(|token| !matches!(token.token, Token::Whitespace(_)))
}

fn token_word_eq(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
}
