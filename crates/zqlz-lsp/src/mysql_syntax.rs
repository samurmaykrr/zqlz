//! sqlparser 0.52's MySQL dialect rejects a lot of valid MySQL. These helpers reshape the
//! token stream so valid MySQL never surfaces as a syntax error, while genuine typos in
//! queries and DML (`SELECT * FORM t`) still do.

use sqlparser::dialect::{Dialect, MySqlDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithLocation};
use zqlz_core::{SqlLexDialect, is_compound_routine_header, split_sql_statement_spans_for_dialect};

pub(crate) fn is_mysql(dialect: &dyn Dialect) -> bool {
    dialect.is::<MySqlDialect>()
}

/// A copy of `sql` in which every kept character stays on the same line and column, so
/// token locations still point into the editor buffer:
/// - text between statements (`DELIMITER` directives, custom terminators such as `//` or
///   `$$`) is blanked and each statement is terminated with `;` instead;
/// - stored-program definitions are blanked, as their `BEGIN ... END` bodies can't be parsed;
/// - the `@` of an account name (`'u'@'localhost'`) is blanked, because sqlparser lexes `@'`
///   as the start of an identifier and then reports an unterminated string.
pub(crate) fn mask_sql(sql: &str) -> String {
    let mut keep = vec![false; sql.len()];
    let mut statement_ends = Vec::new();
    for span in split_sql_statement_spans_for_dialect(sql, SqlLexDialect::MySql) {
        if is_compound_routine_header(&sql[span.start..span.end]) {
            continue;
        }
        keep[span.start..span.end].fill(true);
        statement_ends.push(span.end);
    }

    let mut statement_ends = statement_ends.into_iter().peekable();
    let mut needs_terminator = false;
    let mut masked = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        while statement_ends.next_if(|&end| end <= index).is_some() {
            needs_terminator = true;
        }
        let next = chars.peek().map(|&(_, next)| next);
        masked.push(if keep[index] {
            if character == '@' && matches!(next, Some('\'' | '"' | '`')) {
                ' '
            } else {
                character
            }
        } else if matches!(character, '\n' | '\r') {
            character
        } else if needs_terminator {
            needs_terminator = false;
            ';'
        } else {
            ' '
        });
    }
    masked
}

/// Statement-by-statement: rewrites MySQL-only syntax into something sqlparser accepts, and
/// drops statements it still can't parse unless they are queries or DML. Administrative and
/// DDL statements (`SHOW`, `SET GLOBAL`, `GRANT`, `CREATE TABLE ... PARTITION BY`, ...) are
/// where sqlparser's MySQL coverage is thin, and none of them is schema-validated anyway.
// ponytail: unparseable DDL/admin statements are silently skipped, so a typo in them goes
// unreported; tighten per statement kind if sqlparser's MySQL grammar catches up.
pub(crate) fn prepare_statements(
    dialect: &dyn Dialect,
    tokens: Vec<TokenWithLocation>,
) -> Vec<TokenWithLocation> {
    let mut kept = Vec::with_capacity(tokens.len());
    let mut statement = Vec::new();
    for token in tokens {
        let is_terminator = token.token == Token::SemiColon;
        statement.push(token);
        if is_terminator {
            keep_statement(dialect, std::mem::take(&mut statement), &mut kept);
        }
    }
    keep_statement(dialect, statement, &mut kept);
    kept
}

fn keep_statement(
    dialect: &dyn Dialect,
    statement: Vec<TokenWithLocation>,
    kept: &mut Vec<TokenWithLocation>,
) {
    let statement = normalize(statement);
    let leading = statement.iter().find(|token| is_significant(&token.token));
    let Some(leading) = leading else {
        kept.extend(statement);
        return;
    };
    let strict = matches!(leading.token, Token::LParen)
        || ["SELECT", "WITH", "INSERT", "REPLACE", "UPDATE", "DELETE"]
            .iter()
            .any(|keyword| word_is(&leading.token, keyword));
    let parses = || {
        Parser::new(dialect)
            .with_tokens_with_locations(statement.clone())
            .parse_statements()
            .is_ok()
    };
    if (strict && !has_unsupported_dml_construct(&statement)) || parses() {
        kept.extend(statement);
    } else {
        kept.extend(
            statement
                .into_iter()
                .filter(|token| token.token == Token::SemiColon),
        );
    }
}

/// Valid DML sqlparser has no grammar for and that can't be rewritten away.
fn has_unsupported_dml_construct(statement: &[TokenWithLocation]) -> bool {
    let words: Vec<&Token> = statement
        .iter()
        .map(|token| &token.token)
        .filter(|token| is_significant(token))
        .collect();
    let is_insert = words
        .first()
        .is_some_and(|token| word_is(token, "INSERT") || word_is(token, "REPLACE"));
    // `INSERT INTO t SET a = 1`: SET right after the target table, before any VALUES/SELECT.
    let insert_set = is_insert
        && words
            .iter()
            .take_while(|token| !word_is(token, "VALUES") && !word_is(token, "SELECT"))
            .any(|token| word_is(token, "SET"));
    // `FROM t PARTITION (p0)`; `OVER (PARTITION BY ...)` parses fine.
    let partition_selection = words
        .windows(2)
        .any(|pair| word_is(pair[0], "PARTITION") && *pair[1] == Token::LParen);
    insert_set || partition_selection
}

/// Select modifiers that only tune execution; MySQL reserves them, so they can't be names.
const DROPPED_MODIFIERS: &[&str] = &[
    "SQL_CALC_FOUND_ROWS",
    "SQL_NO_CACHE",
    "SQL_CACHE",
    "SQL_SMALL_RESULT",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
    "HIGH_PRIORITY",
];

fn normalize(tokens: Vec<TokenWithLocation>) -> Vec<TokenWithLocation> {
    let is_update = tokens
        .iter()
        .find(|token| is_significant(&token.token))
        .is_some_and(|token| word_is(&token.token, "UPDATE"));
    let mut out: Vec<TokenWithLocation> = Vec::with_capacity(tokens.len());
    let mut depth = 0usize;
    let mut index = 0usize;
    while index < tokens.len() {
        let token = &tokens[index].token;
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
        let next = next_significant(&tokens, index + 1);
        let next_token = next.map(|next| &tokens[next].token);
        let previous = out.iter().rev().find(|token| is_significant(&token.token));

        // `@x := 1` assigns; as a comparison it has the same shape for the parser.
        if *token == Token::Assignment {
            out.push(with_token(&tokens[index], Token::Eq));
            index += 1;
            continue;
        }
        if word_is(token, "MOD")
            && previous.is_some_and(|previous| ends_operand(&previous.token))
            && next_token != Some(&Token::LParen)
        {
            out.push(with_token(&tokens[index], Token::Mod));
            index += 1;
            continue;
        }
        if DROPPED_MODIFIERS
            .iter()
            .any(|modifier| word_is(token, modifier))
        {
            index += 1;
            continue;
        }
        if word_is(token, "STRAIGHT_JOIN") {
            let after_select = previous.is_some_and(|previous| {
                ["SELECT", "DISTINCT", "ALL", "DISTINCTROW"]
                    .iter()
                    .any(|keyword| word_is(&previous.token, keyword))
            });
            if !after_select {
                out.push(with_token(&tokens[index], Token::make_keyword("JOIN")));
            }
            index += 1;
            continue;
        }
        // Index hints: `USE|FORCE|IGNORE INDEX|KEY [FOR JOIN|ORDER BY|GROUP BY] (...)`.
        if ["USE", "FORCE", "IGNORE"]
            .iter()
            .any(|keyword| word_is(token, keyword))
            && next_token.is_some_and(|next| word_is(next, "INDEX") || word_is(next, "KEY"))
        {
            index = skip_index_hint(&tokens, next.unwrap_or(index) + 1);
            continue;
        }
        if word_is(token, "LOCK") && next_token.is_some_and(|next| word_is(next, "IN")) {
            // `LOCK IN SHARE MODE`
            index = skip_words(&tokens, index, 4);
            continue;
        }
        // `EXPLAIN FORMAT=JSON ...`
        if word_is(token, "FORMAT")
            && next_token == Some(&Token::Eq)
            && previous.is_some_and(|previous| {
                ["EXPLAIN", "ANALYZE", "DESCRIBE", "DESC"]
                    .iter()
                    .any(|keyword| word_is(&previous.token, keyword))
            })
        {
            index = skip_words(&tokens, index, 3);
            continue;
        }
        // `SELECT ... INTO OUTFILE 'f' [export options] [FROM ...]`
        if depth == 0
            && word_is(token, "INTO")
            && next_token.is_some_and(|next| word_is(next, "OUTFILE") || word_is(next, "DUMPFILE"))
        {
            index = skip_until(&tokens, index, |token| word_is(token, "FROM"));
            continue;
        }
        // `UPDATE ... ORDER BY ... LIMIT n`
        if is_update && depth == 0 && (word_is(token, "ORDER") || word_is(token, "LIMIT")) {
            index = skip_until(&tokens, index, |_| false);
            continue;
        }
        out.push(tokens[index].clone());
        index += 1;
    }
    out
}

fn skip_index_hint(tokens: &[TokenWithLocation], mut index: usize) -> usize {
    if let Some(next) = next_significant(tokens, index)
        && word_is(&tokens[next].token, "FOR")
    {
        index = skip_until(tokens, next, |token| *token == Token::LParen);
    }
    let Some(open) = next_significant(tokens, index) else {
        return index;
    };
    if tokens[open].token != Token::LParen {
        return index;
    }
    skip_until(tokens, open, |token| *token == Token::RParen) + 1
}

/// Index of the first token at or after `index` (statement terminator included) that
/// matches `stop`, never stepping past the statement's `;`.
fn skip_until(tokens: &[TokenWithLocation], index: usize, stop: impl Fn(&Token) -> bool) -> usize {
    tokens[index..]
        .iter()
        .position(|token| token.token == Token::SemiColon || stop(&token.token))
        .map_or(tokens.len(), |offset| index + offset)
}

fn skip_words(tokens: &[TokenWithLocation], mut index: usize, count: usize) -> usize {
    for _ in 0..count {
        match next_significant(tokens, index) {
            Some(next) if tokens[next].token != Token::SemiColon => index = next + 1,
            _ => break,
        }
    }
    index
}

fn next_significant(tokens: &[TokenWithLocation], index: usize) -> Option<usize> {
    tokens
        .get(index..)?
        .iter()
        .position(|token| is_significant(&token.token))
        .map(|offset| index + offset)
}

fn is_significant(token: &Token) -> bool {
    !matches!(token, Token::Whitespace(_) | Token::EOF)
}

fn ends_operand(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_)
            | Token::Number(..)
            | Token::RParen
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
    )
}

fn word_is(token: &Token, keyword: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

fn with_token(original: &TokenWithLocation, token: Token) -> TokenWithLocation {
    TokenWithLocation {
        token,
        location: original.location,
    }
}
