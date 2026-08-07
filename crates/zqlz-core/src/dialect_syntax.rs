//! Reusable syntax term metadata for dialect-aware editor features.
//!
//! Driver crates can grow this over time or replace these static lists with
//! generated metadata. Keeping the lists in core prevents editor-only dialect
//! knowledge from drifting away from query execution and completion code.

use sqlparser::dialect::{Dialect as SqlParserDialect, GenericDialect};
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxTermProfile {
    pub base_keywords: &'static [&'static str],
    pub dialect_keywords: &'static [&'static str],
    pub base_functions: &'static [&'static str],
    pub dialect_functions: &'static [&'static str],
    pub base_types: &'static [&'static str],
    pub dialect_types: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntaxContextToken {
    Word(String),
    LParen,
    RParen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignificantSqlToken {
    Word(String),
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncompleteSqlCompletionContext {
    SelectList,
    FromClause,
    JoinClause,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCompletionTriggerContext {
    pub before_cursor: String,
    pub should_show: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxCompletionContextKind {
    AfterDot,
    SelectList,
    ConditionClause,
    FromClause,
    JoinClause,
    CreateTable,
    Subquery,
    CommonTableExpression,
    General,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxCompletionItemKind {
    Field,
    Function,
    Keyword,
    Operator,
    Struct,
    Class,
    Interface,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxOperatorCompletion {
    pub label: &'static str,
    pub detail: &'static str,
    pub insert_text: &'static str,
    pub sort_text: &'static str,
}

pub fn supported_syntax_profiles() -> &'static [&'static str] {
    &[
        "sql",
        "postgresql",
        "mysql",
        "sqlite",
        "duckdb",
        "mssql",
        "clickhouse",
        "mongodb",
        "redis",
    ]
}

pub fn syntax_term_profile(language_profile: &str) -> SyntaxTermProfile {
    let base_terms = !matches!(
        normalize_syntax_profile(language_profile),
        "mongodb" | "redis"
    );

    SyntaxTermProfile {
        base_keywords: if base_terms {
            base_keyword_terms()
        } else {
            &[]
        },
        dialect_keywords: dialect_keyword_terms(language_profile),
        base_functions: if base_terms {
            base_function_terms()
        } else {
            &[]
        },
        dialect_functions: dialect_function_terms(language_profile),
        base_types: if base_terms { base_type_terms() } else { &[] },
        dialect_types: dialect_type_terms(language_profile),
    }
}

pub fn base_keyword_terms() -> &'static [&'static str] {
    &[
        "ADD",
        "ALTER",
        "ARRAY",
        "ALL",
        "AND",
        "AS",
        "ASC",
        "AUTO_INCREMENT",
        "AUTOINCREMENT",
        "BETWEEN",
        "BIGINT",
        "BLOB",
        "BOOLEAN",
        "BY",
        "CASE",
        "CHAR",
        "CHECK",
        "CASCADE",
        "COLUMN",
        "COMMIT",
        "CONFLICT",
        "CREATE",
        "CROSS",
        "CURRENT_TIMESTAMP",
        "DATABASE",
        "DATE",
        "DATETIME",
        "DEFAULT",
        "DECLARE",
        "DECIMAL",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DOUBLE",
        "DROP",
        "ELSE",
        "END",
        "ENUM",
        "EXCEPT",
        "EXPLAIN",
        "FILTER",
        "FLOAT",
        "FULL",
        "FROM",
        "GROUP",
        "GRANT",
        "HAVING",
        "EXISTS",
        "IF",
        "IDENTITY",
        "INDEX",
        "INNER",
        "INSERT",
        "INTO",
        "INTERVAL",
        "INTERSECT",
        "INT",
        "INTEGER",
        "IN",
        "IS",
        "JOIN",
        "JSON",
        "KEY",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MERGE",
        "NATURAL",
        "NOT",
        "NOT NULL",
        "NULL",
        "NUMERIC",
        "ON",
        "OR",
        "ORDER",
        "OUTER",
        "OFFSET",
        "OVER",
        "PARTITION",
        "PRIMARY",
        "PRIMARY KEY",
        "REAL",
        "REVOKE",
        "REFERENCES",
        "RECURSIVE",
        "RESTART",
        "RETURNING",
        "RIGHT",
        "ROLLBACK",
        "ROLLUP",
        "SELECT",
        "SET",
        "SET DEFAULT",
        "SET NULL",
        "TABLE",
        "TEMP",
        "TEXT",
        "THEN",
        "TIMESTAMP",
        "TO",
        "TRANSACTION",
        "TRUNCATE",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "UPSERT",
        "USING",
        "VALUES",
        "VARCHAR",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
        "FOREIGN KEY",
        "GENERATED ALWAYS AS",
        "NO ACTION",
        "ON DELETE",
        "ON UPDATE",
    ]
}

pub fn keyword_term_category(keyword: &str) -> crate::KeywordCategory {
    match keyword.to_ascii_uppercase().as_str() {
        "SELECT" | "FROM" | "WHERE" | "WITH" | "DISTINCT" | "ALL" | "HAVING" | "GROUP" | "BY"
        | "ORDER" | "LIMIT" | "OFFSET" | "UNION" | "INTERSECT" | "EXCEPT" | "RETURNING"
        | "OVER" | "PARTITION" | "WINDOW" | "QUALIFY" => crate::KeywordCategory::Dql,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "SET" | "VALUES" | "INTO" | "UPSERT"
        | "REPLACE" => crate::KeywordCategory::Dml,
        "CREATE"
        | "ALTER"
        | "DROP"
        | "TABLE"
        | "VIEW"
        | "INDEX"
        | "DATABASE"
        | "SCHEMA"
        | "FUNCTION"
        | "PROCEDURE"
        | "TRIGGER"
        | "SEQUENCE"
        | "TYPE"
        | "DOMAIN"
        | "EXTENSION"
        | "COLUMN"
        | "ADD"
        | "PRIMARY"
        | "KEY"
        | "FOREIGN"
        | "REFERENCES"
        | "UNIQUE"
        | "CHECK"
        | "DEFAULT"
        | "CONSTRAINT"
        | "TEMP"
        | "TEMPORARY"
        | "IDENTITY"
        | "AUTOINCREMENT"
        | "AUTO_INCREMENT"
        | "GENERATED"
        | "COLLATE"
        | "CASCADE"
        | "RESTRICT"
        | "NOT NULL"
        | "PRIMARY KEY"
        | "FOREIGN KEY"
        | "GENERATED ALWAYS AS"
        | "NO ACTION"
        | "SET DEFAULT"
        | "SET NULL"
        | "ON DELETE"
        | "ON UPDATE" => crate::KeywordCategory::Ddl,
        "GRANT" | "REVOKE" | "DENY" => crate::KeywordCategory::Dcl,
        "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "TRANSACTION" | "RELEASE" => {
            crate::KeywordCategory::Transaction
        }
        "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS" | "NATURAL" | "ON"
        | "USING" | "AS" | "LATERAL" => crate::KeywordCategory::Clause,
        "AND" | "OR" | "NOT" | "IN" | "LIKE" | "BETWEEN" | "EXISTS" | "IS" | "NULL" | "ASC"
        | "DESC" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" => crate::KeywordCategory::Operator,
        "SHOW" | "PRAGMA" | "EXPLAIN" | "DESCRIBE" | "ANALYZE" | "VACUUM" | "REINDEX"
        | "CLUSTER" | "OPTIMIZE" | "CALL" | "EXEC" | "EXECUTE" | "USE" => {
            crate::KeywordCategory::DatabaseSpecific
        }
        _ => crate::KeywordCategory::Other,
    }
}

pub fn keyword_term_rank(keyword: &str) -> u8 {
    match keyword.to_ascii_uppercase().as_str() {
        "SELECT" => 0,
        "INSERT" => 1,
        "UPDATE" => 2,
        "DELETE" => 3,
        "WITH" => 4,
        "CREATE" => 5,
        "FROM" => 10,
        "WHERE" => 11,
        "JOIN" => 12,
        "ON" => 13,
        "AS" => 14,
        "GROUP" => 15,
        "ORDER" => 16,
        "HAVING" => 17,
        "LIMIT" => 18,
        "UNION" => 19,
        "AND" => 20,
        "OR" => 21,
        "NOT" => 22,
        "IN" => 23,
        "LIKE" => 24,
        "BETWEEN" => 25,
        "EXISTS" => 26,
        "BEGIN" => 30,
        "COMMIT" => 31,
        "ROLLBACK" => 32,
        "ALTER" => 35,
        "DROP" => 36,
        _ => 80,
    }
}

pub fn keyword_category_rank(category: crate::KeywordCategory) -> u8 {
    match category {
        crate::KeywordCategory::Dql => 0,
        crate::KeywordCategory::Dml => 1,
        crate::KeywordCategory::Ddl => 2,
        crate::KeywordCategory::Transaction => 3,
        crate::KeywordCategory::Clause => 4,
        crate::KeywordCategory::Operator => 5,
        crate::KeywordCategory::Dcl => 6,
        crate::KeywordCategory::DatabaseSpecific => 7,
        crate::KeywordCategory::Other => 8,
    }
}

pub fn keyword_term_allowed_in_select_expression(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_uppercase().as_str(),
        "DISTINCT"
            | "AS"
            | "FROM"
            | "CASE"
            | "CAST"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "FILTER"
            | "OVER"
            | "PARTITION"
            | "WINDOW"
            | "TOP"
    )
}

pub fn keyword_category_allowed_in_select_expression(category: crate::KeywordCategory) -> bool {
    matches!(
        category,
        crate::KeywordCategory::Dql
            | crate::KeywordCategory::Clause
            | crate::KeywordCategory::Operator
            | crate::KeywordCategory::DatabaseSpecific
            | crate::KeywordCategory::Other
    )
}

pub fn keyword_allowed_in_select_expression(
    keyword: &str,
    category: crate::KeywordCategory,
) -> bool {
    keyword_category_allowed_in_select_expression(category)
        && keyword_term_allowed_in_select_expression(keyword)
}

pub fn keyword_term_allowed_in_condition(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_uppercase().as_str(),
        "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "LIKE"
            | "BETWEEN"
            | "IS"
            | "NULL"
            | "EXISTS"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
    )
}

pub fn keyword_category_allowed_in_condition(category: crate::KeywordCategory) -> bool {
    matches!(
        category,
        crate::KeywordCategory::Dql
            | crate::KeywordCategory::Operator
            | crate::KeywordCategory::Clause
    )
}

pub fn keyword_allowed_in_condition(keyword: &str, category: crate::KeywordCategory) -> bool {
    keyword_category_allowed_in_condition(category) && keyword_term_allowed_in_condition(keyword)
}

pub fn keyword_term_allowed_in_join_condition(keyword: &str) -> bool {
    matches!(keyword.to_ascii_uppercase().as_str(), "ON" | "USING")
}

pub fn keyword_allowed_in_join_condition(keyword: &str, category: crate::KeywordCategory) -> bool {
    matches!(category, crate::KeywordCategory::Clause)
        && keyword_term_allowed_in_join_condition(keyword)
}

pub fn keyword_term_allowed_in_cte(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_uppercase().as_str(),
        "SELECT" | "FROM" | "WHERE" | "AS"
    )
}

pub fn keyword_allowed_in_cte(keyword: &str, _category: crate::KeywordCategory) -> bool {
    keyword_term_allowed_in_cte(keyword)
}

pub fn keyword_term_allowed_in_from_clause(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_uppercase().as_str(),
        "LATERAL" | "TABLE" | "UNNEST"
    )
}

pub fn keyword_category_allowed_in_from_clause(category: crate::KeywordCategory) -> bool {
    matches!(
        category,
        crate::KeywordCategory::Clause
            | crate::KeywordCategory::Dql
            | crate::KeywordCategory::DatabaseSpecific
    )
}

pub fn keyword_allowed_in_from_clause(keyword: &str, category: crate::KeywordCategory) -> bool {
    keyword_category_allowed_in_from_clause(category)
        && keyword_term_allowed_in_from_clause(keyword)
}

pub fn keyword_allowed_in_completion_context(
    context: SyntaxCompletionContextKind,
    keyword: &str,
    category: crate::KeywordCategory,
) -> bool {
    match context {
        SyntaxCompletionContextKind::SelectList => {
            keyword_allowed_in_select_expression(keyword, category)
        }
        SyntaxCompletionContextKind::ConditionClause => {
            keyword_allowed_in_condition(keyword, category)
        }
        SyntaxCompletionContextKind::FromClause => {
            keyword_allowed_in_from_clause(keyword, category)
        }
        SyntaxCompletionContextKind::JoinClause => {
            keyword_allowed_in_join_condition(keyword, category)
        }
        SyntaxCompletionContextKind::CommonTableExpression => {
            keyword_allowed_in_cte(keyword, category)
        }
        SyntaxCompletionContextKind::AfterDot
        | SyntaxCompletionContextKind::CreateTable
        | SyntaxCompletionContextKind::Subquery
        | SyntaxCompletionContextKind::General => false,
    }
}

pub fn keyword_completion_sort_prefix_for_context(
    context: SyntaxCompletionContextKind,
) -> &'static str {
    match context {
        SyntaxCompletionContextKind::SelectList => "2_condition",
        SyntaxCompletionContextKind::FromClause => "5_from_keyword",
        SyntaxCompletionContextKind::CommonTableExpression => "2_cte",
        SyntaxCompletionContextKind::ConditionClause | SyntaxCompletionContextKind::JoinClause => {
            "4_keyword"
        }
        SyntaxCompletionContextKind::AfterDot
        | SyntaxCompletionContextKind::CreateTable
        | SyntaxCompletionContextKind::Subquery
        | SyntaxCompletionContextKind::General => "4_keyword",
    }
}

pub fn create_table_keyword_insert_text(keyword: &str) -> String {
    match keyword.to_ascii_uppercase().as_str() {
        "CHECK" | "FOREIGN KEY" | "GENERATED ALWAYS AS" => format!("{keyword} ("),
        "CONSTRAINT" | "DEFAULT" | "INDEX" | "REFERENCES" | "ON DELETE" | "ON UPDATE" => {
            format!("{keyword} ")
        }
        _ => keyword.to_string(),
    }
}

pub fn create_table_keyword_sort_text(keyword: &str) -> String {
    let rank = match keyword.to_ascii_uppercase().as_str() {
        "NOT NULL" => 0,
        "PRIMARY KEY" => 1,
        "UNIQUE" => 2,
        "DEFAULT" => 3,
        "NULL" => 4,
        "CHECK" => 5,
        "REFERENCES" => 6,
        "CONSTRAINT" => 7,
        "FOREIGN KEY" => 8,
        "INDEX" => 9,
        _ => 50,
    };
    format!("5_{rank:02}_{keyword}")
}

pub fn data_type_completion_detail(data_type: &crate::DataTypeInfo) -> String {
    data_type
        .description
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("{:?} data type", data_type.category))
}

pub fn data_type_insert_text(data_type: &crate::DataTypeInfo) -> String {
    if data_type.accepts_length {
        format!(
            "{}({})",
            data_type.name,
            data_type.default_length.unwrap_or(255)
        )
    } else {
        data_type.name.to_string()
    }
}

pub fn data_type_sort_text(data_type: &crate::DataTypeInfo) -> String {
    format!("4_{:?}_{}", data_type.category, data_type.name)
}

pub fn data_type_labels(data_type: &crate::DataTypeInfo) -> impl Iterator<Item = &str> {
    std::iter::once(data_type.name.as_ref())
        .chain(data_type.aliases.iter().map(|alias| alias.as_ref()))
}

pub fn data_type_matches_suffix(data_type: &crate::DataTypeInfo, trimmed_text: &str) -> bool {
    let upper = trimmed_text.to_ascii_uppercase();
    data_type_labels(data_type).any(|label| upper.ends_with(&label.to_ascii_uppercase()))
}

pub fn keyword_category_allowed_at_statement_start(category: crate::KeywordCategory) -> bool {
    matches!(
        category,
        crate::KeywordCategory::Dql
            | crate::KeywordCategory::Dml
            | crate::KeywordCategory::Ddl
            | crate::KeywordCategory::Transaction
    )
}

pub fn keyword_relevant_for_context(
    keyword: &str,
    category: crate::KeywordCategory,
    is_start_context: bool,
    relevant_categories: &[crate::KeywordCategory],
) -> bool {
    let rank = keyword_term_rank(keyword);
    if is_start_context {
        keyword_category_allowed_at_statement_start(category)
            && (rank <= 5 || matches!(category, crate::KeywordCategory::Transaction) && rank <= 32)
    } else {
        relevant_categories.contains(&category) && rank <= 32
    }
}

pub fn keyword_info_relevant_for_context(
    keyword: &crate::KeywordInfo,
    is_start_context: bool,
    relevant_categories: &[crate::KeywordCategory],
) -> bool {
    keyword_relevant_for_context(
        keyword.keyword.as_ref(),
        keyword.category,
        is_start_context,
        relevant_categories,
    )
}

pub fn keyword_suffix_for_context(
    keyword: &crate::KeywordInfo,
    context_tokens: &[String],
    filter: &str,
) -> Option<String> {
    let mut parts = keyword.keyword.split_whitespace().collect::<Vec<_>>();
    let suffix = parts.pop()?;
    if parts.is_empty() {
        return None;
    }

    let filter = filter.to_ascii_lowercase();
    if !filter.is_empty() && !suffix.to_ascii_lowercase().starts_with(&filter) {
        return None;
    }

    if !parts.iter().all(|part| {
        context_tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case(part))
    }) {
        return None;
    }

    Some(suffix.to_string())
}

pub fn active_sql_context_tokens(sql: &str) -> Vec<SyntaxContextToken> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let Ok(tokens) = tokenizer.tokenize() else {
        return Vec::new();
    };

    tokens
        .into_iter()
        .filter_map(|token| match token {
            Token::Word(word) if word.quote_style.is_none() => {
                Some(SyntaxContextToken::Word(word.value.to_ascii_lowercase()))
            }
            Token::LParen => Some(SyntaxContextToken::LParen),
            Token::RParen => Some(SyntaxContextToken::RParen),
            _ => None,
        })
        .collect()
}

pub fn active_sql_unquoted_word_tokens(sql: &str) -> Vec<String> {
    active_sql_context_tokens(sql)
        .into_iter()
        .filter_map(|token| match token {
            SyntaxContextToken::Word(word) => Some(word),
            SyntaxContextToken::LParen | SyntaxContextToken::RParen => None,
        })
        .collect()
}

pub fn significant_sql_word_lookback(sql: &str) -> Vec<SignificantSqlToken> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let Ok(tokens) = tokenizer.tokenize() else {
        return Vec::new();
    };

    tokens
        .into_iter()
        .filter_map(|token| match token {
            Token::Whitespace(_) | Token::EOF | Token::LParen | Token::RParen => None,
            Token::Word(word) if word.quote_style.is_none() => {
                Some(SignificantSqlToken::Word(word.value.to_ascii_uppercase()))
            }
            _ => Some(SignificantSqlToken::Other),
        })
        .collect()
}

pub fn incomplete_sql_completion_context(sql: &str) -> Option<IncompleteSqlCompletionContext> {
    let tokens = significant_sql_word_lookback(sql);
    let last_word = tokens.last().and_then(significant_sql_token_word);

    if let Some(last_word) = last_word {
        if last_word == "SELECT" {
            return Some(IncompleteSqlCompletionContext::SelectList);
        }

        if incomplete_sql_table_source_keyword(last_word) {
            return Some(IncompleteSqlCompletionContext::FromClause);
        }

        if last_word == "JOIN" {
            return Some(IncompleteSqlCompletionContext::JoinClause);
        }
    }

    if tokens.len() >= 2 {
        let previous_word = significant_sql_token_word(&tokens[tokens.len() - 2])?;
        if previous_word == "SELECT" {
            return Some(IncompleteSqlCompletionContext::SelectList);
        }
        if previous_word == "JOIN" {
            return Some(IncompleteSqlCompletionContext::JoinClause);
        }
        if incomplete_sql_table_source_keyword(previous_word) {
            return Some(IncompleteSqlCompletionContext::FromClause);
        }
    }

    None
}

fn significant_sql_token_word(token: &SignificantSqlToken) -> Option<&str> {
    match token {
        SignificantSqlToken::Word(word) => Some(word),
        SignificantSqlToken::Other => None,
    }
}

fn incomplete_sql_table_source_keyword(word: &str) -> bool {
    matches!(word, "FROM" | "INTO" | "UPDATE" | "TABLE")
}

pub fn sql_has_query_context_tokens(sql: &str) -> bool {
    active_sql_unquoted_word_tokens(sql)
        .iter()
        .any(|token| sql_query_context_token(token))
}

pub fn sql_query_context_token(token: &str) -> bool {
    matches!(
        token,
        "select" | "with" | "from" | "join" | "insert" | "update" | "delete"
    )
}

pub fn select_wildcard_token_ranges(
    sql: &str,
    dialect: &dyn SqlParserDialect,
) -> Vec<std::ops::Range<usize>> {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let Ok(tokens) = tokenizer.tokenize_with_location() else {
        return Vec::new();
    };

    tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            if !matches!(token.token, Token::Mul) {
                return None;
            }

            if !token_is_select_wildcard(&tokens, index) {
                return None;
            }

            let start =
                sqlparser_location_to_offset(sql, token.location.line, token.location.column)?;
            Some(start..start + 1)
        })
        .collect()
}

pub fn keyword_token_byte_ranges(
    sql: &str,
    dialect: &dyn SqlParserDialect,
    keyword: &str,
) -> Vec<std::ops::Range<usize>> {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let Ok(tokens) = tokenizer.tokenize_with_location() else {
        return Vec::new();
    };

    tokens
        .into_iter()
        .filter_map(|token| {
            let Token::Word(word) = token.token else {
                return None;
            };
            if !word.value.eq_ignore_ascii_case(keyword) {
                return None;
            }

            let start =
                sqlparser_location_to_offset(sql, token.location.line, token.location.column)?;
            Some(start..start + word.value.len())
        })
        .collect()
}

fn token_is_select_wildcard(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
) -> bool {
    let previous = previous_significant_token(tokens, index);
    let next = next_significant_token(tokens, index);

    if previous.is_some_and(|token| matches!(token.token, Token::Period)) {
        return true;
    }

    let previous_allows_projection = previous.is_none_or(|token| {
        token_word_eq(&token.token, "select")
            || token_word_eq(&token.token, "distinct")
            || token_word_eq(&token.token, "all")
            || matches!(token.token, Token::Comma)
    });
    if !previous_allows_projection {
        return false;
    }

    next.is_none_or(|token| {
        token_word_eq(&token.token, "from")
            || matches!(token.token, Token::Comma | Token::RParen | Token::SemiColon)
    })
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

fn next_significant_token(
    tokens: &[sqlparser::tokenizer::TokenWithLocation],
    index: usize,
) -> Option<&sqlparser::tokenizer::TokenWithLocation> {
    tokens
        .get(index + 1..)?
        .iter()
        .find(|token| !matches!(token.token, Token::Whitespace(_)))
}

fn token_word_eq(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
}

pub fn is_inside_active_create_table_column_list(sql: &str) -> bool {
    let tokens = active_sql_context_tokens(sql);
    let Some(table_index) = last_create_table_token_index(&tokens) else {
        return false;
    };

    let Some(open_paren_index) = tokens[table_index + 1..]
        .iter()
        .position(|token| matches!(token, SyntaxContextToken::LParen))
        .map(|relative_index| table_index + 1 + relative_index)
    else {
        return false;
    };

    let mut depth = 0i32;
    for token in tokens[open_paren_index..].iter() {
        match token {
            SyntaxContextToken::LParen => depth += 1,
            SyntaxContextToken::RParen => {
                depth -= 1;
            }
            _ => {}
        }
    }

    depth > 0
}

pub fn create_table_prefers_constraint_terms(
    sql: &str,
    data_types: &[crate::DataTypeInfo],
) -> bool {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let Ok(tokens) = tokenizer.tokenize() else {
        return false;
    };

    let last_significant = tokens
        .iter()
        .rev()
        .find(|token| !matches!(token, Token::Whitespace(_) | Token::EOF));
    if matches!(last_significant, Some(Token::Comma)) {
        return true;
    }

    if !sql.ends_with(char::is_whitespace) {
        return false;
    }

    let trimmed = sql.trim_end();
    data_types
        .iter()
        .any(|data_type| data_type_matches_suffix(data_type, trimmed))
}

pub fn syntax_completion_context_bucket(
    context: SyntaxCompletionContextKind,
    item_kind: SyntaxCompletionItemKind,
    item_label: &str,
    prefer_create_table_constraints: bool,
) -> i32 {
    match context {
        SyntaxCompletionContextKind::AfterDot => match item_kind {
            SyntaxCompletionItemKind::Field => 100,
            _ => 0,
        },
        SyntaxCompletionContextKind::SelectList => match item_kind {
            SyntaxCompletionItemKind::Field => 90,
            SyntaxCompletionItemKind::Keyword if item_label.eq_ignore_ascii_case("FROM") => 65,
            SyntaxCompletionItemKind::Function => 60,
            SyntaxCompletionItemKind::Keyword
                if keyword_term_allowed_in_select_expression(item_label) =>
            {
                40
            }
            SyntaxCompletionItemKind::Keyword => 30,
            _ => 20,
        },
        SyntaxCompletionContextKind::ConditionClause => match item_kind {
            SyntaxCompletionItemKind::Field => 90,
            SyntaxCompletionItemKind::Keyword if keyword_term_allowed_in_condition(item_label) => {
                70
            }
            SyntaxCompletionItemKind::Operator => 65,
            SyntaxCompletionItemKind::Function => 50,
            SyntaxCompletionItemKind::Keyword => 30,
            _ => 20,
        },
        SyntaxCompletionContextKind::FromClause => match item_kind {
            SyntaxCompletionItemKind::Class | SyntaxCompletionItemKind::Interface => 60,
            SyntaxCompletionItemKind::Keyword => 25,
            _ => 10,
        },
        SyntaxCompletionContextKind::JoinClause => match item_kind {
            SyntaxCompletionItemKind::Class | SyntaxCompletionItemKind::Interface => 70,
            SyntaxCompletionItemKind::Keyword => 25,
            _ => 10,
        },
        SyntaxCompletionContextKind::CreateTable => match item_kind {
            SyntaxCompletionItemKind::Keyword if prefer_create_table_constraints => 85,
            SyntaxCompletionItemKind::Struct if prefer_create_table_constraints => 60,
            SyntaxCompletionItemKind::Struct => 75,
            SyntaxCompletionItemKind::Keyword => 55,
            _ => 15,
        },
        SyntaxCompletionContextKind::Subquery => match item_kind {
            SyntaxCompletionItemKind::Field => 85,
            SyntaxCompletionItemKind::Function => 45,
            SyntaxCompletionItemKind::Keyword => 30,
            _ => 15,
        },
        SyntaxCompletionContextKind::CommonTableExpression => match item_kind {
            SyntaxCompletionItemKind::Keyword => 30,
            _ => 15,
        },
        SyntaxCompletionContextKind::General => match item_kind {
            SyntaxCompletionItemKind::Class | SyntaxCompletionItemKind::Interface => 60,
            SyntaxCompletionItemKind::Field => 55,
            SyntaxCompletionItemKind::Keyword => 35,
            SyntaxCompletionItemKind::Function => 30,
            _ => 10,
        },
    }
}

pub fn syntax_operator_completions_for_context(
    context: SyntaxCompletionContextKind,
) -> &'static [SyntaxOperatorCompletion] {
    match context {
        SyntaxCompletionContextKind::ConditionClause | SyntaxCompletionContextKind::JoinClause => {
            condition_operator_completion_terms()
        }
        SyntaxCompletionContextKind::AfterDot
        | SyntaxCompletionContextKind::SelectList
        | SyntaxCompletionContextKind::FromClause
        | SyntaxCompletionContextKind::CreateTable
        | SyntaxCompletionContextKind::Subquery
        | SyntaxCompletionContextKind::CommonTableExpression
        | SyntaxCompletionContextKind::General => &[],
    }
}

pub fn condition_operator_completion_terms() -> &'static [SyntaxOperatorCompletion] {
    &[
        SyntaxOperatorCompletion {
            label: "= (equals)",
            detail: "Equality comparison",
            insert_text: "= ",
            sort_text: "9_operator_eq",
        },
        SyntaxOperatorCompletion {
            label: "<> (not equal)",
            detail: "Inequality comparison",
            insert_text: "<> ",
            sort_text: "9_operator_ne",
        },
        SyntaxOperatorCompletion {
            label: "< (less than)",
            detail: "Less-than comparison",
            insert_text: "< ",
            sort_text: "9_operator_lt",
        },
        SyntaxOperatorCompletion {
            label: "> (greater than)",
            detail: "Greater-than comparison",
            insert_text: "> ",
            sort_text: "9_operator_gt",
        },
        SyntaxOperatorCompletion {
            label: "<= (less or equal)",
            detail: "Less-than-or-equal comparison",
            insert_text: "<= ",
            sort_text: "9_operator_lte",
        },
        SyntaxOperatorCompletion {
            label: ">= (greater or equal)",
            detail: "Greater-than-or-equal comparison",
            insert_text: ">= ",
            sort_text: "9_operator_gte",
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresJsonOperator {
    pub symbol: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub example: &'static str,
    pub sort_text: &'static str,
}

pub fn postgres_json_operators() -> &'static [PostgresJsonOperator] {
    &[
        PostgresJsonOperator {
            symbol: "->",
            name: "Get JSON field (as json)",
            description: "Extracts a JSON object field by key or array element by index, returning json/jsonb. Chain further operators on the result.",
            example: "data->'address'->'city'",
            sort_text: "9_operator_json_arrow",
        },
        PostgresJsonOperator {
            symbol: "->>",
            name: "Get JSON field (as text)",
            description: "Extracts a JSON object field by key or array element by index, returning text. Comparisons on the result are string comparisons — cast for numeric ordering, e.g. (data->>'age')::int.",
            example: "data->>'email'",
            sort_text: "9_operator_json_arrow_text",
        },
        PostgresJsonOperator {
            symbol: "#>",
            name: "Get JSON at path (as json)",
            description: "Extracts JSON at the given path (a text array), returning json/jsonb.",
            example: "data#>'{address,city}'",
            sort_text: "9_operator_json_path",
        },
        PostgresJsonOperator {
            symbol: "#>>",
            name: "Get JSON at path (as text)",
            description: "Extracts JSON at the given path (a text array), returning text.",
            example: "data#>>'{address,city}'",
            sort_text: "9_operator_json_path_text",
        },
        PostgresJsonOperator {
            symbol: "@>",
            name: "Contains",
            description: "True if the left jsonb value contains the right jsonb value. Can use a GIN index on the column.",
            example: "data @> '{\"active\": true}'",
            sort_text: "9_operator_json_contains",
        },
        PostgresJsonOperator {
            symbol: "<@",
            name: "Contained by",
            description: "True if the left jsonb value is contained within the right jsonb value.",
            example: "'{\"active\": true}' <@ data",
            sort_text: "9_operator_json_contained",
        },
        PostgresJsonOperator {
            symbol: "?",
            name: "Key exists",
            description: "True if the text string exists as a top-level key or array element in the jsonb value. Can use a GIN index.",
            example: "data ? 'email'",
            sort_text: "9_operator_json_exists",
        },
        PostgresJsonOperator {
            symbol: "?|",
            name: "Any key exists",
            description: "True if any of the strings in the text array exist as top-level keys or array elements.",
            example: "data ?| array['email', 'phone']",
            sort_text: "9_operator_json_exists_any",
        },
        PostgresJsonOperator {
            symbol: "?&",
            name: "All keys exist",
            description: "True if all of the strings in the text array exist as top-level keys or array elements.",
            example: "data ?& array['email', 'phone']",
            sort_text: "9_operator_json_exists_all",
        },
        PostgresJsonOperator {
            symbol: "@?",
            name: "JSON path exists",
            description: "True if the jsonpath returns any item for the jsonb value. Can use a GIN index.",
            example: "data @? '$.items[*] ? (@.qty > 2)'",
            sort_text: "9_operator_json_path_exists",
        },
        PostgresJsonOperator {
            symbol: "@@",
            name: "JSON path predicate",
            description: "Returns the result of a jsonpath predicate check for the jsonb value.",
            example: "data @@ '$.qty > 2'",
            sort_text: "9_operator_json_path_match",
        },
        PostgresJsonOperator {
            symbol: "#-",
            name: "Delete at path",
            description: "Deletes the field or array element at the given path from a jsonb value.",
            example: "data #- '{address,city}'",
            sort_text: "9_operator_json_delete_path",
        },
    ]
}

pub fn postgres_json_operator(symbol: &str) -> Option<&'static PostgresJsonOperator> {
    postgres_json_operators()
        .iter()
        .find(|operator| operator.symbol == symbol)
}

pub fn syntax_completion_dedup_key(
    item_kind: SyntaxCompletionItemKind,
    label: &str,
    filter_text: Option<&str>,
) -> String {
    let identifier = filter_text.unwrap_or(label).to_ascii_lowercase();
    format!("{}:{identifier}", item_kind.dedup_key_name())
}

pub fn compare_syntax_completion_sort_texts(
    left_sort_text: Option<&str>,
    left_label: &str,
    right_sort_text: Option<&str>,
    right_label: &str,
) -> Ordering {
    let left_sort_text = left_sort_text.unwrap_or(left_label);
    let right_sort_text = right_sort_text.unwrap_or(right_label);

    right_sort_text.cmp(left_sort_text)
}

impl SyntaxCompletionItemKind {
    pub fn dedup_key_name(self) -> &'static str {
        match self {
            SyntaxCompletionItemKind::Field => "FIELD",
            SyntaxCompletionItemKind::Function => "FUNCTION",
            SyntaxCompletionItemKind::Keyword => "KEYWORD",
            SyntaxCompletionItemKind::Operator => "OPERATOR",
            SyntaxCompletionItemKind::Struct => "STRUCT",
            SyntaxCompletionItemKind::Class => "CLASS",
            SyntaxCompletionItemKind::Interface => "INTERFACE",
            SyntaxCompletionItemKind::Other => "TEXT",
        }
    }
}

pub fn sql_completion_trigger_context(
    sql: &str,
    cursor_offset: usize,
    current_word: &str,
    is_manual_trigger: bool,
) -> SqlCompletionTriggerContext {
    let safe_offset = clamp_to_char_boundary(sql, cursor_offset);
    let before_cursor = sql[..safe_offset].to_string();

    if is_manual_trigger {
        return SqlCompletionTriggerContext {
            before_cursor,
            should_show: true,
        };
    }

    let position_to_check = safe_offset.saturating_sub(1);
    let protected_ranges = crate::sql_protected_ranges(sql);
    if safe_offset > 0 && crate::is_position_in_sql_ranges(position_to_check, &protected_ranges) {
        return SqlCompletionTriggerContext {
            before_cursor,
            should_show: false,
        };
    }

    let is_after_dot = before_cursor.ends_with('.')
        || (current_word.is_empty() && before_cursor.trim_end().ends_with('.'));
    let is_after_operator_char = before_cursor
        .chars()
        .next_back()
        .is_some_and(|character| matches!(character, '-' | '>' | '#' | '@' | '?' | '|' | '&'));
    let is_after_trigger_char = before_cursor.ends_with(' ')
        || before_cursor.ends_with('(')
        || before_cursor.ends_with(',')
        || is_after_operator_char
        || is_after_dot;
    let should_show = if current_word.is_empty() {
        is_after_trigger_char || before_cursor.is_empty()
    } else {
        true
    };

    SqlCompletionTriggerContext {
        before_cursor,
        should_show,
    }
}

pub fn syntax_completion_prefix(
    source: &str,
    byte_offset: usize,
    extra_word_chars: &[char],
) -> String {
    let end = clamp_to_char_boundary(source, byte_offset);
    let mut start = end;

    while start > 0 {
        let Some((previous_start, character)) = source[..start].char_indices().next_back() else {
            break;
        };
        if !syntax_completion_word_char(character, extra_word_chars) {
            break;
        }
        start = previous_start;
    }

    source[start..end].to_string()
}

pub fn syntax_completion_word_char(character: char, extra_word_chars: &[char]) -> bool {
    character.is_alphanumeric() || character == '_' || extra_word_chars.contains(&character)
}

pub fn semicolon_insertion_offset(text: &str, dialect: &dyn SqlParserDialect) -> Option<usize> {
    let mut tokenizer = Tokenizer::new(dialect, text);
    let tokens = tokenizer.tokenize_with_location().ok()?;
    let mut seen_significant = false;
    let mut last_significant_is_semicolon = false;
    let mut trailing_line_comment_start = None;

    for token in tokens {
        match &token.token {
            Token::Whitespace(Whitespace::SingleLineComment { .. }) => {
                if seen_significant {
                    trailing_line_comment_start = sqlparser_location_to_offset(
                        text,
                        token.location.line,
                        token.location.column,
                    );
                }
            }
            Token::Whitespace(_) => {}
            Token::EOF => {}
            _ => {
                seen_significant = true;
                last_significant_is_semicolon = matches!(token.token, Token::SemiColon);
                trailing_line_comment_start = None;
            }
        }
    }

    if !seen_significant || last_significant_is_semicolon {
        return None;
    }

    let insert_offset = trailing_line_comment_start
        .map(|comment_start| text[..comment_start].trim_end().len())
        .unwrap_or_else(|| text.trim_end().len());
    (insert_offset > 0).then_some(insert_offset)
}

pub fn unclosed_quote_insertion(
    text: &str,
    diagnostic_start_line: u32,
    diagnostic_start_character: u32,
    dialect: &dyn SqlParserDialect,
) -> Option<(u32, u32, char)> {
    let quote_offset =
        line_column_to_offset(text, diagnostic_start_line, diagnostic_start_character)?;
    let quote = text.get(quote_offset..)?.chars().next()?;
    if !matches!(quote, '\'' | '"') {
        return None;
    }

    let insert_offset = semicolon_insertion_offset(text, dialect).unwrap_or(text.len());
    let (line, character) = line_column_for_offset(text, insert_offset);
    Some((line, character, quote))
}

pub fn sqlparser_location_to_offset(text: &str, line: u64, column: u64) -> Option<usize> {
    let target_line = line.checked_sub(1)? as usize;
    let target_column = column.checked_sub(1)? as usize;
    let mut offset = 0usize;

    for (line_index, line_text) in text.split_inclusive('\n').enumerate() {
        if line_index == target_line {
            let line_without_newline = line_text.trim_end_matches('\n');
            let mut column_offset = 0usize;
            for (character_index, character) in line_without_newline.chars().enumerate() {
                if character_index == target_column {
                    return Some(offset + column_offset);
                }
                column_offset += character.len_utf8();
            }
            return (target_column == line_without_newline.chars().count())
                .then_some(offset + column_offset);
        }
        offset += line_text.len();
    }

    None
}

pub fn line_column_to_offset(text: &str, line: u32, character: u32) -> Option<usize> {
    let mut offset = 0usize;
    for (line_index, line_text) in text.split_inclusive('\n').enumerate() {
        if line_index == line as usize {
            let line_without_newline = line_text.trim_end_matches('\n');
            let character = character as usize;
            return (character <= line_without_newline.len()).then_some(offset + character);
        }
        offset += line_text.len();
    }

    (line == 0 && character == 0 && text.is_empty()).then_some(0)
}

pub fn line_column_for_offset(text: &str, offset: usize) -> (u32, u32) {
    let mut line = 0u32;
    let mut line_start = 0usize;

    for (index, character) in text.char_indices() {
        if index >= offset {
            break;
        }
        if character == '\n' {
            line += 1;
            line_start = index + character.len_utf8();
        }
    }

    (line, (offset.saturating_sub(line_start)) as u32)
}

fn clamp_to_char_boundary(text: &str, offset: usize) -> usize {
    let mut offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn last_create_table_token_index(tokens: &[SyntaxContextToken]) -> Option<usize> {
    let mut table_index = None;

    for index in 0..tokens.len() {
        if !matches!(&tokens[index], SyntaxContextToken::Word(word) if word == "create") {
            continue;
        }

        let mut next_index = index + 1;
        if matches!(
            tokens.get(next_index),
            Some(SyntaxContextToken::Word(word)) if matches!(word.as_str(), "temporary" | "temp")
        ) {
            next_index += 1;
        }

        if matches!(
            tokens.get(next_index),
            Some(SyntaxContextToken::Word(word)) if word == "table"
        ) {
            table_index = Some(next_index);
        }
    }

    table_index
}

pub fn keyword_context_is_start(context: &str) -> bool {
    let context_tokens = active_sql_unquoted_word_tokens(context);
    context_tokens.is_empty()
        || context.trim().is_empty()
        || (context.trim().len() <= 10 && !context_tokens.iter().any(|token| token == "select"))
}

pub fn keyword_context_categories(context: &str) -> Vec<crate::KeywordCategory> {
    let context_tokens = active_sql_unquoted_word_tokens(context);
    let last_clause = context_tokens
        .iter()
        .rev()
        .find_map(|token| keyword_term_is_context_clause(token).then_some(token.as_str()));

    keyword_context_categories_for_clause(last_clause).to_vec()
}

pub fn keyword_context_implies_condition_clause(context: &str) -> bool {
    let tokens = active_sql_unquoted_word_tokens(context);
    let Some(last_clause_index) = tokens
        .iter()
        .rposition(|token| keyword_term_is_context_clause(token))
    else {
        return false;
    };

    tokens
        .get(last_clause_index)
        .is_some_and(|token| keyword_term_implies_condition_clause(token))
}

pub fn keyword_context_categories_for_clause(
    clause: Option<&str>,
) -> &'static [crate::KeywordCategory] {
    use crate::KeywordCategory;

    match clause.map(str::to_ascii_lowercase).as_deref() {
        None => &[
            KeywordCategory::Dql,
            KeywordCategory::Dml,
            KeywordCategory::Ddl,
            KeywordCategory::Transaction,
        ],
        Some("with") => &[KeywordCategory::Dql, KeywordCategory::Clause],
        Some("select") => &[
            KeywordCategory::Dql,
            KeywordCategory::Clause,
            KeywordCategory::Operator,
        ],
        Some("from") | Some("join") => &[KeywordCategory::Clause, KeywordCategory::Dql],
        Some("where") | Some("having") | Some("on") => {
            &[KeywordCategory::Operator, KeywordCategory::Dql]
        }
        Some("group") | Some("order") => &[KeywordCategory::Dql, KeywordCategory::Operator],
        _ => &[KeywordCategory::Dql, KeywordCategory::Clause],
    }
}

pub fn keyword_term_is_context_clause(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_lowercase().as_str(),
        "with" | "select" | "from" | "join" | "where" | "having" | "on" | "group" | "order"
    )
}

pub fn keyword_term_implies_condition_clause(keyword: &str) -> bool {
    matches!(
        keyword.to_ascii_lowercase().as_str(),
        "where" | "having" | "on"
    )
}

pub fn dialect_keyword_terms(language_profile: &str) -> &'static [&'static str] {
    match normalize_syntax_profile(language_profile) {
        "mongodb" => &[
            "true", "false", "null", "$match", "$group", "$sort", "$project", "$limit", "$skip",
            "$lookup", "$unwind", "$set", "$sum", "$avg", "$min", "$max", "$gte", "$lte", "$gt",
            "$lt", "$in", "$nin",
        ],
        "postgresql" => &[
            "ANALYZE",
            "BIGSERIAL",
            "BYTEA",
            "CITEXT",
            "CONCURRENTLY",
            "DO",
            "ILIKE",
            "JSONB",
            "LATERAL",
            "MATERIALIZED",
            "NOTIFY",
            "PLPGSQL",
            "REINDEX",
            "SERIAL",
            "SMALLSERIAL",
            "UNLOGGED",
            "VACUUM",
        ],
        "mysql" => &[
            "ANALYZE",
            "AUTO_INCREMENT",
            "CHARSET",
            "COLLATE",
            "ENGINE",
            "FORCE",
            "IGNORE",
            "KEY",
            "LOCK",
            "MODIFY",
            "OPTIMIZE",
            "REGEXP",
            "REPLACE",
            "DUPLICATE",
            "SQL_CALC_FOUND_ROWS",
            "STRAIGHT_JOIN",
            "UNLOCK",
            "UNSIGNED",
            "ZEROFILL",
        ],
        "sqlite" => &[
            "ABORT",
            "ATTACH",
            "AUTOINCREMENT",
            "DETACH",
            "GLOB",
            "INDEXED",
            "MATCH",
            "PRAGMA",
            "RAISE",
            "REGEXP",
            "REPLACE",
            "ROWID",
            "VACUUM",
            "VIRTUAL",
            "WITHOUT",
        ],
        "duckdb" => &[
            "ATTACH",
            "COPY",
            "DESCRIBE",
            "DETACH",
            "DELIMITER",
            "EXPORT",
            "HEADER",
            "IMPORT",
            "INSTALL",
            "LOAD",
            "PERCENT",
            "PARTITION",
            "PIVOT",
            "PRAGMA",
            "QUALIFY",
            "SAMPLE",
            "SUMMARIZE",
            "UNPIVOT",
        ],
        "mssql" => &[
            "DENY",
            "FETCH",
            "GO",
            "IDENTITY",
            "MATCHED",
            "MERGE",
            "NEXT",
            "NOLOCK",
            "OFFSET",
            "ONLY",
            "OUTPUT",
            "PRINT",
            "ROWS",
            "TEMPORARY",
            "TOP",
            "USE",
        ],
        "clickhouse" => &[
            "ARRAY",
            "CODEC",
            "DAY",
            "FINAL",
            "FORMAT",
            "LOWCARDINALITY",
            "MATERIALIZE",
            "MATERIALIZED",
            "PARTITION",
            "POPULATE",
            "PREWHERE",
            "SAMPLE",
            "SETTINGS",
            "TTL",
            "TUPLE",
        ],
        "redis" => &[
            "APPEND",
            "AUTH",
            "COUNT",
            "CONFIG",
            "DECR",
            "DEL",
            "DISCARD",
            "EXEC",
            "EXISTS",
            "EX",
            "EXPIRE",
            "FLUSHALL",
            "FLUSHDB",
            "GET",
            "HDEL",
            "HGET",
            "HGETALL",
            "HSET",
            "INCR",
            "INFO",
            "JSON.GET",
            "JSON.SET",
            "KEYS",
            "LPOP",
            "LPUSH",
            "LRANGE",
            "MULTI",
            "MATCH",
            "NX",
            "PING",
            "PX",
            "PUBLISH",
            "RPOP",
            "RPUSH",
            "SADD",
            "SCAN",
            "SELECT",
            "SET",
            "SMEMBERS",
            "SREM",
            "SUBSCRIBE",
            "TTL",
            "UNSUBSCRIBE",
            "UNWATCH",
            "WATCH",
            "WITHSCORES",
            "XX",
            "ZADD",
            "ZRANGE",
            "ZREM",
        ],
        _ => &[],
    }
}

pub fn base_function_terms() -> &'static [&'static str] {
    &[
        "AVG", "COUNT", "COALESCE", "IFNULL", "LOWER", "MAX", "MIN", "NULLIF", "SUM", "TRIM",
        "UPPER",
    ]
}

pub fn function_term_rank(function: &str) -> u8 {
    match function.to_ascii_uppercase().as_str() {
        "COUNT" => 0,
        "SUM" => 1,
        "AVG" => 2,
        "MIN" => 3,
        "MAX" => 4,
        "UPPER" => 5,
        "LOWER" => 6,
        "TRIM" => 7,
        "COALESCE" => 8,
        "NULLIF" => 9,
        "IFNULL" => 10,
        _ => 50,
    }
}

pub fn aggregate_function_sort_text(function: &str) -> String {
    let priority = match function.to_ascii_uppercase().as_str() {
        "COUNT" => 0,
        "SUM" => 1,
        "AVG" => 2,
        "MIN" => 3,
        "MAX" => 4,
        _ => 50,
    };
    format!("3_{priority:02}_{}", function.to_ascii_uppercase())
}

pub fn scalar_function_sort_text(function: &str) -> String {
    let priority = match function.to_ascii_uppercase().as_str() {
        "UPPER" => 0,
        "LOWER" => 1,
        "TRIM" => 2,
        "SUBSTRING" | "SUBSTR" => 3,
        "COALESCE" => 4,
        "CONCAT" | "CONCAT_WS" => 5,
        _ => 50,
    };
    format!("4_{priority:02}_{}", function.to_ascii_uppercase())
}

pub fn function_category_sort_bucket(category: crate::FunctionCategory) -> u8 {
    match category {
        crate::FunctionCategory::Aggregate => 3,
        crate::FunctionCategory::String => 4,
        crate::FunctionCategory::Numeric => 5,
        crate::FunctionCategory::DateTime => 6,
        crate::FunctionCategory::Conversion => 7,
        crate::FunctionCategory::Conditional => 8,
        crate::FunctionCategory::Json => 9,
        crate::FunctionCategory::Array => 10,
        crate::FunctionCategory::Window => 11,
        crate::FunctionCategory::DatabaseSpecific => 12,
        crate::FunctionCategory::Other => 13,
    }
}

pub fn dialect_function_sort_text(function: &crate::SqlFunctionInfo) -> String {
    format!(
        "3_{:02}_{:02}_{:?}_{}",
        function_category_sort_bucket(function.category),
        function_term_rank(function.name.as_ref()),
        function.category,
        function.name.to_ascii_uppercase()
    )
}

pub fn dialect_function_detail(function: &crate::SqlFunctionInfo) -> String {
    function
        .description
        .as_ref()
        .map(ToString::to_string)
        .or_else(|| {
            function
                .return_type
                .as_ref()
                .map(|return_type| format!("{:?} function -> {}", function.category, return_type))
        })
        .unwrap_or_else(|| format!("{:?} function", function.category))
}

pub fn function_category_allowed_in_condition(category: crate::FunctionCategory) -> bool {
    matches!(
        category,
        crate::FunctionCategory::String
            | crate::FunctionCategory::Numeric
            | crate::FunctionCategory::DateTime
            | crate::FunctionCategory::Conversion
            | crate::FunctionCategory::Conditional
            | crate::FunctionCategory::Json
            | crate::FunctionCategory::Array
            | crate::FunctionCategory::DatabaseSpecific
            | crate::FunctionCategory::Other
    )
}

pub fn sql_function_call_context(
    text_before_cursor: &str,
    dialect: &dyn SqlParserDialect,
) -> Option<(String, usize)> {
    let mut tokenizer = Tokenizer::new(dialect, text_before_cursor);
    let tokens = tokenizer.tokenize_with_location().ok()?;
    let mut calls: Vec<(String, usize)> = Vec::new();
    let mut previous_word: Option<String> = None;

    for token in tokens {
        match token.token {
            Token::Word(word) => {
                previous_word = Some(word.value);
            }
            Token::LParen => {
                if let Some(function_name) = previous_word.take() {
                    calls.push((function_name, 0));
                }
            }
            Token::RParen => {
                calls.pop();
                previous_word = None;
            }
            Token::Comma => {
                if let Some((_, active_parameter)) = calls.last_mut() {
                    *active_parameter += 1;
                }
                previous_word = None;
            }
            Token::Whitespace(_) => {}
            _ => {
                previous_word = None;
            }
        }
    }

    calls.pop()
}

pub fn function_term_category(function: &str) -> crate::FunctionCategory {
    let function = function.to_ascii_uppercase();
    match function.as_str() {
        "AVG" | "COUNT" | "COUNTIF" | "GROUP_CONCAT" | "MAX" | "MIN" | "SUM" | "STRING_AGG"
        | "ARRAY_AGG" | "BOOL_AND" | "BOOL_OR" | "JSONB_AGG" | "JSONB_OBJECT_AGG" => {
            crate::FunctionCategory::Aggregate
        }
        "ROW_NUMBER" | "DENSE_RANK" | "RANK" | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE"
        | "NTH_VALUE" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" => crate::FunctionCategory::Window,
        "COALESCE" | "IF" | "IFNULL" | "ISNULL" | "NULLIF" => crate::FunctionCategory::Conditional,
        "CAST" | "CONVERT" | "PG_TYPEOF" | "TO_CHAR" | "TO_JSONB" => {
            crate::FunctionCategory::Conversion
        }
        "CONCAT" | "CONCAT_WS" | "LEFT" | "LOWER" | "LTRIM" | "REGEXP_REPLACE" | "RIGHT"
        | "SPLIT_PART" | "SUBSTR" | "SUBSTRING" | "TRIM" | "UPPER" => {
            crate::FunctionCategory::String
        }
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "POWER" | "RANDOM" | "ROUND" | "SQRT" => {
            crate::FunctionCategory::Numeric
        }
        "CURRENT_DATE" | "CURRENT_TIME" | "CURRENT_TIMESTAMP" | "DATE_FORMAT" | "DATE_PART"
        | "DATE_TRUNC" | "DATEDIFF" | "NOW" | "STRFTIME" | "SYSUTCDATETIME" => {
            crate::FunctionCategory::DateTime
        }
        name if name.starts_with("JSON") || name.starts_with("JSONB") => {
            crate::FunctionCategory::Json
        }
        name if name.starts_with("ARRAY") || name == "UNNEST" => crate::FunctionCategory::Array,
        _ => crate::FunctionCategory::DatabaseSpecific,
    }
}

pub fn dialect_function_terms(language_profile: &str) -> &'static [&'static str] {
    match normalize_syntax_profile(language_profile) {
        "postgresql" => &[
            "ARRAY_AGG",
            "ARRAY_LENGTH",
            "BOOL_AND",
            "BOOL_OR",
            "DATE_TRUNC",
            "DATE_PART",
            "DENSE_RANK",
            "GENERATE_SERIES",
            "GEN_RANDOM_UUID",
            "JSON_BUILD_OBJECT",
            "JSONB_AGG",
            "JSONB_BUILD_OBJECT",
            "JSONB_EACH",
            "JSONB_OBJECT_AGG",
            "JSONB_SET",
            "NOW",
            "PG_TYPEOF",
            "REGEXP_REPLACE",
            "ROW_NUMBER",
            "SPLIT_PART",
            "STRING_AGG",
            "TO_CHAR",
            "TO_JSONB",
            "UNNEST",
        ],
        "mysql" => &[
            "CONCAT",
            "CONNECTION_ID",
            "DENSE_RANK",
            "DATE_FORMAT",
            "DATEDIFF",
            "JSON_ARRAY",
            "JSON_CONTAINS",
            "GROUP_CONCAT",
            "IF",
            "JSON_EXTRACT",
            "JSON_LENGTH",
            "JSON_OBJECT",
            "NOW",
            "ROW_NUMBER",
            "TIMESTAMPDIFF",
            "UUID",
        ],
        "sqlite" => &[
            "ABS",
            "CONCAT",
            "DATE",
            "DATETIME",
            "DENSE_RANK",
            "GROUP_CONCAT",
            "HEX",
            "IIF",
            "INSTR",
            "JSON",
            "JSON_ARRAY",
            "JSON_EXTRACT",
            "JSON_OBJECT",
            "JSON_TYPE",
            "JULIANDAY",
            "LAG",
            "LEAD",
            "LIKELIHOOD",
            "LIKELY",
            "NTH_VALUE",
            "PRINTF",
            "QUOTE",
            "RANDOM",
            "ROW_NUMBER",
            "STRFTIME",
            "TOTAL",
            "TYPEOF",
            "UNIXEPOCH",
            "UNLIKELY",
            "ZEROBLOB",
        ],
        "duckdb" => &[
            "APPROX_COUNT_DISTINCT",
            "DATE_DIFF",
            "DATE_PART",
            "EPOCH_MS",
            "JSON_EXTRACT",
            "JSON_EXTRACT_STRING",
            "LIST",
            "LIST_AGG",
            "LIST_CONTAINS",
            "LIST_VALUE",
            "MEDIAN",
            "PIVOT",
            "READ_CSV",
            "READ_JSON",
            "READ_PARQUET",
            "ROW_NUMBER",
            "SPLIT_PART",
            "UNNEST",
        ],
        "mssql" => &[
            "CHARINDEX",
            "CONVERT",
            "DATEADD",
            "DATEDIFF",
            "GETDATE",
            "IIF",
            "ISJSON",
            "ISNULL",
            "JSON_QUERY",
            "JSON_VALUE",
            "LEN",
            "NCHAR",
            "NEWID",
            "OPENJSON",
            "PATINDEX",
            "QUOTENAME",
            "SCOPE_IDENTITY",
            "STRING_AGG",
            "SYSUTCDATETIME",
            "TRY_CONVERT",
        ],
        "clickhouse" => &[
            "ARRAYJOIN",
            "ARRAYMAP",
            "COUNTIF",
            "DATEADD",
            "DATEDIFF",
            "FORMATDATETIME",
            "GROUPARRAY",
            "JSONEXTRACT",
            "JSONEXTRACTBOOL",
            "JSONEXTRACTINT",
            "JSONEXTRACTRAW",
            "JSONEXTRACTSTRING",
            "LENGTHUTF8",
            "MULTIIF",
            "QUANTILE",
            "TODECIMAL128",
            "TOFLOAT64",
            "TOINT64",
            "TODATETIME",
            "TODATETIME64",
            "TOSTARTOFINTERVAL",
            "TOSTARTOFMONTH",
            "TOUINT64",
            "TOUUID",
            "TOYYYYMM",
            "UNIQ",
            "UNIQEXACT",
        ],
        "mongodb" => &[
            "aggregate",
            "find",
            "findOne",
            "insertOne",
            "insertMany",
            "updateOne",
            "updateMany",
            "deleteOne",
            "deleteMany",
            "createIndex",
            "countDocuments",
            "distinct",
        ],
        _ => &[],
    }
}

pub fn base_type_terms() -> &'static [&'static str] {
    &[
        "BIGINT",
        "BLOB",
        "BOOLEAN",
        "CHAR",
        "DATE",
        "DATETIME",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INT",
        "INTEGER",
        "JSON",
        "NUMERIC",
        "REAL",
        "TEXT",
        "TIMESTAMP",
        "VARCHAR",
    ]
}

pub fn dialect_type_terms(language_profile: &str) -> &'static [&'static str] {
    match normalize_syntax_profile(language_profile) {
        "postgresql" => &[
            "BIGSERIAL",
            "BYTEA",
            "CITEXT",
            "CIDR",
            "INET",
            "INTERVAL",
            "JSONB",
            "MACADDR",
            "MACADDR8",
            "MONEY",
            "NAME",
            "SERIAL",
            "SMALLSERIAL",
            "TIMETZ",
            "TIMESTAMPTZ",
            "TSQUERY",
            "TSVECTOR",
            "UUID",
            "XML",
        ],
        "mysql" => &[
            "BIGINT",
            "BINARY",
            "DATETIME",
            "ENUM",
            "JSON",
            "MEDIUMBLOB",
            "MEDIUMINT",
            "LONGBLOB",
            "LONGTEXT",
            "MEDIUMTEXT",
            "TINYBLOB",
            "TINYTEXT",
            "TINYINT",
            "UNSIGNED",
            "VARBINARY",
            "ZEROFILL",
        ],
        "sqlite" => &["ANY", "BLOB", "INTEGER", "NUMERIC", "REAL", "TEXT"],
        "duckdb" => &[
            "BOOL",
            "HUGEINT",
            "LIST",
            "MAP",
            "STRUCT",
            "UUID",
            "UBIGINT",
            "UINTEGER",
            "UNION",
            "USMALLINT",
            "UTINYINT",
        ],
        "mssql" => &[
            "BIT",
            "DATETIME2",
            "DATETIMEOFFSET",
            "GEOGRAPHY",
            "GEOMETRY",
            "HIERARCHYID",
            "IMAGE",
            "MONEY",
            "NCHAR",
            "NTEXT",
            "NVARCHAR",
            "ROWVERSION",
            "SMALLDATETIME",
            "SMALLMONEY",
            "SQL_VARIANT",
            "SYSNAME",
            "UNIQUEIDENTIFIER",
            "VARBINARY",
            "XML",
        ],
        "clickhouse" => &[
            "AGGREGATEFUNCTION",
            "ARRAY",
            "DATE32",
            "DATETIME64",
            "DECIMAL128",
            "DECIMAL256",
            "DECIMAL32",
            "DECIMAL64",
            "ENUM16",
            "ENUM8",
            "FIXEDSTRING",
            "FLOAT32",
            "FLOAT64",
            "INT128",
            "INT16",
            "INT256",
            "INT32",
            "INT64",
            "INT8",
            "IPV4",
            "IPV6",
            "JSONEACHROW",
            "LOWCARDINALITY",
            "MAP",
            "MULTIPOLYGON",
            "NESTED",
            "NOTHING",
            "NULLABLE",
            "POINT",
            "POLYGON",
            "RING",
            "SIMPLEAGGREGATEFUNCTION",
            "STRING",
            "TUPLE",
            "UINT128",
            "UINT16",
            "UINT256",
            "UINT32",
            "UINT64",
            "UINT8",
            "UUID",
        ],
        "mongodb" => &[
            "DECIMAL128",
            "NUMBERLONG",
            "ObjectId",
            "ISODate",
            "Decimal128",
            "BinData",
        ],
        _ => &[],
    }
}

pub fn normalize_syntax_profile(language_profile: &str) -> &'static str {
    match language_profile.trim().to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => "postgresql",
        "mysql" | "mariadb" => "mysql",
        "sqlite" | "turso" => "sqlite",
        "duckdb" => "duckdb",
        "mssql" | "sqlserver" => "mssql",
        "clickhouse" => "clickhouse",
        "redis" => "redis",
        "mongo" | "mongodb" => "mongodb",
        _ => "sql",
    }
}

pub fn is_dialect_reserved_sql_symbol(info: &crate::DialectInfo, word: &str) -> bool {
    info.keywords
        .iter()
        .any(|keyword| keyword.keyword.eq_ignore_ascii_case(word))
        || info
            .functions
            .iter()
            .any(|function| function.name.eq_ignore_ascii_case(word))
        || info.data_types.iter().any(|data_type| {
            data_type.name.eq_ignore_ascii_case(word)
                || data_type
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(word))
        })
}

pub fn is_valid_dialect_sql_identifier(info: &crate::DialectInfo, name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !first.is_alphabetic() && first != '_' {
        return false;
    }

    if chars.any(|character| !character.is_alphanumeric() && character != '_') {
        return false;
    }

    !is_dialect_reserved_sql_symbol(info, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_terms_cover_driver_specific_examples() {
        let postgres = syntax_term_profile("postgresql");
        assert!(postgres.dialect_keywords.contains(&"PLPGSQL"));
        assert!(postgres.dialect_functions.contains(&"JSONB_SET"));
        assert!(postgres.dialect_types.contains(&"TSVECTOR"));

        assert!(
            syntax_term_profile("redis")
                .dialect_keywords
                .contains(&"HGETALL")
        );
        assert!(
            syntax_term_profile("sqlite")
                .dialect_functions
                .contains(&"JSON_OBJECT")
        );
        assert!(
            syntax_term_profile("duckdb")
                .dialect_functions
                .contains(&"LIST_CONTAINS")
        );
        assert!(
            syntax_term_profile("mssql")
                .dialect_types
                .contains(&"SYSNAME")
        );
        assert!(
            syntax_term_profile("clickhouse")
                .dialect_functions
                .contains(&"TODATETIME64")
        );
        assert!(
            syntax_term_profile("mongodb")
                .dialect_keywords
                .contains(&"$avg")
        );
        assert!(
            syntax_term_profile("mongodb")
                .dialect_functions
                .contains(&"findOne")
        );
        assert!(
            syntax_term_profile("mongodb")
                .dialect_types
                .contains(&"ObjectId")
        );
    }

    #[test]
    fn normalize_syntax_profile_trims_driver_ids() {
        assert_eq!(normalize_syntax_profile(" Redis "), "redis");
        assert_eq!(normalize_syntax_profile(" Mongo "), "mongodb");
        assert_eq!(normalize_syntax_profile(" PostgreSQL "), "postgresql");
    }

    #[test]
    fn aliases_share_syntax_terms() {
        assert_eq!(normalize_syntax_profile("postgres"), "postgresql");
        assert_eq!(normalize_syntax_profile("mariadb"), "mysql");
        assert_eq!(normalize_syntax_profile("sqlserver"), "mssql");
        assert_eq!(normalize_syntax_profile("mongo"), "mongodb");
    }

    #[test]
    fn keyword_context_helpers_cover_driver_lsp_contexts() {
        assert!(keyword_term_allowed_in_select_expression("TOP"));
        assert!(keyword_term_allowed_in_condition("BETWEEN"));
        assert!(keyword_term_allowed_in_join_condition("USING"));
        assert!(keyword_term_allowed_in_cte("SELECT"));
        assert!(keyword_term_allowed_in_from_clause("LATERAL"));
        assert!(!keyword_term_allowed_in_from_clause("WHERE"));
        assert!(keyword_allowed_in_select_expression(
            "TOP",
            crate::KeywordCategory::Other
        ));
        assert!(keyword_allowed_in_condition(
            "BETWEEN",
            crate::KeywordCategory::Operator
        ));
        assert!(keyword_allowed_in_join_condition(
            "USING",
            crate::KeywordCategory::Clause
        ));
        assert!(keyword_allowed_in_from_clause(
            "LATERAL",
            crate::KeywordCategory::Clause
        ));
        assert!(keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::SelectList,
            "TOP",
            crate::KeywordCategory::Other
        ));
        assert!(keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::ConditionClause,
            "BETWEEN",
            crate::KeywordCategory::Operator
        ));
        assert!(keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::FromClause,
            "LATERAL",
            crate::KeywordCategory::Clause
        ));
        assert!(keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::JoinClause,
            "USING",
            crate::KeywordCategory::Clause
        ));
        assert!(keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::CommonTableExpression,
            "SELECT",
            crate::KeywordCategory::Dql
        ));
        assert_eq!(
            syntax_operator_completions_for_context(SyntaxCompletionContextKind::ConditionClause)
                .first()
                .map(|operator| operator.insert_text),
            Some("= ")
        );
        assert!(
            syntax_operator_completions_for_context(SyntaxCompletionContextKind::SelectList)
                .is_empty()
        );
        assert!(!keyword_allowed_in_completion_context(
            SyntaxCompletionContextKind::AfterDot,
            "SELECT",
            crate::KeywordCategory::Dql
        ));
        assert_eq!(
            keyword_completion_sort_prefix_for_context(SyntaxCompletionContextKind::FromClause),
            "5_from_keyword"
        );
        assert_eq!(create_table_keyword_insert_text("CHECK"), "CHECK (");
        assert_eq!(create_table_keyword_insert_text("DEFAULT"), "DEFAULT ");
        assert_eq!(create_table_keyword_sort_text("NOT NULL"), "5_00_NOT NULL");
        assert_eq!(
            create_table_keyword_sort_text("PRIMARY KEY"),
            "5_01_PRIMARY KEY"
        );
        let varchar = crate::DataTypeInfo {
            name: "VARCHAR".into(),
            aliases: vec!["CHARACTER VARYING".into()],
            category: crate::DataTypeCategory::String,
            accepts_length: true,
            accepts_scale: false,
            default_length: Some(255),
            max_length: None,
            description: None,
            example: None,
        };
        assert_eq!(data_type_insert_text(&varchar), "VARCHAR(255)");
        assert_eq!(data_type_sort_text(&varchar), "4_String_VARCHAR");
        assert!(data_type_matches_suffix(
            &varchar,
            "CREATE TABLE users (name varchar"
        ));
        assert!(create_table_prefers_constraint_terms(
            "CREATE TABLE users (name VARCHAR ",
            std::slice::from_ref(&varchar)
        ));
        assert!(create_table_prefers_constraint_terms(
            "CREATE TABLE users (id INTEGER, ",
            std::slice::from_ref(&varchar)
        ));
        assert!(!create_table_prefers_constraint_terms(
            "CREATE TABLE users (name DEFAULT 'VARCHAR, ' ",
            std::slice::from_ref(&varchar)
        ));
        assert_eq!(aggregate_function_sort_text("count"), "3_00_COUNT");
        assert_eq!(scalar_function_sort_text("lower"), "4_01_LOWER");
        let length_function = crate::SqlFunctionInfo {
            name: "LENGTH".into(),
            category: crate::FunctionCategory::String,
            description: None,
            signatures: Vec::new(),
            return_type: Some("INTEGER".into()),
        };
        assert_eq!(
            dialect_function_sort_text(&length_function),
            "3_04_50_String_LENGTH"
        );
        assert_eq!(
            dialect_function_detail(&length_function),
            "String function -> INTEGER"
        );
        assert!(function_category_allowed_in_condition(
            crate::FunctionCategory::String
        ));
        assert!(!function_category_allowed_in_condition(
            crate::FunctionCategory::Aggregate
        ));
        let dialect = GenericDialect {};
        assert_eq!(
            sql_function_call_context("SELECT CONCAT('last, first', ", &dialect),
            Some(("CONCAT".to_string(), 1))
        );
        assert_eq!(
            sql_function_call_context("SELECT CONCAT(LOWER(name), ", &dialect),
            Some(("CONCAT".to_string(), 1))
        );
        assert!(keyword_relevant_for_context(
            "SELECT",
            crate::KeywordCategory::Dql,
            true,
            &[]
        ));
        assert!(keyword_term_is_context_clause("WHERE"));
        assert!(keyword_term_implies_condition_clause("ON"));
        assert_eq!(
            active_sql_unquoted_word_tokens(r#"SELECT "where" FROM users WHERE id"#),
            vec!["select", "from", "users", "where", "id"]
        );
        assert_eq!(
            significant_sql_word_lookback(r#"SELECT "from" FROM users"#),
            vec![
                SignificantSqlToken::Word("SELECT".to_string()),
                SignificantSqlToken::Other,
                SignificantSqlToken::Word("FROM".to_string()),
                SignificantSqlToken::Word("USERS".to_string()),
            ]
        );
        assert_eq!(
            significant_sql_word_lookback("SELECT ( FROM users ) WHERE"),
            vec![
                SignificantSqlToken::Word("SELECT".to_string()),
                SignificantSqlToken::Word("FROM".to_string()),
                SignificantSqlToken::Word("USERS".to_string()),
                SignificantSqlToken::Word("WHERE".to_string()),
            ]
        );
        assert_eq!(
            incomplete_sql_completion_context("SELECT "),
            Some(IncompleteSqlCompletionContext::SelectList)
        );
        assert_eq!(
            incomplete_sql_completion_context("SELECT * FROM us"),
            Some(IncompleteSqlCompletionContext::FromClause)
        );
        assert_eq!(
            incomplete_sql_completion_context("SELECT * FROM users JOIN au"),
            Some(IncompleteSqlCompletionContext::JoinClause)
        );
        assert_eq!(incomplete_sql_completion_context("-- FROM users\n"), None);
        assert_eq!(
            incomplete_sql_completion_context(r#"SELECT "FROM" "#),
            Some(IncompleteSqlCompletionContext::SelectList)
        );
        assert!(sql_has_query_context_tokens("SELECT id FROM users"));
        assert!(sql_has_query_context_tokens(
            "UPDATE users SET name = 'Ada'"
        ));
        assert!(!sql_has_query_context_tokens("'SELECT'"));
        assert!(!sql_has_query_context_tokens("-- SELECT\n"));
        assert!(!sql_has_query_context_tokens(r#""select""#));
        assert_eq!(
            select_wildcard_token_ranges(
                "SELECT *, count(*) FROM users WHERE total * tax > 0",
                &dialect
            ),
            vec!["SELECT ".len().."SELECT *".len()]
        );
        assert_eq!(
            keyword_token_byte_ranges("SELECT 1; DROP TABLE users", &dialect, "drop"),
            vec!["SELECT 1; ".len().."SELECT 1; DROP".len()]
        );
        assert!(keyword_token_byte_ranges("'DROP'", &dialect, "drop").is_empty());
        assert!(is_inside_active_create_table_column_list(
            "CREATE TABLE users (id INTEGER"
        ));
        assert!(is_inside_active_create_table_column_list(
            "CREATE TEMP TABLE users (CHECK (id > 0)"
        ));
        assert!(!is_inside_active_create_table_column_list(
            r#"SELECT 'CREATE TABLE users ('"#
        ));
        assert!(!is_inside_active_create_table_column_list(
            "CREATE TABLE users (id INTEGER)"
        ));
        assert_eq!(
            semicolon_insertion_offset("SELECT * FROM users", &dialect),
            Some("SELECT * FROM users".len())
        );
        assert_eq!(
            semicolon_insertion_offset("SELECT * FROM users;", &dialect),
            None
        );
        assert_eq!(
            semicolon_insertion_offset("SELECT * FROM users -- trailing", &dialect),
            Some("SELECT * FROM users".len())
        );
        assert_eq!(
            unclosed_quote_insertion("SELECT 'abc", 0, "SELECT ".len() as u32, &dialect),
            Some((0, "SELECT 'abc".len() as u32, '\''))
        );
        assert_eq!(
            sqlparser_location_to_offset("SELECT\n  id", 2, 3),
            Some("SELECT\n  ".len())
        );
        assert_eq!(line_column_to_offset("SELECT\n  id", 1, 2), Some(9));
        assert_eq!(line_column_for_offset("SELECT\n  id", 9), (1, 2));
        assert!(sql_completion_trigger_context("SELECT ", "SELECT ".len(), "", false).should_show);
        assert!(sql_completion_trigger_context("SEL", "SEL".len(), "SEL", false).should_show);
        assert!(!sql_completion_trigger_context("SELECT", "SELECT".len(), "", false).should_show);
        assert!(
            !sql_completion_trigger_context("SELECT 'FROM ", "SELECT 'FROM ".len(), "", false)
                .should_show
        );
        assert!(
            !sql_completion_trigger_context("SELECT -- FROM ", "SELECT -- FROM ".len(), "", false)
                .should_show
        );
        assert!(
            sql_completion_trigger_context("SELECT 'FROM ", "SELECT 'FROM ".len(), "", true)
                .should_show
        );
        assert_eq!(
            syntax_completion_prefix("db.users.$ma", "db.users.$ma".len(), &['$']),
            "$ma"
        );
        assert_eq!(
            syntax_completion_prefix("GET user:1", "GET user:1".len(), &[':']),
            "user:1"
        );
        let unicode = "SELECT café";
        let offset_inside_character = unicode.len() - 1;
        assert_eq!(
            syntax_completion_prefix(unicode, offset_inside_character, &[]),
            "caf"
        );
        assert!(keyword_context_is_start(""));
        assert!(!keyword_context_is_start("SELECT id FROM users"));
        assert!(keyword_context_implies_condition_clause(
            "SELECT id FROM users WHERE "
        ));
        assert!(!keyword_context_implies_condition_clause(
            "SELECT id FROM users ORDER BY "
        ));
        assert_eq!(
            keyword_context_categories("SELECT id FROM users WHERE "),
            vec![
                crate::KeywordCategory::Operator,
                crate::KeywordCategory::Dql
            ]
        );
        assert_eq!(
            keyword_context_categories_for_clause(Some("where")),
            &[
                crate::KeywordCategory::Operator,
                crate::KeywordCategory::Dql
            ]
        );
        assert!(
            keyword_category_rank(crate::KeywordCategory::Dql)
                < keyword_category_rank(crate::KeywordCategory::Operator)
        );
        assert!(
            syntax_completion_context_bucket(
                SyntaxCompletionContextKind::AfterDot,
                SyntaxCompletionItemKind::Field,
                "id",
                false
            ) > syntax_completion_context_bucket(
                SyntaxCompletionContextKind::AfterDot,
                SyntaxCompletionItemKind::Keyword,
                "SELECT",
                false
            )
        );
        assert!(
            syntax_completion_context_bucket(
                SyntaxCompletionContextKind::ConditionClause,
                SyntaxCompletionItemKind::Keyword,
                "BETWEEN",
                false
            ) > syntax_completion_context_bucket(
                SyntaxCompletionContextKind::ConditionClause,
                SyntaxCompletionItemKind::Keyword,
                "FROM",
                false
            )
        );
        assert!(
            syntax_completion_context_bucket(
                SyntaxCompletionContextKind::CreateTable,
                SyntaxCompletionItemKind::Keyword,
                "NOT NULL",
                true
            ) > syntax_completion_context_bucket(
                SyntaxCompletionContextKind::CreateTable,
                SyntaxCompletionItemKind::Struct,
                "TEXT",
                true
            )
        );
        let primary_key = crate::KeywordInfo::new("PRIMARY KEY", crate::KeywordCategory::Ddl);
        assert_eq!(
            keyword_suffix_for_context(&primary_key, &["primary".to_string()], "k"),
            Some("KEY".to_string())
        );
        assert_eq!(
            keyword_suffix_for_context(&primary_key, &["select".to_string()], "k"),
            None
        );
        assert_eq!(
            syntax_completion_dedup_key(SyntaxCompletionItemKind::Field, "Users", None),
            "FIELD:users"
        );
        assert_eq!(
            syntax_completion_dedup_key(
                SyntaxCompletionItemKind::Keyword,
                "COUNT",
                Some("count(*)")
            ),
            "KEYWORD:count(*)"
        );
        assert_eq!(
            compare_syntax_completion_sort_texts(Some("2_SELECT"), "SELECT", None, "FROM"),
            "FROM".cmp("2_SELECT")
        );
    }

    #[test]
    fn dialect_identifier_validation_uses_driver_metadata() {
        let mut info = crate::DialectInfo::default();
        info.keywords.push(crate::KeywordInfo::new(
            "VACUUM",
            crate::KeywordCategory::Ddl,
        ));
        info.functions.push(crate::SqlFunctionInfo {
            name: "JSONB_SET".into(),
            category: crate::FunctionCategory::Json,
            description: None,
            signatures: Vec::new(),
            return_type: None,
        });
        info.data_types.push(crate::DataTypeInfo {
            name: "VARCHAR".into(),
            aliases: vec!["CHARACTER VARYING".into()],
            category: crate::DataTypeCategory::String,
            accepts_length: true,
            accepts_scale: false,
            default_length: Some(255),
            max_length: None,
            description: None,
            example: None,
        });

        assert!(is_valid_dialect_sql_identifier(&info, "account_id"));
        assert!(is_valid_dialect_sql_identifier(&info, "_account_id"));
        assert!(!is_valid_dialect_sql_identifier(&info, "1account"));
        assert!(!is_valid_dialect_sql_identifier(&info, "account-id"));
        assert!(!is_valid_dialect_sql_identifier(&info, "vacuum"));
        assert!(!is_valid_dialect_sql_identifier(&info, "jsonb_set"));
        assert!(!is_valid_dialect_sql_identifier(&info, "character varying"));
        assert!(is_dialect_reserved_sql_symbol(&info, "VARCHAR"));
    }

    #[test]
    fn supported_profiles_are_normalized_and_unique() {
        let profiles = supported_syntax_profiles();
        for profile in profiles {
            assert_eq!(normalize_syntax_profile(profile), *profile);
        }

        let mut sorted = profiles.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), profiles.len());
    }
}
