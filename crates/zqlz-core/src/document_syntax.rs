use crate::{
    SqlDocumentSymbol, SqlProtectedRange, SqlProtectedRangeKind, SyntaxDocumentSymbolMode,
    SyntaxDriverCapabilities, SyntaxOverlayMode,
    command::{CommandTokenizer, Token as CommandToken, parse_commands},
    dialect_syntax::{normalize_syntax_profile, syntax_term_profile},
    dialects::{uses_command_syntax, uses_document_syntax},
    sql_document_symbols, sql_protected_range_at,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocumentSyntaxToken<'a> {
    pub start: usize,
    pub end: usize,
    pub text: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverSyntaxOverlayKind {
    Keyword,
    Function,
    Type,
    Operator,
    Comment,
    String,
    Number,
    Identifier,
    Punctuation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverSyntaxOverlayToken {
    pub start: usize,
    pub end: usize,
    pub kind: DriverSyntaxOverlayKind,
}

pub fn driver_syntax_overlay_tokens(
    language_profile: &str,
    text: &str,
) -> Vec<DriverSyntaxOverlayToken> {
    let normalized_profile = normalize_syntax_profile(language_profile);
    if uses_document_syntax(normalized_profile) {
        return document_syntax_overlay_tokens(normalized_profile, text);
    }

    if uses_command_syntax(normalized_profile) {
        return command_syntax_overlay_tokens(normalized_profile, text);
    }

    Vec::new()
}

pub fn driver_syntax_overlay_tokens_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
    text: &str,
) -> Vec<DriverSyntaxOverlayToken> {
    match capabilities.overlays {
        SyntaxOverlayMode::Document => document_syntax_overlay_tokens(capabilities.profile, text),
        SyntaxOverlayMode::Command => command_syntax_overlay_tokens(capabilities.profile, text),
        SyntaxOverlayMode::Sql | SyntaxOverlayMode::None => Vec::new(),
    }
}

pub fn driver_syntax_protected_ranges(
    language_profile: &str,
    text: &str,
) -> Vec<SqlProtectedRange> {
    let normalized_profile = normalize_syntax_profile(language_profile);
    if uses_document_syntax(normalized_profile) {
        return document_syntax_protected_ranges(text);
    }

    if uses_command_syntax(normalized_profile) {
        return command_syntax_protected_ranges(text);
    }

    Vec::new()
}

pub fn driver_syntax_protected_ranges_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
    text: &str,
) -> Vec<SqlProtectedRange> {
    match capabilities.overlays {
        SyntaxOverlayMode::Document => document_syntax_protected_ranges(text),
        SyntaxOverlayMode::Command => command_syntax_protected_ranges(text),
        SyntaxOverlayMode::Sql | SyntaxOverlayMode::None => Vec::new(),
    }
}

pub fn driver_document_symbols(language_profile: &str, text: &str) -> Vec<SqlDocumentSymbol> {
    let normalized_profile = normalize_syntax_profile(language_profile);
    if uses_command_syntax(normalized_profile) {
        return command_document_symbols(text);
    }

    if uses_document_syntax(normalized_profile) {
        return mongodb_document_symbols(text);
    }

    sql_document_symbols(text)
}

pub fn driver_document_symbols_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
    text: &str,
) -> Vec<SqlDocumentSymbol> {
    match capabilities.document_symbols {
        SyntaxDocumentSymbolMode::SqlStatements => sql_document_symbols(text),
        SyntaxDocumentSymbolMode::CommandCommands => command_document_symbols(text),
        SyntaxDocumentSymbolMode::MongodbCollections => mongodb_document_symbols(text),
        SyntaxDocumentSymbolMode::None => Vec::new(),
    }
}

pub fn mongodb_syntax_tokens(text: &str) -> Vec<DocumentSyntaxToken<'_>> {
    let mut tokens = Vec::new();
    let mut token_start = None;

    for (index, character) in text.char_indices() {
        if is_mongodb_token_character(character) {
            token_start.get_or_insert(index);
            continue;
        }

        if let Some(start) = token_start.take() {
            push_mongodb_syntax_token(text, start, index, &mut tokens);
        }
    }

    if let Some(start) = token_start {
        push_mongodb_syntax_token(text, start, text.len(), &mut tokens);
    }

    tokens
}

fn command_document_symbols(text: &str) -> Vec<SqlDocumentSymbol> {
    let mut tokenizer = CommandTokenizer::new(text, false);
    parse_commands(&tokenizer.tokenize())
        .into_iter()
        .map(|command| {
            let source_end = command
                .arg_tokens
                .last()
                .map(|token| token.end)
                .unwrap_or(command.command_token.end);
            let target_range = command
                .arg_tokens
                .first()
                .map(|token| token.start..token.end)
                .unwrap_or(command.command_token.start..command.command_token.end);
            let mut label = command.command;
            if let Some(first_arg) = command.args.first().filter(|arg| !arg.is_empty()) {
                label.push(' ');
                label.push_str(first_arg);
            }

            SqlDocumentSymbol {
                label,
                line: command.command_token.line,
                column: command.command_token.column,
                source_range: command.command_token.start..source_end,
                target_range: Some(target_range),
            }
        })
        .collect()
}

fn mongodb_document_symbols(text: &str) -> Vec<SqlDocumentSymbol> {
    let protected_ranges = document_syntax_protected_ranges(text);
    text.lines()
        .scan((0usize, 0usize), |state, line| {
            let start = state.0;
            let line_number = state.1;
            state.0 += line.len() + 1;
            state.1 += 1;
            Some((start, line_number, line))
        })
        .filter_map(|(line_start, line_number, line)| {
            mongodb_document_symbol_for_line(&protected_ranges, line_start, line_number, line)
        })
        .collect()
}

fn mongodb_document_symbol_for_line(
    protected_ranges: &[SqlProtectedRange],
    line_start: usize,
    line_number: usize,
    line: &str,
) -> Option<SqlDocumentSymbol> {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with("//") {
        return None;
    }

    let db_start_in_line = line.find("db.")?;
    let db_start = line_start + db_start_in_line;
    let mut range_index = 0;
    if sql_protected_range_at(db_start, protected_ranges, &mut range_index).is_some() {
        return None;
    }

    let collection_start = db_start_in_line + "db.".len();
    let collection_end = line[collection_start..]
        .find('.')
        .map(|index| collection_start + index)?;
    let method_start = collection_end + 1;
    let method_end = line[method_start..]
        .find(|character: char| !is_mongodb_token_character(character))
        .map(|index| method_start + index)
        .unwrap_or(line.len());
    if method_end == method_start {
        return None;
    }

    let collection = &line[collection_start..collection_end];
    let method = &line[method_start..method_end];
    if collection.is_empty() || method.is_empty() {
        return None;
    }

    let source_end = line_start + line.trim_end().len();
    Some(SqlDocumentSymbol {
        label: format!("{method} {collection}"),
        line: line_number,
        column: db_start_in_line,
        source_range: db_start..source_end,
        target_range: Some(line_start + collection_start..line_start + collection_end),
    })
}

fn document_syntax_protected_ranges(text: &str) -> Vec<SqlProtectedRange> {
    let bytes = text.as_bytes();
    let mut ranges = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                let start = index;
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                        continue;
                    }
                    let current = bytes[index];
                    index += 1;
                    if current == quote {
                        break;
                    }
                }
                ranges.push(SqlProtectedRange {
                    start,
                    end: index,
                    kind: SqlProtectedRangeKind::StringLiteral,
                });
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let start = index;
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                ranges.push(SqlProtectedRange {
                    start,
                    end: index,
                    kind: SqlProtectedRangeKind::Comment,
                });
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let start = index;
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                ranges.push(SqlProtectedRange {
                    start,
                    end: index,
                    kind: SqlProtectedRangeKind::Comment,
                });
            }
            _ => {
                let Some(character) = text[index..].chars().next() else {
                    break;
                };
                index += character.len_utf8();
            }
        }
    }

    ranges
}

fn command_syntax_protected_ranges(text: &str) -> Vec<SqlProtectedRange> {
    let mut tokenizer = CommandTokenizer::new(text, false);
    tokenizer
        .tokenize()
        .into_iter()
        .filter_map(|token| {
            let kind = match &token.token {
                CommandToken::Comment(_) => SqlProtectedRangeKind::Comment,
                CommandToken::Argument(_) if command_argument_is_quoted(text, token.start) => {
                    SqlProtectedRangeKind::StringLiteral
                }
                _ => return None,
            };
            Some(SqlProtectedRange {
                start: token.start,
                end: token.end,
                kind,
            })
        })
        .collect()
}

fn document_syntax_overlay_tokens(
    language_profile: &str,
    text: &str,
) -> Vec<DriverSyntaxOverlayToken> {
    let profile = syntax_term_profile(language_profile);
    let protected_ranges = document_syntax_protected_ranges(text);
    let mut tokens = mongodb_punctuation_tokens(text, &protected_ranges);

    let protected_token_ranges = protected_ranges
        .iter()
        .filter_map(|range| match range.kind {
            SqlProtectedRangeKind::Comment => {
                tokens.push(DriverSyntaxOverlayToken {
                    start: range.start,
                    end: range.end,
                    kind: DriverSyntaxOverlayKind::Comment,
                });
                Some(*range)
            }
            SqlProtectedRangeKind::StringLiteral
                if !document_string_is_object_key(text, range)
                    && !document_string_is_mongodb_operator_value(text, range) =>
            {
                tokens.push(DriverSyntaxOverlayToken {
                    start: range.start,
                    end: range.end,
                    kind: DriverSyntaxOverlayKind::String,
                });
                Some(*range)
            }
            SqlProtectedRangeKind::DollarQuotedString => {
                tokens.push(DriverSyntaxOverlayToken {
                    start: range.start,
                    end: range.end,
                    kind: DriverSyntaxOverlayKind::String,
                });
                Some(*range)
            }
            SqlProtectedRangeKind::StringLiteral => None,
            SqlProtectedRangeKind::QuotedIdentifier => None,
        })
        .collect::<Vec<_>>();

    for token in mongodb_syntax_tokens(text) {
        if syntax_token_is_protected(token.start, &protected_token_ranges) {
            continue;
        }
        if mongodb_token_should_defer_to_parser(token.text) {
            continue;
        }
        let kind = if token.text.starts_with('$') {
            Some(DriverSyntaxOverlayKind::Operator)
        } else if syntax_terms_contain(profile.dialect_functions, token.text)
            || syntax_terms_contain(profile.base_functions, token.text)
        {
            Some(DriverSyntaxOverlayKind::Function)
        } else if syntax_terms_contain(profile.dialect_keywords, token.text)
            || syntax_terms_contain(profile.base_keywords, token.text)
        {
            Some(DriverSyntaxOverlayKind::Keyword)
        } else if syntax_terms_contain(profile.dialect_types, token.text)
            || syntax_terms_contain(profile.base_types, token.text)
        {
            Some(DriverSyntaxOverlayKind::Type)
        } else {
            None
        };

        if let Some(kind) = kind {
            tokens.push(DriverSyntaxOverlayToken {
                start: token.start,
                end: token.end,
                kind,
            });
        }
    }

    tokens.sort_by_key(|token| (token.start, token.end));
    tokens
}

fn mongodb_token_should_defer_to_parser(token: &str) -> bool {
    matches!(token, "true" | "false" | "null" | "undefined")
}

fn command_syntax_overlay_tokens(
    language_profile: &str,
    text: &str,
) -> Vec<DriverSyntaxOverlayToken> {
    let profile = syntax_term_profile(language_profile);
    let mut tokenizer = CommandTokenizer::new(text, false);
    let mut tokens = Vec::new();

    for token in tokenizer.tokenize() {
        let kind = match &token.token {
            CommandToken::Command(command) => {
                let command = command.to_ascii_uppercase();
                if profile.dialect_keywords.contains(&command.as_str())
                    || profile.base_keywords.contains(&command.as_str())
                {
                    Some(DriverSyntaxOverlayKind::Keyword)
                } else {
                    None
                }
            }
            CommandToken::Comment(_) => Some(DriverSyntaxOverlayKind::Comment),
            CommandToken::Argument(argument) => {
                command_argument_overlay_kind(&profile, text, token.start, argument)
            }
            _ => None,
        };

        if let Some(kind) = kind {
            tokens.push(DriverSyntaxOverlayToken {
                start: token.start,
                end: token.end,
                kind,
            });
        }
    }

    tokens
}

fn command_argument_is_quoted(text: &str, start: usize) -> bool {
    text[start..]
        .chars()
        .next()
        .is_some_and(|character| character == '\'' || character == '"')
}

fn command_argument_is_number(argument: &str) -> bool {
    argument.parse::<i64>().is_ok() || argument.parse::<f64>().is_ok()
}

fn command_argument_overlay_kind(
    profile: &crate::dialect_syntax::SyntaxTermProfile,
    text: &str,
    start: usize,
    argument: &str,
) -> Option<DriverSyntaxOverlayKind> {
    if command_argument_is_quoted(text, start) {
        Some(DriverSyntaxOverlayKind::String)
    } else if syntax_terms_contain(profile.dialect_keywords, argument)
        || syntax_terms_contain(profile.base_keywords, argument)
    {
        Some(DriverSyntaxOverlayKind::Keyword)
    } else if syntax_terms_contain(profile.dialect_functions, argument)
        || syntax_terms_contain(profile.base_functions, argument)
    {
        Some(DriverSyntaxOverlayKind::Function)
    } else if syntax_terms_contain(profile.dialect_types, argument)
        || syntax_terms_contain(profile.base_types, argument)
    {
        Some(DriverSyntaxOverlayKind::Type)
    } else if command_argument_is_number(argument) {
        Some(DriverSyntaxOverlayKind::Number)
    } else if !argument.is_empty() {
        Some(DriverSyntaxOverlayKind::Identifier)
    } else {
        None
    }
}

fn syntax_terms_contain(terms: &[&str], token: &str) -> bool {
    terms.iter().any(|term| term.eq_ignore_ascii_case(token))
}

fn mongodb_punctuation_tokens(
    text: &str,
    protected_ranges: &[SqlProtectedRange],
) -> Vec<DriverSyntaxOverlayToken> {
    let mut protected_range_index = 0usize;
    text.char_indices()
        .filter(|(index, character)| {
            if sql_protected_range_at(*index, protected_ranges, &mut protected_range_index)
                .is_some()
            {
                return false;
            }
            matches!(
                character,
                ':' | ',' | '.' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        })
        .map(|(start, character)| DriverSyntaxOverlayToken {
            start,
            end: start + character.len_utf8(),
            kind: DriverSyntaxOverlayKind::Punctuation,
        })
        .collect()
}

fn document_string_is_object_key(text: &str, range: &SqlProtectedRange) -> bool {
    text.get(range.end..).is_some_and(|suffix| {
        suffix.chars().find(|character| !character.is_whitespace()) == Some(':')
    })
}

fn document_string_is_mongodb_operator_value(text: &str, range: &SqlProtectedRange) -> bool {
    let Some(value) = text.get(range.start..range.end) else {
        return false;
    };
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .is_some_and(|value| value.starts_with('$') && value.len() > 1)
}

fn syntax_token_is_protected(start: usize, protected_ranges: &[SqlProtectedRange]) -> bool {
    let mut protected_range_index = 0usize;
    sql_protected_range_at(start, protected_ranges, &mut protected_range_index).is_some()
}

fn is_mongodb_token_character(character: char) -> bool {
    character == '$' || character == '_' || character.is_ascii_alphanumeric()
}

fn push_mongodb_syntax_token<'a>(
    text: &'a str,
    start: usize,
    end: usize,
    tokens: &mut Vec<DocumentSyntaxToken<'a>>,
) {
    let token = &text[start..end];
    if token == "$" || token.chars().all(|character| character.is_ascii_digit()) {
        return;
    }

    tokens.push(DocumentSyntaxToken {
        start,
        end,
        text: token,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mongodb_syntax_tokens_find_operators_and_identifiers() {
        let text = r#"db.orders.find({ status: { $in: ["paid"] }, qty: { $gte: 3 } })"#;
        let tokens = mongodb_syntax_tokens(text);
        let token_texts = tokens.iter().map(|token| token.text).collect::<Vec<_>>();

        assert!(token_texts.contains(&"find"));
        assert!(token_texts.contains(&"$in"));
        assert!(token_texts.contains(&"$gte"));
        assert!(token_texts.contains(&"status"));
        assert!(!token_texts.contains(&"3"));
    }

    #[test]
    fn mongodb_syntax_tokens_ignore_bare_dollar_and_numbers() {
        let tokens = mongodb_syntax_tokens("$ 123 $set value_1");
        let token_texts = tokens.iter().map(|token| token.text).collect::<Vec<_>>();

        assert_eq!(token_texts, vec!["$set", "value_1"]);
    }

    #[test]
    fn driver_syntax_overlay_tokens_classify_mongodb_terms() {
        let text = r#"db.orders.find({ _id: ObjectId("abc"), qty: { $gte: 3 } })"#;
        let tokens = driver_syntax_overlay_tokens("mongodb", text);
        let assignments = tokens
            .iter()
            .map(|token| (&text[token.start..token.end], token.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&("$gte", DriverSyntaxOverlayKind::Operator)));
        assert!(assignments.contains(&("ObjectId", DriverSyntaxOverlayKind::Type)));
        assert!(assignments.contains(&("find", DriverSyntaxOverlayKind::Function)));
        assert!(assignments.contains(&(":", DriverSyntaxOverlayKind::Punctuation)));
        assert!(assignments.contains(&("{", DriverSyntaxOverlayKind::Punctuation)));
    }

    #[test]
    fn driver_syntax_overlay_tokens_match_driver_terms_case_insensitively() {
        let text = r#"db.orders.FIND({ _id: objectid("abc"), createdAt: ISODate("2024-01-01") })"#;
        let tokens = driver_syntax_overlay_tokens("mongodb", text);
        let assignments = tokens
            .iter()
            .map(|token| (&text[token.start..token.end], token.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&("FIND", DriverSyntaxOverlayKind::Function)));
        assert!(assignments.contains(&("objectid", DriverSyntaxOverlayKind::Type)));
        assert!(assignments.contains(&("ISODate", DriverSyntaxOverlayKind::Type)));
    }

    #[test]
    fn driver_syntax_overlay_tokens_protect_mongodb_string_values_and_comments() {
        let text =
            r#"db.logs.find({ "$match": "aggregate should stay string" }) // createIndex ignored"#;
        let tokens = driver_syntax_overlay_tokens("mongodb", text);
        let assignments = tokens
            .iter()
            .map(|token| (&text[token.start..token.end], token.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&("$match", DriverSyntaxOverlayKind::Operator)));
        assert!(assignments.contains(&(
            r#""aggregate should stay string""#,
            DriverSyntaxOverlayKind::String
        )));
        assert!(
            assignments.contains(&("// createIndex ignored", DriverSyntaxOverlayKind::Comment))
        );
        assert!(!assignments.contains(&("aggregate", DriverSyntaxOverlayKind::Function)));
        assert!(!assignments.contains(&("createIndex", DriverSyntaxOverlayKind::Function)));
    }

    #[test]
    fn driver_syntax_overlay_tokens_classify_redis_commands() {
        let text = "JSON.SET user:1 $ \"value\"\n# comment\nGET user:1";
        let tokens = driver_syntax_overlay_tokens("redis", text);
        let assignments = tokens
            .iter()
            .map(|token| (&text[token.start..token.end], token.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&("JSON.SET", DriverSyntaxOverlayKind::Keyword)));
        assert!(assignments.contains(&("user:1", DriverSyntaxOverlayKind::Identifier)));
        assert!(assignments.contains(&("$", DriverSyntaxOverlayKind::Identifier)));
        assert!(assignments.contains(&("\"value\"", DriverSyntaxOverlayKind::String)));
        assert!(assignments.contains(&("# comment", DriverSyntaxOverlayKind::Comment)));
        assert!(assignments.contains(&("GET", DriverSyntaxOverlayKind::Keyword)));

        let option_tokens =
            driver_syntax_overlay_tokens("redis", "ZRANGE leaderboard 0 -1 WITHSCORES");
        let option_assignments = option_tokens
            .iter()
            .map(|token| {
                (
                    &"ZRANGE leaderboard 0 -1 WITHSCORES"[token.start..token.end],
                    token.kind,
                )
            })
            .collect::<Vec<_>>();
        assert!(option_assignments.contains(&("WITHSCORES", DriverSyntaxOverlayKind::Keyword)));
    }

    #[test]
    fn command_argument_overlay_kind_uses_driver_function_and_type_terms() {
        let profile = crate::dialect_syntax::SyntaxTermProfile {
            base_keywords: &[],
            dialect_keywords: &["OPTION"],
            base_functions: &[],
            dialect_functions: &["json.extract"],
            base_types: &[],
            dialect_types: &["ObjectId"],
        };

        assert_eq!(
            command_argument_overlay_kind(&profile, "option", 0, "option"),
            Some(DriverSyntaxOverlayKind::Keyword)
        );
        assert_eq!(
            command_argument_overlay_kind(&profile, "JSON.EXTRACT", 0, "JSON.EXTRACT"),
            Some(DriverSyntaxOverlayKind::Function)
        );
        assert_eq!(
            command_argument_overlay_kind(&profile, "objectid", 0, "objectid"),
            Some(DriverSyntaxOverlayKind::Type)
        );
    }

    #[test]
    fn driver_syntax_overlay_tokens_return_empty_for_sql_profiles() {
        assert!(driver_syntax_overlay_tokens("postgresql", "select 1").is_empty());
    }

    #[test]
    fn driver_syntax_overlay_tokens_use_capabilities_for_non_sql_profiles() {
        let redis = crate::get_syntax_driver_capabilities("redis");
        let redis_text = "ZRANGE leaderboard 0 -1";
        let redis_tokens = driver_syntax_overlay_tokens_for_capabilities(&redis, redis_text);
        assert!(
            redis_tokens
                .iter()
                .any(|token| &redis_text[token.start..token.end] == "ZRANGE")
        );

        let mongodb = crate::get_syntax_driver_capabilities("mongodb");
        let mongo_text = "db.orders.find({ total: { $gte: 100 } })";
        let mongo_tokens = driver_syntax_overlay_tokens_for_capabilities(&mongodb, mongo_text);
        assert!(
            mongo_tokens
                .iter()
                .any(|token| &mongo_text[token.start..token.end] == "find")
        );
        assert!(
            mongo_tokens
                .iter()
                .any(|token| &mongo_text[token.start..token.end] == "$gte")
        );
    }

    #[test]
    fn driver_syntax_protected_ranges_find_mongodb_strings_and_comments() {
        let text = r#"db.orders.find({ label: "ignore )", qty: 1 }) // ) ignored"#;
        let ranges = driver_syntax_protected_ranges("mongodb", text);
        let assignments = ranges
            .iter()
            .map(|range| (&text[range.start..range.end], range.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&(r#""ignore )""#, SqlProtectedRangeKind::StringLiteral)));
        assert!(assignments.contains(&("// ) ignored", SqlProtectedRangeKind::Comment)));
    }

    #[test]
    fn driver_syntax_protected_ranges_find_redis_quoted_arguments_and_comments() {
        let text = "JSON.SET key $ \"[ignored]\"\n# ] ignored";
        let ranges = driver_syntax_protected_ranges("redis", text);
        let assignments = ranges
            .iter()
            .map(|range| (&text[range.start..range.end], range.kind))
            .collect::<Vec<_>>();

        assert!(assignments.contains(&("\"[ignored]\"", SqlProtectedRangeKind::StringLiteral)));
        assert!(assignments.contains(&("# ] ignored", SqlProtectedRangeKind::Comment)));
    }

    #[test]
    fn driver_syntax_protected_ranges_use_capabilities() {
        let redis_text = "JSON.SET key $ \"[ignored]\"\n# ] ignored";
        let redis = crate::get_syntax_driver_capabilities("redis");
        let redis_ranges = driver_syntax_protected_ranges_for_capabilities(&redis, redis_text);
        let redis_assignments = redis_ranges
            .iter()
            .map(|range| (&redis_text[range.start..range.end], range.kind))
            .collect::<Vec<_>>();
        assert!(
            redis_assignments.contains(&("\"[ignored]\"", SqlProtectedRangeKind::StringLiteral))
        );

        let mongo_text = r#"db.orders.find({ label: "ignore )" }) // ) ignored"#;
        let mongodb = crate::get_syntax_driver_capabilities("mongodb");
        let mongo_ranges = driver_syntax_protected_ranges_for_capabilities(&mongodb, mongo_text);
        let mongo_assignments = mongo_ranges
            .iter()
            .map(|range| (&mongo_text[range.start..range.end], range.kind))
            .collect::<Vec<_>>();
        assert!(
            mongo_assignments.contains(&(r#""ignore )""#, SqlProtectedRangeKind::StringLiteral))
        );
        assert!(mongo_assignments.contains(&("// ) ignored", SqlProtectedRangeKind::Comment)));
    }

    #[test]
    fn driver_document_symbols_use_command_syntax_for_redis() {
        let text = "# ignored\nSET session:1 value\nGET session:1";
        let symbols = driver_document_symbols("redis", text);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "SET session:1");
        assert_eq!(symbols[0].line, 1);
        assert_eq!(symbols[0].column, 0);
        assert_eq!(&text[symbols[0].target_range.clone().unwrap()], "session:1");
        assert_eq!(symbols[1].label, "GET session:1");
    }

    #[test]
    fn driver_document_symbols_use_capabilities_for_redis() {
        let text = "# ignored\nSET session:1 value\nGET session:1";
        let capabilities = crate::get_syntax_driver_capabilities("redis");
        let symbols = driver_document_symbols_for_capabilities(&capabilities, text);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "SET session:1");
        assert_eq!(&text[symbols[0].target_range.clone().unwrap()], "session:1");
    }

    #[test]
    fn driver_document_symbols_use_document_syntax_for_mongodb() {
        let text =
            "// ignored\ndb.orders.find({ status: \"open\" })\ndb.users.updateOne({ _id: 1 }, {})";
        let symbols = driver_document_symbols("mongodb", text);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "find orders");
        assert_eq!(symbols[0].line, 1);
        assert_eq!(symbols[0].column, 0);
        assert_eq!(&text[symbols[0].target_range.clone().unwrap()], "orders");
        assert_eq!(symbols[1].label, "updateOne users");
    }

    #[test]
    fn driver_document_symbols_use_capabilities_for_mongodb() {
        let text =
            "// ignored\ndb.orders.find({ status: \"open\" })\ndb.users.updateOne({ _id: 1 }, {})";
        let capabilities = crate::get_syntax_driver_capabilities("mongodb");
        let symbols = driver_document_symbols_for_capabilities(&capabilities, text);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "find orders");
        assert_eq!(&text[symbols[0].target_range.clone().unwrap()], "orders");
    }

    #[test]
    fn driver_document_symbols_ignore_mongodb_db_calls_inside_protected_ranges() {
        let text = r#"db.orders.find({})
"db.fake.find({})"
// db.comments.find({})
db.users.updateOne({ _id: 1 }, {})"#;
        let symbols = driver_document_symbols("mongodb", text);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "find orders");
        assert_eq!(symbols[0].line, 0);
        assert_eq!(symbols[1].label, "updateOne users");
        assert_eq!(symbols[1].line, 3);
    }
}
