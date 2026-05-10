use crate::{FormatterCapability, Result, ZqlzError, get_dialect_profile};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatOptions {
    pub indent_size: usize,
    pub uppercase_keywords: bool,
    pub lines_between_queries: usize,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            indent_size: 4,
            uppercase_keywords: true,
            lines_between_queries: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatRequest {
    pub source: String,
    pub driver_id: String,
    pub object_type: Option<String>,
    pub options: FormatOptions,
}

impl FormatRequest {
    pub fn new(source: impl Into<String>, driver_id: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            driver_id: driver_id.into(),
            object_type: None,
            options: FormatOptions::default(),
        }
    }

    pub fn with_object_type(mut self, object_type: impl Into<String>) -> Self {
        self.object_type = Some(object_type.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatOutcome {
    pub source: String,
}

pub trait CodeFormatterProvider: Send + Sync {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome>;
}

pub fn formatter_provider_for_driver(driver_id: &str) -> Option<Arc<dyn CodeFormatterProvider>> {
    let normalized = normalize_driver_id(driver_id);
    match normalized.as_str() {
        "postgres" => Some(Arc::new(PostgresFormatterProvider)),
        "sqlite" | "turso" | "mssql" | "sqlserver" => Some(Arc::new(SqliteFormatterProvider)),
        "mongodb" => Some(Arc::new(JsonDocumentFormatterProvider)),
        "redis" => Some(Arc::new(RedisCommandFormatterProvider)),
        _ => match get_dialect_profile(&normalized).map(|profile| profile.formatter) {
            Some(FormatterCapability::Sql) => Some(Arc::new(GenericSqlFormatterProvider {
                dialect: sqlformat_dialect_for_driver(&normalized),
            })),
            Some(FormatterCapability::Custom) => None,
            Some(FormatterCapability::None) | None => None,
        },
    }
}

#[derive(Debug, Clone)]
pub struct GenericSqlFormatterProvider {
    dialect: sqlformat::Dialect,
}

impl GenericSqlFormatterProvider {
    pub fn new(driver_id: &str) -> Self {
        Self {
            dialect: sqlformat_dialect_for_driver(&normalize_driver_id(driver_id)),
        }
    }

    fn format_sql(&self, source: &str, options: &FormatOptions) -> String {
        sqlformat::format(
            source,
            &sqlformat::QueryParams::None,
            &sqlformat::FormatOptions {
                indent: sqlformat::Indent::Spaces(options.indent_size as u8),
                uppercase: Some(options.uppercase_keywords),
                lines_between_queries: options.lines_between_queries as u8,
                ignore_case_convert: None,
                inline: false,
                max_inline_block: 50,
                max_inline_arguments: None,
                max_inline_top_level: None,
                joins_as_top_level: false,
                dialect: self.dialect,
            },
        )
    }
}

impl CodeFormatterProvider for GenericSqlFormatterProvider {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome> {
        Ok(FormatOutcome {
            source: ensure_single_trailing_newline(
                &self.format_sql(&request.source, &request.options),
            ),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PostgresFormatterProvider;

impl CodeFormatterProvider for PostgresFormatterProvider {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome> {
        let generic = GenericSqlFormatterProvider::new("postgres");
        let source = if is_create_routine(&request.source) {
            format_postgres_routine(&request.source, &generic, &request.options)
        } else {
            generic.format_sql(&request.source, &request.options)
        };
        Ok(FormatOutcome {
            source: ensure_single_trailing_newline(&source),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SqliteFormatterProvider;

impl CodeFormatterProvider for SqliteFormatterProvider {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome> {
        let generic = GenericSqlFormatterProvider::new(&request.driver_id);
        Ok(FormatOutcome {
            source: ensure_single_trailing_newline(&normalize_bracket_identifier_spacing(
                &generic.format_sql(&request.source, &request.options),
            )),
        })
    }
}

#[derive(Debug, Clone)]
pub struct JsonDocumentFormatterProvider;

impl CodeFormatterProvider for JsonDocumentFormatterProvider {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome> {
        match serde_json::from_str::<serde_json::Value>(&request.source) {
            Ok(value) => serde_json::to_string_pretty(&value)
                .map(|source| FormatOutcome { source })
                .map_err(|error| ZqlzError::Configuration(error.to_string())),
            Err(_) => Ok(FormatOutcome {
                source: request.source.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisCommandFormatterProvider;

impl CodeFormatterProvider for RedisCommandFormatterProvider {
    fn format(&self, request: &FormatRequest) -> Result<FormatOutcome> {
        Ok(FormatOutcome {
            source: format_redis_commands(&request.source),
        })
    }
}

fn normalize_driver_id(driver_id: &str) -> String {
    match driver_id.to_ascii_lowercase().as_str() {
        "postgresql" => "postgres".to_string(),
        "sqlserver" => "mssql".to_string(),
        other => other.to_string(),
    }
}

fn sqlformat_dialect_for_driver(driver_id: &str) -> sqlformat::Dialect {
    match driver_id {
        "postgres" => sqlformat::Dialect::PostgreSql,
        _ => sqlformat::Dialect::Generic,
    }
}

fn is_create_routine(source: &str) -> bool {
    let upper = source.to_ascii_uppercase();
    upper.contains("CREATE")
        && (upper.contains(" FUNCTION ") || upper.contains(" PROCEDURE "))
        && find_keyword(source, "AS").is_some()
}

fn format_postgres_routine(
    source: &str,
    _generic: &GenericSqlFormatterProvider,
    options: &FormatOptions,
) -> String {
    let Some(as_index) = find_keyword(source, "AS") else {
        return GenericSqlFormatterProvider::new("postgres").format_sql(source, options);
    };

    let before_as = source[..as_index].trim();
    let after_as = source[as_index + 2..].trim();
    let mut source = format_postgres_routine_header(before_as, options);
    source.push_str("\nAS ");
    source.push_str(&format_postgres_routine_body(after_as, options));
    source
}

fn format_postgres_routine_body(body: &str, options: &FormatOptions) -> String {
    let body = body.trim();
    let Some(dollar_quote) = parse_leading_dollar_quote(body) else {
        return body.trim_end().to_string();
    };

    let after_open = &body[dollar_quote.len()..];
    let Some(close_index) = after_open.rfind(dollar_quote) else {
        return body.trim_end().to_string();
    };

    let inner = &after_open[..close_index];
    let tail = after_open[close_index + dollar_quote.len()..].trim();
    let normalized_inner = normalize_routine_body_indentation(inner, options.indent_size);
    let mut formatted = String::new();
    formatted.push_str(dollar_quote);
    if !normalized_inner.is_empty() {
        formatted.push('\n');
        formatted.push_str(&normalized_inner);
        formatted.push('\n');
    }
    formatted.push_str(dollar_quote);
    formatted.push_str(tail);
    formatted
}

fn parse_leading_dollar_quote(source: &str) -> Option<&str> {
    let bytes = source.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return None;
    }

    let closing = bytes.iter().enumerate().skip(1).find_map(|(index, byte)| {
        if *byte == b'$' { Some(index) } else { None }
    })?;
    Some(&source[..=closing])
}

fn normalize_routine_body_indentation(source: &str, indent_size: usize) -> String {
    let trimmed = source.trim_matches('\n');
    if trimmed.trim().is_empty() {
        return String::new();
    }

    let lines: Vec<&str> = trimmed.lines().collect();
    let common_indent = lines
        .iter()
        .filter_map(|line| {
            if line.trim().is_empty() {
                None
            } else {
                Some(
                    line.chars()
                        .take_while(|ch| *ch == ' ' || *ch == '\t')
                        .count(),
                )
            }
        })
        .min()
        .unwrap_or(0);
    let body_indent = " ".repeat(indent_size);

    lines
        .into_iter()
        .map(|line| {
            if line.trim().is_empty() {
                String::new()
            } else {
                let stripped = line
                    .char_indices()
                    .nth(common_indent)
                    .map(|(index, _)| &line[index..])
                    .unwrap_or_else(|| line.trim_start());
                format!("{body_indent}{stripped}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn find_keyword(source: &str, keyword: &str) -> Option<usize> {
    let bytes = source.as_bytes();
    let keyword_length = keyword.len();
    let mut index = 0;
    while index + keyword_length <= bytes.len() {
        if source[index..index + keyword_length].eq_ignore_ascii_case(keyword)
            && is_keyword_boundary(bytes, index.checked_sub(1))
            && is_keyword_boundary(bytes, Some(index + keyword_length))
        {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn format_postgres_routine_header(header: &str, _options: &FormatOptions) -> String {
    let compact = collapse_whitespace(header);
    let Some(returns_index) = find_keyword(&compact, "RETURNS") else {
        return compact;
    };
    let Some(language_relative_index) = find_keyword(&compact[returns_index..], "LANGUAGE") else {
        return compact;
    };

    let language_index = returns_index + language_relative_index;
    let signature = compact[..returns_index].trim();
    let returns = compact[returns_index..language_index].trim();
    let language_and_attributes = compact[language_index..].trim();
    let Some(attributes_index) = find_routine_attribute_start(language_and_attributes) else {
        return format!("{signature}\n{returns}\n{language_and_attributes}");
    };

    let language = language_and_attributes[..attributes_index].trim();
    let attributes = language_and_attributes[attributes_index..].trim();
    let indent = "";
    format!("{signature}\n{indent}{returns}\n{indent}{language}\n{indent}{attributes}")
}

fn find_routine_attribute_start(source: &str) -> Option<usize> {
    [
        "IMMUTABLE",
        "STABLE",
        "VOLATILE",
        "STRICT",
        "CALLED",
        "RETURNS",
        "PARALLEL",
        "COST",
        "ROWS",
        "SECURITY",
        "LEAKPROOF",
    ]
    .into_iter()
    .filter_map(|keyword| find_keyword(source, keyword))
    .filter(|index| *index > 0)
    .min()
}

fn collapse_whitespace(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn ensure_single_trailing_newline(source: &str) -> String {
    let trimmed = source.trim_end();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}\n")
    }
}

fn format_redis_commands(source: &str) -> String {
    source
        .lines()
        .map(format_redis_command_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_redis_command_line(line: &str) -> String {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return trimmed.to_string();
    }

    let collapsed = collapse_command_whitespace(trimmed);
    let Some((command, rest)) = split_first_command_token(&collapsed) else {
        return collapsed;
    };

    if rest.is_empty() {
        command.to_ascii_uppercase()
    } else {
        format!("{} {}", command.to_ascii_uppercase(), rest)
    }
}

fn split_first_command_token(source: &str) -> Option<(&str, &str)> {
    let split_index = source.find(char::is_whitespace).unwrap_or(source.len());
    let command = &source[..split_index];
    if command.is_empty() {
        return None;
    }

    Some((command, source[split_index..].trim_start()))
}

fn collapse_command_whitespace(source: &str) -> String {
    let mut collapsed = String::with_capacity(source.len());
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;
    let mut pending_space = false;

    for character in source.chars() {
        if escaped {
            collapsed.push(character);
            escaped = false;
            continue;
        }

        if character == '\\' && (in_single_quote || in_double_quote) {
            collapsed.push(character);
            escaped = true;
            continue;
        }

        match character {
            '\'' if !in_double_quote => {
                if pending_space && !collapsed.is_empty() {
                    collapsed.push(' ');
                    pending_space = false;
                }
                in_single_quote = !in_single_quote;
                collapsed.push(character);
            }
            '"' if !in_single_quote => {
                if pending_space && !collapsed.is_empty() {
                    collapsed.push(' ');
                    pending_space = false;
                }
                in_double_quote = !in_double_quote;
                collapsed.push(character);
            }
            character if character.is_whitespace() && !in_single_quote && !in_double_quote => {
                pending_space = true;
            }
            character => {
                if pending_space && !collapsed.is_empty() {
                    collapsed.push(' ');
                }
                pending_space = false;
                collapsed.push(character);
            }
        }
    }

    collapsed
}

fn is_keyword_boundary(bytes: &[u8], index: Option<usize>) -> bool {
    let Some(index) = index else {
        return true;
    };
    let Some(byte) = bytes.get(index) else {
        return true;
    };
    !byte.is_ascii_alphanumeric() && *byte != b'_'
}

pub fn normalize_bracket_identifier_spacing(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut index = 0;

    while index < sql.len() {
        let Some(relative_open) = sql[index..].find('[') else {
            normalized.push_str(&sql[index..]);
            break;
        };
        let open_index = index + relative_open;

        normalized.push_str(&sql[index..open_index]);

        let mut cursor = open_index + 1;
        let mut closing = None;
        while cursor < sql.len() {
            let byte = sql.as_bytes()[cursor];
            if byte == b']' {
                if cursor + 1 < sql.len() && sql.as_bytes()[cursor + 1] == b']' {
                    cursor += 2;
                    continue;
                }

                closing = Some(cursor);
                break;
            }
            cursor += 1;
        }

        let Some(closing_index) = closing else {
            normalized.push_str(&sql[open_index..]);
            break;
        };

        let inner = &sql[open_index + 1..closing_index];
        normalized.push('[');
        normalized.push_str(inner.trim());
        normalized.push(']');
        index = closing_index + 1;
    }

    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_c_extension_function_preserves_as_body() {
        let provider = PostgresFormatterProvider;
        let request = FormatRequest::new(
            "CREATE OR REPLACE FUNCTION public._lt_q_regex(ltree[], lquery[]) RETURNS boolean LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS '$libdir/ltree', $function$_lt_q_regex$function$",
            "postgres",
        );

        let formatted = provider.format(&request).unwrap().source;
        assert!(formatted.starts_with("CREATE OR REPLACE FUNCTION"));
        assert!(formatted.contains("\nRETURNS boolean"));
        assert!(formatted.contains("\nLANGUAGE c"));
        assert!(formatted.contains("\nAS '$libdir/ltree', $function$_lt_q_regex$function$"));
    }

    #[test]
    fn postgres_plpgsql_body_is_preserved() {
        let provider = PostgresFormatterProvider;
        let body = "$$BEGIN\nRETURN 1;\nEND$$";
        let request = FormatRequest::new(
            format!(
                "CREATE OR REPLACE FUNCTION public.answer() RETURNS integer LANGUAGE plpgsql AS {body}"
            ),
            "postgres",
        );

        let formatted = provider.format(&request).unwrap().source;

        assert!(formatted.contains("$$\n    BEGIN\n    RETURN 1;\n    END\n$$"));
    }

    #[test]
    fn postgres_pg_get_functiondef_output_is_readable() {
        let provider = PostgresFormatterProvider;
        let request = FormatRequest::new(
            "CREATE OR REPLACE FUNCTION zqlz_audit.capture_ddl_command()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN
            RAISE NOTICE 'DDL command observed by zqlz_feature_lab_ddl_audit';
        END;
        $function$
",
            "postgres",
        );

        let formatted = provider.format(&request).unwrap().source;

        assert!(formatted.contains("CREATE OR REPLACE FUNCTION zqlz_audit.capture_ddl_command()"));
        assert!(formatted.contains("\nRETURNS event_trigger"));
        assert!(formatted.contains("\nLANGUAGE plpgsql"));
        assert!(formatted.contains("\nAS $function$\n"));
        assert!(formatted.contains("    BEGIN\n"));
        assert!(formatted.contains("        RAISE NOTICE"));
        assert!(formatted.contains("    END;"));
        assert!(formatted.contains("\n$function$"));
        assert!(formatted.ends_with('\n'));
    }

    #[test]
    fn postgres_dollar_quote_variants_keep_delimiters() {
        let provider = PostgresFormatterProvider;
        let request = FormatRequest::new(
            "CREATE FUNCTION public.answer() RETURNS integer LANGUAGE plpgsql AS $function$
BEGIN
RETURN 1;
END;
$function$;",
            "postgres",
        );

        let formatted = provider.format(&request).unwrap().source;

        assert!(
            formatted.contains("AS $function$\n    BEGIN\n    RETURN 1;\n    END;\n$function$;")
        );
    }

    #[test]
    fn postgres_routine_attributes_stay_in_header() {
        let provider = PostgresFormatterProvider;
        let request = FormatRequest::new(
            "CREATE FUNCTION public.answer() RETURNS integer LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$BEGIN RETURN 1; END$$",
            "postgres",
        );

        let formatted = provider.format(&request).unwrap().source;

        assert!(formatted.contains("\nLANGUAGE plpgsql"));
        assert!(formatted.contains("\nIMMUTABLE PARALLEL SAFE"));
        assert!(formatted.contains("\nAS $$"));
    }

    #[test]
    fn sqlite_trims_bracket_identifier_edges() {
        assert_eq!(
            normalize_bracket_identifier_spacing("SELECT [ users ], [Product ]] Name]"),
            "SELECT [users], [Product ]] Name]"
        );
    }

    #[test]
    fn mongodb_pretty_prints_valid_json() {
        let provider = JsonDocumentFormatterProvider;
        let request = FormatRequest::new("{\"name\":\"Ada\",\"scores\":[1,2]}", "mongodb");

        let formatted = provider.format(&request).unwrap().source;

        assert!(formatted.contains("\n  \"name\": \"Ada\""));
    }

    #[test]
    fn mongodb_keeps_invalid_json_unchanged() {
        let provider = JsonDocumentFormatterProvider;
        let source = "db.users.find({ active: true })";
        let request = FormatRequest::new(source, "mongodb");

        assert_eq!(provider.format(&request).unwrap().source, source);
    }

    #[test]
    fn redis_formats_command_lines() {
        let provider = formatter_provider_for_driver("redis").expect("redis formatter");
        let request = FormatRequest::new(
            "  set    user:1    \"Ada  Lovelace\"\n# keep comment\n get\tuser:1  ",
            "redis",
        );

        let formatted = provider.format(&request).unwrap().source;

        assert_eq!(
            formatted,
            "SET user:1 \"Ada  Lovelace\"\n# keep comment\nGET user:1"
        );
    }
}
