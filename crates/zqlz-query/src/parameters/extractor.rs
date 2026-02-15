//! SQL Parameter Extractor
//!
//! Extracts parameter placeholders from SQL queries, supporting multiple
//! database parameter styles.

use regex::Regex;
use std::sync::LazyLock;

/// A parameter extracted from a SQL query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Parameter {
    /// A named parameter like `:name`, `@name`, or `$name` (non-numeric).
    Named(String),
    /// A positional parameter like `$1`, `$2`, or `?`.
    Positional(usize),
}

impl Parameter {
    /// Returns the parameter name if this is a named parameter.
    pub fn name(&self) -> Option<&str> {
        match self {
            Parameter::Named(name) => Some(name),
            Parameter::Positional(_) => None,
        }
    }

    /// Returns the position if this is a positional parameter.
    pub fn position(&self) -> Option<usize> {
        match self {
            Parameter::Named(_) => None,
            Parameter::Positional(pos) => Some(*pos),
        }
    }

    /// Returns true if this is a named parameter.
    pub fn is_named(&self) -> bool {
        matches!(self, Parameter::Named(_))
    }

    /// Returns true if this is a positional parameter.
    pub fn is_positional(&self) -> bool {
        matches!(self, Parameter::Positional(_))
    }
}

/// The style of parameter placeholder detected in the SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterStyle {
    /// Colon-prefixed named parameters (`:name`) - Oracle, SQLite
    ColonNamed,
    /// At-sign-prefixed named parameters (`@name`) - SQL Server, MySQL
    AtNamed,
    /// Dollar-sign-prefixed named parameters (`$name`) - PostgreSQL style (non-numeric)
    DollarNamed,
    /// Dollar-sign-prefixed positional parameters (`$1`, `$2`) - PostgreSQL
    DollarPositional,
    /// Question mark positional parameters (`?`) - JDBC, MySQL, SQLite
    QuestionMark,
    /// Mixed or unknown style
    Mixed,
}

/// Result of parameter extraction containing both parameters and detected style.
#[derive(Debug, Clone)]
pub struct ExtractionResult {
    /// Extracted parameters in order of first occurrence.
    pub parameters: Vec<Parameter>,
    /// The detected parameter style (or Mixed if multiple styles are used).
    pub style: Option<ParameterStyle>,
}

// Lazy-compiled regex patterns for parameter extraction
static COLON_NAMED_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r":([a-zA-Z_][a-zA-Z0-9_]*)").expect("valid regex"));

static AT_NAMED_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"@([a-zA-Z_][a-zA-Z0-9_]*)").expect("valid regex"));

static DOLLAR_POSITIONAL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$(\d+)").expect("valid regex"));

static DOLLAR_NAMED_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$([a-zA-Z_][a-zA-Z0-9_]*)").expect("valid regex"));

static QUESTION_MARK_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\?").expect("valid regex"));

// Regex to identify string literals and comments that should be skipped
static STRING_LITERAL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"'(?:[^'\\]|\\.)*'|--[^\n]*|/\*[\s\S]*?\*/").expect("valid regex")
});

/// Extracts parameters from a SQL query string.
///
/// This function supports multiple parameter styles:
/// - Named: `:name`, `@name`, `$name` (where name starts with a letter)
/// - Positional: `$1`, `$2`, `?`
///
/// Parameters inside string literals and comments are ignored.
///
/// # Arguments
///
/// * `sql` - The SQL query string to extract parameters from.
///
/// # Returns
///
/// A vector of `Parameter` values representing all unique parameters found,
/// in order of first occurrence.
///
/// # Example
///
/// ```
/// use zqlz_query::parameters::{extract_parameters, Parameter};
///
/// let params = extract_parameters("SELECT * FROM users WHERE id = :id AND name = :name");
/// assert_eq!(params, vec![Parameter::Named("id".into()), Parameter::Named("name".into())]);
///
/// let params = extract_parameters("SELECT * FROM users WHERE id = $1 AND name = $2");
/// assert_eq!(params, vec![Parameter::Positional(1), Parameter::Positional(2)]);
/// ```
pub fn extract_parameters(sql: &str) -> Vec<Parameter> {
    extract_parameters_with_style(sql).parameters
}

/// Extracts parameters from a SQL query string with style detection.
///
/// Similar to `extract_parameters`, but also returns the detected parameter style.
///
/// # Arguments
///
/// * `sql` - The SQL query string to extract parameters from.
///
/// # Returns
///
/// An `ExtractionResult` containing the parameters and detected style.
pub fn extract_parameters_with_style(sql: &str) -> ExtractionResult {
    // Replace string literals and comments with spaces to avoid extracting
    // parameters from within them
    let masked_sql = mask_strings_and_comments(sql);

    let mut parameters: Vec<Parameter> = Vec::new();
    let mut seen: std::collections::HashSet<Parameter> = std::collections::HashSet::new();
    let mut styles_found: Vec<ParameterStyle> = Vec::new();

    // Extract colon-named parameters (:name)
    for cap in COLON_NAMED_REGEX.captures_iter(&masked_sql) {
        if let Some(name_match) = cap.get(1) {
            let name = name_match.as_str().to_string();
            let param = Parameter::Named(name);
            if seen.insert(param.clone()) {
                parameters.push(param);
                if !styles_found.contains(&ParameterStyle::ColonNamed) {
                    styles_found.push(ParameterStyle::ColonNamed);
                }
            }
        }
    }

    // Extract at-named parameters (@name)
    for cap in AT_NAMED_REGEX.captures_iter(&masked_sql) {
        if let Some(name_match) = cap.get(1) {
            let name = name_match.as_str().to_string();
            let param = Parameter::Named(name);
            if seen.insert(param.clone()) {
                parameters.push(param);
                if !styles_found.contains(&ParameterStyle::AtNamed) {
                    styles_found.push(ParameterStyle::AtNamed);
                }
            }
        }
    }

    // Extract dollar-positional parameters ($1, $2, etc.)
    for cap in DOLLAR_POSITIONAL_REGEX.captures_iter(&masked_sql) {
        if let Some(num_match) = cap.get(1) {
            if let Ok(position) = num_match.as_str().parse::<usize>() {
                let param = Parameter::Positional(position);
                if seen.insert(param.clone()) {
                    parameters.push(param);
                    if !styles_found.contains(&ParameterStyle::DollarPositional) {
                        styles_found.push(ParameterStyle::DollarPositional);
                    }
                }
            }
        }
    }

    // Extract dollar-named parameters ($name)
    for cap in DOLLAR_NAMED_REGEX.captures_iter(&masked_sql) {
        if let Some(name_match) = cap.get(1) {
            let name = name_match.as_str().to_string();
            let param = Parameter::Named(name);
            if seen.insert(param.clone()) {
                parameters.push(param);
                if !styles_found.contains(&ParameterStyle::DollarNamed) {
                    styles_found.push(ParameterStyle::DollarNamed);
                }
            }
        }
    }

    // Extract question mark parameters (?)
    // Each ? is a separate positional parameter
    let question_count = QUESTION_MARK_REGEX.find_iter(&masked_sql).count();
    if question_count > 0 {
        for i in 1..=question_count {
            let param = Parameter::Positional(i);
            if seen.insert(param.clone()) {
                parameters.push(param);
            }
        }
        if !styles_found.contains(&ParameterStyle::QuestionMark) {
            styles_found.push(ParameterStyle::QuestionMark);
        }
    }

    let style = match styles_found.len() {
        0 => None,
        1 => Some(styles_found[0]),
        _ => Some(ParameterStyle::Mixed),
    };

    ExtractionResult { parameters, style }
}

/// Masks string literals and comments in SQL with spaces.
///
/// This prevents parameter extraction from finding placeholders within
/// string literals or comments.
fn mask_strings_and_comments(sql: &str) -> String {
    STRING_LITERAL_REGEX
        .replace_all(sql, |caps: &regex::Captures| " ".repeat(caps[0].len()))
        .into_owned()
}
