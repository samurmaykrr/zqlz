//! SQL Parameter Extractor
//!
//! Extracts parameter placeholders from SQL queries, supporting multiple
//! database parameter styles.

use zqlz_core::{SqlParameterPlaceholderKind, sql_parameter_placeholders};

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
    let mut parameters: Vec<Parameter> = Vec::new();
    let mut seen: std::collections::HashSet<Parameter> = std::collections::HashSet::new();
    let mut styles_found: Vec<ParameterStyle> = Vec::new();

    let mut question_index = 0usize;
    for placeholder in sql_parameter_placeholders(sql) {
        let (param, style) = match placeholder.kind {
            SqlParameterPlaceholderKind::Named(name) => {
                let style = match sql.as_bytes().get(placeholder.start) {
                    Some(b':') => ParameterStyle::ColonNamed,
                    Some(b'@') => ParameterStyle::AtNamed,
                    Some(b'$') => ParameterStyle::DollarNamed,
                    _ => ParameterStyle::Mixed,
                };
                (Parameter::Named(name), style)
            }
            SqlParameterPlaceholderKind::DollarPositional(position) => (
                Parameter::Positional(position),
                ParameterStyle::DollarPositional,
            ),
            SqlParameterPlaceholderKind::QuestionMark => {
                question_index += 1;
                (
                    Parameter::Positional(question_index),
                    ParameterStyle::QuestionMark,
                )
            }
        };

        if seen.insert(param.clone()) {
            parameters.push(param);
        }
        if !styles_found.contains(&style) {
            styles_found.push(style);
        }
    }

    let style = match styles_found.len() {
        0 => None,
        1 => Some(styles_found[0]),
        _ => Some(ParameterStyle::Mixed),
    };

    ExtractionResult { parameters, style }
}
