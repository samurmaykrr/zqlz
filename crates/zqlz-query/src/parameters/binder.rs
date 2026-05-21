//! SQL Parameter Binder
//!
//! Binds parameter values to SQL queries, replacing parameter placeholders
//! with their corresponding values or converting them to positional parameters.

use std::collections::HashMap;

use thiserror::Error;
use zqlz_core::{
    BindPlaceholderPolicy, SqlParameterPlaceholderKind, Value, sql_parameter_placeholders,
};

use super::{Parameter, extract_parameters_with_style};

/// Errors that can occur during parameter binding.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum BindError {
    /// A required named parameter was not provided.
    #[error("missing parameter: {0}")]
    MissingParameter(String),

    /// A required positional parameter was not provided.
    #[error("missing positional parameter: ${0}")]
    MissingPositionalParameter(usize),

    /// Parameter count mismatch for positional binding.
    #[error("expected {expected} parameters, got {actual}")]
    ParameterCountMismatch { expected: usize, actual: usize },
}

/// Result type for parameter binding operations.
pub type BindResult<T> = Result<T, BindError>;

/// Result of binding named parameters to a SQL query.
#[derive(Debug, Clone)]
pub struct BoundQuery {
    /// The SQL with placeholders converted to positional ($1, $2, etc.)
    pub sql: String,
    /// The bound values in order of positional parameters.
    pub values: Vec<Value>,
}

/// Detailed result of named parameter binding for a concrete placeholder policy.
#[derive(Debug, Clone)]
pub struct BoundNamedQuery {
    /// SQL rewritten with placeholders formatted for the target policy.
    pub sql: String,
    /// Bound values in execution order.
    pub values: Vec<Value>,
}

/// Result of binding positional parameters for a concrete placeholder policy.
#[derive(Debug, Clone)]
pub struct BoundPositionalQuery {
    /// SQL rewritten with placeholders formatted for the target policy.
    pub sql: String,
    /// Bound values in execution order.
    pub values: Vec<Value>,
}

/// Binds named parameters to a SQL query.
///
/// This function takes a SQL query with named parameter placeholders (`:name`, `@name`, or `$name`)
/// and a map of parameter names to values. It returns the SQL rewritten with PostgreSQL-style
/// positional placeholders (`$1`, `$2`, etc.) and the values in order.
///
/// Parameters can be reused multiple times in the query - each occurrence will reference
/// the same positional parameter.
///
/// # Arguments
///
/// * `sql` - The SQL query string with named parameter placeholders.
/// * `params` - A map of parameter names to values.
///
/// # Returns
///
/// A `BoundQuery` containing the rewritten SQL and ordered values, or an error
/// if any required parameter is missing.
///
/// # Example
///
/// ```
/// use zqlz_query::parameters::binder::bind_named;
/// use zqlz_core::Value;
/// use std::collections::HashMap;
///
/// let mut params = HashMap::new();
/// params.insert("id".to_string(), Value::Int64(42));
/// params.insert("name".to_string(), Value::String("Alice".to_string()));
///
/// let result = bind_named(
///     "SELECT * FROM users WHERE id = :id AND name = :name",
///     &params
/// ).unwrap();
///
/// assert_eq!(result.sql, "SELECT * FROM users WHERE id = $1 AND name = $2");
/// assert_eq!(result.values.len(), 2);
/// ```
pub fn bind_named(sql: &str, params: &HashMap<String, Value>) -> BindResult<BoundQuery> {
    let bound = bind_named_with_policy(sql, params, BindPlaceholderPolicy::DollarNumbered)?;
    Ok(BoundQuery {
        sql: bound.sql,
        values: bound.values,
    })
}

/// Binds named parameters to SQL using the provided placeholder policy.
pub fn bind_named_with_policy(
    sql: &str,
    params: &HashMap<String, Value>,
    placeholder_policy: BindPlaceholderPolicy,
) -> BindResult<BoundNamedQuery> {
    // First extract all parameters to validate they exist
    let extraction = extract_parameters_with_style(sql);
    for param in &extraction.parameters {
        if let Parameter::Named(name) = param
            && !params.contains_key(name)
        {
            return Err(BindError::MissingParameter(name.clone()));
        }
    }

    // Track which parameters we've assigned positions to
    let mut param_positions: HashMap<String, usize> = HashMap::new();
    let mut values: Vec<Value> = Vec::new();

    // Helper to get or assign a position for a named parameter
    let mut get_or_assign_position = |name: &str| -> usize {
        if let Some(&pos) = param_positions.get(name) {
            pos
        } else {
            let pos = values.len() + 1;
            param_positions.insert(name.to_string(), pos);
            if let Some(value) = params.get(name) {
                values.push(value.clone());
            }
            pos
        }
    };

    let mut result = String::with_capacity(sql.len());
    let mut last_end = 0;

    // Process all named parameter patterns in order of appearance
    let all_matches = find_all_named_params(sql);

    for (start, end, name) in all_matches {
        // Append text before this match
        result.push_str(&sql[last_end..start]);

        // Get position for this parameter
        let pos = get_or_assign_position(&name);
        result.push_str(&placeholder_policy.format(pos - 1));

        last_end = end;
    }

    // Append remaining text
    result.push_str(&sql[last_end..]);

    Ok(BoundNamedQuery {
        sql: result,
        values,
    })
}

/// Binds positional parameters to a SQL query.
///
/// This function validates that the provided values match the positional parameters
/// in the query. For PostgreSQL-style `$N` parameters, the values are reordered
/// according to the parameter numbers.
///
/// # Arguments
///
/// * `sql` - The SQL query string with positional parameter placeholders.
/// * `params` - The parameter values in order.
///
/// # Returns
///
/// A vector of values ordered by their positional parameter numbers, or an error
/// if the parameter count doesn't match.
///
/// # Example
///
/// ```
/// use zqlz_query::parameters::binder::bind_positional;
/// use zqlz_core::Value;
///
/// let values = vec![
///     Value::Int64(42),
///     Value::String("Alice".to_string()),
/// ];
///
/// let result = bind_positional(
///     "SELECT * FROM users WHERE id = $1 AND name = $2",
///     &values
/// ).unwrap();
///
/// assert_eq!(result.len(), 2);
/// ```
pub fn bind_positional(sql: &str, params: &[Value]) -> BindResult<Vec<Value>> {
    Ok(bind_positional_with_policy(sql, params, BindPlaceholderPolicy::DollarNumbered)?.values)
}

/// Binds positional parameters and rewrites placeholders for the target policy.
pub fn bind_positional_with_policy(
    sql: &str,
    params: &[Value],
    placeholder_policy: BindPlaceholderPolicy,
) -> BindResult<BoundPositionalQuery> {
    let extraction = extract_parameters_with_style(sql);

    let rewritten_sql = match extraction.style {
        Some(super::ParameterStyle::QuestionMark)
            if matches!(placeholder_policy, BindPlaceholderPolicy::QuestionMark) =>
        {
            sql.to_string()
        }
        Some(super::ParameterStyle::QuestionMark) => {
            rewrite_question_mark_placeholders(sql, placeholder_policy)
        }
        Some(super::ParameterStyle::DollarPositional) => {
            rewrite_dollar_positional_placeholders(sql, placeholder_policy)
        }
        _ => sql.to_string(),
    };

    // For ? style params, just validate count matches
    // (? params are converted to Positional(1), Positional(2), etc. by the extractor)
    if extraction.style == Some(super::ParameterStyle::QuestionMark) {
        let param_count = extraction.parameters.len();
        if param_count != params.len() {
            return Err(BindError::ParameterCountMismatch {
                expected: param_count,
                actual: params.len(),
            });
        }
        return Ok(BoundPositionalQuery {
            sql: rewritten_sql,
            values: params.to_vec(),
        });
    }

    // Find all positional parameters and get the maximum position
    let mut max_position: usize = 0;
    let mut positions_used: Vec<usize> = Vec::new();

    for param in &extraction.parameters {
        if let Parameter::Positional(pos) = param {
            if *pos > max_position {
                max_position = *pos;
            }
            if !positions_used.contains(pos) {
                positions_used.push(*pos);
            }
        }
    }

    // For $N style params, we need exactly max_position values
    // unless there are gaps (which is allowed - e.g., $1 and $3 used but not $2)
    if max_position > 0 {
        if params.len() < max_position {
            // Check which positions are actually used vs. provided
            for pos in &positions_used {
                if *pos > params.len() {
                    return Err(BindError::MissingPositionalParameter(*pos));
                }
            }
        }

        // Return values in order, only for positions actually used
        let mut result = Vec::with_capacity(positions_used.len());
        positions_used.sort();
        for pos in positions_used {
            if pos > params.len() {
                return Err(BindError::MissingPositionalParameter(pos));
            }
            result.push(params[pos - 1].clone());
        }
        return Ok(BoundPositionalQuery {
            sql: rewritten_sql,
            values: result,
        });
    }

    // No parameters found
    if !params.is_empty() {
        return Err(BindError::ParameterCountMismatch {
            expected: 0,
            actual: params.len(),
        });
    }

    Ok(BoundPositionalQuery {
        sql: rewritten_sql,
        values: params.to_vec(),
    })
}

fn rewrite_question_mark_placeholders(sql: &str, policy: BindPlaceholderPolicy) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut parameter_index = 0usize;
    let mut last_end = 0usize;

    for placeholder in sql_parameter_placeholders(sql) {
        if !matches!(placeholder.kind, SqlParameterPlaceholderKind::QuestionMark) {
            continue;
        }

        result.push_str(&sql[last_end..placeholder.start]);
        result.push_str(&policy.format(parameter_index));
        parameter_index += 1;
        last_end = placeholder.end;
    }

    result.push_str(&sql[last_end..]);
    result
}

fn rewrite_dollar_positional_placeholders(sql: &str, policy: BindPlaceholderPolicy) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut last_end = 0usize;

    for placeholder in sql_parameter_placeholders(sql) {
        let SqlParameterPlaceholderKind::DollarPositional(position) = placeholder.kind else {
            continue;
        };

        result.push_str(&sql[last_end..placeholder.start]);
        result.push_str(&policy.format(position.saturating_sub(1)));
        last_end = placeholder.end;
    }

    result.push_str(&sql[last_end..]);
    result
}

/// Find all named parameter matches in SQL, excluding those in strings/comments.
fn find_all_named_params(sql: &str) -> Vec<(usize, usize, String)> {
    sql_parameter_placeholders(sql)
        .into_iter()
        .filter_map(|placeholder| match placeholder.kind {
            SqlParameterPlaceholderKind::Named(name) => {
                Some((placeholder.start, placeholder.end, name))
            }
            SqlParameterPlaceholderKind::DollarPositional(_)
            | SqlParameterPlaceholderKind::QuestionMark => None,
        })
        .collect()
}
