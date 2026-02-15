//! Custom SQL filters for MiniJinja

use minijinja::{Environment, Value};

/// SQL-specific filters
pub struct SqlFilters;

impl SqlFilters {
    /// Quote a string for SQL
    pub fn sqlquote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    /// Create an IN clause from an array
    pub fn inclause(values: Vec<Value>) -> String {
        let quoted: Vec<String> = values
            .iter()
            .map(|v| match v.as_str() {
                Some(s) => Self::sqlquote(s),
                None => v.to_string(),
            })
            .collect();

        format!("({})", quoted.join(", "))
    }

    /// Quote an identifier (table/column name)
    pub fn identifier(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

/// Register all SQL filters with a MiniJinja environment
pub fn register_filters(env: &mut Environment) {
    env.add_filter("sqlquote", |value: String| SqlFilters::sqlquote(&value));
    env.add_filter("inclause", SqlFilters::inclause);
    env.add_filter("identifier", |value: String| SqlFilters::identifier(&value));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlquote() {
        assert_eq!(SqlFilters::sqlquote("hello"), "'hello'");
        assert_eq!(SqlFilters::sqlquote("it's"), "'it''s'");
    }

    #[test]
    fn test_identifier() {
        assert_eq!(SqlFilters::identifier("users"), "\"users\"");
        assert_eq!(SqlFilters::identifier("user\"name"), "\"user\"\"name\"");
    }
}
