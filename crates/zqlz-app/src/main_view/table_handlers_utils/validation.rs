//! Validation utilities for table handlers.
//!
//! This module provides functions to validate user input and parse
//! data values.

/// Validates a table name and returns an error message if invalid.
pub(in crate::main_view) fn validate_table_name(name: &str) -> Option<&'static str> {
    let name = name.trim();

    if name.is_empty() {
        return Some("Table name cannot be empty");
    }

    if name.len() > 128 {
        return Some("Table name is too long (max 128 characters)");
    }

    // Check for invalid starting character
    let first_char = name.chars().next().unwrap();
    if !first_char.is_alphabetic() && first_char != '_' {
        return Some("Table name must start with a letter or underscore");
    }

    // Check for invalid characters (allow alphanumeric, underscore, and some databases allow $)
    for c in name.chars() {
        if !c.is_alphanumeric() && c != '_' && c != '$' {
            return Some("Table name contains invalid characters");
        }
    }

    // Check for reserved SQL keywords (common ones)
    let upper = name.to_uppercase();
    let reserved = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TABLE",
        "INDEX",
        "VIEW",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "ON",
        "USING",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AS",
        "DISTINCT",
        "ALL",
        "ANY",
        "SOME",
        "EXISTS",
        "IN",
        "BETWEEN",
        "LIKE",
        "IS",
        "UNIQUE",
        "PRIMARY",
        "FOREIGN",
        "KEY",
        "REFERENCES",
        "CONSTRAINT",
        "DEFAULT",
        "CHECK",
        "CASCADE",
        "SET",
        "SCHEMA",
        "DATABASE",
        "PROCEDURE",
        "FUNCTION",
        "TRIGGER",
        "GRANT",
        "REVOKE",
        "COMMIT",
        "ROLLBACK",
        "TRANSACTION",
        "BEGIN",
        "DECLARE",
        "CURSOR",
        "FETCH",
        "OPEN",
        "CLOSE",
    ];

    if reserved.contains(&upper.as_str()) {
        return Some("Table name is a reserved SQL keyword");
    }

    None
}

/// Parses a value for inline cell editing.
/// - "NULL" (case-insensitive) -> None (database NULL)
/// - "" (empty string) -> Some("") (empty string in database)
/// - other values -> Some(value)
pub(in crate::main_view) fn parse_inline_value(value: &str) -> Option<String> {
    if value.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(value.to_string())
    }
}
