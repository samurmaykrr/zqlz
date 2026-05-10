fn validate_identifier_like_name(
    raw_name: &str,
    empty_message: &'static str,
    too_long_message: &'static str,
    start_message: &'static str,
    invalid_character_message: &'static str,
    reserved_message: &'static str,
    reserved_keywords: &[&str],
) -> Option<&'static str> {
    let name = raw_name.trim();

    if name.is_empty() {
        return Some(empty_message);
    }

    if name.len() > 128 {
        return Some(too_long_message);
    }

    let Some(first_character) = name.chars().next() else {
        return Some(empty_message);
    };

    if !first_character.is_alphabetic() && first_character != '_' {
        return Some(start_message);
    }

    for character in name.chars() {
        if !character.is_alphanumeric() && character != '_' && character != '$' {
            return Some(invalid_character_message);
        }
    }

    let upper = name.to_uppercase();
    if reserved_keywords.contains(&upper.as_str()) {
        return Some(reserved_message);
    }

    None
}

/// Validate a table name with the app's current constraints.
pub fn validate_table_name(name: &str) -> Option<&'static str> {
    const RESERVED: &[&str] = &[
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

    validate_identifier_like_name(
        name,
        "Table name cannot be empty",
        "Table name is too long (max 128 characters)",
        "Table name must start with a letter or underscore",
        "Table name contains invalid characters",
        "Table name is a reserved SQL keyword",
        RESERVED,
    )
}

/// Validate a view name with the app's current constraints.
pub fn validate_view_name(name: &str) -> Option<&'static str> {
    const RESERVED: &[&str] = &[
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
        "AS",
        "IN",
        "IS",
        "LIKE",
        "BETWEEN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "EXISTS",
        "ALL",
        "ANY",
        "SOME",
        "DISTINCT",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "INTO",
        "VALUES",
        "SET",
        "DEFAULT",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "UNIQUE",
        "CHECK",
        "CONSTRAINT",
        "DATABASE",
        "SCHEMA",
        "GRANT",
        "REVOKE",
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
    ];

    validate_identifier_like_name(
        name,
        "View name cannot be empty",
        "View name is too long (max 128 characters)",
        "View name must start with a letter or underscore",
        "View name contains invalid characters",
        "View name is a reserved SQL keyword",
        RESERVED,
    )
}

/// Validate a saved-query display name.
pub fn validate_query_name(name: &str) -> Option<&'static str> {
    let name = name.trim();

    if name.is_empty() {
        return Some("Query name cannot be empty");
    }

    if name.len() > 128 {
        return Some("Query name is too long (max 128 characters)");
    }

    for character in name.chars() {
        if character == '/'
            || character == '\\'
            || character == '\0'
            || character == '\n'
            || character == '\r'
        {
            return Some("Query name contains invalid characters");
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{validate_query_name, validate_table_name, validate_view_name};

    #[test]
    fn table_name_validation_matches_expected_constraints() {
        assert_eq!(validate_table_name(""), Some("Table name cannot be empty"));
        assert_eq!(
            validate_table_name("1users"),
            Some("Table name must start with a letter or underscore")
        );
        assert_eq!(
            validate_table_name("user-profile"),
            Some("Table name contains invalid characters")
        );
        assert_eq!(
            validate_table_name("select"),
            Some("Table name is a reserved SQL keyword")
        );
        assert_eq!(validate_table_name("users_2025"), None);
    }

    #[test]
    fn view_name_validation_matches_expected_constraints() {
        assert_eq!(validate_view_name(""), Some("View name cannot be empty"));
        assert_eq!(
            validate_view_name("9view"),
            Some("View name must start with a letter or underscore")
        );
        assert_eq!(
            validate_view_name("report view"),
            Some("View name contains invalid characters")
        );
        assert_eq!(
            validate_view_name("from"),
            Some("View name is a reserved SQL keyword")
        );
        assert_eq!(validate_view_name("active_users"), None);
    }

    #[test]
    fn query_name_validation_disallows_problematic_path_and_control_chars() {
        assert_eq!(validate_query_name(""), Some("Query name cannot be empty"));
        assert_eq!(
            validate_query_name("folder/report"),
            Some("Query name contains invalid characters")
        );
        assert_eq!(
            validate_query_name("line\nbreak"),
            Some("Query name contains invalid characters")
        );
        assert_eq!(validate_query_name("Weekly Revenue"), None);
    }
}
