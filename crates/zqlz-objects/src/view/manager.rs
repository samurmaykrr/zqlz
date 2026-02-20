//! View manager implementation
//!
//! Provides functionality for creating, altering, and dropping views
//! and materialized views across different database dialects.

use serde::{Deserialize, Serialize};

/// Specification for creating a new view
///
/// # Examples
///
/// ```
/// use zqlz_objects::ViewSpec;
///
/// let spec = ViewSpec::new("active_users", "SELECT * FROM users WHERE active = true");
/// assert_eq!(spec.name(), "active_users");
/// assert!(!spec.is_materialized());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSpec {
    name: String,
    schema: Option<String>,
    query: String,
    is_materialized: bool,
    columns: Vec<String>,
    with_check_option: Option<CheckOption>,
    comment: Option<String>,
}

impl ViewSpec {
    /// Create a new view specification
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            query: query.into(),
            is_materialized: false,
            columns: Vec::new(),
            with_check_option: None,
            comment: None,
        }
    }

    /// Create a new materialized view specification
    pub fn materialized(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            query: query.into(),
            is_materialized: true,
            columns: Vec::new(),
            with_check_option: None,
            comment: None,
        }
    }

    /// Set the schema for this view
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set explicit column names for the view
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    /// Set the check option for the view
    pub fn with_check_option(mut self, option: CheckOption) -> Self {
        self.with_check_option = Some(option);
        self
    }

    /// Set a comment for the view
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Get the view name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the schema (if set)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the view query
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Check if this is a materialized view
    pub fn is_materialized(&self) -> bool {
        self.is_materialized
    }

    /// Get the explicit column names (if any)
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the check option (if any)
    pub fn check_option(&self) -> Option<&CheckOption> {
        self.with_check_option.as_ref()
    }

    /// Get the comment (if any)
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Get the fully qualified name (schema.name or just name)
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

/// Check option for views (controls INSERT/UPDATE through views)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckOption {
    /// WITH LOCAL CHECK OPTION - only checks conditions in this view
    Local,
    /// WITH CASCADED CHECK OPTION - checks conditions in this view and all underlying views
    Cascaded,
}

/// Database dialect for view generation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewDialect {
    /// PostgreSQL syntax
    PostgreSQL,
    /// MySQL/MariaDB syntax
    MySQL,
    /// SQLite syntax
    SQLite,
    /// Microsoft SQL Server syntax
    MsSql,
}

impl ViewDialect {
    /// Check if this dialect supports materialized views
    pub fn supports_materialized_views(&self) -> bool {
        matches!(self, ViewDialect::PostgreSQL | ViewDialect::MsSql)
    }

    /// Check if this dialect supports check options on views
    pub fn supports_check_option(&self) -> bool {
        !matches!(self, ViewDialect::SQLite)
    }
}

/// Error type for view operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewError {
    /// View name is empty
    EmptyName,
    /// Query is empty
    EmptyQuery,
    /// Materialized views not supported by this dialect
    MaterializedViewNotSupported,
    /// Check option not supported by this dialect
    CheckOptionNotSupported,
    /// Invalid column specification
    InvalidColumns(String),
}

impl std::fmt::Display for ViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewError::EmptyName => write!(f, "View name cannot be empty"),
            ViewError::EmptyQuery => write!(f, "View query cannot be empty"),
            ViewError::MaterializedViewNotSupported => {
                write!(f, "Materialized views are not supported by this dialect")
            }
            ViewError::CheckOptionNotSupported => {
                write!(f, "Check option is not supported by this dialect")
            }
            ViewError::InvalidColumns(msg) => write!(f, "Invalid column specification: {}", msg),
        }
    }
}

impl std::error::Error for ViewError {}

/// View manager for generating view DDL statements
///
/// # Examples
///
/// ```
/// use zqlz_objects::{ViewManager, ViewDialect, ViewSpec};
///
/// let manager = ViewManager::new(ViewDialect::PostgreSQL);
/// let spec = ViewSpec::new("active_users", "SELECT * FROM users WHERE active = true");
/// let sql = manager.build_create_view(&spec).unwrap();
/// assert!(sql.contains("CREATE VIEW"));
/// assert!(sql.contains("active_users"));
/// ```
pub struct ViewManager {
    dialect: ViewDialect,
}

impl ViewManager {
    /// Create a new view manager for the specified dialect
    pub fn new(dialect: ViewDialect) -> Self {
        Self { dialect }
    }

    /// Get the dialect for this manager
    pub fn dialect(&self) -> ViewDialect {
        self.dialect
    }

    /// Validate a view specification
    pub fn validate(&self, spec: &ViewSpec) -> Result<(), ViewError> {
        if spec.name.trim().is_empty() {
            return Err(ViewError::EmptyName);
        }
        if spec.query.trim().is_empty() {
            return Err(ViewError::EmptyQuery);
        }
        if spec.is_materialized && !self.dialect.supports_materialized_views() {
            return Err(ViewError::MaterializedViewNotSupported);
        }
        if spec.with_check_option.is_some() && !self.dialect.supports_check_option() {
            return Err(ViewError::CheckOptionNotSupported);
        }
        Ok(())
    }

    /// Build a CREATE VIEW statement
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_objects::{ViewManager, ViewDialect, ViewSpec, CheckOption};
    ///
    /// let manager = ViewManager::new(ViewDialect::PostgreSQL);
    ///
    /// // Simple view
    /// let spec = ViewSpec::new("v_active_users", "SELECT id, name FROM users WHERE active");
    /// let sql = manager.build_create_view(&spec).unwrap();
    /// assert_eq!(sql, "CREATE VIEW v_active_users AS\nSELECT id, name FROM users WHERE active");
    ///
    /// // View with columns and check option
    /// let spec = ViewSpec::new("v_users", "SELECT id, name FROM users")
    ///     .with_columns(vec!["user_id".into(), "user_name".into()])
    ///     .with_check_option(CheckOption::Cascaded);
    /// let sql = manager.build_create_view(&spec).unwrap();
    /// assert!(sql.contains("(user_id, user_name)"));
    /// assert!(sql.contains("WITH CASCADED CHECK OPTION"));
    /// ```
    pub fn build_create_view(&self, spec: &ViewSpec) -> Result<String, ViewError> {
        self.validate(spec)?;

        let view_type = if spec.is_materialized {
            "MATERIALIZED VIEW"
        } else {
            "VIEW"
        };

        let qualified_name = self.quote_identifier(&spec.qualified_name());

        let columns_clause = if spec.columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", spec.columns.join(", "))
        };

        let check_option_clause = match spec.with_check_option {
            Some(CheckOption::Local) => "\nWITH LOCAL CHECK OPTION",
            Some(CheckOption::Cascaded) => "\nWITH CASCADED CHECK OPTION",
            None => "",
        };

        Ok(format!(
            "CREATE {}{} AS\n{}{}",
            view_type,
            format!(" {}{}", qualified_name, columns_clause),
            spec.query.trim(),
            check_option_clause
        ))
    }

    /// Build a CREATE OR REPLACE VIEW statement (where supported)
    pub fn build_create_or_replace_view(&self, spec: &ViewSpec) -> Result<String, ViewError> {
        self.validate(spec)?;

        if spec.is_materialized {
            return Err(ViewError::MaterializedViewNotSupported);
        }

        let qualified_name = self.quote_identifier(&spec.qualified_name());

        let columns_clause = if spec.columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", spec.columns.join(", "))
        };

        let check_option_clause = match spec.with_check_option {
            Some(CheckOption::Local) => "\nWITH LOCAL CHECK OPTION",
            Some(CheckOption::Cascaded) => "\nWITH CASCADED CHECK OPTION",
            None => "",
        };

        Ok(format!(
            "CREATE OR REPLACE VIEW{}{} AS\n{}{}",
            format!(" {}{}", qualified_name, columns_clause),
            "",
            spec.query.trim(),
            check_option_clause
        ))
    }

    /// Build a DROP VIEW statement
    ///
    /// # Arguments
    /// * `name` - View name (can be schema-qualified)
    /// * `is_materialized` - Whether this is a materialized view
    /// * `if_exists` - Add IF EXISTS clause
    /// * `cascade` - Add CASCADE clause (PostgreSQL)
    pub fn build_drop_view(
        &self,
        name: &str,
        is_materialized: bool,
        if_exists: bool,
        cascade: bool,
    ) -> String {
        let view_type = if is_materialized {
            "MATERIALIZED VIEW"
        } else {
            "VIEW"
        };

        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let cascade_clause = if cascade && matches!(self.dialect, ViewDialect::PostgreSQL) {
            " CASCADE"
        } else {
            ""
        };

        let quoted_name = self.quote_identifier(name);

        format!(
            "DROP {} {}{}{}",
            view_type, if_exists_clause, quoted_name, cascade_clause
        )
    }

    /// Build a REFRESH MATERIALIZED VIEW statement (PostgreSQL only)
    ///
    /// # Arguments
    /// * `name` - Materialized view name
    /// * `concurrently` - Use CONCURRENTLY option (requires unique index)
    pub fn build_refresh_materialized_view(
        &self,
        name: &str,
        concurrently: bool,
    ) -> Option<String> {
        if !matches!(self.dialect, ViewDialect::PostgreSQL) {
            return None;
        }

        let concurrent_clause = if concurrently { "CONCURRENTLY " } else { "" };
        let quoted_name = self.quote_identifier(name);

        Some(format!(
            "REFRESH MATERIALIZED VIEW {}{}",
            concurrent_clause, quoted_name
        ))
    }

    /// Build an ALTER VIEW statement for renaming
    pub fn build_rename_view(&self, old_name: &str, new_name: &str) -> String {
        let quoted_old = self.quote_identifier(old_name);
        let quoted_new = self.quote_identifier(new_name);

        match self.dialect {
            ViewDialect::PostgreSQL => {
                format!("ALTER VIEW {} RENAME TO {}", quoted_old, quoted_new)
            }
            ViewDialect::MySQL => {
                format!("RENAME TABLE {} TO {}", quoted_old, quoted_new)
            }
            ViewDialect::SQLite => {
                format!("ALTER VIEW {} RENAME TO {}", quoted_old, quoted_new)
            }
            ViewDialect::MsSql => {
                format!("EXEC sp_rename '{}', '{}'", old_name, new_name)
            }
        }
    }

    /// Build a COMMENT ON VIEW statement (PostgreSQL only)
    pub fn build_comment(&self, name: &str, comment: Option<&str>) -> Option<String> {
        if !matches!(self.dialect, ViewDialect::PostgreSQL) {
            return None;
        }

        let quoted_name = self.quote_identifier(name);
        let comment_value = match comment {
            Some(c) => format!("'{}'", c.replace('\'', "''")),
            None => "NULL".to_string(),
        };

        Some(format!(
            "COMMENT ON VIEW {} IS {}",
            quoted_name, comment_value
        ))
    }

    /// Quote an identifier based on the dialect
    fn quote_identifier(&self, name: &str) -> String {
        if name.contains('.') {
            name.split('.')
                .map(|part| self.quote_single_identifier(part))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            self.quote_single_identifier(name)
        }
    }

    fn quote_single_identifier(&self, name: &str) -> String {
        match self.dialect {
            ViewDialect::PostgreSQL | ViewDialect::SQLite => {
                if Self::needs_quoting(name) {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    name.to_string()
                }
            }
            ViewDialect::MySQL => {
                if Self::needs_quoting(name) {
                    format!("`{}`", name.replace('`', "``"))
                } else {
                    name.to_string()
                }
            }
            ViewDialect::MsSql => {
                if Self::needs_quoting(name) {
                    format!("[{}]", name.replace(']', "]]"))
                } else {
                    name.to_string()
                }
            }
        }
    }

    fn needs_quoting(name: &str) -> bool {
        let Some(first) = name.chars().next() else {
            return true;
        };
        if !first.is_ascii_alphabetic() && first != '_' {
            return true;
        }
        name.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
            || RESERVED_KEYWORDS.contains(&name.to_uppercase().as_str())
    }
}

static RESERVED_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "TABLE",
    "VIEW",
    "INDEX",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "AS",
    "ON",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "FULL",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "DISTINCT",
    "INTO",
    "VALUES",
    "SET",
    "DEFAULT",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "CONSTRAINT",
    "UNIQUE",
    "CHECK",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IF",
    "EXISTS",
    "IN",
    "BETWEEN",
    "LIKE",
    "IS",
];
