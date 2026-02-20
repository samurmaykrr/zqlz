//! Trigger manager implementation
//!
//! Provides functionality for creating, altering, and dropping triggers
//! across different database dialects.

use serde::{Deserialize, Serialize};

/// When the trigger fires relative to the operation
///
/// # Examples
///
/// ```
/// use zqlz_objects::TriggerTiming;
///
/// let timing = TriggerTiming::Before;
/// assert!(timing.is_before());
/// assert!(!timing.is_instead_of());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerTiming {
    /// Execute before the triggering statement
    Before,
    /// Execute after the triggering statement
    After,
    /// Execute instead of the triggering statement (views only)
    InsteadOf,
}

impl TriggerTiming {
    /// Check if this is a BEFORE trigger
    pub fn is_before(&self) -> bool {
        matches!(self, TriggerTiming::Before)
    }

    /// Check if this is an AFTER trigger
    pub fn is_after(&self) -> bool {
        matches!(self, TriggerTiming::After)
    }

    /// Check if this is an INSTEAD OF trigger
    pub fn is_instead_of(&self) -> bool {
        matches!(self, TriggerTiming::InsteadOf)
    }

    /// Convert to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        }
    }
}

impl Default for TriggerTiming {
    fn default() -> Self {
        TriggerTiming::After
    }
}

/// The DML event that fires the trigger
///
/// # Examples
///
/// ```
/// use zqlz_objects::TriggerEvent;
///
/// let event = TriggerEvent::Insert;
/// assert_eq!(event.as_sql(), "INSERT");
///
/// let events = vec![TriggerEvent::Insert, TriggerEvent::Update];
/// assert!(TriggerEvent::contains_multiple(&events));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerEvent {
    /// Trigger fires on INSERT
    Insert,
    /// Trigger fires on UPDATE
    Update,
    /// Trigger fires on DELETE
    Delete,
    /// Trigger fires on TRUNCATE (PostgreSQL only)
    Truncate,
}

impl TriggerEvent {
    /// Convert to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
            TriggerEvent::Truncate => "TRUNCATE",
        }
    }

    /// Check if a list contains multiple events
    pub fn contains_multiple(events: &[TriggerEvent]) -> bool {
        events.len() > 1
    }
}

/// Level at which the trigger executes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerLevel {
    /// Fire once per statement (default)
    Statement,
    /// Fire once per affected row
    Row,
}

impl TriggerLevel {
    /// Convert to SQL clause
    pub fn as_sql(&self) -> &'static str {
        match self {
            TriggerLevel::Statement => "FOR EACH STATEMENT",
            TriggerLevel::Row => "FOR EACH ROW",
        }
    }
}

impl Default for TriggerLevel {
    fn default() -> Self {
        TriggerLevel::Row
    }
}

/// Specification for creating a new trigger
///
/// # Examples
///
/// ```
/// use zqlz_objects::{TriggerSpec, TriggerTiming, TriggerEvent, TriggerLevel};
///
/// let spec = TriggerSpec::new("audit_insert", "users")
///     .with_timing(TriggerTiming::After)
///     .with_event(TriggerEvent::Insert)
///     .with_level(TriggerLevel::Row)
///     .with_function("audit_log");
///
/// assert_eq!(spec.name(), "audit_insert");
/// assert_eq!(spec.table(), "users");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerSpec {
    name: String,
    table: String,
    schema: Option<String>,
    timing: TriggerTiming,
    events: Vec<TriggerEvent>,
    level: TriggerLevel,
    when_condition: Option<String>,
    function_name: Option<String>,
    body: Option<String>,
    update_columns: Vec<String>,
    comment: Option<String>,
}

impl TriggerSpec {
    /// Create a new trigger specification
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            schema: None,
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            level: TriggerLevel::Row,
            when_condition: None,
            function_name: None,
            body: None,
            update_columns: Vec::new(),
            comment: None,
        }
    }

    /// Set the schema for the trigger
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the trigger timing
    pub fn with_timing(mut self, timing: TriggerTiming) -> Self {
        self.timing = timing;
        self
    }

    /// Add a single event that fires the trigger
    pub fn with_event(mut self, event: TriggerEvent) -> Self {
        self.events = vec![event];
        self
    }

    /// Add multiple events that fire the trigger
    pub fn with_events(mut self, events: Vec<TriggerEvent>) -> Self {
        self.events = events;
        self
    }

    /// Set the trigger level (statement or row)
    pub fn with_level(mut self, level: TriggerLevel) -> Self {
        self.level = level;
        self
    }

    /// Set a WHEN condition for the trigger
    pub fn with_when(mut self, condition: impl Into<String>) -> Self {
        self.when_condition = Some(condition.into());
        self
    }

    /// Set the function to execute (PostgreSQL style)
    pub fn with_function(mut self, function_name: impl Into<String>) -> Self {
        self.function_name = Some(function_name.into());
        self
    }

    /// Set the trigger body (MySQL/SQLite/SQL Server style)
    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Limit UPDATE triggers to specific columns
    pub fn with_update_columns(mut self, columns: Vec<String>) -> Self {
        self.update_columns = columns;
        self
    }

    /// Set a comment for the trigger
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Get the trigger name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get the schema (if set)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the trigger timing
    pub fn timing(&self) -> TriggerTiming {
        self.timing
    }

    /// Get the events
    pub fn events(&self) -> &[TriggerEvent] {
        &self.events
    }

    /// Get the trigger level
    pub fn level(&self) -> TriggerLevel {
        self.level
    }

    /// Get the WHEN condition (if set)
    pub fn when_condition(&self) -> Option<&str> {
        self.when_condition.as_deref()
    }

    /// Get the function name (if set)
    pub fn function_name(&self) -> Option<&str> {
        self.function_name.as_deref()
    }

    /// Get the body (if set)
    pub fn body(&self) -> Option<&str> {
        self.body.as_deref()
    }

    /// Get the update columns (if set)
    pub fn update_columns(&self) -> &[String] {
        &self.update_columns
    }

    /// Get the comment (if set)
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Get the fully qualified table name (schema.table or just table)
    pub fn qualified_table(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.table),
            None => self.table.clone(),
        }
    }
}

/// Database dialect for trigger generation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerDialect {
    /// PostgreSQL syntax (uses functions)
    PostgreSQL,
    /// MySQL/MariaDB syntax (inline body with BEGIN/END)
    MySQL,
    /// SQLite syntax (inline body or select)
    SQLite,
    /// Microsoft SQL Server syntax (inline body)
    MsSql,
}

impl TriggerDialect {
    /// Check if this dialect supports BEFORE triggers
    pub fn supports_before(&self) -> bool {
        !matches!(self, TriggerDialect::MsSql)
    }

    /// Check if this dialect supports INSTEAD OF triggers
    pub fn supports_instead_of(&self) -> bool {
        matches!(
            self,
            TriggerDialect::PostgreSQL | TriggerDialect::SQLite | TriggerDialect::MsSql
        )
    }

    /// Check if this dialect requires a function reference (vs inline body)
    pub fn requires_function(&self) -> bool {
        matches!(self, TriggerDialect::PostgreSQL)
    }

    /// Check if this dialect supports TRUNCATE triggers
    pub fn supports_truncate(&self) -> bool {
        matches!(self, TriggerDialect::PostgreSQL)
    }

    /// Check if this dialect supports FOR EACH STATEMENT
    pub fn supports_statement_level(&self) -> bool {
        matches!(self, TriggerDialect::PostgreSQL)
    }

    /// Check if this dialect supports WHEN conditions
    pub fn supports_when_condition(&self) -> bool {
        matches!(self, TriggerDialect::PostgreSQL | TriggerDialect::SQLite)
    }

    /// Check if this dialect supports UPDATE OF columns
    pub fn supports_update_columns(&self) -> bool {
        matches!(self, TriggerDialect::PostgreSQL | TriggerDialect::SQLite)
    }
}

/// Error type for trigger operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerError {
    /// Trigger name is empty
    EmptyName,
    /// Table name is empty
    EmptyTable,
    /// No events specified
    NoEvents,
    /// BEFORE triggers not supported by this dialect
    BeforeNotSupported,
    /// INSTEAD OF triggers not supported by this dialect
    InsteadOfNotSupported,
    /// TRUNCATE triggers not supported by this dialect
    TruncateNotSupported,
    /// Statement-level triggers not supported by this dialect
    StatementLevelNotSupported,
    /// WHEN condition not supported by this dialect
    WhenConditionNotSupported,
    /// UPDATE OF columns not supported by this dialect
    UpdateColumnsNotSupported,
    /// Missing function name (PostgreSQL requires it)
    MissingFunction,
    /// Missing trigger body (non-PostgreSQL requires it)
    MissingBody,
}

impl std::fmt::Display for TriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerError::EmptyName => write!(f, "Trigger name cannot be empty"),
            TriggerError::EmptyTable => write!(f, "Table name cannot be empty"),
            TriggerError::NoEvents => write!(f, "At least one trigger event must be specified"),
            TriggerError::BeforeNotSupported => {
                write!(f, "BEFORE triggers are not supported by this dialect")
            }
            TriggerError::InsteadOfNotSupported => {
                write!(f, "INSTEAD OF triggers are not supported by this dialect")
            }
            TriggerError::TruncateNotSupported => {
                write!(f, "TRUNCATE triggers are not supported by this dialect")
            }
            TriggerError::StatementLevelNotSupported => {
                write!(
                    f,
                    "Statement-level triggers are not supported by this dialect"
                )
            }
            TriggerError::WhenConditionNotSupported => {
                write!(
                    f,
                    "WHEN conditions on triggers are not supported by this dialect"
                )
            }
            TriggerError::UpdateColumnsNotSupported => {
                write!(f, "UPDATE OF columns is not supported by this dialect")
            }
            TriggerError::MissingFunction => {
                write!(f, "PostgreSQL triggers require a function name")
            }
            TriggerError::MissingBody => {
                write!(f, "This dialect requires a trigger body")
            }
        }
    }
}

impl std::error::Error for TriggerError {}

/// Trigger manager for generating trigger DDL statements
///
/// # Examples
///
/// ```
/// use zqlz_objects::{TriggerManager, TriggerDialect, TriggerSpec, TriggerTiming, TriggerEvent};
///
/// let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
/// let spec = TriggerSpec::new("audit_insert", "users")
///     .with_timing(TriggerTiming::After)
///     .with_event(TriggerEvent::Insert)
///     .with_function("audit_log_func");
///
/// let sql = manager.build_create_trigger(&spec).unwrap();
/// assert!(sql.contains("CREATE TRIGGER"));
/// assert!(sql.contains("audit_insert"));
/// ```
pub struct TriggerManager {
    dialect: TriggerDialect,
}

impl TriggerManager {
    /// Create a new trigger manager for the specified dialect
    pub fn new(dialect: TriggerDialect) -> Self {
        Self { dialect }
    }

    /// Get the dialect for this manager
    pub fn dialect(&self) -> TriggerDialect {
        self.dialect
    }

    /// Validate a trigger specification
    pub fn validate(&self, spec: &TriggerSpec) -> Result<(), TriggerError> {
        if spec.name.trim().is_empty() {
            return Err(TriggerError::EmptyName);
        }
        if spec.table.trim().is_empty() {
            return Err(TriggerError::EmptyTable);
        }
        if spec.events.is_empty() {
            return Err(TriggerError::NoEvents);
        }
        if spec.timing.is_before() && !self.dialect.supports_before() {
            return Err(TriggerError::BeforeNotSupported);
        }
        if spec.timing.is_instead_of() && !self.dialect.supports_instead_of() {
            return Err(TriggerError::InsteadOfNotSupported);
        }
        if spec.events.contains(&TriggerEvent::Truncate) && !self.dialect.supports_truncate() {
            return Err(TriggerError::TruncateNotSupported);
        }
        if spec.level == TriggerLevel::Statement && !self.dialect.supports_statement_level() {
            return Err(TriggerError::StatementLevelNotSupported);
        }
        if spec.when_condition.is_some() && !self.dialect.supports_when_condition() {
            return Err(TriggerError::WhenConditionNotSupported);
        }
        if !spec.update_columns.is_empty() && !self.dialect.supports_update_columns() {
            return Err(TriggerError::UpdateColumnsNotSupported);
        }
        if self.dialect.requires_function() && spec.function_name.is_none() {
            return Err(TriggerError::MissingFunction);
        }
        if !self.dialect.requires_function() && spec.body.is_none() {
            return Err(TriggerError::MissingBody);
        }
        Ok(())
    }

    /// Build a CREATE TRIGGER statement
    pub fn build_create_trigger(&self, spec: &TriggerSpec) -> Result<String, TriggerError> {
        self.validate(spec)?;

        match self.dialect {
            TriggerDialect::PostgreSQL => self.build_postgres_trigger(spec),
            TriggerDialect::MySQL => self.build_mysql_trigger(spec),
            TriggerDialect::SQLite => self.build_sqlite_trigger(spec),
            TriggerDialect::MsSql => self.build_mssql_trigger(spec),
        }
    }

    fn build_postgres_trigger(&self, spec: &TriggerSpec) -> Result<String, TriggerError> {
        let timing = spec.timing.as_sql();
        let events = self.build_events_clause(spec);
        let table = self.quote_identifier(&spec.qualified_table());
        let level = spec.level.as_sql();
        let function = spec
            .function_name
            .as_ref()
            .ok_or(TriggerError::MissingFunction)?;

        let when_clause = spec
            .when_condition
            .as_ref()
            .map(|c| format!("\n    WHEN ({})", c))
            .unwrap_or_default();

        Ok(format!(
            "CREATE TRIGGER {}\n    {} {}\n    ON {}\n    {}{}\n    EXECUTE FUNCTION {}()",
            self.quote_identifier(spec.name()),
            timing,
            events,
            table,
            level,
            when_clause,
            function
        ))
    }

    fn build_mysql_trigger(&self, spec: &TriggerSpec) -> Result<String, TriggerError> {
        let timing = spec.timing.as_sql();
        let event = spec.events.first().ok_or(TriggerError::NoEvents)?.as_sql();
        let table = self.quote_identifier(&spec.qualified_table());
        let body = spec.body.as_ref().ok_or(TriggerError::MissingBody)?;

        Ok(format!(
            "CREATE TRIGGER {}\n{} {} ON {}\nFOR EACH ROW\nBEGIN\n{}\nEND",
            self.quote_identifier(spec.name()),
            timing,
            event,
            table,
            body
        ))
    }

    fn build_sqlite_trigger(&self, spec: &TriggerSpec) -> Result<String, TriggerError> {
        let timing = spec.timing.as_sql();
        let events = self.build_events_clause(spec);
        let table = self.quote_identifier(&spec.qualified_table());
        let body = spec.body.as_ref().ok_or(TriggerError::MissingBody)?;

        let when_clause = spec
            .when_condition
            .as_ref()
            .map(|c| format!("\n    WHEN {}", c))
            .unwrap_or_default();

        Ok(format!(
            "CREATE TRIGGER {}\n    {} {}\n    ON {}\n    FOR EACH ROW{}\nBEGIN\n{}\nEND",
            self.quote_identifier(spec.name()),
            timing,
            events,
            table,
            when_clause,
            body
        ))
    }

    fn build_mssql_trigger(&self, spec: &TriggerSpec) -> Result<String, TriggerError> {
        let events = spec
            .events
            .iter()
            .map(|e| e.as_sql())
            .collect::<Vec<_>>()
            .join(", ");
        let table = self.quote_identifier(&spec.qualified_table());
        let body = spec.body.as_ref().ok_or(TriggerError::MissingBody)?;

        let timing_clause = if spec.timing.is_instead_of() {
            "INSTEAD OF"
        } else {
            "AFTER"
        };

        Ok(format!(
            "CREATE TRIGGER {}\n    ON {}\n    {} {}\nAS\nBEGIN\n{}\nEND",
            self.quote_identifier(spec.name()),
            table,
            timing_clause,
            events,
            body
        ))
    }

    fn build_events_clause(&self, spec: &TriggerSpec) -> String {
        let mut parts = Vec::new();

        for event in &spec.events {
            if *event == TriggerEvent::Update && !spec.update_columns.is_empty() {
                parts.push(format!("UPDATE OF {}", spec.update_columns.join(", ")));
            } else {
                parts.push(event.as_sql().to_string());
            }
        }

        parts.join(" OR ")
    }

    /// Build a DROP TRIGGER statement
    ///
    /// # Arguments
    /// * `name` - Trigger name
    /// * `table` - Table name (required for some dialects)
    /// * `schema` - Schema name (optional)
    /// * `if_exists` - Add IF EXISTS clause
    pub fn build_drop_trigger(
        &self,
        name: &str,
        table: Option<&str>,
        schema: Option<&str>,
        if_exists: bool,
    ) -> String {
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };

        match self.dialect {
            TriggerDialect::PostgreSQL => {
                let qualified_table = match (schema, table) {
                    (Some(s), Some(t)) => format!("{}.{}", s, t),
                    (None, Some(t)) => t.to_string(),
                    _ => "".to_string(),
                };
                if qualified_table.is_empty() {
                    format!(
                        "DROP TRIGGER {}{}",
                        if_exists_clause,
                        self.quote_identifier(name)
                    )
                } else {
                    format!(
                        "DROP TRIGGER {}{} ON {}",
                        if_exists_clause,
                        self.quote_identifier(name),
                        self.quote_identifier(&qualified_table)
                    )
                }
            }
            TriggerDialect::MySQL => {
                let qualified_name = match schema {
                    Some(s) => format!("{}.{}", s, name),
                    None => name.to_string(),
                };
                format!(
                    "DROP TRIGGER {}{}",
                    if_exists_clause,
                    self.quote_identifier(&qualified_name)
                )
            }
            TriggerDialect::SQLite => {
                let qualified_name = match schema {
                    Some(s) => format!("{}.{}", s, name),
                    None => name.to_string(),
                };
                format!(
                    "DROP TRIGGER {}{}",
                    if_exists_clause,
                    self.quote_identifier(&qualified_name)
                )
            }
            TriggerDialect::MsSql => {
                let qualified_name = match schema {
                    Some(s) => format!("{}.{}", s, name),
                    None => name.to_string(),
                };
                if if_exists {
                    format!(
                        "IF OBJECT_ID('{}', 'TR') IS NOT NULL DROP TRIGGER {}",
                        qualified_name,
                        self.quote_identifier(&qualified_name)
                    )
                } else {
                    format!("DROP TRIGGER {}", self.quote_identifier(&qualified_name))
                }
            }
        }
    }

    /// Build an ALTER TRIGGER ENABLE/DISABLE statement
    ///
    /// Returns None for dialects that don't support this operation
    pub fn build_enable_disable(
        &self,
        name: &str,
        table: Option<&str>,
        schema: Option<&str>,
        enable: bool,
    ) -> Option<String> {
        let action = if enable { "ENABLE" } else { "DISABLE" };

        match self.dialect {
            TriggerDialect::PostgreSQL => {
                let qualified_table = match (schema, table) {
                    (Some(s), Some(t)) => format!("{}.{}", s, t),
                    (None, Some(t)) => t.to_string(),
                    _ => return None,
                };
                Some(format!(
                    "ALTER TABLE {} {} TRIGGER {}",
                    self.quote_identifier(&qualified_table),
                    action,
                    self.quote_identifier(name)
                ))
            }
            TriggerDialect::MsSql => {
                let qualified_name = match schema {
                    Some(s) => format!("{}.{}", s, name),
                    None => name.to_string(),
                };
                let qualified_table = match (schema, table) {
                    (Some(s), Some(t)) => format!("{}.{}", s, t),
                    (None, Some(t)) => t.to_string(),
                    _ => return None,
                };
                Some(format!(
                    "{} TRIGGER {} ON {}",
                    action,
                    self.quote_identifier(&qualified_name),
                    self.quote_identifier(&qualified_table)
                ))
            }
            _ => None,
        }
    }

    /// Build a COMMENT ON TRIGGER statement (PostgreSQL only)
    pub fn build_comment(&self, name: &str, table: &str, comment: Option<&str>) -> Option<String> {
        if !matches!(self.dialect, TriggerDialect::PostgreSQL) {
            return None;
        }

        let comment_value = match comment {
            Some(c) => format!("'{}'", c.replace('\'', "''")),
            None => "NULL".to_string(),
        };

        Some(format!(
            "COMMENT ON TRIGGER {} ON {} IS {}",
            self.quote_identifier(name),
            self.quote_identifier(table),
            comment_value
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
            TriggerDialect::PostgreSQL | TriggerDialect::SQLite => {
                if Self::needs_quoting(name) {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    name.to_string()
                }
            }
            TriggerDialect::MySQL => {
                if Self::needs_quoting(name) {
                    format!("`{}`", name.replace('`', "``"))
                } else {
                    name.to_string()
                }
            }
            TriggerDialect::MsSql => {
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
    "TRIGGER",
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
    "BEGIN",
    "AFTER",
    "BEFORE",
    "FOR",
    "EACH",
    "ROW",
    "STATEMENT",
];
