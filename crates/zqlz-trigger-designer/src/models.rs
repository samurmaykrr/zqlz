//! Trigger design models

/// Database dialect for SQL generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatabaseDialect {
    #[default]
    Sqlite,
    Postgres,
    Mysql,
}

impl DatabaseDialect {
    /// Parse dialect from driver name string
    pub fn from_driver_name(name: &str) -> Self {
        let name_lower = name.to_lowercase();
        if name_lower.contains("postgres") {
            Self::Postgres
        } else if name_lower.contains("mysql") || name_lower.contains("mariadb") {
            Self::Mysql
        } else {
            Self::Sqlite
        }
    }
}

/// When the trigger fires relative to the event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TriggerTiming {
    #[default]
    Before,
    After,
    InsteadOf,
}

impl TriggerTiming {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
            Self::InsteadOf => "INSTEAD OF",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Before, Self::After, Self::InsteadOf]
    }

    pub fn all_for_dialect(dialect: DatabaseDialect) -> Vec<Self> {
        match dialect {
            DatabaseDialect::Sqlite => vec![Self::Before, Self::After, Self::InsteadOf],
            DatabaseDialect::Postgres => vec![Self::Before, Self::After, Self::InsteadOf],
            DatabaseDialect::Mysql => vec![Self::Before, Self::After], // MySQL doesn't support INSTEAD OF
        }
    }
}

impl std::fmt::Display for TriggerTiming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The DML event that fires the trigger
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TriggerEvent {
    #[default]
    Insert,
    Update,
    Delete,
}

impl TriggerEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Insert, Self::Update, Self::Delete]
    }
}

impl std::fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Validation errors for trigger design
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    MissingName,
    MissingTableName,
    MissingBody,
    InvalidName(String),
    NoEventSelected,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingName => write!(f, "Trigger name is required"),
            Self::MissingTableName => write!(f, "Table name is required"),
            Self::MissingBody => write!(f, "Trigger body is required"),
            Self::InvalidName(msg) => write!(f, "Invalid name: {}", msg),
            Self::NoEventSelected => write!(
                f,
                "At least one event (INSERT/UPDATE/DELETE) must be selected"
            ),
        }
    }
}

/// Trigger design model for creating/editing triggers
#[derive(Debug, Clone)]
pub struct TriggerDesign {
    /// Trigger name
    pub name: String,
    /// Table the trigger is attached to
    pub table_name: String,
    /// Schema name (optional, not used by SQLite)
    pub schema: Option<String>,
    /// Database dialect
    pub dialect: DatabaseDialect,
    /// When the trigger fires (BEFORE, AFTER, INSTEAD OF)
    pub timing: TriggerTiming,
    /// Events that fire the trigger (can be multiple for Postgres)
    pub events: Vec<TriggerEvent>,
    /// FOR EACH ROW vs FOR EACH STATEMENT (Postgres only)
    pub for_each_row: bool,
    /// WHEN condition (optional)
    pub when_condition: Option<String>,
    /// Trigger body (the SQL to execute)
    pub body: String,
    /// UPDATE OF columns (optional, for UPDATE triggers)
    pub update_columns: Vec<String>,
    /// Whether this is a new trigger (vs editing existing)
    pub is_new: bool,
    /// Comment/description
    pub comment: Option<String>,
}

impl TriggerDesign {
    /// Create a new empty trigger design
    pub fn new(dialect: DatabaseDialect) -> Self {
        Self {
            name: String::new(),
            table_name: String::new(),
            schema: None,
            dialect,
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            for_each_row: true,
            when_condition: None,
            body: Self::default_body(dialect),
            update_columns: Vec::new(),
            is_new: true,
            comment: None,
        }
    }

    /// Create from existing trigger name (for editing)
    pub fn for_editing(name: String, table_name: String, dialect: DatabaseDialect) -> Self {
        Self {
            name,
            table_name,
            schema: None,
            dialect,
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            for_each_row: true,
            when_condition: None,
            body: String::new(),
            update_columns: Vec::new(),
            is_new: false,
            comment: None,
        }
    }

    /// Default trigger body based on dialect
    fn default_body(dialect: DatabaseDialect) -> String {
        match dialect {
            DatabaseDialect::Sqlite => {
                "-- Trigger body\nBEGIN\n    -- Your SQL statements here\n    SELECT 1;\nEND".to_string()
            }
            DatabaseDialect::Postgres => {
                "-- Trigger function body\nBEGIN\n    -- Your SQL statements here\n    RETURN NEW;\nEND".to_string()
            }
            DatabaseDialect::Mysql => {
                "-- Trigger body\nBEGIN\n    -- Your SQL statements here\n    SET NEW.updated_at = NOW();\nEND".to_string()
            }
        }
    }

    /// Validate the trigger design
    pub fn validate(&self) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if self.name.trim().is_empty() {
            errors.push(ValidationError::MissingName);
        }

        if self.table_name.trim().is_empty() {
            errors.push(ValidationError::MissingTableName);
        }

        if self.body.trim().is_empty() {
            errors.push(ValidationError::MissingBody);
        }

        if self.events.is_empty() {
            errors.push(ValidationError::NoEventSelected);
        }

        // Check for invalid characters in name
        if !self.name.is_empty() {
            let valid_chars = self.name.chars().all(|c| c.is_alphanumeric() || c == '_');
            if !valid_chars {
                errors.push(ValidationError::InvalidName(
                    "Name can only contain letters, numbers, and underscores".to_string(),
                ));
            }
        }

        errors
    }

    /// Generate CREATE TRIGGER DDL
    pub fn to_ddl(&self) -> String {
        match self.dialect {
            DatabaseDialect::Sqlite => self.to_sqlite_ddl(),
            DatabaseDialect::Postgres => self.to_postgres_ddl(),
            DatabaseDialect::Mysql => self.to_mysql_ddl(),
        }
    }

    fn to_sqlite_ddl(&self) -> String {
        let mut ddl = String::new();

        // CREATE TRIGGER name
        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        // BEFORE/AFTER/INSTEAD OF
        ddl.push_str("    ");
        ddl.push_str(self.timing.as_str());
        ddl.push(' ');

        // INSERT/UPDATE/DELETE
        let event = self.events.first().unwrap_or(&TriggerEvent::Insert);
        ddl.push_str(event.as_str());

        // UPDATE OF columns
        if *event == TriggerEvent::Update && !self.update_columns.is_empty() {
            ddl.push_str(" OF ");
            ddl.push_str(
                &self
                    .update_columns
                    .iter()
                    .map(|c| self.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        // ON table
        ddl.push_str(" ON ");
        ddl.push_str(&self.quote_identifier(&self.table_name));
        ddl.push('\n');

        // FOR EACH ROW
        ddl.push_str("    FOR EACH ROW\n");

        // WHEN condition
        if let Some(ref cond) = self.when_condition {
            if !cond.trim().is_empty() {
                ddl.push_str("    WHEN (");
                ddl.push_str(cond);
                ddl.push_str(")\n");
            }
        }

        // Body
        ddl.push_str(&self.body);

        ddl
    }

    fn to_postgres_ddl(&self) -> String {
        let mut ddl = String::new();
        let func_name = format!("{}_func", self.name);

        // First create the trigger function
        ddl.push_str("-- Trigger function\n");
        ddl.push_str("CREATE OR REPLACE FUNCTION ");
        ddl.push_str(&self.quote_identifier(&func_name));
        ddl.push_str("()\n");
        ddl.push_str("RETURNS TRIGGER AS $$\n");
        ddl.push_str(&self.body);
        ddl.push_str("\n$$ LANGUAGE plpgsql;\n\n");

        // Then create the trigger
        ddl.push_str("-- Trigger\n");
        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        // BEFORE/AFTER/INSTEAD OF
        ddl.push_str("    ");
        ddl.push_str(self.timing.as_str());
        ddl.push(' ');

        // Events (Postgres supports multiple events with OR)
        let events: Vec<&str> = self.events.iter().map(|e| e.as_str()).collect();
        ddl.push_str(&events.join(" OR "));

        // ON table
        ddl.push_str(" ON ");
        if let Some(ref schema) = self.schema {
            ddl.push_str(&self.quote_identifier(schema));
            ddl.push('.');
        }
        ddl.push_str(&self.quote_identifier(&self.table_name));
        ddl.push('\n');

        // FOR EACH ROW/STATEMENT
        ddl.push_str("    FOR EACH ");
        ddl.push_str(if self.for_each_row {
            "ROW"
        } else {
            "STATEMENT"
        });
        ddl.push('\n');

        // WHEN condition
        if let Some(ref cond) = self.when_condition {
            if !cond.trim().is_empty() {
                ddl.push_str("    WHEN (");
                ddl.push_str(cond);
                ddl.push_str(")\n");
            }
        }

        // EXECUTE FUNCTION
        ddl.push_str("    EXECUTE FUNCTION ");
        ddl.push_str(&self.quote_identifier(&func_name));
        ddl.push_str("();");

        ddl
    }

    fn to_mysql_ddl(&self) -> String {
        let mut ddl = String::new();

        // MySQL requires DELIMITER for triggers with multiple statements
        ddl.push_str("DELIMITER //\n\n");

        // CREATE TRIGGER name
        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        // BEFORE/AFTER (MySQL doesn't support INSTEAD OF)
        ddl.push_str("    ");
        let timing = if self.timing == TriggerTiming::InsteadOf {
            TriggerTiming::Before // Fall back to BEFORE for MySQL
        } else {
            self.timing
        };
        ddl.push_str(timing.as_str());
        ddl.push(' ');

        // MySQL only supports single event per trigger
        let event = self.events.first().unwrap_or(&TriggerEvent::Insert);
        ddl.push_str(event.as_str());

        // ON table
        ddl.push_str(" ON ");
        ddl.push_str(&self.quote_identifier(&self.table_name));
        ddl.push('\n');

        // FOR EACH ROW (MySQL always requires this)
        ddl.push_str("    FOR EACH ROW\n");

        // Body
        ddl.push_str(&self.body);

        ddl.push_str("\n//\n\nDELIMITER ;");

        ddl
    }

    fn quote_identifier(&self, name: &str) -> String {
        match self.dialect {
            DatabaseDialect::Sqlite | DatabaseDialect::Postgres => format!("\"{}\"", name),
            DatabaseDialect::Mysql => format!("`{}`", name),
        }
    }

    /// Try to parse trigger design from SQL (best effort)
    pub fn from_sql(sql: &str, dialect: DatabaseDialect) -> Option<Self> {
        let sql_upper = sql.to_uppercase();
        let mut design = Self::new(dialect);
        design.is_new = false;

        // Extract trigger name
        if let Some(pos) = sql_upper.find("CREATE TRIGGER") {
            let after_create = &sql[pos + 14..];
            let name_end = after_create
                .find(|c: char| c.is_whitespace() || c == '"' || c == '`' || c == '[')
                .unwrap_or(after_create.len());
            let name = after_create[..name_end].trim();
            // Remove quotes if present
            design.name = name
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']')
                .to_string();
        }

        // Extract timing
        if sql_upper.contains("INSTEAD OF") {
            design.timing = TriggerTiming::InsteadOf;
        } else if sql_upper.contains("BEFORE") {
            design.timing = TriggerTiming::Before;
        } else if sql_upper.contains("AFTER") {
            design.timing = TriggerTiming::After;
        }

        // Extract events
        design.events.clear();
        if sql_upper.contains("INSERT") {
            design.events.push(TriggerEvent::Insert);
        }
        if sql_upper.contains("UPDATE") {
            design.events.push(TriggerEvent::Update);
        }
        if sql_upper.contains("DELETE") {
            design.events.push(TriggerEvent::Delete);
        }
        if design.events.is_empty() {
            design.events.push(TriggerEvent::Insert);
        }

        // Extract table name (after ON keyword)
        if let Some(pos) = sql_upper.find(" ON ") {
            let after_on = &sql[pos + 4..];
            let table_end = after_on
                .find(|c: char| c.is_whitespace() && c != ' ')
                .unwrap_or_else(|| {
                    after_on
                        .find(" FOR ")
                        .or_else(|| after_on.find(" WHEN "))
                        .or_else(|| after_on.find(" BEGIN"))
                        .unwrap_or(after_on.len())
                });
            let table = after_on[..table_end].trim();
            design.table_name = table
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']')
                .to_string();
        }

        // Extract body (everything from BEGIN to END for SQLite/MySQL)
        if let Some(begin_pos) = sql_upper.find("BEGIN") {
            design.body = sql[begin_pos..].to_string();
        }

        Some(design)
    }
}
