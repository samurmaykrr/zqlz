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

    pub fn timings(self) -> Vec<TriggerTiming> {
        match self {
            Self::Sqlite | Self::Postgres => {
                vec![
                    TriggerTiming::Before,
                    TriggerTiming::After,
                    TriggerTiming::InsteadOf,
                ]
            }
            Self::Mysql => vec![TriggerTiming::Before, TriggerTiming::After],
        }
    }

    pub fn events(self) -> Vec<TriggerEvent> {
        match self {
            Self::Postgres => vec![
                TriggerEvent::Insert,
                TriggerEvent::Update,
                TriggerEvent::Delete,
                TriggerEvent::Truncate,
            ],
            Self::Sqlite | Self::Mysql => {
                vec![
                    TriggerEvent::Insert,
                    TriggerEvent::Update,
                    TriggerEvent::Delete,
                ]
            }
        }
    }

    pub fn supports_multi_event(self) -> bool {
        matches!(self, Self::Postgres)
    }

    pub fn supports_statement_level(self) -> bool {
        matches!(self, Self::Postgres)
    }

    pub fn supports_when_condition(self) -> bool {
        matches!(self, Self::Postgres | Self::Sqlite)
    }

    pub fn supports_update_columns(self) -> bool {
        matches!(self, Self::Postgres | Self::Sqlite)
    }

    pub fn uses_trigger_function(self) -> bool {
        matches!(self, Self::Postgres)
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
        dialect.timings()
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
    Truncate,
}

impl TriggerEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Insert, Self::Update, Self::Delete, Self::Truncate]
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
    MissingFunctionName,
    InvalidName(String),
    NoEventSelected,
    UnsupportedTiming(TriggerTiming),
    UnsupportedEvent(TriggerEvent),
    MultiEventNotSupported,
    StatementLevelNotSupported,
    WhenConditionNotSupported,
    UpdateColumnsNotSupported,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingName => write!(f, "Trigger name is required"),
            Self::MissingTableName => write!(f, "Table name is required"),
            Self::MissingBody => write!(f, "Trigger body is required"),
            Self::MissingFunctionName => write!(f, "Function name is required"),
            Self::InvalidName(msg) => write!(f, "Invalid name: {}", msg),
            Self::NoEventSelected => write!(
                f,
                "At least one event (INSERT/UPDATE/DELETE) must be selected"
            ),
            Self::UnsupportedTiming(timing) => {
                write!(f, "{} triggers are not supported by this dialect", timing)
            }
            Self::UnsupportedEvent(event) => {
                write!(f, "{} triggers are not supported by this dialect", event)
            }
            Self::MultiEventNotSupported => {
                write!(f, "This dialect supports only one trigger event")
            }
            Self::StatementLevelNotSupported => {
                write!(
                    f,
                    "Statement-level triggers are not supported by this dialect"
                )
            }
            Self::WhenConditionNotSupported => {
                write!(f, "WHEN conditions are not supported by this dialect")
            }
            Self::UpdateColumnsNotSupported => {
                write!(f, "UPDATE OF columns are not supported by this dialect")
            }
        }
    }
}

/// Trigger design model for creating/editing triggers
#[derive(Debug, Clone)]
pub struct TriggerDesign {
    /// Trigger name
    pub name: String,
    /// Table or view the trigger is attached to
    pub table_name: String,
    /// Schema name (optional, not used by SQLite)
    pub schema: Option<String>,
    /// Database dialect
    pub dialect: DatabaseDialect,
    /// Whether the trigger is enabled. Currently persisted for UI state.
    pub enabled: bool,
    /// Target type shown by the designer, e.g. TABLE or VIEW.
    pub trigger_type: String,
    /// When the trigger fires (BEFORE, AFTER, INSTEAD OF)
    pub timing: TriggerTiming,
    /// Events that fire the trigger (can be multiple for Postgres)
    pub events: Vec<TriggerEvent>,
    /// FOR EACH ROW vs FOR EACH STATEMENT (Postgres only)
    pub for_each_row: bool,
    /// WHEN condition (optional)
    pub when_condition: Option<String>,
    /// Trigger function schema for PostgreSQL
    pub function_schema: Option<String>,
    /// Trigger function name for PostgreSQL
    pub function_name: String,
    /// Trigger function arguments for PostgreSQL
    pub function_arguments: String,
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
            enabled: true,
            trigger_type: "TABLE".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            for_each_row: true,
            when_condition: None,
            function_schema: None,
            function_name: String::new(),
            function_arguments: String::new(),
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
            enabled: true,
            trigger_type: "TABLE".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            for_each_row: true,
            when_condition: None,
            function_schema: None,
            function_name: String::new(),
            function_arguments: String::new(),
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
                "BEGIN\n    -- Your SQL statements here\n    SELECT 1;\nEND".to_string()
            }
            DatabaseDialect::Postgres => {
                "BEGIN\n    -- Your SQL statements here\n    RETURN NEW;\nEND".to_string()
            }
            DatabaseDialect::Mysql => {
                "BEGIN\n    -- Your SQL statements here\n    SET NEW.updated_at = NOW();\nEND"
                    .to_string()
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

        if self.dialect.uses_trigger_function() && self.function_name.trim().is_empty() {
            errors.push(ValidationError::MissingFunctionName);
        }

        if self.events.is_empty() {
            errors.push(ValidationError::NoEventSelected);
        }

        if !self.dialect.timings().contains(&self.timing) {
            errors.push(ValidationError::UnsupportedTiming(self.timing));
        }

        for event in &self.events {
            if !self.dialect.events().contains(event) {
                errors.push(ValidationError::UnsupportedEvent(*event));
            }
        }

        if self.events.len() > 1 && !self.dialect.supports_multi_event() {
            errors.push(ValidationError::MultiEventNotSupported);
        }

        if !self.for_each_row && !self.dialect.supports_statement_level() {
            errors.push(ValidationError::StatementLevelNotSupported);
        }

        if self
            .when_condition
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && !self.dialect.supports_when_condition()
        {
            errors.push(ValidationError::WhenConditionNotSupported);
        }

        if !self.update_columns.is_empty() && !self.dialect.supports_update_columns() {
            errors.push(ValidationError::UpdateColumnsNotSupported);
        }

        if !self.name.is_empty() && !is_simple_identifier(&self.name) {
            errors.push(ValidationError::InvalidName(
                "Name can only contain letters, numbers, and underscores".to_string(),
            ));
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

        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        ddl.push_str("    ");
        ddl.push_str(self.timing.as_str());
        ddl.push(' ');

        let event = self.events.first().unwrap_or(&TriggerEvent::Insert);
        ddl.push_str(event.as_str());

        if *event == TriggerEvent::Update && !self.update_columns.is_empty() {
            ddl.push_str(" OF ");
            ddl.push_str(
                &self
                    .update_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        ddl.push_str(" ON ");
        ddl.push_str(&self.qualified_table());
        ddl.push('\n');

        ddl.push_str("    FOR EACH ROW\n");

        if let Some(ref condition) = self.when_condition
            && !condition.trim().is_empty()
        {
            ddl.push_str("    WHEN (");
            ddl.push_str(condition.trim());
            ddl.push_str(")\n");
        }

        ddl.push_str(self.body.trim());
        ddl
    }

    fn to_postgres_ddl(&self) -> String {
        let mut ddl = String::new();
        let function_name = if self.function_name.trim().is_empty() {
            format!("{}_func", self.name)
        } else {
            self.function_name.trim().to_string()
        };
        let function = self.qualified_function(&function_name);
        let arguments = self.function_arguments.trim();

        ddl.push_str("-- Trigger function\n");
        ddl.push_str("CREATE OR REPLACE FUNCTION ");
        ddl.push_str(&function);
        ddl.push('(');
        ddl.push_str(arguments);
        ddl.push_str(")\nRETURNS trigger AS $$\n");
        ddl.push_str(self.body.trim());
        ddl.push_str("\n$$ LANGUAGE plpgsql;\n\n");

        ddl.push_str("-- Trigger\n");
        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        ddl.push_str("    ");
        ddl.push_str(self.timing.as_str());
        ddl.push(' ');

        ddl.push_str(&self.events_clause());
        ddl.push_str(" ON ");
        ddl.push_str(&self.qualified_table());
        ddl.push('\n');

        ddl.push_str("    FOR EACH ");
        ddl.push_str(if self.for_each_row {
            "ROW"
        } else {
            "STATEMENT"
        });
        ddl.push('\n');

        if let Some(ref condition) = self.when_condition
            && !condition.trim().is_empty()
        {
            ddl.push_str("    WHEN (");
            ddl.push_str(condition.trim());
            ddl.push_str(")\n");
        }

        ddl.push_str("    EXECUTE FUNCTION ");
        ddl.push_str(&function);
        ddl.push('(');
        ddl.push_str(arguments);
        ddl.push_str(");");

        if let Some(ref comment) = self.comment
            && !comment.trim().is_empty()
        {
            ddl.push_str("\n\nCOMMENT ON TRIGGER ");
            ddl.push_str(&self.quote_identifier(&self.name));
            ddl.push_str(" ON ");
            ddl.push_str(&self.qualified_table());
            ddl.push_str(" IS '");
            ddl.push_str(&comment.replace('\'', "''"));
            ddl.push_str("';");
        }

        ddl
    }

    fn to_mysql_ddl(&self) -> String {
        let mut ddl = String::new();

        ddl.push_str("DELIMITER //\n\n");
        ddl.push_str("CREATE TRIGGER ");
        ddl.push_str(&self.quote_identifier(&self.name));
        ddl.push('\n');

        ddl.push_str("    ");
        ddl.push_str(self.timing.as_str());
        ddl.push(' ');

        let event = self.events.first().unwrap_or(&TriggerEvent::Insert);
        ddl.push_str(event.as_str());

        ddl.push_str(" ON ");
        ddl.push_str(&self.qualified_table());
        ddl.push('\n');

        ddl.push_str("    FOR EACH ROW\n");
        ddl.push_str(self.body.trim());
        ddl.push_str("\n//\n\nDELIMITER ;");

        ddl
    }

    fn events_clause(&self) -> String {
        self.events
            .iter()
            .map(|event| {
                if *event == TriggerEvent::Update && !self.update_columns.is_empty() {
                    format!(
                        "UPDATE OF {}",
                        self.update_columns
                            .iter()
                            .map(|column| self.quote_identifier(column))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    event.as_str().to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn qualified_table(&self) -> String {
        if let Some(ref schema) = self.schema
            && !schema.trim().is_empty()
        {
            return format!(
                "{}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(&self.table_name)
            );
        }
        self.quote_identifier(&self.table_name)
    }

    fn qualified_function(&self, function_name: &str) -> String {
        if let Some(ref schema) = self.function_schema
            && !schema.trim().is_empty()
        {
            return format!(
                "{}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(function_name)
            );
        }
        self.quote_identifier(function_name)
    }

    fn quote_identifier(&self, name: &str) -> String {
        let quote = match self.dialect {
            DatabaseDialect::Sqlite | DatabaseDialect::Postgres => '"',
            DatabaseDialect::Mysql => '`',
        };
        let escaped = match self.dialect {
            DatabaseDialect::Sqlite | DatabaseDialect::Postgres => name.replace('"', "\"\""),
            DatabaseDialect::Mysql => name.replace('`', "``"),
        };
        format!("{quote}{escaped}{quote}")
    }

    /// Try to parse trigger design from SQL (best effort)
    pub fn from_sql(sql: &str, dialect: DatabaseDialect) -> Option<Self> {
        let sql_upper = sql.to_uppercase();
        let mut design = Self::new(dialect);
        design.is_new = false;

        if let Some(name) = token_after(&sql_upper, sql, "CREATE TRIGGER") {
            design.name = clean_identifier(name);
        }

        if sql_upper.contains("INSTEAD OF") {
            design.timing = TriggerTiming::InsteadOf;
        } else if sql_upper.contains("BEFORE") {
            design.timing = TriggerTiming::Before;
        } else if sql_upper.contains("AFTER") {
            design.timing = TriggerTiming::After;
        }

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
        if sql_upper.contains("TRUNCATE") {
            design.events.push(TriggerEvent::Truncate);
        }
        if design.events.is_empty() {
            design.events.push(TriggerEvent::Insert);
        }

        if let Some(table) = token_after(&sql_upper, sql, " ON ") {
            let table = clean_identifier(table);
            if let Some((schema, table)) = table.split_once('.') {
                design.schema = Some(clean_identifier(schema));
                design.table_name = clean_identifier(table);
            } else {
                design.table_name = table;
            }
        }

        design.for_each_row = !sql_upper.contains("FOR EACH STATEMENT");

        if let Some(function_pos) = sql_upper.find("EXECUTE FUNCTION") {
            let function = sql[function_pos + "EXECUTE FUNCTION".len()..]
                .trim()
                .trim_end_matches(';')
                .trim();
            let (function_name, arguments) = function
                .split_once('(')
                .map(|(name, args)| (name, args.trim_end_matches(')')))
                .unwrap_or((function, ""));
            let function_name = clean_identifier(function_name);
            if let Some((schema, function_name)) = function_name.split_once('.') {
                design.function_schema = Some(clean_identifier(schema));
                design.function_name = clean_identifier(function_name);
            } else {
                design.function_name = function_name;
            }
            design.function_arguments = arguments.trim().to_string();
        }

        if let Some(begin_pos) = sql_upper.find("BEGIN") {
            let body_end = sql_upper[begin_pos..]
                .find("$$ LANGUAGE")
                .map(|offset| begin_pos + offset)
                .unwrap_or(sql.len());
            design.body = sql[begin_pos..body_end].trim().to_string();
        }

        Some(design)
    }
}

fn is_simple_identifier(name: &str) -> bool {
    let Some(first) = name.chars().next() else {
        return false;
    };
    (first.is_alphabetic() || first == '_') && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn token_after<'a>(sql_upper: &str, sql: &'a str, needle: &str) -> Option<&'a str> {
    let pos = sql_upper.find(needle)?;
    let after = &sql[pos + needle.len()..];
    let after = after.trim_start();
    let mut in_quote = false;
    let mut quote_char = '\0';
    for (index, char) in after.char_indices() {
        if in_quote {
            if char == quote_char {
                in_quote = false;
            }
            continue;
        }
        if matches!(char, '"' | '`' | '[') {
            in_quote = true;
            quote_char = if char == '[' { ']' } else { char };
            continue;
        }
        if char.is_whitespace() || char == '(' {
            return Some(&after[..index]);
        }
    }
    Some(after)
}

fn clean_identifier(identifier: &str) -> String {
    identifier
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_design(dialect: DatabaseDialect) -> TriggerDesign {
        let mut design = TriggerDesign::new(dialect);
        design.name = "audit_orders".to_string();
        design.table_name = "orders".to_string();
        design.body = "BEGIN\n    RETURN NEW;\nEND".to_string();
        design.function_name = "audit_order_change".to_string();
        design
    }

    #[test]
    fn postgres_ddl_uses_function_and_dialect_features() {
        let mut design = base_design(DatabaseDialect::Postgres);
        design.schema = Some("public".to_string());
        design.function_schema = Some("audit".to_string());
        design.function_arguments = "42".to_string();
        design.events = vec![
            TriggerEvent::Insert,
            TriggerEvent::Update,
            TriggerEvent::Truncate,
        ];
        design.update_columns = vec!["status".to_string(), "total".to_string()];
        design.for_each_row = false;
        design.when_condition = Some("OLD.status IS DISTINCT FROM NEW.status".to_string());
        design.comment = Some("audit trigger".to_string());

        let ddl = design.to_ddl();

        assert!(ddl.contains("CREATE OR REPLACE FUNCTION \"audit\".\"audit_order_change\"(42)"));
        assert!(ddl.contains("INSERT OR UPDATE OF \"status\", \"total\" OR TRUNCATE"));
        assert!(ddl.contains("ON \"public\".\"orders\""));
        assert!(ddl.contains("FOR EACH STATEMENT"));
        assert!(ddl.contains("WHEN (OLD.status IS DISTINCT FROM NEW.status)"));
        assert!(ddl.contains("EXECUTE FUNCTION \"audit\".\"audit_order_change\"(42);"));
        assert!(ddl.contains(
            "COMMENT ON TRIGGER \"audit_orders\" ON \"public\".\"orders\" IS 'audit trigger';"
        ));
    }

    #[test]
    fn sqlite_ddl_supports_update_columns_and_when() {
        let mut design = base_design(DatabaseDialect::Sqlite);
        design.events = vec![TriggerEvent::Update];
        design.update_columns = vec!["status".to_string()];
        design.when_condition = Some("OLD.status != NEW.status".to_string());
        design.body = "BEGIN\n    SELECT 1;\nEND".to_string();

        let ddl = design.to_ddl();

        assert!(ddl.contains("UPDATE OF \"status\" ON \"orders\""));
        assert!(ddl.contains("WHEN (OLD.status != NEW.status)"));
        assert!(ddl.contains("BEGIN\n    SELECT 1;\nEND"));
    }

    #[test]
    fn mysql_ddl_uses_single_event_and_delimiters() {
        let mut design = base_design(DatabaseDialect::Mysql);
        design.events = vec![TriggerEvent::Delete];
        design.body = "BEGIN\n    SET @deleted = 1;\nEND".to_string();

        let ddl = design.to_ddl();

        assert!(ddl.starts_with("DELIMITER //"));
        assert!(ddl.contains("CREATE TRIGGER `audit_orders`\n    AFTER DELETE ON `orders`"));
        assert!(ddl.contains("FOR EACH ROW"));
        assert!(ddl.ends_with("DELIMITER ;"));
        assert!(!ddl.contains("WHEN"));
        assert!(!ddl.contains("UPDATE OF"));
    }

    #[test]
    fn validation_rejects_unsupported_options() {
        let mut mysql = base_design(DatabaseDialect::Mysql);
        mysql.timing = TriggerTiming::InsteadOf;
        mysql.events = vec![TriggerEvent::Insert, TriggerEvent::Update];
        mysql.when_condition = Some("OLD.id IS NOT NULL".to_string());

        let errors = mysql.validate();

        assert!(errors.contains(&ValidationError::UnsupportedTiming(
            TriggerTiming::InsteadOf
        )));
        assert!(errors.contains(&ValidationError::MultiEventNotSupported));
        assert!(errors.contains(&ValidationError::WhenConditionNotSupported));

        let mut sqlite = base_design(DatabaseDialect::Sqlite);
        sqlite.events = vec![TriggerEvent::Insert, TriggerEvent::Update];
        assert!(
            sqlite
                .validate()
                .contains(&ValidationError::MultiEventNotSupported)
        );

        let mut postgres = base_design(DatabaseDialect::Postgres);
        postgres.function_name.clear();
        assert!(
            postgres
                .validate()
                .contains(&ValidationError::MissingFunctionName)
        );
    }

    #[test]
    fn parses_postgres_trigger_definition() {
        let sql = r#"CREATE TRIGGER trg_order_audit AFTER INSERT OR UPDATE ON public.orders FOR EACH ROW EXECUTE FUNCTION audit.log_row_change('orders');"#;
        let design = TriggerDesign::from_sql(sql, DatabaseDialect::Postgres).unwrap();

        assert_eq!(design.name, "trg_order_audit");
        assert_eq!(design.schema.as_deref(), Some("public"));
        assert_eq!(design.table_name, "orders");
        assert_eq!(design.function_schema.as_deref(), Some("audit"));
        assert_eq!(design.function_name, "log_row_change");
        assert_eq!(design.function_arguments, "'orders'");
        assert!(design.events.contains(&TriggerEvent::Insert));
        assert!(design.events.contains(&TriggerEvent::Update));
    }

    #[test]
    fn parses_sqlite_and_mysql_trigger_definitions() {
        let sqlite = TriggerDesign::from_sql(
            "CREATE TRIGGER trg AFTER UPDATE ON orders BEGIN SELECT 1; END",
            DatabaseDialect::Sqlite,
        )
        .unwrap();
        assert_eq!(sqlite.name, "trg");
        assert_eq!(sqlite.table_name, "orders");
        assert_eq!(sqlite.events, vec![TriggerEvent::Update]);

        let mysql = TriggerDesign::from_sql(
            "CREATE TRIGGER trg BEFORE DELETE ON `orders` FOR EACH ROW BEGIN SET @x = 1; END",
            DatabaseDialect::Mysql,
        )
        .unwrap();
        assert_eq!(mysql.name, "trg");
        assert_eq!(mysql.table_name, "orders");
        assert_eq!(mysql.timing, TriggerTiming::Before);
        assert_eq!(mysql.events, vec![TriggerEvent::Delete]);
    }
}
