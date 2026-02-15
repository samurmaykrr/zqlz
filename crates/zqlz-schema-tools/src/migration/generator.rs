//! Migration generator implementation
//!
//! Generates SQL migration scripts from schema diffs.

use thiserror::Error;

use crate::compare::{
    ColumnDiff, ForeignKeyDiff, IndexDiff, PrimaryKeyChange, SchemaDiff, SequenceDiff, TableDiff,
    TriggerDiff, TypeDiff, ViewDiff,
};
use zqlz_core::{
    ColumnInfo, ConstraintInfo, ConstraintType, ForeignKeyAction, ForeignKeyInfo, FunctionInfo,
    IndexInfo, ProcedureInfo, SequenceInfo, TableInfo, TriggerEvent, TriggerInfo, TriggerTiming,
    TypeInfo, TypeKind, ViewInfo,
};

/// Errors that can occur during migration generation
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Empty diff provided
    #[error("empty diff provided")]
    EmptyDiff,
    /// Unsupported operation for dialect
    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),
    /// Invalid schema element
    #[error("invalid schema element: {0}")]
    InvalidElement(String),
}

/// Result type for migration operations
pub type MigrationResult<T> = Result<T, MigrationError>;

/// SQL dialect for migration generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MigrationDialect {
    #[default]
    PostgreSQL,
    MySQL,
    SQLite,
    MsSql,
}

impl MigrationDialect {
    /// Returns the identifier quote character for this dialect
    pub fn quote_char(&self) -> char {
        match self {
            MigrationDialect::PostgreSQL | MigrationDialect::SQLite => '"',
            MigrationDialect::MySQL => '`',
            MigrationDialect::MsSql => '[',
        }
    }

    /// Returns the closing quote character for MsSql
    pub fn close_quote_char(&self) -> char {
        match self {
            MigrationDialect::MsSql => ']',
            _ => self.quote_char(),
        }
    }

    /// Quotes an identifier
    pub fn quote_identifier(&self, name: &str) -> String {
        let open = self.quote_char();
        let close = self.close_quote_char();
        format!("{}{}{}", open, name, close)
    }

    /// Returns whether this dialect supports IF EXISTS
    pub fn supports_if_exists(&self) -> bool {
        true
    }

    /// Returns whether this dialect supports CASCADE
    pub fn supports_cascade(&self) -> bool {
        matches!(self, MigrationDialect::PostgreSQL)
    }

    /// Returns whether this dialect supports CREATE OR REPLACE for views
    pub fn supports_create_or_replace_view(&self) -> bool {
        matches!(self, MigrationDialect::PostgreSQL | MigrationDialect::MySQL)
    }

    /// Returns whether this dialect supports ALTER COLUMN for type changes
    pub fn supports_alter_column_type(&self) -> bool {
        matches!(
            self,
            MigrationDialect::PostgreSQL | MigrationDialect::MySQL | MigrationDialect::MsSql
        )
    }
}

/// Represents a database migration with up and down scripts
#[derive(Debug, Clone)]
pub struct Migration {
    /// SQL statements to apply the migration
    pub up_sql: Vec<String>,
    /// SQL statements to revert the migration
    pub down_sql: Vec<String>,
}

impl Migration {
    /// Creates a new empty migration
    pub fn new() -> Self {
        Self {
            up_sql: Vec::new(),
            down_sql: Vec::new(),
        }
    }

    /// Creates a migration with the given up and down statements
    pub fn with_statements(up_sql: Vec<String>, down_sql: Vec<String>) -> Self {
        Self { up_sql, down_sql }
    }

    /// Returns true if the migration has no statements
    pub fn is_empty(&self) -> bool {
        self.up_sql.is_empty() && self.down_sql.is_empty()
    }

    /// Returns the combined up SQL as a single string
    pub fn up_script(&self) -> String {
        self.up_sql.join(";\n\n") + if self.up_sql.is_empty() { "" } else { ";" }
    }

    /// Returns the combined down SQL as a single string
    pub fn down_script(&self) -> String {
        self.down_sql.join(";\n\n") + if self.down_sql.is_empty() { "" } else { ";" }
    }

    /// Adds an up statement
    pub fn add_up(&mut self, sql: impl Into<String>) {
        self.up_sql.push(sql.into());
    }

    /// Adds a down statement
    pub fn add_down(&mut self, sql: impl Into<String>) {
        self.down_sql.push(sql.into());
    }

    /// Merges another migration into this one
    pub fn merge(&mut self, other: Migration) {
        self.up_sql.extend(other.up_sql);
        self.down_sql.extend(other.down_sql);
    }
}

impl Default for Migration {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for migration generation
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// SQL dialect to generate for
    pub dialect: MigrationDialect,
    /// Whether to include IF EXISTS/IF NOT EXISTS clauses
    pub use_if_exists: bool,
    /// Whether to include CASCADE where applicable
    pub use_cascade: bool,
    /// Whether to generate comments in the output
    pub include_comments: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            dialect: MigrationDialect::PostgreSQL,
            use_if_exists: true,
            use_cascade: false,
            include_comments: true,
        }
    }
}

impl MigrationConfig {
    /// Creates a new config with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a config for the given dialect
    pub fn for_dialect(dialect: MigrationDialect) -> Self {
        Self {
            dialect,
            ..Default::default()
        }
    }

    /// Sets the dialect
    pub fn with_dialect(mut self, dialect: MigrationDialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Enables or disables IF EXISTS clauses
    pub fn with_if_exists(mut self, use_if_exists: bool) -> Self {
        self.use_if_exists = use_if_exists;
        self
    }

    /// Enables or disables CASCADE clauses
    pub fn with_cascade(mut self, use_cascade: bool) -> Self {
        self.use_cascade = use_cascade;
        self
    }

    /// Enables or disables comments in output
    pub fn with_comments(mut self, include_comments: bool) -> Self {
        self.include_comments = include_comments;
        self
    }
}

/// Generator for SQL migrations from schema diffs
#[derive(Debug)]
pub struct MigrationGenerator {
    config: MigrationConfig,
}

impl Default for MigrationGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationGenerator {
    /// Creates a new generator with default configuration
    pub fn new() -> Self {
        Self {
            config: MigrationConfig::default(),
        }
    }

    /// Creates a new generator with the given configuration
    pub fn with_config(config: MigrationConfig) -> Self {
        Self { config }
    }

    /// Returns the current configuration
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }

    /// Returns the dialect
    pub fn dialect(&self) -> MigrationDialect {
        self.config.dialect
    }

    /// Quotes an identifier using the configured dialect
    fn quote(&self, name: &str) -> String {
        self.config.dialect.quote_identifier(name)
    }

    /// Returns the qualified name (schema.name or just name)
    fn qualified_name(&self, name: &str, schema: Option<&str>) -> String {
        match schema {
            Some(s) => format!("{}.{}", self.quote(s), self.quote(name)),
            None => self.quote(name),
        }
    }

    /// Generates a migration from a schema diff
    pub fn generate(&self, diff: &SchemaDiff) -> MigrationResult<Migration> {
        if diff.is_empty() {
            return Ok(Migration::new());
        }

        let mut migration = Migration::new();

        // Order matters for up migrations:
        // 1. Create types (dependencies first)
        // 2. Create sequences
        // 3. Create tables
        // 4. Modify tables
        // 5. Create views
        // 6. Modify views
        // 7. Create functions
        // 8. Modify functions
        // 9. Create procedures
        // 10. Modify procedures
        // 11. Create triggers
        // 12. Modify triggers
        // Down migrations are in reverse order

        // Types
        for type_info in &diff.added_types {
            let (up, down) = self.generate_create_type(type_info)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for type_diff in &diff.modified_types {
            let (up, down) = self.generate_alter_type(type_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for type_info in &diff.removed_types {
            let (up, down) = self.generate_drop_type(type_info)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Sequences
        for seq in &diff.added_sequences {
            let (up, down) = self.generate_create_sequence(seq)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for seq_diff in &diff.modified_sequences {
            let (up, down) = self.generate_alter_sequence(seq_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for seq in &diff.removed_sequences {
            let (up, down) = self.generate_drop_sequence(seq)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Tables
        for table in &diff.added_tables {
            let (up, down) = self.generate_create_table(table)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for table_diff in &diff.modified_tables {
            let m = self.generate_alter_table(table_diff)?;
            migration.merge(m);
        }
        for table in &diff.removed_tables {
            let (up, down) = self.generate_drop_table(table)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Views
        for view in &diff.added_views {
            let (up, down) = self.generate_create_view(view)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for view_diff in &diff.modified_views {
            let (up, down) = self.generate_alter_view(view_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for view in &diff.removed_views {
            let (up, down) = self.generate_drop_view(view)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Functions
        for func in &diff.added_functions {
            let (up, down) = self.generate_create_function(func)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for func in &diff.removed_functions {
            let (up, down) = self.generate_drop_function(func)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Procedures
        for proc in &diff.added_procedures {
            let (up, down) = self.generate_create_procedure(proc)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for proc in &diff.removed_procedures {
            let (up, down) = self.generate_drop_procedure(proc)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Triggers
        for trigger in &diff.added_triggers {
            let (up, down) = self.generate_create_trigger(trigger)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for trigger_diff in &diff.modified_triggers {
            let (up, down) = self.generate_alter_trigger(trigger_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }
        for trigger in &diff.removed_triggers {
            let (up, down) = self.generate_drop_trigger(trigger)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        Ok(migration)
    }

    // Table operations

    fn generate_create_table(&self, table: &TableInfo) -> MigrationResult<(String, String)> {
        let table_name = self.qualified_name(&table.name, table.schema.as_deref());

        // For up: CREATE TABLE
        let up = format!("CREATE TABLE {} ()", table_name);

        // For down: DROP TABLE
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let cascade = if self.config.use_cascade && self.config.dialect.supports_cascade() {
            " CASCADE"
        } else {
            ""
        };
        let down = format!("DROP TABLE {}{}{}", if_exists, table_name, cascade);

        Ok((up, down))
    }

    fn generate_drop_table(&self, table: &TableInfo) -> MigrationResult<(String, String)> {
        let table_name = self.qualified_name(&table.name, table.schema.as_deref());

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let cascade = if self.config.use_cascade && self.config.dialect.supports_cascade() {
            " CASCADE"
        } else {
            ""
        };
        let up = format!("DROP TABLE {}{}{}", if_exists, table_name, cascade);

        // For down: CREATE TABLE (simplified - in practice would need full DDL)
        let down = format!("CREATE TABLE {} ()", table_name);

        Ok((up, down))
    }

    fn generate_alter_table(&self, table_diff: &TableDiff) -> MigrationResult<Migration> {
        let mut migration = Migration::new();
        let _table_name = table_diff.qualified_name();
        let quoted_table =
            self.qualified_name(&table_diff.table_name, table_diff.schema.as_deref());

        // Add columns
        for col in &table_diff.added_columns {
            let (up, down) = self.generate_add_column(&quoted_table, col)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Modify columns
        for col_diff in &table_diff.modified_columns {
            let m = self.generate_alter_column(&quoted_table, col_diff)?;
            migration.merge(m);
        }

        // Remove columns
        for col in &table_diff.removed_columns {
            let (up, down) = self.generate_drop_column(&quoted_table, col)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Add indexes
        for idx in &table_diff.added_indexes {
            let (up, down) = self.generate_create_index(&quoted_table, idx)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Modify indexes (drop and recreate)
        for idx_diff in &table_diff.modified_indexes {
            let (up, down) = self.generate_recreate_index(&quoted_table, idx_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Remove indexes
        for idx in &table_diff.removed_indexes {
            let (up, down) = self.generate_drop_index(&quoted_table, idx)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Add foreign keys
        for fk in &table_diff.added_foreign_keys {
            let (up, down) = self.generate_add_foreign_key(&quoted_table, fk)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Modify foreign keys (drop and recreate)
        for fk_diff in &table_diff.modified_foreign_keys {
            let (up, down) = self.generate_recreate_foreign_key(&quoted_table, fk_diff)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Remove foreign keys
        for fk in &table_diff.removed_foreign_keys {
            let (up, down) = self.generate_drop_foreign_key(&quoted_table, fk)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Add constraints
        for constraint in &table_diff.added_constraints {
            let (up, down) = self.generate_add_constraint(&quoted_table, constraint)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Remove constraints
        for constraint in &table_diff.removed_constraints {
            let (up, down) = self.generate_drop_constraint(&quoted_table, constraint)?;
            migration.add_up(up);
            migration.add_down(down);
        }

        // Primary key changes
        if let Some(pk_change) = &table_diff.primary_key_change {
            let m = self.generate_primary_key_change(&quoted_table, pk_change)?;
            migration.merge(m);
        }

        Ok(migration)
    }

    // Column operations

    fn generate_add_column(
        &self,
        table: &str,
        col: &ColumnInfo,
    ) -> MigrationResult<(String, String)> {
        let col_def = self.column_definition(col);
        let up = format!("ALTER TABLE {} ADD COLUMN {}", table, col_def);

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!(
            "ALTER TABLE {} DROP COLUMN {}{}",
            table,
            if_exists,
            self.quote(&col.name)
        );

        Ok((up, down))
    }

    fn generate_drop_column(
        &self,
        table: &str,
        col: &ColumnInfo,
    ) -> MigrationResult<(String, String)> {
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!(
            "ALTER TABLE {} DROP COLUMN {}{}",
            table,
            if_exists,
            self.quote(&col.name)
        );

        // For down, we need to re-add the column
        let col_def = self.column_definition(col);
        let down = format!("ALTER TABLE {} ADD COLUMN {}", table, col_def);

        Ok((up, down))
    }

    fn generate_alter_column(
        &self,
        table: &str,
        col_diff: &ColumnDiff,
    ) -> MigrationResult<Migration> {
        let mut migration = Migration::new();
        let col_name = self.quote(&col_diff.column_name);

        // Type change
        if let Some((old_type, new_type)) = &col_diff.type_change {
            let up = match self.config.dialect {
                MigrationDialect::PostgreSQL => {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        table, col_name, new_type
                    )
                }
                MigrationDialect::MySQL => {
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {} {}",
                        table, col_name, new_type
                    )
                }
                MigrationDialect::MsSql => {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {}",
                        table, col_name, new_type
                    )
                }
                MigrationDialect::SQLite => {
                    // SQLite doesn't support ALTER COLUMN TYPE directly
                    format!(
                        "-- SQLite: Cannot alter column type. Need to recreate table.\n-- New type: {}",
                        new_type
                    )
                }
            };
            let down = match self.config.dialect {
                MigrationDialect::PostgreSQL => {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        table, col_name, old_type
                    )
                }
                MigrationDialect::MySQL => {
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {} {}",
                        table, col_name, old_type
                    )
                }
                MigrationDialect::MsSql => {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {}",
                        table, col_name, old_type
                    )
                }
                MigrationDialect::SQLite => {
                    format!(
                        "-- SQLite: Cannot alter column type.\n-- Old type: {}",
                        old_type
                    )
                }
            };
            migration.add_up(up);
            migration.add_down(down);
        }

        // Nullable change
        if let Some((was_nullable, is_nullable)) = &col_diff.nullable_change {
            let (up, down) = match self.config.dialect {
                MigrationDialect::PostgreSQL => {
                    if *is_nullable {
                        (
                            format!(
                                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                                table, col_name
                            ),
                            format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                                table, col_name
                            ),
                        )
                    } else {
                        (
                            format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                                table, col_name
                            ),
                            format!(
                                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                                table, col_name
                            ),
                        )
                    }
                }
                MigrationDialect::MySQL => {
                    // MySQL requires the full column definition
                    let null_str = if *is_nullable { "NULL" } else { "NOT NULL" };
                    let old_null_str = if *was_nullable { "NULL" } else { "NOT NULL" };
                    (
                        format!(
                            "-- MySQL: Need full column definition\nALTER TABLE {} MODIFY COLUMN {} TYPE {}",
                            table, col_name, null_str
                        ),
                        format!(
                            "-- MySQL: Need full column definition\nALTER TABLE {} MODIFY COLUMN {} TYPE {}",
                            table, col_name, old_null_str
                        ),
                    )
                }
                _ => (
                    format!("-- Nullable change not directly supported"),
                    format!("-- Nullable change not directly supported"),
                ),
            };
            migration.add_up(up);
            migration.add_down(down);
        }

        // Default value change
        if let Some((old_default, new_default)) = &col_diff.default_change {
            let (up, down) = match self.config.dialect {
                MigrationDialect::PostgreSQL | MigrationDialect::MsSql => {
                    let up = match new_default {
                        Some(def) => format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                            table, col_name, def
                        ),
                        None => format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                            table, col_name
                        ),
                    };
                    let down = match old_default {
                        Some(def) => format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                            table, col_name, def
                        ),
                        None => format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                            table, col_name
                        ),
                    };
                    (up, down)
                }
                _ => (
                    format!("-- Default change: {:?} -> {:?}", old_default, new_default),
                    format!("-- Default change: {:?} -> {:?}", new_default, old_default),
                ),
            };
            migration.add_up(up);
            migration.add_down(down);
        }

        Ok(migration)
    }

    fn column_definition(&self, col: &ColumnInfo) -> String {
        let mut def = format!("{} {}", self.quote(&col.name), col.data_type);

        if let Some(len) = col.max_length {
            def.push_str(&format!("({})", len));
        } else if col.precision.is_some() || col.scale.is_some() {
            let precision = col.precision.unwrap_or(0);
            if let Some(scale) = col.scale {
                def.push_str(&format!("({}, {})", precision, scale));
            } else {
                def.push_str(&format!("({})", precision));
            }
        }

        if !col.nullable {
            def.push_str(" NOT NULL");
        }

        if let Some(default) = &col.default_value {
            def.push_str(&format!(" DEFAULT {}", default));
        }

        def
    }

    // Index operations

    fn generate_create_index(
        &self,
        table: &str,
        idx: &IndexInfo,
    ) -> MigrationResult<(String, String)> {
        let unique = if idx.is_unique { "UNIQUE " } else { "" };
        let cols: Vec<String> = idx.columns.iter().map(|c| self.quote(c)).collect();
        let up = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            self.quote(&idx.name),
            table,
            cols.join(", ")
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!("DROP INDEX {}{}", if_exists, self.quote(&idx.name));

        Ok((up, down))
    }

    fn generate_drop_index(
        &self,
        _table: &str,
        idx: &IndexInfo,
    ) -> MigrationResult<(String, String)> {
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!("DROP INDEX {}{}", if_exists, self.quote(&idx.name));

        // For down, recreate the index
        let unique = if idx.is_unique { "UNIQUE " } else { "" };
        let cols: Vec<String> = idx.columns.iter().map(|c| self.quote(c)).collect();
        let down = format!(
            "CREATE {}INDEX {} ON table_name ({})",
            unique,
            self.quote(&idx.name),
            cols.join(", ")
        );

        Ok((up, down))
    }

    fn generate_recreate_index(
        &self,
        table: &str,
        idx_diff: &IndexDiff,
    ) -> MigrationResult<(String, String)> {
        // Drop old, create new
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let drop = format!(
            "DROP INDEX {}{}",
            if_exists,
            self.quote(&idx_diff.index_name)
        );

        let unique = if idx_diff.new.is_unique {
            "UNIQUE "
        } else {
            ""
        };
        let cols: Vec<String> = idx_diff.new.columns.iter().map(|c| self.quote(c)).collect();
        let create = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            self.quote(&idx_diff.new.name),
            table,
            cols.join(", ")
        );

        let up = format!("{};\n{}", drop, create);

        // Down: drop new, create old
        let drop_new = format!("DROP INDEX {}{}", if_exists, self.quote(&idx_diff.new.name));
        let old_unique = if idx_diff.old.is_unique {
            "UNIQUE "
        } else {
            ""
        };
        let old_cols: Vec<String> = idx_diff.old.columns.iter().map(|c| self.quote(c)).collect();
        let create_old = format!(
            "CREATE {}INDEX {} ON {} ({})",
            old_unique,
            self.quote(&idx_diff.old.name),
            table,
            old_cols.join(", ")
        );

        let down = format!("{};\n{}", drop_new, create_old);

        Ok((up, down))
    }

    // Foreign key operations

    fn generate_add_foreign_key(
        &self,
        table: &str,
        fk: &ForeignKeyInfo,
    ) -> MigrationResult<(String, String)> {
        let cols: Vec<String> = fk.columns.iter().map(|c| self.quote(c)).collect();
        let ref_cols: Vec<String> = fk
            .referenced_columns
            .iter()
            .map(|c| self.quote(c))
            .collect();
        let ref_table = match &fk.referenced_schema {
            Some(schema) => format!(
                "{}.{}",
                self.quote(schema),
                self.quote(&fk.referenced_table)
            ),
            None => self.quote(&fk.referenced_table),
        };

        let on_update = self.foreign_key_action_sql(fk.on_update);
        let on_delete = self.foreign_key_action_sql(fk.on_delete);

        let up = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
            table,
            self.quote(&fk.name),
            cols.join(", "),
            ref_table,
            ref_cols.join(", "),
            on_update,
            on_delete
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&fk.name)
        );

        Ok((up, down))
    }

    fn generate_drop_foreign_key(
        &self,
        table: &str,
        fk: &ForeignKeyInfo,
    ) -> MigrationResult<(String, String)> {
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&fk.name)
        );

        // For down, recreate the FK
        let cols: Vec<String> = fk.columns.iter().map(|c| self.quote(c)).collect();
        let ref_cols: Vec<String> = fk
            .referenced_columns
            .iter()
            .map(|c| self.quote(c))
            .collect();
        let ref_table = match &fk.referenced_schema {
            Some(schema) => format!(
                "{}.{}",
                self.quote(schema),
                self.quote(&fk.referenced_table)
            ),
            None => self.quote(&fk.referenced_table),
        };
        let on_update = self.foreign_key_action_sql(fk.on_update);
        let on_delete = self.foreign_key_action_sql(fk.on_delete);

        let down = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
            table,
            self.quote(&fk.name),
            cols.join(", "),
            ref_table,
            ref_cols.join(", "),
            on_update,
            on_delete
        );

        Ok((up, down))
    }

    fn generate_recreate_foreign_key(
        &self,
        table: &str,
        fk_diff: &ForeignKeyDiff,
    ) -> MigrationResult<(String, String)> {
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&fk_diff.fk_name)
        );

        let down = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&fk_diff.fk_name)
        );

        Ok((up, down))
    }

    fn foreign_key_action_sql(&self, action: ForeignKeyAction) -> &'static str {
        match action {
            ForeignKeyAction::NoAction => "NO ACTION",
            ForeignKeyAction::Restrict => "RESTRICT",
            ForeignKeyAction::Cascade => "CASCADE",
            ForeignKeyAction::SetNull => "SET NULL",
            ForeignKeyAction::SetDefault => "SET DEFAULT",
        }
    }

    // Constraint operations

    fn generate_add_constraint(
        &self,
        table: &str,
        constraint: &ConstraintInfo,
    ) -> MigrationResult<(String, String)> {
        let constraint_sql = match constraint.constraint_type {
            ConstraintType::Check => {
                format!(
                    "CHECK ({})",
                    constraint.definition.as_deref().unwrap_or("true")
                )
            }
            ConstraintType::Unique => {
                let cols: Vec<String> = constraint.columns.iter().map(|c| self.quote(c)).collect();
                format!("UNIQUE ({})", cols.join(", "))
            }
            _ => {
                return Err(MigrationError::UnsupportedOperation(format!(
                    "constraint type {:?}",
                    constraint.constraint_type
                )));
            }
        };

        let up = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            table,
            self.quote(&constraint.name),
            constraint_sql
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&constraint.name)
        );

        Ok((up, down))
    }

    fn generate_drop_constraint(
        &self,
        table: &str,
        constraint: &ConstraintInfo,
    ) -> MigrationResult<(String, String)> {
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!(
            "ALTER TABLE {} DROP CONSTRAINT {}{}",
            table,
            if_exists,
            self.quote(&constraint.name)
        );

        // For down, recreate constraint
        let constraint_sql = match constraint.constraint_type {
            ConstraintType::Check => {
                format!(
                    "CHECK ({})",
                    constraint.definition.as_deref().unwrap_or("true")
                )
            }
            ConstraintType::Unique => {
                let cols: Vec<String> = constraint.columns.iter().map(|c| self.quote(c)).collect();
                format!("UNIQUE ({})", cols.join(", "))
            }
            _ => format!("-- Constraint type {:?}", constraint.constraint_type),
        };

        let down = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            table,
            self.quote(&constraint.name),
            constraint_sql
        );

        Ok((up, down))
    }

    // Primary key operations

    fn generate_primary_key_change(
        &self,
        table: &str,
        change: &PrimaryKeyChange,
    ) -> MigrationResult<Migration> {
        let mut migration = Migration::new();

        match change {
            PrimaryKeyChange::Added(pk) => {
                let cols: Vec<String> = pk.columns.iter().map(|c| self.quote(c)).collect();
                let pk_name = pk
                    .name
                    .as_ref()
                    .map(|n| format!("CONSTRAINT {} ", self.quote(n)))
                    .unwrap_or_default();
                let up = format!(
                    "ALTER TABLE {} ADD {}PRIMARY KEY ({})",
                    table,
                    pk_name,
                    cols.join(", ")
                );
                let down = format!("ALTER TABLE {} DROP PRIMARY KEY", table);
                migration.add_up(up);
                migration.add_down(down);
            }
            PrimaryKeyChange::Removed(pk) => {
                let up = format!("ALTER TABLE {} DROP PRIMARY KEY", table);
                let cols: Vec<String> = pk.columns.iter().map(|c| self.quote(c)).collect();
                let pk_name = pk
                    .name
                    .as_ref()
                    .map(|n| format!("CONSTRAINT {} ", self.quote(n)))
                    .unwrap_or_default();
                let down = format!(
                    "ALTER TABLE {} ADD {}PRIMARY KEY ({})",
                    table,
                    pk_name,
                    cols.join(", ")
                );
                migration.add_up(up);
                migration.add_down(down);
            }
            PrimaryKeyChange::Modified { old, new } => {
                // Drop old PK, add new PK
                let up_drop = format!("ALTER TABLE {} DROP PRIMARY KEY", table);
                let new_cols: Vec<String> = new.columns.iter().map(|c| self.quote(c)).collect();
                let new_pk_name = new
                    .name
                    .as_ref()
                    .map(|n| format!("CONSTRAINT {} ", self.quote(n)))
                    .unwrap_or_default();
                let up_add = format!(
                    "ALTER TABLE {} ADD {}PRIMARY KEY ({})",
                    table,
                    new_pk_name,
                    new_cols.join(", ")
                );

                let down_drop = format!("ALTER TABLE {} DROP PRIMARY KEY", table);
                let old_cols: Vec<String> = old.columns.iter().map(|c| self.quote(c)).collect();
                let old_pk_name = old
                    .name
                    .as_ref()
                    .map(|n| format!("CONSTRAINT {} ", self.quote(n)))
                    .unwrap_or_default();
                let down_add = format!(
                    "ALTER TABLE {} ADD {}PRIMARY KEY ({})",
                    table,
                    old_pk_name,
                    old_cols.join(", ")
                );

                migration.add_up(format!("{};\n{}", up_drop, up_add));
                migration.add_down(format!("{};\n{}", down_drop, down_add));
            }
        }

        Ok(migration)
    }

    // View operations

    fn generate_create_view(&self, view: &ViewInfo) -> MigrationResult<(String, String)> {
        let view_name = self.qualified_name(&view.name, view.schema.as_deref());
        let definition = view.definition.as_deref().unwrap_or("SELECT 1");

        let mat = if view.is_materialized {
            "MATERIALIZED "
        } else {
            ""
        };
        let up = format!("CREATE {}VIEW {} AS {}", mat, view_name, definition);

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let cascade = if self.config.use_cascade && self.config.dialect.supports_cascade() {
            " CASCADE"
        } else {
            ""
        };
        let down = format!("DROP {}VIEW {}{}{}", mat, if_exists, view_name, cascade);

        Ok((up, down))
    }

    fn generate_drop_view(&self, view: &ViewInfo) -> MigrationResult<(String, String)> {
        let view_name = self.qualified_name(&view.name, view.schema.as_deref());
        let mat = if view.is_materialized {
            "MATERIALIZED "
        } else {
            ""
        };
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let cascade = if self.config.use_cascade && self.config.dialect.supports_cascade() {
            " CASCADE"
        } else {
            ""
        };
        let up = format!("DROP {}VIEW {}{}{}", mat, if_exists, view_name, cascade);

        let definition = view.definition.as_deref().unwrap_or("SELECT 1");
        let down = format!("CREATE {}VIEW {} AS {}", mat, view_name, definition);

        Ok((up, down))
    }

    fn generate_alter_view(&self, view_diff: &ViewDiff) -> MigrationResult<(String, String)> {
        let _view_name = view_diff.qualified_name();
        let quoted_view = self.qualified_name(&view_diff.view_name, view_diff.schema.as_deref());

        if let Some((old_def, new_def)) = &view_diff.definition_change {
            let new_definition = new_def.as_deref().unwrap_or("SELECT 1");
            let old_definition = old_def.as_deref().unwrap_or("SELECT 1");

            if self.config.dialect.supports_create_or_replace_view() {
                let up = format!(
                    "CREATE OR REPLACE VIEW {} AS {}",
                    quoted_view, new_definition
                );
                let down = format!(
                    "CREATE OR REPLACE VIEW {} AS {}",
                    quoted_view, old_definition
                );
                Ok((up, down))
            } else {
                let up = format!(
                    "DROP VIEW IF EXISTS {};\nCREATE VIEW {} AS {}",
                    quoted_view, quoted_view, new_definition
                );
                let down = format!(
                    "DROP VIEW IF EXISTS {};\nCREATE VIEW {} AS {}",
                    quoted_view, quoted_view, old_definition
                );
                Ok((up, down))
            }
        } else {
            Ok((String::new(), String::new()))
        }
    }

    // Sequence operations

    fn generate_create_sequence(&self, seq: &SequenceInfo) -> MigrationResult<(String, String)> {
        let seq_name = self.qualified_name(&seq.name, seq.schema.as_deref());
        let up = format!(
            "CREATE SEQUENCE {} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {}",
            seq_name, seq.start_value, seq.increment_by, seq.min_value, seq.max_value
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!("DROP SEQUENCE {}{}", if_exists, seq_name);

        Ok((up, down))
    }

    fn generate_drop_sequence(&self, seq: &SequenceInfo) -> MigrationResult<(String, String)> {
        let seq_name = self.qualified_name(&seq.name, seq.schema.as_deref());
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!("DROP SEQUENCE {}{}", if_exists, seq_name);

        let down = format!(
            "CREATE SEQUENCE {} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {}",
            seq_name, seq.start_value, seq.increment_by, seq.min_value, seq.max_value
        );

        Ok((up, down))
    }

    fn generate_alter_sequence(
        &self,
        seq_diff: &SequenceDiff,
    ) -> MigrationResult<(String, String)> {
        let seq_name = self.qualified_name(&seq_diff.sequence_name, seq_diff.schema.as_deref());
        let mut up_parts = vec![format!("ALTER SEQUENCE {}", seq_name)];
        let mut down_parts = vec![format!("ALTER SEQUENCE {}", seq_name)];

        if let Some((old, new)) = seq_diff.start_value_change {
            up_parts.push(format!("RESTART WITH {}", new));
            down_parts.push(format!("RESTART WITH {}", old));
        }
        if let Some((old, new)) = seq_diff.increment_change {
            up_parts.push(format!("INCREMENT BY {}", new));
            down_parts.push(format!("INCREMENT BY {}", old));
        }
        if let Some((old, new)) = seq_diff.min_value_change {
            up_parts.push(format!("MINVALUE {}", new));
            down_parts.push(format!("MINVALUE {}", old));
        }
        if let Some((old, new)) = seq_diff.max_value_change {
            up_parts.push(format!("MAXVALUE {}", new));
            down_parts.push(format!("MAXVALUE {}", old));
        }

        Ok((up_parts.join(" "), down_parts.join(" ")))
    }

    // Type operations

    fn generate_create_type(&self, type_info: &TypeInfo) -> MigrationResult<(String, String)> {
        let type_name = self.qualified_name(&type_info.name, type_info.schema.as_deref());

        let up = match type_info.type_kind {
            TypeKind::Enum => {
                let values = type_info
                    .values
                    .as_ref()
                    .map(|v| {
                        v.iter()
                            .map(|s| format!("'{}'", s))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                format!("CREATE TYPE {} AS ENUM ({})", type_name, values)
            }
            _ => {
                if let Some(def) = &type_info.definition {
                    format!("CREATE TYPE {} AS {}", type_name, def)
                } else {
                    format!("CREATE TYPE {}", type_name)
                }
            }
        };

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!("DROP TYPE {}{}", if_exists, type_name);

        Ok((up, down))
    }

    fn generate_drop_type(&self, type_info: &TypeInfo) -> MigrationResult<(String, String)> {
        let type_name = self.qualified_name(&type_info.name, type_info.schema.as_deref());
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!("DROP TYPE {}{}", if_exists, type_name);

        let down = match type_info.type_kind {
            TypeKind::Enum => {
                let values = type_info
                    .values
                    .as_ref()
                    .map(|v| {
                        v.iter()
                            .map(|s| format!("'{}'", s))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                format!("CREATE TYPE {} AS ENUM ({})", type_name, values)
            }
            _ => {
                if let Some(def) = &type_info.definition {
                    format!("CREATE TYPE {} AS {}", type_name, def)
                } else {
                    format!("CREATE TYPE {}", type_name)
                }
            }
        };

        Ok((up, down))
    }

    fn generate_alter_type(&self, type_diff: &TypeDiff) -> MigrationResult<(String, String)> {
        let type_name = self.qualified_name(&type_diff.type_name, type_diff.schema.as_deref());

        // For enums, we can add values but not remove them
        if let Some((old_values, new_values)) = &type_diff.values_change {
            let old_set: std::collections::HashSet<_> = old_values.iter().flatten().collect();
            let new_set: std::collections::HashSet<_> = new_values.iter().flatten().collect();

            let added: Vec<_> = new_set.difference(&old_set).collect();
            let removed: Vec<_> = old_set.difference(&new_set).collect();

            let mut up_statements = Vec::new();
            for value in added {
                up_statements.push(format!("ALTER TYPE {} ADD VALUE '{}'", type_name, value));
            }

            let mut down_statements = Vec::new();
            for value in removed {
                down_statements.push(format!(
                    "-- Cannot remove enum value '{}' directly in PostgreSQL",
                    value
                ));
            }

            let up = up_statements.join(";\n");
            let down = if down_statements.is_empty() {
                "-- No down migration for added enum values".to_string()
            } else {
                down_statements.join(";\n")
            };

            Ok((up, down))
        } else {
            Ok((String::new(), String::new()))
        }
    }

    // Function operations

    fn generate_create_function(&self, func: &FunctionInfo) -> MigrationResult<(String, String)> {
        let func_name = self.qualified_name(&func.name, func.schema.as_deref());
        let definition = func.definition.as_deref().unwrap_or("BEGIN END");

        let up = format!(
            "CREATE OR REPLACE FUNCTION {} RETURNS {} LANGUAGE {} AS $${}$$",
            func_name, func.return_type, func.language, definition
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!("DROP FUNCTION {}{}", if_exists, func_name);

        Ok((up, down))
    }

    fn generate_drop_function(&self, func: &FunctionInfo) -> MigrationResult<(String, String)> {
        let func_name = self.qualified_name(&func.name, func.schema.as_deref());
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!("DROP FUNCTION {}{}", if_exists, func_name);

        let definition = func.definition.as_deref().unwrap_or("BEGIN END");
        let down = format!(
            "CREATE OR REPLACE FUNCTION {} RETURNS {} LANGUAGE {} AS $${}$$",
            func_name, func.return_type, func.language, definition
        );

        Ok((up, down))
    }

    // Procedure operations

    fn generate_create_procedure(&self, proc: &ProcedureInfo) -> MigrationResult<(String, String)> {
        let proc_name = self.qualified_name(&proc.name, proc.schema.as_deref());
        let definition = proc.definition.as_deref().unwrap_or("BEGIN END");

        let up = format!(
            "CREATE OR REPLACE PROCEDURE {} LANGUAGE {} AS $${}$$",
            proc_name, proc.language, definition
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!("DROP PROCEDURE {}{}", if_exists, proc_name);

        Ok((up, down))
    }

    fn generate_drop_procedure(&self, proc: &ProcedureInfo) -> MigrationResult<(String, String)> {
        let proc_name = self.qualified_name(&proc.name, proc.schema.as_deref());
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!("DROP PROCEDURE {}{}", if_exists, proc_name);

        let definition = proc.definition.as_deref().unwrap_or("BEGIN END");
        let down = format!(
            "CREATE OR REPLACE PROCEDURE {} LANGUAGE {} AS $${}$$",
            proc_name, proc.language, definition
        );

        Ok((up, down))
    }

    // Trigger operations

    fn generate_create_trigger(&self, trigger: &TriggerInfo) -> MigrationResult<(String, String)> {
        let trigger_name = self.quote(&trigger.name);
        let table_name = self.qualified_name(&trigger.table_name, trigger.schema.as_deref());
        let timing = match trigger.timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        };
        let events: Vec<&str> = trigger
            .events
            .iter()
            .map(|e| match e {
                TriggerEvent::Insert => "INSERT",
                TriggerEvent::Update => "UPDATE",
                TriggerEvent::Delete => "DELETE",
                TriggerEvent::Truncate => "TRUNCATE",
            })
            .collect();
        let for_each = match trigger.for_each {
            zqlz_core::TriggerForEach::Row => "FOR EACH ROW",
            zqlz_core::TriggerForEach::Statement => "FOR EACH STATEMENT",
        };

        let definition = trigger
            .definition
            .as_deref()
            .unwrap_or("EXECUTE FUNCTION trigger_fn()");
        let up = format!(
            "CREATE TRIGGER {} {} {} ON {} {} {}",
            trigger_name,
            timing,
            events.join(" OR "),
            table_name,
            for_each,
            definition
        );

        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let down = format!(
            "DROP TRIGGER {}{} ON {}",
            if_exists, trigger_name, table_name
        );

        Ok((up, down))
    }

    fn generate_drop_trigger(&self, trigger: &TriggerInfo) -> MigrationResult<(String, String)> {
        let trigger_name = self.quote(&trigger.name);
        let table_name = self.qualified_name(&trigger.table_name, trigger.schema.as_deref());
        let if_exists = if self.config.use_if_exists {
            "IF EXISTS "
        } else {
            ""
        };
        let up = format!(
            "DROP TRIGGER {}{} ON {}",
            if_exists, trigger_name, table_name
        );

        // For down, recreate the trigger
        let timing = match trigger.timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        };
        let events: Vec<&str> = trigger
            .events
            .iter()
            .map(|e| match e {
                TriggerEvent::Insert => "INSERT",
                TriggerEvent::Update => "UPDATE",
                TriggerEvent::Delete => "DELETE",
                TriggerEvent::Truncate => "TRUNCATE",
            })
            .collect();
        let for_each = match trigger.for_each {
            zqlz_core::TriggerForEach::Row => "FOR EACH ROW",
            zqlz_core::TriggerForEach::Statement => "FOR EACH STATEMENT",
        };
        let definition = trigger
            .definition
            .as_deref()
            .unwrap_or("EXECUTE FUNCTION trigger_fn()");

        let down = format!(
            "CREATE TRIGGER {} {} {} ON {} {} {}",
            trigger_name,
            timing,
            events.join(" OR "),
            table_name,
            for_each,
            definition
        );

        Ok((up, down))
    }

    fn generate_alter_trigger(
        &self,
        trigger_diff: &TriggerDiff,
    ) -> MigrationResult<(String, String)> {
        // Most databases don't support ALTER TRIGGER, need to drop and recreate
        // For now, just output a comment
        let trigger_name = self.quote(&trigger_diff.trigger_name);
        let table_name =
            self.qualified_name(&trigger_diff.table_name, trigger_diff.schema.as_deref());

        if let Some((old_enabled, new_enabled)) = trigger_diff.enabled_change {
            let up = if new_enabled {
                format!("ALTER TABLE {} ENABLE TRIGGER {}", table_name, trigger_name)
            } else {
                format!(
                    "ALTER TABLE {} DISABLE TRIGGER {}",
                    table_name, trigger_name
                )
            };
            let down = if old_enabled {
                format!("ALTER TABLE {} ENABLE TRIGGER {}", table_name, trigger_name)
            } else {
                format!(
                    "ALTER TABLE {} DISABLE TRIGGER {}",
                    table_name, trigger_name
                )
            };
            Ok((up, down))
        } else {
            Ok((
                format!(
                    "-- Trigger {} definition changed, manual intervention required",
                    trigger_name
                ),
                format!(
                    "-- Trigger {} definition changed, manual intervention required",
                    trigger_name
                ),
            ))
        }
    }
}
