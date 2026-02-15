//! DDL generation for table designs
//!
//! Generates CREATE TABLE, ALTER TABLE, DROP TABLE, and CREATE INDEX
//! statements for different database dialects. Uses `DialectInfo` from the
//! driver registry so that identifier quoting and auto-increment syntax
//! are determined by the driver rather than hardcoded here.

use crate::models::{
    ColumnDesign, DatabaseDialect, ForeignKeyDesign, IndexDesign, TableDesign, TableOptions,
};
use zqlz_core::{AutoIncrementStyle, DialectInfo, ForeignKeyAction};
use zqlz_drivers::get_dialect_info;

/// DDL Generator for creating SQL statements from table designs
///
/// This is a stateless utility — all methods are associated functions
/// that take the design data as input. Dialect-specific behaviour is
/// read from `DialectInfo` provided by the driver registry.
pub struct DdlGenerator;

impl DdlGenerator {
    /// Resolve the `DialectInfo` for the given design's dialect.
    fn dialect_info(dialect: &DatabaseDialect) -> DialectInfo {
        get_dialect_info(dialect.driver_name())
    }

    /// Wrap an identifier with the dialect's quote character.
    fn quote_ident(name: &str, quote: char) -> String {
        format!("{}{}{}", quote, name, quote)
    }

    /// Generate CREATE TABLE statement
    pub fn generate_create_table(design: &TableDesign) -> anyhow::Result<String> {
        let info = Self::dialect_info(&design.dialect);
        let q = info.identifier_quote;
        let mut ddl = String::new();

        ddl.push_str(&format!(
            "CREATE TABLE {} (\n",
            Self::quote_ident(&design.table_name, q)
        ));

        let column_defs: Vec<String> = design
            .columns
            .iter()
            .map(|col| Self::generate_column_definition(col, &info))
            .collect();

        ddl.push_str(&column_defs.join(",\n"));

        // Composite primary key constraint (more than one PK column)
        let pk_columns: Vec<&str> = design
            .columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect();

        if pk_columns.len() > 1 {
            ddl.push_str(",\n  PRIMARY KEY (");
            ddl.push_str(
                &pk_columns
                    .iter()
                    .map(|c| Self::quote_ident(c, q))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            ddl.push(')');
        }

        for fk in &design.foreign_keys {
            ddl.push_str(",\n");
            ddl.push_str(&Self::generate_foreign_key_constraint(fk, q));
        }

        ddl.push_str("\n)");

        if design.options.has_options() {
            ddl.push_str(&Self::generate_table_options(
                &design.options,
                &design.dialect,
            ));
        }

        ddl.push(';');

        for index in &design.indexes {
            if !index.is_primary {
                ddl.push_str("\n\n");
                ddl.push_str(&Self::generate_create_index(&design.table_name, index, q));
            }
        }

        Ok(ddl)
    }

    /// Generate ALTER TABLE statements for modifying an existing table.
    ///
    /// Compares `original` and `modified` designs and produces a list of
    /// individual DDL statements that should be executed in order.
    /// Handles: table rename, column add/drop/modify, foreign key add/drop,
    /// and index add/drop.
    pub fn generate_alter_table(
        original: &TableDesign,
        modified: &TableDesign,
    ) -> anyhow::Result<Vec<String>> {
        let info = Self::dialect_info(&modified.dialect);
        let q = info.identifier_quote;
        let mut statements = Vec::new();

        // Use the original table name for ALTER statements, then rename at the end
        let table = Self::quote_ident(&original.table_name, q);

        // --- Table rename ---
        if original.table_name != modified.table_name {
            let new_name = Self::quote_ident(&modified.table_name, q);
            match modified.dialect {
                DatabaseDialect::Mysql => {
                    statements.push(format!("ALTER TABLE {} RENAME TO {};", table, new_name));
                }
                _ => {
                    statements.push(format!("ALTER TABLE {} RENAME TO {};", table, new_name));
                }
            }
        }

        // After a potential rename, subsequent statements target the new name
        let table = Self::quote_ident(&modified.table_name, q);

        // --- Dropped columns ---
        // Process drops before adds so we don't accidentally reference removed columns
        for col in &original.columns {
            if !modified.columns.iter().any(|c| c.name == col.name) {
                statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {};",
                    table,
                    Self::quote_ident(&col.name, q)
                ));
            }
        }

        // --- Added columns ---
        for col in &modified.columns {
            if !original.columns.iter().any(|c| c.name == col.name) {
                statements.push(format!(
                    "ALTER TABLE {} ADD COLUMN {};",
                    table,
                    Self::generate_column_definition(col, &info).trim()
                ));
            }
        }

        // --- Modified columns (type, nullable, default, unique changes) ---
        for new_col in &modified.columns {
            if let Some(old_col) = original.columns.iter().find(|c| c.name == new_col.name) {
                let alter_stmts =
                    Self::generate_column_alterations(&table, old_col, new_col, &info);
                statements.extend(alter_stmts);
            }
        }

        // --- Dropped foreign keys ---
        for fk in &original.foreign_keys {
            let still_exists = modified.foreign_keys.iter().any(|f| f.name == fk.name);
            if !still_exists {
                if let Some(ref name) = fk.name {
                    match modified.dialect {
                        DatabaseDialect::Mysql => {
                            statements.push(format!(
                                "ALTER TABLE {} DROP FOREIGN KEY {};",
                                table,
                                Self::quote_ident(name, q)
                            ));
                        }
                        _ => {
                            statements.push(format!(
                                "ALTER TABLE {} DROP CONSTRAINT {};",
                                table,
                                Self::quote_ident(name, q)
                            ));
                        }
                    }
                }
            }
        }

        // --- Added foreign keys ---
        for fk in &modified.foreign_keys {
            let existed = original.foreign_keys.iter().any(|f| f.name == fk.name);
            if !existed {
                statements.push(format!(
                    "ALTER TABLE {} ADD {};",
                    table,
                    Self::generate_foreign_key_constraint(fk, q).trim()
                ));
            }
        }

        // --- Dropped indexes ---
        for idx in &original.indexes {
            if !modified.indexes.iter().any(|i| i.name == idx.name) && !idx.is_primary {
                match modified.dialect {
                    DatabaseDialect::Mysql => {
                        statements.push(format!(
                            "DROP INDEX {} ON {};",
                            Self::quote_ident(&idx.name, q),
                            table
                        ));
                    }
                    _ => {
                        statements.push(format!(
                            "DROP INDEX IF EXISTS {};",
                            Self::quote_ident(&idx.name, q)
                        ));
                    }
                }
            }
        }

        // --- Added indexes ---
        for idx in &modified.indexes {
            if !original.indexes.iter().any(|i| i.name == idx.name) && !idx.is_primary {
                statements.push(Self::generate_create_index(&modified.table_name, idx, q));
            }
        }

        Ok(statements)
    }

    /// Generate ALTER statements for a single column that changed between versions.
    ///
    /// Handles data type, nullable, default value, and unique constraint changes.
    /// The generated SQL varies by dialect because each database has different
    /// ALTER COLUMN syntax.
    fn generate_column_alterations(
        table: &str,
        old: &ColumnDesign,
        new: &ColumnDesign,
        info: &DialectInfo,
    ) -> Vec<String> {
        let q = info.identifier_quote;
        let col = Self::quote_ident(&new.name, q);
        let mut stmts = Vec::new();

        let type_changed =
            old.data_type != new.data_type || old.length != new.length || old.scale != new.scale;
        let nullable_changed = old.nullable != new.nullable;
        let default_changed = old.default_value != new.default_value;
        let unique_changed = old.is_unique != new.is_unique;

        let dialect_id = info.id.as_ref();

        match dialect_id {
            // PostgreSQL supports fine-grained ALTER COLUMN
            "postgresql" | "postgres" => {
                if type_changed {
                    let type_spec = Self::column_type_spec(new);
                    stmts.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                        table, col, type_spec
                    ));
                }
                if nullable_changed {
                    if new.nullable {
                        stmts.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                            table, col
                        ));
                    } else {
                        stmts.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                            table, col
                        ));
                    }
                }
                if default_changed {
                    if let Some(ref default) = new.default_value {
                        stmts.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                            table, col, default
                        ));
                    } else {
                        stmts.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                            table, col
                        ));
                    }
                }
            }
            // MySQL uses MODIFY COLUMN which re-specifies the entire column
            "mysql" | "mariadb" => {
                if type_changed || nullable_changed || default_changed {
                    stmts.push(format!(
                        "ALTER TABLE {} MODIFY COLUMN {};",
                        table,
                        Self::generate_column_definition(new, info).trim()
                    ));
                }
            }
            // SQLite has very limited ALTER TABLE — only ADD COLUMN and
            // RENAME COLUMN are supported. Type/nullable/default changes
            // are not possible without recreating the table. We emit a
            // comment explaining this.
            _ => {
                if type_changed || nullable_changed || default_changed {
                    stmts.push(format!(
                        "-- SQLite does not support ALTER COLUMN for type/nullable/default changes on {}.",
                        col
                    ));
                    stmts.push(format!(
                        "-- Consider recreating the table to apply changes to column {}.",
                        col
                    ));
                }
            }
        }

        // UNIQUE constraint changes are handled via index add/drop,
        // but if the column-level unique flag changed we can emit
        // explicit statements for databases that support it.
        if unique_changed && !new.is_primary_key {
            match dialect_id {
                "postgresql" | "postgres" => {
                    if new.is_unique {
                        let constraint_name =
                            format!("{}_{}_unique", table.replace(q, ""), new.name);
                        stmts.push(format!(
                            "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});",
                            table,
                            Self::quote_ident(&constraint_name, q),
                            col
                        ));
                    } else {
                        let constraint_name =
                            format!("{}_{}_unique", table.replace(q, ""), new.name);
                        stmts.push(format!(
                            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
                            table,
                            Self::quote_ident(&constraint_name, q)
                        ));
                    }
                }
                "mysql" | "mariadb" => {
                    if new.is_unique {
                        let idx_name = format!("{}_{}_unique", table.replace(q, ""), new.name);
                        stmts.push(format!(
                            "CREATE UNIQUE INDEX {} ON {} ({});",
                            Self::quote_ident(&idx_name, q),
                            table,
                            col
                        ));
                    } else {
                        let idx_name = format!("{}_{}_unique", table.replace(q, ""), new.name);
                        stmts.push(format!(
                            "DROP INDEX {} ON {};",
                            Self::quote_ident(&idx_name, q),
                            table
                        ));
                    }
                }
                _ => {
                    // SQLite doesn't support adding/dropping unique constraints
                    // after table creation without recreating the table
                }
            }
        }

        stmts
    }

    /// Build the type specification string (e.g. "VARCHAR(255)" or "DECIMAL(10, 2)")
    fn column_type_spec(column: &ColumnDesign) -> String {
        let mut spec = column.data_type.clone();
        if let Some(length) = column.length {
            if let Some(scale) = column.scale {
                spec.push_str(&format!("({}, {})", length, scale));
            } else {
                spec.push_str(&format!("({})", length));
            }
        }
        spec
    }

    /// Generate DROP TABLE statement
    pub fn generate_drop_table(table_name: &str) -> String {
        format!("DROP TABLE IF EXISTS \"{}\";", table_name)
    }

    /// Generate CREATE INDEX statement
    fn generate_create_index(table_name: &str, index: &IndexDesign, quote: char) -> String {
        let unique = if index.is_unique { "UNIQUE " } else { "" };
        let columns = index
            .columns
            .iter()
            .map(|c| Self::quote_ident(c, quote))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "CREATE {}INDEX {} ON {} ({});",
            unique,
            Self::quote_ident(&index.name, quote),
            Self::quote_ident(table_name, quote),
            columns
        )
    }

    /// Generate column definition SQL (used in CREATE TABLE and ALTER TABLE ADD COLUMN)
    fn generate_column_definition(column: &ColumnDesign, info: &DialectInfo) -> String {
        let q = info.identifier_quote;
        let mut def = format!(
            "  {} {}",
            Self::quote_ident(&column.name, q),
            column.data_type
        );

        if let Some(length) = column.length {
            if let Some(scale) = column.scale {
                def.push_str(&format!("({}, {})", length, scale));
            } else {
                def.push_str(&format!("({})", length));
            }
        }

        if !column.nullable {
            def.push_str(" NOT NULL");
        }

        // Inline PRIMARY KEY for single-column PKs
        if column.is_primary_key && !column.is_part_of_composite_pk {
            def.push_str(" PRIMARY KEY");

            // Auto-increment on the PK is driver-determined
            if column.is_auto_increment {
                if let Some(ref ai) = info.auto_increment {
                    match ai.style {
                        AutoIncrementStyle::Suffix => {
                            def.push(' ');
                            def.push_str(&ai.keyword);
                        }
                        AutoIncrementStyle::TypeName | AutoIncrementStyle::Generated => {
                            // TypeName means the type itself already handles
                            // auto-increment (e.g. PostgreSQL SERIAL).
                            // Generated would use GENERATED ALWAYS AS IDENTITY.
                            // Both are handled at a higher level — when the
                            // user picks the auto-increment type — so nothing
                            // extra to emit here for the PK suffix.
                        }
                    }
                }
            }
        }

        // Auto-increment on non-PK columns (only Suffix style makes sense)
        if column.is_auto_increment && !column.is_primary_key {
            if let Some(ref ai) = info.auto_increment {
                if ai.style == AutoIncrementStyle::Suffix {
                    def.push(' ');
                    def.push_str(&ai.keyword);
                }
            }
        }

        if column.is_unique && !column.is_primary_key {
            def.push_str(" UNIQUE");
        }

        if let Some(ref default) = column.default_value {
            def.push_str(&format!(" DEFAULT {}", default));
        }

        if let Some(ref expr) = column.generated_expression {
            let storage = if column.generated_stored {
                "STORED"
            } else {
                "VIRTUAL"
            };
            def.push_str(&format!(" GENERATED ALWAYS AS ({}) {}", expr, storage));
        }

        def
    }

    /// Generate foreign key constraint SQL fragment
    fn generate_foreign_key_constraint(fk: &ForeignKeyDesign, quote: char) -> String {
        let mut constraint = String::new();

        if let Some(ref name) = fk.name {
            constraint.push_str(&format!("  CONSTRAINT {} ", Self::quote_ident(name, quote)));
        } else {
            constraint.push_str("  ");
        }

        constraint.push_str("FOREIGN KEY (");
        constraint.push_str(
            &fk.columns
                .iter()
                .map(|c| Self::quote_ident(c, quote))
                .collect::<Vec<_>>()
                .join(", "),
        );
        constraint.push_str(&format!(
            ") REFERENCES {} (",
            Self::quote_ident(&fk.referenced_table, quote)
        ));
        constraint.push_str(
            &fk.referenced_columns
                .iter()
                .map(|c| Self::quote_ident(c, quote))
                .collect::<Vec<_>>()
                .join(", "),
        );
        constraint.push(')');

        if fk.on_update != ForeignKeyAction::NoAction {
            constraint.push_str(&format!(" ON UPDATE {}", fk_action_to_sql(&fk.on_update)));
        }

        if fk.on_delete != ForeignKeyAction::NoAction {
            constraint.push_str(&format!(" ON DELETE {}", fk_action_to_sql(&fk.on_delete)));
        }

        constraint
    }

    /// Generate table options string.
    ///
    /// NOTE: This still uses hardcoded fields from `TableOptions` since that
    /// struct has not yet been refactored to a generic `HashMap<String, String>`
    /// driven by `TableOptionDef`. This is a future improvement.
    fn generate_table_options(options: &TableOptions, dialect: &DatabaseDialect) -> String {
        match dialect {
            DatabaseDialect::Sqlite => {
                let mut opts = Vec::new();
                if options.without_rowid {
                    opts.push("WITHOUT ROWID");
                }
                if options.strict {
                    opts.push("STRICT");
                }
                if opts.is_empty() {
                    String::new()
                } else {
                    format!(" {}", opts.join(", "))
                }
            }
            DatabaseDialect::Postgres => String::new(),
            DatabaseDialect::Mysql => {
                let mut opts = Vec::new();
                if let Some(ref engine) = options.engine {
                    opts.push(format!("ENGINE={}", engine));
                }
                if let Some(ref charset) = options.charset {
                    opts.push(format!("DEFAULT CHARSET={}", charset));
                }
                if let Some(ref collation) = options.collation {
                    opts.push(format!("COLLATE={}", collation));
                }
                if let Some(start) = options.auto_increment_start {
                    opts.push(format!("AUTO_INCREMENT={}", start));
                }
                if let Some(ref row_format) = options.row_format {
                    opts.push(format!("ROW_FORMAT={}", row_format));
                }
                if opts.is_empty() {
                    String::new()
                } else {
                    format!(" {}", opts.join(" "))
                }
            }
        }
    }
}

/// Convert ForeignKeyAction to SQL syntax
pub fn fk_action_to_sql(action: &ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
        ForeignKeyAction::SetDefault => "SET DEFAULT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ColumnDesign;

    #[test]
    fn test_generate_simple_table() {
        let design = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(
                ColumnDesign::named("id")
                    .integer()
                    .primary_key()
                    .auto_increment(),
            )
            .with_column(ColumnDesign::named("name").text().not_null())
            .with_column(ColumnDesign::named("email").text().unique());

        let ddl = DdlGenerator::generate_create_table(&design).expect("should generate DDL");

        assert!(ddl.contains("CREATE TABLE \"users\""));
        assert!(ddl.contains("\"id\" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"));
        assert!(ddl.contains("\"name\" TEXT NOT NULL"));
        assert!(ddl.contains("\"email\" TEXT UNIQUE"));
    }

    #[test]
    fn test_generate_table_with_foreign_key() {
        let design = TableDesign::new("posts", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key())
            .with_column(ColumnDesign::named("user_id").integer().not_null())
            .with_foreign_key(
                ForeignKeyDesign::new()
                    .column("user_id")
                    .references("users")
                    .referenced_column("id")
                    .on_delete(ForeignKeyAction::Cascade),
            );

        let ddl = DdlGenerator::generate_create_table(&design).expect("should generate DDL");

        assert!(ddl.contains("FOREIGN KEY (\"user_id\") REFERENCES \"users\" (\"id\")"));
        assert!(ddl.contains("ON DELETE CASCADE"));
    }

    #[test]
    fn test_generate_drop_table() {
        let ddl = DdlGenerator::generate_drop_table("users");
        assert_eq!(ddl, "DROP TABLE IF EXISTS \"users\";");
    }

    #[test]
    fn test_generate_alter_table_add_column() {
        let original = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key());

        let modified = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key())
            .with_column(ColumnDesign::named("email").text());

        let statements =
            DdlGenerator::generate_alter_table(&original, &modified).expect("should generate DDL");

        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("ADD COLUMN"));
        assert!(statements[0].contains("\"email\""));
    }

    #[test]
    fn test_generate_alter_table_drop_column() {
        let original = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key())
            .with_column(ColumnDesign::named("email").text());

        let modified = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key());

        let statements =
            DdlGenerator::generate_alter_table(&original, &modified).expect("should generate DDL");

        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("DROP COLUMN"));
        assert!(statements[0].contains("\"email\""));
    }

    #[test]
    fn test_generate_alter_table_rename() {
        let original = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key());

        let modified = TableDesign::new("accounts", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key());

        let statements =
            DdlGenerator::generate_alter_table(&original, &modified).expect("should generate DDL");

        assert!(statements.iter().any(|s| s.contains("RENAME TO")));
    }

    #[test]
    fn test_generate_alter_no_changes() {
        let design = TableDesign::new("users", DatabaseDialect::Sqlite)
            .with_column(ColumnDesign::named("id").integer().primary_key());

        let statements =
            DdlGenerator::generate_alter_table(&design, &design).expect("should generate DDL");

        assert!(statements.is_empty());
    }
}
