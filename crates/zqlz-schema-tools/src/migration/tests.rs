//! Tests for migration generator

use super::*;
use crate::compare::{ColumnDiff, SchemaDiff, TableDiff};
use zqlz_core::{ColumnInfo, IndexInfo, TableInfo, TableType};

#[test]
fn test_generate_add_table_migration() {
    let generator = MigrationGenerator::new();

    let table = TableInfo {
        name: "users".to_string(),
        schema: Some("public".to_string()),
        table_type: TableType::Table,
        owner: None,
        row_count: None,
        size_bytes: None,
        comment: None,
        index_count: None,
        trigger_count: None,
        key_value_info: None,
    };

    let mut diff = SchemaDiff::new();
    diff.added_tables.push(table);

    let migration = generator.generate(&diff).unwrap();

    assert!(!migration.is_empty());
    assert_eq!(migration.up_sql.len(), 1);
    assert_eq!(migration.down_sql.len(), 1);

    let up = &migration.up_sql[0];
    let down = &migration.down_sql[0];

    assert!(up.contains("CREATE TABLE"));
    assert!(up.contains("\"public\""));
    assert!(up.contains("\"users\""));

    assert!(down.contains("DROP TABLE"));
    assert!(down.contains("IF EXISTS"));
    assert!(down.contains("\"public\""));
    assert!(down.contains("\"users\""));
}

#[test]
fn test_generate_alter_column_migration() {
    let generator = MigrationGenerator::new();

    let mut column_diff = ColumnDiff::new("email");
    column_diff.type_change = Some(("varchar(100)".to_string(), "varchar(255)".to_string()));

    let mut table_diff = TableDiff::new("users", Some("public".to_string()));
    table_diff.modified_columns.push(column_diff);

    let mut diff = SchemaDiff::new();
    diff.modified_tables.push(table_diff);

    let migration = generator.generate(&diff).unwrap();

    assert!(!migration.is_empty());
    assert!(!migration.up_sql.is_empty());

    let up = migration.up_script();
    assert!(up.contains("ALTER TABLE"));
    assert!(up.contains("TYPE"));
    assert!(up.contains("varchar(255)"));
}

#[test]
fn test_migration_sql_ordering() {
    let generator = MigrationGenerator::new();

    let mut diff = SchemaDiff::new();

    // Add a table
    diff.added_tables.push(TableInfo {
        name: "orders".to_string(),
        schema: None,
        table_type: TableType::Table,
        owner: None,
        row_count: None,
        size_bytes: None,
        comment: None,
        index_count: None,
        trigger_count: None,
        key_value_info: None,
    });

    // Remove a table
    diff.removed_tables.push(TableInfo {
        name: "legacy_data".to_string(),
        schema: None,
        table_type: TableType::Table,
        owner: None,
        row_count: None,
        size_bytes: None,
        comment: None,
        index_count: None,
        trigger_count: None,
        key_value_info: None,
    });

    // Modify a table (add column)
    let mut table_diff = TableDiff::new("products", None);
    table_diff.added_columns.push(ColumnInfo {
        name: "description".to_string(),
        ordinal: 0,
        data_type: "text".to_string(),
        nullable: true,
        default_value: None,
        max_length: None,
        precision: None,
        scale: None,
        is_primary_key: false,
        is_auto_increment: false,
        is_unique: false,
        foreign_key: None,
        comment: None,
        ..Default::default()
    });
    diff.modified_tables.push(table_diff);

    let migration = generator.generate(&diff).unwrap();

    // Should have statements for: create table, alter table (add column), drop table
    assert!(migration.up_sql.len() >= 3);

    // Verify the ordering: CREATE TABLE comes before ALTER TABLE
    let up_script = migration.up_script();
    let create_pos = up_script.find("CREATE TABLE").unwrap();
    let drop_pos = up_script.find("DROP TABLE").unwrap();

    // Creates should come before drops in forward migration
    assert!(
        create_pos < drop_pos,
        "CREATE TABLE should come before DROP TABLE in up migration"
    );
}

#[test]
fn test_dialect_identifier_quoting() {
    // PostgreSQL uses double quotes
    let pg_gen =
        MigrationGenerator::with_config(MigrationConfig::for_dialect(MigrationDialect::PostgreSQL));
    assert_eq!(pg_gen.dialect().quote_identifier("users"), "\"users\"");

    // MySQL uses backticks
    let mysql_gen =
        MigrationGenerator::with_config(MigrationConfig::for_dialect(MigrationDialect::MySQL));
    assert_eq!(mysql_gen.dialect().quote_identifier("users"), "`users`");

    // MsSql uses square brackets
    let mssql_gen =
        MigrationGenerator::with_config(MigrationConfig::for_dialect(MigrationDialect::MsSql));
    assert_eq!(mssql_gen.dialect().quote_identifier("users"), "[users]");
}

#[test]
fn test_empty_diff_returns_empty_migration() {
    let generator = MigrationGenerator::new();
    let diff = SchemaDiff::new();

    let migration = generator.generate(&diff).unwrap();

    assert!(migration.is_empty());
    assert!(migration.up_sql.is_empty());
    assert!(migration.down_sql.is_empty());
}

#[test]
fn test_add_index_migration() {
    let generator = MigrationGenerator::new();

    let index = IndexInfo {
        name: "idx_users_email".to_string(),
        columns: vec!["email".to_string()],
        is_unique: true,
        is_primary: false,
        index_type: "btree".to_string(),
        comment: None,
        ..Default::default()
    };

    let mut table_diff = TableDiff::new("users", Some("public".to_string()));
    table_diff.added_indexes.push(index);

    let mut diff = SchemaDiff::new();
    diff.modified_tables.push(table_diff);

    let migration = generator.generate(&diff).unwrap();

    let up = migration.up_script();
    let down = migration.down_script();

    assert!(up.contains("CREATE UNIQUE INDEX"));
    assert!(up.contains("idx_users_email"));
    assert!(up.contains("email"));

    assert!(down.contains("DROP INDEX"));
    assert!(down.contains("idx_users_email"));
}
