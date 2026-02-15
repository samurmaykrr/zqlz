//! Tests for the cross-database schema syncer

use std::collections::HashMap;

use zqlz_core::{ColumnInfo, TableDetails, TableInfo, TableType, ViewInfo};

use super::syncer::{CrossDatabaseSync, SyncConfig};
use super::type_mapper::Dialect;

/// Helper function to create a test table info
fn make_table_info(name: &str, schema: Option<&str>) -> TableInfo {
    TableInfo {
        schema: schema.map(String::from),
        name: name.to_string(),
        table_type: TableType::Table,
        owner: None,
        row_count: None,
        size_bytes: None,
        comment: None,
        index_count: None,
        trigger_count: None,
        key_value_info: None,
    }
}

/// Helper function to create a test column info
fn make_column(name: &str, data_type: &str, nullable: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        ordinal: 0,
        data_type: data_type.to_string(),
        nullable,
        default_value: None,
        max_length: None,
        precision: None,
        scale: None,
        is_primary_key: false,
        is_auto_increment: false,
        is_unique: false,
        foreign_key: None,
        comment: None,
    }
}

/// Helper function to create test table details
fn make_table_details(name: &str, columns: Vec<ColumnInfo>) -> TableDetails {
    TableDetails {
        info: make_table_info(name, None),
        columns,
        primary_key: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
    }
}

mod sync_config_tests {
    use super::*;

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();

        assert_eq!(config.source_dialect, Dialect::PostgreSQL);
        assert_eq!(config.target_dialect, Dialect::PostgreSQL);
        assert!(config.dry_run);
        assert!(config.use_if_exists);
        assert!(config.sync_tables);
        assert!(config.sync_views);
        assert!(config.sync_indexes);
        assert!(config.sync_foreign_keys);
        assert!(config.sync_sequences);
        assert!(config.sync_types);
        assert!(config.exclude_tables.is_empty());
        assert!(config.include_schemas.is_empty());
    }

    #[test]
    fn test_sync_config_new() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);

        assert_eq!(config.source_dialect, Dialect::PostgreSQL);
        assert_eq!(config.target_dialect, Dialect::MySQL);
    }

    #[test]
    fn test_sync_config_builders() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL)
            .with_dry_run(false)
            .with_tables(true)
            .with_views(false)
            .with_indexes(false)
            .with_foreign_keys(false)
            .exclude_table("temp_*")
            .exclude_table("migration_*")
            .include_schema("public");

        assert!(!config.dry_run);
        assert!(config.sync_tables);
        assert!(!config.sync_views);
        assert!(!config.sync_indexes);
        assert!(!config.sync_foreign_keys);
        assert_eq!(config.exclude_tables, vec!["temp_*", "migration_*"]);
        assert_eq!(config.include_schemas, vec!["public"]);
    }
}

mod cross_database_sync_creation_tests {
    use super::*;

    #[test]
    fn test_cross_database_sync_new() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        assert_eq!(sync.config().source_dialect, Dialect::PostgreSQL);
        assert_eq!(sync.config().target_dialect, Dialect::MySQL);
    }

    #[test]
    fn test_cross_database_sync_from_to() {
        let sync = CrossDatabaseSync::from_to(Dialect::MySQL, Dialect::SQLite);

        assert_eq!(sync.config().source_dialect, Dialect::MySQL);
        assert_eq!(sync.config().target_dialect, Dialect::SQLite);
    }

    #[test]
    fn test_cross_database_sync_default() {
        let sync = CrossDatabaseSync::default();

        assert_eq!(sync.config().source_dialect, Dialect::PostgreSQL);
        assert_eq!(sync.config().target_dialect, Dialect::PostgreSQL);
    }
}

mod type_mapping_tests {
    use super::*;

    #[test]
    fn test_map_type_postgres_to_mysql() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::MySQL);

        let result = sync.map_type("SERIAL").unwrap();
        assert_eq!(result, "INT AUTO_INCREMENT");

        let result = sync.map_type("TEXT").unwrap();
        assert_eq!(result, "LONGTEXT");

        let result = sync.map_type("BOOLEAN").unwrap();
        assert_eq!(result, "TINYINT(1)");
    }

    #[test]
    fn test_map_type_mysql_to_postgres() {
        let sync = CrossDatabaseSync::from_to(Dialect::MySQL, Dialect::PostgreSQL);

        let result = sync.map_type("DATETIME").unwrap();
        assert_eq!(result, "TIMESTAMP");

        let result = sync.map_type("TINYINT").unwrap();
        assert_eq!(result, "SMALLINT");
    }

    #[test]
    fn test_map_type_same_dialect() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::PostgreSQL);

        let result = sync.map_type("TEXT").unwrap();
        assert_eq!(result, "TEXT");

        let result = sync.map_type("INTEGER").unwrap();
        assert_eq!(result, "INTEGER");
    }

    #[test]
    fn test_map_columns() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::MySQL);

        let columns = vec![
            make_column("id", "SERIAL", false),
            make_column("name", "TEXT", true),
            make_column("active", "BOOLEAN", false),
        ];

        let result = sync.map_columns(&columns).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].data_type, "INT AUTO_INCREMENT");
        assert_eq!(result[1].data_type, "LONGTEXT");
        assert_eq!(result[2].data_type, "TINYINT(1)");
    }

    #[test]
    fn test_map_columns_preserves_other_properties() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::MySQL);

        let mut col = make_column("name", "TEXT", true);
        col.default_value = Some("'default'".to_string());
        col.is_unique = true;
        col.comment = Some("User name".to_string());

        let result = sync.map_columns(&[col]).unwrap();

        assert_eq!(result[0].name, "name");
        assert!(result[0].nullable);
        assert_eq!(result[0].default_value, Some("'default'".to_string()));
        assert!(result[0].is_unique);
        assert_eq!(result[0].comment, Some("User name".to_string()));
    }
}

mod convert_table_details_tests {
    use super::*;

    #[test]
    fn test_convert_table_details() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::MySQL);

        let columns = vec![
            make_column("id", "SERIAL", false),
            make_column("data", "JSONB", true),
        ];
        let details = make_table_details("users", columns);

        let result = sync.convert_table_details(&details).unwrap();

        assert_eq!(result.info.name, "users");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].data_type, "INT AUTO_INCREMENT");
        assert_eq!(result.columns[1].data_type, "JSON");
    }

    #[test]
    fn test_convert_all_tables() {
        let sync = CrossDatabaseSync::from_to(Dialect::PostgreSQL, Dialect::MySQL);

        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details(
                "users",
                vec![
                    make_column("id", "SERIAL", false),
                    make_column("name", "TEXT", true),
                ],
            ),
        );
        source_details.insert(
            "posts".to_string(),
            make_table_details(
                "posts",
                vec![
                    make_column("id", "BIGSERIAL", false),
                    make_column("content", "TEXT", true),
                ],
            ),
        );

        let result = sync.convert_all_tables(&source_details).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains_key("users"));
        assert!(result.contains_key("posts"));

        let users = result.get("users").unwrap();
        assert_eq!(users.columns[0].data_type, "INT AUTO_INCREMENT");

        let posts = result.get("posts").unwrap();
        assert_eq!(posts.columns[0].data_type, "BIGINT AUTO_INCREMENT");
    }
}

mod sync_plan_tests {
    use super::*;

    #[test]
    fn test_sync_dry_run() {
        // Create a sync from PostgreSQL to MySQL
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).with_dry_run(true);
        let sync = CrossDatabaseSync::new(config);

        // Source schema: users table with id (SERIAL), name (TEXT)
        let source_tables = vec![make_table_info("users", None)];
        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details(
                "users",
                vec![
                    make_column("id", "SERIAL", false),
                    make_column("name", "TEXT", true),
                ],
            ),
        );

        // Target schema: empty (no tables)
        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        // Generate sync plan
        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // Verify plan
        assert!(!plan.is_empty());
        assert_eq!(plan.stats.tables_added, 1);
        assert_eq!(plan.stats.tables_removed, 0);
        assert_eq!(plan.stats.tables_modified, 0);

        // The diff should have the users table as added
        assert_eq!(plan.diff.added_tables.len(), 1);
        assert_eq!(plan.diff.added_tables[0].name, "users");

        // Should generate migration SQL
        let up_script = plan.up_script();
        assert!(!up_script.is_empty());
    }

    #[test]
    fn test_sync_plan_with_modifications() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        // Source: users table with id, name, email
        let source_tables = vec![make_table_info("users", None)];
        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details(
                "users",
                vec![
                    make_column("id", "INTEGER", false),
                    make_column("name", "TEXT", true),
                    make_column("email", "VARCHAR(255)", true), // New column
                ],
            ),
        );

        // Target: users table with id, name (no email)
        let target_tables = vec![make_table_info("users", None)];
        let mut target_details = HashMap::new();
        target_details.insert(
            "users".to_string(),
            make_table_details(
                "users",
                vec![
                    make_column("id", "INT", false),
                    make_column("name", "LONGTEXT", true),
                ],
            ),
        );

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // Should detect added column
        assert_eq!(plan.stats.tables_modified, 1);
        assert_eq!(plan.stats.columns_added, 1);
    }

    #[test]
    fn test_sync_plan_empty_when_identical() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::PostgreSQL);
        let sync = CrossDatabaseSync::new(config);

        let tables = vec![make_table_info("users", None)];
        let mut details = HashMap::new();
        details.insert(
            "users".to_string(),
            make_table_details(
                "users",
                vec![
                    make_column("id", "INTEGER", false),
                    make_column("name", "TEXT", true),
                ],
            ),
        );

        let plan = sync
            .plan_sync(&tables, &tables, &details, &details)
            .unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.stats.total_changes(), 0);
    }
}

mod sync_stats_tests {
    use super::*;

    #[test]
    fn test_sync_stats_total_changes() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        // Source: two tables
        let source_tables = vec![
            make_table_info("users", None),
            make_table_info("posts", None),
        ];
        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details("users", vec![make_column("id", "INTEGER", false)]),
        );
        source_details.insert(
            "posts".to_string(),
            make_table_details("posts", vec![make_column("id", "INTEGER", false)]),
        );

        // Target: empty
        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        assert_eq!(plan.stats.tables_added, 2);
        assert_eq!(plan.stats.total_changes(), 2);
        assert!(!plan.stats.has_breaking_changes());
    }

    #[test]
    fn test_sync_stats_breaking_changes() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        // Source: empty
        let source_tables: Vec<TableInfo> = Vec::new();
        let source_details: HashMap<String, TableDetails> = HashMap::new();

        // Target: one table (will be removed)
        let target_tables = vec![make_table_info("users", None)];
        let mut target_details = HashMap::new();
        target_details.insert(
            "users".to_string(),
            make_table_details("users", vec![make_column("id", "INT", false)]),
        );

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        assert_eq!(plan.stats.tables_removed, 1);
        assert!(plan.stats.has_breaking_changes());
    }
}

mod filter_tests {
    use super::*;

    #[test]
    fn test_exclude_tables_exact_match() {
        let config =
            SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).exclude_table("temp_data");
        let sync = CrossDatabaseSync::new(config);

        let source_tables = vec![
            make_table_info("users", None),
            make_table_info("temp_data", None),
        ];
        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details("users", vec![make_column("id", "INTEGER", false)]),
        );
        source_details.insert(
            "temp_data".to_string(),
            make_table_details("temp_data", vec![make_column("id", "INTEGER", false)]),
        );

        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // temp_data should be excluded
        assert_eq!(plan.diff.added_tables.len(), 1);
        assert_eq!(plan.diff.added_tables[0].name, "users");
    }

    #[test]
    fn test_exclude_tables_wildcard() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).exclude_table("temp_*");
        let sync = CrossDatabaseSync::new(config);

        let source_tables = vec![
            make_table_info("users", None),
            make_table_info("temp_cache", None),
            make_table_info("temp_session", None),
        ];
        let mut source_details = HashMap::new();
        for table in &source_tables {
            source_details.insert(
                table.name.clone(),
                make_table_details(&table.name, vec![make_column("id", "INTEGER", false)]),
            );
        }

        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // Only users should be added, temp_* excluded
        assert_eq!(plan.diff.added_tables.len(), 1);
        assert_eq!(plan.diff.added_tables[0].name, "users");
    }

    #[test]
    fn test_include_schemas() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).include_schema("public");
        let sync = CrossDatabaseSync::new(config);

        let source_tables = vec![
            make_table_info("users", Some("public")),
            make_table_info("internal", Some("private")),
        ];
        let mut source_details = HashMap::new();
        for table in &source_tables {
            source_details.insert(
                table.name.clone(),
                make_table_details(&table.name, vec![make_column("id", "INTEGER", false)]),
            );
        }

        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // Only public schema table should be added
        assert_eq!(plan.diff.added_tables.len(), 1);
        assert_eq!(plan.diff.added_tables[0].name, "users");
    }

    #[test]
    fn test_disable_sync_options() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL)
            .with_tables(false)
            .with_views(false);
        let sync = CrossDatabaseSync::new(config);

        let source_tables = vec![make_table_info("users", None)];
        let mut source_details = HashMap::new();
        source_details.insert(
            "users".to_string(),
            make_table_details("users", vec![make_column("id", "INTEGER", false)]),
        );

        let target_tables: Vec<TableInfo> = Vec::new();
        let target_details: HashMap<String, TableDetails> = HashMap::new();

        let plan = sync
            .plan_sync(
                &source_tables,
                &target_tables,
                &source_details,
                &target_details,
            )
            .unwrap();

        // Tables sync disabled, should be empty
        assert!(plan.diff.added_tables.is_empty());
    }
}

mod view_sync_tests {
    use super::*;

    fn make_view(name: &str, schema: Option<&str>, definition: &str) -> ViewInfo {
        ViewInfo {
            schema: schema.map(String::from),
            name: name.to_string(),
            is_materialized: false,
            definition: Some(definition.to_string()),
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_plan_view_sync_added() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        let source_views = vec![make_view("user_stats", None, "SELECT * FROM users")];
        let target_views: Vec<ViewInfo> = Vec::new();

        let plan = sync.plan_view_sync(&source_views, &target_views).unwrap();

        assert_eq!(plan.diff.added_views.len(), 1);
        assert_eq!(plan.diff.added_views[0].name, "user_stats");
    }

    #[test]
    fn test_plan_view_sync_removed() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let sync = CrossDatabaseSync::new(config);

        let source_views: Vec<ViewInfo> = Vec::new();
        let target_views = vec![make_view("old_view", None, "SELECT 1")];

        let plan = sync.plan_view_sync(&source_views, &target_views).unwrap();

        assert_eq!(plan.diff.removed_views.len(), 1);
        assert_eq!(plan.diff.removed_views[0].name, "old_view");
    }

    #[test]
    fn test_plan_view_sync_disabled() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).with_views(false);
        let sync = CrossDatabaseSync::new(config);

        let source_views = vec![make_view("user_stats", None, "SELECT * FROM users")];
        let target_views: Vec<ViewInfo> = Vec::new();

        let plan = sync.plan_view_sync(&source_views, &target_views).unwrap();

        // Views sync disabled
        assert!(plan.diff.added_views.is_empty());
    }

    #[test]
    fn test_plan_view_sync_schema_filter() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL).include_schema("public");
        let sync = CrossDatabaseSync::new(config);

        let source_views = vec![
            make_view("public_view", Some("public"), "SELECT 1"),
            make_view("private_view", Some("private"), "SELECT 2"),
        ];
        let target_views: Vec<ViewInfo> = Vec::new();

        let plan = sync.plan_view_sync(&source_views, &target_views).unwrap();

        // Only public schema view should be added
        assert_eq!(plan.diff.added_views.len(), 1);
        assert_eq!(plan.diff.added_views[0].name, "public_view");
    }
}

mod convenience_function_tests {
    use super::*;
    use crate::cross_sync::syncer::{map_columns, map_type_between};

    #[test]
    fn test_map_columns_convenience() {
        let columns = vec![
            make_column("id", "SERIAL", false),
            make_column("name", "TEXT", true),
        ];

        let result = map_columns(&columns, Dialect::PostgreSQL, Dialect::MySQL).unwrap();

        assert_eq!(result[0].data_type, "INT AUTO_INCREMENT");
        assert_eq!(result[1].data_type, "LONGTEXT");
    }

    #[test]
    fn test_map_type_between_convenience() {
        let result = map_type_between("SERIAL", Dialect::PostgreSQL, Dialect::MySQL).unwrap();
        assert_eq!(result, "INT AUTO_INCREMENT");

        let result = map_type_between("DATETIME", Dialect::MySQL, Dialect::PostgreSQL).unwrap();
        assert_eq!(result, "TIMESTAMP");
    }
}

mod custom_type_mapping_tests {
    use super::*;

    #[test]
    fn test_custom_type_mapping_via_syncer() {
        let config = SyncConfig::new(Dialect::PostgreSQL, Dialect::MySQL);
        let mut sync = CrossDatabaseSync::new(config);

        // Add custom mapping
        sync.type_mapper_mut().add_custom_mapping(
            Dialect::PostgreSQL,
            "CUSTOM_TYPE",
            Dialect::MySQL,
            "VARCHAR(500)",
        );

        let result = sync.map_type("CUSTOM_TYPE").unwrap();
        assert_eq!(result, "VARCHAR(500)");
    }
}
