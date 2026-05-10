use std::sync::Arc;

use anyhow::Result;
use zqlz_core::{
    Connection, DropViewOptions, SqlObjectName, TableDetails, connection_is_postgres,
    connection_supports_create_or_replace_view,
};
use zqlz_schema_tools::{
    MigrationConfig, MigrationDialect, MigrationGenerator, SchemaComparator, SchemaDiff,
};

use crate::{DatabaseObjectType, VersionEntry, VersionRepository};

/// Non-UI restore execution plan for applying a saved version.
#[derive(Clone, Debug)]
pub struct RestorePlan {
    /// Source version being restored.
    pub version: VersionEntry,
    /// SQL statements to execute in order.
    pub statements: Vec<String>,
    /// User-facing SQL preview.
    pub preview_sql: String,
}

/// Result of restore planning.
#[derive(Clone, Debug)]
pub enum RestorePlanningResult {
    /// A concrete SQL plan is available and can be executed.
    Plan(Box<RestorePlan>),
    /// The target already matches the requested version.
    NoChanges,
    /// This object type cannot be restored through SQL apply flows.
    UnsupportedObjectType(DatabaseObjectType),
}

/// Outcome of applying a restore plan to a live connection.
#[derive(Clone, Debug)]
pub struct RestoreApplyResult {
    /// Number of statements that were executed successfully.
    pub statements_applied: usize,
    /// Whether recording a restore snapshot succeeded.
    pub restore_commit_recorded: bool,
    /// Error text when snapshot recording failed.
    pub restore_commit_record_error: Option<String>,
}

/// Commit message to record after a successful restore apply.
pub fn version_restore_commit_message(version: &VersionEntry) -> String {
    format!(
        "Restore {} {} to version {}",
        version.object_type.display_name().to_lowercase(),
        version.object_name,
        version.short_id()
    )
}

/// Build a restore plan for the provided version and live connection.
pub async fn build_restore_plan(
    connection: Arc<dyn Connection>,
    version: VersionEntry,
) -> Result<RestorePlanningResult> {
    if version.object_type == DatabaseObjectType::Table {
        build_table_restore_plan(connection, version).await
    } else {
        build_sql_restore_plan(&connection, version)
    }
}

/// Execute a restore plan and attempt to record a restore snapshot.
pub async fn apply_restore_plan(
    connection: Arc<dyn Connection>,
    repository: &VersionRepository,
    plan: &RestorePlan,
) -> Result<RestoreApplyResult> {
    for statement in &plan.statements {
        connection.execute(statement, &[]).await?;
    }

    let restore_commit_record_error = repository
        .commit(
            plan.version.connection_id,
            plan.version.object_type,
            plan.version.object_schema.clone(),
            plan.version.object_name.clone(),
            plan.version.content.clone(),
            version_restore_commit_message(&plan.version),
        )
        .err()
        .map(|error| error.to_string());

    Ok(RestoreApplyResult {
        statements_applied: plan.statements.len(),
        restore_commit_recorded: restore_commit_record_error.is_none(),
        restore_commit_record_error,
    })
}

fn migration_dialect_for_connection(connection: &Arc<dyn Connection>) -> MigrationDialect {
    if connection_is_postgres(connection.as_ref()) {
        MigrationDialect::PostgreSQL
    } else if matches!(connection.dialect_id(), Some("mysql") | Some("mariadb")) {
        MigrationDialect::MySQL
    } else if matches!(connection.dialect_id(), Some("mssql") | Some("sqlserver")) {
        MigrationDialect::MsSql
    } else {
        MigrationDialect::SQLite
    }
}

fn quote_qualified_name(
    connection: &Arc<dyn Connection>,
    schema: Option<&str>,
    object_name: &str,
) -> String {
    match schema {
        Some(schema_name) if !schema_name.is_empty() => connection
            .render_qualified_name(&SqlObjectName::with_namespace(schema_name, object_name)),
        _ => connection.quote_identifier(object_name),
    }
}

fn build_sql_restore_plan(
    connection: &Arc<dyn Connection>,
    version: VersionEntry,
) -> Result<RestorePlanningResult> {
    let trimmed_content = version.content.trim();
    if trimmed_content.is_empty() {
        anyhow::bail!("Saved version does not contain any SQL to restore")
    }

    let statements = match version.object_type {
        DatabaseObjectType::View => {
            if connection_is_postgres(connection.as_ref()) {
                vec![trimmed_content.to_string()]
            } else {
                let view_object_name = match version.object_schema.as_deref() {
                    Some(schema_name) if !schema_name.is_empty() => {
                        SqlObjectName::with_namespace(schema_name, &version.object_name)
                    }
                    _ => SqlObjectName::new(&version.object_name),
                };
                let drop_view_sql = connection
                    .drop_view_sql(
                        &view_object_name,
                        DropViewOptions {
                            if_exists: true,
                            cascade: false,
                        },
                    )
                    .unwrap_or_else(|_| {
                        format!(
                            "DROP VIEW IF EXISTS {}",
                            quote_qualified_name(
                                connection,
                                version.object_schema.as_deref(),
                                &version.object_name,
                            )
                        )
                    });
                vec![
                    drop_view_sql,
                    if connection_supports_create_or_replace_view(connection.as_ref()) {
                        connection.normalize_create_view_sql(trimmed_content)
                    } else {
                        trimmed_content.to_string()
                    },
                ]
            }
        }
        _ if version.object_type.is_applyable() => vec![trimmed_content.to_string()],
        _ => {
            return Ok(RestorePlanningResult::UnsupportedObjectType(
                version.object_type,
            ));
        }
    };

    let preview_sql = statements.join(";\n\n") + if statements.is_empty() { "" } else { ";" };

    Ok(RestorePlanningResult::Plan(Box::new(RestorePlan {
        version,
        statements,
        preview_sql,
    })))
}

async fn build_table_restore_plan(
    connection: Arc<dyn Connection>,
    version: VersionEntry,
) -> Result<RestorePlanningResult> {
    let target_snapshot: TableDetails = serde_json::from_str(&version.content)?;
    let schema_introspection = connection.as_schema_introspection().ok_or_else(|| {
        anyhow::anyhow!("Schema introspection is not available for this connection")
    })?;
    let current_snapshot = schema_introspection
        .get_table(version.object_schema.as_deref(), &version.object_name)
        .await?;

    let comparator = SchemaComparator::new();
    let Some(table_diff) = comparator.compare_table_details(&target_snapshot, &current_snapshot)
    else {
        return Ok(RestorePlanningResult::NoChanges);
    };

    let mut schema_diff = SchemaDiff::new();
    schema_diff.modified_tables.push(table_diff);

    let generator = MigrationGenerator::with_config(MigrationConfig::for_dialect(
        migration_dialect_for_connection(&connection),
    ));
    let migration = generator.generate(&schema_diff)?;
    if migration.up_sql.is_empty() {
        return Ok(RestorePlanningResult::NoChanges);
    }

    Ok(RestorePlanningResult::Plan(Box::new(RestorePlan {
        version,
        preview_sql: migration.up_script(),
        statements: migration.up_sql,
    })))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tempfile::tempdir;
    use uuid::Uuid;
    use zqlz_core::{
        ColumnInfo, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject, Dependency,
        DropTableOptions, DropTriggerOptions, DropViewOptions, ForeignKeyInfo, FunctionInfo,
        IndexInfo, PrimaryKeyInfo, ProcedureInfo, QueryResult, Result, SchemaInfo,
        SchemaIntrospection, SequenceInfo, SqlObjectName, StatementResult, TableDetails, TableInfo,
        TableType, Transaction, TriggerInfo, TypeInfo, Value, ViewInfo, ZqlzError,
    };

    use super::{
        RestorePlanningResult, apply_restore_plan, build_restore_plan,
        version_restore_commit_message,
    };
    use crate::{
        DatabaseObjectType, VersionEntry, VersionRepository, VersionStorage, make_object_id,
    };

    struct RestorePlannerTestConnection {
        dialect: Option<&'static str>,
        executed_statements: std::sync::Mutex<Vec<String>>,
        current_table_snapshot: std::sync::Mutex<Option<TableDetails>>,
    }

    impl RestorePlannerTestConnection {
        fn new(dialect: Option<&'static str>) -> Self {
            Self {
                dialect,
                executed_statements: std::sync::Mutex::new(Vec::new()),
                current_table_snapshot: std::sync::Mutex::new(None),
            }
        }

        fn executed_statements(&self) -> Vec<String> {
            self.executed_statements
                .lock()
                .map(|statements| statements.clone())
                .unwrap_or_default()
        }

        fn set_current_table_snapshot(&self, table_details: TableDetails) {
            if let Ok(mut current_table_snapshot) = self.current_table_snapshot.lock() {
                *current_table_snapshot = Some(table_details);
            }
        }

        fn current_table_snapshot(&self) -> Result<TableDetails> {
            self.current_table_snapshot
                .lock()
                .map_err(|_| ZqlzError::NotSupported("table snapshot lock poisoned".to_string()))?
                .clone()
                .ok_or_else(|| {
                    ZqlzError::NotSupported("current table snapshot is not set".to_string())
                })
        }
    }

    #[async_trait]
    impl Connection for RestorePlannerTestConnection {
        fn driver_name(&self) -> &str {
            self.dialect.unwrap_or("test")
        }

        fn dialect_id(&self) -> Option<&'static str> {
            self.dialect
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
            if let Ok(mut statements) = self.executed_statements.lock() {
                statements.push(sql.to_string());
            }

            Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 1,
                error: None,
            })
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
            Ok(QueryResult::empty())
        }

        fn rename_table_sql(
            &self,
            table_name: &SqlObjectName,
            new_table_name: &str,
        ) -> Result<String> {
            Ok(format!(
                "ALTER TABLE {} RENAME TO {}",
                self.render_qualified_name(table_name),
                self.quote_identifier(new_table_name)
            ))
        }

        fn drop_table_sql(
            &self,
            table_name: &SqlObjectName,
            options: DropTableOptions,
        ) -> Result<String> {
            let if_exists = if options.if_exists { " IF EXISTS" } else { "" };
            let cascade = if options.cascade { " CASCADE" } else { "" };
            Ok(format!(
                "DROP TABLE{if_exists} {}{cascade}",
                self.render_qualified_name(table_name)
            ))
        }

        fn drop_view_sql(
            &self,
            view_name: &SqlObjectName,
            options: DropViewOptions,
        ) -> Result<String> {
            let if_exists = if options.if_exists { " IF EXISTS" } else { "" };
            let cascade = if options.cascade { " CASCADE" } else { "" };
            Ok(format!(
                "DROP VIEW{if_exists} {}{cascade}",
                self.render_qualified_name(view_name)
            ))
        }

        fn drop_trigger_sql(
            &self,
            trigger_name: &SqlObjectName,
            _table_name: Option<&SqlObjectName>,
            options: DropTriggerOptions,
        ) -> Result<String> {
            let if_exists = if options.if_exists { " IF EXISTS" } else { "" };
            let cascade = if options.cascade { " CASCADE" } else { "" };
            Ok(format!(
                "DROP TRIGGER{if_exists} {}{cascade}",
                self.render_qualified_name(trigger_name)
            ))
        }

        fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "TRUNCATE TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn duplicate_table_sql(
            &self,
            source_table_name: &SqlObjectName,
            new_table_name: &SqlObjectName,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} AS SELECT * FROM {}",
                self.render_qualified_name(new_table_name),
                self.render_qualified_name(source_table_name)
            ))
        }

        fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "DELETE FROM {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "SELECT 1 FROM {} LIMIT 1",
                self.render_qualified_name(table_name)
            ))
        }

        fn select_rows_sql(
            &self,
            table_name: &SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
        ) -> Result<String> {
            let projection = if projected_columns.is_empty() {
                "*".to_string()
            } else {
                projected_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let mut sql = format!(
                "SELECT {} FROM {}",
                projection,
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            Ok(sql)
        }

        fn select_distinct_rows_sql(
            &self,
            table_name: &SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
            order_by_columns: &[String],
            limit: u64,
        ) -> Result<String> {
            let projection = projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let mut sql = format!(
                "SELECT DISTINCT {} FROM {}",
                projection,
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            if !order_by_columns.is_empty() {
                let order_by = order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(" ORDER BY ");
                sql.push_str(&order_by);
            }
            sql.push_str(&format!(" LIMIT {}", limit));
            Ok(sql)
        }

        fn insert_row_sql(
            &self,
            table_name: &SqlObjectName,
            column_names: &[String],
            value_count: usize,
        ) -> Result<String> {
            let columns = column_names
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = std::iter::repeat_n("?", value_count)
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.render_qualified_name(table_name),
                columns,
                placeholders
            ))
        }

        fn performance_metrics_query_sql(&self) -> Result<String> {
            Ok("SELECT 1".to_string())
        }

        fn normalize_create_view_sql(&self, sql: &str) -> String {
            format!("NORMALIZED::{sql}")
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported(
                "Transactions are not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }

        fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
            Some(self)
        }
    }

    #[async_trait]
    impl SchemaIntrospection for RestorePlannerTestConnection {
        async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
            Err(ZqlzError::NotSupported(
                "list_databases is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
            Err(ZqlzError::NotSupported(
                "list_schemas is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
            Err(ZqlzError::NotSupported(
                "list_tables is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
            Err(ZqlzError::NotSupported(
                "list_views is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_table(&self, _schema: Option<&str>, _name: &str) -> Result<TableDetails> {
            self.current_table_snapshot()
        }

        async fn get_columns(
            &self,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<Vec<ColumnInfo>> {
            Err(ZqlzError::NotSupported(
                "get_columns is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> Result<Vec<IndexInfo>> {
            Err(ZqlzError::NotSupported(
                "get_indexes is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_foreign_keys(
            &self,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<Vec<ForeignKeyInfo>> {
            Err(ZqlzError::NotSupported(
                "get_foreign_keys is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_primary_key(
            &self,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<Option<PrimaryKeyInfo>> {
            Err(ZqlzError::NotSupported(
                "get_primary_key is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_constraints(
            &self,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<Vec<ConstraintInfo>> {
            Err(ZqlzError::NotSupported(
                "get_constraints is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
            Err(ZqlzError::NotSupported(
                "list_functions is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
            Err(ZqlzError::NotSupported(
                "list_procedures is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_triggers(
            &self,
            _schema: Option<&str>,
            _table: Option<&str>,
        ) -> Result<Vec<TriggerInfo>> {
            Err(ZqlzError::NotSupported(
                "list_triggers is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
            Err(ZqlzError::NotSupported(
                "list_sequences is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
            Err(ZqlzError::NotSupported(
                "list_types is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn generate_ddl(&self, _object: &DatabaseObject) -> Result<String> {
            Err(ZqlzError::NotSupported(
                "generate_ddl is not needed by restore planner unit tests".to_string(),
            ))
        }

        async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
            Err(ZqlzError::NotSupported(
                "get_dependencies is not needed by restore planner unit tests".to_string(),
            ))
        }
    }

    fn build_test_version(object_type: DatabaseObjectType, content: &str) -> VersionEntry {
        VersionEntry::new(
            Uuid::new_v4(),
            object_type,
            Some("public".to_string()),
            "users_view".to_string(),
            content.to_string(),
            "snapshot".to_string(),
            None,
        )
    }

    fn build_test_table_details(column_names: &[&str]) -> TableDetails {
        let columns = column_names
            .iter()
            .enumerate()
            .map(|(index, name)| ColumnInfo {
                name: (*name).to_string(),
                ordinal: index,
                data_type: if *name == "id" {
                    "INTEGER".to_string()
                } else {
                    "TEXT".to_string()
                },
                nullable: *name != "id",
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: *name == "id",
                is_auto_increment: false,
                is_unique: *name == "id",
                foreign_key: None,
                comment: None,
                charset: None,
                collation: None,
                generation_expression: None,
                is_generated_stored: false,
                enum_values: None,
            })
            .collect();

        TableDetails {
            info: TableInfo {
                schema: Some("public".to_string()),
                name: "users".to_string(),
                table_type: TableType::Table,
                owner: None,
                row_count: None,
                size_bytes: None,
                comment: None,
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            },
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            triggers: Vec::new(),
        }
    }

    fn build_test_table_version(content: &str) -> VersionEntry {
        VersionEntry::new(
            Uuid::new_v4(),
            DatabaseObjectType::Table,
            Some("public".to_string()),
            "users".to_string(),
            content.to_string(),
            "table snapshot".to_string(),
            None,
        )
    }

    #[tokio::test]
    async fn restore_plan_reports_unsupported_object_type_for_non_applyable_objects() {
        let connection = std::sync::Arc::new(RestorePlannerTestConnection::new(Some("sqlite")));
        let version = build_test_version(DatabaseObjectType::Sequence, "SELECT 1;");

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_ok());

        let planning_result =
            result.expect("restore planning should succeed for typed unsupported output");
        assert!(matches!(
            planning_result,
            RestorePlanningResult::UnsupportedObjectType(DatabaseObjectType::Sequence)
        ));
    }

    #[tokio::test]
    async fn restore_plan_rejects_empty_sql_content_for_applyable_objects() {
        let connection = std::sync::Arc::new(RestorePlannerTestConnection::new(Some("sqlite")));
        let version = build_test_version(DatabaseObjectType::Function, "   ");

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn restore_plan_builds_drop_and_normalized_sql_for_non_postgres_view() {
        let connection = std::sync::Arc::new(RestorePlannerTestConnection::new(Some("mysql")));
        let version = build_test_version(
            DatabaseObjectType::View,
            "create view users_view as select 1",
        );

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_ok());

        let planning_result = result.expect("restore planning should succeed for mysql views");
        match planning_result {
            RestorePlanningResult::Plan(plan) => {
                assert_eq!(plan.statements.len(), 2);
                assert_eq!(
                    plan.statements[0],
                    "DROP VIEW IF EXISTS \"public\".\"users_view\""
                );
                assert_eq!(
                    plan.statements[1],
                    "NORMALIZED::create view users_view as select 1"
                );
                assert!(plan.preview_sql.contains("DROP VIEW IF EXISTS"));
                assert!(plan.preview_sql.contains("NORMALIZED::"));
            }
            _ => panic!("expected a concrete restore plan"),
        }
    }

    #[tokio::test]
    async fn restore_plan_keeps_original_statement_for_postgres_view() {
        let connection = std::sync::Arc::new(RestorePlannerTestConnection::new(Some("postgres")));
        let version = build_test_version(
            DatabaseObjectType::View,
            "CREATE VIEW users_view AS SELECT 1",
        );

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_ok());

        let planning_result = result.expect("restore planning should succeed for postgres views");
        match planning_result {
            RestorePlanningResult::Plan(plan) => {
                assert_eq!(
                    plan.statements,
                    vec!["CREATE VIEW users_view AS SELECT 1".to_string()]
                );
                assert_eq!(
                    plan.preview_sql,
                    "CREATE VIEW users_view AS SELECT 1;".to_string()
                );
            }
            _ => panic!("expected a concrete restore plan"),
        }
    }

    #[tokio::test]
    async fn restore_plan_returns_no_changes_for_matching_table_snapshot() {
        let connection_impl =
            std::sync::Arc::new(RestorePlannerTestConnection::new(Some("sqlite")));
        let connection: std::sync::Arc<dyn Connection> = connection_impl.clone();
        let table_snapshot = build_test_table_details(&["id", "name"]);
        connection_impl.set_current_table_snapshot(table_snapshot.clone());
        let version = build_test_table_version(
            &serde_json::to_string_pretty(&table_snapshot)
                .expect("table snapshot should serialize for restore planning"),
        );

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_ok());

        let planning_result =
            result.expect("table restore planning should return a typed no-changes result");
        assert!(matches!(planning_result, RestorePlanningResult::NoChanges));
    }

    #[tokio::test]
    async fn restore_plan_builds_table_plan_for_supported_table_snapshot_diff() {
        let connection_impl =
            std::sync::Arc::new(RestorePlannerTestConnection::new(Some("sqlite")));
        let connection: std::sync::Arc<dyn Connection> = connection_impl.clone();
        let current_snapshot = build_test_table_details(&["id"]);
        let target_snapshot = build_test_table_details(&["id", "name"]);
        connection_impl.set_current_table_snapshot(current_snapshot);
        let version = build_test_table_version(
            &serde_json::to_string_pretty(&target_snapshot)
                .expect("target table snapshot should serialize for restore planning"),
        );

        let result = build_restore_plan(connection, version).await;
        assert!(result.is_ok());

        let planning_result =
            result.expect("table restore planning should succeed for supported table snapshots");
        match planning_result {
            RestorePlanningResult::Plan(plan) => {
                assert!(
                    !plan.statements.is_empty(),
                    "table restore should emit at least one migration statement"
                );
                assert!(
                    plan.preview_sql.to_lowercase().contains("name"),
                    "preview SQL should include migrated table shape"
                );
            }
            _ => panic!("expected a concrete table restore plan"),
        }
    }

    #[tokio::test]
    async fn apply_restore_plan_executes_all_statements_and_records_restore_commit() {
        let connection_impl =
            std::sync::Arc::new(RestorePlannerTestConnection::new(Some("sqlite")));
        let connection: std::sync::Arc<dyn Connection> = connection_impl.clone();
        let temp_directory = tempdir().expect("temporary directory should be created");
        let database_path = temp_directory.path().join("restore-planner-tests.db");
        let storage = std::sync::Arc::new(
            VersionStorage::with_path(database_path)
                .expect("version storage should initialize for restore planner tests"),
        );
        let repository = VersionRepository::with_storage(storage);

        let version = VersionEntry::new(
            Uuid::new_v4(),
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "calculate_total".to_string(),
            "CREATE OR REPLACE FUNCTION calculate_total() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql"
                .to_string(),
            "original".to_string(),
            None,
        );
        let plan = super::RestorePlan {
            version: version.clone(),
            statements: vec![
                "CREATE OR REPLACE FUNCTION calculate_total() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql"
                    .to_string(),
                "COMMENT ON FUNCTION calculate_total() IS 'restored'".to_string(),
            ],
            preview_sql: "preview".to_string(),
        };

        let apply_result = apply_restore_plan(connection, &repository, &plan).await;
        assert!(apply_result.is_ok());

        let apply_result = apply_result.expect("restore apply should succeed");
        assert_eq!(apply_result.statements_applied, 2);
        assert!(apply_result.restore_commit_recorded);
        assert!(apply_result.restore_commit_record_error.is_none());
        assert_eq!(connection_impl.executed_statements(), plan.statements);

        let versions_result = repository.get_versions(
            version.connection_id,
            &make_object_id(version.object_schema.as_deref(), &version.object_name),
        );
        assert!(versions_result.is_ok());
        let versions = versions_result.expect("history lookup should succeed");
        assert_eq!(versions.len(), 1);
        assert_eq!(
            versions[0].message,
            version_restore_commit_message(&version),
            "restore apply should write a restore commit marker"
        );
    }
}
