//! ClickHouse schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectType, PrimaryKeyInfo, ProcedureInfo,
    Result, SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType,
    TriggerInfo, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use super::ClickHouseConnection;

#[async_trait]
impl SchemaIntrospection for ClickHouseConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query("SELECT name, engine FROM system.databases", &[])
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| DatabaseInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                owner: None,
                encoding: None,
                size_bytes: None,
                comment: row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|s| format!("Engine: {}", s)),
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        // ClickHouse databases act as schemas
        let dbs = self.list_databases().await?;
        Ok(dbs
            .into_iter()
            .map(|db| SchemaInfo {
                name: db.name,
                owner: None,
                comment: db.comment,
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                &format!(
                    "SELECT name, engine, total_rows, total_bytes, comment
                     FROM system.tables
                     WHERE database = '{}' AND is_temporary = 0 AND engine NOT LIKE '%View%'
                     ORDER BY name",
                    database
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| TableInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(database.to_string()),
                table_type: TableType::Table,
                owner: None,
                row_count: row.get(2).and_then(|v| v.as_i64()),
                size_bytes: row.get(3).and_then(|v| v.as_i64()),
                comment: row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string()),
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            })
            .collect())
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                &format!(
                    "SELECT name, engine, as_select, comment
                     FROM system.tables
                     WHERE database = '{}' AND engine LIKE '%View%'
                     ORDER BY name",
                    database
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let engine = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
                ViewInfo {
                    name: row
                        .get(0)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    schema: Some(database.to_string()),
                    is_materialized: engine.contains("Materialized"),
                    definition: row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    owner: None,
                    comment: row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string()),
                }
            })
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let database = schema.unwrap_or(self.database());
        let tables = self.list_tables(Some(database)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        Ok(TableDetails {
            info,
            columns: self.get_columns(Some(database), name).await?,
            primary_key: self.get_primary_key(Some(database), name).await?,
            foreign_keys: Vec::new(), // ClickHouse has no foreign keys
            indexes: self.get_indexes(Some(database), name).await?,
            constraints: self.get_constraints(Some(database), name).await?,
            triggers: Vec::new(), // ClickHouse has no triggers
        })
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                &format!(
                    "SELECT name, position, type, default_kind, default_expression, comment, is_in_primary_key
                     FROM system.columns
                     WHERE database = '{}' AND table = '{}'
                     ORDER BY position",
                    database, table
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let nullable = data_type.starts_with("Nullable");
                let default_kind = row.get(3).and_then(|v| v.as_str()).unwrap_or("");
                let default_expr = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let is_pk = row.get(6).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

                ColumnInfo {
                    name: row
                        .get(0)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize,
                    data_type,
                    nullable,
                    default_value: if default_kind.is_empty() {
                        None
                    } else {
                        default_expr
                    },
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: is_pk,
                    is_auto_increment: false, // ClickHouse doesn't have auto-increment
                    is_unique: false,
                    foreign_key: None,
                    comment: row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    ..Default::default()
                }
            })
            .collect())
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let database = schema.unwrap_or(self.database());
        // ClickHouse stores index info in system.data_skipping_indices
        let result = self
            .query(
                &format!(
                    "SELECT name, expr, type
                     FROM system.data_skipping_indices
                     WHERE database = '{}' AND table = '{}'",
                    database, table
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| IndexInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                columns: vec![], // Expression-based, not column-based
                is_unique: false,
                is_primary: false,
                index_type: row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                comment: row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|s| format!("expr: {}", s)),
                ..Default::default()
            })
            .collect())
    }

    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // ClickHouse does not support foreign keys
        Ok(Vec::new())
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let database = schema.unwrap_or(self.database());
        // Get primary key columns from system.columns
        let result = self
            .query(
                &format!(
                    "SELECT name FROM system.columns
                     WHERE database = '{}' AND table = '{}' AND is_in_primary_key = 1
                     ORDER BY position",
                    database, table
                ),
                &[],
            )
            .await?;

        if result.rows.is_empty() {
            return Ok(None);
        }

        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Some(PrimaryKeyInfo {
            name: None,
            columns,
        }))
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        // ClickHouse has limited constraint support - mainly through ASSUME expressions
        let pk = self.get_primary_key(schema, table).await?;
        let mut constraints = Vec::new();

        if let Some(pk) = pk {
            constraints.push(ConstraintInfo {
                name: "PRIMARY KEY".to_string(),
                constraint_type: ConstraintType::PrimaryKey,
                columns: pk.columns,
                definition: None,
            });
        }

        Ok(constraints)
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        // ClickHouse has built-in functions but user-defined functions are limited
        // List system functions from system.functions
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // ClickHouse does not support stored procedures
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        // ClickHouse does not support triggers
        Ok(Vec::new())
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        // ClickHouse does not have sequences
        Ok(Vec::new())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        // List Enum types from system.data_type_families
        let result = self
            .query(
                "SELECT name FROM system.data_type_families WHERE alias_to = '' AND name LIKE 'Enum%'",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| TypeInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: None,
                type_kind: TypeKind::Enum,
                values: None,
                definition: None,
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        match object.object_type {
            ObjectType::Table | ObjectType::View => {
                let database = object.schema.as_deref().unwrap_or(self.database());
                let result = self
                    .query(
                        &format!("SHOW CREATE TABLE `{}`.`{}`", database, object.name),
                        &[],
                    )
                    .await?;

                if let Some(row) = result.rows.first()
                    && let Some(ddl) = row.get(0).and_then(|v| v.as_str())
                {
                    return Ok(ddl.to_string());
                }
                Err(ZqlzError::NotFound(format!(
                    "DDL not found for {}.{}",
                    database, object.name
                )))
            }
            ObjectType::Database => Ok(format!("CREATE DATABASE `{}`", object.name)),
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation not implemented for {:?}",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        // ClickHouse doesn't expose dependency information
        Ok(Vec::new())
    }
}
