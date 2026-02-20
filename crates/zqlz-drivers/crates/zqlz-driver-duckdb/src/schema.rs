//! DuckDB schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectType,
    PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection, SequenceInfo,
    TableDetails, TableInfo, TableType, TriggerInfo, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use super::DuckDbConnection;

#[async_trait]
impl SchemaIntrospection for DuckDbConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query("SELECT database_name, path FROM duckdb_databases()", &[])
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
                comment: row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()),
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let result = self
            .query(
                "SELECT schema_name FROM information_schema.schemata 
                 WHERE catalog_name = current_database() ORDER BY schema_name",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| SchemaInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT table_name, estimated_size, column_count 
                     FROM duckdb_tables() WHERE schema_name = '{}'",
                    schema
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
                schema: Some(schema.to_string()),
                table_type: TableType::Table,
                owner: None,
                row_count: row.get(1).and_then(|v| v.as_i64()),
                size_bytes: None,
                comment: None,
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            })
            .collect())
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = '{}'",
                    schema
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| ViewInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(schema.to_string()),
                is_materialized: false,
                definition: row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()),
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let schema = schema.unwrap_or("main");
        let tables = self.list_tables(Some(schema)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        Ok(TableDetails {
            info,
            columns: self.get_columns(Some(schema), name).await?,
            primary_key: self.get_primary_key(Some(schema), name).await?,
            foreign_keys: self.get_foreign_keys(Some(schema), name).await?,
            indexes: self.get_indexes(Some(schema), name).await?,
            constraints: self.get_constraints(Some(schema), name).await?,
            triggers: Vec::new(), // DuckDB has no triggers
        })
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT column_name, column_index, data_type, is_nullable, column_default
                     FROM duckdb_columns() 
                     WHERE schema_name = '{}' AND table_name = '{}'
                     ORDER BY column_index",
                    schema, table
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| ColumnInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize,
                data_type: row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                nullable: row.get(3).and_then(|v| v.as_bool()).unwrap_or(true),
                default_value: row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string()),
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: false, // Set via constraints
                is_auto_increment: false,
                is_unique: false,
                foreign_key: None,
                comment: None,
                ..Default::default()
            })
            .collect())
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT index_name, is_unique, is_primary, sql
                     FROM duckdb_indexes() 
                     WHERE schema_name = '{}' AND table_name = '{}'",
                    schema, table
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
                columns: Vec::new(), // DuckDB doesn't expose index columns easily
                is_unique: row.get(1).and_then(|v| v.as_bool()).unwrap_or(false),
                is_primary: row.get(2).and_then(|v| v.as_bool()).unwrap_or(false),
                index_type: "ART".to_string(), // DuckDB uses ART indexes
                comment: None,
                ..Default::default()
            })
            .collect())
    }

    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT 
                        tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS ref_table,
                        ccu.table_schema AS ref_schema,
                        ccu.column_name AS ref_column
                     FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                     JOIN information_schema.constraint_column_usage ccu
                        ON tc.constraint_name = ccu.constraint_name
                     WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_schema = '{}' AND tc.table_name = '{}'",
                    schema, table
                ),
                &[],
            )
            .await?;

        let mut fk_map: std::collections::HashMap<String, ForeignKeyInfo> =
            std::collections::HashMap::new();

        for row in result.rows.iter() {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let column = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_table = row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_schema = row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
            let ref_column = row
                .get(4)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            fk_map
                .entry(name.clone())
                .and_modify(|fk| {
                    fk.columns.push(column.clone());
                    fk.referenced_columns.push(ref_column.clone());
                })
                .or_insert(ForeignKeyInfo {
                    name,
                    columns: vec![column],
                    referenced_table: ref_table,
                    referenced_schema: ref_schema,
                    referenced_columns: vec![ref_column],
                    on_update: ForeignKeyAction::NoAction,
                    on_delete: ForeignKeyAction::NoAction,
                    is_deferrable: false,
                    initially_deferred: false,
                });
        }

        Ok(fk_map.into_values().collect())
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT constraint_name, column_name
                     FROM information_schema.key_column_usage kcu
                     JOIN information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name
                     WHERE tc.constraint_type = 'PRIMARY KEY' 
                        AND tc.table_schema = '{}' AND tc.table_name = '{}'
                     ORDER BY kcu.ordinal_position",
                    schema, table
                ),
                &[],
            )
            .await?;

        if result.rows.is_empty() {
            return Ok(None);
        }

        let name = result.rows[0]
            .get(0)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Some(PrimaryKeyInfo { name, columns }))
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT constraint_name, constraint_type
                     FROM information_schema.table_constraints
                     WHERE table_schema = '{}' AND table_name = '{}'",
                    schema, table
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let constraint_type = match row.get(1).and_then(|v| v.as_str()).unwrap_or("") {
                    "PRIMARY KEY" => ConstraintType::PrimaryKey,
                    "FOREIGN KEY" => ConstraintType::ForeignKey,
                    "UNIQUE" => ConstraintType::Unique,
                    "CHECK" => ConstraintType::Check,
                    _ => ConstraintType::Check,
                };
                ConstraintInfo {
                    name,
                    constraint_type,
                    columns: Vec::new(),
                    definition: None,
                }
            })
            .collect())
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        // DuckDB has built-in functions but no user-defined functions in the traditional sense
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // DuckDB does not support stored procedures
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        // DuckDB does not support triggers
        Ok(Vec::new())
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT sequence_name, start_value, min_value, max_value, increment_by
                     FROM duckdb_sequences() WHERE schema_name = '{}'",
                    schema
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| SequenceInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(schema.to_string()),
                data_type: "BIGINT".to_string(),
                start_value: row.get(1).and_then(|v| v.as_i64()).unwrap_or(1),
                min_value: row.get(2).and_then(|v| v.as_i64()).unwrap_or(1),
                max_value: row.get(3).and_then(|v| v.as_i64()).unwrap_or(i64::MAX),
                increment_by: row.get(4).and_then(|v| v.as_i64()).unwrap_or(1),
                current_value: None,
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                &format!(
                    "SELECT type_name, type_category FROM duckdb_types() 
                     WHERE schema_name = '{}' AND type_category = 'ENUM'",
                    schema
                ),
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
                schema: Some(schema.to_string()),
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
            ObjectType::Table => {
                let schema = object.schema.as_deref().unwrap_or("main");
                let table = self.get_table(Some(schema), &object.name).await?;
                Ok(generate_table_ddl(&table, schema))
            }
            ObjectType::View => {
                let schema = object.schema.as_deref().unwrap_or("main");
                let views = self.list_views(Some(schema)).await?;
                let view = views
                    .into_iter()
                    .find(|v| v.name == object.name)
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("View '{}' not found", object.name))
                    })?;
                Ok(view.definition.unwrap_or_else(|| {
                    format!(
                        "-- View definition not available for {}.{}",
                        schema, object.name
                    )
                }))
            }
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation not implemented for {:?}",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        // DuckDB doesn't expose dependency information easily
        Ok(Vec::new())
    }
}

fn generate_table_ddl(table: &TableDetails, schema: &str) -> String {
    let mut ddl = format!("CREATE TABLE \"{}\".\"{}\" (\n", schema, table.info.name);

    for (i, col) in table.columns.iter().enumerate() {
        let comma = if i < table.columns.len() - 1 || table.primary_key.is_some() {
            ","
        } else {
            ""
        };
        let null_str = if col.nullable { "" } else { " NOT NULL" };
        let default_str = col
            .default_value
            .as_ref()
            .map(|d| format!(" DEFAULT {}", d))
            .unwrap_or_default();

        ddl.push_str(&format!(
            "    \"{}\" {}{}{}{}\n",
            col.name, col.data_type, null_str, default_str, comma
        ));
    }

    if let Some(pk) = &table.primary_key {
        let pk_cols = pk
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        ddl.push_str(&format!("    PRIMARY KEY ({})\n", pk_cols));
    }

    ddl.push_str(");\n");
    ddl
}
