//! PostgreSQL schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo,
    SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent,
    TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use crate::PostgresConnection;

#[async_trait]
impl SchemaIntrospection for PostgresConnection {
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT datname, pg_database_size(datname) as size_bytes, pg_encoding_to_char(encoding) as encoding
                 FROM pg_database 
                 WHERE datistemplate = false 
                 ORDER BY datname",
                &[],
            )
            .await?;

        let databases = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let size_bytes = row.get(1).and_then(|v| v.as_i64());
                let encoding = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                DatabaseInfo {
                    name,
                    owner: None,
                    encoding,
                    size_bytes,
                    comment: None,
                }
            })
            .collect();

        Ok(databases)
    }

    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let result = self
            .query(
                "SELECT schema_name 
                 FROM information_schema.schemata 
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                 ORDER BY schema_name",
                &[],
            )
            .await?;

        let schemas = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                SchemaInfo {
                    name,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(schemas)
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT 
                    t.table_name,
                    t.table_type,
                    pg_stat.n_live_tup as row_count,
                    pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) as size_bytes
                 FROM information_schema.tables t
                 LEFT JOIN pg_stat_user_tables pg_stat ON t.table_name = pg_stat.relname AND t.table_schema = pg_stat.schemaname
                 WHERE t.table_schema = $1
                 ORDER BY t.table_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_type_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("BASE TABLE");
                let row_count = row.get(2).and_then(|v| v.as_i64());
                let size_bytes = row.get(3).and_then(|v| v.as_i64());

                let table_type = match table_type_str {
                    "BASE TABLE" => TableType::Table,
                    "VIEW" => TableType::View,
                    _ => TableType::Table,
                };

                TableInfo {
                    name,
                    schema: Some(schema.to_string()),
                    table_type,
                    owner: None,
                    row_count,
                    size_bytes,
                    comment: None,
                    index_count: None,
                    trigger_count: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(tables)
    }

    #[tracing::instrument(skip(self))]
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT table_name, view_definition 
                 FROM information_schema.views 
                 WHERE table_schema = $1
                 ORDER BY table_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let views = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                ViewInfo {
                    name,
                    schema: Some(schema.to_string()),
                    is_materialized: false,
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
    async fn list_materialized_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT matviewname, definition
                 FROM pg_matviews
                 WHERE schemaname = $1
                 ORDER BY matviewname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let views = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                ViewInfo {
                    name,
                    schema: Some(schema.to_string()),
                    is_materialized: true,
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let schema = schema.unwrap_or("public");
        let tables = self.list_tables(Some(schema)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        let columns = self.get_columns(Some(schema), name).await?;
        let indexes = self.get_indexes(Some(schema), name).await?;
        let foreign_keys = self.get_foreign_keys(Some(schema), name).await?;
        let primary_key = self.get_primary_key(Some(schema), name).await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints: Vec::new(),
            triggers: Vec::new(),
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                 "SELECT 
                    column_name, 
                    ordinal_position,
                    data_type, 
                    is_nullable, 
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_identity
                 FROM information_schema.columns 
                 WHERE table_schema = $1 AND table_name = $2
                 ORDER BY ordinal_position",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let columns = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let ordinal = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_nullable = row.get(3).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
                let default_value = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let max_length = row.get(5).and_then(|v| v.as_i64());
                let precision = row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32);
                let scale = row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32);
                let is_identity = row.get(8).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
                let is_auto_increment = is_identity
                    || default_value
                        .as_ref()
                        .map(|default| default.to_lowercase().contains("nextval("))
                        .unwrap_or(false);

                ColumnInfo {
                    name,
                    ordinal,
                    data_type,
                    nullable: is_nullable,
                    default_value,
                    max_length,
                    precision,
                    scale,
                    is_primary_key: false, // Will be filled by get_primary_key
                    is_auto_increment,
                    is_unique: false,
                    foreign_key: None,
                    comment: None,
                    ..Default::default()
                }
            })
            .collect();

        Ok(columns)
    }

    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let schema = schema.unwrap_or("public");
        // indnkeyatts is the number of key columns (introduced in PostgreSQL 11).
        // Columns beyond that index are non-key INCLUDE columns.  We use a fallback
        // of array_length(ix.indkey, 1) so the query works on older PostgreSQL versions.
        let result = self
            .query(
                "SELECT
                    i.relname AS index_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary,
                    array_agg(
                        a.attname
                        ORDER BY array_position(ix.indkey, a.attnum)
                    ) FILTER (
                        WHERE a.attnum <= coalesce(ix.indnkeyatts, array_length(ix.indkey, 1))
                    ) AS key_columns,
                    array_agg(
                        a.attname
                        ORDER BY array_position(ix.indkey, a.attnum)
                    ) FILTER (
                        WHERE a.attnum > coalesce(ix.indnkeyatts, array_length(ix.indkey, 1))
                    ) AS include_columns,
                    am.amname AS index_method,
                    pg_get_expr(ix.indpred, ix.indrelid) AS where_clause
                 FROM pg_class t
                 JOIN pg_index ix ON t.oid = ix.indrelid
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_am am ON am.oid = i.relam
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE n.nspname = $1 AND t.relname = $2
                 GROUP BY i.relname, ix.indisunique, ix.indisprimary, ix.indnkeyatts,
                          ix.indkey, ix.indpred, ix.indrelid, am.amname
                 ORDER BY i.relname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let indexes = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let is_unique = row.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                let is_primary = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                let columns = row
                    .get(3)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();
                let include_columns = row
                    .get(4)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();
                let index_type = row
                    .get(5)
                    .and_then(|v| v.as_str())
                    .unwrap_or("btree")
                    .to_string();
                let where_clause = row.get(6).and_then(|v| v.as_str()).map(|s| s.to_string());

                Some(IndexInfo {
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                    comment: None,
                    where_clause,
                    include_columns,
                    column_descending: vec![],
                })
            })
            .collect();

        Ok(indexes)
    }

    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT 
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                 FROM information_schema.table_constraints AS tc
                 JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 JOIN information_schema.constraint_column_usage AS ccu
                   ON ccu.constraint_name = tc.constraint_name
                   AND ccu.table_schema = tc.table_schema
                 JOIN information_schema.referential_constraints AS rc
                   ON rc.constraint_name = tc.constraint_name
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND tc.table_schema = $1
                   AND tc.table_name = $2",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let fks = result
            .rows
            .iter()
            .map(|row| {
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
                let ref_column = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let on_update_str = row.get(4).and_then(|v| v.as_str()).unwrap_or("NO ACTION");
                let on_delete_str = row.get(5).and_then(|v| v.as_str()).unwrap_or("NO ACTION");

                ForeignKeyInfo {
                    name,
                    columns: vec![column],
                    referenced_table: ref_table,
                    referenced_schema: Some(schema.to_string()),
                    referenced_columns: vec![ref_column],
                    on_update: parse_fk_action(on_update_str),
                    on_delete: parse_fk_action(on_delete_str),
                    is_deferrable: false,
                    initially_deferred: false,
                }
            })
            .collect();

        Ok(fks)
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT 
                    tc.constraint_name,
                    array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = $1
                   AND tc.table_name = $2
                 GROUP BY tc.constraint_name",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
            // Parse the array_agg column which returns a string array
            let columns = row
                .get(1)
                .and_then(|v| v.as_string_array())
                .unwrap_or_default();

            Ok(Some(PrimaryKeyInfo { name, columns }))
        } else {
            Ok(None)
        }
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let schema = schema.unwrap_or("public");
        // pg_get_constraintdef returns the full constraint definition including the CHECK keyword;
        // we store it as-is so importers have the verbatim expression without needing to
        // reconstruct it from raw column-level data.
        let result = self
            .query(
                "SELECT
                    con.conname AS constraint_name,
                    pg_get_constraintdef(con.oid) AS definition,
                    array_agg(att.attname ORDER BY att.attnum) AS columns
                 FROM pg_constraint con
                 JOIN pg_class rel ON rel.oid = con.conrelid
                 JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                 LEFT JOIN pg_attribute att
                   ON att.attrelid = rel.oid
                   AND att.attnum = ANY(con.conkey)
                 WHERE con.contype = 'c'
                   AND nsp.nspname = $1
                   AND rel.relname = $2
                 GROUP BY con.conname, con.oid
                 ORDER BY con.conname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let constraints = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                let columns = row
                    .get(2)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();

                Some(ConstraintInfo {
                    name,
                    constraint_type: ConstraintType::Check,
                    columns,
                    definition,
                })
            })
            .collect();

        Ok(constraints)
    }

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT 
                    p.proname as function_name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    t.typname as return_type
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 JOIN pg_type t ON p.prorettype = t.oid
                 WHERE n.nspname = $1
                   AND p.prokind = 'f'
                 ORDER BY p.proname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let functions = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let return_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                FunctionInfo {
                    name,
                    schema: Some(schema.to_string()),
                    language: "sql".to_string(), // Default to SQL
                    return_type,
                    parameters: vec![], // TODO: Parse parameters properly
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(functions)
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT 
                    p.proname as procedure_name,
                    pg_get_function_identity_arguments(p.oid) as arguments
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 WHERE n.nspname = $1
                   AND p.prokind = 'p'
                 ORDER BY p.proname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let procedures = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                ProcedureInfo {
                    name,
                    schema: Some(schema.to_string()),
                    language: "sql".to_string(), // Default to SQL
                    parameters: vec![],          // TODO: Parse parameters properly
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(procedures)
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let schema = schema.unwrap_or("public");

        let result = if let Some(tbl) = table {
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname = $1 AND c.relname = $2
                   AND NOT t.tgisinternal
                 ORDER BY t.tgname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(tbl.to_string()),
                ],
            )
            .await?
        } else {
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname = $1
                   AND NOT t.tgisinternal
                 ORDER BY t.tgname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        };

        let triggers = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                TriggerInfo {
                    name,
                    schema: Some(schema.to_string()),
                    table_name,
                    timing: TriggerTiming::Before, // TODO: Parse from definition
                    events: vec![TriggerEvent::Insert], // TODO: Parse from definition
                    for_each: TriggerForEach::Row,
                    definition,
                    enabled: true,
                    comment: None,
                }
            })
            .collect();

        Ok(triggers)
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT sequence_name 
                 FROM information_schema.sequences 
                 WHERE sequence_schema = $1
                 ORDER BY sequence_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let sequences = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                SequenceInfo {
                    name,
                    schema: Some(schema.to_string()),
                    data_type: "bigint".to_string(),
                    start_value: 1,
                    min_value: 1,
                    max_value: i64::MAX,
                    increment_by: 1,
                    current_value: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(sequences)
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        let schema = schema.unwrap_or("public");
        let result = self
            .query(
                "SELECT typname, typtype
                 FROM pg_type t
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 WHERE n.nspname = $1
                   AND typtype = 'e'
                 ORDER BY typname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let types = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                TypeInfo {
                    name,
                    schema: Some(schema.to_string()),
                    type_kind: TypeKind::Enum,
                    values: None, // TODO: Fetch enum values
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(types)
    }

    async fn generate_ddl(&self, _object: &DatabaseObject) -> Result<String> {
        Err(ZqlzError::NotImplemented(
            "DDL generation not yet implemented for PostgreSQL".into(),
        ))
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let schema = schema.unwrap_or("public");

        let result = self
            .query(
                "SELECT
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    CASE c.relkind
                        WHEN 'r' THEN 'Table'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned Table'
                        ELSE 'Other'
                    END AS table_type,
                    CASE WHEN c.relkind = 'p' THEN 'Yes' ELSE 'No' END AS partitioned,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    CASE WHEN c.relkind IN ('v', 'm') THEN '-'
                         ELSE pg_size_pretty(pg_total_relation_size(c.oid))
                    END AS size,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname = $1
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("oid", "OID")
                .width(70.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("owner", "Owner")
                .width(100.0)
                .min_width(60.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("table_type", "Type")
                .width(120.0)
                .min_width(60.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("partitioned", "Partitioned")
                .width(90.0)
                .min_width(60.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("size", "Size")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("primary_key", "Primary Key")
                .width(120.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("foreign_server", "Foreign Server")
                .width(120.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("options", "Options")
                .width(150.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("inherits_tables", "Inherits Tables")
                .width(130.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("inherited_tables_count", "Inherited Count")
                .width(110.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("unlogged", "Unlogged")
                .width(80.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("system_table", "System Table")
                .width(100.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(200.0)
                .min_width(80.0)
                .resizable(true),
        ];

        // Column indices in the query result
        let col_ids = [
            "oid",
            "name",
            "owner",
            "table_type",
            "partitioned",
            "row_count",
            "size",
            "primary_key",
            "foreign_server",
            "options",
            "inherits_tables",
            "inherited_tables_count",
            "unlogged",
            "system_table",
            "comment",
        ];

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut values = std::collections::BTreeMap::new();

                for (query_idx, col_id) in col_ids.iter().enumerate() {
                    let display_value = row
                        .get(query_idx)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string());

                    let display_value = if display_value == "NULL" {
                        "-".to_string()
                    } else {
                        display_value
                    };

                    values.insert(col_id.to_string(), display_value);
                }

                let name = values.get("name").cloned().unwrap_or_default();

                let table_type_str = values.get("table_type").map(|s| s.as_str()).unwrap_or("");
                let object_type = match table_type_str {
                    "View" | "Materialized View" => "view",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name,
                    object_type: object_type.to_string(),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData { columns, rows })
    }
}

fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}
