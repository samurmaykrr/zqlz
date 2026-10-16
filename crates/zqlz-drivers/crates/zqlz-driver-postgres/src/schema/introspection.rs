//! `SchemaIntrospection` implementation for `PostgresConnection`: metadata,
//! relation/routine/type listing, DDL generation, and objects-panel/
//! object-form entry points. This stays a single `impl` block (Rust does not
//! allow splitting one trait impl across files); the column/foreign-key
//! query builders, object-form helpers, and objects-panel data live in
//! sibling modules and are called from here.

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest,
    ObjectFormMode, ObjectFormSpec, ObjectFormSpecRequest, ObjectType,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectRef, ObjectsPanelRow,
    PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection,
    SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent, TriggerForEach, TriggerInfo,
    TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use crate::PostgresConnection;

use super::{postgres_type_kind_from_typtype, resolve_relation_identifiers, schema_qualified_name};

#[async_trait]
impl SchemaIntrospection for PostgresConnection {
    fn object_kind_support(&self) -> Option<zqlz_core::ObjectKindSupport> {
        Some(crate::driver::OBJECT_KINDS)
    }

    fn default_schema_name(&self) -> Option<String> {
        Some("public".to_string())
    }

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
                   AND schema_name NOT LIKE 'pg_temp_%'
                   AND schema_name NOT LIKE 'pg_toast_temp_%'
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    c.relname,
                    c.relkind,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE s.n_live_tup END AS row_count,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE pg_total_relation_size(c.oid) END AS size_bytes,
                    obj_description(c.oid, 'pg_class') AS comment
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                 WHERE n.nspname = $1
                   AND c.relkind IN ('r', 'p', 'f')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    c.relname,
                    c.relkind,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE s.n_live_tup END AS row_count,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE pg_total_relation_size(c.oid) END AS size_bytes,
                    obj_description(c.oid, 'pg_class') AS comment
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND c.relkind IN ('r', 'p', 'f')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await?
        };

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let relkind = row.get(2).and_then(|v| v.as_str()).unwrap_or("r");
                let row_count = row.get(3).and_then(|v| v.as_i64());
                let size_bytes = row.get(4).and_then(|v| v.as_i64());
                let comment = row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string());

                let table_type = match relkind {
                    "p" => TableType::PartitionedTable,
                    "f" => TableType::ForeignTable,
                    _ => TableType::Table,
                };

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                TableInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    table_type,
                    owner: None,
                    row_count,
                    size_bytes,
                    comment,
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT table_schema, table_name, view_definition
                 FROM information_schema.views
                 WHERE table_schema = $1
                 ORDER BY table_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT table_schema, table_name, view_definition
                 FROM information_schema.views
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND table_schema NOT LIKE 'pg_temp_%'
                   AND table_schema NOT LIKE 'pg_toast_temp_%'
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await?
        };

        let views = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ViewInfo {
                    name: display_name,
                    schema: Some(schema_name),
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT schemaname, matviewname, definition
                 FROM pg_matviews
                 WHERE schemaname = $1
                 ORDER BY matviewname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT schemaname, matviewname, definition
                 FROM pg_matviews
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND schemaname NOT LIKE 'pg_temp_%'
                   AND schemaname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY schemaname, matviewname",
                &[],
            )
            .await?
        };

        let views = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ViewInfo {
                    name: display_name,
                    schema: Some(schema_name),
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
        self.schema_engine.get_table(schema, name).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        self.schema_engine.get_columns(schema, table).await
    }

    async fn list_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ColumnInfo>>>> {
        self.schema_engine.list_all_columns(schema).await
    }

    async fn list_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ForeignKeyInfo>>>> {
        self.schema_engine.list_all_foreign_keys(schema).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        self.schema_engine.get_indexes(schema, table).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        self.schema_engine.get_foreign_keys(schema, table).await
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        self.schema_engine.get_primary_key(schema, table).await
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        self.schema_engine.get_constraints(schema, table).await
    }

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
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
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as function_name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    t.typname as return_type
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 JOIN pg_type t ON p.prorettype = t.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        let functions = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let return_type = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                FunctionInfo {
                    name: display_name,
                    schema: Some(schema_name),
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as procedure_name,
                    pg_get_function_identity_arguments(p.oid) as arguments
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 WHERE n.nspname = $1
                   AND p.prokind = 'p'
                 ORDER BY p.proname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as procedure_name,
                    pg_get_function_identity_arguments(p.oid) as arguments
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        let procedures = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ProcedureInfo {
                    name: display_name,
                    schema: Some(schema_name),
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
        let default_schema = schema.unwrap_or("public");
        let (resolved_schema, table_name_filter) = table
            .map(|table_name| resolve_relation_identifiers(schema, table_name, default_schema))
            .map_or_else(
                || (schema.map(ToString::to_string), None),
                |(resolved_schema, resolved_table_name)| {
                    (Some(resolved_schema), Some(resolved_table_name))
                },
            );

        let result = if let Some(tbl) = table_name_filter {
            let schema = resolved_schema.unwrap_or_else(|| default_schema.to_string());
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
                    zqlz_core::Value::String(tbl),
                ],
            )
            .await?
        } else if let Some(schema) = resolved_schema {
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
        } else {
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND NOT t.tgisinternal
                 ORDER BY n.nspname, t.tgname",
                &[],
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
                    schema: schema.map(str::to_string),
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT sequence_schema, sequence_name
                 FROM information_schema.sequences
                 WHERE sequence_schema = $1
                 ORDER BY sequence_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT sequence_schema, sequence_name
                 FROM information_schema.sequences
                 WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND sequence_schema NOT LIKE 'pg_temp_%'
                   AND sequence_schema NOT LIKE 'pg_toast_temp_%'
                 ORDER BY sequence_schema, sequence_name",
                &[],
            )
            .await?
        };

        let sequences = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                SequenceInfo {
                    name: display_name,
                    schema: Some(schema_name),
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
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    t.typname,
                    t.typtype,
                    r.rolname,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_enum e
                        WHERE e.enumtypid = t.oid
                    ) AS enum_values
                 FROM pg_type t
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 LEFT JOIN pg_roles r ON r.oid = t.typowner
                 LEFT JOIN pg_class c ON c.oid = t.typrelid
                 WHERE n.nspname = $1
                   AND t.typtype IN ('e', 'd', 'r', 'c')
                   AND (t.typrelid = 0 OR c.relkind = 'c')
                 ORDER BY t.typname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    t.typname,
                    t.typtype,
                    r.rolname,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_enum e
                        WHERE e.enumtypid = t.oid
                    ) AS enum_values
                 FROM pg_type t
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 LEFT JOIN pg_roles r ON r.oid = t.typowner
                 LEFT JOIN pg_class c ON c.oid = t.typrelid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND t.typtype IN ('e', 'd', 'r', 'c')
                   AND (t.typrelid = 0 OR c.relkind = 'c')
                 ORDER BY n.nspname, t.typname",
                &[],
            )
            .await?
        };

        let types = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let typtype = row.get(2).and_then(|v| v.as_str()).unwrap_or("e");
                let owner = row.get(3).and_then(|v| v.as_str()).map(str::to_string);
                let values = row.get(4).and_then(|value| value.as_string_array());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };
                let type_kind = postgres_type_kind_from_typtype(typtype);

                TypeInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    type_kind,
                    values,
                    definition: None,
                    owner,
                    comment: None,
                }
            })
            .collect();

        Ok(types)
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let (schema, relation_name) =
            resolve_relation_identifiers(object.schema.as_deref(), object.name.as_str(), "public");
        let name = relation_name.as_str();
        let schema_param = zqlz_core::Value::String(schema.to_string());
        let name_param = zqlz_core::Value::String(name.to_string());

        match object.object_type {
            ObjectType::Table => {
                // Columns with precise type information via pg_catalog functions.
                // format_type() returns the full type string including length/precision
                // modifiers (e.g. "character varying(255)" instead of just "character varying"),
                // which information_schema.columns does not reliably provide.
                let col_result = self
                    .query(
                        "SELECT
                             a.attname,
                             pg_catalog.format_type(a.atttypid, a.atttypmod),
                             a.attnotnull,
                             pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                             a.attidentity,
                             a.attgenerated
                         FROM pg_catalog.pg_attribute a
                         LEFT JOIN pg_catalog.pg_attrdef d
                             ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                         WHERE a.attrelid = (
                             SELECT c.oid FROM pg_catalog.pg_class c
                             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                             WHERE c.relname = $2 AND n.nspname = $1
                         )
                         AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                if col_result.rows.is_empty() {
                    return Err(ZqlzError::NotFound(format!(
                        "Table '{}.{}' not found",
                        schema, name
                    )));
                }

                let mut parts: Vec<String> = col_result
                    .rows
                    .iter()
                    .map(|row| {
                        let col_name = row
                            .get(0)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let col_type = row
                            .get(1)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let not_null = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                        let col_default =
                            row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
                        let identity = row.get(4).and_then(|v| v.as_str()).unwrap_or("");
                        let generated = row.get(5).and_then(|v| v.as_str()).unwrap_or("");

                        let mut def = format!("    \"{}\" {}", col_name, col_type);

                        // Identity and generated columns have their own constraint syntax
                        // that supersedes a plain DEFAULT clause.
                        if identity == "a" {
                            def.push_str(" GENERATED ALWAYS AS IDENTITY");
                        } else if identity == "d" {
                            def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
                        } else if generated == "s" {
                            if let Some(expr) = &col_default {
                                def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", expr));
                            }
                        } else {
                            if let Some(default) = &col_default {
                                def.push_str(&format!(" DEFAULT {}", default));
                            }
                            if not_null {
                                def.push_str(" NOT NULL");
                            }
                        }

                        def
                    })
                    .collect();

                // pg_get_constraintdef() produces authoritative constraint DDL for all
                // inline constraint types: primary key, unique, check, and foreign key.
                let con_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_constraintdef(c.oid, true)
                         FROM pg_catalog.pg_constraint c
                         JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                         WHERE t.relname = $2 AND n.nspname = $1
                           AND c.contype IN ('p', 'u', 'c', 'f')
                         ORDER BY c.contype, c.conname",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                for row in &con_result.rows {
                    if let Some(condef) = row.get(0).and_then(|v| v.as_str()) {
                        parts.push(format!("    {}", condef));
                    }
                }

                let qualified = format!("\"{}\".\"{}\"", schema, name);
                let mut ddl = format!("CREATE TABLE {} (\n{}\n);", qualified, parts.join(",\n"));

                // Non-PK indexes are emitted as separate CREATE INDEX statements
                // following the CREATE TABLE, which is the canonical pg_dump format.
                let idx_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)
                         FROM pg_catalog.pg_index i
                         JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                         WHERE t.relname = $2 AND n.nspname = $1
                           AND NOT i.indisprimary
                         ORDER BY i.indexrelid",
                        &[schema_param, name_param],
                    )
                    .await?;

                for row in &idx_result.rows {
                    if let Some(idx_ddl) = row.get(0).and_then(|v| v.as_str()) {
                        ddl.push('\n');
                        ddl.push_str(idx_ddl);
                        ddl.push(';');
                    }
                }

                Ok(ddl)
            }

            ObjectType::View | ObjectType::MaterializedView => {
                let is_materialized = object.object_type == ObjectType::MaterializedView;
                let kind = if is_materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };

                let result = self
                    .query(
                        "SELECT pg_catalog.pg_get_viewdef(c.oid, true)
                         FROM pg_catalog.pg_class c
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relname = $2 AND n.nspname = $1",
                        &[schema_param, name_param],
                    )
                    .await?;

                let view_def = result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("{} '{}.{}' not found", kind, schema, name))
                    })?;

                // Materialized views do not support OR REPLACE.
                let or_replace = if is_materialized { "" } else { "OR REPLACE " };
                Ok(format!(
                    "CREATE {}{}  \"{}\".\"{}\" AS\n{};",
                    or_replace,
                    kind,
                    schema,
                    name,
                    view_def.trim_end()
                ))
            }

            ObjectType::Function | ObjectType::Procedure => {
                let result = if let Some(signature) = object
                    .signature
                    .as_deref()
                    .map(str::trim)
                    .filter(|signature| !signature.is_empty())
                {
                    self.query(
                        "SELECT pg_catalog.pg_get_functiondef(p.oid)
                         FROM pg_catalog.pg_proc p
                         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                         WHERE p.proname = $2
                           AND n.nspname = $1
                           AND pg_catalog.pg_get_function_identity_arguments(p.oid) = $3
                         LIMIT 1",
                        &[
                            schema_param.clone(),
                            name_param.clone(),
                            zqlz_core::Value::String(signature.to_string()),
                        ],
                    )
                    .await?
                } else {
                    self.query(
                        "SELECT pg_catalog.pg_get_functiondef(p.oid)
                         FROM pg_catalog.pg_proc p
                         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                         WHERE p.proname = $2 AND n.nspname = $1
                         LIMIT 1",
                        &[schema_param, name_param],
                    )
                    .await?
                };

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Function or procedure '{}.{}' not found",
                            schema, name
                        ))
                    })
            }

            ObjectType::Trigger => {
                let scoped_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                         FROM pg_catalog.pg_trigger t
                         JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE t.tgname = $2 AND n.nspname = $1
                           AND NOT t.tgisinternal",
                        &[schema_param, name_param],
                    )
                    .await?;

                if let Some(definition) = scoped_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                {
                    return Ok(format!("{};", definition));
                }

                let fallback_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                         FROM pg_catalog.pg_trigger t
                         JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE t.tgname = $1
                           AND NOT t.tgisinternal
                           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                           AND n.nspname NOT LIKE 'pg_temp_%'
                           AND n.nspname NOT LIKE 'pg_toast_temp_%'
                         ORDER BY n.nspname, c.relname
                         LIMIT 1",
                        &[zqlz_core::Value::String(name.to_string())],
                    )
                    .await?;

                fallback_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| format!("{};", s))
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Trigger '{}' not found in schema '{}'",
                            name, schema
                        ))
                    })
            }

            ObjectType::Index => {
                let result = self
                    .query(
                        "SELECT pg_catalog.pg_get_indexdef(c.oid, 0, true)
                         FROM pg_catalog.pg_class c
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relname = $2 AND n.nspname = $1 AND c.relkind = 'i'",
                        &[schema_param, name_param],
                    )
                    .await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| format!("{};", s))
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("Index '{}.{}' not found", schema, name))
                    })
            }

            ObjectType::Sequence => {
                let result = self
                    .query(
                        "SELECT data_type::text, start_value, min_value, max_value,
                                increment_by, cycle, cache_size
                         FROM pg_catalog.pg_sequences
                         WHERE sequencename = $2 AND schemaname = $1",
                        &[schema_param, name_param],
                    )
                    .await?;

                let row = result.rows.first().ok_or_else(|| {
                    ZqlzError::NotFound(format!("Sequence '{}.{}' not found", schema, name))
                })?;

                let data_type = row.get(0).and_then(|v| v.as_str()).unwrap_or("bigint");
                let start = row.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
                let min_val = row.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
                let max_val = row.get(3).and_then(|v| v.as_i64());
                let increment = row.get(4).and_then(|v| v.as_i64()).unwrap_or(1);
                let cycle = row.get(5).and_then(|v| v.as_bool()).unwrap_or(false);
                let cache = row.get(6).and_then(|v| v.as_i64()).unwrap_or(1);

                let max_clause = match max_val {
                    Some(max) => format!("MAXVALUE {}", max),
                    None => "NO MAXVALUE".to_string(),
                };
                let cycle_clause = if cycle { "CYCLE" } else { "NO CYCLE" };

                Ok(format!(
                    "CREATE SEQUENCE \"{}\".\"{}\" AS {} INCREMENT BY {} MINVALUE {} {} START {} CACHE {} {};",
                    schema,
                    name,
                    data_type,
                    increment,
                    min_val,
                    max_clause,
                    start,
                    cache,
                    cycle_clause
                ))
            }

            ObjectType::Type => {
                // Enum types store their labels in pg_enum ordered by enumsortorder.
                let enum_result = self
                    .query(
                        "SELECT string_agg(quote_literal(e.enumlabel), ', '
                                ORDER BY e.enumsortorder)
                         FROM pg_catalog.pg_type t
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                         JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
                         WHERE t.typname = $2 AND n.nspname = $1 AND t.typtype = 'e'
                         GROUP BY t.oid",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                if let Some(labels) = enum_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                {
                    return Ok(format!(
                        "CREATE TYPE \"{}\".\"{}\" AS ENUM ({});",
                        schema, name, labels
                    ));
                }

                // Composite types are backed by a pg_class row of relkind = 'c'.
                let comp_result = self
                    .query(
                        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                         FROM pg_catalog.pg_type t
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                         JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
                         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                         WHERE t.typname = $2 AND n.nspname = $1 AND t.typtype = 'c'
                           AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum",
                        &[schema_param, name_param],
                    )
                    .await?;

                if !comp_result.rows.is_empty() {
                    let fields: Vec<String> = comp_result
                        .rows
                        .iter()
                        .filter_map(|row| {
                            let fname = row.get(0).and_then(|v| v.as_str())?;
                            let ftype = row.get(1).and_then(|v| v.as_str())?;
                            Some(format!("    \"{}\" {}", fname, ftype))
                        })
                        .collect();
                    return Ok(format!(
                        "CREATE TYPE \"{}\".\"{}\" AS (\n{}\n);",
                        schema,
                        name,
                        fields.join(",\n")
                    ));
                }

                Err(ZqlzError::NotFound(format!(
                    "Type '{}.{}' not found or is of an unsupported kind",
                    schema, name
                )))
            }

            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} is not supported in PostgreSQL",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'),
                        '-'
                    ) AS foreign_schema,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'),
                        '-'
                    ) AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
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
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'),
                        '-'
                    ) AS foreign_schema,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'),
                        '-'
                    ) AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await?
        };

        let columns = Self::postgres_table_objects_panel_columns();

        // Column indices in the query result
        let col_ids = [
            "schema_name",
            "oid",
            "name",
            "owner",
            "acl",
            "table_type",
            "partition_of",
            "row_count",
            "primary_key",
            "has_oids",
            "foreign_server",
            "foreign_schema",
            "foreign_table",
            "options",
            "inherits_tables",
            "inherited_tables_count",
            "fill_factor",
            "unlogged",
            "system_table",
            "comment",
        ];

        let build_metadata_values = |schema_name: String,
                                     display_name: String,
                                     owner: Option<String>,
                                     table_type: &str,
                                     options: Option<String>| {
            let mut values = std::collections::BTreeMap::new();
            values.insert("schema_name".to_string(), schema_name);
            values.insert("oid".to_string(), "-".to_string());
            values.insert("name".to_string(), display_name);
            values.insert(
                "owner".to_string(),
                owner.unwrap_or_else(|| "-".to_string()),
            );
            values.insert("table_type".to_string(), table_type.to_string());
            values.insert("partitioned".to_string(), "-".to_string());
            values.insert("row_count".to_string(), "-".to_string());
            values.insert("size".to_string(), "-".to_string());
            values.insert("primary_key".to_string(), "-".to_string());
            values.insert("foreign_server".to_string(), "-".to_string());
            values.insert(
                "options".to_string(),
                options.unwrap_or_else(|| "-".to_string()),
            );
            values.insert("inherits_tables".to_string(), "-".to_string());
            values.insert("inherited_tables_count".to_string(), "-".to_string());
            values.insert("unlogged".to_string(), "-".to_string());
            values.insert("system_table".to_string(), "No".to_string());
            values.insert("comment".to_string(), "-".to_string());
            values
        };

        let mut rows: Vec<ObjectsPanelRow> = result
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
                let schema_name = values
                    .get("schema_name")
                    .cloned()
                    .unwrap_or_else(|| "public".to_string());
                let object_name = name.clone();
                let object_schema = schema_name.clone();

                let display_name = if schema.is_some() {
                    name.clone()
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                let table_type_str = values.get("table_type").map(|s| s.as_str()).unwrap_or("");
                let object_type = match table_type_str {
                    "View" => "view",
                    "Materialized View" => "materialized_view",
                    "Foreign Table" => "foreign_table",
                    "Partitioned" => "partitioned_table",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, object_name)
                            .with_schema_option(Some(object_schema)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        let function_result = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = $1
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        for row in &function_result.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let function_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let signature = row
                .get(2)
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .filter(|value| !value.is_empty());
            let owner = row
                .get(3)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let language = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
            let return_type = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
            let volatility = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
            let security = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
            let returns_set = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
            let strict = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_cost = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_rows = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
            let configuration_parameters =
                row.get(12).and_then(|value| value.as_str()).unwrap_or("-");
            let comment = row.get(13).and_then(|value| value.as_str()).unwrap_or("-");

            let display_name = {
                let base = if schema.is_some() {
                    function_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &function_name)
                };
                signature
                    .as_ref()
                    .map(|value| format!("{}({})", base, value))
                    .unwrap_or(base)
            };

            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), display_name.clone());
            values.insert("function_type".to_string(), "Function".to_string());
            values.insert(
                "owner".to_string(),
                owner.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "parameter".to_string(),
                signature.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert("language".to_string(), language.to_string());
            values.insert("return_type".to_string(), return_type.to_string());
            values.insert("volatility".to_string(), volatility.to_string());
            values.insert("security".to_string(), security.to_string());
            values.insert("returns_set".to_string(), returns_set.to_string());
            values.insert("strict".to_string(), strict.to_string());
            values.insert("estimated_cost".to_string(), estimated_cost.to_string());
            values.insert("estimated_rows".to_string(), estimated_rows.to_string());
            values.insert(
                "configuration_parameters".to_string(),
                configuration_parameters.to_string(),
            );
            values.insert("comment".to_string(), comment.to_string());

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "function".to_string(),
                object_ref: Some(Self::postgres_routine_object_ref(
                    "function",
                    function_name,
                    schema_name,
                    signature,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let procedure_result = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = $1
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        for row in &procedure_result.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let procedure_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let signature = row
                .get(2)
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .filter(|value| !value.is_empty());
            let owner = row
                .get(3)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let language = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
            let return_type = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
            let volatility = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
            let security = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
            let returns_set = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
            let strict = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_cost = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_rows = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
            let configuration_parameters =
                row.get(12).and_then(|value| value.as_str()).unwrap_or("-");
            let comment = row.get(13).and_then(|value| value.as_str()).unwrap_or("-");

            let display_name = {
                let base = if schema.is_some() {
                    procedure_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &procedure_name)
                };
                signature
                    .as_ref()
                    .map(|value| format!("{}({})", base, value))
                    .unwrap_or(base)
            };

            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), display_name.clone());
            values.insert("function_type".to_string(), "Procedure".to_string());
            values.insert(
                "owner".to_string(),
                owner.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "parameter".to_string(),
                signature.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert("language".to_string(), language.to_string());
            values.insert("return_type".to_string(), return_type.to_string());
            values.insert("volatility".to_string(), volatility.to_string());
            values.insert("security".to_string(), security.to_string());
            values.insert("returns_set".to_string(), returns_set.to_string());
            values.insert("strict".to_string(), strict.to_string());
            values.insert("estimated_cost".to_string(), estimated_cost.to_string());
            values.insert("estimated_rows".to_string(), estimated_rows.to_string());
            values.insert(
                "configuration_parameters".to_string(),
                configuration_parameters.to_string(),
            );
            values.insert("comment".to_string(), comment.to_string());

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "procedure".to_string(),
                object_ref: Some(Self::postgres_routine_object_ref(
                    "procedure",
                    procedure_name,
                    schema_name,
                    signature,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let trigger_rows = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, t.tgname, c.relname
                 FROM pg_trigger t
                 JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE NOT t.tgisinternal
                   AND n.nspname = $1
                 ORDER BY n.nspname, t.tgname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, t.tgname, c.relname
                 FROM pg_trigger t
                 JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE NOT t.tgisinternal
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, t.tgname",
                &[],
            )
            .await?
        };

        for row in &trigger_rows.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let trigger_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let table_name = row
                .get(2)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();

            let display_name = if schema.is_some() {
                trigger_name.clone()
            } else {
                schema_qualified_name(&schema_name, &trigger_name)
            };

            let values = build_metadata_values(
                schema_name.clone(),
                display_name.clone(),
                None,
                "Trigger",
                Some(table_name.clone()),
            );

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "trigger".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("trigger", trigger_name)
                        .with_schema_option(Some(schema_name))
                        .with_signature_option(Some(table_name)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let sequences = self.list_sequences(schema).await?;
        for sequence in sequences {
            let schema_name = sequence
                .schema
                .clone()
                .unwrap_or_else(|| "public".to_string());
            let name = sequence.name.clone();

            let values = build_metadata_values(
                schema_name.clone(),
                name.clone(),
                sequence.owner.clone(),
                "Sequence",
                Some(sequence.data_type.clone()),
            );

            let object_name = name
                .split('.')
                .next_back()
                .map(str::to_string)
                .unwrap_or_else(|| name.clone());

            rows.push(ObjectsPanelRow {
                name,
                schema: Some(schema_name.clone()),
                object_type: "sequence".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("sequence", object_name)
                        .with_schema_option(Some(schema_name)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let types = self.list_types(schema).await?;
        for data_type in types {
            let schema_name = data_type
                .schema
                .clone()
                .unwrap_or_else(|| "public".to_string());
            let name = data_type.name.clone();

            let values = build_metadata_values(
                schema_name.clone(),
                name.clone(),
                data_type.owner.clone(),
                Self::postgres_type_kind_label(data_type.type_kind),
                Some(format!("{:?}", data_type.type_kind)),
            );

            let object_name = name
                .split('.')
                .next_back()
                .map(str::to_string)
                .unwrap_or_else(|| name.clone());

            rows.push(ObjectsPanelRow {
                name,
                schema: Some(schema_name.clone()),
                object_type: Self::postgres_type_kind_id(data_type.type_kind).to_string(),
                object_ref: Some(Self::postgres_type_object_ref(
                    data_type.type_kind,
                    object_name,
                    schema_name,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let schema_infos = self.list_schemas().await?;
        for schema_info in schema_infos {
            if let Some(target_schema) = schema
                && target_schema != schema_info.name
            {
                continue;
            }

            let values = build_metadata_values(
                schema_info.name.clone(),
                schema_info.name.clone(),
                schema_info.owner.clone(),
                "Schema",
                None,
            );

            rows.push(ObjectsPanelRow {
                name: schema_info.name.clone(),
                schema: None,
                object_type: "schema".to_string(),
                object_ref: Some(ObjectsPanelObjectRef::new("schema", schema_info.name)),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        match self
            .query(
                "SELECT e.extname, n.nspname, r.rolname
                 FROM pg_extension e
                 JOIN pg_namespace n ON n.oid = e.extnamespace
                 LEFT JOIN pg_roles r ON r.oid = e.extowner
                 ORDER BY n.nspname, e.extname",
                &[],
            )
            .await
        {
            Ok(extension_result) => {
                for row in &extension_result.rows {
                    let schema_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();

                    if let Some(target_schema) = schema
                        && target_schema != schema_name
                    {
                        continue;
                    }

                    let extension_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let display_name = if schema.is_some() {
                        extension_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &extension_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        owner,
                        "Extension",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "extension".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("extension", extension_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL extensions for objects panel");
            }
        }

        match self
            .query(
                "SELECT s.srvname, f.fdwname, r.rolname
                 FROM pg_foreign_server s
                 JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
                 LEFT JOIN pg_roles r ON r.oid = s.srvowner
                 ORDER BY s.srvname",
                &[],
            )
            .await
        {
            Ok(server_result) => {
                for row in &server_result.rows {
                    let server_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let fdw_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);
                    let owner = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        server_name.clone(),
                        owner,
                        "Foreign Server",
                        fdw_name,
                    );

                    rows.push(ObjectsPanelRow {
                        name: server_name.clone(),
                        schema: None,
                        object_type: "foreign_server".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("foreign_server", server_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL foreign servers for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT f.fdwname, r.rolname
                 FROM pg_foreign_data_wrapper f
                 LEFT JOIN pg_roles r ON r.oid = f.fdwowner
                 ORDER BY f.fdwname",
                &[],
            )
            .await
        {
            Ok(fdw_result) => {
                for row in &fdw_result.rows {
                    let fdw_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        fdw_name.clone(),
                        owner,
                        "Foreign Data Wrapper",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: fdw_name.clone(),
                        schema: None,
                        object_type: "foreign_data_wrapper".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "foreign_data_wrapper",
                            fdw_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL foreign data wrappers for objects panel"
                );
            }
        }

        let policy_query = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, c.relname, p.polname
                 FROM pg_policy p
                 JOIN pg_class c ON c.oid = p.polrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                 ORDER BY n.nspname, c.relname, p.polname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await
        } else {
            self.query(
                "SELECT n.nspname, c.relname, p.polname
                 FROM pg_policy p
                 JOIN pg_class c ON c.oid = p.polrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, c.relname, p.polname",
                &[],
            )
            .await
        };

        match policy_query {
            Ok(policy_result) => {
                for row in &policy_result.rows {
                    let schema_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();
                    let table_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let policy_name = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();

                    let display_name = if schema.is_some() {
                        policy_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &policy_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        None,
                        "Policy",
                        Some(table_name),
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "policy".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("policy", policy_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL policies for objects panel");
            }
        }

        match self
            .query(
                "SELECT pubname, puballtables
                 FROM pg_publication
                 ORDER BY pubname",
                &[],
            )
            .await
        {
            Ok(publication_result) => {
                for row in &publication_result.rows {
                    let publication_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let all_tables = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "all tables" } else { "filtered" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        publication_name.clone(),
                        None,
                        "Publication",
                        Some(all_tables),
                    );

                    rows.push(ObjectsPanelRow {
                        name: publication_name.clone(),
                        schema: None,
                        object_type: "publication".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "publication",
                            publication_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL publications for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT subname, subenabled
                 FROM pg_subscription
                 ORDER BY subname",
                &[],
            )
            .await
        {
            Ok(subscription_result) => {
                for row in &subscription_result.rows {
                    let subscription_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let enabled = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "enabled" } else { "disabled" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        subscription_name.clone(),
                        None,
                        "Subscription",
                        Some(enabled),
                    );

                    rows.push(ObjectsPanelRow {
                        name: subscription_name.clone(),
                        schema: None,
                        object_type: "subscription".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "subscription",
                            subscription_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL subscriptions for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT evtname, evtenabled, evtevent
                 FROM pg_event_trigger
                 ORDER BY evtname",
                &[],
            )
            .await
        {
            Ok(event_trigger_result) => {
                for row in &event_trigger_result.rows {
                    let event_trigger_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let event_type = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        event_trigger_name.clone(),
                        None,
                        "Event Trigger",
                        event_type,
                    );

                    rows.push(ObjectsPanelRow {
                        name: event_trigger_name.clone(),
                        schema: None,
                        object_type: "event_trigger".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "event_trigger",
                            event_trigger_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL event triggers for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT lanname, lanpltrusted
                 FROM pg_language
                 ORDER BY lanname",
                &[],
            )
            .await
        {
            Ok(language_result) => {
                for row in &language_result.rows {
                    let language_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let trusted = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "trusted" } else { "untrusted" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        language_name.clone(),
                        None,
                        "Language",
                        Some(trusted),
                    );

                    rows.push(ObjectsPanelRow {
                        name: language_name.clone(),
                        schema: None,
                        object_type: "language".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("language", language_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL languages for objects panel");
            }
        }

        let collation_query = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, c.collname
                 FROM pg_collation c
                 JOIN pg_namespace n ON n.oid = c.collnamespace
                 WHERE n.nspname = $1
                 ORDER BY n.nspname, c.collname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await
        } else {
            self.query(
                "SELECT n.nspname, c.collname
                 FROM pg_collation c
                 JOIN pg_namespace n ON n.oid = c.collnamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, c.collname",
                &[],
            )
            .await
        };

        match collation_query {
            Ok(collation_result) => {
                for row in &collation_result.rows {
                    let schema_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();
                    let collation_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();

                    let display_name = if schema.is_some() {
                        collation_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &collation_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        None,
                        "Collation",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "collation".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("collation", collation_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL collations for objects panel");
            }
        }

        match self
            .query(
                "SELECT spcname, pg_get_userbyid(spcowner)
                 FROM pg_tablespace
                 ORDER BY spcname",
                &[],
            )
            .await
        {
            Ok(tablespace_result) => {
                for row in &tablespace_result.rows {
                    let tablespace_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        tablespace_name.clone(),
                        owner,
                        "Tablespace",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: tablespace_name.clone(),
                        schema: None,
                        object_type: "tablespace".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("tablespace", tablespace_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL tablespaces for objects panel"
                );
            }
        }

        rows.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(ObjectsPanelData { columns, rows })
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "table" => {
                let mut data = self.postgres_relation_objects_panel_data(schema).await?;
                for row in &mut data.rows {
                    if matches!(
                        row.object_type.as_str(),
                        "partitioned_table" | "foreign_table"
                    ) {
                        row.object_type = "table".to_string();
                        if let Some(object_ref) = row.object_ref.as_ref() {
                            row.object_ref = Some(
                                ObjectsPanelObjectRef::new("table", object_ref.name.clone())
                                    .with_schema_option(object_ref.schema.clone()),
                            );
                        }
                    }
                }
                Ok(data.for_kind_and_scope(kind_id, None))
            }
            "partitioned_table" | "foreign_table" | "view" | "materialized_view" => Ok(self
                .postgres_relation_objects_panel_data(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            "function" => Ok(self
                .postgres_routine_objects_panel_data(schema, kind_id, "f", "Function")
                .await?),
            "procedure" => Ok(self
                .postgres_routine_objects_panel_data(schema, kind_id, "p", "Procedure")
                .await?),
            "trigger" => self.postgres_trigger_objects_panel_data(schema).await,
            "extension" => self.postgres_extension_objects_panel_data(schema).await,
            "enum" | "domain" | "range" | "composite_type" => Ok(self
                .list_tables_extended(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            _ => Ok(self
                .list_tables_extended(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(Self::postgres_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        let object_ref = request.object_ref.as_ref();
        let schema = object_ref
            .and_then(|object_ref| object_ref.schema.clone())
            .or_else(|| Some("public".to_string()));
        let name = object_ref.map(|object_ref| object_ref.name.clone());

        match (request.kind_id.as_str(), request.mode) {
            ("enum", ObjectFormMode::Create) => Ok(Some(Self::enum_form_spec(
                request.mode,
                schema,
                name,
                Vec::new(),
            ))),
            ("enum", ObjectFormMode::Edit | ObjectFormMode::Drop) => {
                let labels = if let Some(object_ref) = object_ref {
                    self.list_types(object_ref.schema.as_deref())
                        .await?
                        .into_iter()
                        .find(|info| {
                            info.type_kind == TypeKind::Enum
                                && info
                                    .name
                                    .split('.')
                                    .next_back()
                                    .is_some_and(|type_name| type_name == object_ref.name)
                        })
                        .and_then(|info| info.values)
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                Ok(Some(Self::enum_form_spec(
                    request.mode,
                    schema,
                    name,
                    labels,
                )))
            }
            ("domain", ObjectFormMode::Create) => Ok(Some(Self::domain_form_spec(
                request.mode,
                schema,
                name,
                None,
                false,
                None,
                None,
            ))),
            ("domain", ObjectFormMode::Edit | ObjectFormMode::Drop) => {
                let mut base_type = None;
                let mut not_null = false;
                let mut default_value = None;
                let mut check_expression = None;

                if let Some(object_ref) = object_ref {
                    let result = self
                        .query(
                            "SELECT
                                pg_catalog.format_type(t.typbasetype, t.typtypmod),
                                t.typnotnull,
                                pg_catalog.pg_get_expr(t.typdefaultbin, 0),
                                (
                                    SELECT pg_catalog.pg_get_constraintdef(c.oid)
                                    FROM pg_catalog.pg_constraint c
                                    WHERE c.contypid = t.oid AND c.contype = 'c'
                                    ORDER BY c.conname
                                    LIMIT 1
                                )
                             FROM pg_catalog.pg_type t
                             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                             WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'd'",
                            &[
                                zqlz_core::Value::String(
                                    object_ref
                                        .schema
                                        .clone()
                                        .unwrap_or_else(|| "public".to_string()),
                                ),
                                zqlz_core::Value::String(object_ref.name.clone()),
                            ],
                        )
                        .await?;

                    if let Some(row) = result.rows.first() {
                        base_type = row
                            .get(0)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                        not_null = row
                            .get(1)
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false);
                        default_value = row
                            .get(2)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                        check_expression = row
                            .get(3)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                    }
                }

                Ok(Some(Self::domain_form_spec(
                    request.mode,
                    schema,
                    name,
                    base_type,
                    not_null,
                    default_value,
                    check_expression,
                )))
            }
            ("procedure", ObjectFormMode::Create) => {
                Ok(Some(Self::procedure_form_spec(request.mode, schema, name)))
            }
            _ => Ok(None),
        }
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        let schema = Self::object_form_string(&request.values, "schema");
        let schema = if schema.is_empty() { "public" } else { schema };
        let name = Self::object_form_string(&request.values, "name");
        if name.is_empty() {
            return Err(ZqlzError::Schema("Object name is required".to_string()));
        }

        let qualified_name = self.qualified_type_name(Some(schema), name);

        match (request.kind_id.as_str(), request.mode) {
            ("enum", ObjectFormMode::Create) => {
                let labels = Self::object_form_string_list(&request.values, "labels");
                if labels.is_empty() {
                    return Err(ZqlzError::Schema("Enum labels are required".to_string()));
                }
                Self::validate_unique_values("Enum", &labels)?;
                let label_sql = labels
                    .iter()
                    .map(|label| Self::quote_literal(label))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut statements = vec![format!(
                    "CREATE TYPE {qualified_name} AS ENUM ({label_sql});"
                )];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON TYPE {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("enum", ObjectFormMode::Edit) => {
                let labels = Self::object_form_string_list(&request.values, "labels");
                if labels.is_empty() {
                    return Err(ZqlzError::Schema("Enum labels are required".to_string()));
                }
                Self::validate_unique_values("Enum", &labels)?;
                let current_labels = if let Some(object_ref) = request.object_ref.as_ref() {
                    self.list_types(object_ref.schema.as_deref())
                        .await?
                        .into_iter()
                        .find(|info| {
                            info.type_kind == TypeKind::Enum
                                && info
                                    .name
                                    .split('.')
                                    .next_back()
                                    .is_some_and(|type_name| type_name == object_ref.name)
                        })
                        .and_then(|info| info.values)
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                let labels_to_append = Self::enum_labels_to_append(&labels, &current_labels);
                let mut statements = labels_to_append
                    .iter()
                    .map(|label| {
                        format!(
                            "ALTER TYPE {qualified_name} ADD VALUE {};",
                            Self::quote_literal(label)
                        )
                    })
                    .collect::<Vec<_>>();
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON TYPE {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("enum", ObjectFormMode::Drop) => Ok(vec![format!("DROP TYPE {qualified_name};")]),
            ("domain", ObjectFormMode::Create) => {
                let base_type = Self::object_form_string(&request.values, "base_type");
                if base_type.is_empty() {
                    return Err(ZqlzError::Schema(
                        "Domain base type is required".to_string(),
                    ));
                }
                let mut ddl = format!("CREATE DOMAIN {qualified_name} AS {base_type}");
                let default_value = Self::object_form_string(&request.values, "default");
                if !default_value.is_empty() {
                    ddl.push_str(&format!(" DEFAULT {default_value}"));
                }
                if Self::object_form_bool(&request.values, "not_null") {
                    ddl.push_str(" NOT NULL");
                }
                let check = Self::object_form_string(&request.values, "check");
                if !check.is_empty() {
                    ddl.push_str(&format!(" CHECK ({check})"));
                }
                ddl.push(';');
                let mut statements = vec![ddl];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON DOMAIN {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("domain", ObjectFormMode::Edit) => {
                let mut statements = Vec::new();
                let default_value = Self::object_form_string(&request.values, "default");
                if default_value.is_empty() {
                    statements.push(format!("ALTER DOMAIN {qualified_name} DROP DEFAULT;"));
                } else {
                    statements.push(format!(
                        "ALTER DOMAIN {qualified_name} SET DEFAULT {default_value};"
                    ));
                }
                if Self::object_form_bool(&request.values, "not_null") {
                    statements.push(format!("ALTER DOMAIN {qualified_name} SET NOT NULL;"));
                } else {
                    statements.push(format!("ALTER DOMAIN {qualified_name} DROP NOT NULL;"));
                }
                let check = Self::object_form_string(&request.values, "check");
                if !check.is_empty() {
                    let constraint_name = format!("{}_check", name);
                    statements.push(format!(
                        "ALTER DOMAIN {qualified_name} ADD CONSTRAINT {} CHECK ({check});",
                        self.quote_identifier(&constraint_name)
                    ));
                }
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON DOMAIN {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("domain", ObjectFormMode::Drop) => Ok(vec![format!("DROP DOMAIN {qualified_name};")]),
            ("procedure", ObjectFormMode::Create) => {
                let language = Self::object_form_string(&request.values, "language");
                if language.is_empty() {
                    return Err(ZqlzError::Schema(
                        "Procedure language is required".to_string(),
                    ));
                }
                let body = Self::object_form_string(&request.values, "body");
                if body.is_empty() {
                    return Err(ZqlzError::Schema("Procedure body is required".to_string()));
                }
                let arguments =
                    Self::object_form_string_list(&request.values, "arguments").join(", ");
                let mut statements = vec![format!(
                    "CREATE OR REPLACE PROCEDURE {qualified_name}({arguments})\nLANGUAGE {}\nAS $$\n{body}\n$$;",
                    self.quote_identifier(language)
                )];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON PROCEDURE {qualified_name}({arguments}) IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            _ => Err(ZqlzError::NotSupported(format!(
                "Object form DDL is not supported for {}",
                request.kind_id
            ))),
        }
    }
}
