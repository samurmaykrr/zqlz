//! DuckDB schema introspection.
//!
//! Structured introspection (tables, columns, indexes, foreign keys, ...) is
//! delegated to the shared `zqlz-schema-engine` via [`DuckDbCatalog`]. The
//! DuckDB-specific objects panel, object forms, and DDL synthesis stay here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use zqlz_core::{
    CatalogCapabilities, CatalogSource, ColumnInfo, ConstraintInfo, DatabaseObject, Dependency,
    ForeignKeyInfo, FunctionInfo, IndexInfo, NamespaceModel, ObjectFormDdlRequest, ObjectFormField,
    ObjectFormFieldKind, ObjectFormMode, ObjectFormSection, ObjectFormSpec, ObjectFormSpecRequest,
    ObjectFormValue, ObjectKindSupport, ObjectType, ObjectsPanelAction, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef,
    ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, QueryResult, RawColumnRow, RawConstraintRow,
    RawDatabaseRow, RawForeignKeyRow, RawIndexRow, RawRelationRow, RawSchemaRow, RawSequenceRow,
    RawTypeRow, RelationRef, Result, SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails,
    TableInfo, TableType, TriggerInfo, TypeInfo, Value, ViewInfo, ZqlzError,
};

use super::driver::run_duckdb_query;
use super::DuckDbConnection;

#[async_trait]
impl SchemaIntrospection for DuckDbConnection {
    // ---- structured introspection: delegated to the shared engine ----

    async fn list_databases(&self) -> Result<Vec<zqlz_core::DatabaseInfo>> {
        self.schema_engine.list_databases().await
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.schema_engine.list_schemas().await
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        self.schema_engine.list_tables(schema).await
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        self.schema_engine.list_views(schema).await
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        self.schema_engine.get_table(schema, name).await
    }

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

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        self.schema_engine.get_indexes(schema, table).await
    }

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
        self.schema_engine.list_functions(schema).await
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        self.schema_engine.list_procedures(schema).await
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        self.schema_engine.list_triggers(schema, table).await
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        self.schema_engine.list_sequences(schema).await
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        self.schema_engine.list_types(schema).await
    }

    async fn get_dependencies(&self, object: &DatabaseObject) -> Result<Vec<Dependency>> {
        self.schema_engine.get_dependencies(object).await
    }

    // ---- DuckDB-specific presentation: kept in the driver ----

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "schema" => duckdb_schema_objects_panel_data(self.list_schemas().await?),
            "table" => duckdb_table_objects_panel_data(self.list_tables(schema).await?),
            "view" => duckdb_view_objects_panel_data(self.list_views(schema).await?),
            "sequence" => duckdb_sequence_objects_panel_data(self.list_sequences(schema).await?),
            "type" => duckdb_type_objects_panel_data(self.list_types(schema).await?),
            _ => Ok(ObjectsPanelData::new(duckdb_objects_panel_columns())),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(duckdb_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(duckdb_object_form_spec(request))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        duckdb_object_form_ddl(request)
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
}

/// DuckDB implementation of the raw-catalog port.
pub struct DuckDbCatalog {
    connection: Arc<Mutex<duckdb::Connection>>,
    capabilities: CatalogCapabilities,
}

impl DuckDbCatalog {
    pub fn new(connection: Arc<Mutex<duckdb::Connection>>) -> Self {
        let capabilities = CatalogCapabilities {
            driver_id: "duckdb".to_string(),
            server_version: None,
            namespaces: NamespaceModel::DatabasesAndSchemas {
                default_schema: "main".to_string(),
            },
            objects: ObjectKindSupport {
                tables: true,
                views: true,
                sequences: true,
                types: true,
                ..ObjectKindSupport::NONE
            },
            auto_increment: Default::default(),
            stored_source: Vec::new(),
            deferrable_constraints: false,
            panel_extras: Vec::new(),
        };
        Self {
            connection,
            capabilities,
        }
    }

    fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        run_duckdb_query(&self.connection, sql, params)
    }
}

#[async_trait]
impl CatalogSource for DuckDbCatalog {
    fn capabilities(&self) -> &CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(&self, schema: Option<&str>) -> Result<Vec<RawRelationRow>> {
        let schema = schema.unwrap_or("main").to_string();

        let tables = self.query(
            "SELECT table_name, estimated_size, column_count FROM duckdb_tables() WHERE schema_name = ?",
            &[Value::String(schema.clone())],
        )?;
        let mut relations: Vec<RawRelationRow> = tables
            .rows
            .iter()
            .map(|row| {
                let mut relation = RawRelationRow::new(
                    row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    TableType::Table,
                );
                relation.schema = Some(schema.clone());
                relation.row_estimate = row.get(1).and_then(|v| v.as_i64());
                relation
            })
            .collect();

        let views = self.query(
            "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = ?",
            &[Value::String(schema.clone())],
        )?;
        relations.extend(views.rows.iter().map(|row| {
            let mut relation = RawRelationRow::new(
                row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                TableType::View,
            );
            relation.schema = Some(schema.clone());
            relation.view_definition = row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
            relation
        }));

        Ok(relations)
    }

    async fn fetch_columns(&self, relation: &RelationRef) -> Result<Vec<RawColumnRow>> {
        let schema = relation.schema.as_deref().unwrap_or("main");
        let result = self.query(
            "SELECT column_name, column_index, data_type, is_nullable, column_default
             FROM duckdb_columns()
             WHERE schema_name = ? AND table_name = ?
             ORDER BY column_index",
            &[
                Value::String(schema.to_string()),
                Value::String(relation.name.clone()),
            ],
        )?;

        // DuckDB reports primary-key membership through a separate constraint
        // catalog; fetch it so the engine can flag PK columns.
        let pk = self.query(
            "SELECT kcu.column_name, kcu.ordinal_position
             FROM information_schema.key_column_usage kcu
             JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
             WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = ? AND tc.table_name = ?",
            &[
                Value::String(schema.to_string()),
                Value::String(relation.name.clone()),
            ],
        )?;
        let pk_ordinals: HashMap<String, i64> = pk
            .rows
            .iter()
            .filter_map(|row| {
                Some((
                    row.get(0)?.as_str()?.to_string(),
                    row.get(1).and_then(|v| v.as_i64()).unwrap_or(1),
                ))
            })
            .collect();

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
                RawColumnRow {
                    ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
                    data_type: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    is_nullable: row.get(3).and_then(|v| v.as_bool()).unwrap_or(true),
                    default_value: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
                    primary_key_ordinal: pk_ordinals.get(&name).copied(),
                    name,
                    ..Default::default()
                }
            })
            .collect())
    }

    async fn fetch_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawColumnRow>>>> {
        let schema = schema.unwrap_or("main");
        let result = self.query(
            "SELECT column_name, column_index, data_type, is_nullable, column_default, table_name
             FROM duckdb_columns()
             WHERE schema_name = ?
             ORDER BY table_name, column_index",
            &[Value::String(schema.to_string())],
        )?;

        // Constraint names are auto-generated for unnamed constraints and are not
        // reliably unique across tables, so the join must carry table_name too.
        let pk = self.query(
            "SELECT kcu.column_name, kcu.ordinal_position, tc.table_name
             FROM information_schema.key_column_usage kcu
             JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
                AND kcu.table_name = tc.table_name
             WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ?",
            &[Value::String(schema.to_string())],
        )?;
        let pk_ordinals: HashMap<(String, String), i64> = pk
            .rows
            .iter()
            .filter_map(|row| {
                Some((
                    (
                        row.get(2)?.as_str()?.to_string(),
                        row.get(0)?.as_str()?.to_string(),
                    ),
                    row.get(1).and_then(|v| v.as_i64()).unwrap_or(1),
                ))
            })
            .collect();

        let mut columns_by_relation: HashMap<String, Vec<RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(table) = row.get(5).and_then(|value| value.as_str()) else {
                continue;
            };
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let primary_key_ordinal = pk_ordinals
                .get(&(table.to_string(), name.clone()))
                .copied();

            columns_by_relation
                .entry(table.to_string())
                .or_default()
                .push(RawColumnRow {
                    ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
                    data_type: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    is_nullable: row.get(3).and_then(|v| v.as_bool()).unwrap_or(true),
                    default_value: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
                    primary_key_ordinal,
                    name,
                    ..Default::default()
                });
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(&self, relation: &RelationRef) -> Result<Vec<RawIndexRow>> {
        let schema = relation.schema.as_deref().unwrap_or("main");
        let result = self.query(
            "SELECT index_name, is_unique, is_primary, sql
             FROM duckdb_indexes()
             WHERE schema_name = ? AND table_name = ?",
            &[
                Value::String(schema.to_string()),
                Value::String(relation.name.clone()),
            ],
        )?;

        Ok(result
            .rows
            .iter()
            .map(|row| RawIndexRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                columns: Vec::new(),
                is_unique: row.get(1).and_then(|v| v.as_bool()).unwrap_or(false),
                is_primary: row.get(2).and_then(|v| v.as_bool()).unwrap_or(false),
                method: Some("ART".to_string()),
                ..Default::default()
            })
            .collect())
    }

    async fn fetch_foreign_keys(&self, relation: &RelationRef) -> Result<Vec<RawForeignKeyRow>> {
        let schema = relation.schema.as_deref().unwrap_or("main");
        let result = self.query(
            "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS ref_table,
                    ccu.table_schema AS ref_schema, ccu.column_name AS ref_column
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
             JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
             WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = ? AND tc.table_name = ?",
            &[
                Value::String(schema.to_string()),
                Value::String(relation.name.clone()),
            ],
        )?;

        // Group one row per (constraint, column) into one record per constraint.
        let mut by_name: indexmap_order::OrderedFkMap = Default::default();
        for row in result.rows.iter() {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let column = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_table = row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_schema = row.get(3).and_then(|v| v.as_str()).map(ToString::to_string);
            let ref_column = row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string();
            by_name.push(name, column, ref_table, ref_schema, ref_column);
        }
        Ok(by_name.into_rows())
    }

    async fn fetch_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawForeignKeyRow>>>> {
        let schema = schema.unwrap_or("main");
        let result = self.query(
            "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS ref_table,
                    ccu.table_schema AS ref_schema, ccu.column_name AS ref_column,
                    tc.table_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
             JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
             WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = ?
             ORDER BY tc.table_name, tc.constraint_name",
            &[Value::String(schema.to_string())],
        )?;

        // Grouped per table first: constraint names are not unique across tables.
        let mut rows_by_relation: HashMap<String, indexmap_order::OrderedFkMap> = HashMap::new();
        for row in result.rows.iter() {
            let Some(table) = row.get(5).and_then(|value| value.as_str()) else {
                continue;
            };
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let column = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_table = row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_schema = row.get(3).and_then(|v| v.as_str()).map(ToString::to_string);
            let ref_column = row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string();
            rows_by_relation
                .entry(table.to_string())
                .or_default()
                .push(name, column, ref_table, ref_schema, ref_column);
        }

        Ok(Some(
            rows_by_relation
                .into_iter()
                .map(|(relation, by_name)| (relation, by_name.into_rows()))
                .collect(),
        ))
    }

    async fn fetch_constraints(&self, relation: &RelationRef) -> Result<Vec<RawConstraintRow>> {
        let schema = relation.schema.as_deref().unwrap_or("main");
        let result = self.query(
            "SELECT constraint_name, constraint_type
             FROM information_schema.table_constraints
             WHERE table_schema = ? AND table_name = ?",
            &[
                Value::String(schema.to_string()),
                Value::String(relation.name.clone()),
            ],
        )?;

        Ok(result
            .rows
            .iter()
            .map(|row| RawConstraintRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                kind: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                columns: Vec::new(),
                definition: None,
            })
            .collect())
    }

    async fn fetch_databases(&self) -> Result<Vec<RawDatabaseRow>> {
        let result = self.query("SELECT database_name, path FROM duckdb_databases()", &[])?;
        Ok(result
            .rows
            .iter()
            .map(|row| RawDatabaseRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                comment: row.get(1).and_then(|v| v.as_str()).map(ToString::to_string),
                ..Default::default()
            })
            .collect())
    }

    async fn fetch_schemas(&self) -> Result<Vec<RawSchemaRow>> {
        let result = self.query(
            "SELECT schema_name FROM information_schema.schemata
             WHERE catalog_name = current_database() ORDER BY schema_name",
            &[],
        )?;
        Ok(result
            .rows
            .iter()
            .map(|row| RawSchemaRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                ..Default::default()
            })
            .collect())
    }

    async fn fetch_sequences(&self, schema: Option<&str>) -> Result<Vec<RawSequenceRow>> {
        let schema = schema.unwrap_or("main");
        let result = self.query(
            "SELECT sequence_name, start_value, min_value, max_value, increment_by
             FROM duckdb_sequences() WHERE schema_name = ?",
            &[Value::String(schema.to_string())],
        )?;
        Ok(result
            .rows
            .iter()
            .map(|row| RawSequenceRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
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

    async fn fetch_types(&self, schema: Option<&str>) -> Result<Vec<RawTypeRow>> {
        let schema = schema.unwrap_or("main");
        let result = self.query(
            "SELECT type_name, type_category FROM duckdb_types()
             WHERE schema_name = ? AND type_category = 'ENUM'",
            &[Value::String(schema.to_string())],
        )?;
        Ok(result
            .rows
            .iter()
            .map(|row| RawTypeRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                schema: Some(schema.to_string()),
                kind: "ENUM".to_string(),
                ..Default::default()
            })
            .collect())
    }
}

/// Insertion-ordered grouping of foreign-key rows by constraint name, so a
/// composite key collapses to one record with all its columns in order.
mod indexmap_order {
    use zqlz_core::RawForeignKeyRow;

    #[derive(Default)]
    pub struct OrderedFkMap {
        order: Vec<String>,
        rows: std::collections::HashMap<String, RawForeignKeyRow>,
    }

    impl OrderedFkMap {
        pub fn push(
            &mut self,
            name: String,
            column: String,
            ref_table: String,
            ref_schema: Option<String>,
            ref_column: String,
        ) {
            if !self.rows.contains_key(&name) {
                self.order.push(name.clone());
                self.rows.insert(
                    name.clone(),
                    RawForeignKeyRow {
                        name: name.clone(),
                        referenced_table: ref_table,
                        referenced_schema: ref_schema,
                        ..Default::default()
                    },
                );
            }
            let entry = self.rows.get_mut(&name).expect("entry just inserted");
            entry.columns.push(column);
            entry.referenced_columns.push(ref_column);
        }

        pub fn into_rows(mut self) -> Vec<RawForeignKeyRow> {
            self.order
                .iter()
                .filter_map(|name| self.rows.remove(name))
                .collect()
        }
    }
}

fn duckdb_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(280.0)
            .min_width(120.0)
            .sortable(),
        ObjectsPanelColumn::new("schema", "Schema")
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("type", "Type")
            .width(150.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("rows", "Rows")
            .width(90.0)
            .min_width(60.0)
            .sortable()
            .text_right(),
    ]
}

fn duckdb_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    object_type_label: String,
    rows: Option<i64>,
) -> ObjectsPanelRow {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), name.clone());
    values.insert(
        "schema".to_string(),
        schema.clone().unwrap_or_else(|| "-".to_string()),
    );
    values.insert("type".to_string(), object_type_label);
    values.insert(
        "rows".to_string(),
        rows.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: name.clone(),
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn duckdb_schema_objects_panel_data(schemas: Vec<SchemaInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for schema in schemas {
        data.rows.push(duckdb_row(
            "schema",
            schema.name,
            None,
            "Schema".to_string(),
            None,
        ));
    }
    Ok(data)
}

fn duckdb_table_objects_panel_data(tables: Vec<TableInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for table in tables {
        data.rows.push(duckdb_row(
            "table",
            table.name,
            table.schema,
            "Table".to_string(),
            table.row_count,
        ));
    }
    Ok(data)
}

fn duckdb_view_objects_panel_data(views: Vec<ViewInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for view in views {
        data.rows.push(duckdb_row(
            "view",
            view.name,
            view.schema,
            "View".to_string(),
            None,
        ));
    }
    Ok(data)
}

fn duckdb_sequence_objects_panel_data(sequences: Vec<SequenceInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for sequence in sequences {
        data.rows.push(duckdb_row(
            "sequence",
            sequence.name,
            sequence.schema,
            "Sequence".to_string(),
            sequence.current_value,
        ));
    }
    Ok(data)
}

fn duckdb_type_objects_panel_data(types: Vec<TypeInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for data_type in types {
        data.rows.push(duckdb_row(
            "type",
            data_type.name,
            data_type.schema,
            format!("{:?}", data_type.type_kind),
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn duckdb_objects_panel_manifest() -> ObjectsPanelManifest {
    let columns = duckdb_objects_panel_columns();
    let row_actions = |kind_id: &str| {
        vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("delete", "Drop")
                .group("danger")
                .destructive()
                .object_form(kind_id, ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ]
    };
    ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("schema", "Schema", "Schemas")
                .icon_key("schema")
                .columns(columns.clone())
                .row_actions(row_actions("schema")),
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .icon_key("table")
                .columns(columns.clone())
                .row_actions(row_actions("table"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("view", "View", "Views")
                .icon_key("view")
                .columns(columns.clone())
                .row_actions(row_actions("view"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("sequence", "Sequence", "Sequences")
                .icon_key("sequence")
                .columns(columns.clone())
                .row_actions(row_actions("sequence")),
            ObjectsPanelObjectKind::new("type", "Type", "Types")
                .icon_key("type")
                .columns(columns)
                .row_actions(row_actions("type")),
        ],
        toolbar_actions: vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
            ObjectsPanelAction::new("new_schema", "New Schema")
                .icon_key("create")
                .create_object_kind("schema")
                .object_form("schema", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_table", "New Table")
                .icon_key("create")
                .create_object_kind("table")
                .object_form("table", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_view", "New View")
                .icon_key("create")
                .create_object_kind("view")
                .object_form("view", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_sequence", "New Sequence")
                .icon_key("create")
                .create_object_kind("sequence")
                .object_form("sequence", ObjectFormMode::Create),
        ],
    }
}

fn duckdb_object_form_spec(request: &ObjectFormSpecRequest) -> Option<ObjectFormSpec> {
    let schema = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .unwrap_or_else(|| "main".to_string());
    let name = request
        .object_ref
        .as_ref()
        .map(|object_ref| object_ref.name.clone())
        .unwrap_or_default();
    let fields = match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            vec![ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required()]
        }
        ("table", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("columns", "Columns", ObjectFormFieldKind::TextArea)
                .default_value(ObjectFormValue::String(
                    "id BIGINT PRIMARY KEY,\ncreated_at TIMESTAMP".to_string(),
                ))
                .required(),
        ],
        ("view", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("query", "Query", ObjectFormFieldKind::SqlExpression)
                .default_value(ObjectFormValue::String("SELECT 1 AS value".to_string()))
                .required(),
        ],
        ("sequence", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
        ],
        (_, ObjectFormMode::Drop) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema))
                .read_only(),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(name))
                .read_only(),
            ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox).required(),
        ],
        _ => return None,
    };
    Some(
        ObjectFormSpec::new(&request.kind_id, request.mode, "DuckDB Object")
            .sections(vec![ObjectFormSection::new(fields)]),
    )
}

fn duckdb_form_string(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    key: &str,
) -> String {
    values
        .get(key)
        .and_then(|value| value.as_string())
        .unwrap_or("")
        .trim()
        .to_string()
}

pub(crate) fn duckdb_quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn duckdb_qualified_name(schema: &str, name: &str) -> String {
    format!(
        "{}.{}",
        duckdb_quote_identifier(schema),
        duckdb_quote_identifier(name)
    )
}

pub(crate) fn duckdb_object_form_ddl(request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
    let name = duckdb_form_string(&request.values, "name");
    let schema = duckdb_form_string(&request.values, "schema");
    let schema = if schema.is_empty() { "main" } else { &schema };
    match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Schema name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SCHEMA {}",
                duckdb_quote_identifier(&name)
            )])
        }
        ("table", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Table name is required".to_string()));
            }
            let columns = duckdb_form_string(&request.values, "columns");
            if columns.is_empty() {
                return Err(ZqlzError::Driver("Columns are required".to_string()));
            }
            Ok(vec![format!(
                "CREATE TABLE {} (\n{}\n)",
                duckdb_qualified_name(schema, &name),
                columns
            )])
        }
        ("view", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("View name is required".to_string()));
            }
            let query = duckdb_form_string(&request.values, "query");
            if query.is_empty() {
                return Err(ZqlzError::Driver("Query is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE VIEW {} AS {}",
                duckdb_qualified_name(schema, &name),
                query
            )])
        }
        ("sequence", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Sequence name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SEQUENCE {}",
                duckdb_qualified_name(schema, &name)
            )])
        }
        (_, ObjectFormMode::Drop) => {
            let confirmed = request
                .values
                .get("confirm")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            if !confirmed {
                return Err(ZqlzError::Driver(
                    "Drop confirmation is required".to_string(),
                ));
            }
            let object_name = if name.is_empty() {
                request
                    .object_ref
                    .as_ref()
                    .map(|object_ref| object_ref.name.as_str())
                    .unwrap_or("")
            } else {
                &name
            };
            let object_schema = request
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.schema.as_deref())
                .unwrap_or(schema);
            if object_name.is_empty() {
                return Err(ZqlzError::Driver("Object name is required".to_string()));
            }
            let kind = match request.kind_id.as_str() {
                "schema" => {
                    return Ok(vec![format!(
                        "DROP SCHEMA {}",
                        duckdb_quote_identifier(object_name)
                    )]);
                }
                "table" => "TABLE",
                "view" => "VIEW",
                "sequence" => "SEQUENCE",
                _ => {
                    return Err(ZqlzError::NotSupported(format!(
                        "Dropping DuckDB {} is not supported",
                        request.kind_id
                    )));
                }
            };
            Ok(vec![format!(
                "DROP {} {}",
                kind,
                duckdb_qualified_name(object_schema, object_name)
            )])
        }
        _ => Err(ZqlzError::NotSupported(format!(
            "DuckDB object form is not supported for {} {:?}",
            request.kind_id, request.mode
        ))),
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
