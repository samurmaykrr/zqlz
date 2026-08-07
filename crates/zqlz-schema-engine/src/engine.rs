//! The [`SchemaEngine`] and the pure normalize/compose helpers it owns.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use zqlz_core::{
    AutoIncrementRules, CatalogCapabilities, CatalogDialect, CatalogSource, ColumnInfo,
    ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject, Dependency, DependencyType,
    ForeignKeyAction, ForeignKeyInfo, ForeignKeyRef, FunctionInfo, IndexInfo, NamespaceModel,
    ObjectScope, ObjectType, ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelManifest,
    ObjectFormDdlRequest, ObjectFormSpec, ObjectFormSpecRequest, ObjectKindSource,
    PrimaryKeyInfo, ProcedureInfo, RawColumnRow, RawRelationRow, RawTriggerRow, RelationRef, Result,
    SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent,
    TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

/// A no-op dialect: refuses DDL synthesis and object forms. Sufficient for
/// drivers whose catalog always returns stored DDL (e.g. SQLite) and that have
/// no object forms.
pub struct DefaultDialect;

impl CatalogDialect for DefaultDialect {}

/// Implements [`SchemaIntrospection`] by delegating raw catalog reads to a
/// [`CatalogSource`] and applying shared normalization/composition.
pub struct SchemaEngine {
    source: Arc<dyn CatalogSource>,
    dialect: Arc<dyn CatalogDialect>,
    kinds: Vec<Arc<dyn ObjectKindSource>>,
}

impl SchemaEngine {
    pub fn new(source: Arc<dyn CatalogSource>, dialect: Arc<dyn CatalogDialect>) -> Self {
        Self {
            source,
            dialect,
            kinds: Vec::new(),
        }
    }

    /// Register objects-panel kinds. When non-empty, the panel manifest and
    /// per-kind data come from these instead of the default relation-derived
    /// panel.
    pub fn with_kinds(mut self, kinds: Vec<Arc<dyn ObjectKindSource>>) -> Self {
        self.kinds = kinds;
        self
    }

    fn caps(&self) -> &CatalogCapabilities {
        self.source.capabilities()
    }

    /// Resolve a caller-supplied `(schema, name)` into an unambiguous
    /// [`RelationRef`], splitting qualified `schema.table` names and applying
    /// the namespace model's default schema.
    fn resolve_relation(&self, schema: Option<&str>, name: &str) -> RelationRef {
        let (parsed_schema, bare) = match name.split_once('.') {
            Some((schema_part, table_part)) if !table_part.is_empty() => {
                (Some(schema_part.to_string()), table_part.to_string())
            }
            _ => (None, name.to_string()),
        };

        let schema = schema
            .map(ToString::to_string)
            .or(parsed_schema)
            .or_else(|| self.caps().namespaces.default_schema().map(ToString::to_string));

        RelationRef::new(schema, bare)
    }

    /// Schema string the engine passes to listing fetches.
    fn scope_schema(&self, schema: Option<&str>) -> Option<String> {
        schema
            .map(ToString::to_string)
            .or_else(|| self.caps().namespaces.default_schema().map(ToString::to_string))
    }

    async fn fetch_table_info(
        &self,
        relation: &RelationRef,
    ) -> Result<TableInfo> {
        let relations = self
            .source
            .fetch_relations(relation.schema.as_deref())
            .await?;
        relations
            .into_iter()
            .find(|row| row.name == relation.name)
            .map(normalize_table_info)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", relation.name)))
    }

    async fn primary_key_for(
        &self,
        relation: &RelationRef,
        raw_columns: &[RawColumnRow],
    ) -> Result<Option<PrimaryKeyInfo>> {
        let constraints = normalize_constraints(self.source.fetch_constraints(relation).await?);
        if let Some(pk) = primary_key_from_constraints(&constraints) {
            return Ok(Some(pk));
        }
        let indexes = normalize_indexes(self.source.fetch_indexes(relation).await?);
        if let Some(pk) = primary_key_from_indexes(&indexes) {
            return Ok(Some(pk));
        }
        Ok(primary_key_from_columns(raw_columns))
    }

    async fn default_panel_data(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let scope_schema = self.scope_schema(schema);
        let relations = self.source.fetch_relations(scope_schema.as_deref()).await?;
        let table_infos: Vec<TableInfo> = relations.iter().cloned().map(normalize_table_info).collect();
        let mut data = ObjectsPanelData::from_table_infos(table_infos);

        for extra in &self.caps().panel_extras {
            let mut column = ObjectsPanelColumn::new(extra.extra_key.clone(), extra.title.clone());
            if extra.align_right {
                column = column.text_right();
            }
            data.columns.push(column);
            for (row, relation) in data.rows.iter_mut().zip(relations.iter()) {
                if row.object_kind_id() == extra.kind_id {
                    if let Some(value) = relation.extras.get(&extra.extra_key) {
                        row.values.insert(extra.extra_key.clone(), value.clone());
                    }
                }
            }
        }

        Ok(data)
    }
}

#[async_trait]
impl SchemaIntrospection for SchemaEngine {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        match &self.caps().namespaces {
            NamespaceModel::SingleNamespace { name } => Ok(vec![DatabaseInfo {
                name: name.clone(),
                owner: None,
                encoding: None,
                size_bytes: None,
                comment: None,
            }]),
            _ => Ok(self
                .source
                .fetch_databases()
                .await?
                .into_iter()
                .map(|row| DatabaseInfo {
                    name: row.name,
                    owner: row.owner,
                    encoding: row.encoding,
                    size_bytes: row.size_bytes,
                    comment: row.comment,
                })
                .collect()),
        }
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        match &self.caps().namespaces {
            NamespaceModel::SingleNamespace { name } => Ok(vec![SchemaInfo {
                name: name.clone(),
                owner: None,
                comment: None,
            }]),
            _ => Ok(self
                .source
                .fetch_schemas()
                .await?
                .into_iter()
                .map(|row| SchemaInfo {
                    name: row.name,
                    owner: row.owner,
                    comment: row.comment,
                })
                .collect()),
        }
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        if !self.caps().objects.tables {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_relations(scope_schema.as_deref())
            .await?
            .into_iter()
            .filter(|row| !is_view_kind(row.table_type))
            .map(normalize_table_info)
            .collect())
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        if !self.caps().objects.views {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_relations(scope_schema.as_deref())
            .await?
            .into_iter()
            .filter(|row| row.table_type == TableType::View)
            .map(|row| normalize_view_info(row, false))
            .collect())
    }

    async fn list_materialized_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        if !self.caps().objects.materialized_views {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_relations(scope_schema.as_deref())
            .await?
            .into_iter()
            .filter(|row| row.table_type == TableType::MaterializedView)
            .map(|row| normalize_view_info(row, true))
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let relation = self.resolve_relation(schema, name);

        let info = self.fetch_table_info(&relation).await?;
        let (raw_columns, raw_indexes, raw_foreign_keys, raw_constraints, triggers) = futures::try_join!(
            self.source.fetch_columns(&relation),
            self.source.fetch_indexes(&relation),
            self.source.fetch_foreign_keys(&relation),
            self.source.fetch_constraints(&relation),
            async {
                if self.caps().objects.triggers {
                    self.source
                        .fetch_triggers(relation.schema.as_deref(), Some(&relation.name))
                        .await
                } else {
                    Ok(Vec::new())
                }
            },
        )?;

        let indexes = normalize_indexes(raw_indexes);
        let foreign_keys = normalize_foreign_keys(raw_foreign_keys);
        let constraints = normalize_constraints(raw_constraints);
        let triggers = triggers.into_iter().map(normalize_trigger_info).collect();

        let primary_key = primary_key_from_constraints(&constraints)
            .or_else(|| primary_key_from_indexes(&indexes))
            .or_else(|| primary_key_from_columns(&raw_columns));

        let mut columns = normalize_columns(raw_columns, &self.caps().auto_increment);
        backfill_column_flags(&mut columns, primary_key.as_ref(), &foreign_keys, &indexes);

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints,
            triggers,
        })
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let relation = self.resolve_relation(schema, table);
        let raw = self.source.fetch_columns(&relation).await?;
        Ok(normalize_columns(raw, &self.caps().auto_increment))
    }

    async fn list_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ColumnInfo>>>> {
        let Some(raw_by_relation) = self.source.fetch_all_columns(schema).await? else {
            return Ok(None);
        };

        let rules = &self.caps().auto_increment;
        Ok(Some(
            raw_by_relation
                .into_iter()
                .map(|(relation, raw)| (relation, normalize_columns(raw, rules)))
                .collect(),
        ))
    }

    async fn list_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ForeignKeyInfo>>>> {
        let Some(raw_by_relation) = self.source.fetch_all_foreign_keys(schema).await? else {
            return Ok(None);
        };

        Ok(Some(
            raw_by_relation
                .into_iter()
                .map(|(relation, raw)| (relation, normalize_foreign_keys(raw)))
                .collect(),
        ))
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let relation = self.resolve_relation(schema, table);
        Ok(normalize_indexes(self.source.fetch_indexes(&relation).await?))
    }

    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let relation = self.resolve_relation(schema, table);
        Ok(normalize_foreign_keys(
            self.source.fetch_foreign_keys(&relation).await?,
        ))
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let relation = self.resolve_relation(schema, table);
        let raw_columns = self.source.fetch_columns(&relation).await?;
        self.primary_key_for(&relation, &raw_columns).await
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let relation = self.resolve_relation(schema, table);
        Ok(normalize_constraints(
            self.source.fetch_constraints(&relation).await?,
        ))
    }

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        if !self.caps().objects.functions {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_routines(scope_schema.as_deref())
            .await?
            .into_iter()
            .filter(|row| !row.is_procedure)
            .map(|row| FunctionInfo {
                schema: row.schema,
                name: row.name,
                language: row.language,
                return_type: row.return_type,
                parameters: Vec::new(),
                definition: row.definition,
                owner: row.owner,
                comment: row.comment,
            })
            .collect())
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        if !self.caps().objects.procedures {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_routines(scope_schema.as_deref())
            .await?
            .into_iter()
            .filter(|row| row.is_procedure)
            .map(|row| ProcedureInfo {
                schema: row.schema,
                name: row.name,
                language: row.language,
                parameters: Vec::new(),
                definition: row.definition,
                owner: row.owner,
                comment: row.comment,
            })
            .collect())
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        if !self.caps().objects.triggers {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_triggers(scope_schema.as_deref(), table)
            .await?
            .into_iter()
            .map(normalize_trigger_info)
            .collect())
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        if !self.caps().objects.sequences {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_sequences(scope_schema.as_deref())
            .await?
            .into_iter()
            .map(|row| SequenceInfo {
                schema: row.schema,
                name: row.name,
                data_type: row.data_type,
                start_value: row.start_value,
                min_value: row.min_value,
                max_value: row.max_value,
                increment_by: row.increment_by,
                current_value: row.current_value,
                owner: row.owner,
                comment: row.comment,
            })
            .collect())
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        if !self.caps().objects.types {
            return Ok(Vec::new());
        }
        let scope_schema = self.scope_schema(schema);
        Ok(self
            .source
            .fetch_types(scope_schema.as_deref())
            .await?
            .into_iter()
            .map(|row| TypeInfo {
                schema: row.schema,
                name: row.name,
                type_kind: parse_type_kind(&row.kind),
                values: row.values,
                definition: row.definition,
                owner: row.owner,
                comment: row.comment,
            })
            .collect())
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        if let Some(source) = self.source.fetch_object_source(object).await? {
            return Ok(source);
        }
        let details = if matches!(
            object.object_type,
            ObjectType::Table | ObjectType::View | ObjectType::MaterializedView
        ) {
            self.get_table(object.schema.as_deref(), &object.name).await.ok()
        } else {
            None
        };
        self.dialect.render_object_ddl(object, details.as_ref())
    }

    async fn get_dependencies(&self, object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(self
            .source
            .fetch_dependencies(object)
            .await?
            .into_iter()
            .map(|row| Dependency {
                dependent: row.dependent,
                referenced: row.referenced,
                dependency_type: parse_dependency_type(&row.dependency_type),
            })
            .collect())
    }

    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        self.default_panel_data(schema).await
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        if let Some(kind) = self.kinds.iter().find(|k| k.descriptor().id == kind_id) {
            let scope = ObjectScope::for_schema(self.scope_schema(schema).as_deref());
            return Ok(ObjectsPanelData {
                columns: kind.descriptor().columns,
                rows: kind.rows(&scope).await?,
            });
        }
        Ok(self
            .default_panel_data(schema)
            .await?
            .for_kind_and_scope(kind_id, None))
    }

    async fn list_objects_panel_manifest(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        if !self.kinds.is_empty() {
            return Ok(ObjectsPanelManifest {
                object_kinds: self.kinds.iter().map(|k| k.descriptor()).collect(),
                toolbar_actions: Vec::new(),
            });
        }
        let data = self.default_panel_data(schema).await?;
        Ok(ObjectsPanelManifest::from_data(&data))
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        if let Some(kind) = self.kinds.iter().find(|k| k.descriptor().id == request.kind_id) {
            return kind.form_spec(request).await;
        }
        self.dialect.object_form_spec(request)
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        if let Some(kind) = self.kinds.iter().find(|k| k.descriptor().id == request.kind_id) {
            return kind.form_ddl(request).await;
        }
        self.dialect.generate_object_form_ddl(request)
    }
}

// ---------------------------------------------------------------------------
// Pure normalization helpers — what the engine adds on top of raw records.
// ---------------------------------------------------------------------------

fn is_view_kind(table_type: TableType) -> bool {
    matches!(table_type, TableType::View | TableType::MaterializedView)
}

fn normalize_table_info(row: RawRelationRow) -> TableInfo {
    TableInfo {
        schema: row.schema,
        name: row.name,
        table_type: row.table_type,
        owner: row.owner,
        row_count: row.row_estimate,
        size_bytes: row.size_bytes,
        comment: row.comment,
        index_count: row.index_count,
        trigger_count: row.trigger_count,
        key_value_info: None,
    }
}

fn normalize_view_info(row: RawRelationRow, is_materialized: bool) -> ViewInfo {
    ViewInfo {
        schema: row.schema,
        name: row.name,
        is_materialized,
        definition: row.view_definition,
        owner: row.owner,
        comment: row.comment,
    }
}

fn normalize_columns(rows: Vec<RawColumnRow>, rules: &AutoIncrementRules) -> Vec<ColumnInfo> {
    rows.into_iter()
        .map(|row| {
            let is_primary_key = row.primary_key_ordinal.is_some();
            let is_auto_increment = compute_auto_increment(&row, rules, is_primary_key);
            ColumnInfo {
                name: row.name,
                ordinal: row.ordinal.max(0) as usize,
                data_type: row.data_type,
                nullable: row.is_nullable,
                default_value: row.default_value,
                max_length: row.max_length,
                precision: row.precision,
                scale: row.scale,
                is_primary_key,
                is_auto_increment,
                is_unique: false,
                foreign_key: None,
                comment: row.comment,
                charset: row.charset,
                collation: row.collation,
                generation_expression: row.generation_expression,
                is_generated_stored: row.is_generated_stored,
                enum_values: row.enum_values,
            }
        })
        .collect()
}

fn compute_auto_increment(row: &RawColumnRow, rules: &AutoIncrementRules, is_primary_key: bool) -> bool {
    use zqlz_core::RawIdentity;
    match row.identity {
        RawIdentity::Declared => true,
        RawIdentity::None => false,
        RawIdentity::Unknown => {
            if let Some(default) = &row.default_value {
                let lowered = default.to_lowercase();
                if rules
                    .default_markers
                    .iter()
                    .any(|marker| lowered.contains(&marker.to_lowercase()))
                {
                    return true;
                }
            }
            rules.integer_primary_key
                && is_primary_key
                && row.data_type.eq_ignore_ascii_case("INTEGER")
        }
    }
}

fn normalize_indexes(rows: Vec<zqlz_core::RawIndexRow>) -> Vec<IndexInfo> {
    rows.into_iter()
        .map(|row| IndexInfo {
            name: row.name,
            columns: row.columns,
            is_unique: row.is_unique,
            is_primary: row.is_primary,
            index_type: row.method.unwrap_or_else(|| "btree".to_string()),
            comment: row.comment,
            where_clause: row.where_clause,
            include_columns: row.include_columns,
            column_descending: row.column_descending,
        })
        .collect()
}

fn normalize_foreign_keys(rows: Vec<zqlz_core::RawForeignKeyRow>) -> Vec<ForeignKeyInfo> {
    rows.into_iter()
        .map(|row| ForeignKeyInfo {
            name: row.name,
            columns: row.columns,
            referenced_table: row.referenced_table,
            referenced_schema: row.referenced_schema,
            referenced_columns: row.referenced_columns,
            on_update: parse_fk_action(row.on_update.as_deref()),
            on_delete: parse_fk_action(row.on_delete.as_deref()),
            is_deferrable: row.is_deferrable,
            initially_deferred: row.initially_deferred,
        })
        .collect()
}

fn parse_fk_action(action: Option<&str>) -> ForeignKeyAction {
    // Normalize underscores so MS SQL Server's `SET_NULL`/`NO_ACTION` descriptors
    // parse the same as the space-separated information_schema spelling.
    match action
        .unwrap_or("")
        .trim()
        .replace('_', " ")
        .to_uppercase()
        .as_str()
    {
        "CASCADE" | "C" => ForeignKeyAction::Cascade,
        "SET NULL" | "N" => ForeignKeyAction::SetNull,
        "SET DEFAULT" | "D" => ForeignKeyAction::SetDefault,
        "RESTRICT" | "R" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

fn normalize_constraints(rows: Vec<zqlz_core::RawConstraintRow>) -> Vec<ConstraintInfo> {
    rows.into_iter()
        .map(|row| ConstraintInfo {
            constraint_type: parse_constraint_type(&row.kind),
            name: row.name,
            columns: row.columns,
            definition: row.definition,
        })
        .collect()
}

fn parse_constraint_type(kind: &str) -> ConstraintType {
    match kind.trim().to_uppercase().as_str() {
        "PRIMARY KEY" | "P" => ConstraintType::PrimaryKey,
        "FOREIGN KEY" | "F" => ConstraintType::ForeignKey,
        "UNIQUE" | "U" => ConstraintType::Unique,
        "EXCLUSION" | "EXCLUDE" | "X" => ConstraintType::Exclusion,
        _ => ConstraintType::Check,
    }
}

fn normalize_trigger_info(row: RawTriggerRow) -> TriggerInfo {
    let timing = match row.timing.as_deref().unwrap_or("").trim().to_uppercase().as_str() {
        "BEFORE" => TriggerTiming::Before,
        "INSTEAD OF" | "INSTEAD_OF" => TriggerTiming::InsteadOf,
        _ => TriggerTiming::After,
    };
    let mut events: Vec<TriggerEvent> = row
        .events
        .iter()
        .map(|event| match event.trim().to_uppercase().as_str() {
            "UPDATE" => TriggerEvent::Update,
            "DELETE" => TriggerEvent::Delete,
            "TRUNCATE" => TriggerEvent::Truncate,
            _ => TriggerEvent::Insert,
        })
        .collect();
    if events.is_empty() {
        events.push(TriggerEvent::Insert);
    }
    let for_each = match row.for_each.as_deref().unwrap_or("").trim().to_uppercase().as_str() {
        "STATEMENT" => TriggerForEach::Statement,
        _ => TriggerForEach::Row,
    };

    TriggerInfo {
        schema: row.schema,
        name: row.name,
        table_name: row.table_name,
        timing,
        events,
        for_each,
        definition: row.definition,
        enabled: row.enabled,
        comment: None,
    }
}

fn parse_type_kind(kind: &str) -> TypeKind {
    match kind.trim().to_uppercase().as_str() {
        "ENUM" | "E" => TypeKind::Enum,
        "COMPOSITE" | "C" => TypeKind::Composite,
        "DOMAIN" | "D" => TypeKind::Domain,
        "RANGE" | "R" => TypeKind::Range,
        _ => TypeKind::Base,
    }
}

fn parse_dependency_type(kind: &str) -> DependencyType {
    match kind.trim().to_uppercase().as_str() {
        "AUTOMATIC" | "A" => DependencyType::Automatic,
        "INTERNAL" | "I" => DependencyType::Internal,
        _ => DependencyType::Normal,
    }
}

// ---------------------------------------------------------------------------
// Composition: derive primary keys and cross-mark column flags.
// ---------------------------------------------------------------------------

fn primary_key_from_constraints(constraints: &[ConstraintInfo]) -> Option<PrimaryKeyInfo> {
    constraints
        .iter()
        .find(|constraint| constraint.constraint_type == ConstraintType::PrimaryKey)
        .filter(|constraint| !constraint.columns.is_empty())
        .map(|constraint| PrimaryKeyInfo {
            name: Some(constraint.name.clone()),
            columns: constraint.columns.clone(),
        })
}

fn primary_key_from_indexes(indexes: &[IndexInfo]) -> Option<PrimaryKeyInfo> {
    indexes
        .iter()
        .find(|index| index.is_primary && !index.columns.is_empty())
        .map(|index| PrimaryKeyInfo {
            name: Some(index.name.clone()),
            columns: index.columns.clone(),
        })
}

fn primary_key_from_columns(columns: &[RawColumnRow]) -> Option<PrimaryKeyInfo> {
    let mut members: Vec<&RawColumnRow> = columns
        .iter()
        .filter(|column| column.primary_key_ordinal.is_some())
        .collect();
    if members.is_empty() {
        return None;
    }
    members.sort_by_key(|column| column.primary_key_ordinal.unwrap_or(i64::MAX));
    Some(PrimaryKeyInfo {
        name: None,
        columns: members.into_iter().map(|column| column.name.clone()).collect(),
    })
}

fn backfill_column_flags(
    columns: &mut [ColumnInfo],
    primary_key: Option<&PrimaryKeyInfo>,
    foreign_keys: &[ForeignKeyInfo],
    indexes: &[IndexInfo],
) {
    if let Some(primary_key) = primary_key {
        let pk_columns: HashSet<&str> = primary_key.columns.iter().map(String::as_str).collect();
        for column in columns.iter_mut() {
            if pk_columns.contains(column.name.as_str()) {
                column.is_primary_key = true;
            }
        }
    }

    for index in indexes.iter().filter(|index| index.is_unique && index.columns.len() == 1) {
        if let Some(column) = columns.iter_mut().find(|column| column.name == index.columns[0]) {
            column.is_unique = true;
        }
    }

    for foreign_key in foreign_keys
        .iter()
        .filter(|fk| fk.columns.len() == 1 && fk.referenced_columns.len() == 1)
    {
        if let Some(column) = columns.iter_mut().find(|column| column.name == foreign_key.columns[0]) {
            column.foreign_key = Some(ForeignKeyRef {
                table: foreign_key.referenced_table.clone(),
                column: foreign_key.referenced_columns[0].clone(),
                constraint_name: foreign_key.name.clone(),
            });
        }
    }
}
