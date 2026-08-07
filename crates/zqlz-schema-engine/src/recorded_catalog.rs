//! A [`CatalogSource`] that replays recorded raw-catalog records.
//!
//! Because raw records are exactly what production adapters emit, fixtures are
//! recordings rather than hand-written mocks. The engine can therefore be
//! exercised end-to-end with zero database — both for the engine crate's own
//! tests and, later, for each driver's contract test using a committed
//! snapshot.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use zqlz_core::{
    CatalogCapabilities, CatalogSource, DatabaseObject, RawColumnRow, RawConstraintRow,
    RawDatabaseRow, RawDependencyRow, RawForeignKeyRow, RawIndexRow, RawRelationRow, RawRoutineRow,
    RawSchemaRow, RawSequenceRow, RawTriggerRow, RawTypeRow, RelationRef, Result,
};

fn schema_key(schema: Option<&str>) -> String {
    schema.unwrap_or("").to_string()
}

fn relation_key(relation: &RelationRef) -> String {
    format!("{}\u{1}{}", relation.schema.as_deref().unwrap_or(""), relation.name)
}

/// Recorded catalog data, serde-serializable for on-disk fixtures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub capabilities: CatalogCapabilities,
    #[serde(default)]
    pub databases: Vec<RawDatabaseRow>,
    #[serde(default)]
    pub schemas: Vec<RawSchemaRow>,
    /// Keyed by schema (empty string for `None`).
    #[serde(default)]
    pub relations: BTreeMap<String, Vec<RawRelationRow>>,
    /// Keyed by `schema\u{1}name`.
    #[serde(default)]
    pub columns: BTreeMap<String, Vec<RawColumnRow>>,
    #[serde(default)]
    pub indexes: BTreeMap<String, Vec<RawIndexRow>>,
    #[serde(default)]
    pub foreign_keys: BTreeMap<String, Vec<RawForeignKeyRow>>,
    #[serde(default)]
    pub constraints: BTreeMap<String, Vec<RawConstraintRow>>,
    /// Keyed by schema (empty string for `None`).
    #[serde(default)]
    pub triggers: BTreeMap<String, Vec<RawTriggerRow>>,
    #[serde(default)]
    pub routines: BTreeMap<String, Vec<RawRoutineRow>>,
    #[serde(default)]
    pub sequences: BTreeMap<String, Vec<RawSequenceRow>>,
    #[serde(default)]
    pub types: BTreeMap<String, Vec<RawTypeRow>>,
    /// Keyed by object name.
    #[serde(default)]
    pub object_sources: BTreeMap<String, String>,
}

impl CatalogSnapshot {
    pub fn new(capabilities: CatalogCapabilities) -> Self {
        Self {
            capabilities,
            databases: Vec::new(),
            schemas: Vec::new(),
            relations: BTreeMap::new(),
            columns: BTreeMap::new(),
            indexes: BTreeMap::new(),
            foreign_keys: BTreeMap::new(),
            constraints: BTreeMap::new(),
            triggers: BTreeMap::new(),
            routines: BTreeMap::new(),
            sequences: BTreeMap::new(),
            types: BTreeMap::new(),
            object_sources: BTreeMap::new(),
        }
    }

    pub fn with_relations(mut self, schema: Option<&str>, rows: Vec<RawRelationRow>) -> Self {
        self.relations.insert(schema_key(schema), rows);
        self
    }

    pub fn with_columns(mut self, relation: &RelationRef, rows: Vec<RawColumnRow>) -> Self {
        self.columns.insert(relation_key(relation), rows);
        self
    }

    pub fn with_indexes(mut self, relation: &RelationRef, rows: Vec<RawIndexRow>) -> Self {
        self.indexes.insert(relation_key(relation), rows);
        self
    }

    pub fn with_foreign_keys(mut self, relation: &RelationRef, rows: Vec<RawForeignKeyRow>) -> Self {
        self.foreign_keys.insert(relation_key(relation), rows);
        self
    }

    pub fn with_constraints(mut self, relation: &RelationRef, rows: Vec<RawConstraintRow>) -> Self {
        self.constraints.insert(relation_key(relation), rows);
        self
    }

    pub fn with_triggers(mut self, schema: Option<&str>, rows: Vec<RawTriggerRow>) -> Self {
        self.triggers.insert(schema_key(schema), rows);
        self
    }
}

/// Serves a [`CatalogSnapshot`] as a [`CatalogSource`].
pub struct RecordedCatalog {
    snapshot: CatalogSnapshot,
}

impl RecordedCatalog {
    pub fn new(snapshot: CatalogSnapshot) -> Self {
        Self { snapshot }
    }

    pub fn from_json(json: &str) -> Result<Self> {
        let snapshot: CatalogSnapshot = serde_json::from_str(json)
            .map_err(|error| zqlz_core::ZqlzError::Schema(format!("invalid catalog snapshot: {error}")))?;
        Ok(Self { snapshot })
    }
}

#[async_trait]
impl CatalogSource for RecordedCatalog {
    fn capabilities(&self) -> &CatalogCapabilities {
        &self.snapshot.capabilities
    }

    async fn fetch_relations(&self, schema: Option<&str>) -> Result<Vec<RawRelationRow>> {
        Ok(self
            .snapshot
            .relations
            .get(&schema_key(schema))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_columns(&self, relation: &RelationRef) -> Result<Vec<RawColumnRow>> {
        Ok(self
            .snapshot
            .columns
            .get(&relation_key(relation))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_indexes(&self, relation: &RelationRef) -> Result<Vec<RawIndexRow>> {
        Ok(self
            .snapshot
            .indexes
            .get(&relation_key(relation))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_foreign_keys(&self, relation: &RelationRef) -> Result<Vec<RawForeignKeyRow>> {
        Ok(self
            .snapshot
            .foreign_keys
            .get(&relation_key(relation))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_constraints(&self, relation: &RelationRef) -> Result<Vec<RawConstraintRow>> {
        Ok(self
            .snapshot
            .constraints
            .get(&relation_key(relation))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_databases(&self) -> Result<Vec<RawDatabaseRow>> {
        Ok(self.snapshot.databases.clone())
    }

    async fn fetch_schemas(&self) -> Result<Vec<RawSchemaRow>> {
        Ok(self.snapshot.schemas.clone())
    }

    async fn fetch_routines(&self, schema: Option<&str>) -> Result<Vec<RawRoutineRow>> {
        Ok(self
            .snapshot
            .routines
            .get(&schema_key(schema))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<RawTriggerRow>> {
        let rows = self
            .snapshot
            .triggers
            .get(&schema_key(schema))
            .cloned()
            .unwrap_or_default();
        Ok(match table {
            Some(table) => rows.into_iter().filter(|row| row.table_name == table).collect(),
            None => rows,
        })
    }

    async fn fetch_sequences(&self, schema: Option<&str>) -> Result<Vec<RawSequenceRow>> {
        Ok(self
            .snapshot
            .sequences
            .get(&schema_key(schema))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_types(&self, schema: Option<&str>) -> Result<Vec<RawTypeRow>> {
        Ok(self
            .snapshot
            .types
            .get(&schema_key(schema))
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_object_source(&self, object: &DatabaseObject) -> Result<Option<String>> {
        Ok(self.snapshot.object_sources.get(&object.name).cloned())
    }

    async fn fetch_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<RawDependencyRow>> {
        Ok(Vec::new())
    }
}
