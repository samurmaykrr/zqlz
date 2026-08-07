//! Raw-catalog port for schema introspection.
//!
//! A [`CatalogSource`] is the entire driver-side surface for schema
//! introspection: it runs catalog queries (SQL, PRAGMAs, native commands) and
//! reports **what the catalog said** as typed `Raw*` records. It must not
//! normalize, compose, or apply heuristics — that is the job of the shared
//! schema engine that implements [`SchemaIntrospection`] on top of this port.
//!
//! The honesty rule for adapters: if the catalog did not report a fact, the
//! field is `None`/`Unknown`, never a guessed default. Heuristics (auto
//! increment detection, foreign-key action parsing, primary-key derivation)
//! belong to the engine so they exist exactly once instead of being copied
//! across every driver.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Result, ZqlzError};

use super::{
    DatabaseObject, ObjectFormDdlRequest, ObjectFormSpec, ObjectFormSpecRequest, ObjectType,
    ObjectsPanelObjectKind, ObjectsPanelRow, TableType,
};

/// Where the engine looks for objects of a given kind. Replaces the bare
/// `schema: Option<&str>` parameter so multi-database engines and
/// database-less ones share one vocabulary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectScope {
    pub database: Option<String>,
    pub schema: Option<String>,
}

impl ObjectScope {
    pub fn for_schema(schema: Option<&str>) -> Self {
        Self {
            database: None,
            schema: schema.map(ToString::to_string),
        }
    }
}

/// Fully resolved reference to a relation (table/view) the engine wants
/// details for. The engine performs identifier/schema resolution once before
/// calling the port, so adapters receive an unambiguous schema + bare name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationRef {
    pub schema: Option<String>,
    pub name: String,
}

impl RelationRef {
    pub fn new(schema: Option<String>, name: impl Into<String>) -> Self {
        Self {
            schema,
            name: name.into(),
        }
    }
}

/// How a driver namespaces objects. Replaces the implicit "`schema:
/// Option<&str>`, default to `public`/`main`" dance scattered through every
/// driver today.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceModel {
    /// PostgreSQL, MSSQL, DuckDB: databases contain schemas.
    DatabasesAndSchemas { default_schema: String },
    /// MySQL, ClickHouse: database == schema, one level.
    DatabasesOnly,
    /// SQLite, Turso: one fixed namespace.
    SingleNamespace { name: String },
    /// Redis: numbered databases, no relational namespaces.
    NumberedDatabases { count: u16 },
}

impl NamespaceModel {
    /// Schema the engine substitutes when a caller passes `None`.
    pub fn default_schema(&self) -> Option<&str> {
        match self {
            NamespaceModel::DatabasesAndSchemas { default_schema } => Some(default_schema),
            NamespaceModel::SingleNamespace { name } => Some(name),
            NamespaceModel::DatabasesOnly | NamespaceModel::NumberedDatabases { .. } => None,
        }
    }
}

/// Which object kinds a catalog can enumerate. The engine consults this to
/// answer unsupported kinds with `Ok(vec![])` instead of every driver
/// hand-writing "return empty" stubs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectKindSupport {
    pub tables: bool,
    pub views: bool,
    pub materialized_views: bool,
    pub functions: bool,
    pub procedures: bool,
    pub triggers: bool,
    pub sequences: bool,
    pub types: bool,
}

impl ObjectKindSupport {
    /// Nothing supported — a starting point to switch flags on.
    pub const NONE: Self = Self {
        tables: false,
        views: false,
        materialized_views: false,
        functions: false,
        procedures: false,
        triggers: false,
        sequences: false,
        types: false,
    };

    /// The full relational set (Postgres-like).
    pub const ALL_RELATIONAL: Self = Self {
        tables: true,
        views: true,
        materialized_views: true,
        functions: true,
        procedures: true,
        triggers: true,
        sequences: true,
        types: true,
    };
}

impl Default for ObjectKindSupport {
    fn default() -> Self {
        Self::NONE
    }
}

/// Declarative auto-increment heuristics, applied by the engine instead of
/// each driver inlining its own check.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoIncrementRules {
    /// Column-default substrings (matched case-insensitively) that imply an
    /// auto-increment column, e.g. `"nextval("` for PostgreSQL.
    pub default_markers: Vec<String>,
    /// SQLite rule: an `INTEGER PRIMARY KEY` column is a rowid alias and is
    /// treated as auto-increment.
    pub integer_primary_key: bool,
}

/// Extra objects-panel column a driver surfaces beyond the standard set
/// (e.g. PostgreSQL's OID/Owner/ACL). The engine fills these from
/// [`RawRelationRow::extras`] by matching `extra_key`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelExtraColumn {
    /// Which panel kind this column appears on ("table", "view", ...).
    pub kind_id: String,
    /// Key into `RawRelationRow::extras` that feeds this column.
    pub extra_key: String,
    /// Display title.
    pub title: String,
    /// Right-align numeric-ish values when true.
    pub align_right: bool,
}

/// Static description of a driver's catalog. All cross-cutting policy that was
/// previously copy-pasted per driver lives here as data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogCapabilities {
    pub driver_id: String,
    pub server_version: Option<String>,
    pub namespaces: NamespaceModel,
    pub objects: ObjectKindSupport,
    pub auto_increment: AutoIncrementRules,
    /// Object types for which `fetch_object_source` returns stored DDL; for
    /// anything else the engine synthesizes DDL via the dialect.
    pub stored_source: Vec<ObjectType>,
    /// Whether foreign-key constraints can be `DEFERRABLE` (PostgreSQL only).
    pub deferrable_constraints: bool,
    /// Driver-specific objects-panel columns.
    pub panel_extras: Vec<PanelExtraColumn>,
}

impl CatalogCapabilities {
    /// Minimal capabilities for a single-namespace relational engine that only
    /// knows about tables, views and triggers (SQLite-shaped).
    pub fn single_namespace(driver_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            driver_id: driver_id.into(),
            server_version: None,
            namespaces: NamespaceModel::SingleNamespace {
                name: namespace.into(),
            },
            objects: ObjectKindSupport {
                tables: true,
                views: true,
                triggers: true,
                ..ObjectKindSupport::NONE
            },
            auto_increment: AutoIncrementRules::default(),
            stored_source: Vec::new(),
            deferrable_constraints: false,
            panel_extras: Vec::new(),
        }
    }
}

/// What the catalog reports about a column's identity, kept distinct from the
/// engine's *conclusion* (`ColumnInfo::is_auto_increment`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RawIdentity {
    /// Catalog explicitly reports a non-identity column.
    #[default]
    None,
    /// Catalog reports an identity / auto-increment column.
    Declared,
    /// Catalog does not track identity; the engine should apply heuristics.
    Unknown,
}

/// Raw relation row (table/view/matview/...). The engine fans this out into
/// `list_tables` / `list_views` / `list_tables_extended`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRelationRow {
    pub schema: Option<String>,
    pub name: String,
    pub table_type: TableType,
    pub owner: Option<String>,
    pub row_estimate: Option<i64>,
    pub size_bytes: Option<i64>,
    pub index_count: Option<i64>,
    pub trigger_count: Option<i64>,
    pub comment: Option<String>,
    /// View/matview definition when the catalog returns it.
    pub view_definition: Option<String>,
    /// Driver-specific panel data, keyed to match `CatalogCapabilities::panel_extras`.
    #[serde(default)]
    pub extras: BTreeMap<String, String>,
}

impl RawRelationRow {
    pub fn new(name: impl Into<String>, table_type: TableType) -> Self {
        Self {
            schema: None,
            name: name.into(),
            table_type,
            owner: None,
            row_estimate: None,
            size_bytes: None,
            index_count: None,
            trigger_count: None,
            comment: None,
            view_definition: None,
            extras: BTreeMap::new(),
        }
    }
}

/// Raw column row. The engine derives `is_primary_key`, `is_unique`,
/// `foreign_key`, and `is_auto_increment` — adapters only report observations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawColumnRow {
    pub name: String,
    pub ordinal: i64,
    pub data_type: String,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    pub max_length: Option<i64>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub identity: RawIdentity,
    /// Per-column primary-key membership when the catalog reports it
    /// (e.g. SQLite `PRAGMA table_info` `pk`); `None` when PK comes from a
    /// separate constraint catalog.
    pub primary_key_ordinal: Option<i64>,
    pub enum_values: Option<Vec<String>>,
    pub comment: Option<String>,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub generation_expression: Option<String>,
    pub is_generated_stored: bool,
}

/// Raw index row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawIndexRow {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    /// Index method ("btree", "gin", ...); empty falls back to a default.
    pub method: Option<String>,
    pub where_clause: Option<String>,
    pub include_columns: Vec<String>,
    pub column_descending: Vec<bool>,
    pub comment: Option<String>,
}

/// Raw foreign-key row. `on_update`/`on_delete` carry the catalog's verbatim
/// strings (`"CASCADE"`, `"a"`, `"SET NULL"`); the engine parses them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawForeignKeyRow {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: Option<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_update: Option<String>,
    pub on_delete: Option<String>,
    pub is_deferrable: bool,
    pub initially_deferred: bool,
}

/// Raw constraint row. `kind` is the catalog's verbatim constraint type
/// (`"PRIMARY KEY"`, `"p"`, `"CHECK"`, ...); the engine normalizes it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawConstraintRow {
    pub name: String,
    pub kind: String,
    pub columns: Vec<String>,
    pub definition: Option<String>,
}

/// Raw routine row (function or procedure).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawRoutineRow {
    pub schema: Option<String>,
    pub name: String,
    /// `true` for procedures, `false` for functions.
    pub is_procedure: bool,
    pub language: String,
    pub return_type: String,
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Raw trigger row. Timing/events/for-each carry catalog strings the engine
/// normalizes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawTriggerRow {
    pub schema: Option<String>,
    pub name: String,
    pub table_name: String,
    pub timing: Option<String>,
    pub events: Vec<String>,
    pub for_each: Option<String>,
    pub definition: Option<String>,
    pub enabled: bool,
}

/// Raw sequence row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawSequenceRow {
    pub schema: Option<String>,
    pub name: String,
    pub data_type: String,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment_by: i64,
    pub current_value: Option<i64>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Raw user-defined type row. `kind` is the catalog's verbatim type kind.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawTypeRow {
    pub schema: Option<String>,
    pub name: String,
    pub kind: String,
    pub values: Option<Vec<String>>,
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Raw database row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawDatabaseRow {
    pub name: String,
    pub owner: Option<String>,
    pub encoding: Option<String>,
    pub size_bytes: Option<i64>,
    pub comment: Option<String>,
}

/// Raw schema row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawSchemaRow {
    pub name: String,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Raw dependency edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDependencyRow {
    pub dependent: DatabaseObject,
    pub referenced: DatabaseObject,
    pub dependency_type: String,
}

/// The raw-catalog-data port. One per live connection.
///
/// Adapters implement the `fetch_*` methods they support and rely on the
/// defaults for the rest; the engine never calls a fetch for a kind that
/// [`CatalogCapabilities::objects`] reports as unsupported.
#[async_trait]
pub trait CatalogSource: Send + Sync {
    /// Static description of what this catalog supports.
    fn capabilities(&self) -> &CatalogCapabilities;

    /// Tables, views, materialized views and similar relations in one call —
    /// most catalogs answer this from a single system table.
    async fn fetch_relations(&self, schema: Option<&str>) -> Result<Vec<RawRelationRow>>;

    /// Columns of a relation.
    async fn fetch_columns(&self, relation: &RelationRef) -> Result<Vec<RawColumnRow>>;

    /// Columns of every relation in a schema, keyed by bare relation name.
    ///
    /// Catalogs whose column metadata is itself a queryable relation
    /// (`information_schema.columns`, `sys.columns`, `pragma_table_info`) answer this
    /// with a single round-trip, which is what makes schema-wide warm-up viable —
    /// fetching per relation costs one query per table plus whatever pacing the
    /// caller has to impose to stay off a connection pooler's back.
    ///
    /// `Ok(None)` means "no bulk form, fetch per relation instead"; it is distinct
    /// from `Ok(Some(empty))`, which means the schema genuinely has no columns.
    async fn fetch_all_columns(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawColumnRow>>>> {
        Ok(None)
    }

    async fn fetch_indexes(&self, _relation: &RelationRef) -> Result<Vec<RawIndexRow>> {
        Ok(Vec::new())
    }

    async fn fetch_foreign_keys(&self, _relation: &RelationRef) -> Result<Vec<RawForeignKeyRow>> {
        Ok(Vec::new())
    }

    /// Foreign keys of every relation in a schema, keyed by bare relation name.
    ///
    /// Same contract as [`Self::fetch_all_columns`].
    async fn fetch_all_foreign_keys(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawForeignKeyRow>>>> {
        Ok(None)
    }

    async fn fetch_constraints(&self, _relation: &RelationRef) -> Result<Vec<RawConstraintRow>> {
        Ok(Vec::new())
    }

    async fn fetch_databases(&self) -> Result<Vec<RawDatabaseRow>> {
        Ok(Vec::new())
    }

    async fn fetch_schemas(&self) -> Result<Vec<RawSchemaRow>> {
        Ok(Vec::new())
    }

    async fn fetch_routines(&self, _schema: Option<&str>) -> Result<Vec<RawRoutineRow>> {
        Ok(Vec::new())
    }

    async fn fetch_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<RawTriggerRow>> {
        Ok(Vec::new())
    }

    async fn fetch_sequences(&self, _schema: Option<&str>) -> Result<Vec<RawSequenceRow>> {
        Ok(Vec::new())
    }

    async fn fetch_types(&self, _schema: Option<&str>) -> Result<Vec<RawTypeRow>> {
        Ok(Vec::new())
    }

    /// Stored source text where the catalog keeps it (sqlite_master.sql,
    /// pg_get_viewdef); `None` means the engine should synthesize DDL.
    async fn fetch_object_source(&self, _object: &DatabaseObject) -> Result<Option<String>> {
        Ok(None)
    }

    async fn fetch_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<RawDependencyRow>> {
        Ok(Vec::new())
    }
}

/// Pure, synchronous, no-I/O dialect strategy. Owns string-building concerns
/// (object forms, DDL synthesis) that are not catalog reads.
pub trait CatalogDialect: Send + Sync {
    /// Driver-defined create/edit/drop form for an object kind.
    fn object_form_spec(&self, _request: &ObjectFormSpecRequest) -> Result<Option<ObjectFormSpec>> {
        Ok(None)
    }

    /// Generate executable DDL from a driver-defined object form payload.
    fn generate_object_form_ddl(&self, _request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
        Err(ZqlzError::NotSupported(
            "Object form DDL generation is not supported by this driver".to_string(),
        ))
    }

    /// Synthesize `CREATE` DDL from normalized details when the catalog has no
    /// stored source. Default refuses; SQL drivers that need synthesis override.
    fn render_object_ddl(
        &self,
        object: &DatabaseObject,
        _details: Option<&super::TableDetails>,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(format!(
            "DDL generation is not supported for {:?}",
            object.object_type
        )))
    }
}

/// A registered objects-panel kind. The open extension point: new kinds
/// (Postgres extensions, Redis databases, Mongo collections) are added by
/// registering another source without touching the engine.
#[async_trait]
pub trait ObjectKindSource: Send + Sync {
    /// Descriptor: id, labels, icon, columns, row actions.
    fn descriptor(&self) -> ObjectsPanelObjectKind;

    /// Rows of this kind for the given scope.
    async fn rows(&self, scope: &ObjectScope) -> Result<Vec<ObjectsPanelRow>>;

    async fn form_spec(&self, _request: &ObjectFormSpecRequest) -> Result<Option<ObjectFormSpec>> {
        Ok(None)
    }

    async fn form_ddl(&self, _request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
        Err(ZqlzError::NotSupported(
            "Object form DDL generation is not supported for this object kind".to_string(),
        ))
    }
}
