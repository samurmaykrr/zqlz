//! Connection trait and transaction handling

use crate::{
    DriverCategory, ExplainConfig, QueryResult, Result, SchemaIntrospection, StatementResult,
    TableType, Value, driver_category_from_driver_name,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Policy for formatting SQL bind placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindPlaceholderPolicy {
    /// Use `?` for every parameter.
    #[default]
    QuestionMark,
    /// Use PostgreSQL style placeholders (`$1`, `$2`, ...).
    DollarNumbered,
    /// Use Oracle/SQLite named index placeholders (`:1`, `:2`, ...).
    ColonNumbered,
}

impl BindPlaceholderPolicy {
    /// Format a placeholder for a 0-based parameter index.
    pub fn format(self, parameter_index: usize) -> String {
        let one_based_index = parameter_index.saturating_add(1);
        match self {
            Self::QuestionMark => "?".to_string(),
            Self::DollarNumbered => format!("${}", one_based_index),
            Self::ColonNumbered => format!(":{}", one_based_index),
        }
    }
}

/// How much a driver's reported affected-row count can be trusted.
///
/// A zero count means "the row is gone" only when the driver actually reports
/// matched rows. MySQL reports *changed* rows unless `CLIENT_FOUND_ROWS` is
/// negotiated, and some drivers do not report counts at all, so callers must
/// know which guarantee they have before treating zero as a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AffectedRowCountFidelity {
    /// The count is the number of rows the statement matched. Zero proves that
    /// no row matched.
    #[default]
    Exact,
    /// The count is usually correct but can be suppressed by the server (for
    /// example a SQL Server trigger running `SET NOCOUNT ON`). Zero is
    /// inconclusive.
    BestEffort,
    /// The driver does not report affected-row counts. The value carries no
    /// information at all.
    Unavailable,
}

/// Logical scope requested before selecting a physical/session connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionScope {
    Default,
    Database(String),
    Namespace(String),
    KeyValueDatabase(u16),
}

/// Driver-normalized connection scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConnectionScope {
    pub requested_scope: ConnectionScope,
    pub normalized_scope: ConnectionScope,
    pub physical_database_key: Option<String>,
    pub effective_database: Option<String>,
    pub effective_namespace: Option<String>,
    pub introspection_scope: Option<String>,
    pub requires_dedicated_connection: bool,
}

impl ResolvedConnectionScope {
    pub fn default_scope() -> Self {
        Self {
            requested_scope: ConnectionScope::Default,
            normalized_scope: ConnectionScope::Default,
            physical_database_key: None,
            effective_database: None,
            effective_namespace: None,
            introspection_scope: None,
            requires_dedicated_connection: false,
        }
    }
}

/// Generic key-value value family exposed by drivers such as Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum KeyValueKind {
    #[default]
    String,
    List,
    Set,
    ZSet,
    Hash,
    Stream,
    Json,
    None,
}

impl KeyValueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::Hash => "hash",
            Self::Stream => "stream",
            Self::Json => "json",
            Self::None => "none",
        }
    }
}

/// One logical key-value database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueDatabaseInfo {
    pub index: u16,
    pub size_bytes: Option<i64>,
}

/// Incremental key scan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueScanRequest {
    pub database_index: u16,
    pub limit: usize,
    pub scan_batch_size: usize,
}

/// Key names returned from one or more scan iterations.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KeyValueScanResult {
    pub keys: Vec<String>,
}

/// Summary row for a key-value database browser.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyValueKeySummary {
    pub key: String,
    pub kind: KeyValueKind,
    pub ttl_seconds: Option<i64>,
    pub size_bytes: Option<i64>,
    pub preview: Option<String>,
}

/// Full key data in generic rows.
#[derive(Debug, Clone)]
pub struct KeyValueEntry {
    pub key: String,
    pub kind: KeyValueKind,
    pub ttl_seconds: Option<i64>,
    pub data: QueryResult,
}

/// Save or rename one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueSaveRequest {
    pub original_key: String,
    pub new_key: String,
    pub kind: KeyValueKind,
    pub serialized_value: String,
    pub ttl_seconds: Option<u64>,
    pub temporary_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KeyValueSaveOutcome {
    pub was_renamed: bool,
}

/// Delete keys from active key-value database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueDeleteRequest {
    pub key_names: Vec<String>,
    pub continue_on_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KeyValueDeleteOutcome {
    pub deleted_key_names: Vec<String>,
    pub errors: Vec<String>,
}

/// Update one grid cell for a key entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyValueCellUpdateRequest {
    pub key: String,
    pub kind: KeyValueKind,
    pub column_name: String,
    pub new_value: Option<Value>,
    pub row_values: Vec<Value>,
}

/// Capability trait for key-value stores.
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    async fn list_key_value_databases(&self) -> Result<Vec<KeyValueDatabaseInfo>>;
    async fn scan_keys(&self, request: KeyValueScanRequest) -> Result<KeyValueScanResult>;
    async fn load_key_summaries(&self, database_index: u16) -> Result<Vec<KeyValueKeySummary>>;
    async fn read_key(&self, key: &str, limit: usize) -> Result<KeyValueEntry>;
    async fn save_key(&self, request: KeyValueSaveRequest) -> Result<KeyValueSaveOutcome>;
    async fn update_key_cell(&self, request: KeyValueCellUpdateRequest) -> Result<()>;
    async fn delete_keys(&self, request: KeyValueDeleteRequest) -> Result<KeyValueDeleteOutcome>;
}

/// One document database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentDatabaseInfo {
    pub name: String,
    pub size_bytes: Option<u64>,
    pub empty: bool,
}

/// One collection/document set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentCollectionInfo {
    pub database: String,
    pub name: String,
    pub collection_type: String,
    pub document_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub index_count: Option<u32>,
    pub options_json: Option<serde_json::Value>,
    pub validator_json: Option<serde_json::Value>,
    pub collation_json: Option<serde_json::Value>,
    pub view_on: Option<String>,
    pub pipeline_json: Option<serde_json::Value>,
    pub timeseries_json: Option<serde_json::Value>,
    pub clustered_index_json: Option<serde_json::Value>,
    pub change_stream_pre_and_post_images: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentIndexInfo {
    pub database: String,
    pub collection: String,
    pub name: String,
    pub keys_json: serde_json::Value,
    pub options_json: serde_json::Value,
    pub unique: bool,
    pub sparse: bool,
    pub ttl_seconds: Option<i64>,
    pub partial_filter_json: Option<serde_json::Value>,
    pub collation_json: Option<serde_json::Value>,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentFunctionInfo {
    pub database: String,
    pub name: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentGridFsBucketInfo {
    pub database: String,
    pub name: String,
    pub files_collection: String,
    pub chunks_collection: String,
    pub file_count: Option<u64>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentAdminObjectInfo {
    pub database: String,
    pub kind: String,
    pub name: String,
    pub details_json: serde_json::Value,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DocumentDatabaseObjects {
    pub collections: Vec<DocumentCollectionInfo>,
    pub indexes: Vec<DocumentIndexInfo>,
    pub functions: Vec<DocumentFunctionInfo>,
    pub gridfs_buckets: Vec<DocumentGridFsBucketInfo>,
    pub users: Vec<DocumentAdminObjectInfo>,
    pub roles: Vec<DocumentAdminObjectInfo>,
    pub search_indexes: Vec<DocumentAdminObjectInfo>,
    pub vector_indexes: Vec<DocumentAdminObjectInfo>,
    pub server: Vec<DocumentAdminObjectInfo>,
    pub sharding: Vec<DocumentAdminObjectInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentQueryRequest {
    pub database: String,
    pub collection: String,
    pub filter_json: Option<String>,
    pub projection_json: Option<String>,
    pub sort_json: Option<String>,
    pub skip: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentAggregateRequest {
    pub database: String,
    pub collection: String,
    pub pipeline_json: String,
    pub limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSaveRequest {
    pub database: String,
    pub collection: String,
    pub document_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentReplaceRequest {
    pub database: String,
    pub collection: String,
    pub id_json: String,
    pub document_json: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentCellUpdateRequest {
    pub database: String,
    pub collection: String,
    pub id_json: String,
    pub field_path: String,
    pub new_value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentDeleteRequest {
    pub database: String,
    pub collection: String,
    pub ids_json: Vec<String>,
    pub continue_on_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DocumentDeleteOutcome {
    pub deleted_ids: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSchemaSampleRequest {
    pub database: String,
    pub collection: String,
    pub sample_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentInferredField {
    pub name: String,
    pub types: Vec<String>,
    pub occurrence_count: u64,
    pub is_required: bool,
}

/// Capability trait for document stores.
#[async_trait]
pub trait DocumentStore: Send + Sync {
    async fn list_document_databases(&self) -> Result<Vec<DocumentDatabaseInfo>>;
    async fn list_collections(&self, database: &str) -> Result<Vec<DocumentCollectionInfo>>;
    async fn list_database_objects(&self, database: &str) -> Result<DocumentDatabaseObjects> {
        Ok(DocumentDatabaseObjects {
            collections: self.list_collections(database).await?,
            ..Default::default()
        })
    }
    async fn query_documents(&self, request: DocumentQueryRequest) -> Result<QueryResult>;
    async fn aggregate_documents(&self, request: DocumentAggregateRequest) -> Result<QueryResult>;
    async fn insert_document(&self, request: DocumentSaveRequest) -> Result<Value>;
    async fn replace_document(&self, request: DocumentReplaceRequest) -> Result<()>;
    async fn update_document_cell(&self, request: DocumentCellUpdateRequest) -> Result<()>;
    async fn delete_documents(
        &self,
        request: DocumentDeleteRequest,
    ) -> Result<DocumentDeleteOutcome>;
    async fn sample_schema(
        &self,
        request: DocumentSchemaSampleRequest,
    ) -> Result<Vec<DocumentInferredField>>;
}

/// SQL object name with an optional namespace qualifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlObjectName {
    /// Optional namespace/schema/database qualifier.
    pub namespace: Option<String>,
    /// Unqualified object name.
    pub name: String,
}

impl SqlObjectName {
    /// Create an unqualified object name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    /// Create a qualified object name.
    pub fn with_namespace(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }
}

/// Options for DROP VIEW statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropViewOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Options for DROP TABLE statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropTableOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Options for DROP TRIGGER statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropTriggerOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Parser strategy for EXPLAIN output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainParserKind {
    /// PostgreSQL EXPLAIN JSON parser.
    PostgreSql,
    /// MySQL/MariaDB EXPLAIN JSON parser.
    MySql,
    /// SQLite EXPLAIN QUERY PLAN text parser.
    Sqlite,
    /// Driver exposes EXPLAIN output, but no normalized parser is available.
    Raw,
    /// No parser available for this driver.
    None,
}

/// Semantic default values that require driver-specific SQL rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportSemanticDefault {
    CurrentUser,
    GeneratedUuid,
}

/// Support level for CHECK constraint enforcement on the active connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckConstraintEnforcement {
    /// CHECK constraints are expected to be enforced.
    Enforced,
    /// CHECK constraints may be ignored or not fully enforced.
    NotEnforced,
    /// Unknown/depends on server version; caller should warn conservatively.
    Unknown,
}

/// Capability matrix used when mapping source indexes to target SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportIndexCapabilities {
    pub supports_hash: bool,
    pub supports_gin: bool,
    pub supports_gist: bool,
    pub supports_spgist: bool,
    pub supports_brin: bool,
    pub supports_fulltext: bool,
    pub supports_spatial: bool,
    pub supports_partial: bool,
    pub supports_include: bool,
    pub supports_nulls_ordering: bool,
}

impl ImportIndexCapabilities {
    pub const fn standard() -> Self {
        Self {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: false,
            supports_include: false,
            supports_nulls_ordering: false,
        }
    }
}

/// SQL snippets to disable and re-enable FK checks for bulk import workflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyChecksSql {
    pub disable_sql: String,
    pub enable_sql: String,
}

impl DropViewOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

impl DropTableOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

impl DropTriggerOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

/// Handle for cancelling a running query from any thread.
///
/// This trait allows database drivers to provide a way to interrupt
/// long-running queries. The handle is safe to call from any thread
/// and can be called multiple times (subsequent calls are no-ops).
pub trait QueryCancelHandle: Send + Sync {
    /// Cancel the currently running query on the associated connection.
    ///
    /// This method is safe to call from any thread and is idempotent.
    /// If no query is running, this is a no-op.
    fn cancel(&self);
}

/// Request to update a single cell value
#[derive(Debug, Clone)]
pub struct CellUpdateRequest {
    /// Table name
    pub table_name: String,
    /// Column name to update
    pub column_name: String,
    /// Database column type, when known
    pub column_type: Option<String>,
    /// Database column types for row identifier columns, when known
    pub row_column_types: Vec<(String, String)>,
    /// New value (None for NULL)
    pub new_value: Option<Value>,
    /// Row identifier - can be row index, primary key value, or full row data
    pub row_identifier: RowIdentifier,
}

/// Different ways to identify a row for updating
#[derive(Debug, Clone)]
pub enum RowIdentifier {
    /// Use row index/offset (0-based)
    RowIndex(usize),
    /// Use the values of a key that identifies the row on its own: the primary
    /// key, or a unique key when the table has no primary key.
    PrimaryKey(Vec<(String, Value)>),
    /// Use all column values to identify the row. Tables with duplicate rows
    /// need [`Connection::single_row_dml_scope`] to keep the statement from
    /// writing every copy.
    FullRow(Vec<(String, Value)>),
}

/// How a driver keeps a keyless `UPDATE`/`DELETE` from touching more than one row.
///
/// Rows in a table without a primary or unique key are matched on all their
/// column values, which cannot tell duplicate rows apart. Each dialect narrows
/// that with whatever it has: a physical row identity (SQLite `rowid`,
/// PostgreSQL `ctid`) or a statement-level row limit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SingleRowDmlScope {
    /// The dialect offers no way to narrow the statement, so every matching
    /// duplicate is written.
    #[default]
    Unsupported,
    /// Replacement WHERE clause, already narrowed to one physical row.
    WhereClause(String),
    /// Clause appended after the WHERE clause, leading space included, such as
    /// `" LIMIT 1"`.
    StatementSuffix(&'static str),
}

impl SingleRowDmlScope {
    /// Narrow by a physical row-identity expression, e.g. SQLite's `rowid`.
    pub fn by_row_identity(
        identity_expression: &str,
        qualified_table: &str,
        where_clause: &str,
    ) -> Self {
        Self::WhereClause(format!(
            "{identity_expression} IN (SELECT {identity_expression} FROM {qualified_table} WHERE {where_clause} LIMIT 1)"
        ))
    }

    /// The WHERE clause to use, plus the clause to append to the statement.
    ///
    /// The suffix already carries its leading space, so callers can write
    /// `format!("... WHERE {where_clause}{suffix}")` unconditionally.
    pub fn apply<'a>(&'a self, where_clause: &'a str) -> (&'a str, &'a str) {
        match self {
            Self::Unsupported => (where_clause, ""),
            Self::WhereClause(scoped) => (scoped.as_str(), ""),
            Self::StatementSuffix(suffix) => (where_clause, suffix),
        }
    }
}

/// A database connection
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get the driver name (e.g., "sqlite", "postgresql", "mysql")
    fn driver_name(&self) -> &str;

    /// Execute a statement that modifies data (INSERT/UPDATE/DELETE)
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;

    /// Execute a query that returns rows (SELECT)
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Get the dialect identifier for this connection (e.g., "sqlite", "postgresql")
    ///
    /// This is used by the query service to look up dialect-specific behavior
    /// like EXPLAIN syntax. Returns None if the dialect is unknown.
    fn dialect_id(&self) -> Option<&'static str> {
        None
    }

    /// Broad category used by services/UI to route behavior without concrete
    /// driver-name branching.
    fn driver_category(&self) -> DriverCategory {
        driver_category_from_driver_name(self.driver_name())
    }

    /// Return a lightweight liveness statement for this connection.
    ///
    /// Used by connection heartbeats. Non-SQL drivers must override this with
    /// whatever their `query` implementation accepts as a no-op round trip
    /// (for example `PING` for Redis), otherwise the heartbeat reports healthy
    /// connections as failing.
    fn ping_query_sql(&self) -> &'static str {
        "SELECT 1"
    }

    /// Whether a query is currently occupying this connection.
    ///
    /// Drivers that serialise all traffic through a single guarded client should
    /// report this, because a heartbeat ping would otherwise queue behind a long
    /// user query and time out. An in-flight query is itself evidence the
    /// connection is in use, so heartbeats skip a busy connection rather than
    /// misreading the wait as a dead transport. Drivers that cannot tell return
    /// `false`.
    fn is_busy(&self) -> bool {
        false
    }

    /// Whether periodic heartbeats keep this connection usable.
    ///
    /// Connections that cross a network need regular traffic, because servers,
    /// connection poolers and NAT gateways all reap idle sessions. Embedded
    /// engines talking to a local file have no transport to keep warm and
    /// override this to `false` so the heartbeat skips them.
    fn requires_heartbeat(&self) -> bool {
        true
    }

    /// Whether this driver requires one connection per logical database.
    ///
    /// Drivers that can query only within a single selected database (for example
    /// PostgreSQL, SQL Server, Redis logical databases) can override this to true
    /// so connection managers create per-database connection instances.
    fn requires_database_scoped_connection(&self) -> bool {
        false
    }

    /// Normalize a database/scope name used for per-database connection lookup.
    ///
    /// Drivers may override this when UI labels differ from driver-native names.
    fn normalize_database_scope_name(&self, database_name: &str) -> String {
        database_name.to_string()
    }

    /// Resolve a logical scope into driver-owned connection and introspection
    /// semantics.
    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();
        resolved.normalized_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {}
            ConnectionScope::Database(database_name) => {
                let normalized_database_name =
                    self.normalize_database_scope_name(database_name.trim());
                resolved.normalized_scope =
                    ConnectionScope::Database(normalized_database_name.clone());
                resolved.effective_database = Some(normalized_database_name.clone());
                if self.requires_database_scoped_connection() {
                    resolved.physical_database_key = Some(normalized_database_name);
                    resolved.requires_dedicated_connection = true;
                } else {
                    resolved.effective_namespace = Some(normalized_database_name.clone());
                    resolved.introspection_scope = Some(normalized_database_name);
                }
            }
            ConnectionScope::Namespace(namespace) => {
                let namespace = namespace.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                resolved.effective_namespace = Some(namespace.clone());
                resolved.introspection_scope = Some(namespace);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                let database_name = index.to_string();
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
                resolved.effective_database = Some(database_name.clone());
                if self.requires_database_scoped_connection() {
                    resolved.physical_database_key = Some(database_name);
                    resolved.requires_dedicated_connection = true;
                }
            }
        }

        Ok(resolved)
    }

    /// Resolve the current session database/catalog name when the driver has
    /// one. Drivers own any SQL needed for this.
    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Resolve the current session namespace/schema name when the driver has
    /// one. Drivers own any SQL needed for this.
    async fn current_namespace_name(&self) -> Result<Option<String>> {
        self.resolve_session_namespace().await
    }

    /// Return the EXPLAIN configuration for this connection.
    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::default()
    }

    /// Return the parser kind to use for EXPLAIN output.
    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::None
    }

    /// Quote a SQL identifier for this connection's dialect.
    ///
    /// The default uses SQL-standard double quotes and escapes embedded quotes.
    fn quote_identifier(&self, identifier: &str) -> String {
        let escaped_identifier = identifier.replace('"', "\"\"");
        format!("\"{}\"", escaped_identifier)
    }

    /// Render a possibly-qualified SQL object name.
    fn render_qualified_name(&self, object_name: &SqlObjectName) -> String {
        match object_name.namespace.as_deref() {
            Some(namespace) => {
                format!(
                    "{}.{}",
                    self.quote_identifier(namespace),
                    self.quote_identifier(&object_name.name)
                )
            }
            None => self.quote_identifier(&object_name.name),
        }
    }

    /// Placeholder formatting policy used by this connection.
    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    /// Format a bind placeholder for a 0-based parameter index.
    fn format_bind_placeholder(&self, parameter_index: usize) -> String {
        self.bind_placeholder_policy().format(parameter_index)
    }

    /// Maximum number of bind parameters supported in a single statement.
    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    /// Wrap a base `SELECT` statement with pagination syntax for this dialect.
    ///
    /// `base_sql` should be a complete query without trailing limit/offset clauses.
    fn paginated_select_sql(&self, base_sql: &str, limit: u64, offset: u64) -> String {
        format!("{} LIMIT {} OFFSET {}", base_sql, limit, offset)
    }

    /// Wrap a base `SELECT` statement with limit-only pagination syntax.
    fn limited_select_sql(&self, base_sql: &str, limit: u64) -> String {
        self.paginated_select_sql(base_sql, limit, 0)
    }

    /// Generate a SQL expression suitable for text-search comparisons.
    ///
    /// This hook lets drivers cast non-text expressions to a searchable text type.
    /// Default behavior is conservative and returns the input expression unchanged.
    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    /// Normalize imported CHECK expressions for this connection.
    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    /// Keyword used for generated column storage mode.
    fn generated_column_storage_keyword(&self, requested_stored: bool) -> &'static str {
        if requested_stored {
            "STORED"
        } else {
            "VIRTUAL"
        }
    }

    /// Render SQL for semantic defaults that vary by driver.
    fn semantic_default_sql(&self, _kind: ImportSemanticDefault) -> Option<String> {
        None
    }

    /// Whether this connection supports partial indexes (`WHERE` clause).
    fn supports_partial_indexes(&self) -> bool {
        false
    }

    /// Whether this connection supports include/covering columns in indexes.
    fn supports_include_indexes(&self) -> bool {
        false
    }

    /// Whether this connection supports `NULLS FIRST/LAST` in index ordering.
    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        false
    }

    /// Rich index capability matrix for import-time index translation.
    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_partial: self.supports_partial_indexes(),
            supports_include: self.supports_include_indexes(),
            supports_nulls_ordering: self.supports_nulls_ordering_in_indexes(),
            ..ImportIndexCapabilities::standard()
        }
    }

    /// Generate SQL to rename a table.
    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String>;

    /// Generate SQL to drop a table.
    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String>;

    /// Generate SQL to drop a view.
    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String>;

    /// Generate SQL to drop a trigger.
    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String>;

    /// Generate SQL to truncate/clear all rows from a table.
    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL to duplicate one table into another.
    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String>;

    /// Generate SQL to clear all rows from a table without dropping schema.
    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL that checks whether a table contains at least one row.
    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL for selecting rows from a table.
    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String>;

    /// Generate SQL for selecting distinct rows with ordering and a row limit.
    fn select_distinct_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String>;

    /// Generate SQL for inserting a single row.
    fn insert_row_sql(
        &self,
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String>;

    /// Generate SQL to reset table identity/auto-increment state when supported.
    fn reset_table_identity_sql(&self, _table_name: &SqlObjectName) -> Option<String> {
        None
    }

    /// Generate SQL to restore a sequence/identity counter to a value.
    fn restore_sequence_sql(&self, _sequence_name: &str, _current_value: i64) -> Option<String> {
        None
    }

    /// Read the current sequence/identity value for a table column when available.
    async fn export_sequence_current_value(
        &self,
        _table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Export named enum type definitions as `(name, values)` pairs.
    async fn export_named_enum_definitions(&self) -> Result<Vec<(String, Vec<String>)>> {
        Ok(Vec::new())
    }

    /// Whether and how CHECK constraints are enforced on this connection.
    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Enforced
    }

    /// SQL to toggle foreign-key checks for bulk import operations.
    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        None
    }

    /// Whether target uses schema-level enum type creation during import.
    fn supports_import_named_enum_types(&self) -> bool {
        false
    }

    /// Normalize `CREATE VIEW` SQL before execution.
    ///
    /// Drivers can use this to enforce dialect-specific requirements.
    fn normalize_create_view_sql(&self, sql: &str) -> String {
        sql.trim().to_string()
    }

    /// Resolve the current session namespace/schema for this connection.
    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Whether this connection supports top-level trigger listing.
    ///
    /// Some databases expose triggers as table-level metadata only.
    fn supports_top_level_triggers(&self) -> bool {
        true
    }

    /// Whether this connection supports materialized views.
    fn supports_materialized_views(&self) -> bool {
        false
    }

    /// Whether this connection has a meaningful session namespace/schema.
    fn has_session_namespace(&self) -> bool {
        true
    }

    /// Whether this connection supports a fast exact row count strategy.
    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    /// How much this connection's reported affected-row counts can be trusted.
    ///
    /// Callers that treat a zero count as "no row matched" must consult this
    /// first; see [`AffectedRowCountFidelity`].
    fn affected_row_count_fidelity(&self) -> AffectedRowCountFidelity {
        AffectedRowCountFidelity::Exact
    }

    /// Whether a failed table browse should degrade to schema-only metadata.
    fn should_use_schema_only_table_browse_fallback(
        &self,
        _table_type: TableType,
        _error_message: &str,
    ) -> bool {
        false
    }

    /// Whether failed column introspection should fall back to parsing DDL.
    fn should_use_ddl_column_fallback(&self, _table_type: TableType, _error_message: &str) -> bool {
        false
    }

    /// Return an estimated row count for an object when available.
    async fn estimated_row_count(&self, _table_name: &SqlObjectName) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Generate the driver-specific SQL used to collect performance metrics.
    fn performance_metrics_query_sql(&self) -> Result<String>;

    /// How to keep a keyless `UPDATE`/`DELETE` from touching more than one row.
    ///
    /// Only [`RowIdentifier::FullRow`] statements need this: a key already
    /// matches at most one row. `qualified_table` must be quoted for this
    /// dialect, and `where_clause` is the keyless match this narrows.
    fn single_row_dml_scope(
        &self,
        _qualified_table: &str,
        _where_clause: &str,
    ) -> SingleRowDmlScope {
        SingleRowDmlScope::Unsupported
    }

    /// Update a single cell value
    ///
    /// This is a high-level method that each database driver implements according to
    /// its specific requirements. For example:
    /// - SQLite might use ROWID
    /// - PostgreSQL might use ctid or primary keys
    /// - MySQL might use primary keys
    ///
    /// Returns the number of rows affected.
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value"
        );
        // Default implementation uses the row identifier to build a WHERE clause
        // Drivers can override this for database-specific optimizations.
        // The SET value binds first, so WHERE parameters start after it.
        let mut next_parameter_index = usize::from(request.new_value.is_some());
        let mut next_placeholder = || {
            let placeholder = self.format_bind_placeholder(next_parameter_index);
            next_parameter_index += 1;
            placeholder
        };

        let (where_clause, mut params) = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                // This is database-specific and may not work for all databases
                return Err(crate::ZqlzError::NotSupported(
                    "Row index-based updates not supported by this driver. Use primary key or full row identifier.".to_string()
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => {
                let conditions: Vec<String> = pk_values
                    .iter()
                    .map(|(col, _)| {
                        format!("{} = {}", self.quote_identifier(col), next_placeholder())
                    })
                    .collect();
                let params: Vec<Value> = pk_values.iter().map(|(_, v)| v.clone()).collect();
                (conditions.join(" AND "), params)
            }
            RowIdentifier::FullRow(row_values) => {
                let conditions: Vec<String> = row_values
                    .iter()
                    .map(|(col, val)| {
                        if val == &Value::Null {
                            format!("{} IS NULL", self.quote_identifier(col))
                        } else {
                            format!("{} = {}", self.quote_identifier(col), next_placeholder())
                        }
                    })
                    .collect();
                let params: Vec<Value> = row_values
                    .iter()
                    .filter(|(_, val)| val != &Value::Null)
                    .map(|(_, v)| v.clone())
                    .collect();
                (conditions.join(" AND "), params)
            }
        };

        let scope = match &request.row_identifier {
            RowIdentifier::FullRow(_) => {
                self.single_row_dml_scope(&request.table_name, &where_clause)
            }
            _ => SingleRowDmlScope::Unsupported,
        };
        let (where_clause, statement_suffix) = scope.apply(&where_clause);

        // Build UPDATE statement
        let sql = if let Some(new_val) = &request.new_value {
            params.insert(0, new_val.clone());
            format!(
                "UPDATE {} SET {} = {} WHERE {}{}",
                request.table_name,
                self.quote_identifier(&request.column_name),
                self.format_bind_placeholder(0),
                where_clause,
                statement_suffix
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {}{}",
                request.table_name,
                self.quote_identifier(&request.column_name),
                where_clause,
                statement_suffix
            )
        };

        let result = self.execute(&sql, &params).await?;
        tracing::debug!(
            affected_rows = result.affected_rows,
            "cell update completed"
        );
        Ok(result.affected_rows)
    }

    /// Begin a transaction
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;

    /// Close the connection
    async fn close(&self) -> Result<()>;

    /// Check if the connection is closed
    fn is_closed(&self) -> bool;

    /// Get schema introspection interface if supported
    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        None
    }

    /// Get key-value store interface if supported.
    fn as_key_value_store(&self) -> Option<&dyn KeyValueStore> {
        None
    }

    /// Get document store interface if supported.
    fn as_document_store(&self) -> Option<&dyn DocumentStore> {
        None
    }

    /// Get a handle that can be used to cancel running queries.
    ///
    /// Returns `None` if the driver does not support query cancellation.
    /// The returned handle is safe to use from any thread.
    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        None
    }
}

/// A database transaction
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// Execute a query within the transaction
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute a statement within the transaction
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;
}

/// A prepared statement
#[async_trait]
pub trait PreparedStatement: Send + Sync {
    /// Execute the prepared statement with parameters
    async fn execute(&self, params: &[Value]) -> Result<StatementResult>;

    /// Query the prepared statement with parameters
    async fn query(&self, params: &[Value]) -> Result<QueryResult>;

    /// Close/deallocate the prepared statement
    async fn close(self: Box<Self>) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::SingleRowDmlScope;

    #[test]
    fn row_identity_scope_narrows_a_keyless_match_to_one_row() {
        let scope = SingleRowDmlScope::by_row_identity("rowid", "\"workflows\"", "\"a\" = ?");

        assert_eq!(
            scope.apply("\"a\" = ?"),
            (
                "rowid IN (SELECT rowid FROM \"workflows\" WHERE \"a\" = ? LIMIT 1)",
                ""
            )
        );
    }

    #[test]
    fn statement_suffix_scope_keeps_the_original_where_clause() {
        let scope = SingleRowDmlScope::StatementSuffix(" LIMIT 1");

        assert_eq!(scope.apply("`a` = ?"), ("`a` = ?", " LIMIT 1"));
    }

    #[test]
    fn unsupported_scope_changes_nothing() {
        assert_eq!(SingleRowDmlScope::Unsupported.apply("a = ?"), ("a = ?", ""));
    }
}
