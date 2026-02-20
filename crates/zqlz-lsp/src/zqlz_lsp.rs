//! SQL Language Server Protocol integration
//!
//! Provides IntelliSense, completions, linting, and error diagnostics for SQL queries.
//! Uses sqlparser-rs for accurate SQL parsing and validation.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use lsp_types::{
    CodeAction, CompletionItem, CompletionItemKind, Diagnostic, GotoDefinitionResponse, Hover,
    HoverContents, MarkedString, Position, Range, ParameterInformation, ParameterLabel,
    SignatureHelp, SignatureInformation, Location, Uri, TextEdit, WorkspaceEdit,
};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_services::{DatabaseSchema, SchemaService};
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::RopeExt;

mod command_tokenizer;
mod completion_cache;
mod completions;
mod context_analyzer;
mod diagnostics;
mod dialect;
mod fuzzy_matcher;
mod hover;
mod keywords;
mod parser_pool;
mod redis_validator;
mod schema_validator;
mod snippets;
mod sql_dialect;

#[cfg(test)]
mod tests;

pub use completion_cache::{CacheStats, CompletionCache};
pub use context_analyzer::{ContextAnalyzer, SqlContext as AstSqlContext, TableRef};
pub use dialect::SqlDialect;
pub use sql_dialect::{SqlDialectConfig, get_sql_dialect_config, is_sql_driver};

pub use completions::SqlCompletionProvider;
pub use diagnostics::SqlDiagnostics;
pub use fuzzy_matcher::{FuzzyMatch, FuzzyMatcher, MatchQuality};
pub use hover::SqlHoverProvider;
pub use schema_validator::{SchemaValidator, ValidationIssue, ValidationSeverity};

/// Database object types that can be stored and shown in completions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseObject {
    Table(TableInfo),
    View(ViewInfo),
    Column(ColumnInfo),
    StoredProcedure(ProcedureInfo),
    Function(FunctionInfo),
    Trigger(TriggerInfo),
    Index(IndexInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub comment: Option<String>,
    pub row_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub schema: Option<String>,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub table_name: String,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureInfo {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<ParameterInfo>,
    pub return_type: Option<String>,
    pub definition: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<ParameterInfo>,
    pub return_type: String,
    pub definition: Option<String>,
    pub is_aggregate: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterInfo {
    pub name: String,
    pub data_type: String,
    pub direction: ParameterDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterDirection {
    In,
    Out,
    InOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub name: String,
    pub table_name: String,
    pub event: String,  // INSERT, UPDATE, DELETE
    pub timing: String, // BEFORE, AFTER, INSTEAD OF
    pub definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

/// SQL Language Server configuration
#[allow(dead_code)]
pub struct SqlLsp {
    /// Current database connection
    connection_id: Option<Uuid>,

    /// Connection to get schema information
    connection: Option<Arc<dyn Connection>>,

    /// Database driver type (sqlite, mysql, postgres, etc.)
    pub(crate) driver_type: String,

    /// SQL dialect for this connection
    pub(crate) dialect: SqlDialect,

    /// Cached schema information
    pub(crate) schema_cache: SchemaCache,

    /// Schema service for cached schema introspection
    schema_service: Arc<SchemaService>,

    /// AST-based context analyzer
    context_analyzer: Option<ContextAnalyzer>,

    /// Schema validator for semantic validation
    schema_validator: SchemaValidator,

    /// SQL diagnostics with precise error positioning
    sql_diagnostics: SqlDiagnostics,

    /// Fuzzy matcher for flexible completions
    fuzzy_matcher: FuzzyMatcher,

    /// Completion cache for performance
    completion_cache: CompletionCache,

    /// True while a background schema fetch is in flight.
    /// Used to suppress the keyword fallback in table-name completion contexts so
    /// the user sees an empty list (indicating "loading") rather than irrelevant keywords.
    pub schema_loading: bool,

    /// Monotonically-increasing counter incremented before each background fetch.
    /// The fetching task captures its epoch; if the counter has advanced by the time
    /// the result arrives, the result is discarded so a newer fetch can apply instead.
    fetch_epoch: u64,
}

#[derive(Default, Serialize, Deserialize)]
pub struct SchemaCache {
    /// All database objects
    pub objects: Vec<DatabaseObject>,

    /// Quick lookup maps
    pub tables: HashMap<String, TableInfo>,
    pub views: HashMap<String, ViewInfo>,
    pub columns_by_table: HashMap<String, Vec<ColumnInfo>>,
    pub procedures: HashMap<String, ProcedureInfo>,
    pub functions: HashMap<String, FunctionInfo>,
    pub triggers: HashMap<String, TriggerInfo>,
    pub indexes: HashMap<String, IndexInfo>,

    /// Foreign key relationships: table_name -> Vec<ForeignKeyInfo>
    pub foreign_keys_by_table: HashMap<String, Vec<zqlz_core::ForeignKeyInfo>>,

    /// Reverse foreign key lookup: referenced_table -> Vec<(source_table, fk_info)>
    pub reverse_foreign_keys: HashMap<String, Vec<(String, zqlz_core::ForeignKeyInfo)>>,

    /// Last time the cache was refreshed
    #[serde(skip)]
    pub last_refresh: Option<std::time::SystemTime>,
}

#[allow(dead_code)]
impl SqlLsp {
    #[allow(dead_code)]
    pub fn new(schema_service: Arc<SchemaService>) -> Self {
        Self {
            connection_id: None,
            connection: None,
            driver_type: "generic".to_string(),
            dialect: SqlDialect::Generic,
            schema_cache: SchemaCache::default(),
            schema_service,
            context_analyzer: ContextAnalyzer::new().ok(),
            schema_validator: SchemaValidator::new(),
            sql_diagnostics: SqlDiagnostics::new(),
            fuzzy_matcher: FuzzyMatcher::new(false),
            completion_cache: CompletionCache::default(),
            schema_loading: false,
            fetch_epoch: 0,
        }
    }

    pub fn with_connection(
        connection_id: Uuid,
        connection: Arc<dyn Connection>,
        driver_type: String,
        schema_service: Arc<SchemaService>,
    ) -> Self {
        let dialect = SqlDialect::from_driver(&driver_type);
        Self {
            connection_id: Some(connection_id),
            connection: Some(connection),
            driver_type,
            dialect,
            schema_cache: SchemaCache::default(),
            schema_service,
            context_analyzer: ContextAnalyzer::new().ok(),
            schema_validator: SchemaValidator::new(),
            sql_diagnostics: SqlDiagnostics::new(),
            fuzzy_matcher: FuzzyMatcher::new(false),
            completion_cache: CompletionCache::default(),
            schema_loading: false,
            fetch_epoch: 0,
        }
    }

    /// Update the connection for this LSP instance
    pub fn set_connection(
        &mut self,
        connection_id: Option<Uuid>,
        connection: Option<Arc<dyn Connection>>,
        driver_type: Option<String>,
    ) {
        self.connection_id = connection_id;
        self.connection = connection;

        // Update driver type and dialect if provided
        if let Some(driver) = driver_type {
            tracing::info!("Updating SQL LSP dialect to: {}", driver);
            self.driver_type = driver.clone();
            self.dialect = SqlDialect::from_driver(&driver);
            tracing::info!("SQL LSP now using dialect: {:?}", self.dialect);
        }

        self.schema_cache = SchemaCache::default();
        self.schema_loading = true;
    }

    /// Get the appropriate SQL dialect for parsing
    #[allow(dead_code)]
    fn get_dialect(&self) -> Box<dyn Dialect> {
        match self.driver_type.to_lowercase().as_str() {
            "sqlite" => Box::new(SQLiteDialect {}),
            "mysql" | "mariadb" => Box::new(MySqlDialect {}),
            "postgres" | "postgresql" => Box::new(PostgreSqlDialect {}),
            _ => Box::new(GenericDialect {}),
        }
    }

    /// Get the dialect name for display
    fn get_dialect_name(&self) -> &str {
        match self.dialect {
            SqlDialect::SQLite => "SQLite",
            SqlDialect::MySQL => "MySQL",
            SqlDialect::PostgreSQL => "PostgreSQL",
            SqlDialect::SQLServer => "SQL Server",
            SqlDialect::Redis => "Redis",
            SqlDialect::Generic => "SQL",
        }
    }

    /// Fetches schema data from the database as a pure I/O operation, returning
    /// a populated `SchemaCache` without touching `self`.
    ///
    /// This is intentionally an associated function so callers can run it from a
    /// background task without holding the `RwLock<SqlLsp>` write guard across
    /// any await point. Once the future resolves, pass the result to
    /// [`Self::apply_schema_cache`] while holding the write guard only briefly.
    pub async fn fetch_schema_cache(
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        schema_service: &SchemaService,
    ) -> Result<SchemaCache> {
        let db_schema = schema_service
            .load_database_schema(connection.clone(), connection_id)
            .await?;

        tracing::info!(
            "Schema loaded via SchemaService: {} tables, {} views",
            db_schema.tables.len(),
            db_schema.views.len()
        );

        let mut cache = SchemaCache::default();

        for table_name in &db_schema.tables {
            let table_info = TableInfo {
                name: table_name.clone(),
                schema: None,
                comment: None,
                row_count: None,
            };
            cache.tables.insert(table_name.clone(), table_info.clone());
            cache.objects.push(DatabaseObject::Table(table_info));
        }

        for view_name in &db_schema.views {
            let view_info = ViewInfo {
                name: view_name.clone(),
                schema: None,
                definition: None,
            };
            cache.views.insert(view_name.clone(), view_info.clone());
            cache.objects.push(DatabaseObject::View(view_info));
        }

        // Fetch column details for all tables concurrently. For remote databases
        // each call is a network round-trip, so serial fetching multiplies latency by
        // the number of tables. Firing them in parallel reduces total time to roughly
        // one round-trip regardless of schema size.
        let table_detail_futures: Vec<_> = db_schema
            .tables
            .iter()
            .map(|table_name| {
                let connection = connection.clone();
                let table_name = table_name.clone();
                async move {
                    let result = schema_service
                        .get_table_details(connection, connection_id, &table_name, None)
                        .await;
                    (table_name, result)
                }
            })
            .collect();

        let table_detail_results = futures::future::join_all(table_detail_futures).await;

        for (table_name, result) in table_detail_results {
            match result {
                Ok(details) => {
                    let fk_columns: std::collections::HashSet<String> = details
                        .foreign_keys
                        .iter()
                        .flat_map(|fk| fk.columns.iter().cloned())
                        .collect();

                    let column_infos: Vec<ColumnInfo> = details
                        .columns
                        .iter()
                        .map(|c| ColumnInfo {
                            table_name: table_name.clone(),
                            name: c.name.clone(),
                            data_type: c.data_type.clone(),
                            nullable: c.nullable,
                            default_value: c.default_value.clone(),
                            is_primary_key: c.is_primary_key,
                            is_foreign_key: fk_columns.contains(&c.name),
                            comment: None,
                        })
                        .collect();

                    for col in &column_infos {
                        cache.objects.push(DatabaseObject::Column(col.clone()));
                    }
                    cache.columns_by_table.insert(table_name.clone(), column_infos);

                    for fk in &details.foreign_keys {
                        cache
                            .foreign_keys_by_table
                            .entry(table_name.clone())
                            .or_default()
                            .push(fk.clone());

                        cache
                            .reverse_foreign_keys
                            .entry(fk.referenced_table.clone())
                            .or_default()
                            .push((table_name.clone(), fk.clone()));
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to load details for table {}: {}", table_name, e);
                }
            }
        }

        for trigger_name in &db_schema.triggers {
            let trigger_info = TriggerInfo {
                name: trigger_name.clone(),
                table_name: String::new(),
                event: String::new(),
                timing: String::new(),
                definition: None,
            };
            cache.triggers.insert(trigger_name.clone(), trigger_info.clone());
            cache.objects.push(DatabaseObject::Trigger(trigger_info));
        }

        for function_name in &db_schema.functions {
            let function_info = FunctionInfo {
                name: function_name.clone(),
                schema: None,
                return_type: String::new(),
                parameters: Vec::new(),
                definition: None,
                is_aggregate: false,
                comment: None,
            };
            cache.functions.insert(function_name.clone(), function_info.clone());
            cache.objects.push(DatabaseObject::Function(function_info));
        }

        for procedure_name in &db_schema.procedures {
            let procedure_info = ProcedureInfo {
                name: procedure_name.clone(),
                schema: None,
                parameters: Vec::new(),
                return_type: None,
                definition: None,
                comment: None,
            };
            cache.procedures.insert(procedure_name.clone(), procedure_info.clone());
            cache.objects.push(DatabaseObject::StoredProcedure(procedure_info));
        }

        for (table_name, indexes) in &db_schema.table_indexes {
            for index in indexes {
                let index_info = IndexInfo {
                    name: index.name.clone(),
                    table_name: table_name.clone(),
                    columns: index.columns.clone(),
                    is_unique: index.is_unique,
                };
                cache.indexes.insert(index.name.clone(), index_info.clone());
                cache.objects.push(DatabaseObject::Index(index_info));
            }
        }

        cache.last_refresh = Some(std::time::SystemTime::now());

        let total_columns: usize = cache.columns_by_table.values().map(|v| v.len()).sum();
        tracing::info!(
            "Schema cache built: {} tables, {} views, {} columns, {} indexes, {} triggers, {} functions, {} procedures",
            cache.tables.len(),
            cache.views.len(),
            total_columns,
            cache.indexes.len(),
            cache.triggers.len(),
            cache.functions.len(),
            cache.procedures.len()
        );

        Ok(cache)
    }

    /// Applies a pre-fetched schema cache, replacing the current one.
    ///
    /// Intended to be called while holding the write lock only briefly, after
    /// [`Self::fetch_schema_cache`] has finished all network I/O.
    pub fn apply_schema_cache(&mut self, cache: SchemaCache) {
        self.schema_cache = cache;
        self.schema_loading = false;
    }

    /// Applies the cache only if `epoch` matches the current [`Self::fetch_epoch`].
    ///
    /// Use this variant from background fetch tasks to avoid overwriting a newer
    /// result with an older one when concurrent fetches race (e.g. connect + DDL).
    pub fn apply_schema_cache_if_current(&mut self, cache: SchemaCache, epoch: u64) {
        if self.fetch_epoch == epoch {
            self.apply_schema_cache(cache);
        }
    }

    /// Increments the fetch epoch and returns the new value.
    ///
    /// Call this immediately before spawning a background schema fetch so the task
    /// can pass the epoch back to [`Self::apply_schema_cache_if_current`].
    pub fn next_fetch_epoch(&mut self) -> u64 {
        self.fetch_epoch += 1;
        self.fetch_epoch
    }

    /// Seeds the schema cache with bare table names so that FROM-clause completions
    /// work immediately after the sidebar shows tables, without waiting for the
    /// slower per-table column-detail fetches to complete.
    ///
    /// Uses `entry().or_insert_with()` so richer entries written by a concurrently
    /// completing [`Self::fetch_schema_cache`] call are never overwritten.
    /// Does NOT clear `schema_loading` — that remains the full fetch's responsibility.
    pub fn pre_populate_tables(&mut self, table_names: &[String]) {
        for name in table_names {
            self.schema_cache.tables.entry(name.clone()).or_insert_with(|| TableInfo {
                name: name.clone(),
                schema: None,
                comment: None,
                row_count: None,
            });
        }
    }

    /// Returns the active connection ID, if any.
    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    /// Returns a clone of the schema service handle.
    pub fn schema_service(&self) -> Arc<SchemaService> {
        self.schema_service.clone()
    }

    /// Returns a clone of the active connection, if any.
    pub fn connection(&self) -> Option<Arc<dyn Connection>> {
        self.connection.clone()
    }

    /// Refresh schema cache from the database using SchemaService.
    ///
    /// Delegates to [`Self::fetch_schema_cache`] + [`Self::apply_schema_cache`].
    /// Note: callers that hold a sync write lock on the containing `RwLock` across
    /// this await will block any concurrent readers for the duration. Prefer the
    /// background-spawn pattern in [`super`] for foreground-thread callers.
    pub async fn refresh_schema(&mut self) -> Result<()> {
        tracing::info!("refresh_schema called - using SchemaService");

        let Some(conn) = self.connection.clone() else {
            tracing::warn!("refresh_schema: No connection available");
            return Ok(());
        };

        let Some(conn_id) = self.connection_id else {
            tracing::warn!("refresh_schema: No connection ID available");
            return Ok(());
        };

        let cache = Self::fetch_schema_cache(conn, conn_id, &self.schema_service).await?;
        self.apply_schema_cache(cache);
        Ok(())
    }

    /// Fetch stored procedures (driver-specific)
    async fn fetch_procedures(&self, conn: &dyn Connection) -> Result<Vec<ProcedureInfo>> {
        let mut procedures = Vec::new();

        match self.driver_type.to_lowercase().as_str() {
            "mysql" | "mariadb" => {
                // MySQL: SELECT name, type FROM mysql.proc WHERE db = DATABASE() AND type = 'PROCEDURE'
                let result = conn
                    .query(
                        "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT 
                     FROM INFORMATION_SCHEMA.ROUTINES 
                     WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = DATABASE()",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let comment = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let Some(name) = name {
                        procedures.push(ProcedureInfo {
                            name,
                            schema: None,
                            parameters: vec![], // TODO: Parse parameters
                            return_type: None,
                            definition,
                            comment,
                        });
                    }
                }
            }
            "postgres" | "postgresql" => {
                // PostgreSQL: Query pg_proc for procedures
                let result = conn
                    .query(
                        "SELECT proname, prosrc, obj_description(oid, 'pg_proc') 
                     FROM pg_proc 
                     WHERE prokind = 'p'",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let comment = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let Some(name) = name {
                        procedures.push(ProcedureInfo {
                            name,
                            schema: None,
                            parameters: vec![],
                            return_type: None,
                            definition,
                            comment,
                        });
                    }
                }
            }
            _ => {
                // SQLite and others don't have stored procedures
            }
        }

        Ok(procedures)
    }

    /// Fetch functions (driver-specific)
    async fn fetch_functions(&self, conn: &dyn Connection) -> Result<Vec<FunctionInfo>> {
        let mut functions = Vec::new();

        match self.driver_type.to_lowercase().as_str() {
            "mysql" | "mariadb" => {
                let result = conn
                    .query(
                        "SELECT ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION, ROUTINE_COMMENT 
                     FROM INFORMATION_SCHEMA.ROUTINES 
                     WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = DATABASE()",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let return_type = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let comment = row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let Some(name) = name {
                        functions.push(FunctionInfo {
                            name,
                            schema: None,
                            parameters: vec![],
                            return_type: return_type.unwrap_or_default(),
                            definition,
                            is_aggregate: false,
                            comment,
                        });
                    }
                }
            }
            "postgres" | "postgresql" => {
                let result = conn
                    .query(
                        "SELECT proname, pg_get_function_result(oid), prosrc, 
                            proisagg, obj_description(oid, 'pg_proc') 
                     FROM pg_proc 
                     WHERE prokind = 'f'",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let return_type = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let is_agg = row.get(3).and_then(|v| v.as_bool());
                    let comment = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let Some(name) = name {
                        functions.push(FunctionInfo {
                            name,
                            schema: None,
                            parameters: vec![],
                            return_type: return_type.unwrap_or_default(),
                            definition,
                            is_aggregate: is_agg.unwrap_or(false),
                            comment,
                        });
                    }
                }
            }
            "sqlite" => {
                // SQLite has built-in functions but no user-defined functions in metadata
                // We could list built-in functions here
            }
            _ => {}
        }

        Ok(functions)
    }

    /// Fetch triggers (driver-specific)
    async fn fetch_triggers(&self, conn: &dyn Connection) -> Result<Vec<TriggerInfo>> {
        let mut triggers = Vec::new();

        match self.driver_type.to_lowercase().as_str() {
            "sqlite" => {
                let result = conn
                    .query(
                        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let table_name = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let (Some(name), Some(table_name)) = (name, table_name) {
                        triggers.push(TriggerInfo {
                            name,
                            table_name,
                            event: "UNKNOWN".to_string(),
                            timing: "UNKNOWN".to_string(),
                            definition,
                        });
                    }
                }
            }
            "mysql" | "mariadb" => {
                let result = conn.query(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING, ACTION_STATEMENT 
                     FROM INFORMATION_SCHEMA.TRIGGERS 
                     WHERE TRIGGER_SCHEMA = DATABASE()",
                    &[]
                ).await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let table_name = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let event = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let timing = row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let (Some(name), Some(table_name), Some(event), Some(timing)) =
                        (name, table_name, event, timing)
                    {
                        triggers.push(TriggerInfo {
                            name,
                            table_name,
                            event,
                            timing,
                            definition,
                        });
                    }
                }
            }
            "postgres" | "postgresql" => {
                let result = conn
                    .query(
                        "SELECT t.tgname, c.relname, pg_get_triggerdef(t.oid)
                     FROM pg_trigger t
                     JOIN pg_class c ON t.tgrelid = c.oid
                     WHERE NOT t.tgisinternal",
                        &[],
                    )
                    .await?;

                for row in result.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let table_name = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                    if let (Some(name), Some(table_name)) = (name, table_name) {
                        triggers.push(TriggerInfo {
                            name,
                            table_name,
                            event: "UNKNOWN".to_string(),
                            timing: "UNKNOWN".to_string(),
                            definition,
                        });
                    }
                }
            }
            _ => {}
        }

        Ok(triggers)
    }

    /// Fetch indexes
    async fn fetch_indexes(&self, conn: &dyn Connection) -> Result<Vec<IndexInfo>> {
        let mut indexes = Vec::new();

        match self.driver_type.to_lowercase().as_str() {
            "sqlite" => {
                // Get list of tables first
                let tables_result = conn
                    .query("SELECT name FROM sqlite_master WHERE type = 'table'", &[])
                    .await?;

                for table_row in tables_result.rows {
                    if let Some(table_name) = table_row
                        .get(0)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                    {
                        let result = conn
                            .query(&format!("PRAGMA index_list('{}')", table_name), &[])
                            .await?;

                        for row in result.rows {
                            let name = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                            let is_unique = row.get(2).and_then(|v| v.as_i64());

                            if let Some(name) = name {
                                indexes.push(IndexInfo {
                                    name,
                                    table_name: table_name.clone(),
                                    columns: vec![], // Would need another PRAGMA to get columns
                                    is_unique: is_unique == Some(1),
                                });
                            }
                        }
                    }
                }
            }
            _ => {
                // Other databases would have similar queries
            }
        }

        Ok(indexes)
    }

    /// Get completion items for the current context (auto-trigger mode)
    pub fn get_completions(&mut self, text: &Rope, offset: usize) -> Vec<CompletionItem> {
        self.get_completions_with_trigger(text, offset, false)
    }

    /// Get completion items with explicit trigger support
    ///
    /// # Arguments
    /// * `text` - The SQL text as a Rope
    /// * `offset` - Current cursor position (byte offset)
    /// * `is_manual_trigger` - True when triggered by Ctrl+Space, false for auto-trigger
    pub fn get_completions_with_trigger(
        &mut self,
        text: &Rope,
        offset: usize,
        is_manual_trigger: bool,
    ) -> Vec<CompletionItem> {
        tracing::debug!(
            "SqlLsp::get_completions at offset {} (manual={})",
            offset,
            is_manual_trigger
        );

        let mut completions = Vec::new();

        // Get the current word being typed
        let current_word = self.get_word_at_offset(text, offset).unwrap_or_default();
        let current_word_lower = current_word.to_lowercase();

        tracing::debug!(
            "Current word: '{}', lowercase: '{}'",
            current_word,
            current_word_lower
        );

        let sql = text.to_string();
        let lines_before_cursor = sql[..offset.min(sql.len())].to_string();

        tracing::debug!(
            "Text before cursor (last 50 chars): '{}'",
            lines_before_cursor
                .chars()
                .rev()
                .take(50)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>()
        );

        let is_after_dot = lines_before_cursor.ends_with('.')
            || (current_word.is_empty() && lines_before_cursor.trim_end().ends_with('.'));

        let is_after_trigger_char = lines_before_cursor.ends_with(' ')
            || lines_before_cursor.ends_with('(')
            || lines_before_cursor.ends_with(',')
            || is_after_dot;

        // For manual trigger (Ctrl+Space), always show completions
        // For auto-trigger, only show when:
        // - User has typed at least 1 character, OR
        // - Cursor is after a trigger character (space, dot, parenthesis, comma), OR
        // - At the very start of the editor
        if !is_manual_trigger {
            // Auto-trigger logic - be more proactive
            let should_show = if current_word.is_empty() {
                // Show if after trigger character or at start
                is_after_trigger_char || lines_before_cursor.is_empty()
            } else {
                // Show if user has typed anything
                true
            };

            if !should_show {
                tracing::debug!("Auto-trigger: skipping (no trigger condition met)");
                return completions;
            }
        }

        // Try AST-based context analysis first (more accurate)
        let mut context = if let Some(ref analyzer) = self.context_analyzer {
            #[cfg(test)]
            println!("DEBUG: Using AST-based context analyzer");

            tracing::debug!("Using AST-based context analyzer");
            let ctx = analyzer.analyze(text, offset);
            tracing::debug!(context = ?ctx, "AST analysis complete");

            #[cfg(test)]
            println!("DEBUG: AST returned context: {:?}", ctx);

            ctx
        } else {
            #[cfg(test)]
            println!("DEBUG: Using fallback pattern-based analyzer");

            tracing::debug!("Using fallback pattern-based analyzer");
            // Fallback to simple pattern-based analysis
            let ctx = self.analyze_sql_context(&lines_before_cursor);
            tracing::debug!(context = ?ctx, "Pattern analysis complete");

            #[cfg(test)]
            println!("DEBUG: Pattern returned context: {:?}", ctx);

            ctx
        };

        // If AST returned General for a query with keywords, try pattern-based fallback
        // This handles incomplete queries like "SELECT lo" or incomplete keywords like "sel"
        if matches!(context, AstSqlContext::General) {
            let lower = lines_before_cursor.to_lowercase();
            let has_sql_keywords = lower.contains("select")
                || lower.contains("from")
                || lower.contains("where")
                || lower.contains("insert")
                || lower.contains("update")
                || lower.contains("delete");

            // Also use fallback if query is very short (might be incomplete keyword)
            let is_short = lines_before_cursor.trim().len() < 10;

            if has_sql_keywords || is_short {
                #[cfg(test)]
                println!(
                    "DEBUG: Using pattern-based fallback (has_keywords={}, is_short={})",
                    has_sql_keywords, is_short
                );

                context = self.analyze_sql_context(&lines_before_cursor);

                #[cfg(test)]
                println!("DEBUG: Pattern fallback returned: {:?}", context);
            }
        }

        tracing::debug!("Detected context: {:?}", context);

        #[cfg(test)]
        println!("DEBUG: Detected context: {:?}", context);

        // Track if we're in AfterDot context to avoid fallback keywords
        let is_after_dot = matches!(context, AstSqlContext::AfterDot { .. });

        // Filter based on context and current input
        match context {
            AstSqlContext::SelectList { ref available_tables } => {
                tracing::debug!("In SELECT list, available tables: {:?}", available_tables);

                if available_tables.is_empty() {
                    // No FROM clause yet — DataGrip-style: show functions and keywords only.
                    // Showing unqualified column names here would be misleading noise because
                    // we don't yet know which table the user intends to select from.
                } else {
                    // Show columns from available tables only (higher priority)
                    self.add_columns_from_tables(
                        &available_tables,
                        &current_word_lower,
                        &mut completions,
                    );
                }

                // Show SQL functions (sort_text "3_" / "4_" ranks them below columns "1_")
                self.add_filtered_functions(&current_word_lower, &mut completions);
                // Show only the most relevant keywords for SELECT expressions
                self.add_specific_keywords(
                    &["DISTINCT", "AS", "FROM", "CASE", "CAST"],
                    &current_word_lower,
                    &mut completions,
                );
            }
            AstSqlContext::FromClause => {
                tracing::debug!("In FROM clause");
                // Show tables, views, and CTEs
                self.add_filtered_tables(&current_word_lower, &mut completions);
                self.add_filtered_views(&current_word_lower, &mut completions);

                // If schema is still loading and no completions are available yet,
                // surface a single informational item so the user knows to wait.
                if completions.is_empty() && self.schema_loading {
                    completions.push(lsp_types::CompletionItem {
                        label: "Schema loading…".to_string(),
                        kind: Some(lsp_types::CompletionItemKind::TEXT),
                        detail: Some("Fetching tables from the database".to_string()),
                        insert_text: Some(String::new()),
                        preselect: Some(false),
                        ..Default::default()
                    });
                    return completions;
                }

                // Add CTE names as available "tables"
                if let Some(ref analyzer) = self.context_analyzer {
                    let cte_names = analyzer.extract_cte_names(text, offset);
                    tracing::debug!("Found {} CTEs: {:?}", cte_names.len(), cte_names);
                    for cte_name in cte_names {
                        if current_word_lower.is_empty()
                            || cte_name.to_lowercase().starts_with(&current_word_lower)
                        {
                            completions.push(CompletionItem {
                                label: cte_name.clone(),
                                kind: Some(CompletionItemKind::CLASS),
                                detail: Some("Common Table Expression (CTE)".to_string()),
                                insert_text: Some(format!("{} ", cte_name)),
                                sort_text: Some(format!("0_cte_{}", cte_name)), // High priority
                                ..Default::default()
                            });
                        }
                    }
                }

                // Add dialect-specific keywords that can appear in FROM clause
                // This allows keywords like LATERAL (PostgreSQL) to be suggested
                if !current_word_lower.is_empty() {
                    let dialect_keywords: Vec<String> = self
                        .dialect
                        .keywords()
                        .iter()
                        .map(|k| k.to_string())
                        .collect();
                    for keyword in &dialect_keywords {
                        let keyword_lower = keyword.to_lowercase();
                        if keyword_lower.starts_with(&current_word_lower) {
                            // Skip if already in completions
                            if completions
                                .iter()
                                .any(|c| c.label.to_uppercase() == *keyword)
                            {
                                continue;
                            }

                            completions.push(CompletionItem {
                                label: keyword.clone(),
                                kind: Some(CompletionItemKind::KEYWORD),
                                detail: Some(format!("SQL Keyword ({})", self.get_dialect_name())),
                                insert_text: Some(format!("{} ", keyword)),
                                sort_text: Some(format!("5_{}", keyword)), // Lower priority than tables
                                ..Default::default()
                            });
                        }
                    }
                }
            }
            AstSqlContext::JoinClause { ref existing_tables } => {
                tracing::debug!("In JOIN clause, existing tables: {:?}", existing_tables);
                // Show tables with suggested JOINs based on foreign keys
                self.add_tables_with_fk_suggestions(
                    &existing_tables,
                    &current_word_lower,
                    &mut completions,
                );
                self.add_filtered_views(&current_word_lower, &mut completions);

                // Add CTE names as joinable "tables"
                if let Some(ref analyzer) = self.context_analyzer {
                    let cte_names = analyzer.extract_cte_names(text, offset);
                    for cte_name in cte_names {
                        if current_word_lower.is_empty()
                            || cte_name.to_lowercase().starts_with(&current_word_lower)
                        {
                            completions.push(CompletionItem {
                                label: cte_name.clone(),
                                kind: Some(CompletionItemKind::CLASS),
                                detail: Some("Common Table Expression (CTE)".to_string()),
                                insert_text: Some(format!("{} ", cte_name)),
                                sort_text: Some(format!("0_cte_{}", cte_name)),
                                ..Default::default()
                            });
                        }
                    }
                }

                // Add only JOIN-related keywords
                self.add_specific_keywords(&["ON", "USING"], &current_word_lower, &mut completions);
            }
            AstSqlContext::ConditionClause { ref available_tables } => {
                tracing::debug!(
                    "In WHERE/HAVING clause, available tables count: {}",
                    available_tables.len()
                );

                // Priority 1: Show columns from available tables (highest priority)
                if available_tables.is_empty() {
                    // Fallback: If no tables detected by AST, show ALL columns
                    // This handles cases where AST analysis fails to extract table references
                    self.add_filtered_columns(&current_word_lower, &mut completions);
                } else {
                    self.add_columns_from_tables(
                        &available_tables,
                        &current_word_lower,
                        &mut completions,
                    );
                }

                // Priority 2: Add scalar functions ONLY (no aggregates like COUNT, SUM, AVG)
                // Aggregate functions cannot be used in WHERE clause
                self.add_scalar_functions(&current_word_lower, &mut completions);

                // Priority 3: Add condition/boolean operators and keywords
                self.add_specific_keywords(
                    &[
                        "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL", "EXISTS",
                        "CASE", "WHEN", "THEN", "ELSE", "END",
                    ],
                    &current_word_lower,
                    &mut completions,
                );

                // Priority 4: Add comparison operators as snippets (low priority)
                if current_word_lower.is_empty() || "=".starts_with(&current_word_lower) {
                    completions.push(CompletionItem {
                        label: "= (equals)".to_string(),
                        kind: Some(CompletionItemKind::OPERATOR),
                        detail: Some("Equality comparison".to_string()),
                        insert_text: Some("= ".to_string()),
                        sort_text: Some("9_operator_eq".to_string()),
                        ..Default::default()
                    });
                }
            }
            AstSqlContext::AfterDot { ref table_or_alias, ref available_tables } => {
                tracing::debug!("After dot for table/alias: {}", table_or_alias);

                // Resolve alias to actual table name using available_tables from context
                let table_name = self.resolve_alias_from_context(&table_or_alias, &available_tables);

                tracing::debug!("Resolved '{}' to table '{}'", table_or_alias, table_name);

                // Show ONLY columns for the specific table - NO keywords at all
                // Do case-insensitive lookup for table name
                let columns = self.schema_cache.columns_by_table.get(&table_name)
                    .or_else(|| {
                        // Try case-insensitive lookup
                        let table_name_lower = table_name.to_lowercase();
                        self.schema_cache.columns_by_table
                            .iter()
                            .find(|(k, _)| k.to_lowercase() == table_name_lower)
                            .map(|(_, v)| v)
                    });
                
                if let Some(columns) = columns {
                    tracing::debug!("Found {} columns for table '{}'", columns.len(), table_name);
                    for column in columns {
                        if current_word.is_empty()
                            || column.name.to_lowercase().starts_with(&current_word_lower)
                        {
                            completions.push(CompletionItem {
                                label: column.name.clone(),
                                kind: Some(CompletionItemKind::FIELD),
                                detail: Some(format!(
                                    "{}.{}: {} ({})",
                                    table_name,
                                    column.name,
                                    column.data_type,
                                    if column.nullable { "NULL" } else { "NOT NULL" }
                                )),
                                insert_text: Some(column.name.clone()), // Column name without trailing space
                                sort_text: Some(format!("0_{}", column.name)), // Highest priority
                                documentation: column
                                    .comment
                                    .as_ref()
                                    .map(|c| lsp_types::Documentation::String(c.clone())),
                                ..Default::default()
                            });
                        }
                    }
                } else {
                    tracing::debug!("No columns found for table/alias '{}'", table_name);
                }
            }
            AstSqlContext::CommonTableExpression { .. } => {
                tracing::debug!("In CTE context");
                // Inside CTE - show relevant keywords only
                self.add_specific_keywords(
                    &["SELECT", "FROM", "WHERE", "AS"],
                    &current_word_lower,
                    &mut completions,
                );
            }
            AstSqlContext::Subquery { ref parent_tables } => {
                tracing::debug!("In subquery, parent tables: {:?}", parent_tables);
                // Inside subquery - can reference parent tables
                self.add_columns_from_tables(&parent_tables, &current_word_lower, &mut completions);
            }
            AstSqlContext::General => {
                tracing::debug!("In general context");
                // Show relevant keywords based on position (max 5)
                self.add_filtered_keywords(
                    &current_word_lower,
                    &lines_before_cursor,
                    &mut completions,
                );

                // Add data types (like INTEGER, VARCHAR, etc.)
                self.add_filtered_data_types(&current_word_lower, &mut completions);

                // Add tables if user has typed 2+ characters
                if current_word_lower.len() >= 2 {
                    self.add_filtered_tables(&current_word_lower, &mut completions);
                }
            }
        }

        // Fallback: if no completions found and query is short, show keywords.
        // This handles edge cases like single characters ("s" → SELECT).
        // Do NOT fall back when context is explicitly identified as a table-name position
        // (FromClause / JoinClause): an empty result there means the schema cache is still
        // loading, and showing keywords (e.g. CLUSTER for "cust") is misleading noise.
        let is_table_name_context = matches!(
            context,
            AstSqlContext::FromClause | AstSqlContext::JoinClause { .. }
        );
        if completions.is_empty() && sql.len() < 20 && !is_after_dot && !is_table_name_context {
            #[cfg(test)]
            println!(
                "DEBUG: No completions for short query '{}', adding fallback keywords",
                sql
            );

            tracing::debug!("No completions found for short query, falling back to keywords");
            self.add_filtered_keywords(&current_word_lower, &lines_before_cursor, &mut completions);

            #[cfg(test)]
            println!(
                "DEBUG: After fallback, have {} completions",
                completions.len()
            );
        }

        // Use fuzzy matching to improve results
        if !current_word.is_empty() && current_word.len() >= 2 {
            completions = self.apply_fuzzy_matching(&current_word, completions);
        }

        // Limit results to top 20 for JetBrains-style focused completions
        completions.truncate(20);

        // Add text_edit to each completion for proper replacement
        // This ensures that when a completion is accepted, only the typed word is replaced
        // (not the entire query, which was a bug before)
        let word_range = self.get_word_range_at_offset(text, offset);
        if let Some((start_pos, end_pos)) = word_range {
            for completion in &mut completions {
                // Skip if completion already has text_edit (e.g., from snippets)
                if completion.text_edit.is_none() {
                    let new_text = completion
                        .insert_text
                        .clone()
                        .unwrap_or_else(|| completion.label.clone());

                    completion.text_edit =
                        Some(lsp_types::CompletionTextEdit::Edit(lsp_types::TextEdit {
                            range: lsp_types::Range {
                                start: start_pos,
                                end: end_pos,
                            },
                            new_text,
                        }));
                }
            }
        }

        tracing::debug!("Returning {} completions", completions.len());
        completions
    }

    /// Add columns from a list of table references (handling aliases)
    fn add_columns_from_tables(
        &self,
        tables: &[TableRef],
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        for table_ref in tables {
            tracing::debug!(
                table_name = %table_ref.table_name,
                alias = ?table_ref.alias,
                "add_columns_from_tables: processing table"
            );

            if let Some(columns) = self
                .schema_cache
                .columns_by_table
                .get(&table_ref.table_name)
            {
                for column in columns {
                    // Add column - fuzzy matching will filter if needed
                    let label = if table_ref.alias.is_some() {
                        format!("{}.{}", table_ref.identifier(), column.name)
                    } else {
                        column.name.clone()
                    };

                    completions.push(CompletionItem {
                        label: label.clone(),
                        kind: Some(CompletionItemKind::FIELD),
                        detail: Some(format!(
                            "{}.{}: {}",
                            table_ref.table_name, column.name, column.data_type
                        )),
                        insert_text: Some(label), // Use label as insert_text to handle qualified names
                        sort_text: Some(format!("1_{}_{}", table_ref.table_name, column.name)),
                        documentation: column
                            .comment
                            .as_ref()
                            .map(|c| lsp_types::Documentation::String(c.clone())),
                        ..Default::default()
                    });
                }
            } else {
                tracing::debug!(
                    table_name = %table_ref.table_name,
                    "add_columns_from_tables: no columns found in schema cache"
                );
            }
        }
    }

    /// Add tables with foreign key-based JOIN suggestions
    fn add_tables_with_fk_suggestions(
        &self,
        existing_tables: &[TableRef],
        filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        // First, add all tables normally
        self.add_filtered_tables(filter, completions);

        // Then, boost tables that have foreign key relationships with existing tables
        for existing_table in existing_tables {
            // Check if any table has a foreign key TO this existing table
            if let Some(referencing_tables) = self
                .schema_cache
                .reverse_foreign_keys
                .get(&existing_table.table_name)
            {
                for (source_table, fk_info) in referencing_tables {
                    // Add FK-related table - fuzzy matching will filter
                    completions.push(CompletionItem {
                        label: source_table.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!(
                            "Table (FK: {} → {}.{})",
                            fk_info.columns.join(", "),
                            fk_info.referenced_table,
                            fk_info.referenced_columns.join(", ")
                        )),
                        sort_text: Some(format!("00_{}", source_table)), // Boost priority
                        ..Default::default()
                    });
                }
            }

            // Check if this existing table has foreign keys TO other tables
            if let Some(foreign_keys) = self
                .schema_cache
                .foreign_keys_by_table
                .get(&existing_table.table_name)
            {
                for fk_info in foreign_keys {
                    let ref_table = &fk_info.referenced_table;
                    // Add FK-related table - fuzzy matching will filter
                    completions.push(CompletionItem {
                        label: ref_table.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!(
                            "Table (FK from {}: {} → {})",
                            existing_table.table_name,
                            fk_info.columns.join(", "),
                            fk_info.referenced_columns.join(", ")
                        )),
                        sort_text: Some(format!("00_{}", ref_table)), // Boost priority
                        ..Default::default()
                    });
                }
            }
        }
    }

    /// Add specific keywords to completions
    fn add_specific_keywords(
        &self,
        keywords: &[&str],
        _filter: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        for keyword in keywords {
            // Add keyword - fuzzy matching will filter
            completions.push(CompletionItem {
                label: keyword.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("SQL Keyword".to_string()),
                insert_text: Some(format!("{} ", keyword)), // Add space after keyword
                sort_text: Some(format!("2_{}", keyword)),
                ..Default::default()
            });
        }
    }

    /// Fallback: Analyze SQL context using simple pattern matching (when tree-sitter unavailable)
    fn analyze_sql_context(&self, text_before: &str) -> AstSqlContext {
        let text_lower = text_before.to_lowercase();
        let words: Vec<&str> = text_lower.split_whitespace().collect();

        // Check for table.column pattern (e.g., "users.")
        if text_before.ends_with('.') {
            if let Some(last_word) = text_before.trim_end_matches('.').split_whitespace().last() {
                let table_name = last_word.trim_end_matches('.');
                return AstSqlContext::AfterDot {
                    table_or_alias: table_name.to_string(),
                    available_tables: Vec::new(), // Fallback context - no AST info available
                };
            }
        }

        // The SQL clause keywords we recognise
        let clause_keywords: &[&str] = &[
            "select", "from", "into", "update", "table", "join", "left", "right", "inner",
            "outer", "cross", "full", "where", "and", "or", "not", "on", "having", "group",
            "order", "limit", "offset", "union", "except", "intersect", "set", "values",
            "with", "as",
        ];

        // Scan backward to find the most recent SQL clause keyword, then decide context.
        // For FROM/JOIN we only return that clause if the token immediately after the keyword
        // is still a table-name position (i.e., there are no additional non-keyword tokens
        // between the clause keyword and the cursor). If the table name has already been typed,
        // the user is now writing the next clause keyword, so we return General.
        for i in (0..words.len()).rev() {
            match words[i] {
                kw @ ("from" | "into" | "update" | "table") => {
                    // Count non-keyword tokens after this clause keyword.
                    let tokens_after = words[i + 1..].iter().filter(|&&w| !clause_keywords.contains(&w)).count();
                    let _ = kw;
                    if tokens_after <= 1 {
                        return AstSqlContext::FromClause;
                    }
                    // Table name already typed - user is writing the next part of the query.
                    return AstSqlContext::General;
                }
                "join" => {
                    return AstSqlContext::JoinClause {
                        existing_tables: vec![],
                    };
                }
                "select" | "," => {
                    return AstSqlContext::SelectList {
                        available_tables: vec![],
                    };
                }
                "where" | "and" | "or" | "on" | "having" => {
                    return AstSqlContext::ConditionClause {
                        available_tables: vec![],
                    };
                }
                _ => continue,
            }
        }

        AstSqlContext::General
    }

    fn add_filtered_keywords(
        &self,
        _filter: &str,
        context: &str,
        completions: &mut Vec<CompletionItem>,
    ) {
        // JetBrains-style: Show only 5 most relevant keywords based on context
        // NOTE: Removed minimum character requirement - users should get completions immediately
        // This allows single-character queries like "s" → SELECT, "f" → FROM, etc.

        #[cfg(test)]
        println!(
            "DEBUG add_filtered_keywords: filter='{}', context='{}'",
            _filter, context
        );

        // Context-specific keywords (max 5 per context)
        let mut relevant_keywords = if context.trim().is_empty()
            || context.trim().len() <= 10 && !context.to_lowercase().contains("select")
        {
            // At start of query or very short query without SELECT: show primary statement keywords
            vec![
                "SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "CREATE", "DROP",
            ]
        } else if context.to_lowercase().contains("with")
            && !context.to_lowercase().contains("select")
        {
            // After WITH: show RECURSIVE
            vec!["RECURSIVE", "AS", "SELECT"]
        } else if context.to_lowercase().contains("select")
            && !context.to_lowercase().contains("from")
        {
            // After SELECT, before FROM: show column-related keywords
            vec!["DISTINCT", "AS", "FROM", "CASE", "CAST"]
        } else if context.to_lowercase().contains("from")
            && !context.to_lowercase().contains("where")
        {
            // After FROM: show join and filter keywords
            vec!["JOIN", "LEFT", "INNER", "WHERE", "GROUP", "UNION"]
        } else if context.to_lowercase().contains("where")
            || context.to_lowercase().contains("and")
            || context.to_lowercase().contains("or")
        {
            // In WHERE clause: show condition keywords
            vec!["AND", "OR", "IN", "LIKE", "BETWEEN", "EXISTS", "NOT"]
        } else if context.to_lowercase().contains("group") {
            // After GROUP BY: show aggregation keywords
            vec!["HAVING", "ORDER", "LIMIT"]
        } else if context.to_lowercase().contains("order") {
            // After ORDER BY: show ordering keywords
            vec!["ASC", "DESC", "LIMIT", "OFFSET"]
        } else {
            // Default: most common continuation keywords
            vec!["FROM", "WHERE", "JOIN", "ORDER", "GROUP", "UNION", "CASE"]
        };

        // Add dialect-specific top-level keywords for start-of-query context
        if context.trim().len() <= 10 && !context.to_lowercase().contains("select") {
            match self.dialect {
                SqlDialect::SQLite => {
                    relevant_keywords.push("PRAGMA");
                    relevant_keywords.push("ATTACH");
                    relevant_keywords.push("DETACH");
                    relevant_keywords.push("VACUUM");
                }
                SqlDialect::MySQL => {
                    relevant_keywords.push("SHOW");
                    relevant_keywords.push("USE");
                    relevant_keywords.push("DESCRIBE");
                }
                SqlDialect::PostgreSQL => {
                    relevant_keywords.push("EXPLAIN");
                    relevant_keywords.push("ANALYZE");
                }
                SqlDialect::SQLServer => {
                    relevant_keywords.push("EXEC");
                    relevant_keywords.push("EXECUTE");
                }
                SqlDialect::Redis => {
                    // Redis commands are already in the keywords list from redis_dialect()
                    // Add common command prefixes for better discoverability
                    relevant_keywords.push("GET");
                    relevant_keywords.push("SET");
                    relevant_keywords.push("HGET");
                    relevant_keywords.push("KEYS");
                }
                SqlDialect::Generic => {}
            }
        }

        #[cfg(test)]
        println!("DEBUG: Selected keywords: {:?}", relevant_keywords);

        // Add matching keywords from the relevant set
        // When filter is empty OR when we want fuzzy matching, add ALL keywords
        // and let apply_fuzzy_matching do the filtering and ranking
        let dialect_keywords: Vec<String> = self
            .dialect
            .keywords()
            .iter()
            .map(|k| k.to_string())
            .collect();

        for keyword in &relevant_keywords {
            // Check if the keyword is valid for the current dialect
            if !dialect_keywords.contains(&keyword.to_uppercase())
                && self.dialect != SqlDialect::Generic
            {
                #[cfg(test)]
                println!(
                    "DEBUG: Skipping keyword '{}' - not valid for dialect {:?}",
                    keyword, self.dialect
                );
                continue;
            }

            // Add the keyword - fuzzy matching will filter if needed
            #[cfg(test)]
            println!("DEBUG: Adding keyword: {}", keyword);

            completions.push(CompletionItem {
                label: keyword.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(format!("SQL Keyword ({})", self.get_dialect_name())),
                insert_text: Some(format!("{} ", keyword)), // Add space after keyword
                sort_text: Some(format!("3_{}", keyword)),  // Lower priority than tables/columns
                ..Default::default()
            });
        }

        // Also add any dialect-specific keywords that weren't in relevant_keywords
        // This allows dialect-specific keywords like AUTOINCREMENT, UNSIGNED, etc. to be suggested
        for keyword in &dialect_keywords {
            // Skip if already in completions
            if completions
                .iter()
                .any(|c| c.label.to_uppercase() == *keyword)
            {
                continue;
            }

            #[cfg(test)]
            println!("DEBUG: Adding dialect-specific keyword: {}", keyword);

            completions.push(CompletionItem {
                label: keyword.clone(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(format!("SQL Keyword ({})", self.get_dialect_name())),
                insert_text: Some(format!("{} ", keyword)),
                sort_text: Some(format!("4_{}", keyword)), // Lower priority than context keywords
                ..Default::default()
            });
        }

        // For Generic dialect, also search across ALL dialect-specific keywords for better UX
        // This allows users to discover dialect-specific features even without a specific connection
        if self.dialect == SqlDialect::Generic {
            let all_dialects = [
                SqlDialect::SQLite,
                SqlDialect::MySQL,
                SqlDialect::PostgreSQL,
                SqlDialect::SQLServer,
            ];

            for dialect in &all_dialects {
                for keyword in dialect.keywords() {
                    // Skip if already in completions
                    if completions
                        .iter()
                        .any(|c| c.label.to_uppercase() == *keyword)
                    {
                        continue;
                    }

                    #[cfg(test)]
                    println!(
                        "DEBUG: Adding cross-dialect keyword: {} (from {:?})",
                        keyword, dialect
                    );

                    completions.push(CompletionItem {
                        label: keyword.to_string(),
                        kind: Some(CompletionItemKind::KEYWORD),
                        detail: Some(format!("SQL Keyword ({:?})", dialect)),
                        insert_text: Some(format!("{} ", keyword)),
                        sort_text: Some(format!("5_{}", keyword)), // Even lower priority
                        ..Default::default()
                    });
                }
            }
        }
    }

    fn add_filtered_data_types(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        // Add SQL data types from dialect - let fuzzy matching filter them
        for data_type in self.dialect.data_types() {
            // Skip if already in completions
            if completions
                .iter()
                .any(|c| c.label.to_uppercase() == *data_type)
            {
                continue;
            }

            #[cfg(test)]
            println!("DEBUG: Adding data type: {}", data_type);

            completions.push(CompletionItem {
                label: data_type.to_string(),
                kind: Some(CompletionItemKind::STRUCT),
                detail: Some(format!("Data Type ({})", self.get_dialect_name())),
                insert_text: Some(data_type.to_string()),
                sort_text: Some(format!("4_{}", data_type)), // Same priority as dialect keywords
                ..Default::default()
            });
        }
    }

    fn add_filtered_tables(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        for (_, table) in &self.schema_cache.tables {
            // Add table - fuzzy matching will filter
            completions.push(CompletionItem {
                label: table.name.clone(),
                kind: Some(CompletionItemKind::CLASS),
                detail: Some("Table".to_string()),
                insert_text: Some(format!("{} ", table.name)), // Add space after table name
                sort_text: Some(format!("0_{}", table.name)),
                documentation: table
                    .comment
                    .as_ref()
                    .map(|c| lsp_types::Documentation::String(c.clone())),
                ..Default::default()
            });
        }
    }

    fn add_filtered_views(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        for (_, view) in &self.schema_cache.views {
            // Add view - fuzzy matching will filter
            completions.push(CompletionItem {
                label: view.name.clone(),
                kind: Some(CompletionItemKind::INTERFACE),
                detail: Some("View".to_string()),
                insert_text: Some(format!("{} ", view.name)), // Add space after view name
                sort_text: Some(format!("1_{}", view.name)),
                ..Default::default()
            });
        }
    }

    fn add_filtered_columns(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        #[cfg(test)]
        println!(
            "DEBUG add_filtered_columns: filter='{}', cache has {} tables",
            _filter,
            self.schema_cache.columns_by_table.len()
        );

        for (table, columns) in &self.schema_cache.columns_by_table {
            #[cfg(test)]
            println!(
                "DEBUG: Checking table '{}' with {} columns",
                table,
                columns.len()
            );

            for column in columns {
                // Add column - fuzzy matching will filter if needed
                #[cfg(test)]
                println!(
                    "DEBUG: Adding column '{}' from table '{}'",
                    column.name, table
                );

                completions.push(CompletionItem {
                    label: column.name.clone(),
                    kind: Some(CompletionItemKind::FIELD),
                    detail: Some(format!("{}.{}: {}", table, column.name, column.data_type)),
                    insert_text: Some(column.name.clone()), // Use column name without trailing space
                    sort_text: Some(format!("2_{}_{}", table, column.name)),
                    documentation: column
                        .comment
                        .as_ref()
                        .map(|c| lsp_types::Documentation::String(c.clone())),
                    ..Default::default()
                });
            }
        }
    }

    fn add_filtered_functions(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        // Add user-defined functions from schema cache
        for (_, func) in &self.schema_cache.functions {
            // Add user function - fuzzy matching will filter
            let detail = if func.is_aggregate {
                "Aggregate Function"
            } else {
                "Function"
            };

            completions.push(CompletionItem {
                label: format!("{}()", func.name),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some(format!("{} → {}", detail, func.return_type)),
                sort_text: Some(format!("3_{}", func.name)),
                documentation: func
                    .comment
                    .as_ref()
                    .map(|c| lsp_types::Documentation::String(c.clone())),
                insert_text: Some(format!("{}()", func.name)),
                filter_text: Some(func.name.clone()),
                ..Default::default()
            });
        }

        // Add built-in functions from dialect
        for func_name in self.dialect.functions() {
            // Add built-in function - fuzzy matching will filter
            completions.push(CompletionItem {
                label: format!("{}()", func_name),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some(format!("Built-in Function ({})", self.get_dialect_name())),
                sort_text: Some(format!("4_{}", func_name)), // Lower priority than user functions
                insert_text: Some(format!("{}()", func_name)),
                filter_text: Some(func_name.to_string()),
                ..Default::default()
            });
        }
    }

    /// Add only scalar (non-aggregate) functions to completions
    /// These can be used in WHERE clauses, unlike aggregate functions like COUNT, SUM, etc.
    fn add_scalar_functions(&self, _filter: &str, completions: &mut Vec<CompletionItem>) {
        // Add user-defined SCALAR functions only from schema cache
        for (_, func) in &self.schema_cache.functions {
            // Skip aggregate functions
            if func.is_aggregate {
                continue;
            }

            completions.push(CompletionItem {
                label: format!("{}()", func.name),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some(format!("Scalar Function → {}", func.return_type)),
                sort_text: Some(format!("3_{}", func.name)),
                documentation: func
                    .comment
                    .as_ref()
                    .map(|c| lsp_types::Documentation::String(c.clone())),
                insert_text: Some(format!("{}()", func.name)),
                filter_text: Some(func.name.clone()),
                ..Default::default()
            });
        }

        // Add built-in SCALAR functions from dialect
        for func_name in self.dialect.scalar_functions() {
            // Skip if it's an aggregate function
            if self.dialect.is_aggregate_function(&func_name) {
                continue;
            }

            completions.push(CompletionItem {
                label: format!("{}()", func_name),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some(format!("Scalar Function ({})", self.get_dialect_name())),
                sort_text: Some(format!("4_{}", func_name)), // Lower priority than user functions
                insert_text: Some(format!("{}()", func_name)),
                filter_text: Some(func_name.to_string()),
                ..Default::default()
            });
        }
    }

    /// Get hover information for a position
    pub fn get_hover(&self, text: &Rope, offset: usize) -> Option<Hover> {
        tracing::debug!("get_hover: offset={}", offset);
        let word = match self.get_word_at_offset(text, offset) {
            Some(w) => {
                tracing::debug!("word extracted: '{}'", w);
                w
            }
            None => {
                tracing::debug!("no word at offset");
                return None;
            }
        };

        let word_lower = word.to_lowercase();
        let keyword_upper = word.to_uppercase();
        tracing::debug!("checking keyword: '{}'", keyword_upper);

        // Check built-in SQL keywords and functions FIRST (don't need schema)
        match keyword_upper.as_str() {
            // DML - Data Manipulation Language
            "SELECT" => {
                tracing::debug!("found SELECT keyword");
                return Some(self.create_keyword_hover(
                    "SELECT",
                    "Retrieves data from one or more tables.\n\n**Syntax:**\n`SELECT column1, column2 FROM table_name WHERE condition;`\n\n**Example:**\n`SELECT name, email FROM users WHERE active = 1;`"
                ));
            }
            "FROM" => {
                tracing::info!("✅ Found FROM keyword");
                return Some(self.create_keyword_hover(
                    "FROM",
                    "Specifies the table(s) to retrieve data from.\n\n**Syntax:**\n`SELECT columns FROM table_name;`\n\nCan be used with JOIN clauses to query multiple tables."
                ));
            }
            "WHERE" => {
                tracing::info!("✅ Found WHERE keyword");
                return Some(self.create_keyword_hover(
                    "WHERE",
                    "Filters rows based on specified conditions.\n\n**Syntax:**\n`SELECT * FROM table_name WHERE condition;`\n\n**Example:**\n`SELECT * FROM products WHERE price > 100 AND category = 'electronics';`"
                ));
            }
            _ => {
                tracing::debug!("Not a recognized SQL keyword, checking schema objects");
            }
        }

        // Check if it's a qualified column reference (table.column)
        let sql = text.to_string();
        let before_cursor = &sql[..offset.min(sql.len())];
        if let Some(dot_pos) = before_cursor.rfind('.') {
            let table_part = &before_cursor[..dot_pos];
            if let Some(table_start) = table_part.rfind(|c: char| !c.is_alphanumeric() && c != '_')
            {
                let table_name = &table_part[table_start + 1..];

                // Show column info for qualified reference
                if let Some(columns) = self.schema_cache.columns_by_table.get(table_name) {
                    for col in columns {
                        if col.name.to_lowercase() == word_lower {
                            tracing::trace!(table = table_name, column = %col.name, "Found qualified column");
                            return Some(self.create_column_hover(col, Some(table_name)));
                        }
                    }
                }
            }
        }

        // Check tables
        if let Some(table) = self.schema_cache.tables.get(&word) {
            let mut hover_text = format!("**Table: {}**\n\n", table.name);

            if let Some(schema) = &table.schema {
                hover_text.push_str(&format!("Schema: `{}`\n", schema));
            }

            if let Some(comment) = &table.comment {
                hover_text.push_str(&format!("\n{}\n\n", comment));
            }

            if let Some(row_count) = table.row_count {
                hover_text.push_str(&format!("Rows: ~{}\n\n", row_count));
            }

            if let Some(columns) = self.schema_cache.columns_by_table.get(&word) {
                hover_text.push_str("**Columns:**\n");
                for col in columns {
                    let mut col_line = format!("- `{}`: {}", col.name, col.data_type);

                    if col.is_primary_key {
                        col_line.push_str(" **PK**");
                    }
                    if col.is_foreign_key {
                        col_line.push_str(" **FK**");
                    }
                    if !col.nullable {
                        col_line.push_str(" NOT NULL");
                    }

                    hover_text.push_str(&col_line);
                    hover_text.push('\n');
                }
            }

            // Show foreign key relationships
            if let Some(fks) = self.schema_cache.foreign_keys_by_table.get(&word) {
                if !fks.is_empty() {
                    hover_text.push_str("\n**Foreign Keys:**\n");
                    for fk in fks {
                        hover_text.push_str(&format!(
                            "- `{}` → `{}.{}`\n",
                            fk.columns.join(", "),
                            fk.referenced_table,
                            fk.referenced_columns.join(", ")
                        ));
                    }
                }
            }

            // Show reverse foreign keys (tables that reference this table)
            if let Some(reverse_fks) = self.schema_cache.reverse_foreign_keys.get(&word) {
                if !reverse_fks.is_empty() {
                    hover_text.push_str("\n**Referenced By:**\n");
                    for (source_table, fk) in reverse_fks {
                        hover_text.push_str(&format!(
                            "- `{}.{}` → `{}`\n",
                            source_table,
                            fk.columns.join(", "),
                            fk.referenced_columns.join(", ")
                        ));
                    }
                }
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check columns (unqualified - search all tables)
        for (table_name, columns) in &self.schema_cache.columns_by_table {
            for col in columns {
                if col.name.to_lowercase() == word_lower {
                    return Some(self.create_column_hover(col, Some(table_name)));
                }
            }
        }

        // Check views
        if let Some(view) = self.schema_cache.views.get(&word) {
            let mut hover_text = format!("**View: {}**\n\n", view.name);

            if let Some(schema) = &view.schema {
                hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
            }

            if let Some(definition) = &view.definition {
                hover_text.push_str("**Definition:**\n```sql\n");
                hover_text.push_str(definition);
                hover_text.push_str("\n```\n");
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check procedures
        if let Some(proc) = self.schema_cache.procedures.get(&word) {
            let mut hover_text = format!("**Stored Procedure: {}**\n\n", proc.name);

            if let Some(schema) = &proc.schema {
                hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
            }

            if let Some(comment) = &proc.comment {
                hover_text.push_str(&format!("{}\n\n", comment));
            }

            if !proc.parameters.is_empty() {
                hover_text.push_str("**Parameters:**\n");
                for param in &proc.parameters {
                    let direction = match param.direction {
                        ParameterDirection::In => "IN",
                        ParameterDirection::Out => "OUT",
                        ParameterDirection::InOut => "INOUT",
                    };
                    hover_text.push_str(&format!(
                        "- `{}` ({}): {}\n",
                        param.name, direction, param.data_type
                    ));
                }
                hover_text.push('\n');
            }

            if let Some(return_type) = &proc.return_type {
                hover_text.push_str(&format!("**Returns:** `{}`\n\n", return_type));
            }

            if let Some(definition) = &proc.definition {
                hover_text.push_str("**Definition:**\n```sql\n");
                hover_text.push_str(definition);
                hover_text.push_str("\n```\n");
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check functions
        if let Some(func) = self.schema_cache.functions.get(&word) {
            let mut hover_text = format!(
                "**{}: {}**\n\n",
                if func.is_aggregate {
                    "Aggregate Function"
                } else {
                    "Function"
                },
                func.name
            );

            if let Some(schema) = &func.schema {
                hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
            }

            hover_text.push_str(&format!("**Returns:** `{}`\n\n", func.return_type));

            if let Some(comment) = &func.comment {
                hover_text.push_str(&format!("{}\n\n", comment));
            }

            if !func.parameters.is_empty() {
                hover_text.push_str("**Parameters:**\n");
                for param in &func.parameters {
                    hover_text.push_str(&format!("- `{}`: {}\n", param.name, param.data_type));
                }
                hover_text.push('\n');
            }

            if let Some(definition) = &func.definition {
                hover_text.push_str("**Definition:**\n```sql\n");
                hover_text.push_str(definition);
                hover_text.push_str("\n```\n");
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check indexes
        if let Some(index) = self.schema_cache.indexes.get(&word) {
            let mut hover_text = format!(
                "**{}: {}**\n\n",
                if index.is_unique {
                    "Unique Index"
                } else {
                    "Index"
                },
                index.name
            );

            hover_text.push_str(&format!("Table: `{}`\n", index.table_name));

            if !index.columns.is_empty() {
                hover_text.push_str(&format!("Columns: `{}`\n", index.columns.join(", ")));
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check triggers
        if let Some(trigger) = self.schema_cache.triggers.get(&word) {
            let mut hover_text = format!("**Trigger: {}**\n\n", trigger.name);

            hover_text.push_str(&format!("Table: `{}`\n", trigger.table_name));
            hover_text.push_str(&format!("Timing: {} {}\n\n", trigger.timing, trigger.event));

            if let Some(definition) = &trigger.definition {
                hover_text.push_str("**Definition:**\n```sql\n");
                hover_text.push_str(definition);
                hover_text.push_str("\n```\n");
            }

            return Some(Hover {
                contents: HoverContents::Scalar(MarkedString::String(hover_text)),
                range: None,
            });
        }

        // Check built-in SQL keywords and functions
        let keyword_upper = word.to_uppercase();
        match keyword_upper.as_str() {
            // DML - Data Manipulation Language
            "SELECT" => return Some(self.create_keyword_hover(
                "SELECT",
                "Retrieves data from one or more tables.\n\n**Syntax:**\n```sql\nSELECT column1, column2\nFROM table_name\nWHERE condition;\n```\n\n**Example:**\n```sql\nSELECT name, email FROM users WHERE active = 1;\n```"
            )),
            "FROM" => return Some(self.create_keyword_hover(
                "FROM",
                "Specifies the table(s) to retrieve data from.\n\n**Syntax:**\n```sql\nSELECT columns FROM table_name;\n```\n\nCan be used with JOIN clauses to query multiple tables."
            )),
            "WHERE" => return Some(self.create_keyword_hover(
                "WHERE",
                "Filters rows based on specified conditions.\n\n**Syntax:**\n```sql\nSELECT * FROM table_name WHERE condition;\n```\n\n**Example:**\n```sql\nSELECT * FROM products WHERE price > 100 AND category = 'electronics';\n```"
            )),
            "INSERT" => return Some(self.create_keyword_hover(
                "INSERT",
                "Adds new rows to a table.\n\n**Syntax:**\n```sql\nINSERT INTO table_name (col1, col2) VALUES (val1, val2);\n```\n\n**Example:**\n```sql\nINSERT INTO users (name, email) VALUES ('John', 'john@example.com');\n```"
            )),
            "UPDATE" => return Some(self.create_keyword_hover(
                "UPDATE",
                "Modifies existing rows in a table.\n\n**Syntax:**\n```sql\nUPDATE table_name SET col1 = val1 WHERE condition;\n```\n\n**Example:**\n```sql\nUPDATE users SET status = 'active' WHERE user_id = 123;\n```"
            )),
            "DELETE" => return Some(self.create_keyword_hover(
                "DELETE",
                "Removes rows from a table.\n\n**Syntax:**\n```sql\nDELETE FROM table_name WHERE condition;\n```\n\n**Example:**\n```sql\nDELETE FROM logs WHERE created_at < '2023-01-01';\n```\n\n⚠️ **Warning:** Without a WHERE clause, all rows will be deleted!"
            )),
            
            // JOIN Operations
            "JOIN" => return Some(self.create_keyword_hover(
                "JOIN",
                "Combines rows from two or more tables based on a related column.\n\n**Types:**\n- `INNER JOIN`: Returns matching rows from both tables\n- `LEFT JOIN`: Returns all rows from left table and matching rows from right\n- `RIGHT JOIN`: Returns all rows from right table and matching rows from left\n- `FULL JOIN`: Returns all rows when there's a match in either table\n\n**Example:**\n```sql\nSELECT u.name, o.order_id\nFROM users u\nJOIN orders o ON u.id = o.user_id;\n```"
            )),
            "INNER" => return Some(self.create_keyword_hover(
                "INNER JOIN",
                "Returns rows when there is a match in both tables.\n\n**Syntax:**\n```sql\nSELECT columns\nFROM table1\nINNER JOIN table2 ON table1.col = table2.col;\n```\n\n**Example:**\n```sql\nSELECT customers.name, orders.amount\nFROM customers\nINNER JOIN orders ON customers.id = orders.customer_id;\n```"
            )),
            "LEFT" => return Some(self.create_keyword_hover(
                "LEFT JOIN",
                "Returns all rows from the left table and matched rows from the right table. If no match, NULL values are returned for right table columns.\n\n**Syntax:**\n```sql\nSELECT columns\nFROM table1\nLEFT JOIN table2 ON table1.col = table2.col;\n```\n\n**Example:**\n```sql\nSELECT users.name, orders.order_id\nFROM users\nLEFT JOIN orders ON users.id = orders.user_id;\n```"
            )),
            "RIGHT" => return Some(self.create_keyword_hover(
                "RIGHT JOIN",
                "Returns all rows from the right table and matched rows from the left table. If no match, NULL values are returned for left table columns.\n\n**Syntax:**\n```sql\nSELECT columns\nFROM table1\nRIGHT JOIN table2 ON table1.col = table2.col;\n```"
            )),
            "FULL" => return Some(self.create_keyword_hover(
                "FULL JOIN / FULL OUTER JOIN",
                "Returns all rows when there is a match in either left or right table. Returns NULL for non-matching rows.\n\n**Syntax:**\n```sql\nSELECT columns\nFROM table1\nFULL OUTER JOIN table2 ON table1.col = table2.col;\n```"
            )),
            "CROSS" => return Some(self.create_keyword_hover(
                "CROSS JOIN",
                "Returns the Cartesian product of both tables (all possible combinations).\n\n**Syntax:**\n```sql\nSELECT * FROM table1 CROSS JOIN table2;\n```\n\n⚠️ **Warning:** Can produce very large result sets!"
            )),
            "ON" => return Some(self.create_keyword_hover(
                "ON",
                "Specifies the join condition between tables.\n\n**Example:**\n```sql\nSELECT * FROM users u\nJOIN orders o ON u.id = o.user_id;\n```"
            )),
            
            // Grouping and Aggregation
            "GROUP" => return Some(self.create_keyword_hover(
                "GROUP BY",
                "Groups rows that have the same values into summary rows.\n\n**Syntax:**\n```sql\nSELECT column, aggregate_function(column)\nFROM table_name\nGROUP BY column;\n```\n\n**Example:**\n```sql\nSELECT category, COUNT(*) as total\nFROM products\nGROUP BY category;\n```"
            )),
            "HAVING" => return Some(self.create_keyword_hover(
                "HAVING",
                "Filters grouped rows based on a condition. Similar to WHERE but used after GROUP BY.\n\n**Syntax:**\n```sql\nSELECT column, COUNT(*)\nFROM table_name\nGROUP BY column\nHAVING COUNT(*) > value;\n```\n\n**Example:**\n```sql\nSELECT category, AVG(price)\nFROM products\nGROUP BY category\nHAVING AVG(price) > 100;\n```"
            )),
            
            // Ordering and Limiting
            "ORDER" => return Some(self.create_keyword_hover(
                "ORDER BY",
                "Sorts the result set by one or more columns.\n\n**Syntax:**\n```sql\nSELECT * FROM table_name\nORDER BY column1 [ASC|DESC], column2 [ASC|DESC];\n```\n\n**Example:**\n```sql\nSELECT name, price FROM products\nORDER BY price DESC, name ASC;\n```"
            )),
            "ASC" => return Some(self.create_keyword_hover(
                "ASC",
                "Sorts in ascending order (smallest to largest, A to Z).\n\n**Example:**\n```sql\nSELECT * FROM users ORDER BY created_at ASC;\n```\n\nThis is the default sort order if not specified."
            )),
            "DESC" => return Some(self.create_keyword_hover(
                "DESC",
                "Sorts in descending order (largest to smallest, Z to A).\n\n**Example:**\n```sql\nSELECT * FROM users ORDER BY created_at DESC;\n```"
            )),
            "LIMIT" => return Some(self.create_keyword_hover(
                "LIMIT",
                "Restricts the number of rows returned by the query.\n\n**Syntax:**\n```sql\nSELECT * FROM table_name LIMIT count;\nSELECT * FROM table_name LIMIT offset, count;\n```\n\n**Example:**\n```sql\nSELECT * FROM users ORDER BY created_at DESC LIMIT 10;\n```"
            )),
            "OFFSET" => return Some(self.create_keyword_hover(
                "OFFSET",
                "Skips a specified number of rows before returning results. Used for pagination.\n\n**Syntax:**\n```sql\nSELECT * FROM table_name LIMIT count OFFSET skip;\n```\n\n**Example:**\n```sql\nSELECT * FROM users LIMIT 10 OFFSET 20;  -- Gets rows 21-30\n```"
            )),
            
            // Modifiers
            "DISTINCT" => return Some(self.create_keyword_hover(
                "DISTINCT",
                "Returns only unique rows, removing duplicates.\n\n**Syntax:**\n```sql\nSELECT DISTINCT column1, column2 FROM table_name;\n```\n\n**Example:**\n```sql\nSELECT DISTINCT country FROM users;\n```"
            )),
            "ALL" => return Some(self.create_keyword_hover(
                "ALL",
                "Returns all rows including duplicates (default behavior).\n\nAlso used with comparison operators:\n```sql\nSELECT * FROM products\nWHERE price > ALL (SELECT price FROM discounted_products);\n```"
            )),
            "AS" => return Some(self.create_keyword_hover(
                "AS",
                "Creates an alias for a column or table.\n\n**Syntax:**\n```sql\nSELECT column AS alias_name FROM table AS t;\n```\n\n**Example:**\n```sql\nSELECT \n    first_name || ' ' || last_name AS full_name,\n    price * 1.1 AS price_with_tax\nFROM products;\n```"
            )),
            
            // Aggregate Functions
            "COUNT" => return Some(self.create_keyword_hover(
                "COUNT()",
                "Returns the number of rows matching the criteria.\n\n**Syntax:**\n```sql\nCOUNT(*) -- All rows\nCOUNT(column) -- Non-NULL values\nCOUNT(DISTINCT column) -- Unique non-NULL values\n```\n\n**Example:**\n```sql\nSELECT COUNT(*) as total_users FROM users;\nSELECT COUNT(DISTINCT country) as countries FROM users;\n```"
            )),
            "SUM" => return Some(self.create_keyword_hover(
                "SUM()",
                "Returns the sum of numeric values.\n\n**Syntax:**\n```sql\nSUM(column)\n```\n\n**Example:**\n```sql\nSELECT SUM(amount) as total_revenue FROM orders;\nSELECT category, SUM(quantity) FROM products GROUP BY category;\n```"
            )),
            "AVG" => return Some(self.create_keyword_hover(
                "AVG()",
                "Returns the average of numeric values.\n\n**Syntax:**\n```sql\nAVG(column)\n```\n\n**Example:**\n```sql\nSELECT AVG(price) as avg_price FROM products;\nSELECT category, AVG(rating) FROM products GROUP BY category;\n```"
            )),
            "MAX" => return Some(self.create_keyword_hover(
                "MAX()",
                "Returns the maximum value.\n\n**Syntax:**\n```sql\nMAX(column)\n```\n\n**Example:**\n```sql\nSELECT MAX(price) as highest_price FROM products;\nSELECT MAX(created_at) as last_order FROM orders;\n```"
            )),
            "MIN" => return Some(self.create_keyword_hover(
                "MIN()",
                "Returns the minimum value.\n\n**Syntax:**\n```sql\nMIN(column)\n```\n\n**Example:**\n```sql\nSELECT MIN(price) as lowest_price FROM products;\nSELECT MIN(created_at) as first_order FROM orders;\n```"
            )),
            
            // Conditional Logic
            "CASE" => return Some(self.create_keyword_hover(
                "CASE",
                "Provides conditional logic in SQL queries.\n\n**Syntax:**\n```sql\nCASE\n    WHEN condition1 THEN result1\n    WHEN condition2 THEN result2\n    ELSE default_result\nEND\n```\n\n**Example:**\n```sql\nSELECT name,\n    CASE \n        WHEN age < 18 THEN 'Minor'\n        WHEN age < 65 THEN 'Adult'\n        ELSE 'Senior'\n    END as age_group\nFROM users;\n```"
            )),
            "WHEN" => return Some(self.create_keyword_hover(
                "WHEN",
                "Specifies a condition in a CASE expression.\n\n**Example:**\n```sql\nCASE\n    WHEN status = 'active' THEN 'Active User'\n    WHEN status = 'pending' THEN 'Pending Approval'\n    ELSE 'Inactive'\nEND\n```"
            )),
            "THEN" => return Some(self.create_keyword_hover(
                "THEN",
                "Specifies the result when a WHEN condition is true.\n\nUsed in CASE expressions."
            )),
            "ELSE" => return Some(self.create_keyword_hover(
                "ELSE",
                "Specifies the default result in a CASE expression when no conditions match.\n\n**Example:**\n```sql\nCASE\n    WHEN price < 10 THEN 'Cheap'\n    WHEN price < 100 THEN 'Moderate'\n    ELSE 'Expensive'\nEND\n```"
            )),
            "END" => return Some(self.create_keyword_hover(
                "END",
                "Marks the end of a CASE expression or other control structures."
            )),
            
            // Operators
            "AND" => return Some(self.create_keyword_hover(
                "AND",
                "Logical operator that combines multiple conditions. All conditions must be true.\n\n**Example:**\n```sql\nSELECT * FROM products\nWHERE price > 10 AND category = 'electronics' AND in_stock = 1;\n```"
            )),
            "OR" => return Some(self.create_keyword_hover(
                "OR",
                "Logical operator that combines multiple conditions. At least one condition must be true.\n\n**Example:**\n```sql\nSELECT * FROM users\nWHERE country = 'USA' OR country = 'Canada';\n```"
            )),
            "NOT" => return Some(self.create_keyword_hover(
                "NOT",
                "Logical operator that negates a condition.\n\n**Example:**\n```sql\nSELECT * FROM users WHERE NOT status = 'deleted';\nSELECT * FROM products WHERE category NOT IN ('accessories', 'parts');\n```"
            )),
            "IN" => return Some(self.create_keyword_hover(
                "IN",
                "Checks if a value matches any value in a list.\n\n**Syntax:**\n```sql\ncolumn IN (value1, value2, ...)\ncolumn IN (SELECT column FROM table)\n```\n\n**Example:**\n```sql\nSELECT * FROM users WHERE country IN ('USA', 'UK', 'Canada');\nSELECT * FROM orders WHERE user_id IN (SELECT id FROM premium_users);\n```"
            )),
            "BETWEEN" => return Some(self.create_keyword_hover(
                "BETWEEN",
                "Checks if a value is within a range (inclusive).\n\n**Syntax:**\n```sql\ncolumn BETWEEN value1 AND value2\n```\n\n**Example:**\n```sql\nSELECT * FROM products WHERE price BETWEEN 10 AND 100;\nSELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31';\n```"
            )),
            "LIKE" => return Some(self.create_keyword_hover(
                "LIKE",
                "Pattern matching for strings using wildcards.\n\n**Wildcards:**\n- `%` matches any sequence of characters\n- `_` matches any single character\n\n**Example:**\n```sql\nSELECT * FROM users WHERE email LIKE '%@gmail.com';\nSELECT * FROM products WHERE name LIKE 'Apple%';\nSELECT * FROM codes WHERE code LIKE 'A_C%';\n```"
            )),
            "IS" => return Some(self.create_keyword_hover(
                "IS",
                "Used to check for NULL values.\n\n**Syntax:**\n```sql\ncolumn IS NULL\ncolumn IS NOT NULL\n```\n\n**Example:**\n```sql\nSELECT * FROM users WHERE deleted_at IS NULL;\nSELECT * FROM products WHERE description IS NOT NULL;\n```\n\nNote: Use `IS NULL`, not `= NULL`"
            )),
            "NULL" => return Some(self.create_keyword_hover(
                "NULL",
                "Represents a missing or unknown value.\n\n**Important:**\n- NULL is not equal to anything, including NULL\n- Use `IS NULL` or `IS NOT NULL` to check for NULL\n- Arithmetic operations with NULL return NULL\n- Most aggregate functions ignore NULL values\n\n**Example:**\n```sql\nSELECT * FROM users WHERE phone IS NULL;\nINSERT INTO products (name, price) VALUES ('Item', NULL);\n```"
            )),
            
            // Set Operations
            "UNION" => return Some(self.create_keyword_hover(
                "UNION",
                "Combines results from multiple SELECT statements, removing duplicates.\n\n**Syntax:**\n```sql\nSELECT columns FROM table1\nUNION\nSELECT columns FROM table2;\n```\n\n**Example:**\n```sql\nSELECT name FROM customers\nUNION\nSELECT name FROM suppliers;\n```\n\nUse `UNION ALL` to include duplicates."
            )),
            "INTERSECT" => return Some(self.create_keyword_hover(
                "INTERSECT",
                "Returns only rows that appear in both result sets.\n\n**Example:**\n```sql\nSELECT product_id FROM orders_2023\nINTERSECT\nSELECT product_id FROM orders_2024;\n```"
            )),
            "EXCEPT" => return Some(self.create_keyword_hover(
                "EXCEPT",
                "Returns rows from the first query that are not in the second query.\n\n**Example:**\n```sql\nSELECT email FROM all_users\nEXCEPT\nSELECT email FROM unsubscribed;\n```"
            )),
            
            // DDL - Data Definition Language
            "CREATE" => return Some(self.create_keyword_hover(
                "CREATE",
                "Creates database objects (tables, indexes, views, etc.).\n\n**Examples:**\n```sql\nCREATE TABLE users (\n    id INTEGER PRIMARY KEY,\n    name TEXT NOT NULL,\n    email TEXT UNIQUE\n);\n\nCREATE INDEX idx_email ON users(email);\nCREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;\n```"
            )),
            "TABLE" => return Some(self.create_keyword_hover(
                "TABLE",
                "Used in CREATE, ALTER, and DROP statements to specify table operations.\n\n**Examples:**\n```sql\nCREATE TABLE products (...);\nALTER TABLE products ADD COLUMN price DECIMAL(10,2);\nDROP TABLE old_data;\n```"
            )),
            "ALTER" => return Some(self.create_keyword_hover(
                "ALTER",
                "Modifies the structure of existing database objects.\n\n**Examples:**\n```sql\nALTER TABLE users ADD COLUMN phone VARCHAR(20);\nALTER TABLE users DROP COLUMN temp_field;\nALTER TABLE users RENAME TO customers;\nALTER TABLE users MODIFY COLUMN name VARCHAR(200);\n```"
            )),
            "DROP" => return Some(self.create_keyword_hover(
                "DROP",
                "Deletes database objects.\n\n**Examples:**\n```sql\nDROP TABLE old_users;\nDROP INDEX idx_email;\nDROP VIEW temp_view;\n```\n\n⚠️ **Warning:** This permanently deletes the object and all its data!"
            )),
            "TRUNCATE" => return Some(self.create_keyword_hover(
                "TRUNCATE",
                "Removes all rows from a table quickly, but keeps the table structure.\n\n**Syntax:**\n```sql\nTRUNCATE TABLE table_name;\n```\n\n**Differences from DELETE:**\n- Faster than DELETE\n- Cannot be rolled back in some databases\n- Resets auto-increment counters\n- Does not fire triggers"
            )),
            
            // Constraints
            "PRIMARY" => return Some(self.create_keyword_hover(
                "PRIMARY KEY",
                "Uniquely identifies each row in a table. Cannot contain NULL values.\n\n**Example:**\n```sql\nCREATE TABLE users (\n    id INTEGER PRIMARY KEY,\n    email TEXT\n);\n\n-- Composite primary key\nCREATE TABLE order_items (\n    order_id INTEGER,\n    product_id INTEGER,\n    PRIMARY KEY (order_id, product_id)\n);\n```"
            )),
            "FOREIGN" => return Some(self.create_keyword_hover(
                "FOREIGN KEY",
                "Creates a link between two tables, ensuring referential integrity.\n\n**Example:**\n```sql\nCREATE TABLE orders (\n    id INTEGER PRIMARY KEY,\n    user_id INTEGER,\n    FOREIGN KEY (user_id) REFERENCES users(id)\n        ON DELETE CASCADE\n        ON UPDATE CASCADE\n);\n```"
            )),
            "UNIQUE" => return Some(self.create_keyword_hover(
                "UNIQUE",
                "Ensures all values in a column are different.\n\n**Example:**\n```sql\nCREATE TABLE users (\n    id INTEGER PRIMARY KEY,\n    email TEXT UNIQUE,\n    username TEXT UNIQUE\n);\n```"
            )),
            "CHECK" => return Some(self.create_keyword_hover(
                "CHECK",
                "Ensures all values in a column satisfy a specific condition.\n\n**Example:**\n```sql\nCREATE TABLE products (\n    id INTEGER PRIMARY KEY,\n    price DECIMAL(10,2) CHECK (price > 0),\n    stock INTEGER CHECK (stock >= 0),\n    rating DECIMAL(3,2) CHECK (rating BETWEEN 0 AND 5)\n);\n```"
            )),
            "DEFAULT" => return Some(self.create_keyword_hover(
                "DEFAULT",
                "Provides a default value for a column when no value is specified.\n\n**Example:**\n```sql\nCREATE TABLE users (\n    id INTEGER PRIMARY KEY,\n    status TEXT DEFAULT 'active',\n    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n    login_count INTEGER DEFAULT 0\n);\n```"
            )),
            
            // Subqueries and Exists
            "EXISTS" => return Some(self.create_keyword_hover(
                "EXISTS",
                "Tests for the existence of rows in a subquery. Returns true if subquery returns any rows.\n\n**Example:**\n```sql\nSELECT * FROM users u\nWHERE EXISTS (\n    SELECT 1 FROM orders o WHERE o.user_id = u.id\n);\n```\n\nOften more efficient than using IN with subqueries."
            )),
            "ANY" => return Some(self.create_keyword_hover(
                "ANY",
                "Compares a value to any value in a list or subquery. Returns true if any comparison is true.\n\n**Example:**\n```sql\nSELECT * FROM products\nWHERE price > ANY (SELECT price FROM discounted_products);\n```"
            )),
            "SOME" => return Some(self.create_keyword_hover(
                "SOME",
                "Synonym for ANY. Compares a value to any value returned by a subquery.\n\n**Example:**\n```sql\nSELECT * FROM employees\nWHERE salary > SOME (SELECT salary FROM managers);\n```"
            )),
            
            // String Functions
            "CONCAT" => return Some(self.create_keyword_hover(
                "CONCAT()",
                "Concatenates (joins) two or more strings together.\n\n**Syntax:**\n```sql\nCONCAT(string1, string2, ...)\n```\n\n**Example:**\n```sql\nSELECT CONCAT(first_name, ' ', last_name) as full_name FROM users;\nSELECT CONCAT('Order #', order_id) as order_number FROM orders;\n```"
            )),
            "UPPER" => return Some(self.create_keyword_hover(
                "UPPER()",
                "Converts a string to uppercase.\n\n**Example:**\n```sql\nSELECT UPPER(name) FROM users;\nSELECT * FROM products WHERE UPPER(category) = 'ELECTRONICS';\n```"
            )),
            "LOWER" => return Some(self.create_keyword_hover(
                "LOWER()",
                "Converts a string to lowercase.\n\n**Example:**\n```sql\nSELECT LOWER(email) FROM users;\nSELECT * FROM products WHERE LOWER(name) LIKE '%apple%';\n```"
            )),
            "LENGTH" => return Some(self.create_keyword_hover(
                "LENGTH()",
                "Returns the length of a string.\n\n**Example:**\n```sql\nSELECT name, LENGTH(name) as name_length FROM users;\nSELECT * FROM posts WHERE LENGTH(content) > 1000;\n```"
            )),
            "TRIM" => return Some(self.create_keyword_hover(
                "TRIM()",
                "Removes leading and trailing whitespace from a string.\n\n**Example:**\n```sql\nSELECT TRIM(name) FROM users;\nUPDATE users SET email = TRIM(email);\n```"
            )),
            "SUBSTRING" | "SUBSTR" => return Some(self.create_keyword_hover(
                "SUBSTRING() / SUBSTR()",
                "Extracts a portion of a string.\n\n**Syntax:**\n```sql\nSUBSTRING(string, start, length)\nSUBSTR(string, start, length)\n```\n\n**Example:**\n```sql\nSELECT SUBSTRING(phone, 1, 3) as area_code FROM contacts;\nSELECT SUBSTR(product_code, 4, 6) FROM products;\n```"
            )),
            
            // Date/Time Functions
            "NOW" => return Some(self.create_keyword_hover(
                "NOW()",
                "Returns the current date and time.\n\n**Example:**\n```sql\nSELECT NOW();\nINSERT INTO logs (message, created_at) VALUES ('Event', NOW());\nSELECT * FROM orders WHERE created_at > NOW() - INTERVAL 7 DAY;\n```"
            )),
            "CURRENT_TIMESTAMP" => return Some(self.create_keyword_hover(
                "CURRENT_TIMESTAMP",
                "Returns the current date and time. Similar to NOW().\n\n**Example:**\n```sql\nCREATE TABLE logs (\n    id INTEGER PRIMARY KEY,\n    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\n```"
            )),
            "DATE" => return Some(self.create_keyword_hover(
                "DATE",
                "Data type for storing dates, or function to extract date part.\n\n**As data type:**\n```sql\nCREATE TABLE events (event_date DATE);\n```\n\n**As function:**\n```sql\nSELECT DATE(created_at) FROM orders;\nSELECT * FROM events WHERE DATE(event_time) = '2024-01-15';\n```"
            )),
            "TIME" => return Some(self.create_keyword_hover(
                "TIME",
                "Data type for storing time values, or function to extract time part.\n\n**Example:**\n```sql\nCREATE TABLE schedules (start_time TIME);\nSELECT TIME(created_at) FROM orders;\n```"
            )),
            "YEAR" | "MONTH" | "DAY" => return Some(self.create_keyword_hover(
                "YEAR() / MONTH() / DAY()",
                "Extracts the year, month, or day from a date.\n\n**Example:**\n```sql\nSELECT YEAR(created_at) as year, COUNT(*) FROM orders GROUP BY year;\nSELECT * FROM events WHERE MONTH(event_date) = 12;\nSELECT * FROM logs WHERE DAY(created_at) = 1;\n```"
            )),
            
            // Transaction Control
            "BEGIN" => return Some(self.create_keyword_hover(
                "BEGIN",
                "Starts a new transaction.\n\n**Example:**\n```sql\nBEGIN;\nUPDATE accounts SET balance = balance - 100 WHERE id = 1;\nUPDATE accounts SET balance = balance + 100 WHERE id = 2;\nCOMMIT;\n```"
            )),
            "COMMIT" => return Some(self.create_keyword_hover(
                "COMMIT",
                "Saves all changes made in the current transaction.\n\n**Example:**\n```sql\nBEGIN;\nINSERT INTO orders (user_id, total) VALUES (1, 99.99);\nUPDATE inventory SET stock = stock - 1 WHERE product_id = 123;\nCOMMIT;\n```"
            )),
            "ROLLBACK" => return Some(self.create_keyword_hover(
                "ROLLBACK",
                "Undoes all changes made in the current transaction.\n\n**Example:**\n```sql\nBEGIN;\nDELETE FROM important_data;\n-- Oops, that was a mistake!\nROLLBACK;\n```"
            )),
            
            // Window Functions
            "OVER" => return Some(self.create_keyword_hover(
                "OVER",
                "Defines a window for window functions.\n\n**Example:**\n```sql\nSELECT \n    name,\n    salary,\n    AVG(salary) OVER (PARTITION BY department) as dept_avg,\n    ROW_NUMBER() OVER (ORDER BY salary DESC) as rank\nFROM employees;\n```"
            )),
            "PARTITION" => return Some(self.create_keyword_hover(
                "PARTITION BY",
                "Divides the result set into partitions for window functions.\n\n**Example:**\n```sql\nSELECT \n    product,\n    category,\n    sales,\n    SUM(sales) OVER (PARTITION BY category) as category_total\nFROM products;\n```"
            )),
            "ROWS" => return Some(self.create_keyword_hover(
                "ROWS",
                "Defines the window frame in terms of physical rows.\n\n**Example:**\n```sql\nSELECT \n    date,\n    sales,\n    AVG(sales) OVER (\n        ORDER BY date\n        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW\n    ) as moving_avg\nFROM daily_sales;\n```"
            )),
            
            // Other Common Keywords
            "INDEX" => return Some(self.create_keyword_hover(
                "INDEX",
                "Creates an index to speed up queries on specific columns.\n\n**Example:**\n```sql\nCREATE INDEX idx_email ON users(email);\nCREATE UNIQUE INDEX idx_username ON users(username);\nCREATE INDEX idx_multi ON orders(user_id, created_at);\n```\n\nIndexes improve read performance but slow down writes."
            )),
            "VIEW" => return Some(self.create_keyword_hover(
                "VIEW",
                "A virtual table based on a query.\n\n**Example:**\n```sql\nCREATE VIEW active_users AS\nSELECT id, name, email FROM users WHERE status = 'active';\n\nSELECT * FROM active_users WHERE name LIKE 'John%';\n```"
            )),
            "WITH" => return Some(self.create_keyword_hover(
                "WITH (CTE)",
                "Common Table Expression - creates a temporary named result set.\n\n**Example:**\n```sql\nWITH recent_orders AS (\n    SELECT user_id, COUNT(*) as order_count\n    FROM orders\n    WHERE created_at > NOW() - INTERVAL 30 DAY\n    GROUP BY user_id\n)\nSELECT u.name, ro.order_count\nFROM users u\nJOIN recent_orders ro ON u.id = ro.user_id;\n```"
            )),
            "CAST" => return Some(self.create_keyword_hover(
                "CAST()",
                "Converts a value from one data type to another.\n\n**Syntax:**\n```sql\nCAST(expression AS datatype)\n```\n\n**Example:**\n```sql\nSELECT CAST(price AS INTEGER) FROM products;\nSELECT CAST('2024-01-15' AS DATE);\nSELECT CAST(total AS VARCHAR(20)) FROM orders;\n```"
            )),
            "COALESCE" => return Some(self.create_keyword_hover(
                "COALESCE()",
                "Returns the first non-NULL value in a list.\n\n**Example:**\n```sql\nSELECT COALESCE(phone, email, 'No contact') as contact FROM users;\nSELECT name, COALESCE(discount_price, regular_price) as final_price FROM products;\n```"
            )),
            "IFNULL" | "ISNULL" => return Some(self.create_keyword_hover(
                "IFNULL() / ISNULL()",
                "Returns an alternative value if the expression is NULL.\n\n**Example:**\n```sql\nSELECT name, IFNULL(phone, 'N/A') as phone FROM users;\nSELECT IFNULL(SUM(amount), 0) as total FROM orders;\n```"
            )),
            
            _ => {}
        }

        None
    }

    /// Get definition location for the symbol at the given position
    ///
    /// For SQL, this provides go-to-definition functionality for:
    /// - Table references: Returns location info about the table
    /// - Column references: Returns the table that contains the column
    /// - View/function/procedure references: Returns info about the object
    ///
    /// Since we don't have actual DDL file locations, this returns a GotoDefinitionResponse
    /// with location information about the database object.
    pub fn get_definition(&self, text: &Rope, offset: usize) -> Option<GotoDefinitionResponse> {
        tracing::debug!("get_definition: offset={}", offset);

        // Get the word at the cursor position
        let word = match self.get_word_at_offset(text, offset) {
            Some(w) => {
                tracing::debug!("word extracted: '{}'", w);
                w
            }
            None => {
                tracing::debug!("no word at offset");
                return None;
            }
        };

        if word.is_empty() {
            tracing::debug!("empty word at offset");
            return None;
        }

        let word_lower = word.to_lowercase();
        let sql = text.to_string();
        let before_cursor = &sql[..offset.min(sql.len())];

        // Check if it's a qualified reference (table.column or alias.column)
        if let Some(dot_pos) = before_cursor.rfind('.') {
            let table_part = &before_cursor[..dot_pos];
            if let Some(table_start) = table_part.rfind(|c: char| !c.is_alphanumeric() && c != '_') {
                let table_name = &table_part[table_start + 1..];
                tracing::debug!("qualified column reference: {}.{}", table_name, word);

                // For qualified column references, find the column in the specified table
                if let Some(columns) = self.schema_cache.columns_by_table.get(table_name) {
                    for col in columns {
                        if col.name.to_lowercase() == word_lower {
                            // Found the column - return definition info
                            // For SQL, we return info about where this column comes from
                            tracing::debug!(table = table_name, column = %col.name, "Found column definition");
                            return Some(GotoDefinitionResponse::Scalar(Location {
                                uri: "sql://internal".parse::<Uri>().unwrap(),
                                range: Range {
                                    start: Position { line: 0, character: 0 },
                                    end: Position { line: 0, character: 0 },
                                },
                            }));
                        }
                    }
                }
            }
        }

        // Check tables (unqualified table reference)
        if let Some(table) = self.schema_cache.tables.get(&word) {
            tracing::debug!(table = %table.name, "Found table definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        // Check columns (unqualified - search all tables)
        for (table_name, columns) in &self.schema_cache.columns_by_table {
            for col in columns {
                if col.name.to_lowercase() == word_lower {
                    tracing::debug!(column = %col.name, table = table_name, "Found column definition");
                    // Return definition pointing to the table containing this column
                    return Some(GotoDefinitionResponse::Scalar(Location {
                        uri: "sql://internal".parse::<Uri>().unwrap(),
                        range: Range {
                            start: Position { line: 0, character: 0 },
                            end: Position { line: 0, character: 0 },
                        },
                    }));
                }
            }
        }

        // Check views
        if let Some(view) = self.schema_cache.views.get(&word) {
            tracing::debug!(view = %view.name, "Found view definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        // Check functions
        if let Some(func) = self.schema_cache.functions.get(&word) {
            tracing::debug!(function = %func.name, "Found function definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        // Check procedures
        if let Some(proc) = self.schema_cache.procedures.get(&word) {
            tracing::debug!(procedure = %proc.name, "Found procedure definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        // Check triggers
        if let Some(trigger) = self.schema_cache.triggers.get(&word) {
            tracing::debug!(trigger = %trigger.name, "Found trigger definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        // Check indexes
        if let Some(index) = self.schema_cache.indexes.get(&word) {
            tracing::debug!(index = %index.name, "Found index definition");
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: "sql://internal".parse::<Uri>().unwrap(),
                range: Range {
                    start: Position { line: 0, character: 0 },
                    end: Position { line: 0, character: 0 },
                },
            }));
        }

        tracing::debug!("no definition found for: {}", word);
        None
    }

    /// Find all references to a symbol at the given offset.
    ///
    /// This searches through the text for all occurrences of the identifier
    /// and returns their locations. It excludes SQL keywords from results.
    ///
    /// Returns a list of locations where the symbol is referenced.
    pub fn get_references(&self, text: &Rope, offset: usize) -> Vec<Location> {
        tracing::debug!("get_references: offset={}", offset);

        // Get the word at the cursor position
        let word = match self.get_word_at_offset(text, offset) {
            Some(w) => {
                tracing::debug!("word extracted: '{}'", w);
                w
            }
            None => {
                tracing::debug!("no word at offset");
                return Vec::new();
            }
        };

        if word.is_empty() {
            tracing::debug!("empty word at offset");
            return Vec::new();
        }

        // Check if it's a keyword - don't find references for keywords
        let word_upper = word.to_uppercase();
        if self.is_sql_keyword(&word_upper) {
            tracing::debug!("word is a keyword, skipping references");
            return Vec::new();
        }

        let word_lower = word.to_lowercase();
        let mut references = Vec::new();
        let text_str = text.to_string();

        // Search through the text for all occurrences of this word
        // We need to find word boundaries to avoid partial matches
        let search_result = self.find_all_word_references(&text_str, &word, &word_lower);
        references.extend(search_result);

        // Also search the schema for references to database objects
        // e.g., if user clicks on a column name, find all tables that use that column
        self.find_schema_references(&word_lower, &mut references);

        tracing::debug!("found {} references for: {}", references.len(), word);
        references
    }

    /// Check if a word is a SQL keyword
    fn is_sql_keyword(&self, word: &str) -> bool {
        // Import the keywords
        use crate::keywords::SQL_KEYWORDS;
        SQL_KEYWORDS.iter().any(|&kw| kw == word)
    }

    /// Find all occurrences of a word in text as an identifier (not part of another word)
    fn find_all_word_references(&self, text: &str, word: &str, word_lower: &str) -> Vec<Location> {
        let mut references = Vec::new();

        for (line_idx, line) in text.lines().enumerate() {
            let mut col_idx = 0;
            let chars: Vec<char> = line.chars().collect();
            let word_len = word.len();

            while col_idx < chars.len() {
                // Check if we have a word starting at this position
                let remaining: String = chars[col_idx..].iter().collect();
                let remaining_lower = remaining.to_lowercase();

                if remaining_lower.starts_with(word_lower) {
                    // Check word boundaries (before and after the match)
                    let before_ok = col_idx == 0
                        || !chars[col_idx - 1].is_alphanumeric()
                        && chars[col_idx - 1] != '_';
                    let after_ok = col_idx + word_len >= chars.len()
                        || !chars[col_idx + word_len].is_alphanumeric()
                        && chars[col_idx + word_len] != '_';

                    if before_ok && after_ok {
                        // Found a reference
                        tracing::debug!("found reference at line {}, col {}", line_idx, col_idx);
                        references.push(Location {
                            uri: "sql://internal".parse::<Uri>().unwrap(),
                            range: Range {
                                start: Position {
                                    line: line_idx as u32,
                                    character: col_idx as u32,
                                },
                                end: Position {
                                    line: line_idx as u32,
                                    character: (col_idx + word_len) as u32,
                                },
                            },
                        });
                    }
                }

                // Move to next character
                col_idx += 1;
            }
        }

        references
    }

    /// Find references in the schema (tables, views that use a column, etc.)
    fn find_schema_references(&self, word_lower: &str, references: &mut Vec<Location>) {
        // Check if this is a column name and find tables that reference it
        for (table_name, columns) in &self.schema_cache.columns_by_table {
            for col in columns {
                if col.name.to_lowercase() == *word_lower {
                    // Found a column - add a reference to indicate the table uses this column
                    // For now, we add a reference with the table name in the URI
                    let Ok(uri) = format!("sql://internal/table/{}", table_name).parse::<Uri>() else { continue; };
                    let location = Location {
                        uri,
                        range: Range {
                            start: Position { line: 0, character: 0 },
                            end: Position { line: 0, character: 0 },
                        },
                    };
                    // Only add if not already present
                    if !references.iter().any(|r| r.uri == location.uri) {
                        references.push(location);
                    }
                }
            }
        }

        // Check if this is a table name and find views/other objects that reference it
        if self.schema_cache.tables.contains_key(word_lower) {
            // Find views that join with this table
            for (view_name, _view) in &self.schema_cache.views {
                // Simplified - could check actual JOINs in view definitions
                let Ok(uri) = format!("sql://internal/view/{}", view_name).parse::<Uri>() else { continue; };
                let location = Location {
                    uri,
                    range: Range {
                        start: Position { line: 0, character: 0 },
                        end: Position { line: 0, character: 0 },
                    },
                };
                if !references.iter().any(|r| r.uri == location.uri) {
                    references.push(location);
                }
            }
        }
    }

    /// Rename a symbol at the given offset to a new name.
    ///
    /// This method:
    /// 1. Gets the word at the cursor position
    /// 2. Validates the new name is a valid SQL identifier
    /// 3. Finds all occurrences of the word in the text
    /// 4. Returns a WorkspaceEdit with TextEdits for all locations
    ///
    /// Returns None if:
    /// - No word is at the cursor position
    /// - The word is a SQL keyword
    /// - The new name is not a valid SQL identifier
    pub fn rename(&self, text: &Rope, offset: usize, new_name: &str) -> Option<WorkspaceEdit> {
        tracing::debug!("rename: offset={}, new_name={}", offset, new_name);

        // Validate the new name is a valid SQL identifier
        if !self.is_valid_sql_identifier(new_name) {
            tracing::debug!("invalid SQL identifier: {}", new_name);
            return None;
        }

        // Get the word at the cursor position
        let word = match self.get_word_at_offset(text, offset) {
            Some(w) => {
                tracing::debug!("word extracted: '{}'", w);
                w
            }
            None => {
                tracing::debug!("no word at offset");
                return None;
            }
        };

        if word.is_empty() {
            tracing::debug!("empty word at offset");
            return None;
        }

        // Don't rename SQL keywords
        let word_upper = word.to_uppercase();
        if self.is_sql_keyword(&word_upper) {
            tracing::debug!("word is a keyword, skipping rename");
            return None;
        }

        // Don't rename if the new name is the same as the old
        let word_lower = word.to_lowercase();
        let new_name_lower = new_name.to_lowercase();
        if word_lower == new_name_lower {
            tracing::debug!("new name is the same as old name");
            return None;
        }

        // Find all occurrences of the word in the text
        let text_str = text.to_string();
        let mut text_edits = Vec::new();

        // Search through the text for all occurrences of this word
        // We need to find word boundaries to avoid partial matches
        let chars: Vec<char> = text_str.chars().collect();
        let word_len = word.len();
        let mut col_idx = 0;

        while col_idx < chars.len() {
            // Check if we have a word starting at this position
            let remaining: String = chars[col_idx..].iter().collect();
            let remaining_lower = remaining.to_lowercase();

            if remaining_lower.starts_with(&word_lower) {
                // Check word boundaries (before and after the match)
                let before_ok = col_idx == 0
                    || !chars[col_idx - 1].is_alphanumeric()
                    && chars[col_idx - 1] != '_';
                let after_ok = col_idx + word_len >= chars.len()
                    || !chars[col_idx + word_len].is_alphanumeric()
                    && chars[col_idx + word_len] != '_';

                if before_ok && after_ok {
                    // Calculate the line number for this position
                    let line_idx = text_str[..col_idx.min(text_str.len())]
                        .chars()
                        .filter(|&c| c == '\n')
                        .count();

                    // Get the character position within the line
                    let char_in_line = if let Some(last_newline) = text_str[..col_idx.min(text_str.len())].rfind('\n') {
                        col_idx - last_newline - 1
                    } else {
                        col_idx
                    };

                    tracing::debug!("found rename location at line {}, col {}", line_idx, col_idx);
                    text_edits.push(TextEdit {
                        range: Range {
                            start: Position {
                                line: line_idx as u32,
                                character: char_in_line as u32,
                            },
                            end: Position {
                                line: line_idx as u32,
                                character: (char_in_line + word_len) as u32,
                            },
                        },
                        new_text: new_name.to_string(),
                    });
                }
            }

            // Move to next character
            col_idx += 1;
        }

        if text_edits.is_empty() {
            tracing::debug!("no locations found for rename");
            return None;
        }

        tracing::debug!("found {} locations for rename: {}", text_edits.len(), word);

        Some(WorkspaceEdit {
            changes: Some(HashMap::from([(
                "sql://internal".parse::<Uri>().unwrap(),
                text_edits,
            )])),
            document_changes: None,
            change_annotations: None,
        })
    }

    /// Check if a string is a valid SQL identifier
    fn is_valid_sql_identifier(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        let chars: Vec<char> = name.chars().collect();

        // First character must be a letter or underscore
        if !chars[0].is_alphabetic() && chars[0] != '_' {
            return false;
        }

        // Remaining characters can be letters, digits, or underscores
        for c in &chars[1..] {
            if !c.is_alphanumeric() && *c != '_' {
                return false;
            }
        }

        // Check against SQL keywords
        let name_upper = name.to_uppercase();
        !self.is_sql_keyword(&name_upper)
    }

    /// Get code actions for a given position in the text.
    ///
    /// Code actions provide quick fixes for diagnostics or suggestions
    /// based on the context at the cursor position.
    ///
    /// Returns a list of code actions that can be applied.
    pub fn get_code_actions(&self, text: &Rope, offset: usize, diagnostics: &[Diagnostic]) -> Vec<CodeAction> {
        tracing::debug!("get_code_actions: offset={}", offset);

        let mut code_actions = Vec::new();
        let text_str = text.to_string();

        // First, generate actions from diagnostics
        for diagnostic in diagnostics {
            let range = &diagnostic.range;
            // Check if the diagnostic is at or near our offset
            let diag_start = self.offset_to_usize(&text_str, &range.start);
            let diag_end = self.offset_to_usize(&text_str, &range.end);

            // Include diagnostics that contain or are near the cursor position
            if offset >= diag_start && offset <= diag_end {
                // Generate code actions based on the diagnostic message
                let actions = self.generate_actions_for_diagnostic(diagnostic, &text_str);
                code_actions.extend(actions);
            }
        }

        // If no diagnostic-based actions, generate context-based actions
        if code_actions.is_empty() {
            let context_actions = self.generate_context_actions(&text_str, offset);
            code_actions.extend(context_actions);
        }

        tracing::debug!("found {} code actions", code_actions.len());
        code_actions
    }

    /// Generate code actions for a specific diagnostic
    fn generate_actions_for_diagnostic(&self, diagnostic: &Diagnostic, text: &str) -> Vec<CodeAction> {
        let mut actions = Vec::new();

        let message = &diagnostic.message;
        let message_lower = message.to_lowercase();

        // Action: Add missing semicolon
        if message_lower.contains("expecting") && message_lower.contains(";") {
            if let Some(semi_pos) = self.find_semicolon_insertion_point(text, Some(&diagnostic.range)) {
                actions.push(CodeAction {
                    title: "Add missing semicolon".to_string(),
                    kind: Some(lsp_types::CodeActionKind::QUICKFIX),
                    diagnostics: None,
                    edit: Some(WorkspaceEdit {
                        changes: Some(HashMap::from([(
                            "sql://internal".parse::<Uri>().unwrap(),
                            vec![TextEdit {
                                range: Range {
                                    start: Position {
                                        line: semi_pos.0,
                                        character: semi_pos.1,
                                    },
                                    end: Position {
                                        line: semi_pos.0,
                                        character: semi_pos.1,
                                    },
                                },
                                new_text: ";".to_string(),
                            }],
                        )])),
                        document_changes: None,
                        change_annotations: None,
                    }),
                    command: None,
                    is_preferred: Some(true),
                    disabled: None,
                    data: None,
                });
            }
        }

        // Action: Quote identifier that's a reserved word
        if message_lower.contains("exposing") || message_lower.contains("reserved") || message_lower.contains("keyword") {
            if let Some(word_range) = self.find_unquoted_identifier(text, Some(&diagnostic.range)) {
                let identifier = self.extract_identifier(text, &word_range);
                if !identifier.is_empty() {
                    let quoted = format!("\"{}\"", identifier);
                    actions.push(CodeAction {
                        title: format!("Quote identifier '{}'", identifier),
                        kind: Some(lsp_types::CodeActionKind::QUICKFIX),
                        diagnostics: None,
                        edit: Some(WorkspaceEdit {
                            changes: Some(HashMap::from([(
                                "sql://internal".parse::<Uri>().unwrap(),
                                vec![TextEdit {
                                    range: word_range,
                                    new_text: quoted,
                                }],
                            )])),
                            document_changes: None,
                            change_annotations: None,
                        }),
                        command: None,
                        is_preferred: Some(true),
                        disabled: None,
                        data: None,
                    });
                }
            }
        }

        // Action: Fix string literal - add quotes
        if message_lower.contains("string literal") || message_lower.contains("unterminated") {
            let range = &diagnostic.range;
            let unquoted = self.extract_identifier(text, range);
            if !unquoted.is_empty() && !unquoted.starts_with('\'') {
                let quoted = format!("'{}'", unquoted.replace('\'', "''"));
                actions.push(CodeAction {
                    title: "Add quotes around string".to_string(),
                    kind: Some(lsp_types::CodeActionKind::QUICKFIX),
                    diagnostics: None,
                    edit: Some(WorkspaceEdit {
                        changes: Some(HashMap::from([(
                            "sql://internal".parse::<Uri>().unwrap(),
                            vec![TextEdit {
                                range: range.clone(),
                                new_text: quoted,
                            }],
                        )])),
                        document_changes: None,
                        change_annotations: None,
                    }),
                    command: None,
                    is_preferred: Some(false),
                    disabled: None,
                    data: None,
                });
            }
        }

        actions
    }

    /// Generate context-based code actions (not tied to diagnostics)
    fn generate_context_actions(&self, text: &str, _offset: usize) -> Vec<CodeAction> {
        let mut actions = Vec::new();

        // Action: Add semicolon at end of query if missing
        if !text.trim_end().ends_with(';') && !text.trim().is_empty() {
            let (line, col) = self.find_query_end_position(text);
            if line > 0 || col > 0 {
                actions.push(CodeAction {
                    title: "Add semicolon".to_string(),
                    kind: Some(lsp_types::CodeActionKind::QUICKFIX),
                    diagnostics: None,
                    edit: Some(WorkspaceEdit {
                        changes: Some(HashMap::from([(
                            "sql://internal".parse::<Uri>().unwrap(),
                            vec![TextEdit {
                                range: Range {
                                    start: Position { line, character: col },
                                    end: Position { line, character: col },
                                },
                                new_text: ";".to_string(),
                            }],
                        )])),
                        document_changes: None,
                        change_annotations: None,
                    }),
                    command: None,
                    is_preferred: Some(false),
                    disabled: None,
                    data: None,
                });
            }
        }

        actions
    }

    /// Convert LSP Position to byte offset in text
    fn offset_to_usize(&self, text: &str, position: &Position) -> usize {
        let mut offset = 0;
        for (line_idx, line) in text.lines().enumerate() {
            if line_idx == position.line as usize {
                return offset + position.character as usize;
            }
            offset += line.len() + 1; // +1 for newline
        }
        offset
    }

    /// Find where to insert a semicolon based on the diagnostic
    fn find_semicolon_insertion_point(&self, text: &str, range: Option<&Range>) -> Option<(u32, u32)> {
        if let Some(r) = range {
            // Insert at the end of the range
            return Some((r.end.line, r.end.character + 1));
        }
        // Fallback: find end of last statement
        Some(self.find_query_end_position(text))
    }

    /// Find the end position of the last complete SQL statement
    fn find_query_end_position(&self, text: &str) -> (u32, u32) {
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            return (0, 0);
        }

        let last_line = lines.len() - 1;
        let last_content = lines[last_line];

        (last_line as u32, last_content.len() as u32)
    }

    /// Find an unquoted identifier that might need quoting
    fn find_unquoted_identifier(&self, _text: &str, range: Option<&Range>) -> Option<Range> {
        range.map(|r| r.clone())
    }

    /// Extract identifier from text at the given range
    fn extract_identifier(&self, text: &str, range: &Range) -> String {
        let lines: Vec<&str> = text.lines().collect();
        if range.start.line as usize >= lines.len() {
            return String::new();
        }

        let line = lines[range.start.line as usize];
        let start = range.start.character as usize;
        let end = range.end.character as usize;

        if start >= line.len() || end > line.len() || start >= end {
            return String::new();
        }

        line[start..end].to_string()
    }

    fn create_column_hover(&self, col: &ColumnInfo, table_name: Option<&str>) -> Hover {
        let mut hover_text = format!("**Column: {}**\n\n", col.name);

        if let Some(table) = table_name {
            hover_text.push_str(&format!("Table: `{}`\n", table));
        }

        hover_text.push_str(&format!("Type: `{}`\n", col.data_type));

        let mut attributes = Vec::new();
        if col.is_primary_key {
            attributes.push("PRIMARY KEY");
        }
        if col.is_foreign_key {
            attributes.push("FOREIGN KEY");
        }
        if !col.nullable {
            attributes.push("NOT NULL");
        }

        if !attributes.is_empty() {
            hover_text.push_str(&format!("Attributes: {}\n", attributes.join(", ")));
        }

        if let Some(default) = &col.default_value {
            hover_text.push_str(&format!("Default: `{}`\n", default));
        }

        if let Some(comment) = &col.comment {
            hover_text.push_str(&format!("\n{}\n", comment));
        }

        Hover {
            contents: HoverContents::Scalar(MarkedString::String(hover_text)),
            range: None,
        }
    }

    fn create_keyword_hover(&self, keyword: &str, description: &str) -> Hover {
        let hover_text = format!("**{}**\n\n{}", keyword, description);
        Hover {
            contents: HoverContents::Scalar(MarkedString::String(hover_text)),
            range: None,
        }
    }

    /// Get signature help for SQL function calls
    ///
    /// Returns signature help when the cursor is inside a function call.
    /// Shows function parameters and highlights the current parameter based on cursor position.
    pub fn get_signature_help(&self, text: &Rope, offset: usize) -> Option<SignatureHelp> {
        tracing::debug!("get_signature_help: offset={}", offset);

        let sql = text.to_string();
        let before_cursor = &sql[..offset.min(sql.len())];

        let (func_name, active_param) = self.find_function_call_context(before_cursor)?;

        tracing::debug!("Found function call context: func={}, active_param={}", func_name, active_param);

        let info = self.dialect.dialect_info();
        let func_info = info.functions.iter().find(|f| {
            f.name.eq_ignore_ascii_case(&func_name)
        })?;

        if func_info.signatures.is_empty() {
            return None;
        }

        let signatures: Vec<SignatureInformation> = func_info
            .signatures
            .iter()
            .map(|sig| {
                let params: Vec<ParameterInformation> = sig
                    .parameters
                    .iter()
                    .map(|p| ParameterInformation {
                        label: ParameterLabel::Simple(format!("{}: {}", p.name, p.param_type)),
                        documentation: p.description.as_ref().map(|d| {
                            lsp_types::Documentation::String(d.to_string())
                        }),
                    })
                    .collect();

                SignatureInformation {
                    label: sig.signature.to_string(),
                    documentation: func_info.description.as_ref().map(|d| {
                        lsp_types::Documentation::String(d.to_string())
                    }),
                    parameters: Some(params),
                    active_parameter: None,
                }
            })
            .collect();

        let active_signature = if signatures.len() == 1 { Some(0) } else { None };

        Some(SignatureHelp {
            signatures,
            active_signature,
            active_parameter: Some(active_param as u32),
        })
    }

    /// Find function call context (function name and current parameter index) from text before cursor
    fn find_function_call_context(&self, text: &str) -> Option<(String, usize)> {
        let mut paren_depth = 0i32;
        let mut func_name = String::new();
        let mut param_count = 0usize;
        let mut _in_func = false;
        let mut chars = text.chars().rev().peekable();

        while let Some(c) = chars.next() {
            match c {
                ')' => {
                    paren_depth += 1;
                    if paren_depth > 1 {
                        return None;
                    }
                }
                '(' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        let _in_func = true;
                        while let Some(&next_c) = chars.peek() {
                            if next_c.is_alphanumeric() || next_c == '_' {
                                func_name.insert(0, next_c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if !func_name.is_empty() {
                            return Some((func_name, param_count));
                        }
                        return None;
                    }
                }
                ',' => {
                    if paren_depth == 1 {
                        param_count += 1;
                    }
                }
                _ => {}
            }
        }

        None
    }

    /// Validate SQL and return diagnostics with precise error positions
    ///
    /// Uses tree-sitter for accurate error node detection and sqlparser for
    /// syntax validation. Returns diagnostics with exact line/column positions.
    ///
    /// For non-SQL dialects (like Redis), validation is skipped and no errors
    /// are returned for valid dialect commands.
    pub fn validate_sql(&mut self, text: &Rope) -> Vec<Diagnostic> {
        // Get dialect config for the current connection
        let dialect_config = self.dialect.dialect_config();

        // Use SqlDiagnostics with dialect-aware analysis
        self.sql_diagnostics
            .analyze_with_dialect(text, Some(&self.schema_cache), dialect_config)
    }

    /// Resolve a table alias to the actual table name using the provided table references
    /// This uses the AST-extracted TableRef information for accurate alias resolution
    fn resolve_alias_from_context(&self, identifier: &str, available_tables: &[TableRef]) -> String {
        // Build alias map from available tables
        let alias_map = context_analyzer::build_alias_map(available_tables);
        
        // Try to resolve the identifier using the alias map
        if let Some(table_name) = alias_map.get(identifier) {
            tracing::debug!(
                "Resolved identifier '{}' to table '{}' using context",
                identifier,
                table_name
            );
            return table_name.clone();
        }
        
        // Fallback: return the identifier as-is (might be a table name that's not in the schema yet)
        tracing::debug!(
            "Could not resolve identifier '{}' from context, using as-is",
            identifier
        );
        identifier.to_string()
    }

    /// Resolve a table alias to the actual table name
    fn resolve_alias_to_table(&self, alias: &str, text: &Rope, _offset: usize) -> Option<String> {
        // First check if it's already a known table name
        if self.schema_cache.tables.contains_key(alias) {
            return Some(alias.to_string());
        }

        // Parse the SQL to find alias mappings
        let sql = text.to_string();
        let sql_lower = sql.to_lowercase();

        // Simple regex-based alias extraction from FROM and JOIN clauses
        // Pattern: "table_name AS alias" or "table_name alias"
        // Look for patterns like "FROM users u" or "FROM users AS u"

        // Split by common SQL keywords to find FROM/JOIN clauses
        let _patterns = [
            format!(" {} ", alias.to_lowercase()),
            format!(" as {} ", alias.to_lowercase()),
        ];

        for (table_name, _) in &self.schema_cache.tables {
            let table_lower = table_name.to_lowercase();

            // Check for "table_name alias" pattern
            let pattern1 = format!("{} {}", table_lower, alias.to_lowercase());
            let pattern2 = format!("{} as {}", table_lower, alias.to_lowercase());

            if sql_lower.contains(&pattern1) || sql_lower.contains(&pattern2) {
                tracing::debug!("Resolved alias '{}' to table '{}'", alias, table_name);
                return Some(table_name.clone());
            }
        }

        // If we can't resolve it, assume it might be the table name itself
        None
    }

    /// Apply fuzzy matching to completions and re-rank them
    fn apply_fuzzy_matching(
        &self,
        filter: &str,
        completions: Vec<CompletionItem>,
    ) -> Vec<CompletionItem> {
        // Use the fuzzy matcher to score each completion
        // filter = pattern (what user typed), item.label = candidate (what we're matching against)
        let mut scored_completions: Vec<(CompletionItem, i32)> = completions
            .into_iter()
            .filter_map(|item| {
                // Match against filter_text if available, otherwise use label
                let match_target = item.filter_text.as_ref().unwrap_or(&item.label);
                let match_result = self.fuzzy_matcher.fuzzy_match(filter, match_target);
                // Only include items that actually match (not MatchQuality::None)
                match_result.and_then(|result| {
                    if result.is_match() {
                        Some((item, result.score))
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Sort by score (descending - higher is better)
        scored_completions.sort_by(|a, b| b.1.cmp(&a.1));

        // Return just the items
        scored_completions
            .into_iter()
            .map(|(item, _)| item)
            .collect()
    }

    /// Get the word at a given byte offset
    fn get_word_at_offset(&self, text: &Rope, offset: usize) -> Option<String> {
        let pos = text.offset_to_position(offset);
        let line_idx = pos.line as usize;
        let char_in_line = pos.character as usize;

        let line = text.slice_line(line_idx);
        let line_str = line.to_string();
        let chars: Vec<char> = line_str.chars().collect();

        let mut start = char_in_line;
        let mut end = char_in_line;

        // Find word boundaries
        while start > 0
            && chars
                .get(start - 1)
                .map_or(false, |c| c.is_alphanumeric() || *c == '_')
        {
            start -= 1;
        }

        while end < chars.len()
            && chars
                .get(end)
                .map_or(false, |c| c.is_alphanumeric() || *c == '_')
        {
            end += 1;
        }

        if start < end && end <= chars.len() {
            Some(chars[start..end].iter().collect())
        } else {
            None
        }
    }

    /// Get the LSP Range (start and end Position) for the word at the given offset
    /// Returns None if no word is found at the offset
    /// This is used for text_edit in completions to replace only the typed word
    fn get_word_range_at_offset(&self, text: &Rope, offset: usize) -> Option<(Position, Position)> {
        let pos = text.offset_to_position(offset);
        let line_idx = pos.line as usize;
        let char_in_line = pos.character as usize;

        let line = text.slice_line(line_idx);
        let line_str = line.to_string();
        let chars: Vec<char> = line_str.chars().collect();

        let mut start = char_in_line;
        let end = char_in_line;

        // Find word start boundary (move backwards)
        while start > 0
            && chars
                .get(start - 1)
                .map_or(false, |c| c.is_alphanumeric() || *c == '_')
        {
            start -= 1;
        }

        // Return range from word start to current cursor position
        // The end is the current cursor, not the end of the word
        // This allows us to replace "aud" with "audit_log" when cursor is after "aud"
        Some((
            Position {
                line: line_idx as u32,
                character: start as u32,
            },
            Position {
                line: line_idx as u32,
                character: end as u32,
            },
        ))
    }

    /// Test helper to set schema cache directly
    #[cfg(test)]
    pub fn set_schema_cache(&mut self, cache: SchemaCache) {
        self.schema_cache = cache;
    }

    /// Get the current schema as DatabaseSchema format for use with SchemaMetadataProvider
    ///
    /// This exports the cached schema information in a format that can be used
    /// by the schema metadata overlay to show table/column information.
    pub fn get_schema_for_metadata(&self) -> DatabaseSchema {
        let tables: Vec<String> = self.schema_cache.tables.keys().cloned().collect();
        let views: Vec<String> = self.schema_cache.views.keys().cloned().collect();
        
        // Collect materialized views, functions, procedures, triggers
        let materialized_views: Vec<String> = vec![]; // SchemaCache doesn't track this separately
        let functions: Vec<String> = self.schema_cache.functions.keys().cloned().collect();
        let procedures: Vec<String> = self.schema_cache.procedures.keys().cloned().collect();
        let triggers: Vec<String> = self.schema_cache.triggers.keys().cloned().collect();
        
        // Convert table_infos to the expected format
        let table_infos: Vec<zqlz_core::TableInfo> = self.schema_cache
            .tables
            .values()
            .map(|t| zqlz_core::TableInfo {
                name: t.name.clone(),
                schema: t.schema.clone(),
                table_type: zqlz_core::TableType::Table,
                owner: None,
                row_count: t.row_count,
                size_bytes: None,
                comment: t.comment.clone(),
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            })
            .collect();
        
        // Convert table_indexes to zqlz_core::IndexInfo format
        let mut table_indexes: std::collections::HashMap<String, Vec<zqlz_core::IndexInfo>> = 
            std::collections::HashMap::new();
        for (table_name, index_info) in &self.schema_cache.indexes {
            // SchemaCache stores single IndexInfo per name, wrap in Vec
            let converted = vec![zqlz_core::IndexInfo {
                name: index_info.name.clone(),
                columns: index_info.columns.clone(),
                is_unique: index_info.is_unique,
                is_primary: false, // SchemaCache doesn't track this
                index_type: "btree".to_string(), // Default
                comment: None,
                ..Default::default()
            }];
            table_indexes.insert(table_name.clone(), converted);
        }

        DatabaseSchema {
            table_infos,
            objects_panel_data: None,
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            table_indexes,
            database_name: None,
            schema_name: None,
        }
    }
}

// Note: No Default impl since SqlLsp requires SchemaService parameter
