//! MongoDB driver implementation

use async_trait::async_trait;
use bson::{Bson, Document};
use futures::TryStreamExt;
use mongodb::{
    Client,
    options::{ClientOptions, FindOptions},
};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, CommentStyles, Connection, ConnectionConfig, ConnectionField,
    ConnectionFieldSchema, ConnectionScope, DataTypeCategory, DataTypeInfo, DatabaseDriver,
    DialectInfo, DocumentAggregateRequest, DocumentCellUpdateRequest, DocumentCollectionInfo,
    DocumentDatabaseInfo, DocumentDeleteOutcome, DocumentDeleteRequest, DocumentInferredField,
    DocumentQueryRequest, DocumentReplaceRequest, DocumentSaveRequest, DocumentSchemaSampleRequest,
    DocumentStore, DriverCapabilities, DriverCategory, DropTableOptions, DropTriggerOptions,
    DropViewOptions, ExplainConfig, ExplainParserKind, FunctionCategory, KeywordCategory,
    KeywordInfo, QueryResult, ResolvedConnectionScope, Result, Row, SqlFunctionInfo, SqlObjectName,
    StatementResult, Transaction, Value, ZqlzError,
};

/// MongoDB database driver
///
/// MongoDB is a document-oriented NoSQL database that stores data in
/// flexible, JSON-like BSON documents. This driver provides connectivity
/// and query execution capabilities for MongoDB.
pub struct MongoDbDriver;

impl MongoDbDriver {
    /// Create a new MongoDB driver instance
    pub fn new() -> Self {
        tracing::debug!("MongoDB driver initialized");
        Self
    }
}

impl Default for MongoDbDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for MongoDbDriver {
    fn id(&self) -> &'static str {
        "mongodb"
    }

    fn name(&self) -> &'static str {
        "mongodb"
    }

    fn display_name(&self) -> &'static str {
        "MongoDB"
    }

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn default_port(&self) -> Option<u16> {
        Some(27017)
    }

    fn icon_name(&self) -> &'static str {
        "mongodb"
    }

    fn dialect_info(&self) -> DialectInfo {
        mongodb_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,         // Multi-document transactions (4.0+)
            supports_savepoints: false,          // No savepoints in MongoDB
            supports_prepared_statements: false, // No prepared statements concept
            supports_multiple_statements: false, // Commands are individual
            supports_returning: false,           // No RETURNING clause
            supports_upsert: true,               // updateOne with upsert option
            supports_window_functions: true,     // $setWindowFields aggregation stage
            supports_cte: false,                 // No CTEs (use $lookup instead)
            supports_json: true,                 // Native BSON/JSON support
            supports_full_text_search: true,     // $text operator
            supports_stored_procedures: false,   // No stored procedures
            supports_schemas: false,             // Collections, not schemas
            supports_multiple_databases: true,   // Multiple databases per server
            supports_streaming: true,            // Change streams
            supports_cancellation: true,         // maxTimeMS option
            supports_explain: true,              // explain() method
            supports_foreign_keys: false,        // No foreign keys (use $lookup)
            supports_views: true,                // Views (read-only)
            supports_triggers: true,             // Change streams as triggers
            supports_ssl: true,                  // TLS supported
            max_identifier_length: Some(120),    // Collection name limit
            max_parameters: None,                // No parameter limit
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to MongoDB");

        let connection_string = self.build_connection_string(config);

        let client_options = ClientOptions::parse(&connection_string)
            .await
            .map_err(|e| ZqlzError::Driver(format!("Failed to parse MongoDB options: {}", e)))?;

        let client = Client::with_options(client_options)
            .map_err(|e| ZqlzError::Driver(format!("Failed to create MongoDB client: {}", e)))?;

        // Test the connection by listing databases
        client
            .list_database_names()
            .await
            .map_err(|e| ZqlzError::Driver(format!("Failed to connect to MongoDB: {}", e)))?;

        let database = config
            .get_string("database")
            .or_else(|| config.database.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "admin".to_string());

        Ok(Arc::new(MongoDbConnection::new(
            client,
            database,
            config.clone(),
        )))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing MongoDB connection");
        let conn = self.connect(config).await?;
        // ping command
        conn.execute("{ \"ping\": 1 }", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let scheme = match config
            .get_string("scheme")
            .unwrap_or_else(|| "mongodb".to_string())
            .as_str()
        {
            "mongodb+srv" => "mongodb+srv",
            _ => "mongodb",
        };
        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 27017 };
        let database = config
            .get_string("database")
            .or_else(|| config.database.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "admin".to_string());
        let username = config.username.clone().filter(|s| !s.is_empty());
        let password = config.password.clone().filter(|s| !s.is_empty());

        // Check for additional options
        let auth_source = config
            .get_string("authSource")
            .unwrap_or_else(|| "admin".to_string());
        let replica_set = config.get_string("replicaSet");
        let use_tls = config
            .get_string("tls")
            .or_else(|| config.get_string("ssl"))
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);
        let read_preference = config
            .get_string("readPreference")
            .filter(|value| !value.is_empty());
        let direct_connection = config
            .get_string("directConnection")
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);
        let server_selection_timeout = config
            .get_string("serverSelectionTimeoutMS")
            .filter(|value| !value.is_empty());
        let connect_timeout = config
            .get_string("connectTimeoutMS")
            .filter(|value| !value.is_empty());

        // Build connection string
        let mut conn_str = format!("{}://", scheme);

        // Add credentials if present
        if let (Some(user), Some(pass)) = (&username, &password) {
            conn_str.push_str(&urlencoding::encode(user));
            conn_str.push(':');
            conn_str.push_str(&urlencoding::encode(pass));
            conn_str.push('@');
        }

        // Add host and port
        conn_str.push_str(&host);
        if scheme != "mongodb+srv" {
            conn_str.push(':');
            conn_str.push_str(&port.to_string());
        }

        // Add database
        conn_str.push('/');
        conn_str.push_str(&database);

        // Add options
        let mut options = Vec::new();
        if username.is_some() {
            options.push(format!("authSource={}", urlencoding::encode(&auth_source)));
        }
        if let Some(rs) = replica_set {
            options.push(format!("replicaSet={}", urlencoding::encode(&rs)));
        }
        if use_tls {
            options.push("tls=true".to_string());
        }
        if let Some(value) = read_preference {
            options.push(format!("readPreference={}", urlencoding::encode(&value)));
        }
        if direct_connection {
            options.push("directConnection=true".to_string());
        }
        if let Some(value) = server_selection_timeout {
            options.push(format!(
                "serverSelectionTimeoutMS={}",
                urlencoding::encode(&value)
            ));
        }
        if let Some(value) = connect_timeout {
            options.push(format!("connectTimeoutMS={}", urlencoding::encode(&value)));
        }

        if !options.is_empty() {
            conn_str.push('?');
            conn_str.push_str(&options.join("&"));
        }

        conn_str
    }

    fn connection_string_help(&self) -> &'static str {
        "MongoDB URL format: mongodb://[user:password@]host[:port][/database][?options]\n\
         Examples:\n\
         - mongodb://localhost:27017/mydb\n\
         - mongodb://user:password@localhost:27017/mydb?authSource=admin\n\
         - mongodb://host:27017/mydb?replicaSet=rs0&tls=true"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        use zqlz_core::ConnectionFieldOption;

        ConnectionFieldSchema {
            title: Cow::Borrowed("MongoDB Connection"),
            fields: vec![
                ConnectionField::select(
                    "scheme",
                    "Scheme",
                    vec![
                        ConnectionFieldOption::new("mongodb", "mongodb"),
                        ConnectionFieldOption::new("mongodb+srv", "mongodb+srv"),
                    ],
                )
                .default_value("mongodb"),
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("27017")
                    .default_value("27017")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database")
                    .placeholder("admin")
                    .default_value("admin")
                    .required(),
                ConnectionField::text("username", "Username")
                    .placeholder("username")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::text("authSource", "Auth Source")
                    .placeholder("admin")
                    .default_value("admin")
                    .help_text("Database to authenticate against")
                    .width(0.5)
                    .row_group(3),
                ConnectionField::boolean("tls", "Use TLS/SSL")
                    .help_text("Enable secure connection")
                    .width(0.5)
                    .row_group(3),
                ConnectionField::text("replicaSet", "Replica Set")
                    .placeholder("rs0")
                    .tab("advanced"),
                ConnectionField::select(
                    "readPreference",
                    "Read Preference",
                    vec![
                        ConnectionFieldOption::new("", "Server Default"),
                        ConnectionFieldOption::new("primary", "primary"),
                        ConnectionFieldOption::new("primaryPreferred", "primaryPreferred"),
                        ConnectionFieldOption::new("secondary", "secondary"),
                        ConnectionFieldOption::new("secondaryPreferred", "secondaryPreferred"),
                        ConnectionFieldOption::new("nearest", "nearest"),
                    ],
                )
                .default_value("")
                .tab("advanced"),
                ConnectionField::boolean("directConnection", "Direct Connection")
                    .default_value("false")
                    .tab("advanced"),
                ConnectionField::number(
                    "serverSelectionTimeoutMS",
                    "Server Selection Timeout (ms)",
                )
                .tab("advanced"),
                ConnectionField::number("connectTimeoutMS", "Connect Timeout (ms)").tab("advanced"),
            ],
        }
    }
}

/// MongoDB connection wrapper implementing the Connection trait
pub struct MongoDbConnection {
    client: Client,
    database: String,
    #[allow(dead_code)]
    config: ConnectionConfig,
    closed: AtomicBool,
}

impl MongoDbConnection {
    /// Create a new MongoDB connection wrapper
    pub fn new(client: Client, database: String, config: ConnectionConfig) -> Self {
        Self {
            client,
            database,
            config,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the current database name
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the MongoDB client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the current database object
    pub fn db(&self) -> mongodb::Database {
        self.client.database(&self.database)
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }

    /// Parse a JSON/BSON query string into a Document
    fn parse_query(&self, query: &str) -> Result<Document> {
        let trimmed = query.trim();

        // Try parsing as JSON first
        if trimmed.starts_with('{') {
            serde_json::from_str::<Document>(trimmed)
                .map_err(|e| ZqlzError::Driver(format!("Invalid JSON document: {}", e)))
        } else {
            // Extended JSON format or other formats
            bson::from_slice(trimmed.as_bytes())
                .map_err(|e| ZqlzError::Driver(format!("Invalid BSON document: {}", e)))
        }
    }

    /// Convert a BSON value to our Value type
    fn bson_to_value(bson: &Bson) -> Value {
        match bson {
            Bson::Null => Value::Null,
            Bson::Boolean(b) => Value::Bool(*b),
            Bson::Int32(i) => Value::Int32(*i),
            Bson::Int64(i) => Value::Int64(*i),
            Bson::Double(d) => Value::Float64(*d),
            Bson::String(s) => Value::String(s.clone()),
            Bson::Array(arr) => {
                let values: Vec<Value> = arr.iter().map(Self::bson_to_value).collect();
                Value::Array(values)
            }
            Bson::Document(doc) => {
                Value::Json(serde_json::to_value(doc).unwrap_or(serde_json::Value::Null))
            }
            Bson::ObjectId(oid) => Value::String(oid.to_hex()),
            Bson::DateTime(dt) => Value::String(dt.to_string()),
            Bson::Binary(bin) => Value::Bytes(bin.bytes.clone()),
            Bson::Decimal128(d) => Value::Decimal(d.to_string()),
            Bson::Timestamp(ts) => Value::Int64(ts.time as i64),
            Bson::RegularExpression(re) => Value::String(format!("/{}/{}", re.pattern, re.options)),
            Bson::JavaScriptCode(code) => Value::String(code.clone()),
            Bson::JavaScriptCodeWithScope(code) => Value::String(code.code.clone()),
            Bson::Symbol(sym) => Value::String(sym.clone()),
            Bson::Undefined => Value::Null,
            Bson::MaxKey | Bson::MinKey => Value::Null,
            Bson::DbPointer(_) => Value::String("<DbPointer>".to_string()),
        }
    }

    /// Execute a command on the database
    async fn run_command(&self, command: Document) -> Result<Document> {
        self.db()
            .run_command(command)
            .await
            .map_err(|e| ZqlzError::Driver(format!("MongoDB command failed: {}", e)))
    }

    fn parse_document_json(json: Option<&str>) -> Result<Document> {
        match json.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => serde_json::from_str::<Document>(value).map_err(|error| {
                ZqlzError::Driver(format!("Invalid MongoDB document JSON: {}", error))
            }),
            None => Ok(Document::new()),
        }
    }

    fn parse_required_document_json(json: &str, context: &str) -> Result<Document> {
        serde_json::from_str::<Document>(json).map_err(|error| {
            ZqlzError::Driver(format!("Invalid MongoDB {} JSON: {}", context, error))
        })
    }

    fn parse_pipeline_json(json: &str) -> Result<Vec<Document>> {
        serde_json::from_str::<Vec<Document>>(json).map_err(|error| {
            ZqlzError::Driver(format!(
                "MongoDB aggregate pipeline must be a JSON array: {}",
                error
            ))
        })
    }

    fn parse_id_json(json: &str) -> Result<Bson> {
        if let Ok(object_id) = bson::oid::ObjectId::parse_str(json) {
            return Ok(Bson::ObjectId(object_id));
        }

        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json) {
            if let serde_json::Value::String(value) = &value
                && let Ok(object_id) = bson::oid::ObjectId::parse_str(value)
            {
                return Ok(Bson::ObjectId(object_id));
            }

            return bson::to_bson(&value).map_err(|error| {
                ZqlzError::Driver(format!("Invalid MongoDB _id BSON: {}", error))
            });
        }

        serde_json::from_str::<Bson>(json).or_else(|_| Ok(Bson::String(json.to_string())))
    }

    fn documents_to_query_result(documents: Vec<Document>, execution_time_ms: u64) -> QueryResult {
        let mut column_names = Vec::<String>::new();
        for document in &documents {
            for key in document.keys() {
                if !column_names.contains(key) {
                    column_names.push(key.clone());
                }
            }
        }

        let columns = column_names
            .iter()
            .enumerate()
            .map(|(ordinal, name)| ColumnMeta {
                name: name.clone(),
                data_type: "bson".to_string(),
                nullable: true,
                ordinal,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            })
            .collect();

        let rows = documents
            .into_iter()
            .map(|document| {
                let values = column_names
                    .iter()
                    .map(|name| {
                        document
                            .get(name)
                            .map(Self::bson_to_value)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                Row::new(column_names.clone(), values)
            })
            .collect::<Vec<_>>();

        QueryResult {
            id: Uuid::new_v4(),
            columns,
            total_rows: Some(rows.len() as u64),
            rows,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        }
    }
}

/// URL encoding helper (simple implementation)
pub(crate) mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::with_capacity(s.len() * 3);
        for c in s.chars() {
            match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
                _ => {
                    for b in c.to_string().as_bytes() {
                        result.push_str(&format!("%{:02X}", b));
                    }
                }
            }
        }
        result
    }
}

#[async_trait]
impl Connection for MongoDbConnection {
    fn driver_name(&self) -> &str {
        "mongodb"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mongodb")
    }

    fn driver_category(&self) -> DriverCategory {
        DriverCategory::Document
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = Some(self.database.clone());
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.effective_database = Some(database_name);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some(self.database.clone()))
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::None
    }

    fn rename_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _new_table_name: &str,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table rename operations".to_string(),
        ))
    }

    fn drop_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _options: DropTableOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP TABLE operations".to_string(),
        ))
    }

    fn drop_view_sql(
        &self,
        _view_name: &SqlObjectName,
        _options: DropViewOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP VIEW operations".to_string(),
        ))
    }

    fn drop_trigger_sql(
        &self,
        _trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        _options: DropTriggerOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP TRIGGER operations".to_string(),
        ))
    }

    fn truncate_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL TRUNCATE TABLE operations".to_string(),
        ))
    }

    fn duplicate_table_sql(
        &self,
        _source_table_name: &SqlObjectName,
        _new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table duplication operations".to_string(),
        ))
    }

    fn clear_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table clear operations".to_string(),
        ))
    }

    fn table_has_rows_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table row-existence queries".to_string(),
        ))
    }

    fn select_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL row-selection queries".to_string(),
        ))
    }

    fn select_distinct_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
        _order_by_columns: &[String],
        _limit: u64,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL distinct-row queries".to_string(),
        ))
    }

    fn insert_row_sql(
        &self,
        _table_name: &SqlObjectName,
        _column_names: &[String],
        _value_count: usize,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL INSERT-row statement generation".to_string(),
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL performance metrics queries".to_string(),
        ))
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;

        let start = Instant::now();
        let command = self.parse_query(sql)?;
        let result = self.run_command(command).await?;
        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Check for errors in the response
        if let Some(ok) = result.get("ok")
            && ok.as_f64().unwrap_or(0.0) != 1.0
        {
            let err_msg = result
                .get("errmsg")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown error");
            return Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 0,
                error: Some(format!("MongoDB error: {}", err_msg)),
            });
        }

        // Extract affected count if present
        let affected_rows = result.get("n").and_then(|n| n.as_i64()).unwrap_or(0) as u64;

        // For execute, we return a simple result without query data
        Ok(StatementResult {
            is_query: false,
            result: Some(QueryResult {
                id: Uuid::new_v4(),
                columns: Vec::new(),
                rows: Vec::new(),
                total_rows: Some(affected_rows),
                is_estimated_total: false,
                affected_rows,
                execution_time_ms,
                warnings: Vec::new(),
            }),
            affected_rows,
            error: None,
        })
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;

        let start = Instant::now();
        let command = self.parse_query(sql)?;
        let result = self.run_command(command).await?;
        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Check for errors
        if let Some(ok) = result.get("ok")
            && ok.as_f64().unwrap_or(0.0) != 1.0
        {
            let err_msg = result
                .get("errmsg")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown error");
            return Err(ZqlzError::Driver(format!("MongoDB error: {}", err_msg)));
        }

        // Convert result document to rows
        let mut rows = Vec::new();
        let mut columns = Vec::new();

        // Handle different result types
        if let Some(cursor) = result.get("cursor")
            && let Some(first_batch) = cursor.as_document().and_then(|d| d.get("firstBatch"))
            && let Some(arr) = first_batch.as_array()
        {
            // Cursor-based result (find, aggregate)
            for (i, doc) in arr.iter().enumerate() {
                if let Some(d) = doc.as_document() {
                    if i == 0 {
                        columns = d
                            .keys()
                            .enumerate()
                            .map(|(ordinal, k)| ColumnMeta {
                                name: k.clone(),
                                data_type: "bson".to_string(),
                                nullable: true,
                                ordinal,
                                max_length: None,
                                precision: None,
                                scale: None,
                                auto_increment: false,
                                default_value: None,
                                comment: None,
                                enum_values: None,
                            })
                            .collect();
                    }
                    let column_names: Vec<String> = d.keys().cloned().collect();
                    let values: Vec<Value> =
                        d.iter().map(|(_, v)| Self::bson_to_value(v)).collect();
                    rows.push(Row::new(column_names, values));
                }
            }
        } else {
            // Single document result
            columns = result
                .keys()
                .enumerate()
                .map(|(ordinal, k)| ColumnMeta {
                    name: k.clone(),
                    data_type: "bson".to_string(),
                    nullable: true,
                    ordinal,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: None,
                    enum_values: None,
                })
                .collect();
            let column_names: Vec<String> = result.keys().cloned().collect();
            let values: Vec<Value> = result.iter().map(|(_, v)| Self::bson_to_value(v)).collect();
            rows.push(Row::new(column_names, values));
        }

        let row_count = rows.len() as u64;

        Ok(QueryResult {
            id: Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(row_count),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Err(ZqlzError::NotImplemented(
            "MongoDB transactions require session management - use start_session() instead"
                .to_string(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn as_document_store(&self) -> Option<&dyn DocumentStore> {
        Some(self)
    }
}

#[async_trait]
impl DocumentStore for MongoDbConnection {
    async fn list_document_databases(&self) -> Result<Vec<DocumentDatabaseInfo>> {
        let databases = self.client.list_database_names().await.map_err(|error| {
            ZqlzError::Driver(format!("Failed to list MongoDB databases: {}", error))
        })?;

        Ok(databases
            .into_iter()
            .map(|name| DocumentDatabaseInfo {
                name,
                size_bytes: None,
                empty: false,
            })
            .collect())
    }

    async fn list_collections(&self, database: &str) -> Result<Vec<DocumentCollectionInfo>> {
        let database_handle = self.client.database(database);
        let names = database_handle
            .list_collection_names()
            .await
            .map_err(|error| {
                ZqlzError::Driver(format!("Failed to list MongoDB collections: {}", error))
            })?;

        let mut collections = Vec::with_capacity(names.len());
        for name in names {
            let stats = database_handle
                .run_command(bson::doc! { "collStats": &name })
                .await
                .ok();
            collections.push(DocumentCollectionInfo {
                database: database.to_string(),
                name,
                collection_type: "collection".to_string(),
                document_count: stats
                    .as_ref()
                    .and_then(|doc| doc.get_i64("count").ok())
                    .map(|value| value as u64),
                size_bytes: stats
                    .as_ref()
                    .and_then(|doc| doc.get_i64("size").ok())
                    .map(|value| value as u64),
                index_count: stats
                    .as_ref()
                    .and_then(|doc| doc.get_i32("nindexes").ok())
                    .map(|value| value as u32),
            });
        }

        Ok(collections)
    }

    async fn query_documents(&self, request: DocumentQueryRequest) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = Instant::now();
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let filter = Self::parse_document_json(request.filter_json.as_deref())?;
        let mut options = FindOptions::default();
        options.skip = Some(request.skip);
        options.limit = Some(request.limit.min(i64::MAX as u64) as i64);
        options.projection = Some(Self::parse_document_json(
            request.projection_json.as_deref(),
        )?)
        .filter(|document| !document.is_empty());
        options.sort = Some(Self::parse_document_json(request.sort_json.as_deref())?)
            .filter(|document| !document.is_empty());

        let cursor = collection
            .find(filter)
            .with_options(options)
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB find failed: {}", error)))?;
        let documents = cursor
            .try_collect::<Vec<Document>>()
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))?;
        Ok(Self::documents_to_query_result(
            documents,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn aggregate_documents(&self, request: DocumentAggregateRequest) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = Instant::now();
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let mut pipeline = Self::parse_pipeline_json(&request.pipeline_json)?;
        if request.limit > 0 {
            pipeline.push(bson::doc! { "$limit": request.limit.min(i64::MAX as u64) as i64 });
        }
        let cursor = collection
            .aggregate(pipeline)
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB aggregate failed: {}", error)))?;
        let documents = cursor
            .try_collect::<Vec<Document>>()
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))?;
        Ok(Self::documents_to_query_result(
            documents,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn insert_document(&self, request: DocumentSaveRequest) -> Result<Value> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let document = Self::parse_required_document_json(&request.document_json, "document")?;
        let result = collection
            .insert_one(document)
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB insert failed: {}", error)))?;
        Ok(Self::bson_to_value(&result.inserted_id))
    }

    async fn replace_document(&self, request: DocumentReplaceRequest) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let id = Self::parse_id_json(&request.id_json)?;
        let document = Self::parse_required_document_json(&request.document_json, "document")?;
        let result = collection
            .replace_one(bson::doc! { "_id": id }, document)
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB replace failed: {}", error)))?;
        if result.matched_count == 0 {
            return Err(ZqlzError::NotFound("MongoDB document _id".to_string()));
        }
        Ok(())
    }

    async fn update_document_cell(&self, request: DocumentCellUpdateRequest) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let id = Self::parse_id_json(&request.id_json)?;
        let value = serde_json::to_value(&request.new_value)
            .ok()
            .and_then(|value| bson::to_bson(&value).ok())
            .unwrap_or(Bson::Null);
        let result = collection
            .update_one(
                bson::doc! { "_id": id },
                bson::doc! { "$set": { request.field_path: value } },
            )
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB update failed: {}", error)))?;
        if result.matched_count == 0 {
            return Err(ZqlzError::NotFound("MongoDB document _id".to_string()));
        }
        Ok(())
    }

    async fn delete_documents(
        &self,
        request: DocumentDeleteRequest,
    ) -> Result<DocumentDeleteOutcome> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let mut outcome = DocumentDeleteOutcome::default();
        for id_json in request.ids_json {
            let id = match Self::parse_id_json(&id_json) {
                Ok(id) => id,
                Err(error) => {
                    outcome.errors.push(format!("{}: {}", id_json, error));
                    if !request.continue_on_error {
                        return Ok(outcome);
                    }
                    continue;
                }
            };
            match collection.delete_one(bson::doc! { "_id": id }).await {
                Ok(result) if result.deleted_count > 0 => outcome.deleted_ids.push(id_json),
                Ok(_) => outcome
                    .errors
                    .push(format!("{}: document not found", id_json)),
                Err(error) => outcome.errors.push(format!("{}: {}", id_json, error)),
            }
            if !outcome.errors.is_empty() && !request.continue_on_error {
                return Ok(outcome);
            }
        }
        Ok(outcome)
    }

    async fn sample_schema(
        &self,
        request: DocumentSchemaSampleRequest,
    ) -> Result<Vec<DocumentInferredField>> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let cursor = collection
            .aggregate(vec![bson::doc! {
                "$sample": { "size": request.sample_size.min(i32::MAX as u64) as i32 }
            }])
            .await
            .map_err(|error| {
                ZqlzError::Driver(format!("MongoDB schema sample failed: {}", error))
            })?;
        let documents = cursor
            .try_collect::<Vec<Document>>()
            .await
            .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))?;
        let total = documents.len() as u64;
        let mut fields = std::collections::BTreeMap::<String, (Vec<String>, u64)>::new();
        for document in documents {
            for (name, value) in document {
                let type_name = match value {
                    Bson::Double(_) => "double",
                    Bson::String(_) => "string",
                    Bson::Array(_) => "array",
                    Bson::Document(_) => "document",
                    Bson::Boolean(_) => "bool",
                    Bson::DateTime(_) => "date",
                    Bson::ObjectId(_) => "objectId",
                    Bson::Int32(_) => "int",
                    Bson::Int64(_) => "long",
                    Bson::Decimal128(_) => "decimal",
                    Bson::Null => "null",
                    _ => "bson",
                }
                .to_string();
                let entry = fields.entry(name).or_insert_with(|| (Vec::new(), 0));
                if !entry.0.contains(&type_name) {
                    entry.0.push(type_name);
                }
                entry.1 += 1;
            }
        }
        Ok(fields
            .into_iter()
            .map(|(name, (types, occurrence_count))| DocumentInferredField {
                name,
                types,
                occurrence_count,
                is_required: total > 0 && occurrence_count == total,
            })
            .collect())
    }
}

/// Create MongoDB dialect information
///
/// MongoDB uses a JSON/BSON-based query language rather than SQL.
/// This dialect info describes the MongoDB operations, aggregation stages,
/// and data types available.
pub fn mongodb_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("mongodb"),
        display_name: Cow::Borrowed("MongoDB Query Language"),

        // MongoDB operations as "keywords"
        keywords: vec![
            // Query operators
            keyword("$eq", KeywordCategory::Operator),
            keyword("$ne", KeywordCategory::Operator),
            keyword("$gt", KeywordCategory::Operator),
            keyword("$gte", KeywordCategory::Operator),
            keyword("$lt", KeywordCategory::Operator),
            keyword("$lte", KeywordCategory::Operator),
            keyword("$in", KeywordCategory::Operator),
            keyword("$nin", KeywordCategory::Operator),
            keyword("$and", KeywordCategory::Operator),
            keyword("$or", KeywordCategory::Operator),
            keyword("$not", KeywordCategory::Operator),
            keyword("$nor", KeywordCategory::Operator),
            keyword("$exists", KeywordCategory::Operator),
            keyword("$type", KeywordCategory::Operator),
            keyword("$regex", KeywordCategory::Operator),
            keyword("$text", KeywordCategory::Operator),
            keyword("$where", KeywordCategory::Operator),
            keyword("$all", KeywordCategory::Operator),
            keyword("$elemMatch", KeywordCategory::Operator),
            keyword("$size", KeywordCategory::Operator),
            // Update operators
            keyword("$set", KeywordCategory::Operator),
            keyword("$unset", KeywordCategory::Operator),
            keyword("$inc", KeywordCategory::Operator),
            keyword("$mul", KeywordCategory::Operator),
            keyword("$rename", KeywordCategory::Operator),
            keyword("$min", KeywordCategory::Operator),
            keyword("$max", KeywordCategory::Operator),
            keyword("$currentDate", KeywordCategory::Operator),
            keyword("$addToSet", KeywordCategory::Operator),
            keyword("$pop", KeywordCategory::Operator),
            keyword("$pull", KeywordCategory::Operator),
            keyword("$push", KeywordCategory::Operator),
            keyword("$each", KeywordCategory::Operator),
            keyword("$slice", KeywordCategory::Operator),
            keyword("$sort", KeywordCategory::Operator),
            keyword("$position", KeywordCategory::Operator),
            // Aggregation stages - use DatabaseSpecific since there's no Statement category
            keyword("$match", KeywordCategory::DatabaseSpecific),
            keyword("$project", KeywordCategory::DatabaseSpecific),
            keyword("$group", KeywordCategory::DatabaseSpecific),
            keyword("$limit", KeywordCategory::DatabaseSpecific),
            keyword("$skip", KeywordCategory::DatabaseSpecific),
            keyword("$unwind", KeywordCategory::DatabaseSpecific),
            keyword("$lookup", KeywordCategory::DatabaseSpecific),
            keyword("$graphLookup", KeywordCategory::DatabaseSpecific),
            keyword("$facet", KeywordCategory::DatabaseSpecific),
            keyword("$bucket", KeywordCategory::DatabaseSpecific),
            keyword("$bucketAuto", KeywordCategory::DatabaseSpecific),
            keyword("$addFields", KeywordCategory::DatabaseSpecific),
            keyword("$replaceRoot", KeywordCategory::DatabaseSpecific),
            keyword("$replaceWith", KeywordCategory::DatabaseSpecific),
            keyword("$merge", KeywordCategory::DatabaseSpecific),
            keyword("$out", KeywordCategory::DatabaseSpecific),
            keyword("$count", KeywordCategory::DatabaseSpecific),
            keyword("$sample", KeywordCategory::DatabaseSpecific),
            keyword("$redact", KeywordCategory::DatabaseSpecific),
            keyword("$geoNear", KeywordCategory::DatabaseSpecific),
            keyword("$setWindowFields", KeywordCategory::DatabaseSpecific),
            keyword("$densify", KeywordCategory::DatabaseSpecific),
            keyword("$fill", KeywordCategory::DatabaseSpecific),
            // Commands - use Dql for query commands, Dml for modification commands
            keyword("find", KeywordCategory::Dql),
            keyword("findOne", KeywordCategory::Dql),
            keyword("insert", KeywordCategory::Dml),
            keyword("insertOne", KeywordCategory::Dml),
            keyword("insertMany", KeywordCategory::Dml),
            keyword("update", KeywordCategory::Dml),
            keyword("updateOne", KeywordCategory::Dml),
            keyword("updateMany", KeywordCategory::Dml),
            keyword("delete", KeywordCategory::Dml),
            keyword("deleteOne", KeywordCategory::Dml),
            keyword("deleteMany", KeywordCategory::Dml),
            keyword("aggregate", KeywordCategory::Dql),
            keyword("count", KeywordCategory::Dql),
            keyword("distinct", KeywordCategory::Dql),
            keyword("createIndex", KeywordCategory::Ddl),
            keyword("dropIndex", KeywordCategory::Ddl),
            keyword("createCollection", KeywordCategory::Ddl),
            keyword("drop", KeywordCategory::Ddl),
            keyword("ping", KeywordCategory::DatabaseSpecific),
            keyword("listCollections", KeywordCategory::DatabaseSpecific),
            keyword("listDatabases", KeywordCategory::DatabaseSpecific),
            keyword("listIndexes", KeywordCategory::DatabaseSpecific),
        ],

        // MongoDB aggregation functions
        functions: vec![
            // Accumulator expressions
            function("$sum", FunctionCategory::Aggregate, "Sum of numeric values"),
            function(
                "$avg",
                FunctionCategory::Aggregate,
                "Average of numeric values",
            ),
            function("$min", FunctionCategory::Aggregate, "Minimum value"),
            function("$max", FunctionCategory::Aggregate, "Maximum value"),
            function(
                "$first",
                FunctionCategory::Aggregate,
                "First value in group",
            ),
            function("$last", FunctionCategory::Aggregate, "Last value in group"),
            function("$push", FunctionCategory::Aggregate, "Array of values"),
            function(
                "$addToSet",
                FunctionCategory::Aggregate,
                "Array of unique values",
            ),
            function(
                "$stdDevPop",
                FunctionCategory::Aggregate,
                "Population standard deviation",
            ),
            function(
                "$stdDevSamp",
                FunctionCategory::Aggregate,
                "Sample standard deviation",
            ),
            // String functions
            function("$concat", FunctionCategory::String, "Concatenate strings"),
            function("$substr", FunctionCategory::String, "Substring extraction"),
            function("$toLower", FunctionCategory::String, "Convert to lowercase"),
            function("$toUpper", FunctionCategory::String, "Convert to uppercase"),
            function("$trim", FunctionCategory::String, "Trim whitespace"),
            function("$split", FunctionCategory::String, "Split string to array"),
            function(
                "$strLenCP",
                FunctionCategory::String,
                "String length in code points",
            ),
            function(
                "$regexMatch",
                FunctionCategory::String,
                "Regex pattern match",
            ),
            function("$regexFind", FunctionCategory::String, "Find regex match"),
            function(
                "$regexFindAll",
                FunctionCategory::String,
                "Find all regex matches",
            ),
            // Date functions
            function(
                "$dateToString",
                FunctionCategory::DateTime,
                "Format date as string",
            ),
            function(
                "$dateFromString",
                FunctionCategory::DateTime,
                "Parse string to date",
            ),
            function(
                "$dayOfMonth",
                FunctionCategory::DateTime,
                "Day of month (1-31)",
            ),
            function(
                "$dayOfWeek",
                FunctionCategory::DateTime,
                "Day of week (1-7)",
            ),
            function(
                "$dayOfYear",
                FunctionCategory::DateTime,
                "Day of year (1-366)",
            ),
            function("$month", FunctionCategory::DateTime, "Month (1-12)"),
            function("$year", FunctionCategory::DateTime, "Year"),
            function("$hour", FunctionCategory::DateTime, "Hour (0-23)"),
            function("$minute", FunctionCategory::DateTime, "Minute (0-59)"),
            function("$second", FunctionCategory::DateTime, "Second (0-59)"),
            function(
                "$dateDiff",
                FunctionCategory::DateTime,
                "Difference between dates",
            ),
            function("$dateAdd", FunctionCategory::DateTime, "Add to date"),
            function(
                "$dateSubtract",
                FunctionCategory::DateTime,
                "Subtract from date",
            ),
            // Math functions - use Numeric category
            function("$abs", FunctionCategory::Numeric, "Absolute value"),
            function("$ceil", FunctionCategory::Numeric, "Ceiling"),
            function("$floor", FunctionCategory::Numeric, "Floor"),
            function("$round", FunctionCategory::Numeric, "Round"),
            function("$sqrt", FunctionCategory::Numeric, "Square root"),
            function("$pow", FunctionCategory::Numeric, "Power"),
            function("$log", FunctionCategory::Numeric, "Logarithm"),
            function("$log10", FunctionCategory::Numeric, "Base 10 logarithm"),
            function("$exp", FunctionCategory::Numeric, "Exponential"),
            function("$mod", FunctionCategory::Numeric, "Modulo"),
            function("$add", FunctionCategory::Numeric, "Addition"),
            function("$subtract", FunctionCategory::Numeric, "Subtraction"),
            function("$multiply", FunctionCategory::Numeric, "Multiplication"),
            function("$divide", FunctionCategory::Numeric, "Division"),
            // Array functions - use Array category
            function("$arrayElemAt", FunctionCategory::Array, "Element at index"),
            function(
                "$concatArrays",
                FunctionCategory::Array,
                "Concatenate arrays",
            ),
            function("$filter", FunctionCategory::Array, "Filter array elements"),
            function("$map", FunctionCategory::Array, "Map over array"),
            function("$reduce", FunctionCategory::Array, "Reduce array to value"),
            function("$reverseArray", FunctionCategory::Array, "Reverse array"),
            function("$size", FunctionCategory::Array, "Array size"),
            function("$slice", FunctionCategory::Array, "Array slice"),
            function("$zip", FunctionCategory::Array, "Zip arrays together"),
            function("$in", FunctionCategory::Array, "Element in array"),
            function("$isArray", FunctionCategory::Array, "Check if array"),
            // Type conversion
            function("$toInt", FunctionCategory::Conversion, "Convert to integer"),
            function("$toLong", FunctionCategory::Conversion, "Convert to long"),
            function(
                "$toDouble",
                FunctionCategory::Conversion,
                "Convert to double",
            ),
            function(
                "$toDecimal",
                FunctionCategory::Conversion,
                "Convert to decimal",
            ),
            function(
                "$toString",
                FunctionCategory::Conversion,
                "Convert to string",
            ),
            function(
                "$toObjectId",
                FunctionCategory::Conversion,
                "Convert to ObjectId",
            ),
            function("$toDate", FunctionCategory::Conversion, "Convert to date"),
            function(
                "$toBool",
                FunctionCategory::Conversion,
                "Convert to boolean",
            ),
            function("$type", FunctionCategory::Conversion, "Get BSON type"),
            // Conditional
            function(
                "$cond",
                FunctionCategory::Conditional,
                "Conditional expression",
            ),
            function("$ifNull", FunctionCategory::Conditional, "Null coalesce"),
            function(
                "$switch",
                FunctionCategory::Conditional,
                "Switch/case expression",
            ),
        ],

        // MongoDB data types
        data_types: vec![
            dtype("Double", DataTypeCategory::Float, "64-bit floating point"),
            dtype("String", DataTypeCategory::String, "UTF-8 string"),
            dtype("Object", DataTypeCategory::Other, "Embedded document"),
            dtype("Array", DataTypeCategory::Array, "Array of values"),
            dtype("BinData", DataTypeCategory::Binary, "Binary data"),
            dtype(
                "ObjectId",
                DataTypeCategory::Other,
                "12-byte unique identifier",
            ),
            dtype("Boolean", DataTypeCategory::Boolean, "True or false"),
            dtype("Date", DataTypeCategory::DateTime, "UTC datetime"),
            dtype("Null", DataTypeCategory::Other, "Null value"),
            dtype("Regex", DataTypeCategory::Other, "Regular expression"),
            dtype("JavaScript", DataTypeCategory::Other, "JavaScript code"),
            dtype("Int32", DataTypeCategory::Integer, "32-bit integer"),
            dtype(
                "Timestamp",
                DataTypeCategory::DateTime,
                "Internal timestamp",
            ),
            dtype("Int64", DataTypeCategory::Integer, "64-bit integer"),
            dtype("Decimal128", DataTypeCategory::Decimal, "128-bit decimal"),
            dtype("MinKey", DataTypeCategory::Other, "Minimum BSON value"),
            dtype("MaxKey", DataTypeCategory::Other, "Maximum BSON value"),
        ],

        table_options: Vec::new(),
        auto_increment: None, // MongoDB uses ObjectId for unique IDs

        identifier_quote: '"',
        string_quote: '"', // JSON uses double quotes for strings
        case_sensitive_identifiers: true,
        statement_terminator: ';',

        comment_styles: CommentStyles {
            line_comment: Some(Cow::Borrowed("//")),
            block_comment_start: Some(Cow::Borrowed("/*")),
            block_comment_end: Some(Cow::Borrowed("*/")),
        },

        // MongoDB uses .explain() method on queries, not SQL EXPLAIN syntax
        explain_config: ExplainConfig {
            explain_format: Cow::Borrowed("{\"explain\": {sql}}"),
            query_plan_format: None,
            analyze_format: None,
            explain_description: Cow::Borrowed(
                "Use .explain() method on cursor or add explain:true to command",
            ),
            query_plan_description: None,
            analyze_is_safe: true,
        },
    }
}

/// Helper to create a keyword
fn keyword(name: &'static str, category: KeywordCategory) -> KeywordInfo {
    KeywordInfo::new(name, category)
}

/// Helper to create a function
fn function(
    name: &'static str,
    category: FunctionCategory,
    description: &'static str,
) -> SqlFunctionInfo {
    SqlFunctionInfo {
        name: Cow::Borrowed(name),
        category,
        description: Some(Cow::Borrowed(description)),
        signatures: Vec::new(),
        return_type: None,
    }
}

/// Helper to create a data type
fn dtype(
    name: &'static str,
    category: DataTypeCategory,
    description: &'static str,
) -> DataTypeInfo {
    DataTypeInfo {
        name: Cow::Borrowed(name),
        aliases: Vec::new(),
        category,
        accepts_length: false,
        accepts_scale: false,
        default_length: None,
        max_length: None,
        description: Some(Cow::Borrowed(description)),
        example: None,
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    use zqlz_core::DatabaseDriver;

    #[test]
    fn mongodb_schema_exposes_common_options() {
        let schema = MongoDbDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert_eq!(field("scheme").default_value.as_deref(), Some("mongodb"));
        assert_eq!(field("replicaSet").tab.as_deref(), Some("advanced"));
        assert_eq!(field("readPreference").default_value.as_deref(), Some(""));
        assert_eq!(
            field("directConnection").default_value.as_deref(),
            Some("false")
        );
        assert_eq!(
            field("serverSelectionTimeoutMS").tab.as_deref(),
            Some("advanced")
        );
        assert_eq!(field("connectTimeoutMS").tab.as_deref(), Some("advanced"));
    }

    #[test]
    fn mongodb_connection_string_supports_srv_and_options() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "test");
        config.host = "cluster.example.com".to_string();
        config.database = Some("app".to_string());
        config
            .params
            .insert("scheme".to_string(), "mongodb+srv".to_string());
        config
            .params
            .insert("replicaSet".to_string(), "rs0".to_string());
        config.params.insert(
            "readPreference".to_string(),
            "secondaryPreferred".to_string(),
        );
        config
            .params
            .insert("directConnection".to_string(), "true".to_string());
        config
            .params
            .insert("serverSelectionTimeoutMS".to_string(), "5000".to_string());
        config
            .params
            .insert("connectTimeoutMS".to_string(), "3000".to_string());

        let connection_string = driver.build_connection_string(&config);

        assert!(connection_string.starts_with("mongodb+srv://cluster.example.com/app?"));
        assert!(!connection_string.contains(":27017"));
        assert!(connection_string.contains("replicaSet=rs0"));
        assert!(connection_string.contains("readPreference=secondaryPreferred"));
        assert!(connection_string.contains("directConnection=true"));
        assert!(connection_string.contains("serverSelectionTimeoutMS=5000"));
        assert!(connection_string.contains("connectTimeoutMS=3000"));
    }

    #[test]
    fn parse_id_json_recovers_object_id_from_viewer_hex_string() {
        let id = "507f1f77bcf86cd799439011";

        assert!(matches!(
            MongoDbConnection::parse_id_json(id),
            Ok(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
        assert!(matches!(
            MongoDbConnection::parse_id_json("\"507f1f77bcf86cd799439011\""),
            Ok(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
    }
}
