//! MongoDB driver implementation

use async_trait::async_trait;
use bson::{Bson, Document, oid::ObjectId};
use futures::TryStreamExt;
use mongodb::{
    Client,
    error::{Error as MongoError, ErrorKind, WriteFailure},
    options::{ClientOptions, FindOptions},
    results::{CollectionSpecification, CollectionType},
};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::runtime::Runtime;
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, CommentStyles, Connection, ConnectionConfig, ConnectionField,
    ConnectionFieldSchema, ConnectionScope, ConstraintInfo, DataTypeCategory, DataTypeInfo,
    DatabaseDriver, DatabaseInfo, DatabaseObject, Dependency, DialectBundle, DialectInfo,
    DocumentAdminObjectInfo, DocumentAggregateRequest, DocumentCellUpdateRequest,
    DocumentCollectionInfo, DocumentDatabaseInfo, DocumentDatabaseObjects, DocumentDeleteOutcome,
    DocumentDeleteRequest, DocumentFunctionInfo, DocumentGridFsBucketInfo, DocumentIndexInfo,
    DocumentInferredField, DocumentQueryRequest, DocumentReplaceRequest, DocumentSaveRequest,
    DocumentSchemaSampleRequest, DocumentStore, DriverCapabilities, DriverCategory,
    DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig, ExplainParserKind,
    ForeignKeyInfo, FunctionCategory, FunctionInfo, IndexInfo, KeywordCategory, KeywordInfo,
    ObjectFormDdlRequest, ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection,
    ObjectFormSpec, ObjectFormSpecRequest, ObjectFormValue, ObjectsPanelData, ObjectsPanelManifest,
    PrimaryKeyInfo, ProcedureInfo, QueryResult, ResolvedConnectionScope, Result, Row, SchemaInfo,
    SchemaIntrospection, SequenceInfo, SqlFunctionInfo, SqlObjectName, StatementResult,
    TableDetails, TableInfo, Transaction, TriggerInfo, TypeInfo, Value, ViewInfo, ZqlzError,
    dialect_bundle_from_legacy_info,
};

use crate::objects_panel;

/// MongoDB database driver
///
/// MongoDB is a document-oriented NoSQL database that stores data in
/// flexible, JSON-like BSON documents. This driver provides connectivity
/// and query execution capabilities for MongoDB.
pub struct MongoDbDriver;

const CONFIG_TOML: &str = include_str!("../dialect/config.toml");

fn get_dialect_bundle() -> &'static DialectBundle {
    static BUNDLE: OnceLock<DialectBundle> = OnceLock::new();
    BUNDLE
        .get_or_init(|| dialect_bundle_from_legacy_info(CONFIG_TOML, &mongodb_dialect(), "MongoDB"))
}

fn mongodb_runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();

    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("zqlz-mongodb")
        .enable_all()
        .build()
        .map_err(|error| ZqlzError::Driver(format!("Failed to start MongoDB runtime: {error}")))?;

    Ok(RUNTIME.get_or_init(|| runtime))
}

async fn run_mongodb_future<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    mongodb_runtime()?
        .spawn(future)
        .await
        .map_err(|error| ZqlzError::Driver(format!("MongoDB task failed: {error}")))?
}

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

    fn dialect_bundle(&self) -> Option<&'static DialectBundle> {
        Some(get_dialect_bundle())
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

        let client = run_mongodb_future(async move {
            let client_options = ClientOptions::parse(&connection_string)
                .await
                .map_err(|e| {
                    ZqlzError::Driver(format!("Failed to parse MongoDB options: {}", e))
                })?;

            let client = Client::with_options(client_options).map_err(|e| {
                ZqlzError::Driver(format!("Failed to create MongoDB client: {}", e))
            })?;

            client
                .list_database_names()
                .await
                .map_err(|e| ZqlzError::Driver(format!("Failed to connect to MongoDB: {}", e)))?;

            Ok::<Client, ZqlzError>(client)
        })
        .await?;

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
/// Liveness command issued by connection heartbeats.
const MONGODB_PING_COMMAND: &str = r#"{"ping": 1}"#;

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

        if let Some(command) = Self::parse_shell_query(trimmed)? {
            return Ok(command);
        }

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

    fn parse_shell_query(query: &str) -> Result<Option<Document>> {
        let Some((database, after_db)) = Self::parse_shell_database_target(query)? else {
            return Ok(None);
        };
        let Some((collection, after_collection)) = Self::parse_shell_collection_target(after_db)?
        else {
            return Ok(None);
        };
        if collection.is_empty() {
            return Ok(None);
        }

        if let Some(after_method) = after_collection.strip_prefix("find(") {
            let (arguments, tail) = Self::take_parenthesized_arguments(after_method)?;
            let arguments = Self::split_shell_arguments(&arguments)?;
            let mut command = bson::doc! {
                "find": collection,
                "filter": Self::parse_shell_json_argument(arguments.first(), "find filter")?,
            };
            if let Some(database) = database.as_deref() {
                command.insert("$db", database);
            }

            if let Some(projection) = arguments.get(1) {
                command.insert(
                    "projection",
                    Self::parse_shell_json_document(projection, "find projection")?,
                );
            }

            let mut explain_verbosity = None;
            let mut tail = tail.trim();
            while !tail.is_empty() {
                if let Some(after_sort) = tail.strip_prefix(".sort(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_sort)?;
                    command.insert(
                        "sort",
                        Self::parse_shell_json_document(&argument, "sort specification")?,
                    );
                    tail = next_tail.trim();
                } else if let Some(after_limit) = tail.strip_prefix(".limit(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_limit)?;
                    let limit = argument.trim().parse::<i64>().map_err(|error| {
                        ZqlzError::Driver(format!("Invalid MongoDB limit value: {error}"))
                    })?;
                    command.insert("limit", limit);
                    tail = next_tail.trim();
                } else if let Some(after_explain) = tail.strip_prefix(".explain(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_explain)?;
                    explain_verbosity = Some(Self::parse_shell_explain_verbosity(argument.trim())?);
                    tail = next_tail.trim();
                } else if tail == ";" {
                    break;
                } else {
                    return Err(ZqlzError::Driver(format!(
                        "Unsupported MongoDB cursor method: {tail}"
                    )));
                }
            }

            if let Some(verbosity) = explain_verbosity {
                command = Self::explain_command(command, verbosity);
            }

            return Ok(Some(command));
        }

        if let Some(after_method) = after_collection.strip_prefix("aggregate(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let mut explain_verbosity = None;
            let tail = if let Some(after_explain) = tail.trim().strip_prefix(".explain(") {
                let (argument, tail) = Self::take_parenthesized_arguments(after_explain)?;
                explain_verbosity = Some(Self::parse_shell_explain_verbosity(argument.trim())?);
                tail.trim()
            } else {
                tail.trim()
            };
            if !tail.is_empty() && tail != ";" {
                return Err(ZqlzError::Driver(format!(
                    "Unsupported MongoDB aggregate suffix: {tail}"
                )));
            }
            let pipeline = Self::parse_shell_json_array(&argument, "aggregate pipeline")?;
            let mut command = bson::doc! {
                "aggregate": collection,
                "pipeline": pipeline,
                "cursor": {},
            };
            if let Some(database) = database.as_deref() {
                command.insert("$db", database);
            }
            if let Some(verbosity) = explain_verbosity {
                command = Self::explain_command(command, verbosity);
            }
            return Ok(Some(command));
        }

        Ok(None)
    }

    fn explain_command(mut command: Document, verbosity: &'static str) -> Document {
        let database = command.remove("$db");
        let mut explain_command = bson::doc! {
            "explain": command,
            "verbosity": verbosity,
        };
        if let Some(database) = database {
            explain_command.insert("$db", database);
        }
        explain_command
    }

    fn parse_shell_explain_verbosity(argument: &str) -> Result<&'static str> {
        if argument.is_empty() {
            return Ok("queryPlanner");
        }

        let value = Self::parse_shell_json_value(argument, "explain verbosity")?;
        match value.as_str() {
            Some("queryPlanner") => Ok("queryPlanner"),
            Some("executionStats") => Ok("executionStats"),
            Some("allPlansExecution") => Ok("allPlansExecution"),
            Some(other) => Err(ZqlzError::Driver(format!(
                "Unsupported MongoDB explain verbosity: {other}"
            ))),
            None => Err(ZqlzError::Driver(
                "MongoDB explain verbosity must be a string".to_string(),
            )),
        }
    }

    fn parse_shell_database_target(input: &str) -> Result<Option<(Option<String>, &str)>> {
        let Some(after_db) = input.strip_prefix("db") else {
            return Ok(None);
        };

        if let Some(after_method) = after_db.strip_prefix(".getSiblingDB(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let tail = tail.strip_prefix('.').ok_or_else(|| {
                ZqlzError::Driver("MongoDB getSiblingDB target must be followed by a method".into())
            })?;
            let database =
                serde_json_lenient::from_str::<String>(argument.trim()).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB database name: {error}"))
                })?;
            return Ok(Some((Some(database), tail)));
        }

        if let Some(after_dot) = after_db.strip_prefix('.') {
            return Ok(Some((None, after_dot)));
        }

        Ok(None)
    }

    fn parse_shell_collection_target(input: &str) -> Result<Option<(String, &str)>> {
        if let Some(after_method) = input.strip_prefix("getCollection(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let tail = tail.strip_prefix('.').ok_or_else(|| {
                ZqlzError::Driver(
                    "MongoDB getCollection target must be followed by a method".into(),
                )
            })?;
            let collection =
                serde_json_lenient::from_str::<String>(argument.trim()).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB collection name: {error}"))
                })?;
            return Ok(Some((collection, tail)));
        }

        Ok(input
            .split_once('.')
            .map(|(collection, tail)| (collection.to_string(), tail)))
    }

    fn take_parenthesized_arguments(input: &str) -> Result<(String, &str)> {
        let mut depth = 1usize;
        let mut in_string: Option<char> = None;
        let mut escaped = false;

        for (index, character) in input.char_indices() {
            if let Some(quote) = in_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => in_string = Some(character),
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Ok((input[..index].to_string(), &input[index + 1..]));
                    }
                }
                _ => {}
            }
        }

        Err(ZqlzError::Driver(
            "Unclosed MongoDB shell method call".to_string(),
        ))
    }

    fn split_shell_arguments(arguments: &str) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        let mut start = 0usize;
        let mut depth = 0usize;
        let mut in_string: Option<char> = None;
        let mut escaped = false;

        for (index, character) in arguments.char_indices() {
            if let Some(quote) = in_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => in_string = Some(character),
                '{' | '[' | '(' => depth = depth.saturating_add(1),
                '}' | ']' | ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    let part = arguments[start..index].trim();
                    if !part.is_empty() {
                        parts.push(part.to_string());
                    }
                    start = index + 1;
                }
                _ => {}
            }
        }

        let part = arguments[start..].trim();
        if !part.is_empty() {
            parts.push(part.to_string());
        }

        Ok(parts)
    }

    fn parse_shell_json_argument(argument: Option<&String>, context: &str) -> Result<Document> {
        match argument {
            Some(argument) => Self::parse_shell_json_document(argument, context),
            None => Ok(Document::new()),
        }
    }

    fn parse_shell_json_document(argument: &str, context: &str) -> Result<Document> {
        let value = Self::parse_shell_json_value(argument, context)?;
        bson::to_document(&value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}")))
    }

    fn parse_shell_json_array(argument: &str, context: &str) -> Result<Vec<Bson>> {
        let value = Self::parse_shell_json_value(argument, context)?;
        let array = value
            .as_array()
            .ok_or_else(|| ZqlzError::Driver(format!("MongoDB {context} must be a JSON array")))?;
        array
            .iter()
            .map(|value| {
                bson::to_bson(value).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}"))
                })
            })
            .collect()
    }

    fn parse_shell_json_value(argument: &str, context: &str) -> Result<serde_json::Value> {
        let normalized = Self::quote_unquoted_json_keys(argument);
        serde_json_lenient::from_str(&normalized)
            .map_err(|error| ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}")))
    }

    fn quote_unquoted_json_keys(input: &str) -> String {
        let mut output = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();
        let mut in_string: Option<char> = None;
        let mut escaped = false;
        let mut expecting_key = false;

        while let Some(character) = chars.next() {
            if let Some(quote) = in_string {
                output.push(character);
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => {
                    in_string = Some(character);
                    output.push(character);
                    expecting_key = false;
                }
                '{' | ',' => {
                    output.push(character);
                    expecting_key = true;
                }
                c if expecting_key && c.is_whitespace() => output.push(c),
                c if expecting_key && Self::is_shell_key_start(c) => {
                    let mut key = String::new();
                    key.push(c);
                    while let Some(next) = chars.peek().copied() {
                        if Self::is_shell_key_continue(next) {
                            key.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if chars.peek().is_some_and(|next| *next == ':') {
                        output.push('"');
                        output.push_str(&key);
                        output.push('"');
                        expecting_key = false;
                    } else {
                        output.push_str(&key);
                    }
                }
                ':' => {
                    output.push(character);
                    expecting_key = false;
                }
                _ => {
                    output.push(character);
                    expecting_key = false;
                }
            }
        }

        output
    }

    fn is_shell_key_start(character: char) -> bool {
        character == '$' || character == '_' || character.is_ascii_alphabetic()
    }

    fn is_shell_key_continue(character: char) -> bool {
        character == '$'
            || character == '_'
            || character == '.'
            || character == '-'
            || character.is_ascii_alphanumeric()
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

    fn bson_data_type(bson: &Bson) -> &'static str {
        match bson {
            Bson::Null | Bson::Undefined => "null",
            Bson::Boolean(_) => "bool",
            Bson::Int32(_) => "int",
            Bson::Int64(_) => "long",
            Bson::Double(_) => "double",
            Bson::String(_) | Bson::Symbol(_) => "string",
            Bson::Array(_) | Bson::Document(_) => "json",
            Bson::ObjectId(_) => "objectId",
            Bson::DateTime(_) => "date",
            Bson::Binary(_) => "binary",
            Bson::Decimal128(_) => "decimal",
            Bson::Timestamp(_) => "timestamp",
            Bson::RegularExpression(_) => "regex",
            Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
            Bson::MaxKey => "maxKey",
            Bson::MinKey => "minKey",
            Bson::DbPointer(_) => "dbPointer",
        }
    }

    fn inferred_bson_column_type(documents: &[Document], column_name: &str) -> String {
        let mut inferred_type: Option<&'static str> = None;
        for document in documents {
            let Some(value) = document.get(column_name) else {
                continue;
            };
            let data_type = Self::bson_data_type(value);
            if data_type == "null" {
                continue;
            }
            match inferred_type {
                None => inferred_type = Some(data_type),
                Some(existing) if existing == data_type => {}
                Some("int") if data_type == "long" => inferred_type = Some("long"),
                Some("long") if data_type == "int" => {}
                Some("double" | "decimal") if matches!(data_type, "int" | "long") => {}
                Some("int" | "long") if matches!(data_type, "double" | "decimal") => {
                    inferred_type = Some(data_type)
                }
                Some("json") | Some("bson") => inferred_type = Some("bson"),
                Some(_) => {
                    inferred_type = Some("bson");
                    break;
                }
            }
        }

        inferred_type.unwrap_or("bson").to_string()
    }

    fn document_to_json(document: &Document) -> serde_json::Value {
        serde_json::to_value(document).unwrap_or(serde_json::Value::Null)
    }

    fn bson_to_json(value: &Bson) -> serde_json::Value {
        serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
    }

    fn collection_options_json(collection_info: &CollectionSpecification) -> serde_json::Value {
        serde_json::to_value(&collection_info.options).unwrap_or(serde_json::Value::Null)
    }

    fn index_kind(keys: &Document) -> String {
        let mut kinds = Vec::new();
        for value in keys.values() {
            match value {
                Bson::String(value) if value == "text" => kinds.push("text"),
                Bson::String(value) if value == "2dsphere" || value == "2d" => kinds.push("geo"),
                Bson::String(value) if value == "hashed" => kinds.push("hashed"),
                Bson::String(value) if value == "columnstore" => kinds.push("columnstore"),
                _ => {}
            }
        }

        if keys.keys().any(|key| key == "$**" || key.ends_with(".$**")) && kinds.is_empty() {
            kinds.push("wildcard");
        }

        if kinds.is_empty() {
            "btree".to_string()
        } else {
            kinds.sort_unstable();
            kinds.dedup();
            kinds.join("+")
        }
    }

    /// Execute a command on the database
    async fn run_command(&self, mut command: Document) -> Result<Document> {
        let database_name = Self::command_target_database(&self.database, &mut command);
        let database = self.client.database(&database_name);
        run_mongodb_future(async move {
            database
                .run_command(command)
                .await
                .map_err(|e| ZqlzError::Driver(format!("MongoDB command failed: {}", e)))
        })
        .await
    }

    fn command_target_database(default_database: &str, command: &mut Document) -> String {
        match command.remove("$db") {
            Some(Bson::String(database)) if !database.is_empty() => database,
            _ => default_database.to_string(),
        }
    }

    async fn run_document_admin_command(
        &self,
        database: &str,
        command: Document,
        kind: &str,
        array_field: &str,
    ) -> Vec<DocumentAdminObjectInfo> {
        let database_name = database.to_string();
        let kind_name = kind.to_string();
        let command_name = command.keys().next().cloned().unwrap_or(kind_name.clone());
        let database_handle = self.client.database(database);
        let result = run_mongodb_future(async move {
            database_handle
                .run_command(command)
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))
        })
        .await;

        match result {
            Ok(document) => {
                if !array_field.is_empty()
                    && let Some(Bson::Array(items)) = document.get(array_field)
                {
                    return items
                        .iter()
                        .enumerate()
                        .map(|(index, item)| {
                            let details_json = Self::bson_to_json(item);
                            let name = item
                                .as_document()
                                .and_then(|document| {
                                    document
                                        .get_str("user")
                                        .or_else(|_| document.get_str("role"))
                                        .or_else(|_| document.get_str("_id"))
                                        .ok()
                                })
                                .map(ToString::to_string)
                                .unwrap_or_else(|| format!("{kind_name}-{index}"));
                            DocumentAdminObjectInfo {
                                database: database_name.clone(),
                                kind: kind_name.clone(),
                                name,
                                details_json,
                                unavailable_reason: None,
                            }
                        })
                        .collect();
                }

                vec![DocumentAdminObjectInfo {
                    database: database_name,
                    kind: kind_name,
                    name: command_name,
                    details_json: Self::document_to_json(&document),
                    unavailable_reason: None,
                }]
            }
            Err(error) => vec![DocumentAdminObjectInfo {
                database: database_name,
                kind: kind_name,
                name: command_name,
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(error.to_string()),
            }],
        }
    }

    async fn list_search_index_metadata(
        &self,
        database: &str,
        collections: &[DocumentCollectionInfo],
    ) -> (Vec<DocumentAdminObjectInfo>, Vec<DocumentAdminObjectInfo>) {
        let database_handle = self.client.database(database);
        let mut search_indexes = Vec::new();
        let mut vector_indexes = Vec::new();
        let mut unavailable_reason = None;

        for collection in collections
            .iter()
            .filter(|collection| collection.collection_type != "view")
        {
            let collection_handle = database_handle.collection::<Document>(&collection.name);
            let collection_name = collection.name.clone();
            let result = run_mongodb_future(async move {
                let cursor = collection_handle
                    .aggregate(vec![bson::doc! { "$listSearchIndexes": {} }])
                    .await
                    .map_err(|error| ZqlzError::Driver(error.to_string()))?;
                cursor
                    .try_collect::<Vec<Document>>()
                    .await
                    .map_err(|error| ZqlzError::Driver(error.to_string()))
            })
            .await;

            match result {
                Ok(index_documents) => {
                    for mut document in index_documents {
                        document.insert("collection", collection.name.clone());
                        let is_vector = document
                            .get_str("type")
                            .map(|value| value.eq_ignore_ascii_case("vectorSearch"))
                            .unwrap_or(false)
                            || document
                                .get("latestDefinition")
                                .or_else(|| document.get("definition"))
                                .is_some_and(Self::contains_vector_search_field);
                        let index_name = document
                            .get_str("name")
                            .map(ToString::to_string)
                            .unwrap_or_else(|_| "<unnamed>".to_string());
                        let object = DocumentAdminObjectInfo {
                            database: database.to_string(),
                            kind: if is_vector {
                                "vector_search_index".to_string()
                            } else {
                                "search_index".to_string()
                            },
                            name: format!("{}.{}", collection.name, index_name),
                            details_json: Self::document_to_json(&document),
                            unavailable_reason: None,
                        };
                        if is_vector {
                            vector_indexes.push(object);
                        } else {
                            search_indexes.push(object);
                        }
                    }
                }
                Err(error) => {
                    if unavailable_reason.is_none() {
                        unavailable_reason = Some(format!("{}: {}", collection_name, error));
                    }
                }
            }
        }

        if let Some(reason) = unavailable_reason
            && search_indexes.is_empty()
            && vector_indexes.is_empty()
        {
            let (search_unavailable, vector_unavailable) =
                Self::search_index_unavailable_rows(database, reason);
            search_indexes.push(search_unavailable);
            vector_indexes.push(vector_unavailable);
        }

        (search_indexes, vector_indexes)
    }

    fn search_index_unavailable_rows(
        database: &str,
        reason: String,
    ) -> (DocumentAdminObjectInfo, DocumentAdminObjectInfo) {
        (
            DocumentAdminObjectInfo {
                database: database.to_string(),
                kind: "search_index".to_string(),
                name: "$listSearchIndexes".to_string(),
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(reason.clone()),
            },
            DocumentAdminObjectInfo {
                database: database.to_string(),
                kind: "vector_search_index".to_string(),
                name: "$listSearchIndexes vector".to_string(),
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(reason),
            },
        )
    }

    async fn load_config_documents(
        &self,
        collection_name: &str,
        filter: Document,
    ) -> Result<Vec<Document>> {
        let collection = self
            .client
            .database("config")
            .collection::<Document>(collection_name);
        run_mongodb_future(async move {
            let cursor = collection
                .find(filter)
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))
        })
        .await
    }

    async fn list_sharding_metadata(&self, database: &str) -> Vec<DocumentAdminObjectInfo> {
        let namespace_regex = format!("^{}\\.", Self::regex_escape(database));
        let mut objects = Vec::new();

        let collection_documents = match self
            .load_config_documents(
                "collections",
                bson::doc! { "_id": { "$regex": namespace_regex.clone() } },
            )
            .await
        {
            Ok(documents) => documents,
            Err(error) => {
                objects.push(Self::unavailable_admin_object(
                    database,
                    "sharding",
                    "config.collections",
                    error,
                ));
                Vec::new()
            }
        };

        let collection_uuids = collection_documents
            .iter()
            .filter_map(|document| document.get("uuid").cloned())
            .collect::<Vec<_>>();
        let collection_uuid_names = collection_documents
            .iter()
            .filter_map(|document| {
                Some((
                    document.get("uuid")?.clone(),
                    document.get_str("_id").ok()?.to_string(),
                ))
            })
            .collect::<Vec<_>>();
        for (index, document) in collection_documents.into_iter().enumerate() {
            objects.push(Self::config_document_to_admin_object(
                database,
                "sharded_collection",
                Self::sharded_collection_name(&document, index),
                document,
            ));
        }

        let chunk_filter = if collection_uuids.is_empty() {
            bson::doc! { "ns": { "$regex": namespace_regex.clone() } }
        } else {
            bson::doc! { "uuid": { "$in": collection_uuids } }
        };
        match self.load_config_documents("chunks", chunk_filter).await {
            Ok(chunk_documents) => {
                for (index, document) in chunk_documents.into_iter().enumerate() {
                    let namespace = document.get("uuid").and_then(|uuid| {
                        collection_uuid_names
                            .iter()
                            .find_map(|(collection_uuid, name)| {
                                (collection_uuid == uuid).then_some(name.as_str())
                            })
                    });
                    objects.push(Self::config_document_to_admin_object(
                        database,
                        "chunk",
                        Self::sharding_chunk_name(&document, namespace, index),
                        document,
                    ));
                }
            }
            Err(error) => objects.push(Self::unavailable_admin_object(
                database,
                "sharding",
                "config.chunks",
                error,
            )),
        }

        match self
            .load_config_documents(
                "tags",
                bson::doc! { "ns": { "$regex": namespace_regex.clone() } },
            )
            .await
        {
            Ok(tag_documents) => {
                for (index, document) in tag_documents.into_iter().enumerate() {
                    objects.push(Self::config_document_to_admin_object(
                        database,
                        "zone",
                        Self::sharding_zone_name(&document, index),
                        document,
                    ));
                }
            }
            Err(error) => objects.push(Self::unavailable_admin_object(
                database,
                "sharding",
                "config.tags",
                error,
            )),
        }

        objects
    }

    fn config_document_to_admin_object(
        database: &str,
        kind: &str,
        name: String,
        document: Document,
    ) -> DocumentAdminObjectInfo {
        DocumentAdminObjectInfo {
            database: database.to_string(),
            kind: kind.to_string(),
            name,
            details_json: Self::document_to_json(&document),
            unavailable_reason: None,
        }
    }

    fn unavailable_admin_object(
        database: &str,
        kind: &str,
        name: &str,
        error: ZqlzError,
    ) -> DocumentAdminObjectInfo {
        DocumentAdminObjectInfo {
            database: database.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            details_json: serde_json::Value::Null,
            unavailable_reason: Some(error.to_string()),
        }
    }

    fn regex_escape(value: &str) -> String {
        value
            .chars()
            .flat_map(|character| match character {
                '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '{' | '}' | '[' | ']' | '^'
                | '$' => vec!['\\', character],
                character => vec![character],
            })
            .collect()
    }

    fn sharded_collection_name(document: &Document, index: usize) -> String {
        document
            .get_str("_id")
            .map(ToString::to_string)
            .unwrap_or_else(|_| format!("sharded-collection-{index}"))
    }

    fn sharding_chunk_name(document: &Document, namespace: Option<&str>, index: usize) -> String {
        if let Ok(namespace) = document.get_str("ns") {
            return format!("{namespace} chunk {index}");
        }
        if let Some(namespace) = namespace {
            return format!("{namespace} chunk {index}");
        }
        if let Ok(id) = document.get_str("_id") {
            return id.to_string();
        }
        if let Some(uuid) = document.get("uuid") {
            return format!("chunk {index} {uuid:?}");
        }
        format!("chunk-{index}")
    }

    fn sharding_zone_name(document: &Document, index: usize) -> String {
        match (document.get_str("ns"), document.get_str("tag")) {
            (Ok(namespace), Ok(tag)) => format!("{namespace} zone {tag}"),
            (Ok(namespace), Err(_)) => format!("{namespace} zone {index}"),
            (Err(_), Ok(tag)) => tag.to_string(),
            (Err(_), Err(_)) => format!("zone-{index}"),
        }
    }

    fn gridfs_buckets_from_collections(
        database: &str,
        collections: &[DocumentCollectionInfo],
    ) -> Vec<DocumentGridFsBucketInfo> {
        let mut gridfs_buckets = Vec::new();
        for files_collection in collections
            .iter()
            .filter(|collection| collection.name.ends_with(".files"))
        {
            let bucket = files_collection
                .name
                .strip_suffix(".files")
                .unwrap_or(&files_collection.name);
            let chunks_collection = format!("{bucket}.chunks");
            if collections
                .iter()
                .any(|collection| collection.name == chunks_collection)
            {
                gridfs_buckets.push(DocumentGridFsBucketInfo {
                    database: database.to_string(),
                    name: bucket.to_string(),
                    files_collection: files_collection.name.clone(),
                    chunks_collection,
                    file_count: files_collection.document_count,
                    size_bytes: files_collection.size_bytes,
                });
            }
        }
        gridfs_buckets
    }

    fn server_metadata_commands() -> Vec<Document> {
        vec![
            bson::doc! { "buildInfo": 1 },
            bson::doc! { "getParameter": 1, "featureCompatibilityVersion": 1 },
            bson::doc! { "serverStatus": 1 },
            bson::doc! { "getDefaultRWConcern": 1 },
            bson::doc! { "replSetGetStatus": 1 },
        ]
    }

    fn contains_vector_search_field(value: &Bson) -> bool {
        match value {
            Bson::Document(document) => {
                document
                    .get_str("type")
                    .map(|value| value.eq_ignore_ascii_case("vector"))
                    .unwrap_or(false)
                    || document.values().any(Self::contains_vector_search_field)
            }
            Bson::Array(values) => values.iter().any(Self::contains_vector_search_field),
            _ => false,
        }
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
        let value = serde_json::from_str::<serde_json::Value>(json).map_err(|error| {
            ZqlzError::Driver(format!("Invalid MongoDB {} JSON: {}", context, error))
        })?;
        match Self::json_value_to_bson(value)? {
            Bson::Document(document) => Ok(document),
            _ => Err(ZqlzError::Driver(format!(
                "Invalid MongoDB {} JSON: expected a JSON object",
                context
            ))),
        }
    }

    fn json_value_to_bson(value: serde_json::Value) -> Result<Bson> {
        match value {
            serde_json::Value::Null => Ok(Bson::Null),
            serde_json::Value::Bool(value) => Ok(Bson::Boolean(value)),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Bson::Int64(value))
                } else if let Some(value) = value.as_u64() {
                    i64::try_from(value)
                        .map(Bson::Int64)
                        .map_err(|_| ZqlzError::Driver("MongoDB number is too large".to_string()))
                } else if let Some(value) = value.as_f64() {
                    Ok(Bson::Double(value))
                } else {
                    Err(ZqlzError::Driver("Invalid MongoDB number".to_string()))
                }
            }
            serde_json::Value::String(value) => Ok(Bson::String(value)),
            serde_json::Value::Array(values) => values
                .into_iter()
                .map(Self::json_value_to_bson)
                .collect::<Result<Vec<_>>>()
                .map(Bson::Array),
            serde_json::Value::Object(mut object) => {
                if object.len() == 1
                    && let Some(serde_json::Value::String(value)) = object.remove("$oid")
                {
                    return ObjectId::parse_str(value.trim())
                        .map(Bson::ObjectId)
                        .map_err(|error| {
                            ZqlzError::Driver(format!("Invalid MongoDB ObjectId: {}", error))
                        });
                }

                let mut document = Document::new();
                for (key, value) in object {
                    document.insert(key, Self::json_value_to_bson(value)?);
                }
                Ok(Bson::Document(document))
            }
        }
    }

    fn format_mongodb_write_error(operation: &str, error: &MongoError) -> ZqlzError {
        ZqlzError::Driver(format!(
            "MongoDB {} failed: {}",
            operation,
            Self::mongodb_error_summary(error)
        ))
    }

    fn mongodb_error_summary(error: &MongoError) -> String {
        match error.kind.as_ref() {
            ErrorKind::Write(WriteFailure::WriteError(write_error)) if write_error.code == 121 => {
                Self::mongodb_validation_summary(&write_error.message, write_error.details.as_ref())
            }
            ErrorKind::Write(WriteFailure::WriteError(write_error)) => write_error.message.clone(),
            ErrorKind::Write(WriteFailure::WriteConcernError(write_error)) => {
                write_error.message.clone()
            }
            ErrorKind::Command(command_error) => command_error.message.clone(),
            _ => error.to_string(),
        }
    }

    fn mongodb_validation_summary(message: &str, details: Option<&Document>) -> String {
        let mut reasons = BTreeSet::new();
        if let Some(details) = details {
            Self::collect_mongodb_validation_reasons(
                &Bson::Document(details.clone()),
                &mut reasons,
            );
        }

        if reasons.is_empty() {
            message.to_string()
        } else {
            format!(
                "{}: {}",
                message,
                reasons.into_iter().collect::<Vec<_>>().join("; ")
            )
        }
    }

    fn collect_mongodb_validation_reasons(value: &Bson, reasons: &mut BTreeSet<String>) {
        match value {
            Bson::Document(document) => {
                if let Some(Bson::Array(missing)) = document.get("missingProperties") {
                    let missing = missing.iter().filter_map(Bson::as_str).collect::<Vec<_>>();
                    if !missing.is_empty() {
                        reasons.insert(format!("missing required fields: {}", missing.join(", ")));
                    }
                }

                if let Some(Bson::String(property_name)) = document.get("propertyName") {
                    if let Some(reason) = Self::validation_reason_for_document(document) {
                        reasons.insert(format!("{}: {}", property_name, reason));
                    }
                    if let Some(Bson::Array(details)) = document.get("details") {
                        for detail in details {
                            if let Bson::Document(detail) = detail
                                && let Some(reason) = Self::validation_reason_for_document(detail)
                            {
                                reasons.insert(format!("{} {}", property_name, reason));
                            }
                        }
                    }
                }

                for value in document.values() {
                    Self::collect_mongodb_validation_reasons(value, reasons);
                }
            }
            Bson::Array(values) => {
                for value in values {
                    Self::collect_mongodb_validation_reasons(value, reasons);
                }
            }
            _ => {}
        }
    }

    fn validation_reason_for_document(document: &Document) -> Option<String> {
        let reason = document.get_str("reason").ok()?;
        let expected_type = document
            .get_document("specifiedAs")
            .ok()
            .and_then(|specified_as| specified_as.get_str("bsonType").ok());
        let considered_type = document.get_str("consideredType").ok();
        match (expected_type, considered_type) {
            (Some(expected), Some(actual)) => {
                Some(format!("expected {} but got {}", expected, actual))
            }
            _ => Some(reason.to_string()),
        }
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
                data_type: Self::inferred_bson_column_type(&documents, name),
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

    fn requires_database_scoped_connection(&self) -> bool {
        true
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
                resolved.physical_database_key = Some(database_name.clone());
                resolved.effective_database = Some(database_name);
                resolved.requires_dedicated_connection = true;
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
        ExplainParserKind::Raw
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig {
            explain_format: Cow::Borrowed("{sql}.explain()"),
            query_plan_format: None,
            analyze_format: Some(Cow::Borrowed("{sql}.explain(\"executionStats\")")),
            explain_description: Cow::Borrowed("MongoDB queryPlanner explain"),
            query_plan_description: None,
            analyze_is_safe: true,
        }
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

    fn ping_query_sql(&self) -> &'static str {
        MONGODB_PING_COMMAND
    }

    fn as_document_store(&self) -> Option<&dyn DocumentStore> {
        Some(self)
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }
}

#[async_trait]
impl DocumentStore for MongoDbConnection {
    async fn list_document_databases(&self) -> Result<Vec<DocumentDatabaseInfo>> {
        let client = self.client.clone();
        let databases = run_mongodb_future(async move {
            client.list_database_names().await.map_err(|error| {
                ZqlzError::Driver(format!("Failed to list MongoDB databases: {}", error))
            })
        })
        .await?;

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
        let collection_infos = {
            let database_handle = database_handle.clone();
            run_mongodb_future(async move {
                let mut cursor = database_handle.list_collections().await.map_err(|error| {
                    ZqlzError::Driver(format!("Failed to list MongoDB collections: {}", error))
                })?;
                let mut infos = Vec::new();
                while cursor.advance().await.map_err(|error| {
                    ZqlzError::Driver(format!(
                        "Failed to read MongoDB collection metadata: {}",
                        error
                    ))
                })? {
                    infos.push(cursor.deserialize_current().map_err(|error| {
                        ZqlzError::Driver(format!(
                            "Failed to parse MongoDB collection metadata: {}",
                            error
                        ))
                    })?);
                }
                Ok::<Vec<CollectionSpecification>, ZqlzError>(infos)
            })
            .await?
        };

        let mut collections = Vec::with_capacity(collection_infos.len());
        for collection_info in collection_infos {
            let name = collection_info.name.clone();
            let options_json = Self::collection_options_json(&collection_info);
            let validator_json = options_json.get("validator").cloned();
            let collation_json = options_json.get("collation").cloned();
            let timeseries_json = options_json.get("timeseries").cloned();
            let clustered_index_json = options_json.get("clusteredIndex").cloned();
            let change_stream_pre_and_post_images = options_json
                .get("changeStreamPreAndPostImages")
                .and_then(|value| value.get("enabled"))
                .and_then(|value| value.as_bool());
            let view_on = options_json
                .get("viewOn")
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
            let pipeline_json = options_json.get("pipeline").cloned();
            let collection_type = match collection_info.collection_type {
                CollectionType::View => "view",
                CollectionType::Timeseries => "timeseries",
                CollectionType::Collection => "collection",
                _ => "collection",
            }
            .to_string();
            let stats = {
                let database_handle = database_handle.clone();
                let name = name.clone();
                run_mongodb_future(async move {
                    database_handle
                        .run_command(bson::doc! { "collStats": &name })
                        .await
                        .map_err(|error| {
                            ZqlzError::Driver(format!("MongoDB collStats failed: {}", error))
                        })
                })
                .await
                .ok()
            };
            collections.push(DocumentCollectionInfo {
                database: database.to_string(),
                name,
                collection_type,
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
                options_json: Some(options_json),
                validator_json,
                collation_json,
                view_on,
                pipeline_json,
                timeseries_json,
                clustered_index_json,
                change_stream_pre_and_post_images,
            });
        }

        Ok(collections)
    }

    async fn list_database_objects(&self, database: &str) -> Result<DocumentDatabaseObjects> {
        let collections = self.list_collections(database).await?;
        let database_handle = self.client.database(database);

        let mut indexes = Vec::new();
        for collection in collections
            .iter()
            .filter(|collection| collection.collection_type != "view")
        {
            let collection_handle = database_handle.collection::<Document>(&collection.name);
            let collection_name = collection.name.clone();
            let collection_indexes = run_mongodb_future(async move {
                let cursor = collection_handle.list_indexes().await.map_err(|error| {
                    ZqlzError::Driver(format!(
                        "MongoDB listIndexes failed for {}: {}",
                        collection_name, error
                    ))
                })?;
                cursor.try_collect::<Vec<_>>().await.map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB index cursor failed: {}", error))
                })
            })
            .await
            .unwrap_or_default();

            for index in collection_indexes {
                let keys_json = Self::document_to_json(&index.keys);
                let options_json =
                    serde_json::to_value(&index.options).unwrap_or(serde_json::Value::Null);
                let options = index.options.as_ref();
                let name = options
                    .and_then(|options| options.name.clone())
                    .unwrap_or_else(|| index.keys.keys().cloned().collect::<Vec<_>>().join("_"));
                indexes.push(DocumentIndexInfo {
                    database: database.to_string(),
                    collection: collection.name.clone(),
                    name,
                    keys_json,
                    options_json,
                    unique: options.and_then(|options| options.unique).unwrap_or(false),
                    sparse: options.and_then(|options| options.sparse).unwrap_or(false),
                    ttl_seconds: options
                        .and_then(|options| options.expire_after)
                        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64),
                    partial_filter_json: options
                        .and_then(|options| options.partial_filter_expression.as_ref())
                        .map(Self::document_to_json),
                    collation_json: options
                        .and_then(|options| options.collation.as_ref())
                        .and_then(|collation| serde_json::to_value(collation).ok()),
                    kind: Self::index_kind(&index.keys),
                });
            }
        }

        let mut functions = Vec::new();
        if collections
            .iter()
            .any(|collection| collection.name == "system.js")
        {
            let system_js = database_handle.collection::<Document>("system.js");
            let function_docs = run_mongodb_future(async move {
                let cursor = system_js.find(bson::doc! {}).await.map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB system.js query failed: {}", error))
                })?;
                cursor
                    .try_collect::<Vec<Document>>()
                    .await
                    .map_err(|error| {
                        ZqlzError::Driver(format!("MongoDB system.js cursor failed: {}", error))
                    })
            })
            .await
            .unwrap_or_default();

            for document in function_docs {
                let name = document
                    .get_str("_id")
                    .ok()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<anonymous>".to_string());
                let body = document
                    .get("value")
                    .map(|value| match value {
                        Bson::JavaScriptCode(code) => code.clone(),
                        Bson::JavaScriptCodeWithScope(code) => code.code.clone(),
                        Bson::String(code) => code.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                functions.push(DocumentFunctionInfo {
                    database: database.to_string(),
                    name,
                    body,
                });
            }
        }

        let gridfs_buckets = Self::gridfs_buckets_from_collections(database, &collections);

        let users = self
            .run_document_admin_command(database, bson::doc! { "usersInfo": 1 }, "user", "users")
            .await;
        let roles = self
            .run_document_admin_command(
                database,
                bson::doc! { "rolesInfo": 1, "showPrivileges": true },
                "role",
                "roles",
            )
            .await;
        let (search_indexes, vector_indexes) = self
            .list_search_index_metadata(database, &collections)
            .await;
        let mut server = Vec::new();
        for command in Self::server_metadata_commands() {
            server.extend(
                self.run_document_admin_command("admin", command, "server", "")
                    .await,
            );
        }
        let mut sharding = self
            .run_document_admin_command(
                "admin",
                bson::doc! { "listShards": 1 },
                "sharding",
                "shards",
            )
            .await;
        sharding.extend(self.list_sharding_metadata(database).await);

        Ok(DocumentDatabaseObjects {
            collections,
            indexes,
            functions,
            gridfs_buckets,
            users,
            roles,
            search_indexes,
            vector_indexes,
            server,
            sharding,
        })
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

        let documents = run_mongodb_future(async move {
            let cursor = collection
                .find(filter)
                .with_options(options)
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB find failed: {}", error)))?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
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
        let documents = run_mongodb_future(async move {
            let cursor = collection.aggregate(pipeline).await.map_err(|error| {
                ZqlzError::Driver(format!("MongoDB aggregate failed: {}", error))
            })?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
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
        let result = run_mongodb_future(async move {
            collection
                .insert_one(document)
                .await
                .map_err(|error| Self::format_mongodb_write_error("insert", &error))
        })
        .await?;
        Ok(Self::bson_to_value(&result.inserted_id))
    }

    async fn replace_document(&self, request: DocumentReplaceRequest) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let id = Self::parse_id_json(&request.id_json)?;
        let document = Self::parse_required_document_json(&request.document_json, "document")?;
        let result = run_mongodb_future(async move {
            collection
                .replace_one(bson::doc! { "_id": id }, document)
                .await
                .map_err(|error| Self::format_mongodb_write_error("replace", &error))
        })
        .await?;
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
        let value = bson::to_bson(&request.new_value.to_json_value()).unwrap_or(Bson::Null);
        let result = run_mongodb_future(async move {
            collection
                .update_one(
                    bson::doc! { "_id": id },
                    bson::doc! { "$set": { request.field_path: value } },
                )
                .await
                .map_err(|error| Self::format_mongodb_write_error("update", &error))
        })
        .await?;
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
            let delete_result = {
                let collection = collection.clone();
                run_mongodb_future(async move {
                    collection
                        .delete_one(bson::doc! { "_id": id })
                        .await
                        .map_err(|error| {
                            ZqlzError::Driver(format!("MongoDB delete failed: {}", error))
                        })
                })
                .await
            };
            match delete_result {
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
        let sample_size = request.sample_size.min(i32::MAX as u64) as i32;
        let documents = run_mongodb_future(async move {
            let cursor = collection
                .aggregate(vec![bson::doc! { "$sample": { "size": sample_size } }])
                .await
                .map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB schema sample failed: {}", error))
                })?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
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

fn object_form_string(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> Option<String> {
    values
        .get(field_id)
        .and_then(ObjectFormValue::as_string)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn object_form_bool(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> bool {
    values
        .get(field_id)
        .and_then(ObjectFormValue::as_bool)
        .unwrap_or(false)
}

fn parse_json_form_field(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(value) = object_form_string(values, field_id) else {
        return Ok(None);
    };
    serde_json::from_str::<serde_json::Value>(&value)
        .map(Some)
        .map_err(|error| ZqlzError::Driver(format!("Invalid JSON in {field_id}: {error}")))
}

fn text_field(id: &str, label: &str, default_value: impl Into<String>) -> ObjectFormField {
    ObjectFormField::new(id, label, ObjectFormFieldKind::Text)
        .default_value(ObjectFormValue::String(default_value.into()))
}

fn json_field(id: &str, label: &str, default_value: impl Into<String>) -> ObjectFormField {
    ObjectFormField::new(id, label, ObjectFormFieldKind::TextArea)
        .default_value(ObjectFormValue::String(default_value.into()))
}

fn read_only_field(id: &str, label: &str, value: impl Into<String>) -> ObjectFormField {
    text_field(id, label, value).read_only()
}

fn render_mongodb_command(command: serde_json::Value) -> Result<String> {
    serde_json::to_string_pretty(&command)
        .map_err(|error| ZqlzError::Driver(format!("Failed to render MongoDB command: {error}")))
}

#[async_trait]
impl SchemaIntrospection for MongoDbConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        Ok(self
            .list_document_databases()
            .await?
            .into_iter()
            .map(|database| DatabaseInfo {
                name: database.name,
                owner: None,
                encoding: None,
                size_bytes: database.size_bytes.map(|size| size as i64),
                comment: None,
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(Vec::new())
    }
    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        Ok(Vec::new())
    }
    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        Ok(Vec::new())
    }
    async fn list_tables_extended(&self, _schema: Option<&str>) -> Result<ObjectsPanelData> {
        let databases = self.list_document_databases().await?;
        let mut objects = DocumentDatabaseObjects::default();

        for database in &databases {
            let database_objects = self.list_database_objects(&database.name).await?;
            objects.collections.extend(database_objects.collections);
            objects.indexes.extend(database_objects.indexes);
            objects.functions.extend(database_objects.functions);
            objects
                .gridfs_buckets
                .extend(database_objects.gridfs_buckets);
            objects.users.extend(database_objects.users);
            objects.roles.extend(database_objects.roles);
            objects
                .search_indexes
                .extend(database_objects.search_indexes);
            objects
                .vector_indexes
                .extend(database_objects.vector_indexes);
            objects.server.extend(database_objects.server);
            objects.sharding.extend(database_objects.sharding);
        }

        let databases = databases
            .into_iter()
            .map(|database| {
                (
                    database.name,
                    database
                        .size_bytes
                        .and_then(|size| i64::try_from(size).ok()),
                )
            })
            .collect();

        Ok(objects_panel::document_databases_and_objects(
            databases, objects,
        ))
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        let databases = self.list_document_databases().await?;
        let mut objects = DocumentDatabaseObjects::default();

        for database in &databases {
            let database_objects = self.list_database_objects(&database.name).await?;
            objects.collections.extend(database_objects.collections);
            objects.indexes.extend(database_objects.indexes);
            objects.functions.extend(database_objects.functions);
            objects
                .gridfs_buckets
                .extend(database_objects.gridfs_buckets);
            objects.users.extend(database_objects.users);
            objects.roles.extend(database_objects.roles);
            objects
                .search_indexes
                .extend(database_objects.search_indexes);
            objects
                .vector_indexes
                .extend(database_objects.vector_indexes);
            objects.server.extend(database_objects.server);
            objects.sharding.extend(database_objects.sharding);
        }

        let databases = databases
            .into_iter()
            .map(|database| {
                (
                    database.name,
                    database
                        .size_bytes
                        .and_then(|size| i64::try_from(size).ok()),
                )
            })
            .collect();

        let (_, manifest) =
            objects_panel::document_databases_and_objects_with_manifest(databases, objects);
        Ok(manifest)
    }
    async fn get_table(&self, _schema: Option<&str>, name: &str) -> Result<TableDetails> {
        Err(ZqlzError::NotSupported(format!(
            "MongoDB table details are not supported for {name}"
        )))
    }
    async fn get_columns(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<zqlz_core::ColumnInfo>> {
        Ok(Vec::new())
    }
    async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> Result<Vec<IndexInfo>> {
        Ok(Vec::new())
    }
    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(Vec::new())
    }
    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        Ok(None)
    }
    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        Ok(Vec::new())
    }
    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }
    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        Ok(Vec::new())
    }
    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(Vec::new())
    }
    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }
    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        Ok(Vec::new())
    }
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        Ok(format!("-- MongoDB metadata\n-- name: {}\n", object.name))
    }
    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        let name = request
            .object_ref
            .as_ref()
            .map(|object_ref| object_ref.name.clone())
            .unwrap_or_default();
        let collection = request
            .object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.schema.clone())
            .unwrap_or_default();
        let spec = match (request.kind_id.as_str(), request.mode) {
            ("document_collection", ObjectFormMode::Create) => ObjectFormSpec::new(
                "document_collection",
                request.mode,
                "Create MongoDB Collection",
            )
            .sections(vec![ObjectFormSection::new(vec![
                text_field("name", "Name", "").required(),
                json_field("validator", "Validator JSON", ""),
                text_field("validation_level", "Validation Level", "strict"),
                text_field("validation_action", "Validation Action", "error"),
                json_field("collation", "Collation JSON", ""),
                json_field("timeseries", "Time Series JSON", ""),
                json_field("clustered_index", "Clustered Index JSON", ""),
                ObjectFormField::new("capped", "Capped", ObjectFormFieldKind::Checkbox)
                    .default_value(ObjectFormValue::Bool(false)),
                text_field("size", "Capped Size Bytes", ""),
                text_field("max", "Capped Max Documents", ""),
                ObjectFormField::new(
                    "change_stream_pre_post_images",
                    "Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
            ])]),
            ("document_collection", ObjectFormMode::Edit) => ObjectFormSpec::new(
                "document_collection",
                request.mode,
                "Modify MongoDB Collection",
            )
            .sections(vec![ObjectFormSection::new(vec![
                read_only_field("name", "Name", name),
                json_field("validator", "Validator JSON", ""),
                text_field("validation_level", "Validation Level", ""),
                text_field("validation_action", "Validation Action", ""),
                text_field("expire_after_seconds", "Time Series TTL Seconds", ""),
                text_field("timeseries_granularity", "Time Series Granularity", ""),
                text_field("capped_size", "Capped Size Bytes", ""),
                text_field("capped_max", "Capped Max Documents", ""),
                ObjectFormField::new(
                    "apply_change_stream_pre_post_images",
                    "Apply Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
                ObjectFormField::new(
                    "change_stream_pre_post_images",
                    "Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
            ])]),
            (
                "document_collection"
                | "document_view"
                | "document_index"
                | "document_user"
                | "document_role",
                ObjectFormMode::Drop,
            ) => ObjectFormSpec::new(&request.kind_id, request.mode, "Drop MongoDB Object")
                .sections(vec![ObjectFormSection::new(vec![
                    read_only_field("collection", "Collection", collection),
                    read_only_field("name", "Name", name),
                    ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                        .default_value(ObjectFormValue::Bool(false)),
                ])]),
            ("document_view", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_view", request.mode, "Create MongoDB View").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "View Name", name).required(),
                        text_field("view_on", "Source Collection", "").required(),
                        json_field("pipeline", "Pipeline JSON Array", "[]").required(),
                        json_field("collation", "Collation JSON", ""),
                    ])],
                )
            }
            ("document_view", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_view", request.mode, "Modify MongoDB View").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "View Name", name),
                        text_field("view_on", "Source Collection", "").required(),
                        json_field("pipeline", "Pipeline JSON Array", "[]").required(),
                    ])],
                )
            }
            ("document_index", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_index", request.mode, "Create MongoDB Index")
                    .sections(vec![ObjectFormSection::new(vec![
                        text_field("collection", "Collection", collection).required(),
                        text_field("name", "Index Name", "").required(),
                        json_field("keys", "Keys JSON", "{\"field\": 1}").required(),
                        ObjectFormField::new("unique", "Unique", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                        ObjectFormField::new("sparse", "Sparse", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                        text_field("expire_after_seconds", "TTL Seconds", ""),
                        json_field("partial_filter", "Partial Filter JSON", ""),
                        json_field("collation", "Collation JSON", ""),
                        json_field("additional_options", "Additional Options JSON", ""),
                    ])])
            }
            ("document_user", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_user", request.mode, "Create MongoDB User").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "User", "").required(),
                        text_field("password", "Password", "").required(),
                        json_field("roles", "Roles JSON Array", "[]"),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_user", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_user", request.mode, "Update MongoDB User").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "User", name),
                        text_field("password", "New Password", ""),
                        json_field("roles", "Roles JSON Array", ""),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_role", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_role", request.mode, "Create MongoDB Role").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "Role", "").required(),
                        json_field("privileges", "Privileges JSON Array", "[]"),
                        json_field("roles", "Inherited Roles JSON Array", "[]"),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_role", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_role", request.mode, "Update MongoDB Role").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "Role", name),
                        json_field("privileges", "Privileges JSON Array", ""),
                        json_field("roles", "Inherited Roles JSON Array", ""),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            _ => return Ok(None),
        };
        Ok(Some(spec))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        let values = &request.values;
        let name = object_form_string(values, "name")
            .or_else(|| {
                request
                    .object_ref
                    .as_ref()
                    .map(|object_ref| object_ref.name.clone())
            })
            .ok_or_else(|| ZqlzError::Driver("Name is required".to_string()))?;
        let confirmed = || object_form_bool(values, "confirm");
        let command = match (request.kind_id.as_str(), request.mode) {
            ("document_collection", ObjectFormMode::Create) => {
                let mut command = serde_json::json!({ "create": name });
                for (field_id, command_field) in [
                    ("validator", "validator"),
                    ("collation", "collation"),
                    ("timeseries", "timeseries"),
                    ("clustered_index", "clusteredIndex"),
                ] {
                    if let Some(value) = parse_json_form_field(values, field_id)? {
                        command[command_field] = value;
                    }
                }
                if let Some(value) = object_form_string(values, "validation_level") {
                    command["validationLevel"] = serde_json::Value::String(value);
                }
                if let Some(value) = object_form_string(values, "validation_action") {
                    command["validationAction"] = serde_json::Value::String(value);
                }
                if object_form_bool(values, "capped") {
                    command["capped"] = serde_json::Value::Bool(true);
                    if let Some(value) = object_form_string(values, "size")
                        .and_then(|value| value.parse::<i64>().ok())
                    {
                        command["size"] = value.into();
                    }
                    if let Some(value) = object_form_string(values, "max")
                        .and_then(|value| value.parse::<i64>().ok())
                    {
                        command["max"] = value.into();
                    }
                }
                if object_form_bool(values, "change_stream_pre_post_images") {
                    command["changeStreamPreAndPostImages"] =
                        serde_json::json!({ "enabled": true });
                }
                command
            }
            ("document_collection", ObjectFormMode::Edit) => {
                let mut command = serde_json::json!({ "collMod": name });
                if let Some(value) = parse_json_form_field(values, "validator")? {
                    command["validator"] = value;
                }
                if let Some(value) = object_form_string(values, "validation_level") {
                    command["validationLevel"] = value.into();
                }
                if let Some(value) = object_form_string(values, "validation_action") {
                    command["validationAction"] = value.into();
                }
                if let Some(value) = object_form_string(values, "expire_after_seconds") {
                    if value == "off" {
                        command["expireAfterSeconds"] = value.into();
                    } else if let Ok(seconds) = value.parse::<i64>() {
                        command["expireAfterSeconds"] = seconds.into();
                    }
                }
                if let Some(granularity) = object_form_string(values, "timeseries_granularity") {
                    command["timeseries"] = serde_json::json!({ "granularity": granularity });
                }
                if let Some(size) = object_form_string(values, "capped_size")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    command["cappedSize"] = size.into();
                }
                if let Some(max) = object_form_string(values, "capped_max")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    command["cappedMax"] = max.into();
                }
                if object_form_bool(values, "apply_change_stream_pre_post_images") {
                    command["changeStreamPreAndPostImages"] = serde_json::json!({ "enabled": object_form_bool(values, "change_stream_pre_post_images") });
                }
                if command.get("timeseries").is_some()
                    && command.as_object().is_some_and(|fields| fields.len() > 2)
                {
                    return Err(ZqlzError::Driver(
                        "MongoDB time-series collMod changes must be submitted without other collection modifications".to_string(),
                    ));
                }
                command
            }
            ("document_view", ObjectFormMode::Create) => {
                let mut command = serde_json::json!({
                    "create": name,
                    "viewOn": object_form_string(values, "view_on").ok_or_else(|| ZqlzError::Driver("Source collection is required".to_string()))?,
                    "pipeline": parse_json_form_field(values, "pipeline")?.unwrap_or_else(|| serde_json::json!([])),
                });
                if let Some(value) = parse_json_form_field(values, "collation")? {
                    command["collation"] = value;
                }
                command
            }
            ("document_view", ObjectFormMode::Edit) => {
                serde_json::json!({
                    "collMod": name,
                    "viewOn": object_form_string(values, "view_on").ok_or_else(|| ZqlzError::Driver("Source collection is required".to_string()))?,
                    "pipeline": parse_json_form_field(values, "pipeline")?.unwrap_or_else(|| serde_json::json!([])),
                })
            }
            ("document_index", ObjectFormMode::Create) => {
                let collection = object_form_string(values, "collection")
                    .ok_or_else(|| ZqlzError::Driver("Collection is required".to_string()))?;
                let mut index = serde_json::json!({ "key": parse_json_form_field(values, "keys")?.ok_or_else(|| ZqlzError::Driver("Index keys are required".to_string()))?, "name": name });
                if object_form_bool(values, "unique") {
                    index["unique"] = true.into();
                }
                if object_form_bool(values, "sparse") {
                    index["sparse"] = true.into();
                }
                if let Some(value) = object_form_string(values, "expire_after_seconds")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    index["expireAfterSeconds"] = value.into();
                }
                if let Some(value) = parse_json_form_field(values, "partial_filter")? {
                    index["partialFilterExpression"] = value;
                }
                if let Some(value) = parse_json_form_field(values, "collation")? {
                    index["collation"] = value;
                }
                if let Some(value) = parse_json_form_field(values, "additional_options")? {
                    let options = value.as_object().ok_or_else(|| {
                        ZqlzError::Driver(
                            "Additional index options must be a JSON object".to_string(),
                        )
                    })?;
                    for (key, value) in options {
                        index[key] = value.clone();
                    }
                }
                serde_json::json!({ "createIndexes": collection, "indexes": [index] })
            }
            ("document_collection" | "document_view", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "drop": name })
            }
            ("document_index", ObjectFormMode::Drop) if confirmed() => {
                let collection = object_form_string(values, "collection")
                    .or_else(|| {
                        request
                            .object_ref
                            .as_ref()
                            .and_then(|object_ref| object_ref.schema.clone())
                    })
                    .ok_or_else(|| ZqlzError::Driver("Collection is required".to_string()))?;
                serde_json::json!({ "dropIndexes": collection, "index": name })
            }
            ("document_user", ObjectFormMode::Create) if confirmed() => {
                serde_json::json!({ "createUser": name, "pwd": object_form_string(values, "password").ok_or_else(|| ZqlzError::Driver("Password is required".to_string()))?, "roles": parse_json_form_field(values, "roles")?.unwrap_or_else(|| serde_json::json!([])) })
            }
            ("document_user", ObjectFormMode::Edit) if confirmed() => {
                let mut command = serde_json::json!({ "updateUser": name });
                if let Some(password) = object_form_string(values, "password")
                    && !password.is_empty()
                {
                    command["pwd"] = password.into();
                }
                if let Some(roles) = parse_json_form_field(values, "roles")? {
                    command["roles"] = roles;
                }
                if command.as_object().is_some_and(|fields| fields.len() == 1) {
                    return Err(ZqlzError::Driver(
                        "At least one user change is required".to_string(),
                    ));
                }
                command
            }
            ("document_user", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "dropUser": name })
            }
            ("document_role", ObjectFormMode::Create) if confirmed() => {
                serde_json::json!({ "createRole": name, "privileges": parse_json_form_field(values, "privileges")?.unwrap_or_else(|| serde_json::json!([])), "roles": parse_json_form_field(values, "roles")?.unwrap_or_else(|| serde_json::json!([])) })
            }
            ("document_role", ObjectFormMode::Edit) if confirmed() => {
                let mut command = serde_json::json!({ "updateRole": name });
                if let Some(privileges) = parse_json_form_field(values, "privileges")? {
                    command["privileges"] = privileges;
                }
                if let Some(roles) = parse_json_form_field(values, "roles")? {
                    command["roles"] = roles;
                }
                if command.as_object().is_some_and(|fields| fields.len() == 1) {
                    return Err(ZqlzError::Driver(
                        "At least one role change is required".to_string(),
                    ));
                }
                command
            }
            ("document_role", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "dropRole": name })
            }
            _ => {
                return Err(ZqlzError::Driver(
                    "Confirm the destructive/admin action first".to_string(),
                ));
            }
        };
        Ok(vec![render_mongodb_command(command)?])
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
mod ping_tests {
    use super::*;

    #[test]
    fn ping_statement_parses_as_a_runnable_command_document() {
        let command: Document = serde_json::from_str(MONGODB_PING_COMMAND)
            .expect("heartbeat ping must be a document the driver can run");
        assert!(command.contains_key("ping"));
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    use std::collections::BTreeMap;
    use zqlz_core::{
        DatabaseDriver, HighlightQueryLanguage, ParameterPlaceholderCapability, TreeSitterGrammar,
        syntax_driver_capabilities_from_bundle,
    };

    #[test]
    fn mongodb_dialect_bundle_owns_editor_syntax_capabilities() {
        let driver = MongoDbDriver::new();
        let bundle = driver
            .dialect_bundle()
            .expect("MongoDB driver should expose dialect bundle");
        let capabilities = syntax_driver_capabilities_from_bundle(bundle);

        assert_eq!(capabilities.profile, "mongodb");
        assert_eq!(
            capabilities.tree_sitter_grammar,
            TreeSitterGrammar::Javascript
        );
        assert_eq!(
            capabilities.highlight_query_language,
            HighlightQueryLanguage::MongoDb
        );
        assert_eq!(
            capabilities.parameter_placeholders,
            ParameterPlaceholderCapability::disabled()
        );
        assert!(capabilities.document_syntax);
        assert!(!capabilities.command_syntax);
        assert!(!capabilities.sql_overlays);
    }

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

    #[test]
    fn document_cell_update_value_uses_logical_value_json() {
        assert_eq!(
            bson::to_bson(&Value::Json(serde_json::json!({ "nested": true })).to_json_value())
                .expect("json to bson"),
            Bson::Document(bson::doc! { "nested": true })
        );
        assert_eq!(
            bson::to_bson(&Value::Int64(42).to_json_value()).expect("int to bson"),
            Bson::Int64(42)
        );
    }

    #[test]
    fn parse_required_document_json_preserves_extended_object_id() {
        let id = "507f1f77bcf86cd799439011";
        let document = MongoDbConnection::parse_required_document_json(
            &serde_json::json!({ "warehouseId": { "$oid": id } }).to_string(),
            "document",
        )
        .expect("document json should parse");

        assert!(matches!(
            document.get("warehouseId"),
            Some(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
    }

    #[test]
    fn mongodb_validation_summary_extracts_field_reasons() {
        let details = bson::doc! {
            "details": {
                "schemaRulesNotSatisfied": [
                    {
                        "operatorName": "properties",
                        "propertiesNotSatisfied": [
                            {
                                "propertyName": "warehouseId",
                                "details": [
                                    {
                                        "operatorName": "bsonType",
                                        "specifiedAs": { "bsonType": "objectId" },
                                        "reason": "type did not match",
                                        "consideredType": "string"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "operatorName": "required",
                        "missingProperties": ["updatedAt"]
                    }
                ]
            }
        };

        let summary = MongoDbConnection::mongodb_validation_summary(
            "Document failed validation",
            Some(&details),
        );

        assert!(summary.contains("warehouseId expected objectId but got string"));
        assert!(summary.contains("missing required fields: updatedAt"));
    }

    #[test]
    fn index_kind_detects_mongodb_special_index_families() {
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "email": "hashed" }),
            "hashed"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "body": "text", "location": "2dsphere" }),
            "geo+text"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "$**": 1 }),
            "wildcard"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "payload.$**": 1 }),
            "wildcard"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "$**": "columnstore" }),
            "columnstore"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "tenantId": 1, "updatedAt": -1 }),
            "btree"
        );
    }

    #[test]
    fn atlas_search_unavailable_state_populates_search_and_vector_sections() {
        let (search, vector) = MongoDbConnection::search_index_unavailable_rows(
            "zqlz_feature_lab",
            "$listSearchIndexes is not supported".to_string(),
        );

        assert_eq!(search.kind, "search_index");
        assert_eq!(search.name, "$listSearchIndexes");
        assert_eq!(
            search.unavailable_reason.as_deref(),
            Some("$listSearchIndexes is not supported")
        );
        assert_eq!(vector.kind, "vector_search_index");
        assert_eq!(vector.name, "$listSearchIndexes vector");
        assert_eq!(
            vector.unavailable_reason.as_deref(),
            Some("$listSearchIndexes is not supported")
        );
    }

    #[test]
    fn mongodb_command_target_database_uses_optional_db_field() {
        let mut command = bson::doc! { "$db": "config", "find": "collections" };

        assert_eq!(
            MongoDbConnection::command_target_database("app", &mut command),
            "config"
        );
        assert!(!command.contains_key("$db"));

        let mut command = bson::doc! { "buildInfo": 1 };
        assert_eq!(
            MongoDbConnection::command_target_database("app", &mut command),
            "app"
        );
    }

    #[test]
    fn mongodb_shell_find_converts_to_command_document() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.products.find({ $text: { $search: "developer iot" } }, { sku: 1, name: 1 }).sort({ score: { $meta: "textScore" } }).limit(5)"#,
        )
        .unwrap()
        .expect("shell query command");

        assert_eq!(command.get_str("find").unwrap(), "products");
        assert!(
            command
                .get_document("filter")
                .unwrap()
                .contains_key("$text")
        );
        assert!(
            command
                .get_document("projection")
                .unwrap()
                .contains_key("sku")
        );
        assert_eq!(command.get_i64("limit").unwrap(), 5);
        assert!(command.get_document("sort").unwrap().contains_key("score"));
    }

    #[test]
    fn mongodb_shell_find_explain_wraps_cursor_command() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.products.find({ $text: { $search: "developer iot" } }, { sku: 1, name: 1 }).sort({ score: { $meta: "textScore" } }).limit(5).explain("executionStats")"#,
        )
        .unwrap()
        .expect("shell explain command");

        assert_eq!(command.get_str("verbosity").unwrap(), "executionStats");
        let explained = command.get_document("explain").unwrap();
        assert_eq!(explained.get_str("find").unwrap(), "products");
        assert!(
            explained
                .get_document("filter")
                .unwrap()
                .contains_key("$text")
        );
        assert!(
            explained
                .get_document("projection")
                .unwrap()
                .contains_key("sku")
        );
        assert!(
            explained
                .get_document("sort")
                .unwrap()
                .contains_key("score")
        );
        assert_eq!(explained.get_i64("limit").unwrap(), 5);
    }

    #[test]
    fn mongodb_shell_aggregate_converts_to_command_document() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.orders.aggregate([{ $match: { status: { $in: ["paid", "shipped"] } } }, { $sort: { orderTotal: -1 } }])"#,
        )
        .unwrap()
        .expect("shell aggregate command");

        assert_eq!(command.get_str("aggregate").unwrap(), "orders");
        assert_eq!(command.get_array("pipeline").unwrap().len(), 2);
        assert!(command.get_document("cursor").is_ok());
    }

    #[test]
    fn mongodb_shell_aggregate_explain_wraps_command() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.orders.aggregate([{ $match: { status: "paid" } }]).explain()"#,
        )
        .unwrap()
        .expect("shell aggregate explain command");

        assert_eq!(command.get_str("verbosity").unwrap(), "queryPlanner");
        let explained = command.get_document("explain").unwrap();
        assert_eq!(explained.get_str("aggregate").unwrap(), "orders");
        assert_eq!(explained.get_array("pipeline").unwrap().len(), 1);
    }

    #[test]
    fn mongodb_shell_get_collection_supports_quoted_collection_names() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.getCollection("weird names.select").find({ "emoji_😀": { $in: ["雪", "こんにちは"] } })"#,
        )
        .unwrap()
        .expect("shell getCollection command");

        assert_eq!(command.get_str("find").unwrap(), "weird names.select");
        assert!(
            command
                .get_document("filter")
                .unwrap()
                .contains_key("emoji_😀")
        );
    }

    #[test]
    fn mongodb_shell_get_sibling_db_targets_database() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.getSiblingDB("zqlz_feature_lab").orders.aggregate([{ $match: { status: "paid" } }])"#,
        )
        .unwrap()
        .expect("shell getSiblingDB command");

        assert_eq!(command.get_str("$db").unwrap(), "zqlz_feature_lab");
        assert_eq!(command.get_str("aggregate").unwrap(), "orders");
    }

    #[test]
    fn sharding_metadata_helpers_handle_config_database_shapes() {
        assert_eq!(
            MongoDbConnection::regex_escape("tenant.prod"),
            "tenant\\.prod"
        );
        assert_eq!(
            MongoDbConnection::sharded_collection_name(
                &bson::doc! { "_id": "zqlz_feature_lab.orders", "key": { "tenantId": 1 } },
                0,
            ),
            "zqlz_feature_lab.orders"
        );
        assert_eq!(
            MongoDbConnection::sharding_chunk_name(
                &bson::doc! { "_id": "chunk-a", "ns": "zqlz_feature_lab.orders", "min": { "tenantId": 1 } },
                None,
                3,
            ),
            "zqlz_feature_lab.orders chunk 3"
        );
        assert_eq!(
            MongoDbConnection::sharding_chunk_name(
                &bson::doc! { "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").expect("valid object id"), "uuid": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![1, 2, 3] } },
                Some("zqlz_feature_lab.orders"),
                4,
            ),
            "zqlz_feature_lab.orders chunk 4"
        );
        assert_eq!(
            MongoDbConnection::sharding_zone_name(
                &bson::doc! { "ns": "zqlz_feature_lab.orders", "tag": "east" },
                0,
            ),
            "zqlz_feature_lab.orders zone east"
        );
    }

    #[test]
    fn gridfs_bucket_detection_requires_matching_files_and_chunks_collections() {
        fn collection(name: &str, count: Option<u64>, size: Option<u64>) -> DocumentCollectionInfo {
            DocumentCollectionInfo {
                database: "zqlz_feature_lab".to_string(),
                name: name.to_string(),
                collection_type: "collection".to_string(),
                document_count: count,
                size_bytes: size,
                index_count: None,
                options_json: None,
                validator_json: None,
                collation_json: None,
                view_on: None,
                pipeline_json: None,
                timeseries_json: None,
                clustered_index_json: None,
                change_stream_pre_and_post_images: None,
            }
        }

        let buckets = MongoDbConnection::gridfs_buckets_from_collections(
            "zqlz_feature_lab",
            &[
                collection("media.files", Some(2), Some(512)),
                collection("media.chunks", Some(3), Some(2048)),
                collection("orphan.files", Some(1), Some(256)),
            ],
        );

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "media");
        assert_eq!(buckets[0].files_collection, "media.files");
        assert_eq!(buckets[0].chunks_collection, "media.chunks");
        assert_eq!(buckets[0].file_count, Some(2));
        assert_eq!(buckets[0].size_bytes, Some(512));
    }

    #[test]
    fn mongodb_query_result_infers_json_editor_types_for_nested_fields() {
        let result = MongoDbConnection::documents_to_query_result(
            vec![bson::doc! {
                "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").expect("valid object id"),
                "title": "Guide",
                "payload": { "kind": "guide", "stages": ["group"] },
                "tags": ["aggregation", "orders"],
                "views": 12_i32,
            }],
            0,
        );

        let data_type = |name: &str| {
            result
                .columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.data_type.as_str())
        };

        assert_eq!(data_type("_id"), Some("objectId"));
        assert_eq!(data_type("title"), Some("string"));
        assert_eq!(data_type("payload"), Some("json"));
        assert_eq!(data_type("tags"), Some("json"));
        assert_eq!(data_type("views"), Some("int"));
    }

    #[test]
    fn server_metadata_commands_cover_storage_and_read_write_concern() {
        let command_names = MongoDbConnection::server_metadata_commands()
            .into_iter()
            .filter_map(|command| command.keys().next().cloned())
            .collect::<Vec<_>>();

        assert_eq!(
            command_names,
            vec![
                "buildInfo",
                "getParameter",
                "serverStatus",
                "getDefaultRWConcern",
                "replSetGetStatus",
            ]
        );
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_create_index_command() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "collection".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers_email_unique".to_string()),
        );
        values.insert(
            "keys".to_string(),
            ObjectFormValue::String(r#"{"tenantId": 1, "email": 1}"#.to_string()),
        );
        values.insert("unique".to_string(), ObjectFormValue::Bool(true));

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_index".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"createIndexes\": \"customers\""));
        assert!(ddl[0].contains("\"name\": \"customers_email_unique\""));
        assert!(ddl[0].contains("\"unique\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_merges_additional_index_options() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "collection".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers_wildcard_idx".to_string()),
        );
        values.insert(
            "keys".to_string(),
            ObjectFormValue::String(r#"{"$**": 1}"#.to_string()),
        );
        values.insert(
            "additional_options".to_string(),
            ObjectFormValue::String(
                r#"{"wildcardProjection":{"profile":1},"hidden":true}"#.to_string(),
            ),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_index".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"wildcardProjection\": {"));
        assert!(ddl[0].contains("\"profile\": 1"));
        assert!(ddl[0].contains("\"hidden\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_preserves_view_collation() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("activeCustomers".to_string()),
        );
        values.insert(
            "view_on".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "pipeline".to_string(),
            ObjectFormValue::String(r#"[{"$match":{"active":true}}]"#.to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_view".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"create\": \"activeCustomers\""));
        assert!(ddl[0].contains("\"viewOn\": \"customers\""));
        assert!(ddl[0].contains("\"collation\": {"));
        assert!(ddl[0].contains("\"strength\": 2"));
    }

    #[tokio::test]
    async fn mongodb_object_form_does_not_emit_unsupported_collection_collation_edit() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"customers\""));
        assert!(!ddl[0].contains("\"collation\""));
    }

    #[tokio::test]
    async fn mongodb_collection_edit_form_makes_validation_changes_opt_in() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );

        let spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("collection edit spec");
        let fields = &spec.sections[0].fields;
        let validation_level = fields
            .iter()
            .find(|field| field.id == "validation_level")
            .expect("validation level field");
        let validation_action = fields
            .iter()
            .find(|field| field.id == "validation_action")
            .expect("validation action field");

        assert_eq!(
            validation_level.default_value,
            ObjectFormValue::String(String::new())
        );
        assert_eq!(
            validation_action.default_value,
            ObjectFormValue::String(String::new())
        );

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(!ddl[0].contains("\"validationLevel\""));
        assert!(!ddl[0].contains("\"validationAction\""));
    }

    #[tokio::test]
    async fn mongodb_object_form_only_changes_pre_post_images_when_requested() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(!ddl[0].contains("changeStreamPreAndPostImages"));

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "apply_change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );
        values.insert(
            "change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"changeStreamPreAndPostImages\": {"));
        assert!(ddl[0].contains("\"enabled\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_can_enable_pre_post_images_on_create() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("auditLogs".to_string()),
        );
        values.insert(
            "change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"create\": \"auditLogs\""));
        assert!(ddl[0].contains("\"changeStreamPreAndPostImages\": {"));
        assert!(ddl[0].contains("\"enabled\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_collection_collmod_special_options() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryTelemetry".to_string()),
        );
        values.insert(
            "timeseries_granularity".to_string(),
            ObjectFormValue::String("hours".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"inventoryTelemetry\""));
        assert!(ddl[0].contains("\"timeseries\": {"));
        assert!(ddl[0].contains("\"granularity\": \"hours\""));
        assert!(!ddl[0].contains("\"expireAfterSeconds\""));

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("ingestBuffer".to_string()),
        );
        values.insert(
            "expire_after_seconds".to_string(),
            ObjectFormValue::String("86400".to_string()),
        );
        values.insert(
            "capped_size".to_string(),
            ObjectFormValue::String("2097152".to_string()),
        );
        values.insert(
            "capped_max".to_string(),
            ObjectFormValue::String("10000".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"ingestBuffer\""));
        assert!(ddl[0].contains("\"expireAfterSeconds\": 86400"));
        assert!(ddl[0].contains("\"cappedSize\": 2097152"));
        assert!(ddl[0].contains("\"cappedMax\": 10000"));
    }

    #[tokio::test]
    async fn mongodb_object_form_does_not_emit_unsupported_view_collation_edit() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("activeCustomers".to_string()),
        );
        values.insert(
            "view_on".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "pipeline".to_string(),
            ObjectFormValue::String(r#"[{"$match":{"active":true}}]"#.to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_view".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"activeCustomers\""));
        assert!(!ddl[0].contains("\"collation\""));
    }

    #[tokio::test]
    async fn mongodb_object_form_requires_confirm_for_drop() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );

        let error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Drop,
                object_ref: None,
                values,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Confirm"));
    }

    #[tokio::test]
    async fn mongodb_object_form_requires_password_for_create_user() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Password is required"));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_confirmed_user_and_role_creates() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert(
            "password".to_string(),
            ObjectFormValue::String("secret".to_string()),
        );
        user_values.insert(
            "roles".to_string(),
            ObjectFormValue::String(r#"[{"role":"read","db":"app"}]"#.to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let user_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values: user_values,
            })
            .await
            .unwrap();

        assert!(user_ddl[0].contains("\"createUser\": \"reporter\""));
        assert!(user_ddl[0].contains("\"pwd\": \"secret\""));
        assert!(user_ddl[0].contains("\"role\": \"read\""));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert(
            "privileges".to_string(),
            ObjectFormValue::String(
                r#"[{"resource":{"db":"app","collection":"inventory"},"actions":["find"]}]"#
                    .to_string(),
            ),
        );
        role_values.insert(
            "roles".to_string(),
            ObjectFormValue::String("[]".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let role_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values: role_values,
            })
            .await
            .unwrap();

        assert!(role_ddl[0].contains("\"createRole\": \"inventoryReader\""));
        assert!(role_ddl[0].contains("\"actions\": ["));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_confirmed_user_and_role_updates() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert(
            "roles".to_string(),
            ObjectFormValue::String(r#"[{"role":"read","db":"app"}]"#.to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let user_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: user_values,
            })
            .await
            .unwrap();

        assert!(user_ddl[0].contains("\"updateUser\": \"reporter\""));
        assert!(user_ddl[0].contains("\"role\": \"read\""));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert(
            "privileges".to_string(),
            ObjectFormValue::String(
                r#"[{"resource":{"db":"app","collection":"inventory"},"actions":["find"]}]"#
                    .to_string(),
            ),
        );
        role_values.insert(
            "roles".to_string(),
            ObjectFormValue::String("[]".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let role_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: role_values,
            })
            .await
            .unwrap();

        assert!(role_ddl[0].contains("\"updateRole\": \"inventoryReader\""));
        assert!(role_ddl[0].contains("\"actions\": ["));
    }

    #[tokio::test]
    async fn mongodb_admin_edit_forms_do_not_default_to_empty_role_sets() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );

        let user_spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("user edit spec");
        let user_roles = user_spec.sections[0]
            .fields
            .iter()
            .find(|field| field.id == "roles")
            .expect("user roles field");
        assert_eq!(
            user_roles.default_value,
            ObjectFormValue::String(String::new())
        );

        let role_spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("role edit spec");
        for field_id in ["privileges", "roles"] {
            let field = role_spec.sections[0]
                .fields
                .iter()
                .find(|field| field.id == field_id)
                .expect("role edit field");
            assert_eq!(field.default_value, ObjectFormValue::String(String::new()));
        }

        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));
        let user_error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: user_values,
            })
            .await
            .expect_err("empty user update should fail");
        assert!(user_error.to_string().contains("user change"));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));
        let role_error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: role_values,
            })
            .await
            .expect_err("empty role update should fail");
        assert!(role_error.to_string().contains("role change"));
    }
}
