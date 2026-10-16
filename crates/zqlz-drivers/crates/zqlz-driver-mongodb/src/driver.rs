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
    ObjectFormSpec, ObjectFormSpecRequest, ObjectFormValue, ObjectKindSupport, ObjectsPanelData,
    ObjectsPanelManifest, PrimaryKeyInfo, ProcedureInfo, QueryResult, ResolvedConnectionScope,
    Result, Row, SchemaInfo, SchemaIntrospection, SequenceInfo, SqlFunctionInfo, SqlObjectName,
    StatementResult, TableDetails, TableInfo, Transaction, TriggerInfo, TypeInfo, Value, ViewInfo,
    ZqlzError, dialect_bundle_from_legacy_info,
};

use crate::objects_panel;

/// MongoDB database driver
///
/// MongoDB is a document-oriented NoSQL database that stores data in
/// flexible, JSON-like BSON documents. This driver provides connectivity
/// and query execution capabilities for MongoDB.
pub struct MongoDbDriver;

/// MongoDB is browsed through the document store path (collections, indexes,
/// GridFS buckets, etc.); the relational object kinds surfaced via
/// `list_tables`/`list_views` are intentionally left empty.
pub(crate) const OBJECT_KINDS: ObjectKindSupport = ObjectKindSupport::NONE;

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

    fn object_kinds(&self) -> ObjectKindSupport {
        OBJECT_KINDS
    }

    fn default_schema_name(&self) -> Option<&'static str> {
        None
    }
}

/// MongoDB connection wrapper implementing the Connection trait
/// Liveness command issued by connection heartbeats.
const MONGODB_PING_COMMAND: &str = r#"{"ping": 1}"#;

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

mod connection_helpers;
mod connection_trait;
mod dialect;
mod document_store;
mod schema_introspection;
#[cfg(test)]
mod tests;

pub use connection_helpers::MongoDbConnection;
pub use dialect::mongodb_dialect;
