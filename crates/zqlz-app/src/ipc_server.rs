//! IPC server for the `zqlz` CLI.
//!
//! Listens on a platform-specific endpoint and dispatches requests from the CLI.
//! Wire protocol: 4-byte big-endian u32 length prefix + JSON payload.

#[cfg(unix)]
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use gpui::{Context, Window};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(windows)]
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
#[cfg(windows)]
use tokio::time::{Duration, sleep};
use uuid::Uuid;
use zqlz_connection::SavedConnection;
use zqlz_core::Connection;
use zqlz_query::{QueryHistory, QueryService};
use zqlz_services::ConnectionService;

use crate::main_view::MainView;
use crate::storage::LocalStorage;

// ---------------------------------------------------------------------------
// Protocol types (must match zqlz-cli/src/ipc.rs, plus app-only variants)
// ---------------------------------------------------------------------------

/// A single column in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ColumnMeta {
    name: String,
    data_type: String,
}

/// A single row in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Row {
    /// Display text per cell; kept for CLIs that predate `cells`.
    values: Vec<String>,
    /// Typed cells (SQL NULL is `null`, arrays and JSON are structured).
    #[serde(default)]
    cells: Vec<serde_json::Value>,
}

/// Result of a single SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StatementResult {
    sql: String,
    duration_ms: u64,
    columns: Vec<ColumnMeta>,
    rows: Vec<Row>,
    affected_rows: u64,
    error: Option<String>,
    /// Server notices (`RAISE NOTICE`, ...) raised by the statement.
    #[serde(default)]
    warnings: Vec<String>,
    /// Not run because an earlier statement failed and `continue_on_error` was off.
    #[serde(default)]
    skipped: bool,
}

/// Result of executing one or more SQL statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueryExecution {
    sql: String,
    duration_ms: u64,
    statements: Vec<StatementResult>,
}

/// A single query history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HistoryEntry {
    id: Uuid,
    sql: String,
    connection_id: Option<Uuid>,
    executed_at: chrono::DateTime<chrono::Utc>,
    duration_ms: u64,
    row_count: Option<u64>,
    success: bool,
    error: Option<String>,
}

/// Column description for schema browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
    default_value: Option<String>,
    is_primary_key: bool,
}

/// One table or view in a listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RelationEntry {
    pub name: String,
    /// `table`, `view` or `materialized view`.
    pub kind: String,
}

/// Summary of a saved connection (no credentials).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConnectionSummary {
    id: Uuid,
    name: String,
    driver: String,
    host: Option<String>,
    port: Option<String>,
    database: Option<String>,
    username: Option<String>,
    folder: Option<String>,
    color: Option<String>,
}

impl ConnectionSummary {
    fn from_saved(conn: &SavedConnection) -> Self {
        Self {
            id: conn.id,
            name: conn.name.clone(),
            driver: conn.driver.clone(),
            host: conn.params.get("host").cloned(),
            port: conn.params.get("port").cloned(),
            database: conn.params.get("database").cloned(),
            username: conn.params.get("username").cloned(),
            folder: conn.folder.clone(),
            color: conn.color.clone(),
        }
    }
}

/// Requests sent by the CLI.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
enum Request {
    ListConnections,
    SaveConnection(SavedConnection),
    DeleteConnection(Uuid),
    TestConnection(String),
    ExecuteQuery {
        connection: String,
        database: Option<String>,
        sql: String,
        /// Keep running the remaining statements after one fails. Absent or
        /// `false` stops at the first failure and reports the rest as skipped.
        #[serde(default)]
        continue_on_error: Option<bool>,
    },
    ListDatabases(String),
    ListTables {
        connection: String,
        database: Option<String>,
        /// Answer with [`Response::Relations`] (tables and views, each with its
        /// kind) instead of table names only. Absent from older CLIs.
        #[serde(default)]
        include_views: bool,
    },
    ListColumns {
        connection: String,
        database: Option<String>,
        table: String,
    },
    QueryHistory {
        connection: Option<String>,
        limit: usize,
        search: Option<String>,
    },
    QueryHistoryEntry {
        id: Uuid,
    },
    OpenTargets {
        targets: Vec<String>,
    },
    /// Invoke one MCP tool against the running GUI.
    AgentCall {
        name: String,
        arguments: serde_json::Value,
    },
}

/// Responses sent back to the CLI.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub(crate) enum Response {
    Ok,
    Error(String),
    Connections(Vec<ConnectionSummary>),
    QueryResult(QueryExecution),
    StringList(Vec<String>),
    /// Tables and views with their kind; only sent when the request asked.
    Relations(Vec<RelationEntry>),
    Columns(Vec<ColumnInfo>),
    History(Vec<HistoryEntry>),
    HistoryEntry(HistoryEntry),
    Json(serde_json::Value),
}

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------

/// Work the IPC thread hands to the main window; runs on the GPUI foreground thread.
pub type UiJob = Box<dyn FnOnce(&mut MainView, &mut Window, &mut Context<MainView>) + Send>;
pub type UiJobSender = futures::channel::mpsc::UnboundedSender<UiJob>;
pub type UiJobReceiver = futures::channel::mpsc::UnboundedReceiver<UiJob>;

/// Cloned handles to the `Arc` fields of `AppState` that the IPC server needs.
///
/// All fields are `Arc<…>` so this struct is `Send + Sync` and can safely be
/// moved into a background Tokio task without pulling in GPUI's `Global` bound.
/// UI work goes through `ui_jobs` instead.
#[derive(Clone)]
pub struct IpcServerHandle {
    pub connection_service: Arc<ConnectionService>,
    pub query_service: Arc<QueryService>,
    pub query_history: Arc<RwLock<QueryHistory>>,
    pub storage: Arc<LocalStorage>,
    pub ui_jobs: UiJobSender,
}

impl IpcServerHandle {
    pub(crate) fn post_to_ui(
        &self,
        job: impl FnOnce(&mut MainView, &mut Window, &mut Context<MainView>) + Send + 'static,
    ) -> Result<(), String> {
        self.ui_jobs
            .unbounded_send(Box::new(job))
            .map_err(|_| "The ZQLZ window is not open.".to_string())
    }

    /// Runs `job` on the main window and waits for what it returns.
    pub(crate) async fn on_ui<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut MainView, &mut Window, &mut Context<MainView>) -> T + Send + 'static,
    ) -> Result<T, String> {
        let (reply, reply_receiver) = futures::channel::oneshot::channel();
        self.post_to_ui(move |main_view, window, cx| {
            if reply.send(job(main_view, window, cx)).is_err() {
                tracing::debug!("IPC client went away before its UI job finished");
            }
        })?;
        reply_receiver
            .await
            .map_err(|_| "The ZQLZ window closed before the request finished.".to_string())
    }
}

/// Default IPC endpoint.
///
/// On Unix this is `~/.config/zqlz/ipc.sock`.
/// On Windows this is a per-user named pipe path derived from the config dir.
pub fn default_socket_path() -> Result<PathBuf> {
    zqlz_core::paths::ipc_endpoint().context("could not determine canonical IPC endpoint path")
}

/// Start the IPC server, listening on `socket_path`.
///
/// Spawns a background OS thread that owns its own single-threaded Tokio
/// runtime. GPUI has no Tokio runtime on its main thread, so we cannot call
/// `tokio::spawn` directly from `app.run()`. The background thread is
/// intentionally fire-and-forget: it lives for the lifetime of the process.
pub fn start(handle: IpcServerHandle, socket_path: PathBuf) {
    if let Err(e) = std::thread::Builder::new()
        .name("ipc-server".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("Failed to build IPC server Tokio runtime: {}", e);
                    return;
                }
            };
            runtime.block_on(async move {
                if let Err(e) = run_server(handle, socket_path).await {
                    tracing::error!("IPC server error: {}", e);
                }
            });
        })
    {
        tracing::error!("Failed to spawn IPC server thread: {}", e);
    }
}

#[cfg(unix)]
async fn run_server(handle: IpcServerHandle, socket_path: PathBuf) -> Result<()> {
    // Remove stale socket file if it exists
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("removing stale socket at {}", socket_path.display()))?;
    }

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating socket directory {}", parent.display()))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding IPC socket at {}", socket_path.display()))?;

    tracing::info!(socket = %socket_path.display(), "IPC server listening");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let handle = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, handle).await {
                        tracing::warn!("IPC connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                tracing::error!("IPC accept error: {}", e);
            }
        }
    }
}

#[cfg(windows)]
async fn run_server(handle: IpcServerHandle, socket_path: PathBuf) -> Result<()> {
    let mut listener = create_windows_listener(&socket_path, true)
        .with_context(|| format!("binding IPC named pipe at {}", socket_path.display()))?;

    tracing::info!(pipe = %socket_path.display(), "IPC server listening");

    loop {
        listener.connect().await.with_context(|| {
            format!(
                "accepting IPC named pipe connection at {}",
                socket_path.display()
            )
        })?;

        let connected_stream = listener;
        listener = create_windows_listener(&socket_path, false).with_context(|| {
            format!(
                "preparing next IPC named pipe listener at {}",
                socket_path.display()
            )
        })?;

        let handle = handle.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(connected_stream, handle).await {
                tracing::warn!("IPC connection error: {}", e);
            }
        });
    }
}

#[cfg(windows)]
fn create_windows_listener(
    socket_path: &std::path::Path,
    first_instance: bool,
) -> std::io::Result<NamedPipeServer> {
    let mut options = ServerOptions::new();
    if first_instance {
        options.first_pipe_instance(true);
    }
    options.create(socket_path)
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

async fn handle_connection<S>(mut stream: S, handle: IpcServerHandle) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_request(&mut stream).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);
    // Clients send one request per connection and then only wait, so the
    // stream becoming readable means the caller hung up (Ctrl-C, an MCP
    // cancel). Dropping `dispatch` withdraws the work, including a pending
    // approval dialog.
    let mut probe = [0u8; 1];
    let response = tokio::select! {
        response = dispatch(request, &handle) => response,
        _ = reader.read(&mut probe) => {
            tracing::debug!("IPC client disconnected before its request finished");
            return Ok(());
        }
    };
    write_response(&mut writer, &response).await?;
    Ok(())
}

async fn read_request<S>(stream: &mut S) -> Result<Request>
where
    S: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading IPC request length")?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 64 * 1024 * 1024 {
        anyhow::bail!("IPC request too large ({} bytes)", len);
    }

    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .context("reading IPC request payload")?;

    serde_json::from_slice(&payload).context("deserializing IPC request")
}

async fn write_request<S>(stream: &mut S, request: &Request) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(request).context("serializing IPC request")?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .context("writing IPC request length")?;
    stream
        .write_all(&payload)
        .await
        .context("writing IPC request payload")?;
    Ok(())
}

async fn write_response<S>(stream: &mut S, response: &Response) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(response).context("serializing IPC response")?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .context("writing IPC response length")?;
    stream
        .write_all(&payload)
        .await
        .context("writing IPC response payload")?;
    Ok(())
}

async fn read_response<S>(stream: &mut S) -> Result<Response>
where
    S: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading IPC response length")?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 64 * 1024 * 1024 {
        anyhow::bail!("IPC response too large ({} bytes)", len);
    }

    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .context("reading IPC response payload")?;

    serde_json::from_slice(&payload).context("deserializing IPC response")
}

// ---------------------------------------------------------------------------
// Request dispatch
// ---------------------------------------------------------------------------

async fn dispatch(request: Request, handle: &IpcServerHandle) -> Response {
    match request {
        Request::ListConnections => {
            let summaries = handle
                .connection_service
                .list_saved_connections()
                .iter()
                .map(ConnectionSummary::from_saved)
                .collect();
            Response::Connections(summaries)
        }

        Request::SaveConnection(saved) => {
            handle
                .connection_service
                .persist_saved_connection(saved.clone());
            if let Err(e) = handle.storage.save_connection(&saved) {
                tracing::error!("Failed to persist connection via IPC: {}", e);
            }
            Response::Ok
        }

        Request::DeleteConnection(id) => {
            handle.connection_service.remove_saved_connection(id);
            if let Err(e) = handle.storage.delete_connection(id) {
                tracing::error!("Failed to delete connection from storage via IPC: {}", e);
            }
            Response::Ok
        }

        Request::TestConnection(name_or_id) => match find_saved(handle, &name_or_id) {
            None => Response::Error(format!("no connection matching '{}'", name_or_id)),
            Some(saved) => match handle
                .connection_service
                .manager()
                .test_saved(saved.id)
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => Response::Error(e.to_string()),
            },
        },

        Request::ExecuteQuery {
            connection,
            database,
            sql,
            continue_on_error,
        } => {
            let (saved, request_connection) =
                match acquire_named(handle, &connection, database, true).await {
                    Ok(acquired) => acquired,
                    Err(response) => return response,
                };

            let execution_result = handle
                .query_service
                .execute_query(
                    request_connection.connection.clone(),
                    saved.id,
                    &sql,
                    continue_on_error.unwrap_or(false),
                )
                .await;
            request_connection.release().await;
            match execution_result {
                Ok(execution) => {
                    let statements = execution
                        .statements
                        .into_iter()
                        .map(|s| {
                            let (columns, rows) = if let Some(result) = s.result {
                                let columns = result
                                    .columns
                                    .iter()
                                    .map(|c| ColumnMeta {
                                        name: c.name.clone(),
                                        data_type: c.data_type.clone(),
                                    })
                                    .collect();
                                let rows = result
                                    .rows
                                    .iter()
                                    .map(|r| Row {
                                        values: r.values.iter().map(|v| v.to_string()).collect(),
                                        cells: r.values.iter().map(|v| v.to_wire_json()).collect(),
                                    })
                                    .collect();
                                (columns, rows)
                            } else {
                                (Vec::new(), Vec::new())
                            };
                            StatementResult {
                                sql: s.sql,
                                duration_ms: s.duration_ms,
                                columns,
                                rows,
                                affected_rows: s.affected_rows,
                                error: s.error,
                                warnings: s.warnings,
                                skipped: s.skipped,
                            }
                        })
                        .collect();

                    Response::QueryResult(QueryExecution {
                        sql: execution.sql,
                        duration_ms: execution.duration_ms,
                        statements,
                    })
                }
                Err(e) => Response::Error(e.to_string()),
            }
        }

        Request::ListDatabases(name_or_id) => {
            let (_, request_connection) = match acquire_named(handle, &name_or_id, None, false).await {
                Ok(acquired) => acquired,
                Err(response) => return response,
            };

            let response = match request_connection.connection.as_schema_introspection() {
                None => Response::Error("driver does not support schema introspection".to_string()),
                Some(schema) => match schema.list_databases().await {
                    Ok(databases) => {
                        Response::StringList(databases.into_iter().map(|db| db.name).collect())
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
            };
            request_connection.release().await;
            response
        }

        Request::ListTables {
            connection,
            database,
            include_views,
        } => {
            if include_views {
                list_relation_entries(handle, &connection, database).await
            } else {
                list_tables(handle, &connection, database).await
            }
        }

        Request::ListColumns {
            connection,
            database,
            table,
        } => list_columns(handle, &connection, database, &table).await,

        Request::QueryHistory {
            connection,
            limit,
            search,
        } => {
            let history = handle.query_history.read();

            let connection_id_filter: Option<Uuid> = connection.as_deref().and_then(|name_or_id| {
                if let Ok(id) = Uuid::parse_str(name_or_id) {
                    return Some(id);
                }
                handle
                    .connection_service
                    .list_saved_connections()
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name_or_id))
                    .map(|c| c.id)
            });

            let entries: Vec<HistoryEntry> = if let Some(search_str) = search.as_deref() {
                history
                    .search(search_str)
                    .filter(|e| {
                        connection_id_filter
                            .map(|id| e.connection_id == Some(id))
                            .unwrap_or(true)
                    })
                    .take(limit)
                    .map(history_entry_to_wire)
                    .collect()
            } else if let Some(conn_id) = connection_id_filter {
                history
                    .for_connection(conn_id)
                    .take(limit)
                    .map(history_entry_to_wire)
                    .collect()
            } else {
                history
                    .entries()
                    .take(limit)
                    .map(history_entry_to_wire)
                    .collect()
            };

            Response::History(entries)
        }

        Request::QueryHistoryEntry { id } => {
            let history = handle.query_history.read();
            match history.entries().find(|entry| entry.id == id) {
                Some(entry) => Response::HistoryEntry(history_entry_to_wire(entry)),
                None => Response::Error(format!("query history entry '{}' not found", id)),
            }
        }

        Request::OpenTargets { targets } => {
            let paths: Vec<PathBuf> = targets
                .iter()
                .filter_map(|target| crate::path_from_open_target(target))
                .collect();
            if paths.is_empty() {
                return Response::Ok;
            }
            match handle.post_to_ui(move |main_view, window, cx| {
                for path in &paths {
                    main_view.open_external_path(path, window, cx);
                }
            }) {
                Ok(()) => Response::Ok,
                Err(error) => Response::Error(error),
            }
        }

        Request::AgentCall { name, arguments } => {
            match crate::agent_tools::call(handle, &name, arguments).await {
                Ok(value) => Response::Json(value),
                Err(error) => Response::Error(error),
            }
        }
    }
}

/// Drivers where a "database" is a schema on the one server connection rather
/// than a connection of its own (MySQL): the database named in a request scopes
/// introspection, and queries against it need a session of their own.
pub(crate) fn schema_scoped(driver_name: &str) -> bool {
    driver_name == "mysql"
}

/// Start of the error for a database the server does not have.
pub(crate) const UNKNOWN_DATABASE_PREFIX: &str = "Unknown database";

/// `Err("Unknown database ...")` when `scope` names a database the server does
/// not have. Lists databases, so only call it once a request has already come
/// back empty or failed.
async fn check_database_exists(
    schema: &dyn zqlz_core::SchemaIntrospection,
    scope: Option<&str>,
) -> Result<(), String> {
    let Some(scope) = scope else { return Ok(()) };
    match schema.list_databases().await {
        Ok(databases) if !databases.iter().any(|db| db.name.eq_ignore_ascii_case(scope)) => {
            Err(format!(
                "{UNKNOWN_DATABASE_PREFIX} '{scope}'. Available: {}.",
                databases
                    .iter()
                    .map(|db| db.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
        _ => Ok(()),
    }
}

/// Tables and views of one database/schema.
pub(crate) struct Relations {
    pub tables: Vec<String>,
    pub views: Vec<RelationEntry>,
}

impl Relations {
    pub(crate) fn names(&self) -> Vec<String> {
        let views = self.views.iter().map(|view| view.name.clone());
        self.tables.iter().cloned().chain(views).collect()
    }
}

async fn load_relations(
    schema: &dyn zqlz_core::SchemaIntrospection,
    scope: Option<&str>,
) -> Result<Relations, String> {
    let tables: Vec<String> = schema
        .list_tables(scope)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|t| t.name)
        .collect();
    let mut views: Vec<RelationEntry> = Vec::new();
    let listed = schema.list_views(scope).await.map_err(|e| e.to_string())?;
    let matviews = schema
        .list_materialized_views(scope)
        .await
        .map_err(|e| e.to_string())?;
    for view in listed.into_iter().chain(matviews) {
        if tables.contains(&view.name) || views.iter().any(|seen| seen.name == view.name) {
            continue;
        }
        let kind = if view.is_materialized { "materialized view" } else { "view" };
        views.push(RelationEntry { name: view.name, kind: kind.to_string() });
    }
    if tables.is_empty() && views.is_empty() {
        check_database_exists(schema, scope).await?;
    }
    Ok(Relations { tables, views })
}

pub(crate) async fn list_tables(
    handle: &IpcServerHandle,
    connection: &str,
    database: Option<String>,
) -> Response {
    let (_, request_connection) = match acquire_named(handle, connection, database, false).await {
        Ok(acquired) => acquired,
        Err(response) => return response,
    };

    let response = match request_connection.connection.as_schema_introspection() {
        None => Response::Error("driver does not support schema introspection".to_string()),
        Some(schema) => match schema.list_tables(request_connection.schema()).await {
            Ok(tables) if tables.is_empty() => {
                match check_database_exists(schema, request_connection.schema()).await {
                    Ok(()) => Response::StringList(Vec::new()),
                    Err(error) => Response::Error(error),
                }
            }
            Ok(tables) => Response::StringList(tables.into_iter().map(|t| t.name).collect()),
            Err(e) => Response::Error(e.to_string()),
        },
    };
    request_connection.release().await;
    response
}

/// Tables and views of a database, for `schema tables --views` and
/// `describe_schema`. `Ok(None)` when the driver is not relational or cannot
/// introspect.
pub(crate) async fn list_relations(
    handle: &IpcServerHandle,
    connection: &str,
    database: Option<String>,
) -> Result<Option<Relations>, String> {
    let (_, request_connection) = match acquire_named(handle, connection, database, false).await {
        Ok(acquired) => acquired,
        Err(Response::Error(error)) => return Err(error),
        Err(_) => return Err("unexpected response while connecting".to_string()),
    };

    let conn = &request_connection.connection;
    let result = match conn.as_schema_introspection() {
        Some(schema) if conn.driver_category() == zqlz_core::DriverCategory::Relational => {
            load_relations(schema, request_connection.schema())
                .await
                .map(Some)
        }
        _ => Ok(None),
    };
    request_connection.release().await;
    result
}

async fn list_relation_entries(
    handle: &IpcServerHandle,
    connection: &str,
    database: Option<String>,
) -> Response {
    match list_relations(handle, connection, database.clone()).await {
        Ok(Some(relations)) => {
            let tables = relations.tables.into_iter().map(|name| RelationEntry {
                name,
                kind: "table".to_string(),
            });
            Response::Relations(tables.chain(relations.views).collect())
        }
        // Not relational: whatever the driver calls tables.
        Ok(None) => match list_tables(handle, connection, database).await {
            Response::StringList(names) => Response::Relations(
                names
                    .into_iter()
                    .map(|name| RelationEntry { name, kind: "table".to_string() })
                    .collect(),
            ),
            other => other,
        },
        Err(error) => Response::Error(error),
    }
}

/// Where a requested table lives: a qualifier in the name wins, otherwise the
/// request's database or schema. Text that is not a name path is taken whole.
fn table_scope(request_connection: &RequestConnection, table: &str) -> (Option<String>, String) {
    let request_scope = || request_connection.schema().map(str::to_string);
    match zqlz_core::TableRef::parse(table, request_connection.connection.dialect_id(), None) {
        Ok(parsed) => (parsed.schema.or_else(request_scope), parsed.name),
        Err(_) => (request_scope(), table.to_string()),
    }
}

pub(crate) async fn list_columns(
    handle: &IpcServerHandle,
    connection: &str,
    database: Option<String>,
    table: &str,
) -> Response {
    let (_, request_connection) = match acquire_named(handle, connection, database, false).await {
        Ok(acquired) => acquired,
        Err(response) => return response,
    };

    let (scope, table) = table_scope(&request_connection, table);
    let response = match request_connection.connection.as_schema_introspection() {
        None => Response::Error("driver does not support schema introspection".to_string()),
        Some(schema) => match schema.get_table(scope.as_deref(), &table).await {
            Ok(table_info) => Response::Columns(
                table_info
                    .columns
                    .into_iter()
                    .map(|col| ColumnInfo {
                        is_primary_key: col.is_primary_key,
                        name: col.name,
                        data_type: col.data_type,
                        nullable: col.nullable,
                        default_value: col.default_value,
                    })
                    .collect(),
            ),
            Err(e) => match check_database_exists(schema, scope.as_deref()).await {
                Err(unknown) => Response::Error(unknown),
                Ok(()) => Response::Error(e.to_string()),
            },
        },
    };
    request_connection.release().await;
    response
}

/// Columns, keys, indexes and foreign keys of one table or view, for
/// `describe_schema`. Everything the driver reports is passed through; fields
/// it cannot fill (an empty `data_type`, say) stay empty rather than invented.
pub(crate) async fn describe_table(
    handle: &IpcServerHandle,
    connection: &str,
    database: Option<String>,
    table: &str,
) -> Result<serde_json::Value, String> {
    let (_, request_connection) = match acquire_named(handle, connection, database, false).await {
        Ok(acquired) => acquired,
        Err(Response::Error(error)) => return Err(error),
        Err(_) => return Err("unexpected response while connecting".to_string()),
    };
    let (scope, table) = table_scope(&request_connection, table);
    let result = async {
        let schema = request_connection
            .connection
            .as_schema_introspection()
            .ok_or_else(|| "driver does not support schema introspection".to_string())?;
        let details = match schema.get_table(scope.as_deref(), &table).await {
            Ok(details) => details,
            Err(error) => {
                check_database_exists(schema, scope.as_deref()).await?;
                return Err(error.to_string());
            }
        };
        let is_view = schema
            .list_views(scope.as_deref())
            .await
            .map(|views| views.iter().any(|view| view.name == table))
            .unwrap_or(false);
        let columns: Vec<serde_json::Value> = details
            .columns
            .iter()
            .map(|column| {
                serde_json::json!({
                    "name": column.name,
                    "data_type": column.data_type,
                    "nullable": column.nullable,
                    "default_value": column.default_value,
                    "is_primary_key": column.is_primary_key,
                    "is_auto_increment": column.is_auto_increment,
                    "is_unique": column.is_unique,
                    "comment": column.comment,
                    "generation_expression": column.generation_expression,
                })
            })
            .collect();
        Ok(serde_json::json!({
            "table": table,
            "database": scope,
            "kind": if is_view { "view" } else { "table" },
            "columns": columns,
            "primary_key": details.primary_key,
            "indexes": details.indexes,
            "foreign_keys": details.foreign_keys,
        }))
    }
    .await;
    request_connection.release().await;
    result
}

/// Try to forward launch/open targets to an already running instance.
///
/// Returns:
/// - `Ok(true)` when targets were forwarded and acknowledged.
/// - `Ok(false)` when no IPC server is available.
/// - `Err(..)` for framing/protocol or other connection failures.
pub fn try_forward_open_targets(socket_path: &Path, targets: Vec<String>) -> Result<bool> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building Tokio runtime for IPC handoff")?;

    runtime.block_on(async move { try_forward_open_targets_async(socket_path, targets).await })
}

async fn try_forward_open_targets_async(socket_path: &Path, targets: Vec<String>) -> Result<bool> {
    if targets.is_empty() {
        return Ok(true);
    }

    let request = Request::OpenTargets { targets };

    #[cfg(unix)]
    {
        match UnixStream::connect(socket_path).await {
            Ok(mut stream) => return send_open_targets_request(&mut stream, request).await,
            Err(error) if is_unix_server_unavailable(&error) => return Ok(false),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("connecting to IPC socket at {}", socket_path.display())
                });
            }
        }
    }

    #[cfg(windows)]
    {
        match connect_windows_client(socket_path).await? {
            Some(mut stream) => return send_open_targets_request(&mut stream, request).await,
            None => return Ok(false),
        }
    }

    #[allow(unreachable_code)]
    Ok(false)
}

async fn send_open_targets_request<S>(stream: &mut S, request: Request) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_request(stream, &request).await?;
    let response = read_response(stream).await?;
    match response {
        Response::Ok => Ok(true),
        Response::Error(message) => Err(anyhow::anyhow!(
            "IPC server rejected OpenTargets: {message}"
        )),
        _ => Err(anyhow::anyhow!(
            "IPC server returned unexpected response to OpenTargets"
        )),
    }
}

#[cfg(unix)]
fn is_unix_server_unavailable(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::NotFound | ErrorKind::ConnectionRefused
    )
}

#[cfg(windows)]
async fn connect_windows_client(socket_path: &Path) -> Result<Option<NamedPipeClient>> {
    const PIPE_BUSY: i32 = 231;
    const FILE_NOT_FOUND: i32 = 2;

    for _ in 0..20 {
        match ClientOptions::new().open(socket_path) {
            Ok(client) => return Ok(Some(client)),
            Err(error) if error.raw_os_error() == Some(FILE_NOT_FOUND) => return Ok(None),
            Err(error) if error.raw_os_error() == Some(PIPE_BUSY) => {
                sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }

    Err(anyhow::anyhow!(
        "timed out waiting for IPC named pipe availability"
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The connection serving one IPC request.
///
/// When the GUI already has the saved connection open, IPC borrows that live
/// session instead of replacing it (the manager keys sessions by saved id, so
/// reconnecting would silently swap the user's connection). Otherwise it opens
/// a private connection that is closed when the request finishes.
struct RequestConnection {
    connection: Arc<dyn Connection>,
    owned: bool,
    /// Database to introspect on a schema-scoped driver (see [`schema_scoped`]);
    /// the borrowed session cannot be told to switch database.
    schema: Option<String>,
}

impl RequestConnection {
    fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// `for_query`: the request runs SQL, so a requested database needs a
    /// session of its own on schema-scoped drivers (introspection only needs to
    /// name it).
    async fn acquire(
        handle: &IpcServerHandle,
        saved: &SavedConnection,
        database: Option<String>,
        for_query: bool,
    ) -> Result<Self, String> {
        let connection_service = &handle.connection_service;
        if connection_service.is_connection_active(saved.id) {
            let resolved = connection_service
                .resolve_database_scoped_connection(saved.id, database.clone())
                .await
                .map_err(|e| e.to_string())?;
            if schema_scoped(&resolved.driver_name)
                && let Some(database) = database
            {
                if for_query {
                    let connection =
                        open_database_session(connection_service, saved.id, &database).await?;
                    return Ok(Self { connection, owned: true, schema: None });
                }
                return Ok(Self {
                    connection: resolved.connection,
                    owned: false,
                    schema: Some(database),
                });
            }
            return Ok(Self {
                connection: resolved.connection,
                owned: false,
                schema: None,
            });
        }

        let mut saved = saved.clone();
        if let Some(database) = database {
            saved.params.insert("database".to_string(), database);
        }
        let connection = connection_service
            .manager()
            .open_unregistered(&saved)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self {
            connection,
            owned: true,
            schema: None,
        })
    }

    async fn release(self) {
        if self.owned
            && let Err(error) = self.connection.close().await
        {
            tracing::warn!(%error, "failed to close IPC request connection");
        }
    }
}

/// A private session on `database` of a saved connection, for SQL that must
/// run there without disturbing the GUI's shared session. Unknown databases
/// fail here instead of silently running against the default one.
pub(crate) async fn open_database_session(
    connection_service: &ConnectionService,
    connection_id: Uuid,
    database: &str,
) -> Result<Arc<dyn Connection>, String> {
    let mut saved = connection_service
        .get_saved_connection(connection_id)
        .map_err(|e| e.to_string())?;
    saved
        .params
        .insert("database".to_string(), database.to_string());
    connection_service
        .manager()
        .open_unregistered(&saved)
        .await
        .map_err(|error| {
            let text = error.to_string();
            if text.contains("1049") {
                format!("{UNKNOWN_DATABASE_PREFIX} '{database}'.")
            } else {
                text
            }
        })
}

async fn acquire_named(
    handle: &IpcServerHandle,
    name_or_id: &str,
    database: Option<String>,
    for_query: bool,
) -> Result<(SavedConnection, RequestConnection), Response> {
    let saved = find_saved(handle, name_or_id)
        .ok_or_else(|| Response::Error(format!("no connection matching '{}'", name_or_id)))?;
    let request_connection = RequestConnection::acquire(handle, &saved, database, for_query)
        .await
        .map_err(Response::Error)?;
    Ok((saved, request_connection))
}

/// Find a saved connection by name (case-insensitive) or UUID.
pub(crate) fn find_saved(handle: &IpcServerHandle, name_or_id: &str) -> Option<SavedConnection> {
    let connections = handle.connection_service.list_saved_connections();
    if let Ok(id) = Uuid::parse_str(name_or_id)
        && let Some(conn) = connections.iter().find(|c| c.id == id)
    {
        return Some(conn.clone());
    }
    connections
        .into_iter()
        .find(|c| c.name.eq_ignore_ascii_case(name_or_id))
}

fn history_entry_to_wire(e: &zqlz_query::QueryHistoryEntry) -> HistoryEntry {
    HistoryEntry {
        id: e.id,
        sql: e.sql.clone(),
        connection_id: e.connection_id,
        executed_at: e.executed_at,
        duration_ms: e.duration_ms,
        row_count: e.row_count,
        success: e.success,
        error: e.error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_query_request_stops_on_error_unless_the_field_says_otherwise() {
        let parse = |json: &str| match serde_json::from_str::<Request>(json).unwrap() {
            Request::ExecuteQuery {
                continue_on_error, ..
            } => continue_on_error,
            _ => panic!("expected ExecuteQuery"),
        };
        let base = r#""type":"ExecuteQuery","payload":{"connection":"c","database":null,"sql":"SELECT 1""#;

        assert_eq!(parse(&format!("{{{base}}}}}")), None);
        assert_eq!(
            parse(&format!("{{{base},\"continue_on_error\":true}}}}")),
            Some(true)
        );
    }

    #[test]
    fn statement_result_without_skipped_field_defaults_to_not_skipped() {
        let statement: StatementResult = serde_json::from_str(
            r#"{"sql":"SELECT 1","duration_ms":0,"columns":[],"rows":[],"affected_rows":0,"error":null}"#,
        )
        .unwrap();
        assert!(!statement.skipped);
    }
}
