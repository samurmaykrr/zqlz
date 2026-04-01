//! IPC server for the `zqlz` CLI.
//!
//! Listens on a platform-specific endpoint and dispatches requests from the CLI.
//! Wire protocol: 4-byte big-endian u32 length prefix + JSON payload.

#[cfg(unix)]
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
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
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_query::{QueryHistory, QueryService};

use crate::storage::LocalStorage;

// ---------------------------------------------------------------------------
// Protocol types (must match zqlz-cli/src/ipc.rs, plus app-only variants)
// ---------------------------------------------------------------------------

/// A single column in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnMeta {
    name: String,
    data_type: String,
}

/// A single row in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Row {
    values: Vec<String>,
}

/// Result of a single SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatementResult {
    sql: String,
    duration_ms: u64,
    columns: Vec<ColumnMeta>,
    rows: Vec<Row>,
    affected_rows: u64,
    error: Option<String>,
}

/// Result of executing one or more SQL statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryExecution {
    sql: String,
    duration_ms: u64,
    statements: Vec<StatementResult>,
}

/// A single query history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryEntry {
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
struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
    default_value: Option<String>,
    is_primary_key: bool,
}

/// Summary of a saved connection (no credentials).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectionSummary {
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
    },
    ListDatabases(String),
    ListTables {
        connection: String,
        database: Option<String>,
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
}

/// Responses sent back to the CLI.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
enum Response {
    Ok,
    Error(String),
    Connections(Vec<ConnectionSummary>),
    QueryResult(QueryExecution),
    StringList(Vec<String>),
    Columns(Vec<ColumnInfo>),
    History(Vec<HistoryEntry>),
    HistoryEntry(HistoryEntry),
}

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------

/// Cloned handles to the `Arc` fields of `AppState` that the IPC server needs.
///
/// All fields are `Arc<…>` so this struct is `Send + Sync` and can safely be
/// moved into a background Tokio task without pulling in GPUI's `Global` bound.
#[derive(Clone)]
pub struct IpcServerHandle {
    pub connections: Arc<ConnectionManager>,
    pub query_service: Arc<QueryService>,
    pub query_history: Arc<RwLock<QueryHistory>>,
    pub storage: Arc<LocalStorage>,
}

pub type OpenTargetsQueue = Arc<RwLock<Vec<String>>>;

static OPEN_TARGETS_QUEUE: OnceLock<OpenTargetsQueue> = OnceLock::new();

fn open_targets_queue() -> OpenTargetsQueue {
    OPEN_TARGETS_QUEUE
        .get_or_init(|| Arc::new(RwLock::new(Vec::new())))
        .clone()
}

impl IpcServerHandle {
    pub fn open_targets_queue(&self) -> OpenTargetsQueue {
        open_targets_queue()
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
    let response = dispatch(request, &handle).await;
    write_response(&mut stream, &response).await?;
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
                .connections
                .saved_connections()
                .iter()
                .map(ConnectionSummary::from_saved)
                .collect();
            Response::Connections(summaries)
        }

        Request::SaveConnection(saved) => {
            handle.connections.add_saved(saved.clone());
            if let Err(e) = handle.storage.save_connection(&saved) {
                tracing::error!("Failed to persist connection via IPC: {}", e);
            }
            Response::Ok
        }

        Request::DeleteConnection(id) => {
            handle.connections.remove_saved(id);
            if let Err(e) = handle.storage.delete_connection(id) {
                tracing::error!("Failed to delete connection from storage via IPC: {}", e);
            }
            Response::Ok
        }

        Request::TestConnection(name_or_id) => match find_saved(handle, &name_or_id) {
            None => Response::Error(format!("no connection matching '{}'", name_or_id)),
            Some(saved) => match handle.connections.connect(&saved).await {
                Ok(conn_id) => {
                    let _ = handle.connections.disconnect(conn_id).await;
                    Response::Ok
                }
                Err(e) => Response::Error(e.to_string()),
            },
        },

        Request::ExecuteQuery {
            connection,
            database,
            sql,
        } => {
            let saved = match find_saved(handle, &connection) {
                Some(s) => s,
                None => return Response::Error(format!("no connection matching '{}'", connection)),
            };

            let mut patched = saved.clone();
            if let Some(db) = database {
                patched.params.insert("database".to_string(), db);
            }

            let conn = match handle.connections.connect(&patched).await {
                Ok(id) => match handle.connections.get(id) {
                    Some(c) => c,
                    None => return Response::Error("connection handle lost".to_string()),
                },
                Err(e) => return Response::Error(e.to_string()),
            };

            let connection_id = patched.id;
            let execution_result = handle
                .query_service
                .execute_query(conn.clone(), connection_id, &sql)
                .await;
            if let Err(error) = handle.connections.disconnect(connection_id).await {
                tracing::warn!(
                    connection_id = %connection_id,
                    error = %error,
                    "failed to disconnect IPC query connection"
                );
            }
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
            let saved = match find_saved(handle, &name_or_id) {
                Some(s) => s,
                None => return Response::Error(format!("no connection matching '{}'", name_or_id)),
            };

            match handle.connections.connect(&saved).await {
                Ok(conn_id) => {
                    let databases_result = handle.connections.list_databases(conn_id).await;
                    if let Err(error) = handle.connections.disconnect(conn_id).await {
                        tracing::warn!(
                            connection_id = %conn_id,
                            error = %error,
                            "failed to disconnect IPC list-databases connection"
                        );
                    }
                    match databases_result {
                        Ok(dbs) => Response::StringList(dbs),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Err(e) => Response::Error(e.to_string()),
            }
        }

        Request::ListTables {
            connection,
            database,
        } => {
            let saved = match find_saved(handle, &connection) {
                Some(s) => s,
                None => return Response::Error(format!("no connection matching '{}'", connection)),
            };

            let mut patched = saved.clone();
            if let Some(db) = database {
                patched.params.insert("database".to_string(), db);
            }

            let conn = match handle.connections.connect(&patched).await {
                Ok(id) => match handle.connections.get(id) {
                    Some(c) => c,
                    None => return Response::Error("connection handle lost".to_string()),
                },
                Err(e) => return Response::Error(e.to_string()),
            };

            let schema = match conn.as_schema_introspection() {
                Some(s) => s,
                None => {
                    if let Err(error) = handle.connections.disconnect(patched.id).await {
                        tracing::warn!(
                            connection_id = %patched.id,
                            error = %error,
                            "failed to disconnect IPC list-tables connection"
                        );
                    }
                    return Response::Error(
                        "driver does not support schema introspection".to_string(),
                    );
                }
            };

            let tables_result = schema.list_tables(None).await;
            if let Err(error) = handle.connections.disconnect(patched.id).await {
                tracing::warn!(
                    connection_id = %patched.id,
                    error = %error,
                    "failed to disconnect IPC list-tables connection"
                );
            }
            match tables_result {
                Ok(tables) => Response::StringList(tables.into_iter().map(|t| t.name).collect()),
                Err(e) => Response::Error(e.to_string()),
            }
        }

        Request::ListColumns {
            connection,
            database,
            table,
        } => {
            let saved = match find_saved(handle, &connection) {
                Some(s) => s,
                None => return Response::Error(format!("no connection matching '{}'", connection)),
            };

            let mut patched = saved.clone();
            if let Some(db) = database {
                patched.params.insert("database".to_string(), db);
            }

            let conn = match handle.connections.connect(&patched).await {
                Ok(id) => match handle.connections.get(id) {
                    Some(c) => c,
                    None => return Response::Error("connection handle lost".to_string()),
                },
                Err(e) => return Response::Error(e.to_string()),
            };

            let schema = match conn.as_schema_introspection() {
                Some(s) => s,
                None => {
                    if let Err(error) = handle.connections.disconnect(patched.id).await {
                        tracing::warn!(
                            connection_id = %patched.id,
                            error = %error,
                            "failed to disconnect IPC list-columns connection"
                        );
                    }
                    return Response::Error(
                        "driver does not support schema introspection".to_string(),
                    );
                }
            };

            let table_result = schema.get_table(None, &table).await;
            if let Err(error) = handle.connections.disconnect(patched.id).await {
                tracing::warn!(
                    connection_id = %patched.id,
                    error = %error,
                    "failed to disconnect IPC list-columns connection"
                );
            }
            match table_result {
                Ok(table_info) => {
                    let columns = table_info
                        .columns
                        .into_iter()
                        .map(|col| ColumnInfo {
                            is_primary_key: col.is_primary_key,
                            name: col.name,
                            data_type: col.data_type,
                            nullable: col.nullable,
                            default_value: col.default_value,
                        })
                        .collect();

                    Response::Columns(columns)
                }
                Err(e) => Response::Error(e.to_string()),
            }
        }

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
                    .connections
                    .saved_connections()
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
            if !targets.is_empty() {
                let queue = handle.open_targets_queue();
                queue.write().extend(targets);
            }
            Response::Ok
        }
    }
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

/// Find a saved connection by name (case-insensitive) or UUID.
fn find_saved(handle: &IpcServerHandle, name_or_id: &str) -> Option<SavedConnection> {
    let connections = handle.connections.saved_connections();
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
