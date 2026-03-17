//! IPC server for the `zqlz` CLI.
//!
//! Listens on a platform-specific endpoint and dispatches requests from the CLI.
//! Wire protocol: 4-byte big-endian u32 length prefix + JSON payload.

#[cfg(windows)]
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixListener;
#[cfg(windows)]
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use uuid::Uuid;
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_query::{QueryHistory, QueryService};

use crate::storage::LocalStorage;

// ---------------------------------------------------------------------------
// Protocol types (must match zqlz-cli/src/ipc.rs exactly)
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

/// Default IPC endpoint.
///
/// On Unix this is `~/.config/zqlz/ipc.sock`.
/// On Windows this is a per-user named pipe path derived from the config dir.
pub fn default_socket_path() -> Result<PathBuf> {
    #[cfg(unix)]
    {
        let config_dir = dirs::config_dir().context("could not determine config directory")?;
        return Ok(config_dir.join("zqlz").join("ipc.sock"));
    }

    #[cfg(windows)]
    {
        let config_dir = dirs::config_dir().context("could not determine config directory")?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        config_dir
            .to_string_lossy()
            .to_lowercase()
            .hash(&mut hasher);
        let endpoint = format!(r"\\.\pipe\zqlz-ipc-{:016x}", hasher.finish());
        return Ok(PathBuf::from(endpoint));
    }

    #[allow(unreachable_code)]
    let config_dir = dirs::config_dir().context("could not determine config directory")?;
    Ok(config_dir.join("zqlz").join("ipc.sock"))
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
            match handle
                .query_service
                .execute_query(conn.clone(), connection_id, &sql)
                .await
            {
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
                Ok(conn_id) => match handle.connections.list_databases(conn_id).await {
                    Ok(dbs) => Response::StringList(dbs),
                    Err(e) => Response::Error(e.to_string()),
                },
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
                    return Response::Error(
                        "driver does not support schema introspection".to_string(),
                    );
                }
            };

            match schema.list_tables(None).await {
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
                    return Response::Error(
                        "driver does not support schema introspection".to_string(),
                    );
                }
            };

            match schema.get_table(None, &table).await {
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
    }
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
