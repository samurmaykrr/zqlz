//! ZQLZ CLI — command-line interface for the ZQLZ database IDE
//!
//! Subcommands are dispatched either through a Unix socket to a running ZQLZ
//! GUI instance (IPC path) or directly against the shared storage and driver
//! stack (standalone path).

mod ipc;
mod output;
mod standalone;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::ipc::ConnectionSummary;
use crate::output::OutputOptions;
use crate::standalone::{SavedConnection, load_history_entry};

// ---------------------------------------------------------------------------
// CLI argument definitions
// ---------------------------------------------------------------------------

/// ZQLZ command-line interface
///
/// Connects to a running ZQLZ GUI instance via IPC when available, otherwise
/// operates standalone against the shared storage database.
#[derive(Debug, Parser)]
#[command(name = "zqlz", about = "ZQLZ database IDE — command-line interface")]
struct Cli {
    /// Override the Unix socket path used to reach a running GUI instance.
    /// Defaults to ~/.config/zqlz/ipc.sock
    #[arg(long, env = "ZQLZ_IPC_SOCKET", global = true)]
    socket: Option<PathBuf>,

    /// Skip IPC and always run in standalone mode even when a GUI is running
    #[arg(long, global = true)]
    standalone: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List saved connections or show details for one
    Connections(ConnectionsArgs),

    /// Connect to a saved connection (test connectivity)
    Connect(ConnectArgs),

    /// Execute SQL against a saved connection
    Query(QueryArgs),

    /// Browse schema objects (databases, tables, columns, views, indexes, functions)
    Schema(SchemaArgs),

    /// Show query execution history
    History(HistoryArgs),

    /// Show whether the ZQLZ GUI is running and responsive
    Status,
}

// --- connections ------------------------------------------------------------

#[derive(Debug, Parser)]
struct ConnectionsArgs {
    #[command(subcommand)]
    action: ConnectionsAction,
}

#[derive(Debug, Subcommand)]
enum ConnectionsAction {
    /// List all saved connections
    List,

    /// Show details for a single connection (by name or UUID)
    Show { name_or_id: String },

    /// Add a new connection
    Add(AddConnectionArgs),

    /// Update fields of an existing connection (by name or UUID)
    Update(UpdateConnectionArgs),

    /// Remove a saved connection (by name or UUID)
    Remove { name_or_id: String },
}

#[derive(Debug, Parser)]
struct AddConnectionArgs {
    /// Display name for the connection
    #[arg(long)]
    name: String,

    /// Driver type: postgres, mysql, sqlite, mssql, duckdb, redis, mongodb, clickhouse
    #[arg(long)]
    driver: String,

    /// Hostname or IP address
    #[arg(long)]
    host: Option<String>,

    /// Port number
    #[arg(long)]
    port: Option<u16>,

    /// Database name
    #[arg(long)]
    database: Option<String>,

    /// Username
    #[arg(long)]
    username: Option<String>,

    /// Password (consider using environment variable ZQLZ_PASSWORD instead)
    #[arg(long, env = "ZQLZ_PASSWORD")]
    password: Option<String>,

    /// SQLite / DuckDB file path
    #[arg(long)]
    path: Option<String>,

    /// Folder label for organizing connections in the GUI
    #[arg(long)]
    folder: Option<String>,

    /// Accent color in #RRGGBB hex format (e.g. --color '#FF5733')
    #[arg(long)]
    color: Option<String>,

    /// Extra key=value parameters (repeatable)
    #[arg(long = "param", value_name = "KEY=VALUE", num_args = 1)]
    params: Vec<String>,
}

#[derive(Debug, Parser)]
struct UpdateConnectionArgs {
    /// Connection name or UUID to update
    name_or_id: String,

    /// New display name
    #[arg(long)]
    name: Option<String>,

    /// New driver type
    #[arg(long)]
    driver: Option<String>,

    /// New hostname or IP address
    #[arg(long)]
    host: Option<String>,

    /// New port number
    #[arg(long)]
    port: Option<u16>,

    /// New database name
    #[arg(long)]
    database: Option<String>,

    /// New username
    #[arg(long)]
    username: Option<String>,

    /// New password (consider using environment variable ZQLZ_PASSWORD instead)
    #[arg(long, env = "ZQLZ_PASSWORD")]
    password: Option<String>,

    /// New SQLite / DuckDB file path
    #[arg(long)]
    path: Option<String>,

    /// New folder label
    #[arg(long)]
    folder: Option<String>,

    /// New accent color in #RRGGBB hex format
    #[arg(long)]
    color: Option<String>,

    /// Extra key=value parameter to set or update (repeatable)
    #[arg(long = "param", value_name = "KEY=VALUE", num_args = 1)]
    params: Vec<String>,
}

// --- connect ----------------------------------------------------------------

#[derive(Debug, Parser)]
struct ConnectArgs {
    /// Connection name or UUID
    name_or_id: String,
}

// --- query ------------------------------------------------------------------

#[derive(Debug, Parser)]
struct QueryArgs {
    /// Connection name or UUID
    #[arg(long, short = 'c')]
    connection: String,

    /// Database name (for drivers that require per-database connections)
    #[arg(long, short = 'd')]
    database: Option<String>,

    /// SQL text to execute. Reads from stdin when omitted.
    /// Mutually exclusive with --file.
    #[arg(trailing_var_arg = true)]
    sql: Vec<String>,

    /// Read SQL from a file instead of from positional arguments or stdin.
    /// Mutually exclusive with positional SQL arguments.
    #[arg(long, short = 'F', value_name = "PATH")]
    file: Option<PathBuf>,

    /// Write output to a file instead of stdout
    #[arg(long, short = 'o', value_name = "PATH")]
    output: Option<PathBuf>,

    /// Output format
    #[arg(long, short = 'f', default_value = "table")]
    format: OutputFormat,

    /// Maximum rows to display per statement (0 = unlimited)
    #[arg(long, default_value = "1000")]
    limit: usize,

    /// Suppress column headers in table, CSV, and TSV output
    #[arg(long)]
    no_header: bool,

    /// Render each row as vertical `column = value` pairs (like psql's \\x)
    #[arg(long)]
    expanded: bool,

    /// Plain pipe-separated output with no Unicode box-drawing characters
    #[arg(long)]
    no_align: bool,

    /// Print an explicit `Time: X ms` line after each statement
    #[arg(long)]
    timing: bool,

    /// Suppress row-count and status footer lines
    #[arg(long)]
    quiet: bool,

    /// Wrap all statements in a single BEGIN / COMMIT transaction (standalone
    /// mode only; ignored when running via IPC)
    #[arg(long)]
    single_transaction: bool,

    /// Warn before executing statements that may cause mass data loss
    /// (DELETE/UPDATE without WHERE, TRUNCATE, DROP TABLE/DATABASE)
    #[arg(long)]
    warn: bool,
}

// --- schema -----------------------------------------------------------------

#[derive(Debug, Parser)]
struct SchemaArgs {
    /// Connection name or UUID
    #[arg(long, short = 'c')]
    connection: String,

    #[command(subcommand)]
    action: SchemaAction,
}

#[derive(Debug, Subcommand)]
enum SchemaAction {
    /// List databases
    Databases,

    /// List tables in a database
    Tables {
        /// Database name (uses connection default when omitted)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },

    /// Show columns for a table
    Columns {
        /// Table name
        table: String,

        /// Database name (uses connection default when omitted)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },

    /// List views in a database
    Views {
        /// Database name (uses connection default when omitted)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },

    /// List schemas / namespaces
    Schemas,

    /// List indexes for a table
    Indexes {
        /// Table name
        table: String,

        /// Database name (uses connection default when omitted)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },

    /// List functions
    Functions {
        /// Database name (uses connection default when omitted)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },

    /// Print DDL for a database object
    Ddl {
        /// Object name
        name: String,

        /// Object type
        #[arg(long = "type", short = 't', default_value = "table")]
        object_type: DdlObjectType,

        /// Schema name (optional)
        #[arg(long, short = 's')]
        schema: Option<String>,

        /// Database name (optional)
        #[arg(long, short = 'd')]
        database: Option<String>,
    },
}

/// Object type for DDL generation.
#[derive(Debug, Clone, ValueEnum)]
enum DdlObjectType {
    Table,
    View,
    Index,
    Function,
    Procedure,
    Trigger,
    Sequence,
    Type,
}

impl DdlObjectType {
    fn to_core(&self) -> zqlz_core::ObjectType {
        match self {
            DdlObjectType::Table => zqlz_core::ObjectType::Table,
            DdlObjectType::View => zqlz_core::ObjectType::View,
            DdlObjectType::Index => zqlz_core::ObjectType::Index,
            DdlObjectType::Function => zqlz_core::ObjectType::Function,
            DdlObjectType::Procedure => zqlz_core::ObjectType::Procedure,
            DdlObjectType::Trigger => zqlz_core::ObjectType::Trigger,
            DdlObjectType::Sequence => zqlz_core::ObjectType::Sequence,
            DdlObjectType::Type => zqlz_core::ObjectType::Type,
        }
    }
}

// --- history ----------------------------------------------------------------

#[derive(Debug, Parser)]
struct HistoryArgs {
    #[command(subcommand)]
    action: HistoryAction,
}

#[derive(Debug, Subcommand)]
enum HistoryAction {
    /// List recent query history (default)
    List(HistoryListArgs),

    /// Show the full SQL and metadata for a specific history entry
    Show {
        /// UUID of the history entry to display
        id: Uuid,
    },
}

#[derive(Debug, Parser)]
struct HistoryListArgs {
    /// Connection name or UUID filter (show history for this connection only)
    #[arg(long, short = 'c')]
    connection: Option<String>,

    /// Maximum entries to show
    #[arg(long, default_value = "50")]
    limit: usize,

    /// Search term to filter queries
    #[arg(long, short = 's')]
    search: Option<String>,

    /// Output format
    #[arg(long, short = 'f', default_value = "table")]
    format: OutputFormat,
}

// --- output format ----------------------------------------------------------

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
    Tsv,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("zqlz=warn".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    // Determine whether to attempt IPC
    let socket_path = cli
        .socket
        .unwrap_or_else(|| default_socket_path().expect("could not determine socket path"));

    let use_ipc = !cli.standalone && socket_path.exists();

    match cli.command {
        Command::Connections(args) => handle_connections(args, use_ipc, &socket_path).await,
        Command::Connect(args) => handle_connect(args, use_ipc, &socket_path).await,
        Command::Query(args) => handle_query(args, use_ipc, &socket_path).await,
        Command::Schema(args) => handle_schema(args, use_ipc, &socket_path).await,
        Command::History(args) => handle_history(args, use_ipc, &socket_path).await,
        Command::Status => handle_status(&socket_path).await,
    }
}

fn default_socket_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir().context("could not determine config directory")?;
    Ok(config_dir.join("zqlz").join("ipc.sock"))
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

async fn handle_connections(
    args: ConnectionsArgs,
    use_ipc: bool,
    socket_path: &Path,
) -> Result<()> {
    match args.action {
        ConnectionsAction::List => {
            if use_ipc {
                let response = ipc::send_request(socket_path, ipc::Request::ListConnections).await;
                match response {
                    Ok(ipc::Response::Connections(connections)) => {
                        output::print_connections(&connections);
                        return Ok(());
                    }
                    Ok(other) => bail!("unexpected IPC response: {:?}", other),
                    Err(e) => {
                        eprintln!("IPC unavailable ({}), falling back to standalone", e);
                    }
                }
            }
            let connections = standalone::load_connections()?;
            let summaries: Vec<ConnectionSummary> = connections
                .iter()
                .map(ConnectionSummary::from_saved)
                .collect();
            output::print_connections(&summaries);
        }

        ConnectionsAction::Show { name_or_id } => {
            if use_ipc {
                let response = ipc::send_request(socket_path, ipc::Request::ListConnections).await;
                match response {
                    Ok(ipc::Response::Connections(summaries)) => {
                        let conn = find_summary(&summaries, &name_or_id)?;
                        output::print_connection_detail(conn);
                        return Ok(());
                    }
                    Ok(other) => bail!("unexpected IPC response: {:?}", other),
                    Err(e) => {
                        eprintln!("IPC unavailable ({}), falling back to standalone", e);
                    }
                }
            }
            let connections = standalone::load_connections()?;
            let saved = find_connection(&connections, &name_or_id)?;
            output::print_connection_detail(&ConnectionSummary::from_saved(saved));
        }

        ConnectionsAction::Add(add_args) => {
            let saved = build_saved_connection(add_args)?;
            if use_ipc {
                let response =
                    ipc::send_request(socket_path, ipc::Request::SaveConnection(saved.clone()))
                        .await;
                match response {
                    Ok(ipc::Response::Ok) => {
                        println!("Connection '{}' saved (via GUI)", saved.name);
                        return Ok(());
                    }
                    Ok(other) => bail!("unexpected IPC response: {:?}", other),
                    Err(e) => {
                        eprintln!("IPC unavailable ({}), saving directly to storage", e);
                    }
                }
            }
            standalone::save_connection(&saved)?;
            println!("Connection '{}' saved", saved.name);
        }

        ConnectionsAction::Update(update_args) => {
            // Load and resolve the connection first (needed for both IPC and standalone paths)
            let connections = standalone::load_connections()?;
            let conn = find_connection(&connections, &update_args.name_or_id)?.clone();
            let conn = apply_updates(conn, update_args)?;

            if use_ipc {
                let response =
                    ipc::send_request(socket_path, ipc::Request::SaveConnection(conn.clone()))
                        .await;
                match response {
                    Ok(ipc::Response::Ok) => {
                        println!("Connection '{}' updated (via GUI)", conn.name);
                        return Ok(());
                    }
                    Ok(other) => bail!("unexpected IPC response: {:?}", other),
                    Err(e) => {
                        eprintln!("IPC unavailable ({}), saving directly to storage", e);
                    }
                }
            }
            standalone::save_connection(&conn)?;
            println!("Connection '{}' updated", conn.name);
        }

        ConnectionsAction::Remove { name_or_id } => {
            let connections = standalone::load_connections()?;
            let conn = find_connection(&connections, &name_or_id)?.clone();
            if use_ipc {
                let response =
                    ipc::send_request(socket_path, ipc::Request::DeleteConnection(conn.id)).await;
                match response {
                    Ok(ipc::Response::Ok) => {
                        println!("Connection '{}' removed (via GUI)", conn.name);
                        return Ok(());
                    }
                    Ok(other) => bail!("unexpected IPC response: {:?}", other),
                    Err(e) => {
                        eprintln!("IPC unavailable ({}), removing directly from storage", e);
                    }
                }
            }
            standalone::delete_connection(conn.id)?;
            println!("Connection '{}' removed", conn.name);
        }
    }
    Ok(())
}

async fn handle_connect(args: ConnectArgs, use_ipc: bool, socket_path: &Path) -> Result<()> {
    if use_ipc {
        let response = ipc::send_request(
            socket_path,
            ipc::Request::TestConnection(args.name_or_id.clone()),
        )
        .await;
        match response {
            Ok(ipc::Response::Ok) => {
                println!("Connection successful (tested via GUI)");
                return Ok(());
            }
            Ok(ipc::Response::Error(msg)) => {
                eprintln!("Connection failed: {}", msg);
                std::process::exit(1);
            }
            Ok(other) => bail!("unexpected IPC response: {:?}", other),
            Err(e) => {
                eprintln!("IPC unavailable ({}), testing standalone", e);
            }
        }
    }

    let connections = standalone::load_connections()?;
    let saved = find_connection(&connections, &args.name_or_id)?.clone();
    standalone::test_connection(&saved).await?;
    println!("Connection '{}' OK", saved.name);
    Ok(())
}

async fn handle_query(args: QueryArgs, use_ipc: bool, socket_path: &Path) -> Result<()> {
    let sql = resolve_sql(args.sql, args.file)?;

    if args.warn {
        for warning in detect_destructive(&sql) {
            eprintln!("Warning: {}", warning);
        }
    }

    let opts = OutputOptions {
        no_header: args.no_header,
        expanded: args.expanded,
        no_align: args.no_align,
        timing: args.timing,
        quiet: args.quiet,
    };

    if use_ipc {
        if args.single_transaction {
            eprintln!(
                "Note: --single-transaction is only honoured in standalone mode and will be ignored"
            );
        }
        let response = ipc::send_request(
            socket_path,
            ipc::Request::ExecuteQuery {
                connection: args.connection.clone(),
                database: args.database.clone(),
                sql: sql.clone(),
            },
        )
        .await;
        match response {
            Ok(ipc::Response::QueryResult(execution)) => {
                emit_query_output(
                    &execution,
                    &args.format,
                    args.limit,
                    &opts,
                    args.output.as_deref(),
                )?;
                return Ok(());
            }
            Ok(ipc::Response::Error(msg)) => {
                eprintln!("Query error: {}", msg);
                std::process::exit(1);
            }
            Ok(other) => bail!("unexpected IPC response: {:?}", other),
            Err(e) => {
                eprintln!("IPC unavailable ({}), running standalone", e);
            }
        }
    }

    let connections = standalone::load_connections()?;
    let saved = find_connection(&connections, &args.connection)?.clone();
    let execution = standalone::execute_query(
        &saved,
        args.database.as_deref(),
        &sql,
        args.single_transaction,
    )
    .await?;
    emit_query_output(
        &execution,
        &args.format,
        args.limit,
        &opts,
        args.output.as_deref(),
    )?;
    Ok(())
}

async fn handle_schema(args: SchemaArgs, use_ipc: bool, socket_path: &Path) -> Result<()> {
    // Always load connections from standalone storage so we have full credentials
    // for the fallback driver path. The IPC path uses connection name/id strings
    // directly in the request, so no lookup is needed there.
    let connections = standalone::load_connections()?;
    let saved = find_connection(&connections, &args.connection)?.clone();

    match args.action {
        SchemaAction::Databases => {
            if use_ipc {
                let response = ipc::send_request(
                    socket_path,
                    ipc::Request::ListDatabases(args.connection.clone()),
                )
                .await;
                match response {
                    Ok(ipc::Response::StringList(items)) => {
                        for item in items {
                            println!("{}", item);
                        }
                        return Ok(());
                    }
                    Ok(ipc::Response::Error(msg)) => {
                        eprintln!("Error: {}", msg);
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
            let databases = standalone::list_databases(&saved).await?;
            for db in databases {
                println!("{}", db);
            }
        }

        SchemaAction::Tables { database } => {
            if use_ipc {
                let response = ipc::send_request(
                    socket_path,
                    ipc::Request::ListTables {
                        connection: args.connection.clone(),
                        database: database.clone(),
                    },
                )
                .await;
                match response {
                    Ok(ipc::Response::StringList(items)) => {
                        for item in items {
                            println!("{}", item);
                        }
                        return Ok(());
                    }
                    Ok(ipc::Response::Error(msg)) => {
                        eprintln!("Error: {}", msg);
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
            let tables = standalone::list_tables(&saved, database.as_deref()).await?;
            for table in tables {
                println!("{}", table);
            }
        }

        SchemaAction::Columns { table, database } => {
            if use_ipc {
                let response = ipc::send_request(
                    socket_path,
                    ipc::Request::ListColumns {
                        connection: args.connection.clone(),
                        database: database.clone(),
                        table: table.clone(),
                    },
                )
                .await;
                match response {
                    Ok(ipc::Response::Columns(columns)) => {
                        output::print_columns(&columns);
                        return Ok(());
                    }
                    Ok(ipc::Response::Error(msg)) => {
                        eprintln!("Error: {}", msg);
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
            let columns = standalone::list_columns(&saved, database.as_deref(), &table).await?;
            output::print_columns(&columns);
        }

        // The following actions have no IPC counterparts yet — standalone only.
        SchemaAction::Views { database } => {
            let views = standalone::list_views(&saved, database.as_deref()).await?;
            output::print_views(&views);
        }

        SchemaAction::Schemas => {
            let schemas = standalone::list_schemas(&saved).await?;
            for schema in schemas {
                println!("{}", schema);
            }
        }

        SchemaAction::Indexes { table, database } => {
            let indexes = standalone::list_indexes(&saved, database.as_deref(), &table).await?;
            output::print_indexes(&indexes);
        }

        SchemaAction::Functions { database } => {
            let functions = standalone::list_functions(&saved, database.as_deref()).await?;
            output::print_functions(&functions);
        }

        SchemaAction::Ddl {
            name,
            object_type,
            schema,
            database,
        } => {
            let ddl = standalone::generate_ddl(
                &saved,
                database.as_deref(),
                object_type.to_core(),
                &name,
                schema.as_deref(),
            )
            .await?;
            println!("{}", ddl);
        }
    }
    Ok(())
}

async fn handle_history(args: HistoryArgs, use_ipc: bool, socket_path: &Path) -> Result<()> {
    match args.action {
        HistoryAction::List(list_args) => {
            handle_history_list(list_args, use_ipc, socket_path).await
        }
        HistoryAction::Show { id } => handle_history_show(id, use_ipc, socket_path).await,
    }
}

async fn handle_history_list(
    args: HistoryListArgs,
    use_ipc: bool,
    socket_path: &Path,
) -> Result<()> {
    if use_ipc {
        let response = ipc::send_request(
            socket_path,
            ipc::Request::QueryHistory {
                connection: args.connection.clone(),
                limit: args.limit,
                search: args.search.clone(),
            },
        )
        .await;
        match response {
            Ok(ipc::Response::History(entries)) => {
                output::print_history(&entries, &args.format);
                return Ok(());
            }
            Ok(ipc::Response::Error(msg)) => {
                eprintln!("Error: {}", msg);
                std::process::exit(1);
            }
            Ok(other) => bail!("unexpected IPC response: {:?}", other),
            Err(e) => {
                eprintln!("IPC unavailable ({}), reading from storage directly", e);
            }
        }
    }

    let entries = standalone::load_history(
        args.limit,
        args.connection.as_deref(),
        args.search.as_deref(),
    )?;
    output::print_history(&entries, &args.format);
    Ok(())
}

/// Show the full SQL and metadata for a single history entry identified by UUID.
///
/// The GUI IPC server does not yet have a `QueryHistoryEntry` handler, so this
/// always falls back to the standalone storage path when IPC is unavailable or
/// returns an unexpected response.
async fn handle_history_show(id: Uuid, use_ipc: bool, socket_path: &Path) -> Result<()> {
    if use_ipc {
        let response = ipc::send_request(socket_path, ipc::Request::QueryHistoryEntry { id }).await;
        match response {
            Ok(ipc::Response::HistoryEntry(entry)) => {
                output::print_history_entry(&entry);
                return Ok(());
            }
            Ok(ipc::Response::Error(msg)) => {
                eprintln!("Error: {}", msg);
                std::process::exit(1);
            }
            Ok(_) | Err(_) => {
                // IPC server doesn't know this request yet — fall through to
                // the standalone path silently.
            }
        }
    }

    match load_history_entry(id)? {
        Some(entry) => {
            output::print_history_entry(&entry);
            Ok(())
        }
        None => {
            eprintln!("No history entry found with id: {}", id);
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find a connection by name or UUID string.
fn find_connection<'a>(
    connections: &'a [SavedConnection],
    name_or_id: &str,
) -> Result<&'a SavedConnection> {
    // Try UUID first
    if let Ok(id) = Uuid::parse_str(name_or_id)
        && let Some(conn) = connections.iter().find(|c| c.id == id)
    {
        return Ok(conn);
    }
    // Fall back to name (case-insensitive)
    connections
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name_or_id))
        .ok_or_else(|| anyhow::anyhow!("no connection found matching '{}'", name_or_id))
}

/// Find a connection summary by name or UUID string.
fn find_summary<'a>(
    summaries: &'a [ConnectionSummary],
    name_or_id: &str,
) -> Result<&'a ConnectionSummary> {
    if let Ok(id) = Uuid::parse_str(name_or_id)
        && let Some(conn) = summaries.iter().find(|c| c.id == id)
    {
        return Ok(conn);
    }
    summaries
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name_or_id))
        .ok_or_else(|| anyhow::anyhow!("no connection found matching '{}'", name_or_id))
}

/// Read SQL from positional args, a `--file` path, or stdin (in that priority order).
fn resolve_sql(parts: Vec<String>, file: Option<PathBuf>) -> Result<String> {
    if file.is_some() && !parts.is_empty() {
        bail!("--file and positional SQL arguments are mutually exclusive");
    }
    if let Some(path) = file {
        return std::fs::read_to_string(&path)
            .with_context(|| format!("reading SQL from '{}'", path.display()));
    }
    if !parts.is_empty() {
        return Ok(parts.join(" "));
    }
    use std::io::Read;
    let mut sql = String::new();
    std::io::stdin()
        .read_to_string(&mut sql)
        .context("reading SQL from stdin")?;
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        bail!("no SQL provided (pass as argument, via --file, or via stdin)");
    }
    Ok(trimmed.to_string())
}

/// Render query results and write them to the given file path, or print to
/// stdout when no output path is specified.
fn emit_query_output(
    execution: &ipc::QueryExecution,
    format: &OutputFormat,
    limit: usize,
    opts: &OutputOptions,
    output_path: Option<&std::path::Path>,
) -> Result<()> {
    let rendered = output::render_query_execution(execution, format, limit, opts);
    if let Some(path) = output_path {
        std::fs::write(path, &rendered)
            .with_context(|| format!("writing output to '{}'", path.display()))?;
    } else {
        print!("{rendered}");
    }
    Ok(())
}

/// Build a `SavedConnection` from CLI add-connection arguments.
fn build_saved_connection(args: AddConnectionArgs) -> Result<SavedConnection> {
    let mut conn = SavedConnection::new(args.name, args.driver);

    if let Some(host) = args.host {
        conn = conn.with_param("host", &host);
    }
    if let Some(port) = args.port {
        conn = conn.with_param("port", &port.to_string());
    }
    if let Some(database) = args.database {
        conn = conn.with_param("database", &database);
    }
    if let Some(username) = args.username {
        conn = conn.with_param("username", &username);
    }
    if let Some(password) = args.password {
        conn = conn.with_param("password", &password);
    }
    if let Some(path) = args.path {
        conn = conn.with_param("path", &path);
    }
    if let Some(folder) = args.folder {
        conn.folder = Some(folder);
    }
    if let Some(color) = args.color {
        validate_color(&color)?;
        conn.color = Some(color);
    }
    for param in args.params {
        let (key, value) = param.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("--param must be in KEY=VALUE format, got '{}'", param)
        })?;
        conn = conn.with_param(key, value);
    }

    Ok(conn)
}

/// Apply `UpdateConnectionArgs` fields to an existing `SavedConnection`, returning the modified copy.
fn apply_updates(mut conn: SavedConnection, args: UpdateConnectionArgs) -> Result<SavedConnection> {
    if let Some(name) = args.name {
        conn.name = name;
    }
    if let Some(driver) = args.driver {
        conn.driver = driver;
    }
    if let Some(host) = args.host {
        conn.params.insert("host".to_string(), host);
    }
    if let Some(port) = args.port {
        conn.params.insert("port".to_string(), port.to_string());
    }
    if let Some(database) = args.database {
        conn.params.insert("database".to_string(), database);
    }
    if let Some(username) = args.username {
        conn.params.insert("username".to_string(), username);
    }
    if let Some(password) = args.password {
        conn.params.insert("password".to_string(), password);
    }
    if let Some(path) = args.path {
        conn.params.insert("path".to_string(), path);
    }
    if let Some(folder) = args.folder {
        conn.folder = Some(folder);
    }
    if let Some(color) = args.color {
        validate_color(&color)?;
        conn.color = Some(color);
    }
    for param in args.params {
        let (key, value) = param.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("--param must be in KEY=VALUE format, got '{}'", param)
        })?;
        conn.params.insert(key.to_string(), value.to_string());
    }
    conn.modified_at = chrono::Utc::now();
    Ok(conn)
}

/// Validate that a color string is in `#RRGGBB` hex format.
fn validate_color(color: &str) -> Result<()> {
    if color.starts_with('#')
        && color.len() == 7
        && color[1..].chars().all(|c| c.is_ascii_hexdigit())
    {
        Ok(())
    } else {
        bail!(
            "color must be in #RRGGBB format (e.g. #FF5733), got '{}'",
            color
        )
    }
}

/// Check whether the ZQLZ GUI IPC socket is alive and responsive.
async fn handle_status(socket_path: &PathBuf) -> Result<()> {
    if !socket_path.exists() {
        println!("GUI: not running (no socket at {})", socket_path.display());
        return Ok(());
    }

    match ipc::send_request(socket_path, ipc::Request::ListConnections).await {
        Ok(_) => println!("GUI: running ({})", socket_path.display()),
        Err(e) => println!("GUI: socket exists but not responding — {}", e),
    }
    Ok(())
}

/// Scan SQL text for statements that could cause mass data loss and return
/// human-readable warning messages for each one found.
///
/// This is a best-effort heuristic: it splits on `;` and checks uppercase
/// prefixes and the presence of `WHERE`.  It will produce false positives for
/// `WHERE` in subqueries and miss dynamic SQL, so it should never block
/// execution — only warn.
fn detect_destructive(sql: &str) -> Vec<String> {
    let mut warnings = Vec::new();

    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }

        // Build a preview string safe to display (truncate at char boundary).
        let preview: String = stmt.chars().take(80).collect();
        let preview = if stmt.chars().count() > 80 {
            format!("{}…", preview)
        } else {
            preview
        };

        let upper = stmt.to_ascii_uppercase();
        let upper = upper.trim_start();

        if upper.starts_with("DELETE") || upper.starts_with("UPDATE") {
            if !upper.contains("WHERE") {
                warnings.push(format!(
                    "potentially destructive statement with no WHERE clause: {}",
                    preview
                ));
            }
        } else if upper.starts_with("TRUNCATE") {
            warnings.push(format!("TRUNCATE removes all rows: {}", preview));
        } else if upper.starts_with("DROP TABLE")
            || upper.starts_with("DROP DATABASE")
            || upper.starts_with("DROP SCHEMA")
        {
            warnings.push(format!("irreversible DROP operation: {}", preview));
        }
    }

    warnings
}
