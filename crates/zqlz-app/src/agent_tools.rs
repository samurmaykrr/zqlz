//! MCP tools served to AI agents through `zqlz mcp`.
//!
//! `zqlz mcp` (zqlz-cli) speaks the protocol and owns the tool list; this is
//! where the tools run. Tools that touch the UI run on the main window (see
//! `main_view/agent_handlers.rs`) so the user watches each step happen.

use std::time::{Duration, Instant};

use gpui::App;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use uuid::Uuid;
use zqlz_connection::{AgentAccess, AgentAccessPolicy};
use zqlz_query::QueryEngine;

use crate::app::AppState;
use crate::ipc_server::{
    IpcServerHandle, Relations, Response, UNKNOWN_DATABASE_PREFIX, describe_table, find_saved,
    list_relations, list_tables, schema_scoped,
};
use crate::main_view::{AgentReply, AgentSqlRequest};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
struct ConnectionArgs {
    connection: String,
}

#[derive(Deserialize)]
struct SchemaArgs {
    connection: String,
    database: Option<String>,
    table: Option<String>,
}

#[derive(Deserialize)]
struct SqlArgs {
    connection: String,
    sql: String,
    database: Option<String>,
}

#[derive(Deserialize)]
struct ChangeArgs {
    connection: String,
    sql: String,
    reason: String,
    database: Option<String>,
}

#[derive(Deserialize)]
struct EditArgs {
    sql: String,
    #[serde(default)]
    replace_selection: bool,
}

#[derive(Deserialize)]
struct TableArgs {
    connection: String,
    table: String,
    database: Option<String>,
}

pub async fn call(handle: &IpcServerHandle, name: &str, arguments: Value) -> Result<Value, String> {
    match name {
        "get_context" => {
            handle
                .on_ui(|main_view, _window, cx| main_view.agent_context(cx))
                .await
        }
        "connect" => {
            let args: ConnectionArgs = parse(arguments)?;
            connect(handle, resolve_connection(handle, &args.connection)?).await
        }
        "describe_schema" => {
            let args: SchemaArgs = parse(arguments)?;
            describe_schema(handle, args).await
        }
        "run_query" => {
            let args: SqlArgs = parse(arguments)?;
            if !QueryEngine::new().is_read_only(&args.sql) {
                return Err("run_query only runs SQL that is provably read-only. Use \
                            apply_changes for anything that writes (the user may be asked to \
                            approve it)."
                    .into());
            }
            let request = AgentSqlRequest {
                connection_id: resolve_connection(handle, &args.connection)?,
                database: args.database,
                sql: args.sql,
                write_reason: None,
            };
            run_sql(handle, request).await
        }
        "apply_changes" => {
            let args: ChangeArgs = parse(arguments)?;
            let request = AgentSqlRequest {
                connection_id: resolve_connection(handle, &args.connection)?,
                database: args.database,
                sql: args.sql,
                write_reason: Some(args.reason),
            };
            run_sql(handle, request).await
        }
        "explain_query" => {
            let args: SqlArgs = parse(arguments)?;
            let connection_id = resolve_connection(handle, &args.connection)?;
            let reply = handle
                .on_ui(move |main_view, window, cx| {
                    main_view.agent_explain(connection_id, args.database, args.sql, window, cx)
                })
                .await?;
            await_reply(reply).await
        }
        "edit_editor" => {
            let args: EditArgs = parse(arguments)?;
            handle
                .on_ui(move |main_view, window, cx| {
                    main_view.agent_edit_active_editor(args.sql, args.replace_selection, window, cx)
                })
                .await?
        }
        "open_table" => {
            let args: TableArgs = parse(arguments)?;
            let connection_id = resolve_connection(handle, &args.connection)?;
            ensure_connected(handle, connection_id)?;
            let target = resolve_relation(handle, &args).await?;
            let opened = target.display_name();
            handle
                .on_ui(move |main_view, window, cx| {
                    let table = zqlz_core::TableRef::new(target.database, None, target.name);
                    let table = if target.is_view { table.view() } else { table };
                    main_view.agent_open_table(connection_id, table, window, cx)
                })
                .await?;
            Ok(json!({ "opened": opened }))
        }
        "open_designer" => {
            let args: TableArgs = parse(arguments)?;
            let connection_id = resolve_connection(handle, &args.connection)?;
            ensure_connected(handle, connection_id)?;
            let target = resolve_relation(handle, &args).await?;
            let opened = target.display_name();
            handle
                .on_ui(move |main_view, window, cx| {
                    main_view.agent_open_designer(
                        connection_id,
                        target.name,
                        target.schema_database,
                        window,
                        cx,
                    )
                })
                .await?;
            Ok(json!({ "opened_designer": opened }))
        }
        _ => Err(format!("Unknown tool '{name}'.")),
    }
}

/// Loads per-connection agent access into the UI cache at startup.
pub fn load_agent_access_policy(cx: &mut App) {
    let state = cx.global::<AppState>();
    let mut policy = AgentAccessPolicy::default();
    for saved in state.connection_service.list_saved_connections() {
        match state
            .storage
            .load_setting(&AgentAccess::setting_key(saved.id))
        {
            Ok(Some(value)) => match AgentAccess::parse(&value) {
                Some(access) => {
                    policy.0.insert(saved.id, access);
                }
                None => tracing::warn!(value, connection_id = %saved.id, "unknown agent access"),
            },
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, connection_id = %saved.id, "failed to load agent access")
            }
        }
    }
    cx.set_global(policy);
}

fn parse<T: DeserializeOwned>(arguments: Value) -> Result<T, String> {
    serde_json::from_value(arguments).map_err(|error| format!("Invalid arguments: {error}"))
}

fn resolve_connection(handle: &IpcServerHandle, name_or_id: &str) -> Result<Uuid, String> {
    find_saved(handle, name_or_id)
        .map(|saved| saved.id)
        .ok_or_else(|| format!("No saved connection matches '{name_or_id}'. See get_context."))
}

fn ensure_connected(handle: &IpcServerHandle, connection_id: Uuid) -> Result<(), String> {
    if handle
        .connection_service
        .is_connection_active(connection_id)
    {
        Ok(())
    } else {
        Err("That connection is not connected in ZQLZ. Call the connect tool first.".into())
    }
}

/// Where an `open_table` / `open_designer` request lands once the relation is
/// known to exist.
struct RelationTarget {
    /// Bare name as the tab should open it (qualifiers resolved into `database`).
    name: String,
    /// Database to open it in; `None` keeps the connection's current one.
    database: Option<String>,
    /// The same database, only when it names a schema on a schema-scoped driver
    /// (MySQL), where the designer cannot be pointed at it any other way.
    schema_database: Option<String>,
    is_view: bool,
}

impl RelationTarget {
    fn display_name(&self) -> String {
        match &self.schema_database {
            Some(database) => format!("{database}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// Checks that the requested table or view exists, with close-name suggestions
/// when it does not, instead of opening a tab that can only show a load error.
///
/// On MySQL (database == schema) the name may be qualified (`db.t`,
/// `` `db`.`t` ``) and the `database` argument picks the schema; a name that
/// only exists literally, dot and all, still opens from `database`.
async fn resolve_relation(
    handle: &IpcServerHandle,
    args: &TableArgs,
) -> Result<RelationTarget, String> {
    let schema_scoped = find_saved(handle, &args.connection)
        .is_some_and(|saved| schema_scoped(&saved.driver));
    let mut attempts = vec![(args.database.clone(), args.table.clone())];
    if schema_scoped {
        // With an explicit database the literal name is tried first, so a table
        // named `odd.name` there isn't mistaken for database `odd`.
        let position = if args.database.is_some() { 1 } else { 0 };
        if let Ok(parsed) = zqlz_core::TableRef::parse(&args.table, Some("mysql"), None) {
            match parsed.database {
                Some(database) => attempts.insert(position, (Some(database), parsed.name)),
                // Backtick-quoted bare name: `odd.name`.
                None if parsed.name != args.table => {
                    attempts.insert(0, (args.database.clone(), parsed.name))
                }
                None => {}
            }
        }
    }

    let attempt_count = attempts.len();
    let mut first_miss = None;
    for (index, (database, name)) in attempts.into_iter().enumerate() {
        let relations = match list_relations(handle, &args.connection, database.clone()).await {
            Ok(Some(relations)) => relations,
            // Not a relational driver: nothing to validate against.
            Ok(None) => {
                return Ok(RelationTarget {
                    name,
                    database,
                    schema_database: None,
                    is_view: false,
                });
            }
            // A guessed qualifier that names no database is just a wrong guess.
            Err(_) if index + 1 < attempt_count => continue,
            Err(error) if error.starts_with(UNKNOWN_DATABASE_PREFIX) => return Err(error),
            Err(error) => {
                // Opening is still the user-visible outcome, so don't block on a listing hiccup.
                tracing::warn!(%error, "could not list relations to validate open request");
                return Ok(RelationTarget {
                    name,
                    database,
                    schema_database: None,
                    is_view: false,
                });
            }
        };
        let listed = relations.names();
        if let Some(found) = find_listed(&name, &listed) {
            return Ok(RelationTarget {
                is_view: relations.views.iter().any(|view| &view.name == found),
                schema_database: database.clone().filter(|_| schema_scoped),
                name: if schema_scoped { found.clone() } else { name },
                database,
            });
        }
        first_miss.get_or_insert((database, name, listed));
    }

    let (database, name, listed) = first_miss.expect("at least one attempt");
    let suggestions = suggest_relations(&name, &listed);
    let hint = if suggestions.is_empty() {
        "Use describe_schema to list tables and views.".to_string()
    } else {
        format!("Did you mean: {}?", suggestions.join(", "))
    };
    let place = match (&database, schema_scoped) {
        (Some(database), true) => format!("in database '{database}'"),
        (None, true) => "in the connection's current database".to_string(),
        _ => "on this connection. Names are case-sensitive and may need the schema, e.g. \
              'schema.name'"
            .to_string(),
    };
    Err(format!("No table or view '{name}' {place}. {hint}"))
}

/// `listed` holds names as the driver lists them: `schema.name` on schema-aware
/// drivers, bare otherwise. A bare request matches the name in any schema.
fn find_listed<'a>(requested: &str, listed: &'a [String]) -> Option<&'a String> {
    listed.iter().find(|name| {
        *name == requested || name.split_once('.').is_some_and(|(_, bare)| bare == requested)
    })
}

#[cfg(test)]
fn relation_is_listed(requested: &str, listed: &[String]) -> bool {
    find_listed(requested, listed).is_some()
}

/// Up to five listed names that look like `requested`: same name prefix,
/// containment either way, case-insensitively. Names in the requested schema
/// come first.
fn suggest_relations(requested: &str, listed: &[String]) -> Vec<String> {
    let split = |name: &str| match name.split_once('.') {
        Some((schema, bare)) if !schema.is_empty() && !bare.is_empty() => {
            (Some(schema.to_lowercase()), bare.to_lowercase())
        }
        _ => (None, name.to_lowercase()),
    };
    let (wanted_schema, wanted) = split(requested);

    let mut ranked: Vec<(bool, u8, &String)> = listed
        .iter()
        .filter_map(|name| {
            let (schema, bare) = split(name);
            let rank = if bare.starts_with(&wanted) {
                0
            } else if wanted.starts_with(&bare) {
                1
            } else if bare.contains(&wanted) {
                2
            } else {
                return None;
            };
            let other_schema = wanted_schema.is_some() && wanted_schema != schema;
            Some((other_schema, rank, name))
        })
        .collect();
    ranked.sort();
    ranked.into_iter().take(5).map(|(_, _, name)| name.clone()).collect()
}

async fn run_sql(handle: &IpcServerHandle, request: AgentSqlRequest) -> Result<Value, String> {
    let reply = handle
        .on_ui(move |main_view, window, cx| main_view.agent_run_sql(request, window, cx))
        .await?;
    await_reply(reply).await
}

async fn await_reply(reply: AgentReply) -> Result<Value, String> {
    reply
        .await
        .map_err(|_| "ZQLZ dropped the request (the dialog or window was closed).".to_string())?
}

async fn connect(handle: &IpcServerHandle, connection_id: Uuid) -> Result<Value, String> {
    if handle
        .connection_service
        .is_connection_active(connection_id)
    {
        return Ok(json!({ "connected": true }));
    }

    handle
        .on_ui(move |main_view, window, cx| main_view.agent_connect(connection_id, window, cx))
        .await?;

    let started = Instant::now();
    while started.elapsed() < CONNECT_TIMEOUT {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (connected, connecting) = handle
            .on_ui(move |main_view, _window, cx| {
                main_view.agent_connection_state(connection_id, cx)
            })
            .await?;
        if connected {
            return Ok(json!({ "connected": true }));
        }
        // Give the sidebar a moment to flip into its connecting state before
        // reading "not connecting" as a failure.
        if !connecting && started.elapsed() > Duration::from_secs(1) {
            return Err("Connecting failed; ZQLZ shows the error to the user.".into());
        }
    }
    Err("Timed out waiting for the connection.".into())
}

async fn describe_schema(handle: &IpcServerHandle, args: SchemaArgs) -> Result<Value, String> {
    if let Some(table) = args.table {
        return describe_table(handle, &args.connection, args.database, &table).await;
    }
    match list_relations(handle, &args.connection, args.database.clone()).await? {
        Some(Relations { tables, views }) => {
            let named = |materialized: bool| -> Vec<String> {
                views
                    .iter()
                    .filter(|view| (view.kind == "materialized view") == materialized)
                    .map(|view| view.name.clone())
                    .collect()
            };
            let mut schema = json!({ "tables": tables, "views": named(false) });
            let materialized = named(true);
            if !materialized.is_empty() {
                schema["materialized_views"] = json!(materialized);
            }
            Ok(schema)
        }
        // Not relational: whatever the driver calls tables.
        None => match list_tables(handle, &args.connection, args.database).await {
            Response::StringList(tables) => Ok(json!({ "tables": tables })),
            Response::Error(error) => Err(error),
            _ => Err("Unexpected schema response.".into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn relation_lookup_accepts_qualified_and_bare_names() {
        let listed = names(&["public.users", "zqlz_weird_names.View With Spaces", "plain"]);
        assert!(relation_is_listed("public.users", &listed));
        assert!(relation_is_listed("users", &listed));
        assert!(relation_is_listed("zqlz_weird_names.View With Spaces", &listed));
        assert!(relation_is_listed("plain", &listed));
        assert!(!relation_is_listed("zqlz_weird_names.View With", &listed));
        assert!(!relation_is_listed("Users", &listed));
        assert!(!relation_is_listed("other.users", &listed));
    }

    #[test]
    fn suggestions_prefer_the_requested_schema_and_prefix() {
        let listed = names(&[
            "zqlz_weird_names.View With Spaces",
            "zqlz_weird_names.Another",
            "public.view with spaces too",
            "zqlz_weird_names.Has View With inside",
        ]);
        assert_eq!(
            suggest_relations("zqlz_weird_names.View With", &listed),
            names(&[
                "zqlz_weird_names.View With Spaces",
                "zqlz_weird_names.Has View With inside",
                "public.view with spaces too",
            ])
        );
        assert!(suggest_relations("zzz", &listed).is_empty());
    }

    #[test]
    fn suggestions_are_capped_at_five() {
        let listed: Vec<String> = (0..9).map(|i| format!("public.t{i}")).collect();
        assert_eq!(suggest_relations("t", &listed).len(), 5);
    }
}
