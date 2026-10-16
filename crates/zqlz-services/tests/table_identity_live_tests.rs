//! Replays the app's sidebar → table flows (open, delete, empty) against real
//! servers so table identity bugs show up as failing assertions.
//!
//! PostgreSQL and MySQL tests run only when `ZQLZ_SMOKE_POSTGRES_HOST` /
//! `ZQLZ_SMOKE_MYSQL_HOST` are set (see `driver_smoke_tests.rs`). SQLite always runs.

use std::sync::Arc;

use uuid::Uuid;
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_core::{TableRef, Value};
use zqlz_services::{
    plan_fetch, ConnectionService, DeleteTablesRequest, EmptyTablesRequest, FetchMode,
    FetchOutcome, KeyValueService, QuerySnapshot, RowWindow, SchemaService, TableService,
};

fn env_value(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

struct Harness {
    connections: ConnectionService,
    schemas: Arc<SchemaService>,
    tables: TableService,
    connection_id: Uuid,
}

impl Harness {
    async fn connect(saved: SavedConnection) -> Self {
        let schemas = Arc::new(SchemaService::new());
        let manager = Arc::new(ConnectionManager::new());
        manager.add_saved(saved.clone());
        let connections = ConnectionService::new(
            manager,
            schemas.clone(),
            Arc::new(KeyValueService::new(100)),
        );
        let info = connections
            .connect_and_initialize(&saved)
            .await
            .expect("connect");
        Self {
            connections,
            schemas,
            tables: TableService::new(1000),
            connection_id: info.id,
        }
    }

    async fn execute(&self, sql: &str) {
        let connection = self
            .connections
            .get_connection(self.connection_id)
            .expect("main connection");
        connection
            .execute(sql, &[])
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    /// Mirrors `open_table_viewer_with_session_state`'s async load.
    async fn open(&self, table: TableRef) -> Result<Vec<String>, String> {
        let resolved = self
            .connections
            .resolve_database_scoped_connection(self.connection_id, table.database.clone())
            .await
            .map_err(|error| format!("resolve connection: {error}"))?;
        let connection = resolved.connection;
        let is_view = table.is_view;
        let table = self.tables.resolve_table(connection.clone(), table).await;
        // The viewer's first page: key-ordered, as every later page is.
        let primary_key = self
            .tables
            .primary_key_columns(connection.clone(), &table)
            .await;
        let query = QuerySnapshot {
            pk_columns: primary_key
                .iter()
                .map(|column| connection.quote_identifier(column))
                .collect(),
            page_size: Some(1000),
            current_page: 1,
            fast_exact_count: connection.supports_fast_exact_count(),
            ..QuerySnapshot::default()
        };
        let plan = plan_fetch(FetchMode::Page(1), &query).expect("a first-page plan");
        let rows = self
            .tables
            .fetch_rows(connection.clone(), &table, &query, plan)
            .await
            .map_err(|error| format!("rows: {error}"))?
            .result;
        self.tables
            .load_open_viewer_schema(
                self.schemas.clone(),
                connection,
                self.connection_id,
                &table,
                is_view,
            )
            .await
            .map_err(|error| format!("schema: {error}"))?;
        Ok(labels(&rows.rows))
    }

    /// Mirrors the sidebar "Delete Table" / "Empty Table" handlers.
    async fn menu_delete(&self, table: TableRef) -> Result<(), String> {
        let table_name = table.name.clone();
        let resolved = self
            .connections
            .resolve_connection(self.connection_id, table.scope())
            .await
            .map_err(|error| error.to_string())?;
        let outcome = self
            .tables
            .delete_tables(
                resolved.connection,
                DeleteTablesRequest {
                    tables: vec![table],
                    continue_on_error: false,
                },
            )
            .await;
        if outcome.errors.is_empty() {
            Ok(())
        } else {
            Err(format!("delete {table_name}: {:?}", outcome.errors))
        }
    }

    async fn menu_empty(&self, table: TableRef) -> Result<(), String> {
        let table_name = table.name.clone();
        let resolved = self
            .connections
            .resolve_connection(self.connection_id, table.scope())
            .await
            .map_err(|error| error.to_string())?;
        let outcome = self
            .tables
            .empty_tables(
                resolved.connection,
                EmptyTablesRequest {
                    tables: vec![table],
                    continue_on_error: false,
                },
            )
            .await;
        if outcome.errors.is_empty() {
            Ok(())
        } else {
            Err(format!("empty {table_name}: {:?}", outcome.errors))
        }
    }

    async fn count(&self, qualified_sql_name: &str) -> Option<i64> {
        let connection = self.connections.get_connection(self.connection_id)?;
        let result = connection
            .query(&format!("SELECT COUNT(*) FROM {qualified_sql_name}"), &[])
            .await
            .ok()?;
        match result.rows.first()?.values.first()? {
            Value::Int64(count) => Some(*count),
            Value::Int32(count) => Some(i64::from(*count)),
            other => other.to_string().parse().ok(),
        }
    }
}

fn labels(rows: &[zqlz_core::Row]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| row.values.get(1).map(ToString::to_string))
        .collect()
}

fn suffix() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

#[tokio::test]
async fn sqlite_tables_open_and_mutate_by_identity() {
    let directory = std::env::temp_dir().join(format!("zqlz_identity_{}", suffix()));
    std::fs::create_dir_all(&directory).expect("temp dir");
    let path = directory.join("identity.db");
    let saved = SavedConnection::new("identity".into(), "sqlite".into())
        .with_param("path", &path.to_string_lossy());
    let harness = Harness::connect(saved).await;

    for (sql_name, label) in [
        ("\"items\"", "plain"),
        ("\"odd.name\"", "dotted"),
        ("\"Mixed Case\"", "mixed"),
        ("\"main\"", "named-main"),
    ] {
        harness
            .execute(&format!(
                "CREATE TABLE {sql_name} (id INTEGER PRIMARY KEY, label TEXT)"
            ))
            .await;
        harness
            .execute(&format!("INSERT INTO {sql_name} VALUES (1, '{label}')"))
            .await;
    }
    harness
        .execute("CREATE VIEW \"odd.view\" AS SELECT * FROM items")
        .await;

    let mut failures = Vec::new();
    for (table_name, is_view, expected) in [
        ("items", false, "plain"),
        ("odd.name", false, "dotted"),
        ("Mixed Case", false, "mixed"),
        ("main", false, "named-main"),
        ("odd.view", true, "plain"),
    ] {
        // A sidebar leaf knows the schema; a restored or typed name does not.
        for schema in [Some("main".to_string()), None] {
            let table = TableRef::new(None, schema.clone(), table_name);
            let table = if is_view { table.view() } else { table };
            match harness.open(table).await {
                Ok(rows) if rows == [format!("'{expected}'")] || rows == [expected] => {}
                other => failures.push(format!("open {schema:?}.{table_name}: {other:?}")),
            }
        }
    }

    if let Err(error) = harness
        .menu_empty(TableRef::new(None, Some("main".into()), "odd.name"))
        .await
    {
        failures.push(error);
    }
    if harness.count("\"odd.name\"").await != Some(0) {
        failures.push("empty odd.name did not empty it".into());
    }
    if let Err(error) = harness
        .menu_delete(TableRef::new(None, Some("main".into()), "Mixed Case"))
        .await
    {
        failures.push(error);
    }
    if harness.count("\"Mixed Case\"").await.is_some() {
        failures.push("delete Mixed Case did not drop it".into());
    }

    std::fs::remove_dir_all(&directory).ok();
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test]
async fn postgres_tables_open_and_mutate_by_identity() {
    let Some(host) = env_value("ZQLZ_SMOKE_POSTGRES_HOST") else {
        return;
    };
    let database = env_value("ZQLZ_SMOKE_POSTGRES_DATABASE").unwrap_or_else(|| "postgres".into());
    let saved = SavedConnection::new("identity".into(), "postgres".into())
        .with_param("host", &host)
        .with_param(
            "port",
            &env_value("ZQLZ_SMOKE_POSTGRES_PORT").unwrap_or_else(|| "5432".into()),
        )
        .with_param("database", &database)
        .with_param(
            "user",
            &env_value("ZQLZ_SMOKE_POSTGRES_USER").unwrap_or_else(|| "postgres".into()),
        )
        .with_param(
            "password",
            &env_value("ZQLZ_SMOKE_POSTGRES_PASSWORD").unwrap_or_default(),
        )
        .with_param("ssl_mode", "disable");
    let harness = Harness::connect(saved).await;

    let unique = suffix();
    let schema_a = format!("zid_a_{unique}");
    let schema_b = format!("zid_b_{unique}");
    let other_database = format!("zid_db_{unique}");
    let table = format!("items_{unique}");

    for (schema, label) in [
        (schema_a.as_str(), "a"),
        (schema_b.as_str(), "b"),
        ("public", "public"),
    ] {
        harness
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .await;
        for sql_name in [table.as_str(), "odd.name", "Mixed Case"] {
            let sql_name = if sql_name == table {
                format!("\"{schema}\".\"{table}\"")
            } else {
                format!("\"{schema}\".\"{sql_name}_{unique}\"")
            };
            harness
                .execute(&format!(
                    "CREATE TABLE {sql_name} (id INT PRIMARY KEY, label TEXT)"
                ))
                .await;
            harness
                .execute(&format!("INSERT INTO {sql_name} VALUES (1, '{label}')"))
                .await;
        }
    }
    harness
        .execute(&format!("CREATE DATABASE \"{other_database}\""))
        .await;

    let mut failures = Vec::new();

    // Sidebar leaf (and its context menu) in a schema group: "<schema>.<name>" + the database node.
    for (schema, expected) in [
        (&schema_a, "a"),
        (&schema_b, "b"),
        (&"public".to_string(), "public"),
    ] {
        for name in [
            table.clone(),
            format!("odd.name_{unique}"),
            format!("Mixed Case_{unique}"),
        ] {
            // A sidebar leaf, and the joined name an older session stored.
            for table in [
                TableRef::new(Some(database.clone()), Some(schema.clone()), &name),
                TableRef::new(Some(database.clone()), None, format!("{schema}.{name}")),
            ] {
                match harness.open(table.clone()).await {
                    Ok(rows)
                        if rows.iter().any(|row| row.contains(expected)) && rows.len() == 1 => {}
                    other => failures.push(format!("open {table:?}: {other:?}")),
                }
            }
        }
    }

    // A table in a non-default database.
    {
        let other = Harness::connect(
            SavedConnection::new("other".into(), "postgres".into())
                .with_param("host", &host)
                .with_param(
                    "port",
                    &env_value("ZQLZ_SMOKE_POSTGRES_PORT").unwrap_or_else(|| "5432".into()),
                )
                .with_param("database", &other_database)
                .with_param(
                    "user",
                    &env_value("ZQLZ_SMOKE_POSTGRES_USER").unwrap_or_else(|| "postgres".into()),
                )
                .with_param(
                    "password",
                    &env_value("ZQLZ_SMOKE_POSTGRES_PASSWORD").unwrap_or_default(),
                )
                .with_param("ssl_mode", "disable"),
        )
        .await;
        other
            .execute(&format!(
                "CREATE TABLE public.\"{table}\" (id INT PRIMARY KEY, label TEXT)"
            ))
            .await;
        other
            .execute(&format!(
                "INSERT INTO public.\"{table}\" VALUES (1, 'other-db')"
            ))
            .await;
    }
    match harness
        .open(TableRef::new(
            Some(other_database.clone()),
            Some("public".into()),
            &table,
        ))
        .await
    {
        Ok(rows) if rows.iter().any(|row| row.contains("other-db")) => {}
        other => failures.push(format!("open in other database: {other:?}")),
    }

    // Destructive menu actions on <schema_a>.<table> while browsing `database`.
    if let Err(error) = harness
        .menu_empty(TableRef::new(
            Some(database.clone()),
            Some(schema_a.clone()),
            &table,
        ))
        .await
    {
        failures.push(error);
    }
    let counts = (
        harness.count(&format!("\"{schema_a}\".\"{table}\"")).await,
        harness.count(&format!("public.\"{table}\"")).await,
    );
    if counts != (Some(0), Some(1)) {
        failures.push(format!(
            "empty {schema_a}.{table} via menu: ({schema_a}, public) counts = {counts:?}"
        ));
    }
    if let Err(error) = harness
        .menu_delete(TableRef::new(
            Some(database.clone()),
            Some(schema_a.clone()),
            &table,
        ))
        .await
    {
        failures.push(error);
    }
    let exists = (
        harness
            .count(&format!("\"{schema_a}\".\"{table}\""))
            .await
            .is_some(),
        harness
            .count(&format!("public.\"{table}\""))
            .await
            .is_some(),
    );
    if exists != (false, true) {
        failures.push(format!(
            "delete {schema_a}.{table} via menu: ({schema_a}, public) exists = {exists:?}"
        ));
    }

    for schema in [&schema_a, &schema_b] {
        harness
            .execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await;
    }
    for name in [
        table.clone(),
        format!("odd.name_{unique}"),
        format!("Mixed Case_{unique}"),
    ] {
        harness
            .execute(&format!("DROP TABLE IF EXISTS public.\"{name}\""))
            .await;
    }
    harness
        .connections
        .disconnect(harness.connection_id)
        .await
        .ok();
    let cleanup = Harness::connect(
        SavedConnection::new("cleanup".into(), "postgres".into())
            .with_param("host", &host)
            .with_param(
                "port",
                &env_value("ZQLZ_SMOKE_POSTGRES_PORT").unwrap_or_else(|| "5432".into()),
            )
            .with_param("database", &database)
            .with_param(
                "user",
                &env_value("ZQLZ_SMOKE_POSTGRES_USER").unwrap_or_else(|| "postgres".into()),
            )
            .with_param(
                "password",
                &env_value("ZQLZ_SMOKE_POSTGRES_PASSWORD").unwrap_or_default(),
            )
            .with_param("ssl_mode", "disable"),
    )
    .await;
    cleanup
        .execute(&format!(
            "DROP DATABASE IF EXISTS \"{other_database}\" WITH (FORCE)"
        ))
        .await;

    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test]
async fn mysql_tables_open_and_mutate_by_identity() {
    let Some(host) = env_value("ZQLZ_SMOKE_MYSQL_HOST") else {
        return;
    };
    let default_database = env_value("ZQLZ_SMOKE_MYSQL_DATABASE").unwrap_or_else(|| "mysql".into());
    let saved = SavedConnection::new("identity".into(), "mysql".into())
        .with_param("host", &host)
        .with_param(
            "port",
            &env_value("ZQLZ_SMOKE_MYSQL_PORT").unwrap_or_else(|| "3306".into()),
        )
        .with_param("database", &default_database)
        .with_param(
            "user",
            &env_value("ZQLZ_SMOKE_MYSQL_USER").unwrap_or_else(|| "root".into()),
        )
        .with_param(
            "password",
            &env_value("ZQLZ_SMOKE_MYSQL_PASSWORD").unwrap_or_default(),
        );
    let harness = Harness::connect(saved).await;

    let unique = suffix();
    let database_a = format!("zid_a_{unique}");
    let database_b = format!("zid_b_{unique}");

    for (database, label) in [(&database_a, "a"), (&database_b, "b")] {
        harness
            .execute(&format!("CREATE DATABASE `{database}`"))
            .await;
        for name in ["items", "odd.name", "Mixed Case"] {
            harness
                .execute(&format!(
                    "CREATE TABLE `{database}`.`{name}` (id INT PRIMARY KEY, label TEXT)"
                ))
                .await;
            harness
                .execute(&format!(
                    "INSERT INTO `{database}`.`{name}` VALUES (1, '{label}')"
                ))
                .await;
        }
        harness
            .execute(&format!(
                "CREATE VIEW `{database}`.`items_view` AS SELECT * FROM `{database}`.`items`"
            ))
            .await;
    }

    let mut failures = Vec::new();

    // Sidebar leaf under a database node: bare name + database.
    for (database, expected) in [(&database_a, "a"), (&database_b, "b")] {
        for (name, is_view) in [
            ("items", false),
            ("odd.name", false),
            ("Mixed Case", false),
            ("items_view", true),
        ] {
            let table = TableRef::new(Some(database.clone()), Some(database.clone()), name);
            let table = if is_view { table.view() } else { table };
            match harness.open(table).await {
                Ok(rows) if rows.len() == 1 && rows[0].contains(expected) => {}
                other => failures.push(format!("open {database}.{name}: {other:?}")),
            }
        }
    }

    // Destructive menu actions on <database_a>.items must leave <database_b>.items alone.
    if let Err(error) = harness
        .menu_empty(TableRef::new(
            Some(database_a.clone()),
            Some(database_a.clone()),
            "items",
        ))
        .await
    {
        failures.push(error);
    }
    let counts = (
        harness.count(&format!("`{database_a}`.`items`")).await,
        harness.count(&format!("`{database_b}`.`items`")).await,
    );
    if counts != (Some(0), Some(1)) {
        failures.push(format!(
            "empty {database_a}.items: counts (a, b) = {counts:?}"
        ));
    }

    if let Err(error) = harness
        .menu_empty(TableRef::new(
            Some(database_a.clone()),
            Some(database_a.clone()),
            "odd.name",
        ))
        .await
    {
        failures.push(error);
    }
    if harness.count(&format!("`{database_a}`.`odd.name`")).await != Some(0) {
        failures.push(format!("empty {database_a}.`odd.name` did not empty it"));
    }

    for database in [&database_a, &database_b] {
        harness
            .execute(&format!("DROP DATABASE `{database}`"))
            .await;
    }

    assert!(failures.is_empty(), "{failures:#?}");
}

// ============ Paging against real servers ============

const DIGITS: &str = "(SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 \
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 \
    UNION ALL SELECT 8 UNION ALL SELECT 9)";

/// 2,500 rows whose `label` is NULL on every fifth, so a sort on it has ties
/// and NULLs to place.
fn seed_paging_rows_sql(qualified_table: &str) -> String {
    format!(
        "INSERT INTO {qualified_table} (id, label) \
         SELECT id, CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT('L', id % 13) END \
         FROM (SELECT a.n + b.n * 10 + c.n * 100 + d.n * 1000 + 1 AS id \
               FROM {DIGITS} a, {DIGITS} b, {DIGITS} c, {DIGITS} d) numbers \
         WHERE id <= 2500"
    )
}

/// Paging must give the same rows however a page is read, survive a cancelled
/// fetch, and estimate an unfiltered slow count.
async fn paging_failures(
    harness: &Harness,
    table: &str,
    schema: &str,
    label_sort: &str,
) -> Vec<String> {
    let connection = harness
        .connections
        .get_connection(harness.connection_id)
        .expect("main connection");
    let fetch = |query: QuerySnapshot, mode: FetchMode| {
        let connection = connection.clone();
        async move {
            let plan = plan_fetch(mode, &query).expect("a fetch plan");
            harness
                .tables
                .fetch_rows(
                    connection,
                    &TableRef::new(None, Some(schema.to_string()), table),
                    &query,
                    plan,
                )
                .await
        }
    };
    let ids = |outcome: &FetchOutcome| -> Vec<String> {
        outcome
            .result
            .rows
            .iter()
            .filter_map(|row| row.values.first().map(ToString::to_string))
            .collect()
    };
    let base = QuerySnapshot {
        pk_columns: vec![connection.quote_identifier("id")],
        page_size: Some(300),
        current_page: 1,
        fast_exact_count: connection.supports_fast_exact_count(),
        ..QuerySnapshot::default()
    };
    let mut failures = Vec::new();

    match fetch(base.clone(), FetchMode::Page(1)).await {
        Ok(outcome)
            if outcome
                .total
                .as_ref()
                .is_some_and(|total| total.is_estimated) => {}
        other => failures.push(format!("unfiltered estimate: {:?}", other.map(|o| o.total))),
    }

    let sorted = QuerySnapshot {
        order_by_clauses: vec![label_sort.to_string()],
        ..base.clone()
    };
    let last = fetch(sorted.clone(), FetchMode::Last)
        .await
        .expect("last page");
    if last.plan.page != 9 || last.result.rows.len() != 100 {
        failures.push(format!(
            "last page: page {} with {} rows",
            last.plan.page,
            last.result.rows.len()
        ));
    }
    let forward_last = fetch(sorted.clone(), FetchMode::Page(9))
        .await
        .expect("page 9");
    if ids(&forward_last) != ids(&last) {
        failures.push("last page read from the end differs from page 9 read forward".into());
    }

    let forward = fetch(sorted.clone(), FetchMode::Page(7))
        .await
        .expect("page 7");
    let with_total = QuerySnapshot {
        known_total: last.total.clone(),
        ..sorted.clone()
    };
    let reversed = fetch(with_total, FetchMode::Page(7))
        .await
        .expect("reversed page 7");
    if !matches!(reversed.plan.window, RowWindow::FromEnd { .. }) {
        failures.push(format!(
            "page 7 with an exact total read {:?}",
            reversed.plan.window
        ));
    }
    if ids(&forward) != ids(&reversed) {
        failures.push("page 7 read from the end differs from page 7 read forward".into());
    }

    // Dropping a fetch mid-flight is how the viewer cancels; the connection must
    // still serve the next one.
    if tokio::time::timeout(
        std::time::Duration::from_micros(50),
        fetch(sorted.clone(), FetchMode::Page(5)),
    )
    .await
    .is_ok()
    {
        failures.push("fetch finished before it could be cancelled".into());
    }
    match fetch(sorted, FetchMode::Page(1)).await {
        Ok(outcome) if outcome.result.rows.len() == 300 => {}
        other => failures.push(format!(
            "fetch after a cancelled one: {:?}",
            other.map(|outcome| outcome.result.rows.len())
        )),
    }

    failures
}

#[tokio::test]
async fn postgres_paging_reads_agree() {
    let Some(host) = env_value("ZQLZ_SMOKE_POSTGRES_HOST") else {
        return;
    };
    let harness = Harness::connect(
        SavedConnection::new("paging".into(), "postgres".into())
            .with_param("host", &host)
            .with_param(
                "port",
                &env_value("ZQLZ_SMOKE_POSTGRES_PORT").unwrap_or_else(|| "5432".into()),
            )
            .with_param(
                "database",
                &env_value("ZQLZ_SMOKE_POSTGRES_DATABASE").unwrap_or_else(|| "postgres".into()),
            )
            .with_param(
                "user",
                &env_value("ZQLZ_SMOKE_POSTGRES_USER").unwrap_or_else(|| "postgres".into()),
            )
            .with_param(
                "password",
                &env_value("ZQLZ_SMOKE_POSTGRES_PASSWORD").unwrap_or_default(),
            )
            .with_param("ssl_mode", "disable"),
    )
    .await;
    let schema = format!("zid_paging_{}", suffix());
    let qualified = format!("\"{schema}\".\"items\"");
    harness
        .execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await;
    harness
        .execute(&format!(
            "CREATE TABLE {qualified} (id INT PRIMARY KEY, label TEXT)"
        ))
        .await;
    harness.execute(&seed_paging_rows_sql(&qualified)).await;
    // Rewrite the heap so scan order is no longer key order.
    harness
        .execute(&format!(
            "UPDATE {qualified} SET label = label WHERE id % 2 = 0"
        ))
        .await;
    harness.execute(&format!("ANALYZE {qualified}")).await;

    let failures = paging_failures(&harness, "items", &schema, "\"label\" ASC NULLS FIRST").await;

    harness
        .execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await;
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test]
async fn mysql_paging_reads_agree() {
    let Some(host) = env_value("ZQLZ_SMOKE_MYSQL_HOST") else {
        return;
    };
    let harness = Harness::connect(
        SavedConnection::new("paging".into(), "mysql".into())
            .with_param("host", &host)
            .with_param(
                "port",
                &env_value("ZQLZ_SMOKE_MYSQL_PORT").unwrap_or_else(|| "3306".into()),
            )
            .with_param(
                "database",
                &env_value("ZQLZ_SMOKE_MYSQL_DATABASE").unwrap_or_else(|| "mysql".into()),
            )
            .with_param(
                "user",
                &env_value("ZQLZ_SMOKE_MYSQL_USER").unwrap_or_else(|| "root".into()),
            )
            .with_param(
                "password",
                &env_value("ZQLZ_SMOKE_MYSQL_PASSWORD").unwrap_or_default(),
            ),
    )
    .await;
    let database = format!("zid_paging_{}", suffix());
    let qualified = format!("`{database}`.`items`");
    harness
        .execute(&format!("CREATE DATABASE `{database}`"))
        .await;
    harness
        .execute(&format!(
            "CREATE TABLE {qualified} (id INT PRIMARY KEY, label TEXT)"
        ))
        .await;
    harness.execute(&seed_paging_rows_sql(&qualified)).await;
    harness.execute(&format!("ANALYZE TABLE {qualified}")).await;

    let failures = paging_failures(&harness, "items", &database, "`label` ASC").await;

    harness
        .execute(&format!("DROP DATABASE `{database}`"))
        .await;
    assert!(failures.is_empty(), "{failures:#?}");
}
