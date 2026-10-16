//! Opt-in checks against a real MySQL/MariaDB server. Set
//! `ZQLZ_MYSQL_INTEGRATION=1` plus `ZQLZ_MYSQL_HOST`, `ZQLZ_MYSQL_PORT`,
//! `ZQLZ_MYSQL_USER`, `ZQLZ_MYSQL_PASSWORD` and `ZQLZ_MYSQL_DATABASE`. Each
//! test creates and drops its own scratch database.
use super::*;
use crate::MySqlConnectOptions;
use std::time::Duration;

fn enabled() -> bool {
    std::env::var("ZQLZ_MYSQL_INTEGRATION").as_deref() == Ok("1")
}

fn env(name: &str, default: &str) -> String {
    std::env::var(format!("ZQLZ_MYSQL_{name}")).unwrap_or_else(|_| default.to_string())
}

fn options(database: Option<&str>) -> MySqlConnectOptions {
    MySqlConnectOptions {
        host: env("HOST", "127.0.0.1"),
        port: env("PORT", "3306").parse().expect("numeric port"),
        database: database.map(ToString::to_string),
        user: Some(env("USER", "root")),
        password: Some(env("PASSWORD", "")),
        ssl_opts: None,
        ssh_tunnel: None,
    }
}

/// A connection with a fresh scratch database selected on the session.
async fn scratch_connection(name: &str) -> Option<Arc<MySqlConnection>> {
    if !enabled() {
        eprintln!("skipping live MySQL test; set ZQLZ_MYSQL_INTEGRATION=1");
        return None;
    }
    let default_db = env("DATABASE", "mysql");
    let connection = Arc::new(
        MySqlConnection::connect(options(Some(&default_db)))
            .await
            .expect("connect to MySQL"),
    );
    let database = format!("zqlz_b_exec_{name}");
    for sql in [
        format!("DROP DATABASE IF EXISTS {database}"),
        format!("CREATE DATABASE {database}"),
        format!("USE {database}"),
    ] {
        connection.execute(&sql, &[]).await.expect("prepare scratch database");
    }
    Some(connection)
}

async fn drop_scratch(connection: &MySqlConnection, name: &str) {
    connection
        .execute("USE mysql", &[])
        .await
        .expect("leave scratch database");
    connection
        .execute(&format!("DROP DATABASE IF EXISTS zqlz_b_exec_{name}"), &[])
        .await
        .expect("drop scratch database");
}

fn first_string(result: &QueryResult) -> String {
    result.rows[0].values[0].to_string()
}

#[tokio::test]
async fn session_state_survives_between_calls() {
    let Some(db) = scratch_connection("session").await else {
        return;
    };

    // Variables and USE stick.
    db.execute("SET @y = 7", &[]).await.unwrap();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT @y", &[]).await.unwrap()), "7");
    assert_eq!(
        first_string(&Connection::query(&*db, "SELECT DATABASE()", &[]).await.unwrap()),
        "zqlz_b_exec_session"
    );
    assert_eq!(
        db.current_database_name().await.unwrap().as_deref(),
        Some("zqlz_b_exec_session")
    );

    // One server connection serves every call.
    let id1 = first_string(&Connection::query(&*db, "SELECT CONNECTION_ID()", &[]).await.unwrap());
    let id2 = first_string(&Connection::query(&*db, "SELECT CONNECTION_ID()", &[]).await.unwrap());
    assert_eq!(id1, id2);

    // A user-issued transaction spans calls, and the pool (introspection) does
    // not see it.
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(5))", &[])
        .await
        .unwrap();
    db.execute("START TRANSACTION", &[]).await.unwrap();
    db.execute("INSERT INTO t (v) VALUES ('a')", &[]).await.unwrap();
    let in_transaction = "SELECT COUNT(*) FROM information_schema.INNODB_TRX \
                          WHERE trx_mysql_thread_id = CONNECTION_ID()";
    assert_eq!(first_string(&Connection::query(&*db, in_transaction, &[]).await.unwrap()), "1");
    assert_eq!(first_string(&Connection::query(&*db, "SELECT COUNT(*) FROM t", &[]).await.unwrap()), "1");
    // Inside this crate `MySqlConnection::query` is the pool path: a different
    // server connection, which does not see the uncommitted row.
    assert_eq!(
        first_string(&MySqlConnection::query(&db, "SELECT COUNT(*) FROM zqlz_b_exec_session.t", &[]).await.unwrap()),
        "0"
    );
    db.execute("ROLLBACK", &[]).await.unwrap();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT COUNT(*) FROM t", &[]).await.unwrap()), "0");

    // The Transaction API joins the same session.
    let tx = db.begin_transaction().await.unwrap();
    tx.execute("INSERT INTO t (v) VALUES ('b')", &[]).await.unwrap();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT COUNT(*) FROM t", &[]).await.unwrap()), "1");
    tx.rollback().await.unwrap();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT COUNT(*) FROM t", &[]).await.unwrap()), "0");

    // Dropping an open transaction rolls it back.
    let tx = db.begin_transaction().await.unwrap();
    tx.execute("INSERT INTO t (v) VALUES ('c')", &[]).await.unwrap();
    drop(tx);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(first_string(&Connection::query(&*db, "SELECT COUNT(*) FROM t", &[]).await.unwrap()), "0");

    // Introspection runs on the pool: it sees the default database, not the
    // session's, and never waits for the session.
    assert_eq!(
        first_string(&MySqlConnection::query(&db, "SELECT DATABASE()", &[]).await.unwrap()),
        env("DATABASE", "mysql")
    );

    drop_scratch(&db, "session").await;
}

#[tokio::test]
async fn result_sets_come_from_columns_not_keywords() {
    let Some(db) = scratch_connection("kinds").await else {
        return;
    };
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(20) UNIQUE, n INT DEFAULT 0)", &[])
        .await
        .unwrap();
    db.execute("INSERT INTO t (v, n) VALUES ('a', 1), ('b', 2)", &[]).await.unwrap();

    // Each of these answers with a result set through `execute` as well.
    for sql in [
        "SHOW TABLES",
        "DESCRIBE t",
        "EXPLAIN SELECT * FROM t",
        "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
        "TABLE t",
        "VALUES ROW(1), ROW(2)",
        "SHOW CREATE TABLE t",
        "SELECT 'it\\'s' AS s, 'tab\\there' AS t",
        "# hi\nSELECT /*!50000 1 */ AS one -- tail",
    ] {
        let result = db.execute(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(result.is_query, "{sql} should come back as a result set");
        let rows = result.result.expect(sql);
        assert!(!rows.columns.is_empty() && !rows.rows.is_empty(), "{sql}");
    }
    let escaped = Connection::query(&*db, "SELECT 'it\\'s' AS s", &[]).await.unwrap();
    assert_eq!(first_string(&escaped), "it's");

    // EXECUTE of a prepared statement returns its rows.
    db.execute("PREPARE s FROM 'SELECT ? + 1 AS r'", &[]).await.unwrap();
    db.execute("SET @a = 41", &[]).await.unwrap();
    let executed = db.execute("EXECUTE s USING @a", &[]).await.unwrap();
    assert!(executed.is_query);
    assert_eq!(first_string(&executed.result.unwrap()), "42");
    db.execute("DEALLOCATE PREPARE s", &[]).await.unwrap();

    // SELECT ... INTO produces no rows, only a count.
    let into = db.execute("SELECT 5 INTO @z", &[]).await.unwrap();
    assert!(!into.is_query);
    assert_eq!(first_string(&Connection::query(&*db, "SELECT @z", &[]).await.unwrap()), "5");

    // Commands report affected rows (rows matched, by design).
    assert_eq!(db.execute("UPDATE t SET n = 1 WHERE v = 'a'", &[]).await.unwrap().affected_rows, 1);
    assert_eq!(db.execute("UPDATE t SET n = n + 1", &[]).await.unwrap().affected_rows, 2);
    let upsert = db
        .execute("INSERT INTO t (v, n) VALUES ('a', 9) ON DUPLICATE KEY UPDATE n = VALUES(n)", &[])
        .await
        .unwrap();
    assert!(upsert.affected_rows >= 1, "upsert reported {}", upsert.affected_rows);
    let replace = db.execute("REPLACE INTO t (id, v, n) VALUES (1, 'a', 5)", &[]).await.unwrap();
    assert!(replace.affected_rows >= 1, "replace reported {}", replace.affected_rows);
    assert_eq!(db.execute("DELETE FROM t WHERE v = 'b'", &[]).await.unwrap().affected_rows, 1);

    // The same commands through `query` keep the count too.
    let via_query = Connection::query(&*db, "UPDATE t SET n = 0", &[]).await.unwrap();
    assert!(via_query.columns.is_empty());
    assert_eq!(via_query.affected_rows, 1);

    // CALL: result sets, OUT parameters, DML only.
    db.execute("CREATE PROCEDURE p2() BEGIN SELECT 1 AS a; SELECT 'x' AS b, 2 AS c; END", &[])
        .await
        .unwrap();
    let call = Connection::query(&*db, "CALL p2()", &[]).await.unwrap();
    assert_eq!(call.columns.len(), 1);
    assert_eq!(first_string(&call), "1");
    assert!(
        call.warnings.iter().any(|w| w.contains("Result set 2 (1 row, 2 columns)")),
        "second result set must be reported: {:?}",
        call.warnings
    );
    let via_execute = db.execute("CALL p2()", &[]).await.unwrap();
    assert!(via_execute.is_query);
    assert!(via_execute.result.unwrap().warnings.iter().any(|w| w.contains("Result set 2")));

    db.execute("CREATE PROCEDURE p3(IN a INT, OUT b INT) BEGIN SET b = a * 2; END", &[])
        .await
        .unwrap();
    db.execute("CALL p3(21, @o)", &[]).await.unwrap();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT @o", &[]).await.unwrap()), "42");

    db.execute("CREATE PROCEDURE p4() BEGIN INSERT INTO t (v) VALUES ('fromproc'); END", &[])
        .await
        .unwrap();
    let dml_only = db.execute("CALL p4()", &[]).await.unwrap();
    assert!(!dml_only.is_query);
    assert_eq!(dml_only.affected_rows, 1);

    // Bind parameters in a statement that also has hostile text.
    let bound = db
        .query("SELECT 'it\\'s ?' AS s, ? AS p -- ?\n", &[Value::Int32(7)])
        .await
        .unwrap();
    assert_eq!(bound.rows[0].values[1].to_string(), "7");

    drop_scratch(&db, "kinds").await;
}

#[tokio::test]
async fn warnings_are_surfaced_without_extra_round_trips() {
    let Some(db) = scratch_connection("warn").await else {
        return;
    };
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(5))", &[])
        .await
        .unwrap();

    let div = Connection::query(&*db, "SELECT 1/0 AS x", &[]).await.unwrap();
    assert_eq!(div.warnings, vec!["Warning 1365: Division by 0"]);

    db.execute("SET sql_mode = ''", &[]).await.unwrap();
    let insert = db
        .execute("INSERT INTO t (v) VALUES ('far too long a value')", &[])
        .await
        .unwrap();
    let warnings = insert.result.expect("warnings ride on an empty result").warnings;
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].starts_with("Warning 1265:"), "{warnings:?}");
    assert_eq!(insert.affected_rows, 1);

    // No warnings, nothing reported; later statements do not inherit old ones.
    let clean = Connection::query(&*db, "SELECT 1 AS x", &[]).await.unwrap();
    assert!(clean.warnings.is_empty());
    assert!(db.execute("DELETE FROM t", &[]).await.unwrap().result.is_none());

    // SHOW WARNINGS shows the list once, not twice.
    Connection::query(&*db, "SELECT CAST('abc' AS SIGNED)", &[]).await.unwrap();
    let shown = Connection::query(&*db, "SHOW WARNINGS", &[]).await.unwrap();
    assert_eq!(shown.rows.len(), 1);
    assert!(shown.warnings.is_empty(), "{:?}", shown.warnings);

    // A DROP ... IF EXISTS note is a warning too.
    let note = db.execute("DROP TABLE IF EXISTS no_such_table", &[]).await.unwrap();
    assert!(note.result.unwrap().warnings[0].starts_with("Note 1051:"));

    drop_scratch(&db, "warn").await;
}

#[tokio::test]
async fn errors_read_like_the_mysql_client() {
    let Some(db) = scratch_connection("errors").await else {
        return;
    };

    let missing = Connection::query(&*db, "SELECT * FROM nope", &[]).await.unwrap_err().to_string();
    assert_eq!(missing, "ERROR 42S02 (1146): Table 'zqlz_b_exec_errors.nope' doesn't exist");

    let syntax = db.execute("SELEC 1", &[]).await.unwrap_err().to_string();
    assert!(syntax.starts_with("ERROR 42000 (1064): You have an error in your SQL syntax"), "{syntax}");
    assert!(syntax.contains("near 'SELEC 1' at line 1"), "{syntax}");

    // The session survives a SQL error.
    db.execute("SET @k = 1", &[]).await.unwrap();
    let _ = Connection::query(&*db, "SELECT * FROM nope", &[]).await;
    assert_eq!(first_string(&Connection::query(&*db, "SELECT @k", &[]).await.unwrap()), "1");

    drop_scratch(&db, "errors").await;
}

#[tokio::test]
async fn connect_failures_name_the_cause_once() {
    if !enabled() {
        return;
    }
    let mut wrong_password = options(Some(&env("DATABASE", "mysql")));
    wrong_password.password = Some("definitely-wrong".to_string());
    let error = MySqlConnection::connect(wrong_password).await.err().expect("rejected").to_string();
    assert!(error.contains("Access denied"), "{error}");
    assert!(error.contains("(1045)"), "{error}");
    assert_eq!(error.matches("Failed to connect").count(), 1, "{error}");

    let unknown = MySqlConnection::connect(options(Some("zqlz_nope_db")))
        .await
        .err()
        .expect("unknown database")
        .to_string();
    assert!(unknown.contains("Unknown database 'zqlz_nope_db'"), "{unknown}");
    assert_eq!(unknown.matches("Failed to connect").count(), 1, "{unknown}");

    let mut unreachable = options(None);
    unreachable.port = 1;
    let refused = MySqlConnection::connect(unreachable).await.err().expect("refused").to_string();
    assert!(refused.contains("127.0.0.1:1") && refused.to_lowercase().contains("refused"), "{refused}");
    assert_eq!(refused.matches("Failed to connect").count(), 1, "{refused}");
}

#[tokio::test]
async fn cancel_kills_the_running_statement_and_keeps_the_session() {
    let Some(db) = scratch_connection("cancel").await else {
        return;
    };
    db.execute("SET @kept = 'yes'", &[]).await.unwrap();

    let handle = db.cancel_handle().expect("cancel handle");
    let running = {
        let db = db.clone();
        tokio::spawn(async move { Connection::query(&*db, "SELECT SLEEP(20)", &[]).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = std::time::Instant::now();
    handle.cancel();
    let outcome = running.await.unwrap();

    // SLEEP interrupted by KILL QUERY returns 1 rather than raising an error;
    // the row loop then sees the moved epoch and reports the cancel. A statement
    // the server aborts instead says "interrupted". Either way it came back fast.
    assert!(started.elapsed() < Duration::from_secs(5), "cancel took {:?}", started.elapsed());
    let error = outcome.expect_err("a cancelled statement is not a success");
    assert!(
        matches!(error, ZqlzError::Cancelled) || error.to_string().contains("interrupted"),
        "{error}"
    );

    // Same connection, state intact.
    assert_eq!(first_string(&Connection::query(&*db, "SELECT @kept", &[]).await.unwrap()), "yes");

    // A cancel with nothing running is harmless and does not poison the next call.
    handle.cancel();
    assert_eq!(first_string(&Connection::query(&*db, "SELECT 2", &[]).await.unwrap()), "2");

    drop_scratch(&db, "cancel").await;
}

#[tokio::test]
async fn introspection_does_not_queue_behind_a_running_statement() {
    let Some(db) = scratch_connection("busy").await else {
        return;
    };
    let running = {
        let db = db.clone();
        tokio::spawn(async move { Connection::query(&*db, "SELECT SLEEP(3)", &[]).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(db.is_busy());

    let started = std::time::Instant::now();
    let scope = db.current_database_name().await.unwrap();
    let tables = db
        .as_schema_introspection()
        .unwrap()
        .list_tables(Some("mysql"))
        .await
        .unwrap();
    assert!(!tables.is_empty());
    assert!(started.elapsed() < Duration::from_secs(2), "took {:?}", started.elapsed());
    assert!(scope.is_some());

    running.await.unwrap().unwrap();
    drop_scratch(&db, "busy").await;
}

#[tokio::test]
async fn a_lost_session_reconnects_and_says_so() {
    let Some(db) = scratch_connection("lost").await else {
        return;
    };
    db.execute("SET @gone = 1", &[]).await.unwrap();
    let id = first_string(&Connection::query(&*db, "SELECT CONNECTION_ID()", &[]).await.unwrap());

    // Kill the session from outside.
    let killer = MySqlConnection::connect(options(Some(&env("DATABASE", "mysql")))).await.unwrap();
    killer.execute(&format!("KILL {id}"), &[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The statement that discovers the loss fails; the next one reconnects.
    let mut recovered = None;
    for _ in 0..2 {
        if let Ok(result) = Connection::query(&*db, "SELECT @gone", &[]).await {
            recovered = Some(result);
            break;
        }
    }
    let recovered = recovered.expect("session reconnects");
    assert!(recovered.rows[0].values[0].is_null(), "state was reset");
    assert!(
        recovered.warnings.iter().any(|w| w.contains("re-established")),
        "{:?}",
        recovered.warnings
    );
    // Only the first statement after the loss carries the notice.
    assert!(Connection::query(&*db, "SELECT 1", &[]).await.unwrap().warnings.is_empty());

    killer.close().await.unwrap();
    db.execute("DROP DATABASE IF EXISTS zqlz_b_exec_lost", &[]).await.unwrap();
}

/// Timing of a wide 200k-row read, to catch superlinear behaviour in the driver.
/// Read-only. Opt in with `ZQLZ_MYSQL_PERF=1`.
#[tokio::test]
async fn wide_result_scales_linearly() {
    if !enabled() || std::env::var("ZQLZ_MYSQL_PERF").as_deref() != Ok("1") {
        return;
    }
    let db = MySqlConnection::connect(options(Some("erp_lab"))).await.unwrap();
    let mut timings = Vec::new();
    for limit in [25_000u32, 50_000, 100_000, 200_000] {
        let started = std::time::Instant::now();
        let result = db
            .query(&format!("SELECT * FROM erp_lab.orders_order_items LIMIT {limit}"), &[])
            .await
            .unwrap();
        assert_eq!(result.rows.len(), limit as usize);
        let elapsed = started.elapsed();
        eprintln!("{limit} rows x {} cols: {elapsed:?}", result.columns.len());
        timings.push(elapsed.as_secs_f64());
    }
    // 8x the rows should cost nowhere near 8x^2 the time.
    assert!(timings[3] < timings[0] * 8.0 * 3.0, "{timings:?}");
}
