//! MySQL: valid statements must never produce diagnostics; unknown names exactly one.

use super::test_helpers::create_test_lsp_with_dialect;
use crate::{ColumnInfo, SqlDialect, TableInfo, ViewInfo};
use lsp_types::{Diagnostic, DiagnosticSeverity};
use zqlz_ui::widgets::Rope;

fn column(table: &str, name: &str, pk: bool) -> ColumnInfo {
    ColumnInfo {
        table_name: table.to_string(),
        name: name.to_string(),
        data_type: "INT".to_string(),
        nullable: !pk,
        default_value: None,
        is_primary_key: pk,
        is_foreign_key: false,
        comment: None,
    }
}

/// Modeled on `zqlz_w`: unqualified keys, active database `zqlz_w`, views, names
/// that need backtick quoting.
fn mysql_lsp() -> crate::SqlLsp {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let cache = &mut lsp.schema_cache;
    cache.tables.clear();
    cache.columns_by_table.clear();
    cache.database_name = Some("zqlz_w".to_string());
    let tables: &[(&str, &[&str])] = &[
        (
            "users",
            &["id", "name", "email", "status", "created_at", "payload"],
        ),
        (
            "orders",
            &["id", "user_id", "total", "status", "created_at"],
        ),
        ("order_items", &["id", "order_id", "sku", "qty"]),
        ("select", &["id", "from"]),
        ("Table With Spaces", &["id", "a b"]),
        ("we`ird", &["id", "x"]),
        ("t_gen", &["id", "a", "b", "total"]),
        ("1st_table", &["id"]),
    ];
    for (table, columns) in tables {
        cache.tables.insert(
            table.to_string(),
            TableInfo {
                name: table.to_string(),
                schema: None,
                comment: None,
                row_count: None,
                table_type: zqlz_core::TableType::Table,
            },
        );
        cache.columns_by_table.insert(
            table.to_string(),
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| column(table, c, i == 0))
                .collect(),
        );
    }
    cache.views.insert(
        "v_active_users".to_string(),
        ViewInfo {
            name: "v_active_users".to_string(),
            schema: None,
            definition: None,
            is_materialized: false,
        },
    );
    cache.columns_by_table.insert(
        "v_active_users".to_string(),
        ["id", "name"]
            .iter()
            .map(|c| column("v_active_users", c, false))
            .collect(),
    );
    lsp
}

fn problems(diagnostics: &[Diagnostic]) -> Vec<String> {
    diagnostics
        .iter()
        .filter(|d| {
            matches!(
                d.severity,
                Some(DiagnosticSeverity::ERROR | DiagnosticSeverity::WARNING)
            )
        })
        .map(|d| format!("[{}] {}", d.source.as_deref().unwrap_or("?"), d.message))
        .collect()
}

fn check(sql: &str) -> Vec<String> {
    problems(&mysql_lsp().validate_sql(&Rope::from(sql)))
}

#[test]
fn valid_mysql_statements_produce_no_diagnostics() {
    let cases: &[&str] = &[
        // quoting
        "SELECT `id` FROM `select`",
        "SELECT `from` FROM `select` WHERE `from` > 1",
        "SELECT `a b` FROM `Table With Spaces`",
        "SELECT x FROM `we``ird`",
        "SELECT id FROM `1st_table`",
        "SELECT * FROM `zqlz_w`.`users`",
        "SELECT * FROM zqlz_w.users u WHERE u.id = 1",
        // cross-database + system schemas
        "SELECT * FROM other_db.some_table",
        "SELECT * FROM `other_db`.`some_table` t JOIN users u ON u.id = t.user_id",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'zqlz_w'",
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()",
        "SELECT user, host FROM mysql.user",
        "SELECT * FROM performance_schema.threads",
        "SELECT * FROM performance_schema.events_statements_summary_by_digest LIMIT 5",
        "SELECT * FROM sys.schema_table_statistics",
        "SELECT * FROM sys.`host_summary`",
        "SELECT * FROM v_active_users",
        "SELECT v.name FROM v_active_users v WHERE v.id = 1",
        // derived tables / CTEs
        "SELECT d.c FROM (SELECT COUNT(*) AS c FROM users) AS d",
        "WITH x AS (SELECT id FROM users) SELECT id FROM x",
        "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT n FROM seq",
        "WITH RECURSIVE tree AS (SELECT id, 0 AS depth FROM users WHERE id = 1 UNION ALL SELECT u.id, t.depth + 1 FROM users u JOIN tree t ON u.id = t.id WHERE t.depth < 3) SELECT * FROM tree",
        // aliases in clauses
        "SELECT status AS s, COUNT(*) AS cnt FROM users GROUP BY s HAVING cnt > 1 ORDER BY cnt DESC",
        "SELECT total * 2 AS dbl FROM orders ORDER BY dbl",
        "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
        // LIMIT forms
        "SELECT * FROM users LIMIT 5, 10",
        "SELECT * FROM users LIMIT 10 OFFSET 5",
        // DML
        "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name)",
        "INSERT INTO users (id, name) VALUES (1, 'a') AS new ON DUPLICATE KEY UPDATE name = new.name",
        "INSERT INTO users (id, name) VALUES (1, 'a') AS new(i, n) ON DUPLICATE KEY UPDATE name = n",
        "REPLACE INTO users (id, name) VALUES (1, 'a')",
        "INSERT IGNORE INTO users (id, name) VALUES (1, 'a')",
        "INSERT INTO users SET id = 1, name = 'a'",
        "INSERT INTO users (id) SELECT id FROM orders",
        "UPDATE users u JOIN orders o ON o.user_id = u.id SET u.status = 'x' WHERE o.id = 1",
        "UPDATE users SET name = 'a' WHERE id = 1 ORDER BY id LIMIT 1",
        "DELETE u FROM users u JOIN orders o ON o.user_id = u.id WHERE o.id = 1",
        "DELETE u, o FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1",
        "DELETE FROM u USING users u JOIN orders o ON o.user_id = u.id WHERE o.id = 1",
        "DELETE FROM users WHERE id = 1 ORDER BY id LIMIT 1",
        // select modifiers
        "SELECT * FROM users WHERE id = 1 FOR UPDATE SKIP LOCKED",
        "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT",
        "SELECT * FROM users WHERE id = 1 FOR SHARE",
        "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE",
        "SELECT STRAIGHT_JOIN u.id FROM users u JOIN orders o ON o.user_id = u.id",
        "SELECT u.id FROM users u STRAIGHT_JOIN orders o ON o.user_id = u.id",
        "SELECT * FROM users USE INDEX (PRIMARY) WHERE id = 1",
        "SELECT * FROM users u FORCE INDEX (PRIMARY) WHERE u.id = 1",
        "SELECT * FROM users IGNORE INDEX (PRIMARY) WHERE id = 1",
        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ id FROM users",
        "SELECT /*+ INDEX(users PRIMARY) */ id FROM users",
        "SELECT SQL_CALC_FOUND_ROWS id FROM users LIMIT 5",
        "SELECT FOUND_ROWS()",
        "SELECT id, name INTO OUTFILE '/tmp/x.csv' FIELDS TERMINATED BY ',' FROM users",
        "SELECT id INTO @v FROM users WHERE id = 1",
        "SELECT DISTINCT status FROM users",
        "SELECT SQL_NO_CACHE id FROM users",
        // windows
        "SELECT id, ROW_NUMBER() OVER w AS rn FROM users WINDOW w AS (PARTITION BY status ORDER BY id)",
        "SELECT id, SUM(total) OVER (PARTITION BY user_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders",
        // JSON
        "SELECT jt.* FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS (v INT PATH '$')) AS jt",
        "SELECT payload->'$.a', payload->>'$.b' FROM users",
        "SELECT JSON_EXTRACT(payload, '$.a') FROM users",
        // variables / literals
        "SELECT @user_var, @@session.sql_mode, @@global.max_connections, @@version",
        "SELECT @x := 1",
        "SET @x := 1",
        "SET @x = (SELECT COUNT(*) FROM users)",
        "SELECT CONVERT(name USING utf8mb4) FROM users",
        "SELECT CAST(id AS UNSIGNED) FROM users",
        "SELECT CAST(name AS CHAR(10)) FROM users",
        "SELECT created_at + INTERVAL 1 DAY FROM orders",
        "SELECT DATE_ADD(created_at, INTERVAL 1 MONTH) FROM orders",
        "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL 7 DAY",
        "SELECT 'it\\'s', 'a''b'",
        "SELECT \"double quoted\" AS s",
        "SELECT * FROM users WHERE name = \"bob\"",
        "SELECT X'ABCD', x'41', b'101', 0x41, 0b101",
        "SELECT _utf8mb4'x', _binary'x', N'x'",
        "SELECT name COLLATE utf8mb4_bin FROM users",
        "SELECT * FROM users WHERE name = 'a' COLLATE utf8mb4_general_ci",
        "SELECT name FROM users WHERE name LIKE 'a%' ESCAPE '\\\\'",
        "SELECT name REGEXP '^a' FROM users",
        "SELECT id DIV 2, id MOD 2, id % 2 FROM users",
        "SELECT IFNULL(name, ''), GROUP_CONCAT(name ORDER BY name SEPARATOR ',') FROM users",
        "SELECT DATE_FORMAT(created_at, '%Y-%m') FROM orders",
        "SELECT * FROM users WHERE id <=> NULL",
        "SELECT 1 FROM DUAL",
        "SELECT NOW(), CURDATE(), LAST_INSERT_ID(), DATABASE(), VERSION()",
        "SELECT MATCH(name) AGAINST('x' IN BOOLEAN MODE) FROM users",
        // DDL
        "CREATE TABLE g (a INT, b INT, total INT GENERATED ALWAYS AS (a + b) STORED)",
        "CREATE TABLE g2 (a INT, b INT, total INT AS (a + b) VIRTUAL)",
        "CREATE TABLE n LIKE users",
        "CREATE TABLE n2 AS SELECT id, name FROM users",
        "CREATE TABLE IF NOT EXISTS n3 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TEMPORARY TABLE tmp (id INT)",
        "ALTER TABLE users ADD COLUMN age INT AFTER name",
        "ALTER TABLE users ADD COLUMN age2 INT FIRST",
        "ALTER TABLE users MODIFY COLUMN name VARCHAR(100) NOT NULL",
        "ALTER TABLE users DROP COLUMN age",
        "ALTER TABLE users ADD INDEX idx_name (name)",
        "RENAME TABLE users TO users_old",
        "RENAME TABLE users TO users_old, orders TO orders_old",
        "TRUNCATE TABLE users",
        "TRUNCATE users",
        "DROP TABLE IF EXISTS users, orders",
        "CREATE INDEX idx_a ON users (name)",
        "CREATE VIEW v1 AS SELECT id FROM users",
        "CREATE OR REPLACE VIEW v1 AS SELECT id FROM users",
        "CREATE DATABASE IF NOT EXISTS foo DEFAULT CHARACTER SET utf8mb4",
        // admin / utility
        "SHOW TABLES",
        "SHOW FULL TABLES FROM zqlz_w LIKE 'u%'",
        "SHOW DATABASES",
        "SHOW COLUMNS FROM users",
        "SHOW FULL COLUMNS FROM users FROM zqlz_w",
        "SHOW CREATE TABLE users",
        "SHOW CREATE VIEW v_active_users",
        "SHOW INDEX FROM users",
        "SHOW INDEXES FROM users",
        "SHOW KEYS FROM users",
        "SHOW TABLE STATUS LIKE 'users'",
        "SHOW VARIABLES LIKE 'sql_mode'",
        "SHOW GLOBAL VARIABLES",
        "SHOW STATUS",
        "SHOW SESSION STATUS LIKE 'Threads%'",
        "SHOW PROCESSLIST",
        "SHOW FULL PROCESSLIST",
        "SHOW ENGINES",
        "SHOW WARNINGS",
        "SHOW ERRORS",
        "SHOW GRANTS FOR 'root'@'localhost'",
        "SHOW MASTER STATUS",
        "SHOW SLAVE STATUS",
        "SHOW TRIGGERS",
        "SHOW PROCEDURE STATUS",
        "SHOW FUNCTION STATUS",
        "SHOW CREATE PROCEDURE p",
        "SHOW ENGINE INNODB STATUS",
        "SHOW CHARACTER SET",
        "SHOW COLLATION",
        "SHOW PLUGINS",
        "SHOW EVENTS",
        "DESCRIBE users",
        "DESC users",
        "DESCRIBE users name",
        "EXPLAIN SELECT * FROM users WHERE id = 1",
        "EXPLAIN FORMAT=JSON SELECT * FROM users",
        "EXPLAIN ANALYZE SELECT * FROM users",
        "EXPLAIN FORMAT=TREE SELECT * FROM users",
        "USE zqlz_w",
        "USE `zqlz_w`",
        "SET NAMES utf8mb4",
        "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'",
        "SET CHARACTER SET utf8mb4",
        "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
        "SET GLOBAL max_connections = 200",
        "SET @@session.sql_mode = ''",
        "SET sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')",
        "SET autocommit = 0",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET foreign_key_checks = 0",
        "START TRANSACTION",
        "START TRANSACTION READ ONLY",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT sp1",
        "ROLLBACK TO SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "LOCK TABLES users WRITE, orders READ",
        "LOCK TABLES users AS u WRITE",
        "UNLOCK TABLES",
        "FLUSH TABLES",
        "FLUSH PRIVILEGES",
        "FLUSH LOGS",
        "FLUSH TABLES WITH READ LOCK",
        "ANALYZE TABLE users",
        "ANALYZE TABLE users, orders",
        "OPTIMIZE TABLE users",
        "CHECK TABLE users",
        "REPAIR TABLE users",
        "CHECKSUM TABLE users",
        "GRANT SELECT, INSERT ON zqlz_w.* TO 'u'@'localhost'",
        "GRANT ALL PRIVILEGES ON *.* TO 'u'@'%' WITH GRANT OPTION",
        "REVOKE INSERT ON zqlz_w.* FROM 'u'@'localhost'",
        "CREATE USER 'u'@'localhost' IDENTIFIED BY 'pw'",
        "CREATE USER IF NOT EXISTS 'u'@'%' IDENTIFIED WITH mysql_native_password BY 'pw'",
        "ALTER USER 'u'@'localhost' IDENTIFIED BY 'pw2'",
        "DROP USER IF EXISTS 'u'@'localhost'",
        "CREATE ROLE r1",
        "CALL p(1, @o)",
        "CALL zqlz_w.p()",
        "PREPARE stmt FROM 'SELECT * FROM users WHERE id = ?'",
        "PREPARE stmt FROM @sql",
        "EXECUTE stmt USING @a, @b",
        "EXECUTE stmt",
        "DEALLOCATE PREPARE stmt",
        "DROP PREPARE stmt",
        "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES (id, name)",
        "LOAD DATA LOCAL INFILE '/tmp/x.csv' INTO TABLE users",
        "HANDLER users OPEN",
        "HANDLER users READ FIRST",
        "HANDLER users CLOSE",
        "DO SLEEP(1)",
        "KILL 10",
        "KILL QUERY 10",
        "XA START 'a'",
        "RESET MASTER",
        "PURGE BINARY LOGS TO 'x'",
        "INSTALL PLUGIN foo SONAME 'foo.so'",
        "TABLE users",
        "VALUES ROW(1, 2), ROW(3, 4)",
        "CHANGE MASTER TO MASTER_HOST='h'",
        "START SLAVE",
        "STOP SLAVE",
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'x'",
        "ALTER DATABASE zqlz_w CHARACTER SET utf8mb4",
        "ALTER TABLE users ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN z INT",
        "CREATE TABLE p (id INT) PARTITION BY HASH(id) PARTITIONS 4",
        "SELECT * FROM users PARTITION (p0)",
        // client directive + terminators
        "DELIMITER //\nSELECT 1 //\nDELIMITER ;",
        "DELIMITER $$\nCREATE PROCEDURE p()\nBEGIN\n  DECLARE x INT DEFAULT 0;\n  IF x = 0 THEN SET x = 1; END IF;\n  WHILE x < 3 DO SET x = x + 1; END WHILE;\n  SELECT x;\nEND$$\nDELIMITER ;",
        "CREATE PROCEDURE p()\nBEGIN\n  DECLARE x INT DEFAULT 0;\n  IF x = 0 THEN SET x = 1; END IF;\n  SELECT x;\nEND",
        "CREATE FUNCTION f(a INT) RETURNS INT DETERMINISTIC RETURN a + 1",
        "CREATE FUNCTION f2(a INT) RETURNS INT DETERMINISTIC BEGIN RETURN a + 1; END",
        "CREATE TRIGGER trg BEFORE INSERT ON users FOR EACH ROW SET NEW.created_at = NOW()",
        "CREATE TRIGGER trg2 AFTER UPDATE ON users FOR EACH ROW BEGIN INSERT INTO orders (id) VALUES (OLD.id); END",
        "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO DELETE FROM orders WHERE created_at < NOW() - INTERVAL 30 DAY",
        "CREATE EVENT ev2 ON SCHEDULE EVERY 1 HOUR DO BEGIN DELETE FROM orders WHERE id = 1; END",
        "DROP PROCEDURE IF EXISTS p",
        "DROP TRIGGER IF EXISTS trg",
        "DROP EVENT IF EXISTS ev",
        "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS n FROM users u",
        "SELECT @x, @@sql_mode FROM users",
        "SELECT u.id FROM users u WHERE u.id = ANY (SELECT o.user_id FROM orders o WHERE o.total > u.id)",
        "SELECT * FROM information_schema.tables t JOIN users u ON u.name = t.table_name WHERE t.table_rows > 0",
        "SELECT d.anything FROM other_db.users d",
        "INSERT INTO other_db.users (whatever) VALUES (1)",
        // multi statement
        "SELECT 1; SELECT * FROM users; SHOW TABLES; USE zqlz_w;",
    ];

    let mut failures = Vec::new();
    for sql in cases {
        let found = check(sql);
        if !found.is_empty() {
            failures.push(format!("{sql:?}\n    -> {found:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} false positives:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn invalid_mysql_statements_get_exactly_one_diagnostic() {
    let cases: &[(&str, &str)] = &[
        ("SELECT * FROM missing_table", "missing_table"),
        ("SELECT * FROM `missing_table`", "missing_table"),
        ("SELECT * FROM zqlz_w.missing_table", "missing_table"),
        ("SELECT * FROM `zqlz_w`.`missing_table`", "missing_table"),
        ("SELECT missing_col FROM users", "missing_col"),
        ("SELECT u.missing_col FROM users u", "missing_col"),
        ("SELECT `missing_col` FROM users", "missing_col"),
        ("SELECT id FROM users WHERE missing_col = 1", "missing_col"),
        (
            "UPDATE users SET name = 'a' WHERE missing_col = 1",
            "missing_col",
        ),
        (
            "INSERT INTO users (id, missing_col) VALUES (1, 2)",
            "missing_col",
        ),
        ("INSERT INTO missing_table (id) VALUES (1)", "missing_table"),
        ("DELETE FROM missing_table WHERE 1 = 1", "missing_table"),
        ("DELETE FROM users WHERE missing_col = 1", "missing_col"),
        (
            "DELETE u FROM users u JOIN missing_table m ON m.id = u.id WHERE u.id = 1",
            "missing_table",
        ),
        (
            "UPDATE users u JOIN missing_table m ON m.id = u.id SET u.name = m.name WHERE u.id = 1",
            "missing_table",
        ),
        (
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM missing_table m WHERE m.id = u.id)",
            "missing_table",
        ),
        (
            "SELECT u.missing_col FROM users u WHERE u.id IN (SELECT user_id FROM orders)",
            "missing_col",
        ),
        (
            "SELECT * FROM users u JOIN missing_table m ON m.id = u.id",
            "missing_table",
        ),
        ("SELECT * FROM users LIMIT 5, 10 missing_junk +", "Syntax"),
        ("SELECT * FORM users", "Syntax"),
    ];
    let mut failures = Vec::new();
    for (sql, needle) in cases {
        let diagnostics = mysql_lsp().validate_sql(&Rope::from(*sql));
        let found = problems(&diagnostics);
        if found.len() != 1 || !found[0].contains(needle) {
            failures.push(format!("{sql:?}\n    -> {found:?}"));
            continue;
        }
        // Schema diagnostics must underline the offending name itself.
        let diagnostic = diagnostics
            .iter()
            .find(|d| d.source.as_deref() == Some("schema"));
        if let Some(diagnostic) = diagnostic {
            let start = diagnostic.range.start.character as usize;
            let underlined = sql[start..].trim_start_matches('`');
            if !underlined.starts_with(needle) {
                failures.push(format!("{sql:?}\n    -> range starts at {underlined:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn mysql_syntax_error_is_reported_on_its_own_line_next_to_routines_and_delimiters() {
    let sql = "DELIMITER //\n\
               CREATE PROCEDURE p()\n\
               BEGIN\n\
                 DECLARE x INT DEFAULT 0;\n\
                 SELECT x;\n\
               END //\n\
               DELIMITER ;\n\
               SHOW FULL PROCESSLIST;\n\
               SELECT * FORM users;\n\
               CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN RETURN 1; END;";
    let diagnostics = mysql_lsp().validate_sql(&Rope::from(sql));
    let errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| d.severity == Some(DiagnosticSeverity::ERROR))
        .collect();
    assert_eq!(errors.len(), 1, "{diagnostics:?}");
    assert_eq!(errors[0].range.start.line, 8, "{diagnostics:?}");
    assert_eq!(errors[0].range.start.character, 9, "{diagnostics:?}");
}

fn completion_edits(sql: &str) -> Vec<(String, String, u32)> {
    let mut lsp = mysql_lsp();
    let text = Rope::from(sql);
    lsp.get_completions(&text, sql.len())
        .into_iter()
        .map(|item| match &item.text_edit {
            Some(lsp_types::CompletionTextEdit::Edit(edit)) => (
                item.label,
                edit.new_text.clone(),
                edit.range.start.character,
            ),
            // No edit: the client inserts at the cursor.
            _ => (
                item.label.clone(),
                item.insert_text.unwrap_or(item.label),
                u32::MAX,
            ),
        })
        .collect()
}

fn inserted<'a>(items: &'a [(String, String, u32)], label: &str) -> Option<&'a str> {
    items
        .iter()
        .find(|(item_label, ..)| item_label == label)
        .map(|(_, text, _)| text.as_str())
}

#[test]
fn mysql_table_completions_use_backticks_for_names_that_need_quoting() {
    let items = completion_edits("SELECT * FROM ");
    assert_eq!(inserted(&items, "users"), Some("users "), "{items:?}");
    assert_eq!(
        inserted(&items, "v_active_users"),
        Some("v_active_users "),
        "{items:?}"
    );
    assert_eq!(inserted(&items, "select"), Some("`select` "), "{items:?}");
    assert_eq!(
        inserted(&items, "Table With Spaces"),
        Some("`Table With Spaces` "),
        "{items:?}"
    );
    assert_eq!(
        inserted(&items, "1st_table"),
        Some("`1st_table` "),
        "{items:?}"
    );
    assert_eq!(inserted(&items, "we`ird"), Some("`we``ird` "), "{items:?}");
}

#[test]
fn mysql_database_qualifier_completes_its_tables() {
    for sql in ["SELECT * FROM zqlz_w.", "SELECT * FROM `zqlz_w`."] {
        let items = completion_edits(sql);
        assert_eq!(
            inserted(&items, "orders"),
            Some("orders "),
            "{sql}: {items:?}"
        );
        assert!(
            inserted(&items, "v_active_users").is_some(),
            "{sql}: {items:?}"
        );
        let (.., start) = items.iter().find(|(label, ..)| label == "orders").unwrap();
        assert!(
            *start == u32::MAX || *start as usize == sql.len(),
            "must not overwrite the qualifier: {items:?}"
        );
    }

    let sql = "SELECT * FROM zqlz_w.ord";
    let items = completion_edits(sql);
    let (_, text, start) = items
        .iter()
        .find(|(label, ..)| label == "orders")
        .unwrap_or_else(|| panic!("{items:?}"));
    assert_eq!(
        (text.as_str(), *start as usize),
        ("orders ", sql.len() - 3),
        "{items:?}"
    );
}

#[test]
fn mysql_alias_of_backticked_table_completes_columns() {
    let items = completion_edits("SELECT * FROM `select` s WHERE s.");
    assert_eq!(inserted(&items, "id"), Some("id"), "{items:?}");
    assert_eq!(inserted(&items, "from"), Some("`from`"), "{items:?}");

    let items = completion_edits("SELECT * FROM `Table With Spaces` AS t WHERE t.");
    assert_eq!(inserted(&items, "a b"), Some("`a b`"), "{items:?}");
}

#[test]
fn mysql_function_list_covers_common_functions() {
    let functions = SqlDialect::MySQL.functions();
    let missing: Vec<_> = [
        "IFNULL",
        "GROUP_CONCAT",
        "DATE_FORMAT",
        "STR_TO_DATE",
        "JSON_EXTRACT",
        "FIND_IN_SET",
        "LAST_INSERT_ID",
        "NOW",
        "CURDATE",
        "DATEDIFF",
        "TIMESTAMPDIFF",
        "REGEXP_REPLACE",
        "ROW_NUMBER",
        "COALESCE",
        "DATE_ADD",
        "SUBSTRING_INDEX",
        "CONCAT_WS",
        "JSON_OBJECT",
    ]
    .into_iter()
    .filter(|name| {
        !functions
            .iter()
            .any(|function| function.eq_ignore_ascii_case(name))
    })
    .collect();
    assert!(missing.is_empty(), "missing MySQL functions: {missing:?}");
}

#[test]
fn mysql_function_hover_and_signature_help() {
    let lsp = mysql_lsp();
    for name in ["IFNULL", "GROUP_CONCAT", "DATE_FORMAT", "TIMESTAMPDIFF"] {
        let sql = format!("SELECT {name}(created_at, 1) FROM orders");
        let text = Rope::from(sql.as_str());
        assert!(lsp.get_hover(&text, 9).is_some(), "no hover for {name}");
        let inside = sql.find('(').unwrap() + 1;
        assert!(
            lsp.get_signature_help(&text, inside).is_some(),
            "no signature help for {name}"
        );
    }
}
