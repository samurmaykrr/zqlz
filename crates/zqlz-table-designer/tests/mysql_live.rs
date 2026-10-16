//! Live MySQL checks for the table designer: loaded flags, CREATE TABLE
//! round-trips against `SHOW CREATE TABLE`, and ALTER statements applied to a
//! real server and re-read. Opt in with `ZQLZ_MYSQL_INTEGRATION=1`
//! (`ZQLZ_MYSQL_HOST/PORT/USER/PASSWORD` override the 127.0.0.1:3307 root/root
//! defaults).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, Value};
use zqlz_drivers::mysql::MySqlDriver;
use zqlz_table_designer::{
    CheckConstraintDesign, ColumnDesign, DatabaseDialect, DdlGenerator, ForeignKeyDesign,
    IndexDesign, TableDesign, TableLoader,
};

fn config(database: &str) -> ConnectionConfig {
    let var = |name: &str| std::env::var(format!("ZQLZ_MYSQL_{name}")).ok();
    let host = var("HOST").unwrap_or_else(|| "127.0.0.1".into());
    let port = var("PORT").and_then(|p| p.parse().ok()).unwrap_or(3307);
    let user = var("USER").unwrap_or_else(|| "root".into());
    let mut config = ConnectionConfig::new_mysql(&host, port, database, &user);
    config.password = Some(var("PASSWORD").unwrap_or_else(|| "root".into()));
    config
}

/// A fresh scratch database, or `None` when the integration env is not set.
async fn scratch(database: &str) -> Option<Arc<dyn Connection>> {
    if std::env::var("ZQLZ_MYSQL_INTEGRATION").ok().as_deref() != Some("1") {
        eprintln!("skipping live MySQL designer test; set ZQLZ_MYSQL_INTEGRATION=1");
        return None;
    }
    let admin = MySqlDriver::new().connect(&config("mysql")).await.expect("connect");
    admin.execute(&format!("DROP DATABASE IF EXISTS `{database}`"), &[]).await.unwrap();
    admin.execute(&format!("CREATE DATABASE `{database}`"), &[]).await.unwrap();
    Some(MySqlDriver::new().connect(&config(database)).await.expect("connect"))
}

async fn drop_database(connection: &Arc<dyn Connection>, database: &str) {
    connection
        .execute(&format!("DROP DATABASE IF EXISTS `{database}`"), &[])
        .await
        .unwrap();
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("{error}\n  while running: {sql}"));
}

async fn load(connection: &Arc<dyn Connection>, database: &str, table: &str) -> TableDesign {
    TableLoader::load_table(connection.clone(), Some(database), table, DatabaseDialect::Mysql)
        .await
        .unwrap_or_else(|error| panic!("load {table}: {error}"))
}

async fn show_create(connection: &Arc<dyn Connection>, table: &str) -> String {
    let result = connection
        .query(&format!("SHOW CREATE TABLE `{table}`"), &[])
        .await
        .unwrap();
    match result.rows[0].get(1).unwrap() {
        Value::String(text) => text.clone(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("unexpected SHOW CREATE value {other:?}"),
    }
}

/// Apply the designer's ALTER statements; returns them for assertions.
async fn apply(
    connection: &Arc<dyn Connection>,
    original: &TableDesign,
    modified: &TableDesign,
) -> Vec<String> {
    let statements = DdlGenerator::generate_alter_table(original, modified).unwrap();
    for statement in &statements {
        exec(connection, statement).await;
    }
    statements
}

fn column<'a>(design: &'a TableDesign, name: &str) -> &'a ColumnDesign {
    design.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"))
}

const FIXTURE: &[&str] = &[
    "CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a,b)) COMMENT='p'",
    "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT, CONSTRAINT fk_child FOREIGN KEY (a,b) REFERENCES parent(a,b) ON DELETE CASCADE ON UPDATE SET NULL)",
    "CREATE TABLE gen (id INT PRIMARY KEY, a INT, b INT, s INT GENERATED ALWAYS AS (a+b) STORED, v INT AS (a*2) VIRTUAL, CONSTRAINT chk_a CHECK (a > -1000), j JSON, INDEX fx ((a+b)), INDEX pfx (a), UNIQUE KEY uq_ab (a, b), invis INT INVISIBLE)",
    "CREATE TABLE ft (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ftx (body))",
    "CREATE TABLE idx (id INT PRIMARY KEY, name VARCHAR(100), age INT, KEY name_pfx (name(10)), KEY age_desc (age DESC), KEY comp (name, age), UNIQUE KEY uq_name (name), email VARCHAR(50), UNIQUE INDEX email_u (email))",
    "CREATE TABLE items (item_id CHAR(36) NOT NULL, order_id CHAR(36) NOT NULL, line_number INT NOT NULL, \
       line_total DECIMAL(18,4) GENERATED ALWAYS AS (line_number * 2) STORED, PRIMARY KEY (item_id), \
       UNIQUE KEY uq_order_line (order_id, line_number), KEY idx_order (order_id))",
    "CREATE TABLE defs (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'pk comment', \
       a VARCHAR(10) NOT NULL DEFAULT 'it''s', \
       b TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
       c DECIMAL(10,2) UNSIGNED DEFAULT 1.50, e ENUM('x','y') DEFAULT 'x', s SET('p','q'), z INT ZEROFILL, \
       t TEXT CHARACTER SET latin1 COLLATE latin1_bin, u CHAR(36) DEFAULT (UUID()), flag TINYINT(1) DEFAULT 0, \
       dt DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6), txt TINYTEXT, bl BLOB, vb VARBINARY(16), f FLOAT(7,3), \
       bt BIT(8) DEFAULT b'101', yr YEAR, d DATE DEFAULT '2020-01-02', neg INT DEFAULT -5, \
       bk VARCHAR(5) DEFAULT 'a\\\\b', j JSON DEFAULT (JSON_ARRAY())) COMMENT='table comment' ENGINE=InnoDB",
    "CREATE TABLE `select` (`from` INT PRIMARY KEY, `order` VARCHAR(10), `Mixed Case` INT, `emoji😀` INT, `back``tick` INT, `sp ace` INT, KEY `my key` (`sp ace`, `back``tick`))",
    "CREATE TABLE `Table With Spaces` (id INT PRIMARY KEY, `col with space` TEXT)",
];

async fn create_fixture(connection: &Arc<dyn Connection>) {
    for statement in FIXTURE {
        exec(connection, statement).await;
    }
}

const TABLES: &[&str] = &[
    "parent", "child", "gen", "ft", "idx", "items", "defs", "select", "Table With Spaces",
];

#[tokio::test]
async fn loaded_flags_match_the_server() {
    let database = "zqlz_e_design_flags";
    let Some(connection) = scratch(database).await else { return };
    create_fixture(&connection).await;

    // (table, column) -> (pk, uq, ai)
    let cases = [
        ("items", "item_id", (true, false, false)),
        ("items", "order_id", (false, false, false)),
        ("items", "line_number", (false, false, false)),
        ("idx", "id", (true, false, false)),
        ("idx", "name", (false, true, false)),
        ("idx", "email", (false, true, false)),
        ("idx", "age", (false, false, false)),
        ("gen", "a", (false, false, false)),
        ("gen", "b", (false, false, false)),
        ("parent", "a", (true, false, false)),
        ("parent", "b", (true, false, false)),
        ("defs", "id", (true, false, true)),
        ("defs", "a", (false, false, false)),
    ];
    for (table, name, (pk, uq, ai)) in cases {
        let design = load(&connection, database, table).await;
        let c = column(&design, name);
        assert_eq!((c.is_primary_key, c.is_unique, c.is_auto_increment), (pk, uq, ai), "{table}.{name}");
    }
    let idx = load(&connection, database, "idx").await;
    assert_eq!(column(&idx, "name").unique_constraint_name.as_deref(), Some("uq_name"));
    assert_eq!(column(&idx, "email").unique_constraint_name.as_deref(), Some("email_u"));
    let cols = |name: &str| idx.indexes.iter().find(|i| i.name == name).map(|i| i.columns.clone());
    assert_eq!(cols("name_pfx"), Some(vec!["name(10)".to_string()]));
    assert_eq!(cols("comp"), Some(vec!["name".to_string(), "age".to_string()]));
    assert_eq!(
        idx.indexes.iter().find(|i| i.name == "age_desc").map(|i| i.column_descending.clone()),
        Some(vec![true])
    );

    let gen_t = load(&connection, database, "gen").await;
    assert_eq!(column(&gen_t, "s").generated_expression.as_deref(), Some("(`a` + `b`)"));
    assert!(column(&gen_t, "s").generated_stored);
    assert!(column(&gen_t, "v").generated_expression.is_some() && !column(&gen_t, "v").generated_stored);
    assert_eq!(column(&gen_t, "a").generated_expression, None);
    assert!(column(&gen_t, "invis").invisible);
    assert_eq!(gen_t.indexes.iter().find(|i| i.name == "fx").unwrap().columns, ["(`a` + `b`)"]);
    assert_eq!(gen_t.check_constraints.len(), 1);
    assert_eq!(gen_t.check_constraints[0].name.as_deref(), Some("chk_a"));

    let ft = load(&connection, database, "ft").await;
    assert_eq!(ft.indexes.iter().find(|i| i.name == "ftx").unwrap().index_type, "FULLTEXT");

    let defs = load(&connection, database, "defs").await;
    let d = |name: &str| column(&defs, name);
    // (data_type, length, scale)
    let shape = |c: &ColumnDesign| (c.data_type.to_lowercase(), c.length, c.scale);
    assert_eq!(shape(d("id")), ("bigint unsigned".into(), None, None));
    assert_eq!(shape(d("a")), ("varchar".into(), Some(10), None));
    assert_eq!(shape(d("c")), ("decimal unsigned".into(), Some(10), Some(2)));
    assert_eq!(shape(d("flag")), ("tinyint".into(), Some(1), None));
    assert_eq!(shape(d("dt")), ("datetime".into(), Some(6), None));
    assert_eq!(shape(d("txt")), ("tinytext".into(), None, None));
    assert_eq!(shape(d("bl")), ("blob".into(), None, None));
    assert_eq!(shape(d("e")), ("enum('x','y')".into(), None, None));
    assert_eq!(shape(d("s")), ("set('p','q')".into(), None, None));
    assert_eq!(shape(d("f")), ("float".into(), Some(7), Some(3)));
    assert_eq!(shape(d("z")), ("int unsigned zerofill".into(), Some(10), None));
    assert_eq!(d("a").default_value.as_deref(), Some("'it''s'"));
    assert_eq!(d("b").default_value.as_deref(), Some("CURRENT_TIMESTAMP"));
    assert_eq!(d("b").on_update.as_deref(), Some("CURRENT_TIMESTAMP"));
    assert_eq!(d("c").default_value.as_deref(), Some("1.50"));
    assert_eq!(d("e").default_value.as_deref(), Some("'x'"));
    assert_eq!(d("u").default_value.as_deref(), Some("(uuid())"));
    assert_eq!(d("dt").default_value.as_deref(), Some("CURRENT_TIMESTAMP(6)"));
    assert_eq!(d("bt").default_value.as_deref(), Some("b'101'"));
    assert_eq!(d("d").default_value.as_deref(), Some("'2020-01-02'"));
    assert_eq!(d("neg").default_value.as_deref(), Some("-5"));
    assert_eq!(d("id").default_value, None);
    assert_eq!(d("id").comment.as_deref(), Some("pk comment"));
    assert_eq!(d("t").charset.as_deref(), Some("latin1"));
    assert_eq!(d("t").collation.as_deref(), Some("latin1_bin"));
    assert_eq!(defs.comment.as_deref(), Some("table comment"));
    assert_eq!(defs.options.engine.as_deref(), Some("InnoDB"));
    assert_eq!(defs.options.collation.as_deref(), Some("utf8mb4_0900_ai_ci"));
    assert_eq!(defs.options.charset.as_deref(), Some("utf8mb4"));

    for table in TABLES {
        let design = load(&connection, database, table).await;
        assert!(
            DdlGenerator::generate_alter_table(&design, &design.clone()).unwrap().is_empty(),
            "{table}: unchanged design must produce no statements"
        );
        assert!(design.validate().is_empty(), "{table}: {:?}", design.validate());
    }
    drop_database(&connection, database).await;
}

#[tokio::test]
async fn create_table_matches_show_create_table() {
    let (source_db, copy_db) = ("zqlz_e_design_src", "zqlz_e_design_copy");
    let Some(source) = scratch(source_db).await else { return };
    let copy = scratch(copy_db).await.unwrap();
    create_fixture(&source).await;

    for table in TABLES {
        let design = load(&source, source_db, table).await;
        let mut design = design;
        // Same table in the copy database; same-database FKs follow it there.
        design.schema = Some(copy_db.to_string());
        for foreign_key in &mut design.foreign_keys {
            if foreign_key.referenced_schema.as_deref() == Some(source_db) {
                foreign_key.referenced_schema = Some(copy_db.to_string());
            }
        }
        let sql = DdlGenerator::generate_create_table(&design).unwrap();
        assert_eq!(sql.matches(';').count(), 1, "{table}: one statement expected\n{sql}");
        exec(&copy, &sql).await;
        let (expected, actual) = (show_create(&source, table).await, show_create(&copy, table).await);
        // Index order follows creation order on the server, which the design doesn't keep.
        assert_eq!(sorted_lines(&actual), sorted_lines(&expected), "{table}\n--- generated:\n{sql}");
    }
    drop_database(&source, source_db).await;
    drop_database(&copy, copy_db).await;
}

#[tokio::test]
async fn new_table_from_designer_round_trips() {
    let database = "zqlz_e_design_new";
    let Some(connection) = scratch(database).await else { return };
    exec(&connection, "CREATE TABLE ref_t (id INT PRIMARY KEY)").await;

    let mut design = TableDesign::new("odd `name` with 'quotes'", DatabaseDialect::Mysql);
    let mut id = ColumnDesign::named("id").data_type("BIGINT UNSIGNED").primary_key().auto_increment();
    id.comment = Some("it's \\ fine".into());
    design = design.with_column(id);
    let mut email = ColumnDesign::named("e-mail").data_type("VARCHAR").not_null().unique();
    email.length = Some(190);
    email.default_value = Some("nobody@example.com".into());
    design = design.with_column(email);
    let mut price = ColumnDesign::named("price").data_type("DECIMAL");
    price.length = Some(10);
    price.scale = Some(2);
    price.default_value = Some("0".into());
    design = design.with_column(price);
    let mut stamp = ColumnDesign::named("stamp").data_type("TIMESTAMP");
    stamp.default_value = Some("CURRENT_TIMESTAMP".into());
    stamp.on_update = Some("CURRENT_TIMESTAMP".into());
    design = design.with_column(stamp);
    let mut uid = ColumnDesign::named("uid").data_type("CHAR");
    uid.length = Some(36);
    uid.default_value = Some("uuid()".into());
    design = design.with_column(uid);
    design = design.with_column(ColumnDesign::named("ref").data_type("INT"));
    let mut doubled = ColumnDesign::named("doubled").data_type("INT");
    doubled.generated_expression = Some("`ref` * 2".into());
    // VIRTUAL: a CASCADE FK may not touch the base column of a STORED one.
    doubled.generated_stored = false;
    design = design.with_column(doubled);
    design.indexes.push(IndexDesign::named("ix_email_prefix").column("e-mail(20)").column("ref"));
    let mut fulltext = IndexDesign::named("ft_email").column("e-mail");
    fulltext.index_type = "FULLTEXT".into();
    design.indexes.push(fulltext);
    design.foreign_keys.push(
        ForeignKeyDesign::new()
            .named("fk_ref")
            .column("ref")
            .references("ref_t")
            .referenced_column("id")
            .on_delete(zqlz_core::ForeignKeyAction::Cascade),
    );
    design.check_constraints.push(CheckConstraintDesign::new().named("chk_price").expression("`price` >= 0"));
    design.comment = Some("designer's table".into());
    design.options = zqlz_table_designer::TableOptions::mysql();

    let sql = DdlGenerator::generate_create_table(&design).unwrap();
    exec(&connection, &sql).await;

    let loaded = load(&connection, database, "odd `name` with 'quotes'").await;
    assert_eq!(loaded.columns.len(), design.columns.len());
    assert!(column(&loaded, "id").is_primary_key && column(&loaded, "id").is_auto_increment);
    assert!(column(&loaded, "e-mail").is_unique && !column(&loaded, "e-mail").nullable);
    assert_eq!(column(&loaded, "id").comment.as_deref(), Some("it's \\ fine"));
    assert_eq!(column(&loaded, "e-mail").default_value.as_deref(), Some("'nobody@example.com'"));
    assert_eq!(column(&loaded, "uid").default_value.as_deref(), Some("(uuid())"));
    assert_eq!(column(&loaded, "stamp").on_update.as_deref(), Some("CURRENT_TIMESTAMP"));
    assert_eq!(loaded.comment.as_deref(), Some("designer's table"));
    assert_eq!(loaded.foreign_keys.len(), 1);
    assert_eq!(loaded.check_constraints.len(), 1);

    // Re-generating from what was read back is a fixed point.
    let again = DdlGenerator::generate_create_table(&loaded).unwrap();
    exec(&connection, "DROP TABLE `odd ``name`` with 'quotes'`").await;
    let mut renamed = loaded.clone();
    renamed.foreign_keys[0].name = Some("fk_ref".into());
    exec(&connection, &again).await;
    let reloaded = load(&connection, database, "odd `name` with 'quotes'").await;
    assert!(DdlGenerator::generate_alter_table(&loaded, &reloaded).unwrap().is_empty());
    drop_database(&connection, database).await;
}

#[tokio::test]
async fn alter_statements_apply_and_read_back_without_spurious_diff() {
    let database = "zqlz_e_design_alter";
    let Some(connection) = scratch(database).await else { return };
    create_fixture(&connection).await;
    exec(&connection, "CREATE TABLE aitem (id INT AUTO_INCREMENT PRIMARY KEY, a INT, b VARCHAR(20), c INT)").await;

    // --- columns: add mid-table, rename, retype, drop, comment, default, move ---
    let original = load(&connection, database, "aitem").await;
    let mut modified = original.clone();
    modified.columns[1].name = "a2".into();
    modified.columns[2].length = Some(40);
    modified.columns[2].comment = Some("it's".into());
    modified.columns[2].default_value = Some("hello".into());
    modified.columns.remove(3);
    let mut added = ColumnDesign::named("added col").data_type("DATETIME");
    added.length = Some(3);
    added.default_value = Some("CURRENT_TIMESTAMP(3)".into());
    modified.columns.insert(1, added);
    let mut stored = ColumnDesign::named("twice").data_type("INT");
    stored.generated_expression = Some("a2 * 2".into());
    modified.columns.push(stored);
    let statements = apply(&connection, &original, &modified).await;
    let reloaded = load(&connection, database, "aitem").await;
    let names: Vec<&str> = reloaded.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "added col", "a2", "b", "twice"], "{statements:?}");
    assert_eq!(column(&reloaded, "b").length, Some(40));
    assert_eq!(column(&reloaded, "b").default_value.as_deref(), Some("'hello'"));
    assert_eq!(column(&reloaded, "b").comment.as_deref(), Some("it's"));
    assert!(column(&reloaded, "id").is_auto_increment);
    assert!(DdlGenerator::generate_alter_table(&reloaded, &reloaded.clone()).unwrap().is_empty());
    assert!(
        DdlGenerator::generate_alter_table(&reloaded, &modified).unwrap().iter().all(|s| s.contains("twice")),
        "{:?}",
        DdlGenerator::generate_alter_table(&reloaded, &modified)
    );

    // --- default-only edits, ON UPDATE kept across an unrelated MODIFY ---
    let before = load(&connection, database, "defs").await;
    let mut after = before.clone();
    after.columns.iter_mut().find(|c| c.name == "neg").unwrap().default_value = Some("7".into());
    after.columns.iter_mut().find(|c| c.name == "yr").unwrap().default_value = Some("2024".into());
    after.columns.iter_mut().find(|c| c.name == "a").unwrap().default_value = None;
    after.columns.iter_mut().find(|c| c.name == "b").unwrap().comment = Some("touch".into());
    let statements = apply(&connection, &before, &after).await;
    assert_eq!(statements.len(), 4, "{statements:?}");
    let now = load(&connection, database, "defs").await;
    assert_eq!(column(&now, "neg").default_value.as_deref(), Some("7"));
    assert_eq!(column(&now, "a").default_value, None);
    assert_eq!(column(&now, "b").on_update.as_deref(), Some("CURRENT_TIMESTAMP"));
    assert_eq!(column(&now, "b").default_value.as_deref(), Some("CURRENT_TIMESTAMP"));
    assert_eq!(column(&now, "t").collation.as_deref(), Some("latin1_bin"));
    assert!(DdlGenerator::generate_alter_table(&now, &after).unwrap().is_empty());

    // --- UQ toggles on idx ---
    let before = load(&connection, database, "idx").await;
    let mut after = before.clone();
    after.columns.iter_mut().find(|c| c.name == "name").unwrap().is_unique = false;
    after.columns.iter_mut().find(|c| c.name == "age").unwrap().is_unique = true;
    let statements = apply(&connection, &before, &after).await;
    assert_eq!(statements.len(), 2, "{statements:?}");
    let now = load(&connection, database, "idx").await;
    let flags: Vec<(&str, bool)> = now.columns.iter().map(|c| (c.name.as_str(), c.is_unique)).collect();
    assert_eq!(flags, [("id", false), ("name", false), ("age", true), ("email", true)]);

    // --- indexes: change, drop, add prefix/desc/fulltext/functional ---
    let before = load(&connection, database, "idx").await;
    let mut after = before.clone();
    after.indexes.retain(|i| i.name != "comp");
    after.indexes.iter_mut().find(|i| i.name == "name_pfx").unwrap().columns = vec!["name(20)".into()];
    let mut desc = IndexDesign::named("ix_new").column("age").column("name(5)");
    desc.column_descending = vec![true, false];
    after.indexes.push(desc);
    let mut fulltext = IndexDesign::named("ft_name").column("name");
    fulltext.index_type = "FULLTEXT".into();
    after.indexes.push(fulltext);
    after.indexes.push(IndexDesign::named("fx_age").column("(`age` + 1)").unique());
    apply(&connection, &before, &after).await;
    let now = load(&connection, database, "idx").await;
    assert!(DdlGenerator::generate_alter_table(&now, &after).unwrap().is_empty(), "{:?}", DdlGenerator::generate_alter_table(&now, &after));

    // --- FK / CHECK add, change, drop ---
    let before = load(&connection, database, "child").await;
    let mut after = before.clone();
    after.foreign_keys[0].on_delete = zqlz_core::ForeignKeyAction::Restrict;
    after.check_constraints.push(CheckConstraintDesign::new().named("c_pos").expression("id > 0"));
    apply(&connection, &before, &after).await;
    let now = load(&connection, database, "child").await;
    assert_eq!(now.foreign_keys[0].on_delete, zqlz_core::ForeignKeyAction::Restrict);
    assert_eq!(now.check_constraints.len(), 1);
    let mut gone = now.clone();
    gone.foreign_keys.clear();
    gone.check_constraints.clear();
    apply(&connection, &now, &gone).await;
    let now = load(&connection, database, "child").await;
    assert!(now.foreign_keys.is_empty() && now.check_constraints.is_empty());

    // --- primary key: composite change on an AUTO_INCREMENT key, then drop ---
    let before = load(&connection, database, "aitem").await;
    let mut after = before.clone();
    column_mut(&mut after, "a2").is_primary_key = true;
    column_mut(&mut after, "a2").nullable = false;
    apply(&connection, &before, &after).await;
    let now = load(&connection, database, "aitem").await;
    assert!(column(&now, "id").is_primary_key && column(&now, "a2").is_primary_key && column(&now, "id").is_auto_increment);
    let mut dropped = now.clone();
    column_mut(&mut dropped, "id").is_primary_key = false;
    column_mut(&mut dropped, "id").is_auto_increment = false;
    column_mut(&mut dropped, "a2").is_primary_key = false;
    apply(&connection, &now, &dropped).await;
    let now2 = load(&connection, database, "aitem").await;
    assert!(now2.columns.iter().all(|c| !c.is_primary_key && !c.is_auto_increment));

    // --- table-level: comment, engine, rename ---
    let before = load(&connection, database, "ft").await;
    let mut after = before.clone();
    after.comment = Some("new 'comment'".into());
    after.table_name = "ft_renamed".into();
    apply(&connection, &before, &after).await;
    let now = load(&connection, database, "ft_renamed").await;
    assert_eq!(now.comment.as_deref(), Some("new 'comment'"));

    // --- reserved words, spaces and backticks as names ---
    let before = load(&connection, database, "select").await;
    let mut after = before.clone();
    column_mut(&mut after, "back`tick").name = "back`tick2".into();
    column_mut(&mut after, "order").data_type = "TEXT".into();
    column_mut(&mut after, "order").length = None;
    after.columns.push(ColumnDesign::named("group").data_type("INT"));
    after.indexes.retain(|i| i.name != "my key");
    apply(&connection, &before, &after).await;
    let now = load(&connection, database, "select").await;
    assert!(now.columns.iter().any(|c| c.name == "back`tick2") && now.columns.iter().any(|c| c.name == "group"));
    drop_database(&connection, database).await;
}

fn column_mut<'a>(design: &'a mut TableDesign, name: &str) -> &'a mut ColumnDesign {
    design.columns.iter_mut().find(|c| c.name == name).unwrap()
}

fn sorted_lines(ddl: &str) -> Vec<String> {
    let mut lines: Vec<String> = ddl.lines().map(|line| line.trim_end_matches(',').to_string()).collect();
    lines.sort();
    lines
}
