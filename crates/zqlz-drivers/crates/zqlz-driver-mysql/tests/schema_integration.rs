use std::sync::Arc;

use uuid::Uuid;
use zqlz_core::{
    Connection, ConnectionConfig, DatabaseDriver, DatabaseObject, ForeignKeyAction, ObjectType,
};
use zqlz_driver_mysql::MySqlDriver;

fn integration_enabled(prefix: &str) -> bool {
    std::env::var(format!("{prefix}_INTEGRATION"))
        .ok()
        .as_deref()
        == Some("1")
}

fn test_config(prefix: &str, default_port: u16) -> ConnectionConfig {
    let host = std::env::var(format!("{prefix}_HOST")).unwrap_or_else(|_| "127.0.0.1".to_string());
    let database = std::env::var(format!("{prefix}_DATABASE")).unwrap_or_else(|_| "test".into());
    let username = std::env::var(format!("{prefix}_USER")).unwrap_or_else(|_| "root".into());
    let mut config = ConnectionConfig::new_mysql(&host, default_port, &database, &username);

    if let Ok(port) = std::env::var(format!("{prefix}_PORT"))
        && let Ok(port) = port.parse::<u16>()
    {
        config.port = port;
    }
    if let Ok(password) = std::env::var(format!("{prefix}_PASSWORD"))
        && !password.is_empty()
    {
        config.password = Some(password);
    }
    config
}

async fn connect(prefix: &str, default_port: u16) -> Option<Arc<dyn Connection>> {
    if !integration_enabled(prefix) {
        eprintln!("skipping MySQL integration test; set {prefix}_INTEGRATION=1");
        return None;
    }

    Some(
        MySqlDriver::new()
            .connect(&test_config(prefix, default_port))
            .await
            .expect("connect to MySQL/MariaDB"),
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

async fn execute(connection: &Arc<dyn Connection>, sql: String) {
    connection.execute(&sql, &[]).await.expect("execute SQL");
}

async fn try_execute(connection: &Arc<dyn Connection>, sql: String) -> bool {
    match connection.execute(&sql, &[]).await {
        Ok(_) => true,
        Err(error) => {
            eprintln!("optional MySQL integration SQL failed: {error}");
            false
        }
    }
}

async fn cleanup_showcase_fixture(connection: &Arc<dyn Connection>, prefix: &str) {
    let names = ShowcaseNames::new(prefix);
    let cleanup_sql = [
        format!("DROP EVENT IF EXISTS {}", quote_identifier(&names.event)),
        format!(
            "DROP PROCEDURE IF EXISTS {}",
            quote_identifier(&names.procedure)
        ),
        format!(
            "DROP FUNCTION IF EXISTS {}",
            quote_identifier(&names.function)
        ),
        format!(
            "DROP TRIGGER IF EXISTS {}",
            quote_identifier(&names.trigger)
        ),
        format!("DROP VIEW IF EXISTS {}", quote_identifier(&names.view)),
        format!(
            "DROP TABLE IF EXISTS {}",
            quote_identifier(&names.child_table)
        ),
        format!(
            "DROP TABLE IF EXISTS {}",
            quote_identifier(&names.parent_table)
        ),
    ];

    for sql in cleanup_sql {
        if let Err(error) = connection.execute(&sql, &[]).await {
            eprintln!("cleanup SQL failed: {error}");
        }
    }
}

struct ShowcaseNames {
    parent_table: String,
    child_table: String,
    view: String,
    function: String,
    procedure: String,
    trigger: String,
    event: String,
    fulltext_index: String,
    spatial_index: String,
    status_index: String,
    foreign_key: String,
    check_constraint: String,
}

impl ShowcaseNames {
    fn new(prefix: &str) -> Self {
        Self {
            parent_table: format!("{prefix}_parent"),
            child_table: format!("{prefix}_child"),
            view: format!("{prefix}_view"),
            function: format!("{prefix}_fn"),
            procedure: format!("{prefix}_proc"),
            trigger: format!("{prefix}_trg"),
            event: format!("{prefix}_ev"),
            fulltext_index: format!("{prefix}_ft"),
            spatial_index: format!("{prefix}_sp"),
            status_index: format!("{prefix}_status"),
            foreign_key: format!("{prefix}_fk"),
            check_constraint: format!("{prefix}_chk"),
        }
    }
}

async fn create_showcase_fixture(connection: &Arc<dyn Connection>, names: &ShowcaseNames) -> bool {
    execute(
        connection,
        format!(
            "CREATE TABLE {} (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                code VARCHAR(64) NOT NULL UNIQUE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            quote_identifier(&names.parent_table)
        ),
    )
    .await;

    execute(
        connection,
        format!(
            "CREATE TABLE {} (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                parent_id BIGINT NOT NULL,
                quantity BIGINT NOT NULL DEFAULT 0,
                status ENUM('pending','paid') NOT NULL,
                flags SET('read','write') NULL,
                name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                display_name VARCHAR(140) GENERATED ALWAYS AS (CONCAT(name, '-display')) VIRTUAL,
                parent_score BIGINT GENERATED ALWAYS AS (quantity * 10) STORED,
                geom POINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FULLTEXT KEY {} (name),
                SPATIAL KEY {} (geom),
                KEY {} (status DESC),
                CONSTRAINT {} FOREIGN KEY (parent_id) REFERENCES {} (id)
                    ON DELETE CASCADE ON UPDATE RESTRICT,
                CONSTRAINT {} CHECK (quantity >= 0)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
              COMMENT='zqlz mysql integration'",
            quote_identifier(&names.child_table),
            quote_identifier(&names.fulltext_index),
            quote_identifier(&names.spatial_index),
            quote_identifier(&names.status_index),
            quote_identifier(&names.foreign_key),
            quote_identifier(&names.parent_table),
            quote_identifier(&names.check_constraint)
        ),
    )
    .await;

    execute(
        connection,
        format!(
            "CREATE VIEW {} AS SELECT id, status, name FROM {}",
            quote_identifier(&names.view),
            quote_identifier(&names.child_table)
        ),
    )
    .await;

    execute(
        connection,
        format!(
            "CREATE FUNCTION {}(input_value INT) RETURNS INT DETERMINISTIC
             READS SQL DATA RETURN input_value + 1",
            quote_identifier(&names.function)
        ),
    )
    .await;

    execute(
        connection,
        format!(
            "CREATE PROCEDURE {}(IN input_id BIGINT) SELECT input_id AS selected_id",
            quote_identifier(&names.procedure)
        ),
    )
    .await;

    execute(
        connection,
        format!(
            "CREATE TRIGGER {} BEFORE INSERT ON {}
             FOR EACH ROW SET NEW.name = COALESCE(NEW.name, 'unnamed')",
            quote_identifier(&names.trigger),
            quote_identifier(&names.child_table)
        ),
    )
    .await;

    try_execute(
        connection,
        format!(
            "CREATE EVENT {} ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY
             DO INSERT INTO {} (code) VALUES ('event')",
            quote_identifier(&names.event),
            quote_identifier(&names.parent_table)
        ),
    )
    .await
}

#[tokio::test]
async fn live_mysql_showcase_metadata_roundtrip() {
    let Some(connection) = connect("ZQLZ_MYSQL", 3306).await else {
        return;
    };
    let schema = connection
        .as_schema_introspection()
        .expect("MySQL exposes schema introspection");
    let prefix = format!("zqlz_it_{}", Uuid::new_v4().simple());
    let names = ShowcaseNames::new(&prefix);

    cleanup_showcase_fixture(&connection, &prefix).await;
    let event_created = create_showcase_fixture(&connection, &names).await;

    let columns = schema
        .get_columns(None, &names.child_table)
        .await
        .expect("columns");
    let status = columns
        .iter()
        .find(|column| column.name == "status")
        .expect("status column");
    assert_eq!(
        status.enum_values.as_deref(),
        Some(&["pending".to_string(), "paid".to_string()][..])
    );
    let flags = columns
        .iter()
        .find(|column| column.name == "flags")
        .expect("flags column");
    assert_eq!(
        flags.enum_values.as_deref(),
        Some(&["read".to_string(), "write".to_string()][..])
    );
    let name_column = columns
        .iter()
        .find(|column| column.name == "name")
        .expect("name column");
    assert_eq!(name_column.charset.as_deref(), Some("utf8mb4"));
    assert_eq!(name_column.collation.as_deref(), Some("utf8mb4_unicode_ci"));
    let virtual_column = columns
        .iter()
        .find(|column| column.name == "display_name")
        .expect("virtual generated column");
    assert!(virtual_column.generation_expression.is_some());
    assert!(!virtual_column.is_generated_stored);
    let stored_column = columns
        .iter()
        .find(|column| column.name == "parent_score")
        .expect("stored generated column");
    assert!(stored_column.generation_expression.is_some());
    assert!(stored_column.is_generated_stored);

    let indexes = schema
        .get_indexes(None, &names.child_table)
        .await
        .expect("indexes");
    assert!(indexes.iter().any(|index| {
        index.name == names.fulltext_index && index.index_type.eq_ignore_ascii_case("FULLTEXT")
    }));
    assert!(indexes.iter().any(|index| {
        index.name == names.spatial_index && index.index_type.eq_ignore_ascii_case("SPATIAL")
    }));
    assert!(
        indexes
            .iter()
            .any(|index| index.name == names.status_index && index.column_descending == vec![true])
    );

    let foreign_keys = schema
        .get_foreign_keys(None, &names.child_table)
        .await
        .expect("foreign keys");
    let foreign_key = foreign_keys
        .iter()
        .find(|foreign_key| foreign_key.name == names.foreign_key)
        .expect("fixture FK");
    assert_eq!(foreign_key.on_delete, ForeignKeyAction::Cascade);
    assert_eq!(foreign_key.on_update, ForeignKeyAction::Restrict);

    let constraints = schema
        .get_constraints(None, &names.child_table)
        .await
        .expect("constraints");
    assert!(
        constraints
            .iter()
            .any(|constraint| constraint.name == names.check_constraint)
    );

    let function_rows = schema
        .list_objects_panel_data_for_kind(None, "function")
        .await
        .expect("function rows");
    let function_row = function_rows
        .rows
        .iter()
        .find(|row| row.object_name() == names.function)
        .expect("function row");
    assert_eq!(
        function_row.values.get("deterministic").map(String::as_str),
        Some("YES")
    );
    assert_eq!(
        function_row
            .values
            .get("sql_data_access")
            .map(String::as_str),
        Some("READS SQL DATA")
    );

    let procedure_rows = schema
        .list_objects_panel_data_for_kind(None, "procedure")
        .await
        .expect("procedure rows");
    assert!(
        procedure_rows
            .rows
            .iter()
            .any(|row| row.object_name() == names.procedure)
    );

    let trigger_rows = schema
        .list_objects_panel_data_for_kind(None, "trigger")
        .await
        .expect("trigger rows");
    let trigger_row = trigger_rows
        .rows
        .iter()
        .find(|row| row.object_name() == names.trigger)
        .expect("trigger row");
    assert_eq!(
        trigger_row.values.get("table_name").map(String::as_str),
        Some(names.child_table.as_str())
    );
    assert!(trigger_row.values.contains_key("definer"));
    assert!(
        trigger_row
            .object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.signature.as_deref())
            == Some(names.child_table.as_str())
    );

    if event_created {
        let event_rows = schema
            .list_objects_panel_data_for_kind(None, "event")
            .await
            .expect("event rows");
        assert!(
            event_rows
                .rows
                .iter()
                .any(|row| row.object_name() == names.event)
        );
        let event_ddl = schema
            .generate_ddl(&DatabaseObject {
                object_type: ObjectType::Event,
                schema: None,
                name: names.event.clone(),
                signature: None,
            })
            .await
            .expect("event DDL");
        assert!(event_ddl.contains("CREATE"));
        assert!(event_ddl.contains(&names.event));
    }

    let index_ddl = schema
        .generate_ddl(&DatabaseObject {
            object_type: ObjectType::Index,
            schema: None,
            name: names.fulltext_index.clone(),
            signature: Some(names.child_table.clone()),
        })
        .await
        .expect("index DDL");
    assert!(index_ddl.contains("FULLTEXT"));
    assert!(index_ddl.contains(&names.fulltext_index));

    let fk_ddl = schema
        .generate_ddl(&DatabaseObject {
            object_type: ObjectType::Constraint,
            schema: None,
            name: names.foreign_key.clone(),
            signature: Some(names.child_table.clone()),
        })
        .await
        .expect("FK DDL");
    assert!(fk_ddl.contains("FOREIGN KEY"));
    assert!(fk_ddl.contains("ON DELETE CASCADE"));

    cleanup_showcase_fixture(&connection, &prefix).await;
}

#[tokio::test]
async fn live_mariadb_sequence_metadata_roundtrip() {
    let Some(connection) = connect("ZQLZ_MARIADB", 3306).await else {
        return;
    };
    let schema = connection
        .as_schema_introspection()
        .expect("MariaDB exposes schema introspection");
    let sequence_name = format!("zqlz_it_{}_seq", Uuid::new_v4().simple());

    if let Err(error) = connection
        .execute(
            &format!(
                "DROP SEQUENCE IF EXISTS {}",
                quote_identifier(&sequence_name)
            ),
            &[],
        )
        .await
    {
        eprintln!("sequence cleanup failed: {error}");
    }

    execute(
        &connection,
        format!(
            "CREATE SEQUENCE {} START WITH 10 INCREMENT BY 2",
            quote_identifier(&sequence_name)
        ),
    )
    .await;

    let manifest = schema
        .list_objects_panel_manifest(None)
        .await
        .expect("manifest");
    assert!(
        manifest
            .object_kinds
            .iter()
            .any(|kind| kind.id == "sequence")
    );

    let sequence_rows = schema
        .list_objects_panel_data_for_kind(None, "sequence")
        .await
        .expect("sequence rows");
    assert!(
        sequence_rows
            .rows
            .iter()
            .any(|row| row.object_name() == sequence_name)
    );

    let ddl = schema
        .generate_ddl(&DatabaseObject {
            object_type: ObjectType::Sequence,
            schema: None,
            name: sequence_name.clone(),
            signature: None,
        })
        .await
        .expect("sequence DDL");
    assert!(ddl.contains("CREATE"));
    assert!(ddl.contains(&sequence_name));

    connection
        .execute(
            &format!(
                "DROP SEQUENCE IF EXISTS {}",
                quote_identifier(&sequence_name)
            ),
            &[],
        )
        .await
        .expect("drop sequence");
}

/// Edge cases checked against what `information_schema` / `SHOW CREATE TABLE`
/// report for the same fixture: literal vs expression defaults, functional and
/// prefix indexes, same-named foreign keys on sibling tables, cross-database
/// references, CHECK names, subpartitions, routine parameters, quoted names,
/// and bulk vs per-table agreement.
#[tokio::test]
async fn live_mysql_catalog_edge_cases() {
    let Some(connection) = connect("ZQLZ_MYSQL", 3306).await else {
        return;
    };
    let schema = connection
        .as_schema_introspection()
        .expect("MySQL exposes schema introspection");
    let p = format!("zqlz_ec_{}", &Uuid::new_v4().simple().to_string()[..12]);
    let q = |suffix: &str| quote_identifier(&format!("{p}_{suffix}"));
    let other_db = format!("{p}_x");
    let weird = format!("{p}_we`ird");

    let cleanup = || async {
        for sql in [
            format!("DROP FUNCTION IF EXISTS {}", q("fn")),
            format!("DROP TABLE IF EXISTS {}, {}, {}", q("child1"), q("child2"), q("child3")),
            format!("DROP TABLE IF EXISTS {}, {}, {}", q("parent"), q("defs"), q("idx")),
            format!("DROP TABLE IF EXISTS {}, {}", q("part"), quote_identifier(&weird)),
            format!("DROP DATABASE IF EXISTS {}", quote_identifier(&other_db)),
        ] {
            let _ = connection.execute(&sql, &[]).await;
        }
    };
    cleanup().await;

    for sql in [
        format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                a VARCHAR(10) NOT NULL DEFAULT 'it''s',
                lit_null VARCHAR(5) DEFAULT 'NULL',
                n DECIMAL(10,2) DEFAULT 1.50,
                u VARCHAR(36) DEFAULT (uuid()),
                ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                touched TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP
            )",
            q("defs")
        ),
        format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                age INT,
                CONSTRAINT {} CHECK (age > 0),
                UNIQUE KEY uq_name_age (name, age),
                KEY fx ((age + 1)),
                KEY pfx (name(10), age DESC) COMMENT 'prefix index'
            )",
            q("idx"),
            q("chk_age")
        ),
        format!("CREATE TABLE {} (a INT, b INT, PRIMARY KEY (a, b))", q("parent")),
        format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, x INT, y INT,
                CONSTRAINT fk_same FOREIGN KEY (y, x) REFERENCES {} (a, b)
                ON DELETE CASCADE ON UPDATE SET NULL)",
            q("child1"),
            q("parent")
        ),
        format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, a INT, b INT,
                CONSTRAINT fk_other FOREIGN KEY (a, b) REFERENCES {} (a, b) ON DELETE RESTRICT)",
            q("child2"),
            q("parent")
        ),
        format!("CREATE DATABASE {}", quote_identifier(&other_db)),
        format!(
            "CREATE TABLE {}.remote (id INT PRIMARY KEY)",
            quote_identifier(&other_db)
        ),
        // FK names are unique per database, so the same name can only recur in
        // another one; its rules must not leak into `child1.fk_same`.
        format!(
            "CREATE TABLE {db}.remote2 (id INT PRIMARY KEY, r INT,
                CONSTRAINT fk_same FOREIGN KEY (r) REFERENCES {db}.remote (id) ON DELETE RESTRICT)",
            db = quote_identifier(&other_db)
        ),
        format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, r INT,
                CONSTRAINT fk_remote FOREIGN KEY (r) REFERENCES {}.remote (id))",
            q("child3"),
            quote_identifier(&other_db)
        ),
        format!(
            "CREATE TABLE {} (id INT NOT NULL, PRIMARY KEY (id))
             PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 2
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
            q("part")
        ),
        format!(
            "CREATE TABLE {} (`from` INT PRIMARY KEY, `or``der` VARCHAR(10))",
            quote_identifier(&weird)
        ),
        format!(
            "CREATE FUNCTION {}(a INT, b VARCHAR(10)) RETURNS VARCHAR(20) DETERMINISTIC
             RETURN CONCAT(a, b)",
            q("fn")
        ),
    ] {
        execute(&connection, sql).await;
    }

    // Defaults are SQL that can follow `DEFAULT` verbatim.
    let defs = schema
        .get_columns(None, &format!("{p}_defs"))
        .await
        .expect("defs columns");
    let default_of = |name: &str| {
        defs.iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("column {name}"))
            .default_value
            .clone()
    };
    assert_eq!(default_of("a").as_deref(), Some("'it''s'"));
    assert_eq!(default_of("lit_null").as_deref(), Some("'NULL'"));
    assert_eq!(default_of("n").as_deref(), Some("1.50"));
    assert_eq!(default_of("u").as_deref(), Some("(uuid())"));
    assert_eq!(
        default_of("ts").as_deref(),
        Some("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    );
    assert_eq!(
        default_of("touched").as_deref(),
        Some("NULL ON UPDATE CURRENT_TIMESTAMP")
    );
    assert_eq!(
        defs.iter().find(|c| c.name == "n").map(|c| c.data_type.as_str()),
        Some("decimal(10,2)")
    );

    // Indexes: functional key part, prefix, DESC, comment, unique.
    let idx_table = format!("{p}_idx");
    let indexes = schema.get_indexes(None, &idx_table).await.expect("indexes");
    let index = |name: &str| {
        indexes
            .iter()
            .find(|index| index.name == name)
            .unwrap_or_else(|| panic!("index {name}: {indexes:?}"))
    };
    assert_eq!(index("PRIMARY").columns, ["id"]);
    assert_eq!(index("fx").columns, ["(`age` + 1)"]);
    assert_eq!(index("pfx").columns, ["name", "age"]);
    assert_eq!(index("pfx").include_columns, ["name(10)"]);
    assert_eq!(index("pfx").column_descending, [false, true]);
    assert_eq!(index("pfx").comment.as_deref(), Some("prefix index"));
    assert!(index("uq_name_age").is_unique);

    // Generated index DDL must be valid SQL: drop and recreate from it.
    for name in ["fx", "pfx"] {
        let ddl = schema
            .generate_ddl(&DatabaseObject {
                object_type: ObjectType::Index,
                schema: None,
                name: name.to_string(),
                signature: Some(idx_table.clone()),
            })
            .await
            .expect("index DDL");
        execute(
            &connection,
            format!("DROP INDEX {} ON {}", quote_identifier(name), q("idx")),
        )
        .await;
        execute(&connection, ddl).await;
    }

    let constraints = schema
        .get_constraints(None, &idx_table)
        .await
        .expect("constraints");
    assert!(
        constraints
            .iter()
            .any(|c| c.name == format!("{p}_chk_age") && c.definition.is_some()),
        "user-named CHECK must be listed: {constraints:?}"
    );
    assert!(constraints.iter().any(|c| c.name == "uq_name_age" && c.columns == ["name", "age"]));

    // Same constraint name on sibling tables keeps its own columns and rules.
    let child1 = schema
        .get_foreign_keys(None, &format!("{p}_child1"))
        .await
        .expect("child1 fks");
    assert_eq!(child1.len(), 1);
    assert_eq!(child1[0].columns, ["y", "x"]);
    assert_eq!(child1[0].referenced_columns, ["a", "b"]);
    assert_eq!(child1[0].on_delete, ForeignKeyAction::Cascade);
    assert_eq!(child1[0].on_update, ForeignKeyAction::SetNull);
    let child2 = schema
        .get_foreign_keys(None, &format!("{p}_child2"))
        .await
        .expect("child2 fks");
    assert_eq!(child2[0].on_delete, ForeignKeyAction::Restrict);
    assert_eq!(child2[0].on_update, ForeignKeyAction::NoAction);
    let remote2 = schema
        .get_foreign_keys(Some(&other_db), "remote2")
        .await
        .expect("remote2 fks");
    assert_eq!(remote2.len(), 1);
    assert_eq!(remote2[0].name, "fk_same");
    assert_eq!(remote2[0].columns, ["r"]);
    assert_eq!(remote2[0].on_delete, ForeignKeyAction::Restrict);

    let child3 = schema
        .get_foreign_keys(None, &format!("{p}_child3"))
        .await
        .expect("child3 fks");
    assert_eq!(child3[0].referenced_schema.as_deref(), Some(other_db.as_str()));
    assert_eq!(child3[0].referenced_table, "remote");

    // Bulk and per-table answers agree.
    let all_fks = schema
        .list_all_foreign_keys(None)
        .await
        .expect("bulk fks")
        .expect("bulk fks supported");
    assert_eq!(
        format!("{:?}", all_fks[&format!("{p}_child1")]),
        format!("{child1:?}")
    );
    assert_eq!(
        format!("{:?}", all_fks[&format!("{p}_child3")]),
        format!("{child3:?}")
    );
    let all_indexes = schema
        .list_all_indexes(None)
        .await
        .expect("bulk indexes")
        .expect("bulk indexes supported");
    let indexes = schema.get_indexes(None, &idx_table).await.expect("indexes");
    assert_eq!(format!("{:?}", all_indexes[&idx_table]), format!("{indexes:?}"));
    let all_columns = schema
        .list_all_columns(None)
        .await
        .expect("bulk columns")
        .expect("bulk columns supported");
    assert_eq!(
        format!("{:?}", all_columns[&format!("{p}_defs")]),
        format!("{defs:?}")
    );

    // Names with backticks and reserved words.
    let weird_columns = schema.get_columns(None, &weird).await.expect("weird columns");
    let names: Vec<&str> = weird_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["from", "or`der"]);
    let weird_ddl = schema
        .generate_ddl(&DatabaseObject {
            object_type: ObjectType::Table,
            schema: None,
            name: weird.clone(),
            signature: None,
        })
        .await
        .expect("weird DDL");
    assert!(weird_ddl.contains("`or``der`"));

    // Routine parameters keep their full type.
    let functions = schema.list_functions(None).await.expect("functions");
    let function = functions
        .iter()
        .find(|f| f.name == format!("{p}_fn"))
        .expect("fixture function");
    assert_eq!(function.return_type, "varchar(20)");
    let parameter_types: Vec<&str> = function
        .parameters
        .iter()
        .map(|parameter| parameter.data_type.as_str())
        .collect();
    assert_eq!(parameter_types, ["int", "varchar(10)"]);

    // Objects panel kinds: subpartitions are distinct rows with bounds.
    let partitions = schema
        .list_objects_panel_data_for_kind(None, "partition")
        .await
        .expect("partition rows");
    let part_rows: Vec<(&str, &str)> = partitions
        .rows
        .iter()
        .filter(|row| row.values.get("table_name").map(String::as_str) == Some(&format!("{p}_part")))
        .map(|row| (row.object_name(), row.values["detail"].as_str()))
        .collect();
    assert_eq!(part_rows.len(), 4, "{part_rows:?}");
    assert_eq!(part_rows[0].0, "p0.p0sp0");
    assert!(part_rows[0].1.contains("LESS THAN (10)"), "{part_rows:?}");
    assert!(part_rows[3].1.contains("LESS THAN MAXVALUE"), "{part_rows:?}");

    let index_rows = schema
        .list_objects_panel_data_for_kind(None, "index")
        .await
        .expect("index rows");
    let pfx_row = index_rows
        .rows
        .iter()
        .find(|row| {
            row.object_name() == "pfx"
                && row.values.get("table_name").map(String::as_str) == Some(idx_table.as_str())
        })
        .expect("pfx index row");
    assert_eq!(pfx_row.values["detail"], "BTREE: name(10), age DESC");

    let fk_rows = schema
        .list_objects_panel_data_for_kind(None, "foreign_key")
        .await
        .expect("foreign key rows");
    assert!(fk_rows.rows.iter().any(|row| {
        row.object_name() == "fk_remote"
            && row.values["detail"].contains(&format!("references {other_db}.remote"))
    }));

    for kind in [
        "table", "view", "function", "trigger", "event", "constraint", "engine", "charset",
        "collation", "user", "role", "grant", "variable", "status_variable", "process",
        "tablespace", "plugin",
    ] {
        let data = schema
            .list_objects_panel_data_for_kind(None, kind)
            .await
            .unwrap_or_else(|error| panic!("panel kind {kind}: {error}"));
        assert!(
            data.rows.iter().all(|row| row.name != "Metadata unavailable"),
            "panel kind {kind} degraded: {:?}",
            data.rows.first().map(|row| &row.values)
        );
    }

    cleanup().await;
}
