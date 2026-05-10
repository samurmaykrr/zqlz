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
                status ENUM('pending','paid') NOT NULL,
                flags SET('read','write') NULL,
                name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                display_name VARCHAR(140) GENERATED ALWAYS AS (CONCAT(name, '-display')) VIRTUAL,
                parent_score BIGINT GENERATED ALWAYS AS (parent_id * 10) STORED,
                geom POINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FULLTEXT KEY {} (name),
                SPATIAL KEY {} (geom),
                KEY {} (status DESC),
                CONSTRAINT {} FOREIGN KEY (parent_id) REFERENCES {} (id)
                    ON DELETE CASCADE ON UPDATE RESTRICT,
                CONSTRAINT {} CHECK (parent_id >= 0)
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
