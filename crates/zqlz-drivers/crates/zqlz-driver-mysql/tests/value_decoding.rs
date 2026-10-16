//! Live checks that MySQL values and result-column metadata decode like the
//! `mysql` CLI shows them. Opt in with `ZQLZ_MYSQL_INTEGRATION=1`.

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, QueryResult, Value};
use zqlz_driver_mysql::MySqlDriver;

const DATABASE: &str = "zqlz_a_values";

const CREATE_TABLE: &str = r#"CREATE TABLE ty (
 id INT PRIMARY KEY AUTO_INCREMENT,
 c_tiny TINYINT, c_utiny TINYINT UNSIGNED, c_small SMALLINT, c_med MEDIUMINT, c_umed MEDIUMINT UNSIGNED,
 c_int INT, c_uint INT UNSIGNED, c_big BIGINT, c_ubig BIGINT UNSIGNED,
 c_bool BOOLEAN, c_dec DECIMAL(20,6), c_dec0 DECIMAL(10,0), c_float FLOAT, c_double DOUBLE,
 c_char CHAR(5), c_varchar VARCHAR(50), c_tinytext TINYTEXT, c_text TEXT, c_longtext LONGTEXT,
 c_binary BINARY(4), c_varbinary VARBINARY(10), c_blob BLOB, c_tinyblob TINYBLOB, c_longblob LONGBLOB,
 c_enum ENUM('a','b','c'), c_set SET('x','y','z'),
 c_date DATE, c_time TIME, c_time3 TIME(3), c_dt DATETIME, c_dt6 DATETIME(6), c_ts TIMESTAMP NULL, c_year YEAR,
 c_bit BIT(8), c_bit1 BIT(1), c_bit64 BIT(64), c_json JSON,
 c_point POINT, c_line LINESTRING, c_poly POLYGON, c_geom GEOMETRY, c_gc GEOMETRYCOLLECTION, c_mpoint MULTIPOINT
)"#;

const INSERT_TYPED_ROWS: &str = r#"INSERT INTO ty (c_tiny,c_utiny,c_small,c_med,c_umed,c_int,c_uint,c_big,c_ubig,c_bool,c_dec,c_dec0,c_float,c_double,c_char,c_varchar,c_tinytext,c_text,c_longtext,c_binary,c_varbinary,c_blob,c_tinyblob,c_longblob,c_enum,c_set,c_date,c_time,c_time3,c_dt,c_dt6,c_ts,c_year,c_bit,c_bit1,c_bit64,c_json,c_point,c_line,c_poly,c_geom,c_gc,c_mpoint) VALUES (-128,255,-32768,-8388608,16777215,-2147483648,4294967295,-9223372036854775808,18446744073709551615,TRUE,-12345678901234.123456,-9999999999,3.14159274,2.718281828459045,'ab','héllo wörld 😀','tiny','text value','long',0x01020304,0x0A0B,0xDEADBEEF,0x00,X'','b','x,z','2024-02-29','-838:59:59','12:34:56.789','2024-02-29 00:00:00','2024-02-29 23:59:59.123456','2024-02-29 10:00:00',2024,b'10101010',b'1',b'1111111111111111111111111111111111111111111111111111111111111111','{"a": [1, 2, {"b": null}], "c": "d"}',ST_GeomFromText('POINT(1 2)'),ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'),ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))'),ST_GeomFromText('POINT(3.5 -4.25)'),ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))'),ST_GeomFromText('MULTIPOINT((0 0),(1 1))')), (127,0,32767,8388607,0,2147483647,0,9223372036854775807,0,FALSE,0.000001,0,-0.5,-1e-10,'','',NULL,'','',0x00000000,X'',X'',X'',X'','a','','1000-01-01','00:00:00','00:00:00.000','1000-01-01 00:00:01','1000-01-01 00:00:00.000001','1970-01-01 00:00:01',1901,b'0',b'0',b'0','[]',ST_GeomFromText('POINT(0 0)'),ST_GeomFromText('LINESTRING(0 0,1 0)'),ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'),ST_GeomFromText('POINT(0 0)'),ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'),ST_GeomFromText('MULTIPOINT((0 0))'))"#;

const INSERT_NULL_ROW: &str = "INSERT INTO ty (id) VALUES (3)";

/// Column, expected text in row 1, expected text in row 2 (row 3 is all NULL).
/// Binary columns are expected as upper-case hex like `HEX()`. `c_double` row 2
/// is `-1e-10` on the server; Rust prints that float without an exponent.
const EXPECTED: &[(&str, &str, &str)] = &[
    ("c_tiny", "-128", "127"),
    ("c_utiny", "255", "0"),
    ("c_small", "-32768", "32767"),
    ("c_med", "-8388608", "8388607"),
    ("c_umed", "16777215", "0"),
    ("c_int", "-2147483648", "2147483647"),
    ("c_uint", "4294967295", "0"),
    ("c_big", "-9223372036854775808", "9223372036854775807"),
    ("c_ubig", "18446744073709551615", "0"),
    ("c_bool", "1", "0"),
    ("c_dec", "-12345678901234.123456", "0.000001"),
    ("c_dec0", "-9999999999", "0"),
    ("c_float", "3.14159", "-0.5"),
    ("c_double", "2.718281828459045", "-0.0000000001"),
    ("c_char", "ab", ""),
    ("c_varchar", "héllo wörld 😀", ""),
    ("c_tinytext", "tiny", "NULL"),
    ("c_text", "text value", ""),
    ("c_longtext", "long", ""),
    ("c_binary", "01020304", "00000000"),
    ("c_varbinary", "0A0B", ""),
    ("c_blob", "DEADBEEF", ""),
    ("c_tinyblob", "00", ""),
    ("c_longblob", "", ""),
    ("c_enum", "b", "a"),
    ("c_set", "x,z", ""),
    ("c_date", "2024-02-29", "1000-01-01"),
    ("c_time", "-838:59:59", "00:00:00"),
    ("c_time3", "12:34:56.789", "00:00:00.000"),
    ("c_dt", "2024-02-29 00:00:00", "1000-01-01 00:00:01"),
    ("c_dt6", "2024-02-29 23:59:59.123456", "1000-01-01 00:00:00.000001"),
    ("c_ts", "2024-02-29 10:00:00", "1970-01-01 00:00:01"),
    ("c_year", "2024", "1901"),
    ("c_bit", "10101010", "00000000"),
    ("c_bit1", "1", "0"),
    ("c_bit64", "1111111111111111111111111111111111111111111111111111111111111111", "0000000000000000000000000000000000000000000000000000000000000000"),
    ("c_json", "{\"a\": [1, 2, {\"b\": null}], \"c\": \"d\"}", "[]"),
    ("c_point", "POINT(1 2)", "POINT(0 0)"),
    ("c_line", "LINESTRING(0 0,1 1,2 2)", "LINESTRING(0 0,1 0)"),
    ("c_poly", "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))", "POLYGON((0 0,1 0,1 1,0 0))"),
    ("c_geom", "POINT(3.5 -4.25)", "POINT(0 0)"),
    ("c_gc", "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))", "GEOMETRYCOLLECTION EMPTY"),
    ("c_mpoint", "MULTIPOINT((0 0),(1 1))", "MULTIPOINT((0 0))"),
];

async fn connect() -> Option<Arc<dyn Connection>> {
    if std::env::var("ZQLZ_MYSQL_INTEGRATION").ok().as_deref() != Some("1") {
        eprintln!("skipping MySQL integration test; set ZQLZ_MYSQL_INTEGRATION=1");
        return None;
    }
    let var = |name: &str, default: &str| {
        std::env::var(format!("ZQLZ_MYSQL_{name}")).unwrap_or_else(|_| default.to_string())
    };
    let mut config = ConnectionConfig::new_mysql(
        &var("HOST", "127.0.0.1"),
        var("PORT", "3306").parse().expect("port"),
        &var("DATABASE", "test"),
        &var("USER", "root"),
    );
    config.password = Some(var("PASSWORD", "")).filter(|password| !password.is_empty());
    Some(
        MySqlDriver::new()
            .connect(&config)
            .await
            .expect("connect to MySQL"),
    )
}

async fn execute(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

async fn query(connection: &Arc<dyn Connection>, sql: &str) -> QueryResult {
    connection
        .query(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

/// A cell the way the `mysql` CLI would print it (bytes as hex).
fn cell_text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bytes(bytes) => bytes.iter().map(|byte| format!("{byte:02X}")).collect(),
        other => other.to_string(),
    }
}

#[tokio::test]
async fn live_mysql_values_and_column_types_match_the_server() {
    let Some(connection) = connect().await else {
        return;
    };
    // Every statement names the database: pooled connections do not share `USE`.
    execute(&connection, &format!("DROP DATABASE IF EXISTS {DATABASE}")).await;
    execute(&connection, &format!("CREATE DATABASE {DATABASE}")).await;
    let qualified_create = CREATE_TABLE.replacen("CREATE TABLE ty", &format!("CREATE TABLE {DATABASE}.ty"), 1);
    execute(&connection, &qualified_create).await;
    for insert in [INSERT_TYPED_ROWS, INSERT_NULL_ROW] {
        execute(&connection, &insert.replacen("INSERT INTO ty", &format!("INSERT INTO {DATABASE}.ty"), 1)).await;
    }

    let result = query(&connection, &format!("SELECT * FROM {DATABASE}.ty ORDER BY id")).await;
    assert_eq!(result.rows.len(), 3);

    let mut diffs = Vec::new();
    for (column, row_1, row_2) in EXPECTED {
        let index = result
            .columns
            .iter()
            .position(|meta| meta.name == *column)
            .unwrap_or_else(|| panic!("missing column {column}"));
        for (row, expected) in [(0, *row_1), (1, *row_2), (2, "NULL")] {
            let got = cell_text(&result.rows[row].values[index]);
                    if got != expected {
                diffs.push(format!("{column} row {}: got {got:?}, expected {expected:?}", row + 1));
            }
        }
    }

    // Value variants that matter beyond their text.
    let value = |column: &str, row: usize| {
        let index = result.columns.iter().position(|meta| meta.name == column).unwrap();
        result.rows[row].values[index].clone()
    };
    assert!(matches!(value("c_text", 0), Value::String(_)), "TEXT is text");
    assert!(matches!(value("c_tinytext", 0), Value::String(_)), "TINYTEXT is text");
    assert!(matches!(value("c_longtext", 0), Value::String(_)), "LONGTEXT is text");
    assert!(matches!(value("c_varbinary", 0), Value::Bytes(_)), "VARBINARY is bytes");
    assert!(matches!(value("c_binary", 0), Value::Bytes(_)), "BINARY is bytes");
    assert!(matches!(value("c_blob", 0), Value::Bytes(_)), "BLOB is bytes");
    assert!(matches!(value("c_json", 0), Value::String(_)), "JSON keeps the server text");
    assert_eq!(value("c_int", 0), Value::Int64(-2147483648));
    assert_eq!(value("c_float", 0), Value::Float32(3.14159));
    assert_eq!(value("c_ubig", 0), Value::Decimal("18446744073709551615".to_string()));
    assert_eq!(value("c_varchar", 1), Value::String(String::new()), "empty string is not NULL");
    assert_eq!(value("c_varchar", 2), Value::Null);

    // The declared type: COLUMN_TYPE, minus what the protocol cannot carry.
    let declared = query(
        &connection,
        &format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = '{DATABASE}' AND TABLE_NAME = 'ty' ORDER BY ORDINAL_POSITION"
        ),
    )
    .await;
    assert_eq!(declared.rows.len(), result.columns.len());
    for (meta, row) in result.columns.iter().zip(&declared.rows) {
        let name = row.get(0).and_then(Value::as_str).expect("COLUMN_NAME is text");
        let column_type = row.get(1).and_then(Value::as_str).expect("COLUMN_TYPE is text");
        assert_eq!(meta.name, name);
        let expected = if column_type.starts_with("enum(") {
            "enum"
        } else if column_type.starts_with("set(") {
            "set"
        } else if matches!(
            column_type,
            "point" | "linestring" | "polygon" | "geomcollection" | "geometrycollection" | "multipoint"
        ) {
            "geometry"
        } else {
            column_type
        };
        if meta.data_type != expected {
            diffs.push(format!("{name}: data_type {:?}, declared {column_type:?}", meta.data_type));
        }
    }

    execute(&connection, &format!("DROP DATABASE IF EXISTS {DATABASE}")).await;
    assert!(diffs.is_empty(), "differences from the server:\n{}", diffs.join("\n"));
}

#[tokio::test]
async fn live_mysql_information_schema_and_describe_decode_as_text() {
    let Some(connection) = connect().await else {
        return;
    };
    execute(&connection, &format!("DROP DATABASE IF EXISTS {DATABASE}_d")).await;
    execute(&connection, &format!("CREATE DATABASE {DATABASE}_d")).await;
    execute(
        &connection,
        &format!(
            "CREATE TABLE {DATABASE}_d.dflt (id INT PRIMARY KEY AUTO_INCREMENT, \
             a INT DEFAULT 7, b VARCHAR(10) DEFAULT 'x', c TEXT, d ENUM('p','q') DEFAULT 'q')"
        ),
    )
    .await;

    let columns = query(
        &connection,
        &format!(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_DEFAULT FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = '{DATABASE}_d' AND TABLE_NAME = 'dflt' ORDER BY ORDINAL_POSITION"
        ),
    )
    .await;
    let cells: Vec<Vec<String>> = columns
        .rows
        .iter()
        .map(|row| row.values.iter().map(cell_text).collect())
        .collect();
    assert_eq!(
        cells,
        vec![
            vec!["id", "int", "int", "NULL"],
            vec!["a", "int", "int", "7"],
            vec!["b", "varchar", "varchar(10)", "x"],
            vec!["c", "text", "text", "NULL"],
            vec!["d", "enum", "enum('p','q')", "q"],
        ]
    );
    assert!(columns.rows.iter().all(|row| matches!(row.values[2], Value::String(_))));

    let described = query(&connection, &format!("DESCRIBE {DATABASE}_d.dflt")).await;
    let cells: Vec<Vec<String>> = described
        .rows
        .iter()
        .map(|row| row.values.iter().map(cell_text).collect())
        .collect();
    assert_eq!(cells[1], vec!["a", "int", "YES", "", "7", ""]);
    assert_eq!(cells[0][5], "auto_increment");

    let shown = query(&connection, &format!("SHOW COLUMNS FROM {DATABASE}_d.dflt")).await;
    assert_eq!(cell_text(&shown.rows[2].values[1]), "varchar(10)");

    // What the table designer and schema panel read.
    let schema = connection
        .as_schema_introspection()
        .expect("MySQL exposes schema introspection");
    let columns = schema
        .get_columns(Some(&format!("{DATABASE}_d")), "dflt")
        .await
        .expect("columns");
    assert!(columns.iter().all(|column| !column.data_type.is_empty()));
    let b = columns.iter().find(|column| column.name == "b").expect("b");
    assert_eq!(b.data_type, "varchar(10)");
    // The catalog hands defaults over as SQL literals.
    assert_eq!(b.default_value.as_deref(), Some("'x'"));

    // Expressions: best-effort names, never an invented length.
    let expression = query(&connection, "SELECT @@sql_mode, version()").await;
    assert_eq!(expression.columns[0].data_type, "varchar");
    assert!(expression.columns[1].data_type.starts_with("varchar"));

    execute(&connection, &format!("DROP DATABASE IF EXISTS {DATABASE}_d")).await;
}
