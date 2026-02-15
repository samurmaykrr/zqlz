//! Tests for cross-database synchronization tools

use super::type_mapper::*;

mod dialect_tests {
    use super::*;

    #[test]
    fn test_dialect_as_str() {
        assert_eq!(Dialect::PostgreSQL.as_str(), "postgresql");
        assert_eq!(Dialect::MySQL.as_str(), "mysql");
        assert_eq!(Dialect::SQLite.as_str(), "sqlite");
        assert_eq!(Dialect::MsSql.as_str(), "mssql");
    }

    #[test]
    fn test_dialect_from_str() {
        assert_eq!(Dialect::from_str("postgresql"), Some(Dialect::PostgreSQL));
        assert_eq!(Dialect::from_str("postgres"), Some(Dialect::PostgreSQL));
        assert_eq!(Dialect::from_str("pg"), Some(Dialect::PostgreSQL));
        assert_eq!(Dialect::from_str("PostgreSQL"), Some(Dialect::PostgreSQL));

        assert_eq!(Dialect::from_str("mysql"), Some(Dialect::MySQL));
        assert_eq!(Dialect::from_str("mariadb"), Some(Dialect::MySQL));
        assert_eq!(Dialect::from_str("MySQL"), Some(Dialect::MySQL));

        assert_eq!(Dialect::from_str("sqlite"), Some(Dialect::SQLite));
        assert_eq!(Dialect::from_str("sqlite3"), Some(Dialect::SQLite));

        assert_eq!(Dialect::from_str("mssql"), Some(Dialect::MsSql));
        assert_eq!(Dialect::from_str("sqlserver"), Some(Dialect::MsSql));
        assert_eq!(Dialect::from_str("sql server"), Some(Dialect::MsSql));

        assert_eq!(Dialect::from_str("unknown"), None);
    }
}

mod parsed_type_tests {
    use super::*;

    #[test]
    fn test_parsed_type_new() {
        let pt = ParsedType::new("VARCHAR");
        assert_eq!(pt.base_type, "VARCHAR");
        assert!(pt.params.is_empty());
        assert!(!pt.is_array);
    }

    #[test]
    fn test_parsed_type_with_param() {
        let pt = ParsedType::new("VARCHAR").with_param("255");
        assert_eq!(pt.base_type, "VARCHAR");
        assert_eq!(pt.params, vec!["255"]);
    }

    #[test]
    fn test_parsed_type_as_array() {
        let pt = ParsedType::new("INTEGER").as_array();
        assert!(pt.is_array);
    }

    #[test]
    fn test_parsed_type_to_sql_simple() {
        let pt = ParsedType::new("INTEGER");
        assert_eq!(pt.to_sql(), "INTEGER");
    }

    #[test]
    fn test_parsed_type_to_sql_with_params() {
        let pt = ParsedType::new("VARCHAR").with_param("255");
        assert_eq!(pt.to_sql(), "VARCHAR(255)");
    }

    #[test]
    fn test_parsed_type_to_sql_decimal() {
        let pt = ParsedType::new("DECIMAL").with_param("10").with_param("2");
        assert_eq!(pt.to_sql(), "DECIMAL(10, 2)");
    }

    #[test]
    fn test_parsed_type_to_sql_array() {
        let pt = ParsedType::new("INTEGER").as_array();
        assert_eq!(pt.to_sql(), "INTEGER[]");
    }

    #[test]
    fn test_parsed_type_to_sql_array_with_params() {
        let pt = ParsedType::new("VARCHAR").with_param("100").as_array();
        assert_eq!(pt.to_sql(), "VARCHAR(100)[]");
    }
}

mod type_mapper_creation_tests {
    use super::*;

    #[test]
    fn test_type_mapper_new() {
        let mapper = TypeMapper::new();
        let result = mapper
            .map_type("INTEGER", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "INT");
    }

    #[test]
    fn test_type_mapper_default() {
        let mapper = TypeMapper::default();
        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "LONGTEXT");
    }

    #[test]
    fn test_add_custom_mapping() {
        let mut mapper = TypeMapper::new();
        mapper.add_custom_mapping(
            Dialect::PostgreSQL,
            "MYTYPE",
            Dialect::MySQL,
            "VARCHAR(255)",
        );

        let result = mapper
            .map_type("MYTYPE", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(255)");
    }
}

mod same_dialect_tests {
    use super::*;

    #[test]
    fn test_same_dialect_returns_unchanged() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("VARCHAR(255)", Dialect::PostgreSQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(255)");

        let result = mapper
            .map_type("BIGINT", Dialect::MySQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "BIGINT");
    }
}

mod postgres_to_mysql_tests {
    use super::*;

    #[test]
    fn test_map_postgres_serial_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("SERIAL", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "INT AUTO_INCREMENT");

        let result = mapper
            .map_type("BIGSERIAL", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "BIGINT AUTO_INCREMENT");

        let result = mapper
            .map_type("SMALLSERIAL", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "SMALLINT AUTO_INCREMENT");
    }

    #[test]
    fn test_map_postgres_text_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "LONGTEXT");
    }

    #[test]
    fn test_map_postgres_boolean_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BOOLEAN", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "TINYINT(1)");

        let result = mapper
            .map_type("BOOL", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "TINYINT(1)");
    }

    #[test]
    fn test_map_postgres_bytea_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BYTEA", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "LONGBLOB");
    }

    #[test]
    fn test_map_postgres_uuid_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("UUID", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "CHAR(36)");
    }

    #[test]
    fn test_map_postgres_json_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("JSON", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "JSON");

        let result = mapper
            .map_type("JSONB", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "JSON");
    }

    #[test]
    fn test_map_postgres_timestamp_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("TIMESTAMP", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "DATETIME");

        let result = mapper
            .map_type("TIMESTAMPTZ", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "DATETIME");
    }

    #[test]
    fn test_map_postgres_interval_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INTERVAL", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(255)");
    }

    #[test]
    fn test_map_postgres_inet_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INET", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(45)");
    }

    #[test]
    fn test_map_postgres_money_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("MONEY", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "DECIMAL(19, 4)");
    }
}

mod mysql_to_postgres_tests {
    use super::*;

    #[test]
    fn test_map_mysql_datetime_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("DATETIME", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "TIMESTAMP");
    }

    #[test]
    fn test_map_mysql_tinyint_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("TINYINT", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "SMALLINT");
    }

    #[test]
    fn test_map_mysql_blob_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BLOB", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "BYTEA");

        let result = mapper
            .map_type("LONGBLOB", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "BYTEA");
    }

    #[test]
    fn test_map_mysql_enum_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("ENUM", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(255)");
    }

    #[test]
    fn test_map_mysql_json_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("JSON", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "JSONB");
    }

    #[test]
    fn test_map_mysql_year_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("YEAR", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "SMALLINT");
    }
}

mod sqlite_mapping_tests {
    use super::*;

    #[test]
    fn test_postgres_to_sqlite() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("SERIAL", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "INTEGER PRIMARY KEY");

        let result = mapper
            .map_type("BIGINT", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "INTEGER");

        let result = mapper
            .map_type("BOOLEAN", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "INTEGER");

        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");

        let result = mapper
            .map_type("BYTEA", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "BLOB");
    }

    #[test]
    fn test_sqlite_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INTEGER", Dialect::SQLite, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "INTEGER");

        let result = mapper
            .map_type("REAL", Dialect::SQLite, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "DOUBLE PRECISION");

        let result = mapper
            .map_type("BLOB", Dialect::SQLite, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "BYTEA");
    }

    #[test]
    fn test_sqlite_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INTEGER", Dialect::SQLite, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "BIGINT");

        let result = mapper
            .map_type("REAL", Dialect::SQLite, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "DOUBLE");

        let result = mapper
            .map_type("TEXT", Dialect::SQLite, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "LONGTEXT");
    }
}

mod mssql_mapping_tests {
    use super::*;

    #[test]
    fn test_postgres_to_mssql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("SERIAL", Dialect::PostgreSQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "INT IDENTITY(1,1)");

        let result = mapper
            .map_type("BOOLEAN", Dialect::PostgreSQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "BIT");

        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "NVARCHAR(MAX)");

        let result = mapper
            .map_type("UUID", Dialect::PostgreSQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "UNIQUEIDENTIFIER");

        let result = mapper
            .map_type("TIMESTAMPTZ", Dialect::PostgreSQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "DATETIMEOFFSET");
    }

    #[test]
    fn test_mssql_to_postgres() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BIT", Dialect::MsSql, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "BOOLEAN");

        let result = mapper
            .map_type("UNIQUEIDENTIFIER", Dialect::MsSql, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "UUID");

        let result = mapper
            .map_type("DATETIMEOFFSET", Dialect::MsSql, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "TIMESTAMP WITH TIME ZONE");

        let result = mapper
            .map_type("MONEY", Dialect::MsSql, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "MONEY");

        let result = mapper
            .map_type("IMAGE", Dialect::MsSql, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "BYTEA");
    }

    #[test]
    fn test_mssql_to_mysql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BIT", Dialect::MsSql, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "TINYINT(1)");

        let result = mapper
            .map_type("NVARCHAR", Dialect::MsSql, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR");

        let result = mapper
            .map_type("NTEXT", Dialect::MsSql, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "LONGTEXT");

        let result = mapper
            .map_type("UNIQUEIDENTIFIER", Dialect::MsSql, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "CHAR(36)");
    }

    #[test]
    fn test_mssql_to_sqlite() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INT", Dialect::MsSql, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "INTEGER");

        let result = mapper
            .map_type("FLOAT", Dialect::MsSql, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "REAL");

        let result = mapper
            .map_type("NVARCHAR", Dialect::MsSql, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");

        let result = mapper
            .map_type("VARBINARY", Dialect::MsSql, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "BLOB");
    }
}

mod type_with_params_tests {
    use super::*;

    #[test]
    fn test_varchar_with_length() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("VARCHAR(100)", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(100)");
    }

    #[test]
    fn test_decimal_with_precision_and_scale() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("DECIMAL(10, 2)", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "DECIMAL(10, 2)");
    }

    #[test]
    fn test_char_with_length() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("CHAR(50)", Dialect::MySQL, Dialect::PostgreSQL)
            .unwrap();
        assert_eq!(result, "CHAR(50)");
    }
}

mod convenience_function_tests {
    use super::*;

    #[test]
    fn test_map_type_function_postgres_to_mysql() {
        let result = map_type("SERIAL", "postgresql", "mysql").unwrap();
        assert_eq!(result, "INT AUTO_INCREMENT");
    }

    #[test]
    fn test_map_type_function_mysql_to_postgres() {
        let result = map_type("DATETIME", "mysql", "postgres").unwrap();
        assert_eq!(result, "TIMESTAMP");
    }

    #[test]
    fn test_map_type_function_with_aliases() {
        let result = map_type("TEXT", "pg", "mariadb").unwrap();
        assert_eq!(result, "LONGTEXT");
    }

    #[test]
    fn test_map_type_function_invalid_dialect() {
        let result = map_type("TEXT", "unknown", "mysql");
        assert!(result.is_err());
    }
}

mod error_handling_tests {
    use super::*;

    #[test]
    fn test_empty_type_string() {
        let mapper = TypeMapper::new();
        let result = mapper.map_type("", Dialect::PostgreSQL, Dialect::MySQL);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_type_format_missing_paren() {
        let mapper = TypeMapper::new();
        let result = mapper.map_type("VARCHAR(100", Dialect::PostgreSQL, Dialect::MySQL);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_type_passes_through() {
        let mapper = TypeMapper::new();
        let result = mapper
            .map_type("CUSTOMTYPE", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "CUSTOMTYPE");
    }
}

mod array_type_tests {
    use super::*;

    #[test]
    fn test_integer_array() {
        let mapper = TypeMapper::new();
        let result = mapper
            .map_type("INTEGER[]", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "INT[]");
    }

    #[test]
    fn test_varchar_array_with_length() {
        let mapper = TypeMapper::new();
        let result = mapper
            .map_type("VARCHAR(100)[]", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(100)[]");
    }
}

mod custom_mapping_tests {
    use super::*;

    #[test]
    fn test_custom_mapping_override() {
        let mut mapper = TypeMapper::new();
        mapper.add_custom_mapping(Dialect::PostgreSQL, "TEXT", Dialect::MySQL, "MEDIUMTEXT");

        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "MEDIUMTEXT");
    }

    #[test]
    fn test_custom_mapping_case_insensitive() {
        let mut mapper = TypeMapper::new();
        mapper.add_custom_mapping(
            Dialect::PostgreSQL,
            "mytype",
            Dialect::MySQL,
            "VARCHAR(100)",
        );

        let result = mapper
            .map_type("MYTYPE", Dialect::PostgreSQL, Dialect::MySQL)
            .unwrap();
        assert_eq!(result, "VARCHAR(100)");
    }

    #[test]
    fn test_custom_mapping_does_not_affect_other_dialects() {
        let mut mapper = TypeMapper::new();
        mapper.add_custom_mapping(Dialect::PostgreSQL, "TEXT", Dialect::MySQL, "MEDIUMTEXT");

        let result = mapper
            .map_type("TEXT", Dialect::PostgreSQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");
    }
}

mod mysql_to_mssql_tests {
    use super::*;

    #[test]
    fn test_mysql_to_mssql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("TINYINT", Dialect::MySQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "TINYINT");

        let result = mapper
            .map_type("MEDIUMINT", Dialect::MySQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "INT");

        let result = mapper
            .map_type("LONGTEXT", Dialect::MySQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "NVARCHAR(MAX)");

        let result = mapper
            .map_type("LONGBLOB", Dialect::MySQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "VARBINARY(MAX)");

        let result = mapper
            .map_type("ENUM", Dialect::MySQL, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "NVARCHAR(255)");
    }
}

mod sqlite_to_mssql_tests {
    use super::*;

    #[test]
    fn test_sqlite_to_mssql() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("INTEGER", Dialect::SQLite, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "BIGINT");

        let result = mapper
            .map_type("REAL", Dialect::SQLite, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "FLOAT");

        let result = mapper
            .map_type("TEXT", Dialect::SQLite, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "NVARCHAR(MAX)");

        let result = mapper
            .map_type("BLOB", Dialect::SQLite, Dialect::MsSql)
            .unwrap();
        assert_eq!(result, "VARBINARY(MAX)");
    }
}

mod mysql_to_sqlite_tests {
    use super::*;

    #[test]
    fn test_mysql_to_sqlite() {
        let mapper = TypeMapper::new();

        let result = mapper
            .map_type("BIGINT", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "INTEGER");

        let result = mapper
            .map_type("DOUBLE", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "REAL");

        let result = mapper
            .map_type("VARCHAR", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");

        let result = mapper
            .map_type("MEDIUMBLOB", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "BLOB");

        let result = mapper
            .map_type("DATETIME", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");

        let result = mapper
            .map_type("JSON", Dialect::MySQL, Dialect::SQLite)
            .unwrap();
        assert_eq!(result, "TEXT");
    }
}
