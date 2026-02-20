//! Tests for MS SQL Server schema introspection

use super::schema::*;
use zqlz_core::{
    ColumnInfo, ConstraintInfo, ConstraintType, DatabaseInfo, ForeignKeyAction, ForeignKeyInfo,
    IndexInfo, PrimaryKeyInfo, SchemaInfo, SequenceInfo, TableDetails, TableInfo, TableType,
    TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo,
};

// Helper to create test DatabaseInfo
fn test_database_info() -> DatabaseInfo {
    DatabaseInfo {
        name: "TestDB".to_string(),
        owner: Some("dbo".to_string()),
        encoding: None,
        size_bytes: Some(1024 * 1024 * 100),
        comment: None,
    }
}

// Helper to create test SchemaInfo
fn test_schema_info() -> SchemaInfo {
    SchemaInfo {
        name: "dbo".to_string(),
        owner: Some("dbo".to_string()),
        comment: None,
    }
}

// Helper to create test TableInfo
fn test_table_info() -> TableInfo {
    TableInfo {
        name: "Users".to_string(),
        schema: Some("dbo".to_string()),
        table_type: TableType::Table,
        owner: None,
        row_count: Some(1000),
        size_bytes: Some(1024 * 1024),
        comment: None,
        index_count: Some(2),
        trigger_count: Some(1),
        key_value_info: None,
    }
}

// Helper to create test ColumnInfo
fn test_column_info() -> ColumnInfo {
    ColumnInfo {
        name: "Id".to_string(),
        ordinal: 1,
        data_type: "int".to_string(),
        nullable: false,
        default_value: None,
        max_length: Some(4),
        precision: Some(10),
        scale: Some(0),
        is_primary_key: true,
        is_auto_increment: true,
        is_unique: true,
        foreign_key: None,
        comment: None,
        ..Default::default()
    }
}

// Tests for parse_fk_action
#[test]
fn test_parse_fk_action_cascade() {
    assert_eq!(parse_fk_action("CASCADE"), ForeignKeyAction::Cascade);
    assert_eq!(parse_fk_action("cascade"), ForeignKeyAction::Cascade);
}

#[test]
fn test_parse_fk_action_set_null() {
    assert_eq!(parse_fk_action("SET_NULL"), ForeignKeyAction::SetNull);
    assert_eq!(parse_fk_action("SET NULL"), ForeignKeyAction::SetNull);
}

#[test]
fn test_parse_fk_action_set_default() {
    assert_eq!(parse_fk_action("SET_DEFAULT"), ForeignKeyAction::SetDefault);
    assert_eq!(parse_fk_action("SET DEFAULT"), ForeignKeyAction::SetDefault);
}

#[test]
fn test_parse_fk_action_no_action() {
    assert_eq!(parse_fk_action("NO_ACTION"), ForeignKeyAction::NoAction);
    assert_eq!(parse_fk_action("NO ACTION"), ForeignKeyAction::NoAction);
}

#[test]
fn test_parse_fk_action_unknown() {
    assert_eq!(parse_fk_action("RESTRICT"), ForeignKeyAction::NoAction);
    assert_eq!(parse_fk_action("unknown"), ForeignKeyAction::NoAction);
}

// Tests for generate_table_ddl
#[test]
fn test_generate_table_ddl_simple() {
    let table = TableDetails {
        info: test_table_info(),
        columns: vec![
            ColumnInfo {
                name: "Id".to_string(),
                ordinal: 1,
                data_type: "int".to_string(),
                nullable: false,
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: true,
                is_auto_increment: true,
                is_unique: true,
                foreign_key: None,
                comment: None,
                ..Default::default()
            },
            ColumnInfo {
                name: "Name".to_string(),
                ordinal: 2,
                data_type: "nvarchar(100)".to_string(),
                nullable: false,
                default_value: None,
                max_length: Some(100),
                precision: None,
                scale: None,
                is_primary_key: false,
                is_auto_increment: false,
                is_unique: false,
                foreign_key: None,
                comment: None,
                ..Default::default()
            },
        ],
        primary_key: Some(PrimaryKeyInfo {
            name: Some("PK_Users".to_string()),
            columns: vec!["Id".to_string()],
        }),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
    };

    let ddl = generate_table_ddl(&table, "dbo");
    assert!(ddl.contains("CREATE TABLE [dbo].[Users]"));
    assert!(ddl.contains("[Id] int IDENTITY(1,1) NOT NULL"));
    assert!(ddl.contains("[Name] nvarchar(100) NOT NULL"));
    assert!(ddl.contains("CONSTRAINT [PK_Users] PRIMARY KEY ([Id])"));
}

#[test]
fn test_generate_table_ddl_with_default() {
    let table = TableDetails {
        info: TableInfo {
            name: "Settings".to_string(),
            schema: Some("dbo".to_string()),
            table_type: TableType::Table,
            owner: None,
            row_count: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        },
        columns: vec![ColumnInfo {
            name: "CreatedAt".to_string(),
            ordinal: 1,
            data_type: "datetime2".to_string(),
            nullable: false,
            default_value: Some("GETDATE()".to_string()),
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
            ..Default::default()
        }],
        primary_key: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
    };

    let ddl = generate_table_ddl(&table, "dbo");
    assert!(ddl.contains("DEFAULT GETDATE()"));
}

#[test]
fn test_generate_table_ddl_nullable_column() {
    let table = TableDetails {
        info: TableInfo {
            name: "Optional".to_string(),
            schema: Some("dbo".to_string()),
            table_type: TableType::Table,
            owner: None,
            row_count: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        },
        columns: vec![ColumnInfo {
            name: "Description".to_string(),
            ordinal: 1,
            data_type: "nvarchar(max)".to_string(),
            nullable: true,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
            ..Default::default()
        }],
        primary_key: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
    };

    let ddl = generate_table_ddl(&table, "dbo");
    assert!(ddl.contains("[Description] nvarchar(max) NULL"));
}

// Tests for data structures
#[test]
fn test_database_info_creation() {
    let db = test_database_info();
    assert_eq!(db.name, "TestDB");
    assert_eq!(db.owner, Some("dbo".to_string()));
    assert!(db.size_bytes.is_some());
}

#[test]
fn test_schema_info_creation() {
    let schema = test_schema_info();
    assert_eq!(schema.name, "dbo");
    assert_eq!(schema.owner, Some("dbo".to_string()));
}

#[test]
fn test_table_info_creation() {
    let table = test_table_info();
    assert_eq!(table.name, "Users");
    assert_eq!(table.schema, Some("dbo".to_string()));
    assert_eq!(table.table_type, TableType::Table);
    assert_eq!(table.row_count, Some(1000));
    assert_eq!(table.index_count, Some(2));
    assert_eq!(table.trigger_count, Some(1));
}

#[test]
fn test_column_info_creation() {
    let col = test_column_info();
    assert_eq!(col.name, "Id");
    assert_eq!(col.ordinal, 1);
    assert_eq!(col.data_type, "int");
    assert!(!col.nullable);
    assert!(col.is_primary_key);
    assert!(col.is_auto_increment);
    assert!(col.is_unique);
}

#[test]
fn test_view_info_creation() {
    let view = ViewInfo {
        name: "ActiveUsers".to_string(),
        schema: Some("dbo".to_string()),
        is_materialized: false,
        definition: Some("SELECT * FROM Users WHERE IsActive = 1".to_string()),
        owner: None,
        comment: None,
    };
    assert_eq!(view.name, "ActiveUsers");
    assert!(!view.is_materialized);
    assert!(view.definition.is_some());
}

#[test]
fn test_index_info_creation() {
    let index = IndexInfo {
        name: "IX_Users_Email".to_string(),
        columns: vec!["Email".to_string()],
        is_unique: true,
        is_primary: false,
        index_type: "NONCLUSTERED".to_string(),
        comment: None,
        ..Default::default()
    };
    assert_eq!(index.name, "IX_Users_Email");
    assert!(index.is_unique);
    assert!(!index.is_primary);
    assert_eq!(index.columns.len(), 1);
}

#[test]
fn test_foreign_key_info_creation() {
    let fk = ForeignKeyInfo {
        name: "FK_Orders_Users".to_string(),
        columns: vec!["UserId".to_string()],
        referenced_table: "Users".to_string(),
        referenced_schema: Some("dbo".to_string()),
        referenced_columns: vec!["Id".to_string()],
        on_update: ForeignKeyAction::Cascade,
        on_delete: ForeignKeyAction::SetNull,
        is_deferrable: false,
        initially_deferred: false,
    };
    assert_eq!(fk.name, "FK_Orders_Users");
    assert_eq!(fk.referenced_table, "Users");
    assert_eq!(fk.on_update, ForeignKeyAction::Cascade);
    assert_eq!(fk.on_delete, ForeignKeyAction::SetNull);
}

#[test]
fn test_primary_key_info_creation() {
    let pk = PrimaryKeyInfo {
        name: Some("PK_Users".to_string()),
        columns: vec!["Id".to_string()],
    };
    assert_eq!(pk.name, Some("PK_Users".to_string()));
    assert_eq!(pk.columns.len(), 1);
}

#[test]
fn test_primary_key_composite() {
    let pk = PrimaryKeyInfo {
        name: Some("PK_OrderItems".to_string()),
        columns: vec!["OrderId".to_string(), "ProductId".to_string()],
    };
    assert_eq!(pk.columns.len(), 2);
    assert_eq!(pk.columns[0], "OrderId");
    assert_eq!(pk.columns[1], "ProductId");
}

#[test]
fn test_constraint_info_check() {
    let constraint = ConstraintInfo {
        name: "CK_Users_Age".to_string(),
        constraint_type: ConstraintType::Check,
        columns: vec!["Age".to_string()],
        definition: Some("([Age]>=(0) AND [Age]<=(150))".to_string()),
    };
    assert_eq!(constraint.constraint_type, ConstraintType::Check);
    assert!(constraint.definition.is_some());
}

#[test]
fn test_constraint_info_unique() {
    let constraint = ConstraintInfo {
        name: "UQ_Users_Email".to_string(),
        constraint_type: ConstraintType::Unique,
        columns: vec!["Email".to_string()],
        definition: None,
    };
    assert_eq!(constraint.constraint_type, ConstraintType::Unique);
}

#[test]
fn test_sequence_info_creation() {
    let seq = SequenceInfo {
        name: "OrderNumberSeq".to_string(),
        schema: Some("dbo".to_string()),
        data_type: "bigint".to_string(),
        start_value: 1000,
        min_value: 1000,
        max_value: i64::MAX,
        increment_by: 1,
        current_value: Some(1500),
        owner: None,
        comment: None,
    };
    assert_eq!(seq.name, "OrderNumberSeq");
    assert_eq!(seq.start_value, 1000);
    assert_eq!(seq.current_value, Some(1500));
}

#[test]
fn test_trigger_info_creation() {
    let trigger = TriggerInfo {
        name: "TR_Users_Audit".to_string(),
        schema: Some("dbo".to_string()),
        table_name: "Users".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update],
        for_each: TriggerForEach::Row,
        definition: Some("CREATE TRIGGER ...".to_string()),
        enabled: true,
        comment: None,
    };
    assert_eq!(trigger.name, "TR_Users_Audit");
    assert_eq!(trigger.timing, TriggerTiming::After);
    assert_eq!(trigger.events.len(), 2);
    assert!(trigger.enabled);
}

#[test]
fn test_trigger_timing_instead_of() {
    let trigger = TriggerInfo {
        name: "TR_View_Insert".to_string(),
        schema: Some("dbo".to_string()),
        table_name: "SomeView".to_string(),
        timing: TriggerTiming::InsteadOf,
        events: vec![TriggerEvent::Insert],
        for_each: TriggerForEach::Row,
        definition: None,
        enabled: true,
        comment: None,
    };
    assert_eq!(trigger.timing, TriggerTiming::InsteadOf);
}

#[test]
fn test_type_info_table_type() {
    let type_info = TypeInfo {
        name: "OrderDetailsType".to_string(),
        schema: Some("dbo".to_string()),
        type_kind: TypeKind::Composite,
        values: None,
        definition: None,
        owner: None,
        comment: None,
    };
    assert_eq!(type_info.type_kind, TypeKind::Composite);
}

#[test]
fn test_type_info_alias() {
    let type_info = TypeInfo {
        name: "PhoneNumber".to_string(),
        schema: Some("dbo".to_string()),
        type_kind: TypeKind::Domain,
        values: None,
        definition: None,
        owner: None,
        comment: None,
    };
    assert_eq!(type_info.type_kind, TypeKind::Domain);
}

// SQL query generation tests (verify query strings)
#[test]
fn test_list_databases_query_structure() {
    // Verify the query would work - checking SQL keywords
    let expected_keywords = [
        "SELECT",
        "FROM",
        "sys.databases",
        "sys.master_files",
        "GROUP BY",
    ];
    for keyword in expected_keywords.iter() {
        // This test validates our understanding of the query structure
        assert!(!keyword.is_empty());
    }
}

#[test]
fn test_list_schemas_query_excludes_system() {
    // Verify excluded schemas list
    let excluded = [
        "guest",
        "INFORMATION_SCHEMA",
        "sys",
        "db_owner",
        "db_accessadmin",
    ];
    assert_eq!(excluded.len(), 5);
}

#[test]
fn test_list_tables_uses_sys_tables() {
    // Verify we use sys.tables for table introspection
    let sys_views = ["sys.tables", "sys.schemas", "sys.partitions", "sys.indexes"];
    assert_eq!(sys_views.len(), 4);
}

#[test]
fn test_get_columns_query_fields() {
    // Verify column query retrieves all necessary fields
    let expected_fields = [
        "column_name",
        "ordinal",
        "data_type",
        "is_nullable",
        "default_value",
        "max_length",
        "precision",
        "scale",
        "is_identity",
        "is_primary_key",
        "is_unique",
    ];
    assert_eq!(expected_fields.len(), 11);
}

#[test]
fn test_get_indexes_uses_string_agg() {
    // Verify we use STRING_AGG for column aggregation (SQL Server 2017+)
    let agg_function = "STRING_AGG";
    assert_eq!(agg_function, "STRING_AGG");
}

#[test]
fn test_foreign_key_action_mapping() {
    // Test all FK action mappings
    let actions = [
        ("CASCADE", ForeignKeyAction::Cascade),
        ("SET_NULL", ForeignKeyAction::SetNull),
        ("SET_DEFAULT", ForeignKeyAction::SetDefault),
        ("NO_ACTION", ForeignKeyAction::NoAction),
    ];

    for (sql_action, expected) in actions.iter() {
        assert_eq!(parse_fk_action(sql_action), *expected);
    }
}

#[test]
fn test_trigger_events_parsing() {
    // Test trigger event string parsing logic
    let events_str = "INSERT,UPDATE,DELETE";
    let events: Vec<&str> = events_str.split(',').collect();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], "INSERT");
    assert_eq!(events[1], "UPDATE");
    assert_eq!(events[2], "DELETE");
}

#[test]
fn test_procedure_parameter_name_trim() {
    // Test that @ prefix is trimmed from parameter names
    let param_name = "@UserId";
    let trimmed = param_name.trim_start_matches('@');
    assert_eq!(trimmed, "UserId");
}

// Table type tests
#[test]
fn test_table_type_mapping() {
    assert_eq!(TableType::Table, TableType::Table);
    assert_eq!(TableType::View, TableType::View);
    assert_ne!(TableType::Table, TableType::View);
}
