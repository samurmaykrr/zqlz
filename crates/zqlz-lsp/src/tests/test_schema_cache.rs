//! Tests for schema cache functionality

use super::test_helpers::*;

#[test]
fn test_schema_cache_has_tables() {
    let lsp = create_test_lsp();

    assert_eq!(lsp.schema_cache.tables.len(), 3, "Should have 3 tables");
    assert!(lsp.schema_cache.tables.contains_key("users"));
    assert!(lsp.schema_cache.tables.contains_key("audit_log"));
    assert!(lsp.schema_cache.tables.contains_key("locations"));
}

#[test]
fn test_schema_cache_has_columns() {
    let lsp = create_test_lsp();

    assert_eq!(
        lsp.schema_cache.columns_by_table.len(),
        3,
        "Should have columns for 3 tables"
    );

    // Check users table columns
    let users_cols = lsp.schema_cache.columns_by_table.get("users").unwrap();
    assert_eq!(users_cols.len(), 3, "users table should have 3 columns");
    assert!(users_cols.iter().any(|c| c.name == "user_id"));
    assert!(users_cols.iter().any(|c| c.name == "username"));
    assert!(users_cols.iter().any(|c| c.name == "email"));

    // Check audit_log table columns
    let audit_cols = lsp.schema_cache.columns_by_table.get("audit_log").unwrap();
    assert_eq!(audit_cols.len(), 3, "audit_log table should have 3 columns");
    assert!(audit_cols.iter().any(|c| c.name == "log_id"));
    assert!(audit_cols.iter().any(|c| c.name == "log_timestamp"));
    assert!(audit_cols.iter().any(|c| c.name == "action"));

    // Check locations table columns
    let location_cols = lsp.schema_cache.columns_by_table.get("locations").unwrap();
    assert_eq!(
        location_cols.len(),
        2,
        "locations table should have 2 columns"
    );
    assert!(location_cols.iter().any(|c| c.name == "location_id"));
    assert!(location_cols.iter().any(|c| c.name == "location_name"));
}

#[test]
fn schema_metadata_preserves_resolved_database_and_schema_labels() {
    let mut lsp = create_test_lsp();
    lsp.schema_cache.database_name = Some("erp_lab".to_string());
    lsp.schema_cache.schema_name = Some("zqlz_audit".to_string());
    lsp.schema_cache.schema_names = vec![
        "public".to_string(),
        "zqlz_audit".to_string(),
        "crm".to_string(),
    ];

    let metadata = lsp.get_schema_for_metadata();

    assert_eq!(metadata.database_name.as_deref(), Some("erp_lab"));
    assert_eq!(metadata.schema_name.as_deref(), Some("zqlz_audit"));
    assert_eq!(
        metadata.schema_names,
        vec![
            "public".to_string(),
            "zqlz_audit".to_string(),
            "crm".to_string()
        ]
    );
}

fn table_details(name: &str, columns: &[&str]) -> zqlz_services::TableDetails {
    zqlz_services::TableDetails {
        name: name.to_string(),
        table_type: zqlz_core::TableType::Table,
        columns: columns
            .iter()
            .map(|column| zqlz_services::ColumnInfo {
                name: (*column).to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                is_auto_increment: false,
                comment: None,
                enum_values: None,
            })
            .collect(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
        primary_key_columns: Vec::new(),
        row_count: None,
    }
}

#[test]
fn test_merge_table_details_batch_populates_every_table() {
    let mut lsp = create_test_lsp();
    let batch = std::collections::HashMap::from([
        ("invoices".to_string(), table_details("invoices", &["total"])),
        ("payments".to_string(), table_details("payments", &["paid"])),
    ]);

    lsp.merge_table_details_batch(&batch);

    for (table, column) in [("invoices", "total"), ("payments", "paid")] {
        let columns = lsp
            .schema_cache
            .columns_by_table
            .get(table)
            .unwrap_or_else(|| panic!("{table} should be cached"));
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, column);
    }
}

#[test]
fn test_merge_table_details_batch_replaces_rather_than_appends() {
    let mut lsp = create_test_lsp();
    let first = std::collections::HashMap::from([(
        "invoices".to_string(),
        table_details("invoices", &["total", "dropped_later"]),
    )]);
    let second = std::collections::HashMap::from([(
        "invoices".to_string(),
        table_details("invoices", &["total"]),
    )]);

    lsp.merge_table_details_batch(&first);
    lsp.merge_table_details_batch(&second);

    let columns = lsp
        .schema_cache
        .columns_by_table
        .get("invoices")
        .expect("invoices should be cached");
    assert_eq!(
        columns.len(),
        1,
        "a re-merge must replace the previous column set"
    );

    let invoice_column_objects = lsp
        .schema_cache
        .objects
        .iter()
        .filter(|object| match object {
            crate::DatabaseObject::Column(column) => column.table_name == "invoices",
            _ => false,
        })
        .count();
    assert_eq!(
        invoice_column_objects, 1,
        "dropped columns must not linger in the flat object list"
    );
}

#[test]
fn test_merge_table_details_batch_leaves_other_tables_alone() {
    let mut lsp = create_test_lsp();
    let before = lsp
        .schema_cache
        .columns_by_table
        .get("users")
        .expect("fixture has users")
        .len();

    let batch = std::collections::HashMap::from([(
        "invoices".to_string(),
        table_details("invoices", &["total"]),
    )]);
    lsp.merge_table_details_batch(&batch);

    assert_eq!(
        lsp.schema_cache
            .columns_by_table
            .get("users")
            .expect("users should survive")
            .len(),
        before
    );
}

#[test]
fn test_merge_table_columns_batch_matches_merge_table_details_batch() {
    // The connect-time warm-up carries only columns + foreign keys, so it must
    // land in the cache identically to a full-details merge — including the
    // `is_foreign_key` flag, which is derived from the FK list rather than read
    // off the column.
    let details = {
        let mut details = table_details("orders", &["id", "customer_id"]);
        details.foreign_keys = vec![zqlz_core::ForeignKeyInfo {
            name: "fk_orders_customers".to_string(),
            columns: vec!["customer_id".to_string()],
            referenced_table: "customers".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_update: zqlz_core::ForeignKeyAction::NoAction,
            on_delete: zqlz_core::ForeignKeyAction::Cascade,
            is_deferrable: false,
            initially_deferred: false,
        }];
        details
    };

    let mut via_details = create_test_lsp();
    via_details.merge_table_details_batch(&std::collections::HashMap::from([(
        "orders".to_string(),
        details.clone(),
    )]));

    let mut via_columns = create_test_lsp();
    via_columns.merge_table_columns_batch(&std::collections::HashMap::from([(
        "orders".to_string(),
        zqlz_services::TableColumnSummary {
            columns: details.columns.clone(),
            foreign_keys: details.foreign_keys.clone(),
        },
    )]));

    let detail_columns = via_details
        .schema_cache
        .columns_by_table
        .get("orders")
        .expect("details merge populates orders");
    let column_columns = via_columns
        .schema_cache
        .columns_by_table
        .get("orders")
        .expect("columns merge populates orders");

    let projection = |columns: &Vec<crate::ColumnInfo>| {
        columns
            .iter()
            .map(|column| (column.name.clone(), column.is_foreign_key))
            .collect::<Vec<_>>()
    };
    assert_eq!(projection(detail_columns), projection(column_columns));
    assert!(
        projection(column_columns).contains(&("customer_id".to_string(), true)),
        "the FK column must be flagged: {:?}",
        projection(column_columns)
    );

    assert_eq!(
        via_details.schema_cache.foreign_keys_by_table.get("orders").map(Vec::len),
        via_columns.schema_cache.foreign_keys_by_table.get("orders").map(Vec::len),
    );
    assert_eq!(
        via_details.schema_cache.reverse_foreign_keys.get("customers").map(Vec::len),
        via_columns.schema_cache.reverse_foreign_keys.get("customers").map(Vec::len),
    );
}
