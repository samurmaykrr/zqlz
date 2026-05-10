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
