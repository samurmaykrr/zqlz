//! Tests for MySQL EXPLAIN parser

use super::*;

// ============================================================================
// JSON Format Tests
// ============================================================================

mod json_parsing {
    use super::*;

    #[test]
    fn test_parse_simple_table_scan() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "cost_info": {
                    "query_cost": "10.50"
                },
                "table": {
                    "table_name": "users",
                    "access_type": "ALL",
                    "rows_examined_per_scan": 100,
                    "rows_produced_per_join": 10,
                    "filtered": "10.00"
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.total_cost, Some(10.50));
        assert_eq!(plan.root.node_type, NodeType::SeqScan);
        assert_eq!(plan.root.relation, Some("users".to_string()));
        assert_eq!(plan.root.rows, Some(100));
        assert_eq!(plan.root.actual_rows, Some(10));
    }

    #[test]
    fn test_parse_index_scan() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "table": {
                    "table_name": "orders",
                    "access_type": "ref",
                    "key": "idx_customer_id",
                    "key_length": "4",
                    "ref": ["const"],
                    "rows_examined_per_scan": 50
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.index_name, Some("idx_customer_id".to_string()));
        assert_eq!(plan.root.rows, Some(50));
    }

    #[test]
    fn test_parse_index_only_scan() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "table": {
                    "table_name": "counts",
                    "access_type": "index",
                    "key": "idx_count",
                    "using_index": true,
                    "rows_examined_per_scan": 1000
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexOnlyScan);
        assert!(plan.root.extra.get("using_index").is_some());
    }

    #[test]
    fn test_parse_eq_ref_lookup() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "table": {
                    "table_name": "users",
                    "access_type": "eq_ref",
                    "key": "PRIMARY",
                    "key_length": "4",
                    "ref": ["orders.user_id"],
                    "rows_examined_per_scan": 1
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.index_name, Some("PRIMARY".to_string()));
        assert_eq!(plan.root.rows, Some(1));
    }

    #[test]
    fn test_parse_with_filter() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "table": {
                    "table_name": "products",
                    "access_type": "ALL",
                    "attached_condition": "(products.price > 100)"
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.filter, Some("(products.price > 100)".to_string()));
    }
}

mod json_nested_loop {
    use super::*;

    #[test]
    fn test_parse_simple_join() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "nested_loop": [
                    {
                        "table": {
                            "table_name": "users",
                            "access_type": "ALL",
                            "rows_examined_per_scan": 100
                        }
                    },
                    {
                        "table": {
                            "table_name": "orders",
                            "access_type": "ref",
                            "key": "idx_user_id",
                            "rows_examined_per_scan": 10
                        }
                    }
                ]
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::NestedLoop);
        assert_eq!(plan.root.join_type, Some(JoinType::Inner));
        assert_eq!(plan.root.children.len(), 2);
        assert_eq!(plan.root.children[0].relation, Some("users".to_string()));
        assert_eq!(plan.root.children[1].relation, Some("orders".to_string()));
    }

    #[test]
    fn test_parse_three_table_join() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "nested_loop": [
                    {"table": {"table_name": "a", "access_type": "ALL"}},
                    {"table": {"table_name": "b", "access_type": "ref", "key": "idx_a"}},
                    {"table": {"table_name": "c", "access_type": "ref", "key": "idx_b"}}
                ]
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        // Should be: NestedLoop(NestedLoop(a, b), c)
        assert_eq!(plan.root.node_type, NodeType::NestedLoop);
        assert_eq!(plan.root.children.len(), 2);
        assert_eq!(plan.root.children[0].node_type, NodeType::NestedLoop);
        assert_eq!(plan.root.children[1].relation, Some("c".to_string()));
    }
}

mod json_operations {
    use super::*;

    #[test]
    fn test_parse_ordering_operation() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "ordering_operation": {
                    "using_filesort": true,
                    "table": {
                        "table_name": "users",
                        "access_type": "ALL",
                        "rows_examined_per_scan": 100
                    }
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Sort);
        assert_eq!(plan.root.sort_method, Some("filesort".to_string()));
        assert_eq!(plan.root.children.len(), 1);
        assert_eq!(plan.root.children[0].relation, Some("users".to_string()));
    }

    #[test]
    fn test_parse_grouping_operation() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "grouping_operation": {
                    "using_temporary_table": true,
                    "using_filesort": true,
                    "table": {
                        "table_name": "orders",
                        "access_type": "ALL"
                    }
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::HashAggregate);
        assert!(plan.root.extra.get("using_filesort").is_some());
        assert_eq!(plan.root.children.len(), 1);
    }

    #[test]
    fn test_parse_duplicates_removal() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "duplicates_removal": {
                    "table": {
                        "table_name": "tags",
                        "access_type": "index",
                        "key": "idx_name"
                    }
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Unique);
        assert_eq!(plan.root.children.len(), 1);
    }

    #[test]
    fn test_parse_union_result() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "union_result": {
                    "table_name": "<union1,2>",
                    "using_temporary_table": true,
                    "query_specifications": [
                        {"query_block": {"select_id": 1, "table": {"table_name": "t1", "access_type": "ALL"}}},
                        {"query_block": {"select_id": 2, "table": {"table_name": "t2", "access_type": "ALL"}}}
                    ]
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Append);
        assert_eq!(plan.root.children.len(), 2);
    }
}

// ============================================================================
// Tabular Format Tests
// ============================================================================

mod tabular_parsing {
    use super::*;

    #[test]
    fn test_parse_simple_tabular() {
        let tabular = "1\tSIMPLE\tusers\tALL\tNULL\tNULL\tNULL\tNULL\t100\t100.00\tUsing where";

        let plan = parse_mysql_explain(tabular).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SeqScan);
        assert_eq!(plan.root.relation, Some("users".to_string()));
        assert_eq!(plan.root.rows, Some(100));
    }

    #[test]
    fn test_parse_tabular_with_index() {
        let tabular =
            "1\tSIMPLE\torders\tref\tidx_user\tidx_user\t4\tconst\t10\t100.00\tUsing index";

        let plan = parse_mysql_explain(tabular).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexOnlyScan); // Upgraded due to "Using index"
        assert_eq!(plan.root.index_name, Some("idx_user".to_string()));
    }

    #[test]
    fn test_parse_tabular_multiple_rows() {
        let tabular = "1\tSIMPLE\tusers\tALL\tNULL\tNULL\tNULL\tNULL\t100\t100.00\tNULL
1\tSIMPLE\torders\tref\tidx_user\tidx_user\t4\tusers.id\t10\t100.00\tUsing where";

        let plan = parse_mysql_explain(tabular).unwrap();
        // Should create a nested loop join
        assert_eq!(plan.root.node_type, NodeType::NestedLoop);
        assert_eq!(plan.root.children.len(), 2);
    }

    #[test]
    fn test_parse_tabular_with_header() {
        let tabular =
            "id\tselect_type\ttable\ttype\tpossible_keys\tkey\tkey_len\tref\trows\tfiltered\tExtra
1\tSIMPLE\tusers\tALL\tNULL\tNULL\tNULL\tNULL\t100\t100.00\tUsing where";

        let plan = parse_mysql_explain(tabular).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SeqScan);
        assert_eq!(plan.root.relation, Some("users".to_string()));
    }

    #[test]
    fn test_parse_tabular_const_access() {
        let tabular = "1\tSIMPLE\tusers\tconst\tPRIMARY\tPRIMARY\t4\tconst\t1\t100.00\tNULL";

        let plan = parse_mysql_explain(tabular).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.rows, Some(1));
    }
}

// ============================================================================
// Access Type Mapping Tests
// ============================================================================

mod access_type_mapping {
    use super::*;

    #[test]
    fn test_all_access_type() {
        assert_eq!(mysql_access_type_to_node_type("ALL"), NodeType::SeqScan);
        assert_eq!(mysql_access_type_to_node_type("all"), NodeType::SeqScan);
    }

    #[test]
    fn test_index_access_type() {
        assert_eq!(mysql_access_type_to_node_type("index"), NodeType::IndexScan);
    }

    #[test]
    fn test_range_access_type() {
        assert_eq!(mysql_access_type_to_node_type("range"), NodeType::IndexScan);
    }

    #[test]
    fn test_ref_access_types() {
        assert_eq!(mysql_access_type_to_node_type("ref"), NodeType::IndexScan);
        assert_eq!(
            mysql_access_type_to_node_type("eq_ref"),
            NodeType::IndexScan
        );
        assert_eq!(
            mysql_access_type_to_node_type("ref_or_null"),
            NodeType::IndexScan
        );
    }

    #[test]
    fn test_const_access_type() {
        assert_eq!(mysql_access_type_to_node_type("const"), NodeType::IndexScan);
        assert_eq!(
            mysql_access_type_to_node_type("system"),
            NodeType::IndexScan
        );
    }

    #[test]
    fn test_index_merge_access_type() {
        assert_eq!(
            mysql_access_type_to_node_type("index_merge"),
            NodeType::BitmapIndexScan
        );
    }

    #[test]
    fn test_unknown_access_type() {
        assert_eq!(
            mysql_access_type_to_node_type("unknown_type"),
            NodeType::Unknown
        );
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling {
    use super::*;

    #[test]
    fn test_empty_input() {
        let result = parse_mysql_explain("");
        assert!(matches!(result, Err(MysqlExplainError::EmptyOutput)));
    }

    #[test]
    fn test_whitespace_only() {
        let result = parse_mysql_explain("   \n\t  ");
        assert!(matches!(result, Err(MysqlExplainError::EmptyOutput)));
    }

    #[test]
    fn test_invalid_json() {
        let result = parse_mysql_explain("{invalid json}");
        assert!(matches!(result, Err(MysqlExplainError::InvalidJson(_))));
    }

    #[test]
    fn test_missing_query_block() {
        let result = parse_mysql_explain(r#"{"foo": "bar"}"#);
        assert!(matches!(result, Err(MysqlExplainError::MissingQueryBlock)));
    }

    #[test]
    fn test_empty_nested_loop() {
        let json = r#"{"query_block": {"select_id": 1, "nested_loop": []}}"#;
        let result = parse_mysql_explain(json);
        assert!(matches!(
            result,
            Err(MysqlExplainError::InvalidStructure(_))
        ));
    }
}

// ============================================================================
// Integration Tests (complex scenarios)
// ============================================================================

mod integration {
    use super::*;

    #[test]
    fn test_complex_query_with_sort_and_join() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "cost_info": {"query_cost": "25.50"},
                "ordering_operation": {
                    "using_filesort": true,
                    "nested_loop": [
                        {
                            "table": {
                                "table_name": "orders",
                                "access_type": "range",
                                "key": "idx_date",
                                "rows_examined_per_scan": 100,
                                "attached_condition": "orders.date > '2024-01-01'"
                            }
                        },
                        {
                            "table": {
                                "table_name": "users",
                                "access_type": "eq_ref",
                                "key": "PRIMARY",
                                "rows_examined_per_scan": 1
                            }
                        }
                    ]
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert_eq!(plan.total_cost, Some(25.50));
        assert_eq!(plan.root.node_type, NodeType::Sort);
        assert_eq!(plan.root.sort_method, Some("filesort".to_string()));

        // Check nested loop child
        assert_eq!(plan.root.children.len(), 1);
        let join = &plan.root.children[0];
        assert_eq!(join.node_type, NodeType::NestedLoop);
        assert_eq!(join.children.len(), 2);

        // Check filter on first table
        assert_eq!(
            join.children[0].filter,
            Some("orders.date > '2024-01-01'".to_string())
        );
    }

    #[test]
    fn test_has_sequential_scans() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "nested_loop": [
                    {"table": {"table_name": "t1", "access_type": "ALL"}},
                    {"table": {"table_name": "t2", "access_type": "ref", "key": "idx"}}
                ]
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert!(plan.has_sequential_scans());
    }

    #[test]
    fn test_no_sequential_scans() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "table": {
                    "table_name": "users",
                    "access_type": "const",
                    "key": "PRIMARY"
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        assert!(!plan.has_sequential_scans());
    }

    #[test]
    fn test_iter_nodes() {
        let json = r#"{
            "query_block": {
                "select_id": 1,
                "ordering_operation": {
                    "using_filesort": true,
                    "nested_loop": [
                        {"table": {"table_name": "a", "access_type": "ALL"}},
                        {"table": {"table_name": "b", "access_type": "ref", "key": "idx"}}
                    ]
                }
            }
        }"#;

        let plan = parse_mysql_explain(json).unwrap();
        let node_count = plan.iter_nodes().count();
        // Sort -> NestedLoop -> a, b = 4 nodes
        assert_eq!(node_count, 4);
    }
}
