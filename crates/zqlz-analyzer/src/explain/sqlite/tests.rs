//! Tests for SQLite EXPLAIN QUERY PLAN parser

use super::*;
use crate::explain::plan::NodeType;

mod tree_format_tests {
    use super::*;

    #[test]
    fn test_parse_simple_scan() {
        let output = r#"QUERY PLAN
|--SCAN users"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SeqScan);
        assert_eq!(plan.root.relation.as_deref(), Some("users"));
    }

    #[test]
    fn test_parse_search_with_index() {
        let output = r#"QUERY PLAN
|--SEARCH orders USING INDEX idx_user_id (user_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.relation.as_deref(), Some("orders"));
        assert_eq!(plan.root.index_name.as_deref(), Some("idx_user_id"));
        assert_eq!(plan.root.index_cond.as_deref(), Some("user_id=?"));
    }

    #[test]
    fn test_parse_covering_index_scan() {
        let output = r#"QUERY PLAN
`--SCAN items USING COVERING INDEX idx_all"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexOnlyScan);
        assert_eq!(plan.root.relation.as_deref(), Some("items"));
        assert_eq!(plan.root.index_name.as_deref(), Some("idx_all"));
    }

    #[test]
    fn test_parse_multiple_scans_creates_join_tree() {
        let output = r#"QUERY PLAN
|--SCAN users
|--SEARCH orders USING INDEX idx_user_id (user_id=?)
`--SCAN items"#;

        let plan = parse_sqlite_explain(output).unwrap();
        // Should create join tree
        assert_eq!(plan.root.node_type, NodeType::Append);
        assert_eq!(plan.root.children.len(), 3);
    }

    #[test]
    fn test_parse_order_by_temp_btree() {
        let output = r#"QUERY PLAN
|--SCAN users
`--USE TEMP B-TREE FOR ORDER BY"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Append);
        // First child is scan, second is sort
        assert_eq!(plan.root.children[0].node_type, NodeType::SeqScan);
        assert_eq!(plan.root.children[1].node_type, NodeType::Sort);
    }

    #[test]
    fn test_parse_distinct_temp_btree() {
        let output = r#"QUERY PLAN
|--SCAN users
`--USE TEMP B-TREE FOR DISTINCT"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert!(plan.iter_nodes().any(|n| n.node_type == NodeType::Unique));
    }

    #[test]
    fn test_parse_group_by_temp_btree() {
        let output = r#"QUERY PLAN
|--SCAN orders
`--USE TEMP B-TREE FOR GROUP BY"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert!(
            plan.iter_nodes()
                .any(|n| n.node_type == NodeType::HashAggregate)
        );
    }

    #[test]
    fn test_parse_primary_key_search() {
        let output = r#"QUERY PLAN
`--SEARCH users USING INTEGER PRIMARY KEY (rowid=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.index_name.as_deref(), Some("PRIMARY KEY"));
    }

    #[test]
    fn test_parse_covering_index_search() {
        let output = r#"QUERY PLAN
`--SEARCH orders USING COVERING INDEX idx_user_status (user_id=? AND status=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexOnlyScan);
        assert_eq!(plan.root.index_name.as_deref(), Some("idx_user_status"));
        assert_eq!(
            plan.root.index_cond.as_deref(),
            Some("user_id=? AND status=?")
        );
    }

    #[test]
    fn test_parse_without_query_plan_header() {
        let output = r#"|--SCAN users
`--SEARCH orders USING INDEX idx_user (user_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Append);
        assert_eq!(plan.root.children.len(), 2);
    }
}

mod tabular_format_tests {
    use super::*;

    #[test]
    fn test_parse_simple_tabular() {
        let output = "0|0|0|SCAN users";

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SeqScan);
        assert_eq!(plan.root.relation.as_deref(), Some("users"));
    }

    #[test]
    fn test_parse_tabular_with_index() {
        let output = "0|0|0|SEARCH orders USING INDEX idx_user_id (user_id=?)";

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexScan);
        assert_eq!(plan.root.index_name.as_deref(), Some("idx_user_id"));
    }

    #[test]
    fn test_parse_multiple_tabular_rows() {
        let output = r#"0|0|0|SCAN users
0|1|1|SEARCH orders USING INDEX idx_user (user_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        // Multiple rows create a nested loop join
        assert_eq!(plan.root.node_type, NodeType::NestedLoop);
        assert_eq!(plan.root.children.len(), 2);
        assert_eq!(plan.root.children[0].node_type, NodeType::SeqScan);
        assert_eq!(plan.root.children[1].node_type, NodeType::IndexScan);
    }

    #[test]
    fn test_parse_tabular_with_covering_index() {
        let output = "0|0|0|SCAN items USING COVERING INDEX idx_all";

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::IndexOnlyScan);
    }
}

mod subquery_tests {
    use super::*;

    #[test]
    fn test_parse_correlated_subquery() {
        let output = r#"QUERY PLAN
|--SCAN users
`--CORRELATED SCALAR SUBQUERY 2"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_subquery = plan
            .iter_nodes()
            .any(|n| n.node_type == NodeType::SubqueryScan);
        assert!(has_subquery);
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let output = r#"QUERY PLAN
|--SCAN orders
`--SCALAR SUBQUERY 1"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_subquery = plan
            .iter_nodes()
            .any(|n| n.node_type == NodeType::SubqueryScan);
        assert!(has_subquery);
    }

    #[test]
    fn test_parse_list_subquery() {
        let output = r#"QUERY PLAN
|--SCAN users
`--LIST SUBQUERY 1"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_subquery = plan
            .iter_nodes()
            .any(|n| n.node_type == NodeType::SubqueryScan);
        assert!(has_subquery);
    }
}

mod set_operation_tests {
    use super::*;

    #[test]
    fn test_parse_union_all() {
        let output = r#"QUERY PLAN
`--UNION ALL"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Append);
    }

    #[test]
    fn test_parse_union_distinct() {
        let output = r#"QUERY PLAN
`--UNION"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SetOp);
    }

    #[test]
    fn test_parse_compound_subqueries() {
        let output = r#"QUERY PLAN
`--COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::SetOp);
    }
}

mod cte_tests {
    use super::*;

    #[test]
    fn test_parse_co_routine() {
        let output = r#"QUERY PLAN
|--CO-ROUTINE temp_table
`--SCAN temp_table"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_cte = plan.iter_nodes().any(|n| n.node_type == NodeType::CteScan);
        assert!(has_cte);
    }

    #[test]
    fn test_parse_materialize() {
        let output = r#"QUERY PLAN
`--MATERIALIZE expensive_cte"#;

        let plan = parse_sqlite_explain(output).unwrap();
        assert_eq!(plan.root.node_type, NodeType::Materialize);
    }
}

mod join_tests {
    use super::*;

    #[test]
    fn test_parse_left_join() {
        let output = r#"QUERY PLAN
|--SCAN users
`--LEFT-JOIN orders"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_left_join = plan
            .iter_nodes()
            .any(|n| n.node_type == NodeType::NestedLoop && n.join_type == Some(JoinType::Left));
        assert!(has_left_join);
    }

    #[test]
    fn test_parse_right_join() {
        let output = r#"QUERY PLAN
|--SCAN users
`--RIGHT-JOIN orders"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let has_right_join = plan
            .iter_nodes()
            .any(|n| n.node_type == NodeType::NestedLoop && n.join_type == Some(JoinType::Right));
        assert!(has_right_join);
    }
}

mod error_handling_tests {
    use super::*;

    #[test]
    fn test_empty_output_error() {
        let result = parse_sqlite_explain("");
        assert!(matches!(result, Err(SqliteExplainError::EmptyOutput)));
    }

    #[test]
    fn test_whitespace_only_error() {
        let result = parse_sqlite_explain("   \n\t  ");
        assert!(matches!(result, Err(SqliteExplainError::EmptyOutput)));
    }

    #[test]
    fn test_query_plan_header_only() {
        let output = "QUERY PLAN";
        let result = parse_sqlite_explain(output);
        // Should succeed with empty Result node
        assert!(result.is_ok());
        assert_eq!(result.unwrap().root.node_type, NodeType::Result);
    }
}

mod helper_function_tests {
    use super::*;

    #[test]
    fn test_extract_table_name_simple() {
        let detail = "SCAN users";
        let table = extract_table_name(detail, "SCAN");
        assert_eq!(table.as_deref(), Some("users"));
    }

    #[test]
    fn test_extract_table_name_with_keyword() {
        let detail = "SCAN TABLE users USING INDEX idx";
        let table = extract_table_name(detail, "SCAN");
        assert_eq!(table.as_deref(), Some("users"));
    }

    #[test]
    fn test_extract_table_name_from_search() {
        let detail = "SEARCH orders USING INDEX idx_user (user_id=?)";
        let table = extract_table_name(detail, "SEARCH");
        assert_eq!(table.as_deref(), Some("orders"));
    }

    #[test]
    fn test_extract_index_name() {
        let detail = "SEARCH users USING INDEX idx_email (email=?)";
        let idx = extract_index_name(detail, "INDEX");
        assert_eq!(idx.as_deref(), Some("idx_email"));
    }

    #[test]
    fn test_extract_index_name_covering() {
        let detail = "SCAN items USING COVERING INDEX idx_all";
        let idx = extract_index_name(detail, "COVERING INDEX");
        assert_eq!(idx.as_deref(), Some("idx_all"));
    }

    #[test]
    fn test_extract_index_condition() {
        let detail = "SEARCH users USING INDEX idx_status (status=? AND active=?)";
        let cond = extract_index_condition(detail);
        assert_eq!(cond.as_deref(), Some("status=? AND active=?"));
    }

    #[test]
    fn test_extract_index_condition_none() {
        let detail = "SCAN users";
        let cond = extract_index_condition(detail);
        assert!(cond.is_none());
    }
}

mod operation_mapping_tests {
    use super::*;

    #[test]
    fn test_scan_to_seq_scan() {
        assert_eq!(
            sqlite_operation_to_node_type("SCAN users"),
            NodeType::SeqScan
        );
    }

    #[test]
    fn test_search_to_index_scan() {
        assert_eq!(
            sqlite_operation_to_node_type("SEARCH users USING INDEX idx"),
            NodeType::IndexScan
        );
    }

    #[test]
    fn test_covering_index_to_index_only_scan() {
        assert_eq!(
            sqlite_operation_to_node_type("SCAN users USING COVERING INDEX idx"),
            NodeType::IndexOnlyScan
        );
        assert_eq!(
            sqlite_operation_to_node_type("SEARCH users USING COVERING INDEX idx"),
            NodeType::IndexOnlyScan
        );
    }

    #[test]
    fn test_order_by_to_sort() {
        assert_eq!(
            sqlite_operation_to_node_type("USE TEMP B-TREE FOR ORDER BY"),
            NodeType::Sort
        );
    }

    #[test]
    fn test_distinct_to_unique() {
        assert_eq!(
            sqlite_operation_to_node_type("USE TEMP B-TREE FOR DISTINCT"),
            NodeType::Unique
        );
    }

    #[test]
    fn test_group_by_to_hash_aggregate() {
        assert_eq!(
            sqlite_operation_to_node_type("USE TEMP B-TREE FOR GROUP BY"),
            NodeType::HashAggregate
        );
    }

    #[test]
    fn test_union_all_to_append() {
        assert_eq!(sqlite_operation_to_node_type("UNION ALL"), NodeType::Append);
    }

    #[test]
    fn test_union_to_set_op() {
        assert_eq!(sqlite_operation_to_node_type("UNION"), NodeType::SetOp);
    }

    #[test]
    fn test_co_routine_to_cte_scan() {
        assert_eq!(
            sqlite_operation_to_node_type("CO-ROUTINE my_cte"),
            NodeType::CteScan
        );
    }

    #[test]
    fn test_subquery_to_subquery_scan() {
        assert_eq!(
            sqlite_operation_to_node_type("CORRELATED SCALAR SUBQUERY 1"),
            NodeType::SubqueryScan
        );
    }

    #[test]
    fn test_materialize_to_materialize() {
        assert_eq!(
            sqlite_operation_to_node_type("MATERIALIZE cte"),
            NodeType::Materialize
        );
    }

    #[test]
    fn test_bloom_filter_to_hash() {
        assert_eq!(
            sqlite_operation_to_node_type("BLOOM FILTER ON orders"),
            NodeType::Hash
        );
    }

    #[test]
    fn test_auto_index() {
        assert_eq!(
            sqlite_operation_to_node_type("SEARCH users USING AUTOMATIC COVERING INDEX"),
            NodeType::IndexOnlyScan
        );
        assert_eq!(
            sqlite_operation_to_node_type("SEARCH users USING AUTO-INDEX"),
            NodeType::IndexOnlyScan
        );
    }
}

mod integration_tests {
    use super::*;

    #[test]
    fn test_complex_query_plan() {
        let output = r#"QUERY PLAN
|--SCAN users
|--SEARCH orders USING INDEX idx_user_id (user_id=?)
|--SEARCH items USING COVERING INDEX idx_order_item (order_id=?)
`--USE TEMP B-TREE FOR ORDER BY"#;

        let plan = parse_sqlite_explain(output).unwrap();

        // Should have sequential scan
        assert!(plan.has_sequential_scans());

        // Should have multiple nodes
        let node_count = plan.iter_nodes().count();
        assert!(node_count >= 4);

        // Should have sort operation
        assert!(plan.iter_nodes().any(|n| n.node_type == NodeType::Sort));
    }

    #[test]
    fn test_query_plan_with_subquery() {
        let output = r#"QUERY PLAN
|--SCAN users
|--CORRELATED SCALAR SUBQUERY 1
|  |--SEARCH orders USING INDEX idx_user (user_id=?)
|  `--SEARCH items USING INDEX idx_order (order_id=?)
`--USE TEMP B-TREE FOR GROUP BY"#;

        let plan = parse_sqlite_explain(output).unwrap();

        // Should have subquery
        assert!(
            plan.iter_nodes()
                .any(|n| n.node_type == NodeType::SubqueryScan)
        );

        // Should have aggregate
        assert!(
            plan.iter_nodes()
                .any(|n| n.node_type == NodeType::HashAggregate)
        );
    }

    #[test]
    fn test_iter_nodes_returns_all() {
        let output = r#"QUERY PLAN
|--SCAN users
`--SEARCH orders USING INDEX idx_user (user_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let nodes: Vec<_> = plan.iter_nodes().collect();

        // Should find at least the root and both operations
        assert!(nodes.len() >= 2);
    }

    #[test]
    fn test_find_nodes_by_type() {
        let output = r#"QUERY PLAN
|--SCAN users
|--SCAN products
`--SEARCH orders USING INDEX idx_user (user_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();
        let scans = plan.find_nodes_by_type(NodeType::SeqScan);

        assert_eq!(scans.len(), 2);
    }
}

mod real_world_tests {
    use super::*;

    #[test]
    fn test_ecommerce_query() {
        // Simulates: SELECT * FROM users u JOIN orders o ON u.id = o.user_id
        //            JOIN items i ON o.id = i.order_id WHERE u.status = 'active'
        let output = r#"QUERY PLAN
|--SCAN users
|--SEARCH orders USING INDEX idx_orders_user (user_id=?)
`--SEARCH items USING INDEX idx_items_order (order_id=?)"#;

        let plan = parse_sqlite_explain(output).unwrap();

        // Verify the plan structure
        assert!(plan.has_sequential_scans());

        let index_scans: Vec<_> = plan
            .iter_nodes()
            .filter(|n| n.node_type == NodeType::IndexScan)
            .collect();
        assert_eq!(index_scans.len(), 2);
    }

    #[test]
    fn test_aggregation_query() {
        // Simulates: SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*)
        let output = r#"QUERY PLAN
|--SCAN orders
|--USE TEMP B-TREE FOR GROUP BY
`--USE TEMP B-TREE FOR ORDER BY"#;

        let plan = parse_sqlite_explain(output).unwrap();

        assert!(
            plan.iter_nodes()
                .any(|n| n.node_type == NodeType::HashAggregate)
        );
        assert!(plan.iter_nodes().any(|n| n.node_type == NodeType::Sort));
    }

    #[test]
    fn test_union_query() {
        // Simulates: SELECT * FROM active_users UNION ALL SELECT * FROM archived_users
        let output = r#"QUERY PLAN
|--SCAN active_users
|--UNION ALL
`--SCAN archived_users"#;

        let plan = parse_sqlite_explain(output).unwrap();

        // Should have union operation
        assert!(plan.iter_nodes().any(|n| n.node_type == NodeType::Append));
    }

    #[test]
    fn test_cte_query() {
        // Simulates: WITH recent AS (...) SELECT * FROM recent
        let output = r#"QUERY PLAN
|--CO-ROUTINE recent
|  `--SCAN orders
`--SCAN recent"#;

        let plan = parse_sqlite_explain(output).unwrap();

        // Should have CTE scan
        assert!(plan.iter_nodes().any(|n| n.node_type == NodeType::CteScan));
    }
}
