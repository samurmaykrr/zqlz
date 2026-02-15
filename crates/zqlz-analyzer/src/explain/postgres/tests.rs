//! Tests for the PostgreSQL EXPLAIN parser

use super::*;
use pretty_assertions::assert_eq;

// ============================================================================
// JSON Format Tests
// ============================================================================

#[test]
fn test_parse_simple_seq_scan_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Alias": "users",
                "Startup Cost": 0.00,
                "Total Cost": 10.50,
                "Plan Rows": 100,
                "Plan Width": 36
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::SeqScan);
    assert_eq!(plan.root.relation, Some("users".to_string()));
    assert_eq!(plan.root.alias, Some("users".to_string()));
    assert_eq!(plan.root.cost, Some(NodeCost::new(0.0, 10.5)));
    assert_eq!(plan.root.rows, Some(100));
    assert_eq!(plan.root.width, Some(36));
    assert!(plan.has_sequential_scans());
}

#[test]
fn test_parse_index_scan_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "orders",
                "Index Name": "orders_pkey",
                "Index Cond": "(id = 42)",
                "Startup Cost": 0.42,
                "Total Cost": 8.44,
                "Plan Rows": 1,
                "Plan Width": 48
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::IndexScan);
    assert_eq!(plan.root.index_name, Some("orders_pkey".to_string()));
    assert_eq!(plan.root.index_cond, Some("(id = 42)".to_string()));
    assert!(!plan.has_sequential_scans());
}

#[test]
fn test_parse_nested_plan_nodes_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Hash Join",
                "Join Type": "Inner",
                "Hash Cond": "(o.user_id = u.id)",
                "Startup Cost": 10.00,
                "Total Cost": 100.00,
                "Plan Rows": 500,
                "Plan Width": 72,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Parent Relationship": "Outer",
                        "Relation Name": "orders",
                        "Alias": "o",
                        "Startup Cost": 0.00,
                        "Total Cost": 50.00,
                        "Plan Rows": 1000,
                        "Plan Width": 36
                    },
                    {
                        "Node Type": "Hash",
                        "Parent Relationship": "Inner",
                        "Startup Cost": 5.00,
                        "Total Cost": 10.00,
                        "Plan Rows": 100,
                        "Plan Width": 36,
                        "Plans": [
                            {
                                "Node Type": "Seq Scan",
                                "Relation Name": "users",
                                "Alias": "u",
                                "Startup Cost": 0.00,
                                "Total Cost": 5.00,
                                "Plan Rows": 100,
                                "Plan Width": 36
                            }
                        ]
                    }
                ]
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    // Check root node
    assert_eq!(plan.root.node_type, NodeType::HashJoin);
    assert_eq!(plan.root.join_type, Some(JoinType::Inner));
    assert_eq!(plan.root.join_cond, Some("(o.user_id = u.id)".to_string()));

    // Check children
    assert_eq!(plan.root.children.len(), 2);
    assert_eq!(plan.root.children[0].node_type, NodeType::SeqScan);
    assert_eq!(plan.root.children[0].relation, Some("orders".to_string()));
    assert_eq!(plan.root.children[1].node_type, NodeType::Hash);

    // Check grandchild
    assert_eq!(plan.root.children[1].children.len(), 1);
    assert_eq!(
        plan.root.children[1].children[0].node_type,
        NodeType::SeqScan
    );
    assert_eq!(
        plan.root.children[1].children[0].relation,
        Some("users".to_string())
    );

    // Verify tree structure
    assert_eq!(plan.root.node_count(), 4);
    assert_eq!(plan.root.depth(), 3);
    assert!(plan.has_sequential_scans());
    assert!(plan.has_hash_operations());
}

#[test]
fn test_parse_explain_analyze_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "test",
                "Startup Cost": 0.00,
                "Total Cost": 10.00,
                "Plan Rows": 100,
                "Plan Width": 36,
                "Actual Startup Time": 0.012,
                "Actual Total Time": 0.089,
                "Actual Rows": 95,
                "Actual Loops": 1
            },
            "Planning Time": 0.156,
            "Execution Time": 0.134
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    // Check actual values
    assert_eq!(plan.root.actual_rows, Some(95));
    assert_eq!(plan.root.loops, Some(1));
    assert_eq!(
        plan.root.actual_time_ms,
        Some(ActualTime::new(0.012, 0.089))
    );

    // Check timing
    assert_eq!(plan.planning_time_ms, Some(0.156));
    assert_eq!(plan.execution_time_ms, Some(0.134));
}

#[test]
fn test_parse_sort_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Sort",
                "Sort Key": ["created_at DESC", "id"],
                "Sort Method": "quicksort",
                "Sort Space Used": 64,
                "Startup Cost": 10.00,
                "Total Cost": 15.00,
                "Plan Rows": 100,
                "Plan Width": 36,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "events",
                        "Startup Cost": 0.00,
                        "Total Cost": 10.00,
                        "Plan Rows": 100,
                        "Plan Width": 36
                    }
                ]
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::Sort);
    assert_eq!(
        plan.root.sort_keys,
        vec!["created_at DESC".to_string(), "id".to_string()]
    );
    assert_eq!(plan.root.sort_method, Some("quicksort".to_string()));
    assert_eq!(plan.root.memory_used_kb, Some(64));
}

#[test]
fn test_parse_aggregate_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "HashAggregate",
                "Group Key": ["status", "category"],
                "Hash Buckets": 1024,
                "Hash Batches": 1,
                "Startup Cost": 20.00,
                "Total Cost": 25.00,
                "Plan Rows": 10,
                "Plan Width": 24,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "items",
                        "Startup Cost": 0.00,
                        "Total Cost": 20.00,
                        "Plan Rows": 1000,
                        "Plan Width": 12
                    }
                ]
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::HashAggregate);
    assert_eq!(
        plan.root.group_keys,
        vec!["status".to_string(), "category".to_string()]
    );
    assert_eq!(plan.root.hash_buckets, Some(1024));
    assert_eq!(plan.root.hash_batches, Some(1));
}

#[test]
fn test_parse_filter_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "products",
                "Filter": "(price > 100)",
                "Rows Removed by Filter": 950,
                "Startup Cost": 0.00,
                "Total Cost": 10.00,
                "Plan Rows": 50,
                "Plan Width": 36
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.filter, Some("(price > 100)".to_string()));
    assert_eq!(plan.root.rows_removed_by_filter, Some(950));
}

#[test]
fn test_parse_output_columns_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Output": ["id", "name", "email"],
                "Startup Cost": 0.00,
                "Total Cost": 10.00,
                "Plan Rows": 100,
                "Plan Width": 72
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(
        plan.root.output,
        vec!["id".to_string(), "name".to_string(), "email".to_string()]
    );
}

// ============================================================================
// Text Format Tests
// ============================================================================

#[test]
fn test_parse_simple_seq_scan_text() {
    let text = "Seq Scan on users  (cost=0.00..10.50 rows=100 width=36)";

    let plan = parse_postgres_explain(text).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::SeqScan);
    assert_eq!(plan.root.relation, Some("users".to_string()));
    assert_eq!(plan.root.cost, Some(NodeCost::new(0.0, 10.5)));
    assert_eq!(plan.root.rows, Some(100));
    assert_eq!(plan.root.width, Some(36));
}

#[test]
fn test_parse_index_scan_text() {
    let text = "Index Scan using users_pkey on users  (cost=0.42..8.44 rows=1 width=36)";

    let plan = parse_postgres_explain(text).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::IndexScan);
    assert_eq!(plan.root.relation, Some("users".to_string()));
    assert_eq!(plan.root.index_name, Some("users_pkey".to_string()));
}

#[test]
fn test_parse_nested_text() {
    let text = r#"Hash Join  (cost=10.00..100.00 rows=500 width=72)
   ->  Seq Scan on orders o  (cost=0.00..50.00 rows=1000 width=36)
   ->  Hash  (cost=5.00..10.00 rows=100 width=36)
         ->  Seq Scan on users u  (cost=0.00..5.00 rows=100 width=36)"#;

    let plan = parse_postgres_explain(text).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::HashJoin);
    assert_eq!(plan.root.children.len(), 2);
    assert_eq!(plan.root.children[0].node_type, NodeType::SeqScan);
    assert_eq!(plan.root.children[1].node_type, NodeType::Hash);
    assert_eq!(plan.root.children[1].children.len(), 1);
}

#[test]
fn test_parse_text_with_timing() {
    let text = r#"Seq Scan on test  (cost=0.00..10.00 rows=100 width=36)
Planning Time: 0.156 ms
Execution Time: 0.089 ms"#;

    let plan = parse_postgres_explain(text).expect("parse failed");

    assert_eq!(plan.planning_time_ms, Some(0.156));
    assert_eq!(plan.execution_time_ms, Some(0.089));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_parse_invalid_json() {
    let invalid = "{ not valid json }}}";
    let result = parse_postgres_explain(invalid);
    assert!(result.is_err());
}

#[test]
fn test_parse_missing_plan() {
    let json = r#"[{"SomethingElse": {}}]"#;
    let result = parse_postgres_explain(json);
    assert!(matches!(result, Err(PostgresExplainError::MissingPlan)));
}

#[test]
fn test_parse_empty_text() {
    let result = parse_postgres_explain("");
    assert!(result.is_err());
}

// ============================================================================
// Helper Function Tests
// ============================================================================

#[test]
fn test_extract_between() {
    assert_eq!(extract_between("cost=10..20", "cost=", ".."), Some("10"));
    assert_eq!(
        extract_between("rows=100 width=36", "rows=", " width="),
        Some("100")
    );
    assert_eq!(extract_between("no match here", "foo", "bar"), None);
}

#[test]
fn test_count_indent() {
    assert_eq!(count_indent("no indent"), 0);
    assert_eq!(count_indent("   three spaces"), 3);
    assert_eq!(count_indent("      six spaces"), 6);
}

#[test]
fn test_extract_time_ms() {
    assert_eq!(extract_time_ms("Planning Time: 0.156 ms"), Some(0.156));
    assert_eq!(extract_time_ms("Execution Time: 1.234 ms"), Some(1.234));
    assert_eq!(extract_time_ms("No time here"), None);
}

// ============================================================================
// Integration-style Tests
// ============================================================================

#[test]
fn test_parse_complex_query_plan_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Limit",
                "Startup Cost": 100.00,
                "Total Cost": 110.00,
                "Plan Rows": 10,
                "Plan Width": 96,
                "Plans": [
                    {
                        "Node Type": "Sort",
                        "Sort Key": ["total DESC"],
                        "Startup Cost": 100.00,
                        "Total Cost": 102.50,
                        "Plan Rows": 100,
                        "Plan Width": 96,
                        "Plans": [
                            {
                                "Node Type": "HashAggregate",
                                "Group Key": ["u.id"],
                                "Startup Cost": 80.00,
                                "Total Cost": 90.00,
                                "Plan Rows": 100,
                                "Plan Width": 96,
                                "Plans": [
                                    {
                                        "Node Type": "Hash Join",
                                        "Join Type": "Inner",
                                        "Hash Cond": "(o.user_id = u.id)",
                                        "Startup Cost": 10.00,
                                        "Total Cost": 70.00,
                                        "Plan Rows": 1000,
                                        "Plan Width": 48,
                                        "Plans": [
                                            {
                                                "Node Type": "Seq Scan",
                                                "Relation Name": "orders",
                                                "Alias": "o",
                                                "Startup Cost": 0.00,
                                                "Total Cost": 50.00,
                                                "Plan Rows": 1000,
                                                "Plan Width": 24
                                            },
                                            {
                                                "Node Type": "Hash",
                                                "Startup Cost": 5.00,
                                                "Total Cost": 10.00,
                                                "Plan Rows": 100,
                                                "Plan Width": 24,
                                                "Plans": [
                                                    {
                                                        "Node Type": "Seq Scan",
                                                        "Relation Name": "users",
                                                        "Alias": "u",
                                                        "Startup Cost": 0.00,
                                                        "Total Cost": 5.00,
                                                        "Plan Rows": 100,
                                                        "Plan Width": 24
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            "Planning Time": 1.234,
            "Execution Time": 15.678
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    // Verify structure
    assert_eq!(plan.root.node_type, NodeType::Limit);
    assert_eq!(plan.root.node_count(), 7);
    assert_eq!(plan.root.depth(), 6);

    // Find specific nodes
    let seq_scans = plan.find_nodes_by_type(NodeType::SeqScan);
    assert_eq!(seq_scans.len(), 2);

    let hash_joins = plan.find_nodes_by_type(NodeType::HashJoin);
    assert_eq!(hash_joins.len(), 1);
    assert_eq!(hash_joins[0].join_type, Some(JoinType::Inner));

    // Check timing
    assert_eq!(plan.planning_time_ms, Some(1.234));
    assert_eq!(plan.execution_time_ms, Some(15.678));

    // Verify helper methods
    assert!(plan.has_sequential_scans());
    assert!(plan.has_hash_operations());
}

#[test]
fn test_parse_bitmap_scan_json() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Bitmap Heap Scan",
                "Relation Name": "orders",
                "Startup Cost": 5.00,
                "Total Cost": 20.00,
                "Plan Rows": 50,
                "Plan Width": 36,
                "Plans": [
                    {
                        "Node Type": "Bitmap Index Scan",
                        "Index Name": "orders_status_idx",
                        "Index Cond": "(status = 'pending')",
                        "Startup Cost": 0.00,
                        "Total Cost": 5.00,
                        "Plan Rows": 50,
                        "Plan Width": 0
                    }
                ]
            }
        }
    ]"#;

    let plan = parse_postgres_explain(json).expect("parse failed");

    assert_eq!(plan.root.node_type, NodeType::BitmapHeapScan);
    assert_eq!(plan.root.children.len(), 1);
    assert_eq!(plan.root.children[0].node_type, NodeType::BitmapIndexScan);
    assert_eq!(
        plan.root.children[0].index_name,
        Some("orders_status_idx".to_string())
    );
}

#[test]
fn test_node_iterator_order() {
    let json = r#"[
        {
            "Plan": {
                "Node Type": "Nested Loop",
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "a"
                    },
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "b"
                    }
                ]
            }
        }
    ]"#;

    let plan = parse_json_explain(json).expect("parse failed");
    let nodes: Vec<_> = plan.iter_nodes().collect();

    // Should be depth-first: Nested Loop, Seq Scan, Index Scan
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].node_type, NodeType::NestedLoop);
    assert_eq!(nodes[1].node_type, NodeType::SeqScan);
    assert_eq!(nodes[2].node_type, NodeType::IndexScan);
}
