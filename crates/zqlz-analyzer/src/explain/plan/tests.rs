//! Tests for the Query Plan Model

use super::*;

#[test]
fn test_query_plan_creation() {
    let root = PlanNode::new(NodeType::SeqScan)
        .with_relation("users")
        .with_cost(0.0, 100.0)
        .with_rows(1000);

    let plan = QueryPlan::new(root);

    assert_eq!(plan.total_cost, Some(100.0));
    assert_eq!(plan.total_rows, Some(1000));
    assert!(plan.planning_time_ms.is_none());
    assert!(plan.execution_time_ms.is_none());
}

#[test]
fn test_query_plan_with_timing() {
    let root = PlanNode::new(NodeType::SeqScan);
    let plan = QueryPlan::new(root)
        .with_planning_time(1.5)
        .with_execution_time(25.3);

    assert_eq!(plan.planning_time_ms, Some(1.5));
    assert_eq!(plan.execution_time_ms, Some(25.3));
}

#[test]
fn test_plan_node_builder() {
    let node = PlanNode::new(NodeType::IndexScan)
        .with_relation("orders")
        .with_index("orders_pkey")
        .with_cost(0.42, 8.44)
        .with_rows(1)
        .with_width(36)
        .with_filter("status = 'active'");

    assert_eq!(node.node_type, NodeType::IndexScan);
    assert_eq!(node.relation, Some("orders".to_string()));
    assert_eq!(node.index_name, Some("orders_pkey".to_string()));
    assert_eq!(node.cost, Some(NodeCost::new(0.42, 8.44)));
    assert_eq!(node.rows, Some(1));
    assert_eq!(node.width, Some(36));
    assert_eq!(node.filter, Some("status = 'active'".to_string()));
}

#[test]
fn test_plan_node_tree_traversal() {
    // Build a tree:
    //       HashJoin
    //      /        \
    //   SeqScan   IndexScan
    let leaf1 = PlanNode::new(NodeType::SeqScan).with_relation("users");
    let leaf2 = PlanNode::new(NodeType::IndexScan).with_relation("orders");
    let root = PlanNode::new(NodeType::HashJoin)
        .with_child(leaf1)
        .with_child(leaf2);

    let plan = QueryPlan::new(root);

    // Test iteration
    let nodes: Vec<_> = plan.iter_nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].node_type, NodeType::HashJoin);
    assert_eq!(nodes[1].node_type, NodeType::SeqScan);
    assert_eq!(nodes[2].node_type, NodeType::IndexScan);
}

#[test]
fn test_plan_node_count_and_depth() {
    // Single node
    let single = PlanNode::new(NodeType::Result);
    assert_eq!(single.node_count(), 1);
    assert_eq!(single.depth(), 1);

    // Tree with depth 3
    let leaf1 = PlanNode::new(NodeType::SeqScan);
    let leaf2 = PlanNode::new(NodeType::IndexScan);
    let mid = PlanNode::new(NodeType::Hash).with_child(leaf2);
    let root = PlanNode::new(NodeType::HashJoin)
        .with_child(leaf1)
        .with_child(mid);

    assert_eq!(root.node_count(), 4);
    assert_eq!(root.depth(), 3);
}

#[test]
fn test_plan_find_nodes_by_type() {
    let leaf1 = PlanNode::new(NodeType::SeqScan).with_relation("t1");
    let leaf2 = PlanNode::new(NodeType::SeqScan).with_relation("t2");
    let leaf3 = PlanNode::new(NodeType::IndexScan).with_relation("t3");
    let root = PlanNode::new(NodeType::Append)
        .with_child(leaf1)
        .with_child(leaf2)
        .with_child(leaf3);

    let plan = QueryPlan::new(root);

    let seq_scans = plan.find_nodes_by_type(NodeType::SeqScan);
    assert_eq!(seq_scans.len(), 2);

    let index_scans = plan.find_nodes_by_type(NodeType::IndexScan);
    assert_eq!(index_scans.len(), 1);
}

#[test]
fn test_plan_has_sequential_scans() {
    let plan_with_seq = QueryPlan::new(PlanNode::new(NodeType::SeqScan));
    assert!(plan_with_seq.has_sequential_scans());

    let plan_without = QueryPlan::new(PlanNode::new(NodeType::IndexScan));
    assert!(!plan_without.has_sequential_scans());
}

#[test]
fn test_plan_has_hash_operations() {
    let hash_join = QueryPlan::new(PlanNode::new(NodeType::HashJoin));
    assert!(hash_join.has_hash_operations());

    let hash_agg = QueryPlan::new(PlanNode::new(NodeType::HashAggregate));
    assert!(hash_agg.has_hash_operations());

    let merge_join = QueryPlan::new(PlanNode::new(NodeType::MergeJoin));
    assert!(!merge_join.has_hash_operations());
}

#[test]
fn test_node_cost_execution() {
    let cost = NodeCost::new(10.0, 100.0);
    assert_eq!(cost.execution(), 90.0);
}

#[test]
fn test_plan_node_is_scan() {
    assert!(PlanNode::new(NodeType::SeqScan).is_scan());
    assert!(PlanNode::new(NodeType::IndexScan).is_scan());
    assert!(PlanNode::new(NodeType::IndexOnlyScan).is_scan());
    assert!(PlanNode::new(NodeType::BitmapHeapScan).is_scan());
    assert!(!PlanNode::new(NodeType::HashJoin).is_scan());
    assert!(!PlanNode::new(NodeType::Sort).is_scan());
}

#[test]
fn test_plan_node_is_join() {
    assert!(PlanNode::new(NodeType::NestedLoop).is_join());
    assert!(PlanNode::new(NodeType::HashJoin).is_join());
    assert!(PlanNode::new(NodeType::MergeJoin).is_join());
    assert!(!PlanNode::new(NodeType::SeqScan).is_join());
    assert!(!PlanNode::new(NodeType::Aggregate).is_join());
}

#[test]
fn test_plan_node_is_leaf() {
    let leaf = PlanNode::new(NodeType::SeqScan);
    assert!(leaf.is_leaf());

    let parent = PlanNode::new(NodeType::Limit).with_child(PlanNode::new(NodeType::SeqScan));
    assert!(!parent.is_leaf());
}

#[test]
fn test_plan_node_effective_cost() {
    let node = PlanNode::new(NodeType::Sort).with_cost(100.0, 150.0);
    assert_eq!(node.effective_cost(), Some(50.0));

    let no_cost = PlanNode::new(NodeType::Result);
    assert_eq!(no_cost.effective_cost(), None);
}

#[test]
fn test_node_type_from_postgres_str() {
    assert_eq!(NodeType::from_postgres_str("Seq Scan"), NodeType::SeqScan);
    assert_eq!(
        NodeType::from_postgres_str("Index Scan"),
        NodeType::IndexScan
    );
    assert_eq!(
        NodeType::from_postgres_str("Index Only Scan"),
        NodeType::IndexOnlyScan
    );
    assert_eq!(NodeType::from_postgres_str("Hash Join"), NodeType::HashJoin);
    assert_eq!(
        NodeType::from_postgres_str("Nested Loop"),
        NodeType::NestedLoop
    );
    assert_eq!(NodeType::from_postgres_str("Sort"), NodeType::Sort);
    assert_eq!(
        NodeType::from_postgres_str("Hash Aggregate"),
        NodeType::HashAggregate
    );
    assert_eq!(NodeType::from_postgres_str("Unknown Op"), NodeType::Unknown);
}

#[test]
fn test_node_type_is_potentially_slow() {
    assert!(NodeType::SeqScan.is_potentially_slow());
    assert!(NodeType::NestedLoop.is_potentially_slow());
    assert!(NodeType::Sort.is_potentially_slow());
    assert!(!NodeType::IndexScan.is_potentially_slow());
    assert!(!NodeType::HashJoin.is_potentially_slow());
}

#[test]
fn test_join_type_from_str() {
    assert_eq!(JoinType::from_str("inner"), Some(JoinType::Inner));
    assert_eq!(JoinType::from_str("INNER"), Some(JoinType::Inner));
    assert_eq!(JoinType::from_str("left"), Some(JoinType::Left));
    assert_eq!(JoinType::from_str("Left Outer"), Some(JoinType::Left));
    assert_eq!(JoinType::from_str("full outer"), Some(JoinType::Full));
    assert_eq!(JoinType::from_str("cross"), Some(JoinType::Cross));
    assert_eq!(JoinType::from_str("invalid"), None);
}

#[test]
fn test_actual_time() {
    let time = ActualTime::new(0.01, 0.05);
    assert_eq!(time.startup, 0.01);
    assert_eq!(time.total, 0.05);
}

#[test]
fn test_node_type_description() {
    assert!(!NodeType::SeqScan.description().is_empty());
    assert!(NodeType::SeqScan.description().contains("full table"));
    assert!(NodeType::IndexScan.description().contains("index"));
    assert!(NodeType::Unknown.description().contains("Unknown"));
}

#[test]
fn test_query_plan_serialization() {
    let root = PlanNode::new(NodeType::SeqScan)
        .with_relation("test_table")
        .with_cost(0.0, 10.0)
        .with_rows(100);

    let plan = QueryPlan::new(root);

    let json = serde_json::to_string(&plan).expect("serialization failed");
    let deserialized: QueryPlan = serde_json::from_str(&json).expect("deserialization failed");

    assert_eq!(plan.total_cost, deserialized.total_cost);
    assert_eq!(plan.root.node_type, deserialized.root.node_type);
    assert_eq!(plan.root.relation, deserialized.root.relation);
}
