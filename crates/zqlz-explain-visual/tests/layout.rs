use zqlz_analyzer::{
    QueryAnalysis,
    explain::{JoinType, NodeType, PlanNode, QueryPlan},
};
use zqlz_explain_visual::{ExplainGraph, IconFamily, icon_family, provider_label};

#[test]
fn layout_for_multijoin_plan_keeps_depth_and_branches_readable() {
    let analysis = QueryAnalysis::new(QueryPlan::new(sample_plan()));
    let graph = ExplainGraph::from_analysis(&analysis);

    assert_eq!(graph.nodes().len(), 13);
    for edge in graph.edges() {
        let parent = graph.node(edge.parent_id()).expect("parent");
        let child = graph.node(edge.child_id()).expect("child");
        assert!(child.position().0 > parent.position().0);
    }

    let mut y_positions = graph
        .nodes()
        .iter()
        .map(|node| node.position().1)
        .collect::<Vec<_>>();
    y_positions.sort_by(|left, right| left.total_cmp(right));
    y_positions.dedup_by(|left, right| (*left - *right).abs() < 1.0);
    assert!(y_positions.len() >= 6);
}

#[test]
fn cost_highlight_points_at_the_expensive_node_not_the_root() {
    let analysis = QueryAnalysis::new(QueryPlan::new(sample_plan()));
    let graph = ExplainGraph::from_analysis(&analysis);
    let root = graph.nodes().first().expect("root");
    let product_scan = graph
        .nodes()
        .iter()
        .find(|node| node.display_label() == "products")
        .expect("products");
    let order_items_scan = graph
        .nodes()
        .iter()
        .find(|node| node.display_label() == "order_items")
        .expect("order_items");

    // Cumulative cost still puts the root at 100%, which is exactly why the
    // highlight must not use it.
    assert_eq!(root.cost_percent(graph.root_total_cost()), 1.0);
    assert!(root.self_cost_percent(graph.root_total_cost()) < 0.01);
    assert!(!root.exceeds_threshold(graph.root_total_cost()));

    assert!(product_scan.cost_percent(graph.root_total_cost()) < 0.01);
    assert!(!product_scan.exceeds_threshold(graph.root_total_cost()));

    assert!(order_items_scan.exceeds_threshold(graph.root_total_cost()));

    let highlighted = graph
        .nodes()
        .iter()
        .filter(|node| node.exceeds_threshold(graph.root_total_cost()))
        .count();
    assert_eq!(highlighted, 1);
}

#[test]
fn nodes_render_the_postgres_node_type_as_their_primary_label() {
    let analysis = QueryAnalysis::new(QueryPlan::new(sample_plan()));
    let graph = ExplainGraph::from_analysis(&analysis);

    let order_items_scan = graph
        .nodes()
        .iter()
        .find(|node| node.display_label() == "order_items")
        .expect("order_items");
    assert_eq!(order_items_scan.short_name(), "Seq Scan");
    assert_eq!(
        order_items_scan.secondary_label().as_deref(),
        Some("order_items")
    );

    let hash = graph
        .nodes()
        .iter()
        .find(|node| node.short_name() == "Hash")
        .expect("hash");
    assert_eq!(hash.secondary_label(), None);

    assert!(
        graph
            .nodes()
            .iter()
            .any(|node| node.short_name() == "Index Scan")
    );
}

#[test]
fn row_label_contrasts_estimate_with_actual_when_analyze_data_is_present() {
    let mut estimate_only = PlanNode::new(NodeType::SeqScan)
        .with_relation("events")
        .with_cost(0.0, 100.0)
        .with_rows(2_000_000);
    estimate_only.filter = Some("(status = 'open')".to_string());

    let graph = ExplainGraph::from_analysis(&QueryAnalysis::new(QueryPlan::new(
        estimate_only.clone(),
    )));
    let node = graph.nodes().first().expect("root");
    assert_eq!(node.rows_label().as_deref(), Some("2.0M rows"));

    let mut analyzed = estimate_only;
    analyzed.actual_rows = Some(12);
    analyzed.rows_removed_by_filter = Some(2_000_000);

    let graph = ExplainGraph::from_analysis(&QueryAnalysis::new(QueryPlan::new(analyzed)));
    let node = graph.nodes().first().expect("root");
    assert_eq!(node.rows_label().as_deref(), Some("est 2.0M / act 12"));
    assert!(node.copy_payload().contains("Rows Removed by Filter: 2.0M"));
}

#[test]
fn provider_label_and_icon_family_mapping_work() {
    assert_eq!(provider_label(Some("postgres")), "PostgreSQL");
    assert_eq!(provider_label(Some("sqlite")), "SQLite");
    assert_eq!(
        icon_family(&PlanNode::new(NodeType::NestedLoop)),
        IconFamily::Join
    );
    assert_eq!(
        icon_family(&PlanNode::new(NodeType::BitmapIndexScan)),
        IconFamily::IndexScan
    );
    assert_eq!(
        icon_family(&PlanNode::new(NodeType::Append)),
        IconFamily::Append
    );
}

#[test]
fn detail_payload_includes_provider_specific_fields() {
    let mut node = PlanNode::new(NodeType::NestedLoop)
        .with_cost(4.32, 53.47)
        .with_rows(11)
        .with_width(52);
    node.relation = Some("customers".to_string());
    node.alias = Some("c".to_string());
    node.index_name = Some("customers_email_per_tenant_unique".to_string());
    node.index_cond = Some("tenant_id = ...".to_string());
    node.join_type = Some(JoinType::Inner);
    node.join_cond = Some("o.customer_id = c.customer_id".to_string());
    node.filter = Some("profile @> ...".to_string());
    node.extra.insert(
        "Parent Relationship".to_string(),
        serde_json::json!("Outer"),
    );

    let analysis = QueryAnalysis::new(QueryPlan::new(node));
    let graph = ExplainGraph::from_analysis(&analysis);
    let payload = graph.nodes().first().expect("root").copy_payload();

    assert!(payload.contains("Join Cond: o.customer_id = c.customer_id"));
    assert!(payload.contains("Filter: profile @> ..."));
    assert!(payload.contains("Index: customers_email_per_tenant_unique"));
    assert!(payload.contains("Provider Extra"));
    assert!(payload.contains("Parent Relationship"));
}

fn sample_plan() -> PlanNode {
    let orders_2025 = PlanNode::new(NodeType::BitmapHeapScan)
        .with_relation("orders_2025")
        .with_cost(4.20, 11.28)
        .with_rows(3)
        .with_width(28);
    let orders_2026_q1 = PlanNode::new(NodeType::BitmapHeapScan)
        .with_relation("orders_2026_q1")
        .with_cost(4.20, 11.28)
        .with_rows(3)
        .with_width(28);
    let orders_2026_q2 = PlanNode::new(NodeType::BitmapHeapScan)
        .with_relation("orders_2026_q2")
        .with_cost(4.20, 11.28)
        .with_rows(3)
        .with_width(28);
    let orders_default = PlanNode::new(NodeType::BitmapHeapScan)
        .with_relation("orders_default")
        .with_cost(4.20, 11.28)
        .with_rows(3)
        .with_width(28);
    let append = PlanNode::new(NodeType::Append)
        .with_cost(4.17, 45.18)
        .with_rows(12)
        .with_width(28)
        .with_child(orders_2025)
        .with_child(orders_2026_q1)
        .with_child(orders_2026_q2)
        .with_child(orders_default);
    let customer_scan = PlanNode::new(NodeType::IndexScan)
        .with_relation("customers")
        .with_cost(0.13, 8.20)
        .with_rows(1)
        .with_width(24);
    let inner_loop = PlanNode::new(NodeType::NestedLoop)
        .with_cost(4.32, 53.47)
        .with_rows(11)
        .with_width(52)
        .with_child(customer_scan)
        .with_child(append);
    let hash = PlanNode::new(NodeType::Hash)
        .with_cost(53.47, 53.47)
        .with_rows(11)
        .with_width(52)
        .with_child(inner_loop);
    let order_items = PlanNode::new(NodeType::SeqScan)
        .with_relation("order_items")
        .with_cost(0.0, 16.80)
        .with_rows(680)
        .with_width(52);
    let hash_join = PlanNode::new(NodeType::HashJoin)
        .with_cost(53.63, 75.54)
        .with_rows(1)
        .with_width(88)
        .with_child(order_items)
        .with_child(hash);
    let products = PlanNode::new(NodeType::IndexScan)
        .with_relation("products")
        .with_cost(0.13, 0.23)
        .with_rows(1)
        .with_width(16);
    let outer_loop = PlanNode::new(NodeType::NestedLoop)
        .with_cost(53.78, 75.78)
        .with_rows(1)
        .with_width(104)
        .with_child(hash_join)
        .with_child(products);

    PlanNode::new(NodeType::Sort)
        .with_cost(75.79, 75.79)
        .with_rows(1)
        .with_width(104)
        .with_child(outer_loop)
}
