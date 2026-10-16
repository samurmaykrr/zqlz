//! MySQL 8.4 plans captured from a real server (`tests/fixtures/mysql`), parsed
//! in every format: `EXPLAIN FORMAT=JSON` (`.json`), `EXPLAIN FORMAT=TREE`
//! (`.tree.txt`), `EXPLAIN ANALYZE` (`.analyze.txt`) and the classic table
//! (`.classic.txt`).

use zqlz_analyzer::QueryAnalyzer;
use zqlz_analyzer::explain::{JoinType, NodeType, PlanNode, QueryPlan, parse_mysql_explain};
use zqlz_analyzer::suggestions::SuggestionType;

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/mysql/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"))
}

fn plan(name: &str) -> QueryPlan {
    parse_mysql_explain(&fixture(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn operation(node: &PlanNode) -> &str {
    node.extra
        .get("operation")
        .and_then(|operation| operation.as_str())
        .unwrap_or_else(|| panic!("{:?} node has no operation label", node.node_type))
}

fn nodes_of(plan: &QueryPlan, node_type: NodeType) -> Vec<&PlanNode> {
    plan.find_nodes_by_type(node_type)
}

fn relations(plan: &QueryPlan) -> Vec<String> {
    plan.iter_nodes()
        .filter(|node| node.is_scan())
        .filter_map(|node| node.relation.clone())
        .collect()
}

fn all_fixtures() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(format!(
        "{}/tests/fixtures/mysql",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
    .collect();
    names.sort();
    names
}

#[test]
fn every_fixture_parses_with_meaningful_labels() {
    let mut parsed = 0;
    for name in all_fixtures() {
        // MySQL answers "<not executable by iterator executor>" for UPDATE/DELETE trees;
        // that is reported as an error so the caller falls back to JSON.
        if name.ends_with(".tree.txt") && (name.starts_with("update") || name.starts_with("delete"))
        {
            assert!(parse_mysql_explain(&fixture(&name)).is_err(), "{name}");
            continue;
        }
        let plan = plan(&name);
        parsed += 1;
        for node in plan.iter_nodes() {
            assert_ne!(node.node_type, NodeType::Unknown, "{name}: unknown node");
            assert!(!operation(node).is_empty(), "{name}: empty label");
        }
    }
    assert!(parsed >= 30, "only {parsed} fixtures parsed");
}

#[test]
fn simple_scan_in_every_format() {
    for name in ["scan.json", "scan.classic.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::SeqScan, "{name}");
        assert_eq!(plan.root.relation.as_deref(), Some("orders_order_items"));
        assert_eq!(plan.root.rows, Some(488_256), "{name}");
        assert_eq!(operation(&plan.root), "Table scan", "{name}");
    }

    let json = plan("scan.json");
    assert_eq!(json.total_cost, Some(57708.07));

    for name in ["scan.tree.txt", "scan.analyze.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::Limit, "{name}");
        assert_eq!(operation(&plan.root), "Limit");
        assert_eq!(plan.root.description.as_deref(), Some("5 row(s)"));
        assert_eq!(plan.total_cost, Some(57708.0));
        let scan = &plan.root.children[0];
        assert_eq!(scan.node_type, NodeType::SeqScan);
        assert_eq!(scan.relation.as_deref(), Some("orders_order_items"));
        assert_eq!(scan.rows, Some(488_256));
        assert_eq!(operation(scan), "Table scan");
    }
}

#[test]
fn explain_analyze_carries_actual_rows_loops_and_time() {
    let plan = plan("join3.analyze.txt");
    // root: Limit 20 rows after 1.84 ms
    assert_eq!(plan.root.actual_rows, Some(20));
    assert_eq!(plan.root.loops, Some(1));
    assert_eq!(plan.execution_time_ms, Some(1.84));

    // the inner lookup ran once per outer row: 7 loops of ~0.0686 ms
    let lookup = plan
        .iter_nodes()
        .find(|node| node.relation.as_deref() == Some("i"))
        .unwrap();
    assert_eq!(lookup.loops, Some(7));
    let time = lookup.actual_time_ms.unwrap();
    assert_eq!((time.startup, time.total), (0.068, 0.0686));
    assert_eq!(lookup.rows, Some(3));
    assert_eq!(lookup.actual_rows, Some(3));
}

#[test]
fn explain_analyze_records_rows_a_filter_threw_away() {
    let plan = plan("semi.analyze.txt");
    let filter = nodes_of(&plan, NodeType::Filter)[0];
    assert_eq!(filter.rows_removed_by_filter, Some(450_000));
    assert_eq!(filter.relation.as_deref(), Some("orders_order_items"));

    let materialize = nodes_of(&plan, NodeType::Materialize)[0];
    assert_eq!(materialize.loops, Some(1));
    // the lookup into the materialized table ran for each of the 150000 outer rows
    let lookup = plan
        .iter_nodes()
        .find(|node| node.loops == Some(150_000))
        .unwrap();
    assert_eq!(lookup.actual_rows, Some(0));
    assert!(plan.iter_nodes().all(|node| node.actual_time_ms.is_some()));

    let analysis = QueryAnalyzer::new().analyze(plan);
    assert!(
        analysis
            .suggestions
            .iter()
            .any(|suggestion| suggestion.suggestion_type == SuggestionType::InefficientFilter
                && suggestion.table.as_deref() == Some("orders_order_items"))
    );
}

#[test]
fn primary_key_lookup() {
    for name in ["pk.json", "pk.classic.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::IndexScan, "{name}");
        assert_eq!(plan.root.index_name.as_deref(), Some("PRIMARY"));
        assert_eq!(plan.root.rows, Some(1));
        assert_eq!(operation(&plan.root), "Single-row index lookup", "{name}");
    }

    // MySQL resolves the constant lookup while planning
    let plan = plan("pk.tree.txt");
    assert_eq!(plan.root.node_type, NodeType::Result);
    assert_eq!(operation(&plan.root), "Rows fetched before execution");
}

#[test]
fn three_table_join_in_every_format() {
    for name in [
        "join3.json",
        "join3.classic.txt",
        "join3.tree.txt",
        "join3.analyze.txt",
    ] {
        let plan = plan(name);
        let joins = nodes_of(&plan, NodeType::NestedLoop);
        assert_eq!(joins.len(), 2, "{name}");
        assert!(
            joins
                .iter()
                .all(|join| join.join_type == Some(JoinType::Inner)),
            "{name}"
        );
        assert!(
            joins
                .iter()
                .all(|join| operation(join) == "Nested loop inner join"),
            "{name}"
        );

        // left-deep: ((s ⨝ o) ⨝ i)
        let scans = relations(&plan);
        assert_eq!(scans.len(), 3, "{name}: {scans:?}");
        assert!(scans[0] == "s" || scans[0].starts_with("util_order_statuses"));
        assert_eq!(plan.iter_nodes().filter(|n| n.is_scan()).count(), 3);

        let lookup = plan
            .iter_nodes()
            .find(|node| node.index_name.as_deref() == Some("idx_order_item_order"))
            .unwrap_or_else(|| panic!("{name}: no order_items lookup"));
        assert_eq!(lookup.node_type, NodeType::IndexScan, "{name}");
        assert_eq!(operation(lookup), "Index lookup", "{name}");
    }

    let tree = plan("join3.tree.txt");
    let covering = nodes_of(&tree, NodeType::IndexOnlyScan);
    assert_eq!(covering.len(), 1);
    assert_eq!(operation(covering[0]), "Covering index scan");
    let condition = tree
        .iter_nodes()
        .find(|node| node.index_cond.is_some())
        .unwrap();
    assert!(condition.index_cond.as_ref().unwrap().starts_with("(o.tenant_id"));
}

#[test]
fn join_costs_are_cumulative_so_only_the_expensive_node_stands_out() {
    for name in ["join3.json", "join3.tree.txt"] {
        let plan = plan(name);
        let root_cost = plan.root.cost.unwrap().total;
        assert!(root_cost > 100_000.0, "{name}");
        // no child costs more than its parent
        for node in plan.iter_nodes() {
            let total = node.cost.map(|cost| cost.total).unwrap_or(0.0);
            for child in &node.children {
                if node.is_join() {
                    assert!(child.cost.unwrap().total <= total + 1.0, "{name}");
                }
            }
        }
    }
}

#[test]
fn group_by_order_by_limit() {
    let json = plan("group_order_limit.json");
    assert_eq!(json.root.node_type, NodeType::Sort);
    assert_eq!(json.root.sort_method.as_deref(), Some("filesort"));
    assert_eq!(json.root.children[0].node_type, NodeType::Aggregate);
    assert_eq!(
        json.root.children[0].children[0].node_type,
        NodeType::IndexOnlyScan
    );

    for name in ["group_order_limit.tree.txt", "group_order_limit.analyze.txt"] {
        let plan = plan(name);
        let chain: Vec<NodeType> = std::iter::successors(Some(&plan.root), |node| {
            node.children.first()
        })
        .map(|node| node.node_type)
        .collect();
        assert_eq!(
            chain,
            [
                NodeType::Limit,
                NodeType::Sort,
                NodeType::Result,
                NodeType::GroupAggregate,
                NodeType::IndexOnlyScan,
            ],
            "{name}"
        );
        let sort = &plan.root.children[0];
        assert_eq!(sort.sort_keys, ["c DESC"]);
        // Limit and Sort print no cost; they inherit their input's
        assert_eq!(plan.total_cost, Some(32764.0).filter(|_| name.contains("analyze")).or(Some(32736.0)));
    }

    let classic = plan("group_order_limit.classic.txt");
    assert_eq!(classic.root.node_type, NodeType::IndexOnlyScan);
}

#[test]
fn filesort_and_temporary_tables_are_reported() {
    let analysis = QueryAnalyzer::new().analyze(plan("group_order_limit.json"));
    let messages: Vec<&str> = analysis
        .suggestions
        .iter()
        .map(|suggestion| suggestion.message.as_str())
        .collect();
    assert!(messages.iter().any(|m| m.contains("filesort")), "{messages:?}");
    assert!(
        messages.iter().any(|m| m.contains("temporary table")),
        "{messages:?}"
    );

    // classic "Using filesort" in the Extra column
    let analysis = QueryAnalyzer::new().analyze(plan("window.classic.txt"));
    assert!(
        analysis
            .suggestions
            .iter()
            .any(|suggestion| suggestion.message.contains("filesort"))
    );

    // the tree's Sort node is a filesort too, reported once rather than twice
    let analysis = QueryAnalyzer::new().analyze(plan("window.tree.txt"));
    let sorts = analysis
        .suggestions
        .iter()
        .filter(|suggestion| suggestion.suggestion_type == SuggestionType::LargeSort)
        .count();
    assert_eq!(sorts, 1);
}

#[test]
fn full_scan_of_a_large_table_is_flagged() {
    // the screenshot case: `score 100 suggestions 0` for a scan of 488k rows
    for name in ["scan.json", "scan.classic.txt", "scan.tree.txt"] {
        let analysis = QueryAnalyzer::new().analyze(plan(name));
        let scan = analysis
            .suggestions
            .iter()
            .find(|suggestion| suggestion.suggestion_type == SuggestionType::FullTableScan)
            .unwrap_or_else(|| panic!("{name}: no full table scan suggestion"));
        assert_eq!(scan.table.as_deref(), Some("orders_order_items"));
        assert!(scan.message.contains("488256"), "{}", scan.message);
        assert!(
            scan.message.contains("possible_keys is NULL"),
            "{}",
            scan.message
        );
        assert!(analysis.performance_score < 100, "{name}");
        assert!(analysis.summary.contains("Estimated cost") || name.contains("classic"));
    }
}

#[test]
fn scan_that_skipped_available_indexes_says_so() {
    let analysis = QueryAnalyzer::new().analyze(plan("semi.json"));
    let scan = analysis
        .suggestions
        .iter()
        .find(|suggestion| {
            suggestion.suggestion_type == SuggestionType::FullTableScan
                && suggestion.table.as_deref() == Some("orders_orders")
        })
        .expect("scan of orders_orders");
    assert!(scan.message.contains("skipped available indexes: PRIMARY"));
}

#[test]
fn index_access_is_not_called_a_full_scan() {
    for name in ["pk.json", "join3.json", "join3.tree.txt"] {
        let analysis = QueryAnalyzer::new().analyze(plan(name));
        assert!(
            analysis
                .suggestions
                .iter()
                .all(|suggestion| suggestion.suggestion_type != SuggestionType::FullTableScan),
            "{name}: {:?}",
            analysis.suggestions
        );
    }
}

#[test]
fn subquery_semi_join_materializes_the_subquery() {
    for name in ["semi.json", "semi.classic.txt", "semi.tree.txt"] {
        let plan = plan(name);
        let join = plan
            .iter_nodes()
            .find(|node| node.node_type == NodeType::NestedLoop)
            .unwrap_or_else(|| panic!("{name}: no join"));
        assert_eq!(join.children.len(), 2, "{name}");
        let lookup = &join.children[1];
        assert_eq!(operation(lookup), "Single-row index lookup", "{name}");
        assert_eq!(lookup.relation.as_deref(), Some("<subquery2>"), "{name}");
        let materialize = lookup
            .children
            .first()
            .unwrap_or_else(|| panic!("{name}: lookup has no subquery"));
        assert_eq!(materialize.node_type, NodeType::Materialize, "{name}");
        assert!(
            materialize
                .children
                .iter()
                .any(|node| node.relation.as_deref() == Some("orders_order_items")
                    || node.node_type == NodeType::Filter),
            "{name}"
        );
    }
}

#[test]
fn derived_table_and_cte_are_materialized_not_scanned() {
    for name in [
        "derived.json",
        "derived.classic.txt",
        "derived.tree.txt",
        "cte.json",
        "cte.classic.txt",
        "cte.tree.txt",
    ] {
        let plan = plan(name);
        // reading the temporary table must not look like a full scan of a big table
        assert_eq!(plan.root.node_type, NodeType::CteScan, "{name}");
        assert_eq!(plan.root.children[0].node_type, NodeType::Materialize, "{name}");
        assert!(!plan.has_sequential_scans(), "{name}");
        let analysis = QueryAnalyzer::new().analyze(plan);
        assert!(
            analysis
                .suggestions
                .iter()
                .all(|suggestion| suggestion.suggestion_type != SuggestionType::FullTableScan),
            "{name}"
        );
    }
    assert_eq!(operation(&plan("cte.tree.txt").root.children[0]), "Materialize CTE c");
}

#[test]
fn union_members_hang_below_the_union() {
    for name in ["union.json", "union.classic.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::Append, "{name}");
        assert_eq!(operation(&plan.root), "Union", "{name}");
        assert_eq!(plan.root.children.len(), 2, "{name}");
        assert_eq!(
            relations(&plan),
            ["orders_orders", "orders_order_items"],
            "{name}"
        );
    }

    let tree = plan("union.tree.txt");
    let union = nodes_of(&tree, NodeType::Append)[0];
    assert_eq!(operation(union), "Union materialize with deduplication");
    assert_eq!(union.children.len(), 2);
    assert_eq!(
        relations(&tree),
        ["<union temporary>", "orders_orders", "orders_order_items"]
    );
}

#[test]
fn window_function() {
    let json = plan("window.json");
    assert_eq!(json.root.node_type, NodeType::WindowAgg);
    assert_eq!(operation(&json.root), "Window aggregate");
    assert_eq!(json.root.description.as_deref(), Some("row_number"));

    let tree = plan("window.tree.txt");
    let window = nodes_of(&tree, NodeType::WindowAgg)[0];
    assert!(window
        .description
        .as_ref()
        .unwrap()
        .starts_with("row_number() OVER (PARTITION BY"));
    assert_eq!(window.children[0].node_type, NodeType::Sort);
    // MySQL prints cost=0 for the window and the limit; the sort below is what costs
    assert_eq!(tree.total_cost, Some(17522.0));
}

#[test]
fn update_and_delete_explains_are_not_executed_plans() {
    for name in ["update.json", "update.classic.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::Update, "{name}");
        assert_eq!(plan.root.relation.as_deref(), Some("orders_orders"));
        assert_eq!(plan.root.children[0].index_name.as_deref(), Some("PRIMARY"));
    }
    for name in ["delete.json", "delete.classic.txt"] {
        let plan = plan(name);
        assert_eq!(plan.root.node_type, NodeType::Delete, "{name}");
        assert_eq!(operation(&plan.root), "Delete from");
        assert_eq!(plan.root.children[0].node_type, NodeType::SeqScan, "{name}");
        let analysis = QueryAnalyzer::new().analyze(plan);
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|suggestion| suggestion.suggestion_type == SuggestionType::FullTableScan),
            "{name}"
        );
    }
}

#[test]
fn hash_join_in_tree_output() {
    let text = "-> Inner hash join (a.id = b.a_id)  (cost=10 rows=5)\n    \
                -> Table scan on b  (cost=3 rows=10)\n    \
                -> Hash\n        \
                -> Table scan on a  (cost=1 rows=4)\n";
    let plan = parse_mysql_explain(text).unwrap();
    assert_eq!(plan.root.node_type, NodeType::HashJoin);
    assert_eq!(plan.root.join_cond.as_deref(), Some("a.id = b.a_id"));
    assert_eq!(plan.root.join_type, Some(JoinType::Inner));
    assert_eq!(plan.root.children[1].node_type, NodeType::Hash);
}

#[test]
fn unknown_operations_keep_their_raw_text() {
    let plan = parse_mysql_explain("-> Frobnicate the widgets: 3 of them  (cost=1 rows=2)").unwrap();
    assert_eq!(plan.root.node_type, NodeType::Unknown);
    assert_eq!(operation(&plan.root), "Frobnicate the widgets: 3 of them");
}

#[test]
fn tree_output_with_a_column_header_and_never_executed_nodes() {
    let text = "EXPLAIN\n-> Nested loop inner join  (cost=2 rows=1) (actual time=0.1..0.2 rows=0 loops=1)\n    \
                -> Table scan on a  (cost=1 rows=1) (actual time=0.1..0.1 rows=0 loops=1)\n    \
                -> Table scan on b  (cost=1 rows=1) (never executed)\n";
    let plan = parse_mysql_explain(text).unwrap();
    assert_eq!(plan.root.children.len(), 2);
    let never = &plan.root.children[1];
    assert_eq!(never.loops, Some(0));
    assert_eq!(never.actual_rows, Some(0));
}

#[test]
fn pipe_bordered_classic_table() {
    let text = "+----+-------------+-------+------+---------------+------+---------+------+------+-------+\n\
                | id | select_type | table | type | possible_keys | key  | key_len | ref  | rows | Extra |\n\
                +----+-------------+-------+------+---------------+------+---------+------+------+-------+\n\
                |  1 | SIMPLE      | users | ALL  | NULL          | NULL | NULL    | NULL | 1000 | Using where |\n\
                +----+-------------+-------+------+---------------+------+---------+------+------+-------+\n";
    let plan = parse_mysql_explain(text).unwrap();
    assert_eq!(plan.root.node_type, NodeType::SeqScan);
    assert_eq!(plan.root.rows, Some(1000));
    assert_eq!(plan.root.extra["using_where"], true);
}

#[test]
fn classic_table_without_partitions_or_filtered_columns() {
    // MySQL 5.5 / 5.6 layout
    let text = "id\tselect_type\ttable\ttype\tpossible_keys\tkey\tkey_len\tref\trows\tExtra\n\
                1\tSIMPLE\tusers\tref\tidx_a\tidx_a\t4\tconst\t7\tUsing index condition\n";
    let plan = parse_mysql_explain(text).unwrap();
    assert_eq!(plan.root.node_type, NodeType::IndexScan);
    assert_eq!(plan.root.rows, Some(7));
    // index condition pushdown is not a covering index
    assert_eq!(operation(&plan.root), "Index lookup");
}

#[test]
fn classic_table_with_no_table_is_a_result_node() {
    let text = "id\tselect_type\ttable\tpartitions\ttype\tpossible_keys\tkey\tkey_len\tref\trows\tfiltered\tExtra\n\
                1\tSIMPLE\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNo tables used\n";
    let plan = parse_mysql_explain(text).unwrap();
    assert_eq!(plan.root.node_type, NodeType::Result);
    assert_eq!(plan.root.description.as_deref(), Some("No tables used"));
}

#[test]
fn mariadb_json_shape() {
    let json = r#"{"query_block": {"select_id": 1, "table": {"table_name": "t", "access_type": "ALL", "rows": 1000, "filtered": 100, "attached_condition": "t.a > 1"}}}"#;
    let plan = parse_mysql_explain(json).unwrap();
    assert_eq!(plan.root.rows, Some(1000));
    assert_eq!(plan.root.filter.as_deref(), Some("t.a > 1"));

    let json = r#"{"query_block": {"select_id": 1, "filesort": {"sort_key": "t.a", "temporary_table": {"table": {"table_name": "t", "access_type": "ALL", "rows": 5}}}}}"#;
    let plan = parse_mysql_explain(json).unwrap();
    assert_eq!(plan.root.node_type, NodeType::Sort);
    assert_eq!(plan.root.children[0].node_type, NodeType::Materialize);
    assert_eq!(plan.root.children[0].children[0].relation.as_deref(), Some("t"));
}

#[test]
fn json_subqueries_hang_below_the_query() {
    let json = r#"{"query_block": {"select_id": 1,
        "table": {"table_name": "a", "access_type": "ALL", "rows_examined_per_scan": 3,
            "attached_condition": "a.x = (select 1)"},
        "subqueries": [{"dependent": true, "cacheable": false,
            "query_block": {"select_id": 2, "table": {"table_name": "b", "access_type": "ref", "key": "k"}}}]}}"#;
    let plan = parse_mysql_explain(json).unwrap();
    assert_eq!(plan.root.children.len(), 1);
    let subplan = &plan.root.children[0];
    assert_eq!(subplan.node_type, NodeType::SubPlan);
    assert_eq!(subplan.description.as_deref(), Some("dependent, re-run per outer row"));
    assert_eq!(subplan.children[0].relation.as_deref(), Some("b"));
}

#[test]
fn malformed_input_is_an_error_not_a_panic() {
    let deep = (0..400)
        .map(|depth| format!("{}-> Table scan on t{depth}\n", "    ".repeat(depth)))
        .collect::<String>();
    let inputs: Vec<&str> = vec![
        "",
        "   \n\t ",
        "->",
        "-> ",
        "->\n->\n->",
        "(cost=1 rows=2)",
        "-> (cost=",
        "-> Table scan on  (cost=abc rows=)",
        "-> Table scan on t  (cost=1..",
        "{",
        "{}",
        "{\"query_block\": 5}",
        "{\"query_block\": {\"nested_loop\": 7}}",
        "{\"query_block\": {\"nested_loop\": [1, 2, \"x\"]}}",
        "{\"query_block\": {\"table\": []}}",
        "[1, 2, 3]",
        "just some words",
        "id\tselect_type\ttable\ttype",
        "1\t2\t3",
        "\t\t\t\t\t\t\t\t\t\t\t",
        "| | | |",
        "+---+---+\n+---+---+",
        "\u{0}\u{1}\u{2}",
        &deep,
    ];
    for input in inputs {
        // Ok or Err are both fine; panicking is not.
        let _ = parse_mysql_explain(input);
    }

    assert!(parse_mysql_explain("").is_err());
    assert!(parse_mysql_explain("->").is_err());
    assert!(parse_mysql_explain("just some words").is_err());
    assert!(parse_mysql_explain(&deep).is_err());
    assert!(parse_mysql_explain("{\"query_block\": {\"nested_loop\": 7}}").is_err());
}

#[test]
fn truncated_fixtures_never_panic() {
    for name in all_fixtures() {
        let text = fixture(&name);
        for cut in (0..text.len()).step_by(7) {
            if text.is_char_boundary(cut) {
                let _ = parse_mysql_explain(&text[..cut]);
            }
        }
    }
}
