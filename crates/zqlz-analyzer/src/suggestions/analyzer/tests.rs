//! Tests for Query Analyzer Suggestions

use super::*;
use crate::explain::{NodeType, PlanNode, QueryPlan};

mod severity_level_tests {
    use super::*;

    #[test]
    fn test_is_critical() {
        assert!(SeverityLevel::Critical.is_critical());
        assert!(!SeverityLevel::Warning.is_critical());
        assert!(!SeverityLevel::Info.is_critical());
    }

    #[test]
    fn test_is_warning_or_above() {
        assert!(SeverityLevel::Critical.is_warning_or_above());
        assert!(SeverityLevel::Warning.is_warning_or_above());
        assert!(!SeverityLevel::Info.is_warning_or_above());
    }

    #[test]
    fn test_as_str() {
        assert_eq!(SeverityLevel::Critical.as_str(), "critical");
        assert_eq!(SeverityLevel::Warning.as_str(), "warning");
        assert_eq!(SeverityLevel::Info.as_str(), "info");
    }

    #[test]
    fn test_serialization() {
        let critical = SeverityLevel::Critical;
        let json = serde_json::to_string(&critical).unwrap();
        assert_eq!(json, "\"critical\"");

        let parsed: SeverityLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, SeverityLevel::Critical);
    }
}

mod suggestion_type_tests {
    use super::*;

    #[test]
    fn test_description() {
        assert!(SuggestionType::MissingIndex.description().contains("index"));
        assert!(
            SuggestionType::FullTableScan
                .description()
                .contains("table scan")
        );
    }

    #[test]
    fn test_serialization() {
        let st = SuggestionType::MissingIndex;
        let json = serde_json::to_string(&st).unwrap();
        assert_eq!(json, "\"missing_index\"");
    }
}

mod suggestion_tests {
    use super::*;

    #[test]
    fn test_creation() {
        let suggestion = Suggestion::new(
            SuggestionType::FullTableScan,
            SeverityLevel::Warning,
            "Full scan detected",
            "Add an index",
        );

        assert_eq!(suggestion.suggestion_type, SuggestionType::FullTableScan);
        assert_eq!(suggestion.severity, SeverityLevel::Warning);
        assert_eq!(suggestion.message, "Full scan detected");
        assert_eq!(suggestion.recommendation, "Add an index");
        assert!(suggestion.table.is_none());
        assert!(suggestion.columns.is_empty());
    }

    #[test]
    fn test_builder_methods() {
        let suggestion = Suggestion::new(
            SuggestionType::MissingIndex,
            SeverityLevel::Warning,
            "Missing index",
            "Create index",
        )
        .with_table("users")
        .with_columns(vec!["email".to_string(), "status".to_string()])
        .with_impact(0.8);

        assert_eq!(suggestion.table, Some("users".to_string()));
        assert_eq!(suggestion.columns, vec!["email", "status"]);
        assert!((suggestion.estimated_impact - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_impact_clamping() {
        let suggestion = Suggestion::new(
            SuggestionType::LargeSort,
            SeverityLevel::Info,
            "Sort",
            "Increase work_mem",
        )
        .with_impact(1.5); // Should clamp to 1.0

        assert!((suggestion.estimated_impact - 1.0).abs() < f64::EPSILON);

        let suggestion2 = Suggestion::new(
            SuggestionType::LargeSort,
            SeverityLevel::Info,
            "Sort",
            "Increase work_mem",
        )
        .with_impact(-0.5); // Should clamp to 0.0

        assert!(suggestion2.estimated_impact.abs() < f64::EPSILON);
    }
}

mod query_analysis_tests {
    use super::*;

    fn create_test_plan() -> QueryPlan {
        QueryPlan::new(PlanNode::new(NodeType::SeqScan).with_relation("users"))
    }

    #[test]
    fn test_creation() {
        let plan = create_test_plan();
        let analysis = QueryAnalysis::new(plan);

        assert!(analysis.suggestions.is_empty());
        assert_eq!(analysis.performance_score, 100);
    }

    #[test]
    fn test_add_suggestion_reduces_score() {
        let plan = create_test_plan();
        let mut analysis = QueryAnalysis::new(plan);

        // Critical = -25
        analysis.add_suggestion(Suggestion::new(
            SuggestionType::FullTableScan,
            SeverityLevel::Critical,
            "Critical issue",
            "Fix it",
        ));
        assert_eq!(analysis.performance_score, 75);

        // Warning = -10
        analysis.add_suggestion(Suggestion::new(
            SuggestionType::MissingIndex,
            SeverityLevel::Warning,
            "Warning",
            "Consider",
        ));
        assert_eq!(analysis.performance_score, 65);

        // Info = -3
        analysis.add_suggestion(Suggestion::new(
            SuggestionType::LargeSort,
            SeverityLevel::Info,
            "Info",
            "Maybe",
        ));
        assert_eq!(analysis.performance_score, 62);
    }

    #[test]
    fn test_has_critical_issues() {
        let plan = create_test_plan();
        let mut analysis = QueryAnalysis::new(plan);

        assert!(!analysis.has_critical_issues());

        analysis.add_suggestion(Suggestion::new(
            SuggestionType::FullTableScan,
            SeverityLevel::Critical,
            "Critical",
            "Fix",
        ));

        assert!(analysis.has_critical_issues());
    }

    #[test]
    fn test_has_warnings() {
        let plan = create_test_plan();
        let mut analysis = QueryAnalysis::new(plan);

        assert!(!analysis.has_warnings());

        analysis.add_suggestion(Suggestion::new(
            SuggestionType::LargeSort,
            SeverityLevel::Info,
            "Info",
            "Maybe",
        ));
        assert!(!analysis.has_warnings());

        analysis.add_suggestion(Suggestion::new(
            SuggestionType::MissingIndex,
            SeverityLevel::Warning,
            "Warning",
            "Consider",
        ));
        assert!(analysis.has_warnings());
    }

    #[test]
    fn test_sorted_suggestions() {
        let plan = create_test_plan();
        let mut analysis = QueryAnalysis::new(plan);

        analysis.add_suggestion(Suggestion::new(
            SuggestionType::LargeSort,
            SeverityLevel::Info,
            "Info",
            "Maybe",
        ));
        analysis.add_suggestion(Suggestion::new(
            SuggestionType::FullTableScan,
            SeverityLevel::Critical,
            "Critical",
            "Fix",
        ));
        analysis.add_suggestion(Suggestion::new(
            SuggestionType::MissingIndex,
            SeverityLevel::Warning,
            "Warning",
            "Consider",
        ));

        let sorted = analysis.sorted_suggestions();
        assert_eq!(sorted[0].severity, SeverityLevel::Critical);
        assert_eq!(sorted[1].severity, SeverityLevel::Warning);
        assert_eq!(sorted[2].severity, SeverityLevel::Info);
    }
}

mod analyzer_config_tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = AnalyzerConfig::default();
        assert_eq!(config.high_row_threshold, 10_000);
        assert_eq!(config.large_table_threshold, 1_000);
        assert!((config.filter_efficiency_threshold - 0.5).abs() < f64::EPSILON);
        assert!(config.suggest_indexes);
    }

    #[test]
    fn test_builder() {
        let config = AnalyzerConfig::new()
            .with_high_row_threshold(5000)
            .with_large_table_threshold(500)
            .with_filter_efficiency_threshold(0.7)
            .with_suggest_indexes(false);

        assert_eq!(config.high_row_threshold, 5000);
        assert_eq!(config.large_table_threshold, 500);
        assert!((config.filter_efficiency_threshold - 0.7).abs() < f64::EPSILON);
        assert!(!config.suggest_indexes);
    }
}

mod query_analyzer_tests {
    use super::*;

    #[test]
    fn test_default_creation() {
        let analyzer = QueryAnalyzer::default();
        assert_eq!(analyzer.config().high_row_threshold, 10_000);
    }

    #[test]
    fn test_with_config() {
        let config = AnalyzerConfig::new().with_high_row_threshold(5000);
        let analyzer = QueryAnalyzer::with_config(config);
        assert_eq!(analyzer.config().high_row_threshold, 5000);
    }

    #[test]
    fn test_detect_full_table_scan() {
        let analyzer = QueryAnalyzer::new();

        // Create a plan with a large seq scan
        let plan = QueryPlan::new(
            PlanNode::new(NodeType::SeqScan)
                .with_relation("users")
                .with_rows(15000),
        );

        let analysis = analyzer.analyze(plan);

        assert!(analysis.has_critical_issues());
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::FullTableScan)
        );
    }

    #[test]
    fn test_detect_missing_index() {
        let analyzer = QueryAnalyzer::new();

        // Create a plan with seq scan + filter
        let mut node = PlanNode::new(NodeType::SeqScan)
            .with_relation("users")
            .with_rows(5000);
        node.filter = Some("email = 'test@example.com'".to_string());

        let plan = QueryPlan::new(node);
        let analysis = analyzer.analyze(plan);

        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::MissingIndex)
        );
    }

    #[test]
    fn test_optimal_plan_no_suggestions() {
        let analyzer = QueryAnalyzer::new();

        // Create an optimal plan (index scan, few rows)
        let plan = QueryPlan::new(
            PlanNode::new(NodeType::IndexScan)
                .with_relation("users")
                .with_rows(10)
                .with_index("idx_users_email"),
        );

        let analysis = analyzer.analyze(plan);

        assert!(!analysis.has_warnings());
        assert!(!analysis.has_critical_issues());
        assert_eq!(analysis.performance_score, 100);
        assert!(analysis.summary.contains("optimal"));
    }

    #[test]
    fn test_multiple_seq_scans() {
        let analyzer = QueryAnalyzer::new();

        // Create a plan with multiple seq scans
        let child1 = PlanNode::new(NodeType::SeqScan)
            .with_relation("orders")
            .with_rows(100);
        let child2 = PlanNode::new(NodeType::SeqScan)
            .with_relation("products")
            .with_rows(100);
        let child3 = PlanNode::new(NodeType::SeqScan)
            .with_relation("customers")
            .with_rows(100);

        let plan = QueryPlan::new(
            PlanNode::new(NodeType::NestedLoop)
                .with_child(child1)
                .with_child(
                    PlanNode::new(NodeType::NestedLoop)
                        .with_child(child2)
                        .with_child(child3),
                ),
        );

        let analysis = analyzer.analyze(plan);

        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::MultipleSeqScans)
        );
    }
}

mod extract_columns_tests {
    use super::*;

    #[test]
    fn test_simple_equality() {
        let columns = extract_columns_from_filter("email = 'test@example.com'");
        assert!(columns.contains(&"email".to_string()));
    }

    #[test]
    fn test_comparison_operators() {
        let columns = extract_columns_from_filter("age > 18");
        assert!(columns.contains(&"age".to_string()));
    }

    #[test]
    fn test_and_conditions() {
        let columns = extract_columns_from_filter("status = 'active' AND role = 'admin'");
        assert!(columns.contains(&"status".to_string()));
        assert!(columns.contains(&"role".to_string()));
    }

    #[test]
    fn test_qualified_column() {
        let columns = extract_columns_from_filter("users.email = 'test@example.com'");
        assert!(columns.contains(&"email".to_string()));
    }
}

mod integration_tests {
    use super::*;

    #[test]
    fn test_analyze_complex_query_plan() {
        let analyzer = QueryAnalyzer::with_config(
            AnalyzerConfig::new()
                .with_high_row_threshold(1000)
                .with_large_table_threshold(100),
        );

        // Simulate a query: SELECT * FROM orders JOIN customers ON ... WHERE ...
        let orders_scan = PlanNode::new(NodeType::SeqScan)
            .with_relation("orders")
            .with_rows(5000);

        let customers_scan = PlanNode::new(NodeType::IndexScan)
            .with_relation("customers")
            .with_rows(100)
            .with_index("idx_customers_id");

        let hash = PlanNode::new(NodeType::Hash).with_child(customers_scan);

        let join = PlanNode::new(NodeType::HashJoin)
            .with_child(orders_scan)
            .with_child(hash)
            .with_rows(5000);

        let sort = PlanNode::new(NodeType::Sort)
            .with_child(join)
            .with_rows(5000);

        let plan = QueryPlan::new(sort);
        let analysis = analyzer.analyze(plan);

        // Should detect:
        // 1. Full table scan on orders (critical - 5000 > 1000)
        // 2. Large sort (info - 5000 rows)
        assert!(analysis.has_critical_issues());
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::FullTableScan)
        );
        assert!(
            analysis
                .suggestions
                .iter()
                .any(|s| s.suggestion_type == SuggestionType::LargeSort)
        );

        // Performance score should be reduced
        assert!(analysis.performance_score < 100);

        // Summary should mention issues
        assert!(analysis.summary.contains("critical"));
    }
}
