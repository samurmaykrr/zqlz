use zqlz_analyzer::QueryAnalyzer;
use zqlz_core::{ExplainParserKind, QueryResult};

pub fn parse_and_analyze_explain(
    parser_kind: ExplainParserKind,
    dialect_id: Option<&str>,
    raw_output: Option<&QueryResult>,
    query_plan: Option<&QueryResult>,
    duration_ms: u64,
) -> Option<zqlz_analyzer::QueryAnalysis> {
    let mut plan =
        zqlz_drivers::explain::parse_explain_plan(parser_kind, dialect_id, raw_output, query_plan)?;

    plan.execution_time_ms = Some(duration_ms as f64);

    let analyzer = QueryAnalyzer::new();
    Some(analyzer.analyze(plan))
}
