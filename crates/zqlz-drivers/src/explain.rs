use zqlz_analyzer::explain::QueryPlan;
use zqlz_core::{ExplainParserKind, QueryResult};

pub fn parse_explain_plan(
    parser_kind: ExplainParserKind,
    dialect_id: Option<&str>,
    raw_output: Option<&QueryResult>,
    query_plan: Option<&QueryResult>,
) -> Option<QueryPlan> {
    match parser_kind {
        ExplainParserKind::PostgreSql => {
            #[cfg(feature = "postgres")]
            {
                raw_output
                    .and_then(crate::postgres::parse_postgres_explain_result)
                    .or_else(|| query_plan.and_then(crate::postgres::parse_postgres_explain_result))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (raw_output, query_plan);
                None
            }
        }
        ExplainParserKind::MySql => {
            #[cfg(feature = "mysql")]
            {
                raw_output
                    .and_then(crate::mysql::parse_mysql_explain_result)
                    .or_else(|| query_plan.and_then(crate::mysql::parse_mysql_explain_result))
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = (raw_output, query_plan);
                None
            }
        }
        ExplainParserKind::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                query_plan.and_then(crate::sqlite::parse_sqlite_explain_result)
            }
            #[cfg(not(feature = "sqlite"))]
            {
                let _ = query_plan;
                None
            }
        }
        ExplainParserKind::Raw if dialect_id == Some("mongodb") => {
            #[cfg(feature = "mongodb")]
            {
                raw_output.and_then(crate::mongodb::parse_mongodb_explain_result)
            }
            #[cfg(not(feature = "mongodb"))]
            {
                let _ = raw_output;
                None
            }
        }
        ExplainParserKind::Raw | ExplainParserKind::None => None,
    }
}
