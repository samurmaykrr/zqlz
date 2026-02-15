//! Query EXPLAIN Parser Module
//!
//! This module provides parsers for EXPLAIN output from various databases:
//! - PostgreSQL (JSON and text formats)
//! - MySQL (JSON and tabular formats)
//! - SQLite (tree and tabular formats)
//!
//! # Example
//!
//! ```
//! use zqlz_analyzer::explain::{parse_postgres_explain, parse_mysql_explain, parse_sqlite_explain, QueryPlan, NodeType};
//!
//! // PostgreSQL EXPLAIN
//! let pg_json = r#"[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "users"}}]"#;
//! let plan = parse_postgres_explain(pg_json).unwrap();
//! assert_eq!(plan.root.node_type, NodeType::SeqScan);
//!
//! // MySQL EXPLAIN
//! let mysql_json = r#"{"query_block": {"select_id": 1, "table": {"table_name": "users", "access_type": "ALL"}}}"#;
//! let plan = parse_mysql_explain(mysql_json).unwrap();
//! assert_eq!(plan.root.node_type, NodeType::SeqScan);
//!
//! // SQLite EXPLAIN QUERY PLAN
//! let sqlite_output = "QUERY PLAN\n|--SCAN users";
//! let plan = parse_sqlite_explain(sqlite_output).unwrap();
//! assert_eq!(plan.root.node_type, NodeType::SeqScan);
//! ```

pub mod mysql;
pub mod plan;
pub mod postgres;
pub mod sqlite;

pub use mysql::{
    MysqlExplainError, parse_json_explain as parse_mysql_json_explain, parse_mysql_explain,
    parse_tabular_explain as parse_mysql_tabular_explain,
};
pub use plan::{ActualTime, JoinType, NodeCost, NodeType, PlanNode, PlanNodeIterator, QueryPlan};
pub use postgres::{
    PostgresExplainError, parse_json_explain, parse_postgres_explain, parse_text_explain,
};
pub use sqlite::{
    SqliteExplainError, parse_sqlite_explain, parse_tabular_format as parse_sqlite_tabular,
    parse_tree_format as parse_sqlite_tree, sqlite_operation_to_node_type,
};
