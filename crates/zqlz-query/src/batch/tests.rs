//! Tests for batch query execution

use super::*;
use std::time::Duration;

mod batch_options_tests {
    use super::*;

    #[test]
    fn test_batch_options_default() {
        let options = BatchOptions::default();

        assert_eq!(options.mode, ExecutionMode::Sequential);
        assert!(options.stop_on_error);
        assert!(!options.transaction);
        assert_eq!(options.max_parallelism, 4);
        assert_eq!(options.statement_timeout_ms, 0);
    }

    #[test]
    fn test_batch_options_sequential() {
        let options = BatchOptions::sequential();

        assert_eq!(options.mode, ExecutionMode::Sequential);
    }

    #[test]
    fn test_batch_options_parallel() {
        let options = BatchOptions::parallel();

        assert_eq!(options.mode, ExecutionMode::Parallel);
    }

    #[test]
    fn test_batch_options_builder() {
        let options = BatchOptions::new()
            .with_mode(ExecutionMode::Parallel)
            .with_stop_on_error(false)
            .with_transaction(true)
            .with_max_parallelism(8)
            .with_statement_timeout_ms(5000);

        assert_eq!(options.mode, ExecutionMode::Parallel);
        assert!(!options.stop_on_error);
        assert!(options.transaction);
        assert_eq!(options.max_parallelism, 8);
        assert_eq!(options.statement_timeout_ms, 5000);
    }

    #[test]
    fn test_batch_options_max_parallelism_minimum() {
        let options = BatchOptions::new().with_max_parallelism(0);

        // Should be clamped to at least 1
        assert_eq!(options.max_parallelism, 1);
    }
}

mod execution_mode_tests {
    use super::*;

    #[test]
    fn test_execution_mode_default() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::Sequential);
    }

    #[test]
    fn test_execution_mode_equality() {
        assert_eq!(ExecutionMode::Sequential, ExecutionMode::Sequential);
        assert_eq!(ExecutionMode::Parallel, ExecutionMode::Parallel);
        assert_ne!(ExecutionMode::Sequential, ExecutionMode::Parallel);
    }
}

mod statement_status_tests {
    use super::*;

    #[test]
    fn test_statement_status_values() {
        assert_eq!(StatementStatus::Success, StatementStatus::Success);
        assert_eq!(StatementStatus::Failed, StatementStatus::Failed);
        assert_eq!(StatementStatus::Skipped, StatementStatus::Skipped);
        assert_eq!(StatementStatus::Pending, StatementStatus::Pending);
    }
}

mod statement_error_tests {
    use super::*;

    #[test]
    fn test_statement_error_new() {
        let error = StatementError::new("test error");

        assert_eq!(error.message, "test error");
        assert!(error.code.is_none());
    }

    #[test]
    fn test_statement_error_with_code() {
        let error = StatementError::new("test error").with_code("E001");

        assert_eq!(error.message, "test error");
        assert_eq!(error.code, Some("E001".to_string()));
    }

    #[test]
    fn test_statement_error_display_without_code() {
        let error = StatementError::new("test error");

        assert_eq!(error.to_string(), "test error");
    }

    #[test]
    fn test_statement_error_display_with_code() {
        let error = StatementError::new("test error").with_code("E001");

        assert_eq!(error.to_string(), "[E001] test error");
    }
}

mod batch_result_tests {
    use super::*;
    use zqlz_core::QueryResult;

    #[test]
    fn test_batch_result_success_query() {
        let query_result = QueryResult::empty();
        let result = BatchResult::success_query(
            0,
            "SELECT 1".to_string(),
            query_result,
            Duration::from_millis(100),
        );

        assert_eq!(result.index, 0);
        assert_eq!(result.sql, "SELECT 1");
        assert!(result.is_success());
        assert!(!result.is_failed());
        assert!(!result.is_skipped());
        assert!(result.query_result.is_some());
        assert!(result.error.is_none());
        assert_eq!(result.execution_time, Duration::from_millis(100));
    }

    #[test]
    fn test_batch_result_success_statement() {
        let result = BatchResult::success_statement(
            1,
            "INSERT INTO t VALUES (1)".to_string(),
            5,
            Duration::from_millis(50),
        );

        assert_eq!(result.index, 1);
        assert!(result.is_success());
        assert_eq!(result.affected_rows, 5);
        assert!(result.query_result.is_none());
    }

    #[test]
    fn test_batch_result_failed() {
        let error = StatementError::new("syntax error");
        let result = BatchResult::failed(
            2,
            "INVALID SQL".to_string(),
            error,
            Duration::from_millis(10),
        );

        assert_eq!(result.index, 2);
        assert!(result.is_failed());
        assert!(!result.is_success());
        assert!(result.error.is_some());
    }

    #[test]
    fn test_batch_result_skipped() {
        let result = BatchResult::skipped(3, "SELECT * FROM t".to_string());

        assert_eq!(result.index, 3);
        assert!(result.is_skipped());
        assert!(!result.is_success());
        assert!(!result.is_failed());
        assert_eq!(result.execution_time, Duration::ZERO);
    }
}

mod batch_execution_result_tests {
    use super::*;

    fn create_test_results() -> Vec<BatchResult> {
        vec![
            BatchResult::success_statement(0, "INSERT 1".to_string(), 1, Duration::from_millis(10)),
            BatchResult::success_statement(1, "INSERT 2".to_string(), 2, Duration::from_millis(20)),
            BatchResult::failed(
                2,
                "INVALID".to_string(),
                StatementError::new("error"),
                Duration::from_millis(5),
            ),
            BatchResult::skipped(3, "INSERT 3".to_string()),
        ]
    }

    #[test]
    fn test_batch_execution_result_counts() {
        let results = create_test_results();
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), false, false);

        assert_eq!(batch_result.success_count, 2);
        assert_eq!(batch_result.failure_count, 1);
        assert_eq!(batch_result.skipped_count, 1);
        assert_eq!(batch_result.statement_count(), 4);
    }

    #[test]
    fn test_batch_execution_result_all_succeeded() {
        let results = vec![
            BatchResult::success_statement(0, "INSERT 1".to_string(), 1, Duration::from_millis(10)),
            BatchResult::success_statement(1, "INSERT 2".to_string(), 2, Duration::from_millis(20)),
        ];
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(50), false, false);

        assert!(batch_result.all_succeeded());
        assert!(!batch_result.has_failures());
    }

    #[test]
    fn test_batch_execution_result_has_failures() {
        let results = create_test_results();
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), false, false);

        assert!(!batch_result.all_succeeded());
        assert!(batch_result.has_failures());
    }

    #[test]
    fn test_batch_execution_result_total_affected_rows() {
        let results = vec![
            BatchResult::success_statement(
                0,
                "INSERT 1".to_string(),
                10,
                Duration::from_millis(10),
            ),
            BatchResult::success_statement(
                1,
                "INSERT 2".to_string(),
                20,
                Duration::from_millis(20),
            ),
            BatchResult::success_statement(2, "DELETE".to_string(), 5, Duration::from_millis(15)),
        ];
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), false, false);

        assert_eq!(batch_result.total_affected_rows(), 35);
    }

    #[test]
    fn test_batch_execution_result_failed_results() {
        let results = create_test_results();
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), false, false);

        let failed = batch_result.failed_results();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].index, 2);
    }

    #[test]
    fn test_batch_execution_result_successful_results() {
        let results = create_test_results();
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), false, false);

        let successful = batch_result.successful_results();
        assert_eq!(successful.len(), 2);
    }

    #[test]
    fn test_batch_execution_result_transactional() {
        let results = vec![];
        let batch_result =
            BatchExecutionResult::new(results, Duration::from_millis(100), true, true);

        assert!(batch_result.was_transactional);
        assert!(batch_result.transaction_rolled_back);
    }
}

mod batch_executor_tests {
    use super::*;

    #[test]
    fn test_batch_executor_creation() {
        let executor = BatchExecutor::with_defaults();

        assert_eq!(executor.options().mode, ExecutionMode::Sequential);
        assert!(executor.options().stop_on_error);
    }

    #[test]
    fn test_batch_executor_with_options() {
        let options = BatchOptions::parallel().with_stop_on_error(false);
        let executor = BatchExecutor::new(options);

        assert_eq!(executor.options().mode, ExecutionMode::Parallel);
        assert!(!executor.options().stop_on_error);
    }
}

mod split_statements_tests {
    use super::*;

    #[test]
    fn test_split_simple_statements() {
        let sql = "SELECT 1; SELECT 2; SELECT 3";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
        assert_eq!(statements[2], "SELECT 3");
    }

    #[test]
    fn test_split_statements_no_trailing_semicolon() {
        let sql = "SELECT 1; SELECT 2";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
    }

    #[test]
    fn test_split_statements_with_whitespace() {
        let sql = "  SELECT 1  ;  \n  SELECT 2  ;  ";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
    }

    #[test]
    fn test_split_statements_preserves_string_literals() {
        let sql = r#"SELECT 'hello; world'; SELECT "semi;colon""#;
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 'hello; world'");
        assert_eq!(statements[1], r#"SELECT "semi;colon""#);
    }

    #[test]
    fn test_split_statements_ignores_line_comments() {
        let sql = "SELECT 1; -- this is a comment; with semicolons\nSELECT 2";
        let statements = split_statements(sql);

        // The comment follows the semicolon so it's preserved with the next statement
        // Most importantly, the semicolons inside the comment don't cause splits
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 1");
        // Comment is preserved but doesn't cause extra splits
        assert!(statements[1].contains("SELECT 2"));
        assert!(statements[1].contains("-- this is a comment"));
    }

    #[test]
    fn test_split_statements_ignores_block_comments() {
        let sql = "SELECT 1; /* comment; with; many; semicolons */ SELECT 2";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn test_split_statements_empty_input() {
        let sql = "";
        let statements = split_statements(sql);

        assert!(statements.is_empty());
    }

    #[test]
    fn test_split_statements_whitespace_only() {
        let sql = "   \n\t   ";
        let statements = split_statements(sql);

        assert!(statements.is_empty());
    }

    #[test]
    fn test_split_statements_single_statement() {
        let sql = "SELECT * FROM users WHERE name = 'John'";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "SELECT * FROM users WHERE name = 'John'");
    }

    #[test]
    fn test_split_statements_escaped_quotes() {
        let sql = "SELECT 'it''s a test'; SELECT 1";
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SELECT 'it''s a test'");
        assert_eq!(statements[1], "SELECT 1");
    }

    #[test]
    fn test_split_statements_complex_sql() {
        let sql = r#"
            INSERT INTO logs (msg) VALUES ('query; executed');
            UPDATE users SET name = 'John; Doe' WHERE id = 1;
            DELETE FROM temp -- cleanup; old data
            WHERE created < '2024-01-01';
            SELECT * FROM users /* filter; results */
        "#;
        let statements = split_statements(sql);

        assert_eq!(statements.len(), 4);
        assert!(statements[0].contains("INSERT INTO logs"));
        assert!(statements[1].contains("UPDATE users"));
        assert!(statements[2].contains("DELETE FROM temp"));
        assert!(statements[3].contains("SELECT * FROM users"));
    }
}

mod serialization_tests {
    use super::*;

    #[test]
    fn test_batch_options_serialization() {
        let options = BatchOptions::parallel()
            .with_stop_on_error(false)
            .with_transaction(true);

        let json = serde_json::to_string(&options).unwrap();
        let deserialized: BatchOptions = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.mode, ExecutionMode::Parallel);
        assert!(!deserialized.stop_on_error);
        assert!(deserialized.transaction);
    }

    #[test]
    fn test_execution_mode_serialization() {
        let sequential = ExecutionMode::Sequential;
        let parallel = ExecutionMode::Parallel;

        assert_eq!(
            serde_json::to_string(&sequential).unwrap(),
            "\"Sequential\""
        );
        assert_eq!(serde_json::to_string(&parallel).unwrap(), "\"Parallel\"");
    }

    #[test]
    fn test_statement_status_serialization() {
        let statuses = vec![
            StatementStatus::Success,
            StatementStatus::Failed,
            StatementStatus::Skipped,
            StatementStatus::Pending,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: StatementStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_statement_error_serialization() {
        let error = StatementError::new("test error").with_code("E001");
        let json = serde_json::to_string(&error).unwrap();
        let deserialized: StatementError = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.message, "test error");
        assert_eq!(deserialized.code, Some("E001".to_string()));
    }
}
