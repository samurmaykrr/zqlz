//! SQL LSP Tests
//!
//! Organized by category:
//! - test_helpers: Common test utilities and fixtures
//! - test_context_detection: Tests for SQL context analysis
//! - test_completions: Tests for completion suggestions
//! - test_schema_cache: Tests for schema caching
//! - test_edge_cases: Tests for edge cases and error handling
//! - test_regression: Tests for previously fixed bugs
//! - test_join_completions: Tests for JOIN clause completions with FK suggestions
//! - test_diagnostics: Tests for SQL diagnostics and error reporting
//! - test_hover: Tests for hover information
//! - test_definition: Tests for go-to-definition functionality
//! - test_references: Tests for find references functionality
//! - test_rename: Tests for rename symbol functionality
//! - test_code_actions: Tests for code actions / quick fixes
//! - test_qualified_completions: Tests for table.column qualified completions
//! - test_cte_completions: Tests for Common Table Expression (WITH clause) support
//! - test_subquery_completions: Tests for subquery completions and context
//! - test_snippets: Tests for SQL snippet templates
//! - test_fuzzy_matching: Tests for fuzzy matching and completion ranking
//! - test_dialect_specific: Tests for database-specific SQL features
//! - test_completion_range: Tests for completion text replacement ranges
//! - test_where_clause_priority: Tests for WHERE clause completion priority (columns > scalar functions)

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod test_context_detection;

#[cfg(test)]
mod test_completions;

#[cfg(test)]
mod test_schema_cache;

#[cfg(test)]
mod test_edge_cases;

#[cfg(test)]
mod test_regression;

#[cfg(test)]
mod test_join_completions;

#[cfg(test)]
mod test_diagnostics;

#[cfg(test)]
mod test_hover;

#[cfg(test)]
mod test_definition;

#[cfg(test)]
mod test_references;

#[cfg(test)]
mod test_rename;

#[cfg(test)]
mod test_code_actions;

#[cfg(test)]
mod test_qualified_completions;

#[cfg(test)]
mod test_cte_completions;

#[cfg(test)]
mod test_subquery_completions;

// Snippet tests disabled - snippet feature was removed per user request
// #[cfg(test)]
// mod test_snippets;

#[cfg(test)]
mod test_fuzzy_matching;

#[cfg(test)]
mod test_dialect_specific;

#[cfg(test)]
mod test_completion_range;

#[cfg(test)]
mod test_where_clause_priority;

#[cfg(test)]
mod test_no_snippets;
