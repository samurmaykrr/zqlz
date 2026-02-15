//! Query Analyzer - Optimization Suggestions
//!
//! This module analyzes query execution plans and provides optimization suggestions.
//! It detects common performance issues and recommends improvements.

use crate::explain::{NodeType, QueryPlan};
use serde::{Deserialize, Serialize};

/// Severity level for suggestions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeverityLevel {
    /// Critical issue that should be addressed immediately
    Critical,
    /// Warning that may impact performance
    Warning,
    /// Informational suggestion for optimization
    Info,
}

impl SeverityLevel {
    /// Returns true if this is a critical issue
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Returns true if this is at least a warning
    pub fn is_warning_or_above(&self) -> bool {
        matches!(self, Self::Critical | Self::Warning)
    }

    /// Returns the severity level as a display string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }
}

/// Type of optimization suggestion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionType {
    /// Missing index that could improve performance
    MissingIndex,
    /// Full table scan detected
    FullTableScan,
    /// High row estimation, possibly outdated statistics
    HighRowEstimate,
    /// Inefficient join strategy
    InefficientJoin,
    /// Sort operation on large dataset
    LargeSort,
    /// Memory-intensive hash operation
    HighMemoryUsage,
    /// Filter removing many rows
    InefficientFilter,
    /// Sequential scan on large table
    LargeSeqScan,
    /// Nested loop with many iterations
    ExpensiveNestedLoop,
    /// Multiple sequential scans
    MultipleSeqScans,
}

impl SuggestionType {
    /// Returns a human-readable description of this suggestion type
    pub fn description(&self) -> &'static str {
        match self {
            Self::MissingIndex => "Consider adding an index",
            Self::FullTableScan => "Full table scan detected",
            Self::HighRowEstimate => "High row estimate - statistics may be outdated",
            Self::InefficientJoin => "Inefficient join strategy",
            Self::LargeSort => "Sort on large dataset",
            Self::HighMemoryUsage => "High memory usage",
            Self::InefficientFilter => "Filter removing many rows",
            Self::LargeSeqScan => "Sequential scan on large table",
            Self::ExpensiveNestedLoop => "Expensive nested loop join",
            Self::MultipleSeqScans => "Multiple sequential scans detected",
        }
    }
}

/// A single optimization suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    /// Type of suggestion
    pub suggestion_type: SuggestionType,
    /// Severity level
    pub severity: SeverityLevel,
    /// Human-readable message explaining the issue
    pub message: String,
    /// Suggested action to improve performance
    pub recommendation: String,
    /// Related table name, if applicable
    pub table: Option<String>,
    /// Related column names, if applicable
    pub columns: Vec<String>,
    /// Estimated impact on performance (0.0 - 1.0, higher = more impact)
    pub estimated_impact: f64,
}

impl Suggestion {
    /// Creates a new suggestion
    pub fn new(
        suggestion_type: SuggestionType,
        severity: SeverityLevel,
        message: impl Into<String>,
        recommendation: impl Into<String>,
    ) -> Self {
        Self {
            suggestion_type,
            severity,
            message: message.into(),
            recommendation: recommendation.into(),
            table: None,
            columns: Vec::new(),
            estimated_impact: 0.5,
        }
    }

    /// Sets the related table
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Sets the related columns
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    /// Sets the estimated impact
    pub fn with_impact(mut self, impact: f64) -> Self {
        self.estimated_impact = impact.clamp(0.0, 1.0);
        self
    }
}

/// Result of query analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAnalysis {
    /// The analyzed query plan
    pub plan: QueryPlan,
    /// List of optimization suggestions
    pub suggestions: Vec<Suggestion>,
    /// Overall performance score (0-100, higher = better)
    pub performance_score: u8,
    /// Summary of the analysis
    pub summary: String,
}

impl QueryAnalysis {
    /// Creates a new query analysis
    pub fn new(plan: QueryPlan) -> Self {
        Self {
            plan,
            suggestions: Vec::new(),
            performance_score: 100,
            summary: String::new(),
        }
    }

    /// Adds a suggestion
    pub fn add_suggestion(&mut self, suggestion: Suggestion) {
        // Reduce score based on severity
        let penalty = match suggestion.severity {
            SeverityLevel::Critical => 25,
            SeverityLevel::Warning => 10,
            SeverityLevel::Info => 3,
        };
        self.performance_score = self.performance_score.saturating_sub(penalty);
        self.suggestions.push(suggestion);
    }

    /// Returns true if there are critical issues
    pub fn has_critical_issues(&self) -> bool {
        self.suggestions.iter().any(|s| s.severity.is_critical())
    }

    /// Returns true if there are warnings or critical issues
    pub fn has_warnings(&self) -> bool {
        self.suggestions
            .iter()
            .any(|s| s.severity.is_warning_or_above())
    }

    /// Returns the number of suggestions
    pub fn suggestion_count(&self) -> usize {
        self.suggestions.len()
    }

    /// Returns suggestions sorted by severity (critical first)
    pub fn sorted_suggestions(&self) -> Vec<&Suggestion> {
        let mut sorted: Vec<_> = self.suggestions.iter().collect();
        sorted.sort_by(|a, b| {
            // Critical < Warning < Info (so critical comes first)
            match (&a.severity, &b.severity) {
                (SeverityLevel::Critical, SeverityLevel::Critical) => std::cmp::Ordering::Equal,
                (SeverityLevel::Critical, _) => std::cmp::Ordering::Less,
                (_, SeverityLevel::Critical) => std::cmp::Ordering::Greater,
                (SeverityLevel::Warning, SeverityLevel::Warning) => std::cmp::Ordering::Equal,
                (SeverityLevel::Warning, _) => std::cmp::Ordering::Less,
                (_, SeverityLevel::Warning) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        });
        sorted
    }
}

/// Configuration for the query analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Threshold for high row estimate (rows above this trigger suggestions)
    pub high_row_threshold: u64,
    /// Threshold for large table scan (rows above this on seq scan trigger suggestions)
    pub large_table_threshold: u64,
    /// Threshold for filter efficiency (% removed above this triggers suggestions)
    pub filter_efficiency_threshold: f64,
    /// Whether to suggest indexes for filtered columns
    pub suggest_indexes: bool,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            high_row_threshold: 10_000,
            large_table_threshold: 1_000,
            filter_efficiency_threshold: 0.5,
            suggest_indexes: true,
        }
    }
}

impl AnalyzerConfig {
    /// Creates a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the high row threshold
    pub fn with_high_row_threshold(mut self, threshold: u64) -> Self {
        self.high_row_threshold = threshold;
        self
    }

    /// Sets the large table threshold
    pub fn with_large_table_threshold(mut self, threshold: u64) -> Self {
        self.large_table_threshold = threshold;
        self
    }

    /// Sets the filter efficiency threshold
    pub fn with_filter_efficiency_threshold(mut self, threshold: f64) -> Self {
        self.filter_efficiency_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Sets whether to suggest indexes
    pub fn with_suggest_indexes(mut self, suggest: bool) -> Self {
        self.suggest_indexes = suggest;
        self
    }
}

/// Query analyzer that provides optimization suggestions
#[derive(Debug, Clone)]
pub struct QueryAnalyzer {
    config: AnalyzerConfig,
}

impl Default for QueryAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryAnalyzer {
    /// Creates a new analyzer with default config
    pub fn new() -> Self {
        Self {
            config: AnalyzerConfig::default(),
        }
    }

    /// Creates a new analyzer with custom config
    pub fn with_config(config: AnalyzerConfig) -> Self {
        Self { config }
    }

    /// Returns the analyzer config
    pub fn config(&self) -> &AnalyzerConfig {
        &self.config
    }

    /// Analyzes a query plan and returns suggestions
    pub fn analyze(&self, plan: QueryPlan) -> QueryAnalysis {
        let mut analysis = QueryAnalysis::new(plan.clone());

        // Check for full table scans
        self.check_full_table_scans(&plan, &mut analysis);

        // Check for missing indexes
        self.check_missing_indexes(&plan, &mut analysis);

        // Check for inefficient joins
        self.check_inefficient_joins(&plan, &mut analysis);

        // Check for large sorts
        self.check_large_sorts(&plan, &mut analysis);

        // Check for inefficient filters
        self.check_inefficient_filters(&plan, &mut analysis);

        // Check for multiple seq scans
        self.check_multiple_seq_scans(&plan, &mut analysis);

        // Generate summary
        analysis.summary = self.generate_summary(&analysis);

        analysis
    }

    fn check_full_table_scans(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        for node in plan.iter_nodes() {
            if node.node_type == NodeType::SeqScan {
                let rows = node.rows.unwrap_or(0);
                let table = node
                    .relation
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());

                if rows >= self.config.large_table_threshold {
                    let severity = if rows >= self.config.high_row_threshold {
                        SeverityLevel::Critical
                    } else {
                        SeverityLevel::Warning
                    };

                    let impact = (rows as f64 / self.config.high_row_threshold as f64).min(1.0);

                    analysis.add_suggestion(
                        Suggestion::new(
                            SuggestionType::FullTableScan,
                            severity,
                            format!("Full table scan on '{}' reading {} rows", table, rows),
                            format!(
                                "Consider adding an index on '{}' or filtering on indexed columns",
                                table
                            ),
                        )
                        .with_table(&table)
                        .with_impact(impact),
                    );
                }
            }
        }
    }

    fn check_missing_indexes(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        if !self.config.suggest_indexes {
            return;
        }

        for node in plan.iter_nodes() {
            // Check for seq scans with filters
            if node.node_type == NodeType::SeqScan && node.filter.is_some() {
                let filter = node.filter.as_ref().unwrap();
                let table = node
                    .relation
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                let rows = node.rows.unwrap_or(0);

                if rows >= self.config.large_table_threshold {
                    // Try to extract column names from filter
                    let columns = extract_columns_from_filter(filter);

                    analysis.add_suggestion(
                        Suggestion::new(
                            SuggestionType::MissingIndex,
                            SeverityLevel::Warning,
                            format!("Sequential scan with filter on '{}': {}", table, filter),
                            format!(
                                "Consider creating an index: CREATE INDEX idx_{}_... ON {} (...)",
                                table.replace('.', "_"),
                                table
                            ),
                        )
                        .with_table(&table)
                        .with_columns(columns)
                        .with_impact(0.7),
                    );
                }
            }
        }
    }

    fn check_inefficient_joins(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        for node in plan.iter_nodes() {
            if node.node_type == NodeType::NestedLoop {
                let rows = node.rows.unwrap_or(0);
                let loops = node.loops.unwrap_or(1);

                if rows * loops >= self.config.high_row_threshold {
                    analysis.add_suggestion(
                        Suggestion::new(
                            SuggestionType::ExpensiveNestedLoop,
                            SeverityLevel::Warning,
                            format!(
                                "Nested loop join processing {} rows with {} loops",
                                rows, loops
                            ),
                            "Consider adding indexes on join columns or restructuring the query"
                                .to_string(),
                        )
                        .with_impact(0.6),
                    );
                }
            }
        }
    }

    fn check_large_sorts(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        for node in plan.iter_nodes() {
            if node.node_type == NodeType::Sort {
                let rows = node.rows.unwrap_or(0);
                let memory = node.memory_used_kb.unwrap_or(0);

                if rows >= self.config.high_row_threshold || memory > 1024 {
                    let message = if memory > 0 {
                        format!("Sort operation on {} rows using {}KB memory", rows, memory)
                    } else {
                        format!("Sort operation on {} rows", rows)
                    };

                    analysis.add_suggestion(
                        Suggestion::new(
                            SuggestionType::LargeSort,
                            SeverityLevel::Info,
                            message,
                            "Consider adding an index to avoid sorting or increasing work_mem"
                                .to_string(),
                        )
                        .with_impact(0.4),
                    );
                }
            }
        }
    }

    fn check_inefficient_filters(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        for node in plan.iter_nodes() {
            if let (Some(rows_removed), Some(actual_rows)) =
                (node.rows_removed_by_filter, node.actual_rows)
            {
                let total = rows_removed + actual_rows;
                if total > 0 {
                    let filter_ratio = rows_removed as f64 / total as f64;
                    if filter_ratio >= self.config.filter_efficiency_threshold {
                        let table = node
                            .relation
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string());

                        analysis.add_suggestion(
                            Suggestion::new(
                                SuggestionType::InefficientFilter,
                                SeverityLevel::Info,
                                format!(
                                    "Filter removed {:.0}% of rows ({} of {})",
                                    filter_ratio * 100.0,
                                    rows_removed,
                                    total
                                ),
                                format!(
                                    "Consider adding an index on the filtered column(s) of '{}'",
                                    table
                                ),
                            )
                            .with_table(&table)
                            .with_impact(filter_ratio * 0.5),
                        );
                    }
                }
            }
        }
    }

    fn check_multiple_seq_scans(&self, plan: &QueryPlan, analysis: &mut QueryAnalysis) {
        let seq_scans: Vec<_> = plan
            .iter_nodes()
            .filter(|n| n.node_type == NodeType::SeqScan)
            .collect();

        if seq_scans.len() >= 3 {
            let tables: Vec<_> = seq_scans
                .iter()
                .filter_map(|n| n.relation.clone())
                .collect();

            analysis.add_suggestion(
                Suggestion::new(
                    SuggestionType::MultipleSeqScans,
                    SeverityLevel::Warning,
                    format!(
                        "Query performs {} sequential scans on tables: {}",
                        seq_scans.len(),
                        tables.join(", ")
                    ),
                    "Consider adding indexes or restructuring the query to reduce full table scans"
                        .to_string(),
                )
                .with_impact(0.5),
            );
        }
    }

    fn generate_summary(&self, analysis: &QueryAnalysis) -> String {
        let critical = analysis
            .suggestions
            .iter()
            .filter(|s| s.severity == SeverityLevel::Critical)
            .count();
        let warnings = analysis
            .suggestions
            .iter()
            .filter(|s| s.severity == SeverityLevel::Warning)
            .count();
        let info = analysis
            .suggestions
            .iter()
            .filter(|s| s.severity == SeverityLevel::Info)
            .count();

        if analysis.suggestions.is_empty() {
            "Query plan looks optimal - no issues detected.".to_string()
        } else if critical > 0 {
            format!(
                "Query has {} critical issue(s), {} warning(s), and {} suggestion(s). Performance score: {}/100",
                critical, warnings, info, analysis.performance_score
            )
        } else if warnings > 0 {
            format!(
                "Query has {} warning(s) and {} suggestion(s). Performance score: {}/100",
                warnings, info, analysis.performance_score
            )
        } else {
            format!(
                "Query has {} minor suggestion(s). Performance score: {}/100",
                info, analysis.performance_score
            )
        }
    }
}

/// Extracts column names from a filter expression (best effort)
fn extract_columns_from_filter(filter: &str) -> Vec<String> {
    // Simple heuristic: look for identifiers before operators
    let mut columns = Vec::new();

    // Common patterns: column = value, column > value, etc.
    let operators = [
        "=", "<>", "!=", ">=", "<=", ">", "<", " IS ", " LIKE ", " IN ",
    ];

    for part in filter.split(" AND ").chain(filter.split(" OR ")) {
        let trimmed = part.trim().trim_start_matches('(').trim_end_matches(')');

        for op in &operators {
            if let Some(idx) = trimmed.find(op) {
                let potential_col = trimmed[..idx].trim();
                // Skip if it looks like a value (starts with quote, number, etc.)
                if !potential_col.is_empty()
                    && !potential_col.starts_with('\'')
                    && !potential_col.starts_with('"')
                    && !potential_col
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                {
                    // Clean up the column name
                    let clean = potential_col
                        .trim_start_matches('(')
                        .trim()
                        .split('.')
                        .last()
                        .unwrap_or(potential_col)
                        .to_string();

                    if !clean.is_empty() && !columns.contains(&clean) {
                        columns.push(clean);
                    }
                }
                break;
            }
        }
    }

    columns
}

#[cfg(test)]
mod tests;
