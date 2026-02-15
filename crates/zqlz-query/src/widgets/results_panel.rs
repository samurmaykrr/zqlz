//! Results panel with multi-tab view
//!
//! Displays query results in tabs: Message, Summary, Result, Explain, Problems, Info

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_analyzer::{QueryAnalysis, QueryAnalyzer, QueryPlan};
use zqlz_core::QueryResult;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Selectable, Sizable,
    button::{Button, ButtonCustomVariant, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
    typography::code,
    v_flex,
};

use super::{DiagnosticInfo, DiagnosticInfoSeverity};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResultTab {
    Message,
    Summary,
    Result(usize),  // Index of the result to show
    Explain(usize), // Index of the explain result to show
    Problems,       // SQL diagnostics tab
    Info,
}

/// Sub-tab for Explain view
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainSubTab {
    Visual,
    #[default]
    Plan,
    Op,
    Statistics,
    Info,
}

/// Single statement execution result
#[derive(Clone)]
pub struct StatementResult {
    pub sql: String,
    pub duration_ms: u64,
    pub result: Option<QueryResult>,
    pub error: Option<String>,
    pub affected_rows: u64,
}

/// EXPLAIN result for UI consumption
#[derive(Clone)]
pub struct ExplainResult {
    /// The original SQL that was explained
    pub sql: String,
    /// Execution time of the EXPLAIN itself
    pub duration_ms: u64,
    /// The raw EXPLAIN output as a table (for Op tab - bytecode/opcodes)
    pub raw_output: Option<QueryResult>,
    /// The EXPLAIN QUERY PLAN output (for Plan tab)
    pub query_plan: Option<QueryResult>,
    /// Parsed and analyzed query plan with suggestions
    pub analyzed_plan: Option<QueryAnalysis>,
    /// Error message if EXPLAIN failed
    pub error: Option<String>,
    /// Connection name for display
    pub connection_name: Option<String>,
    /// Database name for display
    pub database_name: Option<String>,
    /// Timestamp when explain was run
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Query execution metadata (multiple statements)
#[derive(Clone)]
pub struct QueryExecution {
    pub sql: String,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: chrono::DateTime<chrono::Utc>,
    pub duration_ms: u64,
    pub connection_name: Option<String>,
    pub database_name: Option<String>,
    pub statements: Vec<StatementResult>,
}

impl QueryExecution {
    pub fn success_count(&self) -> usize {
        self.statements.iter().filter(|s| s.error.is_none()).count()
    }

    pub fn error_count(&self) -> usize {
        self.statements.iter().filter(|s| s.error.is_some()).count()
    }

    pub fn total_rows_affected(&self) -> u64 {
        self.statements.iter().map(|s| s.affected_rows).sum()
    }

    pub fn total_rows_returned(&self) -> usize {
        self.statements
            .iter()
            .filter_map(|s| s.result.as_ref())
            .map(|r| r.rows.len())
            .sum()
    }
}

/// Table delegate for result display (readonly)
struct ResultsTableDelegate {
    columns: Vec<Column>,
    rows: Vec<Vec<String>>,
}

impl ResultsTableDelegate {
    fn new(result: &QueryResult) -> Self {
        // Create row number column as first column (fixed left)
        // Width scales with digit count so large row numbers aren't truncated
        let row_num_width = Self::row_number_column_width(result.rows.len());
        let mut columns: Vec<Column> = vec![
            Column::new("row-num", "#")
                .width(row_num_width)
                .fixed(zqlz_ui::widgets::table::ColumnFixed::Left),
        ];

        // Add data columns
        columns.extend(result.columns.iter().enumerate().map(|(idx, col_meta)| {
            Column::new(format!("col-{}", idx), col_meta.name.clone())
                .width(150.0)
                .resizable(true)
                .sortable()
        }));

        let rows: Vec<Vec<String>> = result
            .rows
            .iter()
            .map(|row| row.values.iter().map(|val| val.to_string()).collect())
            .collect();

        Self { columns, rows }
    }

    /// Calculate the width needed for the row number column based on the maximum
    /// row number that will be displayed.
    fn row_number_column_width(max_row_number: usize) -> f32 {
        let digit_count = if max_row_number == 0 {
            1
        } else {
            (max_row_number as f64).log10().floor() as u32 + 1
        };
        let computed = digit_count as f32 * 8.0 + 44.0;
        computed.max(50.0)
    }
}

impl TableDelegate for ResultsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns
            .get(col_ix)
            .cloned()
            .unwrap_or_else(|| Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix)))
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<ResultsTableDelegate>>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        // First column is the row number
        if col_ix == 0 {
            return div()
                .h_full()
                .flex()
                .items_center()
                .justify_center()
                .px_2()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child((row_ix + 1).to_string())
                .into_any_element();
        }

        // Data columns (offset by 1 because of row number column) - READONLY
        let data_col_ix = col_ix - 1;
        let value = self
            .rows
            .get(row_ix)
            .and_then(|row| row.get(data_col_ix))
            .cloned()
            .unwrap_or_default();

        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .overflow_hidden()
            .text_ellipsis()
            .when(value == "NULL" || value.is_empty(), |this| {
                this.text_color(theme.muted_foreground).child("NULL")
            })
            .when(value != "NULL" && !value.is_empty(), |this| {
                this.child(value)
            })
            .into_any_element()
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _window: &mut Window,
        cx: &mut Context<TableState<ResultsTableDelegate>>,
    ) {
        if col_ix >= self.columns.len() || col_ix == 0 {
            return;
        }

        // Update column sort state
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx == col_ix {
                *col = col.clone().sort(sort);
            } else {
                *col = col.clone().sort(ColumnSort::Default);
            }
        }

        // Sort rows (offset by 1 for row number column)
        let data_col_ix = col_ix - 1;
        self.rows.sort_by(|a, b| {
            let a_val = a.get(data_col_ix).map(|s| s.as_str()).unwrap_or("");
            let b_val = b.get(data_col_ix).map(|s| s.as_str()).unwrap_or("");

            match sort {
                ColumnSort::Ascending => a_val.cmp(b_val),
                ColumnSort::Descending => b_val.cmp(a_val),
                ColumnSort::Default => std::cmp::Ordering::Equal,
            }
        });

        cx.notify();
    }
}

/// Events emitted by the results panel
#[derive(Clone, Debug)]
pub enum ResultsPanelEvent {
    /// User clicked on a diagnostic to go to that location
    GoToLine {
        /// Line number (1-indexed for display)
        line: usize,
        /// Column number (1-indexed for display)
        column: usize,
    },
    /// User requested to reload diagnostics for the active query
    ReloadDiagnostics,
}

/// Results panel for displaying query output
pub struct ResultsPanel {
    /// Focus handle
    focus_handle: FocusHandle,

    /// Current query execution data
    execution: Option<QueryExecution>,

    /// Table states for result grids (one per statement with results)
    table_states: Vec<Entity<TableState<ResultsTableDelegate>>>,

    /// Explain results (one per EXPLAIN executed)
    explain_results: Vec<ExplainResult>,

    /// Table states for explain Op view (raw EXPLAIN output)
    explain_op_table_states: Vec<Entity<TableState<ResultsTableDelegate>>>,

    /// Table states for explain Plan view (EXPLAIN QUERY PLAN output)
    explain_plan_table_states: Vec<Entity<TableState<ResultsTableDelegate>>>,

    /// Active sub-tab for Explain view
    explain_sub_tab: ExplainSubTab,

    /// Active tab
    active_tab: ResultTab,

    /// Whether results are loading
    is_loading: bool,

    /// SQL diagnostics from the active query editor
    problems: Vec<DiagnosticInfo>,

    /// The currently active editor index (for scoping diagnostics)
    active_editor_id: Option<usize>,

    /// Diagnostics loading state (for reload button)
    diagnostics_loading: bool,

    /// Set of closed result tab indices (statement indices that have been closed by user)
    closed_result_tabs: std::collections::HashSet<usize>,

    /// Problems panel severity filters (which severity levels to show)
    problems_show_errors: bool,
    problems_show_warnings: bool,
    problems_show_info: bool,
    problems_show_hints: bool,
}

impl ResultsPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            execution: None,
            table_states: Vec::new(),
            explain_results: Vec::new(),
            explain_op_table_states: Vec::new(),
            explain_plan_table_states: Vec::new(),
            explain_sub_tab: ExplainSubTab::Plan,
            active_tab: ResultTab::Message,
            is_loading: false,
            problems: Vec::new(),
            active_editor_id: None,
            diagnostics_loading: false,
            closed_result_tabs: std::collections::HashSet::new(),
            problems_show_errors: true,
            problems_show_warnings: true,
            problems_show_info: true,
            problems_show_hints: true,
        }
    }

    /// Set the active editor ID - diagnostics will be scoped to this editor
    pub fn set_active_editor_id(&mut self, editor_id: Option<usize>, cx: &mut Context<Self>) {
        if self.active_editor_id != editor_id {
            self.active_editor_id = editor_id;
            tracing::debug!(
                "ResultsPanel: active editor changed to {:?}",
                editor_id
            );
            cx.notify();
        }
    }

    /// Get the active editor ID
    pub fn active_editor_id(&self) -> Option<usize> {
        self.active_editor_id
    }

    /// Set problems/diagnostics from the query editor
    /// Only diagnostics matching the active_editor_id will be displayed
    pub fn set_problems(&mut self, problems: Vec<DiagnosticInfo>, cx: &mut Context<Self>) {
        self.problems = problems;
        self.diagnostics_loading = false;
        cx.notify();
    }

    /// Set diagnostics loading state
    pub fn set_diagnostics_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        if self.diagnostics_loading != loading {
            self.diagnostics_loading = loading;
            cx.notify();
        }
    }

    /// Get current diagnostics loading state
    pub fn is_diagnostics_loading(&self) -> bool {
        self.diagnostics_loading
    }

    /// Get the current problem count
    pub fn problem_count(&self) -> usize {
        self.problems.len()
    }

    /// Get error count from problems
    pub fn error_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|p| matches!(p.severity, DiagnosticInfoSeverity::Error))
            .count()
    }

    /// Get warning count from problems
    pub fn warning_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|p| matches!(p.severity, DiagnosticInfoSeverity::Warning))
            .count()
    }

    /// Get info count from problems
    pub fn info_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|p| matches!(p.severity, DiagnosticInfoSeverity::Info))
            .count()
    }

    /// Get hint count from problems
    pub fn hint_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|p| matches!(p.severity, DiagnosticInfoSeverity::Hint))
            .count()
    }

    /// Get filtered problems based on current severity filters
    fn get_filtered_problems(&self) -> Vec<&DiagnosticInfo> {
        self.problems
            .iter()
            .filter(|p| match p.severity {
                DiagnosticInfoSeverity::Error => self.problems_show_errors,
                DiagnosticInfoSeverity::Warning => self.problems_show_warnings,
                DiagnosticInfoSeverity::Info => self.problems_show_info,
                DiagnosticInfoSeverity::Hint => self.problems_show_hints,
            })
            .collect()
    }

    /// Set the query execution result
    pub fn set_execution(
        &mut self,
        execution: QueryExecution,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Create table states for all statements with results
        self.table_states.clear();
        
        // Reset closed tabs when new execution arrives
        self.closed_result_tabs.clear();

        for statement in &execution.statements {
            if let Some(result) = &statement.result {
                let delegate = ResultsTableDelegate::new(result);
                let table_state = cx.new(|cx| {
                    TableState::new(delegate, window, cx)
                        .col_resizable(true)
                        .sortable(true)
                        .row_selectable(true)
                });
                self.table_states.push(table_state);
            }
        }

        self.execution = Some(execution);
        self.is_loading = false;
        self.active_tab = ResultTab::Message;
        cx.notify();
    }

    /// Add an explain result
    pub fn add_explain_result(
        &mut self,
        result: ExplainResult,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Create table state for Op view (raw EXPLAIN output)
        if let Some(raw) = &result.raw_output {
            let delegate = ResultsTableDelegate::new(raw);
            let table_state = cx.new(|cx| {
                TableState::new(delegate, window, cx)
                    .col_resizable(true)
                    .sortable(true)
                    .row_selectable(true)
            });
            self.explain_op_table_states.push(table_state);
        } else {
            // Push a placeholder - we need to keep indices aligned
            let empty_result = QueryResult::empty();
            let delegate = ResultsTableDelegate::new(&empty_result);
            let table_state = cx.new(|cx| TableState::new(delegate, window, cx));
            self.explain_op_table_states.push(table_state);
        }

        // Create table state for Plan view (EXPLAIN QUERY PLAN output)
        if let Some(plan) = &result.query_plan {
            let delegate = ResultsTableDelegate::new(plan);
            let table_state = cx.new(|cx| {
                TableState::new(delegate, window, cx)
                    .col_resizable(true)
                    .sortable(true)
                    .row_selectable(true)
            });
            self.explain_plan_table_states.push(table_state);
        } else {
            // Push a placeholder
            let empty_result = QueryResult::empty();
            let delegate = ResultsTableDelegate::new(&empty_result);
            let table_state = cx.new(|cx| TableState::new(delegate, window, cx));
            self.explain_plan_table_states.push(table_state);
        }

        let explain_idx = self.explain_results.len();
        self.explain_results.push(result);
        self.is_loading = false;
        self.active_tab = ResultTab::Explain(explain_idx);
        self.explain_sub_tab = ExplainSubTab::Plan;
        cx.notify();
    }

    /// Set loading state
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.is_loading = loading;
        cx.notify();
    }

    /// Clear results
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.execution = None;
        self.table_states.clear();
        self.explain_results.clear();
        self.explain_op_table_states.clear();
        self.explain_plan_table_states.clear();
        self.closed_result_tabs.clear();
        self.is_loading = false;
        cx.notify();
    }

    /// Close a result tab by statement index
    fn close_result_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        self.closed_result_tabs.insert(idx);
        
        // If we just closed the active tab, switch to another tab
        if self.active_tab == ResultTab::Result(idx) {
            // Try to switch to the next non-closed result tab
            if let Some(exec) = &self.execution {
                let next_tab = exec
                    .statements
                    .iter()
                    .enumerate()
                    .find(|(i, s)| {
                        *i != idx && s.result.is_some() && !self.closed_result_tabs.contains(i)
                    })
                    .map(|(i, _)| ResultTab::Result(i));
                
                // If no other result tabs, switch to Summary
                self.active_tab = next_tab.unwrap_or(ResultTab::Summary);
            }
        }
        
        cx.notify();
    }

    /// Get the current result being displayed (if any)
    fn get_current_result(&self) -> Option<(usize, &StatementResult)> {
        if let ResultTab::Result(idx) = self.active_tab {
            if let Some(exec) = &self.execution {
                if let Some(statement) = exec.statements.get(idx) {
                    if statement.result.is_some() {
                        return Some((idx, statement));
                    }
                }
            }
        }
        None
    }

    /// Format statement metadata with number, duration, and row count/affected rows
    fn format_statement_metadata(idx: usize, statement: &StatementResult) -> String {
        let duration_str = format!("{:.3}s", statement.duration_ms as f64 / 1000.0);
        
        if let Some(result) = &statement.result {
            // Query with results
            let row_count = result.rows.len();
            format!(
                "Statement {} - {} - {} row{}",
                idx + 1,
                duration_str,
                row_count,
                if row_count == 1 { "" } else { "s" }
            )
        } else if statement.affected_rows > 0 {
            // DML statement with affected rows
            format!(
                "Statement {} - {} - {} row{} affected",
                idx + 1,
                duration_str,
                statement.affected_rows,
                if statement.affected_rows == 1 { "" } else { "s" }
            )
        } else {
            // DDL or other statement without row counts
            format!("Statement {} - {}", idx + 1, duration_str)
        }
    }

    /// Export current result to CSV
    fn export_csv(&mut self, cx: &mut Context<Self>) {
        let Some((idx, statement)) = self.get_current_result() else {
            return;
        };
        let Some(result) = statement.result.clone() else {
            return;
        };

        let default_filename = format!("result_{}.csv", idx + 1);
        let receiver = cx.prompt_for_new_path(&std::path::PathBuf::from(&default_filename), None);

        cx.spawn(async move |_this, _cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => {
                    tracing::info!("CSV export cancelled by user");
                    return anyhow::Ok(());
                }
            };

            let csv_content = Self::build_csv(&result);
            match std::fs::write(&path, csv_content) {
                Ok(_) => {
                    tracing::info!("Exported {} rows to {}", result.rows.len(), path.display());
                }
                Err(e) => {
                    tracing::error!("Failed to write CSV file: {}", e);
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    /// Build CSV content from query result
    fn build_csv(result: &zqlz_core::QueryResult) -> String {
        let mut csv_content = String::new();

        // Header row
        let header: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        csv_content.push_str(&Self::escape_csv_row(&header));
        csv_content.push('\n');

        // Data rows
        for row in &result.rows {
            let row_values: Vec<String> = row.values.iter().map(|v| v.to_string()).collect();
            let row_refs: Vec<&str> = row_values.iter().map(|s| s.as_str()).collect();
            csv_content.push_str(&Self::escape_csv_row(&row_refs));
            csv_content.push('\n');
        }

        csv_content
    }

    /// Escape a CSV row properly
    fn escape_csv_row(values: &[&str]) -> String {
        values
            .iter()
            .map(|v| {
                if v.contains(',') || v.contains('"') || v.contains('\n') || v.contains('\r') {
                    format!("\"{}\"", v.replace('"', "\"\""))
                } else {
                    v.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Export current result to JSON
    fn export_json(&mut self, cx: &mut Context<Self>) {
        let Some((idx, statement)) = self.get_current_result() else {
            return;
        };
        let Some(result) = statement.result.clone() else {
            return;
        };

        let default_filename = format!("result_{}.json", idx + 1);
        let receiver = cx.prompt_for_new_path(&std::path::PathBuf::from(&default_filename), None);

        cx.spawn(async move |_this, _cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => {
                    tracing::info!("JSON export cancelled by user");
                    return anyhow::Ok(());
                }
            };

            let json_content = Self::build_json(&result);
            match std::fs::write(&path, json_content) {
                Ok(_) => {
                    tracing::info!("Exported {} rows to {}", result.rows.len(), path.display());
                }
                Err(e) => {
                    tracing::error!("Failed to write JSON file: {}", e);
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    /// Build JSON content from query result
    fn build_json(result: &zqlz_core::QueryResult) -> String {
        let column_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();

        let rows: Vec<serde_json::Value> = result
            .rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::Map::new();
                for (idx, value) in row.values.iter().enumerate() {
                    let col_name = column_names.get(idx).copied().unwrap_or("unknown");
                    let json_val = if value.is_null() {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(value.to_string())
                    };
                    obj.insert(col_name.to_string(), json_val);
                }
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
    }

    /// Render the tab bar
    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let mut tab_bar = h_flex()
            .w_full()
            .h(px(32.0))
            .gap_1()
            .px_2()
            .bg(theme.background)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Button::new("tab-message")
                    .ghost()
                    .xsmall()
                    .label("Message")
                    .selected(self.active_tab == ResultTab::Message)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ResultTab::Message;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("tab-summary")
                    .ghost()
                    .xsmall()
                    .label("Summary")
                    .selected(self.active_tab == ResultTab::Summary)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = ResultTab::Summary;
                        cx.notify();
                    })),
            );

        // Add a tab for each statement that has results
        if let Some(exec) = &self.execution {
            for (idx, statement) in exec.statements.iter().enumerate() {
                // Skip closed tabs
                if self.closed_result_tabs.contains(&idx) {
                    continue;
                }
                
                if statement.result.is_some() {
                    let result_idx = idx;
                    // Create a composite tab with label and close button
                    tab_bar = tab_bar.child(
                        h_flex()
                            .gap_1()
                            .items_center()
                            .child(
                                Button::new(format!("tab-result-{}", idx))
                                    .ghost()
                                    .xsmall()
                                    .label(format!("Result {}", idx + 1))
                                    .selected(self.active_tab == ResultTab::Result(result_idx))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.active_tab = ResultTab::Result(result_idx);
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new(format!("close-result-{}", idx))
                                    .ghost()
                                    .xsmall()
                                    .label("×")
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.close_result_tab(result_idx, cx);
                                    })),
                            )
                    );
                }
            }
        }

        // Add a tab for each explain result
        for (idx, _explain) in self.explain_results.iter().enumerate() {
            let explain_idx = idx;
            tab_bar = tab_bar.child(
                Button::new(format!("tab-explain-{}", idx))
                    .ghost()
                    .xsmall()
                    .label(format!("Explain {}", idx + 1))
                    .selected(self.active_tab == ResultTab::Explain(explain_idx))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.active_tab = ResultTab::Explain(explain_idx);
                        cx.notify();
                    })),
            );
        }

        // Add Problems tab
        let problem_count = self.problems.len();
        let error_count = self.error_count();
        let warning_count = self.warning_count();
        let theme = cx.theme();

        tab_bar = tab_bar.child(
            Button::new("tab-problems")
                .ghost()
                .xsmall()
                .map(|btn| {
                    if problem_count > 0 {
                        btn.label(format!("Problems ({})", problem_count))
                    } else {
                        btn.label("Problems")
                    }
                })
                .map(|btn| {
                    if error_count > 0 {
                        btn.custom(ButtonCustomVariant::new(cx).foreground(theme.danger))
                    } else if warning_count > 0 {
                        btn.custom(ButtonCustomVariant::new(cx).foreground(theme.warning))
                    } else {
                        btn
                    }
                })
                .selected(self.active_tab == ResultTab::Problems)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.active_tab = ResultTab::Problems;
                    cx.notify();
                })),
        );

        // Add Info tab
        tab_bar = tab_bar.child(
            Button::new("tab-info")
                .ghost()
                .xsmall()
                .label("Info")
                .selected(self.active_tab == ResultTab::Info)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.active_tab = ResultTab::Info;
                    cx.notify();
                })),
        );

        // Add spacer
        tab_bar = tab_bar.child(div().flex_1());

        // Add reload button when Problems tab is active
        if self.active_tab == ResultTab::Problems {
            tab_bar = tab_bar.child(
                Button::new("reload-diagnostics")
                    .ghost()
                    .xsmall()
                    .label(if self.diagnostics_loading {
                        "Reloading..."
                    } else {
                        "Reload"
                    })
                    .disabled(self.diagnostics_loading)
                    .on_click(cx.listener(|_this, _, _, cx| {
                        cx.emit(ResultsPanelEvent::ReloadDiagnostics);
                    })),
            );
        }

        // Add export buttons when a Result tab is active
        let is_result_tab = matches!(self.active_tab, ResultTab::Result(_));
        if is_result_tab && self.get_current_result().is_some() {
            tab_bar = tab_bar
                .child(
                    Button::new("export-csv")
                        .ghost()
                        .xsmall()
                        .label("Export CSV")
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export_csv(cx);
                        })),
                )
                .child(
                    Button::new("export-json")
                        .ghost()
                        .xsmall()
                        .label("Export JSON")
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export_json(cx);
                        })),
                );
        }

        tab_bar
    }

    /// Render Message tab
    fn render_message_tab(
        &self,
        exec: &QueryExecution,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let success_count = exec.success_count();
        let error_count = exec.error_count();
        let total_count = exec.statements.len();

        v_flex().size_full().child(
            div()
                .id("message-content")
                .flex_1()
                .w_full()
                .overflow_y_scroll()
                .child(
                    v_flex()
                        .p_4()
                        .gap_2()
                        .child(
                            h_flex()
                                .gap_4()
                                .text_sm()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Processed Query:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(total_count.to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Success:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(success_count.to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Error:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(error_count.to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Start Time:"),
                                        )
                                        .child(div().text_color(theme.foreground).child(
                                            exec.start_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                                        )),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("End Time:"),
                                        )
                                        .child(div().text_color(theme.foreground).child(
                                            exec.end_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                                        )),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Elapsed Time:"),
                                        )
                                        .child(div().text_color(theme.foreground).child(format!(
                                            "{:.3}s",
                                            exec.duration_ms as f64 / 1000.0
                                        ))),
                                ),
                        )
                        .child(div().h(px(1.0)).w_full().bg(theme.border))
                        .child(
                            v_flex()
                                .gap_2()
                                .text_sm()
                                .child(
                                    h_flex()
                                        .gap_8()
                                        .child(h_flex().min_w(px(400.0)).child(
                                            div().text_color(theme.muted_foreground).child("Query"),
                                        ))
                                        .child(
                                            h_flex().min_w(px(120.0)).child(
                                                div()
                                                    .text_color(theme.muted_foreground)
                                                    .child("Message"),
                                            ),
                                        )
                                        .child(
                                            h_flex().min_w(px(120.0)).child(
                                                div()
                                                    .text_color(theme.muted_foreground)
                                                    .child("Query Time"),
                                            ),
                                        )
                                        .child(
                                            h_flex().min_w(px(120.0)).child(
                                                div()
                                                    .text_color(theme.muted_foreground)
                                                    .child("Fetch Time"),
                                            ),
                                        ),
                                )
                                .children(exec.statements.iter().map(|statement| {
                                    let sql_preview =
                                        statement.sql.lines().next().unwrap_or(&statement.sql);
                                    let sql_preview = if sql_preview.len() > 60 {
                                        format!("{}...", &sql_preview[..60])
                                    } else {
                                        sql_preview.to_string()
                                    };

                                    h_flex()
                                        .gap_8()
                                        .child(h_flex().min_w(px(400.0)).child(
                                            div().text_color(theme.foreground).child(sql_preview),
                                        ))
                                        .child(
                                            h_flex().min_w(px(120.0)).child(
                                                div()
                                                    .text_color(if statement.error.is_none() {
                                                        theme.success
                                                    } else {
                                                        theme.danger
                                                    })
                                                    .child(if statement.error.is_none() {
                                                        "OK"
                                                    } else {
                                                        "Error"
                                                    }),
                                            ),
                                        )
                                        .child(h_flex().min_w(px(120.0)).child(
                                            div().text_color(theme.foreground).child(format!(
                                                "{:.3}s",
                                                statement.duration_ms as f64 / 1000.0
                                            )),
                                        ))
                                        .child(h_flex().min_w(px(120.0)).child(
                                            div().text_color(theme.foreground).child("0.000s"),
                                        ))
                                })),
                        ),
                ),
        )
    }

    /// Render Summary tab
    fn render_summary_tab(
        &self,
        exec: &QueryExecution,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().child(
            div()
                .id("summary-content")
                .flex_1()
                .w_full()
                .overflow_y_scroll()
                .child(
                    v_flex()
                        .p_4()
                        .gap_2()
                        .text_sm()
                        .child(
                            h_flex()
                                .gap_4()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Processed Query:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(exec.statements.len().to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Success:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(exec.success_count().to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Error:"),
                                        )
                                        .child(
                                            div()
                                                .text_color(theme.foreground)
                                                .child(exec.error_count().to_string()),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Elapsed Time:"),
                                        )
                                        .child(div().text_color(theme.foreground).child(format!(
                                            "{:.3}s",
                                            exec.duration_ms as f64 / 1000.0
                                        ))),
                                ),
                        )
                        .child(div().h(px(1.0)).w_full().bg(theme.border))
                        .child(
                            h_flex()
                                .gap_4()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Start Time:"),
                                        )
                                        .child(
                                            div().text_color(theme.foreground).child(
                                                exec.start_time
                                                    .format("%Y-%m-%d %H:%M:%S%.3f")
                                                    .to_string(),
                                            ),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Connection:"),
                                        )
                                        .child(
                                            div().text_color(theme.foreground).child(
                                                exec.connection_name
                                                    .clone()
                                                    .unwrap_or_else(|| "Unknown".to_string()),
                                            ),
                                        ),
                                ),
                        )
                        .child(
                            h_flex()
                                .gap_4()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("End Time:"),
                                        )
                                        .child(
                                            div().text_color(theme.foreground).child(
                                                exec.end_time
                                                    .format("%Y-%m-%d %H:%M:%S%.3f")
                                                    .to_string(),
                                            ),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .child("Database:"),
                                        )
                                        .child(
                                            div().text_color(theme.foreground).child(
                                                exec.database_name
                                                    .clone()
                                                    .unwrap_or_else(|| "N/A".to_string()),
                                            ),
                                        ),
                                ),
                        )
                        .child(div().h(px(1.0)).w_full().bg(theme.border))
                        .child(
                            v_flex()
                                .gap_2()
                                .child(div().text_color(theme.muted_foreground).child("Queries:"))
                                .children(exec.statements.iter().enumerate().map(
                                    |(idx, statement)| {
                                        div()
                                            .text_color(theme.foreground)
                                            .font_family(theme.mono_font_family.clone())
                                            .p_2()
                                            .bg(theme.muted)
                                            .rounded(px(4.0))
                                            .child(
                                                v_flex()
                                                    .gap_1()
                                                     .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(theme.muted_foreground)
                                                            .child(Self::format_statement_metadata(
                                                                idx, statement
                                                            )),
                                                    )
                                                    .child(statement.sql.clone())
                                                    .when_some(
                                                        statement.error.as_ref(),
                                                        |this, error| {
                                                            this.child(
                                                                div()
                                                                    .text_xs()
                                                                    .text_color(theme.danger)
                                                                    .child(format!(
                                                                        "Error: {}",
                                                                        error
                                                                    )),
                                                            )
                                                        },
                                                    ),
                                            )
                                    },
                                )),
                        ),
                ),
        )
    }

    /// Render Result tab (data table)
    fn render_result_tab(&self, result_idx: usize, cx: &mut Context<Self>) -> impl IntoElement {
        // Find the table state corresponding to this result index
        // We need to map statement index to table state index (only counting statements with results)
        let mut table_state_idx = 0;
        let mut found = false;

        if let Some(exec) = &self.execution {
            for (stmt_idx, statement) in exec.statements.iter().enumerate() {
                if statement.result.is_some() {
                    if stmt_idx == result_idx {
                        found = true;
                        break;
                    }
                    table_state_idx += 1;
                }
            }
        }

        if found && table_state_idx < self.table_states.len() {
            div()
                .size_full()
                .child(Table::new(&self.table_states[table_state_idx]).stripe(true))
                .into_any_element()
        } else {
            self.render_empty_state(cx).into_any_element()
        }
    }

    /// Render Info tab
    fn render_info_tab(&self, exec: &QueryExecution, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .p_4()
            .gap_3()
            .text_sm()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_color(theme.muted_foreground)
                            .child("Time Stamp:"),
                    )
                    .child(
                        div()
                            .text_color(theme.foreground)
                            .child(exec.start_time.format("%Y-%m-%d %H:%M:%S").to_string()),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_color(theme.muted_foreground)
                            .child("Connection:"),
                    )
                    .child(
                        div().text_color(theme.foreground).child(
                            exec.connection_name
                                .clone()
                                .unwrap_or_else(|| "Unknown".to_string()),
                        ),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(div().text_color(theme.muted_foreground).child("Database:"))
                    .child(
                        div().text_color(theme.foreground).child(
                            exec.database_name
                                .clone()
                                .unwrap_or_else(|| "N/A".to_string()),
                        ),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_color(theme.muted_foreground)
                            .child("Query Time:"),
                    )
                    .child(
                        div()
                            .text_color(theme.foreground)
                            .child(format!("{:.3}s", exec.duration_ms as f64 / 1000.0)),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_color(theme.muted_foreground)
                            .child("Fetch Time:"),
                    )
                    .child(div().text_color(theme.foreground).child("0/s")),
            )
            .child(div().h(px(1.0)).w_full().bg(theme.border))
            .child(
                v_flex()
                    .gap_2()
                    .child(div().text_color(theme.muted_foreground).child("Query:"))
                    .child(
                        div()
                            .text_color(theme.foreground)
                            .font_family(theme.mono_font_family.clone())
                            .p_2()
                            .bg(theme.muted)
                            .rounded(px(4.0))
                            .child(exec.sql.clone()),
                    ),
            )
    }

    /// Render the empty state
    fn render_empty_state(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("Run a query to see results"),
        )
    }

    /// Render the loading state
    fn render_loading(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("Executing query..."),
        )
    }

    /// Render the Explain tab (sub-tab bar + content)
    fn render_explain_tab(&self, explain_idx: usize, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .child(self.render_explain_sub_tab_bar(cx))
            .child(div().flex_1().w_full().overflow_hidden().map(
                |this| match self.explain_sub_tab {
                    ExplainSubTab::Visual => {
                        this.child(self.render_explain_visual_view(explain_idx, cx))
                    }
                    ExplainSubTab::Plan => {
                        this.child(self.render_explain_plan_view(explain_idx, cx))
                    }
                    ExplainSubTab::Op => this.child(self.render_explain_op_view(explain_idx, cx)),
                    ExplainSubTab::Statistics => {
                        this.child(self.render_explain_statistics_view(explain_idx, cx))
                    }
                    ExplainSubTab::Info => {
                        this.child(self.render_explain_info_view(explain_idx, cx))
                    }
                },
            ))
    }

    /// Render the Problems tab showing SQL diagnostics
    fn render_problems_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let filtered_problems = self.get_filtered_problems();

        v_flex()
            .size_full()
            // Filter bar with severity counts
            .child(
                h_flex()
                    .w_full()
                    .h(px(32.0))
                    .px_2()
                    .gap_1()
                    .items_center()
                    .bg(theme.muted.opacity(0.3))
                    .border_b_1()
                    .border_color(theme.border)
                    // Error filter button
                    .child({
                        let error_count = self.error_count();
                        Button::new("filter-errors")
                            .ghost()
                            .xsmall()
                            .when(self.problems_show_errors, |b| b.selected(true))
                            .child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(
                                        div()
                                            .size(px(8.0))
                                            .rounded_full()
                                            .bg(theme.danger)
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(
                                                if self.problems_show_errors {
                                                    theme.foreground
                                                } else {
                                                    theme.muted_foreground
                                                }
                                            )
                                            .child(format!("{} Error{}", error_count, if error_count == 1 { "" } else { "s" }))
                                    )
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.problems_show_errors = !this.problems_show_errors;
                                cx.notify();
                            }))
                    })
                    // Warning filter button
                    .child({
                        let warning_count = self.warning_count();
                        Button::new("filter-warnings")
                            .ghost()
                            .xsmall()
                            .when(self.problems_show_warnings, |b| b.selected(true))
                            .child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(
                                        div()
                                            .size(px(8.0))
                                            .rounded_full()
                                            .bg(theme.warning)
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(
                                                if self.problems_show_warnings {
                                                    theme.foreground
                                                } else {
                                                    theme.muted_foreground
                                                }
                                            )
                                            .child(format!("{} Warning{}", warning_count, if warning_count == 1 { "" } else { "s" }))
                                    )
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.problems_show_warnings = !this.problems_show_warnings;
                                cx.notify();
                            }))
                    })
                    // Info filter button
                    .child({
                        let info_count = self.info_count();
                        Button::new("filter-info")
                            .ghost()
                            .xsmall()
                            .when(self.problems_show_info, |b| b.selected(true))
                            .child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(
                                        div()
                                            .size(px(8.0))
                                            .rounded_full()
                                            .bg(theme.info)
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(
                                                if self.problems_show_info {
                                                    theme.foreground
                                                } else {
                                                    theme.muted_foreground
                                                }
                                            )
                                            .child(format!("{} Info", info_count))
                                    )
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.problems_show_info = !this.problems_show_info;
                                cx.notify();
                            }))
                    })
                    // Hint filter button
                    .child({
                        let hint_count = self.hint_count();
                        Button::new("filter-hints")
                            .ghost()
                            .xsmall()
                            .when(self.problems_show_hints, |b| b.selected(true))
                            .child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(
                                        div()
                                            .size(px(8.0))
                                            .rounded_full()
                                            .bg(theme.muted_foreground)
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(
                                                if self.problems_show_hints {
                                                    theme.foreground
                                                } else {
                                                    theme.muted_foreground
                                                }
                                            )
                                            .child(format!("{} Hint{}", hint_count, if hint_count == 1 { "" } else { "s" }))
                                    )
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.problems_show_hints = !this.problems_show_hints;
                                cx.notify();
                            }))
                    })
            )
            // Problems list (filtered)
            .child(
                if filtered_problems.is_empty() {
                    v_flex()
                        .flex_1()
                        .items_center()
                        .justify_center()
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child(
                                    if self.problems.is_empty() {
                                        "No problems detected"
                                    } else {
                                        "No problems match the current filter"
                                    }
                                ),
                        )
                        .into_any_element()
                } else {
                    v_flex()
                        .id("problems-list")
                        .flex_1()
                        .overflow_y_scroll()
                        .children(filtered_problems.iter().enumerate().map(|(idx, problem)| {
                            let line = problem.line + 1; // Convert to 1-indexed for display
                            let col = problem.column + 1;
                            let severity = problem.severity;
                            let message = problem.message.clone();
                            let source = problem.source.clone();

                            h_flex()
                                .id(ElementId::Name(format!("problem-{}", idx).into()))
                                .w_full()
                                .px_3()
                                .py_2()
                                .gap_3()
                                .items_start()
                                .cursor_pointer()
                                .hover(|s| s.bg(theme.muted))
                                .on_click(cx.listener(move |_this, _, _, cx| {
                                    // Emit event to jump to line
                                    cx.emit(ResultsPanelEvent::GoToLine { line, column: col });
                                }))
                                // Severity indicator
                                .child(
                                    div()
                                        .size(px(8.0))
                                        .mt(px(5.0))
                                        .rounded_full()
                                        .bg(match severity {
                                            DiagnosticInfoSeverity::Error => theme.danger,
                                            DiagnosticInfoSeverity::Warning => theme.warning,
                                            DiagnosticInfoSeverity::Info => theme.info,
                                            DiagnosticInfoSeverity::Hint => theme.muted_foreground,
                                        }),
                                )
                                // Message and details
                                .child(
                                    v_flex()
                                        .flex_1()
                                        .gap_1()
                                        .child(div().text_sm().text_color(theme.foreground).child(message))
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child(format!("Ln {}, Col {}", line, col))
                                                .when_some(source, |this, src| {
                                                    this.child(div().child(format!("[{}]", src)))
                                                }),
                                        ),
                                )
                        }))
                        .into_any_element()
                }
            )
            .into_any_element()
    }

    /// Render the sub-tab bar for Explain view
    fn render_explain_sub_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .h(px(28.0))
            .gap_1()
            .px_2()
            .bg(theme.muted)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Button::new("explain-sub-visual")
                    .ghost()
                    .xsmall()
                    .label("Visual")
                    .selected(self.explain_sub_tab == ExplainSubTab::Visual)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Visual;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-plan")
                    .ghost()
                    .xsmall()
                    .label("Plan")
                    .selected(self.explain_sub_tab == ExplainSubTab::Plan)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Plan;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-op")
                    .ghost()
                    .xsmall()
                    .label("Op")
                    .selected(self.explain_sub_tab == ExplainSubTab::Op)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Op;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-statistics")
                    .ghost()
                    .xsmall()
                    .label("Statistics")
                    .selected(self.explain_sub_tab == ExplainSubTab::Statistics)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Statistics;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-info")
                    .ghost()
                    .xsmall()
                    .label("Info")
                    .selected(self.explain_sub_tab == ExplainSubTab::Info)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Info;
                        cx.notify();
                    })),
            )
    }

    /// Render Visual sub-tab with Summary, Plan Tree, and Suggestions
    fn render_explain_visual_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let explain_result = self.explain_results.get(explain_idx);

        if let Some(result) = explain_result {
            if let Some(analysis) = &result.analyzed_plan {
                return v_flex()
                    .size_full()
                    .child(
                        div()
                            .id("explain-visual-content")
                            .flex_1()
                            .w_full()
                            .overflow_y_scroll()
                            .child(
                                v_flex()
                                    .w_full()
                                    // Summary section
                                    .child(self.render_explain_summary(analysis, result, cx))
                                    // Main content: Plan Tree (left) and Suggestions (right)
                                    .child(
                                        h_flex()
                                            .w_full()
                                            .min_h(px(300.0))
                                            .gap_2()
                                            .px_4()
                                            .pb_4()
                                            .child(
                                                v_flex()
                                                    .flex_1()
                                                    .min_w(px(400.0))
                                                    .child(self.render_plan_tree(analysis, cx))
                                            )
                                            .child(
                                                v_flex()
                                                    .w(px(350.0))
                                                    .child(self.render_suggestions(analysis, cx))
                                            )
                                    )
                            )
                    )
                    .into_any_element();
            } else {
                return v_flex()
                    .size_full()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Query plan could not be analyzed")
                    )
                    .into_any_element();
            }
        }

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("No explain result available")
            )
            .into_any_element()
    }

    /// Render the summary section at the top
    fn render_explain_summary(
        &self,
        analysis: &QueryAnalysis,
        result: &ExplainResult,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        
        let total_cost = analysis.plan.total_cost.unwrap_or(0.0);
        let total_rows = analysis.plan.total_rows.unwrap_or(0);
        let execution_time = analysis.plan.execution_time_ms.unwrap_or(result.duration_ms as f64);
        
        // Determine score color
        let score_color = if analysis.performance_score >= 80 {
            theme.success
        } else if analysis.performance_score >= 50 {
            theme.warning
        } else {
            theme.danger
        };

        v_flex()
            .w_full()
            .gap_3()
            .p_4()
            .bg(theme.muted.opacity(0.3))
            .border_b_1()
            .border_color(theme.border)
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child("Query Plan Analysis")
                    )
                    .child(
                        div()
                            .px_2()
                            .py(px(2.0))
                            .rounded(px(4.0))
                            .bg(score_color.opacity(0.2))
                            .border_1()
                            .border_color(score_color)
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(score_color)
                                    .child(format!("Score: {}/100", analysis.performance_score))
                            )
                    )
            )
            .child(
                h_flex()
                    .gap_6()
                    .text_sm()
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_color(theme.muted_foreground)
                                    .child("Total Cost")
                            )
                            .child(
                                div()
                                    .text_color(theme.foreground)
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(format!("{:.2}", total_cost))
                            )
                    )
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_color(theme.muted_foreground)
                                    .child("Estimated Rows")
                            )
                            .child(
                                div()
                                    .text_color(theme.foreground)
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(format!("{}", total_rows))
                            )
                    )
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_color(theme.muted_foreground)
                                    .child("Execution Time")
                            )
                            .child(
                                div()
                                    .text_color(theme.foreground)
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(format!("{:.3}s", execution_time / 1000.0))
                            )
                    )
            )
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child(analysis.summary.clone())
            )
    }

    /// Render the plan tree visualization
    fn render_plan_tree(
        &self,
        analysis: &QueryAnalysis,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child("Execution Plan")
            )
            .child(
                div()
                    .id("plan-tree-content")
                    .flex_1()
                    .p_3()
                    .bg(theme.muted.opacity(0.2))
                    .rounded(px(6.0))
                    .border_1()
                    .border_color(theme.border)
                    .overflow_y_scroll()
                    .child(
                        v_flex()
                            .w_full()
                            .child(self.render_plan_node(&analysis.plan.root, 0, cx))
                    )
            )
    }

    /// Render a single plan node recursively
    fn render_plan_node(
        &self,
        node: &zqlz_analyzer::explain::PlanNode,
        depth: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let indent = depth * 20;
        
        // Node type badge color
        let node_color = if node.is_scan() {
            if node.node_type == zqlz_analyzer::explain::NodeType::SeqScan {
                theme.warning
            } else {
                theme.success
            }
        } else if node.is_join() {
            theme.info
        } else {
            theme.muted_foreground
        };

        // Calculate cost as a heatmap gradient (0-1 scale)
        let cost_intensity = node.cost
            .map(|c| (c.total / 1000.0).min(1.0))
            .unwrap_or(0.0);

        v_flex()
            .gap_1()
            .child(
                h_flex()
                    .gap_2()
                    .pl(px(indent as f32))
                    .py(px(4.0))
                    .items_center()
                    // Node type badge
                    .child(
                        div()
                            .px_2()
                            .py(px(2.0))
                            .rounded(px(4.0))
                            .bg(node_color.opacity(0.2))
                            .border_1()
                            .border_color(node_color)
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::MEDIUM)
                                    .font_family(theme.mono_font_family.clone())
                                    .text_color(node_color)
                                    .child(format!("{:?}", node.node_type))
                            )
                    )
                    // Relation name
                    .when_some(node.relation.as_ref(), |this, relation| {
                        this.child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(theme.foreground)
                                .child(relation.clone())
                        )
                    })
                    // Cost indicator
                    .when_some(node.cost.as_ref(), |this, cost| {
                        this.child(
                            div()
                                .px_2()
                                .py(px(2.0))
                                .rounded(px(3.0))
                                .bg(theme.danger.opacity(cost_intensity as f32 * 0.3))
                                .child(
                                    div()
                                        .text_xs()
                                        .font_family(theme.mono_font_family.clone())
                                        .text_color(theme.muted_foreground)
                                        .child(format!("cost: {:.2}..{:.2}", cost.startup, cost.total))
                                )
                        )
                    })
                    // Row estimate
                    .when_some(node.rows, |this, rows| {
                        this.child(
                            div()
                                .text_xs()
                                .font_family(theme.mono_font_family.clone())
                                .text_color(theme.muted_foreground)
                                .child(format!("rows: {}", rows))
                        )
                    })
            )
            // Filter information
            .when_some(node.filter.as_ref(), |this, filter| {
                this.child(
                    div()
                        .pl(px((indent + 20) as f32))
                        .text_xs()
                        .font_family(theme.mono_font_family.clone())
                        .text_color(theme.muted_foreground)
                        .child(format!("Filter: {}", filter))
                )
            })
            // Child nodes
            .children(node.children.iter().map(|child| {
                self.render_plan_node(child, depth + 1, cx)
            }))
            .into_any_element()
    }

    /// Render the suggestions panel
    fn render_suggestions(
        &self,
        analysis: &QueryAnalysis,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child("Optimization Suggestions")
                    )
                    .child(
                        div()
                            .px_2()
                            .py(px(2.0))
                            .rounded(px(12.0))
                            .bg(theme.muted.opacity(0.5))
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(theme.muted_foreground)
                                    .child(format!("{}", analysis.suggestions.len()))
                            )
                    )
            )
            .child(
                div()
                    .id("suggestions-content")
                    .flex_1()
                    .overflow_y_scroll()
                    .child(
                        v_flex()
                            .w_full()
                            .gap_2()
                            .when(analysis.suggestions.is_empty(), |this| {
                                this.child(
                                    v_flex()
                                        .flex_1()
                                        .items_center()
                                        .justify_center()
                                        .p_4()
                                        .bg(theme.success.opacity(0.1))
                                        .rounded(px(6.0))
                                        .border_1()
                                        .border_color(theme.success)
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.success)
                                                .child("✓ No issues detected - query plan looks optimal!")
                                        )
                                )
                            })
                            .children(
                                analysis.sorted_suggestions().iter().enumerate().map(|(idx, suggestion)| {
                                    let severity_color = match suggestion.severity {
                                        zqlz_analyzer::SeverityLevel::Critical => theme.danger,
                                        zqlz_analyzer::SeverityLevel::Warning => theme.warning,
                                        zqlz_analyzer::SeverityLevel::Info => theme.info,
                                    };

                                    v_flex()
                                        .gap_2()
                                        .p_3()
                                        .bg(severity_color.opacity(0.05))
                                        .rounded(px(6.0))
                                        .border_1()
                                        .border_color(severity_color.opacity(0.3))
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .items_center()
                                                .child(
                                                    div()
                                                        .px_2()
                                                        .py(px(2.0))
                                                        .rounded(px(3.0))
                                                        .bg(severity_color.opacity(0.2))
                                                        .child(
                                                            div()
                                                                .text_xs()
                                                                .font_weight(FontWeight::BOLD)
                                                                .text_color(severity_color)
                                                                .child(suggestion.severity.as_str().to_uppercase())
                                                        )
                                                )
                                                .when_some(suggestion.table.as_ref(), |this, table| {
                                                    this.child(
                                                        div()
                                                            .text_xs()
                                                            .font_family(theme.mono_font_family.clone())
                                                            .text_color(theme.muted_foreground)
                                                            .child(table.clone())
                                                    )
                                                })
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::MEDIUM)
                                                .text_color(theme.foreground)
                                                .child(suggestion.message.clone())
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.muted_foreground)
                                                .child(suggestion.recommendation.clone())
                                        )
                                        .when(!suggestion.columns.is_empty(), |this| {
                                            this.child(
                                                h_flex()
                                                    .gap_1()
                                                    .flex_wrap()
                                                    .children(suggestion.columns.iter().map(|col| {
                                                        div()
                                                            .px_2()
                                                            .py(px(2.0))
                                                            .rounded(px(3.0))
                                                            .bg(theme.muted.opacity(0.3))
                                                            .child(
                                                                div()
                                                                    .text_xs()
                                                                    .font_family(theme.mono_font_family.clone())
                                                                    .text_color(theme.foreground)
                                                                    .child(col.clone())
                                                            )
                                                    }))
                                            )
                                        })
                                })
                            )
                    )
            )
    }

    /// Render Plan sub-tab (EXPLAIN QUERY PLAN output table)
    fn render_explain_plan_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if explain_idx < self.explain_plan_table_states.len() {
            let table_state = &self.explain_plan_table_states[explain_idx];
            div()
                .size_full()
                .child(Table::new(table_state).stripe(true))
                .into_any_element()
        } else {
            self.render_explain_no_data("No query plan data available", cx)
                .into_any_element()
        }
    }

    /// Render Op sub-tab (EXPLAIN opcodes table)
    fn render_explain_op_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if explain_idx < self.explain_op_table_states.len() {
            let table_state = &self.explain_op_table_states[explain_idx];
            div()
                .size_full()
                .child(Table::new(table_state).stripe(true))
                .into_any_element()
        } else {
            self.render_explain_no_data("No opcode data available", cx)
                .into_any_element()
        }
    }

    /// Render Statistics sub-tab (placeholder)
    fn render_explain_statistics_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let explain_result = self.explain_results.get(explain_idx);

        v_flex()
            .size_full()
            .p_4()
            .gap_3()
            .text_sm()
            .when_some(explain_result, |this, result| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_color(theme.muted_foreground)
                                .child("Execution Time:"),
                        )
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .child(format!("{:.3}s", result.duration_ms as f64 / 1000.0)),
                        ),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(div().text_color(theme.muted_foreground).child("Opcodes:"))
                        .child(
                            div().text_color(theme.foreground).child(
                                result
                                    .raw_output
                                    .as_ref()
                                    .map(|r| r.rows.len().to_string())
                                    .unwrap_or_else(|| "N/A".to_string()),
                            ),
                        ),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_color(theme.muted_foreground)
                                .child("Plan Steps:"),
                        )
                        .child(
                            div().text_color(theme.foreground).child(
                                result
                                    .query_plan
                                    .as_ref()
                                    .map(|r| r.rows.len().to_string())
                                    .unwrap_or_else(|| "N/A".to_string()),
                            ),
                        ),
                )
            })
            .when(explain_result.is_none(), |this| {
                this.child(
                    div()
                        .text_color(theme.muted_foreground)
                        .child("No statistics available"),
                )
            })
    }

    /// Render Info sub-tab (metadata about the explain)
    fn render_explain_info_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let explain_result = self.explain_results.get(explain_idx);

        v_flex()
            .size_full()
            .p_4()
            .gap_3()
            .text_sm()
            .when_some(explain_result, |this, result| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(div().text_color(theme.muted_foreground).child("Timestamp:"))
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .child(result.timestamp.format("%Y-%m-%d %H:%M:%S").to_string()),
                        ),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_color(theme.muted_foreground)
                                .child("Connection:"),
                        )
                        .child(
                            div().text_color(theme.foreground).child(
                                result
                                    .connection_name
                                    .clone()
                                    .unwrap_or_else(|| "Unknown".to_string()),
                            ),
                        ),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(div().text_color(theme.muted_foreground).child("Database:"))
                        .child(
                            div().text_color(theme.foreground).child(
                                result
                                    .database_name
                                    .clone()
                                    .unwrap_or_else(|| "N/A".to_string()),
                            ),
                        ),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_color(theme.muted_foreground)
                                .child("Execution Time:"),
                        )
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .child(format!("{:.3}s", result.duration_ms as f64 / 1000.0)),
                        ),
                )
                .child(div().h(px(1.0)).w_full().bg(theme.border))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().text_color(theme.muted_foreground).child("Query:"))
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .font_family(theme.mono_font_family.clone())
                                .p_2()
                                .bg(theme.muted)
                                .rounded(px(4.0))
                                .child(result.sql.clone()),
                        ),
                )
                .when_some(result.error.as_ref(), |this, error| {
                    this.child(div().h(px(1.0)).w_full().bg(theme.border))
                        .child(
                            v_flex()
                                .gap_2()
                                .child(div().text_color(theme.danger).child("Error:"))
                                .child(
                                    div()
                                        .text_color(theme.danger)
                                        .font_family(theme.mono_font_family.clone())
                                        .p_2()
                                        .bg(theme.muted)
                                        .rounded(px(4.0))
                                        .child(error.clone()),
                                ),
                        )
                })
            })
            .when(explain_result.is_none(), |this| {
                this.child(
                    div()
                        .text_color(theme.muted_foreground)
                        .child("No explain result available"),
                )
            })
    }

    /// Render a simple "no data" message for explain views
    fn render_explain_no_data(&self, message: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child(message.to_string()),
        )
    }
}

impl Render for ResultsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("results-panel")
            .key_context("ResultsPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(self.render_tab_bar(cx))
            .child(div().flex_1().w_full().overflow_hidden().map(|this| {
                if self.is_loading {
                    return this.child(self.render_loading(cx));
                }

                // Handle Explain tabs first (they don't require execution)
                if let ResultTab::Explain(idx) = self.active_tab {
                    return this.child(self.render_explain_tab(idx, cx));
                }

                // Handle Problems tab (doesn't require execution)
                if self.active_tab == ResultTab::Problems {
                    return this.child(self.render_problems_tab(cx));
                }

                // Handle execution-related tabs
                if let Some(exec) = &self.execution {
                    match self.active_tab {
                        ResultTab::Message => this.child(self.render_message_tab(exec, cx)),
                        ResultTab::Summary => this.child(self.render_summary_tab(exec, cx)),
                        ResultTab::Result(idx) => this.child(self.render_result_tab(idx, cx)),
                        ResultTab::Info => this.child(self.render_info_tab(exec, cx)),
                        ResultTab::Explain(_) | ResultTab::Problems => unreachable!(), // Already handled above
                    }
                } else {
                    this.child(self.render_empty_state(cx))
                }
            }))
    }
}

impl Focusable for ResultsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ResultsPanel {}
impl EventEmitter<ResultsPanelEvent> for ResultsPanel {}

impl Panel for ResultsPanel {
    fn panel_name(&self) -> &'static str {
        "ResultsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Query Results"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
