//! Results panel with multi-tab view
//!
//! Displays query results in tabs: Message, Summary, Result, Explain, Info

use std::{
    ops::Range,
    time::{Duration, Instant},
};

use gpui::StatefulInteractiveElement as _;
use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_analyzer::QueryAnalysis;
use zqlz_core::QueryResult;
use zqlz_explain_visual::{ExplainGraphInput, ExplainGraphState, ExplainGraphView};
use zqlz_ui::widgets::{
    ActiveTheme, Selectable, Sizable,
    button::{Button, ButtonRounded, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    scroll::{ScrollableElement, Scrollbar, ScrollbarShow},
    table::{Column, ColumnSort, Table, TableDelegate, TableState},
    v_flex,
};

use super::DiagnosticInfo;
use super::explain_analysis_view::ExplainAnalysisView;

const RESULTS_MAX_MATERIALIZED_ROWS: usize = 20_000;
const RESULTS_LIST_SCROLLBAR_WIDTH: f32 = 16.0;
const RESULTS_MESSAGE_ROW_HEIGHT: f32 = 34.0;
const RESULTS_LOADING_TICK_MS: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResultTab {
    Message,
    Summary,
    Result(usize),  // Index of the result to show
    Explain(usize), // Index of the explain result to show
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
    Suggestions,
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
    /// Dialect/provider id used to choose EXPLAIN syntax and parser behavior
    pub provider_id: Option<String>,
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
    source_row_count: usize,
    truncated: bool,
}

impl ResultsTableDelegate {
    fn new(result: &QueryResult) -> Self {
        Self::new_with_content_widths(result, false)
    }

    fn new_plan(result: &QueryResult) -> Self {
        Self::new_with_content_widths(result, true)
    }

    fn new_with_content_widths(result: &QueryResult, fit_content: bool) -> Self {
        let source_row_count = result.rows.len();
        let truncated = source_row_count > RESULTS_MAX_MATERIALIZED_ROWS;
        let rows: Vec<Vec<String>> = result
            .rows
            .iter()
            .take(RESULTS_MAX_MATERIALIZED_ROWS)
            .map(|row| row.values.iter().map(|val| val.to_string()).collect())
            .collect();

        // Create row number column as first column (fixed left)
        // Width scales with digit count so large row numbers aren't truncated
        let visible_row_count = source_row_count.min(RESULTS_MAX_MATERIALIZED_ROWS);
        let row_num_width = Self::row_number_column_width(visible_row_count);
        let mut columns: Vec<Column> = vec![
            Column::new("row-num", "#")
                .width(row_num_width)
                .fixed(zqlz_ui::widgets::table::ColumnFixed::Left),
        ];

        // Add data columns
        columns.extend(result.columns.iter().enumerate().map(|(idx, col_meta)| {
            let width = if fit_content {
                Self::content_column_width(
                    &col_meta.name,
                    rows.iter().filter_map(|row| row.get(idx)),
                )
            } else {
                150.0
            };

            Column::new(format!("col-{}", idx), col_meta.name.clone())
                .width(width)
                .resizable(true)
                .sortable()
        }));

        Self {
            columns,
            rows,
            source_row_count,
            truncated,
        }
    }

    fn is_truncated(&self) -> bool {
        self.truncated
    }

    fn source_row_count(&self) -> usize {
        self.source_row_count
    }

    fn content_column_width<'a>(header: &str, values: impl Iterator<Item = &'a String>) -> f32 {
        let max_chars = values
            .map(|value| value.chars().count())
            .chain(std::iter::once(header.chars().count()))
            .max()
            .unwrap_or(12);
        ((max_chars as f32 * 13.0) + 72.0).clamp(220.0, 3200.0)
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

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        if col_ix == 0 {
            return (row_ix + 1).to_string();
        }

        self.rows
            .get(row_ix)
            .and_then(|row| row.get(col_ix.saturating_sub(1)))
            .cloned()
            .unwrap_or_default()
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

    /// Scroll handle for the Message tab statement list.
    message_scroll_handle: UniformListScrollHandle,

    /// Scroll handle for top result/explain tab overflow.
    tab_bar_scroll_handle: ScrollHandle,

    /// Current query execution data
    execution: Option<QueryExecution>,

    /// Table states for result grids (one per statement with results, lazily created)
    table_states: Vec<Option<Entity<TableState<ResultsTableDelegate>>>>,

    /// Explain results (one per EXPLAIN executed)
    explain_results: Vec<ExplainResult>,

    /// Table states for explain Op view (raw EXPLAIN output, lazily created)
    explain_op_table_states: Vec<Option<Entity<TableState<ResultsTableDelegate>>>>,

    /// Table states for explain Plan view (EXPLAIN QUERY PLAN output, lazily created)
    explain_plan_table_states: Vec<Option<Entity<TableState<ResultsTableDelegate>>>>,

    /// Visual graph states for parsed EXPLAIN output.
    explain_graph_states: Vec<Option<Entity<ExplainGraphState>>>,

    /// Active sub-tab for Explain view
    explain_sub_tab: ExplainSubTab,

    /// Active tab
    active_tab: ResultTab,

    /// Whether results are loading
    is_loading: bool,

    /// Query loading start time for live elapsed feedback.
    loading_started_at: Option<Instant>,

    /// Periodic re-render task for live loading feedback.
    loading_timer_task: Option<Task<()>>,

    /// The currently active editor index (for scoping diagnostics)
    active_editor_id: Option<usize>,

    /// Diagnostics loading state (for reload button)
    diagnostics_loading: bool,

    /// Set of closed result tab indices (statement indices that have been closed by user)
    closed_result_tabs: std::collections::HashSet<usize>,
}

impl ResultsPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            message_scroll_handle: UniformListScrollHandle::new(),
            tab_bar_scroll_handle: ScrollHandle::new(),
            execution: None,
            table_states: Vec::new(),
            explain_results: Vec::new(),
            explain_op_table_states: Vec::new(),
            explain_plan_table_states: Vec::new(),
            explain_graph_states: Vec::new(),
            explain_sub_tab: ExplainSubTab::Plan,
            active_tab: ResultTab::Message,
            is_loading: false,
            loading_started_at: None,
            loading_timer_task: None,
            active_editor_id: None,
            diagnostics_loading: false,
            closed_result_tabs: std::collections::HashSet::new(),
        }
    }

    /// Set the active editor ID - diagnostics will be scoped to this editor
    pub fn set_active_editor_id(&mut self, editor_id: Option<usize>, cx: &mut Context<Self>) {
        if self.active_editor_id != editor_id {
            self.active_editor_id = editor_id;
            tracing::debug!("ResultsPanel: active editor changed to {:?}", editor_id);
            cx.notify();
        }
    }

    /// Get the active editor ID
    pub fn active_editor_id(&self) -> Option<usize> {
        self.active_editor_id
    }

    /// Accept diagnostics updates for compatibility; the Problems panel owns display.
    pub fn set_problems(&mut self, _problems: Vec<DiagnosticInfo>, cx: &mut Context<Self>) {
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

    /// Set the query execution result
    pub fn set_execution(
        &mut self,
        execution: QueryExecution,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Reset lazy table state slots for statements with results
        self.table_states.clear();

        // Reset closed tabs when new execution arrives
        self.closed_result_tabs.clear();

        for statement in &execution.statements {
            if let Some(result) = &statement.result {
                let _ = result;
                self.table_states.push(None);
            }
        }

        self.execution = Some(execution);
        self.message_scroll_handle = UniformListScrollHandle::new();
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
        // Reserve lazy slots for explain tables (keep indices aligned with explain_results)
        self.explain_op_table_states.push(None);
        self.explain_plan_table_states.push(None);
        self.explain_graph_states.push(None);

        let explain_idx = self.explain_results.len();
        let has_visual_plan = result.analyzed_plan.is_some();
        self.explain_results.push(result);
        self.is_loading = false;
        self.active_tab = ResultTab::Explain(explain_idx);
        self.explain_sub_tab = if has_visual_plan {
            ExplainSubTab::Visual
        } else {
            ExplainSubTab::Plan
        };
        self.ensure_explain_plan_table_state(explain_idx, window, cx);
        self.ensure_explain_graph_state(explain_idx, cx);
        cx.notify();
    }

    /// Set loading state
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.is_loading = loading;
        if loading {
            self.loading_started_at = Some(Instant::now());
            self.start_loading_timer(cx);
        } else {
            self.loading_started_at = None;
            self.loading_timer_task = None;
        }
        cx.notify();
    }

    fn start_loading_timer(&mut self, cx: &mut Context<Self>) {
        self.loading_timer_task = Some(cx.spawn(async move |this, cx| {
            loop {
                smol::Timer::after(Duration::from_millis(RESULTS_LOADING_TICK_MS)).await;
                let should_continue = match this.update(cx, |panel, cx| {
                    if panel.is_loading {
                        cx.notify();
                        true
                    } else {
                        false
                    }
                }) {
                    Ok(should_continue) => should_continue,
                    Err(error) => {
                        tracing::debug!(%error, "Stopped results-panel loading timer after panel dropped");
                        false
                    }
                };

                if !should_continue {
                    break;
                }
            }
        }));
    }

    fn loading_elapsed_seconds(&self) -> f64 {
        self.loading_started_at
            .map(|started_at| started_at.elapsed().as_secs_f64())
            .unwrap_or_default()
    }

    /// Clear results
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.execution = None;
        self.message_scroll_handle = UniformListScrollHandle::new();
        self.table_states.clear();
        self.explain_results.clear();
        self.explain_op_table_states.clear();
        self.explain_plan_table_states.clear();
        self.explain_graph_states.clear();
        self.closed_result_tabs.clear();
        self.is_loading = false;
        self.loading_started_at = None;
        self.loading_timer_task = None;
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

    fn close_explain_tab(&mut self, idx: usize, cx: &mut Context<Self>) {
        if idx >= self.explain_results.len() {
            return;
        }

        self.explain_results.remove(idx);
        if idx < self.explain_op_table_states.len() {
            self.explain_op_table_states.remove(idx);
        }
        if idx < self.explain_plan_table_states.len() {
            self.explain_plan_table_states.remove(idx);
        }
        if idx < self.explain_graph_states.len() {
            self.explain_graph_states.remove(idx);
        }

        self.active_tab = match self.active_tab.clone() {
            ResultTab::Explain(active_idx) if active_idx == idx => {
                if self.explain_results.is_empty() {
                    ResultTab::Summary
                } else if idx >= self.explain_results.len() {
                    ResultTab::Explain(self.explain_results.len() - 1)
                } else {
                    ResultTab::Explain(idx)
                }
            }
            ResultTab::Explain(active_idx) if active_idx > idx => {
                ResultTab::Explain(active_idx - 1)
            }
            other => other,
        };

        cx.notify();
    }

    fn result_table_slot_for_statement(&self, statement_idx: usize) -> Option<usize> {
        let mut table_state_idx = 0;
        if let Some(exec) = &self.execution {
            for (idx, statement) in exec.statements.iter().enumerate() {
                if statement.result.is_some() {
                    if idx == statement_idx {
                        return Some(table_state_idx);
                    }
                    table_state_idx += 1;
                }
            }
        }
        None
    }

    fn ensure_result_table_state(
        &mut self,
        statement_idx: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state_idx) = self.result_table_slot_for_statement(statement_idx) else {
            return;
        };

        if table_state_idx >= self.table_states.len()
            || self.table_states[table_state_idx].is_some()
        {
            return;
        }

        let Some(result) = self
            .execution
            .as_ref()
            .and_then(|exec| exec.statements.get(statement_idx))
            .and_then(|statement| statement.result.as_ref())
        else {
            return;
        };

        let delegate = ResultsTableDelegate::new(result);
        let table_state = cx.new(|cx| {
            TableState::new(delegate, window, cx)
                .col_resizable(true)
                .sortable(true)
                .row_selectable(true)
        });
        self.table_states[table_state_idx] = Some(table_state);
    }

    fn ensure_explain_plan_table_state(
        &mut self,
        explain_idx: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if explain_idx >= self.explain_plan_table_states.len()
            || self.explain_plan_table_states[explain_idx].is_some()
        {
            return;
        }

        let Some(query_plan) = self
            .explain_results
            .get(explain_idx)
            .and_then(|result| result.query_plan.as_ref())
        else {
            return;
        };

        let delegate = ResultsTableDelegate::new_plan(query_plan);
        let table_state = cx.new(|cx| {
            TableState::new(delegate, window, cx)
                .col_resizable(true)
                .sortable(true)
                .row_selectable(true)
        });
        self.explain_plan_table_states[explain_idx] = Some(table_state);
    }

    fn ensure_explain_op_table_state(
        &mut self,
        explain_idx: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if explain_idx >= self.explain_op_table_states.len()
            || self.explain_op_table_states[explain_idx].is_some()
        {
            return;
        }

        let Some(raw_output) = self
            .explain_results
            .get(explain_idx)
            .and_then(|result| result.raw_output.as_ref())
        else {
            return;
        };

        let delegate = ResultsTableDelegate::new(raw_output);
        let table_state = cx.new(|cx| {
            TableState::new(delegate, window, cx)
                .col_resizable(true)
                .sortable(true)
                .row_selectable(true)
        });
        self.explain_op_table_states[explain_idx] = Some(table_state);
    }

    fn ensure_explain_graph_state(&mut self, explain_idx: usize, cx: &mut Context<Self>) {
        if explain_idx >= self.explain_graph_states.len()
            || self.explain_graph_states[explain_idx].is_some()
        {
            return;
        }

        let Some(result) = self.explain_results.get(explain_idx) else {
            return;
        };
        let Some(analysis) = result.analyzed_plan.clone() else {
            return;
        };

        let input = ExplainGraphInput {
            provider_id: result.provider_id.clone(),
            connection_name: result.connection_name.clone(),
            duration_ms: result.duration_ms,
        };
        let state = cx.new(|_| ExplainGraphState::new(analysis, input));
        self.explain_graph_states[explain_idx] = Some(state);
    }

    /// Get the current result being displayed (if any)
    fn get_current_result(&self) -> Option<(usize, &StatementResult)> {
        if let ResultTab::Result(idx) = self.active_tab
            && let Some(exec) = &self.execution
            && let Some(statement) = exec.statements.get(idx)
            && statement.result.is_some()
        {
            return Some((idx, statement));
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
                if statement.affected_rows == 1 {
                    ""
                } else {
                    "s"
                }
            )
        } else {
            // DDL or other statement without row counts
            format!("Statement {} - {}", idx + 1, duration_str)
        }
    }

    /// Return the first SQL line, truncated to a fixed character count for compact row rendering.
    fn first_line_preview(sql: &str, max_chars: usize) -> String {
        let first_line = sql.lines().next().unwrap_or(sql);
        let mut chars = first_line.chars();
        let preview: String = chars.by_ref().take(max_chars).collect();

        if chars.next().is_some() {
            format!("{preview}...")
        } else {
            first_line.to_string()
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
            .flex_none()
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
                    .rounded(ButtonRounded::None)
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
                    .rounded(ButtonRounded::None)
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
                                    .rounded(ButtonRounded::None)
                                    .label(format!("Result {}", idx + 1))
                                    .selected(self.active_tab == ResultTab::Result(result_idx))
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.ensure_result_table_state(result_idx, window, cx);
                                        this.active_tab = ResultTab::Result(result_idx);
                                        cx.notify();
                                    })),
                            )
                            .child(
                                Button::new(format!("close-result-{}", idx))
                                    .ghost()
                                    .xsmall()
                                    .rounded(ButtonRounded::None)
                                    .label("×")
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.close_result_tab(result_idx, cx);

                                        if let ResultTab::Result(new_result_idx) = this.active_tab {
                                            this.ensure_result_table_state(
                                                new_result_idx,
                                                window,
                                                cx,
                                            );
                                        }
                                    })),
                            ),
                    );
                }
            }
        }

        // Add a tab for each explain result
        for (idx, _explain) in self.explain_results.iter().enumerate() {
            let explain_idx = idx;
            tab_bar = tab_bar.child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        Button::new(format!("tab-explain-{}", idx))
                            .ghost()
                            .xsmall()
                            .rounded(ButtonRounded::None)
                            .label(format!("Explain {}", idx + 1))
                            .selected(self.active_tab == ResultTab::Explain(explain_idx))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.ensure_explain_plan_table_state(explain_idx, window, cx);
                                this.active_tab = ResultTab::Explain(explain_idx);
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new(format!("close-explain-{}", idx))
                            .ghost()
                            .xsmall()
                            .rounded(ButtonRounded::None)
                            .label("×")
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.close_explain_tab(explain_idx, cx);
                            })),
                    ),
            );
        }

        // Add Info tab
        tab_bar = tab_bar.child(
            Button::new("tab-info")
                .ghost()
                .xsmall()
                .rounded(ButtonRounded::None)
                .label("Info")
                .selected(self.active_tab == ResultTab::Info)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.active_tab = ResultTab::Info;
                    cx.notify();
                })),
        );

        // Add spacer
        tab_bar = tab_bar.child(div().flex_1());

        // Add export buttons when a Result tab is active
        let is_result_tab = matches!(self.active_tab, ResultTab::Result(_));
        if is_result_tab && self.get_current_result().is_some() {
            tab_bar = tab_bar
                .child(
                    Button::new("export-csv")
                        .ghost()
                        .xsmall()
                        .rounded(ButtonRounded::None)
                        .label("Export CSV")
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export_csv(cx);
                        })),
                )
                .child(
                    Button::new("export-json")
                        .ghost()
                        .xsmall()
                        .rounded(ButtonRounded::None)
                        .label("Export JSON")
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.export_json(cx);
                        })),
                );
        }

        div()
            .id("results-tab-bar-scroll")
            .w_full()
            .h(px(32.0))
            .track_scroll(&self.tab_bar_scroll_handle)
            .overflow_x_scrollbar()
            .child(tab_bar)
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

        v_flex()
            .size_full()
            .child(
                v_flex()
                    .id("message-content")
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(
                        v_flex()
                            .w_full()
                            .px_4()
                            .pt_4()
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
                                                exec.start_time
                                                    .format("%Y-%m-%d %H:%M:%S")
                                                    .to_string(),
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
                                                exec.end_time
                                                    .format("%Y-%m-%d %H:%M:%S")
                                                    .to_string(),
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
                                            .child(div().text_color(theme.foreground).child(
                                                format!("{:.3}s", exec.duration_ms as f64 / 1000.0),
                                            )),
                                    ),
                            )
                            .child(div().h(px(1.0)).w_full().bg(theme.border))
                            .child(
                                h_flex()
                                    .w_full()
                                    .h(px(RESULTS_MESSAGE_ROW_HEIGHT))
                                    .px_3()
                                    .gap_8()
                                    .items_center()
                                    .text_sm()
                                    .border_b_1()
                                    .border_color(theme.border)
                                    .child(
                                        h_flex().min_w(px(400.0)).child(
                                            div().text_color(theme.muted_foreground).child("Query"),
                                        ),
                                    )
                                    .child(
                                        h_flex().min_w(px(120.0)).child(
                                            div().text_color(theme.muted_foreground).child("Message"),
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
                            ),
                    )
                    .child(
                        div()
                            .id("message-statements-container")
                            .flex_1()
                            .w_full()
                            .relative()
                            .overflow_hidden()
                            .when(total_count > 0, |this| {
                                this.child(
                                    uniform_list(
                                        "message-statements-list",
                                        total_count,
                                        cx.processor(
                                            move |state: &mut ResultsPanel,
                                                  visible_range: Range<usize>,
                                                  _window,
                                                  cx| {
                                                let Some(execution) = state.execution.as_ref() else {
                                                    return Vec::new();
                                                };

                                                let total_rows = execution.statements.len();
                                                let start = visible_range.start.min(total_rows);
                                                let end = visible_range.end.min(total_rows);

                                                if visible_range.end > total_rows {
                                                    tracing::debug!(
                                                        ?visible_range,
                                                        total_rows,
                                                        "Message list visible range exceeded available rows"
                                                    );
                                                }

                                                let theme = cx.theme();

                                                (start..end)
                                                    .filter_map(|index| {
                                                        let statement = execution.statements.get(index)?;
                                                        let sql_preview =
                                                            Self::first_line_preview(&statement.sql, 60);

                                                        Some(
                                                            h_flex()
                                                                .id(index)
                                                                .w_full()
                                                                .h(px(RESULTS_MESSAGE_ROW_HEIGHT))
                                                                .px_3()
                                                                .gap_8()
                                                                .items_center()
                                                                .border_b_1()
                                                                .border_color(theme.border)
                                                                .when(index % 2 == 1, |this| {
                                                                    this.bg(theme.muted.opacity(0.18))
                                                                })
                                                                .hover(|this| {
                                                                    this.bg(theme.muted.opacity(0.32))
                                                                })
                                                                .child(
                                                                    h_flex()
                                                                        .min_w(px(400.0))
                                                                        .child(
                                                                            div()
                                                                                .text_color(theme.foreground)
                                                                                .overflow_hidden()
                                                                                .text_ellipsis()
                                                                                .whitespace_nowrap()
                                                                                .child(sql_preview),
                                                                        ),
                                                                )
                                                                .child(
                                                                    h_flex().min_w(px(120.0)).child(
                                                                        div()
                                                                            .text_color(if statement
                                                                                .error
                                                                                .is_none()
                                                                            {
                                                                                theme.success
                                                                            } else {
                                                                                theme.danger
                                                                            })
                                                                            .child(if statement
                                                                                .error
                                                                                .is_none()
                                                                            {
                                                                                "OK"
                                                                            } else {
                                                                                "Error"
                                                                            }),
                                                                    ),
                                                                )
                                                                .child(
                                                                    h_flex().min_w(px(120.0)).child(
                                                                        div().text_color(theme.foreground).child(
                                                                            format!(
                                                                                "{:.3}s",
                                                                                statement.duration_ms
                                                                                    as f64
                                                                                    / 1000.0
                                                                            ),
                                                                        ),
                                                                    ),
                                                                )
                                                                .child(
                                                                    h_flex().min_w(px(120.0)).child(
                                                                        div()
                                                                            .text_color(theme.foreground)
                                                                            .child("0.000s"),
                                                                    ),
                                                                )
                                                                .into_any_element(),
                                                        )
                                                    })
                                                    .collect::<Vec<_>>()
                                            },
                                        ),
                                    )
                                    .flex_grow()
                                    .size_full()
                                    .pr(px(RESULTS_LIST_SCROLLBAR_WIDTH))
                                    .track_scroll(&self.message_scroll_handle)
                                    .with_sizing_behavior(ListSizingBehavior::Auto)
                                    .into_any_element(),
                                )
                            })
                            .when(total_count > 0, |this| {
                                this.child(
                                    div()
                                        .absolute()
                                        .top_0()
                                        .right_0()
                                        .bottom_0()
                                        .w(px(RESULTS_LIST_SCROLLBAR_WIDTH))
                                        .child(
                                            Scrollbar::vertical(&self.message_scroll_handle)
                                                .scrollbar_show(ScrollbarShow::Always),
                                        ),
                                )
                            }),
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
                                            .border_1()
                                            .border_color(theme.border.opacity(0.6))
                                            .child(
                                                v_flex()
                                                    .gap_1()
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(theme.muted_foreground)
                                                            .child(
                                                                Self::format_statement_metadata(
                                                                    idx, statement,
                                                                ),
                                                            ),
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
            let Some(table_state) = self.table_states[table_state_idx].as_ref() else {
                return div()
                    .size_full()
                    .p_4()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("Preparing result table…")
                    .into_any_element();
            };

            let (is_truncated, source_row_count, visible_row_count) = {
                let state = table_state.read(cx);
                let delegate = state.delegate();
                (
                    delegate.is_truncated(),
                    delegate.source_row_count(),
                    delegate.rows_count(cx),
                )
            };

            div()
                .size_full()
                .when(is_truncated, |this| {
                    let theme = cx.theme();
                    this.child(
                        div()
                            .w_full()
                            .px_3()
                            .py_2()
                            .text_xs()
                            .bg(theme.warning.opacity(0.12))
                            .text_color(theme.warning)
                            .child(format!(
                                "Showing first {} rows of {} to keep results responsive",
                                visible_row_count, source_row_count
                            )),
                    )
                })
                .child(Table::new(table_state).stripe(true))
                .into_any_element()
        } else {
            self.render_empty_state(cx).into_any_element()
        }
    }

    /// Render Info tab
    fn render_info_tab(&self, exec: &QueryExecution, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div().size_full().child(
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
                                .border_1()
                                .border_color(theme.border.opacity(0.6))
                                .child(exec.sql.clone()),
                        ),
                )
                .overflow_y_scrollbar(),
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
        let elapsed_seconds = self.loading_elapsed_seconds();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_3()
            .child(
                v_flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child("Executing query..."),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("Elapsed {:.1}s", elapsed_seconds)),
                    ),
            )
            .child(
                div()
                    .w(px(240.0))
                    .h(px(4.0))
                    .rounded_full()
                    .overflow_hidden()
                    .bg(theme.muted)
                    .child(
                        div()
                            .h_full()
                            .w(relative(0.35))
                            .rounded_full()
                            .bg(theme.accent)
                            .map(|this| {
                                let offset = ((elapsed_seconds * 0.8).sin() as f32 + 1.0) * 0.325;
                                this.ml(relative(offset))
                            }),
                    ),
            )
    }

    /// Render the Explain tab (sub-tab bar + content)
    fn render_explain_tab(&self, explain_idx: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let active_sub_tab = if self.explain_sub_tab == ExplainSubTab::Op
            && !self.explain_supports_op(explain_idx)
        {
            ExplainSubTab::Plan
        } else {
            self.explain_sub_tab
        };

        v_flex()
            .size_full()
            .child(self.render_explain_sub_tab_bar(explain_idx, cx))
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .map(|this| match active_sub_tab {
                        ExplainSubTab::Visual => {
                            this.child(self.render_explain_visual_view(explain_idx, cx))
                        }
                        ExplainSubTab::Plan => {
                            this.child(self.render_explain_plan_view(explain_idx, cx))
                        }
                        ExplainSubTab::Op => {
                            this.child(self.render_explain_op_view(explain_idx, cx))
                        }
                        ExplainSubTab::Statistics => {
                            this.child(self.render_explain_statistics_view(explain_idx, cx))
                        }
                        ExplainSubTab::Suggestions => {
                            this.child(self.render_explain_suggestions_view(explain_idx, cx))
                        }
                        ExplainSubTab::Info => {
                            this.child(self.render_explain_info_view(explain_idx, cx))
                        }
                    }),
            )
    }

    /// Render the sub-tab bar for Explain view
    fn render_explain_sub_tab_bar(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let supports_op = self.explain_supports_op(explain_idx);

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
                    .rounded(ButtonRounded::None)
                    .label("Visual")
                    .selected(self.explain_sub_tab == ExplainSubTab::Visual)
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let ResultTab::Explain(explain_idx) = this.active_tab {
                            this.ensure_explain_graph_state(explain_idx, cx);
                        }
                        this.explain_sub_tab = ExplainSubTab::Visual;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-plan")
                    .ghost()
                    .xsmall()
                    .rounded(ButtonRounded::None)
                    .label("Plan")
                    .selected(self.explain_sub_tab == ExplainSubTab::Plan)
                    .on_click(cx.listener(|this, _, window, cx| {
                        if let ResultTab::Explain(explain_idx) = this.active_tab {
                            this.ensure_explain_plan_table_state(explain_idx, window, cx);
                        }
                        this.explain_sub_tab = ExplainSubTab::Plan;
                        cx.notify();
                    })),
            )
            .when(supports_op, |this| {
                this.child(
                    Button::new("explain-sub-op")
                        .ghost()
                        .xsmall()
                        .rounded(ButtonRounded::None)
                        .label("Op")
                        .selected(self.explain_sub_tab == ExplainSubTab::Op)
                        .on_click(cx.listener(|this, _, window, cx| {
                            if let ResultTab::Explain(explain_idx) = this.active_tab {
                                this.ensure_explain_op_table_state(explain_idx, window, cx);
                            }
                            this.explain_sub_tab = ExplainSubTab::Op;
                            cx.notify();
                        })),
                )
            })
            .child(
                Button::new("explain-sub-statistics")
                    .ghost()
                    .xsmall()
                    .rounded(ButtonRounded::None)
                    .label("Statistics")
                    .selected(self.explain_sub_tab == ExplainSubTab::Statistics)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Statistics;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-suggestions")
                    .ghost()
                    .xsmall()
                    .rounded(ButtonRounded::None)
                    .label("Suggestions")
                    .selected(self.explain_sub_tab == ExplainSubTab::Suggestions)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Suggestions;
                        cx.notify();
                    })),
            )
            .child(
                Button::new("explain-sub-info")
                    .ghost()
                    .xsmall()
                    .rounded(ButtonRounded::None)
                    .label("Info")
                    .selected(self.explain_sub_tab == ExplainSubTab::Info)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.explain_sub_tab = ExplainSubTab::Info;
                        cx.notify();
                    })),
            )
    }

    fn explain_supports_op(&self, explain_idx: usize) -> bool {
        self.explain_results
            .get(explain_idx)
            .is_some_and(|result| result.raw_output.is_some())
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
                if let Some(Some(state)) = self.explain_graph_states.get(explain_idx) {
                    return ExplainGraphView::new(state.clone()).render(cx);
                }

                return ExplainAnalysisView::new(result, analysis).render_visual(cx);
            } else {
                return v_flex()
                    .size_full()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Query plan could not be analyzed"),
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
                    .child("No explain result available"),
            )
            .into_any_element()
    }

    /// Render Plan sub-tab (EXPLAIN QUERY PLAN output table)
    fn render_explain_plan_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if explain_idx < self.explain_plan_table_states.len() {
            let Some(table_state) = self.explain_plan_table_states[explain_idx].as_ref() else {
                if self
                    .explain_results
                    .get(explain_idx)
                    .and_then(|result| result.query_plan.as_ref())
                    .is_none()
                {
                    return self
                        .render_explain_no_data("No query plan data available", cx)
                        .into_any_element();
                }

                return div()
                    .size_full()
                    .p_4()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("Preparing plan table…")
                    .into_any_element();
            };

            let (is_truncated, source_row_count, visible_row_count) = {
                let state = table_state.read(cx);
                let delegate = state.delegate();
                (
                    delegate.is_truncated(),
                    delegate.source_row_count(),
                    delegate.rows_count(cx),
                )
            };

            div()
                .size_full()
                .when(is_truncated, |this| {
                    let theme = cx.theme();
                    this.child(
                        div()
                            .w_full()
                            .px_3()
                            .py_2()
                            .text_xs()
                            .bg(theme.warning.opacity(0.12))
                            .text_color(theme.warning)
                            .child(format!(
                                "Showing first {} rows of {} to keep results responsive",
                                visible_row_count, source_row_count
                            )),
                    )
                })
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
            let Some(table_state) = self.explain_op_table_states[explain_idx].as_ref() else {
                if self
                    .explain_results
                    .get(explain_idx)
                    .and_then(|result| result.raw_output.as_ref())
                    .is_none()
                {
                    return self
                        .render_explain_no_data("No opcode data available", cx)
                        .into_any_element();
                }

                return div()
                    .size_full()
                    .p_4()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("Preparing opcode table…")
                    .into_any_element();
            };

            let (is_truncated, source_row_count, visible_row_count) = {
                let state = table_state.read(cx);
                let delegate = state.delegate();
                (
                    delegate.is_truncated(),
                    delegate.source_row_count(),
                    delegate.rows_count(cx),
                )
            };

            div()
                .size_full()
                .when(is_truncated, |this| {
                    let theme = cx.theme();
                    this.child(
                        div()
                            .w_full()
                            .px_3()
                            .py_2()
                            .text_xs()
                            .bg(theme.warning.opacity(0.12))
                            .text_color(theme.warning)
                            .child(format!(
                                "Showing first {} rows of {} to keep results responsive",
                                visible_row_count, source_row_count
                            )),
                    )
                })
                .child(Table::new(table_state).stripe(true))
                .into_any_element()
        } else {
            self.render_explain_no_data("No opcode data available", cx)
                .into_any_element()
        }
    }

    /// Render Statistics sub-tab
    fn render_explain_statistics_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if let Some(result) = self.explain_results.get(explain_idx)
            && let Some(analysis) = &result.analyzed_plan
        {
            return ExplainAnalysisView::new(result, analysis).render_statistics(cx);
        }

        let theme = cx.theme();
        v_flex()
            .size_full()
            .p_4()
            .text_sm()
            .child(
                div()
                    .text_color(theme.muted_foreground)
                    .child("No parsed statistics available"),
            )
            .into_any_element()
    }

    fn render_explain_suggestions_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if let Some(result) = self.explain_results.get(explain_idx)
            && let Some(analysis) = &result.analyzed_plan
        {
            return ExplainAnalysisView::new(result, analysis).render_suggestions(cx);
        }

        let theme = cx.theme();
        v_flex()
            .size_full()
            .p_4()
            .text_sm()
            .child(
                div()
                    .text_color(theme.muted_foreground)
                    .child("No parsed suggestions available"),
            )
            .into_any_element()
    }

    /// Render Info sub-tab (metadata about the explain)
    fn render_explain_info_view(
        &self,
        explain_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let explain_result = self.explain_results.get(explain_idx);

        div().size_full().overflow_hidden().child(
            v_flex()
                .size_full()
                .p_4()
                .gap_3()
                .text_sm()
                .overflow_y_scrollbar()
                .when_some(explain_result, |this, result| {
                    this.child(
                        h_flex()
                            .gap_2()
                            .child(div().text_color(theme.muted_foreground).child("Timestamp:"))
                            .child(
                                div().text_color(theme.foreground).child(
                                    result.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
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
                                    .border_1()
                                    .border_color(theme.border.opacity(0.6))
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
                                            .border_1()
                                            .border_color(theme.border.opacity(0.6))
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
                }),
        )
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

                // Handle execution-related tabs
                if let Some(exec) = &self.execution {
                    match self.active_tab {
                        ResultTab::Message => this.child(self.render_message_tab(exec, cx)),
                        ResultTab::Summary => this.child(self.render_summary_tab(exec, cx)),
                        ResultTab::Result(idx) => this.child(self.render_result_tab(idx, cx)),
                        ResultTab::Info => this.child(self.render_info_tab(exec, cx)),
                        ResultTab::Explain(_) => unreachable!(), // Already handled above
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
