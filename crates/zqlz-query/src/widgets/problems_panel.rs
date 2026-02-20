//! Problems Panel
//!
//! Displays a list of diagnostics/errors from the query editor
//! and allows users to click entries to navigate to the problem location.

use gpui::*;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariant, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement,
    v_flex, ActiveTheme, Disableable, Icon, Sizable, ZqlzIcon,
};

/// Events emitted by the ProblemsPanel
#[derive(Clone, Debug)]
pub enum ProblemsPanelEvent {
    /// User clicked on a problem to navigate to it
    NavigateToProblem {
        /// Line number (0-indexed)
        line: usize,
        /// Column number (0-indexed)
        column: usize,
        /// End line (0-indexed)
        end_line: usize,
        /// End column (0-indexed)
        end_column: usize,
    },
}

/// Diagnostic entry for display in the Problems panel
#[derive(Clone, Debug)]
pub struct ProblemEntry {
    /// Line number (0-indexed)
    pub line: usize,
    /// Column number (0-indexed)
    pub column: usize,
    /// End line (0-indexed)
    pub end_line: usize,
    /// End column (0-indexed)
    pub end_column: usize,
    /// Severity level
    pub severity: ProblemSeverity,
    /// Error message
    pub message: String,
    /// Source of the diagnostic (e.g., "sqlparser", "tree-sitter")
    pub source: Option<String>,
}

/// Diagnostic severity for problems display
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProblemSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

impl ProblemSeverity {
    /// Get the icon for this severity
    pub fn icon(&self) -> ZqlzIcon {
        match self {
            ProblemSeverity::Error => ZqlzIcon::XCircle,
            ProblemSeverity::Warning => ZqlzIcon::Warning,
            ProblemSeverity::Info => ZqlzIcon::Info,
            ProblemSeverity::Hint => ZqlzIcon::Lightning,
        }
    }
}

/// Severity filter options for the Problems panel
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SeverityFilter {
    #[default]
    All,
    Errors,
    Warnings,
    Info,
}

impl SeverityFilter {
    /// Check if a problem matches this filter
    pub fn matches(&self, severity: &ProblemSeverity) -> bool {
        match self {
            SeverityFilter::All => true,
            SeverityFilter::Errors => *severity == ProblemSeverity::Error,
            SeverityFilter::Warnings => *severity == ProblemSeverity::Warning,
            SeverityFilter::Info => {
                *severity == ProblemSeverity::Info || *severity == ProblemSeverity::Hint
            }
        }
    }

    /// Get the label for this filter
    pub fn label(&self) -> &'static str {
        match self {
            SeverityFilter::All => "All",
            SeverityFilter::Errors => "Errors",
            SeverityFilter::Warnings => "Warnings",
            SeverityFilter::Info => "Info",
        }
    }
}

/// Problems Panel component
pub struct ProblemsPanel {
    focus_handle: FocusHandle,
    /// All problems (unfiltered)
    all_problems: Vec<ProblemEntry>,
    /// Error count
    errors: usize,
    /// Warning count
    warnings: usize,
    /// Info/Hint count
    infos_hints: usize,
    /// Current severity filter
    severity_filter: SeverityFilter,
    /// Search query text
    search_query: String,
    /// Input state for search field
    search_input: Entity<InputState>,
}

impl ProblemsPanel {
    /// Create a new problems panel
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx));

        // Subscribe to search input changes
        let search_input_clone = search_input.clone();
        cx.subscribe(
            &search_input,
            move |this: &mut ProblemsPanel, _, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let query = search_input_clone.read(cx).value().to_string();
                    this.search_query = query;
                    cx.notify();
                }
            },
        )
        .detach();

        Self {
            focus_handle: cx.focus_handle(),
            all_problems: Vec::new(),
            errors: 0,
            warnings: 0,
            infos_hints: 0,
            severity_filter: SeverityFilter::All,
            search_query: String::new(),
            search_input,
        }
    }

    /// Update the displayed problems
    pub fn update_problems(&mut self, problems: Vec<ProblemEntry>, cx: &mut Context<Self>) {
        self.all_problems = problems.clone();
        self.errors = problems
            .iter()
            .filter(|p| p.severity == ProblemSeverity::Error)
            .count();
        self.warnings = problems
            .iter()
            .filter(|p| p.severity == ProblemSeverity::Warning)
            .count();
        self.infos_hints = problems
            .iter()
            .filter(|p| p.severity == ProblemSeverity::Info || p.severity == ProblemSeverity::Hint)
            .count();
        cx.notify();
    }

    /// Get filtered problems based on current filters
    fn filtered_problems(&self) -> Vec<&ProblemEntry> {
        self.all_problems
            .iter()
            .filter(|p| {
                // Check severity filter
                if !self.severity_filter.matches(&p.severity) {
                    return false;
                }
                // Check search query
                if !self.search_query.is_empty() {
                    let query_lower = self.search_query.to_lowercase();
                    if !p.message.to_lowercase().contains(&query_lower) {
                        // Also check source if present
                        if let Some(source) = &p.source {
                            if !source.to_lowercase().contains(&query_lower) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            })
            .collect()
    }

    /// Set severity filter
    pub fn set_severity_filter(&mut self, filter: SeverityFilter, cx: &mut Context<Self>) {
        self.severity_filter = filter;
        cx.notify();
    }

    /// Clear all problems
    pub fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.all_problems.clear();
        self.errors = 0;
        self.warnings = 0;
        self.infos_hints = 0;
        // Clear search query
        self.search_query.clear();
        self.search_input.update(cx, |state, cx| {
            state.set_value(String::new(), window, cx);
        });
        cx.notify();
    }

    /// Get the total count of problems
    pub fn problem_count(&self) -> usize {
        self.all_problems.len()
    }

    /// Get the count of filtered problems
    pub fn filtered_count(&self) -> usize {
        self.filtered_problems().len()
    }

    /// Render a single problem entry
    fn render_problem(
        &self,
        problem: &ProblemEntry,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let problem_clone = problem.clone();

        // Get color based on severity
        let severity_color = match problem.severity {
            ProblemSeverity::Error => theme.danger,
            ProblemSeverity::Warning => theme.warning,
            ProblemSeverity::Info => theme.info,
            ProblemSeverity::Hint => theme.muted_foreground,
        };
        let severity_icon = problem.severity.icon();

        // Format line:column display
        let _location = format!("{}:{}", problem.line + 1, problem.column + 1);

        div()
            .w_full()
            .px_2()
            .py_1p5()
            .border_b_1()
            .border_color(theme.border)
            .hover(|this| this.bg(theme.border.opacity(0.3)))
            .cursor_pointer()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |_this, _event, _window, cx| {
                    cx.emit(ProblemsPanelEvent::NavigateToProblem {
                        line: problem_clone.line,
                        column: problem_clone.column,
                        end_line: problem_clone.end_line,
                        end_column: problem_clone.end_column,
                    });
                }),
            )
            .child(
                h_flex().gap_2().items_start().children([
                    // Severity icon
                    h_flex()
                        .items_center()
                        .justify_center()
                        .w_5()
                        .text_color(severity_color)
                        .child(Icon::new(severity_icon).size_4()),
                    // Problem details
                    v_flex().flex_1().gap_0p5().children([
                        // Error message
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(problem.message.clone()),
                        // Location and source
                        h_flex()
                            .gap_2()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .children(
                                problem
                                    .source
                                    .as_ref()
                                    .map(|source| div().child(format!("• {}", source))),
                            ),
                    ]),
                ]),
            )
            .into_any_element()
    }
}

impl Render for ProblemsPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Get filtered problems
        let filtered = self.filtered_problems();

        // Render filtered problems
        let problem_elements: Vec<_> = filtered
            .iter()
            .map(|problem| self.render_problem(problem, window, cx))
            .collect();

        let theme = cx.theme();
        let is_empty_filtered = problem_elements.is_empty();
        let total_problems = self.all_problems.len();
        let filtered_count = filtered.len();

        // Build title with counts
        let title = if total_problems > 0 {
            let mut title_parts = Vec::new();
            if self.errors > 0 {
                title_parts.push(format!(
                    "{} error{}",
                    self.errors,
                    if self.errors > 1 { "s" } else { "" }
                ));
            }
            if self.warnings > 0 {
                title_parts.push(format!(
                    "{} warning{}",
                    self.warnings,
                    if self.warnings > 1 { "s" } else { "" }
                ));
            }
            if self.infos_hints > 0 {
                title_parts.push(format!("{} info", self.infos_hints));
            }
            // Add filtered count if filters are active
            if self.severity_filter != SeverityFilter::All || !self.search_query.is_empty() {
                if filtered_count != total_problems {
                    format!(
                        "Problems ({}) [{}/{}]",
                        title_parts.join(", "),
                        filtered_count,
                        total_problems
                    )
                } else {
                    format!("Problems ({})", title_parts.join(", "))
                }
            } else {
                format!("Problems ({})", title_parts.join(", "))
            }
        } else {
            "Problems".to_string()
        };

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                // Toolbar
                h_flex()
                    .w_full()
                    .px_2()
                    .py_1p5()
                    .gap_2()
                    .items_center()
                    .border_b_1()
                    .border_color(theme.border)
                    // Search input
                    .child(
                        h_flex()
                            .gap_1()
                            .items_center()
                            .child(
                                Icon::new(ZqlzIcon::MagnifyingGlass)
                                    .size_4()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(
                                Input::new(&self.search_input)
                                    .small()
                                    .w_40()
                                    .focus_bordered(false)
                                    .bordered(false),
                            ),
                    )
                    .child(
                        // Severity filter buttons
                        h_flex().gap_1().children([
                            Button::new("filter-all")
                                .with_variant(if self.severity_filter == SeverityFilter::All {
                                    ButtonVariant::Secondary
                                } else {
                                    ButtonVariant::Ghost
                                })
                                .xsmall()
                                .compact()
                                .label("All")
                                .text_color(if self.severity_filter == SeverityFilter::All {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .on_click(cx.listener(|this, _event, _window, cx| {
                                    this.set_severity_filter(SeverityFilter::All, cx);
                                })),
                            Button::new("filter-errors")
                                .with_variant(if self.severity_filter == SeverityFilter::Errors {
                                    ButtonVariant::Secondary
                                } else {
                                    ButtonVariant::Ghost
                                })
                                .xsmall()
                                .compact()
                                .label("Errors")
                                .text_color(if self.severity_filter == SeverityFilter::Errors {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .on_click(cx.listener(|this, _event, _window, cx| {
                                    this.set_severity_filter(SeverityFilter::Errors, cx);
                                })),
                            Button::new("filter-warnings")
                                .with_variant(if self.severity_filter == SeverityFilter::Warnings {
                                    ButtonVariant::Secondary
                                } else {
                                    ButtonVariant::Ghost
                                })
                                .xsmall()
                                .compact()
                                .label("Warnings")
                                .text_color(if self.severity_filter == SeverityFilter::Warnings {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .on_click(cx.listener(|this, _event, _window, cx| {
                                    this.set_severity_filter(SeverityFilter::Warnings, cx);
                                })),
                            Button::new("filter-info")
                                .with_variant(if self.severity_filter == SeverityFilter::Info {
                                    ButtonVariant::Secondary
                                } else {
                                    ButtonVariant::Ghost
                                })
                                .xsmall()
                                .compact()
                                .label("Info")
                                .text_color(if self.severity_filter == SeverityFilter::Info {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .on_click(cx.listener(|this, _event, _window, cx| {
                                    this.set_severity_filter(SeverityFilter::Info, cx);
                                })),
                        ]),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child(title),
                    )
                    .child(
                        Button::new("clear-problems")
                            .with_variant(ButtonVariant::Ghost)
                            .xsmall()
                            .label("Clear")
                            .icon(ZqlzIcon::Trash)
                            .disabled(total_problems == 0)
                            .on_click(cx.listener(|this, _event, window, cx| {
                                this.clear(window, cx);
                            })),
                    ),
            )
            .child(if is_empty_filtered {
                // Empty state (no matching filters)
                v_flex()
                    .flex_1()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .child(
                        h_flex()
                            .text_color(theme.muted_foreground.opacity(0.5))
                            .child(Icon::new(ZqlzIcon::MagnifyingGlass).size_12()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("No matching problems"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground.opacity(0.7))
                            .child("Try adjusting your filters"),
                    )
                    .into_any_element()
            } else if total_problems == 0 {
                // Empty state (no problems at all)
                v_flex()
                    .flex_1()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .child(
                        h_flex()
                            .text_color(theme.muted_foreground.opacity(0.5))
                            .child(Icon::new(ZqlzIcon::CheckCircle).size_12()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("No problems detected"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground.opacity(0.7))
                            .child("Your query looks good!"),
                    )
                    .into_any_element()
            } else {
                // Problems list
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .children(problem_elements)
                    .into_any_element()
            })
    }
}

impl Focusable for ProblemsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ProblemsPanel {}
impl EventEmitter<ProblemsPanelEvent> for ProblemsPanel {}

impl Panel for ProblemsPanel {
    fn panel_name(&self) -> &'static str {
        "ProblemsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.all_problems.is_empty() {
            "Problems".to_string()
        } else {
            let error_str = if self.errors > 0 {
                format!("{} ", self.errors)
            } else {
                String::new()
            };
            let warn_str = if self.warnings > 0 {
                format!("{} ", self.warnings)
            } else {
                String::new()
            };
            let info_str = if self.infos_hints > 0 {
                format!("{} ", self.infos_hints)
            } else {
                String::new()
            };
            format!("Problems {}{}{}", error_str, warn_str, info_str)
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
