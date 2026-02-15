//! Column visibility panel component
//!
//! Renders a list of columns with checkboxes to show/hide them in the table.
//! Includes a search box to filter the column list.

use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Icon, Sizable,
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex,
};

use crate::icons::ZqlzIcon;

use super::filter_types::ColumnVisibility;

/// Events emitted by the column visibility panel
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum ColumnVisibilityEvent {
    /// A column's visibility was toggled
    ColumnToggled { column_name: String, visible: bool },
    /// All columns visibility changed
    AllColumnsChanged,
}

/// Column visibility panel state
pub struct ColumnVisibilityState {
    /// Focus handle
    focus_handle: FocusHandle,

    /// All columns with visibility state
    columns: Vec<ColumnVisibility>,

    /// Search input state
    search_input: Entity<InputState>,

    /// Current search query
    search_query: String,

    /// Subscriptions
    _subscriptions: Vec<Subscription>,
}

#[allow(dead_code)]
impl ColumnVisibilityState {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search..."));

        // Subscribe to search input changes
        let search_sub = cx.subscribe_in(
            &search_input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if let InputEvent::Change = event {
                    // Read the value from the input state
                    let query = input.read(cx).text().to_string();
                    this.search_query = query;
                    cx.notify();
                }
            },
        );

        Self {
            focus_handle: cx.focus_handle(),
            columns: Vec::new(),
            search_input,
            search_query: String::new(),
            _subscriptions: vec![search_sub],
        }
    }

    /// Set the columns
    pub fn set_columns(&mut self, columns: Vec<ColumnVisibility>, cx: &mut Context<Self>) {
        self.columns = columns;
        cx.notify();
    }

    /// Add columns from column metadata
    pub fn set_columns_from_meta(
        &mut self,
        columns: impl IntoIterator<Item = (String, String)>,
        cx: &mut Context<Self>,
    ) {
        self.columns = columns
            .into_iter()
            .map(|(name, data_type)| ColumnVisibility::new(name, data_type))
            .collect();
        cx.notify();
    }

    /// Toggle a column's visibility
    pub fn toggle_column(&mut self, column_name: &str, cx: &mut Context<Self>) {
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == column_name) {
            col.visible = !col.visible;
            cx.emit(ColumnVisibilityEvent::ColumnToggled {
                column_name: column_name.to_string(),
                visible: col.visible,
            });
            cx.notify();
        }
    }

    /// Set a column's visibility
    pub fn set_column_visibility(
        &mut self,
        column_name: &str,
        visible: bool,
        cx: &mut Context<Self>,
    ) {
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == column_name) {
            if col.visible != visible {
                col.visible = visible;
                cx.emit(ColumnVisibilityEvent::ColumnToggled {
                    column_name: column_name.to_string(),
                    visible,
                });
                cx.notify();
            }
        }
    }

    /// Show all columns
    pub fn show_all(&mut self, cx: &mut Context<Self>) {
        for col in &mut self.columns {
            col.visible = true;
        }
        cx.emit(ColumnVisibilityEvent::AllColumnsChanged);
        cx.notify();
    }

    /// Hide all columns
    pub fn hide_all(&mut self, cx: &mut Context<Self>) {
        for col in &mut self.columns {
            col.visible = false;
        }
        cx.emit(ColumnVisibilityEvent::AllColumnsChanged);
        cx.notify();
    }

    /// Get visible columns
    pub fn visible_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| c.visible)
            .map(|c| c.name.clone())
            .collect()
    }

    /// Get hidden columns
    pub fn hidden_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| !c.visible)
            .map(|c| c.name.clone())
            .collect()
    }

    /// Check if a column is visible
    pub fn is_column_visible(&self, column_name: &str) -> bool {
        self.columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.visible)
            .unwrap_or(true)
    }

    /// Get filtered columns based on search query
    fn filtered_columns(&self) -> Vec<&ColumnVisibility> {
        if self.search_query.is_empty() {
            self.columns.iter().collect()
        } else {
            let query = self.search_query.to_lowercase();
            self.columns
                .iter()
                .filter(|c| c.name.to_lowercase().contains(&query))
                .collect()
        }
    }

    /// Get count of visible columns
    pub fn visible_count(&self) -> usize {
        self.columns.iter().filter(|c| c.visible).count()
    }

    /// Get total column count
    pub fn total_count(&self) -> usize {
        self.columns.len()
    }
}

impl EventEmitter<ColumnVisibilityEvent> for ColumnVisibilityState {}

impl Focusable for ColumnVisibilityState {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

/// Render the column visibility panel
#[derive(IntoElement)]
pub struct ColumnVisibilityPanel {
    state: Entity<ColumnVisibilityState>,
}

impl ColumnVisibilityPanel {
    pub fn new(state: &Entity<ColumnVisibilityState>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl RenderOnce for ColumnVisibilityPanel {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let state = self.state.read(cx);
        let theme = cx.theme();
        let filtered = state.filtered_columns();

        v_flex()
            .w(px(200.0))
            .h_full()
            .bg(theme.background)
            .border_r_1()
            .border_color(theme.border)
            // Column list
            .child(
                v_flex()
                    .id("column-visibility-list")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_1()
                    .gap_0p5()
                    .children(filtered.iter().map(|col| {
                        let col_name = col.name.clone();
                        let visible = col.visible;

                        h_flex()
                            .items_center()
                            .gap_1()
                            .px_1()
                            .py_0p5()
                            .rounded(px(4.0))
                            .hover(|this| this.bg(theme.list_active))
                            .child(
                                Checkbox::new(format!("col-vis-{}", col_name))
                                    .checked(visible)
                                    .on_click({
                                        let state = self.state.clone();
                                        let col_name = col_name.clone();
                                        move |_, _window, cx| {
                                            state.update(cx, |s, cx| {
                                                s.toggle_column(&col_name, cx);
                                            });
                                        }
                                    }),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(col_name),
                            )
                    })),
            )
            // Search input at bottom
            .child(
                div().border_t_1().border_color(theme.border).p_1().child(
                    Input::new(&state.search_input)
                        .small()
                        .prefix(Icon::new(ZqlzIcon::MagnifyingGlass).size_3()),
                ),
            )
    }
}
