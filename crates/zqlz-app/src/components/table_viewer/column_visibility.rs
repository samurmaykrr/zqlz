//! Column visibility panel component
//!
//! Renders a list of columns with checkboxes to show/hide them in the table.
//! Includes a search box to filter the column list.

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Icon, Sizable, StyledExt,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{ArrowDirection, Input, InputEvent, InputState},
    tooltip::Tooltip,
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

    /// Currently keyboard-selected column inside the filtered list.
    selected_column_name: Option<String>,

    /// Subscriptions
    _subscriptions: Vec<Subscription>,
}

#[allow(dead_code)]
impl ColumnVisibilityState {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Search…")
                .emit_arrow_event(true)
        });

        // Subscribe to search input changes
        let search_sub = cx.subscribe_in(
            &search_input,
            window,
            |this, input, event: &InputEvent, _window, cx| match event {
                InputEvent::Change => {
                    let query = input.read(cx).text().to_string();
                    this.search_query = query;
                    this.sync_selected_column_to_filter();
                    cx.notify();
                }
                InputEvent::Focus => {
                    this.sync_selected_column_to_filter();
                    cx.notify();
                }
                InputEvent::PressArrow { direction } => match direction {
                    ArrowDirection::Up => this.select_previous_filtered(cx),
                    ArrowDirection::Down => this.select_next_filtered(cx),
                    ArrowDirection::Left | ArrowDirection::Right => {}
                },
                InputEvent::PressEnter { .. } => {
                    this.toggle_selected_column(cx);
                }
                _ => {}
            },
        );

        Self {
            focus_handle: cx.focus_handle(),
            columns: Vec::new(),
            search_input,
            search_query: String::new(),
            selected_column_name: None,
            _subscriptions: vec![search_sub],
        }
    }

    /// Set the columns
    pub fn set_columns(&mut self, columns: Vec<ColumnVisibility>, cx: &mut Context<Self>) {
        self.columns = columns;
        self.sync_selected_column_to_filter();
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
        self.sync_selected_column_to_filter();
        cx.notify();
    }

    /// Toggle a column's visibility
    pub fn toggle_column(&mut self, column_name: &str, cx: &mut Context<Self>) {
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == column_name) {
            self.selected_column_name = Some(column_name.to_string());
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
        self.selected_column_name = Some(column_name.to_string());

        if let Some(col) = self.columns.iter_mut().find(|c| c.name == column_name)
            && col.visible != visible
        {
            col.visible = visible;
            cx.emit(ColumnVisibilityEvent::ColumnToggled {
                column_name: column_name.to_string(),
                visible,
            });
            cx.notify();
        }
    }

    /// Show all columns
    pub fn show_all(&mut self, cx: &mut Context<Self>) {
        for col in &mut self.columns {
            col.visible = true;
        }
        self.sync_selected_column_to_filter();
        cx.emit(ColumnVisibilityEvent::AllColumnsChanged);
        cx.notify();
    }

    /// Hide all columns
    pub fn hide_all(&mut self, cx: &mut Context<Self>) {
        for col in &mut self.columns {
            col.visible = false;
        }
        self.sync_selected_column_to_filter();
        cx.emit(ColumnVisibilityEvent::AllColumnsChanged);
        cx.notify();
    }

    /// Focus the search input so the panel is ready for keyboard-driven filtering.
    pub fn focus_search(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.search_input.update(cx, |input, cx| {
            input.focus(window, cx);
        });
    }

    pub fn selected_column_name(&self) -> Option<&str> {
        self.selected_column_name.as_deref()
    }

    fn sync_selected_column_to_filter(&mut self) {
        let filtered_column_names: Vec<String> = self
            .filtered_columns()
            .into_iter()
            .map(|column| column.name.clone())
            .collect();

        if filtered_column_names.is_empty() {
            self.selected_column_name = None;
            return;
        }

        let current_selection = self.selected_column_name.as_deref();
        let selection_still_visible = current_selection
            .map(|selected_name| {
                filtered_column_names
                    .iter()
                    .any(|name| name == selected_name)
            })
            .unwrap_or(false);

        if !selection_still_visible {
            self.selected_column_name = filtered_column_names.into_iter().next();
        }
    }

    fn move_filtered_selection(&mut self, step: isize, cx: &mut Context<Self>) {
        let filtered_column_names: Vec<String> = self
            .filtered_columns()
            .into_iter()
            .map(|column| column.name.clone())
            .collect();

        if filtered_column_names.is_empty() {
            self.selected_column_name = None;
            cx.notify();
            return;
        }

        let current_index = self
            .selected_column_name
            .as_deref()
            .and_then(|selected_name| {
                filtered_column_names
                    .iter()
                    .position(|column_name| column_name == selected_name)
            })
            .unwrap_or(0);

        let filtered_count = filtered_column_names.len() as isize;
        let next_index = (current_index as isize + step).rem_euclid(filtered_count) as usize;
        self.selected_column_name = filtered_column_names.get(next_index).cloned();
        cx.notify();
    }

    pub fn select_next_filtered(&mut self, cx: &mut Context<Self>) {
        self.move_filtered_selection(1, cx);
    }

    pub fn select_previous_filtered(&mut self, cx: &mut Context<Self>) {
        self.move_filtered_selection(-1, cx);
    }

    pub fn toggle_selected_column(&mut self, cx: &mut Context<Self>) {
        if let Some(column_name) = self.selected_column_name.clone() {
            self.toggle_column(&column_name, cx);
        }
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
        let visible_count = state.visible_count();
        let total_count = state.total_count();
        let has_search = !state.search_query.is_empty();
        let selected_column_name = state.selected_column_name().map(str::to_owned);

        v_flex()
            .w(px(200.0))
            .h_full()
            .bg(theme.background)
            .border_r_1()
            .border_color(theme.border)
            .child(
                v_flex()
                    .gap_1()
                    .p_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        h_flex()
                            .items_center()
                            .justify_between()
                            .gap_2()
                            .child(div().text_sm().font_semibold().child("Columns"))
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(format!("{visible_count} of {total_count} visible")),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("column-visibility-show-all")
                                    .label("Show All")
                                    .ghost()
                                    .xsmall()
                                    .disabled(visible_count == total_count)
                                    .on_click({
                                        let state = self.state.clone();
                                        move |_, _window, cx| {
                                            state.update(cx, |state, cx| {
                                                state.show_all(cx);
                                            });
                                        }
                                    }),
                            )
                            .child(
                                Button::new("column-visibility-hide-all")
                                    .label("Hide All")
                                    .ghost()
                                    .xsmall()
                                    .disabled(total_count == 0 || visible_count == 0)
                                    .on_click({
                                        let state = self.state.clone();
                                        move |_, _window, cx| {
                                            state.update(cx, |state, cx| {
                                                state.hide_all(cx);
                                            });
                                        }
                                    }),
                            ),
                    ),
            )
            // Column list
            .child(
                v_flex()
                    .id("column-visibility-list")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_1()
                    .gap_0p5()
                    .when(filtered.is_empty(), |this| {
                        let message = if has_search {
                            "No matching columns"
                        } else {
                            "No columns available"
                        };

                        this.child(
                            v_flex()
                                .flex_1()
                                .items_center()
                                .justify_center()
                                .px_3()
                                .py_6()
                                .gap_1()
                                .text_center()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child(message),
                                )
                                .when(has_search, |this| {
                                    this.child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child("Try a different search term"),
                                    )
                                }),
                        )
                    })
                    .children(filtered.iter().map(|col| {
                        let col_name = col.name.clone();
                        let data_type = col.data_type.clone();
                        let visible = col.visible;
                        let is_selected =
                            selected_column_name.as_deref() == Some(col_name.as_str());
                        let tooltip_text = format!("{} • {}", col_name, data_type);

                        h_flex()
                            .id(format!("col-vis-row-{col_name}"))
                            .items_center()
                            .gap_1()
                            .px_1()
                            .py_0p5()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .when(is_selected, |this| this.bg(theme.list_active))
                            .hover(|this| this.bg(theme.list_active))
                            .on_click({
                                let state = self.state.clone();
                                let col_name = col_name.clone();
                                move |_, _window, cx| {
                                    state.update(cx, |s, cx| {
                                        s.toggle_column(&col_name, cx);
                                    });
                                }
                            })
                            .child(
                                div()
                                    .id(format!("col-vis-checkbox-hitbox-{col_name}"))
                                    .on_click(|_, _window, cx| {
                                        cx.stop_propagation();
                                    })
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
                                    ),
                            )
                            .child(
                                v_flex()
                                    .min_w_0()
                                    .gap_0p5()
                                    .child(
                                        div()
                                            .text_sm()
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .child(col_name),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .child(data_type),
                                    ),
                            )
                            .tooltip(move |window, cx| {
                                Tooltip::new(tooltip_text.clone()).build(window, cx)
                            })
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
