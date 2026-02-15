//! Filter panel component for table viewer
//!
//! Renders a collapsible filter section with multiple filter rows.
//! Each row has: checkbox, column dropdown, operator dropdown, value input, add/remove buttons.

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_query::{ZedInput, ZedInputEvent, ZedInputState};
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{DropdownMenu, PopupMenuItem},
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    v_flex, ActiveTheme, IndexPath, Sizable,
};

use crate::icons::ZqlzIcon;

use super::filter_types::{
    ColumnSelectItem, FilterCondition, FilterOperator, LogicalOperator, SortCriterion,
    SortDirection,
};

/// Events emitted by the filter panel
#[derive(Clone, Debug)]
pub enum FilterPanelEvent {
    /// Filters or sorts changed (but not yet applied)
    Changed,
    /// User clicked "Apply Filter & Sort"
    Apply,
}

/// State for a single filter row
pub struct FilterRowState {
    pub condition: FilterCondition,
    pub column_select: Entity<SelectState<SearchableVec<ColumnSelectItem>>>,
    pub value_input: Entity<InputState>,
    pub value2_input: Option<Entity<InputState>>,
    /// Dedicated input for custom SQL expressions (has helpful placeholder)
    pub custom_sql_input: Entity<ZedInputState>,
}

impl FilterRowState {
    pub fn new(
        id: usize,
        columns: Vec<ColumnSelectItem>,
        window: &mut Window,
        cx: &mut Context<FilterPanelState>,
    ) -> Self {
        let column_select = cx.new(|cx| {
            SelectState::new(SearchableVec::new(columns), None, window, cx).searchable(true)
        });

        let value_input = cx.new(|cx| InputState::new(window, cx).placeholder("Value"));
        let custom_sql_input = cx.new(|cx| {
            ZedInputState::new(window, cx).with_placeholder("SQL expression (e.g. price > 100)")
        });

        Self {
            condition: FilterCondition::new(id),
            column_select,
            value_input,
            value2_input: None,
            custom_sql_input,
        }
    }
}

/// Filter panel state
pub struct FilterPanelState {
    /// Focus handle
    focus_handle: FocusHandle,

    /// Available columns for filtering
    columns: Vec<ColumnSelectItem>,

    /// Filter rows
    filters: Vec<FilterRowState>,

    /// Sort criteria
    pub(crate) sorts: Vec<SortCriterion>,

    /// Next filter ID
    next_filter_id: usize,

    /// Next sort ID
    next_sort_id: usize,

    /// Whether filters have been modified since last apply
    pub is_dirty: bool,

    /// Subscriptions
    _subscriptions: Vec<Subscription>,
}

impl FilterPanelState {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            columns: Vec::new(),
            filters: Vec::new(),
            sorts: Vec::new(),
            next_filter_id: 1,
            next_sort_id: 1,
            is_dirty: false,
            _subscriptions: Vec::new(),
        }
    }

    /// Set the available columns for filtering/sorting
    pub fn set_columns(
        &mut self,
        columns: Vec<ColumnSelectItem>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.columns = columns;

        // Build items list before the loop to avoid borrowing conflict
        let items = self.build_column_items();

        // Update existing filter row column selects
        for filter_row in &mut self.filters {
            filter_row.column_select.update(cx, |state, _cx| {
                state.set_items(SearchableVec::new(items.clone()), window, _cx);
            });
        }

        cx.notify();
    }

    /// Build column items list (with [Custom] option)
    fn build_column_items(&self) -> Vec<ColumnSelectItem> {
        let mut items = vec![ColumnSelectItem::custom()];
        items.extend(self.columns.clone());
        items
    }

    /// Add a new filter row
    pub fn add_filter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let id = self.next_filter_id;
        self.next_filter_id += 1;

        let columns = self.build_column_items();
        let row_state = FilterRowState::new(id, columns, window, cx);

        // Subscribe to column select changes
        let filter_id = id;
        cx.subscribe_in(
            &row_state.column_select,
            window,
            move |this,
                  _select,
                  event: &SelectEvent<SearchableVec<ColumnSelectItem>>,
                  _window,
                  cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.on_column_selected(filter_id, value.clone(), cx);
                }
            },
        )
        .detach();

        // Subscribe to value input changes
        cx.subscribe_in(
            &row_state.value_input,
            window,
            move |this, input, event: &InputEvent, _window, cx| {
                if let InputEvent::Change = event {
                    let value: SharedString = input.read(cx).text().to_string().into();
                    this.on_value_changed(filter_id, value, cx);
                }
            },
        )
        .detach();

        // Subscribe to custom SQL input changes
        cx.subscribe_in(
            &row_state.custom_sql_input,
            window,
            move |this, input, event: &ZedInputEvent, _window, cx| {
                if let ZedInputEvent::Change = event {
                    let value: SharedString = input.read(cx).value().to_string().into();
                    this.on_value_changed(filter_id, value, cx);
                }
            },
        )
        .detach();

        self.filters.push(row_state);
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Add a new filter row with a pre-populated column and value (equals comparison)
    ///
    /// Used by the "Filter" context menu to quickly filter by a cell's value.
    pub fn add_quick_filter(
        &mut self,
        column_name: String,
        value: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let id = self.next_filter_id;
        self.next_filter_id += 1;

        let columns = self.build_column_items();
        let row_state = FilterRowState::new(id, columns.clone(), window, cx);

        // Find the column index and set it in the select
        let column_index = columns.iter().position(|c| c.name == column_name);
        if let Some(idx) = column_index {
            row_state.column_select.update(cx, |state, cx| {
                state.set_selected_index(Some(IndexPath::new(idx)), window, cx);
            });
        }

        // Set the condition's column
        let mut condition = FilterCondition::new(id);
        condition.column = Some(column_name);
        condition.operator = FilterOperator::Equal;
        condition.value = value.clone();

        // Set the value in the input
        row_state.value_input.update(cx, |state, cx| {
            state.set_value(&value, window, cx);
        });

        // Subscribe to column select changes
        let filter_id = id;
        cx.subscribe_in(
            &row_state.column_select,
            window,
            move |this,
                  _select,
                  event: &SelectEvent<SearchableVec<ColumnSelectItem>>,
                  _window,
                  cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.on_column_selected(filter_id, value.clone(), cx);
                }
            },
        )
        .detach();

        // Subscribe to value input changes
        cx.subscribe_in(
            &row_state.value_input,
            window,
            move |this, input, event: &InputEvent, _window, cx| {
                if let InputEvent::Change = event {
                    let value: SharedString = input.read(cx).text().to_string().into();
                    this.on_value_changed(filter_id, value, cx);
                }
            },
        )
        .detach();

        // Subscribe to custom SQL input changes
        cx.subscribe_in(
            &row_state.custom_sql_input,
            window,
            move |this, input, event: &ZedInputEvent, _window, cx| {
                if let ZedInputEvent::Change = event {
                    let value: SharedString = input.read(cx).value().to_string().into();
                    this.on_value_changed(filter_id, value, cx);
                }
            },
        )
        .detach();

        // Create the row state with the pre-populated condition
        let mut row_state = row_state;
        row_state.condition = condition;

        self.filters.push(row_state);
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Remove a filter row by ID
    pub fn remove_filter(&mut self, filter_id: usize, cx: &mut Context<Self>) {
        self.filters.retain(|f| f.condition.id != filter_id);
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Toggle filter enabled state
    pub fn toggle_filter(&mut self, filter_id: usize, cx: &mut Context<Self>) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            filter_row.condition.enabled = !filter_row.condition.enabled;
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Toggle logical operator (AND/OR) for a filter row
    pub fn toggle_logical_operator(&mut self, filter_id: usize, cx: &mut Context<Self>) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            filter_row.condition.logical_operator = filter_row.condition.logical_operator.toggle();
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Move filter row up (earlier in order)
    pub fn move_filter_up(&mut self, filter_id: usize, cx: &mut Context<Self>) {
        if let Some(pos) = self
            .filters
            .iter()
            .position(|f| f.condition.id == filter_id)
        {
            if pos > 0 {
                self.filters.swap(pos, pos - 1);
                self.is_dirty = true;
                cx.emit(FilterPanelEvent::Changed);
                cx.notify();
            }
        }
    }

    /// Move filter row down (later in order)
    pub fn move_filter_down(&mut self, filter_id: usize, cx: &mut Context<Self>) {
        if let Some(pos) = self
            .filters
            .iter()
            .position(|f| f.condition.id == filter_id)
        {
            if pos < self.filters.len() - 1 {
                self.filters.swap(pos, pos + 1);
                self.is_dirty = true;
                cx.emit(FilterPanelEvent::Changed);
                cx.notify();
            }
        }
    }

    /// Set operator for a filter
    pub fn set_operator(
        &mut self,
        filter_id: usize,
        operator: FilterOperator,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            filter_row.condition.operator = operator;

            // Create second input for BETWEEN operators if needed
            if operator.requires_two_values() && filter_row.value2_input.is_none() {
                let value2_input = cx.new(|cx| InputState::new(window, cx).placeholder("?"));

                // Subscribe to second value input changes
                cx.subscribe_in(
                    &value2_input,
                    window,
                    move |this, input, event: &InputEvent, _window, cx| {
                        if let InputEvent::Change = event {
                            let value: SharedString = input.read(cx).text().to_string().into();
                            this.on_value2_changed(filter_id, value, cx);
                        }
                    },
                )
                .detach();

                filter_row.value2_input = Some(value2_input);
            }

            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Handle column selection
    fn on_column_selected(
        &mut self,
        filter_id: usize,
        value: SharedString,
        cx: &mut Context<Self>,
    ) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            if value.as_ref() == "[Custom]" {
                filter_row.condition.column = None;
                filter_row.condition.operator = FilterOperator::Custom;
            } else {
                filter_row.condition.column = Some(value.to_string());
            }
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Handle value input change
    fn on_value_changed(&mut self, filter_id: usize, value: SharedString, cx: &mut Context<Self>) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            filter_row.condition.value = value.to_string();
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Handle second value input change (for BETWEEN operators)
    fn on_value2_changed(&mut self, filter_id: usize, value: SharedString, cx: &mut Context<Self>) {
        if let Some(filter_row) = self
            .filters
            .iter_mut()
            .find(|f| f.condition.id == filter_id)
        {
            filter_row.condition.value2 = Some(value.to_string());
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Add a sort criterion
    pub fn add_sort(&mut self, column: String, cx: &mut Context<Self>) {
        // Don't add duplicate sorts
        if self.sorts.iter().any(|s| s.column == column) {
            return;
        }

        let id = self.next_sort_id;
        self.next_sort_id += 1;
        self.sorts.push(SortCriterion::new(id, column));
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Remove a sort criterion by ID
    pub fn remove_sort(&mut self, sort_id: usize, cx: &mut Context<Self>) {
        self.sorts.retain(|s| s.id != sort_id);
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Toggle sort direction
    pub fn toggle_sort_direction(&mut self, sort_id: usize, cx: &mut Context<Self>) {
        if let Some(sort) = self.sorts.iter_mut().find(|s| s.id == sort_id) {
            sort.direction = sort.direction.toggle();
            self.is_dirty = true;
            cx.emit(FilterPanelEvent::Changed);
            cx.notify();
        }
    }

    /// Move sort criterion left (earlier in order)
    pub fn move_sort_left(&mut self, sort_id: usize, cx: &mut Context<Self>) {
        if let Some(pos) = self.sorts.iter().position(|s| s.id == sort_id) {
            if pos > 0 {
                self.sorts.swap(pos, pos - 1);
                self.is_dirty = true;
                cx.emit(FilterPanelEvent::Changed);
                cx.notify();
            }
        }
    }

    /// Move sort criterion right (later in order)
    pub fn move_sort_right(&mut self, sort_id: usize, cx: &mut Context<Self>) {
        if let Some(pos) = self.sorts.iter().position(|s| s.id == sort_id) {
            if pos < self.sorts.len() - 1 {
                self.sorts.swap(pos, pos + 1);
                self.is_dirty = true;
                cx.emit(FilterPanelEvent::Changed);
                cx.notify();
            }
        }
    }

    /// Clear all sorts
    pub fn clear_sorts(&mut self, cx: &mut Context<Self>) {
        self.sorts.clear();
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Clear all filters and sorts
    pub fn clear_all(&mut self, cx: &mut Context<Self>) {
        self.filters.clear();
        self.sorts.clear();
        self.is_dirty = true;
        cx.emit(FilterPanelEvent::Changed);
        cx.notify();
    }

    /// Get the current filter conditions (for applying)
    pub fn get_filter_conditions(&self) -> Vec<FilterCondition> {
        self.filters.iter().map(|f| f.condition.clone()).collect()
    }

    /// Get the current sort criteria (for applying)
    pub fn get_sort_criteria(&self) -> Vec<SortCriterion> {
        self.sorts.clone()
    }

    /// Check if there are any active filters or sorts
    pub fn has_criteria(&self) -> bool {
        !self.filters.is_empty() || !self.sorts.is_empty()
    }

    /// Mark as not dirty (after apply)
    pub fn mark_applied(&mut self, cx: &mut Context<Self>) {
        self.is_dirty = false;
        cx.notify();
    }

    /// Generate WHERE clause SQL from current filters and copy to clipboard
    pub fn copy_filter_sql(&self, cx: &mut Context<Self>) {
        let filter_conditions = self.get_filter_conditions();
        let enabled_filters: Vec<_> = filter_conditions
            .iter()
            .filter(|f| f.enabled && f.to_sql().is_some())
            .collect();

        if enabled_filters.is_empty() {
            tracing::warn!("No enabled filters to copy");
            return;
        }

        // Build WHERE clause
        let mut where_parts = Vec::new();
        for (idx, filter) in enabled_filters.iter().enumerate() {
            if let Some(sql) = filter.to_sql() {
                if idx > 0 {
                    // Add logical operator from previous filter
                    let prev_filter = enabled_filters[idx - 1];
                    where_parts.push(prev_filter.logical_operator.sql().to_string());
                }
                where_parts.push(sql);
            }
        }

        let where_clause = if where_parts.is_empty() {
            "-- No valid filter conditions".to_string()
        } else {
            format!("WHERE {}", where_parts.join(" "))
        };

        // Copy to clipboard
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(where_clause.clone()));
        tracing::info!("Filter SQL copied to clipboard: {}", where_clause);
    }
}

impl EventEmitter<FilterPanelEvent> for FilterPanelState {}

impl Focusable for FilterPanelState {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

/// Render the filter panel
#[derive(IntoElement)]
pub struct FilterPanel {
    state: Entity<FilterPanelState>,
}

impl FilterPanel {
    pub fn new(state: &Entity<FilterPanelState>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl RenderOnce for FilterPanel {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let panel_state = self.state.clone();
        let state = self.state.read(cx);
        let theme = cx.theme();

        let has_filters = !state.filters.is_empty();
        let has_sorts = !state.sorts.is_empty();
        let is_dirty = state.is_dirty;

        // Pre-render filter rows to avoid closure lifetime issues
        let filter_rows: Vec<_> = state
            .filters
            .iter()
            .enumerate()
            .map(|(idx, filter_row)| {
                render_filter_row(&panel_state, filter_row, idx, state.filters.len(), cx)
            })
            .collect();

        // Pre-render sort chips
        let sort_chips: Vec<_> = state
            .sorts
            .iter()
            .map(|sort| render_sort_chip(&panel_state, sort, cx))
            .collect();

        v_flex()
            .w_full()
            .gap_2()
            .p_2()
            .bg(theme.background)
            .border_b_1()
            .border_color(theme.border)
            // Filter section
            .child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .p_2()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    // Filter section header
                    .child(
                        h_flex().items_center().gap_2().child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Filter"),
                        ),
                    )
                    // Filter rows
                    .children(filter_rows)
                    // Add filter button (if no filters yet)
                    .when(!has_filters, {
                        let panel_state = panel_state.clone();
                        move |this| {
                            this.child(
                                h_flex()
                                    .items_center()
                                    .gap_1()
                                    .child(
                                        Button::new("add-first-filter")
                                            .icon(ZqlzIcon::Plus)
                                            .ghost()
                                            .xsmall()
                                            .tooltip("Add Filter")
                                            .on_click({
                                                let state = panel_state.clone();
                                                move |_, window, cx| {
                                                    state.update(cx, |s, cx| {
                                                        s.add_filter(window, cx)
                                                    });
                                                }
                                            }),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(theme.muted_foreground)
                                            .child("Click \"+\" to add filter criteria"),
                                    ),
                            )
                        }
                    }),
            )
            // Sort section
            .child({
                let panel_state = panel_state.clone();
                v_flex()
                    .w_full()
                    .gap_1()
                    .p_2()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Sort By"),
                            )
                            // Sort chips
                            .children(sort_chips)
                            // Add sort button
                            .child(
                                Button::new("add-sort")
                                    .icon(ZqlzIcon::Plus)
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Add Sort")
                                    .dropdown_menu({
                                        let columns = state.columns.clone();
                                        let state = panel_state.clone();
                                        move |menu, _window, _cx| {
                                            let mut menu = menu;
                                            for col in &columns {
                                                let col_name = col.name.to_string();
                                                let state = state.clone();
                                                menu = menu.item(
                                                    PopupMenuItem::new(col.name.clone()).on_click(
                                                        move |_, _window, cx| {
                                                            state.update(cx, |s, cx| {
                                                                s.add_sort(col_name.clone(), cx);
                                                            });
                                                        },
                                                    ),
                                                );
                                            }
                                            menu
                                        }
                                    }),
                            )
                            .when(!has_sorts, |this| {
                                this.child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("Click \"+\" to add sort criteria"),
                                )
                            }),
                    )
            })
            // Apply button row
            .child({
                let has_criteria = has_filters || has_sorts;
                h_flex()
                    .items_center()
                    .gap_2()
                    .mt_2()
                    .child(
                        Button::new("apply-filter-sort")
                            .label("Apply Filter & Sort")
                            .primary()
                            .small()
                            .on_click({
                                let state = panel_state.clone();
                                move |_, _window, cx| {
                                    state.update(cx, |_s, cx| {
                                        cx.emit(FilterPanelEvent::Apply);
                                    });
                                }
                            }),
                    )
                    .when(has_filters, |this| {
                        this.child(
                            Button::new("copy-filter-sql")
                                .label("Copy Filter SQL")
                                .ghost()
                                .small()
                                .on_click({
                                    let state = panel_state.clone();
                                    move |_, _window, cx| {
                                        state.update(cx, |s, cx| {
                                            s.copy_filter_sql(cx);
                                        });
                                    }
                                }),
                        )
                    })
                    .when(has_criteria, |this| {
                        this.child(
                            Button::new("clear-all-filters")
                                .label("Clear All")
                                .ghost()
                                .small()
                                .on_click({
                                    let state = panel_state.clone();
                                    move |_, _window, cx| {
                                        state.update(cx, |s, cx| s.clear_all(cx));
                                    }
                                }),
                        )
                    })
                    .when(is_dirty, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Criteria Edited"),
                        )
                    })
            })
    }
}

/// Render a single filter row
fn render_filter_row(
    panel_state: &Entity<FilterPanelState>,
    filter_row: &FilterRowState,
    index: usize,
    total: usize,
    cx: &App,
) -> impl IntoElement {
    let theme = cx.theme();
    let filter_id = filter_row.condition.id;
    let enabled = filter_row.condition.enabled;
    let operator = filter_row.condition.operator;
    let logical_operator = filter_row.condition.logical_operator;
    let is_custom = filter_row.condition.column.is_none() || operator.is_custom();
    let requires_value = operator.requires_value() && !is_custom;
    let requires_two_values = operator.requires_two_values();

    h_flex()
        .w_full()
        .items_center()
        .gap_1()
        .py_1()
        // Enabled checkbox
        .child(
            Checkbox::new(format!("filter-enabled-{}", filter_id))
                .checked(enabled)
                .on_click({
                    let state = panel_state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.toggle_filter(filter_id, cx));
                    }
                }),
        )
        // Column selector
        .child(
            div().w(px(180.0)).child(
                Select::new(&filter_row.column_select)
                    .small()
                    .placeholder("Column..."),
            ),
        )
        // Operator dropdown (hidden for custom SQL)
        .when(!is_custom, |this| {
            this.child(render_operator_dropdown(
                panel_state,
                filter_id,
                operator,
                cx,
            ))
        })
        // Custom SQL input (wide input for raw SQL expression)
        .when(is_custom, |this| {
            this.child(
                div()
                    .w(px(280.0))
                    .h(px(32.0))
                    .child(ZedInput::new(&filter_row.custom_sql_input)),
            )
        })
        // Regular value input (for non-custom filters)
        .when(requires_value, |this| {
            this.child(
                div()
                    .w(px(120.0))
                    .child(Input::new(&filter_row.value_input).small().cleanable(true)),
            )
        })
        // Second value input (for BETWEEN)
        .when(requires_two_values, |this| {
            if let Some(ref value2_input) = filter_row.value2_input {
                this.child(
                    h_flex()
                        .items_center()
                        .gap_1()
                        .child(div().text_sm().child("and"))
                        .child(div().w(px(100.0)).child(Input::new(value2_input).small())),
                )
            } else {
                this
            }
        })
        // Add filter button
        .child(
            Button::new(format!("add-filter-{}", filter_id))
                .icon(ZqlzIcon::Plus)
                .ghost()
                .xsmall()
                .tooltip("Add Filter")
                .on_click({
                    let state = panel_state.clone();
                    move |_, window, cx| {
                        state.update(cx, |s, cx| s.add_filter(window, cx));
                    }
                }),
        )
        // Remove/Options button
        .child(
            Button::new(format!("filter-options-{}", filter_id))
                .icon(ZqlzIcon::Ellipsis)
                .ghost()
                .xsmall()
                .tooltip("Filter Options")
                .dropdown_menu({
                    let state = panel_state.clone();
                    let can_move_up = index > 0;
                    let can_move_down = index < total - 1;
                    move |menu, _window, _cx| {
                        menu.when(can_move_up, |menu| {
                            menu.item(PopupMenuItem::new("Move Up").on_click({
                                let state = state.clone();
                                move |_, _window, cx| {
                                    state.update(cx, |s, cx| s.move_filter_up(filter_id, cx));
                                }
                            }))
                        })
                        .when(can_move_down, |menu| {
                            menu.item(PopupMenuItem::new("Move Down").on_click({
                                let state = state.clone();
                                move |_, _window, cx| {
                                    state.update(cx, |s, cx| s.move_filter_down(filter_id, cx));
                                }
                            }))
                        })
                        .when(can_move_up || can_move_down, |menu| menu.separator())
                        .item(
                            PopupMenuItem::new("Remove Filter").on_click({
                                let state = state.clone();
                                move |_, _window, cx| {
                                    state.update(cx, |s, cx| s.remove_filter(filter_id, cx));
                                }
                            }),
                        )
                    }
                }),
        )
        // Show clickable "and"/"or" between filters
        .when(index < total - 1, {
            let state = panel_state.clone();
            move |this| {
                this.child(
                    Button::new(format!("logical-op-{}", filter_id))
                        .label(logical_operator.label())
                        .ghost()
                        .xsmall()
                        .ml_2()
                        .tooltip("Click to toggle AND/OR")
                        .on_click({
                            let state = state.clone();
                            move |_, _window, cx| {
                                state.update(cx, |s, cx| s.toggle_logical_operator(filter_id, cx));
                            }
                        }),
                )
            }
        })
}

/// Render operator dropdown button
fn render_operator_dropdown(
    panel_state: &Entity<FilterPanelState>,
    filter_id: usize,
    current_operator: FilterOperator,
    _cx: &App,
) -> impl IntoElement {
    Button::new(format!("operator-{}", filter_id))
        .label(current_operator.label())
        .ghost()
        .small()
        .dropdown_menu({
            let state = panel_state.clone();
            move |menu, _window, _cx| {
                let mut menu = menu;
                for op in FilterOperator::all() {
                    let operator = *op;
                    let state = state.clone();
                    let is_selected = operator == current_operator;
                    menu = menu.item(
                        PopupMenuItem::new(operator.label())
                            .checked(is_selected)
                            .on_click(move |_, window, cx| {
                                state.update(cx, |s, cx| {
                                    s.set_operator(filter_id, operator, window, cx)
                                });
                            }),
                    );
                }
                menu
            }
        })
}

/// Render a sort chip
fn render_sort_chip(
    panel_state: &Entity<FilterPanelState>,
    sort: &SortCriterion,
    _cx: &App,
) -> impl IntoElement {
    let sort_id = sort.id;
    let direction = sort.direction;
    let icon = match direction {
        SortDirection::Ascending => ZqlzIcon::SortAscending,
        SortDirection::Descending => ZqlzIcon::SortDescending,
    };

    Button::new(format!("sort-chip-{}", sort_id))
        .label(sort.column.clone())
        .icon(icon)
        .small()
        .outline()
        .on_click({
            let state = panel_state.clone();
            move |_, _window, cx| {
                state.update(cx, |s, cx| s.toggle_sort_direction(sort_id, cx));
            }
        })
        .dropdown_menu({
            let state = panel_state.clone();
            move |menu, _window, _cx| {
                menu.item(
                    PopupMenuItem::new("Sort Ascending")
                        .checked(direction == SortDirection::Ascending)
                        .on_click({
                            let state = state.clone();
                            move |_, _window, cx| {
                                state.update(cx, |s, cx| {
                                    if let Some(sort) = s.sorts.iter_mut().find(|s| s.id == sort_id)
                                    {
                                        sort.direction = SortDirection::Ascending;
                                        s.is_dirty = true;
                                        cx.emit(FilterPanelEvent::Changed);
                                        cx.notify();
                                    }
                                });
                            }
                        }),
                )
                .item(
                    PopupMenuItem::new("Sort Descending")
                        .checked(direction == SortDirection::Descending)
                        .on_click({
                            let state = state.clone();
                            move |_, _window, cx| {
                                state.update(cx, |s, cx| {
                                    if let Some(sort) = s.sorts.iter_mut().find(|s| s.id == sort_id)
                                    {
                                        sort.direction = SortDirection::Descending;
                                        s.is_dirty = true;
                                        cx.emit(FilterPanelEvent::Changed);
                                        cx.notify();
                                    }
                                });
                            }
                        }),
                )
                .separator()
                .item(PopupMenuItem::new("Move Left").on_click({
                    let state = state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.move_sort_left(sort_id, cx));
                    }
                }))
                .item(PopupMenuItem::new("Move Right").on_click({
                    let state = state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.move_sort_right(sort_id, cx));
                    }
                }))
                .separator()
                .item(PopupMenuItem::new("Delete").on_click({
                    let state = state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.remove_sort(sort_id, cx));
                    }
                }))
                .item(PopupMenuItem::new("Clear All Sorts").on_click({
                    let state = state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.clear_sorts(cx));
                    }
                }))
                .item(PopupMenuItem::new("Clear All Filters & Sorts").on_click({
                    let state = state.clone();
                    move |_, _window, cx| {
                        state.update(cx, |s, cx| s.clear_all(cx));
                    }
                }))
            }
        })
}

// Implement SelectItem for ColumnSelectItem
impl SelectItem for ColumnSelectItem {
    type Value = SharedString;

    fn title(&self) -> SharedString {
        self.name.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }

    fn render(&self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .gap_2()
            .child(self.name.clone())
            .when(!self.is_custom, |this| {
                this.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(self.data_type.clone()),
                )
            })
    }
}
