//! Window-level tests for the table widget.
//!
//! These open a real GPUI window, paint a small table through the public
//! `Table` element, and drive it by clicking painted pixels — covering the
//! wiring a headless construction test can't: that rows and cells actually
//! lay out, and that a click on a painted cell mutates the selection state
//! it looks like it lands on.

use gpui::{
    App, AppContext as _, Bounds, Context, Entity, IntoElement, Modifiers, ParentElement, Pixels,
    Render, Styled, TestAppContext, VisualTestContext, Window, div, px, size,
};

use crate::widgets::table::{Column, Table, TableDelegate, TableState};

struct MockDelegate {
    rows: usize,
    cols: usize,
}

impl MockDelegate {
    fn new(rows: usize, cols: usize) -> Self {
        Self { rows, cols }
    }
}

impl TableDelegate for MockDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.cols
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.rows
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        Column::new(format!("col{col_ix}"), format!("Column {col_ix}")).width(px(120.))
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        format!("r{row_ix}c{col_ix}")
    }
}

/// Hosts a `Table` as a window root, since `Table` itself is a `RenderOnce`
/// element rather than an `Entity`.
struct TableHarness {
    state: Entity<TableState<MockDelegate>>,
}

impl Render for TableHarness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(Table::new(&self.state))
    }
}

/// Wide and tall enough that a handful of rows and columns paint without
/// the uniform/virtual lists deciding to virtualize any of them away.
const WINDOW_SIZE: (f32, f32) = (480., 400.);

fn init_test(cx: &mut TestAppContext) {
    cx.update(crate::init);
}

/// Open a window showing a table with `rows` rows and `cols` columns, and
/// paint it.
fn open_table(
    cx: &mut TestAppContext,
    rows: usize,
    cols: usize,
) -> (Entity<TableState<MockDelegate>>, VisualTestContext) {
    let window = cx.add_window(|window, cx| {
        let state = cx.new(|cx| TableState::new(MockDelegate::new(rows, cols), window, cx));
        TableHarness { state }
    });
    let harness = window
        .root(cx)
        .expect("table harness should be the window root");
    let state = harness.read_with(cx, |harness, _cx| harness.state.clone());

    let visual_cx = VisualTestContext::from_window(window.into(), cx);
    visual_cx.simulate_resize(size(px(WINDOW_SIZE.0), px(WINDOW_SIZE.1)));
    visual_cx.run_until_parked();
    (state, visual_cx)
}

/// Bounds of a painted cell, by the `debug_selector` `render_cell_styled` sets.
fn cell_bounds(cx: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
    cx.debug_bounds(selector)
        .unwrap_or_else(|| panic!("cell {selector:?} should be painted"))
}

fn click_cell(cx: &mut VisualTestContext, selector: &'static str) {
    let bounds = cell_bounds(cx, selector);
    cx.simulate_click(bounds.center(), Modifiers::none());
    cx.run_until_parked();
}

#[gpui::test]
fn table_state_builder_configures_selection_and_delegate_options(cx: &mut TestAppContext) {
    init_test(cx);

    let window = cx.add_window(|window, cx| {
        TableState::new(MockDelegate::new(4, 3), window, cx)
            .loop_selection(false)
            .col_movable(true)
            .col_resizable(false)
            .sortable(false)
            .row_selectable(true)
            .col_selectable(false)
    });
    let state = window
        .root(cx)
        .expect("table state should be the window root");

    state.read_with(cx, |state, cx| {
        assert!(!state.loop_selection);
        assert!(state.col_movable);
        assert!(!state.col_resizable);
        assert!(!state.sortable);
        assert!(state.row_selectable);
        assert!(!state.col_selectable);
        assert_eq!(state.delegate().rows_count(cx), 4);
        assert_eq!(state.delegate().columns_count(cx), 3);
    });
}

#[gpui::test]
fn clicking_a_cell_selects_it_and_clicking_the_first_column_selects_the_row(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let (state, mut cx) = open_table(cx, 4, 3);

    click_cell(&mut cx, "cell:1:1");
    assert_eq!(
        state.read_with(&cx, |state, _| state.selected_cell()),
        Some((1, 1)),
        "clicking a data cell should select that cell"
    );
    assert_eq!(
        state.read_with(&cx, |state, _| state.selected_row()),
        None,
        "selecting a cell should not also select a row"
    );

    click_cell(&mut cx, "cell:2:0");
    assert_eq!(
        state.read_with(&cx, |state, _| state.selected_row()),
        Some(2),
        "clicking the first column should select the whole row"
    );
    assert_eq!(
        state.read_with(&cx, |state, _| state.selected_cell()),
        None,
        "selecting a row should clear the previous cell selection"
    );
}
