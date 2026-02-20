use std::{ops::Range, rc::Rc, time::Duration};

    use crate::widgets::{
    ActiveTheme, ElementExt, Icon, IconName, StyleSized as _, StyledExt, VirtualListScrollHandle,
    actions::{Cancel, SelectDown, SelectUp},
    h_flex,
    menu::{ContextMenuExt, PopupMenu},
    scroll::{ScrollableMask, Scrollbar},
    v_flex,
    // Checkbox for header select-all
    checkbox::Checkbox,
 };
use gpui::{
    AppContext, Axis, Bounds, ClickEvent, ClipboardItem, Context, Div, DragMoveEvent, ElementId,
    EventEmitter, FocusHandle, Focusable, InteractiveElement, IntoElement, KeyDownEvent,
    ListSizingBehavior, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent, ParentElement,
    Pixels, Point, Render, ScrollStrategy, SharedString, Stateful, StatefulInteractiveElement as _,
    Styled, Task, UniformListScrollHandle, Window, div, prelude::FluentBuilder, px, uniform_list,
};

use super::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SelectionState {
    Column,
    Row,
}

/// The Table event.
#[derive(Clone)]
pub enum TableEvent {
    /// Single click or move to selected row.
    SelectRow(usize),
    /// Double click on the row.
    DoubleClickedRow(usize),
    /// Single click on a cell.
    ClickedCell { row: usize, col: usize },
    /// Double click on a cell.
    DoubleClickedCell { row: usize, col: usize },
    /// Selected column.
    SelectColumn(usize),
    /// The column widths have changed.
    ///
    /// The `Vec<Pixels>` contains the new widths of all columns.
    ColumnWidthsChanged(Vec<Pixels>),
    /// A column has been moved.
    ///
    /// The first `usize` is the original index of the column,
    /// and the second `usize` is the new index of the column.
    MoveColumn(usize, usize),
    /// Cell selection has changed.
    CellSelectionChanged(CellSelection),
    /// Paste data into cells (single cell or TSV grid paste).
    ///
    /// Contains TSV data to be pasted starting from anchor cell.
    PasteCells { anchor: CellPosition, data: String },
    /// Start bulk editing all selected cells.
    ///
    /// Triggered when user types a printable character with multiple cells selected.
    /// The initial_char is the character that triggered the edit.
    StartBulkEdit { initial_char: Option<String> },
    /// Paste the same value to all selected cells (bulk fill).
    ///
    /// Unlike PasteCells which pastes a TSV grid, this fills all selected cells
    /// with the same value.
    BulkPasteCells {
        cells: Vec<CellPosition>,
        value: String,
    },
}

/// The visible range of the rows and columns.
#[derive(Debug, Default)]
pub struct TableVisibleRange {
    /// The visible range of the rows.
    rows: Range<usize>,
    /// The visible range of the columns.
    cols: Range<usize>,
}

impl TableVisibleRange {
    /// Returns the visible range of the rows.
    pub fn rows(&self) -> &Range<usize> {
        &self.rows
    }

    /// Returns the visible range of the columns.
    pub fn cols(&self) -> &Range<usize> {
        &self.cols
    }
}

/// The state for [`Table`].
pub struct TableState<D: TableDelegate> {
    focus_handle: FocusHandle,
    delegate: D,
    pub(super) options: TableOptions,
    /// The bounds of the table container.
    bounds: Bounds<Pixels>,
    /// The bounds of the fixed head cols.
    fixed_head_cols_bounds: Bounds<Pixels>,

    col_groups: Vec<ColGroup>,

    /// Whether the table can loop selection, default is true.
    ///
    /// When the prev/next selection is out of the table bounds, the selection will loop to the other side.
    pub loop_selection: bool,
    /// Whether the table can select column.
    pub col_selectable: bool,
    /// Whether the table can select row.
    pub row_selectable: bool,
    /// Whether the table can sort.
    pub sortable: bool,
    /// Whether the table can resize columns.
    pub col_resizable: bool,
    /// Whether the table can move columns.
    pub col_movable: bool,
    /// Enable/disable fixed columns feature.
    pub col_fixed: bool,

    pub vertical_scroll_handle: UniformListScrollHandle,
    pub horizontal_scroll_handle: VirtualListScrollHandle,

    selected_row: Option<usize>,
    selection_state: SelectionState,
    right_clicked_row: Option<usize>,
    right_clicked_col: Option<usize>,
    /// The column index whose header was right-clicked (for column context menu)
    right_clicked_header_col: Option<usize>,
    selected_col: Option<usize>,
    /// Selected cell (row, col)
    selected_cell: Option<(usize, usize)>,
    /// Flag to prevent row selection when cell is clicked
    cell_was_clicked: bool,
    /// Multi-cell selection state
    cell_selection: CellSelection,

    /// Cached selected row indices for context menu (populated when context menu opens)
    /// This avoids needing to read cell_selection during the on_click handler
    context_menu_selected_rows: Vec<usize>,

    /// The column index that is being resized.
    resizing_col: Option<usize>,

    /// The visible range of the rows and columns.
    visible_range: TableVisibleRange,

    _measure: Vec<Duration>,
    _load_more_task: Task<()>,
}

#[allow(dead_code)]
impl<D> TableState<D>
where
    D: TableDelegate,
{
    /// Create a new TableState with the given delegate.
    pub fn new(delegate: D, _: &mut Window, cx: &mut Context<Self>) -> Self {
        let mut this = Self {
            focus_handle: cx.focus_handle(),
            options: TableOptions::default(),
            delegate,
            col_groups: Vec::new(),
            horizontal_scroll_handle: VirtualListScrollHandle::new(),
            vertical_scroll_handle: UniformListScrollHandle::new(),
            selection_state: SelectionState::Row,
            selected_row: None,
            right_clicked_row: None,
            right_clicked_col: None,
            right_clicked_header_col: None,
            selected_col: None,
            selected_cell: None,
            cell_was_clicked: false,
            cell_selection: CellSelection::new(),
            context_menu_selected_rows: Vec::new(),
            resizing_col: None,
            bounds: Bounds::default(),
            fixed_head_cols_bounds: Bounds::default(),
            visible_range: TableVisibleRange::default(),
            loop_selection: true,
            col_selectable: true,
            row_selectable: true,
            sortable: true,
            col_movable: false,
            col_resizable: true,
            col_fixed: true,
            _load_more_task: Task::ready(()),
            _measure: Vec::new(),
        };

        this.prepare_col_groups(cx);
        this
    }

    /// Returns a reference to the delegate.
    pub fn delegate(&self) -> &D {
        &self.delegate
    }

    /// Returns a mutable reference to the delegate.
    pub fn delegate_mut(&mut self) -> &mut D {
        &mut self.delegate
    }

    /// Set to loop selection, default to true.
    pub fn loop_selection(mut self, loop_selection: bool) -> Self {
        self.loop_selection = loop_selection;
        self
    }

    /// Set to enable/disable column movable, default to true.
    pub fn col_movable(mut self, col_movable: bool) -> Self {
        self.col_movable = col_movable;
        self
    }

    /// Set to enable/disable column resizable, default to true.
    pub fn col_resizable(mut self, col_resizable: bool) -> Self {
        self.col_resizable = col_resizable;
        self
    }

    /// Set to enable/disable column sortable, default true
    pub fn sortable(mut self, sortable: bool) -> Self {
        self.sortable = sortable;
        self
    }

    /// Set to enable/disable row selectable, default true
    pub fn row_selectable(mut self, row_selectable: bool) -> Self {
        self.row_selectable = row_selectable;
        self
    }

    /// Set to enable/disable column selectable, default true
    pub fn col_selectable(mut self, col_selectable: bool) -> Self {
        self.col_selectable = col_selectable;
        self
    }

    /// When we update columns or rows, we need to refresh the table.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.prepare_col_groups(cx);
    }

    /// Scroll to the row at the given index.
    pub fn scroll_to_row(&mut self, row_ix: usize, cx: &mut Context<Self>) {
        self.vertical_scroll_handle
            .scroll_to_item(row_ix, ScrollStrategy::Top);
        cx.notify();
    }

    // Scroll to the column at the given index.
    pub fn scroll_to_col(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        let col_ix = col_ix.saturating_sub(self.fixed_left_cols_count());

        self.horizontal_scroll_handle
            .scroll_to_item(col_ix, ScrollStrategy::Top);
        cx.notify();
    }

    /// Returns the selected row index.
    pub fn selected_row(&self) -> Option<usize> {
        self.selected_row
    }

    /// Sets the selected row to the given index.
    pub fn set_selected_row(&mut self, row_ix: usize, cx: &mut Context<Self>) {
        let is_down = match self.selected_row {
            Some(selected_row) => row_ix > selected_row,
            None => true,
        };

        self.selection_state = SelectionState::Row;
        self.right_clicked_row = None;
        self.right_clicked_col = None;
        self.selected_row = Some(row_ix);
        if let Some(row_ix) = self.selected_row {
            self.vertical_scroll_handle.scroll_to_item(
                row_ix,
                if is_down {
                    ScrollStrategy::Bottom
                } else {
                    ScrollStrategy::Top
                },
            );
        }
        cx.emit(TableEvent::SelectRow(row_ix));
        cx.notify();
    }

    /// Returns the selected column index.
    pub fn selected_col(&self) -> Option<usize> {
        self.selected_col
    }

    /// Sets the selected col to the given index.
    pub fn set_selected_col(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        self.selection_state = SelectionState::Column;
        self.selected_col = Some(col_ix);
        if let Some(col_ix) = self.selected_col {
            self.scroll_to_col(col_ix, cx);
        }
        cx.emit(TableEvent::SelectColumn(col_ix));
        cx.notify();
    }

    /// Sets the selected cell to the given row and column index.
    pub fn set_selected_cell(&mut self, row_ix: usize, col_ix: usize, cx: &mut Context<Self>) {
        self.selected_cell = Some((row_ix, col_ix));
        self.selected_row = None;
        self.selected_col = None;
        self.cell_was_clicked = true;
        cx.notify();
    }

    /// Gets the currently selected cell (row, col) if any.
    pub fn selected_cell(&self) -> Option<(usize, usize)> {
        self.selected_cell
    }

    /// Get the current multi-cell selection state.
    pub fn cell_selection(&self) -> &CellSelection {
        &self.cell_selection
    }

    /// Get the cached selected row indices for the context menu.
    /// This is populated when the context menu opens and can be safely accessed
    /// during the on_click handler without needing to read from the entity.
    pub fn context_menu_selected_rows(&self) -> &[usize] {
        &self.context_menu_selected_rows
    }

    /// Start a new cell selection at the given position.
    /// This clears any previous selection and sets the anchor to this cell.
    pub fn start_cell_selection(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        self.cell_selection.start_selection(row, col);
        cx.emit(TableEvent::CellSelectionChanged(self.cell_selection.clone()));
        cx.notify();
    }

    /// Extend the current selection to include the given cell.
    /// Used for Shift+Click or drag selection.
    pub fn extend_cell_selection(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        self.cell_selection.extend_selection(row, col);
        cx.emit(TableEvent::CellSelectionChanged(self.cell_selection.clone()));
        cx.notify();
    }

    /// Toggle a cell in the selection (for Cmd+Click).
    /// This adds or removes individual cells to/from the selection.
    pub fn toggle_cell_in_selection(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        self.cell_selection.toggle_cell(row, col);
        cx.emit(TableEvent::CellSelectionChanged(self.cell_selection.clone()));
        cx.notify();
    }

    /// Clear the multi-cell selection.
    pub fn clear_cell_selection(&mut self, cx: &mut Context<Self>) {
        if !self.cell_selection.is_empty() {
            self.cell_selection.clear();
            cx.emit(TableEvent::CellSelectionChanged(self.cell_selection.clone()));
            cx.notify();
        }
    }

    /// Select all cells in the table.
    pub fn select_all_cells(&mut self, cx: &mut Context<Self>) {
        let row_count = self.delegate.rows_count(cx);
        let col_count = self.delegate.columns_count(cx);
        
        if row_count > 0 && col_count > 0 {
            self.cell_selection.select_all(row_count, col_count);
            cx.emit(TableEvent::CellSelectionChanged(self.cell_selection.clone()));
            cx.notify();
        }
    }

    /// Clear the selection of the table.
    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        self.selection_state = SelectionState::Row;
        self.selected_row = None;
        self.selected_col = None;
        self.selected_cell = None;
        cx.notify();
    }

    /// Returns the visible range of the rows and columns.
    ///
    /// See [`TableVisibleRange`].
    pub fn visible_range(&self) -> &TableVisibleRange {
        &self.visible_range
    }

    fn prepare_col_groups(&mut self, cx: &mut Context<Self>) {
        self.col_groups = (0..self.delegate.columns_count(cx))
            .map(|col_ix| {
                let column = self.delegate().column(col_ix, cx);
                ColGroup {
                    width: column.width,
                    bounds: Bounds::default(),
                    column,
                }
            })
            .collect();
        cx.notify();
    }

    fn fixed_left_cols_count(&self) -> usize {
        if !self.col_fixed {
            return 0;
        }

        self.col_groups
            .iter()
            .filter(|col| col.column.fixed == Some(ColumnFixed::Left))
            .count()
    }

    /// Resolve column index from an x coordinate (for hit-testing).
    /// Returns `None` if the x coordinate is outside all column bounds.
    ///
    /// This is used for row-level event handling where we need to determine
    /// which column was clicked based on the mouse x position.
    pub fn resolve_col_from_x(&self, x: Pixels) -> Option<usize> {
        for (col_ix, col_group) in self.col_groups.iter().enumerate() {
            let bounds = col_group.bounds;
            if x >= bounds.left() && x < bounds.right() {
                return Some(col_ix);
            }
        }
        None
    }

    /// Resolve cell position from mouse position (for hit-testing).
    /// Returns `None` if the position is outside all column bounds.
    ///
    /// Note: `row_ix` must be determined separately (from the row container).
    pub fn resolve_cell_from_point(&self, row_ix: usize, x: Pixels) -> Option<CellPosition> {
        self.resolve_col_from_x(x).map(|col_ix| CellPosition::new(row_ix, col_ix))
    }

    fn on_row_right_click(
        &mut self,
        _: &MouseDownEvent,
        row_ix: usize,
        col_ix: Option<usize>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) {
        self.right_clicked_row = Some(row_ix);
        self.right_clicked_col = col_ix;
        self.right_clicked_header_col = None;
    }

    fn on_col_header_right_click(
        &mut self,
        _: &MouseDownEvent,
        col_ix: usize,
        _: &mut Window,
        _: &mut Context<Self>,
    ) {
        self.right_clicked_header_col = Some(col_ix);
        self.right_clicked_row = None;
        self.right_clicked_col = None;
    }

    fn on_row_left_click(
        &mut self,
        e: &ClickEvent,
        row_ix: usize,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let col_ix = self.resolve_col_from_x(e.position().x);

        if e.click_count() == 2 {
            // Emit cell-level double-click if we resolved a column
            if let Some(col_ix) = col_ix {
                if col_ix > 0 {
                    cx.emit(TableEvent::DoubleClickedCell {
                        row: row_ix,
                        col: col_ix,
                    });
                }
            }
            cx.emit(TableEvent::DoubleClickedRow(row_ix));
            self.cell_was_clicked = false;
            return;
        }

        // Handle cell-level single-click logic
        if let Some(col_ix) = col_ix {
            let modifiers = e.modifiers();

            if modifiers.shift && self.cell_selection.anchor().is_some() {
                self.extend_cell_selection(row_ix, col_ix, cx);
            } else if modifiers.platform || modifiers.control {
                if self.cell_selection.is_empty() {
                    self.start_cell_selection(row_ix, col_ix, cx);
                } else {
                    self.toggle_cell_in_selection(row_ix, col_ix, cx);
                }
            } else {
                self.clear_cell_selection(cx);
                self.set_selected_cell(row_ix, col_ix, cx);
                if col_ix > 0 {
                    cx.emit(TableEvent::ClickedCell {
                        row: row_ix,
                        col: col_ix,
                    });
                }
            }

            self.cell_was_clicked = true;
        }

        // Skip row selection when a cell was targeted
        if self.cell_was_clicked {
            self.cell_was_clicked = false;
            return;
        }

        if !self.row_selectable {
            return;
        }

        self.set_selected_row(row_ix, cx);
        self.selected_cell = None;
    }

    fn on_col_head_click(&mut self, col_ix: usize, _: &mut Window, cx: &mut Context<Self>) {
        if !self.col_selectable {
            return;
        }

        let Some(col_group) = self.col_groups.get(col_ix) else {
            return;
        };

        if !col_group.column.selectable {
            return;
        }

        self.set_selected_col(col_ix, cx)
    }

    fn has_selection(&self) -> bool {
        self.selected_row.is_some() || self.selected_col.is_some()
    }

    pub(super) fn action_cancel(&mut self, _: &Cancel, _: &mut Window, cx: &mut Context<Self>) {
        // Clear multi-cell selection first
        if !self.cell_selection.is_empty() {
            self.clear_cell_selection(cx);
            return;
        }
        // Then clear other selections
        if self.has_selection() {
            self.clear_selection(cx);
            return;
        }
        cx.propagate();
    }

    pub(super) fn action_select_prev(
        &mut self,
        _: &SelectUp,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rows_count = self.delegate.rows_count(cx);
        if rows_count < 1 {
            return;
        }

        let mut selected_row = self.selected_row.unwrap_or(0);
        if selected_row > 0 {
            selected_row = selected_row.saturating_sub(1);
        } else {
            if self.loop_selection {
                selected_row = rows_count.saturating_sub(1);
            }
        }

        self.set_selected_row(selected_row, cx);
    }

    pub(super) fn action_select_next(
        &mut self,
        _: &SelectDown,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rows_count = self.delegate.rows_count(cx);
        if rows_count < 1 {
            return;
        }

        let selected_row = match self.selected_row {
            Some(selected_row) if selected_row < rows_count.saturating_sub(1) => selected_row + 1,
            Some(selected_row) => {
                if self.loop_selection {
                    0
                } else {
                    selected_row
                }
            }
            _ => 0,
        };

        self.set_selected_row(selected_row, cx);
    }

    pub(super) fn action_select_prev_col(
        &mut self,
        _: &SelectPrevColumn,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut selected_col = self.selected_col.unwrap_or(0);
        let columns_count = self.delegate.columns_count(cx);
        if selected_col > 0 {
            selected_col = selected_col.saturating_sub(1);
        } else {
            if self.loop_selection {
                selected_col = columns_count.saturating_sub(1);
            }
        }
        self.set_selected_col(selected_col, cx);
    }

    pub(super) fn action_select_next_col(
        &mut self,
        _: &SelectNextColumn,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut selected_col = self.selected_col.unwrap_or(0);
        if selected_col < self.delegate.columns_count(cx).saturating_sub(1) {
            selected_col += 1;
        } else {
            if self.loop_selection {
                selected_col = 0;
            }
        }

        self.set_selected_col(selected_col, cx);
    }

    pub(super) fn action_select_all(
        &mut self,
        _: &SelectAll,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Select all cells in the table
        self.select_all_cells(cx);
    }

    pub(super) fn action_copy(
        &mut self,
        _: &Copy,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("action_copy called, selection empty: {}", self.cell_selection.is_empty());
        
        // Only copy if there's a selection
        if self.cell_selection.is_empty() {
            return;
        }

        // Get all selected cells
        let selected_cells = self.cell_selection.selected_cells();
        if selected_cells.is_empty() {
            return;
        }

        tracing::info!("Copying {} cells", selected_cells.len());

        // Determine bounds of selection
        let min_row = selected_cells.iter().map(|c| c.row).min().unwrap();
        let max_row = selected_cells.iter().map(|c| c.row).max().unwrap();
        let min_col = selected_cells.iter().map(|c| c.col).min().unwrap();
        let max_col = selected_cells.iter().map(|c| c.col).max().unwrap();

        // Build TSV string (tab-separated values)
        let mut tsv = String::new();
        for row in min_row..=max_row {
            let mut row_values = Vec::new();
            for col in min_col..=max_col {
                if self.cell_selection.is_selected(row, col) {
                    // Get cell value from delegate
                    let value = self.delegate.cell_text(row, col, cx);
                    row_values.push(value);
                } else {
                    // Empty cell (in case of non-rectangular selection)
                    row_values.push(String::new());
                }
            }
            tsv.push_str(&row_values.join("\t"));
            if row < max_row {
                tsv.push('\n');
            }
        }

        // Write to clipboard
        cx.write_to_clipboard(ClipboardItem::new_string(tsv));
    }

    pub(super) fn action_paste(
        &mut self,
        _: &Paste,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("action_paste called");
        
        // Read from clipboard
        let Some(clipboard_item) = cx.read_from_clipboard() else {
            tracing::warn!("No clipboard item");
            return;
        };

        // Get text from clipboard
        let text = clipboard_item
            .text()
            .unwrap_or_default();
        
        tracing::info!("Clipboard text length: {}", text.len());
        
        if text.is_empty() {
            return;
        }

        // Check if we have multi-cell selection (more than 1 cell)
        let cell_count = self.cell_selection.cell_count();
        
        if cell_count > 1 {
            // BULK PASTE: Fill all selected cells with the clipboard value
            let cells = self.cell_selection.selected_cells();
            tracing::info!("Bulk paste to {} cells", cells.len());
            cx.emit(TableEvent::BulkPasteCells {
                cells,
                value: text,
            });
        } else {
            // SINGLE/TSV PASTE: Paste starting from anchor position
            let anchor = if let Some(anchor_pos) = self.cell_selection.anchor() {
                anchor_pos
            } else if let Some((row, col)) = self.selected_cell {
                CellPosition { row, col }
            } else {
                // No selection, paste at top-left
                CellPosition { row: 0, col: 0 }
            };

            // Emit event for the delegate to handle (TSV grid paste)
            cx.emit(TableEvent::PasteCells {
                anchor,
                data: text,
            });
        }
    }

    /// Handle key down events for bulk editing.
    /// 
    /// When multiple cells are selected and a printable character is typed,
    /// this starts bulk editing mode where the typed value will be applied
    /// to all selected cells.
    pub(super) fn on_key_down(
        &mut self,
        event: &KeyDownEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Only handle if we have a multi-cell selection (more than 1 cell)
        let cell_count = self.cell_selection.cell_count();
        if cell_count <= 1 {
            return;
        }

        // Check if it's a printable character (not a modifier key, function key, etc.)
        // The key_char field contains the character that would be inserted
        if let Some(key_char) = &event.keystroke.key_char {
            // Only trigger for single printable characters
            if key_char.len() == 1 {
                if let Some(c) = key_char.chars().next() {
                    // Must be a printable, non-control character
                    if !c.is_control() && !c.is_whitespace() {
                        tracing::info!("Starting bulk edit with initial char: {:?}", key_char);
                        cx.emit(TableEvent::StartBulkEdit {
                            initial_char: Some(key_char.clone()),
                        });
                    }
                }
            }
        }
    }

    pub(super) fn action_start_editing_cell(
        &mut self,
        _: &StartEditingCell,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("action_start_editing_cell called");
        
        // Check how many cells are selected
        let cell_count = self.cell_selection.cell_count();
        
        // Get current selected cell (anchor)
        let (row, col) = if let Some(anchor) = self.cell_selection.anchor() {
            (anchor.row, anchor.col)
        } else if let Some((row, col)) = self.selected_cell {
            (row, col)
        } else {
            tracing::warn!("No cell selected, cannot start editing");
            return;
        };

        // If multiple cells are selected, start bulk editing mode
        if cell_count > 1 {
            tracing::info!(
                "Starting BULK editing for {} cells (anchor: row={}, col={})",
                cell_count, row, col
            );
            // Emit StartBulkEdit with no initial character (Enter key starts blank edit)
            cx.emit(TableEvent::StartBulkEdit { initial_char: None });
        } else {
            tracing::info!("Starting SINGLE cell editing: row={}, col={}", row, col);
            // Emit ClickedCell event to trigger single-cell editing
            cx.emit(TableEvent::ClickedCell { row, col });
        }
    }

    pub(super) fn action_move_selection_up(
        &mut self,
        _: &MoveSelectionUp,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get current anchor or selected cell
        let current = if let Some(anchor) = self.cell_selection.anchor() {
            anchor
        } else if let Some((row, col)) = self.selected_cell {
            CellPosition { row, col }
        } else {
            // No selection, start at top-left
            CellPosition { row: 0, col: 0 }
        };

        // Move up one row (if possible)
        if current.row > 0 {
            self.start_cell_selection(current.row - 1, current.col, cx);
            self.set_selected_cell(current.row - 1, current.col, cx);
        }
    }

    pub(super) fn action_move_selection_down(
        &mut self,
        _: &MoveSelectionDown,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = if let Some(anchor) = self.cell_selection.anchor() {
            anchor
        } else if let Some((row, col)) = self.selected_cell {
            CellPosition { row, col }
        } else {
            CellPosition { row: 0, col: 0 }
        };

        let row_count = self.delegate.rows_count(cx);
        if current.row + 1 < row_count {
            self.start_cell_selection(current.row + 1, current.col, cx);
            self.set_selected_cell(current.row + 1, current.col, cx);
        }
    }

    pub(super) fn action_move_selection_left(
        &mut self,
        _: &MoveSelectionLeft,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = if let Some(anchor) = self.cell_selection.anchor() {
            anchor
        } else if let Some((row, col)) = self.selected_cell {
            CellPosition { row, col }
        } else {
            CellPosition { row: 0, col: 0 }
        };

        if current.col > 0 {
            self.start_cell_selection(current.row, current.col - 1, cx);
            self.set_selected_cell(current.row, current.col - 1, cx);
        }
    }

    pub(super) fn action_move_selection_right(
        &mut self,
        _: &MoveSelectionRight,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = if let Some(anchor) = self.cell_selection.anchor() {
            anchor
        } else if let Some((row, col)) = self.selected_cell {
            CellPosition { row, col }
        } else {
            CellPosition { row: 0, col: 0 }
        };

        let col_count = self.delegate.columns_count(cx);
        if current.col + 1 < col_count {
            self.start_cell_selection(current.row, current.col + 1, cx);
            self.set_selected_cell(current.row, current.col + 1, cx);
        }
    }

    pub(super) fn action_extend_selection_up(
        &mut self,
        _: &ExtendSelectionUp,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get current selection or start new one
        let current_end = if let Some(range) = &self.cell_selection.range {
            range.end
        } else if let Some((row, col)) = self.selected_cell {
            // Start a new selection at current cell
            self.start_cell_selection(row, col, cx);
            CellPosition { row, col }
        } else {
            return;
        };

        // Extend up one row
        if current_end.row > 0 {
            self.extend_cell_selection(current_end.row - 1, current_end.col, cx);
        }
    }

    pub(super) fn action_extend_selection_down(
        &mut self,
        _: &ExtendSelectionDown,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_end = if let Some(range) = &self.cell_selection.range {
            range.end
        } else if let Some((row, col)) = self.selected_cell {
            self.start_cell_selection(row, col, cx);
            CellPosition { row, col }
        } else {
            return;
        };

        let row_count = self.delegate.rows_count(cx);
        if current_end.row + 1 < row_count {
            self.extend_cell_selection(current_end.row + 1, current_end.col, cx);
        }
    }

    pub(super) fn action_extend_selection_left(
        &mut self,
        _: &ExtendSelectionLeft,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_end = if let Some(range) = &self.cell_selection.range {
            range.end
        } else if let Some((row, col)) = self.selected_cell {
            self.start_cell_selection(row, col, cx);
            CellPosition { row, col }
        } else {
            return;
        };

        if current_end.col > 0 {
            self.extend_cell_selection(current_end.row, current_end.col - 1, cx);
        }
    }

    pub(super) fn action_extend_selection_right(
        &mut self,
        _: &ExtendSelectionRight,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_end = if let Some(range) = &self.cell_selection.range {
            range.end
        } else if let Some((row, col)) = self.selected_cell {
            self.start_cell_selection(row, col, cx);
            CellPosition { row, col }
        } else {
            return;
        };

        let col_count = self.delegate.columns_count(cx);
        if current_end.col + 1 < col_count {
            self.extend_cell_selection(current_end.row, current_end.col + 1, cx);
        }
    }

    /// Scroll table when mouse position is near the edge of the table bounds.
    fn scroll_table_by_col_resizing(
        &mut self,
        mouse_position: Point<Pixels>,
        col_group: &ColGroup,
    ) {
        // Do nothing if pos out of the table bounds right for avoid scroll to the right.
        if mouse_position.x > self.bounds.right() {
            return;
        }

        let mut offset = self.horizontal_scroll_handle.offset();
        let col_bounds = col_group.bounds;

        if mouse_position.x < self.bounds.left()
            && col_bounds.right() < self.bounds.left() + px(20.)
        {
            offset.x += px(1.);
        } else if mouse_position.x > self.bounds.right()
            && col_bounds.right() > self.bounds.right() - px(20.)
        {
            offset.x -= px(1.);
        }

        self.horizontal_scroll_handle.set_offset(offset);
    }

    /// The `ix`` is the index of the col to resize,
    /// and the `size` is the new size for the col.
    fn resize_cols(&mut self, ix: usize, size: Pixels, _: &mut Window, cx: &mut Context<Self>) {
        if !self.col_resizable {
            return;
        }

        let Some(col_group) = self.col_groups.get_mut(ix) else {
            return;
        };

        if !col_group.is_resizable() {
            return;
        }

        let new_width = size.clamp(col_group.column.min_width, col_group.column.max_width);

        // Only update if it actually changed
        if col_group.width != new_width {
            col_group.width = new_width;
            cx.notify();
        }
    }

    fn perform_sort(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        if !self.sortable {
            return;
        }

        let sort = self.col_groups.get(col_ix).and_then(|g| g.column.sort);
        if sort.is_none() {
            return;
        }

        let sort = sort.unwrap();
        let sort = match sort {
            ColumnSort::Ascending => ColumnSort::Default,
            ColumnSort::Descending => ColumnSort::Ascending,
            ColumnSort::Default => ColumnSort::Descending,
        };

        for (ix, col_group) in self.col_groups.iter_mut().enumerate() {
            if ix == col_ix {
                col_group.column.sort = Some(sort);
            } else {
                if col_group.column.sort.is_some() {
                    col_group.column.sort = Some(ColumnSort::Default);
                }
            }
        }

        self.delegate_mut().perform_sort(col_ix, sort, window, cx);

        cx.notify();
    }

    fn move_column(
        &mut self,
        col_ix: usize,
        to_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if col_ix == to_ix {
            return;
        }

        self.delegate.move_column(col_ix, to_ix, window, cx);
        let col_group = self.col_groups.remove(col_ix);
        self.col_groups.insert(to_ix, col_group);

        cx.emit(TableEvent::MoveColumn(col_ix, to_ix));
        cx.notify();
    }

    /// Dispatch delegate's `load_more` method when the visible range is near the end.
    fn load_more_if_need(
        &mut self,
        rows_count: usize,
        visible_end: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let threshold = self.delegate.load_more_threshold();
        // Securely handle subtract logic to prevent attempt to subtract with overflow
        if visible_end >= rows_count.saturating_sub(threshold) {
            if !self.delegate.has_more(cx) {
                return;
            }

            self._load_more_task = cx.spawn_in(window, async move |view, window| {
                _ = view.update_in(window, |view, window, cx| {
                    view.delegate.load_more(window, cx);
                });
            });
        }
    }

    fn update_visible_range_if_need(
        &mut self,
        visible_range: Range<usize>,
        axis: Axis,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Skip when visible range is only 1 item.
        // The visual_list will use first item to measure.
        if visible_range.len() <= 1 {
            return;
        }

        if axis == Axis::Vertical {
            if self.visible_range.rows == visible_range {
                return;
            }
            self.delegate_mut()
                .visible_rows_changed(visible_range.clone(), window, cx);
            self.visible_range.rows = visible_range;
        } else {
            if self.visible_range.cols == visible_range {
                return;
            }
            self.delegate_mut()
                .visible_columns_changed(visible_range.clone(), window, cx);
            self.visible_range.cols = visible_range;
        }
    }

    fn render_cell(&self, col_ix: usize, _window: &mut Window, _cx: &mut Context<Self>) -> Div {
        let Some(col_group) = self.col_groups.get(col_ix) else {
            return div();
        };

        let col_width = col_group.width;
        let col_padding = col_group.column.paddings;
        div()
            .w(col_width)
            .h_full()
            .flex_shrink_0()
            .overflow_hidden()
            .whitespace_nowrap()
            .items_center()
            .table_cell_size(self.options.size)
            .map(|this| match col_padding {
                Some(padding) => this
                    .pl(padding.left)
                    .pr(padding.right)
                    .pt(padding.top)
                    .pb(padding.bottom),
                None => this,
            })
    }

    /// Show Column selection style, when the column is selected and the selection state is Column.
    fn render_col_wrap(&self, col_ix: usize, _: &mut Window, cx: &mut Context<Self>) -> Div {
        let el = h_flex().h_full();
        let selectable = self.col_selectable
            && self
                .col_groups
                .get(col_ix)
                .map(|col_group| col_group.column.selectable)
                .unwrap_or(false);

        if selectable
            && self.selected_col == Some(col_ix)
            && self.selection_state == SelectionState::Column
        {
            el.bg(cx.theme().table_active)
        } else {
            el
        }
    }

    /// DEPRECATED: Per-cell event handler version. Kept for reference.
    /// Use render_cell_styled + row-level event handlers instead for better performance.
    #[allow(dead_code)]
    fn render_cell_with_context(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Div {
        // Delegate to the styled version for rendering, but keep event handlers here
        // for backward compatibility during transition
        let is_selected = self.selected_cell == Some((row_ix, col_ix));
        // Use O(1) cached lookup for selection state
        let is_multi_selected = self.cell_selection.is_selected(row_ix, col_ix);
        // Use cached anchor check when available
        let is_anchor = self.cell_selection.visible_cache().is_anchor(row_ix, col_ix)
            || self.cell_selection.anchor() == Some(CellPosition::new(row_ix, col_ix));

        self.render_col_wrap(col_ix, window, cx).child(
            div()
                // Use NamedInteger to avoid format! string allocation
                .id(ElementId::NamedInteger("cell".into(), (row_ix * 10000 + col_ix) as u64))
                .size_full()
                .relative()
                .on_click(cx.listener(move |this, e: &ClickEvent, _window, cx| {
                    if e.click_count() == 1 {
                        // Check modifiers for multi-cell selection
                        let modifiers = e.modifiers();
                        
                        if modifiers.shift && this.cell_selection.anchor().is_some() {
                            // Shift+Click: extend selection only if there's already a multi-selection anchor
                            this.extend_cell_selection(row_ix, col_ix, cx);
                        } else if modifiers.platform || modifiers.control {
                            // Cmd/Ctrl+Click: multi-select mode
                            if this.cell_selection.is_empty() {
                                // Start new multi-selection
                                this.start_cell_selection(row_ix, col_ix, cx);
                            } else {
                                // Toggle cell in existing selection
                                this.toggle_cell_in_selection(row_ix, col_ix, cx);
                            }
                        } else {
                            // Normal click: clear any multi-selection and select single cell
                            this.clear_cell_selection(cx);
                            this.set_selected_cell(row_ix, col_ix, cx);
                            // Emit ClickedCell to start inline editing (skip row number column)
                            if col_ix > 0 {
                                cx.emit(TableEvent::ClickedCell {
                                    row: row_ix,
                                    col: col_ix,
                                });
                            }
                        }
                        
                        // Don't stop propagation - allow row handler to work too
                    } else if e.click_count() == 2 {
                        // Double click on cell: emit DoubleClickedCell event
                        // but ALSO allow propagation to row handler for DoubleClickedRow
                        if col_ix > 0 {
                            cx.emit(TableEvent::DoubleClickedCell {
                                row: row_ix,
                                col: col_ix,
                            });
                        }
                        
                        // IMPORTANT: Don't stop propagation! Let the row handler also
                        // receive the double-click so it can emit DoubleClickedRow.
                        // This is needed for objects panel which listens to DoubleClickedRow.
                    }
                }))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, e: &MouseDownEvent, _window, _cx| {
                        // Track potential drag start ONLY when Ctrl/Cmd is held
                        // Drag selection is only for multi-select mode
                        let modifiers = e.modifiers;
                        if modifiers.platform || modifiers.control {
                            this.cell_selection.begin_potential_drag(row_ix, col_ix);
                        }
                    }),
                )
                .on_mouse_move(cx.listener(move |this, e: &MouseMoveEvent, _window, cx| {
                    // Only allow drag selection when Ctrl/Cmd is held
                    let modifiers = e.modifiers;
                    if (modifiers.platform || modifiers.control) 
                        && this.cell_selection.should_drag_select(row_ix, col_ix) 
                    {
                        this.extend_cell_selection(row_ix, col_ix, cx);
                    }
                }))
                .on_mouse_up(
                    MouseButton::Left,
                    cx.listener(move |this, _e: &MouseUpEvent, _window, cx| {
                        // End drag tracking
                        this.cell_selection.end_drag();
                        cx.notify();
                    }),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(move |this, e, window, cx| {
                        this.on_row_right_click(e, row_ix, Some(col_ix), window, cx);
                    }),
                )
                .when(is_multi_selected, |this| {
                    // Multi-cell selection background
                    this.child(
                        div()
                            .absolute()
                            .inset_0()
                            .bg(cx.theme().table_active)
                            .when(is_anchor, |div| {
                                // Anchor cell gets a thicker border
                                div.border_2()
                                    .border_color(cx.theme().table_active_border)
                            })
                            .when(!is_anchor, |div| {
                                // Non-anchor cells get a subtle border
                                div.border_1()
                                    .border_color(cx.theme().table_active_border.opacity(0.5))
                            }),
                    )
                })
                .when(is_selected && !is_multi_selected, |this| {
                    // Old single-cell selection (for backward compatibility)
                    this.child(
                        div()
                            .absolute()
                            .inset_0()
                            .bg(cx.theme().table_active)
                            .border_1()
                            .border_color(cx.theme().table_active_border),
                    )
                })
                .child(
                    self.render_cell(col_ix, window, cx)
                        .child(self.measure_render_td(row_ix, col_ix, window, cx)),
                ),
        )
    }

    /// Render a cell with selection styling but WITHOUT event handlers.
    /// This is the performance-optimized version for use with row-level event handling.
    /// 
    /// Event handling should be done at the row level using `resolve_col_from_x()`.
    fn render_cell_styled(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Div {
        let is_selected = self.selected_cell == Some((row_ix, col_ix));
        // Use O(1) cached lookup for selection state
        let is_multi_selected = self.cell_selection.is_selected(row_ix, col_ix);
        // Use cached anchor check when available
        let is_anchor = self.cell_selection.visible_cache().is_anchor(row_ix, col_ix)
            || self.cell_selection.anchor() == Some(CellPosition::new(row_ix, col_ix));

        self.render_col_wrap(col_ix, window, cx).child(
            div()
                // Use a simpler ID without format! allocation
                .id(ElementId::NamedInteger("cell".into(), (row_ix * 10000 + col_ix) as u64))
                .size_full()
                .relative()
                // Selection styling (without event handlers)
                .when(is_multi_selected, |this| {
                    // Multi-cell selection background
                    this.child(
                        div()
                            .absolute()
                            .inset_0()
                            .bg(cx.theme().table_active)
                            .when(is_anchor, |div| {
                                // Anchor cell gets a thicker border
                                div.border_2()
                                    .border_color(cx.theme().table_active_border)
                            })
                            .when(!is_anchor, |div| {
                                // Non-anchor cells get a subtle border
                                div.border_1()
                                    .border_color(cx.theme().table_active_border.opacity(0.5))
                            }),
                    )
                })
                .when(is_selected && !is_multi_selected, |this| {
                    // Old single-cell selection (for backward compatibility)
                    this.child(
                        div()
                            .absolute()
                            .inset_0()
                            .bg(cx.theme().table_active)
                            .border_1()
                            .border_color(cx.theme().table_active_border),
                    )
                })
                .child(
                    self.render_cell(col_ix, window, cx)
                        .child(self.measure_render_td(row_ix, col_ix, window, cx)),
                ),
        )
    }

    fn render_resize_handle(
        &self,
        ix: usize,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        const HANDLE_SIZE: Pixels = px(2.);

        let resizable = self.col_resizable
            && self
                .col_groups
                .get(ix)
                .map(|col| col.is_resizable())
                .unwrap_or(false);
        if !resizable {
            return div().into_any_element();
        }

        let group_id = SharedString::from(format!("resizable-handle:{}", ix));

        h_flex()
            .id(("resizable-handle", ix))
            .group(group_id.clone())
            .occlude()
            .cursor_col_resize()
            .h_full()
            .w(HANDLE_SIZE)
            .ml(-(HANDLE_SIZE))
            .justify_end()
            .items_center()
            .child(
                div()
                    .h_full()
                    .justify_center()
                    .bg(cx.theme().table_row_border)
                    .group_hover(&group_id, |this| this.bg(cx.theme().border).h_full())
                    .w(px(1.)),
            )
            .on_drag_move(
                cx.listener(move |view, e: &DragMoveEvent<ResizeColumn>, window, cx| {
                    match e.drag(cx) {
                        ResizeColumn((entity_id, ix)) => {
                            if cx.entity_id() != *entity_id {
                                return;
                            }

                            // sync col widths into real widths
                            // TODO: Consider to remove this, this may not need now.
                            // for (_, col_group) in view.col_groups.iter_mut().enumerate() {
                            //     col_group.width = col_group.bounds.size.width;
                            // }

                            let ix = *ix;
                            view.resizing_col = Some(ix);

                            let col_group = view
                                .col_groups
                                .get(ix)
                                .expect("BUG: invalid col index")
                                .clone();

                            view.resize_cols(
                                ix,
                                e.event.position.x - HANDLE_SIZE - col_group.bounds.left(),
                                window,
                                cx,
                            );

                            // scroll the table if the drag is near the edge
                            view.scroll_table_by_col_resizing(e.event.position, &col_group);
                        }
                    };
                }),
            )
            .on_drag(ResizeColumn((cx.entity_id(), ix)), |drag, _, _, cx| {
                cx.stop_propagation();
                cx.new(|_| drag.clone())
            })
            .on_mouse_up_out(
                MouseButton::Left,
                cx.listener(|view, _, _, cx| {
                    if view.resizing_col.is_none() {
                        return;
                    }

                    view.resizing_col = None;

                    let new_widths = view.col_groups.iter().map(|g| g.width).collect();
                    cx.emit(TableEvent::ColumnWidthsChanged(new_widths));
                    cx.notify();
                }),
            )
            .into_any_element()
    }

    fn render_sort_icon(
        &self,
        col_ix: usize,
        col_group: &ColGroup,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        if !self.sortable {
            return None;
        }

        let Some(sort) = col_group.column.sort else {
            return None;
        };

        let (icon, is_on) = match sort {
            ColumnSort::Ascending => (IconName::SortAscending, true),
            ColumnSort::Descending => (IconName::SortDescending, true),
            ColumnSort::Default => (IconName::ChevronsUpDown, false),
        };

        Some(
            div()
                .id(("icon-sort", col_ix))
                .p(px(2.))
                .rounded(cx.theme().radius / 2.)
                .map(|this| match is_on {
                    true => this,
                    false => this.opacity(0.5),
                })
                .hover(|this| this.bg(cx.theme().secondary).opacity(7.))
                .active(|this| this.bg(cx.theme().secondary_active).opacity(1.))
                .on_click(
                    cx.listener(move |table, _, window, cx| table.perform_sort(col_ix, window, cx)),
                )
                .child(
                    Icon::new(icon)
                        .size_3()
                        .text_color(cx.theme().secondary_foreground),
                ),
        )
    }

    /// Render the column header.
    /// The children must be one by one items.
    /// Because the horizontal scroll handle will use the child_item_bounds to
    /// calculate the item position for itself's `scroll_to_item` method.
    fn render_th(&mut self, col_ix: usize, window: &mut Window, cx: &mut Context<Self>) -> Div {
        let entity_id = cx.entity_id();
        let col_group = self.col_groups.get(col_ix).expect("BUG: invalid col index");

        let movable = self.col_movable && col_group.column.movable;
        let paddings = col_group.column.paddings;
        let name = col_group.column.name.clone();

        h_flex()
            .h_full()
            .child(
                self.render_cell(col_ix, window, cx)
                    .id(("col-header", col_ix))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.on_col_head_click(col_ix, window, cx);
                    }))
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(move |this, e, window, cx| {
                            this.on_col_header_right_click(e, col_ix, window, cx);
                        }),
                    )
                    .child({
                        let mut header_content = h_flex()
                            .size_full()
                            .justify_between()
                            .items_center()
                            // default header content from delegate
                            .child(self.delegate.render_th(col_ix, window, cx));

                        // If this is the first column (col_ix == 0), render a select-all checkbox at the left
                        if col_ix == 0 {
                            // Capture table entity so the checkbox handler can update selection state
                            let table_entity = cx.entity().clone();
                            // Determine checkbox state: checked when all cells selected, indeterminate when partial
                            let all = {
                                let rows = self.delegate.rows_count(cx);
                                let cols = self.delegate.columns_count(cx);
                                if rows > 0 && cols > 0 {
                                    // All selected if selection covers full table and there are no additional exceptions
                                    if let Some(ref range) = self.cell_selection.range {
                                        range.anchor.row == 0
                                            && range.anchor.col == 0
                                            && range.end.row == rows - 1
                                            && range.end.col == cols - 1
                                            && self.cell_selection.additional_cells.is_empty()
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            };

                            let _any = !self.cell_selection.is_empty();

                            // Create checkbox element. Use Checkbox::new with id derived from table entity and header
                            let checkbox_id = SharedString::from(format!("table-{}-select-all", cx.entity_id()));
                            let checkbox = Checkbox::new(checkbox_id.clone())
                                .checked(all)
                                .on_click(move |new_checked, _window, cx| {
                                    // Update the table entity selection based on new checkbox state
                                    let checked = *new_checked;
                                    table_entity.update(cx, |table, cx| {
                                        if checked {
                                            table.select_all_cells(cx);
                                        } else {
                                            table.clear_cell_selection(cx);
                                        }
                                    });
                                });

                            // Put checkbox at the start of header content
                            header_content = header_content.child(checkbox);
                        }

                        header_content
                            .when_some(paddings, |this, paddings| {
                                // Leave right space for the sort icon, if this column have custom padding
                                let offset_pr =
                                    self.options.size.table_cell_padding().right - paddings.right;
                                this.pr(offset_pr.max(px(0.)))
                            })
                            .children(self.render_sort_icon(col_ix, &col_group, window, cx))
                    })
                    .when(movable, |this| {
                        this.on_drag(
                            DragColumn {
                                entity_id,
                                col_ix,
                                name,
                                width: col_group.width,
                            },
                            |drag, _, _, cx| {
                                cx.stop_propagation();
                                cx.new(|_| drag.clone())
                            },
                        )
                        .drag_over::<DragColumn>(|this, _, _, cx| {
                            this.rounded_l_none()
                                .border_l_2()
                                .border_r_0()
                                .border_color(cx.theme().drag_border)
                        })
                        .on_drop(cx.listener(
                            move |table, drag: &DragColumn, window, cx| {
                                // If the drag col is not the same as the drop col, then swap the cols.
                                if drag.entity_id != cx.entity_id() {
                                    return;
                                }

                                table.move_column(drag.col_ix, col_ix, window, cx);
                            },
                        ))
                    }),
            )
            // resize handle
            .child(self.render_resize_handle(col_ix, window, cx))
            // to save the bounds of this col.
            .on_prepaint({
                let view = cx.entity().clone();
                move |bounds, _, cx| view.update(cx, |r, _| r.col_groups[col_ix].bounds = bounds)
            })
    }

    fn render_table_header(
        &mut self,
        left_columns_count: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let view = cx.entity().clone();
        let horizontal_scroll_handle = self.horizontal_scroll_handle.clone();

        // Reset fixed head columns bounds, if no fixed columns are present
        if left_columns_count == 0 {
            self.fixed_head_cols_bounds = Bounds::default();
        }

        let mut header = self.delegate_mut().render_header(window, cx);
        let style = header.style().clone();

        header
            .h_flex()
            .w_full()
            .h(self.options.size.table_row_height())
            .flex_shrink_0()
            .border_b_1()
            .border_color(cx.theme().border)
            .text_color(cx.theme().table_head_foreground)
            .refine_style(&style)
            .when(left_columns_count > 0, |this| {
                let view = view.clone();

                // Pre-collect fixed-left column indices to avoid cloning col_groups
                let fixed_col_indices: Vec<usize> = (0..self.col_groups.len())
                    .filter(|&i| self.col_groups[i].column.fixed == Some(ColumnFixed::Left))
                    .collect();

                // Render left fixed columns
                this.child(
                    h_flex()
                        .relative()
                        .h_full()
                        .bg(cx.theme().table_head)
                        .children(
                            fixed_col_indices
                                .into_iter()
                                .enumerate()
                                .map(|(col_ix, _)| self.render_th(col_ix, window, cx)),
                        )
                        .child(
                            // Fixed columns border
                            div()
                                .absolute()
                                .top_0()
                                .right_0()
                                .bottom_0()
                                .w_0()
                                .flex_shrink_0()
                                .border_r_1()
                                .border_color(cx.theme().border),
                        )
                        .on_prepaint(move |bounds, _, cx| {
                            view.update(cx, |r, _| r.fixed_head_cols_bounds = bounds)
                        }),
                )
            })
            .child(
                // Columns
                h_flex()
                    .id("table-head")
                    .size_full()
                    .overflow_scroll()
                    .relative()
                    .track_scroll(&horizontal_scroll_handle)
                    .bg(cx.theme().table_head)
                    .child({
                        // Pre-collect scrollable column count to avoid cloning col_groups
                        let scrollable_count = self.col_groups.len().saturating_sub(left_columns_count);

                        h_flex()
                            .relative()
                            .children(
                                (0..scrollable_count)
                                    .map(|col_ix| {
                                        self.render_th(left_columns_count + col_ix, window, cx)
                                    }),
                            )
                            .child(self.delegate.render_last_empty_col(window, cx))
                    }),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_table_row(
        &mut self,
        row_ix: usize,
        rows_count: usize,
        left_columns_count: usize,
        col_sizes: Rc<Vec<gpui::Size<Pixels>>>,
        columns_count: usize,
        is_filled: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let horizontal_scroll_handle = self.horizontal_scroll_handle.clone();
        let is_stripe_row = self.options.stripe && row_ix % 2 != 0;
        let is_selected = self.selected_row == Some(row_ix);
        let view = cx.entity().clone();
        let row_height = self.options.size.table_row_height();

        if row_ix < rows_count {
            let is_last_row = row_ix + 1 == rows_count;
            let need_render_border = is_selected || !is_last_row || !is_filled;

            let mut tr = self.delegate.render_tr(row_ix, window, cx);
            let style = tr.style().clone();

            tr.h_flex()
                .w_full()
                .h(row_height)
                .when(need_render_border, |this| {
                    this.border_b_1().border_color(cx.theme().table_row_border)
                })
                .when(is_stripe_row, |this| this.bg(cx.theme().table_even))
                .refine_style(&style)
                .hover(|this| {
                    if is_selected || self.right_clicked_row == Some(row_ix) {
                        this
                    } else {
                        this.bg(cx.theme().table_hover)
                    }
                })
                .when(left_columns_count > 0, |this| {
                    // Left fixed columns
                    this.child(
                        h_flex()
                            .relative()
                            .h_full()
                            .children({
                                let mut items = Vec::with_capacity(left_columns_count);

                                (0..left_columns_count).for_each(|col_ix| {
                                    items.push(
                                        self.render_cell_styled(row_ix, col_ix, window, cx),
                                    );
                                });

                                items
                            })
                            .child(
                                // Fixed columns border
                                div()
                                    .absolute()
                                    .top_0()
                                    .right_0()
                                    .bottom_0()
                                    .w_0()
                                    .flex_shrink_0()
                                    .border_r_1()
                                    .border_color(cx.theme().border),
                            ),
                    )
                })
                .child(
                    h_flex()
                        .flex_1()
                        .h_full()
                        .overflow_hidden()
                        .relative()
                        .child(
                            crate::widgets::virtual_list::virtual_list(
                                view,
                                row_ix,
                                Axis::Horizontal,
                                col_sizes,
                                {
                                    move |table, visible_range: Range<usize>, window, cx| {
                                        table.update_visible_range_if_need(
                                            visible_range.clone(),
                                            Axis::Horizontal,
                                            window,
                                            cx,
                                        );

                                        let mut items = Vec::with_capacity(
                                            visible_range.end - visible_range.start,
                                        );

                                        visible_range.for_each(|col_ix| {
                                            let col_ix = col_ix + left_columns_count;
                                            let el = table.render_cell_styled(
                                                row_ix, col_ix, window, cx,
                                            );

                                            items.push(el);
                                        });

                                        items
                                    }
                                },
                            )
                            .with_scroll_handle(&self.horizontal_scroll_handle),
                        )
                        .child(self.delegate.render_last_empty_col(window, cx)),
                )
                // Row selected style
                .when_some(self.selected_row, |this, _| {
                    this.when(
                        is_selected && self.selection_state == SelectionState::Row,
                        |this| {
                            this.border_color(gpui::transparent_white()).child(
                                div()
                                    .top(if row_ix == 0 { px(0.) } else { px(-1.) })
                                    .left(px(0.))
                                    .right(px(0.))
                                    .bottom(px(-1.))
                                    .absolute()
                                    .bg(cx.theme().table_active)
                                    .border_1()
                                    .border_color(cx.theme().table_active_border),
                            )
                        },
                    )
                })
                // Row right click row style
                .when(self.right_clicked_row == Some(row_ix), |this| {
                    this.border_color(gpui::transparent_white()).child(
                        div()
                            .top(if row_ix == 0 { px(0.) } else { px(-1.) })
                            .left(px(0.))
                            .right(px(0.))
                            .bottom(px(-1.))
                            .absolute()
                            .border_1()
                            .border_color(cx.theme().selection),
                    )
                })
                // Row-level event handlers (replaces per-cell handlers for performance)
                .on_click(cx.listener(move |this, e, window, cx| {
                    this.on_row_left_click(e, row_ix, window, cx);
                }))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, e: &MouseDownEvent, _window, _cx| {
                        // Track potential drag start for multi-select mode
                        let modifiers = e.modifiers;
                        if modifiers.platform || modifiers.control {
                            if let Some(col_ix) = this.resolve_col_from_x(e.position.x) {
                                this.cell_selection.begin_potential_drag(row_ix, col_ix);
                            }
                        }
                    }),
                )
                .on_mouse_move(cx.listener(move |this, e: &MouseMoveEvent, _window, cx| {
                    let modifiers = e.modifiers;
                    if modifiers.platform || modifiers.control {
                        if let Some(col_ix) = this.resolve_col_from_x(e.position.x) {
                            if this.cell_selection.should_drag_select(row_ix, col_ix) {
                                this.extend_cell_selection(row_ix, col_ix, cx);
                            }
                        }
                    }
                }))
                .on_mouse_up(
                    MouseButton::Left,
                    cx.listener(move |this, _e: &MouseUpEvent, _window, cx| {
                        this.cell_selection.end_drag();
                        cx.notify();
                    }),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(move |this, e: &MouseDownEvent, window, cx| {
                        let col_ix = this.resolve_col_from_x(e.position.x);
                        this.on_row_right_click(e, row_ix, col_ix, window, cx);
                    }),
                )
        } else {
            // Render fake rows to fill the rest table space
            self.delegate
                .render_tr(row_ix, window, cx)
                .h_flex()
                .w_full()
                .h(row_height)
                .border_b_1()
                .border_color(cx.theme().table_row_border)
                .when(is_stripe_row, |this| this.bg(cx.theme().table_even))
                .children((0..columns_count).map(|col_ix| {
                    h_flex()
                        .left(horizontal_scroll_handle.offset().x)
                        .child(self.render_cell(col_ix, window, cx))
                }))
                .child(self.delegate.render_last_empty_col(window, cx))
        }
    }

    /// Calculate the extra rows needed to fill the table empty space when `stripe` is true.
    fn calculate_extra_rows_needed(
        &self,
        total_height: Pixels,
        actual_height: Pixels,
        row_height: Pixels,
    ) -> usize {
        let mut extra_rows_needed = 0;

        let remaining_height = total_height - actual_height;
        if remaining_height > px(0.) {
            extra_rows_needed = (remaining_height / row_height).floor() as usize;
        }

        extra_rows_needed
    }

    #[inline]
    fn measure_render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if !crate::widgets::measure_enable() {
            return self
                .delegate
                .render_td(row_ix, col_ix, window, cx)
                .into_any_element();
        }

        let start = std::time::Instant::now();
        let el = self.delegate.render_td(row_ix, col_ix, window, cx);
        self._measure.push(start.elapsed());
        el.into_any_element()
    }

    fn measure(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        if !crate::widgets::measure_enable() {
            return;
        }

        // Print avg measure time of each td
        if self._measure.len() > 0 {
            let total = self
                ._measure
                .iter()
                .fold(Duration::default(), |acc, d| acc + *d);
            let avg = total / self._measure.len() as u32;
            tracing::trace!(
                cell_count = self._measure.len(),
                total_duration = ?total,
                avg_duration = ?avg,
                "Table render performance"
            );
        }
        self._measure.clear();
    }

    fn render_vertical_scrollbar(
        &mut self,

        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        Some(
            div()
                .occlude()
                .absolute()
                .top(self.options.size.table_row_height())
                .right_0()
                .bottom_0()
                .w(Scrollbar::width())
                .child(Scrollbar::vertical(&self.vertical_scroll_handle).max_fps(60)),
        )
    }

    fn render_horizontal_scrollbar(
        &mut self,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .occlude()
            .absolute()
            .left(self.fixed_head_cols_bounds.size.width)
            .right_0()
            .bottom_0()
            .h(Scrollbar::width())
            .child(Scrollbar::horizontal(&self.horizontal_scroll_handle))
    }
}

impl<D> Focusable for TableState<D>
where
    D: TableDelegate,
{
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
impl<D> EventEmitter<TableEvent> for TableState<D> where D: TableDelegate {}

impl<D> Render for TableState<D>
where
    D: TableDelegate,
{
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.measure(window, cx);

        let columns_count = self.delegate.columns_count(cx);
        let left_columns_count = self
            .col_groups
            .iter()
            .filter(|col| self.col_fixed && col.column.fixed == Some(ColumnFixed::Left))
            .count();
        let rows_count = self.delegate.rows_count(cx);
        let loading = self.delegate.loading(cx);

        let row_height = self.options.size.table_row_height();
        let total_height = self
            .vertical_scroll_handle
            .0
            .borrow()
            .base_handle
            .bounds()
            .size
            .height;
        let actual_height = row_height * rows_count as f32;
        let extra_rows_count =
            self.calculate_extra_rows_needed(total_height, actual_height, row_height);
        let render_rows_count = if self.options.stripe {
            rows_count + extra_rows_count
        } else {
            rows_count
        };
        let right_clicked_row = self.right_clicked_row;
        let is_filled = total_height > Pixels::ZERO && total_height <= actual_height;

        let loading_view = if loading {
            Some(
                self.delegate
                    .render_loading(self.options.size, window, cx)
                    .into_any_element(),
            )
        } else {
            None
        };

        let empty_view = if rows_count == 0 {
            Some(
                div()
                    .size_full()
                    .child(self.delegate.render_empty(window, cx))
                    .into_any_element(),
            )
        } else {
            None
        };

        let inner_table = v_flex()
            .id("table-inner")
            .size_full()
            .overflow_hidden()
            .child(self.render_table_header(left_columns_count, window, cx))
            .context_menu({
                let view = cx.entity().clone();
                move |this, window: &mut Window, cx: &mut Context<PopupMenu>| {
                    // Read state including cell selection rows (before update to avoid borrow issues)
                    let (row_ix, col_ix, header_col_ix, selected_rows) = {
                        let state = view.read(cx);
                        // Get unique row indices from cell selection
                        let selected_rows: Vec<usize> = {
                            let cells = state.cell_selection.selected_cells();
                            let mut rows: std::collections::HashSet<usize> =
                                cells.iter().map(|c| c.row).collect();
                            // Include the right-clicked row if any
                            if let Some(row) = state.right_clicked_row {
                                rows.insert(row);
                            }
                            rows.into_iter().collect()
                        };
                        (
                            state.right_clicked_row,
                            state.right_clicked_col,
                            state.right_clicked_header_col,
                            selected_rows,
                        )
                    };

                    if let Some(header_col_ix) = header_col_ix {
                        // Column header was right-clicked
                        view.update(cx, |menu, cx| {
                            menu.delegate_mut()
                                .column_context_menu(header_col_ix, this, window, cx)
                        })
                    } else if let Some(row_ix) = row_ix {
                        // Cache the selected rows and pass to delegate for context menu handlers
                        view.update(cx, |menu, cx| {
                            menu.context_menu_selected_rows = selected_rows.clone();
                            // Pass selected rows to delegate so it can use them in on_click handlers
                            menu.delegate_mut().set_context_menu_selection(selected_rows);
                            menu.delegate_mut()
                                .context_menu(row_ix, col_ix, this, window, cx)
                        })
                    } else {
                        this
                    }
                }
            })
            .map(|this| {
                if rows_count == 0 {
                    this.children(empty_view)
                } else {
                    this.child(
                        h_flex().id("table-body").flex_grow().size_full().child(
                            uniform_list(
                                "table-uniform-list",
                                render_rows_count,
                                cx.processor(
                                    move |table, visible_range: Range<usize>, window, cx| {
                                        // We must calculate the col sizes here, because the col sizes
                                        // need render_th first, then that method will set the bounds of each col.
                                        let col_sizes: Rc<Vec<gpui::Size<Pixels>>> = Rc::new(
                                            table
                                                .col_groups
                                                .iter()
                                                .skip(left_columns_count)
                                                .map(|col| col.bounds.size)
                                                .collect(),
                                        );

                                        table.load_more_if_need(
                                            rows_count,
                                            visible_range.end,
                                            window,
                                            cx,
                                        );
                                        table.update_visible_range_if_need(
                                            visible_range.clone(),
                                            Axis::Vertical,
                                            window,
                                            cx,
                                        );

                                        if visible_range.end > rows_count {
                                            table.scroll_to_row(
                                                std::cmp::min(
                                                    visible_range.start,
                                                    rows_count.saturating_sub(1),
                                                ),
                                                cx,
                                            );
                                        }

                                        // Update visible selection cache once before rendering all visible cells
                                        // This allows O(1) selection lookups during cell rendering
                                        let visible_cols = table.visible_range.cols.clone();
                                        table.cell_selection.update_visible_cache(
                                            visible_range.clone(),
                                            visible_cols,
                                        );

                                        let mut items = Vec::with_capacity(
                                            visible_range.end.saturating_sub(visible_range.start),
                                        );

                                        // Render fake rows to fill the table
                                        visible_range.for_each(|row_ix| {
                                            // Render real rows for available data
                                            items.push(table.render_table_row(
                                                row_ix,
                                                rows_count,
                                                left_columns_count,
                                                col_sizes.clone(),
                                                columns_count,
                                                is_filled,
                                                window,
                                                cx,
                                            ));
                                        });

                                        items
                                    },
                                ),
                            )
                            .flex_grow()
                            .size_full()
                            .with_sizing_behavior(ListSizingBehavior::Auto)
                            .track_scroll(&self.vertical_scroll_handle)
                            .into_any_element(),
                        ),
                    )
                }
            });

        div()
            .size_full()
            .children(loading_view)
            .when(!loading, |this| {
                this.child(inner_table)
                    .child(ScrollableMask::new(
                        Axis::Horizontal,
                        &self.horizontal_scroll_handle,
                    ))
                    .when(right_clicked_row.is_some(), |this| {
                        this.on_mouse_down_out(cx.listener(|this, _, _, cx| {
                            this.right_clicked_row = None;
                            cx.notify();
                        }))
                    })
            })
            .on_prepaint({
                let state = cx.entity();
                move |bounds, _, cx| state.update(cx, |state, _| state.bounds = bounds)
            })
            .when(!window.is_inspector_picking(cx), |this| {
                this.child(
                    div()
                        .absolute()
                        .top_0()
                        .size_full()
                        .when(self.options.scrollbar_visible.bottom, |this| {
                            this.child(self.render_horizontal_scrollbar(window, cx))
                        })
                        .when(
                            self.options.scrollbar_visible.right && rows_count > 0,
                            |this| this.children(self.render_vertical_scrollbar(window, cx)),
                        ),
                )
            })
    }
}
