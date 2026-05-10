use crate::widgets::{ActiveTheme, Sizable, Size, actions::Cancel};
use gpui::{
    App, Edges, Entity, Focusable, InteractiveElement, IntoElement, KeyBinding, ParentElement,
    Pixels, RenderOnce, Styled, Window, actions, div, prelude::FluentBuilder,
};

mod column;
mod delegate;
mod loading;
mod pagination;
mod selection;
mod state;

pub use column::*;
pub use delegate::*;
pub use pagination::*;
pub use selection::*;
pub use state::*;

actions!(
    table,
    [
        SelectPrevColumn,
        SelectNextColumn,
        SelectAll,
        Copy,
        Paste,
        MoveSelectionUp,
        MoveSelectionDown,
        MoveSelectionLeft,
        MoveSelectionRight,
        ExtendSelectionUp,
        ExtendSelectionDown,
        ExtendSelectionLeft,
        ExtendSelectionRight,
        StartEditingCell,
        MoveToFirstRow,
        MoveToLastRow,
        MoveToFirstColumn,
        MoveToLastColumn,
        TablePageUp,
        TablePageDown,
    ]
);

const CONTEXT: &str = "Table";
pub(crate) fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("escape", Cancel, Some(CONTEXT)),
        KeyBinding::new("cmd-a", SelectAll, Some(CONTEXT)),
        KeyBinding::new("cmd-c", Copy, Some(CONTEXT)),
        KeyBinding::new("cmd-v", Paste, Some(CONTEXT)),
        KeyBinding::new("enter", StartEditingCell, Some(CONTEXT)),
        KeyBinding::new("return", StartEditingCell, Some(CONTEXT)),
        // Arrow keys for cell navigation (move single cell selection)
        KeyBinding::new("up", MoveSelectionUp, Some(CONTEXT)),
        KeyBinding::new("down", MoveSelectionDown, Some(CONTEXT)),
        KeyBinding::new("left", MoveSelectionLeft, Some(CONTEXT)),
        KeyBinding::new("right", MoveSelectionRight, Some(CONTEXT)),
        // Shift+Arrow for extending multi-cell selection
        KeyBinding::new("shift-up", ExtendSelectionUp, Some(CONTEXT)),
        KeyBinding::new("shift-down", ExtendSelectionDown, Some(CONTEXT)),
        KeyBinding::new("shift-left", ExtendSelectionLeft, Some(CONTEXT)),
        KeyBinding::new("shift-right", ExtendSelectionRight, Some(CONTEXT)),
        // Tab navigation across cells when table has focus (not when an Input is focused)
        KeyBinding::new("tab", MoveSelectionRight, Some(CONTEXT)),
        KeyBinding::new("shift-tab", MoveSelectionLeft, Some(CONTEXT)),
        // Jump to first/last row
        KeyBinding::new("cmd-up", MoveToFirstRow, Some(CONTEXT)),
        KeyBinding::new("cmd-down", MoveToLastRow, Some(CONTEXT)),
        // Jump to first/last column
        KeyBinding::new("home", MoveToFirstColumn, Some(CONTEXT)),
        KeyBinding::new("end", MoveToLastColumn, Some(CONTEXT)),
        // Page up/down
        KeyBinding::new("pageup", TablePageUp, Some(CONTEXT)),
        KeyBinding::new("pagedown", TablePageDown, Some(CONTEXT)),
    ]);
}

struct TableOptions {
    scrollbar_visible: Edges<bool>,
    /// Set stripe style of the table.
    stripe: bool,
    /// Set to use border style of the table.
    bordered: bool,
    /// The cell size of the table.
    size: Size,
    /// The header height. Defaults to the row height for the selected size.
    header_height: Option<Pixels>,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            scrollbar_visible: Edges::all(true),
            stripe: false,
            bordered: true,
            size: Size::default(),
            header_height: None,
        }
    }
}

/// A table element.
#[derive(IntoElement)]
pub struct Table<D: TableDelegate> {
    state: Entity<TableState<D>>,
    options: TableOptions,
}

impl<D> Table<D>
where
    D: TableDelegate,
{
    /// Create a new Table element with the given [`TableState`].
    pub fn new(state: &Entity<TableState<D>>) -> Self {
        Self {
            state: state.clone(),
            options: TableOptions::default(),
        }
    }

    /// Set to use stripe style of the table, default to false.
    pub fn stripe(mut self, stripe: bool) -> Self {
        self.options.stripe = stripe;
        self
    }

    /// Set to use border style of the table, default to true.
    pub fn bordered(mut self, bordered: bool) -> Self {
        self.options.bordered = bordered;
        self
    }

    /// Set scrollbar visibility.
    pub fn scrollbar_visible(mut self, vertical: bool, horizontal: bool) -> Self {
        self.options.scrollbar_visible = Edges {
            right: vertical,
            bottom: horizontal,
            ..Default::default()
        };
        self
    }

    /// Set table header height. Defaults to the row height for the selected size.
    pub fn header_height(mut self, height: impl Into<Pixels>) -> Self {
        self.options.header_height = Some(height.into());
        self
    }
}

impl<D> Sizable for Table<D>
where
    D: TableDelegate,
{
    fn with_size(mut self, size: impl Into<Size>) -> Self {
        self.options.size = size.into();
        self
    }
}

impl<D> RenderOnce for Table<D>
where
    D: TableDelegate,
{
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let bordered = self.options.bordered;
        let focus_handle = self.state.focus_handle(cx);
        self.state.update(cx, |state, _| {
            state.options = self.options;
        });

        div()
            .id("table")
            .size_full()
            .key_context(CONTEXT)
            .track_focus(&focus_handle)
            .on_action(window.listener_for(&self.state, TableState::action_cancel))
            .on_action(window.listener_for(&self.state, TableState::action_select_all))
            .on_action(window.listener_for(&self.state, TableState::action_copy))
            .on_action(window.listener_for(&self.state, TableState::action_paste))
            .on_action(window.listener_for(&self.state, TableState::action_start_editing_cell))
            .on_action(window.listener_for(&self.state, TableState::action_move_selection_up))
            .on_action(window.listener_for(&self.state, TableState::action_move_selection_down))
            .on_action(window.listener_for(&self.state, TableState::action_move_selection_left))
            .on_action(window.listener_for(&self.state, TableState::action_move_selection_right))
            .on_action(window.listener_for(&self.state, TableState::action_extend_selection_up))
            .on_action(window.listener_for(&self.state, TableState::action_extend_selection_down))
            .on_action(window.listener_for(&self.state, TableState::action_extend_selection_left))
            .on_action(window.listener_for(&self.state, TableState::action_extend_selection_right))
            .on_action(window.listener_for(&self.state, TableState::action_move_to_first_row))
            .on_action(window.listener_for(&self.state, TableState::action_move_to_last_row))
            .on_action(window.listener_for(&self.state, TableState::action_move_to_first_column))
            .on_action(window.listener_for(&self.state, TableState::action_move_to_last_column))
            .on_action(window.listener_for(&self.state, TableState::action_table_page_up))
            .on_action(window.listener_for(&self.state, TableState::action_table_page_down))
            .on_key_down(window.listener_for(&self.state, TableState::on_key_down))
            .bg(cx.theme().table)
            .when(bordered, |this| {
                this.border_1()
                    .border_color(cx.theme().border.opacity(0.25))
            })
            .child(self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::px;

    #[test]
    fn table_header_height_defaults_to_row_height() {
        let options = TableOptions::default();

        assert_eq!(options.header_height, None);
        assert_eq!(options.size.table_row_height(), px(32.0));
    }

    #[test]
    fn table_options_can_store_header_height_override() {
        let options = TableOptions {
            header_height: Some(px(44.0)),
            ..Default::default()
        };

        assert_eq!(options.header_height, Some(px(44.0)));
    }
}
