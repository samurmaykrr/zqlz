use super::*;

impl TableViewerPanel {
    pub(crate) fn selected_display_rows(&self, cx: &App) -> Vec<usize> {
        let Some(table_state) = &self.table_state else {
            return Vec::new();
        };

        table_state.read_with(cx, |table, _cx| {
            let mut selected_rows: Vec<usize> = if !table.cell_selection().is_empty() {
                table
                    .cell_selection()
                    .selected_cells()
                    .into_iter()
                    .map(|cell| cell.row)
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect()
            } else {
                table.selected_row().into_iter().collect()
            };

            selected_rows.sort_unstable();
            selected_rows
        })
    }

    pub(crate) fn selected_display_cell_anchor(&self, cx: &App) -> Option<(usize, usize)> {
        let table_state = self.table_state.as_ref()?;

        table_state.read_with(cx, |table, _cx| {
            table
                .cell_selection()
                .anchor()
                .map(|anchor| (anchor.row, anchor.col))
                .or_else(|| table.selected_cell())
                .or_else(|| table.selected_row().map(|row| (row, 0)))
        })
    }

    pub(crate) fn has_any_selection(&self, cx: &App) -> bool {
        let Some(table_state) = &self.table_state else {
            return false;
        };

        table_state.read_with(cx, |table, _cx| {
            !table.cell_selection().is_empty() || table.selected_row().is_some()
        })
    }
}
