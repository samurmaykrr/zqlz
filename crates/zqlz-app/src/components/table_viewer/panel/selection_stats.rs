use std::collections::HashSet;

use super::*;

impl TableViewerPanel {
    pub(super) fn update_selection_stats_from_table(&mut self, cx: &App) {
        let Some(table_state) = &self.table_state else {
            self.selection_stats = None;
            return;
        };

        self.selection_stats = table_state.read_with(cx, |table, cx| {
            let selection = table.cell_selection();
            let cell_count = selection.cell_count();
            if cell_count == 0 {
                return None;
            }

            let selected_cells = selection.selected_cells();
            if selected_cells.is_empty() {
                return None;
            }

            let distinct_rows: HashSet<usize> =
                selected_cells.iter().map(|cell| cell.row).collect();
            let selection_label = if cell_count == 1 {
                "1 cell".to_string()
            } else if distinct_rows.len() == 1 {
                format!("{} cells in 1 row", cell_count)
            } else {
                format!("{} cells in {} rows", cell_count, distinct_rows.len())
            };

            let delegate = table.delegate();
            let mut numeric_values: Vec<f64> = Vec::new();
            for cell in &selected_cells {
                let text = delegate.cell_text(cell.row, cell.col, cx);
                if let Ok(number) = text.parse::<f64>() {
                    numeric_values.push(number);
                }
            }

            let numeric_stats = if numeric_values.len() > 1 {
                let sum: f64 = numeric_values.iter().sum();
                let average = sum / numeric_values.len() as f64;
                let min = numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = numeric_values
                    .iter()
                    .cloned()
                    .fold(f64::NEG_INFINITY, f64::max);

                let format_number = |value: f64| -> String {
                    if value == value.floor() && value.abs() < 1e15 {
                        format!("{}", value as i64)
                    } else {
                        format!("{:.2}", value)
                    }
                };

                Some((
                    format_number(sum),
                    format_number(average),
                    format_number(min),
                    format_number(max),
                ))
            } else {
                None
            };

            Some(SelectionStatsSummary {
                selection_label,
                numeric_stats,
            })
        });
    }
}
