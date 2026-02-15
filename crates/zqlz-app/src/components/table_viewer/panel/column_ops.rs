use super::*;

impl TableViewerPanel {
    pub fn freeze_column(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().freeze_column(col_ix);
                table.refresh(cx);
            });
        }
        cx.notify();
    }

    pub fn unfreeze_column(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().unfreeze_column(col_ix);
                table.refresh(cx);
            });
        }
        cx.notify();
    }

    pub fn size_column_to_fit(&mut self, col_ix: usize, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                let optimal_width = table.delegate().calculate_column_width(col_ix);
                let columns = table.delegate_mut().columns_mut();
                if col_ix < columns.len() {
                    columns[col_ix] = columns[col_ix].clone().width(optimal_width);
                }
                table.refresh(cx);
            });
        }
        cx.notify();
    }

    pub fn size_all_columns_to_fit(&mut self, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                let col_count = table.delegate().columns().len();
                let widths: Vec<f32> = (0..col_count)
                    .map(|col_ix| table.delegate().calculate_column_width(col_ix))
                    .collect();

                let columns = table.delegate_mut().columns_mut();
                for (col_ix, width) in widths.into_iter().enumerate() {
                    if col_ix < columns.len() {
                        columns[col_ix] = columns[col_ix].clone().width(width);
                    }
                }

                table.refresh(cx);
            });
        }
        cx.notify();
    }
}
