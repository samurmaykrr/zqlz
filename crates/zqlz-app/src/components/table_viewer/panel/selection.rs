use super::*;

impl TableViewerPanel {
    pub fn toggle_row_selection(&mut self, row_index: usize, cx: &mut Context<Self>) {
        if self.selected_rows.contains(&row_index) {
            self.selected_rows.remove(&row_index);
        } else {
            self.selected_rows.insert(row_index);
        }
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn select_row(&mut self, row_index: usize, cx: &mut Context<Self>) {
        self.selected_rows.clear();
        self.selected_rows.insert(row_index);
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        self.selected_rows.clear();
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn is_row_selected(&self, row_index: usize) -> bool {
        self.selected_rows.contains(&row_index)
    }
}
