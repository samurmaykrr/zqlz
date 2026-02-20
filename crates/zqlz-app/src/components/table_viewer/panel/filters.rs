use super::*;

impl TableViewerPanel {
    pub fn apply_filters(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        cx: &mut Context<Self>,
    ) {
        let Some(filter_state) = self.filter_panel_state.clone() else {
            return;
        };

        let (filters, sorts) = filter_state.read_with(cx, |state, _cx| {
            (state.get_filter_conditions(), state.get_sort_criteria())
        });

        if self.uses_client_side_filters() {
            self.apply_filters_client_side(&filters, &sorts, cx);
        } else {
            let visible_columns: Vec<String> = self
                .column_visibility_state
                .as_ref()
                .map(|state| state.read(cx).visible_columns())
                .unwrap_or_else(|| self.column_meta.iter().map(|c| c.name.clone()).collect());

            cx.emit(TableViewerEvent::ApplyFilters {
                connection_id,
                table_name,
                filters,
                sorts,
                visible_columns,
                search_text: self.search_text.clone(),
            });

            if let Some(pag_state) = &self.pagination_state {
                pag_state.update(cx, |state, cx| {
                    state.current_page = 1;
                    cx.notify();
                });
            }
        }

        filter_state.update(cx, |state, cx| {
            state.mark_applied(cx);
        });
    }

    fn uses_client_side_filters(&self) -> bool {
        matches!(
            self.driver_category,
            DriverCategory::KeyValue | DriverCategory::Document
        )
    }

    /// Apply filter conditions and sort criteria client-side against in-memory data.
    fn apply_filters_client_side(
        &mut self,
        filters: &[super::super::filter_types::FilterCondition],
        sorts: &[super::super::filter_types::SortCriterion],
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let search_text = self.search_text.clone();

        table_state.update(cx, |table_state, cx| {
            let delegate = table_state.delegate_mut();

            // Sort first (operates on all rows), then filter
            delegate.apply_advanced_sorts(sorts);
            delegate.apply_advanced_filters(filters, &search_text);

            cx.notify();
        });

        // Update the row count shown in the footer
        if let Some(table_state) = &self.table_state {
            let delegate = table_state.read(cx).delegate();
            self.row_count = delegate.get_search_match_count();
        }

        cx.notify();
    }

    pub fn hide_column(&mut self, column_name: &str, cx: &mut Context<Self>) {
        if let Some(col_vis_state) = &self.column_visibility_state {
            col_vis_state.update(cx, |state, cx| {
                state.set_column_visibility(column_name, false, cx);
            });
        }
        cx.notify();
    }

    pub fn add_quick_filter(
        &mut self,
        column_name: String,
        value: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.filter_expanded = true;

        if let Some(filter_state) = &self.filter_panel_state {
            filter_state.update(cx, |state, cx| {
                state.add_quick_filter(column_name, value, window, cx);
            });
        }

        cx.notify();
    }

    pub fn apply_sort(
        &mut self,
        column_name: String,
        direction: super::super::filter_types::SortDirection,
        cx: &mut Context<Self>,
    ) {
        

        if let Some(filter_state) = &self.filter_panel_state {
            filter_state.update(cx, |state, cx| {
                state.clear_sorts(cx);
                state.add_sort(column_name.clone(), cx);
                if direction
                    == crate::components::table_viewer::filter_types::SortDirection::Descending
                {
                    if let Some(sort) = state.sorts.last_mut() {
                        sort.direction = direction;
                    }
                }
            });
        }

        if let (Some(connection_id), Some(table_name)) =
            (self.connection_id, self.table_name.clone())
        {
            self.apply_filters(connection_id, table_name, cx);
        }
    }

    pub fn toggle_filter_panel(&mut self, cx: &mut Context<Self>) {
        self.filter_expanded = !self.filter_expanded;
        cx.notify();
    }

    pub fn toggle_column_visibility(&mut self, cx: &mut Context<Self>) {
        self.column_visibility_shown = !self.column_visibility_shown;
        cx.notify();
    }
}
