use super::*;

impl TableViewerDelegate {
    /// Whether this delegate should sort client-side (data already in memory)
    fn uses_client_side_sort(&self) -> bool {
        matches!(
            self.driver_category,
            zqlz_core::DriverCategory::KeyValue | zqlz_core::DriverCategory::Document
        )
    }

    pub fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) {
        use super::super::filter_types::SortDirection;

        if col_ix >= self.columns.len() {
            return;
        }

        // Row number column is not sortable
        if col_ix == 0 {
            return;
        }

        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx == col_ix {
                *col = col.clone().sort(sort);
            } else {
                *col = col.clone().sort(ColumnSort::Default);
            }
        }

        if sort == ColumnSort::Default {
            if self.uses_client_side_sort() {
                self.restore_original_row_order();
                tracing::info!("Client-side sort cleared and original order restored");
            } else {
                let viewer_panel = self.viewer_panel.clone();
                if let Err(error) = viewer_panel.update(cx, |panel, cx| {
                    panel.clear_sort_and_apply(cx);
                }) {
                    tracing::error!("Failed to clear server-side sort: {:?}", error);
                }
            }
            cx.notify();
            return;
        }

        let data_col_ix = col_ix - 1;
        let column_name = self
            .column_meta
            .get(data_col_ix)
            .map(|m| m.name.clone())
            .unwrap_or_default();
        if column_name.is_empty() {
            return;
        }

        let direction = match sort {
            ColumnSort::Ascending => SortDirection::Ascending,
            ColumnSort::Descending => SortDirection::Descending,
            ColumnSort::Default => {
                cx.notify();
                return;
            }
        };

        if self.uses_client_side_sort() {
            self.sort_rows_client_side(data_col_ix, direction);
            tracing::info!("Client-side sort applied on column '{}'", column_name);
            cx.notify();
            return;
        }

        let viewer_panel = self.viewer_panel.clone();
        let table_name = self.table_name.clone();
        let connection_id = self.connection_id;

        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
            cx.emit(TableViewerEvent::SortColumn {
                connection_id,
                table_name,
                column_name,
                direction,
            });
        }) {
            tracing::error!("Failed to emit SortColumn event: {:?}", e);
        }

        tracing::info!("Server-side sort requested");
        cx.notify();
    }

    /// Sort the in-memory rows by a specific data column.
    /// For KeyValue/Document drivers where all data is loaded.
    fn sort_rows_client_side(
        &mut self,
        data_col_ix: usize,
        direction: super::super::filter_types::SortDirection,
    ) {
        use super::super::filter_types::SortDirection;

        self.ensure_row_order_tracking();
        let mut rows_with_order = self.take_rows_with_order();

        rows_with_order.sort_by(|(_, a), (_, b)| {
            let val_a = a
                .get(data_col_ix)
                .map(|v| v.display_for_table())
                .unwrap_or_default();
            let val_b = b
                .get(data_col_ix)
                .map(|v| v.display_for_table())
                .unwrap_or_default();

            // Try numeric comparison first (handles sizes like "1.2 KB", integers, etc.)
            let ordering = match (try_parse_numeric(&val_a), try_parse_numeric(&val_b)) {
                (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                _ => val_a.to_lowercase().cmp(&val_b.to_lowercase()),
            };

            match direction {
                SortDirection::Ascending => ordering,
                SortDirection::Descending => ordering.reverse(),
            }
        });

        self.restore_rows_with_order(rows_with_order);

        self.clear_cell_preview_cache();
        self.recompute_filtered_indices();
    }

    /// Sort the in-memory rows by multiple criteria from the advanced filter panel.
    pub fn apply_advanced_sorts(&mut self, sorts: &[super::super::filter_types::SortCriterion]) {
        use super::super::filter_types::SortDirection;

        if sorts.is_empty() {
            return;
        }

        // Resolve column names to data indices
        let resolved_sorts: Vec<(usize, SortDirection)> = sorts
            .iter()
            .filter_map(|sort| {
                let col_ix = self
                    .column_meta
                    .iter()
                    .position(|m| m.name == sort.column)?;
                Some((col_ix, sort.direction))
            })
            .collect();

        if resolved_sorts.is_empty() {
            return;
        }

        self.ensure_row_order_tracking();
        let mut rows_with_order = self.take_rows_with_order();

        rows_with_order.sort_by(|(_, a), (_, b)| {
            for (col_ix, direction) in &resolved_sorts {
                let val_a = a
                    .get(*col_ix)
                    .map(|v| v.display_for_table())
                    .unwrap_or_default();
                let val_b = b
                    .get(*col_ix)
                    .map(|v| v.display_for_table())
                    .unwrap_or_default();

                let ordering = match (try_parse_numeric(&val_a), try_parse_numeric(&val_b)) {
                    (Some(na), Some(nb)) => {
                        na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => val_a.to_lowercase().cmp(&val_b.to_lowercase()),
                };

                let ordering = match direction {
                    SortDirection::Ascending => ordering,
                    SortDirection::Descending => ordering.reverse(),
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        self.restore_rows_with_order(rows_with_order);

        self.clear_cell_preview_cache();
        self.recompute_filtered_indices();
    }

    /// Re-sync `filtered_row_indices` after a sort reorders `self.rows`.
    /// Re-applies the last advanced filter + search text so the visible set
    /// reflects both the new row order and any active filter criteria.
    fn recompute_filtered_indices(&mut self) {
        if !self.is_filtering {
            return;
        }
        let conditions = self.last_filter_conditions.clone();
        let search_text = self.last_filter_search_text.clone();
        self.apply_advanced_filters(&conditions, &search_text);
    }

    fn restore_original_row_order(&mut self) {
        self.ensure_row_order_tracking();
        let mut rows_with_order = self.take_rows_with_order();
        rows_with_order.sort_by_key(|(original_order, _)| *original_order);
        self.restore_rows_with_order(rows_with_order);
        self.clear_cell_preview_cache();
        self.recompute_filtered_indices();
    }

    fn ensure_row_order_tracking(&mut self) {
        if self.row_original_order.len() == self.rows.len() {
            return;
        }

        tracing::warn!(
            "Row order tracking out of sync (rows={}, order_tokens={}); rebuilding in current order",
            self.rows.len(),
            self.row_original_order.len()
        );

        let row_count = self.rows.len() as u64;
        self.row_original_order = (0..row_count).collect();
        self.next_row_order_token = row_count;
    }

    fn take_rows_with_order(&mut self) -> Vec<(u64, Vec<Value>)> {
        let rows = std::mem::take(&mut self.rows);
        let order = std::mem::take(&mut self.row_original_order);
        order.into_iter().zip(rows).collect()
    }

    fn restore_rows_with_order(&mut self, rows_with_order: Vec<(u64, Vec<Value>)>) {
        let (row_original_order, rows): (Vec<u64>, Vec<Vec<Value>>) =
            rows_with_order.into_iter().unzip();
        self.row_original_order = row_original_order;
        self.rows = rows;
    }
}

/// Attempt to parse a string as a number for numeric sorting.
/// Handles plain numbers and common formatted values.
fn try_parse_numeric(s: &str) -> Option<f64> {
    // Try direct parse first
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }

    // Strip common suffixes (e.g., "1.2 KB", "3 MB") and convert to bytes
    let s_trimmed = s.trim();
    let parts: Vec<&str> = s_trimmed.splitn(2, ' ').collect();
    if parts.len() == 2
        && let Ok(n) = parts[0].parse::<f64>()
    {
        let multiplier = match parts[1].to_uppercase().as_str() {
            "B" => Some(1.0),
            "KB" => Some(1024.0),
            "MB" => Some(1024.0 * 1024.0),
            "GB" => Some(1024.0 * 1024.0 * 1024.0),
            "TB" => Some(1024.0 * 1024.0 * 1024.0 * 1024.0),
            _ => None,
        };
        if let Some(m) = multiplier {
            return Some(n * m);
        }
    }

    None
}
