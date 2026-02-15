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
            ColumnSort::Default => return,
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

        _ = viewer_panel.update(cx, |_panel, cx| {
            cx.emit(TableViewerEvent::SortColumn {
                connection_id,
                table_name,
                column_name,
                direction,
            });
        });

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

        self.rows.sort_by(|a, b| {
            let val_a = a.get(data_col_ix).map(|s| s.as_str()).unwrap_or("");
            let val_b = b.get(data_col_ix).map(|s| s.as_str()).unwrap_or("");

            // Try numeric comparison first (handles sizes like "1.2 KB", integers, etc.)
            let ordering = match (try_parse_numeric(val_a), try_parse_numeric(val_b)) {
                (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                _ => val_a.to_lowercase().cmp(&val_b.to_lowercase()),
            };

            match direction {
                SortDirection::Ascending => ordering,
                SortDirection::Descending => ordering.reverse(),
            }
        });

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

        self.rows.sort_by(|a, b| {
            for (col_ix, direction) in &resolved_sorts {
                let val_a = a.get(*col_ix).map(|s| s.as_str()).unwrap_or("");
                let val_b = b.get(*col_ix).map(|s| s.as_str()).unwrap_or("");

                let ordering = match (try_parse_numeric(val_a), try_parse_numeric(val_b)) {
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

        self.recompute_filtered_indices();
    }

    /// Re-sync `filtered_row_indices` after a sort reorders `self.rows`.
    fn recompute_filtered_indices(&mut self) {
        if self.is_filtering {
            if let Some(ref search_text) = self.search_filter {
                let search_text = search_text.clone();
                self.filtered_row_indices = self
                    .rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| {
                        row.iter()
                            .any(|cell| cell.to_lowercase().contains(&search_text))
                    })
                    .map(|(idx, _)| idx)
                    .collect();
            }
        }
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
    if parts.len() == 2 {
        if let Ok(n) = parts[0].parse::<f64>() {
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
    }

    None
}
