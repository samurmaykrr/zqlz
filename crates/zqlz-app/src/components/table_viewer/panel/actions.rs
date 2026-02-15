use super::*;
use zqlz_ui::widgets::{
    button::ButtonVariant, dialog::DialogButtonProps, ActiveTheme as _, WindowExt as _,
};

impl TableViewerPanel {
    pub fn emit_add_row(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.connection_id else {
            return;
        };

        if matches!(self.driver_category, DriverCategory::KeyValue) {
            cx.emit(TableViewerEvent::AddRedisKey { connection_id });
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };
        let Some(table_name) = &self.table_name else {
            return;
        };

        let all_column_names: Vec<String> = table_state.read_with(cx, |table, _cx| {
            table
                .delegate()
                .column_meta
                .iter()
                .map(|c| c.name.clone())
                .collect()
        });

        cx.emit(TableViewerEvent::AddRow {
            connection_id,
            table_name: table_name.clone(),
            all_column_names,
        });
    }

    /// Emit AddRowForm event to open the form-based row editor for inserting a new row
    pub fn emit_add_row_form(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.connection_id else {
            return;
        };

        if matches!(self.driver_category, DriverCategory::KeyValue) {
            cx.emit(TableViewerEvent::AddRedisKey { connection_id });
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };
        let Some(table_name) = &self.table_name else {
            return;
        };

        let column_meta: Vec<ColumnMeta> =
            table_state.read_with(cx, |table, _cx| table.delegate().column_meta.clone());

        cx.emit(TableViewerEvent::AddRowForm {
            connection_id,
            table_name: table_name.clone(),
            column_meta,
        });
    }

    /// Show confirmation dialog before deleting selected rows
    pub(crate) fn show_delete_confirmation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let selected_count = self.selected_rows.len();
        if selected_count == 0 {
            return;
        }

        let row_label = if selected_count == 1 { "row" } else { "rows" };
        let panel = cx.entity().downgrade();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let panel = panel.clone();

            dialog
                .title("Delete Selected Rows")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete {} {}?",
                            selected_count, row_label
                        )))
                        .child(
                            div()
                                .text_sm()
                                .text_color(_cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _, cx| {
                    _ = panel.update(cx, |panel, cx| {
                        panel.emit_delete_rows(cx);
                    });
                    true
                })
                .confirm()
        });
    }

    pub(crate) fn emit_delete_rows(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        if self.selected_rows.is_empty() {
            return;
        }

        let selected_rows: Vec<usize> = self.selected_rows.iter().copied().collect();

        if self.auto_commit_mode {
            let Some(connection_id) = self.connection_id else {
                return;
            };
            let Some(table_name) = &self.table_name else {
                return;
            };

            let (all_column_names, rows_to_delete) = table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let all_column_names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                let rows_to_delete: Vec<Vec<String>> = selected_rows
                    .iter()
                    .map(|&display_idx| delegate.get_actual_row_index(display_idx))
                    .filter_map(|actual_idx| delegate.rows.get(actual_idx).cloned())
                    .collect();
                (all_column_names, rows_to_delete)
            });

            self.selected_rows.clear();
            cx.notify();

            cx.emit(TableViewerEvent::DeleteRows {
                connection_id,
                table_name: table_name.clone(),
                all_column_names,
                rows_to_delete,
            });
        } else {
            table_state.update(cx, |table, cx| {
                for display_idx in selected_rows {
                    let actual_idx = table.delegate().get_actual_row_index(display_idx);
                    table.delegate_mut().mark_row_for_deletion(actual_idx);
                }
                cx.notify();
            });

            self.selected_rows.clear();
            cx.notify();
        }
    }

    pub fn emit_commit_changes(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };
        let Some(connection_id) = self.connection_id else {
            return;
        };
        let Some(table_name) = &self.table_name else {
            return;
        };

        let (modified_cells, deleted_rows, new_rows, column_meta, _, _, all_rows) = table_state
            .read_with(cx, |table, _cx| {
                table.delegate().get_pending_changes_for_commit()
            });

        if modified_cells.is_empty() && deleted_rows.is_empty() && new_rows.is_empty() {
            return;
        }

        cx.emit(TableViewerEvent::CommitChanges {
            connection_id,
            table_name: table_name.clone(),
            modified_cells,
            deleted_rows,
            new_rows,
            column_meta,
            all_rows,
        });
    }

    pub fn discard_pending_changes(&mut self, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().discard_pending_changes();
                cx.notify();
            });
        }
        cx.notify();
    }

    pub fn emit_generate_sql(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };
        let Some(connection_id) = self.connection_id else {
            return;
        };
        let Some(table_name) = &self.table_name else {
            return;
        };

        let (modified_cells, deleted_rows, new_rows, column_meta, _, _, all_rows) = table_state
            .read_with(cx, |table, _cx| {
                table.delegate().get_pending_changes_for_commit()
            });

        if modified_cells.is_empty() && deleted_rows.is_empty() && new_rows.is_empty() {
            return;
        }

        cx.emit(TableViewerEvent::GenerateChangesSql {
            connection_id,
            table_name: table_name.clone(),
            modified_cells,
            deleted_rows,
            new_rows,
            column_meta,
            all_rows,
        });
    }

    /// Open the row editor for the currently selected cell
    pub fn emit_open_row_editor(&mut self, cx: &mut Context<Self>) {
        tracing::debug!("emit_open_row_editor called");

        let Some(connection_id) = self.connection_id else {
            tracing::debug!("No connection_id available");
            return;
        };
        let Some(table_name) = &self.table_name else {
            tracing::debug!("No table_name available");
            return;
        };
        let Some(table_state) = &self.table_state else {
            tracing::debug!("No table_state available");
            return;
        };

        // Get the currently selected cell
        let selected_cell = table_state.read_with(cx, |table, _cx| table.selected_cell());

        let Some((row_index, _col_index)) = selected_cell else {
            tracing::debug!("No selected cell to open in row editor");
            return;
        };

        let (row_values, column_meta, all_column_names) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let actual_idx = delegate.get_actual_row_index(row_index);

                let row_values = delegate.rows.get(actual_idx).cloned().unwrap_or_default();

                let column_meta = delegate.column_meta.clone();
                let all_column_names: Vec<String> =
                    column_meta.iter().map(|c| c.name.clone()).collect();

                (row_values, column_meta, all_column_names)
            });

        tracing::info!(
            "Opening row editor for row {} in table {}",
            row_index,
            table_name
        );

        cx.emit(TableViewerEvent::EditRow {
            connection_id,
            table_name: table_name.clone(),
            row_index,
            row_values,
            column_meta,
            all_column_names,
        });
    }
}
