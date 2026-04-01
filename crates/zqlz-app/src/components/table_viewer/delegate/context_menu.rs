use super::context_menu_utils::{
    ordered_unique_actual_rows_from_display_rows, pasted_text_for_selection_index,
};
use super::*;

impl TableViewerDelegate {
    pub fn context_menu(
        &mut self,
        row_ix: usize,
        col_ix_opt: Option<usize>,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> PopupMenu {
        use zqlz_ui::widgets::menu::PopupMenuItem;

        let actual_row_ix = self.get_actual_row_index(row_ix);

        if actual_row_ix >= self.rows.len() {
            return menu;
        }

        let col_ix = col_ix_opt.unwrap_or(1);
        if col_ix == 0 {
            return menu;
        }

        let menu_entity = cx.entity();
        let data_col_ix = col_ix - 1;
        let column_meta = self.column_meta.get(data_col_ix).cloned();
        let current_value = self
            .rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .cloned();

        let viewer_panel = self.viewer_panel.clone();
        let table_name = self.table_name.clone();
        let connection_id = self.connection_id;
        let all_row_values = self.rows.get(actual_row_ix).cloned().unwrap_or_default();
        let all_column_names: Vec<String> =
            self.column_meta.iter().map(|c| c.name.clone()).collect();
        let all_column_types: Vec<String> = self
            .column_meta
            .iter()
            .map(|c| c.data_type.clone())
            .collect();
        let can_set_null = column_meta
            .as_ref()
            .map(|column| column.nullable)
            .unwrap_or(false);
        let can_set_empty_string = self.is_string_column(data_col_ix);
        let can_cut = column_meta
            .as_ref()
            .map(|column| column.nullable || self.is_string_column(data_col_ix))
            .unwrap_or(false);
        let can_generate_uuid = self.can_generate_uuid_for_column(data_col_ix);
        let supports_relational_sql_actions =
            matches!(self.driver_category, DriverCategory::Relational);

        let mut selected_display_rows = self.context_menu_selected_rows.clone();
        if !selected_display_rows.contains(&row_ix) {
            selected_display_rows.push(row_ix);
        }
        selected_display_rows.sort_unstable();
        selected_display_rows.dedup();

        let selected_actual_rows = ordered_unique_actual_rows_from_display_rows(
            &selected_display_rows,
            |display_row| self.get_actual_row_index(display_row),
            self.rows.len(),
        );

        let selected_rows_with_values: Vec<(usize, Vec<Value>)> = selected_actual_rows
            .iter()
            .filter_map(|&actual_row| {
                self.rows
                    .get(actual_row)
                    .cloned()
                    .map(|row_values| (actual_row, row_values))
            })
            .collect();
        let selected_count = selected_rows_with_values.len();

        let selected_cell_texts: Vec<String> = selected_rows_with_values
            .iter()
            .map(|(_, row_values)| {
                row_values
                    .get(data_col_ix)
                    .map(|value| value.display_for_table())
                    .unwrap_or_default()
            })
            .collect();

        menu.item({
            let column_meta = column_meta.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if selected_count > 1 {
                format!("Set Selected Cells to Empty String ({})", selected_count)
            } else {
                "Set to Empty String".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(
                    column_meta.is_none() || !can_set_empty_string || selected_rows_with_values.is_empty(),
                )
                .on_click(window.listener_for(&menu_entity, move |table, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        let updates: Vec<(usize, Value)> = selected_rows_with_values
                            .iter()
                            .map(|(actual_row, _)| {
                                (*actual_row, Value::parse_from_string("", &col_meta.data_type))
                            })
                            .collect();

                        table
                            .delegate_mut()
                            .apply_context_menu_cell_mutations(data_col_ix, updates, cx);
                        cx.notify();
                    }
                }))
        })
        .item({
            let column_meta = column_meta.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if selected_count > 1 {
                format!("Set Selected Cells to NULL ({})", selected_count)
            } else {
                "Set to NULL".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(column_meta.is_none() || !can_set_null || selected_rows_with_values.is_empty())
                .on_click(window.listener_for(&menu_entity, move |table, _, _, cx| {
                    if column_meta.is_some() {
                        let updates: Vec<(usize, Value)> = selected_rows_with_values
                            .iter()
                            .map(|(actual_row, _)| (*actual_row, Value::Null))
                            .collect();

                        table
                            .delegate_mut()
                            .apply_context_menu_cell_mutations(data_col_ix, updates, cx);
                        cx.notify();
                    }
                }))
        })
        .separator()
        .item({
            let column_meta = column_meta.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if selected_count > 1 {
                format!("Generate UUID for Selected Cells ({})", selected_count)
            } else {
                "Generate UUID".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(
                    column_meta.is_none() || !can_generate_uuid || selected_rows_with_values.is_empty(),
                )
                .on_click(window.listener_for(&menu_entity, move |table, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        let updates: Vec<(usize, Value)> = selected_rows_with_values
                            .iter()
                            .map(|(actual_row, _)| {
                                let generated_uuid = Uuid::new_v4().to_string();
                                (
                                    *actual_row,
                                    Value::parse_from_string(&generated_uuid, &col_meta.data_type),
                                )
                            })
                            .collect();

                        table
                            .delegate_mut()
                            .apply_context_menu_cell_mutations(data_col_ix, updates, cx);
                        cx.notify();
                    }
                }))
        })
        .item(
            {
                let label = if selected_count > 1 {
                    "Edit in Cell Editor (Single Row Only)".to_string()
                } else {
                    "Edit in Cell Editor".to_string()
                };

                PopupMenuItem::new(label)
                    .disabled(column_meta.is_none() || selected_count > 1)
                    .on_click({
                    let column_meta = column_meta.clone();
                    let current_value = current_value.clone();
                    let viewer_panel = viewer_panel.clone();
                    let table_name = table_name.clone();
                    let all_row_values = all_row_values.clone();
                    let all_column_names = all_column_names.clone();
                    let all_column_types = all_column_types.clone();
                    let raw_bytes = current_value.as_ref().and_then(|v| {
                        if let Value::Bytes(b) = v { Some(b.clone()) } else { None }
                    });
                    let current_value = current_value.clone().unwrap_or(Value::Null);
                    window.listener_for(&menu_entity, move |_this, _, _, cx| {
                        if let Some(col_meta) = &column_meta
                            && let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                                cx.emit(TableViewerEvent::EditCell {
                                    table_name: table_name.clone(),
                                    connection_id,
                                    row: actual_row_ix,
                                    col: data_col_ix,
                                    column_meta: col_meta.clone(),
                                    column_name: col_meta.name.clone(),
                                    column_type: col_meta.data_type.clone(),
                                    current_value: current_value.clone(),
                                    all_row_values: all_row_values.clone(),
                                    all_column_names: all_column_names.clone(),
                                    all_column_types: all_column_types.clone(),
                                    raw_bytes: raw_bytes.clone(),
                                });
                            })
                        {
                            tracing::error!("Failed to emit EditCell event: {:?}", e);
                        }
                    })
                })
            },
        )
        .item({
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let column_meta_vec = self.column_meta.clone();
            let label = if selected_count > 1 {
                "Edit Row in Form (Single Row Only)".to_string()
            } else {
                "Edit Row in Form".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(selected_count > 1)
                .on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::EditRow {
                                connection_id,
                                table_name: table_name.clone(),
                                row_index: actual_row_ix,
                                row_values: all_row_values.clone(),
                                column_meta: column_meta_vec.clone(),
                                all_column_names: all_column_names.clone(),
                            });
                        }) {
                            tracing::error!("Failed to emit EditRow event: {:?}", e);
                        }
                    },
                ))
        })
        .separator()
        .item({
            let column_meta = column_meta.clone();
            let current_value = current_value.clone();
            let viewer_panel = viewer_panel.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if selected_count > 1 {
                format!("Filter by Selected Values ({})", selected_count)
            } else {
                "Filter".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_table, _, _, cx| {
                        if let Some(col_meta) = &column_meta {
                            let mut unique_selected_values = std::collections::HashSet::new();
                            let mut selected_values = Vec::new();

                            for (_, row_values) in &selected_rows_with_values {
                                if let Some(value) = row_values.get(data_col_ix)
                                    && !value.is_null()
                                {
                                    let display_value = value.display_for_table();
                                    if unique_selected_values.insert(display_value.clone()) {
                                        selected_values.push(display_value);
                                    }
                                }
                            }

                            let (operator, value) = if selected_values.len() > 1 {
                                (
                                    crate::components::table_viewer::filter_types::FilterOperator::IsInList,
                                    selected_values.join(", "),
                                )
                            } else {
                                let is_null = current_value
                                    .as_ref()
                                    .map(|v| v.is_null())
                                    .unwrap_or(true);
                                if is_null {
                                    (
                                        crate::components::table_viewer::filter_types::FilterOperator::IsNull,
                                        String::new(),
                                    )
                                } else {
                                    (
                                        crate::components::table_viewer::filter_types::FilterOperator::Equal,
                                        current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default(),
                                    )
                                }
                            };
                            if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                                cx.emit(TableViewerEvent::AddQuickFilter {
                                    column_name: col_meta.name.clone(),
                                    operator,
                                    value,
                                });
                            }) {
                                tracing::error!("Failed to emit AddQuickFilter event: {:?}", e);
                            }
                        }
                }))
        })
        .separator()
        .item({
            let column_meta = column_meta.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let selected_cell_texts = selected_cell_texts.clone();
            let label = if selected_count > 1 {
                format!("Cut Selected Cells ({})", selected_count)
            } else {
                "Cut Cell".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(column_meta.is_none() || !can_cut || selected_rows_with_values.is_empty())
                .on_click(window.listener_for(&menu_entity, move |table, _, _, cx| {
                    let clipboard_text = if selected_cell_texts.len() > 1 {
                        selected_cell_texts.join("\n")
                    } else {
                        selected_cell_texts.first().cloned().unwrap_or_default()
                    };
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(clipboard_text));

                    if let Some(col_meta) = &column_meta {
                        let cut_value = if col_meta.nullable {
                            Value::Null
                        } else {
                            Value::parse_from_string("", &col_meta.data_type)
                        };

                        let updates: Vec<(usize, Value)> = selected_rows_with_values
                            .iter()
                            .map(|(actual_row, _)| (*actual_row, cut_value.clone()))
                            .collect();

                        table
                            .delegate_mut()
                            .apply_context_menu_cell_mutations(data_col_ix, updates, cx);
                        cx.notify();
                    }
                }))
        })
        .item({
            let selected_cell_texts = selected_cell_texts.clone();
            let label = if selected_count > 1 {
                format!("Copy Selected Cells ({})", selected_count)
            } else {
                "Copy Cell".to_string()
            };

            PopupMenuItem::new(label).on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    let text = if selected_cell_texts.len() > 1 {
                        selected_cell_texts.join("\n")
                    } else {
                        selected_cell_texts.first().cloned().unwrap_or_default()
                    };
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
                },
            ))
        })
        .item({
            let column_meta = column_meta.clone();
            PopupMenuItem::new("Copy Field Name")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_table, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                            col_meta.name.clone(),
                        ));
                    }
                }))
        })
        .item({
            let table_name = table_name.clone();
            let all_column_names = all_column_names.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if !supports_relational_sql_actions && selected_count > 1 {
                "Copy Selected Rows as SQL Statements (Relational Only)".to_string()
            } else if !supports_relational_sql_actions {
                "Copy Row as SQL Statement (Relational Only)".to_string()
            } else if selected_count > 1 {
                format!("Copy Selected Rows as SQL Statements ({})", selected_count)
            } else {
                "Copy Row as SQL Statement".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(selected_rows_with_values.is_empty() || !supports_relational_sql_actions)
                .on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    let column_list = all_column_names
                        .iter()
                        .map(|n| format!("\"{}\"", n))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let sql = selected_rows_with_values
                        .iter()
                        .map(|(_, row_values)| {
                            let values: Vec<String> = row_values
                                .iter()
                                .map(|value| match CellValue::from_value(value) {
                                    CellValue::Null => "NULL".to_string(),
                                    CellValue::Value(value) => format!(
                                        "'{}'",
                                        value.display_for_editor().replace('\'', "''")
                                    ),
                                })
                                .collect();

                            format!(
                                "INSERT INTO \"{}\" ({}) VALUES ({});",
                                table_name,
                                column_list,
                                values.join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");

                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql));
                },
            ))
        })
        .item({
            let column_meta = column_meta.clone();
            let selected_rows_with_values = selected_rows_with_values.clone();
            let label = if selected_count > 1 {
                format!("Paste into Selected Cells ({})", selected_count)
            } else {
                "Paste into Cell".to_string()
            };

            PopupMenuItem::new(label)
                .disabled(column_meta.is_none() || selected_rows_with_values.is_empty())
                .on_click(window.listener_for(&menu_entity, move |table, _, _, cx| {
                    if let Some(clipboard_item) = cx.read_from_clipboard()
                        && let Some(text) = clipboard_item.text()
                        && let Some(col_meta) = &column_meta
                    {
                        let clipboard_lines: Vec<&str> = text.lines().collect();
                        let mut updates = Vec::new();

                        for (index, (actual_row, _)) in selected_rows_with_values.iter().enumerate() {
                            let Some(pasted_text) =
                                pasted_text_for_selection_index(&clipboard_lines, &text, index)
                            else {
                                break;
                            };

                            updates.push((
                                *actual_row,
                                Value::parse_from_string(&pasted_text, &col_meta.data_type),
                            ));
                        }

                        table
                            .delegate_mut()
                            .apply_context_menu_cell_mutations(data_col_ix, updates, cx);
                        cx.notify();
                    }
                }))
        })
        .separator()
        .item({
            let viewer_panel = viewer_panel.clone();

            let rows_to_delete = selected_display_rows.clone();
            let delete_count = rows_to_delete.len();

            let label = if delete_count > 1 {
                format!("Delete Selected Rows ({})", delete_count)
            } else {
                "Delete Row".to_string()
            };

            PopupMenuItem::new(label).on_click(window.listener_for(
                &menu_entity,
                move |_this, _, window, cx| {
                    let rows = rows_to_delete.clone();
                    TableViewerPanel::show_delete_confirmation_for_rows(
                        viewer_panel.clone(),
                        rows,
                        window,
                        cx,
                    );
                },
            ))
        })
    }

    fn apply_context_menu_cell_mutations(
        &mut self,
        data_col: usize,
        updates: Vec<(usize, Value)>,
        cx: &mut Context<TableState<Self>>,
    ) {
        if data_col >= self.column_meta.len() {
            return;
        }

        let mut undo_edits = Vec::new();

        for (row, new_value) in updates {
            if row >= self.rows.len() {
                continue;
            }

            let original_value = self
                .rows
                .get(row)
                .and_then(|row_data| row_data.get(data_col))
                .cloned()
                .unwrap_or_default();

            if new_value == original_value {
                continue;
            }

            undo_edits.push(UndoCellEdit {
                row,
                data_col,
                old_value: original_value.clone(),
                new_value: new_value.clone(),
            });

            self.apply_cell_value_change(row, data_col, new_value, original_value, false, cx);
        }

        self.push_undo(UndoEntry { edits: undo_edits });
    }

    pub fn column_context_menu(
        &mut self,
        col_ix: usize,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> PopupMenu {
        use zqlz_ui::widgets::menu::PopupMenuItem;

        if col_ix == 0 {
            return menu;
        }
        let data_col_ix = col_ix - 1;
        let column_meta = self.column_meta.get(data_col_ix).cloned();
        let Some(col_meta) = column_meta else {
            return menu;
        };

        let menu_entity = cx.entity();
        let column_name = col_meta.name.clone();
        let column_type = col_meta.data_type.clone();
        let supports_distinct_values = matches!(self.driver_category, DriverCategory::Relational);

        let mut selected_display_rows = self.context_menu_selected_rows.clone();
        selected_display_rows.sort_unstable();
        selected_display_rows.dedup();

        let selected_actual_rows = ordered_unique_actual_rows_from_display_rows(
            &selected_display_rows,
            |display_row| self.get_actual_row_index(display_row),
            self.rows.len(),
        );

        let is_frozen = self
            .columns
            .get(col_ix)
            .map(|col| col.fixed == Some(ColumnFixed::Left))
            .unwrap_or(false);

        let column_values: Vec<String> = if selected_actual_rows.is_empty() {
            self.rows
                .iter()
                .filter_map(|row| row.get(data_col_ix).map(|v| v.display_for_table()))
                .collect()
        } else {
            selected_actual_rows
                .iter()
                .filter_map(|actual_row| {
                    self.rows
                        .get(*actual_row)
                        .and_then(|row| row.get(data_col_ix))
                        .map(|value| value.display_for_table())
                })
                .collect()
        };

        let copy_label = if selected_actual_rows.is_empty() {
            "Copy Column Values".to_string()
        } else {
            format!("Copy Selected Values ({})", selected_actual_rows.len())
        };

        menu.item({
            PopupMenuItem::new(copy_label).on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    let text = column_values.join("\n");
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
                },
            ))
        })
        .item({
            let column_name = column_name.clone();
            PopupMenuItem::new("Copy Field Name").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(column_name.clone()));
                },
            ))
        })
        .separator()
        .item({
            let viewer_panel = self.viewer_panel.clone();
            let column_name = column_name.clone();
            PopupMenuItem::new("Hide Column").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::HideColumn {
                            column_name: column_name.clone(),
                        });
                    }) {
                        tracing::error!("Failed to emit HideColumn: {:?}", e);
                    }
                },
            ))
        })
        .item({
            let viewer_panel = self.viewer_panel.clone();
            if is_frozen {
                PopupMenuItem::new("Unfreeze Column").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::UnfreezeColumn { col_ix });
                        }) {
                            tracing::error!("Failed to emit UnfreezeColumn: {:?}", e);
                        }
                    },
                ))
            } else {
                PopupMenuItem::new("Freeze Column").on_click(window.listener_for(
                    &menu_entity,
                    move |_this, _, _, cx| {
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::FreezeColumn { col_ix });
                        }) {
                            tracing::error!("Failed to emit FreezeColumn: {:?}", e);
                        }
                    },
                ))
            }
        })
        .separator()
        .item({
            let viewer_panel = self.viewer_panel.clone();
            PopupMenuItem::new("Size Column to Fit").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::SizeColumnToFit { col_ix });
                    }) {
                        tracing::error!("Failed to emit SizeColumnToFit: {:?}", e);
                    }
                },
            ))
        })
        .item({
            let viewer_panel = self.viewer_panel.clone();
            PopupMenuItem::new("Size All Columns to Fit").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::SizeAllColumnsToFit);
                    }) {
                        tracing::error!("Failed to emit SizeAllColumnsToFit: {:?}", e);
                    }
                },
            ))
        })
        .separator()
        .item({
            let viewer_panel = self.viewer_panel.clone();
            let column_name = column_name.clone();
            let connection_id = self.connection_id;
            let table_name = self.table_name.clone();
            let label = if supports_distinct_values {
                "Filter by Distinct Values".to_string()
            } else {
                "Filter by Distinct Values (Relational Only)".to_string()
            };
            PopupMenuItem::new(label)
                .disabled(!supports_distinct_values)
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::LoadDistinctValues {
                            connection_id,
                            table_name: table_name.clone(),
                            column_name: column_name.clone(),
                        });
                    }) {
                        tracing::error!("Failed to emit LoadDistinctValues: {:?}", e);
                    }
                }))
        })
        .separator()
        .item(PopupMenuItem::new(format!("Type: {}", column_type)))
    }
}
