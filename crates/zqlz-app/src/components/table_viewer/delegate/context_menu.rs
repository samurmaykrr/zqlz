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
        let all_row_values: Vec<String> = self
            .rows
            .get(actual_row_ix)
            .map(|r| r.iter().map(|v| v.display_for_table()).collect())
            .unwrap_or_default();
        let all_column_names: Vec<String> =
            self.column_meta.iter().map(|c| c.name.clone()).collect();
        let all_column_types: Vec<String> = self
            .column_meta
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        menu.item({
            let column_meta = column_meta.clone();
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let all_column_types = all_column_types.clone();
            let original_value = current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default();
            PopupMenuItem::new("Set to Empty String")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::SaveCell {
                                table_name: table_name.clone(),
                                connection_id,
                                row: actual_row_ix,
                                col: data_col_ix,
                                column_name: col_meta.name.clone(),
                                new_value: String::new(),
                                original_value: original_value.clone(),
                                all_row_values: all_row_values.clone(),
                                all_column_names: all_column_names.clone(),
                                all_column_types: all_column_types.clone(),
                            });
                        }) {
                            tracing::error!("Failed to emit SaveCell (Set to Empty String): {:?}", e);
                        }
                    }
                }))
        })
        .item({
            let column_meta = column_meta.clone();
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let all_column_types = all_column_types.clone();
            let original_value = current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default();
            PopupMenuItem::new("Set to NULL")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        _ = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::SaveCell {
                                table_name: table_name.clone(),
                                connection_id,
                                row: actual_row_ix,
                                col: data_col_ix,
                                column_name: col_meta.name.clone(),
                                new_value: "NULL".to_string(),
                                original_value: original_value.clone(),
                                all_row_values: all_row_values.clone(),
                                all_column_names: all_column_names.clone(),
                                all_column_types: all_column_types.clone(),
                            });
                        });
                    }
                }))
        })
        .separator()
        .item({
            let column_meta = column_meta.clone();
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let all_column_types = all_column_types.clone();
            let original_value = current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default();
            PopupMenuItem::new("Generate UUID")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        let uuid = Uuid::new_v4().to_string();
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::SaveCell {
                                table_name: table_name.clone(),
                                connection_id,
                                row: actual_row_ix,
                                col: data_col_ix,
                                column_name: col_meta.name.clone(),
                                new_value: uuid,
                                original_value: original_value.clone(),
                                all_row_values: all_row_values.clone(),
                                all_column_names: all_column_names.clone(),
                                all_column_types: all_column_types.clone(),
                            });
                        }) {
                            tracing::error!("Failed to emit SaveCell (Generate UUID): {:?}", e);
                        }
                    }
                }))
        })
        .item(
            PopupMenuItem::new("Edit in Cell Editor")
                .disabled(column_meta.is_none())
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
                    let current_value_str = current_value.as_ref().map(|v| v.display_for_table());
                    window.listener_for(&menu_entity, move |_this, _, _, cx| {
                        if let Some(col_meta) = &column_meta {
                            if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                                cx.emit(TableViewerEvent::EditCell {
                                    table_name: table_name.clone(),
                                    connection_id,
                                    row: actual_row_ix,
                                    col: data_col_ix,
                                    column_name: col_meta.name.clone(),
                                    column_type: col_meta.data_type.clone(),
                                    current_value: current_value_str.clone(),
                                    all_row_values: all_row_values.clone(),
                                    all_column_names: all_column_names.clone(),
                                    all_column_types: all_column_types.clone(),
                                    raw_bytes: raw_bytes.clone(),
                                });
                            }) {
                                tracing::error!("Failed to emit EditCell event: {:?}", e);
                            }
                        }
                    })
                }),
        )
        .item({
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let column_meta_vec = self.column_meta.clone();
            PopupMenuItem::new("Edit Row in Form").on_click(window.listener_for(
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
            PopupMenuItem::new("Filter")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                        if let Some(col_meta) = &column_meta {
                            let is_null = current_value
                                .as_ref()
                                .map(|v| v.is_null())
                                .unwrap_or(true);
                            let (operator, value) = if is_null {
                                (
                                    crate::components::table_viewer::filter_types::FilterOperator::IsNull,
                                    String::new(),
                                )
                            } else {
                                (
                                    crate::components::table_viewer::filter_types::FilterOperator::Equal,
                                    current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default(),
                                )
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
            let current_value = current_value.clone();
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let all_column_types = all_column_types.clone();
            let original_value = current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default();
            PopupMenuItem::new("Cut")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(ref val) = current_value {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(val.display_for_table()));
                    } else {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(String::new()));
                    }
                    if let Some(col_meta) = &column_meta {
                        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                            cx.emit(TableViewerEvent::SaveCell {
                                table_name: table_name.clone(),
                                connection_id,
                                row: actual_row_ix,
                                col: data_col_ix,
                                column_name: col_meta.name.clone(),
                                new_value: "NULL".to_string(),
                                original_value: original_value.clone(),
                                all_row_values: all_row_values.clone(),
                                all_column_names: all_column_names.clone(),
                                all_column_types: all_column_types.clone(),
                            });
                        }) {
                            tracing::error!("Failed to emit SaveCell (Cut): {:?}", e);
                        }
                    }
                }))
        })
        .item({
            let current_value = current_value.clone();
            PopupMenuItem::new("Copy").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    if let Some(ref val) = current_value {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(val.display_for_table()));
                    } else {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(String::new()));
                    }
                },
            ))
        })
        .item({
            let column_meta = column_meta.clone();
            PopupMenuItem::new("Copy Field Name")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(col_meta) = &column_meta {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                            col_meta.name.clone(),
                        ));
                    }
                }))
        })
        .item({
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            PopupMenuItem::new("Copy Row as SQL INSERT").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    let column_list = all_column_names
                        .iter()
                        .map(|n| format!("\"{}\"", n))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let values: Vec<String> = all_row_values
                        .iter()
                        .map(|v| {
                            if v.is_empty() || v == "NULL" {
                                "NULL".to_string()
                            } else {
                                format!("'{}'", v.replace('\'', "''"))
                            }
                        })
                        .collect();

                    let sql = format!(
                        "INSERT INTO \"{}\" ({}) VALUES ({});",
                        table_name,
                        column_list,
                        values.join(", ")
                    );

                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql));
                },
            ))
        })
        .item({
            let column_meta = column_meta.clone();
            let viewer_panel = viewer_panel.clone();
            let table_name = table_name.clone();
            let all_row_values = all_row_values.clone();
            let all_column_names = all_column_names.clone();
            let all_column_types = all_column_types.clone();
            let original_value = current_value.as_ref().map(|v| v.display_for_table()).unwrap_or_default();
            PopupMenuItem::new("Paste")
                .disabled(column_meta.is_none())
                .on_click(window.listener_for(&menu_entity, move |_this, _, _, cx| {
                    if let Some(clipboard_item) = cx.read_from_clipboard() {
                        if let Some(text) = clipboard_item.text() {
                            if let Some(col_meta) = &column_meta {
                                if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                                    cx.emit(TableViewerEvent::SaveCell {
                                        table_name: table_name.clone(),
                                        connection_id,
                                        row: actual_row_ix,
                                        col: data_col_ix,
                                        column_name: col_meta.name.clone(),
                                        new_value: text.to_string(),
                                        original_value: original_value.clone(),
                                        all_row_values: all_row_values.clone(),
                                        all_column_names: all_column_names.clone(),
                                        all_column_types: all_column_types.clone(),
                                    });
                                }) {
                                    tracing::error!("Failed to emit SaveCell (Paste): {:?}", e);
                                }
                            }
                        }
                    }
                }))
        })
        .separator()
        .item({
            let viewer_panel = viewer_panel.clone();

            let mut rows_to_delete = self.context_menu_selected_rows.clone();
            if !rows_to_delete.contains(&actual_row_ix) {
                rows_to_delete.push(actual_row_ix);
            }
            let delete_count = rows_to_delete.len();

            let label = if delete_count > 1 {
                format!("Delete Selected Rows ({})", delete_count)
            } else {
                "Delete Row".to_string()
            };

            PopupMenuItem::new(label).on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    let rows = rows_to_delete.clone();
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::MarkRowsForDeletion {
                            rows_to_delete: rows,
                        });
                    }) {
                        tracing::error!("Failed to emit MarkRowsForDeletion: {:?}", e);
                    }
                },
            ))
        })
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

        let is_frozen = self
            .columns
            .get(col_ix)
            .map(|col| col.fixed == Some(ColumnFixed::Left))
            .unwrap_or(false);

        let column_values: Vec<String> = self
            .rows
            .iter()
            .filter_map(|row| row.get(data_col_ix).map(|v| v.display_for_table()))
            .collect();

        menu.item({
            PopupMenuItem::new("Copy").on_click(window.listener_for(
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
            PopupMenuItem::new("Filter by Distinct Values").on_click(window.listener_for(
                &menu_entity,
                move |_this, _, _, cx| {
                    if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::LoadDistinctValues {
                            connection_id,
                            table_name: table_name.clone(),
                            column_name: column_name.clone(),
                        });
                    }) {
                        tracing::error!("Failed to emit LoadDistinctValues: {:?}", e);
                    }
                },
            ))
        })
        .separator()
        .item(PopupMenuItem::new(format!("Type: {}", column_type)))
    }
}
