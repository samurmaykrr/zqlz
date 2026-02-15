use super::*;

impl TableViewerPanel {
    pub fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let connection_id = self.connection_id;
        let table_name = self.table_name.clone();
        let has_selection = !self.selected_rows.is_empty();

        h_flex()
            .w_full()
            .h(px(36.0))
            .px_2()
            .gap_1()
            .items_center()
            .bg(theme.background)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Button::new("refresh-table")
                    .icon(ZqlzIcon::ArrowsClockwise)
                    .ghost()
                    .small()
                    .tooltip("Refresh Table")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if let (Some(conn_id), Some(tbl_name)) =
                            (this.connection_id, &this.table_name)
                        {
                            cx.emit(TableViewerEvent::RefreshTable {
                                connection_id: conn_id,
                                table_name: tbl_name.clone(),
                                driver_category: this.driver_category,
                                database_name: this.database_name.clone(),
                            });
                        }
                    })),
            )
            .child(
                Button::new("add-row")
                    .icon(ZqlzIcon::Plus)
                    .ghost()
                    .small()
                    .tooltip("Add Row")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.emit_add_row(cx);
                    })),
            )
            .child(
                Button::new("delete-row")
                    .icon(ZqlzIcon::Trash)
                    .ghost()
                    .small()
                    .tooltip("Delete Selected Rows")
                    .disabled(connection_id.is_none() || table_name.is_none() || !has_selection)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.show_delete_confirmation(window, cx);
                    })),
            )
            .child(div().h(px(20.0)).w(px(1.0)).bg(theme.border))
            .child({
                let has_pending_changes = self
                    .table_state
                    .as_ref()
                    .map(|s| s.read(cx).delegate().has_pending_changes())
                    .unwrap_or(false);
                let pending_count = self
                    .table_state
                    .as_ref()
                    .map(|s| s.read(cx).delegate().pending_change_count())
                    .unwrap_or(0);

                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        Button::new("auto-commit")
                            .icon(ZqlzIcon::Lightning)
                            .ghost()
                            .small()
                            .selected(self.auto_commit_mode)
                            .tooltip(if self.auto_commit_mode {
                                "Auto-commit: ON - Edits save immediately. Click to enable batch mode."
                            } else {
                                "Auto-commit: OFF - Edits are batched. Click to enable instant save."
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.set_auto_commit_mode(!this.auto_commit_mode, cx);
                            })),
                    )
                    .when(!self.auto_commit_mode, |this| {
                        this
                            .child(
                                Button::new("toggle-transaction-panel")
                                    .icon(if self.transaction_panel_expanded {
                                        ZqlzIcon::CaretDown
                                    } else {
                                        ZqlzIcon::CaretRight
                                    })
                                    .ghost()
                                    .small()
                                    .tooltip(if self.transaction_panel_expanded {
                                        "Collapse transaction controls"
                                    } else {
                                        "Expand transaction controls"
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.transaction_panel_expanded = !this.transaction_panel_expanded;
                                        cx.notify();
                                    })),
                            )
                            .when(has_pending_changes, |this| {
                                this.child(
                                    div()
                                        .px_2()
                                        .py(px(2.0))
                                        .rounded(px(4.0))
                                        .bg(theme.warning.opacity(0.2))
                                        .text_xs()
                                        .text_color(theme.warning)
                                        .child(format!("{} pending", pending_count)),
                                )
                            })
                            .when(self.transaction_panel_expanded, |this| {
                                this
                                    .child(
                                        Button::new("commit-changes")
                                            .icon(ZqlzIcon::Check)
                                            .ghost()
                                            .small()
                                            .tooltip(format!("Commit Changes ({})", pending_count))
                                            .disabled(!has_pending_changes)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.emit_commit_changes(cx);
                                            })),
                                    )
                                    .child(
                                        Button::new("discard-changes")
                                            .icon(ZqlzIcon::X)
                                            .ghost()
                                            .small()
                                            .tooltip("Discard Changes")
                                            .disabled(!has_pending_changes)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.discard_pending_changes(cx);
                                            })),
                                    )
                                    .child(
                                        Button::new("generate-sql")
                                            .icon(ZqlzIcon::Code)
                                            .ghost()
                                            .small()
                                            .tooltip("Generate SQL for Changes")
                                            .disabled(!has_pending_changes)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.emit_generate_sql(cx);
                                            })),
                                    )
                            })
                    })
                    .when(self.auto_commit_mode && has_pending_changes, |this| {
                        this.child(
                            Button::new("commit-remaining")
                                .label(format!("Commit {} pending", pending_count))
                                .ghost()
                                .small()
                                .tooltip("Commit pending changes from before auto-commit was enabled")
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.emit_commit_changes(cx);
                                })),
                        )
                    })
            })
            .child(div().h(px(20.0)).w(px(1.0)).bg(theme.border))
            .child({
                let filter_active = self.filter_expanded;
                let has_filters = self
                    .filter_panel_state
                    .as_ref()
                    .map(|s| s.read(cx).has_criteria())
                    .unwrap_or(false);

                Button::new("filter-table")
                    .icon(ZqlzIcon::Funnel)
                    .ghost()
                    .when(filter_active || has_filters, |btn| btn.selected(true))
                    .small()
                    .tooltip("Filter & Sort")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.toggle_filter_panel(cx);
                    }))
            })
            .child({
                let column_vis_active = self.column_visibility_shown;
                let has_hidden_columns = self
                    .column_visibility_state
                    .as_ref()
                    .map(|s| {
                        let state = s.read(cx);
                        state.visible_count() < state.total_count()
                    })
                    .unwrap_or(false);

                Button::new("column-visibility")
                    .icon(ZqlzIcon::Columns)
                    .ghost()
                    .when(column_vis_active || has_hidden_columns, |btn| {
                        btn.selected(true)
                    })
                    .small()
                    .tooltip("Column Visibility")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.toggle_column_visibility(cx);
                    }))
            })
            .child({
                let search_active = self.search_visible;
                Button::new("search-table")
                    .icon(ZqlzIcon::MagnifyingGlass)
                    .ghost()
                    .when(search_active, |btn| btn.selected(true))
                    .small()
                    .tooltip("Search (Cmd+F)")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.toggle_search(window, cx);
                    }))
            })
            .child(div().h(px(20.0)).w(px(1.0)).bg(theme.border))
            .child(
                Button::new("copy-cell")
                    .icon(ZqlzIcon::Copy)
                    .ghost()
                    .small()
                    .tooltip("Copy (Cmd+C)")
                    .disabled(connection_id.is_none() || table_name.is_none())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.copy_selection(cx);
                    })),
            )
            .child({
                let has_data = connection_id.is_some() && table_name.is_some();
                let panel = cx.entity().downgrade();
                Button::new("export-table")
                    .icon(ZqlzIcon::Export)
                    .ghost()
                    .small()
                    .tooltip("Export Data")
                    .disabled(!has_data)
                    .dropdown_menu({
                        let panel = panel.clone();
                        move |menu, _window, _cx| {
                            let panel_csv = panel.clone();
                            let panel_json = panel.clone();
                            let panel_sql = panel.clone();
                            menu.item(PopupMenuItem::new("Export as CSV").on_click(
                                move |_, _window, cx| {
                                    _ = panel_csv.update(cx, |this, cx| {
                                        this.export_csv(cx);
                                    });
                                },
                            ))
                            .item(PopupMenuItem::new("Export as JSON").on_click(
                                move |_, _window, cx| {
                                    _ = panel_json.update(cx, |this, cx| {
                                        this.export_json(cx);
                                    });
                                },
                            ))
                            .item(
                                PopupMenuItem::new("Export as SQL INSERT").on_click(
                                    move |_, _window, cx| {
                                        _ = panel_sql.update(cx, |this, cx| {
                                            this.export_sql(cx);
                                        });
                                    },
                                ),
                            )
                        }
                    })
            })
            .child(div().flex_1())
    }
}
