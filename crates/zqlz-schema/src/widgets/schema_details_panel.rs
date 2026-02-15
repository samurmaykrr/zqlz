//! Schema details panel
//!
//! Displays detailed schema information for selected tables/views in the right dock.

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::Button,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex, v_flex, ActiveTheme, Sizable,
};

/// Column information for display
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub default_value: Option<String>,
}

/// Index information for display
#[derive(Clone, Debug)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// Foreign key information for display
#[derive(Clone, Debug)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Schema details for a table or view
#[derive(Clone, Debug)]
pub struct SchemaDetails {
    pub connection_id: Uuid,
    pub object_type: String,
    pub object_name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub create_statement: Option<String>,
}

/// Events emitted by the schema details panel
#[derive(Clone, Debug)]
pub enum SchemaDetailsPanelEvent {
    /// User requested to refresh schema
    Refresh,
}

/// Schema details panel for displaying table/view structure
pub struct SchemaDetailsPanel {
    focus_handle: FocusHandle,
    details: Option<SchemaDetails>,
    is_loading: bool,
    active_connection_id: Option<Uuid>,
    active_table_name: Option<String>,
    /// Flag set by parent when connection is invalidated
    connection_valid: bool,
}

impl SchemaDetailsPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            details: None,
            is_loading: false,
            active_connection_id: None,
            active_table_name: None,
            connection_valid: true,
        }
    }

    /// Set schema details to display
    pub fn set_details(&mut self, details: SchemaDetails, cx: &mut Context<Self>) {
        self.active_connection_id = Some(details.connection_id);
        self.active_table_name = Some(details.object_name.clone());
        self.details = Some(details);
        self.is_loading = false;
        self.connection_valid = true;
        cx.notify();
    }

    /// Clear the displayed details
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.details = None;
        self.is_loading = false;
        self.active_connection_id = None;
        self.active_table_name = None;
        self.connection_valid = true;
        cx.notify();
    }

    /// Set loading state
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.is_loading = loading;
        cx.notify();
    }

    /// Set loading state for a specific table, preventing duplicate concurrent loads
    /// by establishing identity before async work begins.
    pub fn set_loading_for_table(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        cx: &mut Context<Self>,
    ) {
        self.active_connection_id = Some(connection_id);
        self.active_table_name = Some(table_name.to_string());
        self.is_loading = true;
        cx.notify();
    }

    /// Check if the panel is showing details for a specific table
    pub fn is_showing_table(&self, connection_id: Uuid, table_name: &str) -> bool {
        self.active_connection_id == Some(connection_id)
            && self.active_table_name.as_deref() == Some(table_name)
    }

    /// Get the currently active connection ID
    pub fn active_connection(&self) -> Option<Uuid> {
        self.active_connection_id
    }

    /// Clear details if they don't match the given connection
    pub fn clear_if_not_connection(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        if self.active_connection_id.is_some() && self.active_connection_id != Some(connection_id) {
            self.clear(cx);
        }
    }

    /// Mark the connection as invalid (called by parent when connection is closed)
    pub fn invalidate_connection(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        if self.active_connection_id == Some(connection_id) {
            self.connection_valid = false;
            cx.notify();
        }
    }

    fn render_empty_state(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_2()
            .p_4()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .text_center()
                    .child("Select a table or view to see its schema"),
            )
    }

    fn render_loading(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("Loading schema..."),
        )
    }

    fn render_details(&self, details: &SchemaDetails, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .id("schema-details-content")
            .size_full()
            .overflow_y_scroll()
            .child(
                v_flex()
                    .w_full()
                    .p_2()
                    .gap_4()
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_start()
                                    .flex_wrap()
                                    .child(
                                        div()
                                            .px_2()
                                            .py(px(2.0))
                                            .rounded_sm()
                                            .bg(theme.accent)
                                            .text_xs()
                                            .text_color(gpui::white())
                                            .flex_shrink_0()
                                            .child(details.object_type.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_base()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .flex_1()
                                            .line_height(relative(1.4))
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .child(details.object_name.clone()),
                                    ),
                            )
                            .when(details.create_statement.is_some(), |this| {
                                let create_statement =
                                    details.create_statement.clone().unwrap_or_default();
                                this.child(
                                    Button::new("copy-create-statement")
                                        .small()
                                        .label("Copy CREATE TABLE")
                                        .on_click(cx.listener(move |_this, _, _window, cx| {
                                            cx.write_to_clipboard(ClipboardItem::new_string(
                                                create_statement.clone(),
                                            ));
                                            tracing::info!(
                                                "Copied CREATE TABLE statement to clipboard"
                                            );
                                        })),
                                )
                            }),
                    )
                    .child(self.render_columns_section(&details.columns, cx))
                    .when(!details.indexes.is_empty(), |this| {
                        this.child(self.render_indexes_section(&details.indexes, cx))
                    })
                    .when(!details.foreign_keys.is_empty(), |this| {
                        this.child(self.render_foreign_keys_section(&details.foreign_keys, cx))
                    }),
            )
    }

    fn render_columns_section(
        &self,
        columns: &[ColumnInfo],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(format!("Columns ({})", columns.len())),
            )
            .child(
                v_flex()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .overflow_hidden()
                    .children(columns.iter().enumerate().map(|(idx, col)| {
                        h_flex()
                            .w_full()
                            .px_2()
                            .py_1()
                            .gap_2()
                            .items_center()
                            .bg(if idx % 2 == 0 {
                                theme.table_even
                            } else {
                                theme.table
                            })
                            .child(h_flex().gap_1().when(col.primary_key, |this| {
                                this.child(div().text_xs().text_color(theme.warning).child("PK"))
                            }))
                            .child(
                                div()
                                    .flex_1()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(col.name.clone()),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(col.data_type.clone()),
                            )
                            .when(!col.nullable, |this| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .px_1()
                                        .rounded_sm()
                                        .bg(theme.info.opacity(0.2))
                                        .text_color(theme.info)
                                        .child("NOT NULL"),
                                )
                            })
                    })),
            )
    }

    fn render_indexes_section(
        &self,
        indexes: &[IndexInfo],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(format!("Indexes ({})", indexes.len())),
            )
            .child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .children(indexes.iter().map(|idx| {
                        v_flex()
                            .w_full()
                            .px_2()
                            .py_1()
                            .border_1()
                            .border_color(theme.border)
                            .rounded_md()
                            .gap_1()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_start()
                                    .flex_wrap()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .flex_1()
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .child(idx.name.clone()),
                                    )
                                    .when(idx.unique, |this| {
                                        this.child(
                                            div()
                                                .text_xs()
                                                .px_1()
                                                .rounded_sm()
                                                .bg(theme.success.opacity(0.2))
                                                .text_color(theme.success)
                                                .flex_shrink_0()
                                                .child("UNIQUE"),
                                        )
                                    }),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(idx.columns.join(", ")),
                            )
                    })),
            )
    }

    fn render_foreign_keys_section(
        &self,
        foreign_keys: &[ForeignKeyInfo],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(format!("Foreign Keys ({})", foreign_keys.len())),
            )
            .child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .children(foreign_keys.iter().map(|fk| {
                        v_flex()
                            .w_full()
                            .px_2()
                            .py_1()
                            .border_1()
                            .border_color(theme.border)
                            .rounded_md()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(fk.name.clone()),
                            )
                            .child(div().text_xs().text_color(theme.muted_foreground).child(
                                format!(
                                    "({}) -> {}.{}",
                                    fk.columns.join(", "),
                                    fk.referenced_table,
                                    fk.referenced_columns.join(", ")
                                ),
                            ))
                    })),
            )
    }
}

impl Render for SchemaDetailsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("schema-details-panel")
            .key_context("SchemaDetailsPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .map(|this| {
                if self.is_loading {
                    this.child(self.render_loading(cx))
                } else if !self.connection_valid {
                    // Connection no longer exists, show empty state
                    this.child(self.render_empty_state(cx))
                } else if let Some(details) = &self.details.clone() {
                    this.child(self.render_details(details, cx))
                } else {
                    this.child(self.render_empty_state(cx))
                }
            })
    }
}

impl Focusable for SchemaDetailsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for SchemaDetailsPanel {}
impl EventEmitter<SchemaDetailsPanelEvent> for SchemaDetailsPanel {}

impl Panel for SchemaDetailsPanel {
    fn panel_name(&self) -> &'static str {
        "SchemaDetailsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Schema"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
