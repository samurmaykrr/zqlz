//! Version history panel widget
//!
//! Displays the version history for a database object, allowing users to
//! view, compare, and restore previous versions.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Sizable,
    button::{Button, ButtonVariant, ButtonVariants},
    dock::{Panel, PanelEvent, PanelState},
    h_flex, v_flex,
};

use crate::{DatabaseObjectType, VersionEntry, VersionRepository};

/// Events emitted by the version history panel
#[derive(Clone, Debug)]
pub enum VersionHistoryPanelEvent {
    /// User selected a version to view
    VersionSelected(VersionEntry),
    /// User wants to compare two versions
    CompareVersions { from: Uuid, to: Uuid },
    /// User wants to restore/apply a version
    RestoreVersion(VersionEntry),
    /// User wants to view the diff for a version
    ViewDiff(Uuid),
    /// User wants to tag a version
    TagVersion(Uuid),
}

/// Version history panel for viewing object version history
pub struct VersionHistoryPanel {
    focus_handle: FocusHandle,
    repository: Arc<VersionRepository>,
    connection_id: Option<Uuid>,
    object_id: Option<String>,
    object_type: Option<DatabaseObjectType>,
    versions: Vec<VersionEntry>,
    selected_version: Option<Uuid>,
    compare_from: Option<Uuid>,
    is_loading: bool,
}

impl VersionHistoryPanel {
    pub fn new(repository: Arc<VersionRepository>, cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            repository,
            connection_id: None,
            object_id: None,
            object_type: None,
            versions: Vec::new(),
            selected_version: None,
            compare_from: None,
            is_loading: false,
        }
    }

    /// Set the object to display history for
    pub fn set_object(
        &mut self,
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        cx: &mut Context<Self>,
    ) {
        self.connection_id = Some(connection_id);
        self.object_id = Some(object_id.clone());
        self.object_type = Some(object_type);
        self.selected_version = None;
        self.compare_from = None;

        self.load_versions(cx);
    }

    /// Clear the panel
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.connection_id = None;
        self.object_id = None;
        self.object_type = None;
        self.versions.clear();
        self.selected_version = None;
        self.compare_from = None;
        self.is_loading = false;
        cx.notify();
    }

    /// Reload versions from storage
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.load_versions(cx);
    }

    fn load_versions(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.connection_id else {
            return;
        };
        let Some(object_id) = self.object_id.clone() else {
            return;
        };

        self.is_loading = true;
        cx.notify();

        match self.repository.get_versions(connection_id, &object_id) {
            Ok(versions) => {
                self.versions = versions;
                self.is_loading = false;
            }
            Err(e) => {
                tracing::error!("Failed to load versions: {}", e);
                self.versions.clear();
                self.is_loading = false;
            }
        }
        cx.notify();
    }

    fn select_version(&mut self, version_id: Uuid, cx: &mut Context<Self>) {
        self.selected_version = Some(version_id);

        if let Some(version) = self.versions.iter().find(|v| v.id == version_id).cloned() {
            cx.emit(VersionHistoryPanelEvent::VersionSelected(version));
        }

        cx.notify();
    }

    fn toggle_compare(&mut self, version_id: Uuid, cx: &mut Context<Self>) {
        if self.compare_from == Some(version_id) {
            self.compare_from = None;
        } else if let Some(from) = self.compare_from {
            cx.emit(VersionHistoryPanelEvent::CompareVersions {
                from,
                to: version_id,
            });
            self.compare_from = None;
        } else {
            self.compare_from = Some(version_id);
        }
        cx.notify();
    }

    fn view_diff(&mut self, version_id: Uuid, cx: &mut Context<Self>) {
        cx.emit(VersionHistoryPanelEvent::ViewDiff(version_id));
    }

    fn restore_version(&mut self, version_id: Uuid, cx: &mut Context<Self>) {
        if let Some(version) = self.versions.iter().find(|v| v.id == version_id).cloned() {
            cx.emit(VersionHistoryPanelEvent::RestoreVersion(version));
        }
    }

    fn render_empty_state(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_2()
            .child(
                div()
                    .text_color(theme.muted_foreground)
                    .child("No version history"),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("Save a version to start tracking changes"),
            )
    }

    fn render_loading(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .child(div().text_color(theme.muted_foreground).child("Loading..."))
    }

    fn render_version_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("version-list-content")
            .size_full()
            .overflow_y_scroll()
            .gap_1()
            .p_2()
            .children(self.versions.iter().map(|version| {
                let version_id = version.id;
                let is_selected = self.selected_version == Some(version_id);
                let is_compare_from = self.compare_from == Some(version_id);
                let short_id = version.short_id();
                let message = version.message.clone();
                let created_at = version.created_at.format("%Y-%m-%d %H:%M").to_string();
                let author = version
                    .author
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string());

                let bg_color = if is_selected {
                    theme.accent.opacity(0.2)
                } else if is_compare_from {
                    theme.warning.opacity(0.2)
                } else {
                    theme.secondary.opacity(0.0)
                };

                div()
                    .id(ElementId::Name(format!("version-{}", version_id).into()))
                    .w_full()
                    .p_2()
                    .rounded_md()
                    .bg(bg_color)
                    .hover(|s| s.bg(theme.secondary.opacity(0.5)))
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _window, cx| {
                        this.select_version(version_id, cx);
                    }))
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                h_flex()
                                    .justify_between()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.accent)
                                            .child(short_id),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(created_at),
                                    ),
                            )
                            .child(div().text_sm().text_color(theme.foreground).child(message))
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(format!("by {}", author)),
                            ),
                    )
            }))
    }

    fn render_actions(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let has_selection = self.selected_version.is_some();
        let is_comparing = self.compare_from.is_some();
        let selected_version = self.selected_version;

        h_flex()
            .gap_2()
            .p_2()
            .border_t_1()
            .border_color(theme.border)
            .child(
                Button::new("view-diff")
                    .label("View Diff")
                    .small()
                    .disabled(!has_selection)
                    .with_variant(ButtonVariant::Secondary)
                    .when_some(selected_version, |this, vid| {
                        this.on_click(cx.listener(move |this, _, _window, cx| {
                            this.view_diff(vid, cx);
                        }))
                    }),
            )
            .child(
                Button::new("compare")
                    .label(if is_comparing {
                        "Select To..."
                    } else {
                        "Compare"
                    })
                    .small()
                    .disabled(!has_selection && !is_comparing)
                    .with_variant(if is_comparing {
                        ButtonVariant::Primary
                    } else {
                        ButtonVariant::Secondary
                    })
                    .when_some(selected_version, |this, vid| {
                        this.on_click(cx.listener(move |this, _, _window, cx| {
                            this.toggle_compare(vid, cx);
                        }))
                    }),
            )
            .child(
                Button::new("restore")
                    .label("Restore")
                    .small()
                    .disabled(!has_selection)
                    .with_variant(ButtonVariant::Secondary)
                    .when_some(selected_version, |this, vid| {
                        this.on_click(cx.listener(move |this, _, _window, cx| {
                            this.restore_version(vid, cx);
                        }))
                    }),
            )
    }
}

impl EventEmitter<VersionHistoryPanelEvent> for VersionHistoryPanel {}
impl EventEmitter<PanelEvent> for VersionHistoryPanel {}

impl Focusable for VersionHistoryPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Panel for VersionHistoryPanel {
    fn panel_name(&self) -> &'static str {
        "VersionHistoryPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(ref object_id) = self.object_id {
            format!("History: {}", object_id)
        } else {
            "Version History".to_string()
        }
    }

    fn dump(&self, _cx: &App) -> PanelState {
        PanelState::new(self)
    }
}

impl Render for VersionHistoryPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("version-history-panel")
            .key_context("VersionHistoryPanel")
            .size_full()
            .bg(theme.background)
            .track_focus(&self.focus_handle)
            .child(if self.is_loading {
                self.render_loading(cx).into_any_element()
            } else if self.versions.is_empty() {
                self.render_empty_state(cx).into_any_element()
            } else {
                v_flex()
                    .size_full()
                    .child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .child(self.render_version_list(cx)),
                    )
                    .child(self.render_actions(cx))
                    .into_any_element()
            })
    }
}
