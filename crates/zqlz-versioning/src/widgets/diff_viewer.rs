//! Diff viewer widget
//!
//! Displays a side-by-side or unified diff view for comparing versions.

use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Sizable,
    button::{Button, ButtonVariant, ButtonVariants},
    dock::{Panel, PanelEvent, PanelState},
    h_flex, v_flex,
};

use crate::VersionDiff;
use crate::diff::{Change, ChangeType};

/// Display mode for the diff viewer
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DiffDisplayMode {
    /// Side-by-side view
    #[default]
    SideBySide,
    /// Unified diff view
    Unified,
}

/// Events emitted by the diff viewer
#[derive(Clone, Debug)]
pub enum DiffViewerEvent {
    /// User closed the diff viewer
    Close,
    /// User wants to restore the "from" version (includes version ID)
    RestoreFrom(uuid::Uuid),
    /// User wants to restore the "to" version (includes version ID)
    RestoreTo(uuid::Uuid),
}

/// Diff viewer panel for comparing versions
pub struct DiffViewer {
    focus_handle: FocusHandle,
    diff: Option<VersionDiff>,
    display_mode: DiffDisplayMode,
}

impl DiffViewer {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            diff: None,
            display_mode: DiffDisplayMode::default(),
        }
    }

    /// Set the diff to display
    pub fn set_diff(&mut self, diff: VersionDiff, cx: &mut Context<Self>) {
        self.diff = Some(diff);
        cx.notify();
    }

    /// Clear the diff
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.diff = None;
        cx.notify();
    }

    /// Toggle display mode
    pub fn toggle_mode(&mut self, cx: &mut Context<Self>) {
        self.display_mode = match self.display_mode {
            DiffDisplayMode::SideBySide => DiffDisplayMode::Unified,
            DiffDisplayMode::Unified => DiffDisplayMode::SideBySide,
        };
        cx.notify();
    }

    fn render_empty(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_color(theme.muted_foreground)
                .child("No diff to display"),
        )
    }

    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let diff = self.diff.as_ref().unwrap();
        let from_short = diff.from_version.short_id();
        let to_short = diff.to_version.short_id();
        let from_msg = diff.from_version.message.clone();
        let to_msg = diff.to_version.message.clone();

        h_flex()
            .w_full()
            .p_2()
            .gap_4()
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary.opacity(0.3))
            .child(
                h_flex()
                    .flex_1()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.danger)
                            .child(format!("- {}", from_short)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(from_msg),
                    ),
            )
            .child(div().text_color(theme.muted_foreground).child("->"))
            .child(
                h_flex()
                    .flex_1()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.success)
                            .child(format!("+ {}", to_short)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(to_msg),
                    ),
            )
            .child(
                Button::new("toggle-mode")
                    .label(match self.display_mode {
                        DiffDisplayMode::SideBySide => "Unified",
                        DiffDisplayMode::Unified => "Side by Side",
                    })
                    .small()
                    .with_variant(ButtonVariant::Ghost)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.toggle_mode(cx);
                    })),
            )
    }

    fn render_unified_diff(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let diff = self.diff.as_ref().unwrap();

        v_flex()
            .id("unified-diff-content")
            .size_full()
            .overflow_y_scroll()
            .p_2()
            .font_family("monospace")
            .text_sm()
            .children(diff.changes.iter().map(|change| {
                let (bg_color, text_color, prefix) = match change.tag {
                    ChangeType::Delete => (theme.danger.opacity(0.15), theme.danger, "-"),
                    ChangeType::Insert => (theme.success.opacity(0.15), theme.success, "+"),
                    ChangeType::Equal => (Hsla::transparent_black(), theme.foreground, " "),
                };

                let line_num = change.old_index.or(change.new_index).unwrap_or(0) + 1;

                h_flex()
                    .w_full()
                    .bg(bg_color)
                    .child(
                        div()
                            .w(px(40.0))
                            .text_right()
                            .px_2()
                            .text_color(theme.muted_foreground)
                            .child(format!("{}", line_num)),
                    )
                    .child(
                        div()
                            .w(px(16.0))
                            .text_center()
                            .text_color(text_color)
                            .child(prefix),
                    )
                    .child(
                        div()
                            .flex_1()
                            .text_color(text_color)
                            .child(change.value.trim_end().to_string()),
                    )
            }))
    }

    fn render_side_by_side(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let diff = self.diff.as_ref().unwrap();

        let (left_lines, right_lines) = self.split_changes(&diff.changes);

        h_flex()
            .size_full()
            .child(
                v_flex()
                    .id("side-by-side-left")
                    .flex_1()
                    .overflow_y_scroll()
                    .border_r_1()
                    .border_color(theme.border)
                    .p_2()
                    .font_family("monospace")
                    .text_sm()
                    .children(
                        left_lines
                            .iter()
                            .map(|line| self.render_diff_line(line, true, theme)),
                    ),
            )
            .child(
                v_flex()
                    .id("side-by-side-right")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_2()
                    .font_family("monospace")
                    .text_sm()
                    .children(
                        right_lines
                            .iter()
                            .map(|line| self.render_diff_line(line, false, theme)),
                    ),
            )
    }

    fn render_diff_line(
        &self,
        line: &DiffLine,
        is_left: bool,
        theme: &zqlz_ui::widgets::theme::ThemeColor,
    ) -> impl IntoElement {
        let (bg_color, text_color) = match line.change_type {
            Some(ChangeType::Delete) if is_left => (theme.danger.opacity(0.15), theme.danger),
            Some(ChangeType::Insert) if !is_left => (theme.success.opacity(0.15), theme.success),
            Some(_) => (Hsla::transparent_black(), theme.muted_foreground),
            None => (Hsla::transparent_black(), theme.foreground),
        };

        h_flex()
            .w_full()
            .min_h(px(20.0))
            .bg(bg_color)
            .child(
                div()
                    .w(px(40.0))
                    .text_right()
                    .px_2()
                    .text_color(theme.muted_foreground)
                    .child(line.line_num.map(|n| format!("{}", n)).unwrap_or_default()),
            )
            .child(
                div()
                    .flex_1()
                    .text_color(text_color)
                    .child(line.content.clone()),
            )
    }

    fn split_changes(&self, changes: &[Change]) -> (Vec<DiffLine>, Vec<DiffLine>) {
        let mut left = Vec::new();
        let mut right = Vec::new();

        for change in changes {
            match change.tag {
                ChangeType::Equal => {
                    left.push(DiffLine {
                        line_num: change.old_index.map(|i| i + 1),
                        content: change.value.trim_end().to_string(),
                        change_type: None,
                    });
                    right.push(DiffLine {
                        line_num: change.new_index.map(|i| i + 1),
                        content: change.value.trim_end().to_string(),
                        change_type: None,
                    });
                }
                ChangeType::Delete => {
                    left.push(DiffLine {
                        line_num: change.old_index.map(|i| i + 1),
                        content: change.value.trim_end().to_string(),
                        change_type: Some(ChangeType::Delete),
                    });
                    right.push(DiffLine {
                        line_num: None,
                        content: String::new(),
                        change_type: Some(ChangeType::Delete),
                    });
                }
                ChangeType::Insert => {
                    left.push(DiffLine {
                        line_num: None,
                        content: String::new(),
                        change_type: Some(ChangeType::Insert),
                    });
                    right.push(DiffLine {
                        line_num: change.new_index.map(|i| i + 1),
                        content: change.value.trim_end().to_string(),
                        change_type: Some(ChangeType::Insert),
                    });
                }
            }
        }

        (left, right)
    }

    fn render_actions(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .gap_2()
            .p_2()
            .border_t_1()
            .border_color(theme.border)
            .child(
                Button::new("restore-from")
                    .label("Restore Left")
                    .small()
                    .with_variant(ButtonVariant::Secondary)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        if let Some(ref diff) = this.diff {
                            cx.emit(DiffViewerEvent::RestoreFrom(diff.from_version.id));
                        }
                    })),
            )
            .child(
                Button::new("restore-to")
                    .label("Restore Right")
                    .small()
                    .with_variant(ButtonVariant::Secondary)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        if let Some(ref diff) = this.diff {
                            cx.emit(DiffViewerEvent::RestoreTo(diff.to_version.id));
                        }
                    })),
            )
    }
}

#[derive(Clone)]
struct DiffLine {
    line_num: Option<usize>,
    content: String,
    change_type: Option<ChangeType>,
}

impl EventEmitter<DiffViewerEvent> for DiffViewer {}
impl EventEmitter<PanelEvent> for DiffViewer {}

impl Focusable for DiffViewer {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Panel for DiffViewer {
    fn panel_name(&self) -> &'static str {
        "DiffViewer"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(ref diff) = self.diff {
            format!(
                "Diff: {} -> {}",
                diff.from_version.short_id(),
                diff.to_version.short_id()
            )
        } else {
            "Diff Viewer".to_string()
        }
    }

    fn dump(&self, _cx: &App) -> PanelState {
        PanelState::new(self)
    }
}

impl Render for DiffViewer {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("diff-viewer-panel")
            .key_context("DiffViewer")
            .size_full()
            .bg(theme.background)
            .track_focus(&self.focus_handle)
            .child(if self.diff.is_none() {
                self.render_empty(cx).into_any_element()
            } else {
                v_flex()
                    .size_full()
                    .child(self.render_header(cx))
                    .child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .child(match self.display_mode {
                                DiffDisplayMode::Unified => {
                                    self.render_unified_diff(cx).into_any_element()
                                }
                                DiffDisplayMode::SideBySide => {
                                    self.render_side_by_side(cx).into_any_element()
                                }
                            }),
                    )
                    .child(self.render_actions(cx))
                    .into_any_element()
            })
    }
}
