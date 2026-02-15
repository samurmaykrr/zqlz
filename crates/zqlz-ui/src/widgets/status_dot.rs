use crate::widgets::styled::StyledExt;
use crate::widgets::theme::ActiveTheme;
use crate::widgets::{Sizable, Size};
use gpui::{div, px, App, Hsla, IntoElement, RenderOnce, StyleRefinement, Styled, Window};

/// Simple status indicator dot used for connection status and similar small indicators.
#[derive(IntoElement)]
pub struct StatusDot {
    style: StyleRefinement,
    status: ConnectionStatus,
    color: Option<Hsla>,
    size: Size,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
    Unknown,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Unknown
    }
}

impl StatusDot {
    /// Create a new StatusDot.
    pub fn new() -> Self {
        Self {
            style: StyleRefinement::default(),
            status: ConnectionStatus::default(),
            color: None,
            size: Size::Small,
        }
    }

    /// Set the status (Connected / Connecting / Disconnected / Unknown)
    pub fn status(mut self, status: ConnectionStatus) -> Self {
        self.status = status;
        self
    }

    /// Override the dot color.
    pub fn color(mut self, color: impl Into<Hsla>) -> Self {
        self.color = Some(color.into());
        self
    }
}

impl Styled for StatusDot {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl Sizable for StatusDot {
    fn with_size(mut self, size: impl Into<Size>) -> Self {
        self.size = size.into();
        self
    }
}

impl RenderOnce for StatusDot {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        // Determine color based on status (theme-aware by default)
        let color = self.color.unwrap_or_else(|| match self.status {
            ConnectionStatus::Connected => cx.theme().success,
            ConnectionStatus::Connecting => cx.theme().warning,
            ConnectionStatus::Disconnected => cx.theme().danger,
            ConnectionStatus::Unknown => cx.theme().muted_foreground,
        });

        let size_px = match self.size {
            Size::XSmall => px(6.),
            Size::Small => px(8.),
            Size::Medium => px(10.),
            Size::Large => px(14.),
            Size::Size(px_val) => px_val,
        };

        div()
            .flex_none()
            .size(size_px)
            .rounded_full()
            .bg(color)
            // subtle border so the dot reads on any background
            .border_1()
            .border_color(cx.theme().background)
            .refine_style(&self.style)
    }
}
