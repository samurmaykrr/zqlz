use crate::widgets::input::InputState;

/// Hover provider trait for LSP hover functionality.
///
/// Note: The legacy hover popover has been removed.
/// Use the schema metadata overlay via ShowHover action (F1 keybinding) instead.
pub trait HoverProvider {
    /// textDocument/hover
    ///
    /// https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#textDocument_hover
    fn hover(
        &self,
        _text: &ropey::Rope,
        _offset: usize,
        _window: &mut gpui::Window,
        _cx: &mut gpui::App,
    ) -> gpui::Task<anyhow::Result<Option<lsp_types::Hover>>>;
}

impl InputState {
    /// Handle hover trigger.
    ///
    /// Note: Legacy hover popover has been removed.
    /// Schema metadata overlay (ShowHover action) provides similar functionality via F1 keybinding.
    /// This method is kept for compatibility but does nothing.
    pub(super) fn handle_hover_popover(
        &mut self,
        _offset: usize,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<InputState>,
    ) {
        // Hover popover removed - use schema metadata overlay via ShowHover action (F1) instead
    }
}
