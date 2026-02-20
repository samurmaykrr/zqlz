use anyhow::Result;
use gpui::{App, Context, Hsla, MouseMoveEvent, Task, Window};
use ropey::Rope;
use std::rc::Rc;

use crate::widgets::input::{InputState, RopeExt, popovers::ContextMenu};

mod code_actions;
mod completions;
mod definitions;
mod document_colors;
mod hover;

pub use code_actions::*;
pub use completions::*;
pub use definitions::*;
pub use document_colors::*;
pub use hover::*;

/// LSP ServerCapabilities
///
/// https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#serverCapabilities
pub struct Lsp {
    /// The completion provider.
    pub completion_provider: Option<Rc<dyn CompletionProvider>>,
    /// The code action providers.
    pub code_action_providers: Vec<Rc<dyn CodeActionProvider>>,
    /// The hover provider.
    pub hover_provider: Option<Rc<dyn HoverProvider>>,
    /// The definition provider.
    pub definition_provider: Option<Rc<dyn DefinitionProvider>>,
    /// The document color provider.
    pub document_color_provider: Option<Rc<dyn DocumentColorProvider>>,

    document_colors: Vec<(lsp_types::Range, Hsla)>,
    _hover_task: Task<Result<()>>,
    _document_color_task: Task<Result<()>>,
    /// Track the last word range we requested hover for, to avoid duplicate requests
    last_hover_word_range: Option<std::ops::Range<usize>>,
}

impl Default for Lsp {
    fn default() -> Self {
        Self {
            completion_provider: None,
            code_action_providers: vec![],
            hover_provider: None,
            definition_provider: None,
            document_color_provider: None,
            document_colors: vec![],
            _hover_task: Task::ready(Ok(())),
            _document_color_task: Task::ready(Ok(())),
            last_hover_word_range: None,
        }
    }
}

impl Lsp {
    /// Update the LSP when the text changes.
    pub(crate) fn update(
        &mut self,
        text: &Rope,
        window: &mut Window,
        cx: &mut Context<InputState>,
    ) {
        // Clear hover state when text changes since word ranges are now invalid
        self.last_hover_word_range = None;
        self.update_document_colors(text, window, cx);
    }

    /// Reset all LSP states.
    pub(crate) fn reset(&mut self) {
        self.document_colors.clear();
        self._hover_task = Task::ready(Ok(()));
        self._document_color_task = Task::ready(Ok(()));
        self.last_hover_word_range = None;
    }
}

impl InputState {
    pub(crate) fn hide_context_menu(&mut self, cx: &mut Context<Self>) {
        self.context_menu = None;
        self._context_menu_task = Task::ready(Ok(()));
        cx.notify();
    }

    pub fn is_context_menu_open(&self, cx: &App) -> bool {
        let Some(menu) = self.context_menu.as_ref() else {
            return false;
        };

        menu.is_open(cx)
    }

    /// Handles an action for the completion menu, if it exists.
    ///
    /// Return true if the action was handled, otherwise false.
    pub fn handle_action_for_context_menu(
        &mut self,
        action: Box<dyn gpui::Action>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(menu) = self.context_menu.as_ref() else {
            return false;
        };

        let mut handled = false;

        match menu {
            ContextMenu::Completion(menu) => {
                _ = menu.update(cx, |menu, cx| {
                    handled = menu.handle_action(action, window, cx)
                });
            }
            ContextMenu::CodeAction(menu) => {
                _ = menu.update(cx, |menu, cx| {
                    handled = menu.handle_action(action, window, cx)
                });
            }
            ContextMenu::MouseContext(..) => {}
        };

        handled
    }

    /// Apply a list of [`lsp_types::TextEdit`] to mutate the text.
    pub fn apply_lsp_edits(
        &mut self,
        text_edits: &Vec<lsp_types::TextEdit>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for edit in text_edits {
            let start = self.text.position_to_offset(&edit.range.start);
            let end = self.text.position_to_offset(&edit.range.end);

            let range_utf16 = self.range_to_utf16(&(start..end));
            self.replace_text_in_range_silent(Some(range_utf16), &edit.new_text, window, cx);
        }
    }

    pub(super) fn handle_mouse_move(
        &mut self,
        offset: usize,
        event: &MouseMoveEvent,
        window: &mut Window,
        cx: &mut Context<InputState>,
    ) {
        // Check if hover is enabled (respects user settings)
        let hover_enabled = self.hover_enabled;

        if event.modifiers.secondary() {
            self.handle_hover_definition(offset, window, cx);
        } else {
            self.hover_definition.clear();
            // Only show hover popover if enabled
            if hover_enabled {
                self.handle_hover_popover(offset, window, cx);
            }
        }
    }
}
