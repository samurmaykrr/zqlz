use std::time::Duration;

use anyhow::Result;
use gpui::{App, AppContext, Context, Task, Window};
use ropey::Rope;

use crate::widgets::input::{InputState, RopeExt, popovers::HoverPopoverData};
use crate::widgets::text::TextViewState;

/// Delay before making the LSP hover request (like Zed's HOVER_REQUEST_DELAY_MILLIS)
const HOVER_REQUEST_DELAY_MILLIS: u64 = 200;

/// Total delay before displaying the hover popover
const HOVER_DISPLAY_DELAY_MILLIS: u64 = 300;

/// Hover provider
///
/// https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#textDocument_hover
pub trait HoverProvider {
    /// textDocument/hover
    ///
    /// https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#textDocument_hover
    fn hover(
        &self,
        _text: &Rope,
        _offset: usize,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Task<Result<Option<lsp_types::Hover>>>;
}

impl InputState {
    /// Check if the mouse is still within an existing hover popover's range.
    /// This avoids re-requesting hover if we're still in the same symbol.
    fn same_hover_range(&self, offset: usize) -> bool {
        if let Some(data) = &self.hover_popover_data {
            // LSP returns a hover result for the end index of ranges that should be hovered
            // So we use inclusive range check
            data.symbol_range.start <= offset && offset <= data.symbol_range.end
        } else {
            false
        }
    }

    /// Handle hover trigger LSP request.
    /// Uses Zed's pattern of early exits and two-stage delays to minimize work.
    pub(super) fn handle_hover_popover(
        &mut self,
        offset: usize,
        window: &mut Window,
        cx: &mut Context<InputState>,
    ) {
        // Skip if currently selecting text
        if self.selecting {
            return;
        }

        // Check if hover provider exists
        let Some(provider) = self.lsp.hover_provider.clone() else {
            return;
        };

        // EARLY EXIT 1: If we're still within the existing hover result range, don't re-request
        if self.same_hover_range(offset) {
            return;
        }

        // Get the word range for this offset
        let word_range = self.text.word_range(offset).unwrap_or(offset..offset);

        // Don't show hover for empty ranges (whitespace, punctuation, etc.)
        if word_range.is_empty() {
            // Clear any existing hover when moving to whitespace
            if self.hover_popover_data.is_some() {
                self.hover_popover_data = None;
                self.hover_text_view_state = None;
                self.lsp.last_hover_word_range = None;
                cx.notify();
            }
            return;
        }

        // EARLY EXIT 2: Same word range as last request - don't re-request
        if let Some(last_range) = &self.lsp.last_hover_word_range {
            if last_range == &word_range {
                return;
            }
        }

        // Clear existing hover since we're moving to a new word
        // (task will be cancelled by replacement below)
        if self.hover_popover_data.is_some() {
            self.hover_popover_data = None;
            self.hover_text_view_state = None;
            cx.notify();
        }

        // Update the last hover word range
        self.lsp.last_hover_word_range = Some(word_range.clone());

        // Spawn async hover task - this will cancel any previous hover task
        let symbol_range = word_range;
        let editor = cx.entity();
        let text = self.text.clone();

        self.lsp._hover_task = cx.spawn_in(window, async move |_, cx| {
            // Two-stage delay pattern from Zed:
            // 1. Wait before making LSP request
            // 2. Total delay before showing popover

            // Create the display delay timer to wait for later
            let display_delay = cx
                .background_executor()
                .timer(Duration::from_millis(HOVER_DISPLAY_DELAY_MILLIS));

            // Wait the initial request delay before making the LSP request
            cx.background_executor()
                .timer(Duration::from_millis(HOVER_REQUEST_DELAY_MILLIS))
                .await;

            // Make the LSP request
            let result = cx
                .update(|window, cx| provider.hover(&text, symbol_range.start, window, cx))?
                .await?;

            // Wait for the remaining display delay
            display_delay.await;

            // Update the UI with results
            editor
                .update_in(cx, |editor, _window, cx| {
                    match result {
                        Some(hover) => {
                            // Resolve the symbol range from LSP response if provided
                            let mut resolved_range = symbol_range.clone();
                            if let Some(lsp_range) = &hover.range {
                                let start = editor.text.position_to_offset(&lsp_range.start);
                                let end = editor.text.position_to_offset(&lsp_range.end);
                                resolved_range = start..end;
                            }

                            // Don't create popover for empty ranges
                            if resolved_range.is_empty() {
                                editor.hover_popover_data = None;
                                editor.hover_text_view_state = None;
                            } else {
                                // Create the data and TextViewState
                                let data = HoverPopoverData::new(resolved_range, &hover);

                                // Create TextViewState inside update_in where we have a Context
                                let text_view_state =
                                    cx.new(|cx| TextViewState::markdown(&data.content, cx));
                                editor.hover_popover_data = Some(data);
                                editor.hover_text_view_state = Some(text_view_state);
                            }
                        }
                        None => {
                            editor.hover_popover_data = None;
                            editor.hover_text_view_state = None;
                        }
                    }
                    cx.notify();
                })
                .ok();

            Ok(())
        });
    }
}
