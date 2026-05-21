//! LSP integration for the text editor
//!
//! This module provides Language Server Protocol (LSP) integration for code intelligence features
//! like completions, diagnostics, hover info, and go-to-definition.
//!
//! The implementation follows the LSP specification:
//! https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/

use anyhow::Result;
use gpui::{App, Context, Task, Window};
use lsp_types::{
    CodeActionOrCommand, CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse,
    Diagnostic, DiagnosticSeverity, DocumentSymbolResponse, FoldingRange, Hover, InsertTextFormat,
    LinkedEditingRanges, SemanticToken, TextEdit, WorkspaceEdit,
};
use ropey::Rope;
use std::rc::Rc;
use std::sync::Arc;
use zqlz_core::{SyntaxTermProfile, get_syntax_term_profile};

use crate::{DocumentContext, TextEditor};

#[derive(Clone, Debug)]
pub struct CompletionRequestContext {
    pub revision: usize,
    pub cursor_offset: usize,
    pub trigger_offset: usize,
    pub current_prefix: String,
}

#[derive(Clone, Debug)]
pub enum CompletionResolution {
    CachedFilter {
        items: Vec<CompletionItem>,
        trigger_offset: usize,
    },
    Clear,
    Provider {
        request: RequestToken,
        trigger_offset: usize,
        trigger_prefix: String,
    },
}

#[derive(Clone, Debug)]
pub struct HoverRequestContext {
    pub revision: usize,
    pub cursor_offset: usize,
    pub offset: usize,
    pub word_target: Option<crate::WordTarget>,
}

#[derive(Clone, Debug)]
pub enum HoverResolution {
    Provider { request: RequestToken },
    Fallback(Option<HoverState>),
    Clear,
}

#[derive(Clone, Debug)]
pub struct HoverState {
    pub word: String,
    pub documentation: String,
    pub range: std::ops::Range<usize>,
}

#[derive(Clone, Debug)]
pub struct CompletionMenuData {
    pub items: Vec<CompletionItem>,
    pub selected_index: usize,
    pub scroll_offset: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CompletionMenuState {
    pub(crate) items: Vec<CompletionItem>,
    pub(crate) trigger_offset: usize,
    pub(crate) selected_index: usize,
    pub(crate) scroll_offset: usize,
    pub(crate) scroll_accumulator: f32,
}

#[derive(Clone, Debug)]
pub(crate) struct CompletionCache {
    pub(crate) all_items: Vec<CompletionItem>,
    pub(crate) trigger_prefix: String,
    pub(crate) trigger_offset: usize,
}

#[derive(Default)]
pub struct LspUiState {
    completion_menu: Option<CompletionMenuState>,
    completion_cache: Option<CompletionCache>,
    hover_state: Option<HoverState>,
}

impl LspUiState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn select_previous_completion(&mut self) -> bool {
        let Some(menu) = self.completion_menu.as_mut() else {
            return false;
        };
        if menu.selected_index == 0 {
            return false;
        }

        menu.selected_index -= 1;
        if menu.selected_index < menu.scroll_offset {
            menu.scroll_offset = menu.selected_index;
        }
        true
    }

    pub(crate) fn select_next_completion(&mut self, visible_items: usize) -> bool {
        let Some(menu) = self.completion_menu.as_mut() else {
            return false;
        };
        if menu.selected_index >= menu.items.len().saturating_sub(1) {
            return false;
        }

        menu.selected_index += 1;
        let visible_items = visible_items.max(1);
        let visible_end = menu.scroll_offset + visible_items;
        if menu.selected_index >= visible_end {
            menu.scroll_offset = menu.selected_index + 1 - visible_items;
        }
        true
    }

    pub(crate) fn select_completion_slot(&mut self, visible_slot: usize) -> bool {
        let Some(menu) = self.completion_menu.as_mut() else {
            return false;
        };
        let selected_index = menu.scroll_offset + visible_slot;
        if selected_index >= menu.items.len() {
            return false;
        }
        menu.selected_index = selected_index;
        true
    }

    pub(crate) fn scroll_completion_menu(
        &mut self,
        scroll_lines: f32,
        visible_items: usize,
    ) -> bool {
        let Some(menu) = self.completion_menu.as_mut() else {
            return false;
        };
        let visible_items = visible_items.max(1);
        menu.scroll_accumulator += scroll_lines;
        let steps = menu.scroll_accumulator.trunc() as i32;
        menu.scroll_accumulator -= steps as f32;
        if steps == 0 {
            return false;
        }

        let max_offset = menu.items.len().saturating_sub(visible_items);
        let new_offset = (menu.scroll_offset as i32 + steps).clamp(0, max_offset as i32) as usize;
        menu.scroll_offset = new_offset;
        if menu.selected_index < new_offset {
            menu.selected_index = new_offset;
        } else if menu.selected_index >= new_offset + visible_items {
            menu.selected_index = new_offset + visible_items - 1;
        }
        true
    }

    pub(crate) fn completion_cache(&self) -> Option<&CompletionCache> {
        self.completion_cache.as_ref()
    }

    pub(crate) fn set_completion_cache(&mut self, cache: CompletionCache) {
        self.completion_cache = Some(cache);
    }

    pub(crate) fn set_completion_items(
        &mut self,
        items: Vec<CompletionItem>,
        trigger_offset: usize,
    ) {
        if items.is_empty() {
            self.completion_menu = None;
            return;
        }

        let selected_index = items
            .iter()
            .position(|item| item.preselect.unwrap_or(false))
            .unwrap_or(0);
        self.completion_menu = Some(CompletionMenuState {
            items,
            trigger_offset,
            selected_index,
            scroll_offset: 0,
            scroll_accumulator: 0.0,
        });
    }

    pub(crate) fn take_completion_menu_state(&mut self) -> Option<CompletionMenuState> {
        self.completion_menu.take()
    }

    pub fn clear_completion_menu(&mut self) {
        self.completion_menu = None;
    }

    pub fn clear_completion(&mut self) {
        self.completion_menu = None;
        self.completion_cache = None;
    }

    pub(crate) fn clear_document_bound_ui(&mut self) {
        self.clear_completion();
        self.clear_hover_state();
    }

    pub fn has_completion_menu(&self) -> bool {
        self.completion_menu.is_some()
    }

    pub(crate) fn has_completion_items(&self) -> bool {
        self.completion_menu
            .as_ref()
            .is_some_and(|menu| !menu.items.is_empty())
    }

    pub fn completion_menu(&self) -> Option<CompletionMenuData> {
        self.completion_menu
            .as_ref()
            .map(|menu| CompletionMenuData {
                items: menu.items.clone(),
                selected_index: menu.selected_index,
                scroll_offset: menu.scroll_offset,
            })
    }

    pub(crate) fn visible_completions(&self, prefix: &str) -> Vec<CompletionItem> {
        if let Some(menu) = self.completion_menu() {
            return menu.items;
        }

        let Some(cache) = self.completion_cache.as_ref() else {
            return Vec::new();
        };

        let prefix = prefix.to_lowercase();
        let mut scored_items = cache
            .all_items
            .iter()
            .filter_map(|item| {
                let score = completion_match_score(completion_match_target(item), &prefix)?;
                Some((score, item))
            })
            .collect::<Vec<_>>();
        scored_items.sort_by(|(left_score, left_item), (right_score, right_item)| {
            left_score
                .cmp(right_score)
                .then_with(|| completion_sort_key(left_item).cmp(completion_sort_key(right_item)))
                .then_with(|| left_item.label.cmp(&right_item.label))
        });
        scored_items
            .into_iter()
            .map(|(_, item)| item)
            .cloned()
            .collect()
    }

    pub fn hover_state(&self) -> Option<HoverState> {
        self.hover_state.clone()
    }

    pub(crate) fn set_hover_state_if_word_changed(&mut self, hover_state: HoverState) -> bool {
        let should_update = self
            .hover_state
            .as_ref()
            .map(|current| current.word != hover_state.word)
            .unwrap_or(true);
        if should_update {
            self.hover_state = Some(hover_state);
        }
        should_update
    }

    pub fn clear_hover_state(&mut self) {
        self.hover_state = None;
    }

    pub fn has_hover(&self) -> bool {
        self.hover_state.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestToken {
    pub revision: usize,
    pub cursor_offset: usize,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderRequestKind {
    Completion,
    Hover,
    CodeActions,
    ApplyCodeAction,
    Diagnostics,
    SemanticTokens,
    DocumentSymbols,
    FoldingRanges,
    InlayHints,
    Formatting,
    LinkedEditingRanges,
}

#[derive(Default)]
struct RequestTracker {
    generation: u64,
}

impl RequestTracker {
    fn begin(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.generation = self.generation.saturating_add(1);
        RequestToken {
            revision,
            cursor_offset,
            generation: self.generation,
        }
    }

    fn matches(&self, token: RequestToken, revision: usize, cursor_offset: usize) -> bool {
        revision == token.revision
            && cursor_offset == token.cursor_offset
            && self.generation == token.generation
    }
}

pub struct LspRequestState {
    completion: RequestTracker,
    hover: RequestTracker,
    code_actions: RequestTracker,
    apply_code_action: RequestTracker,
    diagnostics: RequestTracker,
    semantic_tokens: RequestTracker,
    document_symbols: RequestTracker,
    folding_ranges: RequestTracker,
    inlay_hints: RequestTracker,
    formatting: RequestTracker,
    linked_editing_ranges: RequestTracker,
    completion_debounce_task: Task<Result<()>>,
    completion_task: Task<Result<()>>,
    hover_debounce_task: Task<Result<()>>,
    hover_task: Task<Result<()>>,
    completion_pending: bool,
    completion_pending_context: Option<CompletionContext>,
}

impl Default for LspRequestState {
    fn default() -> Self {
        Self {
            completion: RequestTracker::default(),
            hover: RequestTracker::default(),
            code_actions: RequestTracker::default(),
            apply_code_action: RequestTracker::default(),
            diagnostics: RequestTracker::default(),
            semantic_tokens: RequestTracker::default(),
            document_symbols: RequestTracker::default(),
            folding_ranges: RequestTracker::default(),
            inlay_hints: RequestTracker::default(),
            formatting: RequestTracker::default(),
            linked_editing_ranges: RequestTracker::default(),
            completion_debounce_task: Task::ready(Ok(())),
            completion_task: Task::ready(Ok(())),
            hover_debounce_task: Task::ready(Ok(())),
            hover_task: Task::ready(Ok(())),
            completion_pending: false,
            completion_pending_context: None,
        }
    }
}

impl LspRequestState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_completion(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.completion.begin(revision, cursor_offset)
    }

    pub fn begin_hover(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.hover.begin(revision, cursor_offset)
    }

    pub fn begin_code_actions(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.code_actions.begin(revision, cursor_offset)
    }

    pub fn begin_apply_code_action(&mut self, revision: usize) -> RequestToken {
        self.apply_code_action.begin(revision, 0)
    }

    pub fn begin_diagnostics(&mut self, revision: usize) -> RequestToken {
        self.diagnostics.begin(revision, 0)
    }

    pub fn begin_semantic_tokens(&mut self, revision: usize) -> RequestToken {
        self.semantic_tokens.begin(revision, 0)
    }

    pub fn begin_document_symbols(&mut self, revision: usize) -> RequestToken {
        self.document_symbols.begin(revision, 0)
    }

    pub fn begin_folding_ranges(&mut self, revision: usize) -> RequestToken {
        self.folding_ranges.begin(revision, 0)
    }

    pub fn begin_inlay_hints(&mut self, revision: usize) -> RequestToken {
        self.inlay_hints.begin(revision, 0)
    }

    pub fn begin_formatting(&mut self, revision: usize) -> RequestToken {
        self.formatting.begin(revision, 0)
    }

    pub fn begin_linked_editing_ranges(
        &mut self,
        revision: usize,
        cursor_offset: usize,
    ) -> RequestToken {
        self.linked_editing_ranges.begin(revision, cursor_offset)
    }

    pub fn begin_provider_request(
        &mut self,
        kind: ProviderRequestKind,
        revision: usize,
        cursor_offset: usize,
    ) -> RequestToken {
        match kind {
            ProviderRequestKind::Completion => self.begin_completion(revision, cursor_offset),
            ProviderRequestKind::Hover => self.begin_hover(revision, cursor_offset),
            ProviderRequestKind::CodeActions => self.begin_code_actions(revision, cursor_offset),
            ProviderRequestKind::ApplyCodeAction => self.begin_apply_code_action(revision),
            ProviderRequestKind::Diagnostics => self.begin_diagnostics(revision),
            ProviderRequestKind::SemanticTokens => self.begin_semantic_tokens(revision),
            ProviderRequestKind::DocumentSymbols => self.begin_document_symbols(revision),
            ProviderRequestKind::FoldingRanges => self.begin_folding_ranges(revision),
            ProviderRequestKind::InlayHints => self.begin_inlay_hints(revision),
            ProviderRequestKind::Formatting => self.begin_formatting(revision),
            ProviderRequestKind::LinkedEditingRanges => {
                self.begin_linked_editing_ranges(revision, cursor_offset)
            }
        }
    }

    pub fn matches_completion(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.completion.matches(token, revision, cursor_offset)
    }

    pub fn matches_hover(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.hover.matches(token, revision, cursor_offset)
    }

    pub fn matches_code_actions(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.code_actions.matches(token, revision, cursor_offset)
    }

    pub fn matches_apply_code_action(&self, token: RequestToken, revision: usize) -> bool {
        self.apply_code_action.matches(token, revision, 0)
    }

    pub fn matches_diagnostics(&self, token: RequestToken, revision: usize) -> bool {
        self.diagnostics.matches(token, revision, 0)
    }

    pub fn matches_semantic_tokens(&self, token: RequestToken, revision: usize) -> bool {
        self.semantic_tokens.matches(token, revision, 0)
    }

    pub fn matches_document_symbols(&self, token: RequestToken, revision: usize) -> bool {
        self.document_symbols.matches(token, revision, 0)
    }

    pub fn matches_folding_ranges(&self, token: RequestToken, revision: usize) -> bool {
        self.folding_ranges.matches(token, revision, 0)
    }

    pub fn matches_inlay_hints(&self, token: RequestToken, revision: usize) -> bool {
        self.inlay_hints.matches(token, revision, 0)
    }

    pub fn matches_formatting(&self, token: RequestToken, revision: usize) -> bool {
        self.formatting.matches(token, revision, 0)
    }

    pub fn matches_linked_editing_ranges(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.linked_editing_ranges
            .matches(token, revision, cursor_offset)
    }

    pub fn matches_provider_request(
        &self,
        kind: ProviderRequestKind,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        match kind {
            ProviderRequestKind::Completion => {
                self.matches_completion(token, revision, cursor_offset)
            }
            ProviderRequestKind::Hover => self.matches_hover(token, revision, cursor_offset),
            ProviderRequestKind::CodeActions => {
                self.matches_code_actions(token, revision, cursor_offset)
            }
            ProviderRequestKind::ApplyCodeAction => self.matches_apply_code_action(token, revision),
            ProviderRequestKind::Diagnostics => self.matches_diagnostics(token, revision),
            ProviderRequestKind::SemanticTokens => self.matches_semantic_tokens(token, revision),
            ProviderRequestKind::DocumentSymbols => self.matches_document_symbols(token, revision),
            ProviderRequestKind::FoldingRanges => self.matches_folding_ranges(token, revision),
            ProviderRequestKind::InlayHints => self.matches_inlay_hints(token, revision),
            ProviderRequestKind::Formatting => self.matches_formatting(token, revision),
            ProviderRequestKind::LinkedEditingRanges => {
                self.matches_linked_editing_ranges(token, revision, cursor_offset)
            }
        }
    }

    pub fn apply_provider_result_if_current<T>(
        &self,
        kind: ProviderRequestKind,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
        result: T,
    ) -> Option<T> {
        self.matches_provider_request(kind, token, revision, cursor_offset)
            .then_some(result)
    }

    pub fn apply_provider_task_result_if_current<T>(
        &self,
        kind: ProviderRequestKind,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
        result: Result<T>,
    ) -> Result<Option<T>> {
        if self.matches_provider_request(kind, token, revision, cursor_offset) {
            return result.map(Some);
        }

        Ok(None)
    }

    pub fn apply_diagnostic_lifecycle_if_current(
        &self,
        token: RequestToken,
        revision: usize,
        diagnostics: Vec<Diagnostic>,
        active_index: Option<usize>,
    ) -> Option<DiagnosticLifecycleSnapshot> {
        self.apply_provider_result_if_current(
            ProviderRequestKind::Diagnostics,
            token,
            revision,
            0,
            DiagnosticLifecycleSnapshot::new(revision, diagnostics, active_index, false),
        )
    }

    pub fn replace_completion_debounce_task(&mut self, task: Task<Result<()>>) {
        self.completion_debounce_task = task;
    }

    pub fn replace_completion_task(&mut self, task: Task<Result<()>>) {
        self.completion_task = task;
    }

    pub fn replace_hover_task(&mut self, task: Task<Result<()>>) {
        self.hover_task = task;
    }

    pub fn replace_hover_debounce_task(&mut self, task: Task<Result<()>>) {
        self.hover_debounce_task = task;
    }

    pub fn queue_completion_refresh(&mut self, trigger: CompletionContext) {
        self.completion_pending = true;
        self.completion_pending_context = Some(trigger);
    }

    pub fn completion_pending(&self) -> bool {
        self.completion_pending
    }

    pub fn take_pending_completion_context(&mut self) -> Option<CompletionContext> {
        self.completion_pending = false;
        self.completion_pending_context.take()
    }

    pub fn clear_pending_completion(&mut self) {
        self.completion_pending = false;
        self.completion_pending_context = None;
    }

    pub fn reset(&mut self) {
        self.completion_debounce_task = Task::ready(Ok(()));
        self.completion_task = Task::ready(Ok(()));
        self.hover_debounce_task = Task::ready(Ok(()));
        self.hover_task = Task::ready(Ok(()));
        self.completion_pending = false;
        self.completion_pending_context = None;
    }

    pub(crate) fn resolve_completion_request(
        &mut self,
        completion_cache: Option<&CompletionCache>,
        allow_provider_requests: bool,
        provider_available: bool,
        context: CompletionRequestContext,
    ) -> CompletionResolution {
        if let Some(cache) = completion_cache {
            let prefix_extends_cache = cache.trigger_offset == context.trigger_offset
                && context
                    .current_prefix
                    .to_lowercase()
                    .starts_with(&cache.trigger_prefix.to_lowercase());

            if prefix_extends_cache {
                let prefix_lower = context.current_prefix.to_lowercase();
                let mut scored_items = cache
                    .all_items
                    .iter()
                    .filter_map(|item| {
                        let score =
                            completion_match_score(completion_match_target(item), &prefix_lower)?;
                        Some((score, item))
                    })
                    .collect::<Vec<_>>();
                scored_items.sort_by(|(left_score, left_item), (right_score, right_item)| {
                    left_score
                        .cmp(right_score)
                        .then_with(|| {
                            completion_sort_key(left_item).cmp(completion_sort_key(right_item))
                        })
                        .then_with(|| left_item.label.cmp(&right_item.label))
                });
                let filtered = scored_items
                    .into_iter()
                    .map(|(_, item)| item)
                    .cloned()
                    .collect();
                return CompletionResolution::CachedFilter {
                    items: filtered,
                    trigger_offset: context.trigger_offset,
                };
            }
        }

        if !provider_available || !allow_provider_requests {
            return CompletionResolution::Clear;
        }

        CompletionResolution::Provider {
            request: self.begin_completion(context.revision, context.cursor_offset),
            trigger_offset: context.trigger_offset,
            trigger_prefix: context.current_prefix,
        }
    }

    pub(crate) fn resolve_hover_request(
        &mut self,
        allow_provider_requests: bool,
        provider_available: bool,
        fallback_hover: Option<HoverState>,
        context: HoverRequestContext,
    ) -> HoverResolution {
        if provider_available && allow_provider_requests {
            return HoverResolution::Provider {
                request: self.begin_hover(context.revision, context.cursor_offset),
            };
        }

        if let Some(hover) = fallback_hover {
            return HoverResolution::Fallback(Some(hover));
        }

        if context.word_target.is_none() {
            HoverResolution::Clear
        } else {
            HoverResolution::Fallback(None)
        }
    }
}

fn completion_match_target(item: &CompletionItem) -> &str {
    item.filter_text.as_deref().unwrap_or(&item.label)
}

fn completion_sort_key(item: &CompletionItem) -> &str {
    item.sort_text.as_deref().unwrap_or(&item.label)
}

fn completion_match_score(candidate: &str, normalized_query: &str) -> Option<usize> {
    if normalized_query.is_empty() {
        return Some(0);
    }

    let candidate = candidate.to_lowercase();
    if candidate.starts_with(normalized_query) {
        return Some(0);
    }

    if let Some(index) = candidate.find(normalized_query) {
        return Some(100 + index);
    }

    let mut score = 1_000usize;
    let mut query_chars = normalized_query.chars();
    let Some(mut query_char) = query_chars.next() else {
        return Some(0);
    };

    for (index, candidate_char) in candidate.chars().enumerate() {
        if candidate_char == query_char {
            score += index;
            if let Some(next_query_char) = query_chars.next() {
                query_char = next_query_char;
            } else {
                return Some(score);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionRequestContext, CompletionResolution, HoverRequestContext, HoverResolution,
        HoverState, LspRequestState, LspUiState, ProviderRequestKind,
    };
    use anyhow::anyhow;
    use lsp_types::{CompletionItem, Diagnostic, DiagnosticSeverity};
    use ropey::Rope;

    #[test]
    fn request_state_rejects_stale_completion_tokens() {
        let mut state = LspRequestState::new();
        let stale = state.begin_completion(1, 4);
        let current = state.begin_completion(2, 5);

        assert!(!state.matches_completion(stale, 2, 5));
        assert!(state.matches_completion(current, 2, 5));
    }

    #[test]
    fn request_state_rejects_stale_hover_tokens_after_cursor_move() {
        let mut state = LspRequestState::new();
        let stale = state.begin_hover(3, 7);
        let current = state.begin_hover(3, 9);

        assert!(!state.matches_hover(stale, 3, 9));
        assert!(state.matches_hover(current, 3, 9));
    }

    #[test]
    fn request_state_rejects_stale_extended_provider_tokens() {
        let mut state = LspRequestState::new();

        let stale_code_actions = state.begin_code_actions(1, 8);
        let current_code_actions = state.begin_code_actions(1, 10);
        assert!(!state.matches_code_actions(stale_code_actions, 1, 10));
        assert!(state.matches_code_actions(current_code_actions, 1, 10));

        let stale_apply_code_action = state.begin_apply_code_action(1);
        let current_apply_code_action = state.begin_apply_code_action(2);
        assert!(!state.matches_apply_code_action(stale_apply_code_action, 2));
        assert!(state.matches_apply_code_action(current_apply_code_action, 2));

        let stale_diagnostics = state.begin_diagnostics(1);
        let current_diagnostics = state.begin_diagnostics(2);
        assert!(!state.matches_diagnostics(stale_diagnostics, 2));
        assert!(state.matches_diagnostics(current_diagnostics, 2));

        let stale_semantic_tokens = state.begin_semantic_tokens(1);
        let current_semantic_tokens = state.begin_semantic_tokens(2);
        assert!(!state.matches_semantic_tokens(stale_semantic_tokens, 2));
        assert!(state.matches_semantic_tokens(current_semantic_tokens, 2));

        let stale_document_symbols = state.begin_document_symbols(3);
        let current_document_symbols = state.begin_document_symbols(4);
        assert!(!state.matches_document_symbols(stale_document_symbols, 4));
        assert!(state.matches_document_symbols(current_document_symbols, 4));

        let stale_folding_ranges = state.begin_folding_ranges(5);
        let current_folding_ranges = state.begin_folding_ranges(6);
        assert!(!state.matches_folding_ranges(stale_folding_ranges, 6));
        assert!(state.matches_folding_ranges(current_folding_ranges, 6));

        let stale_inlay_hints = state.begin_inlay_hints(5);
        let current_inlay_hints = state.begin_inlay_hints(6);
        assert!(!state.matches_inlay_hints(stale_inlay_hints, 6));
        assert!(state.matches_inlay_hints(current_inlay_hints, 6));

        let stale_formatting = state.begin_formatting(6);
        let current_formatting = state.begin_formatting(7);
        assert!(!state.matches_formatting(stale_formatting, 7));
        assert!(state.matches_formatting(current_formatting, 7));

        let stale_linked_editing_ranges = state.begin_linked_editing_ranges(7, 12);
        let current_linked_editing_ranges = state.begin_linked_editing_ranges(7, 14);
        assert!(!state.matches_linked_editing_ranges(stale_linked_editing_ranges, 7, 14));
        assert!(state.matches_linked_editing_ranges(current_linked_editing_ranges, 7, 14));
    }

    #[test]
    fn request_state_exposes_generic_provider_lifecycle_guard() {
        let provider_requests = [
            (ProviderRequestKind::Completion, 1, 3, 2, 3),
            (ProviderRequestKind::Hover, 1, 3, 1, 4),
            (ProviderRequestKind::CodeActions, 1, 3, 1, 4),
            (ProviderRequestKind::ApplyCodeAction, 1, 0, 2, 0),
            (ProviderRequestKind::Diagnostics, 1, 0, 2, 0),
            (ProviderRequestKind::SemanticTokens, 1, 0, 2, 0),
            (ProviderRequestKind::DocumentSymbols, 1, 0, 2, 0),
            (ProviderRequestKind::FoldingRanges, 1, 0, 2, 0),
            (ProviderRequestKind::InlayHints, 1, 0, 2, 0),
            (ProviderRequestKind::Formatting, 1, 0, 2, 0),
            (ProviderRequestKind::LinkedEditingRanges, 1, 3, 1, 4),
        ];

        for (kind, revision, cursor_offset, stale_revision, stale_cursor_offset) in
            provider_requests
        {
            let mut state = LspRequestState::new();
            let stale = state.begin_provider_request(kind, revision, cursor_offset);
            let current = state.begin_provider_request(kind, stale_revision, stale_cursor_offset);

            assert!(!state.matches_provider_request(
                kind,
                stale,
                stale_revision,
                stale_cursor_offset
            ));
            assert!(state.matches_provider_request(
                kind,
                current,
                stale_revision,
                stale_cursor_offset
            ));
        }
    }

    #[test]
    fn request_state_applies_only_current_provider_results() {
        let provider_requests = [
            (ProviderRequestKind::Completion, 1, 3, 2, 3),
            (ProviderRequestKind::Hover, 1, 3, 1, 4),
            (ProviderRequestKind::CodeActions, 1, 3, 1, 4),
            (ProviderRequestKind::ApplyCodeAction, 1, 0, 2, 0),
            (ProviderRequestKind::Diagnostics, 1, 0, 2, 0),
            (ProviderRequestKind::SemanticTokens, 1, 0, 2, 0),
            (ProviderRequestKind::DocumentSymbols, 1, 0, 2, 0),
            (ProviderRequestKind::FoldingRanges, 1, 0, 2, 0),
            (ProviderRequestKind::InlayHints, 1, 0, 2, 0),
            (ProviderRequestKind::Formatting, 1, 0, 2, 0),
            (ProviderRequestKind::LinkedEditingRanges, 1, 3, 1, 4),
        ];

        for (kind, revision, cursor_offset, current_revision, current_cursor_offset) in
            provider_requests
        {
            let mut state = LspRequestState::new();
            let stale = state.begin_provider_request(kind, revision, cursor_offset);
            let current =
                state.begin_provider_request(kind, current_revision, current_cursor_offset);

            assert_eq!(
                state.apply_provider_result_if_current(
                    kind,
                    stale,
                    current_revision,
                    current_cursor_offset,
                    "stale-result"
                ),
                None
            );
            assert_eq!(
                state.apply_provider_result_if_current(
                    kind,
                    current,
                    current_revision,
                    current_cursor_offset,
                    "current-result"
                ),
                Some("current-result")
            );
        }
    }

    #[test]
    fn request_state_surfaces_current_provider_errors_and_drops_stale_ones() {
        let mut state = LspRequestState::new();
        let stale = state.begin_provider_request(ProviderRequestKind::SemanticTokens, 1, 0);
        let current = state.begin_provider_request(ProviderRequestKind::SemanticTokens, 2, 0);

        let stale_result = state
            .apply_provider_task_result_if_current::<&'static str>(
                ProviderRequestKind::SemanticTokens,
                stale,
                2,
                0,
                Err(anyhow!("stale provider failure")),
            )
            .expect("stale errors are ignored with stale provider results");

        assert_eq!(stale_result, None);

        let current_result = state.apply_provider_task_result_if_current(
            ProviderRequestKind::SemanticTokens,
            current,
            2,
            0,
            Ok("current-result"),
        );

        assert_eq!(
            current_result.expect("current provider result"),
            Some("current-result")
        );

        let current_error = state
            .apply_provider_task_result_if_current::<&'static str>(
                ProviderRequestKind::SemanticTokens,
                current,
                2,
                0,
                Err(anyhow!("current provider failure")),
            )
            .expect_err("current provider errors must surface");

        assert_eq!(current_error.to_string(), "current provider failure");
    }

    #[test]
    fn request_state_applies_diagnostic_lifecycle_only_when_current() {
        let mut state = LspRequestState::new();
        let stale = state.begin_diagnostics(3);
        let current = state.begin_diagnostics(4);
        let diagnostics = vec![Diagnostic {
            message: "missing relation".to_string(),
            severity: Some(DiagnosticSeverity::ERROR),
            ..Default::default()
        }];

        assert_eq!(
            state.apply_diagnostic_lifecycle_if_current(stale, 4, diagnostics.clone(), Some(0)),
            None
        );

        let snapshot = state
            .apply_diagnostic_lifecycle_if_current(current, 4, diagnostics, Some(0))
            .expect("current diagnostic lifecycle snapshot");

        assert_eq!(snapshot.revision, 4);
        assert!(!snapshot.stale);
        assert_eq!(snapshot.max_severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(
            snapshot
                .active_diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.message.as_str()),
            Some("missing relation")
        );
    }

    #[test]
    fn ui_state_keeps_completion_cache_when_menu_is_filtered() {
        let mut state = LspUiState::new();
        state.set_completion_cache(super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            trigger_prefix: "se".to_string(),
            trigger_offset: 3,
        });
        state.set_completion_items(
            vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            3,
        );

        assert!(state.has_completion_menu());
        assert_eq!(
            state
                .completion_cache()
                .expect("completion cache")
                .trigger_offset,
            3
        );

        state.clear_completion_menu();

        assert!(state.completion_cache().is_some());
        assert!(!state.has_completion_menu());
        assert_eq!(state.visible_completions("sel").len(), 1);
        assert!(state.visible_completions("missing").is_empty());
    }

    #[test]
    fn ui_state_filters_cached_completions_with_filter_text() {
        let mut state = LspUiState::new();
        state.set_completion_cache(super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "customer_name".to_string(),
                filter_text: Some("name".to_string()),
                ..Default::default()
            }],
            trigger_prefix: "n".to_string(),
            trigger_offset: 3,
        });

        assert_eq!(state.visible_completions("nam").len(), 1);
        assert!(state.visible_completions("customer").is_empty());
    }

    #[test]
    fn ui_state_fuzzy_filters_and_ranks_cached_completions() {
        let mut state = LspUiState::new();
        state.set_completion_cache(super::CompletionCache {
            all_items: vec![
                CompletionItem {
                    label: "GROUP_CONCAT".to_string(),
                    ..Default::default()
                },
                CompletionItem {
                    label: "COUNT".to_string(),
                    ..Default::default()
                },
                CompletionItem {
                    label: "CREATE".to_string(),
                    ..Default::default()
                },
            ],
            trigger_prefix: "g".to_string(),
            trigger_offset: 3,
        });

        let completions = state.visible_completions("gc");

        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].label, "GROUP_CONCAT");
    }

    #[test]
    fn ui_state_completion_helpers_select_and_scroll_visible_window() {
        let mut state = LspUiState::new();
        state.set_completion_items(
            (0..12)
                .map(|index| CompletionItem {
                    label: format!("item-{index}"),
                    ..Default::default()
                })
                .collect(),
            0,
        );

        assert!(!state.select_previous_completion());
        assert!(state.select_next_completion(3));
        assert!(state.select_next_completion(3));
        assert!(state.select_next_completion(3));
        let menu = state.completion_menu.as_ref().expect("completion menu");
        assert_eq!(menu.selected_index, 3);
        assert_eq!(menu.scroll_offset, 1);

        assert!(state.select_completion_slot(2));
        assert_eq!(
            state
                .completion_menu
                .as_ref()
                .expect("completion menu")
                .selected_index,
            3
        );

        assert!(state.scroll_completion_menu(4.0, 3));
        let menu = state.completion_menu.as_ref().expect("completion menu");
        assert_eq!(menu.scroll_offset, 5);
        assert_eq!(menu.selected_index, 5);

        assert!(state.scroll_completion_menu(20.0, 3));
        let menu = state.completion_menu.as_ref().expect("completion menu");
        assert_eq!(menu.scroll_offset, 9);
        assert_eq!(menu.selected_index, 9);

        assert!(state.scroll_completion_menu(-20.0, 3));
        let menu = state.completion_menu.as_ref().expect("completion menu");
        assert_eq!(menu.scroll_offset, 0);
        assert_eq!(menu.selected_index, 2);
    }

    #[test]
    fn ui_state_selects_preselected_completion_item() {
        let mut state = LspUiState::new();
        state.set_completion_items(
            vec![
                CompletionItem {
                    label: "COUNT".to_string(),
                    ..Default::default()
                },
                CompletionItem {
                    label: "GROUP_CONCAT".to_string(),
                    preselect: Some(true),
                    ..Default::default()
                },
            ],
            0,
        );

        assert_eq!(
            state
                .completion_menu
                .as_ref()
                .expect("completion menu")
                .selected_index,
            1
        );
    }

    #[test]
    fn ui_state_tracks_hover_state_separately_from_provider_availability() {
        let mut state = LspUiState::new();
        assert!(state.set_hover_state_if_word_changed(HoverState {
            word: "select".to_string(),
            documentation: "keyword".to_string(),
            range: 0..6,
        }));

        assert!(state.has_hover());
        assert_eq!(state.hover_state().expect("hover state").word, "select");
        assert!(!state.set_hover_state_if_word_changed(HoverState {
            word: "select".to_string(),
            documentation: "keyword docs".to_string(),
            range: 0..6,
        }));
        assert!(state.set_hover_state_if_word_changed(HoverState {
            word: "from".to_string(),
            documentation: "keyword".to_string(),
            range: 7..11,
        }));
        assert_eq!(state.hover_state().expect("hover state").word, "from");

        state.clear_hover_state();

        assert!(!state.has_hover());
    }

    #[test]
    fn request_state_prefers_cached_completion_filter_when_prefix_extends_trigger() {
        let mut state = LspRequestState::new();
        let cache = super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            trigger_prefix: "se".to_string(),
            trigger_offset: 3,
        };

        let resolution = state.resolve_completion_request(
            Some(&cache),
            true,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "sel".to_string(),
            },
        );

        match resolution {
            CompletionResolution::CachedFilter {
                items,
                trigger_offset,
            } => {
                assert_eq!(trigger_offset, 3);
                assert_eq!(items.len(), 1);
            }
            other => panic!("expected cached completion filter, got {other:?}"),
        }
    }

    #[test]
    fn request_state_filters_cached_completion_request_with_filter_text() {
        let mut state = LspRequestState::new();
        let cache = super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "customer_name".to_string(),
                filter_text: Some("name".to_string()),
                ..Default::default()
            }],
            trigger_prefix: "n".to_string(),
            trigger_offset: 3,
        };

        let resolution = state.resolve_completion_request(
            Some(&cache),
            true,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "nam".to_string(),
            },
        );

        match resolution {
            CompletionResolution::CachedFilter { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].label, "customer_name");
            }
            other => panic!("expected cached completion filter, got {other:?}"),
        }
    }

    #[test]
    fn request_state_fuzzy_filters_cached_completion_request() {
        let mut state = LspRequestState::new();
        let cache = super::CompletionCache {
            all_items: vec![
                CompletionItem {
                    label: "GROUP_CONCAT".to_string(),
                    ..Default::default()
                },
                CompletionItem {
                    label: "COUNT".to_string(),
                    ..Default::default()
                },
            ],
            trigger_prefix: "g".to_string(),
            trigger_offset: 3,
        };

        let resolution = state.resolve_completion_request(
            Some(&cache),
            true,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "gc".to_string(),
            },
        );

        match resolution {
            CompletionResolution::CachedFilter { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].label, "GROUP_CONCAT");
            }
            other => panic!("expected cached completion filter, got {other:?}"),
        }
    }

    #[test]
    fn request_state_uses_sort_text_when_cached_scores_tie() {
        let mut state = LspRequestState::new();
        let cache = super::CompletionCache {
            all_items: vec![
                CompletionItem {
                    label: "select_z".to_string(),
                    sort_text: Some("2".to_string()),
                    ..Default::default()
                },
                CompletionItem {
                    label: "select_a".to_string(),
                    sort_text: Some("1".to_string()),
                    ..Default::default()
                },
            ],
            trigger_prefix: "sel".to_string(),
            trigger_offset: 3,
        };

        let resolution = state.resolve_completion_request(
            Some(&cache),
            true,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "sel".to_string(),
            },
        );

        match resolution {
            CompletionResolution::CachedFilter { items, .. } => {
                assert_eq!(items[0].label, "select_a");
                assert_eq!(items[1].label, "select_z");
            }
            other => panic!("expected cached completion filter, got {other:?}"),
        }
    }

    #[test]
    fn request_state_clears_completions_without_provider_or_cache() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_completion_request(
            None,
            true,
            false,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "se".to_string(),
            },
        );

        assert!(matches!(resolution, CompletionResolution::Clear));
    }

    #[test]
    fn sql_completion_provider_supports_subsequence_function_matching() {
        let provider = super::SqlCompletionProvider::for_profile("sqlite");
        let completions = provider.get_word_completions("gc");

        assert!(
            completions
                .iter()
                .any(|completion| completion.label == "GROUP_CONCAT")
        );
        assert_eq!(completions[0].label, "GROUP_CONCAT");
    }

    #[test]
    fn sql_completion_provider_uses_driver_syntax_terms() {
        let provider = super::SqlCompletionProvider::for_profile("redis");
        let completions = provider.get_word_completions("json.g");

        assert!(
            completions
                .iter()
                .any(|completion| completion.label == "JSON.GET")
        );
        assert!(
            completions
                .iter()
                .all(|completion| completion.detail.as_deref() != Some("SQL Keyword"))
        );
    }

    #[test]
    fn sql_completion_provider_uses_mongodb_types() {
        let provider = super::SqlCompletionProvider::for_profile("mongodb");
        let completions = provider.get_word_completions("object");

        assert!(
            completions
                .iter()
                .any(|completion| completion.label == "ObjectId")
        );
    }

    #[test]
    fn sql_completion_provider_uses_driver_word_chars_for_prefixes() {
        let provider = super::SqlCompletionProvider::for_profile("mongodb");
        let text = Rope::from_str("$gr");

        assert_eq!(provider.completion_prefix(&text, "$gr".len()), "$gr");
        assert!(
            provider
                .get_word_completions(&provider.completion_prefix(&text, "$gr".len()))
                .iter()
                .any(|completion| completion.label == "$group")
        );
    }

    #[test]
    fn sql_completion_provider_uses_driver_completion_triggers() {
        let mongo = super::SqlCompletionProvider::for_profile("mongodb");
        let trigger = mongo.completion_trigger_context_for_text(":");

        assert_eq!(
            trigger.and_then(|context| context.trigger_character),
            Some(":".to_string())
        );

        let redis = super::SqlCompletionProvider::for_profile("redis");
        let invoked = redis.completion_trigger_context_for_text(":");

        assert!(matches!(
            invoked.map(|context| context.trigger_kind),
            Some(lsp_types::CompletionTriggerKind::INVOKED)
        ));
    }

    #[test]
    fn sql_completion_provider_hover_uses_driver_syntax_terms() {
        let redis = super::SqlCompletionProvider::for_profile("redis");
        let redis_hover = redis
            .get_hover_documentation("JSON.GET")
            .expect("redis command hover");
        assert!(redis_hover.contains("JSON.GET keyword"));
        assert!(redis_hover.contains("redis"));

        let mongo = super::SqlCompletionProvider::for_profile("mongodb");
        let mongo_hover = mongo
            .get_hover_documentation("ObjectId")
            .expect("mongodb type hover");
        assert!(mongo_hover.contains("ObjectId type"));
        assert!(mongo_hover.contains("mongodb"));

        let redis_select = redis
            .get_hover_documentation("SELECT")
            .expect("redis SELECT command hover");
        assert!(redis_select.contains("redis"));
    }

    #[test]
    fn request_state_returns_provider_hover_when_provider_is_available() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_hover_request(
            true,
            true,
            None,
            HoverRequestContext {
                revision: 2,
                cursor_offset: 8,
                offset: 8,
                word_target: None,
            },
        );

        match resolution {
            HoverResolution::Provider { request } => {
                assert_eq!(request.revision, 2);
                assert_eq!(request.cursor_offset, 8);
            }
            other => panic!("expected provider hover request, got {other:?}"),
        }
    }

    #[test]
    fn request_state_clears_completions_when_policy_blocks_provider_requests() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_completion_request(
            None,
            false,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 4,
                trigger_offset: 0,
                current_prefix: "sel".to_string(),
            },
        );

        assert!(matches!(resolution, CompletionResolution::Clear));
    }

    #[test]
    fn request_state_uses_fallback_hover_when_policy_blocks_provider_requests() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_hover_request(
            false,
            true,
            Some(HoverState {
                word: "select".to_string(),
                documentation: "keyword".to_string(),
                range: 0..6,
            }),
            HoverRequestContext {
                revision: 1,
                cursor_offset: 4,
                offset: 4,
                word_target: None,
            },
        );

        assert!(matches!(resolution, HoverResolution::Fallback(Some(_))));
    }
}

/// Basic syntax-term completion provider.
pub struct SqlCompletionProvider {
    syntax_profile: &'static str,
    terms: SyntaxTermProfile,
    completion_triggers: Vec<char>,
    completion_word_chars: Vec<char>,
}

impl SqlCompletionProvider {
    pub fn new() -> Self {
        Self::for_profile("sql")
    }

    pub fn for_profile(syntax_profile: &'static str) -> Self {
        let capabilities = zqlz_core::get_syntax_driver_capabilities(syntax_profile);
        Self {
            syntax_profile: capabilities.profile,
            terms: get_syntax_term_profile(capabilities.profile),
            completion_triggers: capabilities.completion_triggers,
            completion_word_chars: capabilities.completion_word_chars,
        }
    }

    pub fn for_capabilities(capabilities: &zqlz_core::SyntaxDriverCapabilities) -> Self {
        Self {
            syntax_profile: capabilities.profile,
            terms: get_syntax_term_profile(capabilities.profile),
            completion_triggers: capabilities.completion_triggers.clone(),
            completion_word_chars: capabilities.completion_word_chars.clone(),
        }
    }

    /// Get completions for the current word
    pub fn get_word_completions(&self, prefix: &str) -> Vec<CompletionItem> {
        let prefix_lower = prefix.to_lowercase();
        let mut scored_items = Vec::new();

        self.extend_term_completions(
            &mut scored_items,
            self.terms
                .base_keywords
                .iter()
                .chain(self.terms.dialect_keywords),
            CompletionItemKind::KEYWORD,
            "Keyword",
            &prefix_lower,
            false,
        );
        self.extend_term_completions(
            &mut scored_items,
            self.terms
                .base_functions
                .iter()
                .chain(self.terms.dialect_functions),
            CompletionItemKind::FUNCTION,
            "Function",
            &prefix_lower,
            true,
        );
        self.extend_term_completions(
            &mut scored_items,
            self.terms.base_types.iter().chain(self.terms.dialect_types),
            CompletionItemKind::TYPE_PARAMETER,
            "Type",
            &prefix_lower,
            false,
        );

        scored_items.sort_by(|(left_score, left_item), (right_score, right_item)| {
            left_score
                .cmp(right_score)
                .then_with(|| completion_sort_key(left_item).cmp(completion_sort_key(right_item)))
                .then_with(|| left_item.label.cmp(&right_item.label))
        });
        let mut items = scored_items
            .into_iter()
            .map(|(_, item)| item)
            .collect::<Vec<_>>();
        items.dedup_by(|left, right| left.label == right.label);
        items
    }

    pub fn completion_prefix(&self, text: &Rope, byte_offset: usize) -> String {
        completion_prefix_with_word_chars(text, byte_offset, &self.completion_word_chars)
    }

    pub fn completion_trigger_context_for_text(&self, new_text: &str) -> Option<CompletionContext> {
        completion_trigger_context_for_characters(
            &self.completion_triggers,
            &self.completion_word_chars,
            new_text,
        )
    }

    fn extend_term_completions<'a>(
        &self,
        scored_items: &mut Vec<(usize, CompletionItem)>,
        terms: impl Iterator<Item = &'a &'static str>,
        kind: CompletionItemKind,
        detail_kind: &str,
        prefix_lower: &str,
        snippet: bool,
    ) {
        for term in terms {
            if let Some(score) = completion_match_score(term, prefix_lower) {
                scored_items.push((
                    score,
                    CompletionItem {
                        label: (*term).to_string(),
                        kind: Some(kind),
                        detail: Some(format!(
                            "{} {} ({})",
                            self.syntax_profile.to_uppercase(),
                            detail_kind,
                            self.syntax_profile
                        )),
                        insert_text: snippet.then(|| format!("{}(${{1:value}})", term)),
                        insert_text_format: snippet.then_some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    },
                ));
            }
        }
    }

    /// Get hover documentation for a word
    pub fn get_hover_documentation(&self, word: &str) -> Option<String> {
        self.term_hover_documentation(
            word,
            self.terms
                .base_keywords
                .iter()
                .chain(self.terms.dialect_keywords),
            "keyword",
        )
        .or_else(|| {
            self.term_hover_documentation(
                word,
                self.terms
                    .base_functions
                    .iter()
                    .chain(self.terms.dialect_functions),
                "function",
            )
        })
        .or_else(|| {
            self.term_hover_documentation(
                word,
                self.terms.base_types.iter().chain(self.terms.dialect_types),
                "type",
            )
        })
    }

    fn term_hover_documentation<'a>(
        &self,
        word: &str,
        terms: impl Iterator<Item = &'a &'static str>,
        kind: &str,
    ) -> Option<String> {
        terms
            .copied()
            .find(|term| term.eq_ignore_ascii_case(word))
            .map(|term| {
                format!(
                    "{} {}\n\nSource: `{}` syntax metadata.",
                    term, kind, self.syntax_profile
                )
            })
    }
}

impl Default for SqlCompletionProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for providing code completions
///
/// This trait is implemented by LSP providers to offer context-aware completions
/// based on the current cursor position and document content.
pub trait CompletionProvider: 'static {
    /// Fetch completions for the given position (async)
    ///
    /// # Arguments
    /// * `text` - The current text content as a Rope
    /// * `offset` - The cursor position in bytes
    /// * `trigger` - The completion trigger context (manual, trigger character, etc.)
    /// * `window` - The GPUI window
    /// * `cx` - The application context
    ///
    /// # Returns
    /// An async task that resolves to a list of completion items
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        trigger: CompletionContext,
        window: &mut Window,
        cx: &mut Context<TextEditor>,
    ) -> Task<Result<CompletionResponse>>;

    /// Check if completion should be triggered for the given text insertion
    ///
    /// # Arguments
    /// * `offset` - The position where text was inserted
    /// * `new_text` - The text that was just inserted
    /// * `cx` - The editor context
    ///
    /// # Returns
    /// LSP trigger metadata when completion should be shown.
    fn completion_trigger_context(
        &self,
        offset: usize,
        new_text: &str,
        cx: &mut Context<TextEditor>,
    ) -> Option<CompletionContext>;
}

/// Implementation of CompletionProvider using SqlCompletionProvider
impl CompletionProvider for SqlCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<TextEditor>,
    ) -> Task<Result<CompletionResponse>> {
        let prefix = self.completion_prefix(text, offset);

        let items = self.get_word_completions(&prefix);

        Task::ready(Ok(CompletionResponse::Array(items)))
    }

    fn completion_trigger_context(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<TextEditor>,
    ) -> Option<CompletionContext> {
        self.completion_trigger_context_for_text(new_text)
    }
}

/// Get the word at the given cursor offset
pub fn get_word_at_cursor(text: &Rope, offset: usize) -> String {
    get_word_at_offset(text, offset)
}

/// Get the word prefix at the given byte offset.
fn get_word_at_offset(text: &Rope, byte_offset: usize) -> String {
    completion_prefix_with_word_chars(text, byte_offset, &[])
}

pub fn completion_prefix_with_word_chars(
    text: &Rope,
    byte_offset: usize,
    extra_word_chars: &[char],
) -> String {
    // All ropey char-indexed APIs require a char index, not a byte offset. Clamp
    // to len_chars() so that a cursor sitting exactly at end-of-buffer (where
    // byte_offset == len_bytes()) maps safely to the last char position.
    let char_end = if byte_offset >= text.len_bytes() {
        text.len_chars()
    } else {
        text.byte_to_char(byte_offset)
    };

    // Scan backwards in char space to find the start of the current word.
    let mut char_start = char_end;
    while char_start > 0 {
        let prev = char_start - 1;
        match text.get_char(prev) {
            Some(ch) if zqlz_core::syntax_completion_word_char(ch, extra_word_chars) => {
                char_start = prev
            }
            _ => break,
        }
    }

    // Extract the word slice using char indices (safe, no unwrap).
    text.slice(char_start..char_end).to_string()
}

pub fn completion_trigger_context_for_characters(
    trigger_chars: &[char],
    word_chars: &[char],
    new_text: &str,
) -> Option<CompletionContext> {
    if new_text.len() == 1 {
        let ch = new_text.chars().next()?;

        if trigger_chars.contains(&ch) {
            return Some(CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::TRIGGER_CHARACTER,
                trigger_character: Some(ch.to_string()),
            });
        }

        if zqlz_core::syntax_completion_word_char(ch, word_chars) {
            return Some(CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            });
        }

        return None;
    }

    new_text
        .chars()
        .any(|ch| zqlz_core::syntax_completion_word_char(ch, word_chars))
        .then_some(CompletionContext {
            trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
            trigger_character: None,
        })
}

/// Trait for providing hover information
///
/// This trait is implemented by LSP providers to show documentation, type information,
/// or other contextual information when hovering over text.
pub trait HoverProvider: 'static {
    /// Get hover information for the given position
    ///
    /// # Arguments
    /// * `text` - The current text content as a Rope
    /// * `offset` - The cursor position in bytes
    /// * `window` - The GPUI window
    /// * `cx` - The application context
    ///
    /// # Returns
    /// An async task that resolves to hover information (or None if nothing to show)
    fn hover(
        &self,
        text: &Rope,
        offset: usize,
        window: &mut Window,
        cx: &App,
    ) -> Task<Result<Option<Hover>>>;
}

/// Provider for go-to-definition requests (feat-046).
///
/// Implementors resolve a byte offset in the buffer to a target position within
/// the same buffer. Returning `None` means "no definition found."
pub trait DefinitionProvider: 'static {
    /// Return the byte offset of the definition for the symbol at `offset`, if known.
    fn definition(&self, text: &Rope, offset: usize, document: &DocumentContext) -> Option<usize>;
}

/// Provider for find-references requests (feat-047).
///
/// Implementors return all byte ranges where the symbol at `offset` is used.
/// An empty `Vec` means "no references found."
pub trait ReferencesProvider: 'static {
    /// Return byte ranges of all usages of the symbol at `offset`.
    fn references(
        &self,
        text: &Rope,
        offset: usize,
        document: &DocumentContext,
    ) -> Vec<std::ops::Range<usize>>;
}

/// Provider for symbol rename operations.
pub trait RenameProvider: 'static {
    /// Build the workspace edit required to rename the symbol at `offset`.
    fn rename(
        &self,
        text: &Rope,
        offset: usize,
        new_name: &str,
        document: &DocumentContext,
    ) -> Option<WorkspaceEdit>;
}

/// Provider for code actions at the current cursor position.
pub trait CodeActionProvider: 'static {
    /// Return available code actions for the symbol or diagnostic under `offset`.
    fn code_actions(
        &self,
        text: &Rope,
        offset: usize,
        document: &DocumentContext,
        diagnostics: &[Diagnostic],
    ) -> Vec<CodeActionOrCommand>;
}

/// Provider for document diagnostic refreshes.
pub trait DiagnosticProvider: 'static {
    /// Return diagnostics for the current document snapshot.
    fn diagnostics(&self, text: &Rope, document: &DocumentContext)
    -> Task<Result<Vec<Diagnostic>>>;
}

/// Provider for applying a selected code action through one editor contract.
pub trait ApplyCodeActionProvider: 'static {
    /// Apply the selected action and return the concrete workspace edit, if any.
    fn apply_code_action(
        &self,
        action: &CodeActionOrCommand,
        text: &Rope,
        document: &DocumentContext,
    ) -> Task<Result<Option<WorkspaceEdit>>>;
}

/// Provider for semantic token refreshes.
pub trait SemanticTokenProvider: 'static {
    /// Return semantic tokens for the current document snapshot.
    fn semantic_tokens(
        &self,
        text: &Rope,
        document: &DocumentContext,
    ) -> Task<Result<Vec<SemanticToken>>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticLifecycleSnapshot {
    pub revision: usize,
    pub stale: bool,
    pub diagnostics: Vec<Diagnostic>,
    pub max_severity: Option<DiagnosticSeverity>,
    pub active_diagnostic: Option<Diagnostic>,
    pub inline_diagnostics: Vec<Diagnostic>,
    pub gutter_diagnostics: Vec<Diagnostic>,
    pub block_diagnostics: Vec<Diagnostic>,
}

impl DiagnosticLifecycleSnapshot {
    pub fn new(
        revision: usize,
        diagnostics: Vec<Diagnostic>,
        active_index: Option<usize>,
        stale: bool,
    ) -> Self {
        let max_severity = diagnostics
            .iter()
            .filter_map(|diagnostic| diagnostic.severity)
            .min_by_key(|severity| diagnostic_severity_rank(*severity));
        let active_diagnostic = active_index.and_then(|index| diagnostics.get(index).cloned());

        Self {
            revision,
            stale,
            inline_diagnostics: diagnostics.clone(),
            gutter_diagnostics: diagnostics.clone(),
            block_diagnostics: diagnostics.clone(),
            diagnostics,
            max_severity,
            active_diagnostic,
        }
    }
}

fn diagnostic_severity_rank(severity: DiagnosticSeverity) -> u8 {
    match severity {
        DiagnosticSeverity::ERROR => 1,
        DiagnosticSeverity::WARNING => 2,
        DiagnosticSeverity::INFORMATION => 3,
        DiagnosticSeverity::HINT => 4,
        _ => 5,
    }
}

/// Provider for document-outline symbols.
pub trait DocumentSymbolProvider: 'static {
    /// Return document symbols in LSP shape so callers can preserve hierarchy when available.
    fn document_symbols(
        &self,
        text: &Rope,
        document: &DocumentContext,
    ) -> Task<Result<Option<DocumentSymbolResponse>>>;
}

/// Provider for language-driven fold ranges.
pub trait FoldingRangeProvider: 'static {
    /// Return foldable source ranges for the current document snapshot.
    fn folding_ranges(
        &self,
        text: &Rope,
        document: &DocumentContext,
    ) -> Task<Result<Vec<FoldingRange>>>;
}

/// Provider for linked editing ranges such as paired SQL identifiers or tags.
pub trait LinkedEditingRangeProvider: 'static {
    /// Return linked ranges for the symbol at `offset`, if the language can provide them.
    fn linked_editing_ranges(
        &self,
        text: &Rope,
        offset: usize,
        document: &DocumentContext,
    ) -> Task<Result<Option<LinkedEditingRanges>>>;
}

/// Provider for language-driven inlay hint refreshes.
pub trait InlayHintProvider: 'static {
    /// Return normalized inlay hints for the current document snapshot.
    fn inlay_hints(
        &self,
        text: &Rope,
        document: &DocumentContext,
    ) -> Task<Result<Vec<EditorInlayHint>>>;
}

/// Provider for full-document formatting edits.
pub trait FormattingProvider: 'static {
    /// Return workspace-neutral text edits for the current document snapshot.
    fn formatting(&self, text: &Rope, document: &DocumentContext) -> Task<Result<Vec<TextEdit>>>;
}

/// Preferred side of the anchor position to render an inlay hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InlayHintSide {
    Before,
    After,
}

/// Semantic kind for an editor-owned inlay hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InlayHintKind {
    Type,
    Parameter,
}

/// Normalized inlay hint data used by the editor render pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditorInlayHint {
    /// Byte offset in the buffer that anchors the hint.
    pub byte_offset: usize,
    /// Plain-text label rendered for the hint.
    pub label: String,
    /// Preferred side of the anchor position.
    pub side: InlayHintSide,
    /// Optional semantic kind used for styling.
    pub kind: Option<InlayHintKind>,
    /// Whether to leave visual padding before the label.
    pub padding_left: bool,
    /// Whether to leave visual padding after the label.
    pub padding_right: bool,
}

/// Container for all LSP providers
///
/// This struct holds optional references to various LSP providers. Only the providers
/// that are set will be used. This allows for flexible LSP configuration.
pub struct Lsp {
    /// Optional completion provider for code completions
    completion_provider: Option<Rc<dyn CompletionProvider>>,

    /// Optional hover provider for hover tooltips
    hover_provider: Option<Rc<dyn HoverProvider>>,

    /// Optional definition provider for go-to-definition
    definition_provider: Option<Rc<dyn DefinitionProvider>>,

    /// Optional references provider for find-references
    references_provider: Option<Rc<dyn ReferencesProvider>>,

    /// Optional rename provider for symbol rename.
    rename_provider: Option<Rc<dyn RenameProvider>>,

    /// Optional code action provider.
    code_action_provider: Option<Rc<dyn CodeActionProvider>>,

    /// Optional code-action apply provider.
    apply_code_action_provider: Option<Rc<dyn ApplyCodeActionProvider>>,

    /// Optional diagnostic provider.
    diagnostic_provider: Option<Rc<dyn DiagnosticProvider>>,

    /// Optional semantic-token provider.
    semantic_token_provider: Option<Rc<dyn SemanticTokenProvider>>,

    /// Optional document-symbol provider.
    document_symbol_provider: Option<Rc<dyn DocumentSymbolProvider>>,

    /// Optional folding-range provider.
    folding_range_provider: Option<Rc<dyn FoldingRangeProvider>>,

    /// Optional linked-editing-range provider.
    linked_editing_range_provider: Option<Rc<dyn LinkedEditingRangeProvider>>,

    /// Optional inlay-hint provider.
    inlay_hint_provider: Option<Rc<dyn InlayHintProvider>>,

    /// Optional full-document formatting provider.
    formatting_provider: Option<Rc<dyn FormattingProvider>>,

    /// Back-compat SQL LSP handle set through `TextEditor::set_sql_lsp`.
    ///
    /// The editor itself does not depend on a concrete `zqlz-lsp` type; this handle
    /// is retained so legacy integrations can still indicate "connected" status.
    legacy_sql_lsp: Option<Arc<dyn std::any::Any + Send + Sync>>,

    request_state: LspRequestState,
    ui_state: LspUiState,
}

impl Default for Lsp {
    fn default() -> Self {
        Self {
            completion_provider: None,
            hover_provider: None,
            definition_provider: None,
            references_provider: None,
            rename_provider: None,
            code_action_provider: None,
            apply_code_action_provider: None,
            diagnostic_provider: None,
            semantic_token_provider: None,
            document_symbol_provider: None,
            folding_range_provider: None,
            linked_editing_range_provider: None,
            inlay_hint_provider: None,
            formatting_provider: None,
            legacy_sql_lsp: None,
            request_state: LspRequestState::new(),
            ui_state: LspUiState::new(),
        }
    }
}

impl Lsp {
    /// Create a new empty LSP container with no providers
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all LSP state
    ///
    /// This clears any ongoing tasks and resets internal state.
    pub fn reset(&mut self) {
        self.request_state.reset();
        self.clear_document_bound_ui();
    }

    pub(crate) fn clear_document_bound_ui(&mut self) {
        self.ui_state.clear_document_bound_ui();
    }

    pub(crate) fn clear_completion(&mut self) {
        self.ui_state.clear_completion();
    }

    pub(crate) fn clear_completion_menu(&mut self) {
        self.ui_state.clear_completion_menu();
    }

    pub(crate) fn has_completion_menu(&self) -> bool {
        self.ui_state.has_completion_menu()
    }

    pub(crate) fn has_completion_items(&self) -> bool {
        self.ui_state.has_completion_items()
    }

    pub(crate) fn select_previous_completion(&mut self) -> bool {
        self.ui_state.select_previous_completion()
    }

    pub(crate) fn select_next_completion(&mut self, visible_items: usize) -> bool {
        self.ui_state.select_next_completion(visible_items)
    }

    pub(crate) fn select_completion_slot(&mut self, visible_slot: usize) -> bool {
        self.ui_state.select_completion_slot(visible_slot)
    }

    pub(crate) fn scroll_completion_menu(
        &mut self,
        scroll_lines: f32,
        visible_items: usize,
    ) -> bool {
        self.ui_state
            .scroll_completion_menu(scroll_lines, visible_items)
    }

    pub(crate) fn visible_completions(&self, prefix: &str) -> Vec<CompletionItem> {
        self.ui_state.visible_completions(prefix)
    }

    pub(crate) fn resolve_completion_request(
        &mut self,
        allow_async_provider_requests: bool,
        context: CompletionRequestContext,
    ) -> CompletionResolution {
        self.request_state.resolve_completion_request(
            self.ui_state.completion_cache(),
            allow_async_provider_requests,
            self.completion_provider.is_some(),
            context,
        )
    }

    pub(crate) fn matches_completion_request(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.request_state
            .matches_completion(token, revision, cursor_offset)
    }

    pub(crate) fn replace_completion_task(&mut self, task: Task<Result<()>>) {
        self.request_state.replace_completion_task(task);
    }

    pub(crate) fn replace_completion_debounce_task(&mut self, task: Task<Result<()>>) {
        self.request_state.replace_completion_debounce_task(task);
    }

    pub(crate) fn clear_pending_completion(&mut self) {
        self.request_state.clear_pending_completion();
    }

    pub(crate) fn queue_completion_refresh(&mut self, trigger: CompletionContext) {
        self.request_state.queue_completion_refresh(trigger);
    }

    pub(crate) fn take_pending_completion_context(&mut self) -> Option<CompletionContext> {
        self.request_state.take_pending_completion_context()
    }

    pub(crate) fn set_completion_cache(&mut self, cache: CompletionCache) {
        self.ui_state.set_completion_cache(cache);
    }

    pub(crate) fn set_completion_items(
        &mut self,
        items: Vec<CompletionItem>,
        trigger_offset: usize,
    ) {
        self.ui_state.set_completion_items(items, trigger_offset);
    }

    pub(crate) fn take_completion_menu_state(&mut self) -> Option<CompletionMenuState> {
        self.ui_state.take_completion_menu_state()
    }

    pub(crate) fn clear_hover_state(&mut self) {
        self.ui_state.clear_hover_state();
    }

    pub(crate) fn set_hover_state_if_word_changed(&mut self, hover_state: HoverState) -> bool {
        self.ui_state.set_hover_state_if_word_changed(hover_state)
    }

    pub(crate) fn has_hover_state(&self) -> bool {
        self.ui_state.has_hover()
    }

    pub(crate) fn resolve_hover_request(
        &mut self,
        allow_async_provider_requests: bool,
        fallback_hover: Option<HoverState>,
        context: HoverRequestContext,
    ) -> HoverResolution {
        self.request_state.resolve_hover_request(
            allow_async_provider_requests,
            self.hover_provider.is_some(),
            fallback_hover,
            context,
        )
    }

    pub(crate) fn matches_hover_request(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.request_state
            .matches_hover(token, revision, cursor_offset)
    }

    pub(crate) fn replace_hover_task(&mut self, task: Task<Result<()>>) {
        self.request_state.replace_hover_task(task);
    }

    pub(crate) fn replace_hover_debounce_task(&mut self, task: Task<Result<()>>) {
        self.request_state.replace_hover_debounce_task(task);
    }

    pub(crate) fn set_completion_provider(&mut self, provider: Rc<dyn CompletionProvider>) {
        self.completion_provider = Some(provider);
    }

    pub(crate) fn clear_completion_provider(&mut self) {
        self.completion_provider = None;
    }

    pub(crate) fn set_hover_provider(&mut self, provider: Rc<dyn HoverProvider>) {
        self.hover_provider = Some(provider);
    }

    pub(crate) fn clear_hover_provider(&mut self) {
        self.hover_provider = None;
    }

    pub(crate) fn set_definition_provider(&mut self, provider: Rc<dyn DefinitionProvider>) {
        self.definition_provider = Some(provider);
    }

    pub(crate) fn clear_definition_provider(&mut self) {
        self.definition_provider = None;
    }

    pub(crate) fn set_references_provider(&mut self, provider: Rc<dyn ReferencesProvider>) {
        self.references_provider = Some(provider);
    }

    pub(crate) fn clear_references_provider(&mut self) {
        self.references_provider = None;
    }

    pub(crate) fn set_rename_provider(&mut self, provider: Rc<dyn RenameProvider>) {
        self.rename_provider = Some(provider);
    }

    pub(crate) fn clear_rename_provider(&mut self) {
        self.rename_provider = None;
    }

    pub(crate) fn set_code_action_provider(&mut self, provider: Rc<dyn CodeActionProvider>) {
        self.code_action_provider = Some(provider);
    }

    pub(crate) fn clear_code_action_provider(&mut self) {
        self.code_action_provider = None;
    }

    pub(crate) fn set_diagnostic_provider(&mut self, provider: Rc<dyn DiagnosticProvider>) {
        self.diagnostic_provider = Some(provider);
    }

    pub(crate) fn clear_diagnostic_provider(&mut self) {
        self.diagnostic_provider = None;
    }

    pub(crate) fn request_diagnostics(
        &self,
        text: &Rope,
        document: &DocumentContext,
    ) -> Option<Task<Result<Vec<Diagnostic>>>> {
        self.diagnostic_provider
            .as_ref()
            .map(|provider| provider.diagnostics(text, document))
    }

    pub(crate) fn set_legacy_sql_lsp(&mut self, lsp: Arc<dyn std::any::Any + Send + Sync>) {
        self.legacy_sql_lsp = Some(lsp);
    }

    pub(crate) fn has_any_provider_or_bridge(&self) -> bool {
        self.completion_provider.is_some()
            || self.hover_provider.is_some()
            || self.definition_provider.is_some()
            || self.references_provider.is_some()
            || self.rename_provider.is_some()
            || self.code_action_provider.is_some()
            || self.legacy_sql_lsp.is_some()
    }

    pub(crate) fn completion_trigger_context(
        &self,
        offset: usize,
        new_text: &str,
        cx: &mut Context<TextEditor>,
    ) -> Option<CompletionContext> {
        self.completion_provider
            .as_ref()
            .and_then(|provider| provider.completion_trigger_context(offset, new_text, cx))
    }

    pub(crate) fn request_completions(
        &self,
        rope: &Rope,
        offset: usize,
        trigger: CompletionContext,
        window: &mut Window,
        cx: &mut Context<TextEditor>,
    ) -> Option<Task<Result<CompletionResponse>>> {
        self.completion_provider
            .as_ref()
            .map(|provider| provider.completions(rope, offset, trigger, window, cx))
    }

    pub(crate) fn request_hover(
        &self,
        rope: &Rope,
        offset: usize,
        window: &mut Window,
        cx: &App,
    ) -> Option<Task<Result<Option<Hover>>>> {
        self.hover_provider
            .as_ref()
            .map(|provider| provider.hover(rope, offset, window, cx))
    }

    pub(crate) fn definition_at(
        &self,
        rope: &Rope,
        offset: usize,
        context: &DocumentContext,
    ) -> Option<usize> {
        self.definition_provider
            .as_ref()
            .and_then(|provider| provider.definition(rope, offset, context))
    }

    pub(crate) fn reference_ranges(
        &self,
        rope: &Rope,
        offset: usize,
        context: &DocumentContext,
    ) -> Option<Vec<std::ops::Range<usize>>> {
        self.references_provider
            .as_ref()
            .map(|provider| provider.references(rope, offset, context))
    }

    pub(crate) fn can_rename_at(
        &self,
        rope: &Rope,
        offset: usize,
        probe_name: &str,
        context: &DocumentContext,
    ) -> bool {
        self.rename_provider
            .as_ref()
            .is_some_and(|provider| provider.rename(rope, offset, probe_name, context).is_some())
    }

    pub(crate) fn rename_at(
        &self,
        rope: &Rope,
        offset: usize,
        new_name: &str,
        context: &DocumentContext,
    ) -> Option<WorkspaceEdit> {
        self.rename_provider
            .as_ref()
            .and_then(|provider| provider.rename(rope, offset, new_name, context))
    }

    pub(crate) fn code_actions_at(
        &self,
        rope: &Rope,
        offset: usize,
        context: &DocumentContext,
        diagnostics: &[Diagnostic],
    ) -> Option<Vec<CodeActionOrCommand>> {
        self.code_action_provider
            .as_ref()
            .map(|provider| provider.code_actions(rope, offset, context, diagnostics))
    }

    /// Check if completions are available
    pub fn has_completions(&self) -> bool {
        self.completion_provider.is_some()
    }

    /// Check if hover info is available
    pub fn has_hover(&self) -> bool {
        self.hover_provider.is_some()
    }

    /// Check if go-to-definition is available
    pub fn has_definition(&self) -> bool {
        self.definition_provider.is_some()
    }

    /// Check if find-references is available
    pub fn has_references(&self) -> bool {
        self.references_provider.is_some()
    }

    /// Check if rename is available.
    pub fn has_rename(&self) -> bool {
        self.rename_provider.is_some()
    }

    /// Check if code actions are available.
    pub fn has_code_actions(&self) -> bool {
        self.code_action_provider.is_some()
    }

    /// Check if selected code actions can be applied through a provider.
    pub fn has_apply_code_action(&self) -> bool {
        self.apply_code_action_provider.is_some()
    }

    /// Check if document diagnostics are available.
    pub fn has_diagnostics(&self) -> bool {
        self.diagnostic_provider.is_some()
    }

    /// Check if semantic tokens are available.
    pub fn has_semantic_tokens(&self) -> bool {
        self.semantic_token_provider.is_some()
    }

    /// Check if document symbols are available.
    pub fn has_document_symbols(&self) -> bool {
        self.document_symbol_provider.is_some()
    }

    /// Check if language-driven folding ranges are available.
    pub fn has_folding_ranges(&self) -> bool {
        self.folding_range_provider.is_some()
    }

    /// Check if linked editing ranges are available.
    pub fn has_linked_editing_ranges(&self) -> bool {
        self.linked_editing_range_provider.is_some()
    }

    /// Check if language-driven inlay hints are available.
    pub fn has_inlay_hints(&self) -> bool {
        self.inlay_hint_provider.is_some()
    }

    /// Check if language-driven formatting is available.
    pub fn has_formatting(&self) -> bool {
        self.formatting_provider.is_some()
    }

    pub fn hover_state(&self) -> Option<HoverState> {
        self.ui_state.hover_state()
    }

    pub fn completion_menu(&self) -> Option<CompletionMenuData> {
        self.ui_state.completion_menu()
    }
}

#[cfg(test)]
mod provider_contract_tests {
    use super::{
        ApplyCodeActionProvider, CodeActionProvider, CompletionProvider, DefinitionProvider,
        DiagnosticLifecycleSnapshot, DiagnosticProvider, DocumentSymbolProvider,
        FoldingRangeProvider, FormattingProvider, HoverProvider, InlayHintProvider,
        LinkedEditingRangeProvider, Lsp, ReferencesProvider, RenameProvider, SemanticTokenProvider,
    };
    use crate::{DocumentContext, DocumentIdentity, TextDocument, TextEditor};
    use anyhow::Result;
    use gpui::{App, Context, Task, Window};
    use lsp_types::{
        CodeActionOrCommand, Command, CompletionContext, CompletionResponse, Diagnostic,
        DiagnosticSeverity, DocumentSymbolResponse, FoldingRange, Hover, LinkedEditingRanges,
        Position, Range, SemanticToken, TextEdit, WorkspaceEdit,
    };
    use ropey::Rope;
    use std::rc::Rc;

    struct FakeApplyCodeActionProvider;

    struct FakeCompletionProvider;

    impl CompletionProvider for FakeCompletionProvider {
        fn completions(
            &self,
            _text: &Rope,
            _offset: usize,
            _trigger: CompletionContext,
            _window: &mut Window,
            _cx: &mut Context<TextEditor>,
        ) -> Task<Result<CompletionResponse>> {
            Task::ready(Ok(CompletionResponse::Array(Vec::new())))
        }

        fn completion_trigger_context(
            &self,
            _offset: usize,
            new_text: &str,
            _cx: &mut Context<TextEditor>,
        ) -> Option<CompletionContext> {
            (!new_text.is_empty()).then_some(CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            })
        }
    }

    struct FakeHoverProvider;

    impl HoverProvider for FakeHoverProvider {
        fn hover(
            &self,
            _text: &Rope,
            _offset: usize,
            _window: &mut Window,
            _cx: &App,
        ) -> Task<Result<Option<Hover>>> {
            Task::ready(Ok(Some(Hover {
                contents: lsp_types::HoverContents::Scalar(lsp_types::MarkedString::String(
                    "fake hover".to_string(),
                )),
                range: None,
            })))
        }
    }

    struct FakeDefinitionProvider;

    impl DefinitionProvider for FakeDefinitionProvider {
        fn definition(
            &self,
            _text: &Rope,
            offset: usize,
            _document: &DocumentContext,
        ) -> Option<usize> {
            Some(offset.saturating_sub(1))
        }
    }

    struct FakeReferencesProvider;

    impl ReferencesProvider for FakeReferencesProvider {
        fn references(
            &self,
            _text: &Rope,
            offset: usize,
            _document: &DocumentContext,
        ) -> Vec<std::ops::Range<usize>> {
            std::iter::once(offset..offset.saturating_add(6)).collect()
        }
    }

    struct FakeRenameProvider;

    impl RenameProvider for FakeRenameProvider {
        fn rename(
            &self,
            _text: &Rope,
            _offset: usize,
            new_name: &str,
            _document: &DocumentContext,
        ) -> Option<WorkspaceEdit> {
            (!new_name.is_empty()).then_some(WorkspaceEdit::default())
        }
    }

    struct FakeCodeActionProvider;

    impl CodeActionProvider for FakeCodeActionProvider {
        fn code_actions(
            &self,
            _text: &Rope,
            _offset: usize,
            _document: &DocumentContext,
            _diagnostics: &[Diagnostic],
        ) -> Vec<CodeActionOrCommand> {
            vec![CodeActionOrCommand::Command(Command {
                title: "fake action".to_string(),
                command: "fake.apply".to_string(),
                arguments: None,
            })]
        }
    }

    impl ApplyCodeActionProvider for FakeApplyCodeActionProvider {
        fn apply_code_action(
            &self,
            _action: &CodeActionOrCommand,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Option<WorkspaceEdit>>> {
            Task::ready(Ok(Some(WorkspaceEdit::default())))
        }
    }

    struct FakeSemanticTokenProvider;

    struct FakeDiagnosticProvider;

    impl DiagnosticProvider for FakeDiagnosticProvider {
        fn diagnostics(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Vec<Diagnostic>>> {
            Task::ready(Ok(vec![Diagnostic {
                message: "fake diagnostic".to_string(),
                severity: Some(DiagnosticSeverity::WARNING),
                ..Default::default()
            }]))
        }
    }

    impl SemanticTokenProvider for FakeSemanticTokenProvider {
        fn semantic_tokens(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Vec<SemanticToken>>> {
            Task::ready(Ok(vec![SemanticToken {
                delta_line: 0,
                delta_start: 1,
                length: 6,
                token_type: 2,
                token_modifiers_bitset: 0,
            }]))
        }
    }

    struct FakeDocumentSymbolProvider;

    impl DocumentSymbolProvider for FakeDocumentSymbolProvider {
        fn document_symbols(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Option<DocumentSymbolResponse>>> {
            Task::ready(Ok(Some(DocumentSymbolResponse::Flat(Vec::new()))))
        }
    }

    struct FakeFoldingRangeProvider;

    impl FoldingRangeProvider for FakeFoldingRangeProvider {
        fn folding_ranges(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Vec<FoldingRange>>> {
            Task::ready(Ok(vec![FoldingRange {
                start_line: 0,
                start_character: None,
                end_line: 2,
                end_character: None,
                kind: None,
                collapsed_text: None,
            }]))
        }
    }

    struct FakeLinkedEditingRangeProvider;

    impl LinkedEditingRangeProvider for FakeLinkedEditingRangeProvider {
        fn linked_editing_ranges(
            &self,
            _text: &Rope,
            _offset: usize,
            _document: &DocumentContext,
        ) -> Task<Result<Option<LinkedEditingRanges>>> {
            Task::ready(Ok(Some(LinkedEditingRanges {
                ranges: vec![Range::new(Position::new(0, 0), Position::new(0, 6))],
                word_pattern: None,
            })))
        }
    }

    struct FakeInlayHintProvider;

    impl InlayHintProvider for FakeInlayHintProvider {
        fn inlay_hints(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Vec<super::EditorInlayHint>>> {
            Task::ready(Ok(vec![super::EditorInlayHint {
                byte_offset: 6,
                label: ": integer".to_string(),
                side: super::InlayHintSide::After,
                kind: Some(super::InlayHintKind::Type),
                padding_left: true,
                padding_right: false,
            }]))
        }
    }

    struct FakeFormattingProvider;

    impl FormattingProvider for FakeFormattingProvider {
        fn formatting(
            &self,
            _text: &Rope,
            _document: &DocumentContext,
        ) -> Task<Result<Vec<TextEdit>>> {
            Task::ready(Ok(vec![TextEdit {
                range: Range::new(Position::new(0, 0), Position::new(0, 6)),
                new_text: "SELECT".to_string(),
            }]))
        }
    }

    fn fake_document_context() -> DocumentContext {
        TextDocument::with_text(
            DocumentIdentity::internal().expect("internal document identity"),
            "select one",
        )
        .context()
    }

    #[test]
    fn lsp_exposes_extended_provider_contract_availability() {
        let mut lsp = Lsp::new();

        assert!(!lsp.has_apply_code_action());
        assert!(!lsp.has_diagnostics());
        assert!(!lsp.has_semantic_tokens());
        assert!(!lsp.has_document_symbols());
        assert!(!lsp.has_folding_ranges());
        assert!(!lsp.has_linked_editing_ranges());
        assert!(!lsp.has_inlay_hints());
        assert!(!lsp.has_formatting());

        lsp.apply_code_action_provider = Some(Rc::new(FakeApplyCodeActionProvider));
        lsp.diagnostic_provider = Some(Rc::new(FakeDiagnosticProvider));
        lsp.semantic_token_provider = Some(Rc::new(FakeSemanticTokenProvider));
        lsp.document_symbol_provider = Some(Rc::new(FakeDocumentSymbolProvider));
        lsp.folding_range_provider = Some(Rc::new(FakeFoldingRangeProvider));
        lsp.linked_editing_range_provider = Some(Rc::new(FakeLinkedEditingRangeProvider));
        lsp.inlay_hint_provider = Some(Rc::new(FakeInlayHintProvider));
        lsp.formatting_provider = Some(Rc::new(FakeFormattingProvider));

        assert!(lsp.has_apply_code_action());
        assert!(lsp.has_diagnostics());
        assert!(lsp.has_semantic_tokens());
        assert!(lsp.has_document_symbols());
        assert!(lsp.has_folding_ranges());
        assert!(lsp.has_linked_editing_ranges());
        assert!(lsp.has_inlay_hints());
        assert!(lsp.has_formatting());
    }

    #[test]
    fn fake_core_providers_return_contract_outputs() {
        let document = fake_document_context();
        let text = Rope::from_str("select one");

        let definition = FakeDefinitionProvider.definition(&text, 6, &document);
        assert_eq!(definition, Some(5));

        let references = FakeReferencesProvider.references(&text, 0, &document);
        assert_eq!(references, vec![0..6]);

        let rename = FakeRenameProvider.rename(&text, 0, "renamed", &document);
        assert!(rename.is_some());

        let actions = FakeCodeActionProvider.code_actions(&text, 0, &document, &[]);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn lsp_exposes_core_provider_contract_availability() {
        let mut lsp = Lsp::new();

        assert!(!lsp.has_completions());
        assert!(!lsp.has_hover());
        assert!(!lsp.has_definition());
        assert!(!lsp.has_references());
        assert!(!lsp.has_rename());
        assert!(!lsp.has_code_actions());

        lsp.set_completion_provider(Rc::new(FakeCompletionProvider));
        lsp.set_hover_provider(Rc::new(FakeHoverProvider));
        lsp.set_definition_provider(Rc::new(FakeDefinitionProvider));
        lsp.set_references_provider(Rc::new(FakeReferencesProvider));
        lsp.set_rename_provider(Rc::new(FakeRenameProvider));
        lsp.set_code_action_provider(Rc::new(FakeCodeActionProvider));

        assert!(lsp.has_completions());
        assert!(lsp.has_hover());
        assert!(lsp.has_definition());
        assert!(lsp.has_references());
        assert!(lsp.has_rename());
        assert!(lsp.has_code_actions());
    }

    #[test]
    fn diagnostic_lifecycle_snapshot_exposes_render_outputs() {
        let diagnostics = vec![
            Diagnostic {
                severity: Some(DiagnosticSeverity::WARNING),
                message: "warn".to_string(),
                ..Default::default()
            },
            Diagnostic {
                severity: Some(DiagnosticSeverity::ERROR),
                message: "error".to_string(),
                ..Default::default()
            },
        ];

        let snapshot = DiagnosticLifecycleSnapshot::new(4, diagnostics, Some(1), true);

        assert_eq!(snapshot.revision, 4);
        assert!(snapshot.stale);
        assert_eq!(snapshot.max_severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(
            snapshot
                .active_diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.message.as_str()),
            Some("error")
        );
        assert_eq!(snapshot.inline_diagnostics.len(), 2);
        assert_eq!(snapshot.gutter_diagnostics.len(), 2);
        assert_eq!(snapshot.block_diagnostics.len(), 2);
    }

    #[test]
    fn fake_extended_providers_return_lifecycle_outputs() {
        let document = fake_document_context();
        let text = Rope::from_str("select one");
        let action = CodeActionOrCommand::Command(Command {
            title: "fake action".to_string(),
            command: "fake.apply".to_string(),
            arguments: None,
        });

        let applied_edit = futures::executor::block_on(
            FakeApplyCodeActionProvider.apply_code_action(&action, &text, &document),
        )
        .expect("apply-code-action result");
        assert!(applied_edit.is_some());

        let diagnostics =
            futures::executor::block_on(FakeDiagnosticProvider.diagnostics(&text, &document))
                .expect("diagnostics result");
        assert_eq!(diagnostics.len(), 1);

        let semantic_tokens = futures::executor::block_on(
            FakeSemanticTokenProvider.semantic_tokens(&text, &document),
        )
        .expect("semantic-token result");
        assert_eq!(semantic_tokens[0].length, 6);

        let document_symbols = futures::executor::block_on(
            FakeDocumentSymbolProvider.document_symbols(&text, &document),
        )
        .expect("document-symbol result");
        assert!(matches!(
            document_symbols,
            Some(DocumentSymbolResponse::Flat(_))
        ));

        let folding_ranges =
            futures::executor::block_on(FakeFoldingRangeProvider.folding_ranges(&text, &document))
                .expect("folding-range result");
        assert_eq!(folding_ranges[0].end_line, 2);

        let linked_editing_ranges = futures::executor::block_on(
            FakeLinkedEditingRangeProvider.linked_editing_ranges(&text, 0, &document),
        )
        .expect("linked-editing result");
        assert_eq!(linked_editing_ranges.expect("ranges").ranges.len(), 1);

        let inlay_hints =
            futures::executor::block_on(FakeInlayHintProvider.inlay_hints(&text, &document))
                .expect("inlay-hint result");
        assert_eq!(inlay_hints[0].label, ": integer");

        let formatting_edits =
            futures::executor::block_on(FakeFormattingProvider.formatting(&text, &document))
                .expect("formatting result");
        assert_eq!(formatting_edits[0].new_text, "SELECT");
    }
}
