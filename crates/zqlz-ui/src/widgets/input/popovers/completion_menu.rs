use std::rc::Rc;

use gpui::{
    Action, AnyElement, App, AppContext as _, Context, DismissEvent, Empty, Entity, EventEmitter,
    HighlightStyle, InteractiveElement as _, IntoElement, ParentElement, Pixels, Point, Render,
    RenderOnce, SharedString, Styled, StyledText, Subscription, Window, deferred, div,
    prelude::FluentBuilder, px, relative,
};
use lsp_types::{CompletionItem, CompletionTextEdit};

const MAX_MENU_WIDTH: Pixels = px(450.);
const MAX_MENU_HEIGHT: Pixels = px(300.);
const POPOVER_GAP: Pixels = px(4.);

use crate::widgets::{
    ActiveTheme, IndexPath, Selectable, actions,
    input::{self, InputState, popovers::editor_popover},
    label::Label,
    list::{List, ListDelegate, ListEvent, ListState},
    text::{TextView, TextViewState},
};

// ---------------------------------------------------------------------------
// FuzzyMatcher — moved here from zqlz-lsp so the completion menu can filter
// items client-side without depending on the LSP crate.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchQuality {
    None = 0,
    Fuzzy = 1,
    Acronym = 2,
    Substring = 3,
    Prefix = 4,
}

#[derive(Debug, Clone)]
pub struct FuzzyMatch {
    pub quality: MatchQuality,
    pub score: i32,
    pub matched_indices: Vec<usize>,
}

impl FuzzyMatch {
    pub fn is_match(&self) -> bool {
        self.quality != MatchQuality::None
    }
}

pub struct FuzzyMatcher {
    case_sensitive: bool,
}

impl FuzzyMatcher {
    pub fn new(case_sensitive: bool) -> Self {
        Self { case_sensitive }
    }

    pub fn fuzzy_match(&self, pattern: &str, candidate: &str) -> Option<FuzzyMatch> {
        if pattern.is_empty() {
            return Some(FuzzyMatch {
                quality: MatchQuality::Prefix,
                score: 0,
                matched_indices: Vec::new(),
            });
        }

        let pattern_lower = pattern.to_lowercase();
        let candidate_lower = candidate.to_lowercase();

        let pattern_chars: Vec<char> = if self.case_sensitive {
            pattern.chars().collect()
        } else {
            pattern_lower.chars().collect()
        };

        let candidate_chars: Vec<char> = if self.case_sensitive {
            candidate.chars().collect()
        } else {
            candidate_lower.chars().collect()
        };

        if candidate_lower.starts_with(&pattern_lower) {
            let indices: Vec<usize> = (0..pattern_chars.len()).collect();
            return Some(FuzzyMatch {
                quality: MatchQuality::Prefix,
                score: 1000 + (candidate.len() - pattern.len()) as i32,
                matched_indices: indices,
            });
        }

        if let Some(pos) = candidate_lower.find(&pattern_lower) {
            let indices: Vec<usize> = (pos..pos + pattern_chars.len()).collect();
            let score = 800 - pos as i32;
            return Some(FuzzyMatch {
                quality: MatchQuality::Substring,
                score,
                matched_indices: indices,
            });
        }

        let candidate_no_underscore = candidate_lower.replace('_', "");
        if candidate_no_underscore.contains(&pattern_lower) {
            if let Some(pos) = candidate_no_underscore.find(&pattern_lower) {
                let score = 750 - pos as i32;
                return Some(FuzzyMatch {
                    quality: MatchQuality::Substring,
                    score,
                    matched_indices: Vec::new(),
                });
            }
        }

        if let Some(indices) = self.match_acronym(&pattern_chars, &candidate_chars, candidate) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Acronym,
                score: 600,
                matched_indices: indices,
            });
        }

        if let Some((score, indices)) = self.match_fuzzy(&pattern_chars, &candidate_chars) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Fuzzy,
                score,
                matched_indices: indices,
            });
        }

        Some(FuzzyMatch {
            quality: MatchQuality::None,
            score: 0,
            matched_indices: Vec::new(),
        })
    }

    fn match_acronym(
        &self,
        pattern: &[char],
        candidate: &[char],
        original_candidate: &str,
    ) -> Option<Vec<usize>> {
        let word_starts: Vec<usize> = original_candidate
            .char_indices()
            .enumerate()
            .filter_map(|(idx, (byte_pos, ch))| {
                if idx == 0 || ch.is_uppercase() || {
                    byte_pos > 0 && {
                        let prev_char =
                            original_candidate[..byte_pos].chars().last().unwrap_or(' ');
                        !prev_char.is_alphanumeric()
                    }
                } {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        if word_starts.len() < pattern.len() {
            return None;
        }

        let mut matched_indices = Vec::new();
        let mut word_idx = 0;

        for &pattern_char in pattern {
            let mut found = false;
            while word_idx < word_starts.len() {
                let candidate_idx = word_starts[word_idx];
                if candidate_idx < candidate.len() && candidate[candidate_idx] == pattern_char {
                    matched_indices.push(candidate_idx);
                    word_idx += 1;
                    found = true;
                    break;
                }
                word_idx += 1;
            }

            if !found {
                return None;
            }
        }

        Some(matched_indices)
    }

    fn match_fuzzy(&self, pattern: &[char], candidate: &[char]) -> Option<(i32, Vec<usize>)> {
        if pattern.is_empty() {
            return Some((0, Vec::new()));
        }

        if pattern.len() > candidate.len() {
            return None;
        }

        let mut best_score = None;
        let mut best_indices = Vec::new();

        self.fuzzy_match_recursive(
            pattern,
            candidate,
            0,
            0,
            0,
            Vec::new(),
            &mut best_score,
            &mut best_indices,
        );

        best_score.map(|score| (score, best_indices))
    }

    fn fuzzy_match_recursive(
        &self,
        pattern: &[char],
        candidate: &[char],
        pattern_idx: usize,
        candidate_idx: usize,
        current_score: i32,
        current_indices: Vec<usize>,
        best_score: &mut Option<i32>,
        best_indices: &mut Vec<usize>,
    ) {
        if pattern_idx >= pattern.len() {
            if best_score.is_none() || current_score > best_score.unwrap() {
                *best_score = Some(current_score);
                *best_indices = current_indices;
            }
            return;
        }

        if candidate_idx >= candidate.len() {
            return;
        }

        let pattern_char = pattern[pattern_idx];

        if candidate[candidate_idx] == pattern_char {
            let mut new_indices = current_indices.clone();
            new_indices.push(candidate_idx);

            let score_bonus = if pattern_idx > 0
                && candidate_idx > 0
                && current_indices.last() == Some(&(candidate_idx - 1))
            {
                10
            } else {
                0
            };

            self.fuzzy_match_recursive(
                pattern,
                candidate,
                pattern_idx + 1,
                candidate_idx + 1,
                current_score + score_bonus + 1,
                new_indices,
                best_score,
                best_indices,
            );
        }

        if candidate_idx < candidate.len() - 1 {
            self.fuzzy_match_recursive(
                pattern,
                candidate,
                pattern_idx,
                candidate_idx + 1,
                current_score - 1,
                current_indices,
                best_score,
                best_indices,
            );
        }
    }
}

impl Default for FuzzyMatcher {
    fn default() -> Self {
        Self::new(false)
    }
}

// ---------------------------------------------------------------------------
// CompletionMenuEditor — trait that abstracts text replacement, cursor info,
// and positioning so the menu works with both InputState and EditorWrapper.
// ---------------------------------------------------------------------------

/// Abstraction over text editors that the completion menu can interact with.
///
/// Both `InputState` (GPUI custom input) and `EditorWrapper` (Zed editor) implement
/// this trait so a single `CompletionMenu` can serve both.
///
/// Methods receive `&mut Context<Self>` so implementations can interact with their
/// own entity state (e.g. emit events, call methods that require `Context`).
/// The `CompletionMenu` calls these through `editor.update_in(cx, ...)` which
/// provides the correct context type.
pub trait CompletionMenuEditor: 'static + Sized {
    /// Replace text in the given byte range with `new_text`.
    ///
    /// Implementations should use their native undo-aware text replacement so that
    /// accepting a completion is a single undoable operation.
    fn completion_replace_text_in_range(
        &mut self,
        range: std::ops::Range<usize>,
        new_text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    );

    /// The current cursor offset in bytes.
    fn completion_cursor_offset(&self, cx: &App) -> usize;

    /// The full text content.
    fn completion_text_string(&self, cx: &App) -> String;

    /// Origin of the completion popup relative to the editor's top-left corner.
    ///
    /// Returns `None` if the cursor position cannot be determined (e.g. before
    /// the first layout pass).
    fn completion_origin(&self, cx: &App) -> Option<Point<Pixels>>;

    /// Re-focus the editor after a completion is inserted.
    fn completion_focus_editor(&mut self, window: &mut Window, cx: &mut Context<Self>);
}

// ---------------------------------------------------------------------------
// CompletionMenuItem
// ---------------------------------------------------------------------------

#[derive(IntoElement)]
struct CompletionMenuItem {
    ix: usize,
    item: Rc<CompletionItem>,
    children: Vec<AnyElement>,
    selected: bool,
    matched_indices: Vec<usize>,
}

impl CompletionMenuItem {
    fn new(ix: usize, item: Rc<CompletionItem>, matched_indices: Vec<usize>) -> Self {
        Self {
            ix,
            item,
            children: vec![],
            selected: false,
            matched_indices,
        }
    }
}

impl Selectable for CompletionMenuItem {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl ParentElement for CompletionMenuItem {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}

impl RenderOnce for CompletionMenuItem {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let item = self.item;
        let deprecated = item.deprecated.unwrap_or(false);

        // Build highlight ranges from matched character indices
        let highlights: Vec<_> = build_highlights_from_indices(&item.label, &self.matched_indices, cx);

        div()
            .id(self.ix)
            .flex()
            .flex_col()
            .gap_1()
            .p_2()
            .text_xs()
            .line_height(relative(1.2))
            .rounded_sm()
            .w_full()
            .when(deprecated, |this| this.line_through())
            .hover(|this| this.bg(cx.theme().accent.opacity(0.8)))
            .when(self.selected, |this| {
                this.bg(cx.theme().accent)
                    .text_color(cx.theme().accent_foreground)
            })
            .child(
                div()
                    .font_weight(gpui::FontWeight::SEMIBOLD)
                    .child(StyledText::new(item.label.clone()).with_highlights(highlights)),
            )
            .when(item.detail.is_some(), |this| {
                this.child(
                    div().text_xs().child(
                        Label::new(item.detail.as_deref().unwrap_or("").to_string())
                            .text_color(cx.theme().muted_foreground)
                            .when(deprecated, |el| el.line_through()),
                    ),
                )
            })
            .children(self.children)
    }
}

fn build_highlights_from_indices(
    label: &str,
    indices: &[usize],
    cx: &App,
) -> Vec<(std::ops::Range<usize>, HighlightStyle)> {
    if indices.is_empty() {
        return vec![];
    }

    let label_chars: Vec<(usize, char)> = label.char_indices().collect();
    let style = HighlightStyle {
        color: Some(cx.theme().blue),
        ..Default::default()
    };

    // Group consecutive indices into contiguous byte ranges
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < indices.len() {
        let char_idx = indices[i];
        if char_idx >= label_chars.len() {
            i += 1;
            continue;
        }
        let start_byte = label_chars[char_idx].0;
        let mut end_char_idx = char_idx;

        // Extend while consecutive
        while i + 1 < indices.len() && indices[i + 1] == end_char_idx + 1 {
            end_char_idx = indices[i + 1];
            i += 1;
        }

        let end_byte = if end_char_idx + 1 < label_chars.len() {
            label_chars[end_char_idx + 1].0
        } else {
            label.len()
        };

        ranges.push((start_byte..end_byte, style.clone()));
        i += 1;
    }

    ranges
}

// ---------------------------------------------------------------------------
// Delegate
// ---------------------------------------------------------------------------

struct CompletionMenuDelegate<E: CompletionMenuEditor> {
    query: SharedString,
    menu: Entity<CompletionMenu<E>>,
    /// All raw items from the LSP.
    items: Vec<Rc<CompletionItem>>,
    /// Indices into `items` after fuzzy filtering + sorting.
    filtered: Vec<FilteredEntry>,
    selected_ix: usize,
    matcher: FuzzyMatcher,
}

struct FilteredEntry {
    /// Index into the original `items` vec.
    source_index: usize,
    /// Matched character indices for highlighting.
    matched_indices: Vec<usize>,
}

impl<E: CompletionMenuEditor> CompletionMenuDelegate<E> {
    fn set_items(&mut self, items: Vec<CompletionItem>) {
        self.items = items.into_iter().map(Rc::new).collect();
        self.refilter();
    }

    fn refilter(&mut self) {
        self.filtered.clear();

        let query = self.query.as_ref();

        for (i, item) in self.items.iter().enumerate() {
            // Try matching against the label first, then filter_text
            let candidate = item.filter_text.as_deref().unwrap_or(&item.label);
            if let Some(m) = self.matcher.fuzzy_match(query, candidate) {
                if m.is_match() {
                    // If we matched on filter_text but want to highlight the label,
                    // re-match on the label for display purposes
                    let display_indices = if item.filter_text.is_some() {
                        self.matcher
                            .fuzzy_match(query, &item.label)
                            .filter(|m| m.is_match())
                            .map(|m| m.matched_indices)
                            .unwrap_or_default()
                    } else {
                        m.matched_indices
                    };

                    self.filtered.push(FilteredEntry {
                        source_index: i,
                        matched_indices: display_indices,
                    });
                }
            }
        }

        // Sort: by sort_text (LSP-provided priority), then by match quality
        self.filtered.sort_by(|a, b| {
            let item_a = &self.items[a.source_index];
            let item_b = &self.items[b.source_index];

            let priority_a = item_a.sort_text.as_deref().unwrap_or("9");
            let priority_b = item_b.sort_text.as_deref().unwrap_or("9");

            priority_a.cmp(priority_b)
        });

        if self.selected_ix >= self.filtered.len() {
            self.selected_ix = 0;
        }
    }

    fn selected_item(&self) -> Option<&Rc<CompletionItem>> {
        self.filtered
            .get(self.selected_ix)
            .and_then(|entry| self.items.get(entry.source_index))
    }

    fn filtered_count(&self) -> usize {
        self.filtered.len()
    }
}

impl<E: CompletionMenuEditor> EventEmitter<DismissEvent> for CompletionMenuDelegate<E> {}

impl<E: CompletionMenuEditor> ListDelegate for CompletionMenuDelegate<E> {
    type Item = CompletionMenuItem;

    fn items_count(&self, _: usize, _: &App) -> usize {
        self.filtered.len()
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _: &mut Window,
        _: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let entry = self.filtered.get(ix.row)?;
        let item = self.items.get(entry.source_index)?;
        Some(CompletionMenuItem::new(ix.row, item.clone(), entry.matched_indices.clone()))
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_ix = ix.map(|i| i.row).unwrap_or(0);
        cx.notify();
    }

    fn confirm(&mut self, _: bool, window: &mut Window, cx: &mut Context<ListState<Self>>) {
        let Some(item) = self.selected_item().cloned() else {
            return;
        };

        self.menu.update(cx, |menu, cx| {
            menu.apply_completion(&item, window, cx);
        });
    }
}

// ---------------------------------------------------------------------------
// CompletionMenu<E>
// ---------------------------------------------------------------------------

pub struct CompletionMenu<E: CompletionMenuEditor> {
    offset: usize,
    editor: Entity<E>,
    list: Entity<ListState<CompletionMenuDelegate<E>>>,
    open: bool,
    pub(crate) trigger_start_offset: Option<usize>,
    query: SharedString,
    _subscriptions: Vec<Subscription>,
    doc_text_view_state: Option<Entity<TextViewState>>,
    last_doc_content: Option<String>,
}

impl<E: CompletionMenuEditor> CompletionMenu<E> {
    pub fn new(
        editor: Entity<E>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let view = cx.entity();
            let delegate = CompletionMenuDelegate {
                query: SharedString::default(),
                menu: view,
                items: vec![],
                filtered: vec![],
                selected_ix: 0,
                matcher: FuzzyMatcher::default(),
            };

            let list = cx.new(|cx| ListState::new(delegate, window, cx));

            let _subscriptions =
                vec![cx.subscribe(&list, |this: &mut Self, _, ev: &ListEvent, cx| {
                    if let ListEvent::Confirm(_) = ev {
                        this.hide(cx);
                    }
                    cx.notify();
                })];

            Self {
                offset: 0,
                editor,
                list,
                open: false,
                trigger_start_offset: None,
                query: SharedString::default(),
                _subscriptions,
                doc_text_view_state: None,
                last_doc_content: None,
            }
        })
    }

    /// Apply a selected completion item: replace the text range and refocus.
    fn apply_completion(
        &mut self,
        item: &CompletionItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let trigger_start = self.trigger_start_offset.unwrap_or(self.offset);
        let offset = self.offset;
        let item = item.clone();
        let editor = self.editor.clone();

        cx.spawn_in(window, async move |_, cx| {
            editor.update_in(cx, |editor, window, cx| {
                let text = editor.completion_text_string(cx);
                let cursor_offset = editor.completion_cursor_offset(cx);

                let mut new_text =
                    item.insert_text.clone().unwrap_or_else(|| item.label.clone());
                let mut start_byte = trigger_start;
                let mut end_byte = offset;

                if let Some(text_edit) = item.text_edit.as_ref() {
                    match text_edit {
                        CompletionTextEdit::Edit(edit) => {
                            new_text = edit.new_text.clone();
                            start_byte = position_to_byte_offset(&text, &edit.range.start);
                            end_byte = position_to_byte_offset(&text, &edit.range.end);
                        }
                        CompletionTextEdit::InsertAndReplace(edit) => {
                            new_text = edit.new_text.clone();
                            start_byte =
                                position_to_byte_offset(&text, &edit.replace.start);
                            end_byte = position_to_byte_offset(&text, &edit.replace.end);
                        }
                    }
                }

                // Strip trailing space if the next char is already whitespace
                if new_text.ends_with(' ') && cursor_offset < text.len() {
                    if text[cursor_offset..]
                        .chars()
                        .next()
                        .map(|c| c.is_whitespace())
                        .unwrap_or(false)
                    {
                        new_text = new_text.trim_end().to_string();
                    }
                }

                let start_byte = start_byte.min(text.len());
                let end_byte = end_byte.min(text.len());

                editor.completion_replace_text_in_range(
                    start_byte..end_byte,
                    &new_text,
                    window,
                    cx,
                );
                editor.completion_focus_editor(window, cx);
            })
            .ok();
        })
        .detach();

        self.hide(cx);
    }

    // -----------------------------------------------------------------------
    // Action dispatch (keyboard navigation)
    // -----------------------------------------------------------------------

    pub(crate) fn handle_action(
        &mut self,
        action: Box<dyn Action>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.open {
            return false;
        }

        cx.propagate();
        if action.partial_eq(&input::Enter { secondary: false })
            || action.partial_eq(&input::IndentInline)
        {
            self.on_action_enter(window, cx);
        } else if action.partial_eq(&input::Escape) {
            self.on_action_escape(window, cx);
        } else if action.partial_eq(&input::MoveUp) {
            self.on_action_up(window, cx);
        } else if action.partial_eq(&input::MoveDown) {
            self.on_action_down(window, cx);
        } else {
            return false;
        }

        true
    }

    fn on_action_enter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(item) = self.list.read(cx).delegate().selected_item().cloned() else {
            return;
        };
        self.apply_completion(&item, window, cx);
    }

    fn on_action_escape(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        self.hide(cx);
    }

    fn on_action_up(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.list.update(cx, |this, cx| {
            this.on_action_select_prev(&actions::SelectUp, window, cx)
        });
    }

    fn on_action_down(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.list.update(cx, |this, cx| {
            this.on_action_select_next(&actions::SelectDown, window, cx)
        });
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    pub fn select_prev(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let current = self.list.read(cx).delegate().selected_ix;
        if current > 0 {
            let new_ix = current - 1;
            self.list.update(cx, |this, cx| {
                this.set_selected_index(Some(IndexPath::new(new_ix)), window, cx);
            });
        }
    }

    pub fn select_next(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let current = self.list.read(cx).delegate().selected_ix;
        let count = self.list.read(cx).delegate().filtered_count();
        if current + 1 < count {
            let new_ix = current + 1;
            self.list.update(cx, |this, cx| {
                this.set_selected_index(Some(IndexPath::new(new_ix)), window, cx);
            });
        }
    }

    pub fn confirm_selection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(item) = self.list.read(cx).delegate().selected_item().cloned() else {
            return;
        };
        self.apply_completion(&item, window, cx);
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn hide(&mut self, cx: &mut Context<Self>) {
        self.open = false;
        self.trigger_start_offset = None;
        self.doc_text_view_state = None;
        self.last_doc_content = None;
        cx.notify();
    }

    /// Sets or updates the trigger start offset and the current query string.
    pub fn update_query(&mut self, start_offset: usize, query: impl Into<SharedString>) {
        let should_update = match self.trigger_start_offset {
            None => true,
            Some(existing) if start_offset > existing => true,
            Some(_) => false,
        };

        if should_update {
            self.trigger_start_offset = Some(start_offset);
        }

        self.query = query.into();
    }

    /// Re-runs the fuzzy filter on the existing items with the current query.
    /// Hides the menu if no items match. Does not re-fetch items from the LSP.
    pub fn refilter(
        &mut self,
        offset: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.offset = offset;
        self.list.update(cx, |this, cx| {
            this.delegate_mut().query = self.query.clone();
            this.delegate_mut().refilter();
            this.set_selected_index(Some(IndexPath::new(0)), window, cx);
        });

        if self.list.read(cx).delegate().filtered_count() == 0 {
            self.hide(cx);
        } else {
            cx.notify();
        }
    }

    pub fn show(
        &mut self,
        offset: usize,
        items: impl Into<Vec<CompletionItem>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let items = items.into();
        self.offset = offset;
        self.open = true;
        self.list.update(cx, |this, cx| {
            let longest_ix = items
                .iter()
                .enumerate()
                .max_by_key(|(_, item)| {
                    item.label.len() + item.detail.as_ref().map(|d| d.len()).unwrap_or(0)
                })
                .map(|(ix, _)| ix)
                .unwrap_or(0);

            this.delegate_mut().query = self.query.clone();
            this.delegate_mut().set_items(items);
            this.set_selected_index(Some(IndexPath::new(0)), window, cx);
            this.set_item_to_measure_index(IndexPath::new(longest_ix), window, cx);
        });

        cx.notify();
    }

    fn origin(&self, cx: &App) -> Option<Point<Pixels>> {
        self.editor.read(cx).completion_origin(cx)
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl<E: CompletionMenuEditor> Render for CompletionMenu<E> {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.open {
            return Empty.into_any_element();
        }

        if self.list.read(cx).delegate().filtered_count() == 0 {
            self.open = false;
            return Empty.into_any_element();
        }

        let Some(pos) = self.origin(cx) else {
            return Empty.into_any_element();
        };

        // Documentation panel
        let selected_documentation = self
            .list
            .read(cx)
            .delegate()
            .selected_item()
            .and_then(|item| item.documentation.clone());

        let full_doc_content: Option<String> =
            selected_documentation
                .as_ref()
                .map(|documentation| match documentation {
                    lsp_types::Documentation::String(s) => s.clone(),
                    lsp_types::Documentation::MarkupContent(mc) => mc.value.clone(),
                });

        if full_doc_content != self.last_doc_content {
            self.last_doc_content = full_doc_content.clone();
            self.doc_text_view_state = full_doc_content
                .as_ref()
                .map(|doc| cx.new(|cx: &mut Context<TextViewState>| TextViewState::markdown(doc, cx)));
        }

        let doc_state = self.doc_text_view_state.clone();
        let abs_pos_x = pos.x + MAX_MENU_WIDTH + POPOVER_GAP + MAX_MENU_WIDTH + POPOVER_GAP;
        let vertical_layout = abs_pos_x > window.bounds().size.width;

        deferred(
            div()
                .absolute()
                .left(pos.x)
                .top(pos.y)
                .flex()
                .flex_row()
                .gap(POPOVER_GAP)
                .items_start()
                .when(vertical_layout, |this| this.flex_col())
                .child(
                    editor_popover("completion-menu", cx)
                        .w(MAX_MENU_WIDTH)
                        .min_w(px(200.))
                        .child(List::new(&self.list).max_h(MAX_MENU_HEIGHT)),
                )
                .when_some(doc_state, |this, state| {
                    this.child(
                        div().child(
                            editor_popover("completion-menu-doc", cx)
                                .w(MAX_MENU_WIDTH)
                                .max_h(if vertical_layout {
                                    px(60.)
                                } else {
                                    MAX_MENU_HEIGHT
                                })
                                .overflow_hidden()
                                .px_2()
                                .child(TextView::new(&state)),
                        ),
                    )
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.hide(cx);
                })),
        )
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn position_to_byte_offset(text: &str, position: &lsp_types::Position) -> usize {
    let mut offset = 0;
    let mut line = 0;

    for ch in text.chars() {
        if line >= position.line {
            break;
        }
        if ch == '\n' {
            line += 1;
        }
        offset += ch.len_utf8();
    }

    offset + position.character as usize
}
