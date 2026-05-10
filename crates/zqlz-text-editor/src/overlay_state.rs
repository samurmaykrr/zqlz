use std::time::Duration;

use gpui::{Bounds, Entity, Pixels, Subscription, px};
use zqlz_ui::widgets::input::InputState;

use crate::{
    Anchor, AnchoredRange, Position, SelectionsCollection, TextDocument,
    input_state::CachedEditorLayout,
};

/// Completion menu geometry cached from last paint for hit-testing.
pub(crate) struct CachedCompletionMenuBounds {
    bounds: Bounds<Pixels>,
    item_height: Pixels,
    item_count: usize,
}

impl CachedCompletionMenuBounds {
    pub(crate) fn new(bounds: Bounds<Pixels>, item_height: Pixels, item_count: usize) -> Self {
        Self {
            bounds,
            item_height,
            item_count,
        }
    }

    pub(crate) fn slot_at(&self, point: gpui::Point<Pixels>) -> Option<usize> {
        if !self.bounds.contains(&point) {
            return None;
        }

        let relative_y = point.y - self.bounds.origin.y;
        let slot = (f32::from(relative_y) / f32::from(self.item_height)).floor() as usize;
        Some(slot.min(self.item_count.saturating_sub(1)))
    }
}

#[derive(Clone, Debug)]
pub struct GoToLineSnapshot {
    pub query: String,
    pub is_valid: bool,
    pub total_lines: usize,
}

#[derive(Clone, Debug)]
pub struct ContextMenuSnapshot {
    pub items: Vec<(String, bool, bool)>,
    pub origin_x: f32,
    pub origin_y: f32,
    pub highlighted: Option<usize>,
}

/// Result of hit-testing a pointer position against the context menu.
pub(crate) struct ContextMenuHit {
    /// Whether the pointer is inside the menu bounds.
    inside_menu: bool,
    /// Item index under the pointer, if any non-separator row is under pointer.
    item_index: Option<usize>,
    /// Whether the pointed item is actionable.
    actionable: bool,
}

impl ContextMenuHit {
    fn new(inside_menu: bool, item_index: Option<usize>, actionable: bool) -> Self {
        Self {
            inside_menu,
            item_index,
            actionable,
        }
    }

    pub(crate) fn inside_menu(&self) -> bool {
        self.inside_menu
    }

    pub(crate) fn item_index(&self) -> Option<usize> {
        self.item_index
    }

    pub(crate) fn actionable(&self) -> bool {
        self.actionable
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedGoToLineQuery {
    is_valid: bool,
    line: Option<usize>,
    column: Option<usize>,
}

impl ParsedGoToLineQuery {
    fn new(is_valid: bool, line: Option<usize>, column: Option<usize>) -> Self {
        Self {
            is_valid,
            line,
            column,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.is_valid
    }

    pub(crate) fn line(&self) -> Option<usize> {
        self.line
    }

    pub(crate) fn column(&self) -> Option<usize> {
        self.column
    }
}

/// State for the go-to-line dialog overlay.
pub(crate) struct GoToLineState {
    query: String,
    original_cursor: Position,
    is_valid: bool,
}

/// State for the inline rename dialog.
pub(crate) struct RenameState {
    input: Entity<InputState>,
    word_range: AnchoredRange,
    original_word: String,
    original_cursor: Position,
    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineSuggestion {
    pub text: String,
    pub anchor: Anchor,
}

impl InlineSuggestion {
    pub(crate) fn new(text: String, anchor: Anchor) -> Self {
        Self { text, anchor }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContextMenuAction {
    Cut,
    Copy,
    Paste,
    SelectAll,
    Find,
    FindReplace,
    SelectAllMatches,
    Complete,
    GoToDefinition,
    FindReferences,
    RenameSymbol,
    GoToLine,
    ToggleSoftWrap,
    FoldAll,
    UnfoldAll,
    DuplicateLineDown,
    MoveLineUp,
    MoveLineDown,
    ToggleLineComment,
    SortLinesAscending,
    UniqueLines,
    TransformUppercase,
    TransformLowercase,
    TransformTitleCase,
    TransformSnakeCase,
    TransformCamelCase,
    TransformKebabCase,
    InsertUuidV4,
    InsertUuidV7,
    FormatSql,
}

/// A single entry in the right-click context menu.
#[derive(Clone)]
pub(crate) struct ContextMenuItem {
    label: String,
    disabled: bool,
    is_separator: bool,
    action: Option<ContextMenuAction>,
}

impl ContextMenuItem {
    pub(crate) fn action(
        label: impl Into<String>,
        action: ContextMenuAction,
        disabled: bool,
    ) -> Self {
        Self {
            label: label.into(),
            disabled,
            is_separator: false,
            action: Some(action),
        }
    }

    pub(crate) fn separator() -> Self {
        Self {
            label: String::new(),
            disabled: false,
            is_separator: true,
            action: None,
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn disabled(&self) -> bool {
        self.disabled
    }

    pub(crate) fn is_separator(&self) -> bool {
        self.is_separator
    }

    pub(crate) fn action_kind(&self) -> Option<ContextMenuAction> {
        self.action
    }
}

/// State for the right-click context menu overlay.
pub(crate) struct ContextMenuState {
    items: Vec<ContextMenuItem>,
    origin_x: f32,
    origin_y: f32,
    highlighted: Option<usize>,
}

impl ContextMenuState {
    pub(crate) const MENU_WIDTH: Pixels = px(260.0);
    const MENU_PADDING_Y: Pixels = px(8.0);
    const MENU_MARGIN: Pixels = px(8.0);

    pub(crate) fn new(
        items: Vec<ContextMenuItem>,
        origin_x: f32,
        origin_y: f32,
        highlighted: Option<usize>,
    ) -> Self {
        Self {
            items,
            origin_x,
            origin_y,
            highlighted,
        }
    }

    pub(crate) fn total_height(&self, line_height: Pixels) -> Pixels {
        let item_height = Self::item_height(line_height);
        self.items
            .iter()
            .fold(Self::MENU_PADDING_Y, |height, item| {
                height
                    + if item.is_separator {
                        px(8.0)
                    } else {
                        item_height
                    }
            })
    }

    pub(crate) fn item_height(line_height: Pixels) -> Pixels {
        line_height.clamp(px(18.0), px(24.0)) * 1.35
    }

    pub(crate) fn clamped_origin(
        &self,
        bounds_origin: gpui::Point<Pixels>,
        bounds_size: gpui::Size<Pixels>,
        line_height: Pixels,
    ) -> gpui::Point<Pixels> {
        let max_x = (bounds_size.width - Self::MENU_WIDTH - Self::MENU_MARGIN).max(px(0.0));
        let max_y =
            (bounds_size.height - self.total_height(line_height) - Self::MENU_MARGIN).max(px(0.0));
        let min_x = Self::MENU_MARGIN.min(max_x);
        let min_y = Self::MENU_MARGIN.min(max_y);

        gpui::point(
            bounds_origin.x + px(self.origin_x).clamp(min_x, max_x),
            bounds_origin.y + px(self.origin_y).clamp(min_y, max_y),
        )
    }

    pub(crate) fn origin_x(&self) -> f32 {
        self.origin_x
    }

    pub(crate) fn origin_y(&self) -> f32 {
        self.origin_y
    }

    pub(crate) fn highlighted(&self) -> Option<usize> {
        self.highlighted
    }

    pub(crate) fn bounds(
        &self,
        bounds_origin: gpui::Point<Pixels>,
        bounds_size: gpui::Size<Pixels>,
        line_height: Pixels,
    ) -> gpui::Bounds<Pixels> {
        gpui::Bounds::new(
            self.clamped_origin(bounds_origin, bounds_size, line_height),
            gpui::size(Self::MENU_WIDTH, self.total_height(line_height)),
        )
    }
}

impl GoToLineState {
    pub(crate) fn new(original_cursor: Position) -> Self {
        Self {
            query: String::new(),
            original_cursor,
            is_valid: true,
        }
    }

    pub(crate) fn original_cursor(&self) -> Position {
        self.original_cursor
    }
}

impl RenameState {
    pub(crate) fn new(
        input: Entity<InputState>,
        word_range: AnchoredRange,
        original_word: String,
        original_cursor: Position,
        subscriptions: Vec<Subscription>,
    ) -> Self {
        Self {
            input,
            word_range,
            original_word,
            original_cursor,
            _subscriptions: subscriptions,
        }
    }

    pub(crate) fn current_input_text(&self, cx: &gpui::App) -> String {
        self.input.read(cx).value().to_string()
    }

    pub(crate) fn input_entity(&self) -> &Entity<InputState> {
        &self.input
    }

    pub(crate) fn is_noop_replacement(&self, new_name: &str) -> bool {
        new_name.is_empty() || new_name == self.original_word
    }

    pub(crate) fn resolved_word_range(
        &self,
        document: &TextDocument,
    ) -> Option<std::ops::Range<usize>> {
        document.resolve_anchored_range(self.word_range).ok()
    }

    pub(crate) fn origin_position(&self, document: &TextDocument) -> Position {
        document.clamp_position(
            document
                .resolve_anchor_position(self.word_range.start)
                .unwrap_or(self.original_cursor),
        )
    }

    pub(crate) fn restore_cursor_position(&self, document: &TextDocument) -> Position {
        document.clamp_position(self.original_cursor)
    }
}

/// State for an explicit signature-help overlay.
#[derive(Clone, Debug)]
pub struct SignatureHelpState {
    pub content: String,
    pub anchor: Anchor,
}

impl SignatureHelpState {
    pub(crate) fn new(content: String, anchor: Anchor) -> Self {
        Self { content, anchor }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditPrediction {
    pub text: String,
    pub anchor: Anchor,
}

impl EditPrediction {
    pub(crate) fn new(text: String, anchor: Anchor) -> Self {
        Self { text, anchor }
    }
}

pub(crate) struct EditorOverlayState {
    signature_help: Option<SignatureHelpState>,
    completion_menu_bounds: Option<CachedCompletionMenuBounds>,
    inline_suggestion: Option<InlineSuggestion>,
    edit_prediction: Option<EditPrediction>,
    goto_line: Option<GoToLineState>,
    rename: Option<RenameState>,
    context_menu: Option<ContextMenuState>,
    hover_delay: Duration,
    hover_pending_position: Option<Position>,
    hover_ready_position: Option<Position>,
}

impl Default for EditorOverlayState {
    fn default() -> Self {
        Self {
            signature_help: None,
            completion_menu_bounds: None,
            inline_suggestion: None,
            edit_prediction: None,
            goto_line: None,
            rename: None,
            context_menu: None,
            hover_delay: Duration::from_millis(500),
            hover_pending_position: None,
            hover_ready_position: None,
        }
    }
}

impl EditorOverlayState {
    pub(crate) fn clear_document_bound_overlays(&mut self) {
        self.inline_suggestion = None;
        self.edit_prediction = None;
        self.rename = None;
        self.context_menu = None;
        self.signature_help = None;
    }

    pub(crate) fn retain_primary_cursor_anchored_overlays(
        &mut self,
        document: &TextDocument,
        selections: &SelectionsCollection,
    ) {
        self.retain_inline_suggestion(|suggestion| {
            Self::anchor_matches_primary_cursor(document, selections, suggestion.anchor)
        });
        self.retain_edit_prediction(|prediction| {
            Self::anchor_matches_primary_cursor(document, selections, prediction.anchor)
        });
    }

    pub(crate) fn retain_valid_document_anchored_overlays(&mut self, document: &TextDocument) {
        self.retain_inline_suggestion(|suggestion| {
            document.resolve_anchor_offset(suggestion.anchor).is_ok()
        });
        self.retain_edit_prediction(|prediction| {
            document.resolve_anchor_offset(prediction.anchor).is_ok()
        });
    }

    pub(crate) fn clear_cursor_bound_overlays(&mut self) {
        self.inline_suggestion = None;
        self.edit_prediction = None;
    }

    fn anchor_matches_primary_cursor(
        document: &TextDocument,
        selections: &SelectionsCollection,
        anchor: Anchor,
    ) -> bool {
        if !selections.extra_entries().is_empty() {
            return false;
        }

        let Some(primary) = selections.primary() else {
            return false;
        };

        document
            .resolve_anchor_offset(anchor)
            .ok()
            .is_some_and(|offset| {
                document
                    .position_to_offset(primary.cursor.position())
                    .is_ok_and(|cursor_offset| offset == cursor_offset)
            })
    }

    pub(crate) fn context_menu_snapshot(&self) -> Option<ContextMenuSnapshot> {
        self.context_menu.as_ref().map(|state| ContextMenuSnapshot {
            items: state
                .items
                .iter()
                .map(|item| {
                    (
                        item.label().to_string(),
                        item.is_separator(),
                        item.disabled(),
                    )
                })
                .collect(),
            origin_x: state.origin_x(),
            origin_y: state.origin_y(),
            highlighted: state.highlighted(),
        })
    }

    pub(crate) fn goto_line_snapshot(&self, total_lines: usize) -> Option<GoToLineSnapshot> {
        self.goto_line.as_ref().map(|state| GoToLineSnapshot {
            query: state.query.clone(),
            is_valid: state.is_valid,
            total_lines,
        })
    }

    pub(crate) fn open_goto_line(&mut self, original_cursor: Position) {
        self.goto_line = Some(GoToLineState::new(original_cursor));
    }

    pub(crate) fn has_goto_line(&self) -> bool {
        self.goto_line.is_some()
    }

    pub(crate) fn update_goto_line_query(
        &mut self,
        character: char,
        line_count: usize,
    ) -> Option<ParsedGoToLineQuery> {
        let state = self.goto_line.as_mut()?;
        if character.is_ascii_digit() || character == ':' {
            state.query.push(character);
        } else if character == '\x08' {
            state.query.pop();
        } else {
            return None;
        }

        let parsed = parse_goto_line_query(&state.query, line_count);
        state.is_valid = parsed.is_valid();
        Some(parsed)
    }

    pub(crate) fn take_goto_line(&mut self) -> Option<GoToLineState> {
        self.goto_line.take()
    }

    pub(crate) fn clear_goto_line(&mut self) -> bool {
        self.goto_line.take().is_some()
    }

    pub(crate) fn update_completion_menu_bounds(
        &mut self,
        bounds: Option<CachedCompletionMenuBounds>,
    ) {
        self.completion_menu_bounds = bounds;
    }

    pub(crate) fn completion_menu_bounds(&self) -> Option<&CachedCompletionMenuBounds> {
        self.completion_menu_bounds.as_ref()
    }

    pub(crate) fn set_rename(&mut self, rename: RenameState) {
        self.rename = Some(rename);
    }

    pub(crate) fn rename(&self) -> Option<&RenameState> {
        self.rename.as_ref()
    }

    pub(crate) fn has_rename(&self) -> bool {
        self.rename.is_some()
    }

    pub(crate) fn take_rename(&mut self) -> Option<RenameState> {
        self.rename.take()
    }

    pub(crate) fn clear_rename(&mut self) -> bool {
        self.rename.take().is_some()
    }

    pub(crate) fn open_context_menu(
        &mut self,
        items: Vec<ContextMenuItem>,
        origin_x: f32,
        origin_y: f32,
        highlighted: Option<usize>,
    ) {
        self.context_menu = Some(ContextMenuState::new(
            items,
            origin_x,
            origin_y,
            highlighted,
        ));
    }

    pub(crate) fn take_context_menu(&mut self) -> Option<ContextMenuState> {
        self.context_menu.take()
    }

    pub(crate) fn take_context_menu_highlighted_action(&mut self) -> Option<ContextMenuAction> {
        let state = self.context_menu.take()?;
        let item = state
            .highlighted
            .and_then(|highlighted| state.items.get(highlighted))?;
        if item.disabled || item.is_separator {
            return None;
        }
        item.action_kind()
    }

    pub(crate) fn has_context_menu(&self) -> bool {
        self.context_menu.is_some()
    }

    pub(crate) fn clear_context_menu(&mut self) -> bool {
        self.context_menu.take().is_some()
    }

    pub(crate) fn set_context_menu_highlight(&mut self, highlighted: Option<usize>) -> bool {
        let Some(state) = self.context_menu.as_mut() else {
            return false;
        };
        if state.highlighted == highlighted {
            return false;
        }
        state.highlighted = highlighted;
        true
    }

    pub(crate) fn move_context_menu_highlight(&mut self, delta: i32) -> bool {
        self.context_menu
            .as_mut()
            .is_some_and(|state| state.move_highlight(delta))
    }

    pub(crate) fn context_menu_item_is_enabled(&self, index: usize) -> bool {
        self.context_menu
            .as_ref()
            .and_then(|state| state.items.get(index))
            .is_some_and(|item| !item.disabled)
    }

    pub(crate) fn set_context_menu_highlighted_enabled_item(
        &mut self,
        index: Option<usize>,
    ) -> bool {
        let highlighted = index.filter(|index| self.context_menu_item_is_enabled(*index));
        self.set_context_menu_highlight(highlighted)
    }

    pub(crate) fn context_menu_hit_test(
        &self,
        pointer: gpui::Point<gpui::Pixels>,
        layout: &CachedEditorLayout,
        line_height: gpui::Pixels,
    ) -> Option<ContextMenuHit> {
        let state = self.context_menu.as_ref()?;
        let item_height = ContextMenuState::item_height(line_height);
        let menu_bounds = state.bounds(layout.bounds_origin(), layout.bounds_size(), line_height);
        if !menu_bounds.contains(&pointer) {
            return Some(ContextMenuHit::new(false, None, false));
        }

        let mut row_y = menu_bounds.origin.y + gpui::px(4.0);
        for (index, item) in state.items.iter().enumerate() {
            if item.is_separator {
                row_y += gpui::px(8.0);
                continue;
            }

            let row_bounds = gpui::Bounds::new(
                gpui::point(menu_bounds.origin.x, row_y),
                gpui::size(ContextMenuState::MENU_WIDTH, item_height),
            );
            if row_bounds.contains(&pointer) {
                return Some(ContextMenuHit::new(true, Some(index), !item.disabled));
            }

            row_y += item_height;
        }

        Some(ContextMenuHit::new(true, None, false))
    }

    pub(crate) fn set_signature_help(&mut self, signature_help: SignatureHelpState) {
        self.signature_help = Some(signature_help);
    }

    pub(crate) fn signature_help(&self) -> Option<&SignatureHelpState> {
        self.signature_help.as_ref()
    }

    pub(crate) fn has_signature_help(&self) -> bool {
        self.signature_help.is_some()
    }

    pub(crate) fn clear_signature_help(&mut self) -> bool {
        self.signature_help.take().is_some()
    }

    pub(crate) fn set_inline_suggestion(&mut self, inline_suggestion: InlineSuggestion) {
        self.inline_suggestion = Some(inline_suggestion);
    }

    pub(crate) fn inline_suggestion(&self) -> Option<&InlineSuggestion> {
        self.inline_suggestion.as_ref()
    }

    pub(crate) fn has_inline_suggestion(&self) -> bool {
        self.inline_suggestion.is_some()
    }

    pub(crate) fn retain_inline_suggestion(
        &mut self,
        keep: impl FnOnce(&InlineSuggestion) -> bool,
    ) {
        self.inline_suggestion = self.inline_suggestion.take().filter(keep);
    }

    pub(crate) fn take_inline_suggestion(&mut self) -> Option<InlineSuggestion> {
        self.inline_suggestion.take()
    }

    pub(crate) fn clear_inline_suggestion(&mut self) -> bool {
        self.inline_suggestion.take().is_some()
    }

    pub(crate) fn set_edit_prediction(&mut self, edit_prediction: EditPrediction) {
        self.edit_prediction = Some(edit_prediction);
    }

    pub(crate) fn edit_prediction(&self) -> Option<&EditPrediction> {
        self.edit_prediction.as_ref()
    }

    pub(crate) fn retain_edit_prediction(&mut self, keep: impl FnOnce(&EditPrediction) -> bool) {
        self.edit_prediction = self.edit_prediction.take().filter(keep);
    }

    pub(crate) fn take_edit_prediction(&mut self) -> Option<EditPrediction> {
        self.edit_prediction.take()
    }

    pub(crate) fn clear_edit_prediction(&mut self) -> bool {
        self.edit_prediction.take().is_some()
    }

    pub(crate) fn set_hover_delay(&mut self, delay: Duration) {
        self.hover_delay = delay;
    }

    pub(crate) fn begin_hover_timer(&mut self, position: Position) -> Option<Duration> {
        if self.hover_delay.is_zero() {
            self.hover_ready_position = Some(position);
            return None;
        }

        if self.hover_pending_position == Some(position) {
            return None;
        }

        self.hover_pending_position = Some(position);
        Some(self.hover_delay)
    }

    pub(crate) fn mark_hover_ready_if_pending(&mut self, position: Position) -> bool {
        if self.hover_pending_position != Some(position) {
            return false;
        }

        self.hover_ready_position = Some(position);
        true
    }

    pub(crate) fn take_ready_hover_position(&mut self) -> Option<Position> {
        let position = self.hover_ready_position.take()?;
        self.hover_pending_position = None;
        Some(position)
    }

    pub(crate) fn clear_hover_positions(&mut self) {
        self.hover_pending_position = None;
        self.hover_ready_position = None;
    }
}

pub(crate) fn parse_goto_line_query(query: &str, line_count: usize) -> ParsedGoToLineQuery {
    if query.is_empty() {
        return ParsedGoToLineQuery::new(true, None, None);
    }

    let mut parts = query.split(':');
    let line_text = parts.next().unwrap_or_default();
    let column_text = parts.next();
    if parts.next().is_some() || line_text.is_empty() {
        return ParsedGoToLineQuery::new(false, None, None);
    }

    let Ok(line_number) = line_text.parse::<usize>() else {
        return ParsedGoToLineQuery::new(false, None, None);
    };
    if line_number == 0 || line_number > line_count {
        return ParsedGoToLineQuery::new(false, None, None);
    }

    let column = match column_text {
        Some("") => None,
        Some(text) => match text.parse::<usize>() {
            Ok(0) | Err(_) => {
                return ParsedGoToLineQuery::new(false, Some(line_number - 1), None);
            }
            Ok(column_number) => Some(column_number - 1),
        },
        None => None,
    };

    ParsedGoToLineQuery::new(true, Some(line_number - 1), column)
}

impl ContextMenuState {
    pub(crate) fn first_actionable_index(items: &[ContextMenuItem]) -> Option<usize> {
        items
            .iter()
            .enumerate()
            .find_map(|(index, item)| (!item.is_separator && !item.disabled).then_some(index))
    }

    pub(crate) fn move_highlight(&mut self, delta: i32) -> bool {
        let actionable_indexes: Vec<usize> = self
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| !item.is_separator && !item.disabled)
            .map(|(index, _)| index)
            .collect();
        if actionable_indexes.is_empty() {
            return false;
        }

        let current_position = self.highlighted.and_then(|highlighted| {
            actionable_indexes
                .iter()
                .position(|&index| index == highlighted)
        });
        let next_position = match current_position {
            None => {
                if delta >= 0 {
                    0
                } else {
                    actionable_indexes.len() - 1
                }
            }
            Some(position) => {
                let len = actionable_indexes.len() as i32;
                ((position as i32 + delta).rem_euclid(len)) as usize
            }
        };
        self.highlighted = Some(actionable_indexes[next_position]);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{EditorOverlayState, GoToLineState, ParsedGoToLineQuery, parse_goto_line_query};
    use crate::{ContextMenuItem, ContextMenuState, Position, input_state::CachedEditorLayout};
    use gpui::{point, px, size};
    use std::time::Duration;

    #[test]
    fn default_overlay_state_uses_hover_delay_and_no_overlays() {
        let state = EditorOverlayState::default();

        assert_eq!(state.hover_delay, Duration::from_millis(500));
        assert!(state.context_menu_snapshot().is_none());
        assert!(state.goto_line_snapshot(1).is_none());
    }

    #[test]
    fn overlay_snapshots_preserve_public_contracts() {
        let state = EditorOverlayState {
            context_menu: Some(ContextMenuState::new(
                vec![ContextMenuItem::action(
                    "Copy",
                    super::ContextMenuAction::Copy,
                    false,
                )],
                12.0,
                24.0,
                Some(0),
            )),
            goto_line: Some(GoToLineState {
                query: "42".into(),
                ..GoToLineState::new(Position::zero())
            }),
            ..Default::default()
        };

        let menu = state
            .context_menu_snapshot()
            .expect("context menu snapshot");
        assert_eq!(menu.items, vec![("Copy".to_string(), false, false)]);
        assert_eq!(menu.origin_x, 12.0);
        assert_eq!(menu.origin_y, 24.0);
        assert_eq!(menu.highlighted, Some(0));

        let goto_line = state.goto_line_snapshot(100).expect("goto line snapshot");
        assert_eq!(goto_line.query, "42");
        assert!(goto_line.is_valid);
        assert_eq!(goto_line.total_lines, 100);
    }

    #[test]
    fn clear_document_bound_overlays_keeps_hover_and_goto_state() {
        let mut state = EditorOverlayState {
            context_menu: Some(ContextMenuState::new(Vec::new(), 0.0, 0.0, None)),
            goto_line: Some(GoToLineState {
                query: "7".into(),
                ..GoToLineState::new(Position::zero())
            }),
            hover_pending_position: Some(Position::new(1, 2)),
            ..Default::default()
        };

        state.clear_document_bound_overlays();

        assert!(state.context_menu.is_none());
        assert!(state.goto_line.is_some());
        assert_eq!(state.hover_pending_position, Some(Position::new(1, 2)));
    }

    #[test]
    fn context_menu_helpers_open_take_and_skip_disabled_items() {
        let mut state = EditorOverlayState::default();
        let items = vec![
            ContextMenuItem::action("Cut", super::ContextMenuAction::Cut, true),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Copy", super::ContextMenuAction::Copy, false),
        ];

        let highlighted = ContextMenuState::first_actionable_index(&items);
        state.open_context_menu(items, 10.0, 20.0, highlighted);

        let mut menu = state.take_context_menu().expect("context menu");
        assert!(state.context_menu.is_none());
        assert_eq!(menu.origin_x(), 10.0);
        assert_eq!(menu.origin_y(), 20.0);
        assert_eq!(menu.highlighted(), Some(2));

        assert!(menu.move_highlight(1));
        assert_eq!(menu.highlighted(), Some(2));

        state.open_context_menu(
            vec![ContextMenuItem::action(
                "Copy",
                super::ContextMenuAction::Copy,
                false,
            )],
            10.0,
            20.0,
            Some(0),
        );
        assert_eq!(
            state.take_context_menu_highlighted_action(),
            Some(super::ContextMenuAction::Copy)
        );
        assert!(state.context_menu.is_none());
    }

    #[test]
    fn context_menu_highlight_helpers_report_changes() {
        let mut state = EditorOverlayState::default();
        state.open_context_menu(
            vec![ContextMenuItem::action(
                "Copy",
                super::ContextMenuAction::Copy,
                false,
            )],
            0.0,
            0.0,
            None,
        );

        assert!(state.has_context_menu());
        assert!(state.context_menu_item_is_enabled(0));
        assert!(state.set_context_menu_highlight(Some(0)));
        assert!(!state.set_context_menu_highlight(Some(0)));
        assert!(!state.set_context_menu_highlighted_enabled_item(Some(0)));
        assert!(state.set_context_menu_highlighted_enabled_item(None));
        assert!(state.move_context_menu_highlight(1));
        assert!(state.clear_context_menu());
        assert!(!state.clear_context_menu());
    }

    #[test]
    fn context_menu_hit_test_reports_enabled_disabled_and_outside_rows() {
        let mut state = EditorOverlayState::default();
        state.open_context_menu(
            vec![
                ContextMenuItem::action("Copy", super::ContextMenuAction::Copy, false),
                ContextMenuItem::action("Disabled", super::ContextMenuAction::Copy, true),
            ],
            10.0,
            20.0,
            None,
        );
        let layout = CachedEditorLayout::new(
            0.0,
            px(0.0),
            point(px(100.0), px(200.0)),
            size(px(500.0), px(400.0)),
            px(20.0),
            None,
        );

        let enabled_hit = state
            .context_menu_hit_test(point(px(120.0), px(232.0)), &layout, px(20.0))
            .expect("enabled hit");
        assert!(enabled_hit.inside_menu());
        assert_eq!(enabled_hit.item_index(), Some(0));
        assert!(enabled_hit.actionable());

        let disabled_hit = state
            .context_menu_hit_test(point(px(120.0), px(260.0)), &layout, px(20.0))
            .expect("disabled hit");
        assert!(disabled_hit.inside_menu());
        assert_eq!(disabled_hit.item_index(), Some(1));
        assert!(!disabled_hit.actionable());

        let outside_hit = state
            .context_menu_hit_test(point(px(99.0), px(199.0)), &layout, px(20.0))
            .expect("outside hit");
        assert!(!outside_hit.inside_menu());
        assert_eq!(outside_hit.item_index(), None);
    }

    #[test]
    fn overlay_option_helpers_report_whether_state_changed() {
        let mut state = EditorOverlayState::default();

        assert!(!state.clear_signature_help());
        assert!(!state.clear_inline_suggestion());
        assert!(!state.clear_edit_prediction());

        state.set_inline_suggestion(crate::InlineSuggestion::new(
            "select".into(),
            crate::Anchor::new(0, 0, crate::Bias::Right),
        ));
        state.set_edit_prediction(crate::EditPrediction::new(
            " from users".into(),
            crate::Anchor::new(0, 0, crate::Bias::Right),
        ));

        assert!(state.clear_inline_suggestion());
        assert!(state.clear_edit_prediction());
    }

    #[test]
    fn overlay_owner_helpers_track_goto_rename_and_completion_bounds() {
        let mut state = EditorOverlayState::default();

        state.open_goto_line(Position::new(4, 8));
        assert!(state.has_goto_line());
        assert_eq!(
            state
                .take_goto_line()
                .map(|goto_line| goto_line.original_cursor()),
            Some(Position::new(4, 8))
        );
        assert!(!state.has_goto_line());
        state.open_goto_line(Position::new(4, 8));
        assert!(state.clear_goto_line());
        assert!(!state.clear_goto_line());

        state.update_completion_menu_bounds(Some(super::CachedCompletionMenuBounds::new(
            gpui::Bounds::new(
                gpui::point(gpui::px(1.0), gpui::px(2.0)),
                gpui::size(gpui::px(3.0), gpui::px(4.0)),
            ),
            gpui::px(5.0),
            2,
        )));
        assert!(state.completion_menu_bounds().is_some());
        state.update_completion_menu_bounds(None);
        assert!(state.completion_menu_bounds().is_none());
    }

    #[test]
    fn goto_line_query_helpers_accept_line_and_column() {
        assert_eq!(
            parse_goto_line_query("", 120),
            ParsedGoToLineQuery::new(true, None, None)
        );
        assert_eq!(
            parse_goto_line_query("12", 120),
            ParsedGoToLineQuery::new(true, Some(11), None)
        );
        assert_eq!(
            parse_goto_line_query("12:5", 120),
            ParsedGoToLineQuery::new(true, Some(11), Some(4))
        );
        assert_eq!(
            parse_goto_line_query("12:", 120),
            ParsedGoToLineQuery::new(true, Some(11), None)
        );
    }

    #[test]
    fn goto_line_query_helpers_reject_invalid_targets() {
        assert_eq!(
            parse_goto_line_query("0", 120),
            ParsedGoToLineQuery::new(false, None, None)
        );
        assert_eq!(
            parse_goto_line_query("121", 120),
            ParsedGoToLineQuery::new(false, None, None)
        );
        assert_eq!(
            parse_goto_line_query("12:0", 120),
            ParsedGoToLineQuery::new(false, Some(11), None)
        );
        assert_eq!(
            parse_goto_line_query("12:3:4", 120),
            ParsedGoToLineQuery::new(false, None, None)
        );
    }

    #[test]
    fn goto_line_query_update_mutates_dialog_state_only_for_supported_input() {
        let mut state = EditorOverlayState::default();
        state.open_goto_line(Position::new(1, 2));

        assert_eq!(
            state.update_goto_line_query('1', 20),
            Some(ParsedGoToLineQuery::new(true, Some(0), None))
        );
        assert_eq!(
            state.update_goto_line_query(':', 20),
            Some(ParsedGoToLineQuery::new(true, Some(0), None))
        );
        assert_eq!(
            state.update_goto_line_query('0', 20),
            Some(ParsedGoToLineQuery::new(false, Some(0), None))
        );
        assert!(!state.goto_line.as_ref().expect("goto line").is_valid);

        assert!(state.update_goto_line_query('x', 20).is_none());
        assert_eq!(state.goto_line.as_ref().expect("goto line").query, "1:0");

        assert_eq!(
            state.update_goto_line_query('\x08', 20),
            Some(ParsedGoToLineQuery::new(true, Some(0), None))
        );
    }

    #[test]
    fn overlay_retain_helpers_drop_stale_inline_state() {
        let mut state = EditorOverlayState::default();

        state.set_inline_suggestion(crate::InlineSuggestion::new(
            "select".into(),
            crate::Anchor::new(0, 0, crate::Bias::Right),
        ));
        state.set_edit_prediction(crate::EditPrediction::new(
            " from users".into(),
            crate::Anchor::new(0, 0, crate::Bias::Right),
        ));

        state.retain_inline_suggestion(|suggestion| suggestion.text == "select");
        state.retain_edit_prediction(|prediction| prediction.text == " from orders");

        assert!(state.has_inline_suggestion());
        assert!(state.edit_prediction().is_none());
    }

    #[test]
    fn overlay_anchor_helpers_retain_only_primary_cursor_matches() {
        let document = crate::TextDocument::with_text(
            crate::DocumentIdentity::internal().expect("internal document"),
            "alpha beta",
        );
        let anchor = document.anchor_at(5, crate::Bias::Right).expect("anchor");
        let mut state = EditorOverlayState::default();
        state.set_inline_suggestion(crate::InlineSuggestion::new(" beta".into(), anchor));
        state.set_edit_prediction(crate::EditPrediction::new(" gamma".into(), anchor));

        let selections = crate::SelectionsCollection::single(
            crate::Cursor::at(crate::Position::new(0, 5)),
            crate::Selection::at(crate::Position::new(0, 5)),
        );
        state.retain_primary_cursor_anchored_overlays(&document, &selections);
        assert!(state.has_inline_suggestion());
        assert!(state.edit_prediction().is_some());

        let moved = crate::SelectionsCollection::single(
            crate::Cursor::at(crate::Position::new(0, 3)),
            crate::Selection::at(crate::Position::new(0, 3)),
        );
        state.retain_primary_cursor_anchored_overlays(&document, &moved);
        assert!(!state.has_inline_suggestion());
        assert!(state.edit_prediction().is_none());
    }

    #[test]
    fn hover_helpers_track_pending_ready_and_clear_lifecycle() {
        let mut state = EditorOverlayState::default();
        let position = Position::new(3, 4);

        assert_eq!(
            state.begin_hover_timer(position),
            Some(Duration::from_millis(500))
        );
        assert_eq!(state.begin_hover_timer(position), None);
        assert!(state.mark_hover_ready_if_pending(position));
        assert_eq!(state.take_ready_hover_position(), Some(position));
        assert!(state.hover_pending_position.is_none());

        state.begin_hover_timer(Position::new(5, 6));
        state.clear_hover_positions();
        assert!(state.hover_pending_position.is_none());
        assert!(state.hover_ready_position.is_none());
    }
}
