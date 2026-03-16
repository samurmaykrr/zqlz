use crate::{
    Cursor, Position, SearchEngine, Selection, SelectionsCollection, StructuralRange, TextBuffer,
    TextFindOptions,
    selection::{SelectionEntry, SelectionMode},
};

pub type ExtraCursor = (Cursor, Selection);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionHistoryEntry {
    pub collection: SelectionsCollection,
}

impl SelectionHistoryEntry {
    pub fn new(collection: SelectionsCollection) -> Self {
        Self { collection }
    }

    pub fn primary(&self) -> Option<&SelectionEntry> {
        self.collection.primary()
    }

    pub fn extra_cursors(&self) -> Vec<ExtraCursor> {
        self.collection
            .primary_and_extras()
            .map(|(_, _, extras)| extras)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionState {
    pub collection: SelectionsCollection,
}

impl SelectionState {
    pub fn new(collection: SelectionsCollection) -> Self {
        Self { collection }
    }

    pub fn primary(&self) -> Option<&SelectionEntry> {
        self.collection.primary()
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.primary().map(|entry| &entry.cursor)
    }

    pub fn selection(&self) -> Option<&Selection> {
        self.primary().map(|entry| &entry.selection)
    }

    pub fn extra_cursors(&self) -> Vec<ExtraCursor> {
        self.collection
            .primary_and_extras()
            .map(|(_, _, extras)| extras)
            .unwrap_or_default()
    }

    pub fn expect_cursor(&self) -> &Cursor {
        self.cursor()
            .expect("selection state should always have a primary cursor")
    }

    pub fn expect_selection(&self) -> &Selection {
        self.selection()
            .expect("selection state should always have a primary selection")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiCursorEditPlan {
    pub slot: usize,
    pub start: usize,
    pub end: usize,
    pub replacement: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiCursorCommandPlan {
    pub edits: Vec<MultiCursorEditPlan>,
    pub final_offsets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextReplacementEdit {
    pub range: std::ops::Range<usize>,
    pub replacement: String,
}

impl TextReplacementEdit {
    pub fn insert(offset: usize, text: impl Into<String>) -> Self {
        Self {
            range: offset..offset,
            replacement: text.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionRotationEdit {
    pub range: std::ops::Range<usize>,
    pub replacement: String,
}

impl From<SelectionRotationEdit> for TextReplacementEdit {
    fn from(value: SelectionRotationEdit) -> Self {
        Self {
            range: value.range,
            replacement: value.replacement,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimarySelectionDeletionPlan {
    Linear {
        range: std::ops::Range<usize>,
        cursor_position: Position,
    },
    Block {
        ranges: Vec<std::ops::Range<usize>>,
        cursor_position: Position,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedLineBlockPlan {
    pub first_line: usize,
    pub last_line: usize,
    pub byte_range: std::ops::Range<usize>,
    pub has_trailing_newline: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedLineDeletionPlan {
    pub first_line: usize,
    pub byte_range: std::ops::Range<usize>,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextInsertionPlan {
    pub offset: usize,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateSelectedLinesPlan {
    pub insertions: Vec<TextInsertionPlan>,
    pub target_line: usize,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveSelectedLinesPlan {
    pub byte_range: std::ops::Range<usize>,
    pub replacement: String,
    pub target_line: usize,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WholeLineCopyPlan {
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WholeLineCutPlan {
    pub text: String,
    pub delete_range: std::ops::Range<usize>,
    pub target_line: usize,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WholeLinePastePlan {
    pub offset: usize,
    pub text: String,
    pub target_line: usize,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewlineInsertionPlan {
    pub offset: usize,
    pub text: String,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CutToEndOfLinePlan {
    pub text: String,
    pub delete_range: std::ops::Range<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinLinesPlan {
    pub delete_range: std::ops::Range<usize>,
    pub insert_offset: usize,
    pub insert_text: String,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransposeCharsPlan {
    pub replace_range: std::ops::Range<usize>,
    pub replacement: String,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineIndentEdit {
    pub offset: usize,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineDedentEdit {
    pub range: std::ops::Range<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndentLinesPlan {
    pub edits: Vec<LineIndentEdit>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DedentLinesPlan {
    pub edits: Vec<LineDedentEdit>,
    pub target_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LineCommentEdit {
    Insert { offset: usize, text: String },
    Delete { range: std::ops::Range<usize> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinePrefixEditMode {
    Indent,
    Dedent,
    ToggleComment,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToggleLineCommentPlan {
    pub edits: Vec<LineCommentEdit>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostApplySelection {
    Keep,
    MovePrimaryCursor(Position),
    MovePrimaryCursorToOffset(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedEditBatch {
    pub edits: Vec<TextReplacementEdit>,
    pub post_apply_selection: PostApplySelection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoIndentNewlinePlan {
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertAtCursorPlan {
    pub offset: usize,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertTextPlan {
    pub replace_range: std::ops::Range<usize>,
    pub insert_offset: usize,
    pub replaced_text: String,
    pub inserted_text: String,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteBeforeCursorPlan {
    pub primary_range: std::ops::Range<usize>,
    pub paired_closer_range: Option<std::ops::Range<usize>>,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteAtCursorPlan {
    pub range: std::ops::Range<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSubwordPlan {
    pub range: std::ops::Range<usize>,
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectedTextPlan {
    Linear(String),
    Block(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoSurroundSelectionPlan {
    pub edits: Vec<TextReplacementEdit>,
    pub selection: Selection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoCloseBracketPlan {
    pub batch: PlannedEditBatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryReplacementPlan {
    pub batch: PlannedEditBatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkedTextReplacementPlan {
    pub batch: PlannedEditBatch,
    pub marked_range: Option<std::ops::Range<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkipClosingBracketPlan {
    pub target_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WordTarget {
    pub range: std::ops::Range<usize>,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionQueryPlan {
    pub trigger_offset: usize,
    pub current_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureHelpQueryPlan {
    pub function_name: String,
    pub active_parameter: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameQueryPlan {
    pub current_name: String,
    pub ranges: Vec<std::ops::Range<usize>>,
}

pub struct EditorCoreSnapshot<'a> {
    buffer: &'a TextBuffer,
    selections_collection: SelectionsCollection,
    last_select_line_was_extend: bool,
    selection_history: Vec<SelectionHistoryEntry>,
}

impl<'a> EditorCoreSnapshot<'a> {
    pub fn new(
        buffer: &'a TextBuffer,
        selections_collection: SelectionsCollection,
        last_select_line_was_extend: bool,
        selection_history: Vec<SelectionHistoryEntry>,
    ) -> Self {
        Self {
            buffer,
            selections_collection,
            last_select_line_was_extend,
            selection_history,
        }
    }

    fn with_core<T>(self, run: impl FnOnce(&mut EditorCore<'_>) -> T) -> T {
        let Self {
            buffer,
            mut selections_collection,
            mut last_select_line_was_extend,
            mut selection_history,
        } = self;

        let mut core = EditorCore::from_collection(
            buffer,
            &mut selections_collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );
        run(&mut core)
    }

    pub fn selection_state(self) -> SelectionState {
        self.with_core(|core| core.selection_state())
    }

    pub fn all_cursor_selections(self) -> Vec<(Position, Selection)> {
        self.with_core(|core| core.all_cursor_selections())
    }

    pub fn selection_ranges_with_fallback<F>(self, fallback: F) -> Vec<std::ops::Range<usize>>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        let mut fallback = fallback;
        self.with_core(move |core| core.selection_ranges_with_fallback(&mut fallback))
    }

    pub fn multi_cursor_edit_targets(self) -> Vec<(usize, usize)> {
        self.with_core(|core| core.multi_cursor_edit_targets())
    }

    pub fn multi_cursor_caret_offsets(self) -> Vec<(usize, usize)> {
        self.with_core(|core| core.multi_cursor_caret_offsets())
    }

    pub fn multi_cursor_replace_plan(self, text: &str) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_replace_plan(text))
    }

    pub fn multi_cursor_insert_plan(self, text: &str) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_insert_plan(text))
    }

    pub fn multi_cursor_backspace_plan(self) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_backspace_plan())
    }

    pub fn multi_cursor_delete_plan(self) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_delete_plan())
    }

    pub fn multi_cursor_newline_plan(
        self,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_newline_plan(auto_indent_enabled, indent_unit))
    }

    pub fn multi_cursor_indent_plan(self, indent: &str) -> Option<MultiCursorCommandPlan> {
        self.with_core(|core| core.multi_cursor_indent_plan(indent))
    }

    pub fn plan_text_transform_edits<F>(self, transform: F) -> Vec<TextReplacementEdit>
    where
        F: Fn(&str) -> String,
    {
        let mut transform = transform;
        self.with_core(move |core| core.plan_text_transform_edits(&mut transform))
    }

    pub fn selected_line_replacement_edit<F>(self, transform: F) -> Option<TextReplacementEdit>
    where
        F: Fn(&[String]) -> Vec<String>,
    {
        let mut transform = transform;
        self.with_core(move |core| core.selected_line_replacement_edit(&mut transform))
    }

    pub fn plan_rotated_text_replacements(self) -> Vec<TextReplacementEdit> {
        self.with_core(|core| core.plan_rotated_text_replacements())
    }

    pub fn plan_multi_cursor_newline_edits<F>(
        self,
        replacement_for_line: F,
    ) -> Vec<MultiCursorEditPlan>
    where
        F: FnMut(usize) -> String,
    {
        let mut replacement_for_line = replacement_for_line;
        self.with_core(move |core| core.plan_multi_cursor_newline_edits(&mut replacement_for_line))
    }

    pub fn plan_rotated_selection_edits<F>(self, fallback: F) -> Vec<SelectionRotationEdit>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        let mut fallback = fallback;
        self.with_core(move |core| core.plan_rotated_selection_edits(&mut fallback))
    }

    pub fn selected_line_range(self) -> (usize, usize) {
        self.with_core(|core| core.selected_line_range())
    }

    pub fn primary_selection_byte_range(self) -> Option<std::ops::Range<usize>> {
        self.with_core(|core| core.primary_selection_byte_range())
    }

    pub fn primary_replacement_byte_range(
        self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
    ) -> std::ops::Range<usize> {
        self.with_core(|core| core.primary_replacement_byte_range(explicit_range, marked_range))
    }

    pub fn primary_text_replacement_plan(
        self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
        new_text: &str,
    ) -> PrimaryReplacementPlan {
        self.with_core(|core| {
            core.primary_text_replacement_plan(explicit_range, marked_range, new_text)
        })
    }

    pub fn marked_text_replacement_plan(
        self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
        new_text: &str,
        selected_offset_within_marked_text: Option<usize>,
    ) -> MarkedTextReplacementPlan {
        self.with_core(|core| {
            core.marked_text_replacement_plan(
                explicit_range,
                marked_range,
                new_text,
                selected_offset_within_marked_text,
            )
        })
    }

    pub fn primary_selection_or_fallback_range<F>(
        self,
        fallback: F,
    ) -> Option<std::ops::Range<usize>>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        let mut fallback = fallback;
        self.with_core(move |core| core.primary_selection_or_fallback_range(&mut fallback))
    }

    pub fn primary_selection_contains(self, position: Position) -> bool {
        self.with_core(|core| core.primary_selection_contains(position))
    }

    pub fn selected_line_block_plan(self) -> Option<SelectedLineBlockPlan> {
        self.with_core(|core| core.selected_line_block_plan())
    }

    pub fn selected_line_deletion_plan(self) -> Option<SelectedLineDeletionPlan> {
        self.with_core(|core| core.selected_line_deletion_plan())
    }

    pub fn duplicate_selected_lines_up_plan(self) -> Option<DuplicateSelectedLinesPlan> {
        self.with_core(|core| core.duplicate_selected_lines_up_plan())
    }

    pub fn duplicate_selected_lines_down_plan(self) -> Option<DuplicateSelectedLinesPlan> {
        self.with_core(|core| core.duplicate_selected_lines_down_plan())
    }

    pub fn move_selected_lines_up_plan(self) -> Option<MoveSelectedLinesPlan> {
        self.with_core(|core| core.move_selected_lines_up_plan())
    }

    pub fn move_selected_lines_down_plan(self) -> Option<MoveSelectedLinesPlan> {
        self.with_core(|core| core.move_selected_lines_down_plan())
    }

    pub fn whole_line_copy_plan(self) -> Option<WholeLineCopyPlan> {
        self.with_core(|core| core.whole_line_copy_plan())
    }

    pub fn whole_line_cut_plan(self) -> Option<WholeLineCutPlan> {
        self.with_core(|core| core.whole_line_cut_plan())
    }

    pub fn whole_line_cut_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.whole_line_cut_edit_batch())
    }

    pub fn move_selected_lines_up_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.move_selected_lines_up_edit_batch())
    }

    pub fn move_selected_lines_down_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.move_selected_lines_down_edit_batch())
    }

    pub fn duplicate_selected_lines_up_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.duplicate_selected_lines_up_edit_batch())
    }

    pub fn duplicate_selected_lines_down_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.duplicate_selected_lines_down_edit_batch())
    }

    pub fn selected_line_deletion_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.selected_line_deletion_edit_batch())
    }

    pub fn whole_line_paste_plan(self, clipboard_text: &str) -> Option<WholeLinePastePlan> {
        self.with_core(|core| core.whole_line_paste_plan(clipboard_text))
    }

    pub fn insert_newline_above_plan(self) -> Option<NewlineInsertionPlan> {
        self.with_core(|core| core.insert_newline_above_plan())
    }

    pub fn insert_newline_above_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.insert_newline_above_edit_batch())
    }

    pub fn insert_newline_below_plan(self) -> Option<NewlineInsertionPlan> {
        self.with_core(|core| core.insert_newline_below_plan())
    }

    pub fn insert_newline_below_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.insert_newline_below_edit_batch())
    }

    pub fn cut_to_end_of_line_plan(self) -> Option<CutToEndOfLinePlan> {
        self.with_core(|core| core.cut_to_end_of_line_plan())
    }

    pub fn cut_to_end_of_line_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.cut_to_end_of_line_edit_batch())
    }

    pub fn join_lines_plan(self) -> Option<JoinLinesPlan> {
        self.with_core(|core| core.join_lines_plan())
    }

    pub fn join_lines_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.join_lines_edit_batch())
    }

    pub fn transpose_chars_plan(self) -> Option<TransposeCharsPlan> {
        self.with_core(|core| core.transpose_chars_plan())
    }

    pub fn transpose_chars_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.transpose_chars_edit_batch())
    }

    pub fn insert_at_cursor_edit_batch(self, text: &str) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.insert_at_cursor_edit_batch(text))
    }

    pub fn insert_text_edit_batch(self, text: &str) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.insert_text_edit_batch(text))
    }

    pub fn delete_before_cursor_edit_batch<F>(self, bracket_closer: F) -> Option<PlannedEditBatch>
    where
        F: Fn(char) -> Option<char>,
    {
        self.with_core(|core| core.delete_before_cursor_edit_batch(bracket_closer))
    }

    pub fn delete_at_cursor_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.delete_at_cursor_edit_batch())
    }

    pub fn delete_subword_left_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.delete_subword_left_edit_batch())
    }

    pub fn delete_subword_right_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.delete_subword_right_edit_batch())
    }

    pub fn primary_selection_deletion_edit_batch(self) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.primary_selection_deletion_edit_batch())
    }

    pub fn whole_line_paste_edit_batch(self, clipboard_text: &str) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.whole_line_paste_edit_batch(clipboard_text))
    }

    pub fn line_prefix_edit_batch(
        self,
        indent_size: usize,
        use_tabs: bool,
        mode: LinePrefixEditMode,
    ) -> Option<PlannedEditBatch> {
        self.with_core(|core| core.line_prefix_edit_batch(indent_size, use_tabs, mode))
    }

    pub fn indent_lines_plan(self, indent_size: usize, use_tabs: bool) -> Option<IndentLinesPlan> {
        self.with_core(|core| core.indent_lines_plan(indent_size, use_tabs))
    }

    pub fn dedent_lines_plan(self, indent_size: usize) -> Option<DedentLinesPlan> {
        self.with_core(|core| core.dedent_lines_plan(indent_size))
    }

    pub fn toggle_line_comment_plan(self) -> Option<ToggleLineCommentPlan> {
        self.with_core(|core| core.toggle_line_comment_plan())
    }

    pub fn auto_indent_newline_text(
        self,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<AutoIndentNewlinePlan> {
        self.with_core(|core| core.auto_indent_newline_text(auto_indent_enabled, indent_unit))
    }

    pub fn auto_indent_newline_text_for_line(
        self,
        line: usize,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<AutoIndentNewlinePlan> {
        self.with_core(|core| {
            core.auto_indent_newline_text_for_line(line, auto_indent_enabled, indent_unit)
        })
    }

    pub fn insert_at_cursor_plan(self, text: &str) -> Option<InsertAtCursorPlan> {
        self.with_core(|core| core.insert_at_cursor_plan(text))
    }

    pub fn insert_text_plan(self, text: &str) -> Option<InsertTextPlan> {
        self.with_core(|core| core.insert_text_plan(text))
    }

    pub fn delete_before_cursor_plan<F>(self, bracket_closer: F) -> Option<DeleteBeforeCursorPlan>
    where
        F: Fn(char) -> Option<char>,
    {
        self.with_core(|core| core.delete_before_cursor_plan(bracket_closer))
    }

    pub fn delete_at_cursor_plan(self) -> Option<DeleteAtCursorPlan> {
        self.with_core(|core| core.delete_at_cursor_plan())
    }

    pub fn delete_subword_left_plan(self) -> Option<DeleteSubwordPlan> {
        self.with_core(|core| core.delete_subword_left_plan())
    }

    pub fn delete_subword_right_plan(self) -> Option<DeleteSubwordPlan> {
        self.with_core(|core| core.delete_subword_right_plan())
    }

    pub fn selected_text_plan(self) -> Option<SelectedTextPlan> {
        self.with_core(|core| core.selected_text_plan())
    }

    pub fn auto_surround_selection_plan(
        self,
        opener: char,
        closer: char,
    ) -> Option<AutoSurroundSelectionPlan> {
        self.with_core(|core| core.auto_surround_selection_plan(opener, closer))
    }

    pub fn auto_close_bracket_plan(
        self,
        opener: char,
        closer: char,
        is_safe_position: bool,
        inside_string_or_comment: bool,
    ) -> Option<AutoCloseBracketPlan> {
        self.with_core(|core| {
            core.auto_close_bracket_plan(opener, closer, is_safe_position, inside_string_or_comment)
        })
    }

    pub fn skip_closing_bracket_plan(self, closer: char) -> Option<SkipClosingBracketPlan> {
        self.with_core(|core| core.skip_closing_bracket_plan(closer))
    }

    pub fn find_all_occurrences(self, needle: &str) -> Vec<std::ops::Range<usize>> {
        self.with_core(|core| core.find_all_occurrences(needle))
    }

    pub fn find_word_range_at_offset(self, offset: usize) -> Option<std::ops::Range<usize>> {
        self.with_core(|core| core.find_word_range_at_offset(offset))
    }

    pub fn selection_or_word_under_cursor_text(self) -> Option<String> {
        self.with_core(|core| core.selection_or_word_under_cursor_text())
    }

    pub fn completion_query_plan(self) -> CompletionQueryPlan {
        self.with_core(|core| core.completion_query_plan())
    }

    pub fn word_target_at_offset(self, offset: usize) -> Option<WordTarget> {
        self.with_core(|core| core.word_target_at_offset(offset))
    }

    pub fn rename_target_at_cursor(self) -> Option<WordTarget> {
        self.with_core(|core| core.rename_target_at_cursor())
    }

    pub fn signature_help_query_plan(
        self,
        structural_open_paren: Option<usize>,
    ) -> Option<SignatureHelpQueryPlan> {
        self.with_core(|core| core.signature_help_query_plan(structural_open_paren))
    }

    pub fn rename_query_plan(self, new_name: &str) -> Option<RenameQueryPlan> {
        self.with_core(|core| core.rename_query_plan(new_name))
    }
}

pub struct EditorCore<'a> {
    buffer: &'a TextBuffer,
    cursor: Cursor,
    selection: Selection,
    extra_cursors: Vec<ExtraCursor>,
    legacy_cursor: Option<&'a mut Cursor>,
    legacy_selection: Option<&'a mut Selection>,
    legacy_extra_cursors: Option<&'a mut Vec<ExtraCursor>>,
    selections_collection: &'a mut SelectionsCollection,
    last_select_line_was_extend: &'a mut bool,
    selection_history: &'a mut Vec<SelectionHistoryEntry>,
}

impl Drop for EditorCore<'_> {
    fn drop(&mut self) {
        self.sync_selection_collection_from_fields();

        if let Some(cursor) = self.legacy_cursor.as_deref_mut() {
            *cursor = self.cursor.clone();
        }
        if let Some(selection) = self.legacy_selection.as_deref_mut() {
            *selection = self.selection.clone();
        }
        if let Some(extra_cursors) = self.legacy_extra_cursors.as_deref_mut() {
            *extra_cursors = self.extra_cursors.clone();
        }
    }
}

impl<'a> EditorCore<'a> {
    pub fn from_collection(
        buffer: &'a TextBuffer,
        selections_collection: &'a mut SelectionsCollection,
        last_select_line_was_extend: &'a mut bool,
        selection_history: &'a mut Vec<SelectionHistoryEntry>,
    ) -> Self {
        let (cursor, selection, extra_cursors) = selections_collection
            .primary_and_extras()
            .expect("selections collection should always contain a primary entry");

        Self {
            buffer,
            cursor,
            selection,
            extra_cursors,
            legacy_cursor: None,
            legacy_selection: None,
            legacy_extra_cursors: None,
            selections_collection,
            last_select_line_was_extend,
            selection_history,
        }
    }

    pub fn new(
        buffer: &'a TextBuffer,
        cursor: &'a mut Cursor,
        selection: &'a mut Selection,
        extra_cursors: &'a mut Vec<ExtraCursor>,
        selections_collection: &'a mut SelectionsCollection,
        last_select_line_was_extend: &'a mut bool,
        selection_history: &'a mut Vec<SelectionHistoryEntry>,
    ) -> Self {
        let expected_entries = 1 + extra_cursors.len();
        let collection_matches_fields = selections_collection
            .primary_and_extras()
            .map(|(primary_cursor, primary_selection, extras)| {
                primary_cursor == *cursor
                    && primary_selection == *selection
                    && extras == *extra_cursors
                    && selections_collection.len() == expected_entries
            })
            .unwrap_or(false);

        if !collection_matches_fields {
            *selections_collection = SelectionsCollection::from_primary_and_extras(
                cursor.clone(),
                selection.clone(),
                extra_cursors.clone(),
            );
        }

        Self {
            buffer,
            cursor: cursor.clone(),
            selection: selection.clone(),
            extra_cursors: extra_cursors.clone(),
            legacy_cursor: Some(cursor),
            legacy_selection: Some(selection),
            legacy_extra_cursors: Some(extra_cursors),
            selections_collection,
            last_select_line_was_extend,
            selection_history,
        }
    }

    pub fn selection_state(&self) -> SelectionState {
        SelectionState::new(self.selections_collection.clone())
    }

    fn selection_entries(&self) -> &[SelectionEntry] {
        self.selections_collection.all()
    }

    fn set_selection_collection(&mut self, collection: SelectionsCollection) -> bool {
        let Some((cursor, selection, extra_cursors)) = collection.primary_and_extras() else {
            return false;
        };

        self.cursor = cursor;
        self.selection = selection;
        self.extra_cursors = extra_cursors;
        *self.selections_collection = collection;
        true
    }

    fn set_normalized_selection_collection(&mut self, collection: SelectionsCollection) -> bool {
        self.set_selection_collection(collection.normalized())
    }

    fn sync_selection_collection_from_fields(&mut self) {
        *self.selections_collection = SelectionsCollection::from_primary_and_extras(
            self.cursor.clone(),
            self.selection.clone(),
            self.extra_cursors.clone(),
        );
    }

    fn set_primary_cursor_only(&mut self, cursor_position: Position) {
        self.cursor.set_position(cursor_position);
        self.sync_selection_collection_from_fields();
    }

    fn set_primary_cursor_with_collection_sync(&mut self, cursor_position: Position) {
        self.cursor.set_position(cursor_position);
        self.sync_selection_collection_from_fields();
    }

    fn set_primary_cursor_and_selection_fields(
        &mut self,
        cursor_position: Position,
        selection: Selection,
    ) {
        self.cursor.set_position(cursor_position);
        self.selection = selection;
        self.sync_selection_collection_from_fields();
    }

    pub fn restore_selection_state(&mut self, state: SelectionState) {
        let _ = self.set_selection_collection(state.collection);
    }

    fn restore_selection_history_entry(&mut self, entry: &SelectionHistoryEntry) -> bool {
        self.set_selection_collection(entry.collection.clone())
    }

    pub fn begin_non_extending_selection_action(&mut self) {
        *self.last_select_line_was_extend = false;
    }

    pub fn clear_line_selection_extension(&mut self) {
        *self.last_select_line_was_extend = false;
    }

    pub fn expand_selection(
        &mut self,
        structural_range: Option<StructuralRange>,
        word_range: Option<std::ops::Range<usize>>,
    ) -> bool {
        self.expand_selections(&[(structural_range, word_range)])
    }

    pub fn expand_selections(
        &mut self,
        candidates: &[(Option<StructuralRange>, Option<std::ops::Range<usize>>)],
    ) -> bool {
        if candidates.len() != self.selection_entries().len() {
            return false;
        }

        let mut changed_any = false;
        let mut entries = self.selection_entries().to_vec();

        for (entry, (structural_range, word_range)) in entries.iter_mut().zip(candidates.iter()) {
            let current_range = if entry.selection.has_selection() {
                let range = entry.selection.range();
                let start = self.buffer.position_to_offset(range.start).ok();
                let end = self.buffer.position_to_offset(range.end).ok();
                start.zip(end)
            } else {
                None
            };

            let next_selection = if let Some(structural_range) = structural_range {
                if current_range != Some((structural_range.start, structural_range.end)) {
                    self.selection_from_offsets_preserving_direction(
                        &entry.selection,
                        structural_range.start,
                        structural_range.end,
                    )
                } else {
                    None
                }
            } else {
                None
            }
            .or_else(|| {
                word_range.as_ref().and_then(|word_range| {
                    if current_range != Some((word_range.start, word_range.end)) {
                        self.selection_from_offsets_preserving_direction(
                            &entry.selection,
                            word_range.start,
                            word_range.end,
                        )
                    } else {
                        None
                    }
                })
            });

            if let Some(selection) = next_selection {
                entry.cursor = Cursor::at(selection.head());
                entry.selection = selection;
                changed_any = true;
            }
        }

        if !changed_any {
            return false;
        }

        self.push_selection_snapshot();
        SelectionsCollection::from_entries(
            entries,
            self.selections_collection.primary_index(),
            self.selections_collection.newest_index(),
        )
        .is_some_and(|collection| self.set_normalized_selection_collection(collection))
    }

    pub fn shrink_selection(&mut self) -> bool {
        let Some(state) = self.selection_history.pop() else {
            return false;
        };

        self.restore_selection_history_entry(&state)
    }

    pub fn push_selection_snapshot(&mut self) {
        self.selection_history.push(SelectionHistoryEntry::new(
            self.selections_collection.clone(),
        ));
        if self.selection_history.len() > 100 {
            self.selection_history.remove(0);
        }
    }

    pub fn undo_selection(&mut self) -> bool {
        let Some(state) = self.selection_history.pop() else {
            return false;
        };

        self.restore_selection_history_entry(&state)
    }

    pub fn select_all(&mut self) {
        let buffer_end = Position::new(
            self.buffer.line_count().saturating_sub(1),
            self.buffer
                .line(self.buffer.line_count().saturating_sub(1))
                .map(|line| line.len())
                .unwrap_or(0),
        );
        self.selection.select_all(buffer_end);
        self.set_primary_cursor_only(buffer_end);
    }

    pub fn clear_selection(&mut self) {
        self.selection.clear();
        self.sync_selection_collection_from_fields();
    }

    pub fn clear_extra_cursors(&mut self) {
        self.extra_cursors.clear();
        self.sync_selection_collection_from_fields();
    }

    pub fn toggle_primary_selection_mode(&mut self) -> SelectionMode {
        let new_mode = if self.selection.mode() == SelectionMode::Block {
            SelectionMode::Character
        } else {
            SelectionMode::Block
        };
        self.selection.set_mode(new_mode);
        self.sync_selection_collection_from_fields();
        new_mode
    }

    fn primary_selection_anchor_for_extension(&self) -> Position {
        if self.selection.has_selection() {
            self.selection.anchor()
        } else {
            self.cursor.position()
        }
    }

    fn apply_primary_cursor_motion(
        &mut self,
        extend_selection: bool,
        motion: impl FnOnce(&mut Cursor, &TextBuffer),
    ) {
        let selection_anchor =
            extend_selection.then(|| self.primary_selection_anchor_for_extension());
        motion(&mut self.cursor, self.buffer);

        if let Some(selection_anchor) = selection_anchor {
            self.set_primary_selection_range(selection_anchor, self.cursor.position());
        } else {
            self.collapse_primary_selection_to_cursor();
        }
    }

    pub fn move_primary_cursor_left(&mut self) {
        self.cursor.move_left(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_left_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_left(buffer)
        });
    }

    pub fn move_primary_cursor_right(&mut self) {
        self.cursor.move_right(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_right_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_right(buffer)
        });
    }

    pub fn move_primary_cursor_up(&mut self) {
        self.cursor.move_up(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_up_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| cursor.move_up(buffer));
    }

    pub fn move_primary_cursor_down(&mut self) {
        self.cursor.move_down(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_down_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_down(buffer)
        });
    }

    pub fn move_primary_cursor_to_line_start(&mut self) {
        self.cursor.move_to_line_start();
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_line_end(&mut self) {
        self.cursor.move_to_line_end(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_line_end_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_to_line_end(buffer)
        });
    }

    pub fn move_primary_cursor_to_document_start(&mut self) {
        self.cursor.move_to_document_start();
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_document_start_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, _buffer| {
            cursor.move_to_document_start()
        });
    }

    pub fn move_primary_cursor_to_document_end(&mut self) {
        self.cursor.move_to_document_end(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_document_end_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_to_document_end(buffer)
        });
    }

    pub fn move_primary_cursor_to_next_word_start(&mut self) {
        self.cursor.move_to_next_word_start(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_next_word_start_with_selection(
        &mut self,
        extend_selection: bool,
    ) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_to_next_word_start(buffer)
        });
    }

    pub fn move_primary_cursor_to_prev_word_start(&mut self) {
        self.cursor.move_to_prev_word_start(self.buffer);
        self.sync_selection_collection_from_fields();
    }

    pub fn move_primary_cursor_to_prev_word_start_with_selection(
        &mut self,
        extend_selection: bool,
    ) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            cursor.move_to_prev_word_start(buffer)
        });
    }

    pub fn move_primary_cursor_to_paragraph_start(&mut self) {
        let total_lines = self.buffer.line_count();
        if total_lines == 0 {
            return;
        }

        let is_blank = |line: usize| -> bool {
            self.buffer
                .line(line)
                .map(|text| text.trim().is_empty())
                .unwrap_or(true)
        };

        let mut line = self.cursor.position().line;
        while line > 0 && !is_blank(line) {
            line -= 1;
        }
        while line > 0 && is_blank(line) {
            line -= 1;
        }
        while line > 0 && !is_blank(line - 1) {
            line -= 1;
        }

        self.set_primary_cursor_only(Position::new(line, 0));
    }

    pub fn move_primary_cursor_to_paragraph_start_with_selection(
        &mut self,
        extend_selection: bool,
    ) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            let total_lines = buffer.line_count();
            if total_lines == 0 {
                return;
            }

            let is_blank = |line: usize| -> bool {
                buffer
                    .line(line)
                    .map(|text| text.trim().is_empty())
                    .unwrap_or(true)
            };

            let mut line = cursor.position().line;
            while line > 0 && !is_blank(line) {
                line -= 1;
            }
            while line > 0 && is_blank(line) {
                line -= 1;
            }
            while line > 0 && !is_blank(line - 1) {
                line -= 1;
            }

            cursor.set_position(Position::new(line, 0));
        });
    }

    pub fn move_primary_cursor_to_paragraph_end(&mut self) {
        let total_lines = self.buffer.line_count();
        if total_lines == 0 {
            return;
        }

        let is_blank = |line: usize| -> bool {
            self.buffer
                .line(line)
                .map(|text| text.trim().is_empty())
                .unwrap_or(true)
        };

        let last_line = total_lines.saturating_sub(1);
        let mut line = self.cursor.position().line;
        while line < last_line && !is_blank(line) {
            line += 1;
        }
        while line < last_line && is_blank(line) {
            line += 1;
        }

        self.set_primary_cursor_only(Position::new(line, 0));
    }

    pub fn move_primary_cursor_to_paragraph_end_with_selection(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            let total_lines = buffer.line_count();
            if total_lines == 0 {
                return;
            }

            let is_blank = |line: usize| -> bool {
                buffer
                    .line(line)
                    .map(|text| text.trim().is_empty())
                    .unwrap_or(true)
            };

            let last_line = total_lines.saturating_sub(1);
            let mut line = cursor.position().line;
            while line < last_line && !is_blank(line) {
                line += 1;
            }
            while line < last_line && is_blank(line) {
                line += 1;
            }

            cursor.set_position(Position::new(line, 0));
        });
    }

    pub fn move_primary_cursor_to_next_subword_end(&mut self) {
        let offset = self.cursor.offset(self.buffer);
        let new_offset = Self::next_subword_end_in_buffer(self.buffer, offset);
        if let Ok(position) = self.buffer.offset_to_position(new_offset) {
            self.set_primary_cursor_only(position);
        }
    }

    pub fn move_primary_cursor_to_next_subword_end_with_selection(
        &mut self,
        extend_selection: bool,
    ) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            let offset = cursor.offset(buffer);
            let new_offset = Self::next_subword_end_in_buffer(buffer, offset);
            if let Ok(position) = buffer.offset_to_position(new_offset) {
                cursor.set_position(position);
            }
        });
    }

    pub fn move_primary_cursor_to_prev_subword_start(&mut self) {
        let offset = self.cursor.offset(self.buffer);
        let new_offset = Self::prev_subword_start_in_buffer(self.buffer, offset);
        if let Ok(position) = self.buffer.offset_to_position(new_offset) {
            self.set_primary_cursor_only(position);
        }
    }

    pub fn move_primary_cursor_to_prev_subword_start_with_selection(
        &mut self,
        extend_selection: bool,
    ) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            let offset = cursor.offset(buffer);
            let new_offset = Self::prev_subword_start_in_buffer(buffer, offset);
            if let Ok(position) = buffer.offset_to_position(new_offset) {
                cursor.set_position(position);
            }
        });
    }

    pub fn move_primary_cursor_to_smart_home(&mut self, extend_selection: bool) {
        self.apply_primary_cursor_motion(extend_selection, |cursor, buffer| {
            let line = cursor.position().line;
            let current_column = cursor.position().column;

            let first_non_whitespace_column = buffer
                .line(line)
                .map(|text| {
                    text.chars()
                        .take_while(|character| character.is_whitespace())
                        .count()
                })
                .unwrap_or(0);

            let target_column = if current_column != first_non_whitespace_column {
                first_non_whitespace_column
            } else {
                0
            };

            cursor.set_position(Position::new(line, target_column));
        });
    }

    pub fn move_primary_cursor(&mut self, position: Position, extend_selection: bool) {
        if extend_selection {
            if !self.selection.has_selection() {
                self.selection.start_selection(self.cursor.position());
            }
            self.cursor.set_position(position);
            self.selection.extend_to(position);
            self.sync_selection_collection_from_fields();
        } else {
            self.set_primary_cursor_and_selection_fields(position, Selection::at(position));
        }
    }

    pub fn move_primary_cursor_to_offset(&mut self, offset: usize, extend_selection: bool) -> bool {
        let Ok(position) = self.buffer.offset_to_position(offset) else {
            return false;
        };

        self.move_primary_cursor(self.buffer.clamp_position(position), extend_selection);
        true
    }

    pub fn set_primary_selection(&mut self, selection: Selection) {
        self.set_primary_cursor_and_selection_fields(selection.head(), selection);
    }

    pub fn set_primary_cursor_and_selection(
        &mut self,
        cursor_position: Position,
        selection: Selection,
    ) {
        self.set_primary_cursor_and_selection_fields(cursor_position, selection);
    }

    pub fn set_primary_cursor_preserving_selection(&mut self, cursor_position: Position) {
        self.set_primary_cursor_and_selection(cursor_position, self.selection.clone());
    }

    pub fn set_primary_cursor_at_offset_preserving_selection(&mut self, offset: usize) -> bool {
        let Some(cursor_position) = self.position_for_offset(offset) else {
            return false;
        };

        self.set_primary_cursor_preserving_selection(cursor_position);
        true
    }

    pub fn set_primary_selection_range(&mut self, anchor: Position, head: Position) {
        self.set_primary_selection(Selection::from_anchor_head(anchor, head));
    }

    pub fn select_entire_line(&mut self, line: usize) -> Position {
        let line_start = Position::new(line, 0);
        let line_end = self.line_selection_end_position(line);
        self.set_primary_cursor_and_selection(
            line_start,
            Selection::from_anchor_head(line_start, line_end),
        );
        line_start
    }

    pub fn select_word_at_offset_or_move_primary_cursor(
        &mut self,
        offset: usize,
        fallback_position: Position,
    ) -> Position {
        let Some(word_range) = self.find_word_range_at_offset(offset) else {
            self.move_primary_cursor(fallback_position, false);
            return fallback_position;
        };

        let Some(selection) = self.selection_from_offsets(word_range.start, word_range.end) else {
            self.move_primary_cursor(fallback_position, false);
            return fallback_position;
        };

        let anchor = selection.anchor();
        self.set_primary_selection(selection);
        anchor
    }

    pub fn set_primary_selection_from_offsets(
        &mut self,
        anchor_offset: usize,
        head_offset: usize,
    ) -> bool {
        let Some(selection) = self.selection_from_offsets(anchor_offset, head_offset) else {
            return false;
        };

        self.set_primary_selection(selection);
        true
    }

    pub fn extend_primary_selection_to_cursor(&mut self, anchor: Position) {
        self.set_primary_selection_range(anchor, self.cursor.position());
    }

    pub fn begin_primary_mouse_selection(
        &mut self,
        position: Position,
        extend_selection: bool,
    ) -> Position {
        if extend_selection {
            let anchor = self.primary_selection_anchor_for_extension();
            self.set_primary_selection_range(anchor, position);
            anchor
        } else {
            self.move_primary_cursor(position, false);
            position
        }
    }

    pub fn drag_primary_mouse_selection(&mut self, anchor: Position, head: Position) {
        self.set_primary_selection_range(anchor, head);
    }

    pub fn collapse_primary_selection_to_cursor(&mut self) {
        self.selection.set_position(self.cursor.position());
        self.sync_selection_collection_from_fields();
    }

    pub fn set_single_cursor(&mut self, position: Position) {
        self.extra_cursors.clear();
        self.set_primary_cursor_and_selection_fields(position, Selection::at(position));
    }

    pub fn set_primary_cursor_at_offset(&mut self, offset: usize) -> bool {
        let Ok(position) = self.buffer.offset_to_position(offset) else {
            return false;
        };
        let clamped = self.buffer.clamp_position(position);
        self.set_single_cursor(clamped);
        true
    }

    pub fn set_collapsed_cursors_at_offsets(&mut self, offsets: &[usize]) -> bool {
        let mut positions = Vec::with_capacity(offsets.len());

        for &offset in offsets {
            let Ok(position) = self.buffer.offset_to_position(offset) else {
                return false;
            };
            let position = self.buffer.clamp_position(position);
            positions.push((position, position));
        }

        self.set_multi_selections_from_positions(&positions)
    }

    pub fn restore_multi_cursor_caret_offsets(&mut self, offsets: &[usize]) -> bool {
        self.set_collapsed_cursors_at_offsets(offsets)
    }

    pub fn set_multi_selections_from_offsets(&mut self, offsets: &[(usize, usize)]) -> bool {
        let mut positions = Vec::with_capacity(offsets.len());

        for &(anchor_offset, head_offset) in offsets {
            let Some(anchor) = self.position_for_offset(anchor_offset) else {
                return false;
            };
            let Some(head) = self.position_for_offset(head_offset) else {
                return false;
            };
            positions.push((anchor, head));
        }

        self.set_multi_selections_from_positions(&positions)
    }

    pub fn swap_selection_ends(&mut self) -> bool {
        let mut swapped_any = false;

        if Self::swap_selection_end(&mut self.cursor, &mut self.selection) {
            swapped_any = true;
        }

        for (cursor, selection) in self.extra_cursors.iter_mut() {
            if Self::swap_selection_end(cursor, selection) {
                swapped_any = true;
            }
        }

        if swapped_any {
            self.sync_selection_collection_from_fields();
        }

        swapped_any
    }

    pub fn set_multi_selections_from_positions(
        &mut self,
        positions: &[(Position, Position)],
    ) -> bool {
        let Some(&(first_start, first_end)) = positions.first() else {
            return false;
        };

        let mut collection = SelectionsCollection::single(
            Cursor::at(first_end),
            Selection::from_anchor_head(first_start, first_end),
        );
        for &(start, end) in positions.iter().skip(1) {
            collection.push(Cursor::at(end), Selection::from_anchor_head(start, end));
        }

        self.set_normalized_selection_collection(collection)
    }

    pub fn all_cursor_selections(&self) -> Vec<(Position, Selection)> {
        self.selection_entries()
            .iter()
            .map(|entry| (entry.cursor.position(), entry.selection.clone()))
            .collect()
    }

    pub fn selection_ranges_with_fallback<F>(&self, mut fallback: F) -> Vec<std::ops::Range<usize>>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        self.selection_entries()
            .iter()
            .filter_map(|entry| {
                self.selection_range_for_cursor(&entry.cursor, &entry.selection, &mut fallback)
            })
            .collect()
    }

    pub fn collapse_to_primary_cursor(&mut self) {
        let Some(primary) = self.selections_collection.primary().cloned() else {
            return;
        };
        let collection =
            SelectionsCollection::single(primary.cursor, Selection::at(primary.selection.head()));
        let _ = self.set_selection_collection(collection);
    }

    pub fn add_extra_cursor(&mut self, position: Position, with_selection: Option<Selection>) {
        let already_exists = self
            .selection_entries()
            .iter()
            .any(|entry| entry.cursor.position() == position);
        if already_exists {
            return;
        }

        let mut entries = self.selection_entries().to_vec();
        entries.push(SelectionEntry {
            cursor: Cursor::at(position),
            selection: with_selection.unwrap_or_else(|| Selection::at(position)),
        });
        if let Some(collection) = SelectionsCollection::from_entries(
            entries,
            self.selections_collection.primary().map(|_| 0).unwrap_or(0),
            self.selections_collection.len(),
        ) {
            let _ = self.set_normalized_selection_collection(collection);
        }
    }

    pub fn normalize_extra_cursors(&mut self) {
        let normalized = self.selections_collection.normalized();
        let _ = self.set_selection_collection(normalized);
    }

    pub fn split_selection_into_lines(&mut self) -> bool {
        if !self.is_multiline_selection() {
            return false;
        }

        self.push_selection_snapshot();

        let range = self.selection.range();
        let first_line = range.start.line;
        let last_line = range.end.line;

        let first_line_len = self
            .buffer
            .line(first_line)
            .map(|line| line.len())
            .unwrap_or(0);
        let first_head = Position::new(first_line, first_line_len);
        let first_anchor = Position::new(first_line, range.start.column);

        let mut collection = SelectionsCollection::single(
            Cursor::at(first_head),
            Selection::from_anchor_head(first_anchor, first_head),
        );
        for line in (first_line + 1)..=last_line {
            let end_column = if line == last_line {
                range.end.column
            } else {
                self.buffer.line(line).map(|text| text.len()).unwrap_or(0)
            };
            let head = Position::new(line, end_column);
            let anchor = Position::new(line, 0);
            collection.push(Cursor::at(head), Selection::from_anchor_head(anchor, head));
        }

        let _ = self.set_selection_collection(collection);
        self.normalize_extra_cursors();
        true
    }

    pub fn select_line(&mut self) {
        let line = self.cursor.position().line;
        let line_count = self.buffer.line_count();

        if *self.last_select_line_was_extend && self.selection.has_selection() {
            let current_end = self.selection.end();
            let current_end_line =
                if current_end.column == 0 && current_end.line > self.selection.start().line {
                    current_end.line.saturating_sub(1)
                } else {
                    current_end.line
                };
            let next_line = (current_end_line + 1).min(line_count.saturating_sub(1));
            let end_position = self.line_selection_end_position(next_line);
            let anchor = Position::new(self.selection.start().line, 0);
            self.set_primary_cursor_and_selection_fields(
                end_position,
                Selection::from_anchor_head(anchor, end_position),
            );
        } else {
            let start = Position::new(line, 0);
            let end = self.line_selection_end_position(line);
            self.set_primary_cursor_and_selection_fields(
                end,
                Selection::from_anchor_head(start, end),
            );
            *self.last_select_line_was_extend = true;
            return;
        }

        *self.last_select_line_was_extend = true;
    }

    pub fn select_next_occurrence(
        &mut self,
        word_range: Option<std::ops::Range<usize>>,
        all_occurrences: &[std::ops::Range<usize>],
    ) -> bool {
        if all_occurrences.is_empty() {
            return false;
        }

        if !self.selection.has_selection() {
            let Some(word_range) = word_range else {
                return false;
            };
            return self.select_offset_range(word_range);
        }

        let claimed = self.claimed_selection_ranges();
        let search_after = claimed.iter().map(|range| range.end).max().unwrap_or(0);
        let next = all_occurrences
            .iter()
            .find(|range| {
                range.start >= search_after
                    && !claimed.iter().any(|claimed| claimed.start == range.start)
            })
            .or_else(|| {
                all_occurrences
                    .iter()
                    .find(|range| !claimed.iter().any(|claimed| claimed.start == range.start))
            });

        let Some(byte_range) = next else {
            return false;
        };

        let Ok(start) = self.buffer.offset_to_position(byte_range.start) else {
            return false;
        };
        let Ok(end) = self.buffer.offset_to_position(byte_range.end) else {
            return false;
        };

        let new_selection = Selection::from_anchor_head(start, end);
        self.add_extra_cursor(end, Some(new_selection));
        self.normalize_extra_cursors();

        let previous_primary = claimed
            .first()
            .and_then(|range| self.buffer.offset_to_position(range.end).ok());
        self.set_primary_cursor_with_collection_sync(end);
        if let Some(previous_primary) = previous_primary {
            self.set_primary_cursor_with_collection_sync(previous_primary);
        }
        true
    }

    pub fn select_all_occurrences(&mut self, all_occurrences: &[std::ops::Range<usize>]) -> bool {
        if all_occurrences.is_empty() {
            return false;
        }

        let mut collection = None;
        let mut first = true;
        for byte_range in all_occurrences {
            let Ok(start) = self.buffer.offset_to_position(byte_range.start) else {
                continue;
            };
            let Ok(end) = self.buffer.offset_to_position(byte_range.end) else {
                continue;
            };

            let selection = Selection::from_anchor_head(start, end);
            if first {
                collection = Some(SelectionsCollection::single(Cursor::at(end), selection));
                first = false;
            } else if let Some(collection) = collection.as_mut() {
                collection.push(Cursor::at(end), selection);
            }
        }

        let Some(collection) = collection else {
            return false;
        };
        self.set_normalized_selection_collection(collection)
    }

    pub fn add_cursor_above(&mut self) -> bool {
        let top_line = {
            let mut minimum_line = self.cursor.position().line;
            for entry in self.selection_entries() {
                minimum_line = minimum_line.min(entry.cursor.position().line);
            }
            minimum_line
        };

        if top_line == 0 {
            return false;
        }

        let target_line = top_line - 1;
        let preferred_column = self.cursor.position().column;
        let line_length = self
            .buffer
            .line(target_line)
            .map(|line| line.len())
            .unwrap_or(0);
        let position = Position::new(target_line, preferred_column.min(line_length));
        self.add_extra_cursor(position, None);
        self.normalize_extra_cursors();
        true
    }

    pub fn add_cursor_below(&mut self) -> bool {
        let line_count = self.buffer.line_count();
        let bottom_line = {
            let mut maximum_line = self.cursor.position().line;
            for entry in self.selection_entries() {
                maximum_line = maximum_line.max(entry.cursor.position().line);
            }
            maximum_line
        };

        if bottom_line + 1 >= line_count {
            return false;
        }

        let target_line = bottom_line + 1;
        let preferred_column = self.cursor.position().column;
        let line_length = self
            .buffer
            .line(target_line)
            .map(|line| line.len())
            .unwrap_or(0);
        let position = Position::new(target_line, preferred_column.min(line_length));
        self.add_extra_cursor(position, None);
        self.normalize_extra_cursors();
        true
    }

    pub fn multi_cursor_edit_targets(&self) -> Vec<(usize, usize)> {
        let mut targets = Vec::new();

        for entry in self.selection_entries() {
            if entry.selection.has_selection() {
                let range = entry.selection.range();
                let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
                let end = self.buffer.position_to_offset(range.end).unwrap_or(start);
                targets.push((start.min(end), start.max(end)));
            } else {
                let offset = entry.cursor.offset(self.buffer);
                targets.push((offset, offset));
            }
        }

        targets.sort_by(|left, right| right.0.cmp(&left.0).then(right.1.cmp(&left.1)));
        targets.dedup();
        targets
    }

    pub fn multi_cursor_caret_offsets(&self) -> Vec<(usize, usize)> {
        self.selection_entries()
            .iter()
            .enumerate()
            .map(|(index, entry)| (index, entry.cursor.offset(self.buffer)))
            .collect()
    }

    fn prepare_multi_cursor_command_plan(
        &self,
        mut edits: Vec<MultiCursorEditPlan>,
    ) -> Option<MultiCursorCommandPlan> {
        if edits.is_empty() {
            return None;
        }

        edits.sort_by(|left, right| {
            right
                .start
                .cmp(&left.start)
                .then(right.end.cmp(&left.end))
                .then(left.slot.cmp(&right.slot))
        });

        let mut final_offsets = vec![0usize; self.extra_cursors.len() + 1];
        for (slot, offset) in self.multi_cursor_caret_offsets() {
            if let Some(entry) = final_offsets.get_mut(slot) {
                *entry = offset;
            }
        }

        Some(MultiCursorCommandPlan {
            edits,
            final_offsets,
        })
    }

    pub fn multi_cursor_replace_plan(&self, text: &str) -> Option<MultiCursorCommandPlan> {
        self.prepare_multi_cursor_command_plan(self.plan_multi_cursor_replace_edits(text))
    }

    pub fn multi_cursor_insert_plan(&self, text: &str) -> Option<MultiCursorCommandPlan> {
        self.prepare_multi_cursor_command_plan(self.plan_multi_cursor_insert_edits(text))
    }

    pub fn multi_cursor_backspace_plan(&self) -> Option<MultiCursorCommandPlan> {
        self.prepare_multi_cursor_command_plan(self.plan_multi_cursor_backspace_edits())
    }

    pub fn multi_cursor_delete_plan(&self) -> Option<MultiCursorCommandPlan> {
        self.prepare_multi_cursor_command_plan(self.plan_multi_cursor_delete_edits())
    }

    pub fn multi_cursor_newline_plan(
        &self,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<MultiCursorCommandPlan> {
        let edits = self.plan_multi_cursor_newline_edits(|line| {
            self.auto_indent_newline_text_for_line(line, auto_indent_enabled, indent_unit)
                .map(|plan| plan.text)
                .unwrap_or_else(|| "\n".to_string())
        });
        self.prepare_multi_cursor_command_plan(edits)
    }

    pub fn multi_cursor_indent_plan(&self, indent: &str) -> Option<MultiCursorCommandPlan> {
        self.prepare_multi_cursor_command_plan(self.plan_multi_cursor_indent_edits(indent))
    }

    pub fn plan_text_transform_edits<F>(&self, mut transform: F) -> Vec<TextReplacementEdit>
    where
        F: FnMut(&str) -> String,
    {
        let mut ranges = self.selection_ranges_with_fallback(|position| {
            let offset = self.buffer.position_to_offset(position).ok()?;
            Some(
                self.find_word_range_at_offset(offset)
                    .unwrap_or(offset..offset),
            )
        });

        ranges.sort_by(|left, right| right.start.cmp(&left.start));
        ranges.dedup_by_key(|range| range.start);

        let mut edits = Vec::new();
        for range in ranges {
            if range.start >= range.end {
                continue;
            }

            let Ok(original) = self.buffer.text_for_range(range.clone()) else {
                continue;
            };
            let replacement = transform(&original);
            if replacement == original {
                continue;
            }

            edits.push(TextReplacementEdit { range, replacement });
        }

        edits
    }

    pub fn selected_line_replacement_edit<F>(&self, mut transform: F) -> Option<TextReplacementEdit>
    where
        F: FnMut(&[String]) -> Vec<String>,
    {
        let SelectedLineBlockPlan {
            first_line,
            last_line,
            byte_range,
            has_trailing_newline,
        } = self.selected_line_block_plan()?;

        let lines = (first_line..=last_line)
            .filter_map(|line| {
                self.buffer
                    .line(line)
                    .map(|text| text.trim_end_matches('\n').to_string())
            })
            .collect::<Vec<_>>();
        let transformed_lines = transform(&lines);
        let replacement = if has_trailing_newline {
            format!("{}\n", transformed_lines.join("\n"))
        } else {
            transformed_lines.join("\n")
        };

        Some(TextReplacementEdit {
            range: byte_range,
            replacement,
        })
    }

    pub fn plan_multi_cursor_replace_edits(&self, text: &str) -> Vec<MultiCursorEditPlan> {
        let mut edits = Vec::new();

        for (slot, cursor, selection) in self.cursor_entries() {
            if let Some(range) = self.selection_byte_range(selection) {
                edits.push(MultiCursorEditPlan {
                    slot,
                    start: range.start,
                    end: range.end,
                    replacement: text.to_string(),
                });
            } else {
                let offset = cursor.offset(self.buffer);
                edits.push(MultiCursorEditPlan {
                    slot,
                    start: offset,
                    end: offset,
                    replacement: text.to_string(),
                });
            }
        }

        edits.sort_by(|left, right| {
            right
                .start
                .cmp(&left.start)
                .then(right.end.cmp(&left.end))
                .then(left.slot.cmp(&right.slot))
        });
        edits.dedup_by(|left, right| left.start == right.start && left.end == right.end);
        edits
    }

    pub fn plan_multi_cursor_insert_edits(&self, text: &str) -> Vec<MultiCursorEditPlan> {
        let mut edits = self
            .multi_cursor_caret_offsets()
            .into_iter()
            .map(|(slot, offset)| MultiCursorEditPlan {
                slot,
                start: offset,
                end: offset,
                replacement: text.to_string(),
            })
            .collect::<Vec<_>>();

        edits.sort_by(|left, right| {
            right
                .start
                .cmp(&left.start)
                .then(right.end.cmp(&left.end))
                .then(left.slot.cmp(&right.slot))
        });
        edits.dedup_by(|left, right| left.start == right.start && left.end == right.end);
        edits
    }

    pub fn plan_multi_cursor_backspace_edits(&self) -> Vec<MultiCursorEditPlan> {
        let mut edits = Vec::new();

        for (slot, cursor, selection) in self.cursor_entries() {
            if selection.has_selection() {
                if let Some(range) = self.selection_byte_range(selection) {
                    edits.push(MultiCursorEditPlan {
                        slot,
                        start: range.start,
                        end: range.end,
                        replacement: String::new(),
                    });
                }
                continue;
            }

            let offset = cursor.offset(self.buffer);
            if offset == 0 {
                continue;
            }

            let Ok(previous_boundary) = self.buffer.previous_char_boundary(offset) else {
                continue;
            };

            edits.push(MultiCursorEditPlan {
                slot,
                start: previous_boundary,
                end: offset,
                replacement: String::new(),
            });
        }

        edits
    }

    pub fn plan_multi_cursor_delete_edits(&self) -> Vec<MultiCursorEditPlan> {
        let mut edits = Vec::new();

        for (slot, cursor, selection) in self.cursor_entries() {
            if selection.has_selection() {
                if let Some(range) = self.selection_byte_range(selection) {
                    edits.push(MultiCursorEditPlan {
                        slot,
                        start: range.start,
                        end: range.end,
                        replacement: String::new(),
                    });
                }
                continue;
            }

            let offset = cursor.offset(self.buffer);
            if offset >= self.buffer.len() {
                continue;
            }

            let Ok(next_boundary) = self.buffer.next_char_boundary(offset) else {
                continue;
            };

            edits.push(MultiCursorEditPlan {
                slot,
                start: offset,
                end: next_boundary,
                replacement: String::new(),
            });
        }

        edits
    }

    pub fn plan_multi_cursor_newline_edits<F>(
        &self,
        mut replacement_for_line: F,
    ) -> Vec<MultiCursorEditPlan>
    where
        F: FnMut(usize) -> String,
    {
        let mut edits = Vec::new();

        for (slot, cursor, selection) in self.cursor_entries() {
            let replacement = replacement_for_line(cursor.position().line);

            if selection.has_selection() {
                if let Some(range) = self.selection_byte_range(selection) {
                    edits.push(MultiCursorEditPlan {
                        slot,
                        start: range.start,
                        end: range.end,
                        replacement,
                    });
                }
                continue;
            }

            let offset = cursor.offset(self.buffer);
            edits.push(MultiCursorEditPlan {
                slot,
                start: offset,
                end: offset,
                replacement,
            });
        }

        edits
    }

    pub fn plan_multi_cursor_indent_edits(&self, indent: &str) -> Vec<MultiCursorEditPlan> {
        let mut edits = self
            .multi_cursor_caret_offsets()
            .into_iter()
            .map(|(slot, offset)| MultiCursorEditPlan {
                slot,
                start: offset,
                end: offset,
                replacement: indent.to_string(),
            })
            .collect::<Vec<_>>();

        edits.sort_by(|left, right| {
            right
                .start
                .cmp(&left.start)
                .then(right.end.cmp(&left.end))
                .then(left.slot.cmp(&right.slot))
        });
        edits.dedup_by(|left, right| left.start == right.start && left.end == right.end);
        edits
    }

    pub fn plan_rotated_selection_edits<F>(&self, mut fallback: F) -> Vec<SelectionRotationEdit>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        let mut ranges = self.selection_ranges_with_fallback(&mut fallback);
        ranges.sort_by_key(|range| range.start);

        if ranges.len() <= 1 {
            return Vec::new();
        }

        let texts = ranges
            .iter()
            .map(|range| {
                self.buffer
                    .text_for_range(range.clone())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();

        let count = texts.len();
        let rotated = (0..count)
            .map(|index| texts[(index + count - 1) % count].clone())
            .collect::<Vec<_>>();

        let mut edits = ranges
            .into_iter()
            .zip(rotated)
            .map(|(range, replacement)| SelectionRotationEdit { range, replacement })
            .collect::<Vec<_>>();
        edits.sort_by(|left, right| {
            right
                .range
                .start
                .cmp(&left.range.start)
                .then(right.range.end.cmp(&left.range.end))
        });
        edits
    }

    pub fn plan_rotated_text_replacements(&self) -> Vec<TextReplacementEdit> {
        self.plan_rotated_selection_edits(|position| {
            let offset = self.buffer.position_to_offset(position).ok()?;
            Some(
                self.find_word_range_at_offset(offset)
                    .unwrap_or(offset..offset),
            )
        })
        .into_iter()
        .map(TextReplacementEdit::from)
        .collect()
    }

    pub fn plan_primary_selection_deletion(&self) -> Option<PrimarySelectionDeletionPlan> {
        if !self.selection.has_selection() {
            return None;
        }

        if self.selection.is_block() {
            let block_ranges = self.selection.block_ranges();
            let &(top_line, left_column, _) = block_ranges.first()?;
            let mut ranges = Vec::new();

            for &(line, start_column, end_column) in block_ranges.iter().rev() {
                let start = self
                    .buffer
                    .position_to_offset(Position::new(line, start_column))
                    .ok()?;
                let end = self
                    .buffer
                    .position_to_offset(Position::new(line, end_column))
                    .ok()?;

                if start < end {
                    ranges.push(start..end);
                }
            }

            if ranges.is_empty() {
                return None;
            }

            return Some(PrimarySelectionDeletionPlan::Block {
                ranges,
                cursor_position: Position::new(top_line, left_column),
            });
        }

        let range = self.selection_byte_range(&self.selection)?;
        Some(PrimarySelectionDeletionPlan::Linear {
            range,
            cursor_position: self.selection.range().start,
        })
    }

    pub fn primary_selection_deletion_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.plan_primary_selection_deletion()?;

        match plan {
            PrimarySelectionDeletionPlan::Linear {
                range,
                cursor_position,
            } => Some(PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range,
                    replacement: String::new(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(cursor_position),
            }),
            PrimarySelectionDeletionPlan::Block {
                ranges,
                cursor_position,
            } => Some(PlannedEditBatch {
                edits: ranges
                    .into_iter()
                    .map(|range| TextReplacementEdit {
                        range,
                        replacement: String::new(),
                    })
                    .collect(),
                post_apply_selection: PostApplySelection::MovePrimaryCursor(cursor_position),
            }),
        }
    }

    pub fn selected_line_range(&self) -> (usize, usize) {
        if self.selection.has_selection() {
            let range = self.selection.range();
            (range.start.line, range.end.line)
        } else {
            let line = self.cursor.position().line;
            (line, line)
        }
    }

    pub fn primary_selection_byte_range(&self) -> Option<std::ops::Range<usize>> {
        if !self.selection.has_selection() {
            return None;
        }

        self.selection_byte_range(&self.selection)
    }

    pub fn primary_replacement_byte_range(
        &self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
    ) -> std::ops::Range<usize> {
        explicit_range
            .or(marked_range)
            .or_else(|| self.primary_selection_byte_range())
            .unwrap_or_else(|| {
                let offset = self.cursor.offset(self.buffer);
                offset..offset
            })
    }

    pub fn primary_text_replacement_plan(
        &self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
        new_text: &str,
    ) -> PrimaryReplacementPlan {
        let replace_range = self.primary_replacement_byte_range(explicit_range, marked_range);
        let target_offset = replace_range.start + new_text.len();

        PrimaryReplacementPlan {
            batch: PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: replace_range,
                    replacement: new_text.to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(target_offset),
            },
        }
    }

    pub fn marked_text_replacement_plan(
        &self,
        explicit_range: Option<std::ops::Range<usize>>,
        marked_range: Option<std::ops::Range<usize>>,
        new_text: &str,
        selected_offset_within_marked_text: Option<usize>,
    ) -> MarkedTextReplacementPlan {
        let replace_range = self.primary_replacement_byte_range(explicit_range, marked_range);
        let insert_at = replace_range.start;
        let marked_end = insert_at + new_text.len();
        let target_offset = selected_offset_within_marked_text
            .map(|offset| (insert_at + offset).min(marked_end))
            .unwrap_or(marked_end);

        MarkedTextReplacementPlan {
            batch: PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: replace_range,
                    replacement: new_text.to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(target_offset),
            },
            marked_range: (!new_text.is_empty()).then_some(insert_at..marked_end),
        }
    }

    pub fn primary_selection_or_fallback_range<F>(
        &self,
        fallback: &mut F,
    ) -> Option<std::ops::Range<usize>>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        self.selection_range_for_cursor(&self.cursor, &self.selection, fallback)
    }

    pub fn primary_selection_contains(&self, position: Position) -> bool {
        self.selection.has_selection() && {
            let range = self.selection.range();
            position >= range.start && position <= range.end
        }
    }

    pub fn selected_line_block_plan(&self) -> Option<SelectedLineBlockPlan> {
        let (first_line, last_line) = self.selected_line_range();
        let line_count = self.buffer.line_count();
        let start = self
            .buffer
            .position_to_offset(Position::new(first_line, 0))
            .ok()?;

        if last_line + 1 < line_count {
            let end = self
                .buffer
                .position_to_offset(Position::new(last_line + 1, 0))
                .ok()?;
            Some(SelectedLineBlockPlan {
                first_line,
                last_line,
                byte_range: start..end,
                has_trailing_newline: true,
            })
        } else {
            Some(SelectedLineBlockPlan {
                first_line,
                last_line,
                byte_range: start..self.buffer.len(),
                has_trailing_newline: false,
            })
        }
    }

    pub fn selected_line_deletion_plan(&self) -> Option<SelectedLineDeletionPlan> {
        let block = self.selected_line_block_plan()?;
        let range = if block.has_trailing_newline {
            block.byte_range
        } else if block.first_line > 0 {
            block.byte_range.start.saturating_sub(1)..block.byte_range.end
        } else {
            block.byte_range
        };

        Some(SelectedLineDeletionPlan {
            first_line: block.first_line,
            byte_range: range,
            target_position: Position::new(block.first_line, 0),
        })
    }

    pub fn duplicate_selected_lines_up_plan(&self) -> Option<DuplicateSelectedLinesPlan> {
        let block = self.selected_line_block_plan()?;
        let mut text = self.selected_line_block_text()?;
        if !block.has_trailing_newline {
            text.push('\n');
        }
        let target_line = block.first_line + (block.last_line - block.first_line + 1);
        let target_column = self.cursor.position().column.min(
            self.buffer
                .line(target_line)
                .map(|line| line.len())
                .unwrap_or(0),
        );

        Some(DuplicateSelectedLinesPlan {
            insertions: vec![TextInsertionPlan {
                offset: block.byte_range.start,
                text,
            }],
            target_line,
            target_position: Position::new(target_line, target_column),
        })
    }

    pub fn duplicate_selected_lines_down_plan(&self) -> Option<DuplicateSelectedLinesPlan> {
        let block = self.selected_line_block_plan()?;
        let text = self.selected_line_block_text()?;
        let target_line = block.first_line + (block.last_line - block.first_line + 1);

        let insertions = if block.has_trailing_newline {
            vec![TextInsertionPlan {
                offset: block.byte_range.end,
                text,
            }]
        } else {
            let end = self.buffer.len();
            vec![
                TextInsertionPlan {
                    offset: end,
                    text: "\n".to_string(),
                },
                TextInsertionPlan {
                    offset: end + 1,
                    text: format!("{}\n", text),
                },
            ]
        };

        Some(DuplicateSelectedLinesPlan {
            insertions,
            target_line,
            target_position: Position::new(target_line, self.cursor.position().column),
        })
    }

    pub fn move_selected_lines_up_plan(&self) -> Option<MoveSelectedLinesPlan> {
        let block = self.selected_line_block_plan()?;
        if block.first_line == 0 {
            return None;
        }

        let above_line = self.buffer.line(block.first_line - 1)?;
        let region_start = self
            .buffer
            .position_to_offset(Position::new(block.first_line - 1, 0))
            .ok()?;
        let replacement = format!("{}{}", self.selected_line_block_text()?, above_line);
        let target_line = self.cursor.position().line.saturating_sub(1);
        let target_column = self.cursor.position().column.min(
            self.buffer
                .line(target_line)
                .map(|line| line.len())
                .unwrap_or(0),
        );

        Some(MoveSelectedLinesPlan {
            byte_range: region_start..block.byte_range.end,
            replacement,
            target_line,
            target_position: Position::new(target_line, target_column),
        })
    }

    pub fn move_selected_lines_down_plan(&self) -> Option<MoveSelectedLinesPlan> {
        let block = self.selected_line_block_plan()?;
        if block.last_line + 1 >= self.buffer.line_count() {
            return None;
        }

        let below_line = self.buffer.line(block.last_line + 1)?;
        let below_line_end = self
            .buffer
            .position_to_offset(Position::new(block.last_line + 1, below_line.len()))
            .ok()?;
        let block_text = self.selected_line_block_text()?;
        let mut replacement = format!("{}\n{}", below_line, block_text.trim_end_matches('\n'));
        if block.has_trailing_newline {
            replacement.push('\n');
        }
        let target_line =
            (self.cursor.position().line + 1).min(self.buffer.line_count().saturating_sub(1));
        let target_column = self.cursor.position().column.min(
            self.buffer
                .line(target_line)
                .map(|line| line.len())
                .unwrap_or(0),
        );

        Some(MoveSelectedLinesPlan {
            byte_range: block.byte_range.start..below_line_end,
            replacement,
            target_line,
            target_position: Position::new(target_line, target_column),
        })
    }

    pub fn move_selected_lines_up_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.move_selected_lines_up_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.byte_range,
                replacement: plan.replacement,
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn move_selected_lines_down_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.move_selected_lines_down_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.byte_range,
                replacement: plan.replacement,
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn duplicate_selected_lines_up_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.duplicate_selected_lines_up_plan()?;
        Some(PlannedEditBatch {
            edits: plan
                .insertions
                .into_iter()
                .map(|insertion| TextReplacementEdit::insert(insertion.offset, insertion.text))
                .collect(),
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn duplicate_selected_lines_down_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.duplicate_selected_lines_down_plan()?;
        Some(PlannedEditBatch {
            edits: plan
                .insertions
                .into_iter()
                .map(|insertion| TextReplacementEdit::insert(insertion.offset, insertion.text))
                .collect(),
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn selected_line_deletion_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.selected_line_deletion_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.byte_range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn whole_line_copy_plan(&self) -> Option<WholeLineCopyPlan> {
        let line = self.cursor.position().line;
        let start = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .ok()?;
        let end = if line + 1 < self.buffer.line_count() {
            self.buffer
                .position_to_offset(Position::new(line + 1, 0))
                .ok()?
        } else {
            self.buffer.len()
        };
        let text = self.buffer.text_for_range(start..end).ok()?;

        Some(WholeLineCopyPlan { text })
    }

    pub fn whole_line_cut_plan(&self) -> Option<WholeLineCutPlan> {
        let line = self.cursor.position().line;
        let text = self.whole_line_copy_plan()?.text;
        let start = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .ok()?;
        let delete_end = if line + 1 < self.buffer.line_count() {
            self.buffer
                .position_to_offset(Position::new(line + 1, 0))
                .ok()?
        } else {
            start + text.len()
        };

        Some(WholeLineCutPlan {
            text,
            delete_range: start..delete_end,
            target_line: line.min(self.buffer.line_count().saturating_sub(1)),
            target_position: Position::new(line.min(self.buffer.line_count().saturating_sub(1)), 0),
        })
    }

    pub fn whole_line_cut_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.whole_line_cut_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.delete_range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn whole_line_paste_plan(&self, clipboard_text: &str) -> Option<WholeLinePastePlan> {
        let line = self.cursor.position().line;
        let offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .ok()?;
        let text = if clipboard_text.ends_with('\n') {
            clipboard_text.to_string()
        } else {
            format!("{}\n", clipboard_text)
        };

        Some(WholeLinePastePlan {
            offset,
            text,
            target_line: line + 1,
            target_position: Position::new(line + 1, self.cursor.position().column),
        })
    }

    pub fn whole_line_paste_edit_batch(&self, clipboard_text: &str) -> Option<PlannedEditBatch> {
        let plan = self.whole_line_paste_plan(clipboard_text)?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit::insert(plan.offset, plan.text)],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn insert_newline_above_plan(&self) -> Option<NewlineInsertionPlan> {
        let line = self.cursor.position().line;
        let indent = self.leading_whitespace_for_line(line)?;
        let offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .ok()?;

        Some(NewlineInsertionPlan {
            offset,
            text: format!("{}\n", indent),
            target_position: Position::new(line, indent.len()),
        })
    }

    pub fn insert_newline_above_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.insert_newline_above_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit::insert(plan.offset, plan.text)],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn insert_newline_below_plan(&self) -> Option<NewlineInsertionPlan> {
        let line = self.cursor.position().line;
        let indent = self.leading_whitespace_for_line(line)?;
        let line_length = self.buffer.line(line)?.len();
        let offset = self
            .buffer
            .position_to_offset(Position::new(line, line_length))
            .ok()?;

        Some(NewlineInsertionPlan {
            offset,
            text: format!("\n{}", indent),
            target_position: Position::new(line + 1, indent.len()),
        })
    }

    pub fn insert_newline_below_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.insert_newline_below_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit::insert(plan.offset, plan.text)],
            post_apply_selection: PostApplySelection::MovePrimaryCursor(plan.target_position),
        })
    }

    pub fn cut_to_end_of_line_plan(&self) -> Option<CutToEndOfLinePlan> {
        let position = self.cursor.position();
        let line_text = self.buffer.line(position.line)?;
        let line_end_column = line_text.len();
        let cursor_offset = self.buffer.position_to_offset(position).ok()?;

        if position.column < line_end_column {
            let end_offset = self
                .buffer
                .position_to_offset(Position::new(position.line, line_end_column))
                .ok()?;
            let text = self.buffer.text_for_range(cursor_offset..end_offset).ok()?;

            return Some(CutToEndOfLinePlan {
                text,
                delete_range: cursor_offset..end_offset,
            });
        }

        if position.line + 1 >= self.buffer.line_count() {
            return None;
        }

        let next_line_offset = self
            .buffer
            .position_to_offset(Position::new(position.line + 1, 0))
            .ok()?;

        Some(CutToEndOfLinePlan {
            text: "\n".to_string(),
            delete_range: cursor_offset..next_line_offset,
        })
    }

    pub fn cut_to_end_of_line_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.cut_to_end_of_line_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.delete_range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::Keep,
        })
    }

    pub fn join_lines_plan(&self) -> Option<JoinLinesPlan> {
        let line = self.cursor.position().line;
        if line + 1 >= self.buffer.line_count() {
            return None;
        }

        let line_text = self.buffer.line(line)?;
        let line_end_offset = self
            .buffer
            .position_to_offset(Position::new(line, line_text.len()))
            .ok()?;

        Some(JoinLinesPlan {
            delete_range: line_end_offset..line_end_offset + 1,
            insert_offset: line_end_offset,
            insert_text: " ".to_string(),
            target_offset: line_end_offset + 1,
        })
    }

    pub fn join_lines_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.join_lines_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.delete_range,
                replacement: plan.insert_text,
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn transpose_chars_plan(&self) -> Option<TransposeCharsPlan> {
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let cursor_offset = self.buffer.ceil_char_boundary(cursor_offset);
        let buffer_length = self.buffer.len();

        if buffer_length < 2 {
            return None;
        }

        let (before_start, middle, after_end) = if cursor_offset >= buffer_length {
            let middle = self.buffer.previous_char_boundary(buffer_length).ok()?;
            let before_start = self.buffer.previous_char_boundary(middle).ok()?;
            (before_start, middle, buffer_length)
        } else {
            if cursor_offset == 0 {
                return None;
            }

            let before_start = self.buffer.previous_char_boundary(cursor_offset).ok()?;
            let after_end = self.buffer.next_char_boundary(cursor_offset).ok()?;

            (before_start, cursor_offset, after_end)
        };

        let before_character = self.buffer.text_for_range(before_start..middle).ok()?;
        let after_character = self.buffer.text_for_range(middle..after_end).ok()?;
        let replacement = format!("{}{}", after_character, before_character);
        let target_offset = before_start + replacement.len();

        Some(TransposeCharsPlan {
            replace_range: before_start..after_end,
            replacement,
            target_offset,
        })
    }

    pub fn transpose_chars_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.transpose_chars_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.replace_range,
                replacement: plan.replacement,
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn line_prefix_edit_batch(
        &self,
        indent_size: usize,
        use_tabs: bool,
        mode: LinePrefixEditMode,
    ) -> Option<PlannedEditBatch> {
        match mode {
            LinePrefixEditMode::Indent => {
                let edits = self
                    .indent_lines_plan(indent_size, use_tabs)?
                    .edits
                    .into_iter()
                    .map(|edit| TextReplacementEdit::insert(edit.offset, edit.text))
                    .collect();

                Some(PlannedEditBatch {
                    edits,
                    post_apply_selection: PostApplySelection::Keep,
                })
            }
            LinePrefixEditMode::Dedent => {
                let plan = self.dedent_lines_plan(indent_size)?;
                Some(PlannedEditBatch {
                    edits: plan
                        .edits
                        .into_iter()
                        .map(|edit| TextReplacementEdit {
                            range: edit.range,
                            replacement: String::new(),
                        })
                        .collect(),
                    post_apply_selection: PostApplySelection::MovePrimaryCursor(
                        plan.target_position,
                    ),
                })
            }
            LinePrefixEditMode::ToggleComment => {
                let edits = self
                    .toggle_line_comment_plan()?
                    .edits
                    .into_iter()
                    .map(|edit| match edit {
                        LineCommentEdit::Insert { offset, text } => {
                            TextReplacementEdit::insert(offset, text)
                        }
                        LineCommentEdit::Delete { range } => TextReplacementEdit {
                            range,
                            replacement: String::new(),
                        },
                    })
                    .collect();

                Some(PlannedEditBatch {
                    edits,
                    post_apply_selection: PostApplySelection::Keep,
                })
            }
        }
    }

    pub fn indent_lines_plan(&self, indent_size: usize, use_tabs: bool) -> Option<IndentLinesPlan> {
        let (first_line, last_line) = self.selected_line_range();
        let indent_text = if use_tabs {
            "\t".to_string()
        } else {
            " ".repeat(indent_size)
        };

        let edits = (first_line..=last_line)
            .filter_map(|line| {
                self.buffer
                    .position_to_offset(Position::new(line, 0))
                    .ok()
                    .map(|offset| LineIndentEdit {
                        offset,
                        text: indent_text.clone(),
                    })
            })
            .collect::<Vec<_>>();

        Some(IndentLinesPlan { edits })
    }

    pub fn dedent_lines_plan(&self, indent_size: usize) -> Option<DedentLinesPlan> {
        let (first_line, last_line) = self.selected_line_range();
        let mut edits = Vec::new();

        for line in first_line..=last_line {
            let line_start = self
                .buffer
                .position_to_offset(Position::new(line, 0))
                .ok()?;
            let line_text = self.buffer.line(line)?;
            let remove_count = if line_text.starts_with('\t') {
                1
            } else {
                line_text
                    .chars()
                    .take(indent_size)
                    .take_while(|&character| character == ' ')
                    .count()
            };

            if remove_count > 0 {
                edits.push(LineDedentEdit {
                    range: line_start..line_start + remove_count,
                });
            }
        }

        let cursor_line = self.cursor.position().line;
        let removed_on_cursor_line = edits
            .iter()
            .find(|edit| {
                self.buffer
                    .position_to_offset(Position::new(cursor_line, 0))
                    .ok()
                    .map(|line_start| edit.range.start == line_start)
                    .unwrap_or(false)
            })
            .map(|edit| edit.range.len())
            .unwrap_or(0);
        let target_column = self
            .cursor
            .position()
            .column
            .saturating_sub(removed_on_cursor_line);

        Some(DedentLinesPlan {
            edits,
            target_position: Position::new(cursor_line, target_column),
        })
    }

    pub fn toggle_line_comment_plan(&self) -> Option<ToggleLineCommentPlan> {
        let (first_line, last_line) = self.selected_line_range();
        let all_commented = (first_line..=last_line).all(|line| {
            self.buffer
                .line(line)
                .map(|text| text.trim_start().starts_with("--"))
                .unwrap_or(false)
        });

        let mut edits = Vec::new();

        if all_commented {
            for line in first_line..=last_line {
                let line_start = self
                    .buffer
                    .position_to_offset(Position::new(line, 0))
                    .ok()?;
                let line_text = self.buffer.line(line)?;
                let leading_whitespace = line_text
                    .chars()
                    .take_while(|character| character.is_whitespace())
                    .count();
                let after_whitespace = line_start + leading_whitespace;
                let trimmed = &line_text[leading_whitespace..];
                let strip_length = if trimmed.starts_with("-- ") {
                    3
                } else if trimmed.starts_with("--") {
                    2
                } else {
                    0
                };

                if strip_length > 0 {
                    edits.push(LineCommentEdit::Delete {
                        range: after_whitespace..after_whitespace + strip_length,
                    });
                }
            }
        } else {
            for line in (first_line..=last_line).rev() {
                let line_start = self
                    .buffer
                    .position_to_offset(Position::new(line, 0))
                    .ok()?;
                let line_text = self.buffer.line(line)?;
                let leading_whitespace = line_text
                    .chars()
                    .take_while(|character| character.is_whitespace())
                    .count();
                edits.push(LineCommentEdit::Insert {
                    offset: line_start + leading_whitespace,
                    text: "-- ".to_string(),
                });
            }
        }

        Some(ToggleLineCommentPlan { edits })
    }

    pub fn auto_indent_newline_text(
        &self,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<AutoIndentNewlinePlan> {
        let line = self.cursor.position().line;
        self.auto_indent_newline_text_for_line(line, auto_indent_enabled, indent_unit)
    }

    pub fn auto_indent_newline_text_for_line(
        &self,
        line: usize,
        auto_indent_enabled: bool,
        indent_unit: &str,
    ) -> Option<AutoIndentNewlinePlan> {
        if !auto_indent_enabled {
            return Some(AutoIndentNewlinePlan {
                text: "\n".to_string(),
            });
        }

        let indent = self.leading_whitespace_for_line(line)?;
        let extra_indent = if let Some(line_text) = self.buffer.line(line) {
            let trimmed = line_text.trim_end().to_uppercase();
            let last_word = trimmed
                .rsplit_once(char::is_whitespace)
                .map(|(_, word)| word)
                .unwrap_or(&trimmed);

            if matches!(
                last_word,
                "BEGIN" | "THEN" | "ELSE" | "LOOP" | "AS" | "DECLARE" | "("
            ) {
                indent_unit.to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        Some(AutoIndentNewlinePlan {
            text: format!("\n{}{}", indent, extra_indent),
        })
    }

    pub fn insert_at_cursor_plan(&self, text: &str) -> Option<InsertAtCursorPlan> {
        let offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        Some(InsertAtCursorPlan {
            offset,
            target_offset: offset + text.len(),
        })
    }

    pub fn insert_at_cursor_edit_batch(&self, text: &str) -> Option<PlannedEditBatch> {
        let plan = self.insert_at_cursor_plan(text)?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit::insert(plan.offset, text)],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn insert_text_plan(&self, text: &str) -> Option<InsertTextPlan> {
        let byte_range = if self.selection.has_selection() {
            self.selection_byte_range(&self.selection)?
        } else {
            let offset = self
                .buffer
                .position_to_offset(self.cursor.position())
                .ok()?;
            offset..offset
        };

        let buffer_length = self.buffer.len();
        let replace_range = byte_range.start.min(buffer_length)..byte_range.end.min(buffer_length);
        let insert_offset = replace_range.start;
        let replaced_text = self.buffer.text_for_range(replace_range.clone()).ok()?;

        Some(InsertTextPlan {
            replace_range,
            insert_offset,
            replaced_text,
            inserted_text: text.to_string(),
            target_offset: insert_offset + text.len(),
        })
    }

    pub fn insert_text_edit_batch(&self, text: &str) -> Option<PlannedEditBatch> {
        let plan = self.insert_text_plan(text)?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.replace_range,
                replacement: plan.inserted_text,
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn delete_before_cursor_plan<F>(&self, bracket_closer: F) -> Option<DeleteBeforeCursorPlan>
    where
        F: Fn(char) -> Option<char>,
    {
        let cursor_position = self.cursor.position();
        if cursor_position.line == 0 && cursor_position.column == 0 {
            return None;
        }

        let cursor_offset = self.buffer.position_to_offset(cursor_position).ok()?;
        if cursor_offset == 0 {
            return None;
        }

        let previous_offset = self.buffer.previous_char_boundary(cursor_offset).ok()?;
        let opener = self.buffer.char_at(previous_offset);
        let paired_closer_range = opener
            .and_then(&bracket_closer)
            .filter(|closer| self.buffer.char_at(cursor_offset) == Some(*closer))
            .map(|closer| cursor_offset..cursor_offset + closer.len_utf8());

        Some(DeleteBeforeCursorPlan {
            primary_range: previous_offset..cursor_offset,
            paired_closer_range,
            target_offset: previous_offset,
        })
    }

    pub fn delete_before_cursor_edit_batch<F>(&self, bracket_closer: F) -> Option<PlannedEditBatch>
    where
        F: Fn(char) -> Option<char>,
    {
        let plan = self.delete_before_cursor_plan(bracket_closer)?;
        let mut edits = vec![TextReplacementEdit {
            range: plan.primary_range,
            replacement: String::new(),
        }];
        if let Some(range) = plan.paired_closer_range {
            edits.push(TextReplacementEdit {
                range,
                replacement: String::new(),
            });
        }

        Some(PlannedEditBatch {
            edits,
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn delete_at_cursor_plan(&self) -> Option<DeleteAtCursorPlan> {
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        if cursor_offset >= self.buffer.len() {
            return None;
        }

        let next_offset = self.buffer.next_char_boundary(cursor_offset).ok()?;
        Some(DeleteAtCursorPlan {
            range: cursor_offset..next_offset,
        })
    }

    pub fn delete_at_cursor_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.delete_at_cursor_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::Keep,
        })
    }

    pub fn delete_subword_left_plan(&self) -> Option<DeleteSubwordPlan> {
        let offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let start = Self::prev_subword_start_in_buffer(self.buffer, offset);
        if start >= offset {
            return None;
        }

        Some(DeleteSubwordPlan {
            range: start..offset,
            target_offset: start,
        })
    }

    pub fn delete_subword_left_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.delete_subword_left_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn delete_subword_right_plan(&self) -> Option<DeleteSubwordPlan> {
        let offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let end = Self::next_subword_end_in_buffer(self.buffer, offset);
        if end <= offset {
            return None;
        }

        Some(DeleteSubwordPlan {
            range: offset..end,
            target_offset: offset,
        })
    }

    pub fn delete_subword_right_edit_batch(&self) -> Option<PlannedEditBatch> {
        let plan = self.delete_subword_right_plan()?;
        Some(PlannedEditBatch {
            edits: vec![TextReplacementEdit {
                range: plan.range,
                replacement: String::new(),
            }],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(plan.target_offset),
        })
    }

    pub fn selected_text_plan(&self) -> Option<SelectedTextPlan> {
        if !self.selection.has_selection() {
            return None;
        }

        if self.selection.is_block() {
            let block_ranges = self.selection.block_ranges();
            if block_ranges.is_empty() {
                return None;
            }

            let mut lines = Vec::with_capacity(block_ranges.len());
            for &(line, start_column, end_column) in &block_ranges {
                if let Some(line_text) = self.buffer.line(line) {
                    let safe_start = start_column.min(line_text.len());
                    let safe_end = end_column.min(line_text.len());
                    lines.push(line_text[safe_start..safe_end].to_string());
                } else {
                    lines.push(String::new());
                }
            }

            return Some(SelectedTextPlan::Block(lines));
        }

        let range = self.selection_byte_range(&self.selection)?;
        let selected = self.buffer.text_for_range(range).ok()?;
        Some(SelectedTextPlan::Linear(selected))
    }

    pub fn auto_surround_selection_plan(
        &self,
        opener: char,
        closer: char,
    ) -> Option<AutoSurroundSelectionPlan> {
        if !self.selection.has_selection() {
            return None;
        }

        let range = self.selection.range();
        let start_offset = self.buffer.position_to_offset(range.start).ok()?;
        let end_offset = self.buffer.position_to_offset(range.end).ok()?;

        let new_start_column = range.start.column + opener.len_utf8();
        let new_end_column = if range.start.line == range.end.line {
            range.end.column + opener.len_utf8()
        } else {
            range.end.column
        };

        let edits = vec![
            TextReplacementEdit::insert(end_offset, closer.to_string()),
            TextReplacementEdit::insert(start_offset, opener.to_string()),
        ];

        Some(AutoSurroundSelectionPlan {
            edits,
            selection: Selection::from_anchor_head(
                Position::new(range.start.line, new_start_column),
                Position::new(range.end.line, new_end_column),
            ),
        })
    }

    pub fn auto_close_bracket_plan(
        &self,
        opener: char,
        closer: char,
        is_safe_position: bool,
        inside_string_or_comment: bool,
    ) -> Option<AutoCloseBracketPlan> {
        if !is_safe_position {
            return None;
        }

        let is_quote = opener == '\'' || opener == '"';
        if is_quote && inside_string_or_comment {
            return None;
        }

        let offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;

        let batch = PlannedEditBatch {
            edits: vec![TextReplacementEdit::insert(
                offset,
                format!("{}{}", opener, closer),
            )],
            post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(
                offset + opener.len_utf8(),
            ),
        };

        Some(AutoCloseBracketPlan { batch })
    }

    pub fn skip_closing_bracket_plan(&self, closer: char) -> Option<SkipClosingBracketPlan> {
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let next_character = self.buffer.char_at(cursor_offset)?;
        if next_character != closer {
            return None;
        }

        Some(SkipClosingBracketPlan {
            target_offset: cursor_offset + closer.len_utf8(),
        })
    }

    pub fn find_all_occurrences(&self, needle: &str) -> Vec<std::ops::Range<usize>> {
        if needle.is_empty() {
            return Vec::new();
        }

        SearchEngine::new(
            needle,
            &TextFindOptions {
                case_sensitive: true,
                whole_word: false,
                regex: false,
            },
        )
        .map(|engine| {
            engine
                .find_all_in_rope(&self.buffer.rope())
                .into_iter()
                .map(|matched| matched.range())
                .collect()
        })
        .unwrap_or_default()
    }

    pub fn find_word_range_at_offset(&self, offset: usize) -> Option<std::ops::Range<usize>> {
        if offset > self.buffer.len() {
            return None;
        }

        let offset = self.buffer.floor_char_boundary(offset);

        if matches!(self.buffer.char_at(offset), Some('"') | Some('`')) {
            let quote = self.buffer.char_at(offset)?;
            let quote_end = self.buffer.next_char_boundary(offset).ok()?;

            let mut search_offset = quote_end;
            while let Some((start, end, character)) =
                Self::char_at_offset(self.buffer, search_offset)
            {
                if character == quote {
                    return Some(offset..end);
                }
                search_offset = end.max(start + character.len_utf8());
            }

            let mut search_offset = offset;
            while let Some((start, _, character)) =
                Self::char_before_offset(self.buffer, search_offset)
            {
                if character == quote {
                    return Some(start..quote_end);
                }
                search_offset = start;
            }
        }

        fn is_word_char(character: char) -> bool {
            character.is_alphanumeric() || character == '_'
        }

        let mut start = offset;
        while let Some((previous_start, _, character)) =
            Self::char_before_offset(self.buffer, start)
        {
            if is_word_char(character) {
                start = previous_start;
            } else {
                break;
            }
        }

        let mut end = offset;
        while let Some((_, next_end, character)) = Self::char_at_offset(self.buffer, end) {
            if is_word_char(character) {
                end = next_end;
            } else {
                break;
            }
        }

        (start < end).then_some(start..end)
    }

    pub fn selection_or_word_under_cursor_text(&self) -> Option<String> {
        if let Some(SelectedTextPlan::Linear(text)) = self.selected_text_plan() {
            return Some(text);
        }

        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .ok()?;
        let range = self.find_word_range_at_offset(cursor_offset)?;
        self.buffer.text_for_range(range).ok()
    }

    pub fn completion_query_plan(&self) -> CompletionQueryPlan {
        let cursor_offset = self.cursor.offset(self.buffer);
        let trigger_offset = self
            .find_word_range_at_offset(cursor_offset)
            .map(|range| range.start)
            .unwrap_or(cursor_offset);
        let current_prefix = self
            .buffer
            .text_for_range(trigger_offset..cursor_offset)
            .unwrap_or_default();

        CompletionQueryPlan {
            trigger_offset,
            current_prefix,
        }
    }

    pub fn word_target_at_offset(&self, offset: usize) -> Option<WordTarget> {
        let range = self.find_word_range_at_offset(offset)?;
        let text = self.buffer.text_for_range(range.clone()).ok()?;
        Some(WordTarget { range, text })
    }

    pub fn rename_target_at_cursor(&self) -> Option<WordTarget> {
        let cursor_offset = self.cursor.offset(self.buffer);
        self.word_target_at_offset(cursor_offset)
    }

    pub fn signature_help_query_plan(
        &self,
        structural_open_paren: Option<usize>,
    ) -> Option<SignatureHelpQueryPlan> {
        let cursor_offset = self.cursor.offset(self.buffer);
        let search_start = cursor_offset.saturating_sub(256);
        let search_end = (cursor_offset + 256).min(self.buffer.len());
        let text = self.buffer.text_for_range(search_start..search_end).ok()?;
        let local_cursor_offset = cursor_offset.saturating_sub(search_start).min(text.len());
        let before_cursor = &text[..local_cursor_offset];

        let mut depth = 0usize;
        let mut call_open_paren = None;
        for (index, character) in before_cursor.char_indices().rev() {
            match character {
                ')' => depth = depth.saturating_add(1),
                '(' => {
                    if depth == 0 {
                        call_open_paren = Some(index);
                        break;
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }

        let open_paren = structural_open_paren
            .map(|offset| offset.saturating_sub(search_start))
            .or(call_open_paren)?;

        let before_paren = before_cursor[..open_paren].trim_end();
        let name_end = before_paren.len();
        let mut name_start = name_end;
        for (index, character) in before_paren.char_indices().rev() {
            if character.is_alphanumeric() || character == '_' {
                name_start = index;
            } else {
                break;
            }
        }
        if name_start >= name_end {
            return None;
        }

        let function_name = before_paren[name_start..name_end].to_string();
        let mut nested = 0usize;
        let mut active_parameter = 0u32;
        for character in before_cursor[open_paren + 1..].chars() {
            match character {
                '(' => nested = nested.saturating_add(1),
                ')' => nested = nested.saturating_sub(1),
                ',' if nested == 0 => active_parameter = active_parameter.saturating_add(1),
                _ => {}
            }
        }

        Some(SignatureHelpQueryPlan {
            function_name,
            active_parameter,
        })
    }

    pub fn rename_query_plan(&self, new_name: &str) -> Option<RenameQueryPlan> {
        if !Self::is_valid_identifier(new_name) {
            return None;
        }

        let WordTarget {
            range: _range,
            text: current_name,
        } = self.rename_target_at_cursor()?;
        if current_name == new_name {
            return None;
        }

        let ranges = Self::find_identifier_occurrences(self.buffer, &current_name);
        if ranges.is_empty() {
            return None;
        }

        Some(RenameQueryPlan {
            current_name,
            ranges,
        })
    }

    pub fn is_multiline_selection(&self) -> bool {
        if !self.selection.has_selection() {
            return false;
        }

        let range = self.selection.range();
        range.start.line != range.end.line
    }

    fn select_offset_range(&mut self, range: std::ops::Range<usize>) -> bool {
        let Some(selection) = self.selection_from_offsets(range.start, range.end) else {
            return false;
        };

        self.selection_history.push(SelectionHistoryEntry::new(
            self.selections_collection.clone(),
        ));
        self.set_primary_cursor_and_selection_fields(selection.head(), selection);
        true
    }

    fn is_valid_identifier(name: &str) -> bool {
        let mut characters = name.chars();
        let Some(first) = characters.next() else {
            return false;
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    }

    fn find_identifier_occurrences(
        buffer: &TextBuffer,
        identifier: &str,
    ) -> Vec<std::ops::Range<usize>> {
        if identifier.is_empty() {
            return Vec::new();
        }

        SearchEngine::new(
            identifier,
            &TextFindOptions {
                case_sensitive: true,
                whole_word: true,
                regex: false,
            },
        )
        .map(|engine| {
            engine
                .find_all_in_rope(&buffer.rope())
                .into_iter()
                .map(|matched| matched.range())
                .collect()
        })
        .unwrap_or_default()
    }

    fn position_for_offset(&self, offset: usize) -> Option<Position> {
        self.buffer
            .offset_to_position(offset)
            .ok()
            .map(|position| self.buffer.clamp_position(position))
    }

    fn selection_from_offsets(
        &self,
        anchor_offset: usize,
        head_offset: usize,
    ) -> Option<Selection> {
        let anchor = self.position_for_offset(anchor_offset)?;
        let head = self.position_for_offset(head_offset)?;
        Some(Selection::from_anchor_head(anchor, head))
    }

    fn selection_from_offsets_preserving_direction(
        &self,
        current_selection: &Selection,
        start_offset: usize,
        end_offset: usize,
    ) -> Option<Selection> {
        let start = self.position_for_offset(start_offset)?;
        let end = self.position_for_offset(end_offset)?;
        Some(if current_selection.is_reversed() {
            Selection::from_anchor_head(end, start)
        } else {
            Selection::from_anchor_head(start, end)
        })
    }

    fn claimed_selection_ranges(&self) -> Vec<std::ops::Range<usize>> {
        self.selection_entries()
            .iter()
            .filter(|entry| entry.selection.has_selection())
            .filter_map(|entry| self.selection_byte_range(&entry.selection))
            .collect()
    }

    fn cursor_entries(&self) -> impl Iterator<Item = (usize, &Cursor, &Selection)> + '_ {
        self.selection_entries()
            .iter()
            .enumerate()
            .map(|(index, entry)| (index, &entry.cursor, &entry.selection))
    }

    fn selection_byte_range(&self, selection: &Selection) -> Option<std::ops::Range<usize>> {
        let range = selection.range();
        let start = self.buffer.position_to_offset(range.start).ok()?;
        let end = self.buffer.position_to_offset(range.end).ok()?;
        Some(start.min(end)..start.max(end))
    }

    fn selection_range_for_cursor<F>(
        &self,
        cursor: &Cursor,
        selection: &Selection,
        fallback: &mut F,
    ) -> Option<std::ops::Range<usize>>
    where
        F: FnMut(Position) -> Option<std::ops::Range<usize>>,
    {
        if selection.has_selection() {
            let range = selection.range();
            let start = self.buffer.position_to_offset(range.start).ok()?;
            let end = self.buffer.position_to_offset(range.end).ok()?;
            Some(start.min(end)..start.max(end))
        } else {
            fallback(cursor.position())
        }
    }

    fn selected_line_block_text(&self) -> Option<String> {
        let block = self.selected_line_block_plan()?;
        self.buffer.text_for_range(block.byte_range).ok()
    }

    fn leading_whitespace_for_line(&self, line: usize) -> Option<String> {
        let text = self.buffer.line(line)?;
        Some(
            text.chars()
                .take_while(|character| character.is_whitespace() && *character != '\n')
                .collect(),
        )
    }

    fn line_selection_end_position(&self, line: usize) -> Position {
        let line_count = self.buffer.line_count();
        if line + 1 < line_count {
            Position::new(line + 1, 0)
        } else {
            let line_length = self.buffer.line(line).map(|text| text.len()).unwrap_or(0);
            Position::new(line, line_length)
        }
    }

    fn char_at_offset(buffer: &TextBuffer, offset: usize) -> Option<(usize, usize, char)> {
        if offset >= buffer.len() {
            return None;
        }

        let start = buffer.floor_char_boundary(offset);
        let end = buffer.next_char_boundary(start).ok()?;
        let character = buffer.char_at(start)?;
        Some((start, end, character))
    }

    fn char_before_offset(buffer: &TextBuffer, offset: usize) -> Option<(usize, usize, char)> {
        if offset == 0 || offset > buffer.len() {
            return None;
        }

        let end = buffer.floor_char_boundary(offset);
        if end == 0 {
            return None;
        }

        let start = buffer.previous_char_boundary(end).ok()?;
        let character = buffer.char_at(start)?;
        Some((start, end, character))
    }

    fn is_subword_lower_or_digit(character: char) -> bool {
        character.is_lowercase() || character.is_ascii_digit()
    }

    fn next_subword_end_in_buffer(buffer: &TextBuffer, offset: usize) -> usize {
        let Some((_, mut current_end, first_character)) = Self::char_at_offset(buffer, offset)
        else {
            return offset.min(buffer.len());
        };

        if first_character == '_' {
            while matches!(Self::char_at_offset(buffer, current_end), Some((_, next_end, '_')) if {
                current_end = next_end;
                true
            }) {}
            return current_end;
        }

        if !first_character.is_alphanumeric() {
            while matches!(
                Self::char_at_offset(buffer, current_end),
                Some((_, next_end, character))
                    if !character.is_alphanumeric() && character != '_' && {
                        current_end = next_end;
                        true
                    }
            ) {}
            return current_end;
        }

        if first_character.is_uppercase() {
            if matches!(
                Self::char_at_offset(buffer, current_end),
                Some((_, _, character)) if character.is_uppercase()
            ) {
                let mut previous_upper_start = offset;
                while let Some((next_start, next_end, character)) =
                    Self::char_at_offset(buffer, current_end)
                {
                    if character.is_uppercase() {
                        previous_upper_start = next_start;
                        current_end = next_end;
                        continue;
                    }

                    if Self::is_subword_lower_or_digit(character) && previous_upper_start > offset {
                        return previous_upper_start;
                    }

                    return current_end;
                }

                return current_end;
            }

            while matches!(
                Self::char_at_offset(buffer, current_end),
                Some((_, next_end, character)) if Self::is_subword_lower_or_digit(character) && {
                    current_end = next_end;
                    true
                }
            ) {}
            return current_end;
        }

        while matches!(
            Self::char_at_offset(buffer, current_end),
            Some((_, next_end, character)) if Self::is_subword_lower_or_digit(character) && {
                current_end = next_end;
                true
            }
        ) {}
        current_end
    }

    fn prev_subword_start_in_buffer(buffer: &TextBuffer, offset: usize) -> usize {
        let offset = buffer.floor_char_boundary(offset);
        let Some((mut current_start, _, first_character)) =
            Self::char_before_offset(buffer, offset)
        else {
            return 0;
        };

        if first_character == '_' {
            while matches!(
                Self::char_before_offset(buffer, current_start),
                Some((previous_start, _, '_')) if {
                    current_start = previous_start;
                    true
                }
            ) {}
            return current_start;
        }

        if !first_character.is_alphanumeric() {
            while matches!(
                Self::char_before_offset(buffer, current_start),
                Some((previous_start, _, character))
                    if !character.is_alphanumeric() && character != '_' && {
                        current_start = previous_start;
                        true
                    }
            ) {}
            return current_start;
        }

        if first_character.is_uppercase() {
            while matches!(
                Self::char_before_offset(buffer, current_start),
                Some((previous_start, _, character)) if character.is_uppercase() && {
                    current_start = previous_start;
                    true
                }
            ) {}
            return current_start;
        }

        while matches!(
            Self::char_before_offset(buffer, current_start),
            Some((previous_start, _, character))
                if Self::is_subword_lower_or_digit(character) && {
                    current_start = previous_start;
                    true
                }
        ) {}
        if let Some((previous_start, _, character)) =
            Self::char_before_offset(buffer, current_start)
            && character.is_uppercase()
        {
            current_start = previous_start;
        }
        current_start
    }

    #[cfg(test)]
    pub(crate) fn next_subword_end_in_text(text: &str, offset: usize) -> usize {
        let tail = &text[offset..];
        let mut chars = tail.char_indices().peekable();

        let (_, first_char) = match chars.next() {
            Some(pair) => pair,
            None => return offset,
        };

        if first_char == '_' {
            while chars
                .peek()
                .map(|(_, character)| *character == '_')
                .unwrap_or(false)
            {
                chars.next();
            }
        } else if !first_char.is_alphanumeric() {
            while chars
                .peek()
                .map(|(_, character)| !character.is_alphanumeric() && *character != '_')
                .unwrap_or(false)
            {
                chars.next();
            }
        } else if first_char.is_uppercase() {
            let second_is_upper = chars
                .peek()
                .map(|(_, character)| character.is_uppercase())
                .unwrap_or(false);
            if second_is_upper {
                let mut previous_index = 0;
                let mut previous_was_upper = true;
                loop {
                    match chars.peek() {
                        Some(&(index, character)) if character.is_uppercase() => {
                            previous_index = index;
                            previous_was_upper = true;
                            chars.next();
                        }
                        Some(&(_, character))
                            if character.is_lowercase() || character.is_ascii_digit() =>
                        {
                            if previous_was_upper && previous_index > 0 {
                                return offset + previous_index;
                            }
                            break;
                        }
                        _ => break,
                    }
                }
            } else {
                while chars
                    .peek()
                    .map(|(_, character)| character.is_lowercase() || character.is_ascii_digit())
                    .unwrap_or(false)
                {
                    chars.next();
                }
            }
        } else {
            while chars
                .peek()
                .map(|(_, character)| character.is_lowercase() || character.is_ascii_digit())
                .unwrap_or(false)
            {
                chars.next();
            }
        }

        match chars.peek() {
            Some(&(index, _)) => offset + index,
            None => text.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn prev_subword_start_in_text(text: &str, offset: usize) -> usize {
        if offset == 0 {
            return 0;
        }

        let head = &text[..offset];
        let characters: Vec<char> = head.chars().collect();
        let mut index = characters.len();
        let first = characters[index - 1];

        if first == '_' {
            while index > 0 && characters[index - 1] == '_' {
                index -= 1;
            }
        } else if !first.is_alphanumeric() {
            while index > 0
                && !characters[index - 1].is_alphanumeric()
                && characters[index - 1] != '_'
            {
                index -= 1;
            }
        } else if first.is_uppercase() {
            while index > 0 && characters[index - 1].is_uppercase() {
                index -= 1;
            }
        } else {
            while index > 0
                && (characters[index - 1].is_lowercase() || characters[index - 1].is_ascii_digit())
            {
                index -= 1;
            }
            if index > 0 && characters[index - 1].is_uppercase() {
                index -= 1;
            }
        }

        head.char_indices()
            .nth(index)
            .map(|(byte_index, _)| byte_index)
            .unwrap_or(0)
    }

    fn swap_selection_end(cursor: &mut Cursor, selection: &mut Selection) -> bool {
        if !selection.has_selection() {
            return false;
        }

        let selection_range = selection.range();
        let current_head = cursor.position();
        let opposite_end = if current_head == selection_range.end {
            selection_range.start
        } else {
            selection_range.end
        };

        *selection = Selection::from_anchor_head(current_head, opposite_end);
        cursor.set_position(opposite_end);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AutoCloseBracketPlan, AutoIndentNewlinePlan, AutoSurroundSelectionPlan,
        CompletionQueryPlan, CutToEndOfLinePlan, DedentLinesPlan, DeleteAtCursorPlan,
        DeleteBeforeCursorPlan, DeleteSubwordPlan, DuplicateSelectedLinesPlan, EditorCore,
        EditorCoreSnapshot, IndentLinesPlan, InsertAtCursorPlan, InsertTextPlan, JoinLinesPlan,
        LineCommentEdit, LineDedentEdit, LineIndentEdit, LinePrefixEditMode,
        MarkedTextReplacementPlan, MoveSelectedLinesPlan, MultiCursorEditPlan,
        NewlineInsertionPlan, PlannedEditBatch, PostApplySelection, PrimaryReplacementPlan,
        PrimarySelectionDeletionPlan, SelectedLineBlockPlan, SelectedLineDeletionPlan,
        SelectedTextPlan, SelectionHistoryEntry, SelectionRotationEdit, SkipClosingBracketPlan,
        StructuralRange, TextInsertionPlan, TextReplacementEdit, ToggleLineCommentPlan,
        TransposeCharsPlan, WholeLineCopyPlan, WholeLineCutPlan, WholeLinePastePlan, WordTarget,
    };
    use crate::{
        Cursor, Position, Selection, SelectionsCollection, TextBuffer, selection::SelectionMode,
    };

    type TestCoreState = (
        TextBuffer,
        Cursor,
        Selection,
        Vec<(Cursor, Selection)>,
        SelectionsCollection,
        bool,
        Vec<SelectionHistoryEntry>,
    );

    fn test_core(text: &str) -> TestCoreState {
        (
            TextBuffer::new(text),
            Cursor::new(),
            Selection::new(),
            Vec::new(),
            SelectionsCollection::single(Cursor::new(), Selection::new()),
            false,
            Vec::new(),
        )
    }

    fn test_core_snapshot<'a>(
        buffer: &'a TextBuffer,
        cursor: Cursor,
        selection: Selection,
        extra_cursors: Vec<(Cursor, Selection)>,
        selection_history: Vec<SelectionHistoryEntry>,
    ) -> EditorCoreSnapshot<'a> {
        EditorCoreSnapshot::new(
            buffer,
            SelectionsCollection::from_primary_and_extras(cursor, selection, extra_cursors),
            false,
            selection_history,
        )
    }

    #[test]
    fn selection_state_round_trips_through_restore() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::at(Position::new(0, 5)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );
        let snapshot = core.selection_state();

        core.clear_selection();
        core.restore_selection_state(snapshot.clone());

        assert_eq!(core.selection_state(), snapshot);
    }

    #[test]
    fn core_snapshot_selection_state_rebuilds_collection_from_read_only_state() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2));
        let extra_cursors = vec![(
            Cursor::at(Position::new(0, 5)),
            Selection::at(Position::new(0, 5)),
        )];
        let snapshot = test_core_snapshot(&buffer, cursor, selection, extra_cursors, Vec::new());

        let state = snapshot.selection_state();

        assert_eq!(state.expect_cursor().position(), Position::new(1, 2));
        assert_eq!(state.expect_selection().range().start, Position::new(1, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(1, 2));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(1, 2))
        );
    }

    #[test]
    fn selection_history_restores_from_collection_payload() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            _collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2));
        extra_cursors.push((
            Cursor::at(Position::new(2, 5)),
            Selection::from_anchor_head(Position::new(2, 0), Position::new(2, 5)),
        ));
        let mut collection = SelectionsCollection::from_primary_and_extras(
            cursor.clone(),
            selection.clone(),
            extra_cursors.clone(),
        );

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );
        core.push_selection_snapshot();
        core.clear_selection();
        core.clear_extra_cursors();

        assert!(core.undo_selection());
        let state = core.selection_state();
        assert_eq!(state.collection.len(), 2);
        assert_eq!(state.expect_cursor().position(), Position::new(1, 2));
        assert_eq!(state.expect_selection().range().start, Position::new(1, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(1, 2));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(2, 5));
    }

    #[test]
    fn set_multi_selections_keeps_fields_and_collection_in_sync() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_multi_selections_from_positions(&[
            (Position::new(0, 0), Position::new(0, 5)),
            (Position::new(1, 0), Position::new(1, 4)),
        ]));

        let state = core.selection_state();
        assert_eq!(state.collection.len(), 2);
        assert_eq!(core.cursor.position(), Position::new(0, 5));
        assert_eq!(core.selection.range().start, Position::new(0, 0));
        assert_eq!(core.selection.range().end, Position::new(0, 5));
        assert_eq!(core.extra_cursors.len(), 1);
        assert_eq!(core.extra_cursors[0].0.position(), Position::new(1, 4));
    }

    #[test]
    fn select_all_occurrences_keeps_collection_and_fields_in_sync() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha alpha alpha");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.select_all_occurrences(&[0..5, 6..11, 12..17]));

        let state = core.selection_state();
        assert_eq!(state.collection.len(), 3);
        assert_eq!(core.cursor.position(), Position::new(0, 5));
        assert_eq!(core.selection.range().start, Position::new(0, 0));
        assert_eq!(core.selection.range().end, Position::new(0, 5));
        assert_eq!(core.extra_cursors.len(), 2);
        assert_eq!(core.extra_cursors[0].0.position(), Position::new(0, 11));
        assert_eq!(core.extra_cursors[1].0.position(), Position::new(0, 17));
    }

    #[test]
    fn move_primary_cursor_without_extension_keeps_collection_and_selection_aligned() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor(Position::new(1, 2), false);

        let state = core.selection_state();
        assert_eq!(core.cursor.position(), Position::new(1, 2));
        assert_eq!(core.selection.anchor(), Position::new(1, 2));
        assert_eq!(core.selection.head(), Position::new(1, 2));
        assert_eq!(state.collection.len(), 1);
        assert_eq!(state.expect_cursor().position(), Position::new(1, 2));
        assert_eq!(state.expect_selection().head(), Position::new(1, 2));
    }

    #[test]
    fn core_snapshot_supports_read_only_multi_cursor_queries() {
        let buffer = TextBuffer::new("alpha beta gamma");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 10)),
                Selection::at(Position::new(0, 10)),
            ),
            (
                Cursor::at(Position::new(0, 16)),
                Selection::from_anchor_head(Position::new(0, 11), Position::new(0, 16)),
            ),
        ];

        let targets = test_core_snapshot(
            &buffer,
            cursor.clone(),
            selection.clone(),
            extra_cursors.clone(),
            Vec::new(),
        )
        .multi_cursor_edit_targets();
        assert_eq!(targets, vec![(11, 16), (10, 10), (0, 5)]);

        let selections = test_core_snapshot(&buffer, cursor, selection, extra_cursors, Vec::new())
            .all_cursor_selections();
        assert_eq!(selections.len(), 3);
        assert_eq!(selections[0].0, Position::new(0, 5));
        assert_eq!(selections[1].0, Position::new(0, 10));
        assert_eq!(selections[2].0, Position::new(0, 16));
    }

    #[test]
    fn core_snapshot_plans_newline_edits_without_mutating_editor_state() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 3));
        let extra_cursors = vec![(
            Cursor::at(Position::new(1, 1)),
            Selection::at(Position::new(1, 1)),
        )];

        let edits = test_core_snapshot(&buffer, cursor, selection, extra_cursors, Vec::new())
            .plan_multi_cursor_newline_edits(|line| format!("<{}>", line));

        assert_eq!(
            edits,
            vec![
                MultiCursorEditPlan {
                    slot: 0,
                    start: 1,
                    end: 3,
                    replacement: "<0>".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 1,
                    start: 7,
                    end: 7,
                    replacement: "<1>".to_string(),
                },
            ]
        );
    }

    #[test]
    fn select_line_extends_to_the_next_line_on_repeat() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");
        cursor.set_position(Position::new(0, 2));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.select_line();
        assert_eq!(
            core.selection_state().expect_selection().range().start,
            Position::new(0, 0)
        );
        assert_eq!(
            core.selection_state().expect_selection().range().end,
            Position::new(1, 0)
        );

        core.select_line();
        assert_eq!(
            core.selection_state().expect_selection().range().start,
            Position::new(0, 0)
        );
        assert_eq!(
            core.selection_state().expect_selection().range().end,
            Position::new(2, 0)
        );
    }

    #[test]
    fn select_next_occurrence_adds_a_secondary_selection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("word test word test");
        cursor.set_position(Position::new(0, 2));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.select_next_occurrence(Some(0..4), &[0..4, 10..14]));
        assert_eq!(
            core.selection_state().expect_selection().range().start,
            Position::new(0, 0)
        );
        assert_eq!(
            core.selection_state().expect_selection().range().end,
            Position::new(0, 4)
        );

        assert!(core.select_next_occurrence(Some(0..4), &[0..4, 10..14]));
        let state = core.selection_state();
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(0, 14));
    }

    #[test]
    fn multi_cursor_edit_targets_deduplicate_shared_ranges() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5)),
        ));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::at(Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let targets = core.multi_cursor_edit_targets();
        assert_eq!(targets, vec![(10, 10), (0, 5)]);
    }

    #[test]
    fn set_multi_selections_from_positions_rebuilds_primary_and_extras() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_multi_selections_from_positions(&[
            (Position::new(0, 0), Position::new(0, 5)),
            (Position::new(0, 6), Position::new(0, 10)),
        ]));

        let state = core.selection_state();
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 5));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(0, 10));
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn expand_selection_grows_each_selection_independently() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            _collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha(beta) gamma(delta)");

        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 18)),
            Selection::from_anchor_head(Position::new(0, 17), Position::new(0, 22)),
        ));
        let mut collection = SelectionsCollection::from_primary_and_extras(
            cursor.clone(),
            selection.clone(),
            extra_cursors.clone(),
        );

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.expand_selections(&[
            (
                Some(StructuralRange {
                    start: 5,
                    end: 11,
                    open: '(',
                    close: ')',
                }),
                Some(0..5),
            ),
            (
                Some(StructuralRange {
                    start: 17,
                    end: 24,
                    open: '(',
                    close: ')',
                }),
                Some(17..22),
            ),
        ]));

        let state = core.selection_state();
        assert_eq!(state.expect_selection().range().start, Position::new(0, 5));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 11));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(
            state.extra_cursors()[0].1.range().start,
            Position::new(0, 17)
        );
        assert_eq!(state.extra_cursors()[0].1.range().end, Position::new(0, 24));
    }

    #[test]
    fn shrink_selection_restores_multi_selection_snapshot() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            _collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma delta");

        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 16)),
            Selection::from_anchor_head(Position::new(0, 11), Position::new(0, 16)),
        ));
        let mut collection = SelectionsCollection::from_primary_and_extras(
            cursor.clone(),
            selection.clone(),
            extra_cursors.clone(),
        );

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.expand_selections(&[
            (
                Some(StructuralRange {
                    start: 0,
                    end: 10,
                    open: 'a',
                    close: 'a',
                }),
                Some(0..5),
            ),
            (
                Some(StructuralRange {
                    start: 11,
                    end: 22,
                    open: 'd',
                    close: 'd',
                }),
                Some(11..16),
            ),
        ]));
        assert!(core.shrink_selection());

        let state = core.selection_state();
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 5));
        assert_eq!(
            state.extra_cursors()[0].1.range().start,
            Position::new(0, 11)
        );
        assert_eq!(state.extra_cursors()[0].1.range().end, Position::new(0, 16));
    }

    #[test]
    fn all_cursor_selections_returns_primary_then_extra_entries() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::from_anchor_head(Position::new(0, 6), Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let selections = core.all_cursor_selections();
        assert_eq!(selections.len(), 2);
        assert_eq!(selections[0].0, Position::new(0, 5));
        assert_eq!(selections[0].1.range().start, Position::new(0, 0));
        assert_eq!(selections[0].1.range().end, Position::new(0, 5));
        assert_eq!(selections[1].0, Position::new(0, 10));
        assert_eq!(selections[1].1.range().start, Position::new(0, 6));
        assert_eq!(selections[1].1.range().end, Position::new(0, 10));
    }

    #[test]
    fn selection_ranges_with_fallback_combines_selections_and_cursor_ranges() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 6)),
            Selection::at(Position::new(0, 6)),
        ));
        extra_cursors.push((
            Cursor::at(Position::new(0, 16)),
            Selection::from_anchor_head(Position::new(0, 16), Position::new(0, 11)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let ranges = core.selection_ranges_with_fallback(|position| {
            if position == Position::new(0, 6) {
                Some(6..10)
            } else {
                None
            }
        });

        assert_eq!(ranges, vec![0..5, 6..10, 11..16]);
    }

    #[test]
    fn set_single_cursor_clears_secondary_cursor_state() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 3));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 3));
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.set_single_cursor(Position::new(1, 1));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(1, 1));
        assert_eq!(state.expect_selection().anchor(), Position::new(1, 1));
        assert_eq!(state.expect_selection().head(), Position::new(1, 1));
        assert!(state.extra_cursors().is_empty());
        assert_eq!(state.collection.len(), 1);
    }

    #[test]
    fn move_primary_cursor_updates_selection_collection_without_dropping_extras() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        extra_cursors.push((
            Cursor::at(Position::new(1, 1)),
            Selection::at(Position::new(1, 1)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor(Position::new(0, 3), false);

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 3));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 3));
        assert_eq!(state.expect_selection().head(), Position::new(0, 3));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 3))
        );
    }

    #[test]
    fn move_primary_cursor_extends_selection_from_existing_primary_cursor() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 1));
        selection.set_position(Position::new(0, 1));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor(Position::new(0, 4), true);

        let state = core.selection_state();
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 1));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
        assert_eq!(state.collection.len(), 1);
    }

    #[test]
    fn move_primary_cursor_to_offset_preserves_secondary_cursors() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.move_primary_cursor_to_offset(3, false));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 3));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 3));
        assert_eq!(state.expect_selection().head(), Position::new(0, 3));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn set_primary_selection_updates_cursor_and_collection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha  beta");
        extra_cursors.push((
            Cursor::at(Position::new(0, 8)),
            Selection::at(Position::new(0, 8)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.set_primary_selection(Selection::from_anchor_head(
            Position::new(0, 0),
            Position::new(0, 5),
        ));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 5));
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 5));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn set_primary_cursor_and_selection_preserves_explicit_cursor_position() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.set_primary_cursor_and_selection(
            Position::new(0, 0),
            Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 0)),
        );

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(1, 0));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 0))
        );
    }

    #[test]
    fn set_primary_cursor_preserving_selection_keeps_selection_and_extras() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4));
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.set_primary_cursor_preserving_selection(Position::new(0, 2));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 2));
        assert_eq!(state.expect_selection().range().start, Position::new(0, 1));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 4));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 2))
        );
    }

    #[test]
    fn set_primary_selection_range_preserves_direction_and_updates_collection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha  beta");
        extra_cursors.push((
            Cursor::at(Position::new(0, 8)),
            Selection::at(Position::new(0, 8)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.set_primary_selection_range(Position::new(0, 5), Position::new(0, 0));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert!(state.expect_selection().is_reversed());
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 5));
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn collapse_primary_selection_to_cursor_clears_selection_and_rebuilds_collection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 4));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4));
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.collapse_primary_selection_to_cursor();

        let state = core.selection_state();
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 4));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 4))
        );
    }

    #[test]
    fn clear_selection_rebuilds_collection_without_dropping_secondary_cursors() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 4));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4));
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.clear_selection();

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 4));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 4));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn extend_primary_selection_to_cursor_uses_current_cursor_position() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha  beta");
        cursor.set_position(Position::new(0, 4));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.extend_primary_selection_to_cursor(Position::new(0, 1));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 4));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 1));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
        assert_eq!(state.collection.len(), 1);
    }

    #[test]
    fn selection_state_reads_collection_as_authoritative_source() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 2));
        extra_cursors.push((
            Cursor::at(Position::new(1, 1)),
            Selection::at(Position::new(1, 1)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 2));
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 2));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 2))
        );
    }

    #[test]
    fn from_collection_drives_selection_state_without_legacy_fields() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let mut collection = SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(1, 2)),
            Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
            vec![(
                Cursor::at(Position::new(0, 1)),
                Selection::at(Position::new(0, 1)),
            )],
        );
        let mut last_select_line_was_extend = false;
        let mut selection_history = Vec::new();

        let core = EditorCore::from_collection(
            &buffer,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(1, 2));
        assert_eq!(state.expect_selection().range().start, Position::new(1, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(1, 2));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(0, 1));
    }

    #[test]
    fn set_primary_cursor_at_offset_resolves_buffer_coordinates() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        extra_cursors.push((
            Cursor::at(Position::new(1, 0)),
            Selection::at(Position::new(1, 0)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_primary_cursor_at_offset(7));
        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(1, 1));
        assert!(state.extra_cursors().is_empty());
    }

    #[test]
    fn swap_selection_ends_flips_primary_selection_direction() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.swap_selection_ends());

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert!(state.expect_selection().is_reversed());
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 5));
        assert_eq!(state.collection.len(), 1);
    }

    #[test]
    fn swap_selection_ends_updates_secondary_cursors_and_collection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(1, 4)),
            Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 4)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.swap_selection_ends());

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(1, 0));
        assert!(state.extra_cursors()[0].1.is_reversed());
        assert_eq!(state.collection.len(), 2);
        assert_eq!(
            state
                .collection
                .all()
                .get(1)
                .map(|entry| entry.cursor.position()),
            Some(Position::new(1, 0))
        );
    }

    #[test]
    fn plan_primary_selection_deletion_returns_linear_range_for_reversed_selection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 5), Position::new(0, 2));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_primary_selection_deletion(),
            Some(PrimarySelectionDeletionPlan::Linear {
                range: 2..5,
                cursor_position: Position::new(0, 2),
            })
        );
    }

    #[test]
    fn plan_primary_selection_deletion_returns_block_ranges_in_reverse_order() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("abcd\nefgh\nijkl");
        cursor.set_position(Position::new(2, 3));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(2, 3));
        selection.set_mode(SelectionMode::Block);

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_primary_selection_deletion(),
            Some(PrimarySelectionDeletionPlan::Block {
                ranges: vec![11..13, 6..8, 1..3],
                cursor_position: Position::new(0, 1),
            })
        );
    }

    #[test]
    fn selected_line_range_uses_selection_lines_when_present() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(2, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.selected_line_range(), (0, 2));
    }

    #[test]
    fn selected_line_range_falls_back_to_cursor_line_without_selection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(1, 2));
        selection.set_position(Position::new(1, 2));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.selected_line_range(), (1, 1));
    }

    #[test]
    fn primary_selection_byte_range_normalizes_reversed_selection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 5), Position::new(0, 2));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.primary_selection_byte_range(), Some(2..5));
    }

    #[test]
    fn core_snapshot_exposes_selected_line_range_and_primary_selection_byte_range() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(2, 0));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .selected_line_range(),
            (0, 2)
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .primary_selection_byte_range(),
            Some(1..9)
        );
    }

    #[test]
    fn primary_selection_or_fallback_range_prefers_selection_when_present() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 5), Position::new(0, 2));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.primary_selection_or_fallback_range(&mut |_| Some(0..1)),
            Some(2..5)
        );
    }

    #[test]
    fn primary_selection_or_fallback_range_uses_fallback_when_selection_is_empty() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 6));
        selection.set_position(Position::new(0, 6));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.primary_selection_or_fallback_range(&mut |position| {
                if position == Position::new(0, 6) {
                    Some(6..10)
                } else {
                    None
                }
            }),
            Some(6..10)
        );
    }

    #[test]
    fn primary_selection_contains_checks_only_active_primary_selection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 2), Position::new(0, 5));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.primary_selection_contains(Position::new(0, 3)));
        assert!(!core.primary_selection_contains(Position::new(0, 6)));
    }

    #[test]
    fn core_snapshot_exposes_selection_fallback_and_contains_queries() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::from_anchor_head(Position::new(0, 2), Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .primary_selection_or_fallback_range(|_| Some(0..1)),
            Some(2..5)
        );
        assert!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .primary_selection_contains(Position::new(0, 4))
        );
    }

    #[test]
    fn selected_line_block_plan_covers_selected_lines_with_trailing_newline() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo\n");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(1, 3));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.selected_line_block_plan(),
            Some(SelectedLineBlockPlan {
                first_line: 0,
                last_line: 1,
                byte_range: 0..9,
                has_trailing_newline: true,
            })
        );
    }

    #[test]
    fn selected_line_block_plan_uses_document_end_for_final_line() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(2, 1));
        selection.set_position(Position::new(2, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.selected_line_block_plan(),
            Some(SelectedLineBlockPlan {
                first_line: 2,
                last_line: 2,
                byte_range: 9..12,
                has_trailing_newline: false,
            })
        );
    }

    #[test]
    fn selected_line_deletion_plan_eats_preceding_newline_for_final_line() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(2, 1));
        selection.set_position(Position::new(2, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.selected_line_deletion_plan(),
            Some(SelectedLineDeletionPlan {
                first_line: 2,
                byte_range: 8..12,
                target_position: Position::new(2, 0),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_selected_line_block_and_deletion_plans() {
        let buffer = TextBuffer::new("zero\none\ntwo\n");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(1, 3));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .selected_line_block_plan(),
            Some(SelectedLineBlockPlan {
                first_line: 0,
                last_line: 1,
                byte_range: 0..9,
                has_trailing_newline: true,
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .selected_line_deletion_plan(),
            Some(SelectedLineDeletionPlan {
                first_line: 0,
                byte_range: 0..9,
                target_position: Position::new(0, 0),
            })
        );
    }

    #[test]
    fn duplicate_selected_lines_up_plan_inserts_block_at_selection_start() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(1, 3));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.duplicate_selected_lines_up_plan(),
            Some(DuplicateSelectedLinesPlan {
                insertions: vec![TextInsertionPlan {
                    offset: 0,
                    text: "zero\none\n".to_string(),
                }],
                target_line: 2,
                target_position: Position::new(2, 2),
            })
        );
    }

    #[test]
    fn duplicate_selected_lines_down_plan_appends_newline_when_block_ends_document() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(2, 1));
        selection.set_position(Position::new(2, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.duplicate_selected_lines_down_plan(),
            Some(DuplicateSelectedLinesPlan {
                insertions: vec![
                    TextInsertionPlan {
                        offset: 12,
                        text: "\n".to_string(),
                    },
                    TextInsertionPlan {
                        offset: 13,
                        text: "two\n".to_string(),
                    },
                ],
                target_line: 3,
                target_position: Position::new(3, 1),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_duplicate_selected_lines_plans() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(2, 1));
        let selection = Selection::at(Position::new(2, 1));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .duplicate_selected_lines_up_plan(),
            Some(DuplicateSelectedLinesPlan {
                insertions: vec![TextInsertionPlan {
                    offset: 9,
                    text: "two\n".to_string(),
                }],
                target_line: 3,
                target_position: Position::new(3, 0),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .duplicate_selected_lines_down_plan(),
            Some(DuplicateSelectedLinesPlan {
                insertions: vec![
                    TextInsertionPlan {
                        offset: 12,
                        text: "\n".to_string(),
                    },
                    TextInsertionPlan {
                        offset: 13,
                        text: "two\n".to_string(),
                    },
                ],
                target_line: 3,
                target_position: Position::new(3, 1),
            })
        );
    }

    #[test]
    fn move_selected_lines_up_plan_swaps_block_with_line_above() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo\n");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(2, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.move_selected_lines_up_plan(),
            Some(MoveSelectedLinesPlan {
                byte_range: 0..13,
                replacement: "one\ntwo\nzero\n".to_string(),
                target_line: 0,
                target_position: Position::new(0, 2),
            })
        );
    }

    #[test]
    fn move_selected_lines_down_plan_swaps_block_with_line_below() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo\nthree");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(2, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.move_selected_lines_down_plan(),
            Some(MoveSelectedLinesPlan {
                byte_range: 5..18,
                replacement: "three\none\ntwo\n".to_string(),
                target_line: 2,
                target_position: Position::new(2, 2),
            })
        );
    }

    #[test]
    fn move_selected_lines_up_plan_carries_clamped_target_cursor_position() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("longer\na\nshort\n");
        cursor.set_position(Position::new(1, 4));
        let mut selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.move_selected_lines_up_plan(),
            Some(MoveSelectedLinesPlan {
                byte_range: 0..9,
                replacement: "a\nlonger\n".to_string(),
                target_line: 0,
                target_position: Position::new(0, 4),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_move_selected_lines_plans() {
        let buffer = TextBuffer::new("zero\none\ntwo\nthree");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(2, 1));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .move_selected_lines_up_plan(),
            Some(MoveSelectedLinesPlan {
                byte_range: 0..13,
                replacement: "one\ntwo\nzero\n".to_string(),
                target_line: 0,
                target_position: Position::new(0, 2),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .move_selected_lines_down_plan(),
            Some(MoveSelectedLinesPlan {
                byte_range: 5..18,
                replacement: "three\none\ntwo\n".to_string(),
                target_line: 2,
                target_position: Position::new(2, 2),
            })
        );
    }

    #[test]
    fn whole_line_copy_plan_includes_newline_when_not_last_line() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(1, 1));
        selection.set_position(Position::new(1, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.whole_line_copy_plan(),
            Some(WholeLineCopyPlan {
                text: "one\n".to_string(),
            })
        );
    }

    #[test]
    fn whole_line_cut_plan_targets_same_line_slot_after_delete() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(1, 1));
        selection.set_position(Position::new(1, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.whole_line_cut_plan(),
            Some(WholeLineCutPlan {
                text: "one\n".to_string(),
                delete_range: 5..9,
                target_line: 1,
                target_position: Position::new(1, 0),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_whole_line_copy_and_cut_plans() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(2, 0));
        let selection = Selection::at(Position::new(2, 0));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .whole_line_copy_plan(),
            Some(WholeLineCopyPlan {
                text: "two".to_string(),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .whole_line_cut_plan(),
            Some(WholeLineCutPlan {
                text: "two".to_string(),
                delete_range: 9..12,
                target_line: 2,
                target_position: Position::new(2, 0),
            })
        );
    }

    #[test]
    fn whole_line_paste_plan_normalizes_missing_trailing_newline() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::at(Position::new(1, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .whole_line_paste_plan("alpha"),
            Some(WholeLinePastePlan {
                offset: 5,
                text: "alpha\n".to_string(),
                target_line: 2,
                target_position: Position::new(2, 2),
            })
        );
    }

    #[test]
    fn whole_line_paste_plan_preserves_existing_trailing_newline() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("zero\none\ntwo");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.whole_line_paste_plan("alpha\n"),
            Some(WholeLinePastePlan {
                offset: 0,
                text: "alpha\n".to_string(),
                target_line: 1,
                target_position: Position::new(1, 0),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_whole_line_paste_plan() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(2, 0));
        let selection = Selection::at(Position::new(2, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .whole_line_paste_plan("two\n"),
            Some(WholeLinePastePlan {
                offset: 9,
                text: "two\n".to_string(),
                target_line: 3,
                target_position: Position::new(3, 0),
            })
        );
    }

    #[test]
    fn insert_newline_above_plan_preserves_indentation() {
        let buffer = TextBuffer::new("    zero\none");
        let cursor = Cursor::at(Position::new(0, 3));
        let selection = Selection::at(Position::new(0, 3));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .insert_newline_above_plan(),
            Some(NewlineInsertionPlan {
                offset: 0,
                text: "    \n".to_string(),
                target_position: Position::new(0, 4),
            })
        );
    }

    #[test]
    fn insert_newline_below_plan_uses_line_end_offset_and_indent() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("  zero\none");
        cursor.set_position(Position::new(0, 1));
        selection.set_position(Position::new(0, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.insert_newline_below_plan(),
            Some(NewlineInsertionPlan {
                offset: 7,
                text: "\n  ".to_string(),
                target_position: Position::new(1, 2),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_newline_insertion_plans() {
        let buffer = TextBuffer::new("zero");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .insert_newline_above_plan(),
            Some(NewlineInsertionPlan {
                offset: 0,
                text: "\n".to_string(),
                target_position: Position::new(0, 0),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .insert_newline_below_plan(),
            Some(NewlineInsertionPlan {
                offset: 4,
                text: "\n".to_string(),
                target_position: Position::new(1, 0),
            })
        );
    }

    #[test]
    fn cut_to_end_of_line_plan_cuts_remaining_text_before_line_end() {
        let buffer = TextBuffer::new("alpha beta\ngamma");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::at(Position::new(0, 6));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .cut_to_end_of_line_plan(),
            Some(CutToEndOfLinePlan {
                text: "beta\n".to_string(),
                delete_range: 6..11,
            })
        );
    }

    #[test]
    fn cut_to_end_of_line_plan_cuts_newline_at_line_end() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 5));
        selection.set_position(Position::new(0, 5));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.cut_to_end_of_line_plan(),
            Some(CutToEndOfLinePlan {
                text: "\n".to_string(),
                delete_range: 5..6,
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_cut_to_end_of_line_plan() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .cut_to_end_of_line_plan(),
            None
        );
    }

    #[test]
    fn join_lines_plan_replaces_newline_with_single_space() {
        let buffer = TextBuffer::new("alpha\n  beta");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .join_lines_plan(),
            Some(JoinLinesPlan {
                delete_range: 6..7,
                insert_offset: 6,
                insert_text: " ".to_string(),
                target_offset: 7,
            })
        );
    }

    #[test]
    fn join_lines_plan_is_none_for_last_line() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(1, 0));
        selection.set_position(Position::new(1, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.join_lines_plan(), None);
    }

    #[test]
    fn core_snapshot_exposes_join_lines_plan() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .join_lines_plan(),
            Some(JoinLinesPlan {
                delete_range: 6..7,
                insert_offset: 6,
                insert_text: " ".to_string(),
                target_offset: 7,
            })
        );
    }

    #[test]
    fn transpose_chars_plan_swaps_adjacent_characters_at_cursor() {
        let buffer = TextBuffer::new("abcd");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::at(Position::new(0, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .transpose_chars_plan(),
            Some(TransposeCharsPlan {
                replace_range: 1..3,
                replacement: "cb".to_string(),
                target_offset: 3,
            })
        );
    }

    #[test]
    fn transpose_chars_plan_swaps_previous_two_characters_at_buffer_end() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("abcd");
        cursor.set_position(Position::new(0, 4));
        selection.set_position(Position::new(0, 4));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.transpose_chars_plan(),
            Some(TransposeCharsPlan {
                replace_range: 2..4,
                replacement: "dc".to_string(),
                target_offset: 4,
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_transpose_chars_plan() {
        let buffer = TextBuffer::new("éa");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::at(Position::new(0, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .transpose_chars_plan(),
            Some(TransposeCharsPlan {
                replace_range: 0..3,
                replacement: "aé".to_string(),
                target_offset: 3,
            })
        );
    }

    #[test]
    fn transpose_chars_plan_handles_cursor_inside_multibyte_character() {
        let buffer = TextBuffer::new("éa");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .transpose_chars_plan(),
            Some(TransposeCharsPlan {
                replace_range: 0..3,
                replacement: "aé".to_string(),
                target_offset: 3,
            })
        );
    }

    #[test]
    fn indent_lines_plan_adds_indent_to_each_selected_line() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 1));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .indent_lines_plan(4, false),
            Some(IndentLinesPlan {
                edits: vec![
                    LineIndentEdit {
                        offset: 0,
                        text: "    ".to_string(),
                    },
                    LineIndentEdit {
                        offset: 6,
                        text: "    ".to_string(),
                    },
                ],
            })
        );
    }

    #[test]
    fn dedent_lines_plan_removes_one_indent_level_and_retargets_cursor() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("    alpha\n\tbeta");
        cursor.set_position(Position::new(0, 3));
        selection.set_position(Position::new(0, 3));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.dedent_lines_plan(4),
            Some(DedentLinesPlan {
                edits: vec![LineDedentEdit { range: 0..4 }],
                target_position: Position::new(0, 0),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_indent_and_dedent_plans() {
        let buffer = TextBuffer::new("\tbeta");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .indent_lines_plan(2, true),
            Some(IndentLinesPlan {
                edits: vec![LineIndentEdit {
                    offset: 0,
                    text: "\t".to_string(),
                }],
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .dedent_lines_plan(4),
            Some(DedentLinesPlan {
                edits: vec![LineDedentEdit { range: 0..1 }],
                target_position: Position::new(0, 0),
            })
        );
    }

    #[test]
    fn toggle_line_comment_plan_inserts_comment_prefix_after_indentation() {
        let buffer = TextBuffer::new("  alpha\nbeta");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .toggle_line_comment_plan(),
            Some(ToggleLineCommentPlan {
                edits: vec![
                    LineCommentEdit::Insert {
                        offset: 8,
                        text: "-- ".to_string(),
                    },
                    LineCommentEdit::Insert {
                        offset: 2,
                        text: "-- ".to_string(),
                    },
                ],
            })
        );
    }

    #[test]
    fn toggle_line_comment_plan_removes_existing_comment_prefixes() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("  -- alpha\n-- beta");
        cursor.set_position(Position::new(0, 0));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.toggle_line_comment_plan(),
            Some(ToggleLineCommentPlan {
                edits: vec![
                    LineCommentEdit::Delete { range: 2..5 },
                    LineCommentEdit::Delete { range: 11..14 },
                ],
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_toggle_line_comment_plan() {
        let buffer = TextBuffer::new("-- beta");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .toggle_line_comment_plan(),
            Some(ToggleLineCommentPlan {
                edits: vec![LineCommentEdit::Delete { range: 0..3 }],
            })
        );
    }

    #[test]
    fn auto_indent_newline_text_preserves_current_indent_when_enabled() {
        let buffer = TextBuffer::new("    select");
        let cursor = Cursor::at(Position::new(0, 4));
        let selection = Selection::at(Position::new(0, 4));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_indent_newline_text(true, "    "),
            Some(AutoIndentNewlinePlan {
                text: "\n    ".to_string(),
            })
        );
    }

    #[test]
    fn auto_indent_newline_text_adds_extra_indent_after_block_openers() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("BEGIN");
        cursor.set_position(Position::new(0, 5));
        selection.set_position(Position::new(0, 5));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.auto_indent_newline_text(true, "  "),
            Some(AutoIndentNewlinePlan {
                text: "\n  ".to_string(),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_auto_indent_newline_text() {
        let buffer = TextBuffer::new("loop");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .auto_indent_newline_text(false, "\t"),
            Some(AutoIndentNewlinePlan {
                text: "\n".to_string(),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_indent_newline_text(true, "\t"),
            Some(AutoIndentNewlinePlan {
                text: "\n\t".to_string(),
            })
        );
    }

    #[test]
    fn insert_at_cursor_plan_uses_current_cursor_offset() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::at(Position::new(0, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .insert_at_cursor_plan("ZZ"),
            Some(InsertAtCursorPlan {
                offset: 2,
                target_offset: 4,
            })
        );
    }

    #[test]
    fn insert_text_plan_replaces_primary_selection_range() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 2), Position::new(0, 5));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.insert_text_plan("ZZ"),
            Some(InsertTextPlan {
                replace_range: 2..5,
                insert_offset: 2,
                replaced_text: "pha".to_string(),
                inserted_text: "ZZ".to_string(),
                target_offset: 4,
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_insert_plans() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .insert_at_cursor_plan("!"),
            Some(InsertAtCursorPlan {
                offset: 5,
                target_offset: 6,
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .insert_text_plan("!"),
            Some(InsertTextPlan {
                replace_range: 5..5,
                insert_offset: 5,
                replaced_text: String::new(),
                inserted_text: "!".to_string(),
                target_offset: 6,
            })
        );
    }

    #[test]
    fn delete_before_cursor_plan_detects_paired_bracket_cleanup() {
        let buffer = TextBuffer::new("()");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .delete_before_cursor_plan(|opener| match opener {
                    '(' => Some(')'),
                    _ => None,
                }),
            Some(DeleteBeforeCursorPlan {
                primary_range: 0..1,
                paired_closer_range: Some(1..2),
                target_offset: 0,
            })
        );
    }

    #[test]
    fn delete_at_cursor_plan_deletes_next_character_boundary() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("éa");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.delete_at_cursor_plan(),
            Some(DeleteAtCursorPlan { range: 0..2 })
        );
    }

    #[test]
    fn core_snapshot_exposes_delete_character_plans() {
        let buffer = TextBuffer::new("abc");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .delete_before_cursor_plan(|_| None),
            Some(DeleteBeforeCursorPlan {
                primary_range: 0..1,
                paired_closer_range: None,
                target_offset: 0,
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .delete_at_cursor_plan(),
            Some(DeleteAtCursorPlan { range: 1..2 })
        );
    }

    #[test]
    fn delete_subword_left_plan_uses_prev_subword_boundary() {
        let buffer = TextBuffer::new("fooBar");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::at(Position::new(0, 6));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .delete_subword_left_plan(),
            Some(DeleteSubwordPlan {
                range: 3..6,
                target_offset: 3,
            })
        );
    }

    #[test]
    fn delete_subword_right_plan_uses_next_subword_boundary() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("fooBar");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.delete_subword_right_plan(),
            Some(DeleteSubwordPlan {
                range: 0..3,
                target_offset: 0,
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_delete_subword_plans() {
        let buffer = TextBuffer::new("snake_case");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .delete_subword_left_plan(),
            Some(DeleteSubwordPlan {
                range: 0..5,
                target_offset: 0,
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .delete_subword_right_plan(),
            Some(DeleteSubwordPlan {
                range: 5..6,
                target_offset: 5,
            })
        );
    }

    #[test]
    fn delete_subword_plans_handle_multibyte_boundaries_without_whole_buffer_text() {
        let buffer = TextBuffer::new("βetaΓamma");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .delete_subword_left_plan(),
            Some(DeleteSubwordPlan {
                range: 0..5,
                target_offset: 0,
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .delete_subword_right_plan(),
            Some(DeleteSubwordPlan {
                range: 5..11,
                target_offset: 5,
            })
        );
    }

    #[test]
    fn selected_text_plan_returns_linear_selection_text() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::from_anchor_head(Position::new(0, 2), Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .selected_text_plan(),
            Some(SelectedTextPlan::Linear("pha".to_string()))
        );
    }

    #[test]
    fn selected_text_plan_returns_block_selection_lines() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("abcd\nefgh");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(1, 3));
        selection.set_mode(SelectionMode::Block);

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.selected_text_plan(),
            Some(SelectedTextPlan::Block(vec![
                "bc".to_string(),
                "fg".to_string()
            ]))
        );
    }

    #[test]
    fn core_snapshot_exposes_selected_text_plan() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .selected_text_plan(),
            None
        );
        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor,
                Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4)),
                Vec::new(),
                Vec::new(),
            )
            .selected_text_plan(),
            Some(SelectedTextPlan::Linear("lph".to_string()))
        );
    }

    #[test]
    fn auto_surround_selection_plan_wraps_selection_and_updates_selection_columns() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 4));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_surround_selection_plan('(', ')'),
            Some(AutoSurroundSelectionPlan {
                edits: vec![
                    TextReplacementEdit::insert(4, ")"),
                    TextReplacementEdit::insert(1, "("),
                ],
                selection: Selection::from_anchor_head(Position::new(0, 2), Position::new(0, 5)),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_auto_surround_selection_plan() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(1, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_surround_selection_plan('"', '"'),
            Some(AutoSurroundSelectionPlan {
                edits: vec![
                    TextReplacementEdit::insert(8, "\""),
                    TextReplacementEdit::insert(1, "\""),
                ],
                selection: Selection::from_anchor_head(Position::new(0, 2), Position::new(1, 2)),
            })
        );
    }

    #[test]
    fn auto_close_bracket_plan_inserts_pair_and_places_cursor_between() {
        let buffer = TextBuffer::new("alpha");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::at(Position::new(0, 2));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_close_bracket_plan('(', ')', true, false),
            Some(AutoCloseBracketPlan {
                batch: PlannedEditBatch {
                    edits: vec![TextReplacementEdit::insert(2, "()")],
                    post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(3),
                },
            })
        );
    }

    #[test]
    fn auto_close_bracket_plan_rejects_quote_inside_string_or_comment() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha");
        cursor.set_position(Position::new(0, 1));
        selection.set_position(Position::new(0, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.auto_close_bracket_plan('"', '"', true, true), None);
        assert_eq!(core.auto_close_bracket_plan('(', ')', false, false), None);
    }

    #[test]
    fn core_snapshot_exposes_auto_close_bracket_plan() {
        let buffer = TextBuffer::new("");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .auto_close_bracket_plan('[', ']', true, false),
            Some(AutoCloseBracketPlan {
                batch: PlannedEditBatch {
                    edits: vec![TextReplacementEdit::insert(0, "[]")],
                    post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(1),
                },
            })
        );
    }

    #[test]
    fn skip_closing_bracket_plan_advances_over_matching_closer() {
        let buffer = TextBuffer::new("()");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .skip_closing_bracket_plan(')'),
            Some(SkipClosingBracketPlan { target_offset: 2 })
        );
    }

    #[test]
    fn skip_closing_bracket_plan_is_none_when_next_character_differs() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("(]");
        cursor.set_position(Position::new(0, 1));
        selection.set_position(Position::new(0, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.skip_closing_bracket_plan(')'), None);
    }

    #[test]
    fn core_snapshot_exposes_skip_closing_bracket_plan() {
        let buffer = TextBuffer::new("]");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .skip_closing_bracket_plan(']'),
            Some(SkipClosingBracketPlan { target_offset: 1 })
        );
    }

    #[test]
    fn find_all_occurrences_returns_all_matches_in_document_order() {
        let buffer = TextBuffer::new("aba aba aba");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .find_all_occurrences("aba"),
            vec![0..3, 4..7, 8..11]
        );
    }

    #[test]
    fn find_all_occurrences_returns_empty_for_empty_needle() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha");
        cursor.set_position(Position::new(0, 1));
        selection.set_position(Position::new(0, 1));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.find_all_occurrences("").is_empty());
    }

    #[test]
    fn core_snapshot_exposes_find_all_occurrences() {
        let buffer = TextBuffer::new("hello hello");
        let cursor = Cursor::at(Position::new(0, 0));
        let selection = Selection::at(Position::new(0, 0));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .find_all_occurrences("lo"),
            vec![3..5, 9..11]
        );
    }

    #[test]
    fn find_word_range_at_offset_finds_ascii_identifier_boundaries() {
        let buffer = TextBuffer::new("foo_bar baz");
        let cursor = Cursor::at(Position::new(0, 4));
        let selection = Selection::at(Position::new(0, 4));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .find_word_range_at_offset(4),
            Some(0..7)
        );
    }

    #[test]
    fn selection_or_word_under_cursor_text_prefers_linear_selection_then_word() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 7));
        let selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 4));

        assert_eq!(
            test_core_snapshot(&buffer, cursor.clone(), selection, Vec::new(), Vec::new(),)
                .selection_or_word_under_cursor_text(),
            Some("lph".to_string())
        );

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor,
                Selection::at(Position::new(0, 7)),
                Vec::new(),
                Vec::new(),
            )
            .selection_or_word_under_cursor_text(),
            Some("beta".to_string())
        );
    }

    #[test]
    fn core_snapshot_exposes_word_range_and_selection_or_word_queries() {
        let buffer = TextBuffer::new("foo βeta");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .find_word_range_at_offset(5),
            Some(4..9)
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .selection_or_word_under_cursor_text(),
            Some("βeta".to_string())
        );
    }

    #[test]
    fn find_word_range_at_offset_expands_quoted_region_without_materializing_document() {
        let buffer = TextBuffer::new("select `quoted_name` from dual");
        let cursor = Cursor::at(Position::new(0, 7));
        let selection = Selection::at(Position::new(0, 7));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .find_word_range_at_offset(7),
            Some(7..20)
        );
    }

    #[test]
    fn completion_query_plan_returns_trigger_offset_and_prefix() {
        let buffer = TextBuffer::new("select foo_bar");
        let cursor = Cursor::at(Position::new(0, 14));
        let selection = Selection::at(Position::new(0, 14));

        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .completion_query_plan(),
            CompletionQueryPlan {
                trigger_offset: 7,
                current_prefix: "foo_bar".to_string(),
            }
        );
    }

    #[test]
    fn word_target_at_offset_returns_range_and_text() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 7));
        selection.set_position(Position::new(0, 7));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.word_target_at_offset(7),
            Some(WordTarget {
                range: 6..10,
                text: "beta".to_string(),
            })
        );
    }

    #[test]
    fn core_snapshot_exposes_completion_and_rename_targets() {
        let buffer = TextBuffer::new("rename_me");
        let cursor = Cursor::at(Position::new(0, 3));
        let selection = Selection::at(Position::new(0, 3));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new(),
            )
            .word_target_at_offset(3),
            Some(WordTarget {
                range: 0..9,
                text: "rename_me".to_string(),
            })
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .rename_target_at_cursor(),
            Some(WordTarget {
                range: 0..9,
                text: "rename_me".to_string(),
            })
        );
    }

    #[test]
    fn toggle_primary_selection_mode_rebuilds_collection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(core.toggle_primary_selection_mode(), SelectionMode::Block);

        let state = core.selection_state();
        assert_eq!(state.expect_selection().mode(), SelectionMode::Block);
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.selection.mode()),
            Some(SelectionMode::Block)
        );
    }

    #[test]
    fn move_primary_cursor_down_preserves_vertical_affinity() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("abcd\nx\nabcd");
        cursor.set_position(Position::new(0, 3));
        selection.set_position(Position::new(0, 3));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_down();
        core.move_primary_cursor_down();

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(2, 3));
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(2, 3))
        );
    }

    #[test]
    fn move_primary_cursor_to_paragraph_boundaries_updates_collection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("one\n\ntwo\nthree\n\nfour");
        cursor.set_position(Position::new(2, 1));
        selection.set_position(Position::new(2, 1));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_to_paragraph_end();
        assert_eq!(
            core.selection_state().expect_cursor().position(),
            Position::new(5, 0)
        );

        core.move_primary_cursor_to_paragraph_start();
        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(2, 0));
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(2, 0))
        );
    }

    #[test]
    fn move_primary_cursor_to_next_and_previous_subword_updates_collection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alphaBeta gamma");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_to_next_subword_end();
        assert_eq!(
            core.selection_state().expect_cursor().position(),
            Position::new(0, 5)
        );

        core.move_primary_cursor_to_prev_subword_start();
        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(
            state
                .collection
                .primary()
                .map(|entry| entry.cursor.position()),
            Some(Position::new(0, 0))
        );
    }

    #[test]
    fn non_extending_motion_collapses_existing_primary_selection() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_left_with_selection(false);

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 4));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 4));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
    }

    #[test]
    fn extending_motion_preserves_original_anchor_across_repeated_moves() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 2));
        selection.set_position(Position::new(0, 2));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_right_with_selection(true);
        core.move_primary_cursor_right_with_selection(true);

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 4));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 2));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
    }

    #[test]
    fn smart_home_moves_to_indent_then_line_start() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("    alpha");
        cursor.set_position(Position::new(0, 7));
        selection.set_position(Position::new(0, 7));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_to_smart_home(false);
        assert_eq!(
            core.selection_state().expect_cursor().position(),
            Position::new(0, 4)
        );

        core.move_primary_cursor_to_smart_home(false);
        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 0));
        assert_eq!(state.expect_selection().head(), Position::new(0, 0));
    }

    #[test]
    fn smart_home_with_selection_preserves_anchor_and_updates_collection() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("    alpha");
        cursor.set_position(Position::new(0, 7));
        selection.set_position(Position::new(0, 7));
        extra_cursors.push((
            Cursor::at(Position::new(0, 2)),
            Selection::at(Position::new(0, 2)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.move_primary_cursor_to_smart_home(true);

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 4));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 7));
        assert_eq!(state.expect_selection().head(), Position::new(0, 4));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.collection.len(), 2);
    }

    #[test]
    fn plan_multi_cursor_backspace_edits_uses_selections_and_previous_boundaries() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::at(Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_backspace_edits(),
            vec![
                MultiCursorEditPlan {
                    slot: 0,
                    start: 0,
                    end: 5,
                    replacement: String::new(),
                },
                MultiCursorEditPlan {
                    slot: 1,
                    start: 9,
                    end: 10,
                    replacement: String::new(),
                },
            ]
        );
    }

    #[test]
    fn plan_multi_cursor_delete_edits_skips_cursors_at_buffer_end() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha");
        cursor.set_position(Position::new(0, 2));
        selection.set_position(Position::new(0, 2));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::at(Position::new(0, 5)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_delete_edits(),
            vec![MultiCursorEditPlan {
                slot: 0,
                start: 2,
                end: 3,
                replacement: String::new(),
            }]
        );
    }

    #[test]
    fn plan_multi_cursor_newline_edits_uses_per_line_replacements() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(0, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 3));
        extra_cursors.push((
            Cursor::at(Position::new(1, 1)),
            Selection::at(Position::new(1, 1)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_newline_edits(|line| format!("<{}>", line)),
            vec![
                MultiCursorEditPlan {
                    slot: 0,
                    start: 1,
                    end: 3,
                    replacement: "<0>".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 1,
                    start: 7,
                    end: 7,
                    replacement: "<1>".to_string(),
                },
            ]
        );
    }

    #[test]
    fn plan_multi_cursor_indent_edits_deduplicates_shared_offsets() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        selection.set_position(Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::at(Position::new(0, 5)),
        ));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::at(Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_indent_edits("  "),
            vec![
                MultiCursorEditPlan {
                    slot: 2,
                    start: 10,
                    end: 10,
                    replacement: "  ".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 0,
                    start: 5,
                    end: 5,
                    replacement: "  ".to_string(),
                },
            ]
        );
    }

    #[test]
    fn core_snapshot_supports_read_only_multi_cursor_caret_offsets() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::at(Position::new(0, 5));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 5)),
                Selection::at(Position::new(0, 5)),
            ),
            (
                Cursor::at(Position::new(0, 10)),
                Selection::at(Position::new(0, 10)),
            ),
        ];

        let offsets = test_core_snapshot(&buffer, cursor, selection, extra_cursors, Vec::new())
            .multi_cursor_caret_offsets();

        assert_eq!(offsets, vec![(0, 5), (1, 5), (2, 10)]);
    }

    #[test]
    fn core_snapshot_builds_multi_cursor_replace_command_plan_with_final_offsets() {
        let buffer = TextBuffer::new("alpha beta gamma");
        let primary_cursor = Cursor::at(Position::new(0, 5));
        let primary_selection =
            Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 5)),
                Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5)),
            ),
            (
                Cursor::at(Position::new(0, 10)),
                Selection::at(Position::new(0, 10)),
            ),
        ];

        let plan = test_core_snapshot(
            &buffer,
            primary_cursor,
            primary_selection,
            extra_cursors,
            Vec::new(),
        )
        .multi_cursor_replace_plan("x")
        .expect("expected multi-cursor replace plan");

        assert_eq!(plan.final_offsets, vec![5, 5, 10]);
        assert_eq!(
            plan.edits,
            vec![
                MultiCursorEditPlan {
                    slot: 2,
                    start: 10,
                    end: 10,
                    replacement: "x".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 0,
                    start: 0,
                    end: 5,
                    replacement: "x".to_string(),
                },
            ]
        );
    }

    #[test]
    fn multi_cursor_newline_plan_uses_auto_indent_and_preserves_slot_offsets() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("begin\n  value");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(1, 2)),
            Selection::at(Position::new(1, 2)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let plan = core
            .multi_cursor_newline_plan(true, "  ")
            .expect("expected multi-cursor newline plan");

        assert_eq!(plan.final_offsets, vec![5, 8]);
        assert_eq!(
            plan.edits,
            vec![
                MultiCursorEditPlan {
                    slot: 1,
                    start: 8,
                    end: 8,
                    replacement: "\n  ".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 0,
                    start: 0,
                    end: 5,
                    replacement: "\n  ".to_string(),
                },
            ]
        );
    }

    #[test]
    fn restore_multi_cursor_caret_offsets_rebuilds_collection_from_offsets() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");
        cursor.set_position(Position::new(0, 2));
        selection.set_position(Position::new(0, 2));
        extra_cursors.push((
            Cursor::at(Position::new(1, 1)),
            Selection::at(Position::new(1, 1)),
        ));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.restore_multi_cursor_caret_offsets(&[0, 6, 11]));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 0));
        assert_eq!(state.expect_selection().head(), Position::new(0, 0));
        assert_eq!(state.extra_cursors().len(), 2);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(1, 0));
        assert_eq!(state.extra_cursors()[1].0.position(), Position::new(2, 0));
        assert_eq!(state.collection.len(), 3);
    }

    #[test]
    fn plan_text_transform_edits_uses_word_fallback_and_skips_duplicate_ranges() {
        let buffer = TextBuffer::new("alpha beta gamma");
        let primary_cursor = Cursor::at(Position::new(0, 5));
        let primary_selection = Selection::at(Position::new(0, 5));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 5)),
                Selection::at(Position::new(0, 5)),
            ),
            (
                Cursor::at(Position::new(0, 16)),
                Selection::at(Position::new(0, 16)),
            ),
        ];

        let edits = test_core_snapshot(
            &buffer,
            primary_cursor,
            primary_selection,
            extra_cursors,
            Vec::new(),
        )
        .plan_text_transform_edits(|text| text.to_uppercase());

        assert_eq!(
            edits,
            vec![
                TextReplacementEdit {
                    range: 11..16,
                    replacement: "GAMMA".to_string(),
                },
                TextReplacementEdit {
                    range: 0..5,
                    replacement: "ALPHA".to_string(),
                },
            ]
        );
    }

    #[test]
    fn selected_line_replacement_edit_preserves_trailing_newline_shape() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("one\ntwo\nthree\n");
        cursor.set_position(Position::new(1, 3));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 3));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let edit = core
            .selected_line_replacement_edit(|lines| lines.iter().rev().cloned().collect())
            .expect("expected selected line replacement");

        assert_eq!(edit.range, 0..8);
        assert_eq!(edit.replacement, "two\none\n");
    }

    #[test]
    fn plan_rotated_text_replacements_uses_word_fallback_ranges() {
        let buffer = TextBuffer::new("alpha beta gamma");
        let primary_cursor = Cursor::at(Position::new(0, 16));
        let primary_selection =
            Selection::from_anchor_head(Position::new(0, 11), Position::new(0, 16));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 5)),
                Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5)),
            ),
            (
                Cursor::at(Position::new(0, 10)),
                Selection::at(Position::new(0, 10)),
            ),
        ];

        let edits = test_core_snapshot(
            &buffer,
            primary_cursor,
            primary_selection,
            extra_cursors,
            Vec::new(),
        )
        .plan_rotated_text_replacements();

        assert_eq!(
            edits,
            vec![
                TextReplacementEdit {
                    range: 11..16,
                    replacement: "beta".to_string(),
                },
                TextReplacementEdit {
                    range: 6..10,
                    replacement: "alpha".to_string(),
                },
                TextReplacementEdit {
                    range: 0..5,
                    replacement: "gamma".to_string(),
                },
            ]
        );
    }

    #[test]
    fn line_prefix_edit_batch_converts_indent_to_insert_replacements() {
        let buffer = TextBuffer::new("one\ntwo");
        let snapshot = test_core_snapshot(
            &buffer,
            Cursor::at(Position::new(1, 1)),
            Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 1)),
            Vec::new(),
            Vec::new(),
        );

        let batch = snapshot
            .line_prefix_edit_batch(2, false, LinePrefixEditMode::Indent)
            .expect("indent batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit::insert(0, "  "),
                    TextReplacementEdit::insert(4, "  "),
                ],
                post_apply_selection: PostApplySelection::Keep,
            }
        );
    }

    #[test]
    fn line_prefix_edit_batch_converts_dedent_to_deletions_and_cursor_move() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("    one\n  two");
        cursor.set_position(Position::new(0, 4));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 2));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let batch = core
            .line_prefix_edit_batch(4, false, LinePrefixEditMode::Dedent)
            .expect("dedent batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit {
                        range: 0..4,
                        replacement: String::new(),
                    },
                    TextReplacementEdit {
                        range: 8..10,
                        replacement: String::new(),
                    },
                ],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(0, 0)),
            }
        );
    }

    #[test]
    fn line_prefix_edit_batch_converts_toggle_comment_to_mixed_replacements() {
        let buffer = TextBuffer::new("  one\n  two");
        let snapshot = test_core_snapshot(
            &buffer,
            Cursor::at(Position::new(1, 1)),
            Selection::from_anchor_head(Position::new(0, 0), Position::new(1, 1)),
            Vec::new(),
            Vec::new(),
        );

        let batch = snapshot
            .line_prefix_edit_batch(4, false, LinePrefixEditMode::ToggleComment)
            .expect("comment batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit::insert(8, "-- "),
                    TextReplacementEdit::insert(2, "-- "),
                ],
                post_apply_selection: PostApplySelection::Keep,
            }
        );
    }

    #[test]
    fn move_selected_lines_up_edit_batch_wraps_structural_replacement_and_cursor_target() {
        let buffer = TextBuffer::new("zero\none\ntwo\n");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::from_anchor_head(Position::new(1, 0), Position::new(2, 1));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .move_selected_lines_up_edit_batch()
            .expect("move up batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 0..13,
                    replacement: "one\ntwo\nzero\n".to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(0, 2)),
            }
        );
    }

    #[test]
    fn duplicate_selected_lines_down_edit_batch_preserves_ordered_insertions() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(2, 1));
        let selection = Selection::at(Position::new(2, 1));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .duplicate_selected_lines_down_edit_batch()
            .expect("duplicate down batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit::insert(12, "\n"),
                    TextReplacementEdit::insert(13, "two\n"),
                ],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(3, 1)),
            }
        );
    }

    #[test]
    fn join_lines_edit_batch_uses_offset_based_cursor_retargeting() {
        let buffer = TextBuffer::new("alpha\n  beta");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .join_lines_edit_batch()
            .expect("join lines batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 6..7,
                    replacement: " ".to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(7),
            }
        );
    }

    #[test]
    fn transpose_chars_edit_batch_uses_offset_based_cursor_retargeting() {
        let buffer = TextBuffer::new("abcd");
        let cursor = Cursor::at(Position::new(0, 2));
        let selection = Selection::at(Position::new(0, 2));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .transpose_chars_edit_batch()
            .expect("transpose batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 1..3,
                    replacement: "cb".to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(3),
            }
        );
    }

    #[test]
    fn insert_text_edit_batch_replaces_selection_and_retargets_to_insert_end() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 5));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .insert_text_edit_batch("x")
            .expect("insert text batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 0..5,
                    replacement: "x".to_string(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(1),
            }
        );
    }

    #[test]
    fn delete_before_cursor_edit_batch_includes_paired_closer_cleanup() {
        let buffer = TextBuffer::new("()");
        let cursor = Cursor::at(Position::new(0, 1));
        let selection = Selection::at(Position::new(0, 1));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .delete_before_cursor_edit_batch(|ch| match ch {
                '(' => Some(')'),
                '[' => Some(']'),
                '{' => Some('}'),
                _ => None,
            })
            .expect("delete before cursor batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit {
                        range: 0..1,
                        replacement: String::new(),
                    },
                    TextReplacementEdit {
                        range: 1..2,
                        replacement: String::new(),
                    },
                ],
                post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(0),
            }
        );
    }

    #[test]
    fn whole_line_cut_edit_batch_deletes_line_and_retargets_cursor() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(1, 1));
        let selection = Selection::at(Position::new(1, 1));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .whole_line_cut_edit_batch()
            .expect("whole line cut batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 5..9,
                    replacement: String::new(),
                }],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(1, 0)),
            }
        );
    }

    #[test]
    fn cut_to_end_of_line_edit_batch_is_delete_only() {
        let buffer = TextBuffer::new("alpha beta\ngamma");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::at(Position::new(0, 6));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .cut_to_end_of_line_edit_batch()
            .expect("cut to end batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range: 6..11,
                    replacement: String::new(),
                }],
                post_apply_selection: PostApplySelection::Keep,
            }
        );
    }

    #[test]
    fn primary_selection_deletion_edit_batch_preserves_block_ordering_and_cursor_target() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("abcd\nefgh\nijkl");
        cursor.set_position(Position::new(1, 2));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(2, 3));
        selection.set_mode(SelectionMode::Block);

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let batch = core
            .primary_selection_deletion_edit_batch()
            .expect("primary selection deletion batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![
                    TextReplacementEdit {
                        range: 11..13,
                        replacement: String::new(),
                    },
                    TextReplacementEdit {
                        range: 6..8,
                        replacement: String::new(),
                    },
                    TextReplacementEdit {
                        range: 1..3,
                        replacement: String::new(),
                    },
                ],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(0, 1)),
            }
        );
    }

    #[test]
    fn whole_line_paste_edit_batch_inserts_text_and_moves_cursor_to_next_line() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let cursor = Cursor::at(Position::new(1, 2));
        let selection = Selection::at(Position::new(1, 2));

        let batch = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .whole_line_paste_edit_batch("alpha")
            .expect("whole line paste batch");

        assert_eq!(
            batch,
            PlannedEditBatch {
                edits: vec![TextReplacementEdit::insert(5, "alpha\n")],
                post_apply_selection: PostApplySelection::MovePrimaryCursor(Position::new(2, 2)),
            }
        );
    }

    #[test]
    fn primary_replacement_byte_range_prefers_explicit_then_marked_then_selection_then_cursor() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new()
            )
            .primary_replacement_byte_range(Some(2..4), Some(6..8)),
            2..4
        );
        assert_eq!(
            test_core_snapshot(
                &buffer,
                cursor.clone(),
                selection.clone(),
                Vec::new(),
                Vec::new()
            )
            .primary_replacement_byte_range(None, Some(6..8)),
            6..8
        );
        assert_eq!(
            test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
                .primary_replacement_byte_range(None, None),
            0..5
        );

        let cursor_only = test_core_snapshot(
            &buffer,
            Cursor::at(Position::new(0, 6)),
            Selection::at(Position::new(0, 6)),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(cursor_only.primary_replacement_byte_range(None, None), 6..6);
    }

    #[test]
    fn primary_text_replacement_plan_retargets_to_end_of_inserted_text() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));

        let plan = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .primary_text_replacement_plan(None, None, "omega");

        assert_eq!(
            plan,
            PrimaryReplacementPlan {
                batch: PlannedEditBatch {
                    edits: vec![TextReplacementEdit {
                        range: 0..5,
                        replacement: "omega".to_string(),
                    }],
                    post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(5),
                },
            }
        );
    }

    #[test]
    fn primary_text_replacement_plan_retargets_explicit_completion_range_to_insert_end() {
        let buffer = TextBuffer::new("sel value");
        let cursor = Cursor::at(Position::new(0, 3));
        let selection = Selection::at(Position::new(0, 3));

        let plan = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .primary_text_replacement_plan(Some(0..3), None, "select");

        assert_eq!(
            plan,
            PrimaryReplacementPlan {
                batch: PlannedEditBatch {
                    edits: vec![TextReplacementEdit {
                        range: 0..3,
                        replacement: "select".to_string(),
                    }],
                    post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(6),
                },
            }
        );
    }

    #[test]
    fn marked_text_replacement_plan_tracks_marked_range_and_selected_offset() {
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 6));
        let selection = Selection::at(Position::new(0, 6));

        let plan = test_core_snapshot(&buffer, cursor, selection, Vec::new(), Vec::new())
            .marked_text_replacement_plan(Some(6..6), None, "xyz", Some(1));

        assert_eq!(
            plan,
            MarkedTextReplacementPlan {
                batch: PlannedEditBatch {
                    edits: vec![TextReplacementEdit {
                        range: 6..6,
                        replacement: "xyz".to_string(),
                    }],
                    post_apply_selection: PostApplySelection::MovePrimaryCursorToOffset(7),
                },
                marked_range: Some(6..9),
            }
        );
    }

    #[test]
    fn plan_multi_cursor_replace_edits_deduplicates_shared_ranges() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");
        cursor.set_position(Position::new(0, 5));
        let mut selection = Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5)),
        ));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::at(Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_replace_edits("x"),
            vec![
                MultiCursorEditPlan {
                    slot: 2,
                    start: 10,
                    end: 10,
                    replacement: "x".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 0,
                    start: 0,
                    end: 5,
                    replacement: "x".to_string(),
                },
            ]
        );
    }

    #[test]
    fn plan_multi_cursor_insert_edits_deduplicates_shared_offsets() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 5));
        selection.set_position(Position::new(0, 5));
        extra_cursors.push((
            Cursor::at(Position::new(0, 5)),
            Selection::at(Position::new(0, 5)),
        ));
        extra_cursors.push((
            Cursor::at(Position::new(0, 10)),
            Selection::at(Position::new(0, 10)),
        ));

        let core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert_eq!(
            core.plan_multi_cursor_insert_edits("  "),
            vec![
                MultiCursorEditPlan {
                    slot: 2,
                    start: 10,
                    end: 10,
                    replacement: "  ".to_string(),
                },
                MultiCursorEditPlan {
                    slot: 0,
                    start: 5,
                    end: 5,
                    replacement: "  ".to_string(),
                },
            ]
        );
    }

    #[test]
    fn core_snapshot_plans_rotated_selection_edits_in_document_order() {
        let buffer = TextBuffer::new("alpha beta gamma");
        let primary_cursor = Cursor::at(Position::new(0, 16));
        let primary_selection =
            Selection::from_anchor_head(Position::new(0, 11), Position::new(0, 16));
        let extra_cursors = vec![
            (
                Cursor::at(Position::new(0, 5)),
                Selection::from_anchor_head(Position::new(0, 0), Position::new(0, 5)),
            ),
            (
                Cursor::at(Position::new(0, 10)),
                Selection::at(Position::new(0, 10)),
            ),
        ];

        let edits = test_core_snapshot(
            &buffer,
            primary_cursor,
            primary_selection,
            extra_cursors,
            Vec::new(),
        )
        .plan_rotated_selection_edits(|position| match position.column {
            10 => Some(6..10),
            _ => None,
        });

        assert_eq!(
            edits,
            vec![
                SelectionRotationEdit {
                    range: 11..16,
                    replacement: "beta".to_string(),
                },
                SelectionRotationEdit {
                    range: 6..10,
                    replacement: "alpha".to_string(),
                },
                SelectionRotationEdit {
                    range: 0..5,
                    replacement: "gamma".to_string(),
                },
            ]
        );
    }

    #[test]
    fn set_collapsed_cursors_at_offsets_rebuilds_primary_and_extras() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_collapsed_cursors_at_offsets(&[0, 6, 11]));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.expect_selection().range().start, Position::new(0, 0));
        assert_eq!(state.expect_selection().range().end, Position::new(0, 0));
        assert_eq!(state.extra_cursors().len(), 2);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(1, 0));
        assert_eq!(state.extra_cursors()[1].0.position(), Position::new(2, 0));
    }

    #[test]
    fn set_primary_selection_from_offsets_preserves_reversed_direction() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta gamma");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_primary_selection_from_offsets(10, 6));

        let state = core.selection_state();
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 10));
        assert_eq!(state.expect_selection().head(), Position::new(0, 6));
        assert!(state.expect_selection().is_reversed());
        assert_eq!(state.expect_cursor().position(), Position::new(0, 6));
    }

    #[test]
    fn select_entire_line_uses_line_start_as_drag_anchor() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta");
        cursor.set_position(Position::new(1, 2));
        selection.set_position(Position::new(1, 2));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let drag_anchor = core.select_entire_line(1);

        let state = core.selection_state();
        assert_eq!(drag_anchor, Position::new(1, 0));
        assert_eq!(state.expect_cursor().position(), Position::new(1, 0));
        assert_eq!(state.expect_selection().anchor(), Position::new(1, 0));
        assert_eq!(state.expect_selection().head(), Position::new(1, 4));
    }

    #[test]
    fn select_word_at_offset_or_move_primary_cursor_prefers_word_anchor() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let drag_anchor = core.select_word_at_offset_or_move_primary_cursor(7, Position::new(0, 7));

        let state = core.selection_state();
        assert_eq!(drag_anchor, Position::new(0, 6));
        assert_eq!(state.expect_cursor().position(), Position::new(0, 10));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 6));
        assert_eq!(state.expect_selection().head(), Position::new(0, 10));
    }

    #[test]
    fn select_word_at_offset_or_move_primary_cursor_falls_back_when_no_word_exists() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("  ");
        cursor.set_position(Position::new(0, 0));
        selection.set_position(Position::new(0, 0));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let drag_anchor = core.select_word_at_offset_or_move_primary_cursor(0, Position::new(0, 0));

        let state = core.selection_state();
        assert_eq!(drag_anchor, Position::new(0, 0));
        assert_eq!(state.expect_cursor().position(), Position::new(0, 0));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 0));
        assert_eq!(state.expect_selection().head(), Position::new(0, 0));
    }

    #[test]
    fn begin_primary_mouse_selection_extends_from_existing_anchor() {
        let (
            buffer,
            mut cursor,
            _selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 6));
        let mut selection = Selection::from_anchor_head(Position::new(0, 1), Position::new(0, 6));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        let drag_anchor = core.begin_primary_mouse_selection(Position::new(0, 9), true);

        let state = core.selection_state();
        assert_eq!(drag_anchor, Position::new(0, 1));
        assert_eq!(state.expect_cursor().position(), Position::new(0, 9));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 1));
        assert_eq!(state.expect_selection().head(), Position::new(0, 9));
    }

    #[test]
    fn drag_primary_mouse_selection_updates_head_without_losing_anchor() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha beta");
        cursor.set_position(Position::new(0, 6));
        selection.set_position(Position::new(0, 6));

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        core.drag_primary_mouse_selection(Position::new(0, 2), Position::new(0, 9));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 9));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 2));
        assert_eq!(state.expect_selection().head(), Position::new(0, 9));
    }

    #[test]
    fn set_multi_selections_from_offsets_rebuilds_primary_and_extras() {
        let (
            buffer,
            mut cursor,
            mut selection,
            mut extra_cursors,
            mut collection,
            mut last_select_line_was_extend,
            mut selection_history,
        ) = test_core("alpha\nbeta\ngamma");

        let mut core = EditorCore::new(
            &buffer,
            &mut cursor,
            &mut selection,
            &mut extra_cursors,
            &mut collection,
            &mut last_select_line_was_extend,
            &mut selection_history,
        );

        assert!(core.set_multi_selections_from_offsets(&[(0, 5), (10, 6)]));

        let state = core.selection_state();
        assert_eq!(state.expect_cursor().position(), Position::new(0, 5));
        assert_eq!(state.expect_selection().anchor(), Position::new(0, 0));
        assert_eq!(state.expect_selection().head(), Position::new(0, 5));
        assert_eq!(state.extra_cursors().len(), 1);
        assert_eq!(state.extra_cursors()[0].0.position(), Position::new(1, 0));
        assert_eq!(state.extra_cursors()[0].1.anchor(), Position::new(1, 4));
        assert_eq!(state.extra_cursors()[0].1.head(), Position::new(1, 0));
        assert!(state.extra_cursors()[0].1.is_reversed());
    }
}
