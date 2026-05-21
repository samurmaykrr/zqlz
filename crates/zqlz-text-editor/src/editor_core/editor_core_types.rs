use crate::{Cursor, Position, Selection, SelectionsCollection, selection::SelectionEntry};

pub type ExtraCursor = (Cursor, Selection);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralRange {
    pub start: usize,
    pub end: usize,
    pub open: char,
    pub close: char,
}

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
pub struct ToggleBlockCommentPlan {
    pub batch: PlannedEditBatch,
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
