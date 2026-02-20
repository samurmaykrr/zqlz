//! Vim modal editing mode for the text editor.
//!
//! Implements the four primary vim modes:
//! - **Normal** – the default mode; motion and operator keys move/transform text.
//! - **Insert** – character input inserts text at the cursor (same as regular editing).
//! - **Visual** – like Normal but cursor movement extends a selection.
//! - **Command** – the `:` command line at the bottom of the editor.
//!
//! ## Key concepts
//!
//! A vim key sequence consists of an optional _count_, an optional _operator_ (`d`, `c`,
//! `y`), and a _motion_ or _text object_. For example, `d2w` means "delete two words".
//! This module tracks the pending operator and count in [`VimState`] so they can be
//! applied once a motion arrives.
//!
//! The entry point is [`VimState::handle_key`], which returns a [`VimAction`] describing
//! what `TextEditor` should do. Keeping all vim logic in this pure module makes it easy
//! to unit-test without a GPUI runtime.

use crate::buffer::{Position, TextBuffer};
use crate::cursor::Cursor;

// ============================================================================
// Mode
// ============================================================================

/// The current editing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VimMode {
    /// Navigation mode. Operators and motions transform text without inserting.
    #[default]
    Normal,
    /// Text-insertion mode. Every printable keystroke inserts a character.
    Insert,
    /// Selection mode. Cursor movement extends the selection.
    Visual,
    /// The `:` command line is active.
    Command,
}

// ============================================================================
// Pending operator
// ============================================================================

/// An operator that is waiting for a motion to define its range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOperator {
    Delete,
    Change,
    Yank,
}

// ============================================================================
// Actions returned to the editor
// ============================================================================

/// What the editor should do as a result of a vim keypress.
#[derive(Debug, PartialEq, Eq)]
pub enum VimAction {
    /// Carry on – either the key was fully consumed internally or it's a no-op.
    None,
    /// Enter Insert mode.
    EnterInsert,
    /// Return to Normal mode (from Insert, Visual, or Command).
    EnterNormal,
    /// Enter Visual mode.
    EnterVisual,
    /// Enter Command mode.
    EnterCommand,
    /// Move the cursor without changing the selection anchor.
    MoveCursor(CursorMotion),
    /// Extend the Visual selection to the new cursor position implied by the motion.
    ExtendSelection(CursorMotion),
    /// Delete the range described by the motion, then stay in Normal mode.
    DeleteMotion(CursorMotion),
    /// Delete the range described by the motion, then enter Insert mode.
    ChangeMotion(CursorMotion),
    /// Yank (copy) the range described by the motion.
    YankMotion(CursorMotion),
    /// Delete the current visual selection.
    DeleteSelection,
    /// Change (delete then insert) the current visual selection.
    ChangeSelection,
    /// Yank the current visual selection.
    YankSelection,
    /// Paste after cursor (lowercase `p`).
    PasteAfter,
    /// Paste before cursor (uppercase `P`).
    PasteBefore,
    /// Undo.
    Undo,
    /// Redo.
    Redo,
    /// Execute the command string accumulated so far and return to Normal.
    ExecuteCommand(String),
    /// Join the current line with the next line.
    JoinLines,
    /// Indent the current line or selection.
    Indent,
    /// Dedent the current line or selection.
    Dedent,
    /// Open a new line below and enter Insert mode (`o`).
    OpenLineBelow,
    /// Open a new line above and enter Insert mode (`O`).
    OpenLineAbove,
    /// Delete the character under the cursor (`x`), staying in Normal.
    DeleteCharAtCursor,
    /// Replace the character under the cursor (`r`), then stay in Normal.
    /// The contained character is the replacement.
    ReplaceChar(char),
    /// Uppercase/lowercase transformations (`gU`/`gu`) - currently no-op placeholder.
    TransformCase {
        uppercase: bool,
        motion: CursorMotion,
    },
}

// ============================================================================
// Cursor motions
// ============================================================================

/// A motion that describes how/where to move the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorMotion {
    /// `h` – move left one character.
    Left,
    /// `l` – move right one character.
    Right,
    /// `k` – move up one line.
    Up,
    /// `j` – move down one line.
    Down,
    /// `w` – to the start of the next word.
    WordForward,
    /// `b` – to the start of the previous word.
    WordBackward,
    /// `e` – to the end of the current (or next) word.
    WordEnd,
    /// `0` – to the start of the line.
    LineStart,
    /// `$` – to the end of the line.
    LineEnd,
    /// `^` – to the first non-blank character of the line.
    LineFirstNonBlank,
    /// `g_` – to the last non-blank character of the line.
    LineLastNonBlank,
    /// `gg` – to the first line.
    DocumentStart,
    /// `G` – to the last line.
    DocumentEnd,
    /// Inner-word text object (`iw`).
    InnerWord,
    /// A-word text object (`aw`, includes surrounding whitespace).
    AWord,
    /// Inner-quoted-string text object (`i"`, `i'`, `` i` ``).
    InnerQuoted(char),
    /// A-quoted-string text object (`a"`, `a'`, `` a` ``).
    AQuoted(char),
    /// Full current line (used for `dd`, `cc`, `yy`).
    CurrentLine,
    /// Applied `count` times (wraps another motion).
    Repeated {
        count: usize,
        motion: Box<CursorMotion>,
    },
}

// ============================================================================
// State machine
// ============================================================================

/// Persistent vim state tracked across keystrokes.
#[derive(Debug, Default)]
pub struct VimState {
    /// Current editing mode.
    pub mode: VimMode,

    /// Digit accumulator for count prefixes (e.g. `3` in `3w`).
    pending_count: String,

    /// Operator waiting for a motion (`d`, `c`, `y`), if any.
    pending_operator: Option<PendingOperator>,

    /// Awaiting a second key for a two-key sequence (e.g. `g` → `gg`/`g_`/`gU`).
    awaiting_g: bool,

    /// Awaiting `r<char>` – replace mode.
    awaiting_replace: bool,

    /// Awaiting a text-object disambiguator (`i` or `a`) after an operator.
    awaiting_text_object: Option<TextObjectKind>,

    /// The `:` command line buffer (only used in Command mode).
    pub command_buffer: String,
}

/// Which text-object prefix is pending (`i` inner or `a` a/around).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextObjectKind {
    Inner,
    Around,
}

impl VimState {
    /// Create a new `VimState` starting in Normal mode.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` when vim mode is enabled (i.e. when this struct is actually
    /// being used by the editor). Currently always returns `true`; the editor feature-
    /// gates by only creating the struct when vim is toggled on.
    pub fn is_enabled(&self) -> bool {
        true
    }

    /// Returns the current mode label, suitable for a mode indicator in the UI.
    pub fn mode_label(&self) -> &'static str {
        match self.mode {
            VimMode::Normal => "NORMAL",
            VimMode::Insert => "INSERT",
            VimMode::Visual => "VISUAL",
            VimMode::Command => "COMMAND",
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Consume the pending count string and return it as `usize`, defaulting to 1.
    fn take_count(&mut self) -> usize {
        if self.pending_count.is_empty() {
            return 1;
        }
        let n: usize = self.pending_count.parse().unwrap_or(1).max(1);
        self.pending_count.clear();
        n
    }

    /// Wrap a motion in a [`CursorMotion::Repeated`] when count > 1.
    fn maybe_repeated(&mut self, motion: CursorMotion) -> CursorMotion {
        let count = self.take_count();
        if count == 1 {
            motion
        } else {
            CursorMotion::Repeated {
                count,
                motion: Box::new(motion),
            }
        }
    }

    /// Clear all pending-operator / text-object state and reset the count.
    fn reset_pending(&mut self) {
        self.pending_count.clear();
        self.pending_operator = None;
        self.awaiting_g = false;
        self.awaiting_replace = false;
        self.awaiting_text_object = None;
    }

    /// Dispatch a completed (operator + motion) pair to a [`VimAction`].
    fn apply_operator(&mut self, motion: CursorMotion) -> VimAction {
        match self.pending_operator.take() {
            None => {
                // Plain motion – Normal vs Visual mode differ
                if self.mode == VimMode::Visual {
                    VimAction::ExtendSelection(motion)
                } else {
                    VimAction::MoveCursor(motion)
                }
            }
            Some(PendingOperator::Delete) => VimAction::DeleteMotion(motion),
            Some(PendingOperator::Change) => VimAction::ChangeMotion(motion),
            Some(PendingOperator::Yank) => VimAction::YankMotion(motion),
        }
    }

    // ── Main key handler ─────────────────────────────────────────────────────

    /// Process a single keypress in the current mode.
    ///
    /// Returns the [`VimAction`] that `TextEditor` should execute.
    /// The `key` string is the GPUI `keystroke.key` value (e.g. `"h"`, `"escape"`, `"enter"`).
    /// The `key_char` is `keystroke.key_char` (the printable character if any).
    pub fn handle_key(&mut self, key: &str, key_char: Option<&str>, shift: bool) -> VimAction {
        match self.mode {
            VimMode::Insert => self.handle_insert_key(key),
            VimMode::Command => self.handle_command_key(key, key_char),
            VimMode::Normal => self.handle_normal_key(key, key_char, shift),
            VimMode::Visual => self.handle_visual_key(key, key_char, shift),
        }
    }

    // ── Insert mode ──────────────────────────────────────────────────────────

    fn handle_insert_key(&mut self, key: &str) -> VimAction {
        match key {
            "escape" => {
                self.reset_pending();
                self.mode = VimMode::Normal;
                VimAction::EnterNormal
            }
            // All other keys are handled by the normal editing path.
            _ => VimAction::None,
        }
    }

    // ── Command mode ─────────────────────────────────────────────────────────

    fn handle_command_key(&mut self, key: &str, key_char: Option<&str>) -> VimAction {
        match key {
            "escape" => {
                self.command_buffer.clear();
                self.mode = VimMode::Normal;
                VimAction::EnterNormal
            }
            "enter" => {
                let cmd = self.command_buffer.trim().to_owned();
                self.command_buffer.clear();
                self.mode = VimMode::Normal;
                VimAction::ExecuteCommand(cmd)
            }
            "backspace" => {
                self.command_buffer.pop();
                VimAction::None
            }
            _ => {
                if let Some(text) = key_char {
                    self.command_buffer.push_str(text);
                }
                VimAction::None
            }
        }
    }

    // ── Normal mode ──────────────────────────────────────────────────────────

    fn handle_normal_key(&mut self, key: &str, key_char: Option<&str>, shift: bool) -> VimAction {
        // ── r<char> – replace one character ──────────────────────────────────
        if self.awaiting_replace {
            self.awaiting_replace = false;
            if let Some(text) = key_char {
                if let Some(ch) = text.chars().next() {
                    self.reset_pending();
                    return VimAction::ReplaceChar(ch);
                }
            }
            // Non-printable cancels replace
            self.reset_pending();
            return VimAction::None;
        }

        // ── text objects after i/a ────────────────────────────────────────────
        if let Some(obj_kind) = self.awaiting_text_object.take() {
            return self.handle_text_object(key, obj_kind);
        }

        // ── two-key g-prefixed sequences ──────────────────────────────────────
        if self.awaiting_g {
            self.awaiting_g = false;
            match key {
                "g" => {
                    // gg – start of document
                    let motion = self.maybe_repeated(CursorMotion::DocumentStart);
                    return self.apply_operator(motion);
                }
                "_" => {
                    let motion = self.maybe_repeated(CursorMotion::LineLastNonBlank);
                    return self.apply_operator(motion);
                }
                "u" => {
                    // gu<motion> – lowercase (stub: plain move)
                    let count = self.take_count();
                    let motion = if count > 1 {
                        CursorMotion::Repeated {
                            count,
                            motion: Box::new(CursorMotion::WordForward),
                        }
                    } else {
                        CursorMotion::WordForward
                    };
                    return VimAction::TransformCase {
                        uppercase: false,
                        motion,
                    };
                }
                "U" => {
                    let count = self.take_count();
                    let motion = if count > 1 {
                        CursorMotion::Repeated {
                            count,
                            motion: Box::new(CursorMotion::WordForward),
                        }
                    } else {
                        CursorMotion::WordForward
                    };
                    return VimAction::TransformCase {
                        uppercase: true,
                        motion,
                    };
                }
                _ => {
                    self.reset_pending();
                    return VimAction::None;
                }
            }
        }

        // ── Count digits ──────────────────────────────────────────────────────
        // Leading `0` is the "line start" motion, not a count digit.
        if key.len() == 1 {
            let ch = key.chars().next().unwrap_or('\0');
            if ch.is_ascii_digit() && (ch != '0' || !self.pending_count.is_empty()) {
                self.pending_count.push(ch);
                return VimAction::None;
            }
        }

        // ── Mode transitions ──────────────────────────────────────────────────
        match key {
            // `i` / `a` are overloaded: with a pending operator they start a text
            // object (`di w`, `da"`); without one they enter Insert mode.
            "i" if !shift => {
                if self.pending_operator.is_some() {
                    self.awaiting_text_object = Some(TextObjectKind::Inner);
                    return VimAction::None;
                }
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::EnterInsert;
            }
            "a" if !shift => {
                if self.pending_operator.is_some() {
                    self.awaiting_text_object = Some(TextObjectKind::Around);
                    return VimAction::None;
                }
                // `a` alone → Insert after cursor
                self.reset_pending();
                self.mode = VimMode::Insert;
                // The editor will move right by one and then enter insert.
                return VimAction::EnterInsert; // editor handles the right-by-one
            }
            "I" | "i" if shift => {
                // `I` → go to first non-blank, then Insert
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::ChangeMotion(CursorMotion::LineFirstNonBlank);
            }
            "A" => {
                // `A` → go to end of line, then Insert
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::ChangeMotion(CursorMotion::LineEnd);
            }
            "o" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::OpenLineBelow;
            }
            "O" | "o" if shift => {
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::OpenLineAbove;
            }
            "v" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Visual;
                return VimAction::EnterVisual;
            }
            "escape" | "ctrl-[" => {
                self.reset_pending();
                // Already in Normal; clear any selection.
                return VimAction::EnterNormal;
            }
            ":" => {
                self.reset_pending();
                self.mode = VimMode::Command;
                return VimAction::EnterCommand;
            }

            // ── Operators ────────────────────────────────────────────────────
            "d" if !shift => {
                if self.pending_operator == Some(PendingOperator::Delete) {
                    // `dd` → delete current line
                    self.pending_operator = None;
                    let count = self.take_count();
                    let motion = if count > 1 {
                        CursorMotion::Repeated {
                            count,
                            motion: Box::new(CursorMotion::CurrentLine),
                        }
                    } else {
                        CursorMotion::CurrentLine
                    };
                    return VimAction::DeleteMotion(motion);
                }
                self.pending_operator = Some(PendingOperator::Delete);
                return VimAction::None;
            }
            "c" if !shift => {
                if self.pending_operator == Some(PendingOperator::Change) {
                    // `cc` → change current line
                    self.pending_operator = None;
                    let count = self.take_count();
                    let motion = if count > 1 {
                        CursorMotion::Repeated {
                            count,
                            motion: Box::new(CursorMotion::CurrentLine),
                        }
                    } else {
                        CursorMotion::CurrentLine
                    };
                    return VimAction::ChangeMotion(motion);
                }
                self.pending_operator = Some(PendingOperator::Change);
                return VimAction::None;
            }
            "y" if !shift => {
                if self.pending_operator == Some(PendingOperator::Yank) {
                    // `yy` → yank current line
                    self.pending_operator = None;
                    let count = self.take_count();
                    let motion = if count > 1 {
                        CursorMotion::Repeated {
                            count,
                            motion: Box::new(CursorMotion::CurrentLine),
                        }
                    } else {
                        CursorMotion::CurrentLine
                    };
                    return VimAction::YankMotion(motion);
                }
                self.pending_operator = Some(PendingOperator::Yank);
                return VimAction::None;
            }
            "D" => {
                // Delete from cursor to end of line
                self.reset_pending();
                return VimAction::DeleteMotion(CursorMotion::LineEnd);
            }
            "C" => {
                // Change from cursor to end of line
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::ChangeMotion(CursorMotion::LineEnd);
            }
            "Y" => {
                // Yank current line (alias for `yy`)
                self.reset_pending();
                return VimAction::YankMotion(CursorMotion::CurrentLine);
            }

            // ── Motions ──────────────────────────────────────────────────────
            "h" | "left" => {
                let motion = self.maybe_repeated(CursorMotion::Left);
                return self.apply_operator(motion);
            }
            "l" | "right" => {
                let motion = self.maybe_repeated(CursorMotion::Right);
                return self.apply_operator(motion);
            }
            "k" | "up" => {
                let motion = self.maybe_repeated(CursorMotion::Up);
                return self.apply_operator(motion);
            }
            "j" | "down" => {
                let motion = self.maybe_repeated(CursorMotion::Down);
                return self.apply_operator(motion);
            }
            "w" => {
                let motion = self.maybe_repeated(CursorMotion::WordForward);
                return self.apply_operator(motion);
            }
            "b" => {
                let motion = self.maybe_repeated(CursorMotion::WordBackward);
                return self.apply_operator(motion);
            }
            "e" => {
                let motion = self.maybe_repeated(CursorMotion::WordEnd);
                return self.apply_operator(motion);
            }
            "0" => {
                // Line start (only reached if pending_count is empty, checked above)
                self.reset_pending();
                let motion = CursorMotion::LineStart;
                return self.apply_operator(motion);
            }
            "$" | "end" => {
                let count = self.take_count();
                // count > 1 means "end of line N lines down" – simplified to current line end
                let motion = if count > 1 {
                    CursorMotion::Repeated {
                        count: count - 1,
                        motion: Box::new(CursorMotion::Down),
                    }
                } else {
                    CursorMotion::LineEnd
                };
                return self.apply_operator(motion);
            }
            "^" | "home" => {
                self.reset_pending();
                let motion = CursorMotion::LineFirstNonBlank;
                return self.apply_operator(motion);
            }
            "g" => {
                self.awaiting_g = true;
                return VimAction::None;
            }
            "G" => {
                let motion = self.maybe_repeated(CursorMotion::DocumentEnd);
                return self.apply_operator(motion);
            }

            // ── Paste ────────────────────────────────────────────────────────
            "p" if !shift => {
                self.reset_pending();
                return VimAction::PasteAfter;
            }
            "P" | "p" if shift => {
                self.reset_pending();
                return VimAction::PasteBefore;
            }

            // ── Undo / Redo ──────────────────────────────────────────────────
            "u" if !shift => {
                self.reset_pending();
                return VimAction::Undo;
            }
            "r" if shift => {
                // Ctrl+R – redo (represented as shift+r in key matching for ctrl-r)
                // Actually ctrl-r is handled via ctrl modifier in the editor; this is a fallback.
                self.reset_pending();
                return VimAction::Redo;
            }

            // ── Replace ──────────────────────────────────────────────────────
            "r" if !shift => {
                self.awaiting_replace = true;
                return VimAction::None;
            }

            // ── x – delete character at cursor ───────────────────────────────
            "x" if !shift => {
                let count = self.take_count();
                self.reset_pending();
                if count > 1 {
                    // Delete count chars (simplify to repeated single deletes)
                    return VimAction::DeleteMotion(CursorMotion::Repeated {
                        count,
                        motion: Box::new(CursorMotion::Right),
                    });
                }
                return VimAction::DeleteCharAtCursor;
            }
            "X" | "x" if shift => {
                // Delete char before cursor (backspace equivalent)
                self.reset_pending();
                return VimAction::DeleteMotion(CursorMotion::Left);
            }

            // ── J – join lines ───────────────────────────────────────────────
            "J" if shift => {
                self.reset_pending();
                return VimAction::JoinLines;
            }

            // ── >> / << – indent / dedent ────────────────────────────────────
            ">" if shift => {
                // The second `>` in `>>` arrives as a separate keypress.
                // We treat a bare `>` with shift as indent trigger.
                self.reset_pending();
                return VimAction::Indent;
            }
            "<" if shift => {
                self.reset_pending();
                return VimAction::Dedent;
            }

            _ => {
                // Unknown key – reset pending state.
                self.reset_pending();
            }
        }

        VimAction::None
    }

    // ── Visual mode ──────────────────────────────────────────────────────────

    fn handle_visual_key(&mut self, key: &str, key_char: Option<&str>, shift: bool) -> VimAction {
        match key {
            "escape" => {
                self.reset_pending();
                self.mode = VimMode::Normal;
                return VimAction::EnterNormal;
            }
            "d" | "x" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Normal;
                return VimAction::DeleteSelection;
            }
            "c" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Insert;
                return VimAction::ChangeSelection;
            }
            "y" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Normal;
                return VimAction::YankSelection;
            }
            "p" if !shift => {
                self.reset_pending();
                self.mode = VimMode::Normal;
                return VimAction::PasteAfter;
            }
            _ => {}
        }

        // For motions in Visual mode, reuse normal key handling but force
        // ExtendSelection via apply_operator (self.mode == Visual).
        self.handle_normal_key(key, key_char, shift)
    }

    // ── Text object resolver ─────────────────────────────────────────────────

    fn handle_text_object(&mut self, key: &str, kind: TextObjectKind) -> VimAction {
        let motion = match key {
            "w" => match kind {
                TextObjectKind::Inner => CursorMotion::InnerWord,
                TextObjectKind::Around => CursorMotion::AWord,
            },
            "\"" | "'" | "`" => {
                let quote_char = key.chars().next().unwrap_or('"');
                match kind {
                    TextObjectKind::Inner => CursorMotion::InnerQuoted(quote_char),
                    TextObjectKind::Around => CursorMotion::AQuoted(quote_char),
                }
            }
            _ => {
                self.reset_pending();
                return VimAction::None;
            }
        };
        self.apply_operator(motion)
    }
}

// ============================================================================
// Motion execution helpers
// ============================================================================

/// Apply a [`CursorMotion`] to `cursor` given `buffer`, returning the new position.
///
/// This is a pure function used by the editor to calculate where to move and/or
/// what range to operate on when an operator+motion pair resolves.
pub fn resolve_motion(motion: &CursorMotion, cursor: &Cursor, buffer: &TextBuffer) -> Position {
    match motion {
        CursorMotion::Left => {
            let mut c = cursor.clone();
            c.move_left(buffer);
            c.position()
        }
        CursorMotion::Right => {
            let mut c = cursor.clone();
            c.move_right(buffer);
            c.position()
        }
        CursorMotion::Up => {
            let mut c = cursor.clone();
            c.move_up(buffer);
            c.position()
        }
        CursorMotion::Down => {
            let mut c = cursor.clone();
            c.move_down(buffer);
            c.position()
        }
        CursorMotion::WordForward => {
            let mut c = cursor.clone();
            c.move_to_next_word_start(buffer);
            c.position()
        }
        CursorMotion::WordBackward => {
            let mut c = cursor.clone();
            c.move_to_prev_word_start(buffer);
            c.position()
        }
        CursorMotion::WordEnd => {
            // Move to end of current/next word: advance right until we hit a non-word char,
            // then walk forward into the next word and to its end.
            let text = buffer.text();
            let start_offset = buffer.position_to_offset(cursor.position()).unwrap_or(0);
            let target_offset = word_end_offset(&text, start_offset);
            buffer
                .offset_to_position(target_offset)
                .unwrap_or(cursor.position())
        }
        CursorMotion::LineStart => Position::new(cursor.position().line, 0),
        CursorMotion::LineEnd => {
            let mut c = cursor.clone();
            c.move_to_line_end(buffer);
            c.position()
        }
        CursorMotion::LineFirstNonBlank => {
            let line = cursor.position().line;
            let col = buffer
                .line(line)
                .map(|text| text.chars().position(|ch| !ch.is_whitespace()).unwrap_or(0))
                .unwrap_or(0);
            Position::new(line, col)
        }
        CursorMotion::LineLastNonBlank => {
            let line = cursor.position().line;
            let col = buffer
                .line(line)
                .map(|text| {
                    let trimmed = text.trim_end_matches('\n').trim_end();
                    trimmed.len()
                })
                .unwrap_or(0);
            Position::new(line, col)
        }
        CursorMotion::DocumentStart => Position::new(0, 0),
        CursorMotion::DocumentEnd => {
            let mut c = cursor.clone();
            c.move_to_document_end(buffer);
            c.position()
        }
        CursorMotion::CurrentLine => {
            // "Line" means from start to end of current line.
            let line = cursor.position().line;
            let line_len = buffer.line(line).map(|t| t.len()).unwrap_or(0);
            Position::new(line, line_len)
        }
        CursorMotion::InnerWord | CursorMotion::AWord => {
            // Move to end of current word.
            let text = buffer.text();
            let start_offset = buffer.position_to_offset(cursor.position()).unwrap_or(0);
            let end = word_end_offset(&text, start_offset);
            buffer.offset_to_position(end).unwrap_or(cursor.position())
        }
        CursorMotion::InnerQuoted(quote_char) | CursorMotion::AQuoted(quote_char) => {
            // Find the closing quote character on the current line.
            let text = buffer.text();
            let start_offset = buffer.position_to_offset(cursor.position()).unwrap_or(0);
            let end = find_closing_quote(&text, start_offset, *quote_char);
            buffer.offset_to_position(end).unwrap_or(cursor.position())
        }
        CursorMotion::Repeated { count, motion } => {
            let mut current_pos = cursor.position();
            for _ in 0..*count {
                let temp_cursor = Cursor::at(current_pos);
                current_pos = resolve_motion(motion, &temp_cursor, buffer);
            }
            current_pos
        }
    }
}

/// Find the byte offset of the end of the word starting at or after `from`.
fn word_end_offset(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = from;

    // If we're already at a word character, walk to the end of it.
    if i < len && is_word_byte(bytes[i]) {
        while i < len && is_word_byte(bytes[i]) {
            i += 1;
        }
        return i.saturating_sub(0); // position after last word char
    }

    // Skip non-word chars, then walk to end of the next word.
    while i < len && !is_word_byte(bytes[i]) {
        i += 1;
    }
    while i < len && is_word_byte(bytes[i]) {
        i += 1;
    }
    i
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Find the closing quote character after `from` on the current line.
fn find_closing_quote(text: &str, from: usize, quote: char) -> usize {
    let mut i = from;
    let bytes = text.as_bytes();
    let len = bytes.len();

    // Skip opening quote if at one
    if i < len && bytes[i] == quote as u8 {
        i += 1;
    }

    while i < len {
        if bytes[i] == quote as u8 {
            return i + 1;
        }
        if bytes[i] == b'\n' {
            break; // Don't cross line boundaries
        }
        i += 1;
    }
    from // No closing quote found; return original
}

// ============================================================================
// Command execution
// ============================================================================

/// The result of executing a `:` command.
#[derive(Debug, PartialEq, Eq)]
pub enum CommandResult {
    /// Handled; editor should stay open.
    Ok,
    /// The command caused a find-replace operation; the editor should run it.
    FindReplace {
        pattern: String,
        replacement: String,
    },
    /// Unknown command – editor may show an error.
    Unknown(String),
}

/// Parse and execute a `:` command string.
///
/// Supports: `w` (save – no-op here, editor handles), `q` / `wq`, `nohl`,
/// and `%s/pattern/replacement/g`.
pub fn execute_command(cmd: &str) -> CommandResult {
    let trimmed = cmd.trim();

    match trimmed {
        "w" | "wq" | "q" => CommandResult::Ok,
        "nohl" | "nohlsearch" => CommandResult::Ok,
        _ if trimmed.starts_with("%s/") => {
            // :%s/pattern/replacement/[g]
            parse_substitute(trimmed)
        }
        _ if trimmed.starts_with("s/") => parse_substitute(trimmed),
        _ => CommandResult::Unknown(trimmed.to_owned()),
    }
}

fn parse_substitute(cmd: &str) -> CommandResult {
    // Strip leading `%s/` or `s/`
    let body = cmd
        .trim_start_matches('%')
        .trim_start_matches('s')
        .trim_start_matches('/');

    // Split on `/`; ignore trailing flags like `g`
    let mut parts = body.splitn(3, '/');
    let pattern = parts.next().unwrap_or("").to_owned();
    let replacement = parts.next().unwrap_or("").to_owned();

    if pattern.is_empty() {
        return CommandResult::Ok;
    }

    CommandResult::FindReplace {
        pattern,
        replacement,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn normal_state() -> VimState {
        VimState::new()
    }

    // ── Mode transitions ──────────────────────────────────────────────────────

    #[test]
    fn test_insert_mode_entry_exit() {
        let mut state = normal_state();
        assert_eq!(state.mode, VimMode::Normal);

        let action = state.handle_key("i", None, false);
        assert_eq!(action, VimAction::EnterInsert);
        assert_eq!(state.mode, VimMode::Insert);

        let action = state.handle_key("escape", None, false);
        assert_eq!(action, VimAction::EnterNormal);
        assert_eq!(state.mode, VimMode::Normal);
    }

    #[test]
    fn test_visual_mode_entry_exit() {
        let mut state = normal_state();
        let action = state.handle_key("v", None, false);
        assert_eq!(action, VimAction::EnterVisual);
        assert_eq!(state.mode, VimMode::Visual);

        let action = state.handle_key("escape", None, false);
        assert_eq!(action, VimAction::EnterNormal);
        assert_eq!(state.mode, VimMode::Normal);
    }

    #[test]
    fn test_command_mode_entry_exit() {
        let mut state = normal_state();
        let action = state.handle_key(":", None, false);
        assert_eq!(action, VimAction::EnterCommand);
        assert_eq!(state.mode, VimMode::Command);

        // Escape exits command mode
        let action = state.handle_key("escape", None, false);
        assert_eq!(action, VimAction::EnterNormal);
        assert_eq!(state.mode, VimMode::Normal);
    }

    // ── Normal mode motions ───────────────────────────────────────────────────

    #[test]
    fn test_hjkl_motions() {
        let mut state = normal_state();
        assert_eq!(
            state.handle_key("h", None, false),
            VimAction::MoveCursor(CursorMotion::Left)
        );
        assert_eq!(
            state.handle_key("l", None, false),
            VimAction::MoveCursor(CursorMotion::Right)
        );
        assert_eq!(
            state.handle_key("k", None, false),
            VimAction::MoveCursor(CursorMotion::Up)
        );
        assert_eq!(
            state.handle_key("j", None, false),
            VimAction::MoveCursor(CursorMotion::Down)
        );
    }

    #[test]
    fn test_word_motions() {
        let mut state = normal_state();
        assert_eq!(
            state.handle_key("w", None, false),
            VimAction::MoveCursor(CursorMotion::WordForward)
        );
        assert_eq!(
            state.handle_key("b", None, false),
            VimAction::MoveCursor(CursorMotion::WordBackward)
        );
        assert_eq!(
            state.handle_key("e", None, false),
            VimAction::MoveCursor(CursorMotion::WordEnd)
        );
    }

    #[test]
    fn test_line_motions() {
        let mut state = normal_state();
        assert_eq!(
            state.handle_key("0", None, false),
            VimAction::MoveCursor(CursorMotion::LineStart)
        );
        // $ requires shift on most keyboards, but key string is "$"
        assert_eq!(
            state.handle_key("$", None, false),
            VimAction::MoveCursor(CursorMotion::LineEnd)
        );
        assert_eq!(
            state.handle_key("G", None, true),
            VimAction::MoveCursor(CursorMotion::DocumentEnd)
        );
    }

    #[test]
    fn test_gg_motion() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("g", None, false), VimAction::None);
        assert!(state.awaiting_g);
        assert_eq!(
            state.handle_key("g", None, false),
            VimAction::MoveCursor(CursorMotion::DocumentStart)
        );
    }

    // ── Count prefix ─────────────────────────────────────────────────────────

    #[test]
    fn test_count_prefix() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("3", None, false), VimAction::None);
        assert_eq!(state.pending_count, "3");

        let action = state.handle_key("w", None, false);
        assert_eq!(
            action,
            VimAction::MoveCursor(CursorMotion::Repeated {
                count: 3,
                motion: Box::new(CursorMotion::WordForward)
            })
        );
        assert!(state.pending_count.is_empty());
    }

    // ── Operators ─────────────────────────────────────────────────────────────

    #[test]
    fn test_delete_word() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("d", None, false), VimAction::None);
        assert_eq!(state.pending_operator, Some(PendingOperator::Delete));

        let action = state.handle_key("w", None, false);
        assert_eq!(action, VimAction::DeleteMotion(CursorMotion::WordForward));
        assert!(state.pending_operator.is_none());
    }

    #[test]
    fn test_dd_deletes_current_line() {
        let mut state = normal_state();
        state.handle_key("d", None, false);
        let action = state.handle_key("d", None, false);
        assert_eq!(action, VimAction::DeleteMotion(CursorMotion::CurrentLine));
    }

    #[test]
    fn test_change_word() {
        let mut state = normal_state();
        state.handle_key("c", None, false);
        let action = state.handle_key("w", None, false);
        assert_eq!(action, VimAction::ChangeMotion(CursorMotion::WordForward));
    }

    #[test]
    fn test_yank_current_line() {
        let mut state = normal_state();
        state.handle_key("y", None, false);
        let action = state.handle_key("y", None, false);
        assert_eq!(action, VimAction::YankMotion(CursorMotion::CurrentLine));
    }

    // ── Text objects ─────────────────────────────────────────────────────────

    #[test]
    fn test_delete_inner_word() {
        let mut state = normal_state();
        state.handle_key("d", None, false);
        state.handle_key("i", None, false);
        let action = state.handle_key("w", None, false);
        assert_eq!(action, VimAction::DeleteMotion(CursorMotion::InnerWord));
    }

    #[test]
    fn test_delete_inner_quoted() {
        let mut state = normal_state();
        state.handle_key("d", None, false);
        state.handle_key("i", None, false);
        let action = state.handle_key("\"", None, false);
        assert_eq!(
            action,
            VimAction::DeleteMotion(CursorMotion::InnerQuoted('"'))
        );
    }

    // ── Replace ──────────────────────────────────────────────────────────────

    #[test]
    fn test_replace_char() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("r", None, false), VimAction::None);
        assert!(state.awaiting_replace);
        let action = state.handle_key("a", Some("a"), false);
        assert_eq!(action, VimAction::ReplaceChar('a'));
    }

    // ── Visual mode operations ────────────────────────────────────────────────

    #[test]
    fn test_visual_delete_selection() {
        let mut state = normal_state();
        state.handle_key("v", None, false);
        assert_eq!(state.mode, VimMode::Visual);

        let action = state.handle_key("d", None, false);
        assert_eq!(action, VimAction::DeleteSelection);
        assert_eq!(state.mode, VimMode::Normal);
    }

    #[test]
    fn test_visual_yank_selection() {
        let mut state = normal_state();
        state.handle_key("v", None, false);
        let action = state.handle_key("y", None, false);
        assert_eq!(action, VimAction::YankSelection);
        assert_eq!(state.mode, VimMode::Normal);
    }

    // ── Command mode ─────────────────────────────────────────────────────────

    #[test]
    fn test_command_accumulate_and_execute() {
        let mut state = normal_state();
        state.handle_key(":", None, false);
        state.handle_key("w", Some("w"), false);
        let action = state.handle_key("enter", None, false);
        assert_eq!(action, VimAction::ExecuteCommand("w".to_owned()));
        assert_eq!(state.mode, VimMode::Normal);
    }

    #[test]
    fn test_command_substitute_parse() {
        let result = execute_command("%s/foo/bar/g");
        assert_eq!(
            result,
            CommandResult::FindReplace {
                pattern: "foo".to_owned(),
                replacement: "bar".to_owned()
            }
        );
    }

    #[test]
    fn test_command_wq_ok() {
        assert_eq!(execute_command("wq"), CommandResult::Ok);
        assert_eq!(execute_command("q"), CommandResult::Ok);
    }

    // ── Undo / Redo ──────────────────────────────────────────────────────────

    #[test]
    fn test_undo_redo_keys() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("u", None, false), VimAction::Undo);
        // ctrl-r is typically handled via control modifier – test the raw shift case
        assert_eq!(state.handle_key("r", None, true), VimAction::Redo);
    }

    // ── x – delete char ──────────────────────────────────────────────────────

    #[test]
    fn test_x_delete_char() {
        let mut state = normal_state();
        assert_eq!(
            state.handle_key("x", None, false),
            VimAction::DeleteCharAtCursor
        );
    }

    // ── Paste ────────────────────────────────────────────────────────────────

    #[test]
    fn test_paste_after_before() {
        let mut state = normal_state();
        assert_eq!(state.handle_key("p", None, false), VimAction::PasteAfter);
        assert_eq!(state.handle_key("P", None, true), VimAction::PasteBefore);
    }

    // ── Mode label ───────────────────────────────────────────────────────────

    #[test]
    fn test_mode_labels() {
        let mut state = normal_state();
        assert_eq!(state.mode_label(), "NORMAL");
        state.mode = VimMode::Insert;
        assert_eq!(state.mode_label(), "INSERT");
        state.mode = VimMode::Visual;
        assert_eq!(state.mode_label(), "VISUAL");
        state.mode = VimMode::Command;
        assert_eq!(state.mode_label(), "COMMAND");
    }
}
