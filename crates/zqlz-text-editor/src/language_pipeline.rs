use crate::{
    AnchoredCodeAction, AnchoredInlayHint, Bias, Cursor, Diagnostic, DiagnosticLevel,
    EditorInlayHint, FoldRegion, Highlight, HighlightKind, Position, SyntaxHighlighter,
    SyntaxRefreshStrategy, SyntaxSnapshot, TextBuffer, buffer::Change, detect_folds,
    detect_folds_in_range,
};
use gpui::Task;
use lsp_types::CodeActionOrCommand;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldInvalidation {
    Clean,
    Full,
    Range(std::ops::Range<usize>),
}

#[derive(Clone, Debug)]
pub struct LanguagePipelineSnapshot {
    pub syntax: SyntaxSnapshot,
    pub syntax_generation: u64,
    pub diagnostics: Vec<Highlight>,
    pub fold_regions: Vec<FoldRegion>,
    pub anchored_diagnostics: Vec<AnchoredDiagnostic>,
    pub inlay_hints: Arc<Vec<EditorInlayHint>>,
    pub anchored_inlay_hints: Arc<Vec<AnchoredInlayHint>>,
    pub code_actions: Vec<CodeActionOrCommand>,
    pub anchored_code_actions: Vec<AnchoredCodeAction>,
    pub reference_ranges: Vec<std::ops::Range<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnchoredDiagnostic {
    pub range: crate::AnchoredRange,
    pub kind: HighlightKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldRefresh {
    Full,
    Range(std::ops::Range<usize>),
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyntaxParseToken {
    revision: usize,
    generation: u64,
}

pub struct LanguagePipelineState {
    syntax_highlighter: Option<SyntaxHighlighter>,
    syntax_snapshot: SyntaxSnapshot,
    syntax_generation: u64,
    syntax_parse_task: Task<anyhow::Result<()>>,
    syntax_parse_generation: u64,
    syntax_reparse_queued: bool,
    cached_highlights: Arc<Vec<Highlight>>,
    diagnostics: Vec<Highlight>,
    diagnostics_stale: bool,
    fold_regions: Vec<FoldRegion>,
    inlay_hints: Arc<Vec<EditorInlayHint>>,
    code_actions: Vec<CodeActionOrCommand>,
    reference_ranges: Vec<std::ops::Range<usize>>,
    pending_fold_invalidation: FoldInvalidation,
}

impl Default for LanguagePipelineState {
    fn default() -> Self {
        Self::new()
    }
}

impl LanguagePipelineState {
    pub fn new() -> Self {
        let syntax_highlighter = match SyntaxHighlighter::new() {
            Ok(highlighter) => Some(highlighter),
            Err(error) => {
                tracing::error!(%error, "Failed to initialize SQL syntax highlighter");
                None
            }
        };

        Self {
            syntax_highlighter,
            syntax_snapshot: SyntaxSnapshot::empty(0),
            syntax_generation: 0,
            syntax_parse_task: Task::ready(Ok(())),
            syntax_parse_generation: 0,
            syntax_reparse_queued: false,
            cached_highlights: Arc::new(Vec::new()),
            diagnostics: Vec::new(),
            diagnostics_stale: false,
            fold_regions: Vec::new(),
            inlay_hints: Arc::new(Vec::new()),
            code_actions: Vec::new(),
            reference_ranges: Vec::new(),
            pending_fold_invalidation: FoldInvalidation::Clean,
        }
    }

    pub fn snapshot(&self, buffer: &TextBuffer, cursor: &Cursor) -> LanguagePipelineSnapshot {
        let anchored_diagnostics = self.anchored_diagnostics(buffer);
        let anchored_inlay_hints = self.anchored_inlay_hints(buffer);

        LanguagePipelineSnapshot {
            syntax: self.syntax_snapshot.clone(),
            syntax_generation: self.syntax_generation,
            diagnostics: self.diagnostics().to_vec(),
            fold_regions: self.fold_regions.clone(),
            anchored_diagnostics,
            inlay_hints: self.inlay_hints(),
            anchored_inlay_hints,
            code_actions: self.code_actions.clone(),
            anchored_code_actions: self.anchored_code_actions(cursor),
            reference_ranges: self.reference_ranges.clone(),
        }
    }

    pub fn syntax_snapshot(&self) -> &SyntaxSnapshot {
        &self.syntax_snapshot
    }

    pub fn syntax_highlights(&self) -> &[Highlight] {
        self.cached_highlights.as_ref()
    }

    pub fn syntax_highlights_arc(&self) -> Arc<Vec<Highlight>> {
        self.cached_highlights.clone()
    }

    pub fn highlight_kind_at(&self, offset: usize) -> HighlightKind {
        for highlight in self.cached_highlights.iter() {
            if highlight.start > offset {
                break;
            }
            if offset >= highlight.start && offset < highlight.end {
                return highlight.kind;
            }
        }
        HighlightKind::Default
    }

    pub fn has_syntax_highlighting(&self) -> bool {
        self.syntax_highlighter.is_some()
    }

    pub fn syntax_generation(&self) -> u64 {
        self.syntax_generation
    }

    pub fn refresh_syntax(
        &mut self,
        buffer: &TextBuffer,
        revision: usize,
        highlight_enabled: bool,
        syntax_refresh_strategy: &SyntaxRefreshStrategy,
    ) {
        let snapshot = if matches!(syntax_refresh_strategy, SyntaxRefreshStrategy::Disabled) {
            Some(SyntaxSnapshot::empty(revision))
        } else if highlight_enabled {
            self.syntax_highlighter
                .as_mut()
                .map(|highlighter| match syntax_refresh_strategy {
                    SyntaxRefreshStrategy::Disabled => SyntaxSnapshot::empty(revision),
                    SyntaxRefreshStrategy::FullDocument => {
                        highlighter.snapshot_rope(&buffer.rope(), revision)
                    }
                    SyntaxRefreshStrategy::VisibleRange(byte_range) => highlighter
                        .snapshot_rope_for_range(&buffer.rope(), revision, byte_range.clone()),
                })
            // When the highlighter is taken for async reparse, keep the
            // current (possibly interpolated) snapshot instead of replacing
            // it with an empty one. This avoids a race where bumping the
            // generation here would cause the async result to be rejected
            // by `accept_reparsed_syntax`, leaving highlights permanently empty.
        } else {
            Some(SyntaxSnapshot::empty(revision))
        };

        if let Some(snapshot) = snapshot {
            self.bump_syntax_parse_generation();
            self.set_syntax_snapshot(snapshot);
        }
    }

    pub fn interpolate_syntax(
        &mut self,
        changes: &[crate::buffer::Change],
        next_revision: usize,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
    ) {
        if !syntax_highlighting_enabled || !highlight_enabled {
            self.bump_syntax_parse_generation();
            self.set_syntax_snapshot(SyntaxSnapshot::empty(next_revision));
            return;
        }

        let snapshot = self.syntax_snapshot.interpolate(changes, next_revision);
        self.bump_syntax_parse_generation();
        self.set_syntax_snapshot(snapshot);
    }

    pub fn invalidate_syntax_tree(&mut self) {
        if let Some(ref mut highlighter) = self.syntax_highlighter {
            highlighter.invalidate_tree();
        }
    }

    pub fn take_syntax_highlighter(&mut self) -> Option<SyntaxHighlighter> {
        self.syntax_highlighter.take()
    }

    pub fn restore_syntax_highlighter(&mut self, highlighter: SyntaxHighlighter) {
        self.syntax_highlighter = Some(highlighter);
    }

    pub fn set_syntax_parse_task(&mut self, task: Task<anyhow::Result<()>>) {
        self.syntax_parse_task = task;
    }

    pub fn reset_syntax_parse_task(&mut self) {
        self.syntax_parse_task = Task::ready(Ok(()));
    }

    pub fn take_queued_syntax_reparse(&mut self) -> bool {
        std::mem::take(&mut self.syntax_reparse_queued)
    }

    pub fn begin_syntax_reparse(
        &mut self,
        revision: usize,
    ) -> Option<(SyntaxParseToken, SyntaxHighlighter)> {
        let Some(highlighter) = self.syntax_highlighter.take() else {
            self.syntax_reparse_queued = true;
            return None;
        };

        self.syntax_reparse_queued = false;
        self.bump_syntax_parse_generation();
        Some((
            SyntaxParseToken {
                revision,
                generation: self.syntax_parse_generation,
            },
            highlighter,
        ))
    }

    pub fn accept_reparsed_syntax(
        &mut self,
        token: SyntaxParseToken,
        current_revision: usize,
        snapshot: SyntaxSnapshot,
        buffer: &TextBuffer,
        folding_enabled: bool,
    ) -> Option<FoldRefresh> {
        if current_revision != token.revision
            || snapshot.revision() != token.revision
            || self.syntax_parse_generation != token.generation
        {
            return None;
        }

        self.set_syntax_snapshot(snapshot);
        Some(self.refresh_folds(buffer, None, folding_enabled))
    }

    pub fn diagnostics(&self) -> &[Highlight] {
        if self.diagnostics_stale {
            &[]
        } else {
            &self.diagnostics
        }
    }

    pub fn diagnostics_vec(&self) -> Vec<Highlight> {
        self.diagnostics().to_vec()
    }

    pub fn stored_diagnostics_vec(&self) -> Vec<Highlight> {
        self.diagnostics.clone()
    }

    pub fn set_diagnostics(&mut self, diagnostics: Vec<Highlight>) {
        self.diagnostics = diagnostics;
        self.diagnostics_stale = false;
    }

    pub fn apply_external_diagnostics(
        &mut self,
        buffer: &TextBuffer,
        diagnostics_enabled: bool,
        diagnostics: Vec<Diagnostic>,
    ) {
        if !diagnostics_enabled {
            self.clear_diagnostics();
            return;
        }

        self.set_diagnostics(Self::project_external_diagnostics(buffer, diagnostics));
    }

    pub fn clear_diagnostics(&mut self) {
        self.diagnostics.clear();
        self.diagnostics_stale = false;
    }

    pub fn invalidate_diagnostics(&mut self) {
        self.diagnostics_stale = true;
    }

    pub fn diagnostics_are_stale(&self) -> bool {
        self.diagnostics_stale
    }

    pub fn pending_edited_line_range(&self) -> Option<std::ops::Range<usize>> {
        match &self.pending_fold_invalidation {
            FoldInvalidation::Range(range) => Some(range.clone()),
            _ => None,
        }
    }

    pub fn set_syntax_snapshot(&mut self, snapshot: SyntaxSnapshot) {
        self.syntax_snapshot = snapshot;
        self.syntax_generation = self.syntax_generation.wrapping_add(1);
        self.cached_highlights = self.syntax_snapshot.highlights();
    }

    pub fn anchored_diagnostics(&self, buffer: &TextBuffer) -> Vec<AnchoredDiagnostic> {
        self.diagnostics()
            .iter()
            .filter_map(|highlight| {
                Some(AnchoredDiagnostic {
                    range: buffer
                        .anchored_range(highlight.start..highlight.end, Bias::Left, Bias::Right)
                        .ok()?,
                    kind: highlight.kind,
                })
            })
            .collect()
    }

    pub fn inlay_hints(&self) -> Arc<Vec<EditorInlayHint>> {
        self.inlay_hints.clone()
    }

    pub fn set_inlay_hints(&mut self, hints: Vec<EditorInlayHint>) {
        self.inlay_hints = Arc::new(hints);
    }

    pub fn clear_inlay_hints(&mut self) {
        self.inlay_hints = Arc::new(Vec::new());
    }

    pub fn anchored_inlay_hints(&self, buffer: &TextBuffer) -> Arc<Vec<AnchoredInlayHint>> {
        Arc::new(
            self.inlay_hints
                .iter()
                .filter_map(|hint| {
                    Some(AnchoredInlayHint {
                        anchor: buffer.anchor_at(hint.byte_offset, Bias::Right).ok()?,
                        label: hint.label.clone(),
                        side: hint.side,
                        kind: hint.kind,
                        padding_left: hint.padding_left,
                        padding_right: hint.padding_right,
                    })
                })
                .collect(),
        )
    }

    pub fn code_actions(&self) -> &[CodeActionOrCommand] {
        &self.code_actions
    }

    pub fn set_code_actions(&mut self, actions: Vec<CodeActionOrCommand>) {
        self.code_actions = actions;
    }

    pub fn clear_code_actions(&mut self) {
        self.code_actions.clear();
    }

    pub fn anchored_code_actions(&self, cursor: &Cursor) -> Vec<AnchoredCodeAction> {
        self.code_actions
            .iter()
            .map(|action| AnchoredCodeAction {
                line: cursor.position().line,
                label: match action {
                    CodeActionOrCommand::CodeAction(action) => action.title.clone(),
                    CodeActionOrCommand::Command(command) => command.title.clone(),
                },
            })
            .collect()
    }

    pub fn reference_ranges(&self) -> &[std::ops::Range<usize>] {
        &self.reference_ranges
    }

    pub fn reference_ranges_vec(&self) -> Vec<std::ops::Range<usize>> {
        self.reference_ranges.clone()
    }

    pub fn set_reference_ranges(&mut self, ranges: Vec<std::ops::Range<usize>>) {
        self.reference_ranges = ranges;
    }

    pub fn clear_reference_ranges(&mut self) {
        self.reference_ranges.clear();
    }

    pub fn fold_regions(&self) -> &[FoldRegion] {
        &self.fold_regions
    }

    pub fn fold_regions_vec(&self) -> Vec<FoldRegion> {
        self.fold_regions.clone()
    }

    pub fn record_buffer_changes(&mut self, changes: &[Change], buffer: &TextBuffer) {
        if let Some(range) = edited_line_range_from_changes(changes, buffer) {
            self.pending_fold_invalidation = match &self.pending_fold_invalidation {
                FoldInvalidation::Clean => FoldInvalidation::Range(range),
                FoldInvalidation::Full => FoldInvalidation::Full,
                FoldInvalidation::Range(existing) => FoldInvalidation::Range(
                    existing.start.min(range.start)..existing.end.max(range.end),
                ),
            };
        }
    }

    pub fn apply_buffer_change(
        &mut self,
        buffer: &TextBuffer,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
        folding_enabled: bool,
    ) -> FoldRefresh {
        self.record_buffer_changes(buffer.changes(), buffer);
        self.invalidate_diagnostics();
        self.clear_inlay_hints();
        self.interpolate_syntax(
            buffer.changes(),
            buffer.revision(),
            highlight_enabled,
            syntax_highlighting_enabled,
        );
        self.take_fold_refresh(buffer, folding_enabled)
    }

    pub fn refresh_buffer_state(
        &mut self,
        buffer: &TextBuffer,
        highlight_enabled: bool,
        syntax_refresh_strategy: &SyntaxRefreshStrategy,
        folding_enabled: bool,
    ) -> FoldRefresh {
        self.refresh_syntax(
            buffer,
            buffer.revision(),
            highlight_enabled,
            syntax_refresh_strategy,
        );
        self.refresh_folds(buffer, None, folding_enabled)
    }

    pub fn mark_full_fold_refresh(&mut self) {
        self.pending_fold_invalidation = FoldInvalidation::Full;
    }

    pub fn take_fold_refresh(&mut self, buffer: &TextBuffer, folding_enabled: bool) -> FoldRefresh {
        let invalidation =
            std::mem::replace(&mut self.pending_fold_invalidation, FoldInvalidation::Clean);

        match invalidation {
            FoldInvalidation::Clean => FoldRefresh::Full,
            FoldInvalidation::Full => self.refresh_folds(buffer, None, folding_enabled),
            FoldInvalidation::Range(line_range) => {
                self.refresh_folds(buffer, Some(line_range), folding_enabled)
            }
        }
    }

    pub fn refresh_folds(
        &mut self,
        buffer: &TextBuffer,
        edited_line_range: Option<std::ops::Range<usize>>,
        folding_enabled: bool,
    ) -> FoldRefresh {
        if !folding_enabled {
            self.fold_regions.clear();
            return FoldRefresh::Disabled;
        }

        if let Some(line_range) = edited_line_range {
            let mut next_regions = self.fold_regions.clone();
            next_regions.retain(|region| {
                region.end_line < line_range.start || region.start_line >= line_range.end
            });
            next_regions.extend(detect_folds_in_range(buffer, line_range.clone()));
            next_regions
                .sort_by_key(|region| (region.start_line, std::cmp::Reverse(region.end_line)));
            self.fold_regions = next_regions;
            FoldRefresh::Range(line_range)
        } else {
            self.fold_regions = detect_folds(buffer);
            FoldRefresh::Full
        }
    }

    fn bump_syntax_parse_generation(&mut self) {
        self.syntax_parse_generation = self.syntax_parse_generation.saturating_add(1);
    }

    fn project_external_diagnostics(
        buffer: &TextBuffer,
        diagnostics: Vec<Diagnostic>,
    ) -> Vec<Highlight> {
        diagnostics
            .into_iter()
            .filter_map(|diag| {
                let start = buffer
                    .position_to_offset(Position::new(diag.line, diag.column))
                    .ok()?;

                let end = diag
                    .end_line
                    .zip(diag.end_column)
                    .and_then(|(end_line, end_col)| {
                        buffer
                            .position_to_offset(Position::new(end_line, end_col))
                            .ok()
                    })
                    .unwrap_or(start + 1)
                    .max(start + 1);

                let kind = match diag.severity {
                    DiagnosticLevel::Error => HighlightKind::Error,
                    DiagnosticLevel::Warning | DiagnosticLevel::Info | DiagnosticLevel::Hint => {
                        HighlightKind::Error
                    }
                };

                Some(Highlight { start, end, kind })
            })
            .collect()
    }
}

pub(crate) fn edited_line_range_from_changes(
    changes: &[Change],
    buffer: &TextBuffer,
) -> Option<std::ops::Range<usize>> {
    let mut start_line = usize::MAX;
    let mut end_line = 0usize;

    for change in changes {
        let start = buffer.offset_to_position(change.offset).ok()?.line;
        let end_offset = change.offset + change.new_text.len().max(change.old_text.len());
        let end = buffer
            .offset_to_position(end_offset.min(buffer.len()))
            .ok()?
            .line;
        start_line = start_line.min(start);
        end_line = end_line.max(end.saturating_add(1));
    }

    if start_line == usize::MAX {
        None
    } else {
        Some(start_line..end_line.max(start_line + 1))
    }
}

pub fn build_language_pipeline_snapshot(
    syntax: SyntaxSnapshot,
    diagnostics: Vec<Highlight>,
    fold_regions: Vec<FoldRegion>,
) -> LanguagePipelineSnapshot {
    LanguagePipelineSnapshot {
        syntax,
        syntax_generation: 0,
        diagnostics,
        fold_regions,
        anchored_diagnostics: Vec::new(),
        inlay_hints: Arc::new(Vec::new()),
        anchored_inlay_hints: Arc::new(Vec::new()),
        code_actions: Vec::new(),
        anchored_code_actions: Vec::new(),
        reference_ranges: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{FoldRefresh, LanguagePipelineState, build_language_pipeline_snapshot};
    use crate::{
        Cursor, Diagnostic, DiagnosticLevel, Highlight, HighlightKind, Position, SyntaxSnapshot,
        TextBuffer,
    };
    use lsp_types::CodeActionOrCommand;

    #[test]
    fn pipeline_snapshot_preserves_combined_language_state() {
        let snapshot = build_language_pipeline_snapshot(
            SyntaxSnapshot::empty(7),
            vec![Highlight {
                start: 1,
                end: 3,
                kind: HighlightKind::Error,
            }],
            Vec::new(),
        );

        assert_eq!(snapshot.syntax.revision(), 7);
        assert_eq!(snapshot.diagnostics.len(), 1);
    }

    #[test]
    fn refresh_folds_supports_incremental_range_refresh() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select\nfrom\nwhere");
        let refresh = pipeline.refresh_folds(&buffer, Some(0..2), true);
        assert_eq!(refresh, FoldRefresh::Range(0..2));
    }

    #[test]
    fn record_buffer_changes_tracks_pending_range_invalidation() {
        let mut pipeline = LanguagePipelineState::new();
        let mut buffer = TextBuffer::new("select\nfrom");
        buffer.insert(0, "-- ").expect("insert change");

        pipeline.record_buffer_changes(buffer.changes(), &buffer);

        assert_eq!(
            pipeline.take_fold_refresh(&buffer, true),
            FoldRefresh::Range(0..1)
        );
    }

    #[test]
    fn apply_buffer_change_centralizes_language_invalidation() {
        let mut pipeline = LanguagePipelineState::new();
        let mut buffer = TextBuffer::new("begin\nselect 1\nend");
        pipeline.set_diagnostics(vec![Highlight {
            start: 0,
            end: 5,
            kind: HighlightKind::Error,
        }]);
        pipeline.set_inlay_hints(vec![crate::EditorInlayHint {
            byte_offset: 6,
            label: "hint".to_string(),
            side: crate::InlayHintSide::After,
            kind: None,
            padding_left: false,
            padding_right: false,
        }]);

        buffer.insert(0, "-- ").expect("buffer edit");

        let refresh = pipeline.apply_buffer_change(&buffer, true, true, true);

        assert_eq!(refresh, FoldRefresh::Range(0..1));
        assert!(pipeline.diagnostics().is_empty());
        assert!(pipeline.diagnostics_are_stale());
        assert!(pipeline.inlay_hints().is_empty());
        assert_eq!(pipeline.syntax_snapshot().revision(), buffer.revision());
    }

    #[test]
    fn stale_diagnostics_are_hidden_until_replaced() {
        let mut pipeline = LanguagePipelineState::new();
        pipeline.set_diagnostics(vec![Highlight {
            start: 0,
            end: 1,
            kind: HighlightKind::Error,
        }]);
        pipeline.invalidate_diagnostics();

        assert!(pipeline.diagnostics().is_empty());
        assert!(pipeline.diagnostics_are_stale());
    }

    #[test]
    fn apply_external_diagnostics_projects_line_column_ranges_into_highlights() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select\nfrom users");

        pipeline.apply_external_diagnostics(
            &buffer,
            true,
            vec![Diagnostic {
                line: 1,
                column: 0,
                end_line: Some(1),
                end_column: Some(4),
                message: "bad token".to_string(),
                severity: DiagnosticLevel::Error,
                source: None,
            }],
        );

        assert_eq!(
            pipeline.diagnostics(),
            &[Highlight {
                start: 7,
                end: 11,
                kind: HighlightKind::Error,
            }]
        );
    }

    #[test]
    fn apply_external_diagnostics_clears_state_when_diagnostics_are_disabled() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select 1");
        pipeline.set_diagnostics(vec![Highlight {
            start: 0,
            end: 1,
            kind: HighlightKind::Error,
        }]);

        pipeline.apply_external_diagnostics(&buffer, false, Vec::new());

        assert!(pipeline.diagnostics().is_empty());
    }

    #[test]
    fn anchored_diagnostics_follow_buffer_anchors() {
        let mut pipeline = LanguagePipelineState::new();
        pipeline.set_diagnostics(vec![Highlight {
            start: 0,
            end: 5,
            kind: HighlightKind::Error,
        }]);
        let buffer = TextBuffer::new("alpha beta");

        let anchored = pipeline.anchored_diagnostics(&buffer);
        assert_eq!(anchored.len(), 1);
        assert_eq!(anchored[0].kind, HighlightKind::Error);
    }

    #[test]
    fn refresh_syntax_populates_highlights_and_snapshot_revision() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select 1");
        pipeline.refresh_syntax(
            &buffer,
            11,
            true,
            &crate::SyntaxRefreshStrategy::FullDocument,
        );

        assert_eq!(pipeline.syntax_snapshot().revision(), 11);
        assert!(!pipeline.syntax_highlights().is_empty());
    }

    #[test]
    fn refresh_syntax_clears_highlights_when_policy_disables_syntax() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select 1");
        pipeline.refresh_syntax(&buffer, 4, true, &crate::SyntaxRefreshStrategy::Disabled);

        assert_eq!(pipeline.syntax_snapshot().revision(), 4);
        assert!(pipeline.syntax_highlights().is_empty());
    }

    #[test]
    fn syntax_generation_increments_when_snapshot_changes() {
        let mut pipeline = LanguagePipelineState::new();

        assert_eq!(pipeline.syntax_generation(), 0);

        pipeline.set_syntax_snapshot(SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 6,
                kind: HighlightKind::Keyword,
            }],
            1,
        ));
        let first_generation = pipeline.syntax_generation();

        pipeline.set_syntax_snapshot(SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 4,
                kind: HighlightKind::Identifier,
            }],
            1,
        ));

        assert!(pipeline.syntax_generation() > first_generation);
    }

    #[test]
    fn refresh_folds_clears_regions_when_policy_disables_folding() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("begin\nselect 1;\nend");
        pipeline.refresh_folds(&buffer, None, true);

        assert_eq!(
            pipeline.refresh_folds(&buffer, None, false),
            FoldRefresh::Disabled
        );
        assert!(pipeline.fold_regions().is_empty());
    }

    #[test]
    fn refresh_buffer_state_supports_viewport_local_syntax_strategy() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("alpha\nSELECT beta\nomega");

        let refresh = pipeline.refresh_buffer_state(
            &buffer,
            true,
            &crate::SyntaxRefreshStrategy::VisibleRange(6..17),
            true,
        );

        assert_eq!(refresh, FoldRefresh::Full);
        assert!(
            pipeline
                .syntax_highlights()
                .iter()
                .all(|highlight| highlight.start >= 6)
        );
        assert!(
            pipeline
                .syntax_highlights()
                .iter()
                .all(|highlight| highlight.end <= 17)
        );
        assert!(pipeline.syntax_highlights().iter().any(|highlight| {
            highlight.start == 6 && highlight.end == 12 && highlight.kind == HighlightKind::Keyword
        }));
    }

    #[test]
    fn accept_reparsed_syntax_rejects_stale_tokens_and_revisions() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("select 1");
        let (stale_token, highlighter) = pipeline
            .begin_syntax_reparse(3)
            .expect("syntax reparse token");
        pipeline.restore_syntax_highlighter(highlighter);
        let (current_token, _) = pipeline.begin_syntax_reparse(4).expect("fresh token");

        assert!(
            pipeline
                .accept_reparsed_syntax(stale_token, 4, SyntaxSnapshot::empty(3), &buffer, true)
                .is_none()
        );

        let refresh = pipeline
            .accept_reparsed_syntax(current_token, 4, SyntaxSnapshot::empty(4), &buffer, true)
            .expect("fresh syntax accepted");
        assert_eq!(refresh, FoldRefresh::Full);
        assert_eq!(pipeline.syntax_snapshot().revision(), 4);
    }

    #[test]
    fn begin_syntax_reparse_queues_follow_up_requests_while_busy() {
        let mut pipeline = LanguagePipelineState::new();
        let (token, highlighter) = pipeline
            .begin_syntax_reparse(1)
            .expect("syntax reparse token");

        assert!(pipeline.begin_syntax_reparse(2).is_none());
        assert!(pipeline.take_queued_syntax_reparse());
        assert!(!pipeline.take_queued_syntax_reparse());

        pipeline.restore_syntax_highlighter(highlighter);

        let (next_token, _) = pipeline.begin_syntax_reparse(2).expect("follow-up token");
        assert_eq!(token.revision, 1);
        assert_eq!(next_token.revision, 2);
    }

    #[test]
    fn snapshot_projects_semantic_language_state_through_buffer_and_cursor() {
        let mut pipeline = LanguagePipelineState::new();
        let buffer = TextBuffer::new("alpha beta");
        let cursor = Cursor::at(Position::new(0, 2));

        pipeline.set_inlay_hints(vec![crate::EditorInlayHint {
            byte_offset: 6,
            label: "hint".to_string(),
            side: crate::InlayHintSide::After,
            kind: None,
            padding_left: false,
            padding_right: false,
        }]);
        pipeline.set_code_actions(vec![CodeActionOrCommand::Command(lsp_types::Command {
            title: "Fix".to_string(),
            command: "fix".to_string(),
            arguments: None,
        })]);
        pipeline.set_reference_ranges(std::iter::once(0..5).collect());

        let snapshot = pipeline.snapshot(&buffer, &cursor);

        assert_eq!(snapshot.inlay_hints.len(), 1);
        assert_eq!(snapshot.anchored_inlay_hints.len(), 1);
        assert_eq!(snapshot.anchored_code_actions.len(), 1);
        assert_eq!(snapshot.anchored_code_actions[0].line, 0);
        assert_eq!(snapshot.reference_ranges, vec![0..5]);
        assert_eq!(
            buffer
                .resolve_anchor_offset(snapshot.anchored_inlay_hints[0].anchor)
                .expect("resolve inlay anchor"),
            6
        );
    }
}
