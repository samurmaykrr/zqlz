use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{Result, bail};
use gpui::Task;
use lsp_types::{CodeActionOrCommand, Uri};
use ropey::Rope;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    AnchoredDiagnostic, BufferSnapshot, Cursor, Diagnostic, DisplaySnapshot, FoldDisplayState,
    FoldRefresh, FoldRegion, Highlight, HighlightKind, LanguagePipelineSnapshot, SyntaxHighlighter,
    SyntaxRefreshStrategy, TextBuffer,
    buffer::Change,
    language_pipeline::{LanguagePipelineState, SyntaxParseToken},
};

const INTERNAL_SQL_DOCUMENT_URI: &str = "sql://internal";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocumentIdentity {
    Internal { uri: Uri },
    External { uri: Uri, path: Option<PathBuf> },
}

impl DocumentIdentity {
    pub fn internal() -> Option<Self> {
        Some(Self::Internal {
            uri: INTERNAL_SQL_DOCUMENT_URI.parse::<Uri>().ok()?,
        })
    }

    pub fn internal_with_label(label: impl AsRef<str>) -> Option<Self> {
        let mut normalized = String::new();
        for character in label.as_ref().chars() {
            if character.is_ascii_alphanumeric() {
                normalized.push(character.to_ascii_lowercase());
            } else if matches!(character, '-' | '_' | ' ' | '.') {
                normalized.push('-');
            }
        }
        let normalized = normalized.trim_matches('-');
        let unique_label = if normalized.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            format!("{}-{}", normalized, Uuid::new_v4())
        };
        let uri = format!("sql://internal/{unique_label}")
            .parse::<Uri>()
            .ok()?;
        Some(Self::Internal { uri })
    }

    pub fn from_path(path: impl Into<PathBuf>) -> Option<Self> {
        let path = path.into();
        let uri = Url::from_file_path(&path)
            .ok()?
            .as_str()
            .parse::<Uri>()
            .ok()?;
        Some(Self::External {
            uri,
            path: Some(path),
        })
    }

    pub fn uri(&self) -> &Uri {
        match self {
            DocumentIdentity::Internal { uri } | DocumentIdentity::External { uri, .. } => uri,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            DocumentIdentity::Internal { .. } => None,
            DocumentIdentity::External { path, .. } => path.as_deref(),
        }
    }

    pub fn is_current_document_uri(&self, uri: &Uri) -> bool {
        self.uri() == uri
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentSettings {
    pub indent_size: usize,
    pub use_tabs: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EncodingPolicy {
    #[default]
    Utf8,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DocumentFileState {
    pub path: Option<PathBuf>,
    pub saved_mtime: Option<SystemTime>,
    pub encoding: EncodingPolicy,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DocumentCapability {
    #[default]
    ReadWrite,
    ReadOnly,
}

impl DocumentCapability {
    pub fn allows_edits(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

impl fmt::Display for DocumentCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadWrite => formatter.write_str("read-write"),
            Self::ReadOnly => formatter.write_str("read-only"),
        }
    }
}

impl DocumentFileState {
    pub fn from_identity(identity: &DocumentIdentity) -> Self {
        Self {
            path: identity.path().map(Path::to_path_buf),
            saved_mtime: None,
            encoding: EncodingPolicy::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineEnding {
    Lf,
    CrLf,
}

impl LineEnding {
    pub fn detect(text: &str) -> Self {
        if text.contains("\r\n") {
            Self::CrLf
        } else {
            Self::Lf
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
        }
    }
}

impl Default for DocumentSettings {
    fn default() -> Self {
        Self {
            indent_size: 4,
            use_tabs: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentContext {
    pub id: Uuid,
    pub identity: DocumentIdentity,
    pub settings: DocumentSettings,
    pub saved_revision: usize,
    pub line_ending: LineEnding,
    pub file_state: DocumentFileState,
    pub is_dirty: bool,
    pub capability: DocumentCapability,
    pub lifecycle_events: Vec<DocumentLifecycleEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocumentLifecycleEvent {
    Saved {
        revision: usize,
        saved_mtime: Option<SystemTime>,
    },
    Reloaded {
        previous_revision: usize,
        revision: usize,
        line_ending: LineEnding,
    },
}

impl DocumentContext {
    pub fn is_current_document_uri(&self, uri: &Uri) -> bool {
        self.identity.is_current_document_uri(uri)
    }
}

pub struct TextDocument {
    id: Uuid,
    identity: DocumentIdentity,
    settings: DocumentSettings,
    saved_revision: usize,
    line_ending: LineEnding,
    file_state: DocumentFileState,
    capability: DocumentCapability,
    lifecycle_events: Vec<DocumentLifecycleEvent>,
    buffer: TextBuffer,
    language_pipeline: LanguagePipelineState,
    display_state: FoldDisplayState,
}

impl TextDocument {
    pub fn new(identity: DocumentIdentity) -> Self {
        Self::with_text(identity, "")
    }

    pub fn with_text(identity: DocumentIdentity, text: impl AsRef<str>) -> Self {
        let text = text.as_ref();
        let buffer = TextBuffer::new(text);
        let file_state = DocumentFileState::from_identity(&identity);
        Self {
            id: Uuid::new_v4(),
            identity,
            settings: DocumentSettings::default(),
            saved_revision: buffer.revision(),
            line_ending: LineEnding::detect(text),
            file_state,
            capability: DocumentCapability::default(),
            lifecycle_events: Vec::new(),
            buffer,
            language_pipeline: LanguagePipelineState::new(),
            display_state: FoldDisplayState::new(),
        }
    }

    pub fn internal() -> Option<Self> {
        Some(Self::new(DocumentIdentity::internal()?))
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn identity(&self) -> &DocumentIdentity {
        &self.identity
    }

    pub fn set_identity(&mut self, identity: DocumentIdentity) {
        self.file_state.path = identity.path().map(Path::to_path_buf);
        self.identity = identity;
    }

    pub fn file_state(&self) -> &DocumentFileState {
        &self.file_state
    }

    pub fn set_file_state(&mut self, file_state: DocumentFileState) {
        self.file_state = file_state;
    }

    pub fn mark_file_saved(&mut self, saved_mtime: Option<SystemTime>) {
        self.mark_buffer_saved();
        self.file_state.saved_mtime = saved_mtime;
        self.lifecycle_events.push(DocumentLifecycleEvent::Saved {
            revision: self.saved_revision,
            saved_mtime,
        });
    }

    pub fn mark_file_reloaded(
        &mut self,
        text: impl AsRef<str>,
        saved_mtime: Option<SystemTime>,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
        folding_enabled: bool,
    ) {
        let previous_revision = self.buffer.revision();
        self.replace_text(
            text,
            highlight_enabled,
            syntax_highlighting_enabled,
            folding_enabled,
        );
        self.file_state.saved_mtime = saved_mtime;
        self.lifecycle_events
            .push(DocumentLifecycleEvent::Reloaded {
                previous_revision,
                revision: self.saved_revision,
                line_ending: self.line_ending,
            });
    }

    pub fn lifecycle_events(&self) -> &[DocumentLifecycleEvent] {
        &self.lifecycle_events
    }

    pub fn buffer(&self) -> &TextBuffer {
        &self.buffer
    }

    pub fn text(&self) -> String {
        self.buffer.text()
    }

    pub fn rope(&self) -> Rope {
        self.buffer.rope()
    }

    pub fn buffer_revision(&self) -> usize {
        self.buffer.revision()
    }

    pub fn buffer_snapshot(&self) -> BufferSnapshot {
        self.buffer.snapshot()
    }

    pub fn line_count(&self) -> usize {
        self.buffer.line_count()
    }

    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn max_line_byte_len(&self) -> usize {
        self.buffer.max_line_byte_len()
    }

    pub fn start_buffer_transaction(&mut self) -> crate::TransactionId {
        self.buffer.start_transaction_at()
    }

    pub fn end_buffer_transaction(&mut self, id: crate::TransactionId) {
        self.buffer.end_transaction_at(id);
    }

    pub fn apply_change(&mut self, change: &Change) -> Result<()> {
        self.buffer.apply_change(change)
    }

    pub fn restore_buffer_snapshot(&mut self, snapshot: &BufferSnapshot) {
        self.buffer.restore_snapshot(snapshot);
    }

    pub fn latest_change(&self) -> Option<Change> {
        self.buffer.changes().last().cloned()
    }

    pub fn clear_buffer_changes(&mut self) {
        self.buffer.clear_changes();
    }

    pub fn offset_to_position(&self, offset: usize) -> Result<crate::Position> {
        self.buffer.offset_to_position(offset)
    }

    pub fn position_to_offset(&self, position: crate::Position) -> Result<usize> {
        self.buffer.position_to_offset(position)
    }

    pub fn cursor_offset(&self, cursor: &crate::Cursor) -> usize {
        self.position_to_offset(cursor.position()).unwrap_or(0)
    }

    pub fn clamp_position(&self, position: crate::Position) -> crate::Position {
        self.buffer.clamp_position(position)
    }

    pub fn anchor_at(&self, offset: usize, bias: crate::Bias) -> Result<crate::Anchor> {
        self.buffer.anchor_at(offset, bias)
    }

    pub fn anchor_before(&self, offset: usize) -> Result<crate::Anchor> {
        self.buffer.anchor_before(offset)
    }

    pub fn anchor_for_position(
        &self,
        position: crate::Position,
        bias: crate::Bias,
    ) -> Result<crate::Anchor> {
        self.buffer.anchor_for_position(position, bias)
    }

    pub fn anchored_range(
        &self,
        range: std::ops::Range<usize>,
        start_bias: crate::Bias,
        end_bias: crate::Bias,
    ) -> Result<crate::AnchoredRange> {
        self.buffer.anchored_range(range, start_bias, end_bias)
    }

    pub fn resolve_anchored_range(
        &self,
        range: crate::AnchoredRange,
    ) -> Result<std::ops::Range<usize>> {
        self.buffer.resolve_anchored_range(range)
    }

    pub fn resolve_anchor_position(&self, anchor: crate::Anchor) -> Result<crate::Position> {
        self.buffer.resolve_anchor_position(anchor)
    }

    pub fn resolve_anchor_offset(&self, anchor: crate::Anchor) -> Result<usize> {
        self.buffer.resolve_anchor_offset(anchor)
    }

    pub fn line(&self, line: usize) -> Option<String> {
        self.buffer.line(line)
    }

    pub fn char_at(&self, offset: usize) -> Option<char> {
        self.buffer.char_at(offset)
    }

    pub fn text_for_range(&self, range: std::ops::Range<usize>) -> Result<String> {
        self.buffer.text_for_range(range)
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<String> {
        self.buffer.slice(range)
    }

    pub fn floor_char_boundary(&self, offset: usize) -> usize {
        self.buffer.floor_char_boundary(offset)
    }

    pub fn ceil_char_boundary(&self, offset: usize) -> usize {
        self.buffer.ceil_char_boundary(offset)
    }

    pub fn previous_char_boundary(&self, offset: usize) -> Result<usize> {
        self.buffer.previous_char_boundary(offset)
    }

    pub fn next_char_boundary(&self, offset: usize) -> Result<usize> {
        self.buffer.next_char_boundary(offset)
    }

    pub fn delete_range(&mut self, range: std::ops::Range<usize>) -> Result<()> {
        self.buffer.delete(range)
    }

    pub fn insert(&mut self, offset: usize, text: &str) -> Result<()> {
        self.buffer.insert(offset, text)
    }

    pub fn capability(&self) -> DocumentCapability {
        self.capability
    }

    pub fn set_capability(&mut self, capability: DocumentCapability) {
        self.capability = capability;
    }

    pub fn is_read_only(&self) -> bool {
        !self.capability.allows_edits()
    }

    pub fn ensure_edits_allowed(&self) -> Result<()> {
        if self.capability.allows_edits() {
            return Ok(());
        }

        bail!("document is {}", self.capability)
    }

    pub fn settings(&self) -> DocumentSettings {
        self.settings
    }

    pub fn set_settings(&mut self, settings: DocumentSettings) {
        self.settings = settings;
    }

    pub fn saved_revision(&self) -> usize {
        self.saved_revision
    }

    pub fn line_ending(&self) -> LineEnding {
        self.line_ending
    }

    pub fn set_line_ending(&mut self, line_ending: LineEnding) {
        self.line_ending = line_ending;
    }

    pub fn mark_saved(&mut self, revision: usize) {
        self.saved_revision = revision;
    }

    pub fn mark_buffer_saved(&mut self) {
        self.saved_revision = self.buffer.revision();
    }

    pub fn is_dirty(&self, revision: usize) -> bool {
        revision != self.saved_revision
    }

    pub fn is_buffer_dirty(&self) -> bool {
        self.is_dirty(self.buffer.revision())
    }

    pub fn refresh_buffer_state(
        &mut self,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
        folding_enabled: bool,
    ) {
        let syntax_refresh_strategy = if syntax_highlighting_enabled {
            SyntaxRefreshStrategy::FullDocument
        } else {
            SyntaxRefreshStrategy::Disabled
        };
        let fold_refresh = self.language_pipeline.refresh_buffer_state(
            &self.buffer,
            highlight_enabled,
            &syntax_refresh_strategy,
            folding_enabled,
        );
        self.apply_fold_refresh(fold_refresh);
    }

    pub fn refresh_buffer_state_with_strategy(
        &mut self,
        highlight_enabled: bool,
        syntax_refresh_strategy: &SyntaxRefreshStrategy,
        folding_enabled: bool,
    ) {
        let fold_refresh = self.language_pipeline.refresh_buffer_state(
            &self.buffer,
            highlight_enabled,
            syntax_refresh_strategy,
            folding_enabled,
        );
        self.apply_fold_refresh(fold_refresh);
    }

    pub fn apply_buffer_change(
        &mut self,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
        folding_enabled: bool,
    ) {
        let fold_refresh = self.language_pipeline.apply_buffer_change(
            &self.buffer,
            highlight_enabled,
            syntax_highlighting_enabled,
            folding_enabled,
        );
        self.apply_fold_refresh(fold_refresh);
    }

    pub fn syntax_highlights(&self) -> &[Highlight] {
        self.language_pipeline.syntax_highlights()
    }

    pub fn syntax_highlights_arc(&self) -> Arc<Vec<Highlight>> {
        self.language_pipeline.syntax_highlights_arc()
    }

    pub fn highlight_kind_at(&self, offset: usize) -> HighlightKind {
        self.language_pipeline.highlight_kind_at(offset)
    }

    pub fn has_syntax_highlighting(&self) -> bool {
        self.language_pipeline.has_syntax_highlighting()
    }

    pub fn syntax_language_profile(&self) -> &'static str {
        self.language_pipeline.syntax_language_profile()
    }

    pub fn syntax_revision(&self) -> usize {
        self.language_pipeline.syntax_snapshot().revision()
    }

    pub fn refresh_stored_diagnostics(&mut self) {
        let diagnostics = self.language_pipeline.stored_diagnostics_vec();
        self.language_pipeline.set_diagnostics(diagnostics);
    }

    pub fn diagnostics(&self) -> &[Highlight] {
        self.language_pipeline.diagnostics()
    }

    pub fn has_diagnostics(&self) -> bool {
        !self.language_pipeline.diagnostics().is_empty()
    }

    pub fn apply_external_diagnostics(
        &mut self,
        diagnostics_enabled: bool,
        diagnostics: Vec<Diagnostic>,
    ) {
        self.language_pipeline.apply_external_diagnostics(
            &self.buffer,
            diagnostics_enabled,
            diagnostics,
        );
    }

    pub fn anchored_diagnostics(&self) -> Vec<AnchoredDiagnostic> {
        self.language_pipeline.anchored_diagnostics(&self.buffer)
    }

    pub fn pending_edited_line_range(&self) -> Option<std::ops::Range<usize>> {
        self.language_pipeline.pending_edited_line_range()
    }

    pub fn code_actions(&self) -> &[CodeActionOrCommand] {
        self.language_pipeline.code_actions()
    }

    pub fn set_code_actions(&mut self, actions: Vec<CodeActionOrCommand>) {
        self.language_pipeline.set_code_actions(actions);
    }

    pub fn clear_code_actions(&mut self) {
        self.language_pipeline.clear_code_actions();
    }

    pub fn has_code_actions(&self) -> bool {
        !self.language_pipeline.code_actions().is_empty()
    }

    pub fn set_reference_ranges(&mut self, ranges: Vec<std::ops::Range<usize>>) {
        self.language_pipeline.set_reference_ranges(ranges);
    }

    pub fn clear_reference_ranges(&mut self) {
        self.language_pipeline.clear_reference_ranges();
    }

    pub fn set_syntax_language_profile(&mut self, language_profile: &'static str) -> bool {
        self.language_pipeline
            .set_syntax_language_profile(language_profile)
    }

    pub fn set_syntax_term_overrides(
        &mut self,
        overrides: crate::syntax::SyntaxTermOverrides,
    ) -> bool {
        self.language_pipeline.set_syntax_term_overrides(overrides)
    }

    pub fn set_driver_syntax_terms(
        &mut self,
        overrides: crate::syntax::SyntaxTermOverrides,
    ) -> bool {
        self.language_pipeline.set_driver_syntax_terms(overrides)
    }

    pub fn clear_syntax_term_overrides(&mut self) -> bool {
        self.language_pipeline.clear_syntax_term_overrides()
    }

    pub fn set_syntax_capabilities_override(
        &mut self,
        capabilities: zqlz_core::SyntaxDriverCapabilities,
    ) -> bool {
        self.language_pipeline
            .set_syntax_capabilities_override(capabilities)
    }

    pub fn clear_syntax_capabilities_override(&mut self) -> bool {
        self.language_pipeline.clear_syntax_capabilities_override()
    }

    pub fn begin_syntax_reparse(
        &mut self,
        revision: usize,
    ) -> Option<(SyntaxParseToken, SyntaxHighlighter)> {
        self.language_pipeline.begin_syntax_reparse(revision)
    }

    pub fn set_syntax_parse_task(&mut self, task: Task<anyhow::Result<()>>) {
        self.language_pipeline.set_syntax_parse_task(task);
    }

    pub fn restore_syntax_highlighter(&mut self, highlighter: SyntaxHighlighter) {
        self.language_pipeline
            .restore_syntax_highlighter(highlighter);
    }

    pub fn take_queued_syntax_reparse(&mut self) -> bool {
        self.language_pipeline.take_queued_syntax_reparse()
    }

    pub fn apply_reparsed_syntax(
        &mut self,
        token: SyntaxParseToken,
        snapshot: crate::SyntaxSnapshot,
        folding_enabled: bool,
    ) -> bool {
        let current_revision = self.buffer.revision();
        let Some(fold_refresh) = self.language_pipeline.accept_reparsed_syntax(
            token,
            current_revision,
            snapshot,
            &self.buffer,
            folding_enabled,
        ) else {
            return false;
        };

        self.apply_fold_refresh(fold_refresh);
        true
    }

    pub fn replace_text(
        &mut self,
        text: impl AsRef<str>,
        highlight_enabled: bool,
        syntax_highlighting_enabled: bool,
        folding_enabled: bool,
    ) {
        let text = text.as_ref();
        self.line_ending = LineEnding::detect(text);
        self.buffer = TextBuffer::new(text);
        self.mark_buffer_saved();
        self.language_pipeline.invalidate_syntax_tree();
        self.refresh_buffer_state(
            highlight_enabled,
            syntax_highlighting_enabled,
            folding_enabled,
        );
        self.language_pipeline.reset_syntax_parse_task();
    }

    pub fn rebuild_display_lines_cache(&mut self) {
        self.display_state.sync_all(
            self.buffer.line_count(),
            self.language_pipeline.fold_regions(),
        );
    }

    pub fn rebuild_display_lines_cache_for_range(&mut self, line_range: std::ops::Range<usize>) {
        self.display_state.sync_range(
            self.buffer.line_count(),
            self.language_pipeline.fold_regions(),
            line_range,
        );
    }

    pub fn ensure_unfolded_display_lines_cache(&mut self) {
        let total_lines = self.buffer.line_count();
        if self.display_state.display_lines().len() == total_lines {
            return;
        }

        self.display_state
            .ensure_unfolded_display_lines_cache(total_lines);
    }

    pub fn apply_fold_refresh(&mut self, fold_refresh: FoldRefresh) {
        match fold_refresh {
            FoldRefresh::Disabled => self.ensure_unfolded_display_lines_cache(),
            FoldRefresh::Range(line_range) => {
                self.rebuild_display_lines_cache_for_range(line_range)
            }
            FoldRefresh::Full => self.rebuild_display_lines_cache(),
        }
    }

    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.display_state.display_lines()
    }

    pub fn display_line_count(&self) -> usize {
        self.display_state.display_line_count()
    }

    pub fn update_wrap_rows(
        &mut self,
        buffer_lines: &[usize],
        wrap_column: usize,
        visual_rows: &[usize],
    ) -> bool {
        self.display_state
            .update_wrap_rows(buffer_lines, wrap_column, visual_rows)
    }

    pub fn set_display_tab_size(&mut self, tab_size: usize) {
        self.display_state.set_tab_size(tab_size);
    }

    pub fn fold_regions(&self) -> &[FoldRegion] {
        self.language_pipeline.fold_regions()
    }

    pub fn is_line_folded(&self, start_line: usize) -> bool {
        self.display_state.is_line_folded(start_line)
    }

    pub fn collapse_line(&mut self, start_line: usize) -> bool {
        self.display_state.collapse_line(start_line)
    }

    pub fn expand_line(&mut self, start_line: usize) -> bool {
        self.display_state.expand_line(start_line)
    }

    pub fn clear_collapsed_lines(&mut self) {
        self.display_state.clear_collapsed_lines();
    }

    pub fn collapse_all_folds(&mut self) {
        self.display_state
            .collapse_all(self.language_pipeline.fold_regions());
    }

    pub fn language_pipeline_snapshot(&self, primary_cursor: &Cursor) -> LanguagePipelineSnapshot {
        self.language_pipeline
            .snapshot(&self.buffer, primary_cursor)
    }

    pub fn display_snapshot(
        &self,
        buffer_snapshot: BufferSnapshot,
        language: &LanguagePipelineSnapshot,
        soft_wrap: bool,
    ) -> DisplaySnapshot {
        self.display_state.snapshot(
            buffer_snapshot,
            &language.fold_regions,
            soft_wrap,
            language.syntax.highlights(),
            &language.anchored_diagnostics,
            language.anchored_inlay_hints.clone(),
            &language.anchored_code_actions,
        )
    }

    pub fn context(&self) -> DocumentContext {
        DocumentContext {
            id: self.id,
            identity: self.identity.clone(),
            settings: self.settings,
            saved_revision: self.saved_revision,
            line_ending: self.line_ending,
            file_state: self.file_state.clone(),
            is_dirty: self.is_buffer_dirty(),
            capability: self.capability,
            lifecycle_events: self.lifecycle_events.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DocumentCapability, DocumentFileState, DocumentIdentity, DocumentLifecycleEvent,
        DocumentSettings, EncodingPolicy, LineEnding, TextDocument,
    };
    use std::time::{Duration, SystemTime};

    #[test]
    fn document_identity_from_path_preserves_path_and_uri() {
        let identity = DocumentIdentity::from_path("/tmp/query.sql").expect("path identity");

        assert_eq!(
            identity.path(),
            Some(std::path::Path::new("/tmp/query.sql"))
        );
        assert!(identity.uri().as_str().starts_with("file://"));
    }

    #[test]
    fn text_document_tracks_saved_revision_separately_from_settings() {
        let mut document = TextDocument::internal().expect("internal document");
        document.set_settings(DocumentSettings {
            indent_size: 2,
            use_tabs: true,
        });
        document.mark_saved(4);

        assert_eq!(document.settings().indent_size, 2);
        assert!(document.settings().use_tabs);
        assert_eq!(document.saved_revision(), 4);
        assert!(!document.is_dirty(4));
        assert!(document.is_dirty(5));
    }

    #[test]
    fn text_document_owns_buffer_language_and_display_state() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "begin\nselect 1\nend",
        );

        document.refresh_buffer_state(true, true, true);

        assert_eq!(document.text(), "begin\nselect 1\nend");
        assert_eq!(document.syntax_revision(), 0);
        assert_eq!(document.display_line_count(), document.line_count());
    }

    #[test]
    fn replace_text_resets_document_buffer_and_saved_revision_together() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1",
        );
        document.insert(0, "-- ").expect("insert into buffer");

        assert!(document.is_buffer_dirty());

        document.replace_text("select 2", true, true, true);

        assert_eq!(document.text(), "select 2");
        assert_eq!(document.saved_revision(), document.buffer_revision());
        assert!(!document.is_buffer_dirty());
    }

    #[test]
    fn document_context_reflects_external_identity_and_saved_revision() {
        let identity = DocumentIdentity::from_path("/tmp/query.sql").expect("external identity");
        let mut document = TextDocument::with_text(identity.clone(), "select 1");
        document.mark_saved(7);

        let context = document.context();

        assert_eq!(context.identity, identity);
        assert_eq!(context.saved_revision, 7);
        assert_eq!(context.line_ending, LineEnding::Lf);
        assert!(context.is_dirty);
        assert_eq!(context.file_state.encoding, EncodingPolicy::Utf8);
        assert_eq!(
            context.file_state.path,
            Some(std::path::PathBuf::from("/tmp/query.sql"))
        );
        assert_eq!(
            context.identity.path(),
            Some(std::path::Path::new("/tmp/query.sql"))
        );
    }

    #[test]
    fn text_document_snapshots_file_metadata_and_dirty_state() {
        let identity = DocumentIdentity::from_path("/tmp/query.sql").expect("external identity");
        let mut document = TextDocument::with_text(identity, "select 1");
        let saved_mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(42);

        document.mark_file_saved(Some(saved_mtime));
        document
            .insert(document.byte_len(), " -- changed")
            .expect("insert tracked change");

        let context = document.context();
        assert!(context.is_dirty);
        assert_eq!(context.file_state.saved_mtime, Some(saved_mtime));
        assert_eq!(
            context.file_state.path,
            Some(std::path::PathBuf::from("/tmp/query.sql"))
        );
        assert_eq!(context.file_state.encoding, EncodingPolicy::Utf8);
    }

    #[test]
    fn text_document_allows_explicit_file_state_override() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1",
        );
        let file_state = DocumentFileState {
            path: Some(std::path::PathBuf::from("/tmp/virtual.sql")),
            saved_mtime: None,
            encoding: EncodingPolicy::Utf8,
        };

        document.set_file_state(file_state.clone());

        assert_eq!(document.file_state(), &file_state);
        assert_eq!(document.context().file_state, file_state);
    }

    #[test]
    fn text_document_detects_and_snapshots_line_endings() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1\r\nselect 2\r\n",
        );

        assert_eq!(document.line_ending(), LineEnding::CrLf);
        assert_eq!(document.context().line_ending, LineEnding::CrLf);
        assert_eq!(document.line_ending().as_str(), "\r\n");

        document.replace_text("select 3\n", true, true, true);

        assert_eq!(document.line_ending(), LineEnding::Lf);
        assert_eq!(document.context().line_ending, LineEnding::Lf);
    }

    #[test]
    fn text_document_allows_explicit_line_ending_policy() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1",
        );

        document.set_line_ending(LineEnding::CrLf);

        assert_eq!(document.line_ending(), LineEnding::CrLf);
        assert_eq!(document.context().line_ending, LineEnding::CrLf);
    }

    #[test]
    fn text_document_dirty_state_comes_from_saved_revision_not_identity() {
        let identity = DocumentIdentity::from_path("/tmp/rebound.sql").expect("external identity");
        let mut document = TextDocument::with_text(identity, "select 1");
        let current_revision = document.buffer_revision();

        document.mark_saved(current_revision);
        assert!(!document.is_buffer_dirty());

        document
            .insert(document.byte_len(), " -- changed")
            .expect("insert tracked change");
        assert!(document.is_buffer_dirty());
    }

    #[test]
    fn text_document_snapshots_read_only_capability() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1",
        );

        assert_eq!(document.capability(), DocumentCapability::ReadWrite);
        assert!(!document.is_read_only());
        assert!(document.ensure_edits_allowed().is_ok());

        document.set_capability(DocumentCapability::ReadOnly);

        let context = document.context();
        assert_eq!(context.capability, DocumentCapability::ReadOnly);
        assert!(document.is_read_only());
        assert!(document.ensure_edits_allowed().is_err());
    }

    #[test]
    fn text_document_records_save_lifecycle_events() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::from_path("/tmp/query.sql").expect("external identity"),
            "select 1",
        );
        let saved_mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(300);
        document
            .insert(document.byte_len(), " -- changed")
            .expect("insert tracked change");

        document.mark_file_saved(Some(saved_mtime));

        assert!(!document.is_buffer_dirty());
        assert_eq!(
            document.lifecycle_events(),
            &[DocumentLifecycleEvent::Saved {
                revision: document.saved_revision(),
                saved_mtime: Some(saved_mtime),
            }]
        );
        assert_eq!(
            document.context().lifecycle_events,
            document.lifecycle_events()
        );
    }

    #[test]
    fn text_document_records_reload_lifecycle_events() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::from_path("/tmp/query.sql").expect("external identity"),
            "select 1",
        );
        document
            .insert(document.byte_len(), " -- changed")
            .expect("insert tracked change");
        let previous_revision = document.buffer_revision();
        let saved_mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(600);

        document.mark_file_reloaded("select 2\r\n", Some(saved_mtime), true, true, true);

        assert_eq!(document.text(), "select 2\r\n");
        assert_eq!(document.line_ending(), LineEnding::CrLf);
        assert!(!document.is_buffer_dirty());
        assert_eq!(document.file_state().saved_mtime, Some(saved_mtime));
        assert_eq!(
            document.lifecycle_events(),
            &[DocumentLifecycleEvent::Reloaded {
                previous_revision,
                revision: document.saved_revision(),
                line_ending: LineEnding::CrLf,
            }]
        );
    }
}
