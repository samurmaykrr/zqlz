use std::path::{Path, PathBuf};

use lsp_types::Uri;
use url::Url;
use uuid::Uuid;

use crate::{
    FoldDisplayState, FoldRefresh, SyntaxRefreshStrategy, TextBuffer,
    language_pipeline::LanguagePipelineState,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentSettings {
    pub indent_size: usize,
    pub use_tabs: bool,
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
    pub buffer: TextBuffer,
    pub language_pipeline: LanguagePipelineState,
    pub display_state: FoldDisplayState,
}

impl TextDocument {
    pub fn new(identity: DocumentIdentity) -> Self {
        Self::with_text(identity, "")
    }

    pub fn with_text(identity: DocumentIdentity, text: impl AsRef<str>) -> Self {
        let buffer = TextBuffer::new(text.as_ref());
        Self {
            id: Uuid::new_v4(),
            identity,
            settings: DocumentSettings::default(),
            saved_revision: buffer.revision(),
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
        self.identity = identity;
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

    pub fn apply_reparsed_syntax(
        &mut self,
        token: crate::language_pipeline::SyntaxParseToken,
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
        self.buffer = TextBuffer::new(text.as_ref());
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

    pub fn context(&self) -> DocumentContext {
        DocumentContext {
            id: self.id,
            identity: self.identity.clone(),
            settings: self.settings,
            saved_revision: self.saved_revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DocumentIdentity, DocumentSettings, TextDocument};

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

        assert_eq!(document.buffer.text(), "begin\nselect 1\nend");
        assert_eq!(document.language_pipeline.syntax_snapshot().revision(), 0);
        assert_eq!(
            document.display_state.display_lines().len(),
            document.buffer.line_count()
        );
    }

    #[test]
    fn replace_text_resets_document_buffer_and_saved_revision_together() {
        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal identity"),
            "select 1",
        );
        document
            .buffer
            .insert(0, "-- ")
            .expect("insert into buffer");

        assert!(document.is_buffer_dirty());

        document.replace_text("select 2", true, true, true);

        assert_eq!(document.buffer.text(), "select 2");
        assert_eq!(document.saved_revision(), document.buffer.revision());
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
        assert_eq!(
            context.identity.path(),
            Some(std::path::Path::new("/tmp/query.sql"))
        );
    }

    #[test]
    fn text_document_dirty_state_comes_from_saved_revision_not_identity() {
        let identity = DocumentIdentity::from_path("/tmp/rebound.sql").expect("external identity");
        let mut document = TextDocument::with_text(identity, "select 1");
        let current_revision = document.buffer.revision();

        document.mark_saved(current_revision);
        assert!(!document.is_buffer_dirty());

        document
            .buffer
            .insert(document.buffer.len(), " -- changed")
            .expect("insert tracked change");
        assert!(document.is_buffer_dirty());
    }
}
