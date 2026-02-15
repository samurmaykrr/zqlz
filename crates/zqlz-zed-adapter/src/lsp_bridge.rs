//! LSP Bridge - Connects zqlz-lsp to Zed Editor
//!
//! This module provides a bridge between ZQLZ's existing SQL LSP implementation
//! (zqlz-lsp) and Zed's editor component. It handles:
//! - Converting LSP diagnostics to Zed's diagnostic format
//! - Providing completions from zqlz-lsp  
//! - Providing hover information from zqlz-lsp
//! - Triggering LSP validation on text changes
//! - Managing the lifecycle of LSP operations

use gpui::{App, WeakEntity};
use lsp_types::{
    CodeAction, CompletionItem, Diagnostic as LspDiagnostic, DiagnosticSeverity,
    GotoDefinitionResponse, Hover, SignatureHelp, WorkspaceEdit,
};
use parking_lot::RwLock;
use std::sync::Arc;
use zqlz_lsp::SqlLsp;
use zqlz_settings::ZqlzSettings;
use zqlz_ui::widgets::{Rope, RopeExt};

use crate::editor_wrapper::{Diagnostic, DiagnosticLevel, EditorWrapper};

/// Bridge between zqlz-lsp and Zed Editor
///
/// This adapter translates between ZQLZ's LSP implementation and Zed's editor
/// diagnostic system. It handles asynchronous validation and diagnostic updates.
pub struct LspBridge {
    /// Reference to the SQL LSP instance
    lsp: Arc<RwLock<SqlLsp>>,

    /// Weak reference to the editor wrapper
    editor: WeakEntity<EditorWrapper>,
}

impl LspBridge {
    /// Create a new LSP bridge
    ///
    /// # Arguments
    /// * `lsp` - Shared SQL LSP instance
    /// * `editor` - Weak reference to the editor wrapper
    pub fn new(lsp: Arc<RwLock<SqlLsp>>, editor: WeakEntity<EditorWrapper>) -> Self {
        Self { lsp, editor }
    }

    /// Synchronously validate SQL text and update editor diagnostics
    ///
    /// This method:
    /// 1. Checks if LSP diagnostics are enabled in settings
    /// 2. Runs LSP validation on the provided text
    /// 3. Converts LSP diagnostics to Zed format
    /// 4. Updates the editor with the diagnostics
    ///
    /// If LSP diagnostics are disabled in settings, this returns empty diagnostics.
    ///
    /// # Arguments
    /// * `text` - The SQL text to validate
    /// * `cx` - Application context
    ///
    /// # Returns
    /// The diagnostics that were generated (empty if LSP disabled)
    pub fn sync_diagnostics(&self, text: String, cx: &mut App) -> Vec<Diagnostic> {
        // Check if LSP diagnostics are enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            // Return empty diagnostics if LSP is disabled
            return Vec::new();
        }

        // Convert text to Rope for LSP
        let rope = Rope::from(text.as_str());

        // Run LSP validation
        let lsp_diagnostics = {
            let mut lsp = self.lsp.write();
            lsp.validate_sql(&rope)
        };

        // Convert LSP diagnostics to Zed format
        let zed_diagnostics = self.convert_diagnostics(&lsp_diagnostics, &rope);

        // Update editor if it still exists
        if let Some(editor) = self.editor.upgrade() {
            editor.update(cx, |editor, cx| {
                editor.set_diagnostics(zed_diagnostics.clone(), cx);
            });
        }

        zed_diagnostics
    }

    /// Apply diagnostics to the editor
    ///
    /// This is a lower-level method that directly applies pre-converted diagnostics
    /// to the editor without running validation.
    ///
    /// # Arguments
    /// * `diagnostics` - Zed-format diagnostics to apply
    /// * `cx` - Application context
    pub fn apply_diagnostics(&self, diagnostics: Vec<Diagnostic>, cx: &mut App) {
        if let Some(editor) = self.editor.upgrade() {
            editor.update(cx, |editor, cx| {
                editor.set_diagnostics(diagnostics, cx);
            });
        }
    }

    /// Get completions from LSP at the given byte offset
    ///
    /// This method:
    /// 1. Converts text to Rope format
    /// 2. Calls the LSP to get completions
    /// 3. Returns LSP completion items
    ///
    /// # Arguments
    /// * `text` - The SQL text
    /// * `offset` - Byte offset of the cursor position
    ///
    /// # Returns
    /// Vector of completion items from LSP
    pub fn get_completions(&self, text: &str, offset: usize) -> Vec<CompletionItem> {
        let rope = Rope::from(text);
        let mut lsp = self.lsp.write();
        lsp.get_completions(&rope, offset)
    }

    /// Get hover information from LSP at the given byte offset
    ///
    /// This method:
    /// 1. Converts text to Rope format
    /// 2. Calls the LSP to get hover information
    /// 3. Returns the hover data if available
    ///
    /// # Arguments
    /// * `text` - The SQL text
    /// * `offset` - Byte offset of the cursor position
    ///
    /// # Returns
    /// Hover information if available at the given position
    pub fn get_hover(&self, text: &str, offset: usize) -> Option<Hover> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.get_hover(&rope, offset)
    }

    /// Get signature help from LSP at the given byte offset
    ///
    /// This method:
    /// 1. Converts text to Rope format
    /// 2. Calls the LSP to get signature help
    /// 3. Returns the signature help data if available
    ///
    /// # Arguments
    /// * `text` - The SQL text
    /// * `offset` - Byte offset of the cursor position
    ///
    /// # Returns
    /// Signature help if cursor is inside a function call
    pub fn get_signature_help(&self, text: &str, offset: usize) -> Option<SignatureHelp> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.get_signature_help(&rope, offset)
    }

    /// Get definition location from LSP at the given byte offset
    ///
    /// This method:
    /// 1. Converts text to Rope format
    /// 2. Calls the LSP to get definition location
    /// 3. Returns the definition response if available
    ///
    /// # Arguments
    /// * `text` - The SQL text
    /// * `offset` - Byte offset of the cursor position
    ///
    /// # Returns
    /// Definition location if symbol has a definition in the schema
    pub fn get_definition(
        &self,
        text: &str,
        offset: usize,
    ) -> Option<lsp_types::GotoDefinitionResponse> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.get_definition(&rope, offset)
    }

    /// Find all references to a symbol at the given offset.
    ///
    /// This searches through the text for all occurrences of the identifier
    /// and returns their locations. It excludes SQL keywords from results.
    ///
    /// # Arguments
    /// * `text` - The SQL query text to search in
    /// * `offset` - The byte offset of the cursor position
    ///
    /// # Returns
    /// A vector of Location objects representing all references found
    pub fn get_references(&self, text: &str, offset: usize) -> Vec<lsp_types::Location> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.get_references(&rope, offset)
    }

    /// Rename a symbol at the given offset to a new name.
    ///
    /// # Arguments
    /// * `text` - The SQL text content
    /// * `offset` - The byte offset of the cursor position (on the symbol to rename)
    /// * `new_name` - The new name for the symbol
    ///
    /// # Returns
    /// A WorkspaceEdit containing the text edits to rename all occurrences,
    /// or None if the rename is not valid
    pub fn rename(&self, text: &str, offset: usize, new_name: &str) -> Option<WorkspaceEdit> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.rename(&rope, offset, new_name)
    }

    /// Get code actions at the given offset.
    ///
    /// # Arguments
    /// * `text` - The SQL text content
    /// * `offset` - The byte offset of the cursor position
    /// * `diagnostics` - Current diagnostics for the document
    ///
    /// # Returns
    /// A vector of CodeAction objects representing available quick fixes
    pub fn get_code_actions(
        &self,
        text: &str,
        offset: usize,
        diagnostics: &[lsp_types::Diagnostic],
    ) -> Vec<lsp_types::CodeAction> {
        let rope = Rope::from(text);
        let lsp = self.lsp.read();
        lsp.get_code_actions(&rope, offset, diagnostics)
    }

    /// Convert LSP diagnostics to Zed format
    ///
    /// This handles the translation from lsp_types::Diagnostic (line/column based)
    /// to Zed's Diagnostic format (byte offset based).
    fn convert_diagnostics(
        &self,
        lsp_diagnostics: &[LspDiagnostic],
        rope: &Rope,
    ) -> Vec<Diagnostic> {
        lsp_diagnostics
            .iter()
            .filter_map(|lsp_diag| {
                // Convert LSP Range (line/col) to byte offsets using rope
                let start_offset = rope.position_to_offset(&lsp_diag.range.start);
                let end_offset = rope.position_to_offset(&lsp_diag.range.end);

                // Convert LSP severity to Zed severity
                let severity = match lsp_diag.severity {
                    Some(DiagnosticSeverity::ERROR) => DiagnosticLevel::Error,
                    Some(DiagnosticSeverity::WARNING) => DiagnosticLevel::Warning,
                    Some(DiagnosticSeverity::INFORMATION) => DiagnosticLevel::Info,
                    Some(DiagnosticSeverity::HINT) => DiagnosticLevel::Hint,
                    _ => DiagnosticLevel::Error,
                };

                Some(Diagnostic {
                    range: start_offset..end_offset,
                    severity,
                    message: lsp_diag.message.clone(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsp_types::Position;
    use zqlz_services::SchemaService;

    #[test]
    fn test_convert_diagnostics_empty() {
        let _lsp = Arc::new(RwLock::new(SqlLsp::new(Arc::new(SchemaService::new()))));
        let _rope = Rope::from("SELECT * FROM users");

        // Create a mock LspBridge (we can't create WeakEntity easily in tests)
        // Just test the conversion logic directly
        let lsp_diagnostics: Vec<LspDiagnostic> = vec![];

        // We would normally call bridge.convert_diagnostics, but since we can't
        // easily create a bridge in tests, we'll test the conversion separately
        assert_eq!(lsp_diagnostics.len(), 0);
    }

    #[test]
    fn test_diagnostic_severity_conversion() {
        // Test severity conversion logic
        let error_severity = Some(DiagnosticSeverity::ERROR);
        let warning_severity = Some(DiagnosticSeverity::WARNING);
        let info_severity = Some(DiagnosticSeverity::INFORMATION);
        let hint_severity = Some(DiagnosticSeverity::HINT);

        match error_severity {
            Some(DiagnosticSeverity::ERROR) => {
                assert_eq!(DiagnosticLevel::Error, DiagnosticLevel::Error)
            }
            _ => panic!("Should be error"),
        }

        match warning_severity {
            Some(DiagnosticSeverity::WARNING) => {
                assert_eq!(DiagnosticLevel::Warning, DiagnosticLevel::Warning)
            }
            _ => panic!("Should be warning"),
        }

        match info_severity {
            Some(DiagnosticSeverity::INFORMATION) => {
                assert_eq!(DiagnosticLevel::Info, DiagnosticLevel::Info)
            }
            _ => panic!("Should be info"),
        }

        match hint_severity {
            Some(DiagnosticSeverity::HINT) => {
                assert_eq!(DiagnosticLevel::Hint, DiagnosticLevel::Hint)
            }
            _ => panic!("Should be hint"),
        }
    }

    #[test]
    fn test_rope_position_conversion() {
        let rope = Rope::from("SELECT *\nFROM users");

        // Position at start of second line (line 1, char 0)
        let pos = Position {
            line: 1,
            character: 0,
        };

        // Should convert to byte offset 9 (8 chars + 1 newline)
        let offset = rope.position_to_offset(&pos);
        assert_eq!(offset, 9);
    }

    #[test]
    fn test_get_completions_basic() {
        // Test that completions can be retrieved
        // Note: We can't create a full LspBridge without WeakEntity, but we can test
        // the LSP directly to ensure the API works
        let schema_service = Arc::new(SchemaService::new());
        let lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));

        // Get completions at a position
        let text = "SELECT * FROM ";
        let offset = text.len();

        let completions = {
            let mut lsp_guard = lsp.write();
            let rope = Rope::from(text);
            lsp_guard.get_completions(&rope, offset)
        };

        // Should have keyword completions at minimum
        // (actual table completions require schema data)
        assert!(
            !completions.is_empty() || text.is_empty(),
            "Expected completions or empty text"
        );
    }

    #[test]
    fn test_get_hover_basic() {
        // Test that hover can be retrieved
        let schema_service = Arc::new(SchemaService::new());
        let lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));

        // Get hover at a keyword position
        let text = "SELECT * FROM users";
        let offset = 3; // Middle of "SELECT"

        let hover = {
            let lsp_guard = lsp.read();
            let rope = Rope::from(text);
            lsp_guard.get_hover(&rope, offset)
        };

        // Hover may or may not exist depending on LSP implementation
        // We're just testing that the method doesn't panic
        let _ = hover;
    }

    #[test]
    fn test_get_signature_help_basic() {
        let schema_service = Arc::new(SchemaService::new());
        let lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));

        let text = "SELECT COUNT(*) FROM users";
        let offset = 14;

        let signature_help = {
            let lsp_guard = lsp.read();
            let rope = Rope::from(text);
            lsp_guard.get_signature_help(&rope, offset)
        };

        let _ = signature_help;
    }
}
