use crate::document::DocumentSettings;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CursorShape {
    Block,
    Bar,
    Underline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SoftWrapMode {
    None,
    EditorWidth,
    PreferredLineLength(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorSettings {
    pub blink: bool,
    pub shape: CursorShape,
}

impl Default for CursorSettings {
    fn default() -> Self {
        Self {
            blink: true,
            shape: CursorShape::Bar,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GutterSettings {
    pub show_line_numbers: bool,
    pub show_relative_line_numbers: bool,
    pub show_gutter: bool,
    pub show_fold_controls: bool,
}

impl Default for GutterSettings {
    fn default() -> Self {
        Self {
            show_line_numbers: true,
            show_relative_line_numbers: false,
            show_gutter: true,
            show_fold_controls: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchSettings {
    pub case_sensitive: bool,
    pub whole_word: bool,
    pub use_regex: bool,
    pub search_in_selection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionSettings {
    pub automatically_show: bool,
    pub accept_on_enter: bool,
    pub commit_characters: bool,
}

impl Default for CompletionSettings {
    fn default() -> Self {
        Self {
            automatically_show: true,
            accept_on_enter: true,
            commit_characters: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrollSettings {
    pub vertical_margin_lines: usize,
    pub horizontal_margin_columns: usize,
    pub sensitivity: u16,
    pub scroll_beyond_last_line: bool,
}

impl Default for ScrollSettings {
    fn default() -> Self {
        Self {
            vertical_margin_lines: 3,
            horizontal_margin_columns: 5,
            sensitivity: 100,
            scroll_beyond_last_line: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditorSettings {
    pub cursor: CursorSettings,
    pub highlight_current_line: bool,
    pub highlight_selection_matches: bool,
    pub gutter: GutterSettings,
    pub show_diagnostics: bool,
    pub show_folding: bool,
    pub soft_wrap: SoftWrapMode,
    pub document: DocumentSettings,
    pub search: SearchSettings,
    pub hover_delay: Duration,
    pub completion: CompletionSettings,
    pub scroll: ScrollSettings,
}

impl Default for EditorSettings {
    fn default() -> Self {
        Self {
            cursor: CursorSettings::default(),
            highlight_current_line: true,
            highlight_selection_matches: true,
            gutter: GutterSettings::default(),
            show_diagnostics: true,
            show_folding: true,
            soft_wrap: SoftWrapMode::None,
            document: DocumentSettings::default(),
            search: SearchSettings::default(),
            hover_delay: Duration::from_millis(500),
            completion: CompletionSettings::default(),
            scroll: ScrollSettings::default(),
        }
    }
}

impl EditorSettings {
    pub fn with_document_settings(mut self, document: DocumentSettings) -> Self {
        self.document = document;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editor_settings_round_trip_through_json() {
        let settings = EditorSettings::default().with_document_settings(DocumentSettings {
            indent_size: 2,
            use_tabs: true,
        });

        let json = serde_json::to_string(&settings).expect("settings should serialize");
        let restored: EditorSettings =
            serde_json::from_str(&json).expect("settings should deserialize");

        assert_eq!(restored, settings);
        assert_eq!(restored.document.indent_size, 2);
        assert!(restored.document.use_tabs);
    }

    #[test]
    fn editor_settings_cover_current_behavior_surfaces() {
        let settings = EditorSettings::default();

        assert_eq!(settings.cursor.shape, CursorShape::Bar);
        assert!(settings.highlight_current_line);
        assert!(settings.highlight_selection_matches);
        assert!(settings.gutter.show_line_numbers);
        assert!(settings.gutter.show_gutter);
        assert!(settings.show_diagnostics);
        assert!(settings.show_folding);
        assert_eq!(settings.soft_wrap, SoftWrapMode::None);
        assert_eq!(settings.document, DocumentSettings::default());
        assert!(!settings.search.case_sensitive);
        assert_eq!(settings.hover_delay, Duration::from_millis(500));
        assert!(settings.completion.automatically_show);
        assert_eq!(settings.scroll.vertical_margin_lines, 3);
    }
}
