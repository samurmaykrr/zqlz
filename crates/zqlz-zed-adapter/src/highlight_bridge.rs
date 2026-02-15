//! Bridge for syntax highlighting themes between Zed and ZQLZ
//!
//! Converts Zed's syntax highlighting theme to ZQLZ's HighlightTheme format.

use std::sync::Arc;

use gpui::HighlightStyle;
use theme::{SyntaxTheme, Theme as ZedTheme};
use zqlz_ui::widgets::highlighter::{
    FontStyle as ZqlzFontStyle, FontWeightContent, HighlightTheme, HighlightThemeStyle,
    StatusColors, SyntaxColors, ThemeStyle,
};
use zqlz_ui::widgets::theme::ThemeMode;

fn extract_style(syntax: &SyntaxTheme, name: &str) -> Option<ThemeStyle> {
    let style = syntax.get(name);
    if style.color.is_none()
        && style.background_color.is_none()
        && style.font_weight.is_none()
        && style.font_style.is_none()
    {
        return None;
    }

    Some(ThemeStyle {
        color: style.color,
        font_style: style.font_style.map(|s| match s {
            gpui::FontStyle::Normal => ZqlzFontStyle::Normal,
            gpui::FontStyle::Italic => ZqlzFontStyle::Italic,
            gpui::FontStyle::Oblique => ZqlzFontStyle::Italic,
        }),
        font_weight: style.font_weight.map(|w| {
            let weight = w.0 as u16;
            match weight {
                100 => FontWeightContent::Thin,
                200 => FontWeightContent::ExtraLight,
                300 => FontWeightContent::Light,
                400 => FontWeightContent::Normal,
                500 => FontWeightContent::Medium,
                600 => FontWeightContent::Semibold,
                700 => FontWeightContent::Bold,
                800 => FontWeightContent::ExtraBold,
                900 => FontWeightContent::Black,
                _ => FontWeightContent::Normal,
            }
        }),
    })
}

fn convert_syntax_colors(syntax: &SyntaxTheme) -> SyntaxColors {
    SyntaxColors {
        attribute: extract_style(syntax, "attribute"),
        boolean: extract_style(syntax, "boolean"),
        comment: extract_style(syntax, "comment"),
        comment_doc: extract_style(syntax, "comment.doc"),
        constant: extract_style(syntax, "constant"),
        constructor: extract_style(syntax, "constructor"),
        embedded: extract_style(syntax, "embedded"),
        emphasis: extract_style(syntax, "emphasis"),
        emphasis_strong: extract_style(syntax, "emphasis.strong"),
        enum_: extract_style(syntax, "enum"),
        function: extract_style(syntax, "function"),
        hint: extract_style(syntax, "hint"),
        keyword: extract_style(syntax, "keyword"),
        label: extract_style(syntax, "label"),
        link_text: extract_style(syntax, "link_text"),
        link_uri: extract_style(syntax, "link_uri"),
        number: extract_style(syntax, "number"),
        operator: extract_style(syntax, "operator"),
        predictive: extract_style(syntax, "predictive"),
        preproc: extract_style(syntax, "preproc"),
        primary: extract_style(syntax, "primary"),
        property: extract_style(syntax, "property"),
        punctuation: extract_style(syntax, "punctuation"),
        punctuation_bracket: extract_style(syntax, "punctuation.bracket"),
        punctuation_delimiter: extract_style(syntax, "punctuation.delimiter"),
        punctuation_list_marker: extract_style(syntax, "punctuation.list_marker"),
        punctuation_special: extract_style(syntax, "punctuation.special"),
        string: extract_style(syntax, "string"),
        string_escape: extract_style(syntax, "string.escape"),
        string_regex: extract_style(syntax, "string.regex"),
        string_special: extract_style(syntax, "string.special"),
        string_special_symbol: extract_style(syntax, "string.special.symbol"),
        tag: extract_style(syntax, "tag"),
        tag_doctype: extract_style(syntax, "tag.doctype"),
        text_literal: extract_style(syntax, "text.literal"),
        title: extract_style(syntax, "title"),
        type_: extract_style(syntax, "type"),
        variable: extract_style(syntax, "variable"),
        variable_special: extract_style(syntax, "variable.special"),
        variant: extract_style(syntax, "variant"),
    }
}

fn convert_status_colors(zed: &ZedTheme) -> StatusColors {
    let status = &zed.styles.status;
    StatusColors {
        error: Some(status.error),
        error_background: Some(status.error_background),
        error_border: Some(status.error_border),
        warning: Some(status.warning),
        warning_background: Some(status.warning_background),
        warning_border: Some(status.warning_border),
        info: Some(status.info),
        info_background: Some(status.info_background),
        info_border: Some(status.info_border),
        success: Some(status.created),
        success_background: Some(status.created_background),
        success_border: Some(status.created_border),
        hint: Some(status.hint),
        hint_background: Some(status.hint_background),
        hint_border: Some(status.hint_border),
    }
}

pub struct HighlightThemeBridge;

impl HighlightThemeBridge {
    pub fn from_zed_theme(zed: &ZedTheme) -> Arc<HighlightTheme> {
        let style = HighlightThemeStyle {
            editor_background: Some(zed.styles.colors.editor_background),
            editor_foreground: Some(zed.styles.colors.editor_foreground),
            editor_active_line: Some(zed.styles.colors.editor_active_line_background),
            editor_line_number: Some(zed.styles.colors.editor_line_number),
            editor_active_line_number: Some(zed.styles.colors.editor_active_line_number),
            status: convert_status_colors(zed),
            syntax: convert_syntax_colors(&zed.styles.syntax),
        };

        Arc::new(HighlightTheme {
            name: zed.name.to_string(),
            appearance: if zed.appearance.is_light() {
                ThemeMode::Light
            } else {
                ThemeMode::Dark
            },
            style,
        })
    }
}
