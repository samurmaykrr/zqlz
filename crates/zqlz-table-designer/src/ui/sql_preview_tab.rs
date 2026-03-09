use gpui::*;
use zqlz_ui::widgets::{ActiveTheme, Sizable, button::Button, h_flex, v_flex};

use crate::panel::TableDesignerPanel;

/// SQL keywords to highlight
const SQL_KEYWORDS: &[&str] = &[
    "CREATE",
    "TABLE",
    "ALTER",
    "DROP",
    "ADD",
    "COLUMN",
    "CONSTRAINT",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "INDEX",
    "UNIQUE",
    "NOT",
    "NULL",
    "DEFAULT",
    "CHECK",
    "IF",
    "EXISTS",
    "CASCADE",
    "RESTRICT",
    "SET",
    "ON",
    "DELETE",
    "UPDATE",
    "INSERT",
    "INTO",
    "VALUES",
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "IN",
    "AS",
    "RENAME",
    "TO",
    "AUTOINCREMENT",
    "AUTO_INCREMENT",
    "GENERATED",
    "ALWAYS",
    "STORED",
    "VIRTUAL",
    "WITHOUT",
    "ROWID",
    "STRICT",
    "UNLOGGED",
    "TABLESPACE",
    "ENGINE",
    "CHARSET",
    "COLLATE",
    "ROW_FORMAT",
    "BEFORE",
    "AFTER",
    "FOR",
    "EACH",
    "ROW",
    "BEGIN",
    "END",
    "TRIGGER",
    "FUNCTION",
    "PROCEDURE",
    "RETURN",
    "RETURNS",
    "DECLARE",
    "INCLUDE",
    "USING",
    "BTREE",
    "HASH",
    "GIN",
    "GIST",
];

/// Build highlights for a single line of SQL
fn highlight_sql_line(
    line: &str,
    keyword_color: Hsla,
    comment_color: Hsla,
    string_color: Hsla,
) -> (SharedString, Vec<(std::ops::Range<usize>, HighlightStyle)>) {
    let shared = SharedString::from(line.to_string());
    let mut highlights: Vec<(std::ops::Range<usize>, HighlightStyle)> = Vec::new();

    let trimmed = line.trim();
    if trimmed.starts_with("--") {
        let start = line.find("--").unwrap_or(0);
        highlights.push((
            start..line.len(),
            HighlightStyle {
                color: Some(comment_color),
                ..Default::default()
            },
        ));
        return (shared, highlights);
    }

    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                i += 1;
            }
            if i < bytes.len() {
                i += 1;
            }
            highlights.push((
                start..i,
                HighlightStyle {
                    color: Some(string_color),
                    ..Default::default()
                },
            ));
        } else if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &line[start..i];
            if SQL_KEYWORDS.iter().any(|kw| kw.eq_ignore_ascii_case(word)) {
                highlights.push((
                    start..i,
                    HighlightStyle {
                        color: Some(keyword_color),
                        font_weight: Some(FontWeight::BOLD),
                        ..Default::default()
                    },
                ));
            }
        } else {
            i += 1;
        }
    }

    (shared, highlights)
}

/// Render the SQL preview tab content
pub(in crate::panel) fn render_sql_preview_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let keyword_color = theme.link;
    let comment_color = theme.muted_foreground;
    let string_color = theme.success;

    let ddl = this
        .ddl_preview
        .clone()
        .unwrap_or_else(|| "-- Click to generate DDL preview".to_string());

    let mut line_elements: Vec<AnyElement> = Vec::new();
    for line in ddl.lines() {
        let (text, highlights) =
            highlight_sql_line(line, keyword_color, comment_color, string_color);
        line_elements.push(
            div()
                .child(StyledText::new(text).with_highlights(highlights))
                .into_any_element(),
        );
    }

    v_flex()
        .size_full()
        .p_2()
        .child(
            div()
                .id("sql-preview")
                .flex_1()
                .overflow_scroll()
                .p_3()
                .rounded_md()
                .bg(theme.secondary)
                .border_1()
                .border_color(theme.border)
                .font_family("monospace")
                .text_sm()
                .children(line_elements),
        )
        .child(
            h_flex().justify_end().pt_2().child(
                Button::new("copy-ddl")
                    .label("Copy to Clipboard")
                    .small()
                    .on_click(cx.listener(|this, _, _window, cx| {
                        if let Some(ref ddl) = this.ddl_preview {
                            cx.write_to_clipboard(ClipboardItem::new_string(ddl.clone()));
                            tracing::info!("DDL copied to clipboard");
                        }
                    })),
            ),
        )
}
