use gpui::SharedString;

// NOTE: The Zed markdown crate API has changed significantly (moved from mdast to pulldown_cmark).
// This is a temporary stub to allow compilation. Full markdown parsing needs to be rewritten
// to use the new parser::parse_markdown() API from the markdown crate.
// See: task-1.2 notes in PRD - markdown API migration is deferred to Phase 2.
//
// The old API used markdown::mdast and markdown::to_mdast().
// The new API uses markdown::parser::parse_markdown() which returns pull down_cmark events.
// This requires a complete rewrite of the parsing logic.

use crate::widgets::{
    highlighter::HighlightTheme,
    text::{
        document::ParsedDocument,
        node::{BlockNode, Paragraph},
    },
};

/// Parse Markdown into a tree of nodes.
///
/// TODO: Rewrite to use new markdown crate API (pulldown_cmark-based)
/// The new API is: markdown::parser::parse_markdown(text) -> Vec<(Range<usize>, MarkdownEvent)>
/// See: ~/.cargo/git/checkouts/zed-a70e2ad075855582/0ce484e/crates/markdown/src/parser.rs
///
/// TODO: Remove `highlight_theme` option, this should be in render stage.
pub(crate) fn parse(
    source: &str,
    _cx: &mut crate::widgets::text::node::NodeContext,
    _highlight_theme: &HighlightTheme,
) -> Result<ParsedDocument, SharedString> {
    // Temporary stub: return plain text document until markdown parser is rewritten
    // This allows compilation to succeed while we complete Zed dependency setup (Phase 1, Task 1.2)
    //
    // Impact: Markdown formatting in hover popovers, diagnostic messages, and completion
    // documentation will display as plain text until this is properly implemented.
    Ok(ParsedDocument {
        source: source.to_string().into(),
        blocks: vec![BlockNode::Paragraph(Paragraph::new(source.to_string()))],
    })
}
