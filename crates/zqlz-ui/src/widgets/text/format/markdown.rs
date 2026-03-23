use gpui::SharedString;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

use crate::widgets::{
    highlighter::HighlightTheme,
    text::{
        document::ParsedDocument,
        node::{BlockNode, Paragraph},
    },
};

/// Parse Markdown into a tree of nodes.
///
/// This intentionally builds a plain-text paragraph from markdown events.
///
/// It is a narrow compatibility layer that avoids rendering raw markdown source
/// while we keep the previous node-based renderer stable. A full AST-level
/// markdown renderer is still deferred because it requires a larger rewrite.
pub(crate) fn parse(
    source: &str,
    _cx: &mut crate::widgets::text::node::NodeContext,
    _highlight_theme: &HighlightTheme,
) -> Result<ParsedDocument, SharedString> {
    let plain_text = markdown_to_plain_text(source);

    Ok(ParsedDocument {
        source: source.to_string().into(),
        blocks: vec![BlockNode::Paragraph(Paragraph::new(plain_text))],
    })
}

fn markdown_to_plain_text(source: &str) -> String {
    let mut output = String::new();
    let mut list_depth = 0usize;
    let parser = Parser::new_ext(source, Options::all());

    for event in parser {
        match event {
            Event::Text(text) | Event::Code(text) => output.push_str(text.as_ref()),
            Event::SoftBreak | Event::HardBreak => output.push('\n'),
            Event::Rule => {
                if !output.ends_with('\n') && !output.is_empty() {
                    output.push('\n');
                }
                output.push_str("---\n");
            }
            Event::Start(Tag::Item) => {
                if !output.is_empty() && !output.ends_with('\n') {
                    output.push('\n');
                }
                output.push_str(&"  ".repeat(list_depth));
                output.push_str("- ");
            }
            Event::Start(Tag::List(_)) => {
                list_depth = list_depth.saturating_add(1);
            }
            Event::End(TagEnd::List(_)) => {
                list_depth = list_depth.saturating_sub(1);
                if !output.ends_with('\n') && !output.is_empty() {
                    output.push('\n');
                }
            }
            Event::End(TagEnd::Paragraph)
            | Event::End(TagEnd::Heading(_))
            | Event::End(TagEnd::Item)
            | Event::End(TagEnd::BlockQuote(_))
            | Event::End(TagEnd::CodeBlock) => {
                if !output.ends_with('\n') && !output.is_empty() {
                    output.push('\n');
                }
            }
            Event::FootnoteReference(label) => {
                output.push('[');
                output.push_str(label.as_ref());
                output.push(']');
            }
            Event::InlineMath(text) | Event::DisplayMath(text) => output.push_str(text.as_ref()),
            Event::InlineHtml(_)
            | Event::Html(_)
            | Event::TaskListMarker(_)
            | Event::Start(_)
            | Event::End(_) => {}
        }
    }

    output.trim().to_string()
}
