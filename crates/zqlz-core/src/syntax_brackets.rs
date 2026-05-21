use crate::{
    BracketCapability, SyntaxDriverCapabilities, driver_syntax_protected_ranges,
    get_syntax_driver_capabilities, sql_protected_range_at, sql_protected_ranges,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxBracketScanMode {
    Disabled,
    Standard,
    DriverSyntax,
    Sql,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SyntaxBracketPair {
    pub open: usize,
    pub close: usize,
}

pub fn syntax_bracket_scan_mode_for_profile(language_profile: &str) -> SyntaxBracketScanMode {
    let capabilities = get_syntax_driver_capabilities(language_profile);
    syntax_bracket_scan_mode_for_capabilities(&capabilities)
}

pub fn syntax_bracket_scan_mode_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
) -> SyntaxBracketScanMode {
    match capabilities.brackets {
        BracketCapability::None => SyntaxBracketScanMode::Disabled,
        BracketCapability::Standard => {
            if capabilities.command_syntax || capabilities.document_syntax {
                SyntaxBracketScanMode::DriverSyntax
            } else {
                SyntaxBracketScanMode::Standard
            }
        }
        BracketCapability::TreeSitter => {
            if capabilities.sql_overlays {
                SyntaxBracketScanMode::Sql
            } else if capabilities.command_syntax || capabilities.document_syntax {
                SyntaxBracketScanMode::DriverSyntax
            } else {
                SyntaxBracketScanMode::Standard
            }
        }
    }
}

pub fn syntax_bracket_pairs_for_profile(
    language_profile: &str,
    text: &str,
    base_offset: usize,
    cursor_offset: usize,
) -> Vec<SyntaxBracketPair> {
    let capabilities = get_syntax_driver_capabilities(language_profile);
    syntax_bracket_pairs_for_capabilities(&capabilities, text, base_offset, cursor_offset)
}

pub fn syntax_bracket_pairs_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
    text: &str,
    base_offset: usize,
    cursor_offset: usize,
) -> Vec<SyntaxBracketPair> {
    match syntax_bracket_scan_mode_for_capabilities(capabilities) {
        SyntaxBracketScanMode::Disabled => Vec::new(),
        SyntaxBracketScanMode::Standard => {
            syntax_bracket_pairs_with_protected_ranges(text, base_offset, cursor_offset, &[])
        }
        SyntaxBracketScanMode::DriverSyntax => {
            let protected_ranges = driver_syntax_protected_ranges(capabilities.profile, text);
            syntax_bracket_pairs_with_protected_ranges(
                text,
                base_offset,
                cursor_offset,
                &protected_ranges,
            )
        }
        SyntaxBracketScanMode::Sql => {
            let protected_ranges = sql_protected_ranges(text);
            syntax_bracket_pairs_with_protected_ranges(
                text,
                base_offset,
                cursor_offset,
                &protected_ranges,
            )
        }
    }
}

fn syntax_bracket_pairs_with_protected_ranges(
    text: &str,
    base_offset: usize,
    cursor_offset: usize,
    protected_ranges: &[crate::SqlProtectedRange],
) -> Vec<SyntaxBracketPair> {
    let mut stack: Vec<(char, usize)> = Vec::new();
    let mut enclosing: Vec<SyntaxBracketPair> = Vec::new();
    let mut index = 0usize;
    let mut protected_range_index = 0usize;

    while index < text.len() {
        if let Some(protected_range) =
            sql_protected_range_at(index, protected_ranges, &mut protected_range_index)
        {
            index = protected_range.end;
            continue;
        }

        let Some(character) = text[index..].chars().next() else {
            break;
        };
        let byte_offset = base_offset + index;

        match character {
            '(' | '[' | '{' => stack.push((character, byte_offset)),
            ')' | ']' | '}' => {
                let expected_open = match character {
                    ')' => '(',
                    ']' => '[',
                    '}' => '{',
                    _ => unreachable!(),
                };
                if stack.last().map(|(open, _)| *open) == Some(expected_open)
                    && let Some((_, open_offset)) = stack.pop()
                    && open_offset < cursor_offset
                    && byte_offset >= cursor_offset
                {
                    enclosing.push(SyntaxBracketPair {
                        open: open_offset,
                        close: byte_offset,
                    });
                }
            }
            _ => {}
        }

        index += character.len_utf8();
    }

    enclosing.reverse();
    enclosing
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HighlightQueryLanguage, ParameterPlaceholderCapability, SyntaxDriverCapabilities,
        TreeSitterGrammar,
    };

    fn test_capabilities(
        profile: &'static str,
        brackets: BracketCapability,
        command_syntax: bool,
        document_syntax: bool,
        sql_overlays: bool,
    ) -> SyntaxDriverCapabilities {
        SyntaxDriverCapabilities {
            profile,
            tree_sitter_grammar: TreeSitterGrammar::None,
            highlight_query_language: HighlightQueryLanguage::None,
            brackets,
            line_comment_prefix: None,
            block_comment_delimiters: None,
            parameter_placeholders: ParameterPlaceholderCapability::disabled(),
            command_syntax,
            document_syntax,
            sql_overlays,
            dollar_quoted_strings: false,
            formatter: crate::FormatterCapability::Sql,
            indent_after_keywords: Vec::new(),
            auto_close_pairs: Vec::new(),
            folding: crate::dialects::SQL_FOLDING_RULES,
            execution_unit: crate::SyntaxExecutionUnitMode::SqlStatement,
            document_symbols: crate::SyntaxDocumentSymbolMode::SqlStatements,
            overlays: crate::SyntaxOverlayMode::Sql,
            diagnostics: crate::SQL_DIAGNOSTIC_RULES,
            completion_triggers: Vec::new(),
            completion_word_chars: Vec::new(),
        }
    }

    #[test]
    fn sql_scan_ignores_strings_comments_and_quoted_identifiers() {
        let text = "SELECT '(' AS literal, \"weird)name\" FROM users -- ) ignored\nWHERE id IN (SELECT id FROM groups /* ) ignored */ WHERE active = true)";
        let cursor_offset = text.find("active").expect("active");
        let pairs = syntax_bracket_pairs_for_profile("postgresql", text, 0, cursor_offset);

        let expected_open = text.find("(SELECT").expect("subquery");
        let expected_close = text.rfind(')').expect("closing subquery");
        assert_eq!(
            pairs,
            vec![SyntaxBracketPair {
                open: expected_open,
                close: expected_close,
            }]
        );
    }

    #[test]
    fn standard_scan_does_not_apply_sql_protected_ranges() {
        let text = "JSON.SET key $.path [1, 2]";
        let cursor_offset = text.find('1').expect("array value");
        let pairs = syntax_bracket_pairs_for_profile("redis", text, 0, cursor_offset);

        assert_eq!(
            pairs,
            vec![SyntaxBracketPair {
                open: text.find('[').expect("array opener"),
                close: text.find(']').expect("array closer"),
            }]
        );
    }

    #[test]
    fn mongodb_scan_ignores_document_strings_and_comments() {
        let text = r#"db.orders.find({ label: "ignore ) ] }", qty: { $gte: 3 } }) // ) ignored"#;
        let cursor_offset = text.find("$gte").expect("operator");
        let pairs = syntax_bracket_pairs_for_profile("mongodb", text, 0, cursor_offset);

        assert_eq!(
            pairs,
            vec![
                SyntaxBracketPair {
                    open: text.find("find(").expect("find call") + "find".len(),
                    close: text.find(") //").expect("find call close"),
                },
                SyntaxBracketPair {
                    open: text.find("({").expect("find argument") + 1,
                    close: text.find("}) //").expect("find argument close"),
                },
                SyntaxBracketPair {
                    open: text.find("{ $gte").expect("operator object"),
                    close: text.find(" } })").expect("operator object close") + 1,
                },
            ]
        );
    }

    #[test]
    fn redis_scan_ignores_quoted_arguments_and_comments() {
        let text =
            "JSON.SET key $ \"{\\\"ignored\\\": [1]}\"\n# ] ignored\nJSON.GET key $.items[0]";
        let cursor_offset = text.rfind('0').expect("array index");
        let pairs = syntax_bracket_pairs_for_profile("redis", text, 0, cursor_offset);

        assert_eq!(
            pairs,
            vec![SyntaxBracketPair {
                open: text.rfind('[').expect("array opener"),
                close: text.rfind(']').expect("array closer"),
            }]
        );
    }

    #[test]
    fn mode_comes_from_syntax_profile() {
        assert_eq!(
            syntax_bracket_scan_mode_for_profile("postgresql"),
            SyntaxBracketScanMode::Sql
        );
        assert_eq!(
            syntax_bracket_scan_mode_for_profile("mssql"),
            SyntaxBracketScanMode::Sql
        );
        assert_eq!(
            syntax_bracket_scan_mode_for_profile("mongodb"),
            SyntaxBracketScanMode::DriverSyntax
        );
        assert_eq!(
            syntax_bracket_scan_mode_for_profile("redis"),
            SyntaxBracketScanMode::DriverSyntax
        );
    }

    #[test]
    fn mode_can_come_from_explicit_driver_capabilities() {
        let disabled = test_capabilities("custom", BracketCapability::None, false, false, false);
        assert_eq!(
            syntax_bracket_scan_mode_for_capabilities(&disabled),
            SyntaxBracketScanMode::Disabled
        );

        let command = test_capabilities(
            "custom-command",
            BracketCapability::Standard,
            true,
            false,
            false,
        );
        assert_eq!(
            syntax_bracket_scan_mode_for_capabilities(&command),
            SyntaxBracketScanMode::DriverSyntax
        );

        let sql = test_capabilities(
            "custom-sql",
            BracketCapability::TreeSitter,
            false,
            false,
            true,
        );
        assert_eq!(
            syntax_bracket_scan_mode_for_capabilities(&sql),
            SyntaxBracketScanMode::Sql
        );
    }

    #[test]
    fn explicit_capabilities_can_disable_bracket_pairs() {
        let capabilities =
            test_capabilities("custom", BracketCapability::None, false, false, false);
        let pairs = syntax_bracket_pairs_for_capabilities(&capabilities, "SELECT (1)", 0, 8);

        assert!(pairs.is_empty());
    }
}
