//! Command Tokenizer for Redis and other command-based dialects
//!
//! This provides proper tokenization that handles:
//! - Quoted strings with escapes
//! - Comments
//! - Multi-token commands
//! - Proper Unicode handling
//!
//! Unlike regex-based parsing, this maintains position information and
//! handles edge cases correctly.

/// A token in a command
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Command name (e.g., SET, GET)
    Command(String),
    /// Argument (string, number, flag)
    Argument(String),
    /// Comment
    Comment(String),
    /// Newline (statement terminator for Redis)
    Newline,
    /// End of input
    Eof,
}

/// Token with position information
#[derive(Debug, Clone)]
pub struct PositionedToken {
    pub token: Token,
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub column: usize,
}

/// Tokenizer for command-based languages
pub struct CommandTokenizer {
    input: String,
    position: usize,
    line: usize,
    column: usize,
    case_sensitive: bool,
}

impl CommandTokenizer {
    pub fn new(input: impl Into<String>, case_sensitive: bool) -> Self {
        Self {
            input: input.into(),
            position: 0,
            line: 0,
            column: 0,
            case_sensitive,
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Vec<PositionedToken> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token();
            let is_eof = matches!(token.token, Token::Eof);
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        tokens
    }

    /// Get the next token
    pub fn next_token(&mut self) -> PositionedToken {
        self.skip_whitespace_except_newline();

        let start = self.position;
        let line = self.line;
        let column = self.column;

        if self.is_eof() {
            return PositionedToken {
                token: Token::Eof,
                start,
                end: start,
                line,
                column,
            };
        }

        let ch = self.current_char();

        // Handle newline (statement terminator)
        if ch == '\n' {
            self.advance();
            self.line += 1;
            self.column = 0;
            return PositionedToken {
                token: Token::Newline,
                start,
                end: self.position,
                line,
                column,
            };
        }

        // Handle comments (Redis uses # for comments)
        if ch == '#' {
            return self.read_comment(start, line, column);
        }

        // Handle quoted strings
        if ch == '"' || ch == '\'' {
            return self.read_quoted_string(start, line, column);
        }

        // Handle unquoted tokens (commands and arguments)
        self.read_unquoted_token(start, line, column)
    }

    fn current_char(&self) -> char {
        self.input[self.position..].chars().next().unwrap_or('\0')
    }

    #[allow(dead_code)]
    fn peek_char(&self, offset: usize) -> char {
        self.input[self.position + offset..]
            .chars()
            .next()
            .unwrap_or('\0')
    }

    fn advance(&mut self) {
        if let Some(ch) = self.input[self.position..].chars().next() {
            self.position += ch.len_utf8();
            self.column += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    fn skip_whitespace_except_newline(&mut self) {
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == ' ' || ch == '\t' || ch == '\r' {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_comment(&mut self, start: usize, line: usize, column: usize) -> PositionedToken {
        self.advance(); // skip #

        let mut content = String::new();
        while !self.is_eof() && self.current_char() != '\n' {
            content.push(self.current_char());
            self.advance();
        }

        PositionedToken {
            token: Token::Comment(content),
            start,
            end: self.position,
            line,
            column,
        }
    }

    fn read_quoted_string(&mut self, start: usize, line: usize, column: usize) -> PositionedToken {
        let quote = self.current_char();
        self.advance(); // skip opening quote

        let mut content = String::new();
        let mut escaped = false;

        while !self.is_eof() {
            let ch = self.current_char();

            if escaped {
                // Handle escape sequences
                match ch {
                    'n' => content.push('\n'),
                    'r' => content.push('\r'),
                    't' => content.push('\t'),
                    '\\' => content.push('\\'),
                    '"' => content.push('"'),
                    '\'' => content.push('\''),
                    _ => {
                        content.push('\\');
                        content.push(ch);
                    }
                }
                escaped = false;
                self.advance();
            } else if ch == '\\' {
                escaped = true;
                self.advance();
            } else if ch == quote {
                self.advance(); // skip closing quote
                break;
            } else {
                content.push(ch);
                self.advance();
            }
        }

        PositionedToken {
            token: Token::Argument(content),
            start,
            end: self.position,
            line,
            column,
        }
    }

    fn read_unquoted_token(&mut self, start: usize, line: usize, column: usize) -> PositionedToken {
        let mut content = String::new();

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_whitespace() || ch == '#' {
                break;
            }
            content.push(ch);
            self.advance();
        }

        // First token on a line is the command
        let is_command = column == 0 || {
            // Check if this is the first non-whitespace token on the line
            self.input[..start]
                .lines()
                .last()
                .map(|l| l.trim().is_empty())
                .unwrap_or(true)
        };

        let token = if is_command {
            Token::Command(if self.case_sensitive {
                content
            } else {
                content.to_uppercase()
            })
        } else {
            Token::Argument(content)
        };

        PositionedToken {
            token,
            start,
            end: self.position,
            line,
            column,
        }
    }
}

/// Parsed command with arguments
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub command: String,
    pub args: Vec<String>,
    pub command_token: PositionedToken,
    pub arg_tokens: Vec<PositionedToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCompletionContext {
    pub current_tokens: Vec<String>,
    pub command_path: Vec<String>,
    pub active_prefix: Option<String>,
}

/// Parse tokenized input into commands
pub fn parse_commands(tokens: &[PositionedToken]) -> Vec<ParsedCommand> {
    let mut commands = Vec::new();
    let mut current_command: Option<(String, PositionedToken)> = None;
    let mut current_args: Vec<String> = Vec::new();
    let mut current_arg_tokens: Vec<PositionedToken> = Vec::new();

    for token in tokens {
        match &token.token {
            Token::Command(cmd) => {
                // Save previous command if any
                if let Some((command, command_token)) = current_command.take() {
                    commands.push(ParsedCommand {
                        command,
                        args: current_args,
                        command_token,
                        arg_tokens: current_arg_tokens,
                    });
                    current_args = Vec::new();
                    current_arg_tokens = Vec::new();
                }
                current_command = Some((cmd.clone(), token.clone()));
            }
            Token::Argument(arg) => {
                current_args.push(arg.clone());
                current_arg_tokens.push(token.clone());
            }
            Token::Newline | Token::Eof => {
                // Save current command if any
                if let Some((command, command_token)) = current_command.take() {
                    commands.push(ParsedCommand {
                        command,
                        args: current_args,
                        command_token,
                        arg_tokens: current_arg_tokens,
                    });
                    current_args = Vec::new();
                    current_arg_tokens = Vec::new();
                }
            }
            Token::Comment(_) => {
                // Skip comments
            }
        }
    }

    commands
}

pub fn command_completion_context(
    input: &str,
    cursor_offset: usize,
    is_manual_trigger: bool,
) -> Option<CommandCompletionContext> {
    let cursor_offset = clamp_to_char_boundary(input, cursor_offset);
    let before_cursor = &input[..cursor_offset];
    if !is_manual_trigger && before_cursor.trim_end_matches([' ', '\t']).is_empty() {
        return None;
    }

    let line_start = before_cursor.rfind('\n').map_or(0, |index| index + 1);
    let current_line = &before_cursor[line_start..];
    let mut tokenizer = CommandTokenizer::new(current_line, false);
    let tokens = tokenizer.tokenize();

    if tokens.iter().any(|token| {
        matches!(token.token, Token::Comment(_)) && token.start <= current_line.trim_start().len()
    }) {
        return None;
    }

    let commands = parse_commands(&tokens);
    let current_tokens: Vec<String> = match commands.as_slice() {
        [] => Vec::new(),
        [command] => std::iter::once(command.command.clone())
            .chain(command.args.iter().cloned())
            .collect(),
        _ => return None,
    };

    if current_tokens.is_empty() {
        return Some(CommandCompletionContext {
            current_tokens,
            command_path: Vec::new(),
            active_prefix: None,
        });
    }

    let trailing_space = current_line.ends_with([' ', '\t']);
    let active_prefix = (!trailing_space)
        .then(|| current_tokens.last().cloned())
        .flatten();
    let command_path = if trailing_space {
        current_tokens.clone()
    } else {
        current_tokens
            .get(..current_tokens.len().saturating_sub(1))
            .unwrap_or_default()
            .to_vec()
    };

    Some(CommandCompletionContext {
        current_tokens,
        command_path,
        active_prefix,
    })
}

pub fn command_completion_insert_text(snippet: &str, command_path: &[&str]) -> String {
    if command_path.is_empty() {
        return snippet.to_string();
    }

    let prefix = command_path.join(" ");
    snippet
        .strip_prefix(&prefix)
        .map(str::trim_start)
        .filter(|remaining| !remaining.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            snippet
                .split_whitespace()
                .nth(command_path.len())
                .map(|subcommand| format!("{subcommand} "))
                .unwrap_or_default()
        })
}

fn clamp_to_char_boundary(text: &str, offset: usize) -> usize {
    let mut offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

pub fn command_matches_prefix(command: &str, prefix: &str) -> bool {
    prefix.is_empty() || command.starts_with(prefix)
}

pub fn command_prefix_value(prefix: &str) -> Option<&str> {
    (!prefix.is_empty()).then_some(prefix)
}

pub fn command_metadata_subcommand_candidate(
    keyword: &crate::KeywordDef,
    command_path: &[&str],
    prefix: Option<&str>,
) -> Option<String> {
    command_subcommand_candidate(&keyword.name, command_path, prefix).or_else(|| {
        keyword
            .snippet
            .as_deref()
            .and_then(|snippet| command_subcommand_candidate(snippet, command_path, prefix))
    })
}

pub fn command_metadata_top_level_candidate(
    keyword: &crate::KeywordDef,
    prefix: Option<&str>,
) -> Option<String> {
    command_top_level_candidate(&keyword.name, prefix).or_else(|| {
        keyword
            .snippet
            .as_deref()
            .and_then(|snippet| command_top_level_candidate(snippet, prefix))
    })
}

pub fn command_top_level_candidate(command_text: &str, prefix: Option<&str>) -> Option<String> {
    let candidate = command_text
        .split_whitespace()
        .next()
        .map(str::to_ascii_uppercase)?;
    if prefix.is_none_or(|prefix| candidate.starts_with(prefix)) {
        Some(candidate)
    } else {
        None
    }
}

pub fn command_subcommand_candidate(
    command_text: &str,
    command_path: &[&str],
    prefix: Option<&str>,
) -> Option<String> {
    if command_path.is_empty() {
        return None;
    }

    let parts: Vec<&str> = command_text.split_whitespace().collect();
    if parts.len() <= command_path.len() {
        return None;
    }

    let path_matches = parts[..command_path.len()]
        .iter()
        .zip(command_path.iter())
        .all(|(part, path_segment)| part.eq_ignore_ascii_case(path_segment));
    if !path_matches {
        return None;
    }

    let candidate = parts[command_path.len()].to_ascii_uppercase();
    if prefix.is_none_or(|prefix| candidate.starts_with(prefix)) {
        Some(candidate)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_command() {
        let mut tokenizer = CommandTokenizer::new("SET key value", false);
        let tokens = tokenizer.tokenize();

        assert_eq!(tokens.len(), 4); // SET, key, value, EOF
        assert!(matches!(tokens[0].token, Token::Command(_)));
        assert!(matches!(tokens[1].token, Token::Argument(_)));
        assert!(matches!(tokens[2].token, Token::Argument(_)));
    }

    #[test]
    fn test_quoted_string() {
        let mut tokenizer = CommandTokenizer::new(r#"SET "my key" "hello world""#, false);
        let tokens = tokenizer.tokenize();

        let commands = parse_commands(&tokens);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "SET");
        assert_eq!(commands[0].args, vec!["my key", "hello world"]);
    }

    #[test]
    fn test_escaped_quotes() {
        let mut tokenizer = CommandTokenizer::new(r#"SET key "value with \"quotes\"""#, false);
        let tokens = tokenizer.tokenize();

        let commands = parse_commands(&tokens);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].args[1], r#"value with "quotes""#);
    }

    #[test]
    fn test_multiline_commands() {
        let input = "SET key1 value1\nGET key2\nDEL key3";
        let mut tokenizer = CommandTokenizer::new(input, false);
        let tokens = tokenizer.tokenize();

        let commands = parse_commands(&tokens);
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].command, "SET");
        assert_eq!(commands[1].command, "GET");
        assert_eq!(commands[2].command, "DEL");
    }

    #[test]
    fn test_comments() {
        let input = "SET key value # this is a comment\nGET key";
        let mut tokenizer = CommandTokenizer::new(input, false);
        let tokens = tokenizer.tokenize();

        let commands = parse_commands(&tokens);
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args.len(), 2); // comment is ignored
    }

    #[test]
    fn command_completion_insert_text_uses_metadata_suffix() {
        assert_eq!(
            command_completion_insert_text("CONFIG GET ${1:parameter}", &["CONFIG"]),
            "GET ${1:parameter}"
        );
        assert_eq!(
            command_completion_insert_text("CONFIG GET", &["CONFIG"]),
            "GET"
        );
        assert_eq!(
            command_completion_insert_text("ACL LIST ", &["ACL"]),
            "LIST "
        );
    }

    #[test]
    fn command_subcommand_candidate_uses_path_and_prefix() {
        assert_eq!(
            command_subcommand_candidate("ACL LIST", &["ACL"], Some("LI")),
            Some("LIST".to_string())
        );
        assert_eq!(
            command_subcommand_candidate("ACL GETUSER", &["ACL"], Some("LI")),
            None
        );
        assert_eq!(
            command_subcommand_candidate("CONFIG GET ${1:parameter}", &["CONFIG"], None),
            Some("GET".to_string())
        );
    }

    #[test]
    fn command_top_level_candidate_uses_metadata_name_and_snippet() {
        assert_eq!(
            command_top_level_candidate("CONFIG GET", Some("CO")),
            Some("CONFIG".to_string())
        );
        assert_eq!(command_top_level_candidate("CONFIG GET", Some("GE")), None);

        let keyword = crate::KeywordDef {
            name: String::new(),
            category: crate::dialect_config::KeywordCategory::Other,
            snippet: Some("ACL LIST".to_string()),
            description: None,
            documentation: None,
        };
        assert_eq!(
            command_metadata_top_level_candidate(&keyword, Some("AC")),
            Some("ACL".to_string())
        );
    }

    #[test]
    fn command_completion_context_tracks_command_path_and_prefix() {
        assert_eq!(
            command_completion_context("CONFIG G", "CONFIG G".len(), false),
            Some(CommandCompletionContext {
                current_tokens: vec!["CONFIG".to_string(), "G".to_string()],
                command_path: vec!["CONFIG".to_string()],
                active_prefix: Some("G".to_string()),
            })
        );
        assert_eq!(
            command_completion_context("CONFIG ", "CONFIG ".len(), false),
            Some(CommandCompletionContext {
                current_tokens: vec!["CONFIG".to_string()],
                command_path: vec!["CONFIG".to_string()],
                active_prefix: None,
            })
        );
    }

    #[test]
    fn command_completion_context_skips_comments_and_empty_auto_trigger() {
        assert_eq!(command_completion_context("", 0, false), None);
        assert_eq!(
            command_completion_context("", 0, true),
            Some(CommandCompletionContext {
                current_tokens: Vec::new(),
                command_path: Vec::new(),
                active_prefix: None,
            })
        );
        assert_eq!(
            command_completion_context("# CONFIG ", "# CONFIG ".len(), true),
            None
        );
        assert_eq!(
            command_completion_context("SET key\nCONFIG G", "SET key\nCONFIG G".len(), false),
            Some(CommandCompletionContext {
                current_tokens: vec!["CONFIG".to_string(), "G".to_string()],
                command_path: vec!["CONFIG".to_string()],
                active_prefix: Some("G".to_string()),
            })
        );
    }
}
