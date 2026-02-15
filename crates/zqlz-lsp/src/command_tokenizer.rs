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

use std::fmt;

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
}
