//! Parser pool for efficient tree-sitter parser reuse
//!
//! Following Zed's PARSERS pattern for managing tree-sitter parser instances

use parking_lot::Mutex;
use std::sync::LazyLock;
use tree_sitter::Parser;

/// Global parser pool
static PARSER_POOL: LazyLock<Mutex<Vec<Parser>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Handle that returns a parser to the pool on drop
pub struct ParserHandle {
    parser: Option<Parser>,
}

impl ParserHandle {
    /// Get a reference to the parser
    pub fn parser(&mut self) -> &mut Parser {
        self.parser
            .as_mut()
            .expect("Parser already returned to pool")
    }
}

impl Drop for ParserHandle {
    fn drop(&mut self) {
        if let Some(parser) = self.parser.take() {
            let mut pool = PARSER_POOL.lock();
            pool.push(parser);
        }
    }
}

/// Acquire a parser from the pool, or create a new one
pub fn acquire_parser() -> anyhow::Result<ParserHandle> {
    let mut parser = {
        let mut pool = PARSER_POOL.lock();
        pool.pop()
    };

    if parser.is_none() {
        parser = Some(Parser::new());
    }

    let mut p = parser.unwrap();
    p.set_language(&tree_sitter_sequel::LANGUAGE.into())?;

    Ok(ParserHandle { parser: Some(p) })
}

/// Execute a function with a parser from the pool
pub fn with_parser<F, R>(f: F) -> anyhow::Result<R>
where
    F: FnOnce(&mut Parser) -> anyhow::Result<R>,
{
    let mut handle = acquire_parser()?;
    f(handle.parser())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_pool() {
        let result1 = with_parser(|parser| {
            let tree = parser.parse("SELECT 1", None).unwrap();
            assert!(!tree.root_node().has_error());
            Ok(())
        });
        assert!(result1.is_ok());

        // Parser should be returned to pool
        let pool_size = PARSER_POOL.lock().len();
        assert_eq!(pool_size, 1);

        // Reuse parser
        let result2 = with_parser(|parser| {
            let tree = parser.parse("SELECT * FROM users", None).unwrap();
            assert!(!tree.root_node().has_error());
            Ok(())
        });
        assert!(result2.is_ok());
    }

    #[test]
    fn test_concurrent_parsers() {
        use std::thread;

        let handles: Vec<_> = (0..4)
            .map(|i| {
                thread::spawn(move || {
                    with_parser(|parser| {
                        let sql = format!("SELECT {} FROM table{}", i, i);
                        let tree = parser.parse(&sql, None).unwrap();
                        assert!(!tree.root_node().has_error());
                        Ok(())
                    })
                })
            })
            .collect();

        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
    }
}
