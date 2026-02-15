// This module contains small compile-time smoke tests for the UI extraction.
// They are not run by default but help ensure exported functions have correct
// signatures. Kept in source tree for developer convenience.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_mod_compiles() {
        // nothing to do - compilation is the test
    }
}
