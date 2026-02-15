//! SQLite schema introspection
//!
//! Additional schema introspection utilities specific to SQLite.

// Schema introspection is implemented directly on SqliteConnection
// in connection.rs via the SchemaIntrospection trait.
//
// This file is reserved for any SQLite-specific schema utilities
// that don't fit the generic trait interface.
