//! Redis-specific operation modules.
//!
//! This module contains operations specific to Redis databases:
//! - Loading keys from Redis databases
//! - Opening Redis databases and keys in the viewer
//! - Deleting Redis keys

mod database;
mod delete;
mod keys;
