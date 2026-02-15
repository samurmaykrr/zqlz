//! ZQLZ Versioning - Local version control for database objects
//!
//! This crate provides git-like version control for stored procedures,
//! views, triggers, functions, and other database objects.
//!
//! # Features
//!
//! - **Version History**: Track changes to database objects over time
//! - **Diffing**: Compare versions with unified diff output
//! - **Tags**: Name specific versions for easy reference (like git tags)
//! - **Persistence**: SQLite-backed storage that survives restarts
//! - **Tracking**: Explicitly track which objects to version control
//!
//! # Example
//!
//! ```rust,ignore
//! use zqlz_versioning::{VersionRepository, DatabaseObjectType};
//! use uuid::Uuid;
//!
//! let repo = VersionRepository::new()?;
//! let connection_id = Uuid::new_v4();
//!
//! // Commit a new version
//! let v1 = repo.commit(
//!     connection_id,
//!     DatabaseObjectType::Function,
//!     Some("public".to_string()),
//!     "calculate_total".to_string(),
//!     "CREATE FUNCTION calculate_total(...) ...".to_string(),
//!     "Initial implementation".to_string(),
//! )?;
//!
//! // Tag it as v1.0
//! repo.tag(v1.id, "v1.0", Some("First release"))?;
//!
//! // Later, commit a new version
//! let v2 = repo.commit(
//!     connection_id,
//!     DatabaseObjectType::Function,
//!     Some("public".to_string()),
//!     "calculate_total".to_string(),
//!     "CREATE FUNCTION calculate_total(...) -- updated ...".to_string(),
//!     "Added tax calculation".to_string(),
//! )?;
//!
//! // Get the diff between versions
//! let diff = repo.diff(v1.id, v2.id)?;
//! println!("{}", diff.unified_diff);
//! ```

mod diff;
mod import_export;
mod object_types;
mod repository;
mod storage;
pub mod sync;
pub mod widgets;

pub use diff::{Change, ChangeType, DiffEngine};
pub use import_export::{
    ExportedTag, ExportedVersion, ImportOptions, ImportResult, VersionExport, export_versions,
    export_versions_to_json, import_versions, import_versions_from_json,
};
pub use object_types::DatabaseObjectType;
pub use repository::{CurrentDiff, VersionDiff, VersionEntry, VersionRepository};
pub use storage::{TrackedObject, VersionStorage, VersionTag, VersionedObjectInfo, make_object_id};
pub use sync::{
    DatabaseObject, PullConnection, PullOptions, PullResult, PullStatus, PullSummary,
    PushConnection, PushOptions, PushResult, PushStatus, pull_all, pull_from_database, push_all,
    push_to_database,
};
