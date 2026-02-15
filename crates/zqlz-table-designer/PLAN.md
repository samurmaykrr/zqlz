# ZQLZ Table Designer Crate Plan

## Overview

Extract the table designer functionality from `zqlz-app` into a standalone crate `zqlz-table-designer`. This crate will provide a reusable table design panel that can be integrated into any GPUI application.

---

## 1. Crate Structure

```
crates/zqlz-table-designer/
├── Cargo.toml
├── src/
│   ├── table_designer.rs          # Main library entry point
│   ├── panel.rs                   # TableDesignerPanel UI component
│   ├── models/
│   │   ├── mod.rs
│   │   ├── table_design.rs        # TableDesign, ColumnDesign, IndexDesign, ForeignKeyDesign
│   │   ├── table_options.rs       # TableOptions, dialect-specific options
│   │   ├── data_types.rs          # DataTypeInfo, DataTypeCategory, dialect data types
│   │   └── validation.rs          # ValidationError and validation logic
│   ├── service/
│   │   ├── mod.rs
│   │   ├── ddl_generator.rs       # DDL generation (CREATE, ALTER, DROP)
│   │   ├── table_loader.rs        # Load existing table structure from connection
│   │   └── dialect.rs             # DatabaseDialect enum and helpers
│   ├── ui/
│   │   ├── mod.rs
│   │   ├── fields_tab.rs          # Column/fields editing tab
│   │   ├── indexes_tab.rs         # Index management tab
│   │   ├── foreign_keys_tab.rs    # Foreign key management tab
│   │   ├── options_tab.rs         # Dialect-specific options tab
│   │   ├── sql_preview_tab.rs     # DDL preview tab
│   │   ├── toolbar.rs             # Add/Remove/Reorder toolbar
│   │   └── tab_bar.rs             # Tab bar component
│   └── events.rs                  # TableDesignerEvent definitions
└── tests/
    ├── mod.rs
    ├── ddl_generation_tests.rs    # DDL generation tests
    ├── validation_tests.rs        # Model validation tests
    ├── table_loader_tests.rs      # Table loading tests (requires connection)
    └── integration_tests.rs       # Full panel integration tests
```

---

## 2. Dependencies

### Cargo.toml

```toml
[package]
name = "zqlz-table-designer"
version.workspace = true
edition.workspace = true
license.workspace = true

[lib]
path = "src/table_designer.rs"

[dependencies]
# Core
zqlz-core = { workspace = true }
zqlz-ui = { workspace = true }

# GPUI
gpui = { workspace = true }

# Utilities
uuid = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

# Async
async-trait = { workspace = true }

[dev-dependencies]
# Testing with in-memory SQLite
zqlz-drivers = { workspace = true }
rusqlite = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
tempfile = { workspace = true }
```

### Key Dependency Relationships

```
zqlz-table-designer
├── zqlz-core         # Connection trait, schema types (ColumnInfo, IndexInfo, ForeignKeyInfo, ForeignKeyAction)
├── zqlz-ui           # UI widgets (Button, Checkbox, Input, Tab, TabBar, Panel trait)
└── gpui              # GPUI framework
```

**NOT depending on:**
- `zqlz-app` - The app depends on us, not vice versa
- `zqlz-services` - We absorb the TableDesignService functionality
- `zqlz-drivers` - Only for dev-dependencies (testing)
- `zqlz-connection` - Connection management stays in app layer

---

## 3. Public API

### Entry Point (`src/table_designer.rs`)

```rust
//! Table Designer for ZQLZ
//!
//! A standalone GPUI panel for designing and modifying database tables.
//!
//! ## Features
//! - Visual column editor with type selection
//! - Index management
//! - Foreign key constraint editor
//! - Dialect-specific options (SQLite, PostgreSQL, MySQL)
//! - DDL preview and generation
//!
//! ## Usage
//!
//! ```rust
//! use zqlz_table_designer::{TableDesignerPanel, TableDesign, DatabaseDialect};
//! use zqlz_core::Connection;
//!
//! // Create a new table designer for a new table
//! let design = TableDesign::new("users", DatabaseDialect::Sqlite);
//! let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
//!
//! // Or load an existing table for editing
//! let design = TableDesign::from_table_details(table_details, dialect);
//! let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
//! ```

pub mod models;
pub mod service;
pub mod events;
mod panel;
mod ui;

// Re-exports for convenience
pub use events::TableDesignerEvent;
pub use models::{
    ColumnDesign, DatabaseDialect, DataTypeCategory, DataTypeInfo, ForeignKeyDesign,
    IndexDesign, TableDesign, TableOptions, ValidationError,
};
pub use panel::TableDesignerPanel;
pub use service::{DdlGenerator, TableLoader};
```

### Main Types

#### `TableDesignerPanel`

```rust
impl TableDesignerPanel {
    /// Create a new table designer panel
    pub fn new(
        connection_id: Uuid,
        design: TableDesign,
        cx: &mut Context<Self>,
    ) -> Self;
    
    /// Get the current design state
    pub fn design(&self) -> &TableDesign;
    
    /// Check if there are unsaved changes
    pub fn is_dirty(&self) -> bool;
    
    /// Generate DDL for the current design
    pub fn generate_ddl(&self) -> Result<String>;
}

// Implements Panel, Render, Focusable, EventEmitter<TableDesignerEvent>
```

#### `TableDesign`

```rust
impl TableDesign {
    /// Create a new empty table design
    pub fn new(table_name: impl Into<String>, dialect: DatabaseDialect) -> Self;
    
    /// Create from existing table details (for editing)
    pub fn from_table_details(details: &TableDetails, dialect: DatabaseDialect) -> Self;
    
    /// Validate the design
    pub fn validate(&self) -> Vec<ValidationError>;
    
    /// Add a column
    pub fn add_column(&mut self) -> &mut ColumnDesign;
    
    /// Add an index
    pub fn add_index(&mut self) -> &mut IndexDesign;
    
    /// Add a foreign key
    pub fn add_foreign_key(&mut self) -> &mut ForeignKeyDesign;
}
```

#### `TableDesignerEvent`

```rust
pub enum TableDesignerEvent {
    /// User wants to save the design
    Save {
        connection_id: Uuid,
        design: TableDesign,
        is_new: bool,
    },
    /// User cancelled
    Cancel,
    /// Request DDL preview (optional, for async generation)
    PreviewDdl {
        design: TableDesign,
    },
}
```

---

## 4. Migration Plan

### Phase 1: Create New Crate Structure

1. Create `crates/zqlz-table-designer/` directory
2. Create `Cargo.toml` with dependencies
3. Add to workspace in root `Cargo.toml`

### Phase 2: Move Models

1. Move `TableDesign`, `ColumnDesign`, `IndexDesign`, `ForeignKeyDesign` from `zqlz-services`
2. Move `TableOptions`, `DatabaseDialect` from `zqlz-services`
3. Move `DataTypeInfo`, `DataTypeCategory` from `zqlz-services`
4. Move `ValidationError` and validation logic

### Phase 3: Move Service Logic

1. Extract DDL generation into `DdlGenerator`
2. Extract table loading into `TableLoader`
3. Keep the service stateless - no connection management

### Phase 4: Move UI Components

1. Move `TableDesignerPanel` from `zqlz-app`
2. Split render methods into separate modules (`fields_tab.rs`, etc.)
3. Clean up to use zqlz-ui widgets exclusively

### Phase 5: Update Dependents

1. Update `zqlz-services` to remove moved code
2. Update `zqlz-app` to depend on `zqlz-table-designer`
3. Update imports in `MainView`

### Phase 6: Add Tests

1. Unit tests for DDL generation
2. Unit tests for validation
3. Integration tests with in-memory SQLite
4. GPUI tests for panel rendering

---

## 5. Testing Strategy

### 5.1 Unit Tests

#### DDL Generation Tests (`tests/ddl_generation_tests.rs`)

```rust
#[test]
fn test_create_table_sqlite_basic() {
    let design = TableDesign::new("users", DatabaseDialect::Sqlite)
        .with_column(ColumnDesign::new("id").integer().primary_key())
        .with_column(ColumnDesign::new("name").text().not_null());
    
    let ddl = DdlGenerator::generate_create_table(&design).unwrap();
    
    assert_eq!(ddl, indoc! {r#"
        CREATE TABLE "users" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT NOT NULL
        );
    "#});
}

#[test]
fn test_create_table_postgres_with_types() {
    // Test PostgreSQL-specific types and syntax
}

#[test]
fn test_create_table_mysql_with_engine() {
    // Test MySQL ENGINE, CHARSET options
}

#[test]
fn test_alter_table_add_column() {
    // Test ALTER TABLE generation
}

#[test]
fn test_create_index() {
    // Test CREATE INDEX generation
}

#[test]
fn test_add_foreign_key() {
    // Test FOREIGN KEY constraint generation
}
```

#### Validation Tests (`tests/validation_tests.rs`)

```rust
#[test]
fn test_validate_empty_table_name() {
    let design = TableDesign::new("", DatabaseDialect::Sqlite);
    let errors = design.validate();
    
    assert!(errors.iter().any(|e| e.field == "table_name"));
}

#[test]
fn test_validate_duplicate_column_names() {
    let design = TableDesign::new("users", DatabaseDialect::Sqlite)
        .with_column(ColumnDesign::new("id"))
        .with_column(ColumnDesign::new("id")); // Duplicate
    
    let errors = design.validate();
    
    assert!(errors.iter().any(|e| e.field.contains("column")));
}

#[test]
fn test_validate_foreign_key_references() {
    // FK must reference valid columns
}

#[test]
fn test_validate_index_columns_exist() {
    // Index columns must exist in table
}
```

### 5.2 Integration Tests

#### Table Loader Tests (`tests/table_loader_tests.rs`)

```rust
#[tokio::test]
async fn test_load_table_from_sqlite() {
    // Create an in-memory SQLite database
    let conn = SqliteConnection::open_in_memory().await.unwrap();
    
    // Create a table with various features
    conn.execute(indoc! {r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    "#}, &[]).await.unwrap();
    
    // Load the table design
    let loader = TableLoader::new(&conn);
    let design = loader.load_table("users").await.unwrap();
    
    // Verify columns
    assert_eq!(design.columns.len(), 4);
    assert!(design.columns[0].is_primary_key);
    assert!(design.columns[0].is_auto_increment);
    assert!(!design.columns[1].nullable);
    assert!(design.columns[2].is_unique);
}

#[tokio::test]
async fn test_load_table_with_indexes() {
    // Test loading a table with multiple indexes
}

#[tokio::test]
async fn test_load_table_with_foreign_keys() {
    // Test loading a table with foreign key constraints
}
```

### 5.3 GPUI Panel Tests

#### Panel Integration Tests (`tests/integration_tests.rs`)

```rust
#[gpui::test]
fn test_panel_creates_with_new_table(cx: &mut TestAppContext) {
    let design = TableDesign::new("users", DatabaseDialect::Sqlite);
    let connection_id = Uuid::new_v4();
    
    let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
    
    panel.read(cx, |panel, _cx| {
        assert!(panel.design().is_new);
        assert_eq!(panel.design().table_name, "users");
        assert!(!panel.is_dirty());
    });
}

#[gpui::test]
fn test_panel_add_column(cx: &mut TestAppContext) {
    let design = TableDesign::new("users", DatabaseDialect::Sqlite);
    let connection_id = Uuid::new_v4();
    
    let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
    
    panel.update(cx, |panel, cx| {
        panel.add_column(cx);
    });
    
    panel.read(cx, |panel, _cx| {
        assert_eq!(panel.design().columns.len(), 1);
        assert!(panel.is_dirty());
    });
}

#[gpui::test]
fn test_panel_emits_save_event(cx: &mut TestAppContext) {
    let design = TableDesign::new("users", DatabaseDialect::Sqlite)
        .with_column(ColumnDesign::new("id").integer().primary_key());
    let connection_id = Uuid::new_v4();
    
    let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
    
    let events = Rc::new(RefCell::new(Vec::new()));
    let events_clone = events.clone();
    
    cx.subscribe(&panel, move |_panel, event, _cx| {
        events_clone.borrow_mut().push(event.clone());
    });
    
    panel.update(cx, |panel, cx| {
        panel.handle_save(cx);
    });
    
    assert_eq!(events.borrow().len(), 1);
    assert!(matches!(events.borrow()[0], TableDesignerEvent::Save { .. }));
}

#[gpui::test]
fn test_panel_validation_prevents_save(cx: &mut TestAppContext) {
    // Empty table name should prevent save
    let design = TableDesign::new("", DatabaseDialect::Sqlite);
    let connection_id = Uuid::new_v4();
    
    let panel = cx.new(|cx| TableDesignerPanel::new(connection_id, design, cx));
    
    panel.read(cx, |panel, _cx| {
        let errors = panel.design().validate();
        assert!(!errors.is_empty());
    });
}
```

### 5.4 Test Utilities

Create test helpers in `tests/common/mod.rs`:

```rust
pub fn create_test_sqlite_connection() -> Arc<dyn Connection> {
    // Create in-memory SQLite for testing
}

pub fn create_sample_table_design(dialect: DatabaseDialect) -> TableDesign {
    TableDesign::new("test_table", dialect)
        .with_column(ColumnDesign::new("id").integer().primary_key())
        .with_column(ColumnDesign::new("name").text().not_null())
        .with_column(ColumnDesign::new("email").text().unique())
}
```

---

## 6. Success Criteria

1. **Clean separation**: `zqlz-table-designer` has no dependency on `zqlz-app`
2. **Reusable**: Can be used in any GPUI application
3. **Well-tested**: >80% code coverage on DDL generation and validation
4. **Documented**: Public API has doc comments with examples
5. **Type-safe**: All dialect-specific logic is encapsulated

---

## 7. Future Enhancements

After initial extraction:

1. **Column type picker dropdown** - Visual type selection per dialect
2. **Schema browser integration** - For FK table/column selection
3. **Diff view** - Show ALTER statements for modifications
4. **Undo/Redo** - Design change history
5. **Templates** - Common table patterns (audit columns, soft delete, etc.)
