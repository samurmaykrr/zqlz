# ZQLZ Architecture Guide

> A comprehensive guide to the ZQLZ database IDE architecture, component structure, and development patterns.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Layer Architecture](#layer-architecture)
4. [Crate Reference](#crate-reference)
5. [Core Widgets & Components](#core-widgets--components)
6. [Data Flow Patterns](#data-flow-patterns)
7. [Where to Edit What](#where-to-edit-what)
8. [Common Development Tasks](#common-development-tasks)

---

## Overview

ZQLZ is a modern database IDE built with:

- **GPUI** - Zed's high-performance UI framework
- **gpui-component** - Reusable UI component library
- **Rust** - Systems programming language

The application follows a layered architecture with clear separation between infrastructure, domain, services, and UI layers.

---

## Project Structure

```
zqlz-private/
├── Cargo.toml                    # Workspace manifest
├── CLAUDE.md                     # AI coding guidelines
├── ARCHITECTURE.md               # This file
├── assets/
│   ├── keymaps/                  # Keyboard shortcuts
│   │   └── default.json
│   └── themes/                   # UI themes
└── crates/
    │
    │── zqlz-app/                 # Main application entry point
    │
    │── zqlz-core/                # Core traits & abstractions
    │
    │── zqlz-drivers/             # Database driver aggregator
    │   └── crates/
    │       ├── zqlz-driver-postgres/
    │       ├── zqlz-driver-mysql/
    │       ├── zqlz-driver-sqlite/
    │       ├── zqlz-driver-mssql/
    │       ├── zqlz-driver-duckdb/
    │       ├── zqlz-driver-redis/
    │       ├── zqlz-driver-mongodb/
    │       └── zqlz-driver-clickhouse/
    │
    │── zqlz-connection/          # Connection management & widgets
    │── zqlz-query/               # Query execution & widgets
    │── zqlz-schema/              # Schema introspection & widgets
    │── zqlz-editor/              # SQL editor functionality
    │── zqlz-lsp/                 # SQL Language Server
    │── zqlz-services/            # Service layer (business logic)
    │── zqlz-ui/                  # UI widget library
    │── zqlz-settings/            # Settings persistence
    │
    │── zqlz-table-designer/      # Table structure designer
    │── zqlz-trigger-designer/    # Trigger designer
    │── zqlz-interchange/         # Import/Export (CSV, etc.)
    │── zqlz-versioning/          # Database object version control
    │── zqlz-templates/           # SQL templating (DBT-like)
    │── zqlz-analyzer/            # Query analysis
    │── zqlz-objects/             # Database object models
    │── zqlz-admin/               # Database administration
    │── zqlz-schema-tools/        # Cross-database schema sync
    └── zqlz-monitor/             # Database monitoring
```

---

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    UI Layer (zqlz-app)                          │
│  MainView, ConnectionSidebar, QueryEditor, ResultsPanel, etc.   │
├─────────────────────────────────────────────────────────────────┤
│                 Widget Library (zqlz-ui)                        │
│  DockArea, Table, Tree, Input, Menu, Theme, Notifications       │
├─────────────────────────────────────────────────────────────────┤
│               Service Layer (zqlz-services)                     │
│  QueryService, SchemaService, TableService, ConnectionService   │
├─────────────────────────────────────────────────────────────────┤
│                   Domain Layer                                  │
│  zqlz-query, zqlz-schema, zqlz-connection, zqlz-editor,         │
│  zqlz-lsp, zqlz-table-designer, zqlz-versioning, etc.           │
├─────────────────────────────────────────────────────────────────┤
│               Infrastructure Layer                              │
│  zqlz-core (traits), zqlz-drivers (implementations)             │
└─────────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Purpose | Crates |
|-------|---------|--------|
| **UI** | User interface, event handling, rendering | `zqlz-app` |
| **Widget** | Reusable UI components | `zqlz-ui` |
| **Service** | Business logic orchestration | `zqlz-services` |
| **Domain** | Feature-specific logic and widgets | `zqlz-query`, `zqlz-schema`, etc. |
| **Infrastructure** | Core abstractions and drivers | `zqlz-core`, `zqlz-drivers` |

---

## Crate Reference

### zqlz-app (Main Application)

**Purpose**: Entry point, main window, dock layout orchestration

**Key Files**:
| File | Purpose |
|------|---------|
| `src/main.rs` | Application entry point, GPUI initialization |
| `src/app.rs` | `AppState` - global state (connections, services) |
| `src/main_view/mod.rs` | `MainView` - main window with dock layout |
| `src/main_view/connection_handlers.rs` | Connection event handling |
| `src/main_view/query_handlers.rs` | Query execution handling |
| `src/app_init.rs` | Panel registration |
| `src/keymaps.rs` | Keybinding loading |
| `src/actions.rs` | Application actions |
| `src/components/` | App-specific UI components |

**AppState Structure**:
```rust
pub struct AppState {
    pub connections: Arc<ConnectionManager>,
    pub query_service: Arc<QueryService>,
    pub schema_service: Arc<SchemaService>,
    pub table_service: Arc<TableService>,
    pub connection_service: Arc<ConnectionService>,
    pub version_repository: Arc<VersionRepository>,
    pub storage: Arc<LocalStorage>,
}
```

---

### zqlz-core (Core Abstractions)

**Purpose**: Foundation traits that all crates depend on

**Key Traits**:
```rust
// Database driver trait - implement for new databases
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> DriverCapabilities;
    fn dialect_info(&self) -> DialectInfo;
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>>;
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()>;
    fn connection_field_schema(&self) -> ConnectionFieldSchema;
}

// Connection trait - represents an active connection
#[async_trait]
pub trait Connection: Send + Sync {
    fn driver_name(&self) -> &str;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;
    async fn close(&self) -> Result<()>;
    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection>;
    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>>;
}

// Schema introspection - optional capability
#[async_trait]
pub trait SchemaIntrospection: Send + Sync {
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>>;
    async fn get_table_details(&self, table_name: &str) -> Result<TableDetails>;
    // ...
}
```

**Key Types**:
- `ConnectionConfig` - Connection configuration
- `Value` - Database value enum (String, Int, Float, etc.)
- `QueryResult`, `Row`, `ColumnMeta` - Query result types
- `DialectInfo` - SQL dialect metadata

---

### zqlz-drivers (Database Drivers)

**Purpose**: Database-specific implementations

| Driver | Database | Status |
|--------|----------|--------|
| `zqlz-driver-postgres` | PostgreSQL | Active |
| `zqlz-driver-mysql` | MySQL/MariaDB | Active |
| `zqlz-driver-sqlite` | SQLite | Active |
| `zqlz-driver-mssql` | SQL Server | Active |
| `zqlz-driver-duckdb` | DuckDB | Active |
| `zqlz-driver-redis` | Redis | Active |
| `zqlz-driver-mongodb` | MongoDB | Active |
| `zqlz-driver-clickhouse` | ClickHouse | Active |

**DriverRegistry**:
```rust
pub struct DriverRegistry {
    drivers: HashMap<String, Arc<dyn DatabaseDriver>>,
}

impl DriverRegistry {
    pub fn with_defaults() -> Self;  // All drivers pre-registered
    pub fn get(&self, name: &str) -> Option<Arc<dyn DatabaseDriver>>;
}
```

---

### zqlz-connection (Connection Management)

**Purpose**: Connection lifecycle, pooling, persistence

**Key Types**:
```rust
pub struct ConnectionManager {
    drivers: DriverRegistry,
    active: RwLock<HashMap<Uuid, Arc<dyn Connection>>>,
    saved: RwLock<Vec<SavedConnection>>,
}

pub struct ConnectionPool { /* bb8-based pooling */ }
pub struct ReconnectingConnection { /* Auto-reconnect wrapper */ }
pub struct HealthChecker { /* Connection health monitoring */ }
```

**Widgets**:
- `ConnectionSidebar` - Left sidebar with connection list
- `ConnectionEntry` - Individual connection item in list

---

### zqlz-query (Query Execution)

**Purpose**: Query execution, history, results display

**Key Types**:
```rust
pub struct QueryService { /* Query execution orchestration */ }
pub struct QueryEngine { /* Core query execution */ }
pub struct QueryHistory { /* Query history storage */ }
pub struct BatchExecutor { /* Multi-statement batch execution */ }
```

**Widgets**:
- `QueryEditor` - SQL editor with syntax highlighting
- `QueryTabsPanel` - Tab container for multiple editors
- `ResultsPanel` - Query results (grid, messages, EXPLAIN)

---

### zqlz-schema (Schema Introspection)

**Purpose**: Schema caching, DDL generation, schema browser

**Key Types**:
```rust
pub struct SchemaCache { /* Basic schema caching */ }
pub struct LazySchemaCache { /* On-demand loading with LRU */ }
pub struct DdlGenerator { /* DDL generation from schema */ }
pub struct DependencyAnalyzer { /* Object dependency analysis */ }
```

**Widgets**:
- `SchemaTreePanel` - Tree view of database objects
- `SchemaDetailsPanel` - Details for selected object
- `ObjectsPanel` - Combined schema browser

---

### zqlz-ui (Widget Library)

**Purpose**: Reusable UI widgets built on GPUI

| Category | Widgets |
|----------|---------|
| **Dock System** | `DockArea`, `DockItem`, `Dock`, `TabPanel`, `StackPanel`, `Tiles` |
| **Data Display** | `Table`, `Tree`, `List`, `VirtualList` |
| **Input** | `Input`, `Select`, `Checkbox`, `Switch`, `Slider`, `Radio` |
| **Containers** | `Root`, `Popover`, `Dialog`, `Sheet`, `Collapsible` |
| **Navigation** | `Menu`, `ContextMenu`, `TitleBar`, `Tab` |
| **Feedback** | `Notification`, `Spinner`, `Skeleton`, `Badge` |
| **Typography** | `Label`, `Text`, `Kbd` |
| **Theme** | `Theme`, `ThemeRegistry`, `ThemeMode` |

**Dock System** (Critical for layout):
```rust
pub struct DockArea {
    items: DockItem,           // Center content
    left_dock: Option<Entity<Dock>>,
    bottom_dock: Option<Entity<Dock>>,
    right_dock: Option<Entity<Dock>>,
}

pub enum DockItem {
    Split { axis, items, view: Entity<StackPanel> },
    Tabs { items, view: Entity<TabPanel> },
    Panel { view: Arc<dyn PanelView> },
    Tiles { items, view: Entity<Tiles> },
}
```

---

### zqlz-services (Service Layer)

**Purpose**: Business logic orchestration

| Service | Purpose |
|---------|---------|
| `QueryService` | Query execution with caching |
| `SchemaService` | Schema operations with caching |
| `TableService` | Table browsing, cell editing |
| `TableDesignService` | Table structure design |
| `ConnectionService` | Connection lifecycle |

---

### zqlz-lsp (SQL Language Server)

**Purpose**: IntelliSense for SQL

| Component | Purpose |
|-----------|---------|
| `SqlCompletionProvider` | Auto-complete suggestions |
| `SqlDiagnostics` | Error/warning detection |
| `SqlHoverProvider` | Hover information |
| `SignatureProvider` | Function signature help |
| `ContextAnalyzer` | SQL context detection |
| `SchemaValidator` | Schema-aware validation |

---

### zqlz-settings (Settings)

**Purpose**: Application settings with persistence

```rust
pub struct ZqlzSettings {
    pub appearance: AppearanceSettings,  // Theme, colors
    pub fonts: FontSettings,             // UI and editor fonts
    pub editor: EditorSettings,          // Tab size, line numbers
    pub connections: ConnectionSettings, // Default limits
}
```

**Persistence Locations**:
- Settings: `~/.config/zqlz/settings.json`
- Layouts: `~/.config/zqlz/workspaces/<id>/layout.json`
- Connections: `~/.config/zqlz/connections.json`

---

## Core Widgets & Components

### Component Hierarchy

```
Root (Theme wrapper, dialogs, notifications)
└── MainView
    ├── TitleBar
    │   └── Connection selector, toolbar buttons
    │
    ├── DockArea
    │   ├── Left Dock
    │   │   └── ConnectionSidebar
    │   │       ├── Connection list
    │   │       └── SchemaTreePanel
    │   │
    │   ├── Center
    │   │   ├── QueryTabsPanel
    │   │   │   └── QueryEditor (multiple)
    │   │   ├── ObjectsPanel
    │   │   └── TableViewer
    │   │
    │   ├── Bottom Dock
    │   │   └── ResultsPanel
    │   │       ├── Results grid
    │   │       ├── Messages
    │   │       └── EXPLAIN view
    │   │
    │   └── Right Dock
    │       ├── SchemaDetailsPanel
    │       └── CellEditorPanel
    │
    └── StatusBar
        └── Connection status, diagnostics count
```

### Key Widget Locations

| Widget | Crate | Path |
|--------|-------|------|
| `MainView` | zqlz-app | `src/main_view/mod.rs` |
| `ConnectionSidebar` | zqlz-connection | `src/widgets/sidebar.rs` |
| `QueryEditor` | zqlz-query | `src/widgets/editor.rs` |
| `QueryTabsPanel` | zqlz-query | `src/widgets/tabs_panel.rs` |
| `ResultsPanel` | zqlz-query | `src/widgets/results_panel.rs` |
| `SchemaTreePanel` | zqlz-schema | `src/widgets/tree_panel.rs` |
| `SchemaDetailsPanel` | zqlz-schema | `src/widgets/details_panel.rs` |
| `ObjectsPanel` | zqlz-schema | `src/widgets/objects_panel.rs` |
| `DockArea` | zqlz-ui | `src/widgets/dock/dock_area.rs` |
| `Table` | zqlz-ui | `src/widgets/table/` |
| `Tree` | zqlz-ui | `src/widgets/tree/` |

---

## Data Flow Patterns

### Query Execution Flow

```
User Action (Cmd+Enter)
    │
    ▼
QueryEditor.execute_query()
    │
    ▼
QueryTabsPanel (emits QueryTabsPanelEvent::QueryExecuted)
    │
    ▼
MainView.handle_query_tabs_event()
    │
    ▼
AppState.query_service.execute()
    │
    ▼
ConnectionManager.get(connection_id)
    │
    ▼
Connection.query(sql, params)  [background thread]
    │
    ▼
ResultsPanel.set_results()
    │
    ▼
cx.notify() → UI re-renders
```

### Connection Flow

```
User Action (Double-click connection)
    │
    ▼
ConnectionSidebar (emits ConnectionSidebarEvent::Connect)
    │
    ▼
MainView.handle_sidebar_event()
    │
    ▼
AppState.connection_service.connect()
    │
    ▼
ConnectionManager.connect(saved_connection)
    │
    ▼
DatabaseDriver.connect(config)
    │
    ▼
Arc<dyn Connection> created
    │
    ▼
WorkspaceState.set_active_connection()
    │
    ▼
SchemaTreePanel.refresh()
```

### State Management Pattern

```rust
// Centralized state
pub struct WorkspaceState {
    active_connection_id: Option<Uuid>,
    connection_statuses: HashMap<Uuid, bool>,
    active_editor_id: Option<EditorId>,
}

// Events for state changes
pub enum WorkspaceStateEvent {
    ActiveConnectionChanged(Option<Uuid>),
    ConnectionStatusChanged { id: Uuid, connected: bool },
    QueryStarted { editor_id: EditorId },
    QueryCompleted { editor_id: EditorId, success: bool },
}

// Components subscribe to state changes
cx.subscribe(&workspace_state, |this, _state, event, cx| {
    match event {
        WorkspaceStateEvent::ActiveConnectionChanged(id) => {
            this.update_connection_indicator(*id, cx);
        }
        // ...
    }
});
```

---

## Where to Edit What

### Quick Reference Table

| Task | File(s) to Edit |
|------|-----------------|
| Add new panel | `zqlz-app/src/app_init.rs` + panel crate |
| Add keybinding | `assets/keymaps/default.json` |
| Add action | `zqlz-app/src/actions.rs` + handler in `main_view/` |
| Add database driver | `zqlz-drivers/crates/zqlz-driver-{name}/` |
| Modify dock layout | `zqlz-app/src/main_view/mod.rs` |
| Add setting | `zqlz-settings/src/zqlz_settings.rs` |
| Add service | `zqlz-services/src/` |
| Add UI widget | `zqlz-ui/src/widgets/` |
| Modify query editor | `zqlz-query/src/widgets/editor.rs` |
| Modify results display | `zqlz-query/src/widgets/results_panel.rs` |
| Modify schema tree | `zqlz-schema/src/widgets/tree_panel.rs` |
| Modify connection sidebar | `zqlz-connection/src/widgets/sidebar.rs` |

---

## Common Development Tasks

### Adding a New Panel

1. **Create the panel component** in the appropriate crate:

```rust
// zqlz-schema/src/widgets/my_panel.rs
use zqlz_ui::dock::{Panel, PanelEvent};

pub struct MyPanel {
    // state
}

impl MyPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self { }
    }
}

impl Panel for MyPanel {
    fn title(&self, _cx: &App) -> SharedString {
        "My Panel".into()
    }

    fn icon(&self, _cx: &App) -> Option<Icon> {
        Some(Icon::new(IconName::Database))
    }
}

impl Render for MyPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div().child("Hello from MyPanel")
    }
}
```

2. **Register the panel** in `zqlz-app/src/app_init.rs`:

```rust
register_panel(cx, "MyPanel", |state, window, cx| {
    Box::new(cx.new(|cx| MyPanel::new(cx)))
});
```

3. **Add to dock layout** in `zqlz-app/src/main_view/mod.rs`:

```rust
dock_area.update(cx, |area, cx| {
    area.add_panel_to_right_dock(
        Arc::new(cx.new(|cx| MyPanel::new(cx))),
        cx,
    );
});
```

---

### Adding a New Database Driver

1. **Create the crate** under `crates/zqlz-drivers/crates/`:

```
zqlz-driver-newdb/
├── Cargo.toml
└── src/
    └── zqlz_driver_newdb.rs
```

2. **Implement the traits**:

```rust
// zqlz_driver_newdb.rs
use zqlz_core::{DatabaseDriver, Connection, ConnectionConfig};

pub struct NewDbDriver;

#[async_trait]
impl DatabaseDriver for NewDbDriver {
    fn name(&self) -> &'static str { "newdb" }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities::default()
    }

    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        // Implementation
    }

    // ... other methods
}

pub struct NewDbConnection { /* ... */ }

#[async_trait]
impl Connection for NewDbConnection {
    // Implementation
}
```

3. **Register in DriverRegistry** (`zqlz-drivers/src/registry.rs`):

```rust
impl DriverRegistry {
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(Arc::new(PostgresDriver));
        registry.register(Arc::new(NewDbDriver));  // Add here
        registry
    }
}
```

4. **Add to workspace** in root `Cargo.toml`:

```toml
[workspace]
members = [
    "crates/zqlz-drivers/crates/zqlz-driver-newdb",
]
```

---

### Adding an Action & Keybinding

1. **Define the action** in `zqlz-app/src/actions.rs`:

```rust
actions!(
    zqlz,
    [
        // Existing actions...
        MyNewAction,
    ]
);
```

2. **Add keybinding** in `assets/keymaps/default.json`:

```json
{
  "context": "MainView",
  "bindings": {
    "cmd-shift-m": "zqlz::MyNewAction"
  }
}
```

3. **Handle the action** in `main_view/mod.rs`:

```rust
impl MainView {
    fn new(/* ... */) -> Self {
        // In render or initialization
        .on_action(cx.listener(Self::handle_my_new_action))
    }

    fn handle_my_new_action(
        &mut self,
        _action: &MyNewAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Implementation
    }
}
```

---

### Adding a Setting

1. **Add to settings struct** in `zqlz-settings/src/zqlz_settings.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EditorSettings {
    pub tab_size: usize,
    pub show_line_numbers: bool,
    pub my_new_setting: bool,  // Add here
}

impl Default for EditorSettings {
    fn default() -> Self {
        Self {
            tab_size: 4,
            show_line_numbers: true,
            my_new_setting: false,  // Default value
        }
    }
}
```

2. **Add UI controls** in `zqlz-settings/src/widgets/settings_panel.rs`:

```rust
fn render_editor_settings(&self, cx: &mut Context<Self>) -> impl IntoElement {
    div()
        .child(
            Checkbox::new("my_new_setting")
                .label("My New Setting")
                .checked(self.settings.editor.my_new_setting)
                .on_click(cx.listener(|this, _, cx| {
                    this.settings.editor.my_new_setting = !this.settings.editor.my_new_setting;
                    this.save_settings(cx);
                }))
        )
}
```

---

### Adding a Service

1. **Create the service** in `zqlz-services/src/`:

```rust
// zqlz-services/src/my_service.rs
pub struct MyService {
    connections: Arc<ConnectionManager>,
}

impl MyService {
    pub fn new(connections: Arc<ConnectionManager>) -> Self {
        Self { connections }
    }

    pub async fn do_operation(&self, connection_id: Uuid) -> Result<SomeResult> {
        let conn = self.connections.get(connection_id)?;
        // Implementation
    }
}
```

2. **Export in lib.rs**:

```rust
// zqlz-services/src/lib.rs
mod my_service;
pub use my_service::MyService;
```

3. **Add to AppState** in `zqlz-app/src/app.rs`:

```rust
pub struct AppState {
    // Existing fields...
    pub my_service: Arc<MyService>,
}

impl AppState {
    pub fn new() -> Self {
        let connections = Arc::new(ConnectionManager::new());
        Self {
            connections: connections.clone(),
            my_service: Arc::new(MyService::new(connections.clone())),
            // ...
        }
    }
}
```

---

### Modifying the Dock Layout

Edit `MainView::new()` in `crates/zqlz-app/src/main_view/mod.rs`:

```rust
impl MainView {
    pub fn new(state: Arc<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let dock_area = cx.new(|cx| DockArea::new(cx));

        dock_area.update(cx, |area, cx| {
            // Left dock - connection sidebar
            area.set_left_dock(
                cx.new(|cx| Dock::new(DockPosition::Left, cx))
                    .with_panel(Arc::new(connection_sidebar.clone())),
                cx,
            );

            // Center - main content
            area.set_center(
                DockItem::tabs(vec![
                    DockItem::panel(Arc::new(objects_panel)),
                    DockItem::panel(Arc::new(query_tabs)),
                ]),
                cx,
            );

            // Bottom dock - results
            area.set_bottom_dock(
                cx.new(|cx| Dock::new(DockPosition::Bottom, cx))
                    .with_panel(Arc::new(results_panel)),
                cx,
            );

            // Right dock - details
            area.set_right_dock(
                cx.new(|cx| Dock::new(DockPosition::Right, cx))
                    .with_panel(Arc::new(details_panel)),
                cx,
            );
        });

        Self { dock_area, /* ... */ }
    }
}
```

---

## Design Patterns

### Entity<T> Pattern (GPUI State Management)

```rust
// Create an entity
let editor = cx.new(|cx| QueryEditor::new(cx));

// Read state
let text = editor.read(cx).get_text();

// Update state
editor.update(cx, |editor, cx| {
    editor.set_text("SELECT 1", cx);
    cx.notify();  // Trigger re-render
});
```

### Event Subscription Pattern

```rust
// Emit events
impl EventEmitter<QueryEditorEvent> for QueryEditor {}

cx.emit(QueryEditorEvent::QueryExecuted(results));

// Subscribe to events
let subscription = cx.subscribe(&query_editor, |this, _editor, event, cx| {
    match event {
        QueryEditorEvent::QueryExecuted(results) => {
            this.show_results(results.clone(), cx);
        }
    }
});

// Store subscription to keep it alive
self._subscriptions.push(subscription);
```

### Background Execution Pattern

```rust
fn execute_query(&mut self, cx: &mut Context<Self>) {
    self.is_executing = true;
    cx.notify();

    let connection = self.connection.clone();
    let sql = self.get_sql();

    cx.spawn(async move |this, cx| {
        // Background: Execute query
        let result = connection.query(&sql, &[]).await;

        // Foreground: Update UI
        _ = this.update_in(cx, |this, window, cx| {
            this.is_executing = false;
            match result {
                Ok(data) => this.show_results(data, cx),
                Err(e) => {
                    window.push_notification(Notification::error(&e.to_string()), cx);
                }
            }
            cx.notify();
        });

        Ok::<_, anyhow::Error>(())
    }).detach();
}
```

---

## File Naming Conventions

- **No `mod.rs` files** - Use explicit paths like `src/crate_name.rs`
- **Large focused files** - Keep related logic together (workspace.rs can be 400KB+)
- **Cargo.toml lib path** - Use `[lib] path = "src/crate_name.rs"`

Example:
```
crates/zqlz-workspace/
├── Cargo.toml              # [lib] path = "src/zqlz_workspace.rs"
└── src/
    ├── zqlz_workspace.rs   # Main logic (large file OK!)
    ├── dock.rs             # Dock-specific logic
    └── pane.rs             # Pane management
```

---

## Further Reading

- **CLAUDE.md** - AI coding guidelines and patterns
- **GPUI_COMPONENT_REFERENCE.md** - Available UI components
- **PLAN.md** - Product specification
- **/zed/** - Reference implementation (study Zed's codebase)
- **/gpui-component/** - UI component library source
