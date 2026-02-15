# ZQLZ

A high-performance, cross-platform database IDE built with Rust and GPUI.

## Overview

ZQLZ is a modern database management tool designed for developers who value speed, reliability, and a native user experience. Built entirely in Rust using the GPUI rendering framework, it delivers sub-second startup times and smooth performance while maintaining a small binary footprint.

### Project Status

**Current Version:** 0.1.0-alpha
**License:** MIT OR Apache-2.0
**Platforms:** macOS (primary), Windows and Linux (planned)

Phases 1-3 complete (Foundation, Query System, Schema Browser). Currently supports SQLite with PostgreSQL and MySQL drivers in development.

### Design Philosophy

- **Performance First:** Native Rust implementation with zero-cost abstractions
- **Developer Experience:** VSCode-inspired keyboard shortcuts and workflow
- **Extensibility:** Pluggable driver architecture for easy database support
- **Privacy:** Local-first with encrypted credential storage

## Features

### Currently Available

**Query Editor**
- Powered by Zed's professional editor component
- Advanced SQL IntelliSense with context-aware completions
- Fuzzy matching for tables, columns, and keywords
- Real-time syntax highlighting and error diagnostics
- Multi-cursor editing with Cmd+D (select next occurrence)
- Advanced text selections and transformations
- Query history tracking

**Schema Browser**
- Hierarchical object tree (tables, views, indexes, triggers, functions, procedures)
- DDL generation for all database objects
- Foreign key and constraint visualization
- Schema caching for performance

**Connection Management**
- Encrypted credential storage using ring
- Connection grouping and organization
- Saved connection profiles
- Connection testing and validation

**User Interface**
- 4-panel dockable layout (left, right, center, bottom)
- Resizable and collapsible panels
- Multi-tab query and results interface
- Light and dark theme support
- Layout persistence across sessions

### In Development

- PostgreSQL driver (high priority)
- MySQL driver (high priority)
- Inline data editing
- CSV/JSON/Excel export
- MiniJinja template mode for DBT-like SQL templating
- Git-like version control for stored procedures and views

## Architecture

### System Design

ZQLZ follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                      APPLICATION LAYER                      │
│  zqlz-app: UI components, event handlers, SQL LSP           │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                       UI FRAMEWORK                          │
│  zqlz-ui: Widget library, dock system, theme engine         │
│  zqlz-settings: Configuration and layout persistence        │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      SERVICE LAYER                          │
│  zqlz-services: Business logic, DTOs, orchestration         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      DOMAIN LAYER                           │
│  zqlz-query: Query execution engine                         │
│  zqlz-schema: Schema introspection and caching              │
│  zqlz-templates: MiniJinja SQL templating                   │
│  zqlz-versioning: Object version control                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   INFRASTRUCTURE LAYER                      │
│  zqlz-core: Core traits and abstractions                    │
│  zqlz-drivers: Database driver implementations              │
│  zqlz-connection: Connection management and pooling         │
└─────────────────────────────────────────────────────────────┘
```

### Crate Organization

| Crate | Responsibility | Lines of Code |
|-------|----------------|---------------|
| `zqlz-core` | Core traits (DatabaseDriver, Connection, SchemaIntrospection) | ~2,000 |
| `zqlz-drivers` | Database-specific implementations (currently SQLite) | ~1,500 |
| `zqlz-connection` | Connection lifecycle, credential storage | ~800 |
| `zqlz-query` | Query execution, parsing, history | ~1,200 |
| `zqlz-templates` | MiniJinja templating with SQL filters | ~500 |
| `zqlz-schema` | Schema caching, DDL generation | ~600 |
| `zqlz-versioning` | Version control for database objects | ~400 |
| `zqlz-services` | Service layer and view models | ~1,800 |
| `zqlz-settings` | Settings management and persistence | ~400 |
| `zqlz-zed-adapter` | Adapter layer for Zed editor integration | ~300 |
| `zqlz-ui` | Comprehensive widget library | ~38,000 |
| `zqlz-app` | Application entry point, components, SQL LSP | ~25,000 |

**Total:** ~72,500 lines of Rust across 220+ source files

### Technology Stack

**Core Framework**
- GPUI 0.2.2 - GPU-accelerated UI rendering (from Zed editor)
- Zed Editor components - Professional text editing with multi-cursor, vim mode
- Rust 2024 edition with async/await via Tokio

**Database Drivers**
- SQLite: `rusqlite` with bundled libsqlite3
- PostgreSQL: `tokio-postgres` (planned)
- MySQL: `mysql_async` (planned)

**Key Dependencies**
- Zed editor, text, and language crates for SQL editing
- Tree-sitter for syntax highlighting (SQL, JSON, and 15+ languages)
- MiniJinja for SQL templating
- sqlparser-rs for SQL parsing and validation
- ring for encryption
- serde for serialization
- similar for text diffing

**Development Tools**
- cargo for build and package management
- tracing for structured logging
- criterion for benchmarking

## Getting Started

### Prerequisites

- Rust 1.75 or later
- macOS 12.0+ (current platform support)

### Installation

#### From Source

```bash
git clone https://github.com/yourorg/zqlz.git
cd zqlz
cargo build --release
```

The compiled binary will be available at `target/release/zqlz-app`.

#### Running in Development Mode

```bash
cargo run -p zqlz-app
```

### First Run

1. Launch ZQLZ
2. Click "New Connection" in the left sidebar
3. Select SQLite and choose a database file
4. Click "Connect"
5. Browse schema in the left panel, write queries in the center editor

### Configuration

ZQLZ stores configuration in platform-specific directories:

- **macOS:** `~/Library/Application Support/com.yourorg.zqlz/`
- **Linux:** `~/.config/zqlz/` (planned)
- **Windows:** `%APPDATA%\zqlz\` (planned)

Configuration files:
- `settings.json` - User preferences (theme, fonts, editor settings)
- `connections.json` - Saved connection profiles (credentials encrypted)
- `layouts/*.json` - Workspace layout state

## Development

### Project Structure

```
zqlz/
├── crates/
│   ├── zqlz-core/          # Core abstractions
│   ├── zqlz-drivers/       # Database drivers
│   ├── zqlz-connection/    # Connection management
│   ├── zqlz-query/         # Query execution
│   ├── zqlz-templates/     # SQL templating
│   ├── zqlz-schema/        # Schema introspection
│   ├── zqlz-versioning/    # Version control
│   ├── zqlz-services/      # Service layer
│   ├── zqlz-settings/      # Settings management
│   ├── zqlz-zed-adapter/   # Zed editor adapter
│   ├── zqlz-ui/            # Widget library
│   ├── zqlz-lsp/           # SQL language server
│   └── zqlz-app/           # Main application
├── assets/
│   ├── icons/              # Application icons
│   └── themes/             # Theme definitions
├── examples/               # Example databases
├── tests/                  # Integration tests
└── docs/                   # Documentation
```

### Building

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Build specific crate
cargo build -p zqlz-core

# Run tests
cargo test

# Run tests with logging
RUST_LOG=debug cargo test

# Check code without building
cargo check
```

### Code Style

This project follows the Zed editor's coding conventions:

- Prefer large, focused files over many small files
- Avoid `mod.rs` - use explicit library paths in `Cargo.toml`
- Use full words for variable names (no abbreviations)
- Avoid functions that panic - use `?` to propagate errors
- Never silently discard errors with `let _ =`
- Comment only "why", not "what"

See `CLAUDE.md` for complete coding guidelines.

### Running with Logging

```bash
# Enable debug logging
RUST_LOG=zqlz_app=debug cargo run

# Enable trace logging for specific module
RUST_LOG=zqlz_app::sql_lsp=trace cargo run

# JSON formatted logs
RUST_LOG=debug cargo run 2>&1 | bunyan
```

Crash logs are automatically saved to:
- macOS: `~/Library/Logs/com.yourorg.zqlz/crashes/`

Application logs:
- macOS: `~/Library/Logs/com.yourorg.zqlz/app/`

### Adding a New Database Driver

1. Create a new module in `crates/zqlz-drivers/src/`:
   ```
   your_db/
   ├── mod.rs          # Driver implementation
   ├── connection.rs   # Connection trait impl
   └── schema.rs       # Schema introspection
   ```

2. Implement the core traits:
   ```rust
   pub struct YourDbDriver;

   impl DatabaseDriver for YourDbDriver {
       fn id(&self) -> &'static str { "yourdb" }
       fn name(&self) -> &'static str { "YourDB" }
       // ... implement required methods
   }

   impl Connection for YourDbConnection {
       // ... implement query execution, schema access
   }

   impl SchemaIntrospection for YourDbSchema {
       // ... implement schema listing methods
   }
   ```

3. Register the driver in `crates/zqlz-drivers/src/lib.rs`:
   ```rust
   pub fn register_all_drivers(registry: &mut DriverRegistry) {
       registry.register(Arc::new(SqliteDriver::new()));
       registry.register(Arc::new(YourDbDriver::new()));
   }
   ```

4. Add database-specific dependencies to `Cargo.toml`

See `crates/zqlz-drivers/src/sqlite/` for a complete reference implementation.

## Testing

### Unit Tests

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p zqlz-core

# Run specific test
cargo test test_connection_manager
```

### Integration Tests

```bash
# Run integration tests
cargo test --test '*'

# Run with example database
cargo test --test integration -- --test-db=examples/comprehensive.db
```

### Testing with Real Databases

```bash
# SQLite (default)
cargo test

# PostgreSQL (when implemented)
TEST_POSTGRES_URL=postgresql://user:pass@localhost/testdb cargo test

# MySQL (when implemented)
TEST_MYSQL_URL=mysql://user:pass@localhost/testdb cargo test
```

## Performance

### Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench query_execution

# Generate HTML reports
cargo bench -- --output-format html
```

### Current Performance Metrics

- **Startup Time:** <1s (cold start on macOS M1)
- **Binary Size:** ~15MB (release build, stripped)
- **Memory Usage:** ~80MB idle, ~200MB with large result sets
- **Query Execution Overhead:** <50ms for cached schemas
- **Result Rendering:** 60fps for 100k+ row virtualized tables

### Profiling

```bash
# CPU profiling with cargo-flamegraph
cargo flamegraph --bin zqlz-app

# Memory profiling with valgrind (Linux)
valgrind --tool=massif target/release/zqlz-app

# macOS Instruments
cargo build --release
instruments -t "Time Profiler" target/release/zqlz-app
```

## Contributing

Contributions are welcome! Please see our contributing guidelines:

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the code style guidelines
4. Add tests for new functionality
5. Ensure all tests pass (`cargo test`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to your fork (`git push origin feature/amazing-feature`)
8. Open a Pull Request

### Pull Request Guidelines

- Keep PRs focused on a single feature or fix
- Include tests for new functionality
- Update documentation as needed
- Follow existing code style and conventions
- Write clear commit messages
- Ensure CI passes before requesting review

### Reporting Issues

When reporting bugs, please include:
- ZQLZ version (`zqlz --version`)
- Operating system and version
- Database type and version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs (check `~/Library/Logs/com.yourorg.zqlz/`)

## Roadmap

### Immediate (v0.2.0)
- PostgreSQL driver implementation
- MySQL driver implementation
- CSV/JSON export functionality
- Inline data editing

### Short-term (v0.3.0)
- MiniJinja template mode UI
- MSSQL, MongoDB, Redis drivers
- Stored procedure execution
- Data import functionality

### Medium-term (v0.4.0)
- Visual schema designer
- Schema comparison and sync
- Version control UI integration
- Command palette

### Long-term (v1.0.0)
- SSH tunneling support
- SSL/TLS configuration
- Plugin/extension system
- Cross-platform releases (Windows, Linux)
- Cloud database support

See `PLAN.md` for detailed feature breakdown and implementation status.

## Architecture Decisions

### Why Rust?

- **Performance:** Native compilation with zero-cost abstractions
- **Safety:** Memory safety without garbage collection
- **Concurrency:** Fearless concurrency with ownership system
- **Tooling:** Excellent package management and build system

### Why GPUI?

- **Performance:** GPU-accelerated rendering from day one
- **Native Feel:** Platform-native UI without web technologies
- **Proven:** Powers Zed editor, a production-grade IDE
- **Modern:** Built for 2024+ with async-first design

### Why SQLite First?

- **Simplicity:** No server setup required for testing
- **Ubiquity:** Embedded in countless applications
- **Feature Complete:** Supports advanced SQL features
- **Testing:** Easy to create test databases programmatically

### Zed Editor Integration

ZQLZ integrates Zed's professional editor component for SQL editing:

- **Production-Ready:** Leverage Zed's battle-tested editor infrastructure
- **Feature-Rich:** Get multi-cursor, vim mode, advanced selections out-of-the-box
- **Minimal Adapter:** The `zqlz-zed-adapter` crate provides a thin wrapper around Zed's Editor
- **Clean Architecture:** EditorWrapper, BufferManager, and SettingsBridge isolate Zed-specific code
- **LSP Compatible:** Existing zqlz-lsp integrates seamlessly via diagnostic conversion

See `plans/prd.json` for the complete Zed integration architecture and implementation details.

### Service Layer Pattern

The service layer (`zqlz-services`) provides:
- Clean separation between UI and business logic
- Testable business logic without UI dependencies
- Consistent error handling and logging
- View models tailored for UI consumption
- Easy to swap implementations (e.g., local vs remote)

## Security

### Credential Storage

Connection credentials are encrypted using the `ring` cryptography library:
- AES-256-GCM for encryption
- Random nonces for each encryption
- OS keychain integration (planned)
- Never stored in plain text

### SQL Injection Prevention

- All driver implementations use parameterized queries
- Template mode sanitizes inputs via MiniJinja filters
- SQL parser validates queries before execution
- Schema validation prevents invalid table/column references

### Reporting Security Issues

Please report security vulnerabilities privately to security@yourorg.com. Do not open public issues for security concerns.

## License

This project is dual-licensed under:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

You may choose either license for your use.

## Acknowledgments

- **Zed Editor** - For GPUI framework and architectural inspiration
- **DBeaver** - For feature reference and UX patterns
- **Navicat** - For object management workflow ideas
- **VSCode** - For keyboard shortcut conventions

## Resources

- **Documentation:** [docs/](docs/)
- **Contributing Guide:** [CONTRIBUTING.md](CONTRIBUTING.md) (planned)
- **Code Style Guide:** [CLAUDE.md](CLAUDE.md)
- **Detailed Plan:** [PLAN.md](PLAN.md)
- **Issue Tracker:** GitHub Issues (link when public)

---

**Built with Rust. Powered by GPUI. Designed for developers.**
