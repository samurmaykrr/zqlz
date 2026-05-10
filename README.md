# ZQLZ

A database IDE built with Rust and GPUI.

![App](./ss.png)

## Current Status

**Currently Supported:** SQLite, Postgresql, MySQL and redis (in development)

This is an early-stage project. Only SQLite is fully functional. PostgreSQL and MySQL and redis support is in development.

## Model

ZQLZ is free for local database work. The app supports unlimited local connections and does not require an account for local workflows.

Optional hosted sync is account-based and funds development. Sync is planned as a flat $10/month per user service for encrypted backup and cross-device sync of connections, settings, snippets, and related convenience data. Users may pay more if they want to support the project.

The hosted sync backend, ZQLZ brand assets, official release infrastructure, and hosted service access are separate from the GPL client license.

## Running

### Prerequisites

- Rust 1.75 or later
- macOS 12.0+, Windows, or Linux (macOS remains the primary and most routinely validated platform)

### Development

```bash
# GUI app
cargo run -p zqlz-app

# CLI (also acts as launcher when no subcommand is given)
cargo run -p zqlz-cli -- --help
```

### Release Build

```bash
cargo build --release

# GUI executable
./target/release/zqlz-editor

# CLI executable
./target/release/zqlz --help
```

## License

GPL-3.0-or-later
