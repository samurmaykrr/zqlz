# ZQLZ

A database IDE built with Rust and GPUI.

![App](./ss.png)

## Current Status

**Currently Supported:** SQLite, Postgresql, MySQL and redis (in development)

This is an early-stage project. Only SQLite is fully functional. PostgreSQL and MySQL and redis support is in development.

## Running

### Prerequisites

- Rust 1.75 or later
- macOS 12.0+, Windows, or Linux (macOS remains the primary and most routinely validated platform)

### Development

```bash
cargo run # runs the default feature flags
```

### Release Build

```bash
cargo build --release
./target/release/zqlz-app
```

## License

GPL
