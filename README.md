# ZQLZ

A database IDE built with Rust and GPUI.

![App](./ss.png)

## Current Status

**Currently Supported:** SQLite, Postgresql, MySQL and redis (in development)

This is an early-stage project. Only SQLite is fully functional. PostgreSQL and MySQL and redis support is in development.

## Running

### Prerequisites

- Rust 1.75 or later
- macOS 12.0+ (currently only macOS is supported but you can build for other platforms with some adjustments)

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
