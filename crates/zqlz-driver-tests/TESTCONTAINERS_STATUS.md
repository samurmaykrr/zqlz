# Automatic Docker Container Management - Implementation Notes

## Status

✅ **Completed**: Basic testcontainers-rs integration
⚠️ **Partial**: Container startup and health checking
🔄 **In Progress**: Sample database initialization (Pagila/Sakila)

## What Works

The automatic Docker container management using testcontainers-rs is now functional:

- Containers start automatically when tests run (no manual `./manage-test-env.sh up` needed)
- Container lifecycle is managed automatically (cleanup on test exit)
- Thread-safe container initialization and reuse across tests
- Proper mutex handling to avoid `Send` issues with tokio

## Current Limitations

### 1. Sample Database Initialization

The automatic containers do NOT yet include Pagila/Sakila sample data. The testcontainers create basic empty databases:
- PostgreSQL: `test` database with `postgres` user
- MySQL: `test` database with `root` user  
- Redis: Empty instance

To use the full Pagila/Sakila test data, you must currently use manual container management:

```bash
export ZQLZ_TEST_MANUAL_CONTAINERS=1
./manage-test-env.sh up
cargo test --all-features
```

### 2. Container Health Checks

The containers start but may not be immediately ready to accept connections. MySQL in particular needs time to initialize. Tests may fail with connection errors on first run.

## Recommended Usage

**For full test suite** (recommended):
```bash
export ZQLZ_TEST_MANUAL_CONTAINERS=1
./manage-test-env.sh up
cargo test -p zqlz-driver-tests --all-features
./manage-test-env.sh down
```

**For basic integration tests** (automatic mode):
```bash
# Tests that don't require Pagila/Sakila data
cargo test -p zqlz-driver-tests --lib test_containers
cargo test -p zqlz-driver-tests --lib fixtures::tests
```

## Future Enhancements

To make automatic mode fully functional, we need to:

1. **Add health check waiting**: Use testcontainers `WaitFor` strategies to ensure containers are ready
2. **Initialize sample data**: Create custom Docker images or use init scripts to load Pagila/Sakila
3. **Handle schema creation**: Automatically apply SQL schemas when containers start
4. **Add retry logic**: Implement connection retries with exponential backoff

Example of what's needed:

```rust
// Future implementation
let image = Postgres::default()
    .with_init_script("pagila-schema.sql")
    .with_init_script("pagila-data.sql")
    .wait_for(WaitFor::message_on_stderr("database system is ready"));
```

## Testing the Implementation

The basic container management works:

```bash
# Test that containers can start
cargo test -p zqlz-driver-tests --lib test_containers::tests -- --nocapture

# Test container reuse
cargo test -p zqlz-driver-tests --lib test_containers::tests::test_container_reuse

# Test parallel startup
cargo test -p zqlz-driver-tests --lib test_containers::tests::test_init_all_containers
```

All of these tests should pass and demonstrate that Docker containers can be managed automatically from Rust.

## Migration Path

1. **Phase 1** (Current): Automatic containers for simple tests, manual for full suite
2. **Phase 2**: Add custom images with Pagila/Sakila pre-loaded
3. **Phase 3**: Add health checks and wait strategies
4. **Phase 4**: Make automatic mode the default

## Technical Details

### Send-Safety Fix

The original implementation had issues with `MutexGuard` not being `Send`. This was fixed by scoping the guard acquisition:

```rust
// ❌ Wrong - guard held across await
let mut guard = CONTAINER.lock()?;
if let Some(ref container) = *guard {
    return Ok(container.info.clone());
}
let container = image.start().await?;  // Error: MutexGuard not Send

// ✅ Correct - guard dropped before await
{
    let guard = CONTAINER.lock()?;
    if let Some(ref container) = *guard {
        return Ok(container.info.clone());
    }
}  // guard dropped here
let container = image.start().await?;  // OK
{
    let mut guard = CONTAINER.lock()?;
    *guard = Some(container);
}
```

### Environment Variable Control

Tests check `ZQLZ_TEST_MANUAL_CONTAINERS` to decide between automatic and manual containers:

```rust
fn use_manual_containers() -> bool {
    env::var("ZQLZ_TEST_MANUAL_CONTAINERS")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .map(|v| v != 0)
        .unwrap_or(false)
}
```

This allows gradual migration and flexibility during development.
