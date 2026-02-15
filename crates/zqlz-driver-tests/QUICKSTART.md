# Quick Reference: Running ZQLZ Driver Tests

## TL;DR

**Recommended way (with full Pagila/Sakila data):**
```bash
cd crates/zqlz-driver-tests
export ZQLZ_TEST_MANUAL_CONTAINERS=1
./manage-test-env.sh up
cargo test --all-features
./manage-test-env.sh down
```

**Quick way (automatic containers, no sample data):**
```bash
cd crates/zqlz-driver-tests
cargo test --lib test_containers
```

## Commands

### Start Test Environment

```bash
# Manual mode (docker-compose with Pagila/Sakila)
./manage-test-env.sh up
```

### Stop Test Environment

```bash
./manage-test-env.sh down
```

### Run Tests

```bash
# All tests (requires manual containers)
export ZQLZ_TEST_MANUAL_CONTAINERS=1
cargo test --all-features

# Specific test module
cargo test connection_tests

# Specific test case
cargo test test_connect_with_valid_credentials

# With output
cargo test -- --nocapture

# Container management tests only (works without manual setup)
cargo test --lib test_containers
```

### Check Status

```bash
./manage-test-env.sh status
```

### View Logs

```bash
./manage-test-env.sh logs
```

### Connection Info

```bash
./manage-test-env.sh info
```

## Environment Variables

- `ZQLZ_TEST_MANUAL_CONTAINERS=1` - Use docker-compose managed containers
- `ZQLZ_TEST_MANUAL_CONTAINERS=0` - Use automatic testcontainers (default)

## Common Issues

### Tests fail with "connection refused"

**Solution**: Start manual containers
```bash
export ZQLZ_TEST_MANUAL_CONTAINERS=1
./manage-test-env.sh up
```

### Tests fail with "table does not exist"

**Cause**: Automatic mode doesn't have Pagila/Sakila data yet

**Solution**: Use manual mode (see above)

### Port already in use

**Solution**: Stop conflicting services
```bash
# Check what's using the ports
lsof -i :5433  # PostgreSQL
lsof -i :3307  # MySQL
lsof -i :6380  # Redis

# Stop old containers
./manage-test-env.sh down
```

### Docker not running

**Solution**: Start Docker Desktop or Docker daemon

## File Locations

- Test crate: `crates/zqlz-driver-tests/`
- Docker config: `crates/zqlz-driver-tests/docker/docker-compose.test.yml`
- Management script: `crates/zqlz-driver-tests/manage-test-env.sh`
- Test modules: `crates/zqlz-driver-tests/*.rs`

## For More Information

- Full documentation: `crates/zqlz-driver-tests/README.md`
- Implementation details: `crates/zqlz-driver-tests/TESTCONTAINERS_IMPLEMENTATION.md`
- Current status: `crates/zqlz-driver-tests/TESTCONTAINERS_STATUS.md`
