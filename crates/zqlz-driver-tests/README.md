# ZQLZ Database Driver Testing Suite

Comprehensive database driver testing framework using industry-standard Pagila/Sakila benchmark databases with Docker-managed test environments.

## 🎯 Overview

This testing suite ensures **cross-database compatibility** and **driver correctness** by running unified parameterized tests across multiple database drivers:

- **PostgreSQL** (using Pagila database)
- **MySQL** (using Sakila database)
- **SQLite** (using Sakila database)
- **Redis** (for connection/pooling tests)

### Test Coverage

- **750+ planned test cases** across 47 feature categories
- **30 of 47 features complete** (~64% implementation progress)
- **Parameterized tests** using `rstest` - one test definition runs on all drivers
- **Real-world data** from Pagila/Sakila DVD rental store databases
- **Automated Docker** environment management

### Implementation Status

✅ **Completed Features (30/47)**:
- Test infrastructure & fixtures (2 features)
- Connection tests - basic & pooling (2 features)
- CRUD operations - INSERT, SELECT, UPDATE, DELETE, UPSERT (5 features)
- Transaction tests - basic & savepoints (2 features)
- Query tests - JOINs, subqueries, CTEs (3 features)
- Parameter tests - prepared statements (1 feature)
- Data type tests - numeric, string, date/time, boolean/NULL (4 features)
- Schema tests - databases, tables, columns, keys, indexes (6 features)
- Error handling - syntax, constraints, type, connection errors (4 features)
- Cargo configuration (1 feature)

🚧 **In Progress**:
- Documentation improvements
- Additional edge case tests
- Performance benchmarking

📋 **Planned Features (17/47)**:
- SSL/TLS configuration tests
- Transaction isolation levels
- Window functions & set operations
- Named parameters
- JSON & binary data types
- Views introspection
- Edge cases (empty results, large datasets, special characters, boundaries)
- Performance tests
- Redis-specific operations
- Query plan analysis (EXPLAIN)
- CI/CD integration

## 🚀 Quick Start

**Latest Updates (Feb 2026)**:
- ✅ **Automatic Docker Management** - Basic testcontainers-rs integration complete!
  - ⚠️ Automatic mode currently only supports basic empty databases
  - ✅ Use manual mode for full Pagila/Sakila test data (recommended)
- ✅ Connection error tests (Feature 35) - Network failures, auth errors, timeouts
- ✅ Type error handling tests (Feature 34) - Type mismatches across all drivers
- ✅ Constraint violation tests (Feature 33) - Primary keys, foreign keys, NOT NULL, CHECK
- ✅ Schema introspection tests (Features 20-25) - Complete metadata support
- ✅ Advanced query tests - JOINs, subqueries, CTEs
- ✅ Transaction savepoints (Feature 12) - Partial rollback testing

### Prerequisites

- Docker (Docker Desktop or Docker Engine)
- Rust 1.70+

### Running Tests

**Recommended approach** - Uses docker-compose with full Pagila/Sakila data:

```bash
cd crates/zqlz-driver-tests

# Set environment variable to use manual containers
export ZQLZ_TEST_MANUAL_CONTAINERS=1

# Start containers with sample data
./manage-test-env.sh up

# Run all tests
cargo test --all-features

# Stop containers when done
./manage-test-env.sh down
```

**Automatic mode** - Containers start automatically (basic databases only):

```bash
cd crates/zqlz-driver-tests

# Run tests - containers start automatically but without Pagila/Sakila data
# Most tests will fail because they expect sample data
cargo test --lib test_containers

# Run with output
cargo test --lib test_containers -- --nocapture
```

**Note**: Automatic container management is implemented but does not yet include Pagila/Sakila sample data initialization. See `TESTCONTAINERS_STATUS.md` for details and roadmap.

## 🏗️ Architecture

### Automatic Container Management

Tests now use **testcontainers-rs** for automatic Docker lifecycle management:

- **No manual setup required** - Containers start automatically when tests run
- **Lazy initialization** - Containers start on first test execution
- **Container reuse** - Same containers shared across all tests for speed
- **Automatic cleanup** - Containers stop when test process exits
- **Random ports** - Avoids port conflicts with other services

Container lifecycle:
```
Test starts → Container check → Start if needed → Run test → Reuse for next test → Process exits → Auto cleanup
```

For more control, you can still use docker-compose:
```bash
export ZQLZ_TEST_MANUAL_CONTAINERS=1
./manage-test-env.sh up
```

### Test Strategy

Tests use **parameterized testing** with `rstest`:

```rust
#[rstest]
#[case(TestDriver::Postgres)]
#[case(TestDriver::Mysql)]
#[case(TestDriver::Sqlite)]
async fn test_basic_select(#[case] driver: TestDriver) {
    let conn = test_connection(driver).await;
    let result = conn.query("SELECT COUNT(*) FROM actor", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
}
```

One test definition automatically runs across all applicable drivers!

### Test Fixtures & Helpers

The `fixtures` module provides helper functions for common test operations:

```rust
use crate::fixtures::{TestDriver, test_connection};

// Get a connection - containers start automatically!
let conn = test_connection(TestDriver::Postgres).await;

// Drivers available:
// - TestDriver::Postgres (uses Pagila database, random port)
// - TestDriver::Mysql (uses Sakila database, random port)
// - TestDriver::Sqlite (uses temporary file)
// - TestDriver::Redis (random port)
```

### Container Information

**Automatic mode (default)**:
- Containers use **random available ports** to avoid conflicts
- Connection details retrieved automatically from testcontainers
- No configuration needed

**Manual mode** (set `ZQLZ_TEST_MANUAL_CONTAINERS=1`):


**PostgreSQL** (Port 5433):
```
Host: localhost:5433
Database: pagila
User: test_user
Password: test_password
Connection: postgresql://test_user:test_password@localhost:5433/pagila
```

**MySQL** (Port 3307):
```
Host: localhost:3307
Database: sakila
User: test_user
Password: test_password
Connection: mysql://test_user:test_password@localhost:3307/sakila
```

**SQLite**:
```
File: docker/sqlite/sakila.db
```

**Redis** (Port 6380):
```
Host: localhost:6380
Connection: redis://localhost:6380
```

### Sample Databases

#### Pagila (PostgreSQL)
PostgreSQL port of the Sakila database. Features:
- 15 tables with realistic relationships
- Many-to-many relationships
- ~200 actors, ~1000 films, ~16000 rentals
- Views, functions, triggers
- Fulltext search examples

Repository: https://github.com/devrimgunduz/pagila

#### Sakila (MySQL/SQLite)
Original MySQL sample database modeling a DVD rental store. Features:
- Same schema as Pagila
- Normalized design with clear relationships
- Consistent naming conventions
- Generated dataset of reasonable size

Repository: https://github.com/jOOQ/sakila

## 📁 Project Structure

```
crates/zqlz-driver-tests/
├── Cargo.toml                 # Test crate configuration
├── README.md                  # This file
├── manage-test-env.sh         # Environment management script
├── docker/
│   ├── docker-compose.test.yml
│   ├── postgres/
│   │   ├── init-pagila.sh     # PostgreSQL initialization
│   │   └── Dockerfile
│   ├── mysql/
│   │   ├── init-sakila.sh     # MySQL initialization
│   │   └── Dockerfile
│   └── sqlite/
│       ├── init-sakila.sh     # SQLite DB generation
│       └── sakila.db
└── (test modules - no src/ directory)
    ├── zqlz_driver_tests.rs   # Library root with module declarations
    ├── fixtures.rs            # Shared test fixtures & helpers
    ├── connection_tests.rs    # Connection & authentication tests
    ├── select_tests.rs        # SELECT queries & filtering
    ├── insert_tests.rs        # INSERT operations & batching
    ├── update_tests.rs        # UPDATE operations & conditions
    ├── delete_tests.rs        # DELETE operations & cascades
    ├── upsert_tests.rs        # UPSERT/INSERT ON CONFLICT
    ├── transaction_tests.rs   # Transactions, commits, savepoints
    ├── query_tests.rs         # JOINs, subqueries, CTEs, windows
    ├── parameter_tests.rs     # Prepared statements & binding
    ├── pool_tests.rs          # Connection pooling
    ├── datatype_tests.rs      # All SQL data types
    ├── schema_tests.rs        # Schema introspection & metadata
    └── error_tests.rs         # Error handling scenarios
    
    # Planned (not yet implemented):
    # ├── edge_case_tests.rs     # Edge cases & stress tests
    # ├── performance_tests.rs   # Performance benchmarks
    # ├── redis_tests.rs         # Redis-specific tests
    # └── explain_tests.rs       # Query plan analysis
```

## 🧪 Test Categories

### 1. Connection Tests (80 test cases)
- Basic connections
- SSL/TLS configuration
- Connection pooling
- Timeout handling
- Credential validation

### 2. CRUD Tests (141 test cases)
- INSERT (single, batch, with RETURNING)
- SELECT (WHERE, ORDER BY, LIMIT, aggregations)
- UPDATE (single, batch, conditional)
- DELETE (single, batch, cascade)
- UPSERT (driver-specific syntax)

### 3. Transaction Tests (45 test cases)
- BEGIN/COMMIT/ROLLBACK
- Savepoints
- Isolation levels
- Concurrent transactions
- Deadlock detection

### 4. Query Tests (114 test cases)
- JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries
- CTEs (Common Table Expressions)
- Window functions
- Set operations (UNION, INTERSECT, EXCEPT)

### 5. Parameter Tests (30 test cases)
- Positional parameters
- Named parameters
- Type inference
- SQL injection prevention

### 6. Data Type Tests (159 test cases)
- Numeric types
- String types
- Date/time types
- Boolean and NULL
- JSON data
- Binary data (BLOB/BYTEA)

### 7. Schema Tests (110 test cases)
- Database listing
- Table introspection
- Column metadata
- Primary keys
- Foreign keys
- Indexes
- Views

### 8. Error Tests (75 test cases)
- Syntax errors
- Constraint violations
- Type mismatches
- Connection errors

### 9. Edge Case Tests (84 test cases)
- Empty result sets
- Large data sets (10k+ rows)
- Special characters
- Boundary values
- Unicode handling

### 10. Performance Tests (30 test cases)
- Query execution benchmarks
- Concurrent operations
- Connection pool under load

### 11. Redis Tests (13 test cases)
- Key-value operations
- Data structures (lists, sets, hashes)
- Expiry and TTL

### 12. EXPLAIN Tests (18 test cases)
- Query plan analysis
- Index usage detection

## 🛠️ Management Commands

The `manage-test-env.sh` script provides easy environment management:

```bash
# Start all test services
./manage-test-env.sh up

# Stop all test services
./manage-test-env.sh down

# Restart all services
./manage-test-env.sh restart

# View logs from all services
./manage-test-env.sh logs

# Run the full test suite
./manage-test-env.sh test

# Check service status
./manage-test-env.sh status

# Display connection information
./manage-test-env.sh info
```

## 📝 Writing New Tests

### 1. Basic Parameterized Test

Tests that work across all SQL drivers:

```rust
use rstest::rstest;
use crate::fixtures::{TestDriver, test_connection};

#[rstest]
#[case(TestDriver::Postgres)]
#[case(TestDriver::Mysql)]
#[case(TestDriver::Sqlite)]
async fn test_my_new_feature(#[case] driver: TestDriver) {
    // Arrange
    let conn = test_connection(driver).await;
    
    // Act
    let result = conn.query("SELECT COUNT(*) FROM actor", &[]).await;
    
    // Assert
    assert!(result.is_ok());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
}
```

### 2. Driver-Specific Tests

For features only available on certain drivers:

```rust
#[rstest]
#[case(TestDriver::Postgres)]
#[case(TestDriver::Mysql)]
async fn test_ssl_connection(#[case] driver: TestDriver) {
    // Only PostgreSQL and MySQL support SSL configuration
    let mut config = match driver {
        TestDriver::Postgres => ConnectionConfig::new_postgres("localhost", 5433, "pagila", "test_user"),
        TestDriver::Mysql => ConnectionConfig::new_mysql("localhost", 3307, "sakila", "test_user"),
        _ => panic!("SSL not supported on this driver"),
    };
    config.ssl_mode = Some(SslMode::Require);
    // ... test SSL connection
}
```

### 3. Integration Tests (No Docker Required)

Tests that work without Docker containers using SQLite:

```rust
#[tokio::test]
async fn integration_test_basic_query() {
    use zqlz_core::{ConnectionConfig, DatabaseDriver};
    use zqlz_driver_sqlite::SqliteDriver;
    
    let config = ConnectionConfig::new(":memory:", 0, "", "");
    let conn = SqliteDriver::new().connect(&config).await.unwrap();
    
    // Create test table
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", &[]).await.unwrap();
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'Test')", &[]).await.unwrap();
    
    // Query and verify
    let result = conn.query("SELECT * FROM test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
}
```

### 4. Error Handling Tests

Use timeout wrappers to prevent hangs on network failures:

```rust
#[rstest]
#[case(TestDriver::Postgres)]
async fn test_connection_timeout(#[case] driver: TestDriver) {
    let config = ConnectionConfig::new_postgres("192.0.2.1", 5432, "db", "user"); // Unreachable IP
    
    // Wrap with timeout to prevent test from hanging
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        PostgresDriver::new().connect(&config)
    ).await;
    
    // Verify timeout or connection error
    assert!(result.is_err() || result.as_ref().is_ok_and(|r| r.is_err()));
}
```

### 5. Using Sample Data

All tests have access to Pagila/Sakila tables:

**Common Tables:**
- `actor` (actor_id, first_name, last_name)
- `film` (film_id, title, description, release_year, rating)
- `film_actor` (actor_id, film_id) - many-to-many
- `customer`, `rental`, `payment`, etc.

## 🔄 CI/CD Integration

Tests are designed to run in GitHub Actions:

```yaml
name: Database Driver Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        # ... configuration
      mysql:
        image: mysql:8.0
        # ... configuration
      redis:
        image: redis:7
        # ... configuration
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
      - run: cargo test -p zqlz-driver-tests
```

## 🐛 Troubleshooting

### Docker Environment Issues

#### Containers won't start

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :5433  # PostgreSQL
lsof -i :3307  # MySQL
lsof -i :6380  # Redis

# Remove old containers and volumes
docker-compose -f docker/docker-compose.test.yml down -v

# Rebuild from scratch
./manage-test-env.sh down
./manage-test-env.sh up
```

#### Tests fail to connect

```bash
# Verify services are healthy
./manage-test-env.sh status

# Check service logs for errors
./manage-test-env.sh logs

# Restart environment
./manage-test-env.sh restart

# Test individual service connections
docker exec -it zqlz-test-postgres psql -U test_user -d pagila -c "SELECT 1;"
docker exec -it zqlz-test-mysql mysql -u test_user -ptest_password sakila -e "SELECT 1;"
```

#### Sample data not loaded

```bash
# Check initialization logs
docker logs zqlz-test-postgres
docker logs zqlz-test-mysql

# Manually verify data exists
docker exec -it zqlz-test-postgres psql -U test_user -d pagila -c "SELECT COUNT(*) FROM actor;"
docker exec -it zqlz-test-mysql mysql -u test_user -ptest_password sakila -e "SELECT COUNT(*) FROM actor;"

# Expected: 200 actors in both databases
```

### Test Compilation Issues

#### Missing driver dependencies

```bash
# Ensure all driver crates are available
cargo check -p zqlz-driver-postgres
cargo check -p zqlz-driver-mysql
cargo check -p zqlz-driver-sqlite
cargo check -p zqlz-driver-redis

# Update dependencies
cargo update -p zqlz-driver-tests
```

#### Type errors with ConnectionConfig

```rust
// ✅ Correct - Use builder methods
let config = ConnectionConfig::new_postgres("localhost", 5433, "pagila", "test_user");

// ❌ Wrong - ConnectionConfig doesn't implement Default
let config = ConnectionConfig { 
    host: "localhost".to_string(),
    ..Default::default()  // Error: no Default trait
};
```

#### "Use of moved value" errors in nested Results

```rust
// ✅ Correct - Use .as_ref() to avoid consuming Result
let result = driver.connect(&config).await;
assert!(result.is_err() || result.as_ref().is_ok_and(|r| r.is_err()));

// ❌ Wrong - Consumes result in first check
assert!(result.is_err() || (result.is_ok() && result.unwrap().is_err()));
```

### Test Execution Issues

#### Tests hang indefinitely

Some connection tests may hang on unreachable hosts. Use timeouts:

```rust
let result = tokio::time::timeout(
    std::time::Duration::from_secs(10),
    driver.connect(&config)
).await;
```

#### Integration tests vs Docker tests

- **Integration tests** (prefix: `integration_`) use SQLite and work without Docker
- **Parameterized tests** (using `#[rstest]`) require Docker containers

Run only integration tests:
```bash
cargo test -p zqlz-driver-tests --lib integration
```

#### Permission denied on manage-test-env.sh

```bash
chmod +x crates/zqlz-driver-tests/manage-test-env.sh
```

## 📊 Test Results

Test results show:
- ✅ Passed tests with driver info
- ❌ Failed tests with error details
- ⏭️ Skipped tests (driver-specific features)

Example output:
```
test connection_tests::test_basic_connection[Postgres] ... ok
test connection_tests::test_basic_connection[Mysql] ... ok
test connection_tests::test_basic_connection[Sqlite] ... ok
test crud_tests::test_insert_single_row[Postgres] ... ok
test crud_tests::test_insert_single_row[Mysql] ... ok
test crud_tests::test_insert_single_row[Sqlite] ... ok
```

## 🤝 Contributing

When adding new database functionality to ZQLZ, follow these guidelines:

### Adding New Tests

1. **Choose the appropriate test module** based on feature category:
   - Connection behavior → `connection_tests.rs`
   - CRUD operations → `insert_tests.rs`, `select_tests.rs`, `update_tests.rs`, `delete_tests.rs`
   - Complex queries → `query_tests.rs`
   - Transactions → `transaction_tests.rs`
   - Data types → `datatype_tests.rs`
   - Schema metadata → `schema_tests.rs`
   - Error scenarios → `error_tests.rs`
   - Parameters/binding → `parameter_tests.rs`
   - Connection pooling → `pool_tests.rs`

2. **Use parameterized tests** for cross-driver features:
   ```rust
   #[rstest]
   #[case(TestDriver::Postgres)]
   #[case(TestDriver::Mysql)]
   #[case(TestDriver::Sqlite)]
   async fn test_new_feature(#[case] driver: TestDriver) { ... }
   ```

3. **Test against sample databases** - Use existing Pagila/Sakila tables when possible:
   - `actor`, `film`, `customer`, `rental`, `payment`
   - Realistic relationships and data volumes
   - Consistent across all databases

4. **Document driver-specific behavior** in test comments:
   ```rust
   // Note: SQLite doesn't support CREATE INDEX CONCURRENTLY
   if !matches!(driver, TestDriver::Sqlite) {
       conn.execute("CREATE INDEX CONCURRENTLY ...", &[]).await.unwrap();
   }
   ```

5. **Add integration tests** for features that work without Docker:
   ```rust
   #[tokio::test]
   async fn integration_test_feature() {
       // Uses SQLite in-memory, no Docker required
   }
   ```

### Before Submitting PRs

1. **Run the full test suite**:
   ```bash
   ./manage-test-env.sh up
   cargo test -p zqlz-driver-tests --all-features
   ```

2. **Ensure tests pass on all drivers**:
   - PostgreSQL (Pagila database)
   - MySQL (Sakila database)
   - SQLite (Sakila database)
   - Redis (where applicable)

3. **Check for compilation warnings**:
   ```bash
   cargo clippy -p zqlz-driver-tests
   ```

4. **Update documentation** if adding new test categories or changing architecture

5. **Update `plans/prd.json`** and `progress.txt` to track feature completion

### Code Style Guidelines

- Use descriptive test names: `test_insert_with_returning_clause` not `test_insert2`
- Include helpful assertion messages: `assert!(result.is_ok(), "Failed to connect: {:?}", result.err())`
- Group related tests in `mod` blocks within test files
- Add comments explaining non-obvious test logic or driver differences
- Use `#[ignore]` for slow tests (>5 seconds) with comment explaining why

### Testing Best Practices

1. **Isolation**: Each test should be independent and not rely on state from other tests
2. **Cleanup**: Tests should clean up any data they create (or use transactions that rollback)
3. **Timeouts**: Use timeouts for network operations to prevent hanging tests
4. **Error handling**: Test both success and failure paths
5. **Assertions**: Be specific - test exact values, not just "result is ok"

## ❓ FAQ

### General Questions

**Q: Do I need Docker to run any tests?**  
A: No! Integration tests (prefixed with `integration_`) use SQLite in-memory databases and work without Docker. However, most parameterized tests require Docker containers.

**Q: How long does it take to start the test environment?**  
A: First time: ~2-5 minutes (downloading images + loading sample data). Subsequent starts: ~30 seconds.

**Q: Can I run tests for just one driver?**  
A: Yes, but you need to filter by test name pattern. For example:
```bash
cargo test -p zqlz-driver-tests test_basic_connection -- Postgres
```

**Q: How much disk space do the Docker images use?**  
A: Approximately 2GB total for PostgreSQL, MySQL, and Redis images plus sample data.

**Q: Are the sample databases (Pagila/Sakila) modified during tests?**  
A: No, most tests use SELECT queries. Tests that modify data should use transactions and rollback, or create temporary test tables.

### Development Questions

**Q: I added a new test but it's not running. Why?**  
A: Check that:
1. The test module is declared in `zqlz_driver_tests.rs` with `#[cfg(test)] pub mod your_module;`
2. The test function has `#[rstest]` or `#[tokio::test]` attribute
3. The test function is `async` if it uses `await`
4. Docker containers are running (if not an integration test)

**Q: How do I test a feature that only works on one driver?**  
A: Use driver-specific test cases:
```rust
#[rstest]
#[case(TestDriver::Postgres)]  // Only test on PostgreSQL
async fn test_postgres_only_feature(#[case] driver: TestDriver) { ... }
```

**Q: What's the difference between `error_tests.rs` test patterns?**  
A: 
- **Syntax errors**: Malformed SQL that parser rejects
- **Constraint violations**: Valid SQL that violates database constraints (PK, FK, NOT NULL, CHECK)
- **Type errors**: Valid SQL with incompatible types (string vs integer, etc.)
- **Connection errors**: Network failures, auth errors, timeouts

**Q: How do I access the test databases directly?**  
A: Use Docker exec:
```bash
# PostgreSQL
docker exec -it zqlz-test-postgres psql -U test_user -d pagila

# MySQL
docker exec -it zqlz-test-mysql mysql -u test_user -ptest_password sakila

# SQLite
sqlite3 crates/zqlz-driver-tests/docker/sqlite/sakila.db
```

**Q: Can I use a different sample database?**  
A: Yes, but Pagila/Sakila are recommended because:
- Industry-standard benchmark databases
- Consistent schema across PostgreSQL/MySQL/SQLite
- Realistic relationships and data volumes
- Well-documented structure

To use custom data, modify the Docker initialization scripts in `docker/{postgres,mysql,sqlite}/`.

### Debugging Questions

**Q: Why do my connection tests timeout?**  
A: Common causes:
1. Docker containers aren't running (`./manage-test-env.sh status`)
2. Wrong ports (use 5433 for Postgres, 3307 for MySQL, not defaults)
3. Network issues or firewall blocking localhost connections
4. Containers still initializing (wait for health checks)

**Q: Tests pass locally but fail in CI. Why?**  
A: Possible reasons:
1. CI containers use different ports or credentials
2. Sample data not loaded in CI environment
3. Timing issues (CI may be slower, adjust timeouts)
4. Missing feature flags in CI cargo test command

**Q: How do I debug a failing parameterized test?**  
A: Run just one driver case:
```bash
# Run only PostgreSQL variant
cargo test -p zqlz-driver-tests test_name -- Postgres --nocapture

# Or use --exact for exact match
cargo test -p zqlz-driver-tests test_name::Postgres --exact --nocapture
```

## 📚 References

- **Pagila**: https://github.com/devrimgunduz/pagila
- **Sakila**: https://github.com/jOOQ/sakila
- **Testcontainers**: https://github.com/testcontainers/testcontainers-rs
- **Rstest**: https://github.com/la10736/rstest

## 📄 License

Same as parent project.
