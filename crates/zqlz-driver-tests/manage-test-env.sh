#!/bin/bash
# Database Driver Test Environment Manager
# Usage: ./manage-test-env.sh [up|down|restart|logs|test]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.test.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    log_success "Docker is available"
}

start_services() {
    log_info "Starting test database services..."
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose.test.yml up -d
    
    log_info "Waiting for services to be healthy..."
    
    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL..."
    for i in {1..30}; do
        if docker exec zqlz-test-postgres pg_isready -U test_user -d pagila &> /dev/null; then
            log_success "PostgreSQL is ready"
            break
        fi
        sleep 1
    done
    
    # Wait for MySQL
    log_info "Waiting for MySQL..."
    for i in {1..30}; do
        if docker exec zqlz-test-mysql mysqladmin ping -h localhost -u test_user -ptest_password &> /dev/null 2>&1; then
            log_success "MySQL is ready"
            break
        fi
        sleep 1
    done

    # mysql:8 initialization uses a temporary server that can report healthy,
    # then restarts once init scripts complete. Wait for the Sakila import marker.
    log_info "Waiting for MySQL init scripts to complete..."
    for i in {1..240}; do
        if docker exec zqlz-test-mysql mysql -h 127.0.0.1 -u test_user -ptest_password -D sakila -e "SELECT COUNT(*) FROM actor" &> /dev/null 2>&1; then
            log_success "MySQL Sakila data already available"
            break
        fi

        if docker logs zqlz-test-mysql 2>&1 | grep -q "Sakila database initialized successfully!"; then
            log_success "MySQL Sakila import completed"
            break
        fi

        if [ "$i" -eq 240 ]; then
            log_error "MySQL did not finish Sakila initialization in time"
            docker logs --tail 120 zqlz-test-mysql
            exit 1
        fi
        sleep 1
    done

    # Wait for final MySQL server restart to bind the external port.
    log_info "Waiting for final MySQL server startup on port 3306..."
    for i in {1..120}; do
        if docker logs zqlz-test-mysql 2>&1 | grep -q "port: 3306"; then
            log_success "MySQL final server is running on port 3306"
            break
        fi

        if [ "$i" -eq 120 ]; then
            log_error "MySQL final server did not start on port 3306 in time"
            docker logs --tail 120 zqlz-test-mysql
            exit 1
        fi
        sleep 1
    done
    
    # Wait for Redis
    log_info "Waiting for Redis..."
    for i in {1..30}; do
        if docker exec zqlz-test-redis redis-cli ping &> /dev/null; then
            log_success "Redis is ready"
            break
        fi
        sleep 1
    done
    
    log_success "All test services are running and healthy!"
    echo ""
    show_connection_info
}

stop_services() {
    log_info "Stopping test database services..."
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose.test.yml down
    log_success "Test services stopped"
}

restart_services() {
    stop_services
    start_services
}

show_logs() {
    log_info "Showing logs from test services..."
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose.test.yml logs -f
}

show_connection_info() {
    echo -e "${GREEN}=== Test Database Connection Information ===${NC}"
    echo ""
    echo -e "${BLUE}PostgreSQL (Pagila):${NC}"
    echo "  Host: localhost"
    echo "  Port: 5433"
    echo "  Database: pagila"
    echo "  User: test_user"
    echo "  Password: test_password"
    echo "  Connection String: postgresql://test_user:test_password@localhost:5433/pagila"
    echo ""
    echo -e "${BLUE}MySQL (Sakila):${NC}"
    echo "  Host: localhost"
    echo "  Port: 3307"
    echo "  Database: sakila"
    echo "  User: test_user"
    echo "  Password: test_password"
    echo "  Connection String: mysql://test_user:test_password@localhost:3307/sakila"
    echo ""
    echo -e "${BLUE}Redis:${NC}"
    echo "  Host: localhost"
    echo "  Port: 6380"
    echo "  Connection String: redis://localhost:6380"
    echo ""
    echo -e "${BLUE}SQLite:${NC}"
    echo "  File: crates/zqlz-driver-tests/docker/sqlite/sakila.db"
    echo ""
}

run_tests() {
    log_info "Running database driver tests..."
    cd "$SCRIPT_DIR/.."
    cargo test -p zqlz-driver-tests --all-features -- --nocapture
}

show_status() {
    log_info "Checking service status..."
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose.test.yml ps
}

case "${1:-}" in
    up|start)
        check_docker
        start_services
        ;;
    down|stop)
        stop_services
        ;;
    restart)
        check_docker
        restart_services
        ;;
    logs)
        show_logs
        ;;
    test)
        run_tests
        ;;
    status)
        show_status
        ;;
    info)
        show_connection_info
        ;;
    *)
        echo "Database Driver Test Environment Manager"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  up, start     - Start all test database services"
        echo "  down, stop    - Stop all test database services"
        echo "  restart       - Restart all test database services"
        echo "  logs          - Show logs from all services"
        echo "  test          - Run the test suite"
        echo "  status        - Show service status"
        echo "  info          - Show connection information"
        echo ""
        exit 1
        ;;
esac
