#!/bin/bash
set -e

echo "Initializing SQLite with Sakila database..."

SQLITE_DB="/tmp/sakila.db"

# Download Sakila schema and data if not exists
if [ ! -f /tmp/sakila-schema.sql ]; then
    echo "Downloading Sakila schema..."
    wget -q -O /tmp/sakila-schema.sql https://raw.githubusercontent.com/jOOQ/sakila/main/sqlite-sakila-db/sqlite-sakila-schema.sql
fi

if [ ! -f /tmp/sakila-data.sql ]; then
    echo "Downloading Sakila data..."
    wget -q -O /tmp/sakila-data.sql https://raw.githubusercontent.com/jOOQ/sakila/main/sqlite-sakila-db/sqlite-sakila-insert-data.sql
fi

echo "Creating SQLite database..."
rm -f "$SQLITE_DB"

echo "Loading Sakila schema..."
sqlite3 "$SQLITE_DB" < /tmp/sakila-schema.sql

echo "Loading Sakila data..."
sqlite3 "$SQLITE_DB" < /tmp/sakila-data.sql

echo "Sakila database initialized successfully!"

# Verify installation
echo "Verifying installation..."
sqlite3 "$SQLITE_DB" "SELECT COUNT(*) as actor_count FROM actor;"
sqlite3 "$SQLITE_DB" "SELECT COUNT(*) as film_count FROM film;"

echo "SQLite database created at: $SQLITE_DB"
