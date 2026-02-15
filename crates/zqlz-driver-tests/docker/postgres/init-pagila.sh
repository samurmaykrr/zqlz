#!/bin/bash
set -e

echo "Initializing PostgreSQL with Pagila database..."

# Download Pagila schema and data if not exists
if [ ! -f /tmp/pagila-schema.sql ]; then
    echo "Downloading Pagila schema..."
    wget -q -O /tmp/pagila-schema.sql https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql
fi

if [ ! -f /tmp/pagila-data.sql ]; then
    echo "Downloading Pagila data..."
    wget -q -O /tmp/pagila-data.sql https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-insert-data.sql
fi

echo "Loading Pagila schema..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/pagila-schema.sql

echo "Loading Pagila data..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/pagila-data.sql

echo "Pagila database initialized successfully!"

# Verify installation
echo "Verifying installation..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) as actor_count FROM actor;"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) as film_count FROM film;"
