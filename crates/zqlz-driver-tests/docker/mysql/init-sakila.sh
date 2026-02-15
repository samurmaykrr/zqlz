#!/bin/bash
set -e

echo "Initializing MySQL with Sakila database..."

# Download Sakila schema and data if not exists
if [ ! -f /tmp/sakila-schema.sql ]; then
    echo "Downloading Sakila schema..."
    wget -q -O /tmp/sakila-schema.sql https://raw.githubusercontent.com/jOOQ/sakila/main/mysql-sakila-db/mysql-sakila-schema.sql
fi

if [ ! -f /tmp/sakila-data.sql ]; then
    echo "Downloading Sakila data..."
    wget -q -O /tmp/sakila-data.sql https://raw.githubusercontent.com/jOOQ/sakila/main/mysql-sakila-db/mysql-sakila-insert-data.sql
fi

echo "Loading Sakila schema..."
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < /tmp/sakila-schema.sql

echo "Loading Sakila data..."
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" < /tmp/sakila-data.sql

echo "Sakila database initialized successfully!"

# Verify installation
echo "Verifying installation..."
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) as actor_count FROM actor;"
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) as film_count FROM film;"
