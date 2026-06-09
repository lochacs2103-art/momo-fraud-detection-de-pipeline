#!/bin/bash
# Download JDBC drivers vào docker/spark/jars/
# Chạy 1 lần trước khi docker compose up
#
# Tại sao cần JDBC jar?
# Spark chạy trên JVM. Để connect PostgreSQL, JVM cần PostgreSQL JDBC driver (file .jar)
# Nếu không có jar này → java.lang.ClassNotFoundException: org.postgresql.Driver

set -e
JARS_DIR="$(dirname "$0")/jars"
mkdir -p "$JARS_DIR"

PG_JDBC_VERSION="42.7.1"
PG_JDBC_URL="https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar"
PG_JDBC_JAR="$JARS_DIR/postgresql-${PG_JDBC_VERSION}.jar"

if [ -f "$PG_JDBC_JAR" ]; then
    echo "PostgreSQL JDBC driver already exists: $PG_JDBC_JAR"
else
    echo "Downloading PostgreSQL JDBC driver ${PG_JDBC_VERSION}..."
    curl -L -o "$PG_JDBC_JAR" "$PG_JDBC_URL"
    echo "Downloaded: $PG_JDBC_JAR"
fi

echo "Done. Jars in $JARS_DIR:"
ls -lh "$JARS_DIR"
