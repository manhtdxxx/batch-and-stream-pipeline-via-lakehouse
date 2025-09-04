#!/bin/bash
set -e

echo ">>> Waiting for PostgreSQL..."
until nc -z metastore-db 5432; do
  sleep 1
done

HIVE_VERSION=3.1.2
HIVE_HOME=/opt/apache-hive-$HIVE_VERSION-bin

echo ">>> Initializing Hive schema..."
if $HIVE_HOME/bin/schematool -dbType postgres -info --verbose; then
  echo ">>> Schema exists, skipping initSchema"
else
  echo ">>> Schema not found, initializing..."
  $HIVE_HOME/bin/schematool -dbType postgres -initSchema --verbose
  echo ">>> Schema initialization completed"
fi

echo ">>> Starting Hive Metastore service..."
exec $HIVE_HOME/bin/hive --service metastore