#!/bin/bash
# Custom entrypoint cho hive-metastore
# Kiểm tra schema đã tồn tại chưa → nếu có thì SKIP init
# Tránh lỗi "relation already exists" khi restart container

set -e

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS=" -Xmx1G \
  -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://hive-metastore-db:5432/metastore \
  -Djavax.jdo.option.ConnectionUserName=hive \
  -Djavax.jdo.option.ConnectionPassword=hive"

echo "Checking if Hive schema already exists..."

# Dùng schematool -validate để check, nếu pass thì schema đã có
if /opt/hive/bin/schematool -dbType postgres -validate > /dev/null 2>&1; then
    echo "Schema already exists, skipping initialization."
else
    echo "Schema not found, initializing..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
    echo "Schema initialized successfully."
fi

echo "Starting Hive Metastore Server..."
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
