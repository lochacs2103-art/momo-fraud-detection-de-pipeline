FROM trinodb/trino:435

# Copy configs vào image — tránh WSL2 bind mount issues
COPY config.properties /etc/trino/config.properties
COPY catalog/hive.properties /etc/trino/catalog/hive.properties

# Copy HDFS configs
COPY hdfs/core-site.xml /etc/trino/hdfs/core-site.xml
COPY hdfs/hdfs-site.xml /etc/trino/hdfs/hdfs-site.xml
