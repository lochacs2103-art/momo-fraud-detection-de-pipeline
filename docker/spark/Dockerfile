FROM apache/spark:3.5.1

USER root

# Copy Spark config
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Copy Hadoop configs (needed for HDFS connectivity)
# These are copied into spark folder during build via copy-hadoop-configs target
COPY hadoop-configs/core-site.xml /opt/spark/conf/core-site.xml
COPY hadoop-configs/hdfs-site.xml /opt/spark/conf/hdfs-site.xml

# Create dir for JDBC jars
RUN mkdir -p /opt/spark/extra-jars
