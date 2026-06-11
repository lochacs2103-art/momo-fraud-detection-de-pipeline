FROM apache/spark:3.5.1

USER root

# Install Python dependencies vào image luôn
# Không cần pip install mỗi lần chạy
RUN pip install --no-cache-dir \
    pyyaml==6.0.1 \
    structlog==23.2.0 \
    pandas==1.5.3 \
    pyarrow==11.0.0

# Copy Spark config
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Copy Hadoop configs (needed for HDFS connectivity)
COPY hadoop-configs/core-site.xml /opt/spark/conf/core-site.xml
COPY hadoop-configs/hdfs-site.xml /opt/spark/conf/hdfs-site.xml

# Create dir for JDBC jars
RUN mkdir -p /opt/spark/extra-jars
