FROM apache/hive:3.1.3

USER root
COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
