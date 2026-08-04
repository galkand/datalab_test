FROM apache/hive:3.1.3

USER root

# PostgreSQL JDBC-driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql.jar
RUN chmod 0644 /opt/hive/lib/postgresql.jar

# Wrapper entrypoint: prepares local scratch directories and then delegates to /entrypoint.sh
COPY docker-entrypoint-wrapper.sh /docker-entrypoint-wrapper.sh
RUN chmod +x /docker-entrypoint-wrapper.sh \
 && chown hive:hive /docker-entrypoint-wrapper.sh

# Removing beeline warning: ensure HOME exists for user hive
RUN mkdir -p /home/hive/.beeline \
 && chown -R hive:hive /home/hive \
 && chmod 0755 /home/hive

ENV HOME=/home/hive


USER hive

ENTRYPOINT ["/docker-entrypoint-wrapper.sh"]
