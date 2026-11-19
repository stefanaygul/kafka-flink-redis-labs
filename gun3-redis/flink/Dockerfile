# Flink 1.20.5 (Java 17) + Kafka SQL connector.
# Connector Flink dağıtımında yok ve Flink sürümüne bağlı: 1.20.x → 3.x-1.20.
# Dosya Maven Central'dan indirilir ve sha256 ile doğrulanır.
FROM flink:1.20.5-scala_2.12-java17

ADD --checksum=sha256:b860724fd97d7da0901bfc3859680442f4c1aa2b35ab944407558b393a1cf93e --chown=flink:flink \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar

RUN mkdir -p /flink-data/checkpoints /flink-data/savepoints && chown -R flink:flink /flink-data
