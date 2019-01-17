FROM openjdk:8-jre-slim

MAINTAINER devel@wizzie.io

RUN apt-get update -y && apt-get install -y gettext-base && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/normalizer

ADD normalizer-dist.tar.gz /opt/
RUN rm -f /opt/normalizer/config/*

ADD log4j2_env.xml /opt/normalizer/config/
ADD config_env.json /opt/normalizer/config/
ADD normalizer-start.sh /bin

ENV METRIC_ENABLE=true \
    METRIC_INTERVAL=60000 \
    MULTI_ID=false \
    NUM_STREAM_THREADS=1 \
    MAX_OUTPUT_KAFKA_TOPICS=500 \
    BOOTSTRAP_CLASSNAME=io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper \
    BOOTSTRAP_KAFKA_TOPICS=[\"__normalizer_bootstrap\"] \
    METRIC_KAFKA_TOPIC=__metrics \
    LOG_LEVEL=info \
    GENERAL_LOG_LEVEL=info \
    JVM_OPTIONS="-Djdk.nio.maxCachedBufferSize=262144 -Xmx30m -Xms30m -XX:MaxDirectMemorySize=15m -XX:MaxMetaspaceSize=30m -XX:SurvivorRatio=6 -Xss512k -XX:ReservedCodeCacheSize=15m -XX:NewSize=15m" \
    MALLOC_ARENA_MAX=1 \
    DEFAULT_TIMESTAMP_EXTRACTOR=org.apache.kafka.streams.processor.WallclockTimestampExtractor \
    PRODUCER_BUFFER_MEMORY=102400 \
    CONSUMER_FETCH_MAX_BYTES=102400 \
    CONSUMER_MAX_POLL_RECORDS=50 \
    CONSUMER_FETCH_MAX_WAIT_MS=500 \
    CONSUMER_SEND_BUFFER_BYTES=131072 \
    CONSUMER_RECEIVE_BUFFER_BYTES=65536 \
    PRODUCER_SEND_BUFFER_BYTES=131072 \
    PRODUCER_RECEIVE_BUFFER_BYTES=32768 \
    BUFFERED_RECORDS_PER_PARTITION=500




CMD exec /bin/normalizer-start.sh


