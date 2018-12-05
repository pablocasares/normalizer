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
    JVM_OPTIONS="-Xmx512m -Xms512m"

CMD exec /bin/normalizer-start.sh


