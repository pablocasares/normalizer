FROM java:openjdk-8-jdk

MAINTAINER devel@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base
RUN mkdir -p /var/log/normalizer
RUN export MALLOC_ARENA_MAX=2


ADD normalizer-dist.tar.gz /opt/
RUN rm -f /opt/normalizer/config/*

ADD log4j2_env.xml /opt/normalizer/config/
ADD config_env.json /opt/normalizer/config/
ADD normalizer-start.sh /bin

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV MULTI_ID false
ENV NUM_STREAM_THREADS 1
ENV MAX_OUTPUT_KAFKA_TOPICS 500
ENV BOOTSTRAP_KAFKA_TOPICS [\"__normalizer_bootstrap\"]
ENV METRIC_KAFKA_TOPIC __metrics

ENV LOG_LEVEL info
ENV GENERAL_LOG_LEVEL info

ENV JVM_OPTIONS -Xmx512m -Xms512m

ENV MALLOC_ARENA_MAX 1
ENV M_MMAP_THRESHOLD 64

CMD exec /bin/normalizer-start.sh


