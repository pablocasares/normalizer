package io.wizzie.normalizer.builder;

import com.codahale.metrics.jvm.JmxAttributeGauge;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.metrics.MetricsConstant;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.builder.config.ConfigProperties;
import io.wizzie.normalizer.base.utils.Utils;
import io.wizzie.normalizer.exceptions.PlanBuilderException;
import io.wizzie.normalizer.model.PlanModel;
import io.wizzie.normalizer.serializers.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;


public class Builder implements Listener {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);

    public KafkaStreams streams;
    Config config;
    StreamBuilder streamBuilder;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;
    Thread streamMonitor;
    volatile boolean closed = false;

    public Builder(Config config) throws Exception {
        this.config = config;

        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        metricsManager = new MetricsManager(config.getMapConf());
        metricsManager.start();

        Map<String, Object> metricDataBag = config.getOrDefault(
                MetricsConstant.METRIC_DATABAG, new HashMap<>()
        );

        metricDataBag.put("host", Utils.getIdentifier());
        config.put(MetricsConstant.METRIC_DATABAG, metricDataBag);

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(ConfigProperties.BOOTSTRAPPER_CLASSNAME))
                .listener(this)
                .withConfigInstance(config)
                .build();
    }

    public void close() throws Exception {
        if (!closed) {
            log.info("Closing builder.");
            metricsManager.interrupt();
            streamBuilder.close();
            bootstrapper.close();
            if (streams != null && streams.state().isRunning()) streams.close(Duration.ofMinutes(1));
            log.info("Closed builder.");
            closed = true;
        }
    }

    private void registerKafkaMetrics(Config config, MetricsManager metricsManager) {
        Integer streamThreads = config.getOrDefault(NUM_STREAM_THREADS_CONFIG, 1);
        String appId = config.get(APPLICATION_ID_CONFIG);


        log.info("Register kafka jvm metrics: ");

        for (int i = 1; i <= streamThreads; i++) {
            try {

                // PRODUCER
                log.info(" * {}", "producer." + i + ".messages_send_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".messages_send_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-producer"), "record-send-rate"));

                log.info(" * {}", "producer." + i + ".output_bytes_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-producer"), "outgoing-byte-rate"));

                log.info(" * {}", "producer." + i + ".incoming_bytes_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-producer"), "incoming-byte-rate"));

                // CONSUMER
                log.info(" * {}", "consumer." + i + ".max_lag");
                metricsManager.registerMetric("consumer.stream-" + i + ".max_lag",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-consumer"), "records-lag-max"));

                log.info(" * {}", "consumer." + i + ".output_bytes_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-consumer"), "outgoing-byte-rate"));

                log.info(" * {}", "consumer." + i + ".incoming_bytes_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-consumer"), "incoming-byte-rate"));

                log.info(" * {}", "consumer." + i + ".records_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".records_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i + "-consumer"), "records-consumed-rate"));

                // STREAMS
                log.info(" * {}", "streams.stream-" + i + ".process-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".process-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i), "process-latency-avg"));

                log.info(" * {}", "streams.stream-" + i + ".poll-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".poll-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i), "poll-latency-avg"));

                log.info(" * {}", "streams.stream-" + i + ".commit-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".commit-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "normalizer") + "-StreamThread-"
                                + i), "commit-latency-avg"));

            } catch (MalformedObjectNameException e) {
                log.error("kafka jvm metrics not found", e);
            }
        }
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String streamConfig) {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streamMonitor.interrupt();
            streams.close(Duration.ofMinutes(1));
            log.info("Clean Normalizer process");
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            StreamsBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            Config configWithNewAppId = config.clone();
            String appId = configWithNewAppId.get(APPLICATION_ID_CONFIG);
            configWithNewAppId.put(APPLICATION_ID_CONFIG, String.format("%s_%s", appId, "normalizer"));
            configWithNewAppId.put(CLIENT_ID_CONFIG, String.format("%s_%s", appId, "normalizer"));

            Properties properties = configWithNewAppId.getProperties();

            properties.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
            properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), Integer.MAX_VALUE);
            properties.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 305000);
            properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                    Integer.MAX_VALUE);

            properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");

            streams = new KafkaStreams(builder.build(), properties);

            streams.setUncaughtExceptionHandler((thread, exception) -> {
                log.error(exception.getMessage(), exception);

                try {
                    log.info("Stopping normalizer service");
                    close();
                    log.info("Closing normalizer service");
                } catch (Exception e) {
                    log.error(exception.getMessage(), exception);
                }
            });

            streams.cleanUp();
            streams.start();

            registerKafkaMetrics(config, metricsManager);

            streamMonitor = new Thread(new StreamMonitor(this));
            streamMonitor.start();

            log.info("Started Normalizer with conf {}", config.getProperties());
        } catch (PlanBuilderException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
