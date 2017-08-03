package io.wizzie.ks.normalizer.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.normalizer.exceptions.PlanBuilderException;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import io.wizzie.ks.normalizer.model.PlanModel;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.wizzie.ks.normalizer.builder.config.ConfigProperties.BOOTSTRAPER_CLASSNAME;


public class Builder implements Listener{
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;

    public Builder(Config config) throws Exception {
        this.config = config;
        metricsManager = new MetricsManager(config.clone());
        metricsManager.start();

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(BOOTSTRAPER_CLASSNAME))
                .listener(this)
                .withConfigInstance(config)
                .build();
    }

    public void close() throws Exception {
        metricsManager.interrupt();
        streamBuilder.close();
        bootstrapper.close();
        if (streams != null) streams.close();
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String streamConfig) {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streams.close();
            log.info("Clean Normalizer process");
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            KStreamBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            streams = new KafkaStreams(builder, config.getProperties());

            streams.setUncaughtExceptionHandler((thread, exception) -> log.error(exception.getMessage(), exception));
            streams.start();

            log.info("Started Normalizer with conf {}", config.getProperties());
        } catch (PlanBuilderException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
