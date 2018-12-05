package io.wizzie.normalizer.funcs.impl.debug;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogMapper extends MapperFunction {

   private final Logger log = LoggerFactory.getLogger(getClass());


    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        log.info(String.format("KEY: %s - VALUE: %s", key, value));
        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {
    }
}
