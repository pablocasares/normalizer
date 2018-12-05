package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class UpperCaseStringMapper extends MapperFunction {

    String dimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) checkNotNull(properties.get("dimension"), "dimension cannot be null");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if(value != null) {
            String string = (String) value.get(dimension);

            if(string != null)
                value.put(dimension, string.toUpperCase());
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
