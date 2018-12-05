package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class MinValueMapper extends MapperFunction {

    String dimension;
    String newDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) checkNotNull(properties.get("dimension"), "dimension cannot be null");
        newDimension = (String) properties.getOrDefault("min_dimension_name", "min_value");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if(value != null) {
            List arrayValue = (List<Number>) value.get(dimension);

            if(arrayValue != null)
                value.put(newDimension, Collections.min(arrayValue));

        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
