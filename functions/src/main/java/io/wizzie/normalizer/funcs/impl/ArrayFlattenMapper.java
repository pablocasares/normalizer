package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FlatMapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArrayFlattenMapper extends FlatMapperFunction {

    String flatDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        flatDimension = (String) properties.get("flatDimension");
    }

    @Override
    public Iterable<KeyValue<String, Map<String, Object>>> process(String key, Map<String, Object> value) {
        List<KeyValue<String, Map<String, Object>>> results = Collections.singletonList(new KeyValue<>(key, value));

        if (value != null && flatDimension != null) {

            if (value.containsKey(flatDimension)) {
                List<Object> array = (List<Object>) value.remove(flatDimension);

                if (array != null) {
                    results = array.stream().map(val -> {

                        Map<String, Object> newValue = new HashMap<>();
                        newValue.putAll(value);

                        if (val instanceof Map) {
                            newValue.putAll((Map<String, Object>) val);
                        } else {
                            newValue.put(flatDimension, val);
                        }

                        KeyValue<String, Map<String, Object>> kv = new KeyValue<>(key, newValue);

                        return kv;

                    }).collect(Collectors.toList());
                }

            }
        }
        return results;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" {")
                .append("flatDimension: ").append(flatDimension).append("} ");

        return builder.toString();
    }

    @Override
    public void stop() {

    }
}
