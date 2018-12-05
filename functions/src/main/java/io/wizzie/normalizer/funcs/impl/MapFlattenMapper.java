package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapFlattenMapper extends MapperFunction {

    String flatDimension;
    String keyDimension;
    String outputDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        flatDimension = (String) properties.get("flat_dimension");
        keyDimension = (String) properties.get("key_dimension");
        outputDimension = (String) properties.get("output_dimension");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        // DATA: {"A":{"dim":"AA"}, "C":{"dim":"BB"}}
        // OUT: [{key:A, dim: AAA},{key:C, dim: BB}]

        if (value != null && flatDimension != null) {
            Map<String, Object> newValue = new HashMap<>(value);

            if (value.containsKey(flatDimension)) {
                Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) newValue.remove(flatDimension);

                if (map != null) {
                    List<Map<String, Object>> results = map.entrySet().stream().map(val -> {

                        Map<String, Object> generatedMessage = new HashMap<>();
                        generatedMessage.putAll(val.getValue());
                        generatedMessage.put(keyDimension, val.getKey());

                        return generatedMessage;

                    }).collect(Collectors.toList());

                    newValue.put(outputDimension, results);
                }

            }
            return new KeyValue<>(key, newValue);
        } else {
            return new KeyValue<>(key, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" {")
                .append("flat_dimension: ").append(flatDimension).append("} ");

        return builder.toString();
    }

    @Override
    public void stop() {

    }
}
