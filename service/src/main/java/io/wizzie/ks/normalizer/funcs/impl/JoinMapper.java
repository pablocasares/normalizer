package io.wizzie.ks.normalizer.funcs.impl;

import io.wizzie.ks.normalizer.funcs.MapperFunction;
import io.wizzie.ks.normalizer.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class JoinMapper extends MapperFunction {

    public static final String DIMENSION_NAME = "dimensionName";
    public static final String OR_DEFAULT = "orDefault";
    public static final String FROM_DIMENSION = "fromDimension";
    public static final String VALUES = "values";
    public static final String DELIMITIER = "delimitier";
    public static final String DELETE = "delete";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";

    String delimitier;
    List<Map<String, Object>> dimensionsToJoin;
    String newDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        newDimension = (String) checkNotNull(properties.get(DIMENSION_NAME), String.format(ERROR_MESSAGE_PATTERN, DIMENSION_NAME));
        dimensionsToJoin = (List<Map<String, Object>>) properties.getOrDefault(VALUES, new ArrayList<>());

        dimensionsToJoin.forEach(map -> {
            checkNotNull(map.get(FROM_DIMENSION), String.format(ERROR_MESSAGE_PATTERN, FROM_DIMENSION));
            checkNotNull(map.get(OR_DEFAULT), String.format(ERROR_MESSAGE_PATTERN, OR_DEFAULT));
        });

        delimitier = (String) properties.getOrDefault(DELIMITIER, "-");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        KeyValue<String, Map<String, Object>> returnedValue = new KeyValue<>(key, new HashMap<>());

        if (value != null && newDimension != null) {

            String joined = dimensionsToJoin.stream()
                    .map(m ->
                    {
                        Object objectValue = (boolean) m.getOrDefault(DELETE, false) ? value.remove(m.get(FROM_DIMENSION)) : value.get(m.get(FROM_DIMENSION));
                        return (objectValue != null ? objectValue : m.get(OR_DEFAULT)).toString();
                    })
                    .collect(Collectors.joining(delimitier));

            value.put(newDimension, joined);
            returnedValue = new KeyValue<>(key, value);
        }

        return returnedValue;
    }

    @Override
    public void stop() {

    }
}