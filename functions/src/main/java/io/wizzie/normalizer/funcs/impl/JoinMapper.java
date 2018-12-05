package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.*;
import java.util.stream.Collectors;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class JoinMapper extends MapperFunction {

    public static final String DIMENSION_NAME = "dimensionName";
    public static final String OR_DEFAULT = "orDefault";
    public static final String FROM_DIMENSION = "fromDimension";
    public static final String VALUES = "values";
    public static final String DELIMITER = "delimiter";
    public static final String DELETE = "delete";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";

    String delimiter;
    List<Map<String, Object>> dimensionsToJoin;
    String newDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        newDimension = (String) checkNotNull(properties.get(DIMENSION_NAME), String.format(ERROR_MESSAGE_PATTERN, DIMENSION_NAME));
        dimensionsToJoin = (List<Map<String, Object>>) properties.getOrDefault(VALUES, new ArrayList<>());

        dimensionsToJoin.forEach(map -> {
            checkNotNull(map.get(FROM_DIMENSION), String.format(ERROR_MESSAGE_PATTERN, FROM_DIMENSION));
        });

        delimiter = (String) properties.getOrDefault(DELIMITER, "-");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        KeyValue<String, Map<String, Object>> returnedValue = new KeyValue<>(key, new HashMap<>());

        if (value != null && newDimension != null) {

            String joined = dimensionsToJoin.stream()
                    .map(m ->
                    {
                        String objectValue = (boolean) m.getOrDefault(DELETE, false) ?
                                (String) value.remove(m.get(FROM_DIMENSION)) : (String) value.get(m.get(FROM_DIMENSION));
                        return (objectValue != null ? objectValue : (String) m.get(OR_DEFAULT));
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(delimiter));

            value.put(newDimension, joined);
            returnedValue = new KeyValue<>(key, value);
        }

        return returnedValue;
    }

    @Override
    public void stop() {

    }
}
