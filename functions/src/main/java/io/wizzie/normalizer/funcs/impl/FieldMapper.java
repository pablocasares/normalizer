package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;


public class FieldMapper extends MapperFunction {

    public static final String VALUE = "value";
    public static final String DIMENSION = "dimension";
    public static final String DIMENSIONS = "dimensions";
    public static final String OVERWRITE = "overwrite";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";


    List<Map<String, Object>> dimensionsToAdd;


    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {

        dimensionsToAdd = (List<Map<String, Object>>) properties.getOrDefault(DIMENSIONS, new ArrayList<>());

        dimensionsToAdd.forEach(map -> {
            checkNotNull(map.get(DIMENSION), String.format(ERROR_MESSAGE_PATTERN, DIMENSION));
            checkNotNull(map.get(VALUE), String.format(ERROR_MESSAGE_PATTERN, VALUE));
        });

    }


    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
      
        if (value != null) {

            Map<String, Object> newEvent = new HashMap<>();
            newEvent.putAll(value);

            dimensionsToAdd.stream()
                    .forEach(m ->
                    {
                        if (newEvent.get(m.get(DIMENSION)) == null || (boolean) m.getOrDefault(OVERWRITE, false)) {
                            newEvent.put((String) m.get(DIMENSION), m.get(VALUE));
                        }
                    });
       
            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {

    }
}
