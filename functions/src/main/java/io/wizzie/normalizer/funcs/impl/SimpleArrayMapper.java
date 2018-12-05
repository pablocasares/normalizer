package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class SimpleArrayMapper extends MapperFunction {
    private static final Logger log = LoggerFactory.getLogger(SimpleArrayMapper.class);

    String dimension;
    Map<String, Integer> dimensionToIndex;
    boolean deleteDimension = true;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");
        dimensionToIndex = checkNotNull((Map<String, Integer>) properties.get("dimensionToIndex"), "dimensionToIndex cannot be null");
        deleteDimension = (boolean) properties.getOrDefault("deleteDimension", true);
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
        Map<String, Object> newEvent = null;

        if (value != null) {
            List<Object> array = (List<Object>) value.get(dimension);
            newEvent = new HashMap<>();
            newEvent.putAll(value);

            if (array != null) {
                Integer maxIndex = array.size() - 1;

                for (Map.Entry<String, Integer> entry : dimensionToIndex.entrySet()) {
                    Integer index = entry.getValue();
                    String newDimension = entry.getKey();

                    if (index <= maxIndex) {
                        newEvent.put(newDimension, array.get(index));
                    } else {
                        log.debug(String.format("The index [%s] isn't available index. MaxIndex [%s]. Array[%s]", index, maxIndex, array));
                    }
                }

                if (deleteDimension) {
                    newEvent.remove(dimension);
                }
            } else {
                log.debug(String.format("The array dimension [%s] is null, discard it", dimension));
            }
        }

        return KeyValue.pair(key, newEvent);
    }

    @Override
    public void stop() {

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("dimension: ").append(dimension).append(", ")
                .append("dimensionToIndex: ").append(dimensionToIndex).append(", ")
                .append("deleteDimension: ").append(deleteDimension)
                .append("}");

        return builder.toString();
    }
}
