package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;


public class ArrayDecompositionMapper extends MapperFunction {

    public static final String DIMENSION_TO_BUILD = "dimensionToBuild";
    public static final String DIMENSION = "dimension";
    public static final String DELETE_DIMENSION = "deleteDimension";

    private final String ERROR_MESSAGE_PATTERN = "%s cannot be null";


    List<String> dimensionToBuild;
    String dimension;
    Boolean deleteDimension;


    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {

        dimension = (String) properties.get(DIMENSION);
        dimensionToBuild = (List<String>) properties.get(DIMENSION_TO_BUILD);
        deleteDimension = (Boolean) properties.getOrDefault(DELETE_DIMENSION, false);

        checkNotNull(dimension, "You need to set "+ DIMENSION);
        checkNotNull(dimensionToBuild, "You need to set " + DIMENSION_TO_BUILD);
    }


    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
      
        if (value != null) {

            Map<String, Object> newEvent = new HashMap<>(value);

            List<String> dimensionArrayValue = deleteDimension ?
                    (List<String>) newEvent.remove(dimension): (List<String>) newEvent.get(dimension);

            if(dimensionArrayValue != null) {
                Integer maxGets = dimensionArrayValue.size();
                Integer currentGets = 0;

                for(String dimToBuild : dimensionToBuild) {
                    if(currentGets < maxGets) {
                        Object currentDimValue = dimensionArrayValue.get(currentGets);
                        newEvent.put(dimToBuild, currentDimValue);
                        currentGets++;
                    }
                }
            }

            return new KeyValue<>(key, newEvent);
        } else {
            return new KeyValue<>(key, null);
        }
    }

    @Override
    public void stop() {

    }
}
