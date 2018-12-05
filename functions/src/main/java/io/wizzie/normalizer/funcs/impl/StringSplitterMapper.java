package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class StringSplitterMapper extends MapperFunction {
    private final Logger log = LoggerFactory.getLogger(getClass());

    String dimension;
    String delimiter;
    List<String> fieldNames;
    boolean removeDimension;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");
        delimiter = checkNotNull((String) properties.get("delimiter"), "delimiter cannot be null");
        fieldNames = checkNotNull((List<String>) properties.get("fields"), "fields cannot be null");
        removeDimension = (boolean) properties.getOrDefault("delete_dimension", false);
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if(value != null) {
            if(value.containsKey(dimension)) {
                String data = (String) (removeDimension ? value.remove(dimension) : value.get(dimension));

                String [] tokens = data.split(delimiter, fieldNames.size());

                for(int i = 0; i < tokens.length; i++)
                    value.put(fieldNames.get(i), tokens[i].trim());
            }
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {}

}
