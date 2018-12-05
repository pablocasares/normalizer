package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class ClassificationMapper extends MapperFunction {

    String dimension;
    String targetDimension;
    List<Number> intervals;
    List<String> classification;
    Number unknownValue;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = checkNotNull((String) properties.get("dimension"), "dimension cannot be null");
        targetDimension = checkNotNull((String) properties.get("new_dimension"), "new_dimension cannot be null");
        intervals = checkNotNull((List<Number>) properties.get("intervals"), "intervals cannot be null");
        classification = checkNotNull((List<String>) properties.get("classification"), "classification cannot be null");
        unknownValue = checkNotNull((Number) properties.get("unknown_value"), "unknown_value cannot be null");
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if (value != null) {
            if (value.containsKey(dimension)) {

                Number data = (Number) value.get(dimension);
                String classificationName = "not_classified";

                if (data == null || data.doubleValue() == unknownValue.doubleValue()) {
                    classificationName = "unknown";
                } else {
                    int i;
                    for (i = 0; i < intervals.size(); i++) {
                        if (data.doubleValue() <= intervals.get(i).doubleValue()) {
                            classificationName = classification.get(i);
                            break;
                        }
                    }

                    if (classificationName.equals("not_classified")) {
                        classificationName = classification.get(i);
                    }
                }
                value.put(targetDimension, classificationName);
            }
        }

        return new KeyValue<>(key, value);
    }

    @Override
    public void stop() {

    }
}
