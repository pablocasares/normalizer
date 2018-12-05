package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.util.List;
import java.util.Map;

import static io.wizzie.normalizer.base.utils.Constants.__KEY;

public class MultiValueFieldFilter extends FilterFunc {
    String dimension;
    List<Object> dimensionValues;
    Boolean isDimensionKey = false;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        dimension = (String) properties.get("dimension");
        dimensionValues = (List<Object>) properties.get("values");

        if (dimension == null || dimension.equals(__KEY)) isDimensionKey = true;

    }

    @Override
    public Boolean process(String key, Map<String, Object> value) {
        if (value != null) {
            if (isDimensionKey) {
                return dimensionValues.contains(key);
            } else {
                Object currentValue = value.get(dimension);
                return currentValue != null && dimensionValues.contains(currentValue);
            }
        } else {
            return false;
        }
    }

    @Override
    public void stop() {

    }
}
